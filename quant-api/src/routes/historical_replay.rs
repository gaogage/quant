//! 策略历史回放 —— 复用共享 MVO 核心 `mvo_engine::run_daily_simulation`，
//! 与在线模拟/实盘走同一套逐日盯市 NAV 复利（策略权重 + 体制 + 杠杆），
//! run_daily_simulation(reset=true) 内部清表/重置账号 + 写 snapshot + 落交易。
//!
//! POST /api/v1/quant/paper/historical-replay

use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::routes::mvo_engine::{compute_metrics, run_daily_simulation, DailyNav};
use crate::routes::rebalance::PriceSource;
use crate::routes::sync::{check_paper_account_data_readiness, DataReadinessGate};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct HistoricalReplayRequest {
    pub paper_account_id: String,
    pub start_date: String,
    pub end_date: String,
}

pub async fn historical_replay(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HistoricalReplayRequest>,
) -> impl IntoResponse {
    match run_historical_replay(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})).into_response(),
        Err(e) => Json(json!({"code": 1, "message": e})).into_response(),
    }
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let s = s.trim();
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y%m%d") {
        return Ok(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d);
    }
    Err(format!("日期格式错误: {} (需要 YYYYMMDD 或 YYYY-MM-DD)", s))
}

async fn run_historical_replay(db: &sqlx::PgPool, req: HistoricalReplayRequest) -> Result<Value, String> {
    let account_id = req.paper_account_id.trim().to_string();
    let start = parse_date(&req.start_date)?;
    let end = parse_date(&req.end_date)?;

    // 1. 账号信息（策略 + 杠杆 + 初始资金）
    let (strategy_id, lev_enabled, lev_mult, lev_mode, init_cap): (Option<String>, bool, f64, String, Decimal) =
        sqlx::query_as(
            "SELECT strategy_version_id, COALESCE(leverage_enabled,false), COALESCE(leverage_multiplier,1.0),
                    COALESCE(leverage_mode,'fixed'), initial_capital
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(&account_id)
        .fetch_optional(db).await.map_err(|e| format!("query account: {e}"))?
        .ok_or("account not found")?;
    let cap_f64: f64 = init_cap.to_string().parse().unwrap_or(1_000_000.0);

    // 2. 加载策略配置（A股选股方式 + ETF + 杠杆参数都在策略里）
    // 账号挂的 strategy_version_id 决定策略；杠杆参数读账号 leverage_* 字段。
    let sid = strategy_id.as_deref().ok_or("账号未挂策略（strategy_version_id 空）")?;
    let rs = crate::routes::strategy::load_resolved_strategy(db, sid)
        .await
        .map_err(|e| format!("load strategy: {}", e))?;

    let readiness_report = check_paper_account_data_readiness(
        db,
        &account_id,
        Some((start, end)),
        DataReadinessGate::BlockRequiredRed,
        "paper_replay",
    )
    .await?;

    // 3. 共享逐日模拟（reset=true，EodClose）——盯市 NAV 复利，内部清表/重置账号 + 写 snapshot
    //    run_daily_simulation 内部已做：清 paper_order/fill/position/nav_snapshot/replay/margin_trade
    //    + 重置账号 current_nav/peak_nav/cash/max_drawdown/margin/total_trades。
    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .map_err(|e| format!("tushare: {}", e))?;
    let cache = Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::scheduler::MvoWeightCache>,
    ));
    let navs = run_daily_simulation(
        db,
        &account_id,
        &rs,
        start,
        end,
        PriceSource::EodClose,
        &cache,
        &tushare,
        true,
        lev_enabled,
        lev_mult,
        &lev_mode,
    )
    .await?;
    if navs.is_empty() {
        return Err("回放无有效交易日".into());
    }

    // 4. 绩效指标（基于 navs 的 net_return）
    let net_rets: Vec<f64> = navs.iter().map(|d| d.net_return).collect();
    let m = compute_metrics(&net_rets, rs.mvo.as_ref().unwrap().risk_free_rate);
    let final_nav = navs.last().unwrap().nav;
    // 峰值与最大回撤从 navs 推导（替代旧累乘循环里的 peak/max_dd）
    let peak = navs.iter().map(|d| d.nav).fold(cap_f64, f64::max);
    let max_dd = {
        let mut p = cap_f64;
        let mut dd = 0.0f64;
        for d in &navs {
            if d.nav > p {
                p = d.nav;
            }
            let cur = if p > 0.0 { (p - d.nav) / p } else { 0.0 };
            if cur > dd {
                dd = cur;
            }
        }
        dd
    };

    // 5. 末期交易笔数（run_daily_simulation 内部已落交易与持仓，这里仅统计）
    let total_trades: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM paper_order WHERE paper_account_id = $1",
    )
    .bind(&account_id)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // 6. 逐年收益
    let yearly = compute_yearly_from_navs(&navs);

    // 7. 更新账号最终状态 —— 用回放结果完整填充，使标题栏与绩效指标一致
    sqlx::query(
        "UPDATE paper_account SET
            current_nav=$1,
            peak_nav=$2,
            cash=0,
            max_drawdown_pct=$3,
            total_trades=$4,
            updated_at=NOW()
         WHERE paper_account_id=$5",
    )
    .bind(Decimal::from_f64_retain(final_nav).unwrap_or(init_cap))
    .bind(Decimal::from_f64_retain(peak).unwrap_or(init_cap))
    .bind(max_dd * 100.0)
    .bind(total_trades)
    .bind(&account_id)
    .execute(db)
    .await
    .ok();

    // 8. 删除旧回放记录，确保每个账号只有一条回放
    sqlx::query("DELETE FROM paper_replay WHERE paper_account_id = $1")
        .bind(&account_id)
        .execute(db)
        .await
        .ok();

    // 9. 写 paper_replay
    let rid = format!(
        "rp-{}",
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
    );
    sqlx::query(
        "INSERT INTO paper_replay (replay_id,paper_account_id,start_date,end_date,annual_return_pct,cumulative_return_pct,sharpe_ratio,sortino_ratio,calmar_ratio,max_drawdown_pct,volatility_pct,win_rate_pct,trading_days,yearly_returns)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
    )
    .bind(&rid).bind(&account_id).bind(start).bind(end)
    .bind(m.annual_return * 100.0).bind(m.cumulative_return * 100.0)
    .bind(m.sharpe).bind(m.sortino).bind(m.calmar)
    .bind(m.max_drawdown * 100.0).bind(m.volatility * 100.0).bind(m.win_rate * 100.0)
    .bind(m.trading_days as i32).bind(serde_json::Value::Array(yearly))
    .execute(db).await.map_err(|e| format!("save replay: {}", e))?;

    Ok(json!({
        "replay_id": rid,
        "strategy_id": rs.strategy_id,
        "start_date": start, "end_date": end,
        "annual_return_pct": (m.annual_return * 1000.0).round() / 10.0,
        "cumulative_return_pct": (m.cumulative_return * 1000.0).round() / 10.0,
        "sharpe_ratio": (m.sharpe * 100.0).round() / 100.0,
        "sortino_ratio": (m.sortino * 100.0).round() / 100.0,
        "calmar_ratio": (m.calmar * 100.0).round() / 100.0,
        "max_drawdown_pct": (m.max_drawdown * 1000.0).round() / 10.0,
        "volatility_pct": (m.volatility * 1000.0).round() / 10.0,
        "win_rate_pct": (m.win_rate * 1000.0).round() / 10.0,
        "trading_days": m.trading_days, "total_trades": total_trades,
        "leverage": if lev_enabled { lev_mult } else { 1.0 },
        "data_readiness": readiness_report,
    }))
}

/// 逐年收益（基于 navs 的 net_return 复利聚合）→ jsonb 数组
/// 替代旧 compute_yearly(&daily)——run_daily_simulation 统一以 DailyNav 输出后口径。
fn compute_yearly_from_navs(navs: &[DailyNav]) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let mut cur_year = 0i32;
    let mut yr_nav = 1.0f64;
    for d in navs {
        let y = d.date.year();
        if y != cur_year {
            if cur_year != 0 {
                out.push(json!({"year": cur_year.to_string(), "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0}));
            }
            cur_year = y;
            yr_nav = 1.0;
        }
        yr_nav *= 1.0 + d.net_return;
    }
    if cur_year != 0 {
        out.push(json!({"year": cur_year.to_string(), "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0}));
    }
    out
}
