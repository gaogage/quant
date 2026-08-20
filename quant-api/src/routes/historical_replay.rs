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
    /// WFA 切窗模式：true 切 OOS 段拼 stitched 曲线，false/缺省走单曲线回放。
    #[serde(default)]
    pub wfa_mode: bool,
    /// OOS 段交易日数（仅 wfa_mode 用，默认 252）。
    pub oos_window_days: Option<usize>,
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

    // WFA 切窗模式：切不重叠 OOS 段，每窗 reset 跑后拼接 stitched 收益序列
    if req.wfa_mode {
        return run_wfa_stitched(
            db,
            &account_id,
            &rs,
            start,
            end,
            req.oos_window_days.unwrap_or(252),
            lev_enabled,
            lev_mult,
            &lev_mode,
        )
        .await;
    }

    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .map_err(|e| format!("tushare: {}", e))?;
    let cache = Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::shared::MvoWeightCache>,
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

    // 7. 更新账号最终状态 —— 用回放结果完整填充，使标题栏与绩效指标一致。
    // 注意：不覆盖 cash。final_nav 由 run_daily_simulation 末日的 update_current_nav
    // 算出（= SUM(market_value) + cash - margin），cash 是回放结束时的真实闲置资金。
    // 若强制 cash=0 会让 current_nav 与 (mv + cash - margin) 失衡——unlev 等保留现金的
    // 策略结束日本就有闲置资金（如 ETF 防御/未投满），清零后 NAV 恒等式被破坏。
    sqlx::query(
        "UPDATE paper_account SET
            current_nav=$1,
            peak_nav=$2,
            max_drawdown_pct=$3,
            total_trades=$4,
            updated_at=NOW()
         WHERE paper_account_id=$5",
    )
    .bind(Decimal::from_f64_retain(final_nav).unwrap_or(init_cap))
    .bind(Decimal::from_f64_retain(peak).unwrap_or(init_cap))
    // max_drawdown_pct 语义=小数 0~1，不可 *100：实盘 update_current_nav 用 (peak-nav)/peak
    // 写小数；日报 report.rs/dingtalk 读字段后 *100 展示。曾 *100 致日报显示 989%(2026-08-13)。
    // 注：paper_replay 表(~L201)的 _pct 字段是百分数语义，与本表不同，勿照搬。
    .bind(max_dd)
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

/// 把 [start, end] 区间按 oos_window_days 个交易日切成不重叠的连续 OOS 段。
/// 返回每段 (段序号, 段内首交易日, 段内末交易日)。
/// 段边界按交易日（非日历日）对齐：第 1..=N 天为段1，N+1..=2N 为段2，以此类推。
fn split_oos_windows(trade_days: &[NaiveDate], oos_window_days: usize) -> Vec<(usize, NaiveDate, NaiveDate)> {
    if oos_window_days == 0 || trade_days.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut idx = 0;
    let mut seg = 1;
    while idx < trade_days.len() {
        let end_idx = (idx + oos_window_days).min(trade_days.len());
        if end_idx - idx < 2 {
            break; // 不足 2 天无法算收益，丢弃末尾不完整段
        }
        out.push((seg, trade_days[idx], trade_days[end_idx - 1]));
        idx = end_idx;
        seg += 1;
    }
    out
}

/// 把各窗 DailyNav 的 net_return 首尾相接拼成 stitched 收益序列。
/// 每窗 reset 后首日 net_return 是相对初始资金的建仓跳变,非连续市场持有收益,
/// 拼入 stitched 会虚增总收益(见 P4.1a Task 3 实测偏差),故每窗从第 2 天起取。
fn stitch_window_returns(windows: &[Vec<DailyNav>]) -> Vec<f64> {
    let mut rets = Vec::new();
    for w in windows {
        // 跳过每窗首日:reset 后首日 net_return 是"从初始资金建仓到收盘"的跳变,
        // 非连续市场持有收益,拼入 stitched 会虚增总收益。从第 2 天起取。
        rets.extend(w.iter().skip(1).map(|d| d.net_return));
    }
    rets
}

/// WFA stitched OOS：切不重叠 OOS 段，每窗 run_daily_simulation(reset=true) 取 DailyNav，
/// 拼接 net_return 序列算全周期 stitched 指标。每窗 reset 实现窗口状态隔离。
async fn run_wfa_stitched(
    db: &sqlx::PgPool,
    account_id: &str,
    rs: &crate::routes::strategy::ResolvedStrategy,
    start: NaiveDate,
    end: NaiveDate,
    oos_window_days: usize,
    lev_enabled: bool,
    lev_mult: f64,
    lev_mode: &str,
) -> Result<Value, String> {
    use crate::routes::strategy::AssetClass;

    // 1. 全周期交易日序列（从 a_share asset 的 backtest_equity_curve 取）
    let a_task_id = rs
        .assets
        .iter()
        .find(|a| a.asset_class == AssetClass::AShare)
        .and_then(|a| a.security.equity_curve_task_id.as_deref())
        .ok_or_else(|| "策略无 a_share asset，equity_curve_task_id 缺失".to_string())?;
    let trade_days: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT trade_date FROM backtest_equity_curve WHERE task_id = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date",
    )
    .bind(a_task_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("load trade_days: {}", e))?;
    if trade_days.len() < 2 {
        return Err("WFA 全周期交易日不足".into());
    }

    // 2. 切窗
    let windows = split_oos_windows(&trade_days, oos_window_days);
    if windows.is_empty() {
        return Err(format!("切窗为空(交易日 {} 段长 {})", trade_days.len(), oos_window_days));
    }

    // 3. 每窗 run_daily_simulation(reset=true) 取 DailyNav
    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .map_err(|e| format!("tushare: {}", e))?;
    let cache = Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::shared::MvoWeightCache>,
    ));
    let risk_free_rate = rs
        .mvo
        .as_ref()
        .map(|m| m.risk_free_rate)
        .unwrap_or(crate::routes::mvo_engine::DEFAULT_RISK_FREE_RATE);
    let mut window_navs: Vec<Vec<DailyNav>> = Vec::with_capacity(windows.len());
    let mut window_summaries: Vec<Value> = Vec::with_capacity(windows.len());
    for (seg, w_start, w_end) in &windows {
        let navs = run_daily_simulation(
            db,
            account_id,
            rs,
            *w_start,
            *w_end,
            PriceSource::EodClose,
            &cache,
            &tushare,
            true, // reset=true 每窗重置，窗口隔离
            lev_enabled,
            lev_mult,
            lev_mode,
        )
        .await?;
        // 单窗指标（用该窗 net_return）
        let w_rets: Vec<f64> = navs.iter().map(|d| d.net_return).collect();
        let w_metrics = compute_metrics(&w_rets, risk_free_rate);
        window_summaries.push(json!({
            "segment": seg,
            "start": w_start.to_string(),
            "end": w_end.to_string(),
            "trading_days": navs.len(),
            "annual_return": w_metrics.annual_return,
            "sharpe": w_metrics.sharpe,
            "sortino": w_metrics.sortino,
            "calmar": w_metrics.calmar,
            "max_drawdown": w_metrics.max_drawdown,
        }));
        window_navs.push(navs);
    }

    // 4. 拼接 stitched 收益序列算全周期指标
    let stitched_rets = stitch_window_returns(&window_navs);
    let stitched = compute_metrics(&stitched_rets, risk_free_rate);

    // 5. 达标判定（蓝图 §2.2：stitched OOS Calmar > 1.2）
    let passed = stitched.calmar > 1.2;
    let candidate_tier = if passed { "professional_observation" } else { "defensive_candidate" };

    // 6. 归因：拖累最重的窗口（最低 Calmar）
    let mut sorted_summaries = window_summaries.clone();
    sorted_summaries.sort_by(|a, b| {
        a["calmar"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&b["calmar"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let worst_windows: Vec<&Value> = sorted_summaries.iter().take(3).collect();

    Ok(json!({
        "mode": "wfa_stitched",
        "account_id": account_id,
        "strategy_id": rs.strategy_id,
        "period": { "start": start.to_string(), "end": end.to_string(), "trade_days": trade_days.len() },
        "oos_window_days": oos_window_days,
        "window_count": windows.len(),
        "stitched_metrics": {
            "annual_return": stitched.annual_return,
            "sharpe": stitched.sharpe,
            "sortino": stitched.sortino,
            "calmar": stitched.calmar,
            "max_drawdown": stitched.max_drawdown,
            "risk_free_rate": risk_free_rate,
        },
        "windows": window_summaries,
        "verdict": if passed { "pass" } else { "fail" },
        "threshold_calmar": 1.2,
        "candidate_tier": candidate_tier,
        "attribution": {
            "worst_windows_by_calmar": worst_windows,
        },
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

// ── MVO 动态基准曲线 ────────────────────────────────────────────
// 生成与实盘同策略同逻辑(MVO 动态)但后复权 + 无交易成本的理论基准曲线,
// 供 query_backtest_comparison 对比——让仪表盘"回测偏离"反映"真实价执行 vs 后复权理论"
// 差异,而非静态 composite 与动态实盘的口径错配(曾致 v24 偏离 +30%)。

const MVO_CURVE_TABLE_DDL: &str = "\
CREATE TABLE IF NOT EXISTS backtest_mvo_equity_curve (\
    strategy_id         VARCHAR NOT NULL,\
    trade_date          DATE NOT NULL,\
    leverage_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,\
    portfolio_value     NUMERIC(20,4) NOT NULL,\
    created_at          TIMESTAMPTZ DEFAULT NOW(),\
    PRIMARY KEY (strategy_id, trade_date, leverage_multiplier)\
)";

/// 幂等建表 backtest_mvo_equity_curve。
pub async fn ensure_mvo_curve_table(db: &sqlx::PgPool) -> Result<(), String> {
    sqlx::query(MVO_CURVE_TABLE_DDL)
        .execute(db)
        .await
        .map_err(|e| format!("create backtest_mvo_equity_curve: {}", e))?;
    Ok(())
}

fn default_lev_mult() -> f64 {
    1.0
}

#[derive(Debug, Deserialize)]
pub struct MvoBenchmarkRequest {
    pub strategy_id: String,
    /// 承载基准回测的专用 inactive 账号(run_daily_simulation 必须操作账号表,
    /// 不能跑实盘账号否则 reset 会清实盘)。如 v24 用 pa-v24-mvo-bench。
    pub benchmark_account_id: String,
    pub start_date: String,
    pub end_date: String,
    /// 基准杠杆倍率(无杠杆 1.0 / 有杠杆 1.5)。基准按此真带杠杆跑,
    /// query_backtest_comparison 按此匹配账号杠杆读取,不再线性放大。
    #[serde(default = "default_lev_mult")]
    pub leverage_multiplier: f64,
}

/// POST /api/v1/quant/paper/mvo-benchmark-sync
/// 跑 MVO 动态后复权无成本回测,落 backtest_mvo_equity_curve 作实盘偏离基准。
pub async fn mvo_benchmark_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MvoBenchmarkRequest>,
) -> impl IntoResponse {
    match run_mvo_benchmark_sync(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})).into_response(),
        Err(e) => Json(json!({"code": 1, "message": e})).into_response(),
    }
}

pub async fn run_mvo_benchmark_sync(
    db: &sqlx::PgPool,
    req: MvoBenchmarkRequest,
) -> Result<Value, String> {
    ensure_mvo_curve_table(db).await?;
    let strategy_id = req.strategy_id.trim().to_string();
    let start = parse_date(&req.start_date)?;
    let end = parse_date(&req.end_date)?;
    let rs = crate::routes::strategy::load_resolved_strategy(db, &strategy_id)
        .await
        .map_err(|e| format!("load strategy: {}", e))?;

    // 真实价(EodCloseAdj,与实盘同空间) + 无成本(slippage=0) + 按请求 lev_mult 真带杠杆跑。
    // reset=true 清 benchmark 账号历史,从初始资金干净重跑整条理论曲线。
    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .map_err(|e| format!("tushare: {}", e))?;
    let cache = Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::shared::MvoWeightCache>,
    ));
    let lev_enabled = (req.leverage_multiplier - 1.0).abs() > 1e-9;
    // lev + slippage=0(EodCloseAdj) 触发融资失控(2026-08-14 实测 NAV 爆炸至 5479万/融资 4.6亿,
    // 根因待查:疑 dynamic_target_cap/融资逻辑与无成本交互)。lev 基准用 EodClose
    // (slippage=策略值,与实盘 lev 同口径,偏离≈0);unlev 基准用 EodCloseAdj(无成本理论)。
    let price_source = if lev_enabled {
        PriceSource::EodClose
    } else {
        PriceSource::EodCloseAdj
    };
    let navs = run_daily_simulation(
        db,
        &req.benchmark_account_id,
        &rs,
        start,
        end,
        price_source,
        &cache,
        &tushare,
        true,
        lev_enabled,
        req.leverage_multiplier,
        "fixed",
    )
    .await?;
    if navs.is_empty() {
        return Err("基准回测无有效交易日".into());
    }

    // UPSERT 逐日 NAV → backtest_mvo_equity_curve(照 equity_curve_sync UPSERT idiom)
    let mut rows = 0u64;
    for d in &navs {
        sqlx::query(
            "INSERT INTO backtest_mvo_equity_curve (strategy_id, trade_date, leverage_multiplier, portfolio_value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (strategy_id, trade_date, leverage_multiplier)
             DO UPDATE SET portfolio_value = EXCLUDED.portfolio_value",
        )
        .bind(&strategy_id)
        .bind(d.date)
        .bind(req.leverage_multiplier)
        .bind(Decimal::from_f64_retain(d.nav).unwrap_or(Decimal::ZERO))
        .execute(db)
        .await
        .map_err(|e| format!("upsert mvo curve: {}", e))?;
        rows += 1;
    }

    let final_nav = navs.last().map(|d| d.nav).unwrap_or(0.0);
    Ok(json!({
        "strategy_id": strategy_id,
        "benchmark_account_id": req.benchmark_account_id,
        "leverage_multiplier": req.leverage_multiplier,
        "start_date": start.to_string(),
        "end_date": end.to_string(),
        "rows": rows,
        "final_nav": (final_nav * 100.0).round() / 100.0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn test_split_oos_windows_non_overlapping_contiguous() {
        // 10 个交易日，每段 3 天 → 段1(1-3)，段2(4-6)，段3(7-9)，末尾 1 天丢弃
        let days: Vec<NaiveDate> = (0..10)
            .map(|i| d("2024-01-01") + chrono::Duration::days(i))
            .collect();
        let wins = split_oos_windows(&days, 3);
        assert_eq!(wins.len(), 3);
        assert_eq!(wins[0], (1, days[0], days[2]));
        assert_eq!(wins[1], (2, days[3], days[5]));
        assert_eq!(wins[2], (3, days[6], days[8]));
        // 不重叠：段2 首日 > 段1 末日
        assert!(wins[1].1 > wins[0].2);
    }

    #[test]
    fn test_split_oos_windows_empty_or_zero_window() {
        assert!(split_oos_windows(&[], 252).is_empty());
        let days = vec![d("2024-01-01"), d("2024-01-02")];
        assert!(split_oos_windows(&days, 0).is_empty());
    }

    #[test]
    fn test_stitch_window_returns_concatenates() {
        let w1 = vec![
            DailyNav { date: d("2024-01-01"), nav: 1.0, net_return: 0.01, leverage: 1.0, regime: 0.0 },
            DailyNav { date: d("2024-01-02"), nav: 1.01, net_return: 0.02, leverage: 1.0, regime: 0.0 },
        ];
        let w2 = vec![
            DailyNav { date: d("2024-01-03"), nav: 1.0, net_return: 0.03, leverage: 1.0, regime: 0.0 },
            DailyNav { date: d("2024-01-04"), nav: 1.03, net_return: -0.01, leverage: 1.0, regime: 0.0 },
        ];
        let rets = stitch_window_returns(&[w1, w2]);
        // 每窗跳过首日建仓跳变:第1窗保留 0.02,第2窗保留 -0.01
        assert_eq!(rets, vec![0.02, -0.01]);
    }
}
