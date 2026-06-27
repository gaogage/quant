//! v19 策略历史回放 —— 复用共享 MVO 核心 `mvo_engine::simulate_v19_daily_returns`，
//! 与实盘调度器走同一套权重(真 v19 GA)/体制/杠杆逻辑，并通过 `trading` 模块
//! 落计划+实际交易，保证回放与实盘交易记录路径一致。
//!
//! POST /api/v1/quant/paper/historical-replay-v19

use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::routes::mvo_engine::{compute_metrics, simulate_v19_daily_returns};
use crate::routes::scheduler::load_strategy_config;
use crate::routes::sync::{check_paper_account_data_readiness, DataReadinessGate};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct V19ReplayRequest {
    pub paper_account_id: String,
    pub start_date: String,
    pub end_date: String,
}

pub async fn historical_replay_v19(
    State(state): State<Arc<AppState>>,
    Json(req): Json<V19ReplayRequest>,
) -> impl IntoResponse {
    match run_v19_replay(&state.db, req).await {
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

async fn run_v19_replay(db: &sqlx::PgPool, req: V19ReplayRequest) -> Result<Value, String> {
    let account_id = req.paper_account_id.trim().to_string();
    let start = parse_date(&req.start_date)?;
    let end = parse_date(&req.end_date)?;

    // 1. 账号信息（策略 + 杠杆 + 初始资金 + 强平/警告线）
    let (strategy_id, lev_enabled, lev_mult, lev_mode, init_cap, liq_thr, warn_thr): (Option<String>, bool, f64, String, Decimal, Option<f64>, Option<f64>) =
        sqlx::query_as(
            "SELECT strategy_version_id, COALESCE(leverage_enabled,false), COALESCE(leverage_multiplier,1.0),
                    COALESCE(leverage_mode,'fixed'), initial_capital, liquidation_threshold, warning_threshold
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(&account_id)
        .fetch_optional(db).await.map_err(|e| format!("query account: {e}"))?
        .ok_or("account not found")?;
    let cap_f64: f64 = init_cap.to_string().parse().unwrap_or(1_000_000.0);

    // 2. 加载策略配置（A股选股方式 + ETF + 杠杆参数都在策略里）
    let sc = load_strategy_config(db, strategy_id.as_deref().unwrap_or("v19")).await;

    // 杠杆感知 dynamic_target_cap：杠杆账号用保守 tc（避免回撤过大），无杠杆用高 tc（释放 alpha）
    // 通过 MVO_TARGET_CAP 环境变量传递给 compute_mvo_weights_for_date
    if lev_enabled {
        std::env::set_var("MVO_TARGET_CAP", "0.30");
    } else {
        std::env::set_var("MVO_TARGET_CAP", "0.80");
    }

    let readiness_report = check_paper_account_data_readiness(
        db,
        &account_id,
        Some((start, end)),
        DataReadinessGate::BlockRequiredRed,
        "paper_replay_v19",
    )
    .await?;

    // 3. 清空账号旧数据
    for table in &[
        "paper_order",
        "paper_fill",
        "paper_position",
        "paper_nav_snapshot",
        "paper_replay",
        "paper_margin_trade",
    ] {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE paper_account_id = $1",
            table
        ))
        .bind(&account_id)
        .execute(db)
        .await
        .map_err(|e| format!("clean {}: {}", table, e))?;
    }
    // 重置账号到初始状态（不改 status）
    sqlx::query("UPDATE paper_account SET current_nav=$1,peak_nav=$1,cash=$1,max_drawdown_pct=0,margin_amount=0,total_trades=0,created_at=$2,updated_at=NOW() WHERE paper_account_id=$3")
        .bind(init_cap).bind(start).bind(&account_id).execute(db).await.map_err(|e| format!("reset: {}", e))?;

    // 4. 共享核心：逐日 v19 收益（真 GA 权重 + 体制 + vol_target 杠杆）
    let daily = simulate_v19_daily_returns(
        db,
        &sc,
        start,
        end,
        lev_enabled,
        lev_mult,
        &lev_mode,
        liq_thr,
        warn_thr,
    )
    .await?;
    let (daily, _rebalances) = daily;
    if daily.is_empty() {
        return Err("v19 模拟无有效交易日".into());
    }

    // 5. 逐日 NAV 复利累乘 → 写 paper_nav_snapshot
    let mut nav = cap_f64;
    let mut peak = cap_f64;
    let mut max_dd = 0.0f64;
    let net_rets: Vec<f64> = daily.iter().map(|d| d.net_return).collect();
    for d in &daily {
        nav *= 1.0 + d.net_return;
        if nav > peak {
            peak = nav;
        }
        let dd = if peak > 0.0 { (peak - nav) / peak } else { 0.0 };
        if dd > max_dd {
            max_dd = dd;
        }
        let cum = nav / cap_f64 - 1.0;
        let nav_dec = Decimal::from_f64_retain(nav).unwrap_or(init_cap);
        let sid = format!("ns-{}", uuid::Uuid::new_v4());
        sqlx::query(
            "INSERT INTO paper_nav_snapshot (nav_snapshot_id,paper_account_id,snapshot_date,nav,cash,market_value,position_count,daily_return,cumulative_return,max_drawdown)
             VALUES ($1,$2,$3,$4,0,$4,0,$5,$6,$7) ON CONFLICT DO NOTHING",
        )
        .bind(&sid).bind(&account_id).bind(d.date).bind(nav_dec)
        .bind(Decimal::from_f64_retain(d.net_return).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64_retain(cum).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64_retain(dd).unwrap_or(Decimal::ZERO))
        .execute(db).await.ok();
    }

    // 6. 绩效指标
    let m = compute_metrics(&net_rets);
    let final_nav = nav;

    // 7. 末期持仓展示（通过 trading 模块落计划+实际交易，与实盘同路径）
    //    末日杠杆决定融资金额：持仓 = nav × lev，融资 margin = nav × (lev-1)
    let last_lev = daily.last().map(|d| d.leverage).unwrap_or(1.0);
    let total_trades = build_final_positions(db, &account_id, &sc, end, final_nav, last_lev)
        .await
        .unwrap_or(0);

    // 8. 逐年收益
    let yearly = compute_yearly(&daily);

    // 9. 更新账号最终状态
    sqlx::query("UPDATE paper_account SET current_nav=$1,total_trades=$2,max_drawdown_pct=$3,updated_at=NOW() WHERE paper_account_id=$4")
        .bind(Decimal::from_f64_retain(final_nav).unwrap_or(init_cap))
        .bind(total_trades as i64).bind(max_dd * 100.0).bind(&account_id).execute(db).await.ok();

    // 10. 写 paper_replay
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
    .bind(m.trading_days as i32).bind(&yearly)
    .execute(db).await.map_err(|e| format!("save replay: {}", e))?;

    Ok(json!({
        "replay_id": rid,
        "strategy_id": sc.strategy_id,
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

/// 末期持仓展示：按 v19 末期 MVO 权重 + 体制，把 A股个股 + ETF 落成计划+实际交易和持仓。
/// 与实盘 sync_positions_from_backtest 一致地经 trading 模块记录，可在账号详情追溯。
async fn build_final_positions(
    db: &sqlx::PgPool,
    account_id: &str,
    sc: &crate::routes::scheduler::StrategyConfig,
    date: NaiveDate,
    total_nav: f64,
    leverage: f64,
) -> Result<usize, String> {
    use crate::routes::scheduler::{compute_mvo_weights_for_date, detect_regime_exposure};
    use crate::routes::trading::{execute_simulated_trade, update_current_nav, PlannedTrade};

    let weights = compute_mvo_weights_for_date(db, date, sc).await;
    let regime = detect_regime_exposure(db, date).await;
    if weights.is_empty() {
        return Ok(0);
    }

    // 杠杆建模：总购买力 = 净值 × 末日杠杆；融资 margin = 净值 × (lev-1)
    let lev = leverage.max(1.0);
    let invest_base = total_nav * lev;

    // 资产暴露 = weight × regime（现金 = 1 - regime）
    let a_pct = weights[0] * regime;

    // A股个股：取该策略权益曲线对应回测任务的末期持仓，按 a_pct 分配
    let a_positions = sqlx::query_as::<_, (String, Decimal, Decimal)>(
        "SELECT symbol, COALESCE(quantity,0), COALESCE(market_value,0)
         FROM backtest_position
         WHERE task_id = $1 AND position_date = (SELECT MAX(position_date) FROM backtest_position WHERE task_id = $1)
           AND quantity > 0 AND market_value > 0
         ORDER BY market_value DESC",
    ).bind(&sc.equity_curve_task_id).fetch_all(db).await.unwrap_or_default();

    let total_a_mv: f64 = a_positions
        .iter()
        .map(|(_, _, mv)| mv.to_string().parse::<f64>().unwrap_or(0.0))
        .sum();
    let a_capital = invest_base * a_pct;
    let a_scale = if total_a_mv > 0.0 {
        a_capital / total_a_mv
    } else {
        0.0
    };

    let mut n = 0usize;
    let mut placed_mv = 0.0f64;
    for (sym, qty, mv) in &a_positions {
        let q = qty.to_string().parse::<f64>().unwrap_or(0.0);
        let m = mv.to_string().parse::<f64>().unwrap_or(0.0);
        if q <= 0.0 || m <= 0.0 {
            continue;
        }
        let price = m / q;
        let sq = q * a_scale;
        let sm = m * a_scale;
        if sm < 1.0 {
            continue;
        }
        let trade = PlannedTrade {
            account_id: account_id.to_string(),
            symbol: sym.clone(),
            side: "buy".into(),
            target_quantity: Decimal::from_f64_retain(sq).unwrap_or(Decimal::ZERO),
            target_price: Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO),
            price_upper_limit: None,
            price_lower_limit: None,
            slippage_pct: 0.0,
            target_value: Decimal::from_f64_retain(sm).unwrap_or(Decimal::ZERO),
            reason: Some(format!("v19回放末期持仓 A股 regime={:.0}%", regime * 100.0)),
            strategy_version_id: Some("phase7-professional-v1".to_string()),
        };
        if execute_simulated_trade(db, &trade).await.is_ok() {
            upsert_position(db, account_id, sym, sq, price, sm).await;
            placed_mv += sm;
            n += 1;
        }
    }

    // ETF：weights[1..] × regime × total_nav，末期收盘价取 daily_bar_adj
    for (i, etf) in sc.etf_symbols.iter().enumerate() {
        let w = weights.get(i + 1).copied().unwrap_or(0.0) * regime;
        let val = invest_base * w;
        if val < 1.0 {
            continue;
        }
        let price: Option<f64> = sqlx::query_scalar::<_, f64>(
            "SELECT close::double precision FROM market_stock_daily_bar_adj WHERE symbol=$1 AND trade_date<=$2 ORDER BY trade_date DESC LIMIT 1",
        ).bind(etf).bind(date).fetch_optional(db).await.ok().flatten();
        let Some(price) = price.filter(|p| *p > 0.0) else {
            continue;
        };
        let qty = val / price;
        let trade = PlannedTrade {
            account_id: account_id.to_string(),
            symbol: etf.clone(),
            side: "buy".into(),
            target_quantity: Decimal::from_f64_retain(qty).unwrap_or(Decimal::ZERO),
            target_price: Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO),
            price_upper_limit: None,
            price_lower_limit: None,
            slippage_pct: 0.0,
            target_value: Decimal::from_f64_retain(val).unwrap_or(Decimal::ZERO),
            reason: Some(format!("v19回放末期持仓 ETF w={:.1}%", w * 100.0)),
            strategy_version_id: Some("phase7-professional-v1".to_string()),
        };
        if execute_simulated_trade(db, &trade).await.is_ok() {
            upsert_position(db, account_id, etf, qty, price, val).await;
            placed_mv += val;
            n += 1;
        }
    }

    // 融资金额 = 净值 × (杠杆-1)；现金 = 总购买力 - 已建仓市值
    let margin = (total_nav * (lev - 1.0)).max(0.0);
    let cash = (invest_base - placed_mv).max(0.0);
    sqlx::query("UPDATE paper_account SET cash=$1, margin_amount=$2 WHERE paper_account_id=$3")
        .bind(Decimal::from_f64_retain(cash).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64_retain(margin).unwrap_or(Decimal::ZERO))
        .bind(account_id)
        .execute(db)
        .await
        .ok();

    // 融资流水追溯：margin>0 时补一条 borrow 流水（账户余额已上方直接设定，此处仅落流水不重复改账户）
    if margin > 0.0 {
        let mt_id = format!(
            "mt-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        );
        sqlx::query(
            "INSERT INTO paper_margin_trade (margin_trade_id, paper_account_id, side, amount, reason)
             VALUES ($1, $2, 'borrow', $3, 'v19回放杠杆建仓融资')",
        )
        .bind(&mt_id).bind(account_id)
        .bind(Decimal::from_f64_retain(margin).unwrap_or(Decimal::ZERO))
        .execute(db).await.ok();
    }

    let _ = update_current_nav(db, account_id).await;
    Ok(n)
}

async fn upsert_position(
    db: &sqlx::PgPool,
    account_id: &str,
    sym: &str,
    qty: f64,
    price: f64,
    mv: f64,
) {
    let pid = format!(
        "pp-{}",
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
    );
    let q = Decimal::from_f64_retain(qty).unwrap_or(Decimal::ZERO);
    let p = Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO);
    let m = Decimal::from_f64_retain(mv).unwrap_or(Decimal::ZERO);
    sqlx::query(
        "INSERT INTO paper_position (paper_position_id,paper_account_id,symbol,quantity,avg_cost,market_price,market_value)
         VALUES ($1,$2,$3,$4,$5,$5,$6)
         ON CONFLICT (paper_account_id,symbol) DO UPDATE SET quantity=EXCLUDED.quantity,market_price=EXCLUDED.market_price,market_value=EXCLUDED.market_value,avg_cost=EXCLUDED.avg_cost",
    ).bind(&pid).bind(account_id).bind(sym).bind(q).bind(p).bind(m).execute(db).await.ok();
}

/// 逐年收益（按 net_return 复利聚合）→ jsonb 数组
fn compute_yearly(daily: &[crate::routes::mvo_engine::DailyReturn]) -> Value {
    let mut out: Vec<Value> = Vec::new();
    let mut cur_year = 0i32;
    let mut yr_nav = 1.0f64;
    for d in daily {
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
    Value::Array(out)
}
