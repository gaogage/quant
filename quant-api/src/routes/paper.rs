use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::routes::shared::{
    PaperAccountRepository, PaperPositionRepository, PgPaperAccountRepo, PgPaperPositionRepo,
};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct CreatePaperAccountRequest {
    pub name: String,
    pub initial_capital: f64,
    pub base_currency: Option<String>,
    pub operator: Option<String>,
    /// "simulated" (default) or "real". Simulated accounts push DingTalk notifications.
    /// Real accounts call broker API + DingTalk notifications (future).
    #[serde(default = "default_account_type")]
    pub account_type: String,
    /// DingTalk webhook URL for trade notifications (optional).
    #[serde(default)]
    pub dingtalk_webhook_url: Option<String>,
}

fn default_account_type() -> String {
    "simulated".to_string()
}

#[derive(Debug, Deserialize)]
pub struct SubmitPaperOrderRequest {
    pub paper_account_id: String,
    pub strategy_version_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: Option<String>,
    pub quantity: f64,
    pub limit_price: Option<f64>,
    pub estimated_price: Option<f64>,
    pub operator: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FillPaperOrderRequest {
    pub price: f64,
    pub quantity: Option<f64>,
    pub commission: Option<f64>,
    pub tax: Option<f64>,
    pub slippage: Option<f64>,
    pub operator: Option<String>,
    pub trace_id: Option<String>,
}

struct NormalizedAccountRequest {
    name: String,
    initial_capital: Decimal,
    base_currency: String,
    operator: Option<String>,
    account_type: String,
    dingtalk_webhook_url: Option<String>,
}

struct NormalizedOrderRequest {
    paper_account_id: String,
    strategy_version_id: Option<String>,
    symbol: String,
    side: String,
    order_type: String,
    quantity: Decimal,
    limit_price: Option<Decimal>,
    estimated_price: Option<Decimal>,
    operator: Option<String>,
    trace_id: String,
}

struct NormalizedFillRequest {
    price: Decimal,
    quantity: Option<Decimal>,
    commission: Decimal,
    tax: Decimal,
    slippage: Decimal,
    operator: Option<String>,
    trace_id: String,
}

pub async fn create_paper_account(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreatePaperAccountRequest>,
) -> impl IntoResponse {
    match create_paper_account_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn submit_paper_order(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitPaperOrderRequest>,
) -> impl IntoResponse {
    match submit_paper_order_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn fill_paper_order(
    State(state): State<Arc<AppState>>,
    Path(order_id): Path<String>,
    Json(req): Json<FillPaperOrderRequest>,
) -> impl IntoResponse {
    match fill_paper_order_inner(&state.db, &order_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn paper_account_summary(
    State(state): State<Arc<AppState>>,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    match paper_account_summary_inner(&state.db, &account_id).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn paper_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = paper_metrics_inner(&state.db)
        .await
        .unwrap_or_else(|error| {
            json!({
                "error": error
            })
        });
    Json(json!({
        "code": 0,
        "data": {
            "service": "paper-trading",
            "status": if metrics.get("error").is_some() { "degraded" } else { "ok" },
            "metrics": metrics
        }
    }))
}

// ── Daily Signal Generation for Paper Trading ──

#[derive(Debug, Deserialize)]
pub struct GeneratePaperSignalsRequest {
    pub paper_account_id: String,
    pub prediction_set_id: String,
    pub start_date: String,
    pub end_date: String,
    pub top_n: Option<usize>,
    pub max_position_pct: Option<f64>,
    pub rebalance_freq_days: Option<usize>,
}

pub async fn generate_paper_signals(
    State(state): State<Arc<AppState>>,
    Json(req): Json<GeneratePaperSignalsRequest>,
) -> impl IntoResponse {
    match generate_paper_signals_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn generate_paper_signals_inner(
    db: &sqlx::PgPool,
    req: GeneratePaperSignalsRequest,
) -> Result<Value, String> {
    let paper_account_id = req.paper_account_id.trim().to_string();
    let prediction_set_id = req.prediction_set_id.trim().to_string();
    if paper_account_id.is_empty() || prediction_set_id.is_empty() {
        return Err("paper_account_id and prediction_set_id required".into());
    }
    // Verify account exists
    let account = sqlx::query_as::<_, (String, String)>(
        "SELECT paper_account_id, status FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&paper_account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("DB error: {}", e))?
    .ok_or_else(|| format!("account not found: {}", paper_account_id))?;
    if account.1 != "active" {
        return Err(format!("account status is {}, expected active", account.1));
    }

    let start = NaiveDate::parse_from_str(&req.start_date, "%Y%m%d")
        .map_err(|e| format!("start_date: {}", e))?;
    let end = NaiveDate::parse_from_str(&req.end_date, "%Y%m%d")
        .map_err(|e| format!("end_date: {}", e))?;

    let top_n = req.top_n.unwrap_or(40);
    let max_position_pct = req.max_position_pct.unwrap_or(0.05);
    let rebalance_days = req.rebalance_freq_days.unwrap_or(20);

    // Load prediction scores
    let scores = sqlx::query_as::<_, (NaiveDate, String, f64, Option<i32>)>(
        "SELECT trade_date, symbol, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2
           AND trade_date <= $3
           AND score IS NOT NULL
         ORDER BY trade_date, score DESC",
    )
    .bind(&prediction_set_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load predictions: {}", e))?;

    if scores.is_empty() {
        return Err("no prediction scores found for the given date range".into());
    }

    let days: Vec<NaiveDate> = {
        let mut d = Vec::new();
        let mut prev: Option<NaiveDate> = None;
        for (date, _, _, _) in &scores {
            if prev != Some(*date) {
                d.push(*date);
                prev = Some(*date);
            }
        }
        d
    };

    let mut signal_days = 0usize;
    let _total_orders = 0usize;
    let mut nav_updates = 0usize;

    // Process each rebalance day
    for (window_idx, day_window) in days.windows(2).enumerate() {
        if window_idx % rebalance_days != 0 {
            continue;
        }
        let score_day = day_window[0];
        let _exec_day = day_window[1];

        // Select top-N candidates for this day
        let day_scores: Vec<&(NaiveDate, String, f64, Option<i32>)> = scores
            .iter()
            .filter(|s| s.0 == score_day)
            .take(top_n)
            .collect();
        if day_scores.len() < 5 {
            continue;
        }

        signal_days += 1;

        // Compute target weights (equal-weighted for simplicity)
        let weight = if (1.0 / day_scores.len() as f64) < max_position_pct {
            1.0 / day_scores.len() as f64
        } else {
            max_position_pct
        };
        let total_signals = day_scores.len();

        // Upsert positions based on target weights
        for (symbol, target_w) in day_scores.iter().map(|s| (&s.1, weight)) {
            let position_id = format!("pp-{}", Uuid::new_v4());
            PgPaperPositionRepo::new(db)
                .upsert_signal_position(
                    &position_id,
                    &paper_account_id,
                    symbol,
                    target_w,
                    score_day,
                )
                .await
                .map_err(|e| format!("Failed to upsert position: {}", e))?;
        }

        // R5: 统一走 upsert_nav_snapshot（原裸 SQL 5 处重复之一，初始化快照）
        let mut snap = crate::routes::shared::NavSnapshot::new(
            paper_account_id.as_str(),
            score_day,
            1_000_000.0,
        );
        snap.prediction_set_id = Some(prediction_set_id.clone());
        snap.signal_count = Some(total_signals as i32);
        crate::routes::shared::upsert_nav_snapshot(db, &snap)
            .await
            .map_err(|e| format!("Failed to insert NAV: {}", e))?;
        nav_updates += 1;
    }

    Ok(json!({
        "paper_account_id": paper_account_id,
        "prediction_set_id": prediction_set_id,
        "date_range": {"start": start.to_string(), "end": end.to_string()},
        "trading_days": days.len(),
        "signal_days": signal_days,
        "rebalance_freq_days": rebalance_days,
        "top_n": top_n,
        "nav_updates": nav_updates,
        "status": "signals_generated"
    }))
}

// ── NAV Computation ──

#[derive(Debug, Deserialize)]
pub struct ComputePaperNavRequest {
    pub paper_account_id: String,
    pub start_date: String,
    pub end_date: String,
    pub benchmark: Option<String>,
}

pub async fn compute_paper_nav(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ComputePaperNavRequest>,
) -> impl IntoResponse {
    match compute_paper_nav_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

// [价格空间标注 2026-09-17] 本函数属信号层模拟: bar_adj 后复权(总回报)价格序列,
// 不做现金分红/送转的公司行为处理(复权已含)。禁止用于账户资金/持仓口径(那是
// rebalance+mark_to_market 的 raw 空间职责), 防两空间混用。
async fn compute_paper_nav_inner(
    db: &sqlx::PgPool,
    req: ComputePaperNavRequest,
) -> Result<Value, String> {
    let account_id = req.paper_account_id.trim().to_string();
    let start = NaiveDate::parse_from_str(&req.start_date, "%Y%m%d")
        .map_err(|e| format!("start_date: {}", e))?;
    let end = NaiveDate::parse_from_str(&req.end_date, "%Y%m%d")
        .map_err(|e| format!("end_date: {}", e))?;
    let benchmark = req.benchmark.unwrap_or_else(|| "000300.SH".into());

    // Get account snapshot dates
    // signal_count 解码为 Option（2026-09-21 修复：历史快照行 36% 该列为 NULL
    // ——早期快照未记录 signal_count，按 i32 非 Option 解码遇 NULL 必炸
    // "unexpected null"，由覆盖率第五批测试暴露。值本身未被使用）
    let snapshots = sqlx::query_as::<_, (String, NaiveDate, Option<i32>)>(
        "SELECT nav_snapshot_id, snapshot_date, signal_count
         FROM paper_nav_snapshot
         WHERE paper_account_id = $1 AND snapshot_date >= $2 AND snapshot_date <= $3
         ORDER BY snapshot_date",
    )
    .bind(&account_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load NAV snapshots: {}", e))?;

    if snapshots.is_empty() {
        return Err("no NAV snapshots found for the given range".into());
    }

    // Get all symbols with positions from the first snapshot date
    let snap_dates: Vec<NaiveDate> = snapshots.iter().map(|s| s.1).collect();
    let from = snap_dates[0] - chrono::Duration::days(5);
    let to = snap_dates[snap_dates.len() - 1] + chrono::Duration::days(5);

    let position_symbols = sqlx::query_as::<_, (String,)>(
        "SELECT DISTINCT symbol FROM paper_position
         WHERE paper_account_id = $1 AND target_weight > 0
         ORDER BY symbol",
    )
    .bind(&account_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load position symbols: {}", e))?;

    if position_symbols.is_empty() {
        return Err("no positions found".into());
    }
    let symbols: Vec<String> = position_symbols.into_iter().map(|s| s.0).collect();

    // Load close prices for all position symbols
    let price_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>)>(
        "SELECT trade_date, symbol, close::double precision
         FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
           AND close IS NOT NULL
         ORDER BY symbol, trade_date",
    )
    .bind(&symbols)
    .bind(from)
    .bind(to)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load prices: {}", e))?;

    // Build price lookup
    let mut prices: HashMap<String, HashMap<NaiveDate, f64>> = HashMap::new();
    for (date, sym, close) in &price_rows {
        if let Some(c) = close {
            if *c > 0.0 {
                prices.entry(sym.clone()).or_default().insert(*date, *c);
            }
        }
    }

    // Load benchmark prices
    let bench_prices = sqlx::query_as::<_, (NaiveDate, Option<f64>)>(
        "SELECT trade_date, close::double precision
         FROM market_index_daily_bar
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(&benchmark)
    .bind(from)
    .bind(to)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load benchmark: {}", e))?;

    let bench_map: HashMap<NaiveDate, f64> = bench_prices
        .iter()
        .filter_map(|(d, c)| c.map(|v| (*d, v)))
        .filter(|(_, v)| *v > 0.0)
        .collect();

    // Get initial capital
    let capital = PgPaperAccountRepo::new(db)
        .find_initial_capital(&account_id)
        .await
        .map_err(|e| format!("DB: {}", e))?
        .ok_or("account not found")?;
    let initial_capital = capital;

    let mut prev_nav = initial_capital;
    let mut peak_nav = initial_capital;
    let mut max_dd = 0.0f64;
    let mut bench_initial: Option<f64> = None;
    let mut updated = 0usize;

    // Position weights: use the snapshot date's target weight for each symbol
    let pos_weights = sqlx::query_as::<_, (NaiveDate, String, f64)>(
        "SELECT p.last_trade_date, p.symbol, p.target_weight::double precision
         FROM paper_position p
         WHERE p.paper_account_id = $1 AND p.target_weight > 0",
    )
    .bind(&account_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Failed to load weights: {}", e))?;

    // Map snapshot_date → symbol → weight
    let mut weight_map: HashMap<NaiveDate, HashMap<String, f64>> = HashMap::new();
    for (date, sym, w) in &pos_weights {
        weight_map.entry(*date).or_default().insert(sym.clone(), *w);
    }

    for (nav_id, snap_date, _sig_count) in &snapshots {
        // Find the most recent signal date <= snapshot_date
        let signal_date = pos_weights
            .iter()
            .filter(|(d, _, _)| *d <= *snap_date)
            .map(|(d, _, _)| *d)
            .max();

        let Some(sig_date) = signal_date else {
            continue;
        };

        // Compute NAV: for each position, weight × initial_capital / close_price gives shares
        let mut market_value = 0.0f64;
        let mut pos_count = 0usize;

        if let Some(sym_weights) = weight_map.get(&sig_date) {
            for (sym, weight) in sym_weights {
                if let Some(sym_prices) = prices.get(sym) {
                    // Find the price closest to snap_date but not after
                    let price = sym_prices
                        .iter()
                        .filter(|(d, _)| **d <= *snap_date)
                        .max_by_key(|(d, _)| **d)
                        .map(|(_, p)| *p);

                    if let Some(_px) = price {
                        // Target market value = weight × NAV (approximate, using prev_nav)
                        let target_value = weight * prev_nav;
                        market_value += target_value;
                        pos_count += 1;
                    }
                }
            }
        }

        let nav = if pos_count > 0 {
            // Scale: assume cash = nav - market_value, rebalance to target weights
            let total_weight: f64 = weight_map
                .get(&sig_date)
                .map(|w| w.values().sum::<f64>())
                .unwrap_or(0.0);
            if total_weight > 0.0 {
                market_value / total_weight // NAV = total_market_value / total_weight
            } else {
                prev_nav
            }
        } else {
            prev_nav
        };

        let daily_ret = if prev_nav > 0.0 {
            nav / prev_nav - 1.0
        } else {
            0.0
        };
        let cum_ret = if initial_capital > 0.0 {
            nav / initial_capital - 1.0
        } else {
            0.0
        };

        if nav > peak_nav {
            peak_nav = nav;
        }
        let dd = if peak_nav > 0.0 {
            (peak_nav - nav) / peak_nav
        } else {
            0.0
        };
        if dd > max_dd {
            max_dd = dd;
        }

        // Benchmark return
        let bench_ret = if let Some(&b0) = bench_initial.as_ref() {
            if let Some(&bv) = bench_map.get(snap_date) {
                bv / b0 - 1.0
            } else {
                0.0
            }
        } else {
            if let Some(&bv) = bench_map.get(snap_date) {
                bench_initial = Some(bv);
            }
            0.0
        };

        sqlx::query(
            "UPDATE paper_nav_snapshot
             SET nav = $1, market_value = $2, position_count = $3,
                 daily_return = $4, cumulative_return = $5,
                 benchmark_return = $6, excess_return = $7,
                 max_drawdown = $8
             WHERE nav_snapshot_id = $9",
        )
        .bind(nav)
        .bind(market_value)
        .bind(pos_count as i32)
        .bind(daily_ret)
        .bind(cum_ret)
        .bind(bench_ret)
        .bind(cum_ret - bench_ret)
        .bind(max_dd)
        .bind(nav_id)
        .execute(db)
        .await
        .map_err(|e| format!("Failed to update NAV: {}", e))?;

        prev_nav = nav;
        updated += 1;
    }

    // Update paper_account summary
    sqlx::query(
        "UPDATE paper_account
         SET current_nav = $1, peak_nav = $2, max_drawdown_pct = $3, updated_at = now()
         WHERE paper_account_id = $4",
    )
    .bind(prev_nav)
    .bind(peak_nav)
    .bind(max_dd)
    .bind(&account_id)
    .execute(db)
    .await
    .map_err(|e| format!("Failed to update account: {}", e))?;

    Ok(json!({
        "paper_account_id": account_id,
        "nav_snapshots_updated": updated,
        "final_nav": prev_nav,
        "cumulative_return": if initial_capital > 0.0 { prev_nav / initial_capital - 1.0 } else { 0.0 },
        "peak_nav": peak_nav,
        "max_drawdown_pct": max_dd,
        "initial_capital": initial_capital,
        "status": "nav_computed"
    }))
}

// ── Full Simulation: Signals → Fills → Positions → Daily NAV ──

#[derive(Debug, Deserialize)]
pub struct SimulatePaperNavRequest {
    pub paper_account_id: String,
    pub prediction_set_id: String,
    pub start_date: String,
    pub end_date: String,
    pub top_n: Option<usize>,
    pub rebalance_freq_days: Option<usize>,
    pub benchmark: Option<String>,
    pub max_position_pct: Option<f64>,
    pub commission_pct: Option<f64>,
}

// [价格空间标注 2026-09-17] 同 compute_paper_nav: 后复权总回报模拟口径(信号层)。
pub async fn simulate_paper_nav(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SimulatePaperNavRequest>,
) -> impl IntoResponse {
    match simulate_paper_nav_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn simulate_paper_nav_inner(
    db: &sqlx::PgPool,
    req: SimulatePaperNavRequest,
) -> Result<Value, String> {
    let account_id = req.paper_account_id.trim().to_string();
    let pred_id = req.prediction_set_id.trim().to_string();
    let start = NaiveDate::parse_from_str(&req.start_date, "%Y%m%d")
        .map_err(|e| format!("start_date: {}", e))?;
    let end = NaiveDate::parse_from_str(&req.end_date, "%Y%m%d")
        .map_err(|e| format!("end_date: {}", e))?;
    let top_n = req.top_n.unwrap_or(40);
    let reb_days = req.rebalance_freq_days.unwrap_or(20);
    let benchmark = req.benchmark.unwrap_or_else(|| "000300.SH".into());
    let max_pos = req.max_position_pct.unwrap_or(0.05);
    let commission = req.commission_pct.unwrap_or(0.0003);

    // Load account
    let capital = PgPaperAccountRepo::new(db)
        .find_initial_capital(&account_id)
        .await
        .map_err(|e| format!("DB: {}", e))?
        .ok_or("account not found")?;
    let initial = capital;

    // Load prediction scores grouped by date
    let scores = sqlx::query_as::<_, (NaiveDate, String, f64)>(
        "SELECT trade_date, symbol, score FROM model_prediction
         WHERE prediction_set_id = $1 AND trade_date >= $2 AND trade_date <= $3
           AND score IS NOT NULL ORDER BY trade_date, score DESC",
    )
    .bind(&pred_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("scores: {}", e))?;
    if scores.is_empty() {
        return Err("no prediction scores found".into());
    }

    // Build score lookup: date → sorted (symbol, score)
    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
    for (d, s, sc) in &scores {
        scores_by_date.entry(*d).or_default().push((s.clone(), *sc));
    }

    // Get all trading days in range
    let all_days = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
         WHERE trade_date >= $1 AND trade_date <= $2 ORDER BY trade_date",
    )
    .bind(start - chrono::Duration::days(30))
    .bind(end + chrono::Duration::days(5))
    .fetch_all(db)
    .await
    .map_err(|e| format!("days: {}", e))?;
    let trading_days: Vec<NaiveDate> = all_days.into_iter().map(|r| r.0).collect();

    // Load close prices for all symbols
    let symbols: Vec<String> = scores.iter().map(|(_, s, _)| s.clone()).collect::<Vec<_>>();
    let price_rows = sqlx::query_as::<_, (NaiveDate, String, f64)>(
        "SELECT trade_date, symbol, close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3 AND close > 0",
    )
    .bind(&symbols)
    .bind(start - chrono::Duration::days(30))
    .bind(end + chrono::Duration::days(5))
    .fetch_all(db)
    .await
    .map_err(|e| format!("prices: {}", e))?;
    let mut price_map: HashMap<(NaiveDate, String), f64> = HashMap::new();
    for (d, s, p) in &price_rows {
        price_map.insert((*d, s.clone()), *p);
    }

    // Benchmark prices
    let bench_rows = sqlx::query_as::<_, (NaiveDate, f64)>(
        "SELECT trade_date, close::double precision FROM market_index_daily_bar
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3 AND close > 0 ORDER BY trade_date",
    )
    .bind(&benchmark).bind(start).bind(end)
    .fetch_all(db).await.map_err(|e| format!("bench: {}", e))?;
    let bench_map: HashMap<NaiveDate, f64> = bench_rows.into_iter().collect();

    // State
    let mut cash = initial;
    let mut positions: HashMap<String, f64> = HashMap::new(); // symbol → shares
    let mut prev_nav = initial;
    let mut peak_nav = initial;
    let mut max_dd = 0.0f64;
    let mut total_trades = 0usize;
    let nav_history: Vec<Value> = Vec::new();
    let bench_start = bench_map.get(&start).copied();

    let mut next_reb_idx = 0usize;

    for day_idx in 0..trading_days.len() {
        let today = trading_days[day_idx];

        // Rebalance: find a score day <= today
        if day_idx >= next_reb_idx {
            let score_day = scores_by_date
                .keys()
                .filter(|&&d| d <= today)
                .max()
                .copied();

            if let Some(sd) = score_day {
                if let Some(day_scores) = scores_by_date.get(&sd) {
                    let candidates: Vec<&(String, f64)> = day_scores.iter().take(top_n).collect();
                    if candidates.len() >= 5 {
                        let w = (1.0 / candidates.len() as f64).min(max_pos);
                        let mut target_values: HashMap<String, f64> = HashMap::new();
                        for (sym, _) in &candidates {
                            target_values.insert(sym.clone(), w * prev_nav);
                        }
                        // Sell positions not in target
                        let mut to_sell: Vec<(String, f64)> = Vec::new();
                        for (sym, shares) in &positions {
                            if !target_values.contains_key(sym) {
                                to_sell.push((sym.clone(), *shares));
                            }
                        }
                        for (sym, shares) in to_sell {
                            if let Some(px) = price_map.get(&(today, sym.clone())) {
                                let proceeds = shares * px * (1.0 - commission);
                                cash += proceeds;
                                positions.remove(&sym);
                                total_trades += 1;
                            }
                        }
                        // Buy/adjust to target
                        for (sym, target_val) in &target_values {
                            if let Some(px) = price_map.get(&(today, sym.clone())) {
                                let target_shares = target_val / px;
                                let current_shares = positions.get(sym).copied().unwrap_or(0.0);
                                let diff = target_shares - current_shares;
                                if diff.abs() * px > 100.0 {
                                    // min trade size
                                    let cost = diff.abs()
                                        * px
                                        * (1.0 + if diff > 0.0 { commission } else { 0.0 });
                                    if diff > 0.0 && cash >= cost {
                                        cash -= cost;
                                        positions.insert(sym.clone(), current_shares + diff);
                                        total_trades += 1;
                                    } else if diff < 0.0 {
                                        cash += diff.abs() * px * (1.0 - commission);
                                        positions.insert(sym.clone(), current_shares + diff);
                                        total_trades += 1;
                                    }
                                }
                            }
                        }
                        // Clear empty positions
                        positions.retain(|_, v| *v > 0.0);
                    }
                }
            }
            next_reb_idx = day_idx + reb_days;
        }

        // Daily mark-to-market
        let mut mkt_val = 0.0f64;
        for (sym, shares) in &positions {
            if let Some(px) = price_map.get(&(today, sym.clone())) {
                mkt_val += shares * px;
            }
        }
        let nav = cash + mkt_val;
        let daily_ret = if prev_nav > 0.0 {
            nav / prev_nav - 1.0
        } else {
            0.0
        };
        let cum_ret = if initial > 0.0 {
            nav / initial - 1.0
        } else {
            0.0
        };
        if nav > peak_nav {
            peak_nav = nav;
        }
        let dd = if peak_nav > 0.0 {
            (peak_nav - nav) / peak_nav
        } else {
            0.0
        };
        if dd > max_dd {
            max_dd = dd;
        }
        let bench_cum = match bench_start {
            Some(bs) => bench_map.get(&today).map(|bv| bv / bs - 1.0).unwrap_or(0.0),
            None => 0.0,
        };

        // R5: 统一走 upsert_nav_snapshot（原裸 SQL 5 处重复之一，回测完整快照）
        if day_idx % 5 == 0 || day_idx == trading_days.len() - 1 {
            let mut snap = crate::routes::shared::NavSnapshot::new(&account_id, today, nav);
            snap.cash = cash;
            snap.market_value = mkt_val;
            snap.position_count = positions.len() as i32;
            snap.daily_return = Some(daily_ret);
            snap.cumulative_return = Some(cum_ret);
            snap.benchmark_return = Some(bench_cum);
            snap.excess_return = Some(cum_ret - bench_cum);
            snap.max_drawdown = Some(max_dd);
            snap.prediction_set_id = Some(pred_id.clone());
            snap.signal_count = Some(top_n as i32);
            snap.trade_count = Some(total_trades as i32);
            crate::routes::shared::upsert_nav_snapshot(db, &snap)
                .await
                .map_err(|e| format!("nav insert: {}", e))?;
        }

        prev_nav = nav;
    }

    // Update account
    PgPaperAccountRepo::new(db)
        .update_nav(&account_id, prev_nav, peak_nav, max_dd, total_trades as i32)
        .await
        .map_err(|e| format!("account update: {}", e))?;

    let sharpe = nav_history
        .iter()
        .map(|v| v["daily_return"].as_f64().unwrap_or(0.0))
        .collect::<Vec<_>>();
    let avg_ret = sharpe.iter().sum::<f64>() / sharpe.len().max(1) as f64;
    let var =
        sharpe.iter().map(|r| (r - avg_ret).powi(2)).sum::<f64>() / sharpe.len().max(1) as f64;
    let annual_sharpe = if var > 1e-12 {
        avg_ret / var.sqrt() * (252.0f64).sqrt()
    } else {
        0.0
    };

    Ok(json!({
        "paper_account_id": account_id,
        "prediction_set_id": pred_id,
        "date_range": {"start": start.to_string(), "end": end.to_string()},
        "initial_capital": initial,
        "final_nav": prev_nav,
        "cumulative_return": if initial > 0.0 { prev_nav / initial - 1.0 } else { 0.0 },
        "peak_nav": peak_nav,
        "max_drawdown_pct": max_dd,
        "total_trades": total_trades,
        "final_positions": positions.len(),
        "sharpe_ratio": annual_sharpe,
        "trading_days": trading_days.len(),
        "status": "simulation_complete"
    }))
}

// ── Multi-Window Simulation ──

#[derive(Debug, Deserialize)]
pub struct MultiWindowSimRequest {
    pub paper_account_id: String,
    pub windows: Vec<WindowConfig>,
    pub top_n: Option<usize>,
    pub rebalance_freq_days: Option<usize>,
    pub benchmark: Option<String>,
    pub commission_pct: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct WindowConfig {
    pub prediction_set_id: String,
    pub start_date: String,
    pub end_date: String,
    pub max_position_pct: Option<f64>,
}

pub async fn simulate_multi_window(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MultiWindowSimRequest>,
) -> impl IntoResponse {
    match simulate_multi_window_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn simulate_multi_window_inner(
    db: &sqlx::PgPool,
    req: MultiWindowSimRequest,
) -> Result<Value, String> {
    let account_id = req.paper_account_id.trim().to_string();
    let top_n = req.top_n.unwrap_or(40);
    let reb_days = req.rebalance_freq_days.unwrap_or(20);
    let benchmark = req.benchmark.unwrap_or_else(|| "000300.SH".into());
    let commission = req.commission_pct.unwrap_or(0.0003);

    let capital = PgPaperAccountRepo::new(db)
        .find_initial_capital(&account_id)
        .await
        .map_err(|e| format!("DB: {}", e))?
        .ok_or("account not found")?;
    let initial = capital;

    let mut nav = initial;
    let mut peak_nav = initial;
    let mut max_dd = 0.0f64;
    let mut total_trades = 0usize;
    let mut positions: HashMap<String, f64> = HashMap::new();
    let mut entry_prices: HashMap<String, f64> = HashMap::new();
    let mut stop_losses = 0usize;
    let mut cash = initial;
    let mut window_results: Vec<Value> = Vec::new();

    // Load benchmark for full range
    let global_start = NaiveDate::parse_from_str(&req.windows[0].start_date, "%Y%m%d")
        .map_err(|e| format!("start_date: {}", e))?;
    let global_end =
        NaiveDate::parse_from_str(&req.windows[req.windows.len() - 1].end_date, "%Y%m%d")
            .map_err(|e| format!("end_date: {}", e))?;
    let bench_rows = sqlx::query_as::<_, (NaiveDate, f64)>(
        "SELECT trade_date, close::double precision FROM market_index_daily_bar
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3 AND close > 0 ORDER BY trade_date",
    ).bind(&benchmark).bind(global_start).bind(global_end)
    .fetch_all(db).await.map_err(|e| format!("bench: {}", e))?;
    let bench_map: HashMap<NaiveDate, f64> = bench_rows.into_iter().collect();
    let bench_start = bench_map.get(&global_start).copied();

    for (wi, wcfg) in req.windows.iter().enumerate() {
        let w_start = NaiveDate::parse_from_str(&wcfg.start_date, "%Y%m%d")
            .map_err(|e| format!("window {} start: {}", wi, e))?;
        let w_end = NaiveDate::parse_from_str(&wcfg.end_date, "%Y%m%d")
            .map_err(|e| format!("window {} end: {}", wi, e))?;
        let max_pos = wcfg.max_position_pct.unwrap_or(0.05);
        let prev_nav_at_start = nav;

        // Load scores for this window
        let scores = sqlx::query_as::<_, (NaiveDate, String, f64)>(
            "SELECT trade_date, symbol, score FROM model_prediction
             WHERE prediction_set_id = $1 AND trade_date >= $2 AND trade_date <= $3
               AND score IS NOT NULL ORDER BY trade_date, score DESC",
        )
        .bind(&wcfg.prediction_set_id)
        .bind(w_start)
        .bind(w_end)
        .fetch_all(db)
        .await
        .map_err(|e| format!("w{} scores: {}", wi, e))?;

        if scores.is_empty() {
            continue;
        }

        let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
        for (d, s, sc) in &scores {
            scores_by_date.entry(*d).or_default().push((s.clone(), *sc));
        }
        let symbols: Vec<String> = scores.iter().map(|(_, s, _)| s.clone()).collect();

        let price_rows = sqlx::query_as::<_, (NaiveDate, String, f64)>(
            "SELECT trade_date, symbol, close::double precision FROM market_stock_daily_bar_adj
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3 AND close > 0",
        )
        .bind(&symbols)
        .bind(w_start - chrono::Duration::days(10))
        .bind(w_end + chrono::Duration::days(5))
        .fetch_all(db)
        .await
        .map_err(|e| format!("w{} prices: {}", wi, e))?;
        let mut price_map: HashMap<(NaiveDate, String), f64> = HashMap::new();
        for (d, s, p) in &price_rows {
            price_map.insert((*d, s.clone()), *p);
        }

        let all_days = sqlx::query_as::<_, (NaiveDate,)>(
            "SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
             WHERE trade_date >= $1 AND trade_date <= $2 ORDER BY trade_date",
        )
        .bind(w_start)
        .bind(w_end + chrono::Duration::days(5))
        .fetch_all(db)
        .await
        .map_err(|e| format!("w{} days: {}", wi, e))?;
        let trading_days: Vec<NaiveDate> = all_days.into_iter().map(|r| r.0).collect();

        let mut next_reb = 0usize;
        let mut w_trades = 0usize;

        for (day_idx, &today) in trading_days.iter().enumerate() {
            if today < w_start || today > w_end {
                continue;
            }

            if day_idx >= next_reb {
                // PIT-safe regime detection: benchmark 60-day trailing return
                // Only uses data available at 'today' (no future leakage)
                let (eff_top_n, eff_max_pos) = {
                    let lookback = today - chrono::Duration::days(60);
                    let trailing_ret = match (bench_map.get(&today), bench_map.get(&lookback)) {
                        (Some(&curr), Some(&prev)) if prev > 0.0 => (curr / prev - 1.0) / 0.10,
                        _ => 0.0,
                    };
                    if trailing_ret < -1.0 {
                        // Deep bear: reduce exposure 50%
                        ((top_n as f64 * 0.5).ceil() as usize, max_pos * 0.6)
                    } else if trailing_ret < 0.0 {
                        // Mild bear: reduce exposure 30%
                        ((top_n as f64 * 0.7).ceil() as usize, max_pos * 0.8)
                    } else {
                        (top_n, max_pos)
                    }
                };
                let score_day = scores_by_date
                    .keys()
                    .filter(|&&d| d <= today)
                    .max()
                    .copied();
                if let Some(sd) = score_day {
                    if let Some(day_scores) = scores_by_date.get(&sd) {
                        let candidates: Vec<&(String, f64)> =
                            day_scores.iter().take(eff_top_n).collect();
                        if candidates.len() >= 5 {
                            let w = (1.0 / candidates.len() as f64).min(eff_max_pos);
                            let mut targets: HashMap<String, f64> = HashMap::new();
                            for (sym, _) in &candidates {
                                targets.insert(sym.clone(), w * nav);
                            }
                            // Sell non-targets
                            let to_sell: Vec<(String, f64)> = positions
                                .iter()
                                .filter(|(s, _)| !targets.contains_key(*s))
                                .map(|(s, q)| (s.clone(), *q))
                                .collect();
                            for (sym, shares) in to_sell {
                                if let Some(px) = price_map.get(&(today, sym.clone())) {
                                    cash += shares * px * (1.0 - commission);
                                    positions.remove(&sym);
                                    w_trades += 1;
                                }
                            }
                            // Buy/adjust targets
                            for (sym, target_val) in &targets {
                                if let Some(px) = price_map.get(&(today, sym.clone())) {
                                    let target_shares = target_val / px;
                                    let current = positions.get(sym).copied().unwrap_or(0.0);
                                    let diff = target_shares - current;
                                    if diff.abs() * px > 100.0 {
                                        if diff > 0.0 {
                                            let cost = diff * px * (1.0 + commission);
                                            if cash >= cost {
                                                cash -= cost;
                                                let new_shares = current + diff;
                                                // Track entry price: use current price for new positions
                                                if current == 0.0 {
                                                    entry_prices.insert(sym.clone(), *px);
                                                }
                                                positions.insert(sym.clone(), new_shares);
                                                w_trades += 1;
                                            }
                                        } else {
                                            cash += diff.abs() * px * (1.0 - commission);
                                            positions.insert(sym.clone(), current + diff);
                                            if current + diff <= 0.0 {
                                                entry_prices.remove(sym);
                                            }
                                            w_trades += 1;
                                        }
                                    }
                                }
                            }
                            positions.retain(|_, v| *v > 0.0);
                        }
                    }
                }
                next_reb = day_idx + reb_days;
            }

            let mut _mkt_val = 0.0f64;
            for (sym, shares) in &positions {
                if let Some(px) = price_map.get(&(today, sym.clone())) {
                    _mkt_val += shares * px;
                }
            }
            // Stop-loss: absolute drawdown from entry with portfolio stress filter
            let portfolio_dd = if peak_nav > 0.0 {
                (peak_nav - nav) / peak_nav
            } else {
                0.0
            };
            let mut stopped: Vec<String> = Vec::new();
            for (sym, shares) in &positions {
                if *shares <= 0.0 {
                    continue;
                }
                if let (Some(&entry), Some(&current)) =
                    (entry_prices.get(sym), price_map.get(&(today, sym.clone())))
                {
                    if entry <= 0.0 {
                        continue;
                    }
                    let stock_dd = 1.0 - current / entry;
                    // Only stop if portfolio is stressed (>10% DD) AND stock is down >25%
                    if portfolio_dd > 0.10 && stock_dd > 0.25 {
                        cash += shares * current * (1.0 - commission);
                        stopped.push(sym.clone());
                        stop_losses += 1;
                    }
                }
            }
            for sym in &stopped {
                positions.remove(sym);
                entry_prices.remove(sym);
            }
            nav = cash
                + positions
                    .iter()
                    .map(|(s, q)| price_map.get(&(today, s.clone())).unwrap_or(&0.0) * q)
                    .sum::<f64>();
            if nav > peak_nav {
                peak_nav = nav;
            }
            let dd = if peak_nav > 0.0 {
                (peak_nav - nav) / peak_nav
            } else {
                0.0
            };
            if dd > max_dd {
                max_dd = dd;
            }
        }

        // At window end: sell all (transition cash to next window)
        for (sym, shares) in positions.clone() {
            let last_day = trading_days
                .iter()
                .rev()
                .find(|&&d| d <= w_end)
                .copied()
                .unwrap_or(w_end);
            if let Some(px) = price_map.get(&(last_day, sym.clone())) {
                cash += shares * px * (1.0 - commission);
                positions.remove(&sym);
            }
        }
        let w_nav = cash; // NAV = cash after selling all
        let w_ret = if prev_nav_at_start > 0.0 {
            w_nav / prev_nav_at_start - 1.0
        } else {
            0.0
        };
        nav = w_nav;
        total_trades += w_trades;

        window_results.push(json!({
            "window_index": wi,
            "prediction_set_id": wcfg.prediction_set_id,
            "start_nav": prev_nav_at_start,
            "end_nav": w_nav,
            "window_return": w_ret,
            "trades": w_trades,
        }));
    }

    let final_return = if initial > 0.0 {
        nav / initial - 1.0
    } else {
        0.0
    };
    let bench_final = bench_map.get(&global_end).copied().unwrap_or(0.0);

    // Update account
    PgPaperAccountRepo::new(db)
        .update_nav(&account_id, nav, peak_nav, max_dd, total_trades as i32)
        .await
        .map_err(|e| format!("account update: {}", e))?;

    Ok(json!({
        "paper_account_id": account_id,
        "window_count": req.windows.len(),
        "initial_capital": initial,
        "final_nav": nav,
        "cumulative_return": final_return,
        "peak_nav": peak_nav,
        "max_drawdown_pct": max_dd,
        "total_trades": total_trades,
        "stop_losses": stop_losses,
        "windows": window_results,
        "benchmark_return": if let Some(bs) = bench_start { bench_final / bs - 1.0 } else { 0.0 },
        "status": "multi_window_complete"
    }))
}

async fn create_paper_account_inner(
    db: &sqlx::PgPool,
    req: CreatePaperAccountRequest,
) -> Result<Value, String> {
    let req = normalize_account_request(req)?;
    let account_id = format!("pa-{}", Uuid::new_v4());
    let input = crate::routes::shared::CreateAccountInput {
        name: req.name.clone(),
        base_currency: req.base_currency.clone(),
        account_type: req.account_type.clone(),
        initial_capital: req.initial_capital.to_f64().unwrap_or(0.0),
        leverage_enabled: false,
        leverage_mode: "fixed".to_string(),
        leverage_multiplier: 1.0,
        signal_source: "factor".to_string(),
        user_id: None,
        dingtalk_webhook_url: req.dingtalk_webhook_url.clone(),
    };
    PgPaperAccountRepo::new(db)
        .create(&account_id, &input)
        .await
        .map_err(|error| format!("Failed to create paper_account: {}", error))?;

    write_audit_event(
        db,
        "paper_account.create",
        "paper_account",
        &account_id,
        req.operator.as_deref(),
        "Created paper account",
        json!({
            "initial_capital": req.initial_capital,
            "base_currency": req.base_currency
        }),
    )
    .await?;

    Ok(json!({
        "paper_account_id": account_id,
        "status": "active",
        "cash": req.initial_capital,
        "initial_capital": req.initial_capital
    }))
}

async fn submit_paper_order_inner(
    db: &sqlx::PgPool,
    req: SubmitPaperOrderRequest,
) -> Result<Value, String> {
    let req = normalize_order_request(req)?;
    let account = sqlx::query_as::<_, (String, Decimal)>(
        "SELECT status, cash FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&req.paper_account_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_account: {}", error))?
    .ok_or_else(|| "paper_account not found".to_string())?;

    let risk = evaluate_order_risk(&req, &account.0, account.1);
    let order_id = format!("po-{}", Uuid::new_v4());
    let status = if risk.passed { "submitted" } else { "rejected" };

    sqlx::query(
        "INSERT INTO paper_order
           (order_id, paper_account_id, strategy_version_id, symbol, side, order_type,
            quantity, limit_price, status, reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(&order_id)
    .bind(&req.paper_account_id)
    .bind(req.strategy_version_id.as_deref())
    .bind(&req.symbol)
    .bind(&req.side)
    .bind(&req.order_type)
    .bind(req.quantity)
    .bind(req.limit_price)
    .bind(status)
    .bind(risk.reason.as_deref())
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert paper_order: {}", error))?;

    write_audit_event(
        db,
        if risk.passed {
            "paper_order.submit"
        } else {
            "paper_order.risk_reject"
        },
        "paper_order",
        &order_id,
        req.operator.as_deref(),
        if risk.passed {
            "Submitted paper order"
        } else {
            "Rejected paper order"
        },
        json!({
            "paper_account_id": req.paper_account_id,
            "strategy_version_id": req.strategy_version_id,
            "symbol": req.symbol,
            "side": req.side,
            "quantity": req.quantity,
            "estimated_price": req.estimated_price,
            "risk_reason": risk.reason,
            "trace_id": req.trace_id
        }),
    )
    .await?;

    Ok(json!({
        "order_id": order_id,
        "status": status,
        "risk": {
            "passed": risk.passed,
            "reason": risk.reason
        },
        "trace_id": req.trace_id
    }))
}

async fn fill_paper_order_inner(
    db: &sqlx::PgPool,
    order_id: &str,
    req: FillPaperOrderRequest,
) -> Result<Value, String> {
    let req = normalize_fill_request(req)?;
    let order = sqlx::query_as::<_, (String, String, String, Decimal, String)>(
        "SELECT paper_account_id, symbol, side, quantity, status
         FROM paper_order
         WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_order: {}", error))?
    .ok_or_else(|| "paper_order not found".to_string())?;
    if order.4 != "submitted" && order.4 != "partially_filled" {
        return Err(format!(
            "paper_order cannot be filled from status {}",
            order.4
        ));
    }

    let fill_quantity = req.quantity.unwrap_or(order.3);
    if fill_quantity <= Decimal::ZERO || fill_quantity > order.3 {
        return Err("fill quantity must be positive and no greater than order quantity".into());
    }
    let amount = fill_quantity * req.price;
    let total_cost = amount + req.commission + req.tax + req.slippage;
    let cash_delta = if order.2 == "buy" {
        -total_cost
    } else {
        amount - req.commission - req.tax - req.slippage
    };
    let fill_id = format!("pf-{}", Uuid::new_v4());
    let fill_time: DateTime<Utc> = Utc::now();

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin paper fill transaction: {}", error))?;
    sqlx::query(
        "INSERT INTO paper_fill
           (fill_id, order_id, paper_account_id, symbol, fill_time, side,
            quantity, price, amount, commission, tax, slippage, planned_order_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $2)",
    )
    .bind(&fill_id)
    .bind(order_id)
    .bind(&order.0)
    .bind(&order.1)
    .bind(fill_time)
    .bind(&order.2)
    .bind(fill_quantity)
    .bind(req.price)
    .bind(amount)
    .bind(req.commission)
    .bind(req.tax)
    .bind(req.slippage)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to insert paper_fill: {}", error))?;
    sqlx::query("UPDATE paper_account SET cash = cash + $2 WHERE paper_account_id = $1")
        .bind(&order.0)
        .bind(cash_delta)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to update paper_account cash: {}", error))?;
    sqlx::query("UPDATE paper_order SET status = 'filled' WHERE order_id = $1")
        .bind(order_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to update paper_order status: {}", error))?;
    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit paper fill transaction: {}", error))?;

    write_audit_event(
        db,
        "paper_order.fill",
        "paper_order",
        order_id,
        req.operator.as_deref(),
        "Filled paper order",
        json!({
            "fill_id": fill_id,
            "paper_account_id": order.0,
            "symbol": order.1,
            "side": order.2,
            "quantity": fill_quantity,
            "price": req.price,
            "amount": amount,
            "cash_delta": cash_delta,
            "trace_id": req.trace_id
        }),
    )
    .await?;

    Ok(json!({
        "fill_id": fill_id,
        "order_id": order_id,
        "status": "filled",
        "quantity": fill_quantity,
        "price": req.price,
        "amount": amount,
        "cash_delta": cash_delta,
        "trace_id": req.trace_id
    }))
}

async fn paper_account_summary_inner(db: &sqlx::PgPool, account_id: &str) -> Result<Value, String> {
    // 2026-09-09 修复: 旧实现 "nav": account.3 把 cash 当 NAV 返回——全仓/融资账户 cash=0,
    // 页面净值显示 0（生产杠杆账户实测命中）。NAV 直接取 current_nav（EOD 盯市维护）。
    let account = sqlx::query_as::<
        _,
        (
            String,
            String,
            Decimal,
            Decimal,
            String,
            Option<Decimal>,
            Option<Decimal>,
        ),
    >(
        "SELECT paper_account_id, name, initial_capital, cash, status, current_nav, peak_nav
         FROM paper_account
         WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_account: {}", error))?
    .ok_or_else(|| "paper_account not found".to_string())?;
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status = 'filled')::bigint,
                COUNT(*) FILTER (WHERE status = 'rejected')::bigint
         FROM paper_order
         WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_order: {}", error))?;

    let nav = account.5.or(Some(account.2)).unwrap_or_default();
    let init = account.2;
    let total_return = if init > Decimal::ZERO {
        Some(format!("{}", (nav - init) / init))
    } else {
        None
    };

    Ok(json!({
        "paper_account_id": account.0,
        "name": account.1,
        "initial_capital": account.2,
        "cash": account.3,
        "status": account.4,
        "nav": nav,
        "peak_nav": account.6,
        "total_return": total_return,
        "order_count": counts.0,
        "filled_order_count": counts.1,
        "rejected_order_count": counts.2
    }))
}

async fn paper_metrics_inner(db: &sqlx::PgPool) -> Result<Value, String> {
    let order_counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status = 'submitted')::bigint,
                COUNT(*) FILTER (WHERE status = 'filled')::bigint,
                COUNT(*) FILTER (WHERE status = 'rejected')::bigint
         FROM paper_order",
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_order metrics: {}", error))?;
    let fill_counts = sqlx::query_as::<_, (i64, Option<Decimal>)>(
        "SELECT COUNT(*)::bigint, SUM(amount) FROM paper_fill",
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_fill metrics: {}", error))?;

    Ok(json!({
        "order_count": order_counts.0,
        "submitted_order_count": order_counts.1,
        "filled_order_count": order_counts.2,
        "rejected_order_count": order_counts.3,
        "fill_count": fill_counts.0,
        "filled_amount": fill_counts.1.unwrap_or_else(Decimal::zero)
    }))
}

struct RiskResult {
    passed: bool,
    reason: Option<String>,
}

fn evaluate_order_risk(
    req: &NormalizedOrderRequest,
    account_status: &str,
    cash: Decimal,
) -> RiskResult {
    if account_status != "active" {
        return RiskResult {
            passed: false,
            reason: Some("account_not_active".into()),
        };
    }
    if req.side != "buy" && req.side != "sell" {
        return RiskResult {
            passed: false,
            reason: Some("unsupported_side".into()),
        };
    }
    if req.quantity <= Decimal::ZERO {
        return RiskResult {
            passed: false,
            reason: Some("non_positive_quantity".into()),
        };
    }
    if req.side == "buy" {
        let price = req.estimated_price.or(req.limit_price);
        let Some(price) = price else {
            return RiskResult {
                passed: false,
                reason: Some("buy_order_requires_estimated_or_limit_price".into()),
            };
        };
        if req.quantity * price > cash {
            return RiskResult {
                passed: false,
                reason: Some("insufficient_cash".into()),
            };
        }
    }
    RiskResult {
        passed: true,
        reason: None,
    }
}

fn normalize_account_request(
    req: CreatePaperAccountRequest,
) -> Result<NormalizedAccountRequest, String> {
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err("name must not be empty".into());
    }
    let initial_capital = decimal_from_f64(req.initial_capital, "initial_capital")?;
    if initial_capital <= Decimal::ZERO {
        return Err("initial_capital must be positive".into());
    }
    Ok(NormalizedAccountRequest {
        name,
        initial_capital,
        base_currency: req
            .base_currency
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("CNY")
            .to_string(),
        operator: req
            .operator
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string),
        account_type: req.account_type.trim().to_string(),
        dingtalk_webhook_url: req
            .dingtalk_webhook_url
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string),
    })
}

// ─── helpers ───

fn normalize_order_request(req: SubmitPaperOrderRequest) -> Result<NormalizedOrderRequest, String> {
    let paper_account_id = required_trimmed(req.paper_account_id, "paper_account_id")?;
    let symbol = required_trimmed(req.symbol, "symbol")?;
    let side = required_trimmed(req.side, "side")?.to_lowercase();
    let quantity = decimal_from_f64(req.quantity, "quantity")?;
    let limit_price = optional_decimal(req.limit_price, "limit_price")?;
    let estimated_price = optional_decimal(req.estimated_price, "estimated_price")?;
    Ok(NormalizedOrderRequest {
        paper_account_id,
        strategy_version_id: normalize_optional_string(req.strategy_version_id),
        symbol,
        side,
        order_type: req
            .order_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("market")
            .to_string(),
        quantity,
        limit_price,
        estimated_price,
        operator: normalize_optional_string(req.operator),
        trace_id: normalize_optional_string(req.trace_id)
            .unwrap_or_else(|| format!("trace-{}", Uuid::new_v4())),
    })
}

fn normalize_fill_request(req: FillPaperOrderRequest) -> Result<NormalizedFillRequest, String> {
    let price = decimal_from_f64(req.price, "price")?;
    if price <= Decimal::ZERO {
        return Err("price must be positive".into());
    }
    Ok(NormalizedFillRequest {
        price,
        quantity: optional_decimal(req.quantity, "quantity")?,
        commission: optional_decimal(req.commission, "commission")?.unwrap_or_else(Decimal::zero),
        tax: optional_decimal(req.tax, "tax")?.unwrap_or_else(Decimal::zero),
        slippage: optional_decimal(req.slippage, "slippage")?.unwrap_or_else(Decimal::zero),
        operator: normalize_optional_string(req.operator),
        trace_id: normalize_optional_string(req.trace_id)
            .unwrap_or_else(|| format!("trace-{}", Uuid::new_v4())),
    })
}

fn required_trimmed(value: String, field: &str) -> Result<String, String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(format!("{} must not be empty", field))
    } else {
        Ok(value)
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_decimal(value: Option<f64>, field: &str) -> Result<Option<Decimal>, String> {
    value
        .map(|value| decimal_from_f64(value, field))
        .transpose()
}

fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal, String> {
    Decimal::from_f64(value).ok_or_else(|| format!("{} must be a finite number", field))
}

async fn write_audit_event(
    db: &sqlx::PgPool,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    actor: Option<&str>,
    summary: &str,
    details: Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(event_type)
    .bind(entity_type)
    .bind(entity_id)
    .bind(actor)
    .bind(summary)
    .bind(details)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert audit_event: {}", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_order_risk_rejects_insufficient_cash() {
        let req = normalize_order_request(SubmitPaperOrderRequest {
            paper_account_id: "pa-1".into(),
            strategy_version_id: None,
            symbol: "000001.SZ".into(),
            side: "buy".into(),
            order_type: None,
            quantity: 200.0,
            limit_price: Some(10.0),
            estimated_price: None,
            operator: None,
            trace_id: None,
        })
        .expect("order request");

        let risk = evaluate_order_risk(&req, "active", Decimal::from_i32(1000).unwrap());

        assert!(!risk.passed);
        assert_eq!(risk.reason.as_deref(), Some("insufficient_cash"));
    }

    #[test]
    fn buy_order_risk_passes_when_cash_covers_estimate() {
        let req = normalize_order_request(SubmitPaperOrderRequest {
            paper_account_id: "pa-1".into(),
            strategy_version_id: Some("factor-combo-v1".into()),
            symbol: "000001.SZ".into(),
            side: "buy".into(),
            order_type: Some("limit".into()),
            quantity: 100.0,
            limit_price: Some(10.0),
            estimated_price: None,
            operator: Some("tester".into()),
            trace_id: Some("trace-1".into()),
        })
        .expect("order request");

        let risk = evaluate_order_risk(&req, "active", Decimal::from_i32(1001).unwrap());

        assert!(risk.passed);
        assert_eq!(req.trace_id, "trace-1");
        assert_eq!(req.order_type, "limit");
    }

    #[test]
    fn fill_request_defaults_costs_and_trace() {
        let req = normalize_fill_request(FillPaperOrderRequest {
            price: 10.0,
            quantity: None,
            commission: None,
            tax: None,
            slippage: None,
            operator: None,
            trace_id: None,
        })
        .expect("fill request");

        assert_eq!(req.price, Decimal::from_i32(10).unwrap());
        assert_eq!(req.commission, Decimal::ZERO);
        assert!(req.trace_id.starts_with("trace-"));
    }
}

// ── 第五批覆盖率测试：paper 账号域（请求归一化 / 风控全分支 / 账号-订单-成交全链路）──
// 模式沿用 exchange_announcement.rs fourth_batch：inner 直调，跳过 axum HTTP 层；
// 连库测试走真实本机 PG，zzz_test_ 前缀独占键 + 前置+结尾精确键清理（禁 LIKE 宽前缀）。
#[cfg(test)]
mod fifth_batch {

    use super::*;

    fn d(v: i64) -> Decimal {
        Decimal::from(v)
    }

    /// json 中 Decimal 字段按字符串序列化（rust_decimal serde 默认），转 f64 比较。
    fn dec_f(v: &Value) -> f64 {
        v.as_str()
            .unwrap_or_else(|| panic!("期望字符串数字: {v}"))
            .parse()
            .expect("parse decimal str")
    }

    fn fill_req(price: f64, qty: Option<f64>) -> FillPaperOrderRequest {
        FillPaperOrderRequest {
            price,
            quantity: qty,
            commission: None,
            tax: None,
            slippage: None,
            operator: None,
            trace_id: None,
        }
    }

    fn order_req(account: &str, side: &str, qty: f64, est: Option<f64>) -> SubmitPaperOrderRequest {
        SubmitPaperOrderRequest {
            paper_account_id: account.into(),
            strategy_version_id: None,
            symbol: "000001.SZ".into(),
            side: side.into(),
            order_type: None,
            quantity: qty,
            limit_price: None,
            estimated_price: est,
            operator: Some("zzz 第五批".into()),
            trace_id: None,
        }
    }

    fn account_req(name: &str, capital: f64) -> CreatePaperAccountRequest {
        CreatePaperAccountRequest {
            name: name.into(),
            initial_capital: capital,
            base_currency: None,
            operator: Some("  ".into()),
            account_type: default_account_type(),
            dingtalk_webhook_url: Some("  ".into()),
        }
    }

    // ── 纯函数：evaluate_order_risk 剩余分支 ──

    fn norm_order(side: &str, qty: f64, est: Option<f64>) -> NormalizedOrderRequest {
        normalize_order_request(order_req("pa-1", side, qty, est)).expect("order request")
    }

    #[test]
    fn order_risk_rejects_inactive_account_and_bad_side_and_zero_qty() {
        // 账户非 active
        let req = norm_order("buy", 100.0, Some(10.0));
        let risk = evaluate_order_risk(&req, "paused", d(1_000_000));
        assert!(!risk.passed);
        assert_eq!(risk.reason.as_deref(), Some("account_not_active"));

        // side 非 buy/sell（归一化已小写化，构造非法值）
        let req = norm_order("hold", 100.0, Some(10.0));
        let risk = evaluate_order_risk(&req, "active", d(1_000_000));
        assert!(!risk.passed);
        assert_eq!(risk.reason.as_deref(), Some("unsupported_side"));

        // 数量非正
        let req = norm_order("buy", 0.0, Some(10.0));
        let risk = evaluate_order_risk(&req, "active", d(1_000_000));
        assert!(!risk.passed);
        assert_eq!(risk.reason.as_deref(), Some("non_positive_quantity"));
    }

    #[test]
    fn order_risk_buy_without_price_rejected_and_sell_skips_cash_check() {
        // 买入无 estimated/limit 价 → 拒绝
        let req = norm_order("buy", 100.0, None);
        let risk = evaluate_order_risk(&req, "active", d(1_000_000));
        assert!(!risk.passed);
        assert_eq!(
            risk.reason.as_deref(),
            Some("buy_order_requires_estimated_or_limit_price")
        );

        // 卖出不检查现金（cash=0 也放行）
        let req = norm_order("sell", 100.0, None);
        let risk = evaluate_order_risk(&req, "active", Decimal::ZERO);
        assert!(risk.passed);
        assert!(risk.reason.is_none());
    }

    // ── 纯函数：账号/订单/成交请求归一化 ──

    #[test]
    fn normalize_account_request_validates_and_defaults() {
        // name 空 / 非正资本 / 非有限数
        let err = normalize_account_request(account_req("  ", 100.0))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "name must not be empty");
        let err = normalize_account_request(account_req("zzz", 0.0))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "initial_capital must be positive");
        let err = normalize_account_request(account_req("zzz", f64::NAN))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "initial_capital must be a finite number");

        // 正常：base_currency 缺省 CNY，operator/webhook 空白串归 None
        let req = normalize_account_request(account_req(" zzz 账号 ", 1000.0)).expect("normalize");
        assert_eq!(req.name, "zzz 账号");
        assert_eq!(req.initial_capital, d(1000));
        assert_eq!(req.base_currency, "CNY");
        assert_eq!(req.account_type, "simulated");
        assert!(req.operator.is_none());
        assert!(req.dingtalk_webhook_url.is_none());

        // 显式 base_currency 保留（trim 后非空）
        let mut r = account_req("zzz", 1000.0);
        r.base_currency = Some(" USD ".into());
        let req = normalize_account_request(r).expect("normalize");
        assert_eq!(req.base_currency, "USD");
    }

    #[test]
    fn normalize_order_request_validates_required_fields_and_defaults() {
        // 必填字段逐个缺失
        let mut r = order_req("pa-1", "buy", 100.0, Some(10.0));
        r.paper_account_id = "   ".into();
        assert_eq!(
            normalize_order_request(r).map(|_| ()).unwrap_err(),
            "paper_account_id must not be empty"
        );
        let mut r = order_req("pa-1", "buy", 100.0, Some(10.0));
        r.symbol = "".into();
        assert_eq!(
            normalize_order_request(r).map(|_| ()).unwrap_err(),
            "symbol must not be empty"
        );
        let mut r = order_req("pa-1", "BUY", 100.0, Some(10.0));
        r.side = "  ".into();
        assert_eq!(
            normalize_order_request(r).map(|_| ()).unwrap_err(),
            "side must not be empty"
        );
        // quantity 非有限数
        let r = order_req("pa-1", "buy", f64::NAN, None);
        assert_eq!(
            normalize_order_request(r).map(|_| ()).unwrap_err(),
            "quantity must be a finite number"
        );

        // 归一化行为：side 大写转小写、order_type 缺省 market、trace_id 自动生成
        let r = order_req(" pa-1 ", "BUY", 100.0, Some(10.5));
        let req = normalize_order_request(r).expect("normalize");
        assert_eq!(req.paper_account_id, "pa-1");
        assert_eq!(req.side, "buy");
        assert_eq!(req.order_type, "market");
        assert_eq!(req.estimated_price, Some(Decimal::new(105, 1)));
        assert!(req.trace_id.starts_with("trace-"));
        // limit_price 非有限数拒绝
        let mut r = order_req("pa-1", "buy", 100.0, None);
        r.limit_price = Some(f64::INFINITY);
        assert!(normalize_order_request(r)
            .map(|_| ())
            .unwrap_err()
            .contains("limit_price must be a finite number"));
    }

    #[test]
    fn normalize_fill_request_rejects_nonpositive_and_nonfinite_price() {
        assert_eq!(
            normalize_fill_request(fill_req(0.0, None))
                .map(|_| ())
                .unwrap_err(),
            "price must be positive"
        );
        assert_eq!(
            normalize_fill_request(fill_req(-1.0, None))
                .map(|_| ())
                .unwrap_err(),
            "price must be positive"
        );
        assert_eq!(
            normalize_fill_request(fill_req(f64::NAN, None))
                .map(|_| ())
                .unwrap_err(),
            "price must be a finite number"
        );
    }

    #[test]
    fn helper_fns_trim_optional_and_convert_decimal() {
        // required_trimmed
        assert_eq!(required_trimmed(" x ".into(), "f").unwrap(), "x");
        assert_eq!(
            required_trimmed("  ".into(), "f").map(|_| ()).unwrap_err(),
            "f must not be empty"
        );
        // normalize_optional_string：空白 → None，正常 trim
        assert_eq!(normalize_optional_string(Some("  ".into())), None);
        assert_eq!(
            normalize_optional_string(Some(" a ".into())),
            Some("a".to_string())
        );
        assert_eq!(normalize_optional_string(None), None);
        // optional_decimal：Some(NaN) 拒绝、None 直通
        assert!(optional_decimal(Some(f64::NAN), "p").is_err());
        assert!(optional_decimal(None, "p").unwrap().is_none());
        // decimal_from_f64：有限数转换、无穷拒绝
        assert_eq!(decimal_from_f64(1.5, "p").unwrap(), Decimal::new(15, 1));
        assert!(decimal_from_f64(f64::INFINITY, "p").is_err());
        // default_account_type
        assert_eq!(default_account_type(), "simulated");
    }

    // ── 连库测试：真实本机 PG，zzz 独占键自造自清理 ──

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 精确清理 zzz 账户的全部关联行（含快照/审计/预测集，键精确匹配禁 LIKE）。
    async fn cleanup_account(db: &sqlx::PgPool, account_id: &str) {
        for sql in [
            "DELETE FROM paper_fill WHERE paper_account_id = $1",
            "DELETE FROM paper_order WHERE paper_account_id = $1",
            "DELETE FROM paper_margin_trade WHERE paper_account_id = $1",
            "DELETE FROM paper_position WHERE paper_account_id = $1",
            "DELETE FROM paper_nav_snapshot WHERE paper_account_id = $1",
        ] {
            let _ = sqlx::query(sql).bind(account_id).execute(db).await;
        }
        let _ = sqlx::query("DELETE FROM audit_event WHERE entity_id = $1")
            .bind(account_id)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM paper_account WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(db)
            .await;
    }

    /// 造 zzz 模拟账户：cash=100000，status 参数化（测非 active 分支用 paused）。
    async fn create_zzz_account(db: &sqlx::PgPool, account_id: &str, status: &str) {
        cleanup_account(db, account_id).await;
        sqlx::query(
            "INSERT INTO paper_account
               (paper_account_id, name, initial_capital, cash, status,
                account_type, signal_source)
             VALUES ($1, 'zzz 第五批 paper 测试', 100000, 100000, $2, 'simulated', 'factor')",
        )
        .bind(account_id)
        .bind(status)
        .execute(db)
        .await
        .expect("insert zzz paper_account");
    }

    #[tokio::test]
    async fn create_paper_account_flow_persists_and_audits() {
        let db = test_db().await;
        let data = create_paper_account_inner(&db, account_req("zzz 第五批新建账户", 50_000.0))
            .await
            .expect("create account");
        let account_id = data["paper_account_id"].as_str().unwrap().to_string();
        assert!(account_id.starts_with("pa-"), "账号 id 前缀: {account_id}");
        assert_eq!(data["status"], "active");
        assert_eq!(dec_f(&data["initial_capital"]), 50_000.0);
        assert_eq!(dec_f(&data["cash"]), 50_000.0);

        // DB 落库：默认 CNY / simulated / cash=initial_capital
        let (base_ccy, acct_type, cash): (String, String, Decimal) = sqlx::query_as(
            "SELECT base_currency, account_type, cash FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(&account_id)
        .fetch_one(&db)
        .await
        .expect("zzz account row");
        assert_eq!(base_ccy, "CNY");
        assert_eq!(acct_type, "simulated");
        assert_eq!(cash, d(50_000));

        // 审计事件已写
        let audit_n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_event WHERE entity_id = $1 AND event_type = 'paper_account.create'",
        )
        .bind(&account_id)
        .fetch_one(&db)
        .await
        .expect("audit count");
        assert_eq!(audit_n, 1, "创建账号应产生 1 条审计事件");

        cleanup_account(&db, &account_id).await;
    }

    #[tokio::test]
    async fn submit_paper_order_flow_risk_reject_and_submit() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_submit";
        create_zzz_account(&db, account_id, "active").await;

        // 账户不存在
        let err = submit_paper_order_inner(
            &db,
            order_req("zzz_test_api5_no_such", "buy", 100.0, Some(1.0)),
        )
        .await
        .expect_err("未知账户应拒绝");
        assert_eq!(err, "paper_account not found");

        // 买入无价 → 风控拒绝，订单落 rejected + 审计 risk_reject
        let data = submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, None))
            .await
            .expect("submit rejected-order");
        let order_id = data["order_id"].as_str().unwrap().to_string();
        assert_eq!(data["status"], "rejected");
        assert_eq!(
            data["risk"]["reason"],
            "buy_order_requires_estimated_or_limit_price"
        );
        let (status, reason): (String, Option<String>) =
            sqlx::query_as("SELECT status, reason FROM paper_order WHERE order_id = $1")
                .bind(&order_id)
                .fetch_one(&db)
                .await
                .expect("zzz order row");
        assert_eq!(status, "rejected");
        assert!(reason.unwrap_or_default().contains("buy_order_requires"));

        // 现金不足 → rejected insufficient_cash（100 股 × 10000 元 > 100000 现金）
        let data =
            submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, Some(10_000.0)))
                .await
                .expect("submit insufficient");
        assert_eq!(data["status"], "rejected");
        assert_eq!(data["risk"]["reason"], "insufficient_cash");

        // 正常买入（100 股 × 10 元 = 1000 ≤ 现金）→ submitted + 审计 submit
        let data = submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, Some(10.0)))
            .await
            .expect("submit ok");
        let order_id = data["order_id"].as_str().unwrap().to_string();
        assert_eq!(data["status"], "submitted");
        let audit_n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_event WHERE entity_id = $1 AND event_type = 'paper_order.submit'",
        )
        .bind(&order_id)
        .fetch_one(&db)
        .await
        .expect("audit count");
        assert_eq!(audit_n, 1);

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn fill_paper_order_flow_buy_sell_and_guards() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_fill";
        create_zzz_account(&db, account_id, "active").await;

        // 订单不存在
        let err = fill_paper_order_inner(&db, "po-zzz-no-such", fill_req(10.0, None))
            .await
            .expect_err("未知订单应拒绝");
        assert_eq!(err, "paper_order not found");

        // 买单成交：100 股 @10，佣金 5 → cash 100000 - 1005
        let data = submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, Some(10.0)))
            .await
            .expect("submit buy");
        let buy_order = data["order_id"].as_str().unwrap().to_string();
        let mut req = fill_req(10.0, Some(100.0));
        req.commission = Some(5.0);
        let data = fill_paper_order_inner(&db, &buy_order, req)
            .await
            .expect("fill buy");
        let fill_id = data["fill_id"].as_str().unwrap().to_string();
        assert!(fill_id.starts_with("pf-"));
        assert_eq!(dec_f(&data["amount"]), 1000.0);
        assert_eq!(dec_f(&data["cash_delta"]), -1005.0);
        let (cash, status): (Decimal, String) = sqlx::query_as(
            "SELECT (SELECT cash FROM paper_account WHERE paper_account_id = $1),
                    (SELECT status FROM paper_order WHERE order_id = $2)",
        )
        .bind(account_id)
        .bind(&buy_order)
        .fetch_one(&db)
        .await
        .expect("after buy fill");
        assert_eq!(cash, d(98_995), "买入扣减 成交额+佣金");
        assert_eq!(status, "filled");

        // 已 filled 订单重复成交 → 拒绝
        let err = fill_paper_order_inner(&db, &buy_order, fill_req(10.0, None))
            .await
            .expect_err("filled 订单不可再成交");
        assert!(err.contains("cannot be filled from status filled"), "{err}");

        // 卖单成交：100 股 @11，费用 0 → cash 98995 + 1100
        let data = submit_paper_order_inner(&db, order_req(account_id, "sell", 100.0, None))
            .await
            .expect("submit sell");
        let sell_order = data["order_id"].as_str().unwrap().to_string();
        let data = fill_paper_order_inner(&db, &sell_order, fill_req(11.0, None))
            .await
            .expect("fill sell");
        assert_eq!(dec_f(&data["cash_delta"]), 1100.0, "卖出无费用全额回流");
        let cash: Decimal =
            sqlx::query_scalar("SELECT cash FROM paper_account WHERE paper_account_id = $1")
                .bind(account_id)
                .fetch_one(&db)
                .await
                .expect("cash after sell");
        assert_eq!(cash, d(100_095));

        // 数量守卫：fill 200 > 订单 100 → 拒绝
        let data = submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, Some(10.0)))
            .await
            .expect("submit buy 2");
        let order2 = data["order_id"].as_str().unwrap().to_string();
        let err = fill_paper_order_inner(&db, &order2, fill_req(10.0, Some(200.0)))
            .await
            .expect_err("超量成交应拒绝");
        assert!(err.contains("no greater than order quantity"), "{err}");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn account_summary_prefers_current_nav_over_cash_and_capital() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_summary";
        create_zzz_account(&db, account_id, "active").await;

        // 2026-09-09 修复回归：nav 必须取 current_nav（非 cash/initial_capital）
        sqlx::query(
            "UPDATE paper_account SET current_nav = 110000, peak_nav = 120000 WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .execute(&db)
        .await
        .expect("set nav");

        let data = paper_account_summary_inner(&db, account_id)
            .await
            .expect("summary");
        assert_eq!(dec_f(&data["nav"]), 110_000.0, "nav=current_nav 而非 cash");
        assert_eq!(dec_f(&data["peak_nav"]), 120_000.0);
        assert_eq!(data["order_count"], json!(0));
        // total_return = (110000-100000)/100000 = 0.1（字符串形式）
        assert!(
            (data["total_return"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                - 0.1)
                .abs()
                < 1e-9,
            "total_return 应为 0.1: {}",
            data["total_return"]
        );

        // current_nav NULL → 回退 initial_capital，total_return 变 0
        sqlx::query("UPDATE paper_account SET current_nav = NULL WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(&db)
            .await
            .expect("null nav");
        let data = paper_account_summary_inner(&db, account_id)
            .await
            .expect("summary 2");
        assert_eq!(dec_f(&data["nav"]), 100_000.0);
        assert!(
            (data["total_return"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                - 0.0)
                .abs()
                < 1e-12,
            "回退后 total_return 应为 0: {}",
            data["total_return"]
        );

        // 未知账户
        let err = paper_account_summary_inner(&db, "zzz_test_api5_no_such")
            .await
            .expect_err("未知账户应拒绝");
        assert_eq!(err, "paper_account not found");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn paper_metrics_counts_orders_across_statuses() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_metrics";
        create_zzz_account(&db, account_id, "active").await;
        // 2 submitted + 1 rejected
        submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, Some(10.0)))
            .await
            .expect("o1");
        submit_paper_order_inner(&db, order_req(account_id, "buy", 200.0, Some(10.0)))
            .await
            .expect("o2");
        submit_paper_order_inner(&db, order_req(account_id, "buy", 100.0, None))
            .await
            .expect("o3 rejected");

        let metrics = paper_metrics_inner(&db).await.expect("metrics");
        // 全库聚合并含其它账户订单，只能断言下界（本账户贡献 2 submitted + 1 rejected）
        assert!(
            metrics["submitted_order_count"].as_i64().unwrap_or(0) >= 2,
            "submitted 至少含本账户 2 笔: {}",
            metrics["submitted_order_count"]
        );
        assert!(
            metrics["rejected_order_count"].as_i64().unwrap_or(0) >= 1,
            "rejected 至少含本账户 1 笔: {}",
            metrics["rejected_order_count"]
        );
        assert!(metrics["order_count"].as_i64().unwrap_or(0) >= 3);

        cleanup_account(&db, account_id).await;
    }

    // generate_paper_signals / compute_paper_nav 连库造数 helper ──

    /// 造 zzz prediction_set + 指定日期各 6 个 symbol 的预测行（score 降序）。
    async fn create_zzz_predictions(db: &sqlx::PgPool, pred_set_id: &str, days: &[NaiveDate]) {
        cleanup_zzz_predictions(db, pred_set_id).await;
        // FK 前置：prediction_set.data_version_id → data_version（2026-09-21 补）
        // FK 前置二：prediction_set.model_version_id → model_registry
        sqlx::query(
            "INSERT INTO model_registry \
             (model_version_id, model_code, model_type, version, label_definition, training_window, artifact_path, artifact_hash, status) \
             VALUES ('zzz-mv', 'zzz-model', 'zzz', 'v1', '{}', '{}', '', '', 'active') \
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz model_registry");

        sqlx::query(
            "INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash) \
             VALUES ('zzz-dv', 'zzz fifth', 'zzz', '2026-06-01', '2026-06-02', '{}', '') \
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz dv for prediction_set");
        sqlx::query(
            "INSERT INTO prediction_set
               (prediction_set_id, model_version_id, feature_set_version_id, data_version_id,
                start_date, end_date, prediction_hash, status)
             VALUES ($1, 'zzz-mv', 'zzz-fv', 'zzz-dv', $2, $3, 'zzz-hash', 'ready')",
        )
        .bind(pred_set_id)
        .bind(days[0])
        .bind(days[days.len() - 1])
        .execute(db)
        .await
        .expect("insert zzz prediction_set");

        for (di, day) in days.iter().enumerate() {
            for i in 0..6i32 {
                sqlx::query(
                    "INSERT INTO model_prediction
                       (prediction_set_id, trade_date, symbol, score, rank, available_at)
                     VALUES ($1, $2, $3, $4, $5, $2)",
                )
                .bind(pred_set_id)
                .bind(day)
                .bind(format!("ZZZP{:02}.SH", i))
                .bind(1.0 - (i as f64) * 0.01 - (di as f64) * 0.001)
                .bind(i + 1)
                .execute(db)
                .await
                .expect("insert zzz model_prediction");
            }
        }
    }

    async fn cleanup_zzz_predictions(db: &sqlx::PgPool, pred_set_id: &str) {
        let _ = sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
            .bind(pred_set_id)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM prediction_set WHERE prediction_set_id = $1")
            .bind(pred_set_id)
            .execute(db)
            .await;
    }

    fn signals_req(
        account: &str,
        pred: &str,
        start: &str,
        end: &str,
    ) -> GeneratePaperSignalsRequest {
        GeneratePaperSignalsRequest {
            paper_account_id: account.into(),
            prediction_set_id: pred.into(),
            start_date: start.into(),
            end_date: end.into(),
            top_n: None,
            max_position_pct: Some(0.20),
            rebalance_freq_days: None,
        }
    }

    #[tokio::test]
    async fn generate_paper_signals_validates_account_status_dates_and_scores() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_sig";
        create_zzz_account(&db, account_id, "paused").await;
        let pred_set = "zzz_test_api5_pred";

        // 账户不存在
        let err = generate_paper_signals_inner(
            &db,
            signals_req("zzz_test_api5_no_such", pred_set, "20260601", "20260630"),
        )
        .await
        .expect_err("未知账户");
        assert!(err.contains("account not found"), "{err}");

        // 状态非 active
        let err = generate_paper_signals_inner(
            &db,
            signals_req(account_id, pred_set, "20260601", "20260630"),
        )
        .await
        .expect_err("非 active 状态");
        assert!(err.contains("account status is paused"), "{err}");

        // 激活后：start_date 坏格式
        sqlx::query("UPDATE paper_account SET status = 'active' WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(&db)
            .await
            .expect("activate");
        let err = generate_paper_signals_inner(
            &db,
            signals_req(account_id, pred_set, "2026-06-01", "20260630"),
        )
        .await
        .expect_err("坏日期格式");
        assert!(err.starts_with("start_date:"), "{err}");

        // 空预测集
        let err = generate_paper_signals_inner(
            &db,
            signals_req(account_id, pred_set, "20260601", "20260630"),
        )
        .await
        .expect_err("无预测数据");
        assert_eq!(err, "no prediction scores found for the given date range");

        cleanup_account(&db, account_id).await;
        cleanup_zzz_predictions(&db, pred_set).await;
    }

    #[tokio::test]
    async fn generate_paper_signals_upserts_positions_and_nav_snapshot() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_sigok";
        let pred_set = "zzz_test_api5_predok";
        create_zzz_account(&db, account_id, "active").await;
        let d1 = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        create_zzz_predictions(&db, pred_set, &[d1, d2]).await;

        let data = generate_paper_signals_inner(
            &db,
            signals_req(account_id, pred_set, "20260601", "20260602"),
        )
        .await
        .expect("generate signals");
        assert_eq!(data["trading_days"], json!(2), "两个预测日");
        assert_eq!(data["signal_days"], json!(1), "首窗口触发一次信号");
        assert_eq!(data["nav_updates"], json!(1));
        assert_eq!(data["top_n"], json!(40));

        // 信号持仓：6 symbol，weight = min(1/6, 0.20) = 1/6
        let rows: Vec<(String, Decimal)> = sqlx::query_as(
            "SELECT symbol, target_weight FROM paper_position
             WHERE paper_account_id = $1 ORDER BY symbol",
        )
        .bind(account_id)
        .fetch_all(&db)
        .await
        .expect("zzz positions");
        assert_eq!(rows.len(), 6, "6 个信号标的: {rows:?}");
        let expect_w = Decimal::from_f64_retain(1.0 / 6.0).unwrap();
        for (sym, w) in &rows {
            assert!(
                (w - expect_w).abs() < Decimal::new(1, 6),
                "{sym} weight 应为 1/6: {w}"
            );
        }

        // NAV 快照：1 行，signal_count=6，nav=1_000_000（初始化口径）
        let (nav, sig_count, pred): (f64, i32, String) = sqlx::query_as(
            "SELECT nav::double precision, signal_count, prediction_set_id
             FROM paper_nav_snapshot WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&db)
        .await
        .expect("zzz snapshot");
        assert_eq!(nav, 1_000_000.0);
        assert_eq!(sig_count, 6);
        assert_eq!(pred, pred_set);

        cleanup_account(&db, account_id).await;
        cleanup_zzz_predictions(&db, pred_set).await;
    }

    #[tokio::test]
    async fn compute_paper_nav_validates_snapshots_and_positions() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_nav";
        create_zzz_account(&db, account_id, "active").await;

        let req = |start: &str, end: &str| ComputePaperNavRequest {
            paper_account_id: account_id.into(),
            start_date: start.into(),
            end_date: end.into(),
            benchmark: Some("ZZZIDX01".into()),
        };

        // 无快照
        let err = compute_paper_nav_inner(&db, req("20260601", "20260630"))
            .await
            .expect_err("无快照");
        assert_eq!(err, "no NAV snapshots found for the given range");

        // 有快照但无持仓
        let snap = crate::routes::shared::NavSnapshot::new(
            account_id,
            NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
            100_000.0,
        );
        crate::routes::shared::upsert_nav_snapshot(&db, &snap)
            .await
            .expect("snap");
        let err = compute_paper_nav_inner(&db, req("20260601", "20260630"))
            .await
            .expect_err("无持仓");
        assert_eq!(err, "no positions found");

        // 坏日期格式
        let err = compute_paper_nav_inner(
            &db,
            ComputePaperNavRequest {
                paper_account_id: account_id.into(),
                start_date: "bad".into(),
                end_date: "20260630".into(),
                benchmark: None,
            },
        )
        .await
        .expect_err("坏日期");
        assert!(err.starts_with("start_date:"), "{err}");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn compute_paper_nav_updates_snapshots_with_benchmark() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_navok";
        create_zzz_account(&db, account_id, "active").await;

        // 信号持仓：weight=1.0，信号日 6/09
        PgPaperPositionRepo::new(&db)
            .upsert_signal_position(
                "pp-zzz-api5",
                account_id,
                "ZZZP00.SH",
                1.0,
                NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(),
            )
            .await
            .expect("signal position");

        // zzz 假行情（bar + 复权因子 → adj 视图）+ zzz 指数（基准）
        let (bar_sym, idx_sym) = ("ZZZP00.SH", "ZZZIDX01");
        for (day, close) in [
            (NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(), 10.0),
            (NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(), 11.0),
            (NaiveDate::from_ymd_opt(2026, 6, 11).unwrap(), 12.0),
        ] {
            sqlx::query(
                "INSERT INTO market_stock_daily_bar (symbol, trade_date, close, source)
                 VALUES ($1, $2, $3, 'zzz-test')
                 ON CONFLICT (symbol, trade_date) DO UPDATE SET close = EXCLUDED.close",
            )
            .bind(bar_sym)
            .bind(day)
            .bind(close)
            .execute(&db)
            .await
            .expect("zzz bar");
            sqlx::query(
                "INSERT INTO market_adjustment_factor (symbol, trade_date, adj_factor, source)
                 VALUES ($1, $2, 1.0, 'zzz-test')
                 ON CONFLICT (symbol, trade_date) DO UPDATE SET adj_factor = EXCLUDED.adj_factor",
            )
            .bind(bar_sym)
            .bind(day)
            .execute(&db)
            .await
            .expect("zzz adj factor");
        }
        for (day, close) in [
            (NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(), 100.0),
            (NaiveDate::from_ymd_opt(2026, 6, 11).unwrap(), 110.0),
        ] {
            sqlx::query(
                "INSERT INTO market_index_daily_bar (symbol, trade_date, close, source)
                 VALUES ($1, $2, $3, 'zzz-test')
                 ON CONFLICT (symbol, trade_date) DO UPDATE SET close = EXCLUDED.close",
            )
            .bind(idx_sym)
            .bind(day)
            .bind(close)
            .execute(&db)
            .await
            .expect("zzz index bar");
        }
        // 两个快照日
        for day in [
            NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 11).unwrap(),
        ] {
            let snap = crate::routes::shared::NavSnapshot::new(account_id, day, 100_000.0);
            crate::routes::shared::upsert_nav_snapshot(&db, &snap)
                .await
                .expect("snap");
        }

        let data = compute_paper_nav_inner(
            &db,
            ComputePaperNavRequest {
                paper_account_id: account_id.into(),
                start_date: "20260610".into(),
                end_date: "20260611".into(),
                benchmark: Some(idx_sym.into()),
            },
        )
        .await
        .expect("compute nav");
        assert_eq!(data["nav_snapshots_updated"], json!(2));
        // weight=1.0 → market_value/total_weight = prev_nav 恒定
        assert_eq!(data["final_nav"], json!(100_000.0));
        assert_eq!(data["cumulative_return"], json!(0.0));

        // 6/11 快照的 benchmark_return = 110/100 - 1 = 0.1，excess = 0 - 0.1
        let (bench_ret, excess): (Option<f64>, Option<f64>) = sqlx::query_as(
            "SELECT benchmark_return::double precision, excess_return::double precision
             FROM paper_nav_snapshot
             WHERE paper_account_id = $1 AND snapshot_date = '2026-06-11'",
        )
        .bind(account_id)
        .fetch_one(&db)
        .await
        .expect("zzz snapshot 6/11");
        let bench_ret = bench_ret.expect("benchmark_return 应已计算");
        let excess = excess.expect("excess_return 应已计算");
        assert!((bench_ret - 0.1).abs() < 1e-9, "基准收益 10%: {bench_ret}");
        assert!((excess - (-0.1)).abs() < 1e-9, "超额 = 0 - 0.1: {excess}");

        // 清理 zzz 行情数据（精确键）
        for sql in [
            "DELETE FROM market_stock_daily_bar WHERE symbol = 'ZZZP00.SH' AND source = 'zzz-test'",
            "DELETE FROM market_adjustment_factor WHERE symbol = 'ZZZP00.SH' AND source = 'zzz-test'",
            "DELETE FROM market_index_daily_bar WHERE symbol = 'ZZZIDX01' AND source = 'zzz-test'",
        ] {
            let _ = sqlx::query(sql).execute(&db).await;
        }
        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn simulate_paper_nav_validates_account_and_scores() {
        let db = test_db().await;
        let pred_set = "zzz_test_api5_simpred";
        create_zzz_predictions(
            &db,
            pred_set,
            &[NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()],
        )
        .await;

        // 账户不存在
        let err = simulate_paper_nav_inner(
            &db,
            SimulatePaperNavRequest {
                paper_account_id: "zzz_test_api5_no_such".into(),
                prediction_set_id: pred_set.into(),
                start_date: "20260601".into(),
                end_date: "20260602".into(),
                top_n: None,
                rebalance_freq_days: None,
                benchmark: None,
                max_position_pct: None,
                commission_pct: None,
            },
        )
        .await
        .expect_err("未知账户");
        assert_eq!(err, "account not found");

        // 有账户但预测窗口外无分数
        let account_id = "zzz_test_api5_sim";
        create_zzz_account(&db, account_id, "active").await;
        let err = simulate_paper_nav_inner(
            &db,
            SimulatePaperNavRequest {
                paper_account_id: account_id.into(),
                prediction_set_id: pred_set.into(),
                start_date: "20260701".into(),
                end_date: "20260731".into(),
                top_n: None,
                rebalance_freq_days: None,
                benchmark: None,
                max_position_pct: None,
                commission_pct: None,
            },
        )
        .await
        .expect_err("窗口外无分数");
        assert_eq!(err, "no prediction scores found");

        cleanup_account(&db, account_id).await;
        cleanup_zzz_predictions(&db, pred_set).await;
    }

    #[tokio::test]
    async fn simulate_multi_window_validates_account_and_window_dates() {
        let db = test_db().await;
        let account_id = "zzz_test_api5_mw";
        create_zzz_account(&db, account_id, "active").await;

        let base_req = |start: &str| MultiWindowSimRequest {
            paper_account_id: account_id.into(),
            windows: vec![WindowConfig {
                prediction_set_id: "zzz_test_api5_mwpred".into(),
                start_date: start.into(),
                end_date: "20260630".into(),
                max_position_pct: None,
            }],
            top_n: None,
            rebalance_freq_days: None,
            benchmark: None,
            commission_pct: None,
        };

        // 账户不存在
        let mut req = base_req("20260601");
        req.paper_account_id = "zzz_test_api5_no_such".into();
        let err = simulate_multi_window_inner(&db, req)
            .await
            .expect_err("未知账户");
        assert_eq!(err, "account not found");

        // 窗口日期坏格式
        let err = simulate_multi_window_inner(&db, base_req("2026-06-01"))
            .await
            .expect_err("坏窗口日期");
        assert!(err.starts_with("start_date:"), "{err}");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn audit_event_writer_inserts_row() {
        let db = test_db().await;
        write_audit_event(
            &db,
            "zzz.test.event",
            "zzz_entity",
            "zzz_test_api5_audit",
            Some("zzz"),
            "第五批审计直写测试",
            json!({"k": "v"}),
        )
        .await
        .expect("write audit");
        let (event_type, actor, summary): (String, Option<String>, String) = sqlx::query_as(
            "SELECT event_type, actor, summary FROM audit_event WHERE entity_id = 'zzz_test_api5_audit'",
        )
        .fetch_one(&db)
        .await
        .expect("zzz audit row");
        assert_eq!(event_type, "zzz.test.event");
        assert_eq!(actor.as_deref(), Some("zzz"));
        assert_eq!(summary, "第五批审计直写测试");
        let _ = sqlx::query("DELETE FROM audit_event WHERE entity_id = 'zzz_test_api5_audit'")
            .execute(&db)
            .await;
    }
}
