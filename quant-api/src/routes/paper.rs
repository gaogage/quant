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

use crate::routes::shared::{PaperAccountRepository, PgPaperAccountRepo};
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
            sqlx::query(
                "INSERT INTO paper_position
                   (paper_position_id, paper_account_id, symbol, quantity, avg_cost,
                    target_weight, last_trade_date, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, 0, $5, $6, now(), now())
                 ON CONFLICT (paper_account_id, symbol)
                 DO UPDATE SET target_weight = EXCLUDED.target_weight,
                               last_trade_date = EXCLUDED.last_trade_date,
                               updated_at = now()",
            )
            .bind(&position_id)
            .bind(&paper_account_id)
            .bind(symbol)
            .bind(target_w) // quantity = weight (simplified)
            .bind(target_w)
            .bind(score_day)
            .execute(db)
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
    let snapshots = sqlx::query_as::<_, (String, NaiveDate, i32)>(
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

        for day_idx in 0..trading_days.len() {
            let today = trading_days[day_idx];
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
    let account = sqlx::query_as::<_, (String, String, Decimal, Decimal, String)>(
        "SELECT paper_account_id, name, initial_capital, cash, status
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

    Ok(json!({
        "paper_account_id": account.0,
        "name": account.1,
        "initial_capital": account.2,
        "cash": account.3,
        "status": account.4,
        "nav": account.3,
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
