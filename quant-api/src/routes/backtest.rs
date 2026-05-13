/// 回测路由

use axum::{extract::State, response::IntoResponse, Json};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use quant_backtest::engine::{BacktestConfig, BacktestMode, StrategySignal};
use quant_backtest::portfolio::FeeConfig;
use quant_backtest::runner::BacktestRunner;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct RunBacktestReq {
    pub symbols: Vec<String>,
    pub weights: Option<Vec<f64>>,  // equal weight if None
    pub benchmark: Option<String>,
    pub start_date: String,
    pub end_date: String,
    pub initial_capital: Option<f64>,
    pub mode: Option<String>,  // fast/standard/audit
}

pub async fn run_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunBacktestReq>,
) -> impl IntoResponse {
    let task_id = format!("bt-{}", Uuid::new_v4());
    info!(task_id, symbols = req.symbols.len(), "启动回测");

    let start = NaiveDate::parse_from_str(&req.start_date, "%Y%m%d").unwrap();
    let end = NaiveDate::parse_from_str(&req.end_date, "%Y%m%d").unwrap();
    let capital = Decimal::from_f64(req.initial_capital.unwrap_or(1_000_000.0)).unwrap();

    let mode = match req.mode.as_deref() {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    };

    let config = BacktestConfig {
        initial_capital: capital,
        benchmark: req.benchmark.unwrap_or_else(|| "000300.SH".into()),
        start_date: start,
        end_date: end,
        fee_config: FeeConfig::default(),
        mode,
        max_position_pct: Decimal::new(10, 2),
        risk_control: Default::default(),
    };

    // Equal-weight strategy signals
    let n = req.symbols.len() as f64;
    let weights: Vec<f64> = req.weights.unwrap_or_else(|| vec![1.0 / n; req.symbols.len()]);
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();

    // Generate daily signals: hold equal weight every day
    let target_weights: HashMap<String, Decimal> = req.symbols.iter()
        .zip(weights.iter())
        .map(|(s, w)| (s.clone(), Decimal::from_f64(*w).unwrap()))
        .collect();

    // Get trading days for signal generation
    let trading_days: Vec<NaiveDate> = sqlx::query_as(
        "SELECT trade_date FROM market_trade_calendar
         WHERE exchange = 'SSE' AND is_open = true
         AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(d,): (NaiveDate,)| d)
    .collect();

    for day in &trading_days {
        signals.insert(*day, StrategySignal {
            date: *day,
            target_weights: target_weights.clone(),
        });
    }

    // Run backtest
    let runner = BacktestRunner::new(state.db.clone());
    match runner.run(&task_id, config, &signals).await {
        Ok(output) => {
            let first_day = output.equity_curve.first().map(|(d,_)| d.to_string()).unwrap_or_default();
            let last_day = output.equity_curve.last().map(|(d,_)| d.to_string()).unwrap_or_default();
            Json(json!({
                "code": 0,
                "data": {
                    "task_id": task_id,
                    "status": "completed",
                    "debug": {
                        "signals_count": signals.len(),
                        "first_signal_date": signals.keys().min().map(|d| d.to_string()),
                        "signal_symbols": target_weights.keys().collect::<Vec<_>>(),
                        "first_eq_date": first_day,
                        "last_eq_date": last_day,
                    },
                    "metrics": {
                        "total_return_pct": output.metrics.total_return_pct,
                        "annual_return_pct": output.metrics.annual_return_pct,
                        "sharpe_ratio": output.metrics.sharpe_ratio,
                        "max_drawdown_pct": output.metrics.max_drawdown_pct,
                        "calmar_ratio": output.metrics.calmar_ratio,
                        "benchmark_return_pct": output.metrics.benchmark_return_pct,
                        "excess_return_pct": output.metrics.excess_return_pct,
                        "num_trades": output.metrics.num_trades,
                    },
                    "trades": output.trades.len(),
                    "equity_points": output.equity_curve.len(),
                }
            }))
        }
        Err(e) => {
            Json(json!({"code": 1, "message": e.to_string()}))
        }
    }
}

// ─── Factor-based backtest ────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RunFactorBacktestReq {
    pub combo_name: String,
    #[serde(default = "default_combo_version")]
    pub version: String,
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    /// Rebalance frequency: "daily", "weekly", "monthly" (or integer trading days)
    #[serde(default = "default_rebalance")]
    pub rebalance: String,
    /// Entry delay in trading days: 0=enter next day, N=wait N days after signal
    #[serde(default = "default_entry_delay")]
    pub entry_delay: usize,
    /// Minimum daily trading amount in CNY (e.g. 50000000 = 50M). 0 = no filter.
    #[serde(default)]
    pub min_amount: f64,
    #[serde(default = "default_max_pct")]
    pub max_position_pct: f64,
    pub benchmark: Option<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default = "default_capital")]
    pub initial_capital: f64,
    pub mode: Option<String>,
}

fn default_combo_version() -> String { "1.0.0".into() }
fn default_top_n() -> usize { 20 }
fn default_rebalance() -> String { "monthly".into() }
fn default_entry_delay() -> usize { 0 }
fn default_max_pct() -> f64 { 0.10 }
fn default_capital() -> f64 { 1_000_000.0 }

pub async fn run_factor_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunFactorBacktestReq>,
) -> impl IntoResponse {
    let task_id = format!("fbt-{}", Uuid::new_v4());
    let start = NaiveDate::parse_from_str(&req.start_date, "%Y%m%d").unwrap();
    let end = NaiveDate::parse_from_str(&req.end_date, "%Y%m%d").unwrap();
    let capital = Decimal::from_f64(req.initial_capital).unwrap();

    let reb_freq = match req.rebalance.as_str() {
        "daily" => 1,
        "weekly" => 5,
        "monthly" => 20,
        s => s.parse::<usize>().unwrap_or(20),
    };

    let sig_config = quant_backtest::signal_generator::SignalConfig {
        combo_name: req.combo_name.clone(),
        version: req.version.clone(),
        top_n: req.top_n,
        rebalance_freq_days: reb_freq,
        entry_delay_days: req.entry_delay,
        min_daily_amount_cny: if req.min_amount > 0.0 { Some(req.min_amount) } else { None },
        max_position_pct: Decimal::from_f64(req.max_position_pct).unwrap(),
    };

    info!(task_id, combo=%req.combo_name, top_n=req.top_n, reb=reb_freq, "Generating factor signals");

    let signals = match quant_backtest::signal_generator::generate_signals(
        &state.db, &sig_config, start, end,
    ).await {
        Ok(s) => s,
        Err(e) => return Json(json!({"code": 1, "message": e})),
    };

    info!(task_id, signals = signals.len(), "Signals generated, running backtest");

    let mode = match req.mode.as_deref() {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    };

    let config = BacktestConfig {
        initial_capital: capital,
        benchmark: req.benchmark.unwrap_or_else(|| "000300.SH".into()),
        start_date: start,
        end_date: end,
        fee_config: FeeConfig::default(),
        mode,
        max_position_pct: Decimal::from_f64(req.max_position_pct).unwrap(),
        risk_control: Default::default(),
    };

    let runner = BacktestRunner::new(state.db.clone());
    match runner.run(&task_id, config, &signals).await {
        Ok(output) => {
            Json(json!({
                "code": 0,
                "data": {
                    "task_id": task_id,
                    "config": {
                        "combo": req.combo_name,
                        "top_n": req.top_n,
                        "rebalance": req.rebalance,
                        "entry_delay": req.entry_delay,
                        "signals_count": signals.len(),
                    },
                    "metrics": {
                        "total_return_pct": output.metrics.total_return_pct,
                        "annual_return_pct": output.metrics.annual_return_pct,
                        "sharpe_ratio": output.metrics.sharpe_ratio,
                        "max_drawdown_pct": output.metrics.max_drawdown_pct,
                        "calmar_ratio": output.metrics.calmar_ratio,
                        "benchmark_return_pct": output.metrics.benchmark_return_pct,
                        "excess_return_pct": output.metrics.excess_return_pct,
                        "num_trades": output.metrics.num_trades,
                        "win_rate_pct": output.metrics.win_rate_pct,
                    },
                    "trades": output.trades.len(),
                    "equity_points": output.equity_curve.len(),
                }
            }))
        }
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}
