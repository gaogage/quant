/// 回测路由
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use quant_backtest::engine::{
    BacktestConfig, BacktestMode, ExecutionPrice, ExecutionTiming, RiskControlConfig,
    StrategySignal,
};
use quant_backtest::portfolio::FeeConfig;
use quant_backtest::runner::{BacktestDataCache, BacktestRunner};
use quant_backtest::signal_generator::{
    MarketRegimePolicy, PortfolioConstructionMethod, PredictionBlendConfig, ScoreDirection,
    SignalDataCache, TradableUniverseProfile,
};

use crate::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct BacktestListQuery {
    pub strategy_version_id: Option<String>,
    pub data_version_id: Option<String>,
    pub status: Option<String>,
    pub benchmark_symbol: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl BacktestListQuery {
    fn normalized_page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn normalized_page_size(&self) -> i64 {
        self.page_size.unwrap_or(20).clamp(1, 200)
    }

    fn offset(&self) -> i64 {
        (self.normalized_page() - 1) * self.normalized_page_size()
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct EquityCurveQuery {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunBacktestReq {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub research_dataset_id: Option<String>,
    pub feature_set_version_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub portfolio_policy_id: Option<String>,
    pub symbols: Vec<String>,
    pub weights: Option<Vec<f64>>, // equal weight if None
    pub benchmark: Option<String>,
    pub start_date: String,
    pub end_date: String,
    pub initial_capital: Option<f64>,
    pub mode: Option<String>, // fast/standard/audit
    pub rebalance_frequency: Option<String>,
    pub cost_model: Option<CostModelReq>,
    pub execution_rules: Option<ExecutionRulesReq>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CostModelReq {
    pub commission_rate: Option<f64>,
    pub min_commission: Option<f64>,
    pub tax_rate: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub cost_multiplier: Option<f64>,
    pub impact_cost_coefficient: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionRulesReq {
    pub execution_timing: Option<String>,
    pub execution_price: Option<String>,
    pub max_participation_rate: Option<f64>,
}

fn parse_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .map_err(|_| format!("{} must use YYYYMMDD format", field))
}

fn parse_mode(value: Option<&str>) -> BacktestMode {
    match value {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    }
}

fn parse_execution_timing(value: Option<&str>) -> Result<ExecutionTiming, String> {
    match value {
        None | Some("next_open") => Ok(ExecutionTiming::NextOpen),
        Some("same_close_debug") => Ok(ExecutionTiming::SameCloseDebug),
        Some(other) => Err(format!("unsupported execution_timing: {}", other)),
    }
}

fn parse_execution_price(value: Option<&str>) -> Result<ExecutionPrice, String> {
    match value {
        None | Some("open") => Ok(ExecutionPrice::Open),
        Some("close") => Ok(ExecutionPrice::Close),
        Some(other) => Err(format!("unsupported execution_price: {}", other)),
    }
}

fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal, String> {
    Decimal::from_f64(value).ok_or_else(|| format!("{} must be a finite number", field))
}

fn apply_cost_model(base: FeeConfig, req: Option<&CostModelReq>) -> Result<FeeConfig, String> {
    let Some(req) = req else {
        return Ok(base);
    };
    Ok(FeeConfig {
        commission_rate: match req.commission_rate {
            Some(value) => decimal_from_f64(value, "cost_model.commission_rate")?,
            None => base.commission_rate,
        },
        min_commission: match req.min_commission {
            Some(value) => decimal_from_f64(value, "cost_model.min_commission")?,
            None => base.min_commission,
        },
        tax_rate: match req.tax_rate {
            Some(value) => decimal_from_f64(value, "cost_model.tax_rate")?,
            None => base.tax_rate,
        },
        slippage_bps: match req.slippage_bps {
            Some(value) => decimal_from_f64(value, "cost_model.slippage_bps")?,
            None => base.slippage_bps,
        },
        cost_multiplier: match req.cost_multiplier {
            Some(value) => decimal_from_f64(value, "cost_model.cost_multiplier")?,
            None => base.cost_multiplier,
        },
        impact_cost_coefficient: match req.impact_cost_coefficient {
            Some(value) => decimal_from_f64(value, "cost_model.impact_cost_coefficient")?,
            None => base.impact_cost_coefficient,
        },
    })
}

fn build_backtest_config(req: &RunBacktestReq) -> Result<BacktestConfig, String> {
    if req.symbols.is_empty() {
        return Err("symbols must not be empty".into());
    }
    if let Some(weights) = &req.weights {
        if weights.len() != req.symbols.len() {
            return Err("weights length must match symbols length".into());
        }
    }

    let start = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end < start {
        return Err("end_date must be greater than or equal to start_date".into());
    }

    let initial_capital = req.initial_capital.unwrap_or(1_000_000.0);
    let capital = Decimal::from_f64(initial_capital)
        .ok_or_else(|| "initial_capital must be a finite number".to_string())?;
    let execution_timing = parse_execution_timing(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_timing.as_deref()),
    )?;
    let execution_price = parse_execution_price(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_price.as_deref()),
    )?;
    let max_participation_rate = req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.max_participation_rate)
        .map(|value| decimal_from_f64(value, "execution_rules.max_participation_rate"))
        .transpose()?;

    Ok(BacktestConfig {
        initial_capital: capital,
        benchmark: req.benchmark.clone().unwrap_or_else(|| "000300.SH".into()),
        start_date: start,
        end_date: end,
        fee_config: apply_cost_model(FeeConfig::default(), req.cost_model.as_ref())?,
        mode: parse_mode(req.mode.as_deref()),
        max_position_pct: Decimal::new(10, 2),
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        research_dataset_id: req.research_dataset_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        portfolio_policy_id: req.portfolio_policy_id.clone(),
        symbols: req.symbols.clone(),
        rebalance_frequency: req
            .rebalance_frequency
            .clone()
            .unwrap_or_else(|| "daily".into()),
        execution_timing,
        execution_price,
        max_participation_rate,
        risk_control: Default::default(),
    })
}

pub async fn run_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunBacktestReq>,
) -> impl IntoResponse {
    let task_id = format!("bt-{}", Uuid::new_v4());
    info!(task_id, symbols = req.symbols.len(), "启动回测");

    let config = match build_backtest_config(&req) {
        Ok(config) => config,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    let start = config.start_date;
    let end = config.end_date;

    // Equal-weight strategy signals
    let weights: Vec<f64> = req
        .weights
        .clone()
        .unwrap_or_else(|| vec![1.0 / req.symbols.len() as f64; req.symbols.len()]);
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();

    // Generate daily signals: hold equal weight every day
    let target_weights: HashMap<String, Decimal> = req
        .symbols
        .iter()
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
        signals.insert(
            *day,
            StrategySignal {
                date: *day,
                target_weights: target_weights.clone(),
            },
        );
    }

    // Run backtest
    let runner = BacktestRunner::new(state.db.clone());
    match runner.run(&task_id, config, &signals).await {
        Ok(output) => {
            let first_day = output
                .equity_curve
                .first()
                .map(|(d, _)| d.to_string())
                .unwrap_or_default();
            let last_day = output
                .equity_curve
                .last()
                .map(|(d, _)| d.to_string())
                .unwrap_or_default();
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
                        "turnover": output.metrics.turnover,
                        "num_trades": output.metrics.num_trades,
                    },
                    "trades": output.trades.len(),
                    "equity_points": output.equity_curve.len(),
                }
            }))
        }
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

pub async fn list_backtests(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BacktestListQuery>,
) -> impl IntoResponse {
    let rows: Vec<(
        String,
        String,
        String,
        String,
        Vec<String>,
        NaiveDate,
        NaiveDate,
        String,
        i32,
        Option<NaiveDate>,
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
        Option<i32>,
    )> = sqlx::query_as(
        r#"SELECT t.task_id, t.status, t.strategy_version_id, t.data_version_id,
                  t.symbols, t.start_date, t.end_date, t.benchmark_symbol, t.progress,
                  t.last_completed_date,
                  r.total_return, r.sharpe_ratio, r.max_drawdown, r.total_trades
           FROM backtest_task t
           LEFT JOIN backtest_result r ON r.task_id = t.task_id
           WHERE ($1::varchar IS NULL OR t.strategy_version_id = $1)
             AND ($2::varchar IS NULL OR t.data_version_id = $2)
             AND ($3::varchar IS NULL OR t.status = $3)
             AND ($4::varchar IS NULL OR t.benchmark_symbol = $4)
           ORDER BY t.created_at DESC
           LIMIT $5 OFFSET $6"#,
    )
    .bind(query.strategy_version_id.as_deref())
    .bind(query.data_version_id.as_deref())
    .bind(query.status.as_deref())
    .bind(query.benchmark_symbol.as_deref())
    .bind(query.normalized_page_size())
    .bind(query.offset())
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(json!({"code": 0, "data": {
        "page": query.normalized_page(),
        "page_size": query.normalized_page_size(),
        "items": rows.into_iter().map(|row| json!({
            "task_id": row.0,
            "status": row.1,
            "strategy_version_id": row.2,
            "data_version_id": row.3,
            "symbols": row.4,
            "start_date": row.5,
            "end_date": row.6,
            "benchmark_symbol": row.7,
            "progress": row.8,
            "last_completed_date": row.9,
            "metrics": {
                "total_return": row.10,
                "sharpe_ratio": row.11,
                "max_drawdown": row.12,
                "total_trades": row.13,
            }
        })).collect::<Vec<_>>(),
    }}))
}

pub async fn backtest_summary(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let task: Option<(
        String,
        String,
        String,
        String,
        Vec<String>,
        NaiveDate,
        NaiveDate,
        String,
        i32,
        Option<NaiveDate>,
    )> = sqlx::query_as(
        r#"SELECT task_id, status, strategy_version_id, data_version_id,
                  symbols, start_date, end_date, benchmark_symbol, progress,
                  last_completed_date
           FROM backtest_task
           WHERE task_id = $1"#,
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(task) = task else {
        return Json(json!({"code": 1, "message": "backtest summary not found"}));
    };

    let result: Option<(
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Option<i32>,
        Option<Decimal>,
        String,
    )> = sqlx::query_as(
        r#"SELECT r.total_return, r.annualized_return, r.benchmark_return, r.excess_return,
                  r.sharpe_ratio, r.sortino_ratio, r.information_ratio, r.max_drawdown,
                  r.turnover, r.total_trades, r.win_rate, r.reproducibility_hash
           FROM backtest_result r
           WHERE r.task_id = $1"#,
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(result) = result else {
        return Json(json!({"code": 1, "message": "backtest summary not found"}));
    };

    Json(json!({"code": 0, "data": {
        "task": {
            "task_id": task.0,
            "status": task.1,
            "strategy_version_id": task.2,
            "data_version_id": task.3,
            "symbols": task.4,
            "start_date": task.5,
            "end_date": task.6,
            "benchmark_symbol": task.7,
            "progress": task.8,
            "last_completed_date": task.9,
        },
        "metrics": {
            "total_return": result.0,
            "annualized_return": result.1,
            "benchmark_return": result.2,
            "excess_return": result.3,
            "sharpe_ratio": result.4,
            "sortino_ratio": result.5,
            "information_ratio": result.6,
            "max_drawdown": result.7,
            "turnover": result.8,
            "total_trades": result.9,
            "win_rate": result.10,
            "reproducibility_hash": result.11,
        }
    }}))
}

pub async fn backtest_equity_curve(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Query(query): Query<EquityCurveQuery>,
) -> impl IntoResponse {
    let start = match query
        .start_date
        .as_deref()
        .map(|value| parse_yyyymmdd(value, "start_date"))
        .transpose()
    {
        Ok(value) => value,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    let end = match query
        .end_date
        .as_deref()
        .map(|value| parse_yyyymmdd(value, "end_date"))
        .transpose()
    {
        Ok(value) => value,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };

    let rows: Vec<(NaiveDate, Decimal)> = sqlx::query_as(
        r#"SELECT trade_date, portfolio_value
           FROM backtest_equity_curve
           WHERE task_id = $1
             AND ($2::date IS NULL OR trade_date >= $2)
             AND ($3::date IS NULL OR trade_date <= $3)
           ORDER BY trade_date"#,
    )
    .bind(&task_id)
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(json!({"code": 0, "data": {
        "task_id": task_id,
        "points": rows.into_iter().map(|row| json!({
            "trade_date": row.0,
            "portfolio_value": row.1,
        })).collect::<Vec<_>>(),
    }}))
}

// ─── Factor-based backtest ────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct RunFactorBacktestReq {
    pub combo_name: String,
    #[serde(default = "default_combo_version")]
    pub version: String,
    #[serde(default = "default_factor_strategy_version")]
    pub strategy_version_id: String,
    #[serde(default = "default_data_version")]
    pub data_version_id: String,
    pub research_dataset_id: Option<String>,
    pub feature_set_version_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub prediction_blend_weight: Option<f64>,
    pub prediction_min_percentile: Option<f64>,
    pub portfolio_policy_id: Option<String>,
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
    /// Skip top N% of ranked stocks to avoid value traps (extreme reversal = junk)
    #[serde(default)]
    pub skip_top_pct: f64,
    pub max_pairwise_correlation: Option<f64>,
    #[serde(default = "default_correlation_lookback_days")]
    pub correlation_lookback_days: usize,
    #[serde(default)]
    pub kelly_fraction: f64,
    #[serde(default = "default_kelly_lookback_days")]
    pub kelly_lookback_days: usize,
    #[serde(default = "default_max_gross_exposure")]
    pub max_gross_exposure: f64,
    #[serde(default = "default_score_direction")]
    pub score_direction: String,
    #[serde(default = "default_portfolio_method")]
    pub portfolio_method: String,
    #[serde(default = "default_risk_budget_lookback_days")]
    pub risk_budget_lookback_days: usize,
    #[serde(default)]
    pub capacity_penalty_strength: f64,
    pub industry_max_weight_pct: Option<f64>,
    #[serde(default)]
    pub score_candidate_pool_size: Option<usize>,
    pub universe_profile: Option<String>,
    pub cost_model: Option<CostModelReq>,
    pub execution_rules: Option<ExecutionRulesReq>,
    pub benchmark: Option<String>,
    pub market_regime: Option<MarketRegimeBacktestReq>,
    pub portfolio_drawdown_reduce_start_pct: Option<f64>,
    pub portfolio_drawdown_reduce_full_pct: Option<f64>,
    pub portfolio_drawdown_min_exposure: Option<f64>,
    pub portfolio_drawdown_peak_lookback_days: Option<usize>,
    pub portfolio_drawdown_recovery_start_pct: Option<f64>,
    pub portfolio_drawdown_recovery_full_pct: Option<f64>,
    pub portfolio_drawdown_recovery_boost: Option<f64>,
    pub portfolio_volatility_target_pct: Option<f64>,
    pub portfolio_volatility_lookback_days: Option<usize>,
    pub portfolio_volatility_min_exposure: Option<f64>,
    pub portfolio_volatility_max_exposure: Option<f64>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default = "default_capital")]
    pub initial_capital: f64,
    pub mode: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MarketRegimeBacktestReq {
    pub enabled: Option<bool>,
    pub policy: Option<String>,
    pub benchmark: Option<String>,
    pub lookback_days: Option<usize>,
    pub min_observations: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RunPredictionBacktestReq {
    pub prediction_set_id: String,
    #[serde(default = "default_prediction_strategy_version")]
    pub strategy_version_id: String,
    #[serde(default = "default_data_version")]
    pub data_version_id: String,
    pub research_dataset_id: Option<String>,
    pub feature_set_version_id: Option<String>,
    pub portfolio_policy_id: Option<String>,
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    #[serde(default = "default_rebalance")]
    pub rebalance: String,
    #[serde(default = "default_entry_delay")]
    pub entry_delay: usize,
    #[serde(default)]
    pub min_amount: f64,
    #[serde(default = "default_max_pct")]
    pub max_position_pct: f64,
    #[serde(default)]
    pub skip_top_pct: f64,
    pub max_pairwise_correlation: Option<f64>,
    #[serde(default = "default_correlation_lookback_days")]
    pub correlation_lookback_days: usize,
    #[serde(default)]
    pub kelly_fraction: f64,
    #[serde(default = "default_kelly_lookback_days")]
    pub kelly_lookback_days: usize,
    #[serde(default = "default_max_gross_exposure")]
    pub max_gross_exposure: f64,
    #[serde(default = "default_score_direction")]
    pub score_direction: String,
    #[serde(default = "default_portfolio_method")]
    pub portfolio_method: String,
    #[serde(default = "default_risk_budget_lookback_days")]
    pub risk_budget_lookback_days: usize,
    #[serde(default)]
    pub capacity_penalty_strength: f64,
    pub industry_max_weight_pct: Option<f64>,
    pub cost_model: Option<CostModelReq>,
    pub execution_rules: Option<ExecutionRulesReq>,
    pub benchmark: Option<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default = "default_capital")]
    pub initial_capital: f64,
    pub mode: Option<String>,
}

pub(crate) struct FactorBacktestRunOutput {
    pub signals_count: usize,
    pub metrics: quant_backtest::metrics::BacktestMetrics,
    pub trades: usize,
    pub equity_points: usize,
}

fn default_combo_version() -> String {
    "1.0.0".into()
}
fn default_factor_strategy_version() -> String {
    "factor-debug-strategy".into()
}
fn default_prediction_strategy_version() -> String {
    "prediction-set-strategy".into()
}
fn default_data_version() -> String {
    "debug-data".into()
}
fn default_top_n() -> usize {
    20
}
fn default_rebalance() -> String {
    "monthly".into()
}
fn default_entry_delay() -> usize {
    0
}
fn default_max_pct() -> f64 {
    0.10
}
fn default_correlation_lookback_days() -> usize {
    60
}
fn default_kelly_lookback_days() -> usize {
    60
}
fn default_max_gross_exposure() -> f64 {
    1.0
}
fn default_score_direction() -> String {
    "descending".into()
}
fn default_portfolio_method() -> String {
    "heuristic".into()
}
fn default_risk_budget_lookback_days() -> usize {
    60
}
fn default_capital() -> f64 {
    1_000_000.0
}

fn parse_score_direction(value: &str) -> Result<ScoreDirection, String> {
    match value {
        "descending" | "desc" => Ok(ScoreDirection::Descending),
        "ascending" | "asc" => Ok(ScoreDirection::Ascending),
        other => Err(format!("unsupported score_direction: {}", other)),
    }
}

fn parse_portfolio_method(value: &str) -> Result<PortfolioConstructionMethod, String> {
    match value {
        "heuristic" | "legacy" => Ok(PortfolioConstructionMethod::Heuristic),
        "risk_budget" | "risk-budget" => Ok(PortfolioConstructionMethod::RiskBudget),
        other => Err(format!("unsupported portfolio_method: {}", other)),
    }
}

fn parse_tradable_universe_profile(value: Option<&str>) -> Result<TradableUniverseProfile, String> {
    value
        .map(TradableUniverseProfile::parse)
        .unwrap_or(Ok(TradableUniverseProfile::All))
}

fn build_prediction_blend_config(
    prediction_set_id: Option<&String>,
    prediction_blend_weight: Option<f64>,
    prediction_min_percentile: Option<f64>,
) -> Result<Option<PredictionBlendConfig>, String> {
    let prediction_weight = prediction_blend_weight.unwrap_or(0.0);
    if !prediction_weight.is_finite() || !(0.0..=1.0).contains(&prediction_weight) {
        return Err("prediction_blend_weight must be between 0 and 1".into());
    }
    if let Some(min_percentile) = prediction_min_percentile {
        if !min_percentile.is_finite() || !(0.0..=1.0).contains(&min_percentile) {
            return Err("prediction_min_percentile must be between 0 and 1".into());
        }
    }
    if prediction_weight <= f64::EPSILON && prediction_min_percentile.is_none() {
        return Ok(None);
    }
    let prediction_set_id = prediction_set_id
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "prediction_set_id is required when prediction overlay is enabled".to_string()
        })?;
    Ok(Some(PredictionBlendConfig {
        prediction_set_id: prediction_set_id.to_string(),
        factor_weight: 1.0 - prediction_weight,
        prediction_weight,
        prediction_min_percentile,
    }))
}

fn optional_unit_f64(value: Option<f64>, name: &str) -> Result<Option<f64>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(format!("{} must be between 0 and 1", name));
    }
    Ok(Some(value))
}

fn optional_decimal_pct(value: Option<f64>, name: &str) -> Result<Option<Decimal>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(format!("{} must be between 0 and 1", name));
    }
    decimal_from_f64(value, name).map(Some)
}

fn optional_positive_decimal_pct(
    value: Option<f64>,
    name: &str,
) -> Result<Option<Decimal>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) || value <= 0.0 {
        return Err(format!("{} must be greater than 0 and at most 1", name));
    }
    decimal_from_f64(value, name).map(Some)
}

fn build_portfolio_risk_control(req: &RunFactorBacktestReq) -> Result<RiskControlConfig, String> {
    let start = optional_decimal_pct(
        req.portfolio_drawdown_reduce_start_pct,
        "portfolio_drawdown_reduce_start_pct",
    )?;
    let full = optional_decimal_pct(
        req.portfolio_drawdown_reduce_full_pct,
        "portfolio_drawdown_reduce_full_pct",
    )?;
    let min_exposure = optional_decimal_pct(
        req.portfolio_drawdown_min_exposure,
        "portfolio_drawdown_min_exposure",
    )?;
    let recovery_start = optional_decimal_pct(
        req.portfolio_drawdown_recovery_start_pct,
        "portfolio_drawdown_recovery_start_pct",
    )?;
    let recovery_full = optional_decimal_pct(
        req.portfolio_drawdown_recovery_full_pct,
        "portfolio_drawdown_recovery_full_pct",
    )?;
    let recovery_boost = optional_decimal_pct(
        req.portfolio_drawdown_recovery_boost,
        "portfolio_drawdown_recovery_boost",
    )?;
    let volatility_target = optional_positive_decimal_pct(
        req.portfolio_volatility_target_pct,
        "portfolio_volatility_target_pct",
    )?;
    let volatility_min_exposure = optional_decimal_pct(
        req.portfolio_volatility_min_exposure,
        "portfolio_volatility_min_exposure",
    )?;
    let volatility_max_exposure = optional_decimal_pct(
        req.portfolio_volatility_max_exposure,
        "portfolio_volatility_max_exposure",
    )?;

    let provided = start.is_some() || full.is_some() || min_exposure.is_some();
    let complete = start.is_some() && full.is_some() && min_exposure.is_some();
    if provided && !complete {
        return Err(
            "portfolio drawdown risk control requires start_pct, full_pct, and min_exposure".into(),
        );
    }
    if let (Some(start), Some(full)) = (start, full) {
        if full <= start {
            return Err(
                "portfolio_drawdown_reduce_full_pct must be greater than portfolio_drawdown_reduce_start_pct"
                    .into(),
            );
        }
    }
    let recovery_provided =
        recovery_start.is_some() || recovery_full.is_some() || recovery_boost.is_some();
    let recovery_complete = recovery_start.is_some() && recovery_full.is_some();
    if recovery_provided && !recovery_complete {
        return Err(
            "portfolio drawdown recovery requires recovery_start_pct and recovery_full_pct".into(),
        );
    }
    if let (Some(start), Some(full)) = (recovery_start, recovery_full) {
        if full <= start {
            return Err(
                "portfolio_drawdown_recovery_full_pct must be greater than portfolio_drawdown_recovery_start_pct"
                    .into(),
            );
        }
    }
    let volatility_lookback_days = match req.portfolio_volatility_lookback_days {
        Some(days) if days < 2 => {
            return Err("portfolio_volatility_lookback_days must be at least 2".into());
        }
        Some(days) => Some(days),
        None => None,
    };
    let volatility_provided = volatility_target.is_some()
        || volatility_lookback_days.is_some()
        || volatility_min_exposure.is_some()
        || volatility_max_exposure.is_some();
    if volatility_provided && volatility_target.is_none() {
        return Err(
            "portfolio volatility risk control requires portfolio_volatility_target_pct".into(),
        );
    }
    if let (Some(min_exposure), Some(max_exposure)) =
        (volatility_min_exposure, volatility_max_exposure)
    {
        if max_exposure < min_exposure {
            return Err(
                "portfolio_volatility_max_exposure must be greater than or equal to portfolio_volatility_min_exposure"
                    .into(),
            );
        }
    }

    Ok(RiskControlConfig {
        portfolio_drawdown_reduce_start_pct: start,
        portfolio_drawdown_reduce_full_pct: full,
        portfolio_drawdown_min_exposure: min_exposure,
        portfolio_drawdown_peak_lookback_days: req
            .portfolio_drawdown_peak_lookback_days
            .filter(|days| *days > 0),
        portfolio_drawdown_recovery_start_pct: recovery_start,
        portfolio_drawdown_recovery_full_pct: recovery_full,
        portfolio_drawdown_recovery_boost: recovery_boost,
        portfolio_volatility_target_pct: volatility_target,
        portfolio_volatility_lookback_days: volatility_lookback_days,
        portfolio_volatility_min_exposure: volatility_min_exposure,
        portfolio_volatility_max_exposure: volatility_max_exposure,
        ..RiskControlConfig::default()
    })
}

fn build_market_regime_policy(
    req: Option<&MarketRegimeBacktestReq>,
    default_benchmark: &str,
) -> Result<Option<MarketRegimePolicy>, String> {
    let Some(req) = req else {
        return Ok(None);
    };
    if matches!(req.enabled, Some(false)) {
        return Ok(None);
    }

    let benchmark = req
        .benchmark
        .as_deref()
        .unwrap_or(default_benchmark)
        .trim()
        .to_string();
    let policy_name = req
        .policy
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("professional_default");
    let mut policy = match policy_name {
        "professional_default" => MarketRegimePolicy::professional_default(benchmark),
        "drawdown_control_v1" => MarketRegimePolicy::drawdown_control_v1(benchmark),
        "drawdown_control_v2" => MarketRegimePolicy::drawdown_control_v2(benchmark),
        "quality_risk_off_v1" => MarketRegimePolicy::quality_risk_off_v1(benchmark),
        "quality_crash_guard_v1" => MarketRegimePolicy::quality_crash_guard_v1(benchmark),
        other => return Err(format!("unsupported market_regime policy: {}", other)),
    };
    if let Some(lookback_days) = req.lookback_days {
        policy.lookback_days = lookback_days.max(1);
    }
    if let Some(min_observations) = req.min_observations {
        policy.min_observations = min_observations.max(1);
    }

    Ok(Some(policy))
}

pub async fn run_factor_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunFactorBacktestReq>,
) -> impl IntoResponse {
    let task_id = format!("fbt-{}", Uuid::new_v4());
    let config_summary = json!({
        "combo": req.combo_name,
        "top_n": req.top_n,
        "rebalance": req.rebalance,
        "entry_delay": req.entry_delay,
    });

    match execute_factor_backtest(&state.db, &task_id, req).await {
        Ok(output) => Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "config": {
                    "combo": config_summary["combo"],
                    "top_n": config_summary["top_n"],
                    "rebalance": config_summary["rebalance"],
                    "entry_delay": config_summary["entry_delay"],
                    "signals_count": output.signals_count,
                },
                "metrics": {
                    "total_return_pct": output.metrics.total_return_pct,
                    "annual_return_pct": output.metrics.annual_return_pct,
                    "sharpe_ratio": output.metrics.sharpe_ratio,
                    "max_drawdown_pct": output.metrics.max_drawdown_pct,
                    "calmar_ratio": output.metrics.calmar_ratio,
                    "benchmark_return_pct": output.metrics.benchmark_return_pct,
                    "excess_return_pct": output.metrics.excess_return_pct,
                    "turnover": output.metrics.turnover,
                    "num_trades": output.metrics.num_trades,
                    "win_rate_pct": output.metrics.win_rate_pct,
                },
                "trades": output.trades,
                "equity_points": output.equity_points,
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

pub async fn run_prediction_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunPredictionBacktestReq>,
) -> impl IntoResponse {
    let task_id = format!("pbt-{}", Uuid::new_v4());
    let config_summary = json!({
        "prediction_set_id": req.prediction_set_id,
        "top_n": req.top_n,
        "rebalance": req.rebalance,
        "entry_delay": req.entry_delay,
    });

    match execute_prediction_backtest(&state.db, &task_id, req).await {
        Ok(output) => Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "config": {
                    "prediction_set_id": config_summary["prediction_set_id"],
                    "top_n": config_summary["top_n"],
                    "rebalance": config_summary["rebalance"],
                    "entry_delay": config_summary["entry_delay"],
                    "signals_count": output.signals_count,
                },
                "metrics": {
                    "total_return_pct": output.metrics.total_return_pct,
                    "annual_return_pct": output.metrics.annual_return_pct,
                    "sharpe_ratio": output.metrics.sharpe_ratio,
                    "max_drawdown_pct": output.metrics.max_drawdown_pct,
                    "calmar_ratio": output.metrics.calmar_ratio,
                    "benchmark_return_pct": output.metrics.benchmark_return_pct,
                    "excess_return_pct": output.metrics.excess_return_pct,
                    "turnover": output.metrics.turnover,
                    "num_trades": output.metrics.num_trades,
                    "win_rate_pct": output.metrics.win_rate_pct,
                },
                "trades": output.trades,
                "equity_points": output.equity_points,
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

pub(crate) async fn execute_factor_backtest(
    db: &sqlx::PgPool,
    task_id: &str,
    req: RunFactorBacktestReq,
) -> Result<FactorBacktestRunOutput, String> {
    execute_factor_backtest_with_signal_cache(db, task_id, req, None).await
}

pub(crate) async fn execute_factor_backtest_with_signal_cache(
    db: &sqlx::PgPool,
    task_id: &str,
    req: RunFactorBacktestReq,
    signal_cache: Option<&mut SignalDataCache>,
) -> Result<FactorBacktestRunOutput, String> {
    execute_factor_backtest_with_caches(db, task_id, req, signal_cache, None).await
}

pub(crate) async fn execute_factor_backtest_with_caches(
    db: &sqlx::PgPool,
    task_id: &str,
    req: RunFactorBacktestReq,
    signal_cache: Option<&mut SignalDataCache>,
    backtest_cache: Option<&mut BacktestDataCache>,
) -> Result<FactorBacktestRunOutput, String> {
    let start = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end < start {
        return Err("end_date must be greater than or equal to start_date".into());
    }
    let capital = decimal_from_f64(req.initial_capital, "initial_capital")?;
    let fee_config = match apply_cost_model(FeeConfig::default(), req.cost_model.as_ref()) {
        Ok(config) => config,
        Err(message) => return Err(message),
    };
    let benchmark = req.benchmark.clone().unwrap_or_else(|| "000300.SH".into());
    let execution_timing = match parse_execution_timing(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_timing.as_deref()),
    ) {
        Ok(value) => value,
        Err(message) => return Err(message),
    };
    let execution_price = match parse_execution_price(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_price.as_deref()),
    ) {
        Ok(value) => value,
        Err(message) => return Err(message),
    };
    let max_participation_rate = match req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.max_participation_rate)
        .map(|value| decimal_from_f64(value, "execution_rules.max_participation_rate"))
        .transpose()
    {
        Ok(value) => value,
        Err(message) => return Err(message),
    };

    let reb_freq = match req.rebalance.as_str() {
        "daily" => 1,
        "weekly" => 5,
        "monthly" => 20,
        s => s.parse::<usize>().unwrap_or(20),
    };
    let score_direction = parse_score_direction(&req.score_direction)?;
    let portfolio_method = parse_portfolio_method(&req.portfolio_method)?;
    let universe_profile = parse_tradable_universe_profile(req.universe_profile.as_deref())?;
    let industry_max_weight_pct =
        optional_unit_f64(req.industry_max_weight_pct, "industry_max_weight_pct")?;

    let sig_config = quant_backtest::signal_generator::SignalConfig {
        combo_name: req.combo_name.clone(),
        version: req.version.clone(),
        top_n: req.top_n,
        rebalance_freq_days: reb_freq,
        entry_delay_days: req.entry_delay,
        min_daily_amount_cny: if req.min_amount > 0.0 {
            Some(req.min_amount)
        } else {
            None
        },
        max_position_pct: decimal_from_f64(req.max_position_pct, "max_position_pct")?,
        skip_top_pct: req.skip_top_pct,
        max_pairwise_correlation: req.max_pairwise_correlation,
        correlation_lookback_days: req.correlation_lookback_days,
        kelly_fraction: req.kelly_fraction,
        kelly_lookback_days: req.kelly_lookback_days,
        max_gross_exposure: req.max_gross_exposure,
        score_direction,
        portfolio_method,
        risk_budget_lookback_days: req.risk_budget_lookback_days,
        capacity_penalty_strength: req.capacity_penalty_strength,
        industry_max_weight_pct,
        score_candidate_pool_size: req.score_candidate_pool_size.filter(|size| *size > 0),
        universe_profile,
        prediction_blend: build_prediction_blend_config(
            req.prediction_set_id.as_ref(),
            req.prediction_blend_weight,
            req.prediction_min_percentile,
        )?,
    };

    info!(task_id, combo=%req.combo_name, top_n=req.top_n, reb=reb_freq, "Generating factor signals");

    let regime_policy = build_market_regime_policy(req.market_regime.as_ref(), &benchmark)?;
    let signals = match (regime_policy.as_ref(), signal_cache) {
        (Some(policy), Some(cache)) => {
            quant_backtest::signal_generator::generate_regime_signals_with_cache(
                db,
                &sig_config,
                policy,
                start,
                end,
                cache,
            )
            .await
        }
        (Some(policy), None) => {
            quant_backtest::signal_generator::generate_regime_signals(
                db,
                &sig_config,
                policy,
                start,
                end,
            )
            .await
        }
        (None, Some(cache)) => {
            quant_backtest::signal_generator::generate_signals_with_cache(
                db,
                &sig_config,
                start,
                end,
                cache,
            )
            .await
        }
        (None, None) => {
            quant_backtest::signal_generator::generate_signals(db, &sig_config, start, end).await
        }
    }?;

    info!(
        task_id,
        signals = signals.len(),
        "Signals generated, running backtest"
    );

    let mode = match req.mode.as_deref() {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    };

    let config = BacktestConfig {
        initial_capital: capital,
        benchmark,
        start_date: start,
        end_date: end,
        fee_config,
        mode,
        max_position_pct: Decimal::from_f64(req.max_position_pct).unwrap(),
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        research_dataset_id: req.research_dataset_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        portfolio_policy_id: req.portfolio_policy_id.clone(),
        symbols: signals
            .values()
            .flat_map(|signal| signal.target_weights.keys().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect(),
        rebalance_frequency: req.rebalance.clone(),
        execution_timing,
        execution_price,
        max_participation_rate,
        risk_control: build_portfolio_risk_control(&req)?,
    };

    let runner = BacktestRunner::new(db.clone());
    match runner
        .run_with_cache(task_id, config, &signals, backtest_cache)
        .await
    {
        Ok(output) => Ok(FactorBacktestRunOutput {
            signals_count: signals.len(),
            trades: output.trades.len(),
            equity_points: output.equity_curve.len(),
            metrics: output.metrics,
        }),
        Err(e) => Err(e.to_string()),
    }
}

pub(crate) async fn execute_prediction_backtest(
    db: &sqlx::PgPool,
    task_id: &str,
    req: RunPredictionBacktestReq,
) -> Result<FactorBacktestRunOutput, String> {
    let prediction_set_id = req.prediction_set_id.trim().to_string();
    if prediction_set_id.is_empty() {
        return Err("prediction_set_id must not be empty".into());
    }
    let start = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end < start {
        return Err("end_date must be greater than or equal to start_date".into());
    }
    let capital = decimal_from_f64(req.initial_capital, "initial_capital")?;
    let fee_config = apply_cost_model(FeeConfig::default(), req.cost_model.as_ref())?;
    let execution_timing = parse_execution_timing(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_timing.as_deref()),
    )?;
    let execution_price = parse_execution_price(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_price.as_deref()),
    )?;
    let max_participation_rate = req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.max_participation_rate)
        .map(|value| decimal_from_f64(value, "execution_rules.max_participation_rate"))
        .transpose()?;

    let reb_freq = match req.rebalance.as_str() {
        "daily" => 1,
        "weekly" => 5,
        "monthly" => 20,
        s => s.parse::<usize>().unwrap_or(20),
    };
    let score_direction = parse_score_direction(&req.score_direction)?;
    let portfolio_method = parse_portfolio_method(&req.portfolio_method)?;
    let industry_max_weight_pct =
        optional_unit_f64(req.industry_max_weight_pct, "industry_max_weight_pct")?;

    let sig_config = quant_backtest::signal_generator::PredictionSignalConfig {
        prediction_set_id: prediction_set_id.clone(),
        top_n: req.top_n,
        rebalance_freq_days: reb_freq,
        entry_delay_days: req.entry_delay,
        min_daily_amount_cny: if req.min_amount > 0.0 {
            Some(req.min_amount)
        } else {
            None
        },
        max_position_pct: decimal_from_f64(req.max_position_pct, "max_position_pct")?,
        skip_top_pct: req.skip_top_pct,
        max_pairwise_correlation: req.max_pairwise_correlation,
        correlation_lookback_days: req.correlation_lookback_days,
        kelly_fraction: req.kelly_fraction,
        kelly_lookback_days: req.kelly_lookback_days,
        max_gross_exposure: req.max_gross_exposure,
        score_direction,
        portfolio_method,
        risk_budget_lookback_days: req.risk_budget_lookback_days,
        capacity_penalty_strength: req.capacity_penalty_strength,
        industry_max_weight_pct,
    };

    info!(
        task_id,
        prediction_set_id=%prediction_set_id,
        top_n=req.top_n,
        reb=reb_freq,
        "Generating prediction signals"
    );

    let signals =
        quant_backtest::signal_generator::generate_prediction_signals(db, &sig_config, start, end)
            .await?;

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
        fee_config,
        mode,
        max_position_pct: Decimal::from_f64(req.max_position_pct).unwrap(),
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        research_dataset_id: req.research_dataset_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        prediction_set_id: Some(prediction_set_id),
        portfolio_policy_id: req.portfolio_policy_id.clone(),
        symbols: signals
            .values()
            .flat_map(|signal| signal.target_weights.keys().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect(),
        rebalance_frequency: req.rebalance.clone(),
        execution_timing,
        execution_price,
        max_participation_rate,
        risk_control: Default::default(),
    };

    let runner = BacktestRunner::new(db.clone());
    runner
        .run(task_id, config, &signals)
        .await
        .map(|output| FactorBacktestRunOutput {
            signals_count: signals.len(),
            trades: output.trades.len(),
            equity_points: output.equity_curve.len(),
            metrics: output.metrics,
        })
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_yyyymmdd_rejects_invalid_date() {
        let err = parse_yyyymmdd("2024-01-01", "start_date").unwrap_err();
        assert!(err.contains("start_date"));
    }

    #[test]
    fn build_config_requires_weight_count_to_match_symbols() {
        let req = RunBacktestReq {
            strategy_version_id: "strategy-v1".into(),
            data_version_id: "data-v1".into(),
            research_dataset_id: None,
            feature_set_version_id: None,
            prediction_set_id: None,
            portfolio_policy_id: None,
            symbols: vec!["000001.SZ".into(), "600000.SH".into()],
            weights: Some(vec![0.5]),
            benchmark: None,
            start_date: "20240101".into(),
            end_date: "20240131".into(),
            initial_capital: Some(1_000_000.0),
            mode: None,
            rebalance_frequency: None,
            cost_model: None,
            execution_rules: None,
        };

        let err = build_backtest_config(&req).unwrap_err();
        assert!(err.contains("weights"));
    }

    #[test]
    fn build_config_accepts_cost_and_capacity_rules() {
        let req = RunBacktestReq {
            strategy_version_id: "strategy-v1".into(),
            data_version_id: "data-v1".into(),
            research_dataset_id: None,
            feature_set_version_id: None,
            prediction_set_id: None,
            portfolio_policy_id: None,
            symbols: vec!["000001.SZ".into()],
            weights: Some(vec![1.0]),
            benchmark: None,
            start_date: "20240101".into(),
            end_date: "20240131".into(),
            initial_capital: Some(1_000_000.0),
            mode: None,
            rebalance_frequency: None,
            cost_model: Some(CostModelReq {
                commission_rate: None,
                min_commission: None,
                tax_rate: None,
                slippage_bps: Some(0.0002),
                cost_multiplier: Some(1.5),
                impact_cost_coefficient: Some(0.02),
            }),
            execution_rules: Some(ExecutionRulesReq {
                execution_timing: Some("next_open".into()),
                execution_price: Some("open".into()),
                max_participation_rate: Some(0.10),
            }),
        };

        let config = build_backtest_config(&req).unwrap();

        assert_eq!(
            config.fee_config.cost_multiplier,
            Decimal::from_f64(1.5).unwrap()
        );
        assert_eq!(
            config.fee_config.impact_cost_coefficient,
            Decimal::from_f64(0.02).unwrap()
        );
        assert_eq!(
            config.max_participation_rate,
            Some(Decimal::from_f64(0.10).unwrap())
        );
    }

    #[test]
    fn prediction_blend_requires_prediction_set_when_weight_is_positive() {
        let err = build_prediction_blend_config(None, Some(0.25), None).unwrap_err();

        assert!(err.contains("prediction_set_id"));
    }

    #[test]
    fn prediction_blend_weight_builds_factor_prediction_weights() {
        let prediction_set_id = " pred-quality-growth-v1 ".to_string();

        let blend = build_prediction_blend_config(Some(&prediction_set_id), Some(0.35), None)
            .expect("valid blend")
            .expect("blend enabled");

        assert_eq!(blend.prediction_set_id, "pred-quality-growth-v1");
        assert!((blend.factor_weight - 0.65).abs() < f64::EPSILON);
        assert!((blend.prediction_weight - 0.35).abs() < f64::EPSILON);
        assert_eq!(blend.prediction_min_percentile, None);
    }

    #[test]
    fn prediction_filter_can_enable_overlay_without_blend_weight() {
        let prediction_set_id = "pred-quality-growth-v1".to_string();

        let blend = build_prediction_blend_config(Some(&prediction_set_id), None, Some(0.2))
            .expect("valid prediction filter")
            .expect("filter enabled");

        assert_eq!(blend.prediction_set_id, prediction_set_id);
        assert_eq!(blend.factor_weight, 1.0);
        assert_eq!(blend.prediction_weight, 0.0);
        assert_eq!(blend.prediction_min_percentile, Some(0.2));
    }

    #[test]
    fn backtest_list_query_normalizes_page_size_bounds() {
        let query = BacktestListQuery {
            page: Some(0),
            page_size: Some(5000),
            ..BacktestListQuery::default()
        };

        assert_eq!(query.normalized_page(), 1);
        assert_eq!(query.normalized_page_size(), 200);
        assert_eq!(query.offset(), 0);
    }

    #[test]
    fn market_regime_request_builds_professional_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: None,
            benchmark: Some("000905.SH".to_string()),
            lookback_days: Some(126),
            min_observations: Some(20),
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000905.SH");
        assert_eq!(policy.lookback_days, 126);
        assert_eq!(policy.min_observations, 20);
        assert!(policy
            .rules
            .contains_key(&quant_backtest::signal_generator::MarketRegime::Bear));
    }

    #[test]
    fn market_regime_request_builds_drawdown_control_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("drawdown_control_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.bear_drawdown_threshold, 0.12);
        assert_eq!(policy.high_volatility_threshold, 0.24);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.35)
        );
    }

    #[test]
    fn market_regime_request_builds_drawdown_control_v2_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("drawdown_control_v2".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.bear_drawdown_threshold, 0.08);
        assert_eq!(policy.high_volatility_threshold, 0.20);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.25)
        );
    }

    #[test]
    fn market_regime_request_builds_quality_risk_off_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_risk_off_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.bear_drawdown_threshold, 0.12);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.80)
        );
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.score_direction),
            None
        );
    }

    #[test]
    fn market_regime_request_builds_quality_crash_guard_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_crash_guard_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.bear_drawdown_threshold, 0.25);
        assert_eq!(policy.high_volatility_threshold, 0.50);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.85)
        );
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.score_direction),
            None
        );
    }

    #[test]
    fn factor_request_builds_portfolio_drawdown_risk_control() {
        let req = RunFactorBacktestReq {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            strategy_version_id: "strategy-v1".to_string(),
            data_version_id: "data-v1".to_string(),
            research_dataset_id: None,
            feature_set_version_id: None,
            prediction_set_id: None,
            prediction_blend_weight: None,
            prediction_min_percentile: None,
            portfolio_policy_id: None,
            top_n: 20,
            rebalance: "20".to_string(),
            entry_delay: 0,
            min_amount: 0.0,
            max_position_pct: 0.1,
            skip_top_pct: 0.0,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            score_direction: "descending".to_string(),
            portfolio_method: "risk_budget".to_string(),
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
            industry_max_weight_pct: None,
            score_candidate_pool_size: None,
            universe_profile: None,
            cost_model: None,
            execution_rules: None,
            benchmark: Some("000300.SH".to_string()),
            market_regime: None,
            start_date: "20250101".to_string(),
            end_date: "20250131".to_string(),
            initial_capital: 1_000_000.0,
            mode: Some("standard".to_string()),
            portfolio_drawdown_reduce_start_pct: Some(0.05),
            portfolio_drawdown_reduce_full_pct: Some(0.15),
            portfolio_drawdown_min_exposure: Some(0.4),
            portfolio_drawdown_peak_lookback_days: Some(252),
            portfolio_drawdown_recovery_start_pct: Some(0.30),
            portfolio_drawdown_recovery_full_pct: Some(0.70),
            portfolio_drawdown_recovery_boost: Some(1.0),
            portfolio_volatility_target_pct: Some(0.16),
            portfolio_volatility_lookback_days: Some(60),
            portfolio_volatility_min_exposure: Some(0.45),
            portfolio_volatility_max_exposure: Some(1.0),
        };

        let risk_control = build_portfolio_risk_control(&req).expect("valid risk control");

        assert_eq!(
            risk_control.portfolio_drawdown_reduce_start_pct,
            Some(Decimal::new(5, 2))
        );
        assert_eq!(
            risk_control.portfolio_drawdown_reduce_full_pct,
            Some(Decimal::new(15, 2))
        );
        assert_eq!(
            risk_control.portfolio_drawdown_min_exposure,
            Some(Decimal::new(4, 1))
        );
        assert_eq!(
            risk_control.portfolio_drawdown_peak_lookback_days,
            Some(252)
        );
        assert_eq!(
            risk_control.portfolio_drawdown_recovery_start_pct,
            Some(Decimal::new(30, 2))
        );
        assert_eq!(
            risk_control.portfolio_drawdown_recovery_full_pct,
            Some(Decimal::new(70, 2))
        );
        assert_eq!(
            risk_control.portfolio_drawdown_recovery_boost,
            Some(Decimal::ONE)
        );
        assert_eq!(
            risk_control.portfolio_volatility_target_pct,
            Some(Decimal::new(16, 2))
        );
        assert_eq!(risk_control.portfolio_volatility_lookback_days, Some(60));
        assert_eq!(
            risk_control.portfolio_volatility_min_exposure,
            Some(Decimal::new(45, 2))
        );
        assert_eq!(
            risk_control.portfolio_volatility_max_exposure,
            Some(Decimal::ONE)
        );
    }
}
