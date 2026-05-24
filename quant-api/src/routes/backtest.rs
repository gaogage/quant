/// 回测路由
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use quant_backtest::engine::{
    BacktestConfig, BacktestMode, BacktestPersistenceMode, ExecutionCarryPolicy, ExecutionPrice,
    ExecutionScheduleProfile, ExecutionTiming, RiskControlConfig, StrategySignal,
};
use quant_backtest::portfolio::FeeConfig;
use quant_backtest::runner::{BacktestDataCache, BacktestMarketDataPrewarmReport, BacktestRunner};
use quant_backtest::signal_generator::{
    prewarm_market_feature_cache, CandidateRankingProfile, CandidateRiskFilterProfile,
    CapacityRiskBudgetProfile, CashUtilizationProfile, EventGateConfig, EventGateMode,
    ExecutionImpactBudgetProfile, MarketFeaturePrewarmReport, MarketRegime, MarketRegimePolicy,
    PortfolioConstructionMethod, PredictionBlendConfig, RiskContributionControlProfile,
    ScoreDirection, SignalDataCache, StyleRiskBudgetProfile, TradableUniverseProfile,
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
    pub mode: Option<String>,             // fast/standard/audit
    pub persistence_mode: Option<String>, // full/summary_only
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
    pub execution_schedule_profile: Option<String>,
    pub execution_carry_policy: Option<String>,
    pub execution_daily_target_move_limit_pct: Option<f64>,
    pub execution_max_carry_days: Option<usize>,
    pub max_participation_rate: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EffectiveCoverageReq {
    pub enabled: Option<bool>,
    pub mode: Option<String>,
    pub min_rows: Option<usize>,
    pub include_rebalance_warmup: Option<bool>,
    pub warmup_trading_days: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EffectiveCoverageRunSummary {
    pub requested_start_date: NaiveDate,
    pub effective_start_date: NaiveDate,
    pub adjusted: bool,
    pub mode: String,
    pub min_rows: usize,
    pub observed_rows: i64,
    pub coverage_start_date: NaiveDate,
    pub warmup_start_date: Option<NaiveDate>,
    pub warmup_trading_days: Option<usize>,
    pub combo_name: String,
    pub version: String,
    pub universe_profile: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveCoverageMode {
    AdjustStart,
    GuardOnly,
}

const DEFAULT_EFFECTIVE_COVERAGE_WARMUP_TRADING_DAYS: usize = 19;

impl EffectiveCoverageMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::AdjustStart => "adjust_start",
            Self::GuardOnly => "guard_only",
        }
    }
}

fn parse_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .map_err(|_| format!("{} must use YYYYMMDD format", field))
}

fn parse_effective_coverage_mode(value: Option<&str>) -> Result<EffectiveCoverageMode, String> {
    match value.unwrap_or("adjust_start").trim() {
        "" | "adjust_start" | "auto_adjust_start" => Ok(EffectiveCoverageMode::AdjustStart),
        "guard_only" | "strict" => Ok(EffectiveCoverageMode::GuardOnly),
        other => Err(format!("unsupported effective_coverage.mode: {}", other)),
    }
}

fn effective_coverage_enabled(value: &EffectiveCoverageReq) -> bool {
    value.enabled.unwrap_or(true)
}

async fn resolve_effective_factor_coverage(
    db: &sqlx::PgPool,
    req: &RunFactorBacktestReq,
    requested_start: NaiveDate,
    end: NaiveDate,
) -> Result<(NaiveDate, Option<EffectiveCoverageRunSummary>), String> {
    let Some(policy) = req.effective_coverage.as_ref() else {
        return Ok((requested_start, None));
    };
    if !effective_coverage_enabled(policy) {
        return Ok((requested_start, None));
    }

    let mode = parse_effective_coverage_mode(policy.mode.as_deref())?;
    let min_rows = policy.min_rows.unwrap_or(req.top_n.max(1)).max(1);
    let include_warmup = policy.include_rebalance_warmup.unwrap_or(true);
    let warmup_trading_days = include_warmup
        .then(|| {
            policy
                .warmup_trading_days
                .unwrap_or(DEFAULT_EFFECTIVE_COVERAGE_WARMUP_TRADING_DAYS)
        })
        .filter(|days| *days > 0);
    let universe_profile = parse_tradable_universe_profile(req.universe_profile.as_deref())?;
    let sql = effective_factor_coverage_sql(universe_profile);
    let row = sqlx::query_as::<_, (NaiveDate, i64)>(&sql)
        .bind(&req.combo_name)
        .bind(&req.version)
        .bind(requested_start)
        .bind(end)
        .bind(min_rows as i64)
        .fetch_optional(db)
        .await
        .map_err(|error| format!("Failed to resolve effective factor coverage: {}", error))?;

    let Some((coverage_start, observed_rows)) = row else {
        return Err(format!(
            "No effective factor coverage found for {}:{} between {} and {} with min_rows={}",
            req.combo_name, req.version, requested_start, end, min_rows
        ));
    };

    let warmup_start = if let Some(warmup_days) = warmup_trading_days {
        Some(resolve_warmup_start_date(db, requested_start, end, warmup_days).await?)
    } else {
        None
    };
    let first_eligible_start = warmup_start
        .map(|start| start.max(coverage_start))
        .unwrap_or(coverage_start);

    if mode == EffectiveCoverageMode::GuardOnly && first_eligible_start > requested_start {
        return Err(format!(
            "requested start_date {} is before effective factor coverage start {} for {}:{}",
            requested_start, first_eligible_start, req.combo_name, req.version
        ));
    }

    let effective_start = match mode {
        EffectiveCoverageMode::AdjustStart => first_eligible_start.max(requested_start),
        EffectiveCoverageMode::GuardOnly => requested_start,
    };

    Ok((
        effective_start,
        Some(EffectiveCoverageRunSummary {
            requested_start_date: requested_start,
            effective_start_date: effective_start,
            adjusted: effective_start != requested_start,
            mode: mode.as_str().to_string(),
            min_rows,
            observed_rows,
            coverage_start_date: coverage_start,
            warmup_start_date: warmup_start,
            warmup_trading_days,
            combo_name: req.combo_name.clone(),
            version: req.version.clone(),
            universe_profile: req.universe_profile.clone(),
        }),
    ))
}

fn effective_factor_coverage_sql(universe_profile: TradableUniverseProfile) -> String {
    let (join_sql, filter_sql) = match universe_profile {
        TradableUniverseProfile::All => ("", ""),
        TradableUniverseProfile::ListedNonSt => (
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol",
            "\n           AND ms.list_status = 'L' AND COALESCE(ms.is_st, false) = false",
        ),
        TradableUniverseProfile::MainBoardNonSt => (
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol",
            "\n           AND ms.list_status = 'L'
           AND COALESCE(ms.is_st, false) = false
           AND ms.exchange IN ('SSE', 'SZSE')
           AND COALESCE(ms.market, '') NOT ILIKE '%创业%'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'",
        ),
    };

    format!(
        "SELECT mfv.trade_date, COUNT(*)::bigint AS score_rows
         FROM multi_factor_value mfv{join_sql}
         WHERE mfv.combo_name = $1
           AND mfv.version = $2
           AND mfv.trade_date >= $3
           AND mfv.trade_date <= $4
           AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){filter_sql}
         GROUP BY mfv.trade_date
         HAVING COUNT(*) >= $5
         ORDER BY mfv.trade_date ASC
         LIMIT 1"
    )
}

async fn resolve_warmup_start_date(
    db: &sqlx::PgPool,
    requested_start: NaiveDate,
    end: NaiveDate,
    warmup_trading_days: usize,
) -> Result<NaiveDate, String> {
    if warmup_trading_days == 0 {
        return Ok(requested_start);
    }
    sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE exchange = 'SSE'
           AND is_open = true
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date ASC
         OFFSET $3
         LIMIT 1",
    )
    .bind(requested_start)
    .bind(end)
    .bind(warmup_trading_days as i64)
    .fetch_optional(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to resolve effective coverage warmup start: {}",
            error
        )
    })?
    .map(|row| row.0)
    .ok_or_else(|| {
        format!(
            "No open trading day found after {} warmup trading days between {} and {}",
            warmup_trading_days, requested_start, end
        )
    })
}

fn parse_mode(value: Option<&str>) -> BacktestMode {
    match value {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    }
}

fn parse_persistence_mode(value: Option<&str>) -> Result<BacktestPersistenceMode, String> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("full") | Some("standard") | Some("detail") | Some("detailed") => {
            Ok(BacktestPersistenceMode::Full)
        }
        Some("summary_only") | Some("summary-only") | Some("summary") | Some("discovery") => {
            Ok(BacktestPersistenceMode::SummaryOnly)
        }
        Some(other) => Err(format!("unsupported persistence_mode: {}", other)),
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

fn parse_execution_schedule_profile(
    value: Option<&str>,
) -> Result<ExecutionScheduleProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ExecutionScheduleProfile::parse)
        .unwrap_or(Ok(ExecutionScheduleProfile::Immediate))
}

fn parse_execution_carry_policy(value: Option<&str>) -> Result<ExecutionCarryPolicy, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ExecutionCarryPolicy::parse)
        .unwrap_or(Ok(ExecutionCarryPolicy::Expire))
}

fn parse_execution_daily_target_move_limit(
    rules: Option<&ExecutionRulesReq>,
) -> Result<Option<Decimal>, String> {
    rules
        .and_then(|rules| rules.execution_daily_target_move_limit_pct)
        .map(|value| decimal_from_f64(value, "execution_daily_target_move_limit_pct"))
        .transpose()
}

fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal, String> {
    Decimal::from_f64(value).ok_or_else(|| format!("{} must be a finite number", field))
}

fn positive_notional(value: f64) -> Option<f64> {
    if value.is_finite() && value > 0.0 {
        Some(value)
    } else {
        None
    }
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
    let execution_schedule_profile = parse_execution_schedule_profile(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_schedule_profile.as_deref()),
    )?;
    let execution_carry_policy = parse_execution_carry_policy(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_carry_policy.as_deref()),
    )?;
    let execution_daily_target_move_limit_pct =
        parse_execution_daily_target_move_limit(req.execution_rules.as_ref())?;
    let execution_max_carry_days = req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.execution_max_carry_days);
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
        execution_schedule_profile,
        execution_carry_policy,
        execution_daily_target_move_limit_pct,
        execution_max_carry_days,
        max_participation_rate,
        persistence_mode: parse_persistence_mode(req.persistence_mode.as_deref())?,
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
    pub event_gate_combo_name: Option<String>,
    #[serde(default = "default_combo_version")]
    pub event_gate_version: String,
    pub event_gate_mode: Option<String>,
    pub event_gate_min_score: Option<f64>,
    pub event_gate_boost_weight: Option<f64>,
    pub event_gate_score_direction: Option<String>,
    pub event_gate_active_regimes: Option<Vec<String>>,
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
    pub capacity_risk_budget: Option<String>,
    pub cash_utilization: Option<String>,
    pub execution_impact_budget: Option<String>,
    pub style_risk_budget: Option<String>,
    pub candidate_risk_filter: Option<String>,
    pub candidate_ranking: Option<String>,
    pub risk_contribution_control: Option<String>,
    #[serde(default)]
    pub rebalance_hysteresis_pct: Option<f64>,
    #[serde(default)]
    pub partial_rebalance_ratio: Option<f64>,
    #[serde(default)]
    pub score_candidate_pool_size: Option<usize>,
    pub universe_profile: Option<String>,
    pub effective_coverage: Option<EffectiveCoverageReq>,
    pub cost_model: Option<CostModelReq>,
    pub execution_rules: Option<ExecutionRulesReq>,
    pub benchmark: Option<String>,
    pub market_regime: Option<MarketRegimeBacktestReq>,
    pub stop_loss_pct: Option<f64>,
    pub take_profit_pct: Option<f64>,
    pub trailing_stop_pct: Option<f64>,
    pub time_stop_days: Option<u32>,
    pub reentry_cooldown_days: Option<u32>,
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
    pub portfolio_sharpe_reduce_start: Option<f64>,
    pub portfolio_sharpe_reduce_full: Option<f64>,
    pub portfolio_sharpe_lookback_days: Option<usize>,
    pub portfolio_sharpe_min_exposure: Option<f64>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default = "default_capital")]
    pub initial_capital: f64,
    pub mode: Option<String>,
    pub persistence_mode: Option<String>,
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
    pub capacity_risk_budget: Option<String>,
    pub cash_utilization: Option<String>,
    pub execution_impact_budget: Option<String>,
    pub style_risk_budget: Option<String>,
    pub candidate_risk_filter: Option<String>,
    pub candidate_ranking: Option<String>,
    pub risk_contribution_control: Option<String>,
    #[serde(default)]
    pub rebalance_hysteresis_pct: Option<f64>,
    #[serde(default)]
    pub partial_rebalance_ratio: Option<f64>,
    pub cost_model: Option<CostModelReq>,
    pub execution_rules: Option<ExecutionRulesReq>,
    pub benchmark: Option<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default = "default_capital")]
    pub initial_capital: f64,
    pub mode: Option<String>,
    pub persistence_mode: Option<String>,
}

pub(crate) struct FactorBacktestRunOutput {
    pub signals_count: usize,
    pub metrics: quant_backtest::metrics::BacktestMetrics,
    pub trades: usize,
    pub equity_points: usize,
    pub effective_coverage: Option<EffectiveCoverageRunSummary>,
    pub market_data_prewarm_report: Option<BacktestMarketDataPrewarmReport>,
    pub market_feature_prewarm_report: Option<MarketFeaturePrewarmReport>,
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
        "min_variance" | "min-variance" | "minimum_variance" | "minimum-variance" => {
            Ok(PortfolioConstructionMethod::MinVariance)
        }
        other => Err(format!("unsupported portfolio_method: {}", other)),
    }
}

fn parse_tradable_universe_profile(value: Option<&str>) -> Result<TradableUniverseProfile, String> {
    value
        .map(TradableUniverseProfile::parse)
        .unwrap_or(Ok(TradableUniverseProfile::All))
}

fn parse_style_risk_budget_profile(value: Option<&str>) -> Result<StyleRiskBudgetProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(StyleRiskBudgetProfile::parse)
        .unwrap_or(Ok(StyleRiskBudgetProfile::Off))
}

fn parse_capacity_risk_budget_profile(
    value: Option<&str>,
) -> Result<CapacityRiskBudgetProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CapacityRiskBudgetProfile::parse)
        .unwrap_or(Ok(CapacityRiskBudgetProfile::Off))
}

fn parse_cash_utilization_profile(value: Option<&str>) -> Result<CashUtilizationProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CashUtilizationProfile::parse)
        .unwrap_or(Ok(CashUtilizationProfile::Off))
}

fn parse_execution_impact_budget_profile(
    value: Option<&str>,
) -> Result<ExecutionImpactBudgetProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ExecutionImpactBudgetProfile::parse)
        .unwrap_or(Ok(ExecutionImpactBudgetProfile::Off))
}

fn parse_candidate_risk_filter_profile(
    value: Option<&str>,
) -> Result<CandidateRiskFilterProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CandidateRiskFilterProfile::parse)
        .unwrap_or(Ok(CandidateRiskFilterProfile::Off))
}

fn parse_candidate_ranking_profile(value: Option<&str>) -> Result<CandidateRankingProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CandidateRankingProfile::parse)
        .unwrap_or(Ok(CandidateRankingProfile::Off))
}

fn parse_risk_contribution_control_profile(
    value: Option<&str>,
) -> Result<RiskContributionControlProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(RiskContributionControlProfile::parse)
        .unwrap_or(Ok(RiskContributionControlProfile::Off))
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

fn build_event_gate_config(req: &RunFactorBacktestReq) -> Result<Option<EventGateConfig>, String> {
    let mode = req
        .event_gate_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let combo_name = req
        .event_gate_combo_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let boost_weight = req.event_gate_boost_weight.unwrap_or(0.0);
    let min_score = req.event_gate_min_score.unwrap_or(0.0);
    if mode.is_none() && combo_name.is_none() && boost_weight <= f64::EPSILON {
        return Ok(None);
    }
    let combo_name = combo_name.ok_or_else(|| {
        "event_gate_combo_name is required when event gate is enabled".to_string()
    })?;
    let mode = match mode.unwrap_or("boost_positive") {
        "boost_positive" | "boost-positive" => EventGateMode::BoostPositive,
        "exclude_negative" | "exclude-negative" => EventGateMode::ExcludeNegative,
        "require_positive" | "require-positive" => EventGateMode::RequirePositive,
        other => return Err(format!("unsupported event_gate_mode: {}", other)),
    };
    if !min_score.is_finite() {
        return Err("event_gate_min_score must be a finite number".into());
    }
    if !boost_weight.is_finite() || boost_weight < 0.0 || boost_weight > 1.0 {
        return Err("event_gate_boost_weight must be between 0 and 1".into());
    }
    let score_direction = req
        .event_gate_score_direction
        .as_deref()
        .map(parse_score_direction)
        .transpose()?
        .unwrap_or(ScoreDirection::Descending);
    let active_regimes = parse_event_gate_active_regimes(req.event_gate_active_regimes.as_deref())?;
    Ok(Some(EventGateConfig {
        combo_name: combo_name.to_string(),
        version: req.event_gate_version.clone(),
        mode,
        score_direction,
        min_score,
        boost_weight,
        active_regimes,
    }))
}

fn parse_event_gate_active_regimes(values: Option<&[String]>) -> Result<Vec<MarketRegime>, String> {
    let Some(values) = values else {
        return Ok(Vec::new());
    };
    values
        .iter()
        .map(|value| parse_market_regime_name(value))
        .collect()
}

fn parse_market_regime_name(value: &str) -> Result<MarketRegime, String> {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "bull" => Ok(MarketRegime::Bull),
        "bear" => Ok(MarketRegime::Bear),
        "high_volatility" | "high_vol" => Ok(MarketRegime::HighVolatility),
        "sideways" => Ok(MarketRegime::Sideways),
        "mixed" => Ok(MarketRegime::Mixed),
        other => Err(format!("unsupported event_gate_active_regime: {}", other)),
    }
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
    let sharpe_start = match req.portfolio_sharpe_reduce_start {
        Some(value) => Some(decimal_from_f64(value, "portfolio_sharpe_reduce_start")?),
        None => None,
    };
    let sharpe_full = match req.portfolio_sharpe_reduce_full {
        Some(value) => Some(decimal_from_f64(value, "portfolio_sharpe_reduce_full")?),
        None => None,
    };
    let sharpe_min_exposure = optional_decimal_pct(
        req.portfolio_sharpe_min_exposure,
        "portfolio_sharpe_min_exposure",
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
    let sharpe_lookback_days = match req.portfolio_sharpe_lookback_days {
        Some(days) if days < 2 => {
            return Err("portfolio_sharpe_lookback_days must be at least 2".into());
        }
        Some(days) => Some(days),
        None => None,
    };
    let sharpe_provided = sharpe_start.is_some()
        || sharpe_full.is_some()
        || sharpe_lookback_days.is_some()
        || sharpe_min_exposure.is_some();
    let sharpe_complete =
        sharpe_start.is_some() && sharpe_full.is_some() && sharpe_min_exposure.is_some();
    if sharpe_provided && !sharpe_complete {
        return Err(
            "portfolio sharpe risk control requires reduce_start, reduce_full, and min_exposure"
                .into(),
        );
    }
    if let (Some(start), Some(full)) = (sharpe_start, sharpe_full) {
        if full >= start {
            return Err(
                "portfolio_sharpe_reduce_full must be less than portfolio_sharpe_reduce_start"
                    .into(),
            );
        }
    }

    Ok(RiskControlConfig {
        stop_loss_pct: optional_decimal_pct(req.stop_loss_pct, "stop_loss_pct")?,
        take_profit_pct: optional_decimal_pct(req.take_profit_pct, "take_profit_pct")?,
        trailing_stop_pct: optional_decimal_pct(req.trailing_stop_pct, "trailing_stop_pct")?,
        time_stop_days: req.time_stop_days.filter(|days| *days > 0),
        reentry_cooldown_days: req.reentry_cooldown_days.filter(|days| *days > 0),
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
        portfolio_sharpe_reduce_start: sharpe_start,
        portfolio_sharpe_reduce_full: sharpe_full,
        portfolio_sharpe_lookback_days: sharpe_lookback_days,
        portfolio_sharpe_min_exposure: sharpe_min_exposure,
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
        "quality_crash_guard_v2" => MarketRegimePolicy::quality_crash_guard_v2(benchmark),
        "quality_crash_guard_v3" => MarketRegimePolicy::quality_crash_guard_v3(benchmark),
        "quality_bear_window_guard_v1" => {
            MarketRegimePolicy::quality_bear_window_guard_v1(benchmark)
        }
        "quality_bear_window_guard_v2" => {
            MarketRegimePolicy::quality_bear_window_guard_v2(benchmark)
        }
        "quality_regime_alpha_switch_v1" => {
            MarketRegimePolicy::quality_regime_alpha_switch_v1(benchmark)
        }
        "quality_regime_alpha_switch_value_v1" => {
            MarketRegimePolicy::quality_regime_alpha_switch_value_v1(benchmark)
        }
        "quality_regime_alpha_switch_recovery_v1" => {
            MarketRegimePolicy::quality_regime_alpha_switch_recovery_v1(benchmark)
        }
        "quality_regime_alpha_switch_blend_v1" => {
            MarketRegimePolicy::quality_regime_alpha_switch_blend_v1(benchmark)
        }
        "quality_regime_alpha_overlay_value_05pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_overlay_value_05pct_v1(benchmark)
        }
        "quality_regime_alpha_overlay_value_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_overlay_value_10pct_v1(benchmark)
        }
        "quality_regime_alpha_overlay_blend_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_overlay_blend_10pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_value_10pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_value_15pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_blend_10pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(
                benchmark,
            )
        }
        "quality_all_regime_event_window_sleeve_05pct_v1" => {
            MarketRegimePolicy::quality_all_regime_event_window_sleeve_05pct_v1(benchmark)
        }
        "quality_all_regime_event_window_sleeve_10pct_v1" => {
            MarketRegimePolicy::quality_all_regime_event_window_sleeve_10pct_v1(benchmark)
        }
        "quality_all_regime_event_window_sleeve_15pct_v1" => {
            MarketRegimePolicy::quality_all_regime_event_window_sleeve_15pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1(
                benchmark,
            )
        }
        "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1(benchmark)
        }
        "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1" => {
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1(benchmark)
        }
        "quality_bear_position_guard_v1" => {
            MarketRegimePolicy::quality_bear_position_guard_v1(benchmark)
        }
        "quality_bear_position_guard_v2" => {
            MarketRegimePolicy::quality_bear_position_guard_v2(benchmark)
        }
        "quality_bear_position_guard_v3" => {
            MarketRegimePolicy::quality_bear_position_guard_v3(benchmark)
        }
        "quality_event_window_position_guard_v1" => {
            MarketRegimePolicy::quality_event_window_position_guard_v1(benchmark)
        }
        "quality_event_window_position_guard_v2" => {
            MarketRegimePolicy::quality_event_window_position_guard_v2(benchmark)
        }
        "quality_event_window_position_guard_v3" => {
            MarketRegimePolicy::quality_event_window_position_guard_v3(benchmark)
        }
        "quality_event_window_return_sharpe_router_v1" => {
            MarketRegimePolicy::quality_event_window_return_sharpe_router_v1(benchmark)
        }
        "quality_event_window_return_sharpe_router_v2" => {
            MarketRegimePolicy::quality_event_window_return_sharpe_router_v2(benchmark)
        }
        "quality_event_window_return_sharpe_router_v3" => {
            MarketRegimePolicy::quality_event_window_return_sharpe_router_v3(benchmark)
        }
        "quality_event_window_return_sharpe_router_v4" => {
            MarketRegimePolicy::quality_event_window_return_sharpe_router_v4(benchmark)
        }
        "quality_state_alpha_selector_v1" => {
            MarketRegimePolicy::quality_state_alpha_selector_v1(benchmark)
        }
        "quality_state_alpha_selector_v2" => {
            MarketRegimePolicy::quality_state_alpha_selector_v2(benchmark)
        }
        "quality_state_alpha_selector_v3" => {
            MarketRegimePolicy::quality_state_alpha_selector_v3(benchmark)
        }
        "quality_state_alpha_overlay_selector_v1" => {
            MarketRegimePolicy::quality_state_alpha_overlay_selector_v1(benchmark)
        }
        "quality_state_alpha_overlay_selector_v2" => {
            MarketRegimePolicy::quality_state_alpha_overlay_selector_v2(benchmark)
        }
        "quality_state_alpha_overlay_selector_v3" => {
            MarketRegimePolicy::quality_state_alpha_overlay_selector_v3(benchmark)
        }
        "quality_state_sharpe_bridge_router_v1" => {
            MarketRegimePolicy::quality_state_sharpe_bridge_router_v1(benchmark)
        }
        "quality_state_sharpe_bridge_router_v2" => {
            MarketRegimePolicy::quality_state_sharpe_bridge_router_v2(benchmark)
        }
        "quality_state_sharpe_bridge_router_v3" => {
            MarketRegimePolicy::quality_state_sharpe_bridge_router_v3(benchmark)
        }
        "quality_frontier_regime_bridge_router_v1" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v1(benchmark)
        }
        "quality_frontier_regime_bridge_router_v2" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v2(benchmark)
        }
        "quality_frontier_regime_bridge_router_v3" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v3(benchmark)
        }
        "quality_frontier_regime_bridge_router_v4" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v4(benchmark)
        }
        "quality_frontier_regime_bridge_router_v5" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v5(benchmark)
        }
        "quality_frontier_regime_bridge_router_v6" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v6(benchmark)
        }
        "quality_frontier_regime_bridge_router_v7" => {
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v7(benchmark)
        }
        "quality_mixed_event_state_selector_v1" => {
            MarketRegimePolicy::quality_mixed_event_state_selector_v1(benchmark)
        }
        "quality_mixed_event_state_selector_v2" => {
            MarketRegimePolicy::quality_mixed_event_state_selector_v2(benchmark)
        }
        "quality_mixed_event_state_overlay_selector_v1" => {
            MarketRegimePolicy::quality_mixed_event_state_overlay_selector_v1(benchmark)
        }
        "quality_mixed_event_state_overlay_selector_v2" => {
            MarketRegimePolicy::quality_mixed_event_state_overlay_selector_v2(benchmark)
        }
        "quality_mixed_orthogonal_alpha_selector_v1" => {
            MarketRegimePolicy::quality_mixed_orthogonal_alpha_selector_v1(benchmark)
        }
        "quality_mixed_orthogonal_alpha_selector_v2" => {
            MarketRegimePolicy::quality_mixed_orthogonal_alpha_selector_v2(benchmark)
        }
        "quality_mixed_orthogonal_alpha_selector_v3" => {
            MarketRegimePolicy::quality_mixed_orthogonal_alpha_selector_v3(benchmark)
        }
        "quality_mixed_orthogonal_risk_memory_router_v1" => {
            MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v1(benchmark)
        }
        "quality_mixed_orthogonal_risk_memory_router_v2" => {
            MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v2(benchmark)
        }
        "quality_mixed_orthogonal_risk_memory_router_v3" => {
            MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v3(benchmark)
        }
        "quality_nonlinear_alpha_router_v1" => {
            MarketRegimePolicy::quality_nonlinear_alpha_router_v1(benchmark)
        }
        "quality_nonlinear_alpha_router_v2" => {
            MarketRegimePolicy::quality_nonlinear_alpha_router_v2(benchmark)
        }
        "quality_nonlinear_alpha_risk_memory_router_v1" => {
            MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v1(benchmark)
        }
        "quality_nonlinear_alpha_risk_memory_router_v2" => {
            MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v2(benchmark)
        }
        "quality_nonlinear_alpha_risk_memory_router_v3" => {
            MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v3(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v1" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v1(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v2" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v2(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v3" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v3(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v4" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v4(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v5" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v5(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v6" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v6(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v7" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v7(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v8" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v8(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v9" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v9(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v10" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v10(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v11" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v11(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v12" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v12(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v13" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v13(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v14" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v14(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v15" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v15(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v16" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v16(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v17" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v17(benchmark)
        }
        "quality_mixed_state_risk_memory_router_v18" => {
            MarketRegimePolicy::quality_mixed_state_risk_memory_router_v18(benchmark)
        }
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
                    "sortino_ratio": output.metrics.sortino_ratio,
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
                "effective_coverage": output.effective_coverage,
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
                    "sortino_ratio": output.metrics.sortino_ratio,
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
                "effective_coverage": output.effective_coverage,
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
    mut signal_cache: Option<&mut SignalDataCache>,
    mut backtest_cache: Option<&mut BacktestDataCache>,
) -> Result<FactorBacktestRunOutput, String> {
    let start = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end < start {
        return Err("end_date must be greater than or equal to start_date".into());
    }
    let reb_freq = match req.rebalance.as_str() {
        "daily" => 1,
        "weekly" => 5,
        "monthly" => 20,
        s => s.parse::<usize>().unwrap_or(20),
    };
    let (effective_start, effective_coverage) =
        resolve_effective_factor_coverage(db, &req, start, end).await?;
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
    let execution_schedule_profile = parse_execution_schedule_profile(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_schedule_profile.as_deref()),
    )?;
    let execution_carry_policy = parse_execution_carry_policy(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_carry_policy.as_deref()),
    )?;
    let execution_daily_target_move_limit_pct =
        parse_execution_daily_target_move_limit(req.execution_rules.as_ref())?;
    let execution_max_carry_days = req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.execution_max_carry_days);
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

    let score_direction = parse_score_direction(&req.score_direction)?;
    let portfolio_method = parse_portfolio_method(&req.portfolio_method)?;
    let universe_profile = parse_tradable_universe_profile(req.universe_profile.as_deref())?;
    let industry_max_weight_pct =
        optional_unit_f64(req.industry_max_weight_pct, "industry_max_weight_pct")?;
    let capacity_risk_budget_profile =
        parse_capacity_risk_budget_profile(req.capacity_risk_budget.as_deref())?;
    let cash_utilization_profile = parse_cash_utilization_profile(req.cash_utilization.as_deref())?;
    let execution_impact_budget_profile =
        parse_execution_impact_budget_profile(req.execution_impact_budget.as_deref())?;
    let style_risk_budget_profile =
        parse_style_risk_budget_profile(req.style_risk_budget.as_deref())?;
    let candidate_risk_filter_profile =
        parse_candidate_risk_filter_profile(req.candidate_risk_filter.as_deref())?;
    let candidate_ranking_profile =
        parse_candidate_ranking_profile(req.candidate_ranking.as_deref())?;
    let risk_contribution_control_profile =
        parse_risk_contribution_control_profile(req.risk_contribution_control.as_deref())?;

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
        portfolio_notional_cny: positive_notional(req.initial_capital),
        max_participation_rate: max_participation_rate.and_then(|value| value.to_f64()),
        capacity_risk_budget_profile,
        cash_utilization_profile,
        execution_impact_budget_profile,
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
        style_risk_budget_profile,
        candidate_risk_filter_profile,
        candidate_ranking_profile,
        risk_contribution_control_profile,
        rebalance_hysteresis_pct: req.rebalance_hysteresis_pct.unwrap_or(0.0),
        partial_rebalance_ratio: req.partial_rebalance_ratio.unwrap_or(1.0),
        score_candidate_pool_size: req.score_candidate_pool_size.filter(|size| *size > 0),
        universe_profile,
        prediction_blend: build_prediction_blend_config(
            req.prediction_set_id.as_ref(),
            req.prediction_blend_weight,
            req.prediction_min_percentile,
        )?,
        event_gate: build_event_gate_config(&req)?,
        score_overlay: None,
        portfolio_sleeve: None,
    };

    info!(
        task_id,
        combo=%req.combo_name,
        top_n=req.top_n,
        reb=reb_freq,
        start=%effective_start,
        "Generating factor signals"
    );

    let regime_policy = build_market_regime_policy(req.market_regime.as_ref(), &benchmark)?;
    let signals = match (regime_policy.as_ref(), signal_cache.as_deref_mut()) {
        (Some(policy), Some(cache)) => {
            quant_backtest::signal_generator::generate_regime_signals_with_cache(
                db,
                &sig_config,
                policy,
                effective_start,
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
                effective_start,
                end,
            )
            .await
        }
        (None, Some(cache)) => {
            quant_backtest::signal_generator::generate_signals_with_cache(
                db,
                &sig_config,
                effective_start,
                end,
                cache,
            )
            .await
        }
        (None, None) => {
            quant_backtest::signal_generator::generate_signals(
                db,
                &sig_config,
                effective_start,
                end,
            )
            .await
        }
    }?;

    info!(
        task_id,
        signals = signals.len(),
        "Signals generated, running backtest"
    );

    let all_symbols: Vec<String> = signals
        .values()
        .flat_map(|signal| signal.target_weights.keys().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let market_feature_prewarm_report = if let Some(cache) = signal_cache.as_deref_mut() {
        let lookback_days = req
            .correlation_lookback_days
            .max(req.kelly_lookback_days)
            .max(req.risk_budget_lookback_days)
            .max(1);
        Some(
            prewarm_market_feature_cache(
                db,
                cache,
                &req.data_version_id,
                effective_start,
                end,
                effective_start,
                end,
                effective_start,
                end,
                lookback_days,
                &all_symbols,
            )
            .await
            .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    let runner = BacktestRunner::new(db.clone());
    let market_data_prewarm_report = if let Some(cache) = backtest_cache.as_deref_mut() {
        Some(
            runner
                .prewarm_market_data_cache(cache, &benchmark, &all_symbols, effective_start, end)
                .await
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };

    let mode = match req.mode.as_deref() {
        Some("fast") => BacktestMode::Fast,
        Some("audit") => BacktestMode::Audit,
        _ => BacktestMode::Standard,
    };
    let persistence_mode = parse_persistence_mode(req.persistence_mode.as_deref())?;

    let config = BacktestConfig {
        initial_capital: capital,
        benchmark,
        start_date: effective_start,
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
        symbols: all_symbols,
        rebalance_frequency: req.rebalance.clone(),
        execution_timing,
        execution_price,
        execution_schedule_profile,
        execution_carry_policy,
        execution_daily_target_move_limit_pct,
        execution_max_carry_days,
        max_participation_rate,
        persistence_mode,
        risk_control: build_portfolio_risk_control(&req)?,
    };

    match runner
        .run_with_cache(task_id, config, &signals, backtest_cache)
        .await
    {
        Ok(output) => Ok(FactorBacktestRunOutput {
            signals_count: signals.len(),
            trades: output.trades.len(),
            equity_points: output.equity_curve.len(),
            metrics: output.metrics,
            effective_coverage,
            market_data_prewarm_report,
            market_feature_prewarm_report,
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
    let execution_schedule_profile = parse_execution_schedule_profile(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_schedule_profile.as_deref()),
    )?;
    let execution_carry_policy = parse_execution_carry_policy(
        req.execution_rules
            .as_ref()
            .and_then(|rules| rules.execution_carry_policy.as_deref()),
    )?;
    let execution_daily_target_move_limit_pct =
        parse_execution_daily_target_move_limit(req.execution_rules.as_ref())?;
    let execution_max_carry_days = req
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.execution_max_carry_days);
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
    let capacity_risk_budget_profile =
        parse_capacity_risk_budget_profile(req.capacity_risk_budget.as_deref())?;
    let cash_utilization_profile = parse_cash_utilization_profile(req.cash_utilization.as_deref())?;
    let execution_impact_budget_profile =
        parse_execution_impact_budget_profile(req.execution_impact_budget.as_deref())?;
    let style_risk_budget_profile =
        parse_style_risk_budget_profile(req.style_risk_budget.as_deref())?;
    let candidate_risk_filter_profile =
        parse_candidate_risk_filter_profile(req.candidate_risk_filter.as_deref())?;
    let candidate_ranking_profile =
        parse_candidate_ranking_profile(req.candidate_ranking.as_deref())?;
    let risk_contribution_control_profile =
        parse_risk_contribution_control_profile(req.risk_contribution_control.as_deref())?;

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
        portfolio_notional_cny: positive_notional(req.initial_capital),
        max_participation_rate: max_participation_rate.and_then(|value| value.to_f64()),
        capacity_risk_budget_profile,
        cash_utilization_profile,
        execution_impact_budget_profile,
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
        style_risk_budget_profile,
        candidate_risk_filter_profile,
        candidate_ranking_profile,
        risk_contribution_control_profile,
        rebalance_hysteresis_pct: req.rebalance_hysteresis_pct.unwrap_or(0.0),
        partial_rebalance_ratio: req.partial_rebalance_ratio.unwrap_or(1.0),
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
    let persistence_mode = parse_persistence_mode(req.persistence_mode.as_deref())?;

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
        execution_schedule_profile,
        execution_carry_policy,
        execution_daily_target_move_limit_pct,
        execution_max_carry_days,
        max_participation_rate,
        persistence_mode,
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
            effective_coverage: None,
            market_data_prewarm_report: None,
            market_feature_prewarm_report: None,
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
            persistence_mode: None,
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
            persistence_mode: Some("summary_only".into()),
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
                execution_schedule_profile: None,
                execution_carry_policy: None,
                execution_daily_target_move_limit_pct: None,
                execution_max_carry_days: None,
                max_participation_rate: Some(0.10),
            }),
        };

        let config = build_backtest_config(&req).unwrap();

        assert_eq!(
            config.persistence_mode,
            BacktestPersistenceMode::SummaryOnly
        );
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
    fn event_gate_request_builds_conditional_signal_config() {
        let req = factor_risk_control_request_template(
            Some("phase7_event_window_earnings_v1"),
            Some("exclude_negative"),
            Some(0.0),
            Some(0.0),
            None,
        );

        let gate = build_event_gate_config(&req)
            .expect("valid event gate")
            .expect("event gate enabled");

        assert_eq!(gate.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(gate.mode, EventGateMode::ExcludeNegative);
        assert_eq!(gate.score_direction, ScoreDirection::Descending);
    }

    #[test]
    fn event_gate_request_accepts_active_regime_scope() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "event_gate_combo_name": "phase7_valuation_v1",
            "event_gate_mode": "exclude_negative",
            "event_gate_min_score": 0.35,
            "event_gate_active_regimes": ["bear", "high_volatility"]
        }))
        .expect("factor request");

        let gate = build_event_gate_config(&req)
            .expect("valid event gate")
            .expect("event gate enabled");

        assert_eq!(
            gate.active_regimes,
            vec![MarketRegime::Bear, MarketRegime::HighVolatility]
        );
    }

    #[test]
    fn run_factor_backtest_request_accepts_rebalance_smoothing() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "rebalance_hysteresis_pct": 0.01,
            "partial_rebalance_ratio": 0.50
        }))
        .expect("factor request");

        assert_eq!(req.rebalance_hysteresis_pct, Some(0.01));
        assert_eq!(req.partial_rebalance_ratio, Some(0.50));
        assert!(req.effective_coverage.is_none());
    }

    #[test]
    fn run_factor_backtest_request_accepts_explicit_effective_coverage() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20160201",
            "end_date": "20260515",
            "effective_coverage": {
                "enabled": true,
                "mode": "adjust_start",
                "min_rows": 80
            }
        }))
        .expect("factor request");

        let coverage = req.effective_coverage.expect("effective coverage");
        assert_eq!(coverage.enabled, Some(true));
        assert_eq!(coverage.mode.as_deref(), Some("adjust_start"));
        assert_eq!(coverage.min_rows, Some(80));
    }

    #[test]
    fn run_factor_backtest_request_accepts_candidate_risk_filter() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "candidate_risk_filter": "low_volatility_low_correlation_v1"
        }))
        .expect("factor request");

        let profile = parse_candidate_risk_filter_profile(req.candidate_risk_filter.as_deref())
            .expect("candidate risk filter");

        assert_eq!(
            profile,
            CandidateRiskFilterProfile::LowVolatilityLowCorrelationV1
        );
    }

    #[test]
    fn run_factor_backtest_request_accepts_risk_contribution_control() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "risk_contribution_control": "soft_single_name_20pct_v1"
        }))
        .expect("factor request");

        let profile =
            parse_risk_contribution_control_profile(req.risk_contribution_control.as_deref())
                .expect("risk contribution control");

        assert_eq!(
            profile,
            RiskContributionControlProfile::SoftSingleName20PctV1
        );
    }

    #[test]
    fn run_factor_backtest_request_accepts_capacity_risk_budget() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "capacity_risk_budget": "capacity_participation_strict_v1"
        }))
        .expect("factor request");

        let profile = parse_capacity_risk_budget_profile(req.capacity_risk_budget.as_deref())
            .expect("capacity risk budget");

        assert_eq!(profile, CapacityRiskBudgetProfile::ParticipationStrictV1);
    }

    #[test]
    fn run_factor_backtest_request_accepts_cash_utilization_profile() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "cash_utilization": "fillable_gross_90_v1"
        }))
        .expect("factor request");

        let profile = parse_cash_utilization_profile(req.cash_utilization.as_deref())
            .expect("cash utilization");

        assert_eq!(profile, CashUtilizationProfile::FillableGross90V1);
    }

    #[test]
    fn run_factor_backtest_request_accepts_execution_impact_budget() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "execution_impact_budget": "impact_turnover_20pct_v1"
        }))
        .expect("factor request");

        let profile = parse_execution_impact_budget_profile(req.execution_impact_budget.as_deref())
            .expect("execution impact budget");

        assert_eq!(profile, ExecutionImpactBudgetProfile::Turnover20PctV1);
    }

    #[test]
    fn run_factor_backtest_request_accepts_execution_schedule_profile() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "execution_rules": {
                "execution_schedule_profile": "twap_15d_v1",
                "execution_carry_policy": "roll_forward_v1",
                "execution_daily_target_move_limit_pct": 0.05,
                "execution_max_carry_days": 20
            }
        }))
        .expect("factor request");

        let profile = parse_execution_schedule_profile(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_schedule_profile.as_deref()),
        )
        .expect("execution schedule profile");

        assert_eq!(profile, ExecutionScheduleProfile::Twap15dV1);
        let carry_policy = parse_execution_carry_policy(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_carry_policy.as_deref()),
        )
        .expect("execution carry policy");
        assert_eq!(carry_policy, ExecutionCarryPolicy::RollForwardV1);
        let rules = req.execution_rules.as_ref().expect("execution rules");
        assert_eq!(rules.execution_daily_target_move_limit_pct, Some(0.05));
        assert_eq!(rules.execution_max_carry_days, Some(20));
    }

    #[test]
    fn run_factor_backtest_request_accepts_min_variance_portfolio_method() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "portfolio_method": "min_variance"
        }))
        .expect("factor request");

        let method = parse_portfolio_method(&req.portfolio_method).expect("portfolio method");

        assert_eq!(method, PortfolioConstructionMethod::MinVariance);
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
    fn market_regime_request_builds_quality_crash_guard_v2_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_crash_guard_v2".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.bear_drawdown_threshold, 0.22);
        assert_eq!(policy.high_volatility_threshold, 0.45);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.75)
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
    fn market_regime_request_builds_quality_crash_guard_v3_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_crash_guard_v3".to_string()),
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
            Some(0.80)
        );
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::HighVolatility)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.68)
        );
    }

    #[test]
    fn market_regime_request_builds_quality_bear_window_guard_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_bear_window_guard_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.lookback_days, 126);
        assert_eq!(policy.bear_drawdown_threshold, 0.16);
        assert_eq!(policy.high_volatility_threshold, 0.32);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.78)
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
    fn market_regime_request_builds_regime_alpha_switch_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_regime_alpha_switch_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.lookback_days, 126);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.combo_name.as_deref()),
            Some("phase7_industry_residual_quality_v1")
        );
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.score_direction),
            Some(quant_backtest::signal_generator::ScoreDirection::Descending)
        );
    }

    #[test]
    fn market_regime_request_builds_low_risk_portfolio_sleeve_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        let sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("bear low-risk sleeve");
        assert_eq!(sleeve.combo_name, "phase7_price_volume_expanded_v1");
        assert_eq!(
            sleeve.score_direction,
            quant_backtest::signal_generator::ScoreDirection::Ascending
        );
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn market_regime_request_builds_event_portfolio_sleeve_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        let sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("bear event sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(
            sleeve.score_direction,
            quant_backtest::signal_generator::ScoreDirection::Descending
        );
        assert!((sleeve.weight - 0.10).abs() < 1e-9);
    }

    #[test]
    fn market_regime_request_builds_fractional_event_portfolio_sleeve_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some(
                "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            ),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        let sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("bear event sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert!((sleeve.weight - 0.125).abs() < 1e-9);
    }

    #[test]
    fn market_regime_request_builds_upper_bound_event_portfolio_sleeve_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        let sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("bear event sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn market_regime_request_builds_event_window_regime_placement_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some(
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1"
                    .to_string(),
            ),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert!(policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .is_none());
        let sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::HighVolatility)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("high-vol event sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn market_regime_request_builds_bull_sleeve_alpha_selector_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_state_alpha_selector_v2".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        let bull_sleeve = policy
            .rules
            .get(&quant_backtest::signal_generator::MarketRegime::Bull)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("bull value/recovery sleeve");
        assert_eq!(
            bull_sleeve.combo_name,
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert!((bull_sleeve.weight - 0.10).abs() < 1e-9);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.max_gross_exposure),
            Some(0.75)
        );
    }

    #[test]
    fn market_regime_request_builds_event_quality_segment_policies() {
        for (policy_name, combo_name) in [
            (
                "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1",
                "phase7_event_surprise_v1",
            ),
            (
                "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1",
                "phase7_event_earnings_v1",
            ),
        ] {
            let req = MarketRegimeBacktestReq {
                enabled: Some(true),
                policy: Some(policy_name.to_string()),
                benchmark: None,
                lookback_days: None,
                min_observations: None,
            };

            let policy = build_market_regime_policy(Some(&req), "000300.SH")
                .expect("valid regime policy")
                .expect("enabled policy");

            let sleeve = policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.portfolio_sleeve.as_ref())
                .expect("bear event quality sleeve");
            assert_eq!(sleeve.combo_name, combo_name);
            assert!((sleeve.weight - 0.15).abs() < 1e-9);
        }
    }

    #[test]
    fn market_regime_request_builds_quality_bear_position_guard_policy() {
        let req = MarketRegimeBacktestReq {
            enabled: Some(true),
            policy: Some("quality_bear_position_guard_v1".to_string()),
            benchmark: None,
            lookback_days: None,
            min_observations: None,
        };

        let policy = build_market_regime_policy(Some(&req), "000300.SH")
            .expect("valid regime policy")
            .expect("enabled policy");

        assert_eq!(policy.benchmark, "000300.SH");
        assert_eq!(policy.lookback_days, 126);
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.top_n),
            Some(25)
        );
        assert_eq!(
            policy
                .rules
                .get(&quant_backtest::signal_generator::MarketRegime::Bear)
                .and_then(|rule| rule.rebalance_freq_days),
            Some(80)
        );
    }

    #[test]
    fn factor_request_builds_portfolio_drawdown_risk_control() {
        let req = factor_risk_control_request_template(None, None, None, None, None);

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
        assert_eq!(
            risk_control.portfolio_sharpe_reduce_start,
            Some(Decimal::new(60, 2))
        );
        assert_eq!(
            risk_control.portfolio_sharpe_reduce_full,
            Some(Decimal::ZERO)
        );
        assert_eq!(risk_control.portfolio_sharpe_lookback_days, Some(120));
        assert_eq!(
            risk_control.portfolio_sharpe_min_exposure,
            Some(Decimal::new(55, 2))
        );
        assert_eq!(risk_control.stop_loss_pct, Some(Decimal::new(12, 2)));
        assert_eq!(risk_control.trailing_stop_pct, Some(Decimal::new(18, 2)));
        assert_eq!(risk_control.take_profit_pct, None);
        assert_eq!(risk_control.time_stop_days, Some(120));
        assert_eq!(risk_control.reentry_cooldown_days, Some(10));
    }

    fn factor_risk_control_request_template(
        event_gate_combo_name: Option<&str>,
        event_gate_mode: Option<&str>,
        event_gate_min_score: Option<f64>,
        event_gate_boost_weight: Option<f64>,
        event_gate_active_regimes: Option<Vec<&str>>,
    ) -> RunFactorBacktestReq {
        RunFactorBacktestReq {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            strategy_version_id: "strategy-v1".to_string(),
            data_version_id: "data-v1".to_string(),
            research_dataset_id: None,
            feature_set_version_id: None,
            prediction_set_id: None,
            prediction_blend_weight: None,
            prediction_min_percentile: None,
            event_gate_combo_name: event_gate_combo_name.map(str::to_string),
            event_gate_version: "1.0.0".to_string(),
            event_gate_mode: event_gate_mode.map(str::to_string),
            event_gate_min_score,
            event_gate_boost_weight,
            event_gate_score_direction: Some("descending".to_string()),
            event_gate_active_regimes: event_gate_active_regimes
                .map(|values| values.into_iter().map(str::to_string).collect::<Vec<_>>()),
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
            capacity_risk_budget: None,
            cash_utilization: None,
            execution_impact_budget: None,
            style_risk_budget: None,
            candidate_risk_filter: None,
            candidate_ranking: None,
            risk_contribution_control: None,
            rebalance_hysteresis_pct: None,
            partial_rebalance_ratio: None,
            score_candidate_pool_size: None,
            universe_profile: None,
            effective_coverage: None,
            cost_model: None,
            execution_rules: None,
            benchmark: Some("000300.SH".to_string()),
            market_regime: None,
            stop_loss_pct: Some(0.12),
            take_profit_pct: None,
            trailing_stop_pct: Some(0.18),
            time_stop_days: Some(120),
            reentry_cooldown_days: Some(10),
            start_date: "20250101".to_string(),
            end_date: "20250131".to_string(),
            initial_capital: 1_000_000.0,
            mode: Some("standard".to_string()),
            persistence_mode: None,
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
            portfolio_sharpe_reduce_start: Some(0.60),
            portfolio_sharpe_reduce_full: Some(0.0),
            portfolio_sharpe_lookback_days: Some(120),
            portfolio_sharpe_min_exposure: Some(0.55),
        }
    }
}
