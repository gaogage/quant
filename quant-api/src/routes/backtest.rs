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
use serde_json::{json, Value};
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
    load_open_trading_days_cached, prewarm_factor_signal_batch_feature_cache,
    prewarm_market_feature_cache, score_days_for_signal_dates, CandidateRankingProfile,
    CandidateRiskFilterProfile, CapacityRiskBudgetProfile, CashUtilizationProfile, EventGateConfig,
    EventGateMode, ExecutionImpactBudgetProfile, FactorScoreOverlayConfig,
    FactorSignalBatchPrewarmReport, FactorSignalFeaturePrewarmSpec, MarketFeaturePrewarmReport,
    MarketFeatureSnapshotScope, MarketRegime, MarketRegimePolicy, PortfolioConstructionMethod,
    PredictionBlendConfig, ReturnRiskFeatureCacheMode, RiskContributionControlProfile,
    ScoreDirection, SignalConfig, SignalDataCache, StressFillConfidenceExposureProfile,
    StyleRiskBudgetProfile, TradableUniverseProfile,
};

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

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

#[derive(Debug, Clone, Deserialize)]
pub struct CleanupStaleBacktestTasksReq {
    pub dry_run: Option<bool>,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
}

fn cleanup_backtest_dry_run_default(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}

fn cleanup_backtest_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(600).clamp(60, 86_400)
}

fn cleanup_backtest_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}

fn stale_backtest_cleanup_terminal_status(status: &str) -> &'static str {
    match status {
        "cancel_requested" => "cancelled",
        _ => "timeout",
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

#[derive(Debug, Deserialize, Clone, Default)]
pub struct CostModelReq {
    pub commission_rate: Option<f64>,
    pub min_commission: Option<f64>,
    pub tax_rate: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub cost_multiplier: Option<f64>,
    pub impact_cost_coefficient: Option<f64>,
    /// 冲击成本指数（平方根=0.5，线性=1.0），默认走 base.impact_cost_exponent
    pub impact_cost_exponent: Option<f64>,
    /// 过户费率（沪市双边，默认万0.1），默认走 base.transfer_fee_rate
    pub transfer_fee_rate: Option<f64>,
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

fn effective_coverage_lookup_combo_name(req: &RunFactorBacktestReq) -> &str {
    match req.combo_name.as_str() {
        "phase7_quality_recovery_acceleration_v1" => "phase7_financial_quality_v1",
        _ => req.combo_name.as_str(),
    }
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
    let coverage_combo_name = effective_coverage_lookup_combo_name(req);
    let row = sqlx::query_as::<_, (NaiveDate, i64)>(&sql)
        .bind(coverage_combo_name)
        .bind(&req.version)
        .bind(requested_start)
        .bind(end)
        .bind(min_rows as i64)
        .fetch_optional(db)
        .await
        .map_err(|error| format!("Failed to resolve effective factor coverage: {}", error))?;

    let Some((coverage_start, observed_rows)) = row else {
        return Err(format!(
            "No effective factor coverage found for {}:{} (coverage source {}:{}) between {} and {} with min_rows={}",
            req.combo_name, req.version, coverage_combo_name, req.version, requested_start, end, min_rows
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
    let (join_sql, filter_sql) = build_universe_filter(universe_profile);

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

fn build_universe_filter(profile: TradableUniverseProfile) -> (&'static str, String) {
    // PIT 合规 ST 过滤：使用 market_stock_name_history，
    // 仅排除在 trade_date 当时已经进入 ST 状态的股票（不会用未来数据）
    let pit_st_not_in = "\n           AND mfv.symbol NOT IN (
               SELECT symbol FROM market_stock_name_history
               WHERE is_st = true
                 AND start_date <= mfv.trade_date
                 AND (end_date IS NULL OR end_date >= mfv.trade_date))";

    match profile {
        TradableUniverseProfile::All => ("", String::new()),
        TradableUniverseProfile::ListedNonSt => (
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol",
            format!("\n           AND ms.list_status = 'L'{}", pit_st_not_in),
        ),
        TradableUniverseProfile::MainChinextNonSt => (
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol",
            format!(
                "\n           AND ms.list_status = 'L'
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market IN ('主板', '创业板')
           AND ms.symbol NOT LIKE '688%SH'
           AND ms.symbol NOT IN (
               SELECT symbol FROM market_stock_suspension
               WHERE trade_date = mfv.trade_date AND suspend_type = 'S'
           )
           AND ms.symbol NOT IN (
               SELECT symbol FROM market_stock_limit
               WHERE trade_date = mfv.trade_date
           ){}",
                pit_st_not_in
            ),
        ),
        TradableUniverseProfile::MainBoardNonSt => (
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol",
            format!(
                "\n           AND ms.list_status = 'L'
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market = '主板'
           AND ms.symbol NOT LIKE '300%SZ'
           AND ms.symbol NOT LIKE '301%SZ'
           AND ms.symbol NOT LIKE '688%SH'
           AND ms.symbol NOT IN (
               SELECT symbol FROM market_stock_suspension
               WHERE trade_date = mfv.trade_date AND suspend_type = 'S'
           )
           AND ms.symbol NOT IN (
               SELECT symbol FROM market_stock_limit
               WHERE trade_date = mfv.trade_date
           ){}",
                pit_st_not_in
            ),
        ),
    }
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
        impact_cost_exponent: match req.impact_cost_exponent {
            Some(value) => decimal_from_f64(value, "cost_model.impact_cost_exponent")?,
            None => base.impact_cost_exponent,
        },
        transfer_fee_rate: match req.transfer_fee_rate {
            Some(value) => decimal_from_f64(value, "cost_model.transfer_fee_rate")?,
            None => base.transfer_fee_rate,
        },
    })
}

fn build_backtest_config(
    req: &RunBacktestReq,
    fee_base: FeeConfig,
) -> Result<BacktestConfig, String> {
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
        // C4(app_config): fee_base 由调用方注入
        fee_config: apply_cost_model(fee_base, req.cost_model.as_ref())?,
        mode: parse_mode(req.mode.as_deref()),
        max_position_pct: Decimal::new(10, 2),
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        research_dataset_id: req.research_dataset_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        portfolio_policy_id: req.portfolio_policy_id.clone(),
        parameters: json!({
            "request_type": "symbol_weight_backtest",
            "strategy_version_id": req.strategy_version_id,
            "data_version_id": req.data_version_id,
            "research_dataset_id": req.research_dataset_id,
            "feature_set_version_id": req.feature_set_version_id,
            "prediction_set_id": req.prediction_set_id,
            "portfolio_policy_id": req.portfolio_policy_id,
            "symbols": req.symbols,
            "weights": req.weights,
            "benchmark": req.benchmark,
            "start_date": start,
            "end_date": end,
            "rebalance_frequency": req.rebalance_frequency,
            "cost_model": cost_model_snapshot(&req.cost_model),
            "execution_rules": execution_rules_snapshot(&req.execution_rules),
            "persistence_mode": req.persistence_mode
        }),
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

    let fee_base = crate::routes::shared::backtest_fee_base(&state.db).await;
    let config = match build_backtest_config(&req, fee_base) {
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

/// POST /api/v1/quant/backtests/cleanup-stale
///
/// 清理 heartbeat 超时的 backtest_task 元数据。默认 dry_run=true；实际清理必须显式传
/// dry_run=false。该接口只更新任务终态和审计错误信息，不删除回测曲线、交易、持仓或结果。
pub async fn cleanup_stale_backtest_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleBacktestTasksReq>,
) -> impl IntoResponse {
    match cleanup_stale_backtest_tasks_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn cleanup_stale_backtest_tasks_inner(
    db: &sqlx::PgPool,
    req: CleanupStaleBacktestTasksReq,
) -> Result<Value, String> {
    let dry_run = cleanup_backtest_dry_run_default(req.dry_run);
    let default_timeout_seconds =
        cleanup_backtest_default_timeout_seconds(req.default_timeout_seconds);
    let limit = cleanup_backtest_limit(req.limit);

    let rows: Vec<(
        String,
        String,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT task_id, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, started_at, created_at
         FROM backtest_task
         WHERE status IN ('running', 'cancel_requested')
           AND COALESCE(last_heartbeat_at, started_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("cleanup stale backtest tasks query failed: {error}"))?;

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                started_at,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.or(*started_at).unwrap_or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "task_id": task_id,
                    "status": status,
                    "next_status": stale_backtest_cleanup_terminal_status(status),
                    "progress": progress,
                    "last_heartbeat_at": fmt_rfc3339_local(*last_heartbeat_at),
                    "started_at": fmt_rfc3339_local(*started_at),
                    "created_at": fmt_rfc3339_local(Some(*created_at)),
                    "observed_at": fmt_rfc3339_local(Some(observed_at)),
                    "heartbeat_timeout_seconds": timeout_seconds,
                })
            },
        )
        .collect::<Vec<_>>();

    if dry_run || rows.is_empty() {
        return Ok(json!({
            "dry_run": dry_run,
            "candidate_count": candidates.len(),
            "updated_task_count": 0,
            "candidates": candidates,
        }));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();

    let result = sqlx::query(
        "UPDATE backtest_task
         SET status = CASE
                 WHEN status = 'cancel_requested' THEN 'cancelled'
                 ELSE 'timeout'
             END,
             progress = CASE
                 WHEN status = 'cancel_requested' THEN progress
                 ELSE GREATEST(progress, 0)
             END,
             completed_at = now(),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 CASE
                     WHEN status = 'cancel_requested' THEN
                         'stale cancel_requested backtest finalized by cleanup-stale: no worker acknowledgement within configured timeout'
                     ELSE
                         'stale running backtest timed out by cleanup-stale: no heartbeat within configured timeout'
                 END
             )
         WHERE task_id = ANY($1) AND status IN ('running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale backtest tasks update failed: {error}"))?;

    Ok(json!({
        "dry_run": false,
        "candidate_count": candidates.len(),
        "updated_task_count": result.rows_affected(),
        "candidates": candidates,
    }))
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
    pub prediction_min_score: Option<f64>,
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
    #[serde(default = "default_kelly_fraction")] // P0: 启用Kelly得分加权
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
    pub stress_fill_confidence_exposure: Option<String>,
    #[serde(default)]
    pub rebalance_hysteresis_pct: Option<f64>,
    #[serde(default)]
    pub partial_rebalance_ratio: Option<f64>,
    #[serde(default = "default_score_candidate_pool_size")] // P0: 扩大候选池
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
    pub return_risk_feature_cache_mode: Option<String>,
    /// P4.2b score overlay:叠加第二个 combo 到主 combo,带独立 score_direction + weight。
    /// 缺省 None。设置后 SignalConfig.score_overlay 生效,用于验证 overlay alpha 补偿。
    pub overlay_combo_name: Option<String>,
    #[serde(default = "default_combo_version")]
    pub overlay_version: String,
    pub overlay_score_direction: Option<String>,
    pub overlay_weight: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MarketRegimeBacktestReq {
    pub enabled: Option<bool>,
    pub policy: Option<String>,
    pub benchmark: Option<String>,
    pub lookback_days: Option<usize>,
    pub min_observations: Option<usize>,
}

#[derive(Debug, Default, Deserialize, Clone)]
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
    #[serde(default = "default_kelly_fraction")] // P0
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
    pub stress_fill_confidence_exposure: Option<String>,
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
    pub market_regime: Option<String>,
    pub portfolio_volatility_target_pct: Option<f64>,
    pub portfolio_volatility_lookback_days: Option<usize>,
    pub portfolio_volatility_min_exposure: Option<f64>,
    pub portfolio_volatility_max_exposure: Option<f64>,
    pub trailing_stop_pct: Option<f64>,
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
    40 // P0: 20→40, 提升分散度
}
fn default_rebalance() -> String {
    "biweekly".into() // P0: monthly→biweekly, 更好捕获短期信号
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
fn default_kelly_fraction() -> f64 {
    0.25 // P0: 启用得分加权 (Kelly), 替代等权
}
fn default_score_candidate_pool_size() -> Option<usize> {
    Some(200) // P0: 扩大候选池至200
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
        "stress_fill_aware_risk_budget"
        | "stress-fill-aware-risk-budget"
        | "stress_fill_risk_budget"
        | "stress-fill-risk-budget"
        | "ml_stress_fill_risk_budget"
        | "ml-stress-fill-risk-budget" => {
            Ok(PortfolioConstructionMethod::StressFillAwareRiskBudget)
        }
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

fn parse_stress_fill_confidence_exposure_profile(
    value: Option<&str>,
) -> Result<StressFillConfidenceExposureProfile, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(StressFillConfidenceExposureProfile::parse)
        .unwrap_or(Ok(StressFillConfidenceExposureProfile::Off))
}

fn build_prediction_blend_config(
    prediction_set_id: Option<&String>,
    prediction_blend_weight: Option<f64>,
    prediction_min_percentile: Option<f64>,
    prediction_min_score: Option<f64>,
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
    if let Some(min_score) = prediction_min_score {
        if !min_score.is_finite() {
            return Err("prediction_min_score must be finite".into());
        }
    }
    if prediction_weight <= f64::EPSILON
        && prediction_min_percentile.is_none()
        && prediction_min_score.is_none()
    {
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
        prediction_min_score,
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

/// P4.2b:从请求构造 score overlay 配置。overlay_combo_name 缺省 None → 不启用 overlay。
fn build_score_overlay_config(
    req: &RunFactorBacktestReq,
) -> Result<Option<FactorScoreOverlayConfig>, String> {
    let Some(combo_name) = req.overlay_combo_name.as_ref() else {
        return Ok(None);
    };
    if combo_name.trim().is_empty() {
        return Ok(None);
    }
    let direction = parse_score_direction(
        req.overlay_score_direction
            .as_deref()
            .unwrap_or("descending"),
    )?;
    let weight = req.overlay_weight.unwrap_or(0.3).clamp(0.0, 1.0);
    Ok(Some(FactorScoreOverlayConfig {
        combo_name: combo_name.clone(),
        version: req.overlay_version.clone(),
        weight,
        score_direction: direction,
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

fn factor_rebalance_frequency(rebalance: &str) -> usize {
    match rebalance {
        "daily" => 1,
        "weekly" => 5,
        "monthly" => 20,
        // quarterly 此前落入 parse fallback → 20（与 monthly 相同），生产配置
        // 的 quarterly 语义被静默降频错配（2026-09-04 F1 实验发现）
        "quarterly" => 60,
        value => value.parse::<usize>().unwrap_or(20),
    }
}

fn parse_execution_max_participation_rate(
    req: &RunFactorBacktestReq,
) -> Result<Option<Decimal>, String> {
    req.execution_rules
        .as_ref()
        .and_then(|rules| rules.max_participation_rate)
        .map(|value| decimal_from_f64(value, "execution_rules.max_participation_rate"))
        .transpose()
}

fn cost_model_snapshot(req: &Option<CostModelReq>) -> Value {
    match req {
        Some(model) => json!({
            "commission_rate": model.commission_rate,
            "min_commission": model.min_commission,
            "tax_rate": model.tax_rate,
            "slippage_bps": model.slippage_bps,
            "cost_multiplier": model.cost_multiplier,
            "impact_cost_coefficient": model.impact_cost_coefficient,
            "impact_cost_exponent": model.impact_cost_exponent,
            "transfer_fee_rate": model.transfer_fee_rate
        }),
        None => Value::Null,
    }
}

fn execution_rules_snapshot(req: &Option<ExecutionRulesReq>) -> Value {
    match req {
        Some(rules) => json!({
            "execution_timing": rules.execution_timing,
            "execution_price": rules.execution_price,
            "execution_schedule_profile": rules.execution_schedule_profile,
            "execution_carry_policy": rules.execution_carry_policy,
            "execution_daily_target_move_limit_pct": rules.execution_daily_target_move_limit_pct,
            "execution_max_carry_days": rules.execution_max_carry_days,
            "max_participation_rate": rules.max_participation_rate
        }),
        None => Value::Null,
    }
}

fn factor_backtest_parameter_snapshot(
    req: &RunFactorBacktestReq,
    effective_start: NaiveDate,
    end: NaiveDate,
    benchmark: &str,
) -> Value {
    let mut snapshot = json!({
        "request_type": "factor_backtest",
        "combo_name": req.combo_name,
        "version": req.version,
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "research_dataset_id": req.research_dataset_id,
        "feature_set_version_id": req.feature_set_version_id,
        "prediction_set_id": req.prediction_set_id,
        "prediction_blend_weight": req.prediction_blend_weight,
        "prediction_min_percentile": req.prediction_min_percentile,
        "prediction_min_score": req.prediction_min_score,
        "event_gate_combo_name": req.event_gate_combo_name,
        "event_gate_version": req.event_gate_version,
        "event_gate_mode": req.event_gate_mode,
        "event_gate_min_score": req.event_gate_min_score,
        "event_gate_boost_weight": req.event_gate_boost_weight,
        "event_gate_score_direction": req.event_gate_score_direction,
        "event_gate_active_regimes": req.event_gate_active_regimes,
        "portfolio_policy_id": req.portfolio_policy_id,
        "top_n": req.top_n,
        "rebalance": req.rebalance,
        "entry_delay": req.entry_delay
    });
    if let Some(object) = snapshot.as_object_mut() {
        object.insert("min_amount".into(), json!(req.min_amount));
        object.insert("max_position_pct".into(), json!(req.max_position_pct));
        object.insert("skip_top_pct".into(), json!(req.skip_top_pct));
        object.insert(
            "max_pairwise_correlation".into(),
            json!(req.max_pairwise_correlation),
        );
        object.insert(
            "correlation_lookback_days".into(),
            json!(req.correlation_lookback_days),
        );
        object.insert("kelly_fraction".into(), json!(req.kelly_fraction));
        object.insert("kelly_lookback_days".into(), json!(req.kelly_lookback_days));
        object.insert("max_gross_exposure".into(), json!(req.max_gross_exposure));
        object.insert("score_direction".into(), json!(req.score_direction));
        object.insert("portfolio_method".into(), json!(req.portfolio_method));
        object.insert(
            "risk_budget_lookback_days".into(),
            json!(req.risk_budget_lookback_days),
        );
        object.insert(
            "capacity_penalty_strength".into(),
            json!(req.capacity_penalty_strength),
        );
        object.insert(
            "industry_max_weight_pct".into(),
            json!(req.industry_max_weight_pct),
        );
        object.insert(
            "capacity_risk_budget".into(),
            json!(req.capacity_risk_budget),
        );
        object.insert("cash_utilization".into(), json!(req.cash_utilization));
        object.insert(
            "execution_impact_budget".into(),
            json!(req.execution_impact_budget),
        );
        object.insert("style_risk_budget".into(), json!(req.style_risk_budget));
        object.insert(
            "candidate_risk_filter".into(),
            json!(req.candidate_risk_filter),
        );
        object.insert("candidate_ranking".into(), json!(req.candidate_ranking));
        object.insert(
            "risk_contribution_control".into(),
            json!(req.risk_contribution_control),
        );
        object.insert(
            "stress_fill_confidence_exposure".into(),
            json!(req.stress_fill_confidence_exposure),
        );
        object.insert(
            "rebalance_hysteresis_pct".into(),
            json!(req.rebalance_hysteresis_pct),
        );
        object.insert(
            "partial_rebalance_ratio".into(),
            json!(req.partial_rebalance_ratio),
        );
        object.insert(
            "score_candidate_pool_size".into(),
            json!(req.score_candidate_pool_size),
        );
        object.insert("universe_profile".into(), json!(req.universe_profile));
        object.insert(
            "effective_coverage_requested".into(),
            json!(req.effective_coverage.is_some()),
        );
        object.insert("cost_model".into(), cost_model_snapshot(&req.cost_model));
        object.insert(
            "execution_rules".into(),
            execution_rules_snapshot(&req.execution_rules),
        );
        object.insert("benchmark".into(), json!(benchmark));
        object.insert("effective_start_date".into(), json!(effective_start));
        object.insert("end_date".into(), json!(end));
        object.insert("market_regime".into(), json!(req.market_regime));
        object.insert(
            "return_risk_feature_cache_mode".into(),
            json!(req.return_risk_feature_cache_mode),
        );
        object.insert("persistence_mode".into(), json!(req.persistence_mode));
    }
    snapshot
}

fn prediction_backtest_parameter_snapshot(
    req: &RunPredictionBacktestReq,
    prediction_set_id: &str,
    benchmark: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Value {
    let mut snapshot = json!({
        "request_type": "prediction_backtest",
        "prediction_set_id": prediction_set_id,
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "research_dataset_id": req.research_dataset_id,
        "feature_set_version_id": req.feature_set_version_id,
        "portfolio_policy_id": req.portfolio_policy_id,
        "top_n": req.top_n,
        "rebalance": req.rebalance,
        "entry_delay": req.entry_delay,
        "min_amount": req.min_amount,
        "max_position_pct": req.max_position_pct,
        "skip_top_pct": req.skip_top_pct,
        "max_pairwise_correlation": req.max_pairwise_correlation,
        "correlation_lookback_days": req.correlation_lookback_days,
        "kelly_fraction": req.kelly_fraction,
        "kelly_lookback_days": req.kelly_lookback_days,
        "max_gross_exposure": req.max_gross_exposure,
        "score_direction": req.score_direction,
        "portfolio_method": req.portfolio_method,
        "risk_budget_lookback_days": req.risk_budget_lookback_days
    });
    if let Some(object) = snapshot.as_object_mut() {
        object.insert(
            "capacity_penalty_strength".into(),
            json!(req.capacity_penalty_strength),
        );
        object.insert(
            "industry_max_weight_pct".into(),
            json!(req.industry_max_weight_pct),
        );
        object.insert(
            "capacity_risk_budget".into(),
            json!(req.capacity_risk_budget),
        );
        object.insert("cash_utilization".into(), json!(req.cash_utilization));
        object.insert(
            "execution_impact_budget".into(),
            json!(req.execution_impact_budget),
        );
        object.insert("style_risk_budget".into(), json!(req.style_risk_budget));
        object.insert(
            "candidate_risk_filter".into(),
            json!(req.candidate_risk_filter),
        );
        object.insert("candidate_ranking".into(), json!(req.candidate_ranking));
        object.insert(
            "risk_contribution_control".into(),
            json!(req.risk_contribution_control),
        );
        object.insert(
            "stress_fill_confidence_exposure".into(),
            json!(req.stress_fill_confidence_exposure),
        );
        object.insert(
            "rebalance_hysteresis_pct".into(),
            json!(req.rebalance_hysteresis_pct),
        );
        object.insert(
            "partial_rebalance_ratio".into(),
            json!(req.partial_rebalance_ratio),
        );
        object.insert("cost_model".into(), cost_model_snapshot(&req.cost_model));
        object.insert(
            "execution_rules".into(),
            execution_rules_snapshot(&req.execution_rules),
        );
        object.insert("benchmark".into(), json!(benchmark));
        object.insert("start_date".into(), json!(start));
        object.insert("end_date".into(), json!(end));
        object.insert("market_regime".into(), json!(req.market_regime));
        object.insert(
            "portfolio_volatility_target_pct".into(),
            json!(req.portfolio_volatility_target_pct),
        );
        object.insert(
            "portfolio_volatility_lookback_days".into(),
            json!(req.portfolio_volatility_lookback_days),
        );
        object.insert(
            "portfolio_volatility_min_exposure".into(),
            json!(req.portfolio_volatility_min_exposure),
        );
        object.insert(
            "portfolio_volatility_max_exposure".into(),
            json!(req.portfolio_volatility_max_exposure),
        );
        object.insert("trailing_stop_pct".into(), json!(req.trailing_stop_pct));
        object.insert("persistence_mode".into(), json!(req.persistence_mode));
    }
    snapshot
}

fn build_factor_signal_config(
    req: &RunFactorBacktestReq,
    rebalance_freq_days: usize,
    max_participation_rate: Option<Decimal>,
) -> Result<SignalConfig, String> {
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
    let stress_fill_confidence_exposure_profile = parse_stress_fill_confidence_exposure_profile(
        req.stress_fill_confidence_exposure.as_deref(),
    )?;

    Ok(SignalConfig {
        combo_name: req.combo_name.clone(),
        version: req.version.clone(),
        top_n: req.top_n,
        rebalance_freq_days,
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
        stress_fill_confidence_exposure_profile,
        rebalance_hysteresis_pct: req.rebalance_hysteresis_pct.unwrap_or(0.0),
        partial_rebalance_ratio: req.partial_rebalance_ratio.unwrap_or(1.0),
        score_candidate_pool_size: req.score_candidate_pool_size.filter(|size| *size > 0),
        universe_profile,
        prediction_blend: build_prediction_blend_config(
            req.prediction_set_id.as_ref(),
            req.prediction_blend_weight,
            req.prediction_min_percentile,
            req.prediction_min_score,
        )?,
        event_gate: build_event_gate_config(req)?,
        score_overlay: build_score_overlay_config(req)?,
        portfolio_sleeve: None,
    })
}

fn build_market_feature_snapshot_scope(
    req: &RunFactorBacktestReq,
    effective_start: NaiveDate,
    end: NaiveDate,
) -> Result<MarketFeatureSnapshotScope, String> {
    let mode = parse_return_risk_feature_cache_mode(req)?;
    Ok(MarketFeatureSnapshotScope::new(
        &req.data_version_id,
        effective_start,
        end,
        effective_start,
        end,
    )
    .with_return_risk_feature_cache_mode(mode))
}

fn parse_return_risk_feature_cache_mode(
    req: &RunFactorBacktestReq,
) -> Result<ReturnRiskFeatureCacheMode, String> {
    match req
        .return_risk_feature_cache_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        None | Some("raw_matrix") | Some("raw-matrix") => Ok(ReturnRiskFeatureCacheMode::RawMatrix),
        Some("stats_matrix_experimental") | Some("stats-matrix-experimental") => Ok(
            ReturnRiskFeatureCacheMode::StatsMatrixExperimental,
        ),
        Some(value) => Err(format!(
            "return_risk_feature_cache_mode must be raw_matrix or stats_matrix_experimental, got {}",
            value
        )),
    }
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
        "north_flow_regime_confirm_v1" => {
            MarketRegimePolicy::north_flow_regime_confirm_v1(benchmark)
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
        "quality_state_alpha_h1h20_selector" => {
            MarketRegimePolicy::quality_state_alpha_h1h20_selector(benchmark)
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

pub(crate) async fn prewarm_factor_signal_cache_for_requests(
    db: &sqlx::PgPool,
    requests: &[RunFactorBacktestReq],
    signal_cache: &mut SignalDataCache,
) -> Result<FactorSignalBatchPrewarmReport, String> {
    let mut specs = Vec::with_capacity(requests.len());
    for req in requests {
        let start = parse_yyyymmdd(&req.start_date, "start_date")?;
        let end = parse_yyyymmdd(&req.end_date, "end_date")?;
        if end < start {
            return Err("end_date must be greater than or equal to start_date".into());
        }
        let rebalance_freq_days = factor_rebalance_frequency(&req.rebalance);
        let (effective_start, _) = resolve_effective_factor_coverage(db, req, start, end).await?;
        let max_participation_rate = parse_execution_max_participation_rate(req)?;
        let config = build_factor_signal_config(req, rebalance_freq_days, max_participation_rate)?;
        let benchmark = req.benchmark.clone().unwrap_or_else(|| "000300.SH".into());
        let regime_policy = build_market_regime_policy(req.market_regime.as_ref(), &benchmark)?;
        let return_risk_feature_cache_mode = parse_return_risk_feature_cache_mode(req)?;
        specs.push(FactorSignalFeaturePrewarmSpec {
            data_version_id: req.data_version_id.clone(),
            train_start: effective_start,
            train_end: end,
            test_start: effective_start,
            test_end: end,
            feature_start: effective_start,
            feature_end: end,
            config,
            regime_policy,
            return_risk_feature_cache_mode,
        });
    }

    prewarm_factor_signal_batch_feature_cache(db, signal_cache, &specs).await
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
    let reb_freq = factor_rebalance_frequency(&req.rebalance);
    let (effective_start, effective_coverage) =
        resolve_effective_factor_coverage(db, &req, start, end).await?;
    let capital = decimal_from_f64(req.initial_capital, "initial_capital")?;
    // C4(app_config)
    let fee_config = match apply_cost_model(
        crate::routes::shared::backtest_fee_base(db).await,
        req.cost_model.as_ref(),
    ) {
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
    let max_participation_rate = match parse_execution_max_participation_rate(&req) {
        Ok(value) => value,
        Err(message) => return Err(message),
    };
    let sig_config = build_factor_signal_config(&req, reb_freq, max_participation_rate)?;

    info!(
        task_id,
        combo=%req.combo_name,
        top_n=req.top_n,
        reb=reb_freq,
        start=%effective_start,
        "Generating factor signals"
    );

    let regime_policy = build_market_regime_policy(req.market_regime.as_ref(), &benchmark)?;
    let snapshot_scope = match build_market_feature_snapshot_scope(&req, effective_start, end) {
        Ok(value) => value,
        Err(message) => return Err(message),
    };
    let signals = match (regime_policy.as_ref(), signal_cache.as_deref_mut()) {
        (Some(policy), Some(cache)) => {
            quant_backtest::signal_generator::generate_regime_signals_with_cache_and_market_feature_snapshot(
                db,
                &sig_config,
                policy,
                effective_start,
                end,
                cache,
                &snapshot_scope,
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
            quant_backtest::signal_generator::generate_signals_with_cache_and_market_feature_snapshot(
                db,
                &sig_config,
                effective_start,
                end,
                cache,
                &snapshot_scope,
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
    let market_feature_prewarm_report = if let Some(cache) = signal_cache {
        let lookback_days = req
            .correlation_lookback_days
            .max(req.kelly_lookback_days)
            .max(req.risk_budget_lookback_days)
            .max(1);
        let signal_dates = signals.keys().copied().collect::<Vec<_>>();
        let trading_days = load_open_trading_days_cached(db, cache, effective_start, end).await?;
        let score_days = score_days_for_signal_dates(
            trading_days.as_ref(),
            &signal_dates,
            sig_config.entry_delay_days,
        );
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
                &score_days,
                parse_return_risk_feature_cache_mode(&req)?,
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
                .prewarm_market_data_cache(
                    cache,
                    &req.data_version_id,
                    &benchmark,
                    &all_symbols,
                    effective_start,
                    end,
                )
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
        benchmark: benchmark.clone(),
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
        parameters: factor_backtest_parameter_snapshot(&req, effective_start, end, &benchmark),
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
    let stress_fill_confidence_exposure_profile = parse_stress_fill_confidence_exposure_profile(
        req.stress_fill_confidence_exposure.as_deref(),
    )?;

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
        stress_fill_confidence_exposure_profile,
        market_regime: req.market_regime.as_ref().map(|r| r.to_string()),
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

    let benchmark = req.benchmark.clone().unwrap_or_else(|| "000300.SH".into());
    let config = BacktestConfig {
        initial_capital: capital,
        benchmark: benchmark.clone(),
        start_date: start,
        end_date: end,
        fee_config,
        mode,
        max_position_pct: Decimal::from_f64(req.max_position_pct).unwrap(),
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        research_dataset_id: req.research_dataset_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        prediction_set_id: Some(prediction_set_id.clone()),
        portfolio_policy_id: req.portfolio_policy_id.clone(),
        parameters: prediction_backtest_parameter_snapshot(
            &req,
            &prediction_set_id,
            &benchmark,
            start,
            end,
        ),
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
        risk_control: RiskControlConfig {
            trailing_stop_pct: req.trailing_stop_pct.and_then(Decimal::from_f64),
            portfolio_volatility_target_pct: req
                .portfolio_volatility_target_pct
                .and_then(Decimal::from_f64),
            portfolio_volatility_lookback_days: req.portfolio_volatility_lookback_days,
            portfolio_volatility_min_exposure: req
                .portfolio_volatility_min_exposure
                .and_then(Decimal::from_f64),
            portfolio_volatility_max_exposure: req
                .portfolio_volatility_max_exposure
                .and_then(Decimal::from_f64),
            ..RiskControlConfig::default()
        },
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
    fn cleanup_backtest_defaults_are_conservative() {
        assert!(cleanup_backtest_dry_run_default(None));
        assert!(!cleanup_backtest_dry_run_default(Some(false)));
        assert_eq!(cleanup_backtest_default_timeout_seconds(None), 600);
        assert_eq!(cleanup_backtest_default_timeout_seconds(Some(10)), 60);
        assert_eq!(
            cleanup_backtest_default_timeout_seconds(Some(100_000)),
            86_400
        );
        assert_eq!(cleanup_backtest_limit(None), 100);
        assert_eq!(cleanup_backtest_limit(Some(0)), 1);
        assert_eq!(cleanup_backtest_limit(Some(2_000)), 1_000);
    }

    #[test]
    fn stale_backtest_cleanup_status_transition_is_terminal() {
        assert_eq!(stale_backtest_cleanup_terminal_status("running"), "timeout");
        assert_eq!(
            stale_backtest_cleanup_terminal_status("cancel_requested"),
            "cancelled"
        );
    }

    #[test]
    fn effective_coverage_main_board_filter_excludes_non_stock_blank_market() {
        let sql = effective_factor_coverage_sql(TradableUniverseProfile::MainBoardNonSt);

        assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
        assert!(sql.contains("ms.list_status = 'L'"));
        assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
        assert!(sql.contains("ms.market = '主板'"));
    }

    #[test]
    fn effective_coverage_main_chinext_filter_excludes_star_market() {
        let sql = effective_factor_coverage_sql(TradableUniverseProfile::MainChinextNonSt);

        assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
        assert!(sql.contains("ms.list_status = 'L'"));
        assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
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

        let err = build_backtest_config(&req, FeeConfig::default()).unwrap_err();
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
                ..Default::default()
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

        let config = build_backtest_config(&req, FeeConfig::default()).unwrap();

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
        let err = build_prediction_blend_config(None, Some(0.25), None, None).unwrap_err();

        assert!(err.contains("prediction_set_id"));
    }

    #[test]
    fn prediction_blend_weight_builds_factor_prediction_weights() {
        let prediction_set_id = " pred-quality-growth-v1 ".to_string();

        let blend = build_prediction_blend_config(Some(&prediction_set_id), Some(0.35), None, None)
            .expect("valid blend")
            .expect("blend enabled");

        assert_eq!(blend.prediction_set_id, "pred-quality-growth-v1");
        assert!((blend.factor_weight - 0.65).abs() < f64::EPSILON);
        assert!((blend.prediction_weight - 0.35).abs() < f64::EPSILON);
        assert_eq!(blend.prediction_min_percentile, None);
        assert_eq!(blend.prediction_min_score, None);
    }

    #[test]
    fn prediction_filter_can_enable_overlay_without_blend_weight() {
        let prediction_set_id = "pred-quality-growth-v1".to_string();

        let blend = build_prediction_blend_config(Some(&prediction_set_id), None, Some(0.2), None)
            .expect("valid prediction filter")
            .expect("filter enabled");

        assert_eq!(blend.prediction_set_id, prediction_set_id);
        assert_eq!(blend.factor_weight, 1.0);
        assert_eq!(blend.prediction_weight, 0.0);
        assert_eq!(blend.prediction_min_percentile, Some(0.2));
        assert_eq!(blend.prediction_min_score, None);
    }

    #[test]
    fn prediction_min_score_can_enable_overlay_without_blend_weight() {
        let prediction_set_id = "pred-quality-growth-v1".to_string();

        let blend = build_prediction_blend_config(Some(&prediction_set_id), None, None, Some(0.0))
            .expect("valid prediction score gate")
            .expect("score gate enabled");

        assert_eq!(blend.prediction_set_id, prediction_set_id);
        assert_eq!(blend.factor_weight, 1.0);
        assert_eq!(blend.prediction_weight, 0.0);
        assert_eq!(blend.prediction_min_percentile, None);
        assert_eq!(blend.prediction_min_score, Some(0.0));
    }

    #[test]
    fn factor_backtest_parameter_snapshot_captures_audit_fields() {
        let mut req = factor_risk_control_request_template(None, None, None, None, None);
        req.combo_name = "full_pit_icir_37f".to_string();
        req.prediction_set_id = Some("pred-fullperiod-nlqr-20140101-20260630".to_string());
        req.prediction_blend_weight = Some(0.5);
        req.score_direction = "ascending".to_string();
        req.cost_model = Some(CostModelReq {
            commission_rate: Some(0.0003),
            min_commission: None,
            tax_rate: None,
            slippage_bps: Some(0.0002),
            cost_multiplier: Some(1.5),
            impact_cost_coefficient: Some(0.02),
            ..Default::default()
        });
        req.execution_rules = Some(ExecutionRulesReq {
            execution_timing: Some("next_open".to_string()),
            execution_price: Some("open".to_string()),
            execution_schedule_profile: Some("twap".to_string()),
            execution_carry_policy: Some("carry".to_string()),
            execution_daily_target_move_limit_pct: Some(0.2),
            execution_max_carry_days: Some(5),
            max_participation_rate: Some(0.1),
        });

        let snapshot = factor_backtest_parameter_snapshot(
            &req,
            NaiveDate::from_ymd_opt(2014, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
            "000300.SH",
        );

        assert_eq!(snapshot["request_type"], "factor_backtest");
        assert_eq!(snapshot["combo_name"], "full_pit_icir_37f");
        assert_eq!(snapshot["prediction_blend_weight"], 0.5);
        assert_eq!(
            snapshot["prediction_set_id"],
            "pred-fullperiod-nlqr-20140101-20260630"
        );
        assert_eq!(snapshot["score_direction"], "ascending");
        assert_eq!(snapshot["execution_rules"]["execution_timing"], "next_open");
        assert_eq!(snapshot["cost_model"]["cost_multiplier"], 1.5);
        assert_eq!(snapshot["effective_start_date"], "2014-01-02");
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
    fn effective_coverage_for_derived_pit_quality_recovery_uses_source_combo() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_quality_recovery_acceleration_v1",
            "version": "1.0.0",
            "start_date": "20200101",
            "end_date": "20221230",
            "effective_coverage": {
                "enabled": true,
                "mode": "adjust_start",
                "min_rows": 100
            }
        }))
        .expect("factor request");

        assert_eq!(
            effective_coverage_lookup_combo_name(&req),
            "phase7_financial_quality_v1"
        );
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
    fn run_factor_backtest_request_accepts_stress_fill_aware_risk_budget_portfolio_method() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "portfolio_method": "stress_fill_aware_risk_budget"
        }))
        .expect("factor request");

        let method = parse_portfolio_method(&req.portfolio_method).expect("portfolio method");

        assert_eq!(
            method,
            PortfolioConstructionMethod::StressFillAwareRiskBudget
        );
    }

    #[test]
    fn run_factor_backtest_request_accepts_stress_fill_confidence_exposure_profile() {
        let req: RunFactorBacktestReq = serde_json::from_value(json!({
            "combo_name": "phase7_financial_quality_v1",
            "start_date": "20250102",
            "end_date": "20250131",
            "stress_fill_confidence_exposure": "prediction_confidence_v1"
        }))
        .expect("factor request");

        let profile = parse_stress_fill_confidence_exposure_profile(
            req.stress_fill_confidence_exposure.as_deref(),
        )
        .expect("stress fill confidence exposure");

        assert_eq!(
            profile,
            StressFillConfidenceExposureProfile::PredictionConfidenceV1
        );

        let headroom_profile = parse_stress_fill_confidence_exposure_profile(Some(
            "prediction_confidence_ascending_capacity_headroom_v1",
        ))
        .expect("stress fill confidence headroom exposure");

        assert_eq!(
            headroom_profile,
            StressFillConfidenceExposureProfile::PredictionConfidenceAscendingCapacityHeadroomV1
        );
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

    #[test]
    fn market_feature_snapshot_scope_defaults_to_raw_return_risk_matrix_cache() {
        let req = factor_risk_control_request_template(None, None, None, None, None);
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

        let scope = build_market_feature_snapshot_scope(&req, start, end).expect("scope");

        assert!(!scope.prefer_return_risk_stats_cache());
    }

    #[test]
    fn market_feature_snapshot_scope_can_opt_into_stats_return_risk_cache_experiment() {
        let mut req = factor_risk_control_request_template(None, None, None, None, None);
        req.return_risk_feature_cache_mode = Some("stats_matrix_experimental".to_string());
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

        let scope = build_market_feature_snapshot_scope(&req, start, end).expect("scope");

        assert!(scope.prefer_return_risk_stats_cache());
    }

    #[test]
    fn market_feature_snapshot_scope_rejects_unknown_return_risk_cache_mode() {
        let mut req = factor_risk_control_request_template(None, None, None, None, None);
        req.return_risk_feature_cache_mode = Some("stats_matrix".to_string());
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

        let error = build_market_feature_snapshot_scope(&req, start, end).unwrap_err();

        assert!(error.contains("return_risk_feature_cache_mode"));
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
            prediction_min_score: None,
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
            stress_fill_confidence_exposure: None,
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
            return_risk_feature_cache_mode: None,
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
            overlay_combo_name: None,
            overlay_version: "1.0.0".to_string(),
            overlay_score_direction: None,
            overlay_weight: None,
        }
    }
}

#[cfg(test)]
mod f1_sleeve_tests {
    use super::*;

    /// F1（一致预期修正动量）sleeve 参数适配实验——分解调仓频率与 kelly 各自影响。
    /// 机制理由（非挖掘）：F1 是月频更新信号——10日调仓频率错配（换手翻倍）；
    /// kelly 每日再平衡对月内不变的分数只加噪声。生产 strategy_config 为 quarterly。
    ///
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api f1_sleeve -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn f1_sleeve_full_engine_backtest_2014_2026() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");

        for (label, rebalance, kelly, task) in [
            ("Q-k0", "quarterly", 0.0, "fbt-f1-qk0-20260904"),
            ("Q-k025", "quarterly", 0.25, "fbt-f1-qk025-20260904"),
            ("M-k0", "monthly", 0.0, "fbt-f1-mk0-20260904"),
        ] {
            let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
                "combo_name": "ar_consensus_eps_rev_solo",
                "version": "v1",
                "strategy_version_id": "factor-combo-v1",
                "data_version_id": "dv-eod-20260901",
                "top_n": 40,
                "rebalance": rebalance,
                "start_date": "20140102",
                "end_date": "20260903",
                "entry_delay": 0,
                "min_amount": 0,
                "skip_top_pct": 0.0,
                "kelly_fraction": kelly,
                "max_position_pct": 0.1,
                "max_gross_exposure": 0.95,
                "score_direction": "descending",
                "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st",
                "benchmark": "000300.SH"
            }))
            .expect("req");

            let out = execute_factor_backtest(&db, task, req)
                .await
                .expect("backtest");
            println!(
                "[F1-{}] ann={:.2}% sharpe={:.3} sortino={:.3} maxdd={:.2}% calmar={:.3} \
                 excess={:.2}% turnover={:.1} trades={} win={:.1}%",
                label,
                out.metrics.annual_return_pct,
                out.metrics.sharpe_ratio,
                out.metrics.sortino_ratio,
                out.metrics.max_drawdown_pct,
                out.metrics.calmar_ratio,
                out.metrics.excess_return_pct,
                out.metrics.turnover,
                out.metrics.num_trades,
                out.metrics.win_rate_pct
            );
        }
    }
}

#[cfg(test)]
mod h20_regime_tests {
    use super::*;

    /// h20 sleeve 回撤治理实验：原参数基底（rebalance 10、kelly 0.25、
    /// indneutral_val_v1 combo，对齐 fbt-b6435e7e 曲线）+ 5 种 regime policy。
    /// 目标：最大 Sharpe 下的最小回撤（用户 2026-09-04 指令）。
    ///
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api h20_regime -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn h20_sleeve_regime_policy_comparison() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");

        let policies: Vec<(&str, Option<&str>)> = vec![
            ("baseline-none", None),
            ("professional_default", Some("professional_default")),
            ("drawdown_control_v1", Some("drawdown_control_v1")),
            ("drawdown_control_v2", Some("drawdown_control_v2")),
            ("quality_crash_guard_v2", Some("quality_crash_guard_v2")),
            (
                "quality_bear_window_guard_v1",
                Some("quality_bear_window_guard_v1"),
            ),
        ];

        for (label, policy) in policies {
            let mut req_json = serde_json::json!({
                "combo_name": "full_pit_icir_indneutral_val_v1",
                "version": "1.0.0",
                "strategy_version_id": "factor-combo-v1",
                "data_version_id": "dv-eod-20260901",
                "top_n": 40,
                "rebalance": "10",
                "start_date": "20140102",
                "end_date": "20260903",
                "entry_delay": 0,
                "min_amount": 0,
                "skip_top_pct": 0.0,
                "kelly_fraction": 0.25,
                "max_position_pct": 0.1,
                "max_gross_exposure": 0.95,
                "score_direction": "descending",
                "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st",
                "benchmark": "000300.SH"
            });
            if let Some(p) = policy {
                req_json["market_regime"] = serde_json::json!({
                    "enabled": true,
                    "policy": p,
                    "benchmark": "000300.SH"
                });
            }
            let req: RunFactorBacktestReq = serde_json::from_value(req_json).expect("req");
            let task = format!("fbt-h20-rg-{}-20260904", label);
            let out = execute_factor_backtest(&db, &task, req)
                .await
                .expect("backtest");
            println!(
                "[h20-{}] ann={:.2}% sharpe={:.3} maxdd={:.2}% calmar={:.3} excess={:.2}% turnover={:.1}",
                label,
                out.metrics.annual_return_pct,
                out.metrics.sharpe_ratio,
                out.metrics.max_drawdown_pct,
                out.metrics.calmar_ratio,
                out.metrics.excess_return_pct,
                out.metrics.turnover
            );
        }
    }
}

#[cfg(test)]
mod f1_22f_tests {
    use super::*;

    /// 22f vs 21f(生产 fund_v2) sleeve 回测对照：F1 进 combo 的净增量。
    /// 参数严格对齐 h20 原曲线（top_n 40、rebalance 10、kelly 0.25）。
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api f1_22f -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn f1_22f_vs_21f_sleeve_backtest() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");

        for (label, combo, task) in [
            (
                "21f-iso-无F1",
                "full_pit_icir_indneutral_val_v1",
                "fbt-rebuild79-0905",
            ),
            (
                "23f-iso-含F1",
                "full_pit_icir_23f_indneutral_v2",
                "fbt-rebuild48b-0905",
            ),
        ] {
            let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
                "combo_name": combo,
                "version": "1.0.0",
                "strategy_version_id": "factor-combo-v1",
                "data_version_id": "dv-eod-20260901",
                "top_n": 40,
                "rebalance": "10",
                "start_date": "20140102",
                "end_date": "20260903",
                "entry_delay": 0,
                "min_amount": 0,
                "skip_top_pct": 0.0,
                "kelly_fraction": 0.25,
                "max_position_pct": 0.1,
                "max_gross_exposure": 0.95,
                "score_direction": "descending",
                "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st",
                "benchmark": "000300.SH"
            }))
            .expect("req");

            let out = execute_factor_backtest(&db, task, req)
                .await
                .expect("backtest");
            println!(
                "[{}] trades={} turnover={:.1} (旧引擎21f基线: ann 11.14%/sharpe 0.595)",
                label, out.metrics.num_trades, out.metrics.turnover
            );
        }
    }
}

#[cfg(test)]
mod iso23_solo {
    use super::*;

    /// 单组 23f（含 F1 中性化）——绕开双组循环的 task 冲突。
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn iso23_solo_backtest() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
            "combo_name": "full_pit_icir_23f_indneutral_v2",
            "version": "1.0.0",
            "strategy_version_id": "factor-combo-v1",
            "data_version_id": "dv-eod-20260901",
            "top_n": 40, "rebalance": "10",
            "start_date": "20140102", "end_date": "20260903",
            "entry_delay": 0, "min_amount": 0, "skip_top_pct": 0.0,
            "kelly_fraction": 0.25, "max_position_pct": 0.1, "max_gross_exposure": 0.95,
            "score_direction": "descending", "portfolio_method": "heuristic",
            "universe_profile": "main_board_non_st", "benchmark": "000300.SH"
        }))
        .expect("req");
        let out = execute_factor_backtest(&db, "fbt-iso23-solo-2230", req)
            .await
            .expect("backtest");
        println!(
            "[23f-solo] trades={} turnover={:.1}",
            out.metrics.num_trades, out.metrics.turnover
        );
    }
}

#[cfg(test)]
mod f1_quarterly_rerun {
    use super::*;

    /// 重跑1：F1 sleeve 正确 quarterly(60日)调仓——此前解析 bug 实际跑的是 20 日。
    /// 对照：72 因子新基线 sleeve（同引擎同参数）。
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn f1_sleeve_quarterly_correct() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        for (label, combo, ver, rebal, kelly, task) in [
            (
                "F1-Q60-k0",
                "ar_consensus_eps_rev_solo",
                "v1",
                "quarterly",
                0.0,
                "fbt-f1q60-k0-0907",
            ),
            (
                "F1-Q60-k025",
                "ar_consensus_eps_rev_solo",
                "v1",
                "quarterly",
                0.25,
                "fbt-f1q60-k025-0907",
            ),
            (
                "72f-Q60-k025(基线)",
                "full_pit_icir_indneutral_val_v1",
                "1.0.0",
                "quarterly",
                0.25,
                "fbt-72fq60-k025-0907",
            ),
            (
                "72f-10d-k025(原参)",
                "full_pit_icir_indneutral_val_v1",
                "1.0.0",
                "10",
                0.25,
                "fbt-72f10d-k025-0907",
            ),
        ] {
            let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
                "combo_name": combo, "version": ver,
                "strategy_version_id": "factor-combo-v1", "data_version_id": "dv-eod-20260901",
                "top_n": 40, "rebalance": rebal,
                "start_date": "20140102", "end_date": "20260903",
                "entry_delay": 0, "min_amount": 0, "skip_top_pct": 0.0,
                "kelly_fraction": kelly, "max_position_pct": 0.1, "max_gross_exposure": 0.95,
                "score_direction": "descending", "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st", "benchmark": "000300.SH"
            }))
            .expect("req");
            let out = execute_factor_backtest(&db, task, req).await.expect("bt");
            println!(
                "[{}] trades={} turnover={:.1}",
                label, out.metrics.num_trades, out.metrics.turnover
            );
        }
    }
}

#[cfg(test)]
mod ddctrl_mu_rerun {
    use super::*;

    /// 重跑3：dd_ctrl_v1 sleeve 用 72 因子新 combo 重测。
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn ddctrl_72f_rerun() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        for (label, policy, task) in [
            (
                "72f-base-none",
                None::<&str>,
                format!(
                    "fbt-72f-v3-none-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                ),
            ),
            (
                "72f-dd_ctrl_v1",
                Some("drawdown_control_v1"),
                format!(
                    "fbt-72f-v3-ddv1-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                ),
            ),
        ] {
            let mut rj = serde_json::json!({
                "combo_name": "full_pit_icir_indneutral_val_v1", "version": "1.0.0",
                "strategy_version_id": "factor-combo-v1", "data_version_id": "dv-eod-20260901",
                "top_n": 40, "rebalance": "10",
                "start_date": "20140102", "end_date": "20260903",
                "entry_delay": 0, "min_amount": 0, "skip_top_pct": 0.0,
                "kelly_fraction": 0.25, "max_position_pct": 0.1, "max_gross_exposure": 0.95,
                "score_direction": "descending", "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st", "benchmark": "000300.SH"
            });
            if let Some(p) = policy {
                rj["market_regime"] = serde_json::json!({"enabled": true, "policy": p});
            }
            let req: RunFactorBacktestReq = serde_json::from_value(rj).expect("req");
            let out = execute_factor_backtest(&db, &task, req).await.expect("bt");
            println!(
                "[{}] trades={} turnover={:.1}",
                label, out.metrics.num_trades, out.metrics.turnover
            );
        }
    }
}

#[cfg(test)]
mod sleeve_freq_scan {
    use super::*;

    /// sleeve 频率扫描：5/10/20/40 日频的 dd_ctrl sleeve 绩效对比。
    /// 机制：dd_ctrl 的回撤检测窗口 vs 调仓响应速度的平衡。
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn sleeve_frequency_scan() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        for rebal in ["5", "10", "20", "40"] {
            let task = format!("fbt-sleeve-freq-{}-0907", rebal);
            // 清旧
            let _ = sqlx::query("DELETE FROM backtest_task WHERE task_id=$1")
                .bind(&task)
                .execute(&db)
                .await;
            let _ = sqlx::query("DELETE FROM backtest_equity_curve WHERE task_id=$1")
                .bind(&task)
                .execute(&db)
                .await;
            let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
                "combo_name": "full_pit_icir_indneutral_val_v1", "version": "1.0.0",
                "strategy_version_id": "factor-combo-v1", "data_version_id": "dv-eod-20260901",
                "top_n": 40, "rebalance": rebal,
                "start_date": "20140102", "end_date": "20260903",
                "entry_delay": 0, "min_amount": 0, "skip_top_pct": 0.0,
                "kelly_fraction": 0.25, "max_position_pct": 0.1, "max_gross_exposure": 0.95,
                "score_direction": "descending", "portfolio_method": "heuristic",
                "universe_profile": "main_board_non_st", "benchmark": "000300.SH",
                "market_regime": {"enabled": true, "policy": "drawdown_control_v1"}
            }))
            .expect("req");
            let out = execute_factor_backtest(&db, &task, req).await.expect("bt");
            println!("[sleeve-{}] trades={}", rebal, out.metrics.num_trades);
        }
    }
}

#[cfg(test)]
mod sleeve_param_grid {
    use super::*;

    /// Sleeve 参数网格：top_n × kelly，dd_ctrl_v1，72 因子 combo。
    /// 输出每组 sleeve 的绩效指标，找最佳组合。
    #[tokio::test]
    #[ignore = "慢测:全周期引擎回放/参数扫描(DB+Tushare,分钟级),显式跑: -- --ignored"]
    async fn sleeve_param_grid_scan() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for top_n in [20usize, 30, 40, 50] {
            for kelly in [0.0f64, 0.15, 0.25, 0.50] {
                let task = format!("fbt-grid-{}-{}-{}", top_n, (kelly * 100.0) as u64, ts);
                let req: RunFactorBacktestReq = serde_json::from_value(serde_json::json!({
                    "combo_name": "full_pit_icir_indneutral_val_v1", "version": "1.0.0",
                    "strategy_version_id": "factor-combo-v1", "data_version_id": "dv-eod-20260901",
                    "top_n": top_n, "rebalance": "10",
                    "start_date": "20140102", "end_date": "20260903",
                    "entry_delay": 0, "min_amount": 0, "skip_top_pct": 0.0,
                    "kelly_fraction": kelly, "max_position_pct": 0.1, "max_gross_exposure": 0.95,
                    "score_direction": "descending", "portfolio_method": "heuristic",
                    "universe_profile": "main_board_non_st", "benchmark": "000300.SH",
                    "market_regime": {"enabled": true, "policy": "drawdown_control_v1"}
                }))
                .expect("req");
                let out = match execute_factor_backtest(&db, &task, req).await {
                    Ok(o) => o,
                    Err(e) => {
                        println!("[grid] top_n={} kelly={} ERR: {}", top_n, kelly, e);
                        continue;
                    }
                };
                println!(
                    "[grid] top_n={} kelly={:.2} trades={}",
                    top_n, kelly, out.metrics.num_trades
                );
            }
        }
    }
}

// ═══ 第十批测试：连库 handler + 参数校验早退 + 纯函数（2026-09-22）═══
//
// 覆盖面：
// - 连库只读 handler：list_backtests / backtest_summary / backtest_equity_curve
// - 连库写 handler：cleanup_stale_backtest_tasks_inner（超时状态迁移）
// - 连库纯函数：resolve_effective_factor_coverage / resolve_warmup_start_date
// - 参数校验早退（不触库）：run_backtest / run_factor_backtest / run_prediction_backtest
//   —— 禁止真跑回测（会 spawn 重型引擎），只测 build/parse 阶段的 Err 早退分支
// - 纯函数：decimal_from_f64 / positive_notional / apply_cost_model / optional_unit_f64 /
//   optional_decimal_pct / optional_positive_decimal_pct / factor_rebalance_frequency /
//   parse_execution_max_participation_rate / cost_model_snapshot / execution_rules_snapshot
//
// 写路径纪律：自造行键（task_id / combo_name）一律 zzz_test_bt10_{scope}_* 前缀，
// 测试前置 + 结尾双端精确清理（子表先删：equity → result → task；mfv 独立无 FK）。
// 共享父行（strategy_definition / strategy_version / data_version）幂等插入且不删，
// 防并行测试互拆（第九批 experiment_run 实锤先例）。
// cleanup-stale 候选查询扫全库超时 running/cancel_requested，并行测试的行可能进候选集
// —— 断言只核验本测试自己造的行，updated_task_count 放宽为 >= 自己行数。
#[cfg(test)]
mod tenth_batch {
    use super::*;

    /// cleanup 三连测互斥锁：apply 模式走全库 UPDATE（超时 running/cancel_requested
    /// 全部迁移终态），并行时先跑者会把后跑者刚造的候选行迁成终态，导致后者
    /// updated_task_count=0 / dry_run 断言"行仍 running"失败——dry_run 与两个
    /// apply 测试必须串行（REPORT_WRITE_TEST_LOCK 同款模式）。
    static CLEANUP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // ── 基础 helper ──

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 构造真实 AppState（PG + Tushare 经 dotenv 加载 quant/.env；
    /// cargo test cwd=quant-api 时 ../.env 命中，与 second_batch/fourth_batch 先例一致）。
    async fn test_state() -> Arc<AppState> {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        let db = test_db().await;
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("Tushare client init（dotenv 加载 quant/.env 后需 TUSHARE_TOKEN）");
        Arc::new(AppState {
            start_time: chrono::Utc::now(),
            db,
            tushare,
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    /// handler 直调后解包响应体为 serde_json::Value（crud.rs second_batch 先例）。
    async fn resp_json(resp: impl axum::response::IntoResponse) -> Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    /// NUMERIC 表列经 sqlx→serde 是字符串形态（rust_decimal 默认 serde=str），
    /// jsonb 路径才是 number——本 helper 双形态取 f64，断言统一走近似比较。
    fn json_decimal_f64(v: &Value) -> f64 {
        v.as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| v.as_f64())
            .unwrap_or_else(|| panic!("非数值形态: {v}"))
    }

    fn dec(value: &str) -> Decimal {
        value.parse::<Decimal>().expect("合法 Decimal 字面量")
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("合法日期")
    }

    /// 按场景前缀双端清理本测试专属行（子表先删，FK CASCADE 兜底但仍显式删）。
    async fn cleanup_scope(db: &sqlx::PgPool, scope: &str) {
        let prefix = format!("zzz_test_bt10_{scope}%");
        let _ = sqlx::query("DELETE FROM multi_factor_value WHERE combo_name LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_equity_curve WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_result WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_task WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
    }

    /// 共享父行：strategy_definition → strategy_version（主 sv + list 专用隔离 sv）+ data_version。
    /// 幂等插入，结尾不删（防并行测试互拆）。backtest_task 对两表均为 ON DELETE RESTRICT。
    async fn seed_shared_parents(db: &sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO strategy_definition
               (strategy_id, strategy_code, name, strategy_type, status)
             VALUES (999999910, 'zzz_test_bt10_strategy', 'zzz 第十批回测', 'zzz', 'active')
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz strategy_definition");
        for (sv, ver) in [
            ("zzz_test_bt10_sv", "v10"),
            ("zzz_test_bt10_lst_sv", "v10lst"),
        ] {
            sqlx::query(
                "INSERT INTO strategy_version
                   (strategy_version_id, strategy_code, version,
                    parameter_schema, default_parameters, status)
                 VALUES ($1, 'zzz_test_bt10_strategy', $2, '{}', '{}', 'active')
                 ON CONFLICT DO NOTHING",
            )
            .bind(sv)
            .bind(ver)
            .execute(db)
            .await
            .expect("insert zzz strategy_version");
        }
        sqlx::query(
            "INSERT INTO data_version
               (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
             VALUES ('zzz_test_bt10_dv', 'zzz 第十批数据', 'zzz',
                     '2026-01-01', '2026-01-31', '{}', 'zzz-bt10')
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz data_version");
    }

    /// 造 zzz backtest_task。心跳时间 = now() - heartbeat_age_secs（NULL 表示不写心跳，
    /// 候选查询会 COALESCE 回退 started_at/created_at——本 helper 不写 started_at，
    /// 故 NULL 时回退到 created_at = now() - created_age_secs）。
    #[allow(clippy::too_many_arguments)]
    async fn insert_bt_task(
        db: &sqlx::PgPool,
        task_id: &str,
        strategy_version_id: &str,
        status: &str,
        progress: i32,
        created_age_secs: i32,
        heartbeat_age_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
        last_completed_date: Option<NaiveDate>,
        error_message: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO backtest_task
               (task_id, strategy_version_id, data_version_id, benchmark_symbol, symbols,
                start_date, end_date, initial_capital, rebalance_frequency,
                cost_model, slippage_model, execution_rules, parameters,
                status, progress, last_heartbeat_at, heartbeat_timeout_seconds,
                created_at, last_completed_date, error_message)
             VALUES ($1, $2, 'zzz_test_bt10_dv', '000300.SH', ARRAY['ZZZ900.SH'],
                     '2026-01-05', '2026-01-30', 100000, 'monthly',
                     '{}', '{}', '{}', '{}',
                     $3, $4,
                     CASE WHEN $5::int IS NULL THEN NULL
                          ELSE now() - ($5::int * interval '1 second') END,
                     $6,
                     now() - ($7::int * interval '1 second'),
                     $8, $9)",
        )
        .bind(task_id)
        .bind(strategy_version_id)
        .bind(status)
        .bind(progress)
        .bind(heartbeat_age_secs)
        .bind(heartbeat_timeout_secs)
        .bind(created_age_secs)
        .bind(last_completed_date)
        .bind(error_message)
        .execute(db)
        .await
        .expect("insert zzz backtest_task");
    }

    /// 造 zzz backtest_result（全指标列，值与断言配套）。
    async fn insert_bt_result(db: &sqlx::PgPool, task_id: &str) {
        sqlx::query(
            "INSERT INTO backtest_result
               (result_id, task_id, total_return, annualized_return, benchmark_return,
                excess_return, sharpe_ratio, sortino_ratio, information_ratio,
                max_drawdown, turnover, total_trades, win_rate, reproducibility_hash)
             VALUES ($1, $2, 0.18, 0.20, 0.10, 0.08, 1.5, 1.8, 1.2,
                     -0.05, 3.5, 42, 0.55, $3)",
        )
        .bind(format!("{task_id}_r"))
        .bind(task_id)
        .bind(format!("{task_id}-hash"))
        .execute(db)
        .await
        .expect("insert zzz backtest_result");
    }

    /// 造 zzz backtest_equity_curve 点列（cash 固定 0，非被测列）。
    async fn insert_bt_equity(db: &sqlx::PgPool, task_id: &str, points: &[(NaiveDate, f64)]) {
        for (d, v) in points {
            sqlx::query(
                "INSERT INTO backtest_equity_curve
                   (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2, $3, 0)",
            )
            .bind(task_id)
            .bind(d)
            .bind(Decimal::from_f64(*v).expect("equity value to decimal"))
            .execute(db)
            .await
            .expect("insert zzz equity point");
        }
    }

    /// 造 zzz multi_factor_value（effective coverage 数据源，无 FK，
    /// UNIQUE(combo_name, version, symbol, trade_date) 幂等跳过）。
    async fn insert_mfv(
        db: &sqlx::PgPool,
        combo: &str,
        symbol: &str,
        trade_date: NaiveDate,
        available_at: Option<NaiveDate>,
    ) {
        sqlx::query(
            "INSERT INTO multi_factor_value
               (combo_name, version, symbol, trade_date, raw_score, available_at)
             VALUES ($1, '1.0.0', $2, $3, 1.0, $4)
             ON CONFLICT DO NOTHING",
        )
        .bind(combo)
        .bind(symbol)
        .bind(trade_date)
        .bind(available_at)
        .execute(db)
        .await
        .expect("insert zzz multi_factor_value");
    }

    /// RunFactorBacktestReq 字段量大（约 70 个，serde default 全覆盖），
    /// 经 serde_json::from_value 构造：必填仅 combo_name / start_date / end_date，
    /// Option 字段缺失自动 None。extra 覆盖默认键。
    fn factor_req(extra: Value) -> RunFactorBacktestReq {
        let mut value = json!({
            "combo_name": "zzz_test_bt10_combo",
            "start_date": "20260101",
            "end_date": "20260131",
        });
        let Some(base) = value.as_object_mut() else {
            unreachable!("json! 对象字面量必为 object");
        };
        if let Some(ext) = extra.as_object() {
            for (k, v) in ext {
                base.insert(k.clone(), v.clone());
            }
        }
        serde_json::from_value(value).expect("RunFactorBacktestReq 反序列化")
    }

    // ── 纯函数：decimal / notional / cost model ──

    #[test]
    fn decimal_from_f64_accepts_finite_rejects_non_finite() {
        assert_eq!(decimal_from_f64(2.5, "zzz_field").unwrap(), dec("2.5"));
        assert_eq!(decimal_from_f64(0.0, "zzz_field").unwrap(), Decimal::ZERO);
        assert_eq!(
            decimal_from_f64(-0.0003, "zzz_field").unwrap(),
            dec("-0.0003")
        );
        // from_f64 对 NaN/±Inf 返回 None → Err，报错文案含字段名
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = decimal_from_f64(bad, "zzz_field").expect_err("非有限值必须报错");
            assert_eq!(err, "zzz_field must be a finite number", "实际错误: {err}");
        }
    }

    #[test]
    fn positive_notional_accepts_only_positive_finite() {
        assert_eq!(positive_notional(100.0), Some(100.0));
        assert_eq!(positive_notional(0.0001), Some(0.0001));
        // 0 / 负数 / NaN / Inf 全部折叠为 None（signal_generator notional 语义）
        assert_eq!(positive_notional(0.0), None);
        assert_eq!(positive_notional(-1.0), None);
        assert_eq!(positive_notional(f64::NAN), None);
        assert_eq!(positive_notional(f64::INFINITY), None);
    }

    #[test]
    fn apply_cost_model_without_request_returns_base_and_partial_overrides() {
        // base 用非默认值，与 FeeConfig::default 区分，证明 None 分支原样透传
        let base = FeeConfig {
            commission_rate: dec("0.0011"),
            min_commission: dec("2.5"),
            tax_rate: dec("0.0012"),
            slippage_bps: dec("0.0002"),
            cost_multiplier: dec("1.4"),
            impact_cost_coefficient: dec("0.7"),
            impact_cost_exponent: dec("0.6"),
            transfer_fee_rate: dec("0.000015"),
        };
        let untouched = apply_cost_model(base.clone(), None).expect("None 分支必须 Ok");
        assert_eq!(untouched.commission_rate, dec("0.0011"));
        assert_eq!(untouched.min_commission, dec("2.5"));
        assert_eq!(untouched.tax_rate, dec("0.0012"));
        assert_eq!(untouched.slippage_bps, dec("0.0002"));
        assert_eq!(untouched.cost_multiplier, dec("1.4"));
        assert_eq!(untouched.impact_cost_coefficient, dec("0.7"));
        assert_eq!(untouched.impact_cost_exponent, dec("0.6"));
        assert_eq!(untouched.transfer_fee_rate, dec("0.000015"));

        // 部分覆盖：只给 2 个字段，其余 6 个保留 base
        let req = CostModelReq {
            commission_rate: Some(0.0005),
            slippage_bps: Some(0.0003),
            ..CostModelReq::default()
        };
        let partial = apply_cost_model(base, Some(&req)).expect("部分覆盖必须 Ok");
        assert_eq!(partial.commission_rate, dec("0.0005"));
        assert_eq!(partial.slippage_bps, dec("0.0003"));
        assert_eq!(partial.min_commission, dec("2.5"));
        assert_eq!(partial.tax_rate, dec("0.0012"));
        assert_eq!(partial.transfer_fee_rate, dec("0.000015"));
        assert_eq!(partial.impact_cost_exponent, dec("0.6"));
    }

    #[test]
    fn apply_cost_model_rejects_non_finite_input() {
        let req = CostModelReq {
            commission_rate: Some(f64::NAN),
            ..CostModelReq::default()
        };
        let err = apply_cost_model(FeeConfig::default(), Some(&req))
            .expect_err("NaN commission_rate 必须报错");
        assert_eq!(
            err, "cost_model.commission_rate must be a finite number",
            "实际错误: {err}"
        );

        let req = CostModelReq {
            tax_rate: Some(f64::INFINITY),
            ..CostModelReq::default()
        };
        let err =
            apply_cost_model(FeeConfig::default(), Some(&req)).expect_err("Inf tax_rate 必须报错");
        assert_eq!(
            err, "cost_model.tax_rate must be a finite number",
            "实际错误: {err}"
        );
    }

    #[test]
    fn optional_unit_f64_and_decimal_pct_enforce_unit_interval() {
        // None 透传
        assert_eq!(optional_unit_f64(None, "zzz_unit").unwrap(), None);
        assert_eq!(optional_decimal_pct(None, "zzz_pct").unwrap(), None);
        // 闭区间 [0, 1] 边界合法
        assert_eq!(optional_unit_f64(Some(0.0), "zzz_unit").unwrap(), Some(0.0));
        assert_eq!(optional_unit_f64(Some(1.0), "zzz_unit").unwrap(), Some(1.0));
        assert_eq!(optional_unit_f64(Some(0.5), "zzz_unit").unwrap(), Some(0.5));
        // decimal_pct 额外做 f64→Decimal 转换
        assert_eq!(
            optional_decimal_pct(Some(0.25), "zzz_pct").unwrap(),
            Some(dec("0.25"))
        );
        // 越界 / 非有限值报错，文案含字段名
        for bad in [1.5, -0.1, f64::NAN] {
            assert!(
                optional_unit_f64(Some(bad), "zzz_unit").is_err(),
                "值 {bad}"
            );
            assert!(
                optional_decimal_pct(Some(bad), "zzz_pct").is_err(),
                "值 {bad}"
            );
        }
        let err = optional_unit_f64(Some(1.5), "zzz_unit").expect_err("越界必须报错");
        assert_eq!(err, "zzz_unit must be between 0 and 1", "实际错误: {err}");
    }

    #[test]
    fn optional_positive_decimal_pct_excludes_zero_lower_bound() {
        assert_eq!(
            optional_positive_decimal_pct(None, "zzz_pos").unwrap(),
            None
        );
        // 0 是 (0, 1] 的排他下界——与 optional_decimal_pct 的关键差异
        let err = optional_positive_decimal_pct(Some(0.0), "zzz_pos").expect_err("0 必须报错");
        assert_eq!(
            err, "zzz_pos must be greater than 0 and at most 1",
            "实际错误: {err}"
        );
        assert_eq!(
            optional_positive_decimal_pct(Some(0.5), "zzz_pos").unwrap(),
            Some(dec("0.5"))
        );
        assert_eq!(
            optional_positive_decimal_pct(Some(1.0), "zzz_pos").unwrap(),
            Some(dec("1.0"))
        );
        for bad in [1.2, -0.5, f64::NAN] {
            assert!(
                optional_positive_decimal_pct(Some(bad), "zzz_pos").is_err(),
                "值 {bad}"
            );
        }
    }

    #[test]
    fn factor_rebalance_frequency_maps_and_falls_back() {
        assert_eq!(factor_rebalance_frequency("daily"), 1);
        assert_eq!(factor_rebalance_frequency("weekly"), 5);
        assert_eq!(factor_rebalance_frequency("monthly"), 20);
        // quarterly 显式 60（2026-09-04 F1 实验修复的静默降频错配，回归锚点）
        assert_eq!(factor_rebalance_frequency("quarterly"), 60);
        // 整数字符串按交易日数解析
        assert_eq!(factor_rebalance_frequency("7"), 7);
        assert_eq!(factor_rebalance_frequency("15"), 15);
        // 无法解析的值 fallback 到 20（与 monthly 等频）
        assert_eq!(factor_rebalance_frequency("junk"), 20);
        assert_eq!(factor_rebalance_frequency(""), 20);
    }

    #[test]
    fn parse_execution_max_participation_rate_optional_paths() {
        // 无 execution_rules → None
        let req = factor_req(json!({}));
        assert_eq!(parse_execution_max_participation_rate(&req).unwrap(), None);
        // 有 rules 但字段缺省 → None
        let req = factor_req(json!({ "execution_rules": { "execution_price": "open" } }));
        assert_eq!(parse_execution_max_participation_rate(&req).unwrap(), None);
        // 合法值 → Decimal
        let req = factor_req(json!({ "execution_rules": { "max_participation_rate": 0.25 } }));
        assert_eq!(
            parse_execution_max_participation_rate(&req).unwrap(),
            Some(dec("0.25"))
        );
        // null 字段 → None（JSON 路径无法携带 NaN，见下方直构用例）
        let req = factor_req(json!({ "execution_rules": { "max_participation_rate": null } }));
        assert_eq!(parse_execution_max_participation_rate(&req).unwrap(), None);

        // 非有限值报错（ExecutionRulesReq 直构注入 f64::NAN）
        let mut req = factor_req(json!({}));
        req.execution_rules = Some(ExecutionRulesReq {
            execution_timing: None,
            execution_price: None,
            execution_schedule_profile: None,
            execution_carry_policy: None,
            execution_daily_target_move_limit_pct: None,
            execution_max_carry_days: None,
            max_participation_rate: Some(f64::NAN),
        });
        let err = parse_execution_max_participation_rate(&req).expect_err("NaN 必须报错");
        assert_eq!(
            err, "execution_rules.max_participation_rate must be a finite number",
            "实际错误: {err}"
        );
    }

    #[test]
    fn cost_model_snapshot_null_and_full_field_pass_through() {
        // None → JSON null（parameters 快照语义：未配置即缺省）
        assert!(cost_model_snapshot(&None).is_null());

        let req = CostModelReq {
            commission_rate: Some(0.0005),
            impact_cost_exponent: Some(0.5),
            ..CostModelReq::default()
        };
        let snap = cost_model_snapshot(&Some(req));
        // Option<f64> → json number 或 null，全 8 键逐一透传
        assert_eq!(snap["commission_rate"].as_f64(), Some(0.0005));
        assert_eq!(snap["impact_cost_exponent"].as_f64(), Some(0.5));
        assert!(snap["min_commission"].is_null());
        assert!(snap["tax_rate"].is_null());
        assert!(snap["slippage_bps"].is_null());
        assert!(snap["cost_multiplier"].is_null());
        assert!(snap["impact_cost_coefficient"].is_null());
        assert!(snap["transfer_fee_rate"].is_null());
        for key in [
            "commission_rate",
            "min_commission",
            "tax_rate",
            "slippage_bps",
            "cost_multiplier",
            "impact_cost_coefficient",
            "impact_cost_exponent",
            "transfer_fee_rate",
        ] {
            assert!(snap.get(key).is_some(), "缺少键 {key}");
        }
    }

    #[test]
    fn execution_rules_snapshot_null_and_field_pass_through() {
        assert!(execution_rules_snapshot(&None).is_null());

        let rules: ExecutionRulesReq =
            serde_json::from_value(json!({ "execution_timing": "next_open" }))
                .expect("ExecutionRulesReq 反序列化");
        let rules = Some(rules);
        let snap = execution_rules_snapshot(&rules);
        assert_eq!(snap["execution_timing"].as_str(), Some("next_open"));
        assert!(snap["execution_price"].is_null());
        assert!(snap["execution_schedule_profile"].is_null());
        assert!(snap["execution_carry_policy"].is_null());
        assert!(snap["execution_daily_target_move_limit_pct"].is_null());
        assert!(snap["execution_max_carry_days"].is_null());
        assert!(snap["max_participation_rate"].is_null());

        let rules: ExecutionRulesReq = serde_json::from_value(json!({
            "execution_max_carry_days": 5,
            "max_participation_rate": 0.2
        }))
        .expect("ExecutionRulesReq 反序列化");
        let snap = execution_rules_snapshot(&Some(rules));
        assert_eq!(snap["execution_max_carry_days"].as_u64(), Some(5));
        assert_eq!(snap["max_participation_rate"].as_f64(), Some(0.2));
        assert!(snap["execution_timing"].is_null());
    }

    // ── 连库纯函数：resolve_warmup_start_date ──
    // 交易日历锚点（psql 实证）：2024-01 起开盘日序列
    // 01-02, 01-03, 01-04, 01-05, 01-08, 01-09, 01-10, 01-11, 01-12, 01-15 …

    #[tokio::test]
    async fn resolve_warmup_zero_days_and_five_day_offset() {
        let db = test_db().await;
        // warmup=0 短路分支：不触库直接返回 requested_start
        let requested = date(2024, 1, 2);
        assert_eq!(
            resolve_warmup_start_date(&db, requested, date(2024, 12, 31), 0)
                .await
                .expect("warmup=0 必须 Ok"),
            requested
        );
        // OFFSET 5 = 第 6 个开盘日：01-02,03,04,05,08 → 09
        assert_eq!(
            resolve_warmup_start_date(&db, date(2024, 1, 2), date(2024, 12, 31), 5)
                .await
                .expect("warmup=5 必须 Ok"),
            date(2024, 1, 9)
        );
    }

    #[tokio::test]
    async fn resolve_warmup_errors_when_calendar_exhausted() {
        let db = test_db().await;
        // 2024-01-02..05 只有 4 个开盘日（02,03,04,05），OFFSET 10 越界 → Err
        let err = resolve_warmup_start_date(&db, date(2024, 1, 2), date(2024, 1, 5), 10)
            .await
            .expect_err("日历耗尽必须报错");
        assert!(
            err.contains("No open trading day found after 10 warmup trading days"),
            "实际错误: {err}"
        );
        assert!(
            err.contains("2024-01-02") && err.contains("2024-01-05"),
            "错误信息须含起止日期: {err}"
        );
    }

    // ── 连库纯函数：resolve_effective_factor_coverage ──

    #[tokio::test]
    async fn coverage_without_policy_or_disabled_skips_lookup() {
        let db = test_db().await;
        let requested = date(2026, 1, 1);
        let end = date(2026, 1, 31);
        // 无 policy：不触库（combo 不存在，若触库必 Err——组合断言证明短路）
        let req = factor_req(json!({ "combo_name": "zzz_test_bt10_no_such_combo" }));
        let (start, summary) = resolve_effective_factor_coverage(&db, &req, requested, end)
            .await
            .expect("无 policy 必须 Ok");
        assert_eq!(start, requested);
        assert!(summary.is_none());

        // enabled=false：显式关闭同样短路
        let req = factor_req(json!({
            "combo_name": "zzz_test_bt10_no_such_combo",
            "effective_coverage": { "enabled": false }
        }));
        let (start, summary) = resolve_effective_factor_coverage(&db, &req, requested, end)
            .await
            .expect("enabled=false 必须 Ok");
        assert_eq!(start, requested);
        assert!(summary.is_none());
    }

    #[tokio::test]
    async fn coverage_adjust_start_shifts_to_first_covered_day() {
        let db = test_db().await;
        let scope = "covadj";
        cleanup_scope(&db, scope).await;
        let combo = format!("zzz_test_bt10_{scope}_combo");
        // 2 symbol × 2 天（01-05 / 01-06），available_at = trade_date（PIT 当日可见）
        for day in [date(2026, 1, 5), date(2026, 1, 6)] {
            insert_mfv(&db, &combo, "ZZZ900.SH", day, Some(day)).await;
            insert_mfv(&db, &combo, "ZZZ901.SH", day, Some(day)).await;
        }
        let req = factor_req(json!({
            "combo_name": combo,
            "effective_coverage": {
                "enabled": true, "mode": "adjust_start", "min_rows": 2,
                "include_rebalance_warmup": false
            }
        }));
        let (start, summary) =
            resolve_effective_factor_coverage(&db, &req, date(2026, 1, 1), date(2026, 1, 31))
                .await
                .expect("adjust_start 必须 Ok");
        // requested=01-01 早于首个满足 min_rows 的天 → 前移到 01-05
        assert_eq!(start, date(2026, 1, 5));
        let summary = summary.expect("开启 coverage 必产出 summary");
        assert_eq!(summary.requested_start_date, date(2026, 1, 1));
        assert_eq!(summary.effective_start_date, date(2026, 1, 5));
        assert_eq!(summary.coverage_start_date, date(2026, 1, 5));
        assert!(summary.adjusted);
        assert_eq!(summary.mode, "adjust_start");
        assert_eq!(summary.min_rows, 2);
        assert_eq!(summary.observed_rows, 2);
        assert_eq!(summary.warmup_start_date, None);
        assert_eq!(summary.warmup_trading_days, None);
        assert_eq!(summary.combo_name, combo);
        assert_eq!(summary.version, "1.0.0");
        assert_eq!(summary.universe_profile, None);
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn coverage_guard_only_rejects_pre_coverage_start() {
        let db = test_db().await;
        let scope = "covguard";
        cleanup_scope(&db, scope).await;
        let combo = format!("zzz_test_bt10_{scope}_combo");
        for day in [date(2026, 1, 5), date(2026, 1, 6)] {
            insert_mfv(&db, &combo, "ZZZ900.SH", day, Some(day)).await;
            insert_mfv(&db, &combo, "ZZZ901.SH", day, Some(day)).await;
        }
        let req = factor_req(json!({
            "combo_name": combo,
            "effective_coverage": {
                "enabled": true, "mode": "guard_only", "min_rows": 2,
                "include_rebalance_warmup": false
            }
        }));
        // guard_only 不前移起点，requested 早于 coverage 起点直接拒绝
        let err = resolve_effective_factor_coverage(&db, &req, date(2026, 1, 1), date(2026, 1, 31))
            .await
            .expect_err("guard_only 早起点必须报错");
        assert!(
            err.contains("requested start_date 2026-01-01 is before effective factor coverage start 2026-01-05"),
            "实际错误: {err}"
        );
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn coverage_pit_window_excludes_future_available_rows() {
        let db = test_db().await;
        let scope = "covpit";
        cleanup_scope(&db, scope).await;
        let combo = format!("zzz_test_bt10_{scope}_combo");
        // 01-05 两行 available_at=02-01（PIT 未来可见，不计入）
        insert_mfv(
            &db,
            &combo,
            "ZZZ900.SH",
            date(2026, 1, 5),
            Some(date(2026, 2, 1)),
        )
        .await;
        insert_mfv(
            &db,
            &combo,
            "ZZZ901.SH",
            date(2026, 1, 5),
            Some(date(2026, 2, 1)),
        )
        .await;
        // 01-06 两行当日可见（计入）
        insert_mfv(
            &db,
            &combo,
            "ZZZ900.SH",
            date(2026, 1, 6),
            Some(date(2026, 1, 6)),
        )
        .await;
        insert_mfv(
            &db,
            &combo,
            "ZZZ901.SH",
            date(2026, 1, 6),
            Some(date(2026, 1, 6)),
        )
        .await;
        let req = factor_req(json!({
            "combo_name": combo,
            "effective_coverage": {
                "enabled": true, "mode": "adjust_start", "min_rows": 2,
                "include_rebalance_warmup": false
            }
        }));
        let (start, summary) =
            resolve_effective_factor_coverage(&db, &req, date(2026, 1, 1), date(2026, 1, 31))
                .await
                .expect("PIT 过滤后仍有覆盖天");
        // 01-05 整天被 PIT 排除 → 首个覆盖天 = 01-06
        assert_eq!(start, date(2026, 1, 6));
        let summary = summary.expect("summary 必产");
        assert_eq!(summary.coverage_start_date, date(2026, 1, 6));
        assert_eq!(summary.observed_rows, 2);
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn coverage_missing_data_returns_error() {
        let db = test_db().await;
        let scope = "covmiss";
        cleanup_scope(&db, scope).await;
        let combo = format!("zzz_test_bt10_{scope}_combo");
        // 只造 1 symbol × 1 天，min_rows=2 → 无任何天满足 HAVING → Err
        insert_mfv(
            &db,
            &combo,
            "ZZZ900.SH",
            date(2026, 1, 5),
            Some(date(2026, 1, 5)),
        )
        .await;
        let req = factor_req(json!({
            "combo_name": combo,
            "effective_coverage": {
                "enabled": true, "min_rows": 2, "include_rebalance_warmup": false
            }
        }));
        let err = resolve_effective_factor_coverage(&db, &req, date(2026, 1, 1), date(2026, 1, 31))
            .await
            .expect_err("无覆盖数据必须报错");
        assert!(
            err.contains("No effective factor coverage found")
                && err.contains(&combo)
                && err.contains("min_rows=2"),
            "实际错误: {err}"
        );
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn coverage_warmup_interacts_with_coverage_start() {
        let db = test_db().await;
        let scope = "covwarm";
        cleanup_scope(&db, scope).await;
        let combo = format!("zzz_test_bt10_{scope}_combo");
        for day in [date(2026, 1, 5), date(2026, 1, 6)] {
            insert_mfv(&db, &combo, "ZZZ900.SH", day, Some(day)).await;
            insert_mfv(&db, &combo, "ZZZ901.SH", day, Some(day)).await;
        }
        // warmup_trading_days=1：2026-01-01 起第 2 个开盘日 = 01-06
        // （2026 首个开盘日 01-05，OFFSET 1 → 01-06）
        let req = factor_req(json!({
            "combo_name": combo,
            "effective_coverage": {
                "enabled": true, "mode": "adjust_start", "min_rows": 2,
                "include_rebalance_warmup": true, "warmup_trading_days": 1
            }
        }));
        let (start, summary) =
            resolve_effective_factor_coverage(&db, &req, date(2026, 1, 1), date(2026, 1, 31))
                .await
                .expect("warmup 交互必须 Ok");
        // first_eligible = max(warmup_start=01-06, coverage_start=01-05) = 01-06
        assert_eq!(start, date(2026, 1, 6));
        let summary = summary.expect("summary 必产");
        assert_eq!(summary.warmup_start_date, Some(date(2026, 1, 6)));
        assert_eq!(summary.warmup_trading_days, Some(1));
        assert_eq!(summary.effective_start_date, date(2026, 1, 6));
        assert!(summary.adjusted);
        cleanup_scope(&db, scope).await;
    }

    // ── 连库 handler：list_backtests ──

    #[tokio::test]
    async fn list_backtests_unknown_filter_returns_empty_items() {
        let state = test_state().await;
        // 不存在的 strategy_version_id 过滤 → 空列表；
        // 同时断言分页归一化：page=0 → 1，page_size=999 → clamp 200
        let query = BacktestListQuery {
            strategy_version_id: Some("zzz_test_bt10_nonexistent".into()),
            page: Some(0),
            page_size: Some(999),
            ..BacktestListQuery::default()
        };
        let body = resp_json(list_backtests(State(state), Query(query)).await).await;
        assert_eq!(body["code"].as_i64(), Some(0));
        assert_eq!(body["data"]["page"].as_i64(), Some(1));
        assert_eq!(body["data"]["page_size"].as_i64(), Some(200));
        assert_eq!(body["data"]["items"].as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn list_backtests_joins_metrics_orders_desc_and_pages() {
        let state = test_state().await;
        let db = test_db().await;
        let scope = "lst";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        // 3 行按 created_at 拉开 1 小时级差（禁同毫秒碰撞）：
        //   t_a(-3h, running, 无 result) / t_b(-2h, completed, 有 result) / t_c(-1h, completed, 无 result)
        insert_bt_task(
            &db,
            "zzz_test_bt10_lst_t_a",
            "zzz_test_bt10_lst_sv",
            "running",
            40,
            10_800,
            None,
            None,
            None,
            None,
        )
        .await;
        insert_bt_task(
            &db,
            "zzz_test_bt10_lst_t_b",
            "zzz_test_bt10_lst_sv",
            "completed",
            100,
            7_200,
            None,
            None,
            Some(date(2026, 1, 28)),
            None,
        )
        .await;
        insert_bt_task(
            &db,
            "zzz_test_bt10_lst_t_c",
            "zzz_test_bt10_lst_sv",
            "completed",
            70,
            3_600,
            None,
            None,
            None,
            None,
        )
        .await;
        insert_bt_result(&db, "zzz_test_bt10_lst_t_b").await;

        // 第一页 size=2：created_at DESC → t_c, t_b
        let query = BacktestListQuery {
            strategy_version_id: Some("zzz_test_bt10_lst_sv".into()),
            page: Some(1),
            page_size: Some(2),
            ..BacktestListQuery::default()
        };
        let body = resp_json(list_backtests(State(state.clone()), Query(query)).await).await;
        assert_eq!(body["code"].as_i64(), Some(0));
        let items = body["data"]["items"].as_array().expect("items 数组");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["task_id"].as_str(), Some("zzz_test_bt10_lst_t_c"));
        assert_eq!(items[1]["task_id"].as_str(), Some("zzz_test_bt10_lst_t_b"));
        // 公共字段（symbols 数组 / NaiveDate str / progress number / status 过滤器未启用）
        assert_eq!(items[0]["status"].as_str(), Some("completed"));
        assert_eq!(
            items[0]["strategy_version_id"].as_str(),
            Some("zzz_test_bt10_lst_sv")
        );
        assert_eq!(
            items[0]["data_version_id"].as_str(),
            Some("zzz_test_bt10_dv")
        );
        assert_eq!(
            items[0]["symbols"].as_array().map(|a| a[0].as_str()),
            Some(Some("ZZZ900.SH"))
        );
        assert_eq!(items[0]["start_date"].as_str(), Some("2026-01-05"));
        assert_eq!(items[0]["end_date"].as_str(), Some("2026-01-30"));
        assert_eq!(items[0]["benchmark_symbol"].as_str(), Some("000300.SH"));
        assert_eq!(items[0]["progress"].as_i64(), Some(70));
        // LEFT JOIN 无 result → metrics 全 null
        assert!(items[0]["metrics"]["total_return"].is_null());
        assert!(items[0]["metrics"]["total_trades"].is_null());
        // 有 result → NUMERIC 列字符串形态，近似断言
        assert!((json_decimal_f64(&items[1]["metrics"]["total_return"]) - 0.18).abs() < 1e-9);
        assert!((json_decimal_f64(&items[1]["metrics"]["sharpe_ratio"]) - 1.5).abs() < 1e-9);
        assert!((json_decimal_f64(&items[1]["metrics"]["max_drawdown"]) - (-0.05)).abs() < 1e-9);
        assert_eq!(items[1]["metrics"]["total_trades"].as_i64(), Some(42));
        assert_eq!(items[1]["last_completed_date"].as_str(), Some("2026-01-28"));
        assert!(items[0]["last_completed_date"].is_null());

        // 第二页 size=2 → 余 1 行 t_a
        let query = BacktestListQuery {
            strategy_version_id: Some("zzz_test_bt10_lst_sv".into()),
            page: Some(2),
            page_size: Some(2),
            ..BacktestListQuery::default()
        };
        let body = resp_json(list_backtests(State(state), Query(query)).await).await;
        let items = body["data"]["items"].as_array().expect("items 数组");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["task_id"].as_str(), Some("zzz_test_bt10_lst_t_a"));
        assert_eq!(items[0]["status"].as_str(), Some("running"));
        cleanup_scope(&db, scope).await;
    }

    // ── 连库 handler：backtest_summary ──

    #[tokio::test]
    async fn backtest_summary_unknown_task_reports_not_found() {
        let state = test_state().await;
        let body = resp_json(
            backtest_summary(State(state), Path("zzz_test_bt10_no_such_task".to_string())).await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(body["message"].as_str(), Some("backtest summary not found"));
    }

    #[tokio::test]
    async fn backtest_summary_task_without_result_reports_not_found() {
        let state = test_state().await;
        let db = test_db().await;
        let scope = "sum0";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        // task 存在但 backtest_result 缺失 → 第二级 not found 分支
        insert_bt_task(
            &db,
            "zzz_test_bt10_sum0_task1",
            "zzz_test_bt10_sv",
            "running",
            50,
            600,
            None,
            None,
            None,
            None,
        )
        .await;
        let body = resp_json(
            backtest_summary(State(state), Path("zzz_test_bt10_sum0_task1".to_string())).await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(body["message"].as_str(), Some("backtest summary not found"));
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn backtest_summary_returns_task_and_metric_blocks() {
        let state = test_state().await;
        let db = test_db().await;
        let scope = "sum1";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        insert_bt_task(
            &db,
            "zzz_test_bt10_sum1_task1",
            "zzz_test_bt10_sv",
            "completed",
            100,
            600,
            None,
            None,
            Some(date(2026, 1, 28)),
            None,
        )
        .await;
        insert_bt_result(&db, "zzz_test_bt10_sum1_task1").await;

        let body = resp_json(
            backtest_summary(State(state), Path("zzz_test_bt10_sum1_task1".to_string())).await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(0));
        let task = &body["data"]["task"];
        assert_eq!(task["task_id"].as_str(), Some("zzz_test_bt10_sum1_task1"));
        assert_eq!(task["status"].as_str(), Some("completed"));
        assert_eq!(
            task["strategy_version_id"].as_str(),
            Some("zzz_test_bt10_sv")
        );
        assert_eq!(task["data_version_id"].as_str(), Some("zzz_test_bt10_dv"));
        assert_eq!(task["start_date"].as_str(), Some("2026-01-05"));
        assert_eq!(task["end_date"].as_str(), Some("2026-01-30"));
        assert_eq!(task["benchmark_symbol"].as_str(), Some("000300.SH"));
        assert_eq!(task["progress"].as_i64(), Some(100));
        assert_eq!(task["last_completed_date"].as_str(), Some("2026-01-28"));
        let metrics = &body["data"]["metrics"];
        assert!((json_decimal_f64(&metrics["total_return"]) - 0.18).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["annualized_return"]) - 0.20).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["benchmark_return"]) - 0.10).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["excess_return"]) - 0.08).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["sharpe_ratio"]) - 1.5).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["sortino_ratio"]) - 1.8).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["information_ratio"]) - 1.2).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["max_drawdown"]) - (-0.05)).abs() < 1e-9);
        assert!((json_decimal_f64(&metrics["turnover"]) - 3.5).abs() < 1e-9);
        assert_eq!(metrics["total_trades"].as_i64(), Some(42));
        assert!((json_decimal_f64(&metrics["win_rate"]) - 0.55).abs() < 1e-9);
        assert_eq!(
            metrics["reproducibility_hash"].as_str(),
            Some("zzz_test_bt10_sum1_task1-hash")
        );
        cleanup_scope(&db, scope).await;
    }

    // ── 连库 handler：backtest_equity_curve ──

    #[tokio::test]
    async fn backtest_equity_curve_unknown_task_returns_empty_points() {
        let state = test_state().await;
        let body = resp_json(
            backtest_equity_curve(
                State(state),
                Path("zzz_test_bt10_no_such_task".to_string()),
                Query(EquityCurveQuery::default()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(0));
        assert_eq!(
            body["data"]["task_id"].as_str(),
            Some("zzz_test_bt10_no_such_task")
        );
        assert_eq!(body["data"]["points"].as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn backtest_equity_curve_returns_points_with_date_window() {
        let state = test_state().await;
        let db = test_db().await;
        let scope = "eq1";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        insert_bt_task(
            &db,
            "zzz_test_bt10_eq1_task1",
            "zzz_test_bt10_sv",
            "completed",
            100,
            600,
            None,
            None,
            None,
            None,
        )
        .await;
        insert_bt_equity(
            &db,
            "zzz_test_bt10_eq1_task1",
            &[
                (date(2026, 1, 5), 1_000_000.0),
                (date(2026, 1, 6), 1_005_000.0),
                (date(2026, 1, 7), 1_012_000.0),
            ],
        )
        .await;

        // 无窗口：全量 3 点按 trade_date 升序
        let body = resp_json(
            backtest_equity_curve(
                State(state.clone()),
                Path("zzz_test_bt10_eq1_task1".to_string()),
                Query(EquityCurveQuery::default()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(0));
        let points = body["data"]["points"].as_array().expect("points 数组");
        assert_eq!(points.len(), 3);
        assert_eq!(points[0]["trade_date"].as_str(), Some("2026-01-05"));
        assert_eq!(points[2]["trade_date"].as_str(), Some("2026-01-07"));
        assert!((json_decimal_f64(&points[0]["portfolio_value"]) - 1_000_000.0).abs() < 1e-6);
        assert!((json_decimal_f64(&points[2]["portfolio_value"]) - 1_012_000.0).abs() < 1e-6);

        // start 窗口：只剩 06 / 07 两点
        let query = EquityCurveQuery {
            start_date: Some("20260106".into()),
            end_date: None,
        };
        let body = resp_json(
            backtest_equity_curve(
                State(state.clone()),
                Path("zzz_test_bt10_eq1_task1".to_string()),
                Query(query),
            )
            .await,
        )
        .await;
        let points = body["data"]["points"].as_array().expect("points 数组");
        assert_eq!(points.len(), 2);
        assert_eq!(points[0]["trade_date"].as_str(), Some("2026-01-06"));

        // 双端闭区间 [06, 06]：单点
        let query = EquityCurveQuery {
            start_date: Some("20260106".into()),
            end_date: Some("20260106".into()),
        };
        let body = resp_json(
            backtest_equity_curve(
                State(state),
                Path("zzz_test_bt10_eq1_task1".to_string()),
                Query(query),
            )
            .await,
        )
        .await;
        let points = body["data"]["points"].as_array().expect("points 数组");
        assert_eq!(points.len(), 1);
        assert!((json_decimal_f64(&points[0]["portfolio_value"]) - 1_005_000.0).abs() < 1e-6);
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn backtest_equity_curve_rejects_malformed_date_query() {
        let state = test_state().await;
        // 2026 年 1 月 32 日：YYYYMMDD 词法合法但日期语义非法 → parse_yyyymmdd 拒绝
        let query = EquityCurveQuery {
            start_date: Some("20260132".into()),
            end_date: None,
        };
        let body = resp_json(
            backtest_equity_curve(
                State(state.clone()),
                Path("zzz_test_bt10_eqx".to_string()),
                Query(query),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("start_date must use YYYYMMDD format")
        );

        // 非数字同样拒绝（end_date 分支）
        let query = EquityCurveQuery {
            start_date: None,
            end_date: Some("not-a-date".into()),
        };
        let body = resp_json(
            backtest_equity_curve(
                State(state),
                Path("zzz_test_bt10_eqx".to_string()),
                Query(query),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("end_date must use YYYYMMDD format")
        );
    }

    // ── 连库写 handler：cleanup_stale_backtest_tasks_inner ──
    // 候选查询扫全库超时行，断言只核验本测试自己造的行；计数放宽 >= 自己行数。

    #[tokio::test]
    async fn cleanup_stale_dry_run_keeps_rows_untouched() {
        let _cleanup_guard = CLEANUP_TEST_LOCK.lock().await;
        let db = test_db().await;
        let scope = "cln1";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        // 心跳 2 天前 + 行级超时 60s → 稳定进候选集
        insert_bt_task(
            &db,
            "zzz_test_bt10_cln1_t1",
            "zzz_test_bt10_sv",
            "running",
            40,
            172_800,
            Some(172_800),
            Some(60),
            None,
            None,
        )
        .await;
        // dry_run 缺省 = true
        let req = CleanupStaleBacktestTasksReq {
            dry_run: None,
            default_timeout_seconds: None,
            limit: None,
        };
        let report = cleanup_stale_backtest_tasks_inner(&db, req)
            .await
            .expect("dry_run 必须 Ok");
        assert_eq!(report["dry_run"].as_bool(), Some(true));
        assert_eq!(report["updated_task_count"].as_i64(), Some(0));
        assert!(
            report["candidate_count"].as_i64().unwrap_or(0) >= 1,
            "至少含本测试造的行"
        );
        // 候选数组含自己的行且 next_status 预测为 timeout
        let candidates = report["candidates"].as_array().expect("candidates 数组");
        let mine = candidates
            .iter()
            .find(|c| c["task_id"].as_str() == Some("zzz_test_bt10_cln1_t1"))
            .expect("自己的行必须出现在候选集");
        assert_eq!(mine["status"].as_str(), Some("running"));
        assert_eq!(mine["next_status"].as_str(), Some("timeout"));
        assert_eq!(mine["progress"].as_i64(), Some(40));
        assert_eq!(mine["heartbeat_timeout_seconds"].as_i64(), Some(60));
        assert!(mine["last_heartbeat_at"].is_string());
        assert!(mine["observed_at"].is_string());
        // dry_run 不落库：行状态保持 running
        let (status,): (String,) = sqlx::query_as(
            "SELECT status FROM backtest_task WHERE task_id = 'zzz_test_bt10_cln1_t1'",
        )
        .fetch_one(&db)
        .await
        .expect("dry_run 后行仍在");
        assert_eq!(status, "running");
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn cleanup_stale_apply_marks_running_timeout() {
        let _cleanup_guard = CLEANUP_TEST_LOCK.lock().await;
        let db = test_db().await;
        let scope = "cln2";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        // 预置非空 error_message，验证 CONCAT 追加语义（原信息保留 + '; ' 连接）
        insert_bt_task(
            &db,
            "zzz_test_bt10_cln2_t1",
            "zzz_test_bt10_sv",
            "running",
            55,
            172_800,
            Some(172_800),
            Some(60),
            None,
            Some("zzz prior failure"),
        )
        .await;
        let req = CleanupStaleBacktestTasksReq {
            dry_run: Some(false),
            default_timeout_seconds: None,
            // limit 放宽到上限，防并行测试的候选行挤占本行
            limit: Some(1000),
        };
        let report = cleanup_stale_backtest_tasks_inner(&db, req)
            .await
            .expect("apply 必须 Ok");
        assert_eq!(report["dry_run"].as_bool(), Some(false));
        assert!(
            report["updated_task_count"].as_i64().unwrap_or(0) >= 1,
            "并行语义：至少更新本测试的行"
        );
        let (status, error_message, completed_at): (
            String,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
        ) = sqlx::query_as(
            "SELECT status, error_message, completed_at FROM backtest_task \
                 WHERE task_id = 'zzz_test_bt10_cln2_t1'",
        )
        .fetch_one(&db)
        .await
        .expect("apply 后行仍在");
        assert_eq!(status, "timeout");
        let error = error_message.expect("error_message 必须被写入");
        assert!(
            error.starts_with("zzz prior failure; ")
                && error.contains(
                    "stale running backtest timed out by cleanup-stale: no heartbeat within configured timeout"
                ),
            "实际 error_message: {error}"
        );
        assert!(completed_at.is_some(), "completed_at 必须落库");
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn cleanup_stale_apply_finalizes_cancel_requested() {
        let _cleanup_guard = CLEANUP_TEST_LOCK.lock().await;
        let db = test_db().await;
        let scope = "cln3";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        insert_bt_task(
            &db,
            "zzz_test_bt10_cln3_t1",
            "zzz_test_bt10_sv",
            "cancel_requested",
            70,
            172_800,
            Some(172_800),
            Some(60),
            None,
            None,
        )
        .await;
        let req = CleanupStaleBacktestTasksReq {
            dry_run: Some(false),
            default_timeout_seconds: None,
            limit: Some(1000),
        };
        let report = cleanup_stale_backtest_tasks_inner(&db, req)
            .await
            .expect("apply 必须 Ok");
        // cancel_requested 的终态预测为 cancelled（非 timeout）
        let candidates = report["candidates"].as_array().expect("candidates 数组");
        let mine = candidates
            .iter()
            .find(|c| c["task_id"].as_str() == Some("zzz_test_bt10_cln3_t1"))
            .expect("自己的行必须出现在候选集");
        assert_eq!(mine["next_status"].as_str(), Some("cancelled"));
        let (status, error_message): (String, Option<String>) = sqlx::query_as(
            "SELECT status, error_message FROM backtest_task \
             WHERE task_id = 'zzz_test_bt10_cln3_t1'",
        )
        .fetch_one(&db)
        .await
        .expect("apply 后行仍在");
        assert_eq!(status, "cancelled");
        let error = error_message.expect("error_message 必须被写入");
        assert!(
            error.contains(
                "stale cancel_requested backtest finalized by cleanup-stale: no worker acknowledgement within configured timeout"
            ),
            "实际 error_message: {error}"
        );
        cleanup_scope(&db, scope).await;
    }

    #[tokio::test]
    async fn cleanup_stale_ignores_fresh_and_custom_timeout_rows() {
        let db = test_db().await;
        let scope = "cln4";
        cleanup_scope(&db, scope).await;
        seed_shared_parents(&db).await;
        // 新鲜心跳（now()）+ 默认超时 → 未超时
        insert_bt_task(
            &db,
            "zzz_test_bt10_cln4_fresh",
            "zzz_test_bt10_sv",
            "running",
            10,
            0,
            Some(0),
            None,
            None,
            None,
        )
        .await;
        // 心跳 2 天前但行级超时 30 天（2_592_000s）→ 观察时间晚于超时线 → 未超时
        insert_bt_task(
            &db,
            "zzz_test_bt10_cln4_bigto",
            "zzz_test_bt10_sv",
            "running",
            10,
            172_800,
            Some(172_800),
            Some(2_592_000),
            None,
            None,
        )
        .await;
        let req = CleanupStaleBacktestTasksReq {
            dry_run: Some(true),
            default_timeout_seconds: None,
            limit: None,
        };
        let report = cleanup_stale_backtest_tasks_inner(&db, req)
            .await
            .expect("dry_run 必须 Ok");
        let candidates = report["candidates"].as_array().expect("candidates 数组");
        for task_id in ["zzz_test_bt10_cln4_fresh", "zzz_test_bt10_cln4_bigto"] {
            assert!(
                candidates
                    .iter()
                    .all(|c| c["task_id"].as_str() != Some(task_id)),
                "{task_id} 不应进入候选集"
            );
        }
        cleanup_scope(&db, scope).await;
    }

    // ── 参数校验早退：run_backtest / run_factor_backtest / run_prediction_backtest ──
    // 只测 build/parse 阶段 Err 早退（任何 DB 访问之前返回 code=1），禁止真跑回测。

    fn plain_run_req(
        symbols: Vec<String>,
        weights: Option<Vec<f64>>,
        start: &str,
        end: &str,
    ) -> RunBacktestReq {
        serde_json::from_value(json!({
            "strategy_version_id": "zzz_test_bt10_sv",
            "data_version_id": "zzz_test_bt10_dv",
            "symbols": symbols,
            "weights": weights,
            "start_date": start,
            "end_date": end
        }))
        .expect("RunBacktestReq 反序列化")
    }

    #[tokio::test]
    async fn run_backtest_rejects_symbol_weight_mismatches() {
        let state = test_state().await;
        // weights 数量与 symbols 不匹配 → build_backtest_config 第一道 Err（不触库）
        let req = plain_run_req(
            vec!["600000.SH".into(), "000001.SZ".into()],
            Some(vec![0.5, 0.3, 0.2]),
            "20260101",
            "20260131",
        );
        let body = resp_json(run_backtest(State(state.clone()), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("weights length must match symbols length")
        );

        // symbols 空 → 第一道校验
        let req = plain_run_req(vec![], None, "20260101", "20260131");
        let body = resp_json(run_backtest(State(state), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(body["message"].as_str(), Some("symbols must not be empty"));
    }

    #[tokio::test]
    async fn run_backtest_rejects_invalid_date_window() {
        let state = test_state().await;
        // 非 YYYYMMDD 格式（连字符日期）
        let req = plain_run_req(vec!["600000.SH".into()], None, "2026-01-01", "20260131");
        let body = resp_json(run_backtest(State(state.clone()), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("start_date must use YYYYMMDD format")
        );

        // end < start
        let req = plain_run_req(vec!["600000.SH".into()], None, "20260201", "20260101");
        let body = resp_json(run_backtest(State(state), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("end_date must be greater than or equal to start_date")
        );
    }

    #[tokio::test]
    async fn run_factor_backtest_rejects_malformed_dates_before_db_access() {
        let state = test_state().await;
        // execute_factor_backtest 第一行即 parse start_date，任何 DB 访问之前
        let req = factor_req(json!({ "start_date": "20261301" }));
        let body = resp_json(run_factor_backtest(State(state.clone()), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("start_date must use YYYYMMDD format")
        );

        let req = factor_req(json!({ "start_date": "20260201", "end_date": "20260101" }));
        let body = resp_json(run_factor_backtest(State(state), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("end_date must be greater than or equal to start_date")
        );
    }

    #[tokio::test]
    async fn run_prediction_backtest_rejects_blank_set_and_bad_dates() {
        let state = test_state().await;
        // 空白 prediction_set_id：trim 后为空 → execute_prediction_backtest 首道校验
        let req: RunPredictionBacktestReq = serde_json::from_value(json!({
            "prediction_set_id": "   ",
            "start_date": "20260101",
            "end_date": "20260131"
        }))
        .expect("RunPredictionBacktestReq 反序列化");
        let body = resp_json(run_prediction_backtest(State(state.clone()), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("prediction_set_id must not be empty")
        );

        // end < start
        let req: RunPredictionBacktestReq = serde_json::from_value(json!({
            "prediction_set_id": "zzz_test_bt10_ps",
            "start_date": "20260131",
            "end_date": "20260101"
        }))
        .expect("RunPredictionBacktestReq 反序列化");
        let body = resp_json(run_prediction_backtest(State(state), Json(req)).await).await;
        assert_eq!(body["code"].as_i64(), Some(1));
        assert_eq!(
            body["message"].as_str(),
            Some("end_date must be greater than or equal to start_date")
        );
    }
}
