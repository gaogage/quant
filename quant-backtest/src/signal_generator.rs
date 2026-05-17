//! Factor-based signal generator
//!
//! Converts factor combination scores from `multi_factor_value` into daily
//! `StrategySignal` objects for the backtest engine.
//!
//! Strategy: rank all stocks by combo score each rebalance day, pick top-N,
//! assign equal weight.

use std::collections::{HashMap, HashSet};

use chrono::{Duration, NaiveDate};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::info;

use crate::engine::StrategySignal;

/// Signal generation parameters
#[derive(Debug, Clone)]
pub struct SignalConfig {
    /// Combo name in multi_factor_value
    pub combo_name: String,
    /// Combo version
    pub version: String,
    /// Number of stocks to hold
    pub top_n: usize,
    /// Rebalance frequency in trading days (1=daily, 5=weekly, 20=monthly)
    pub rebalance_freq_days: usize,
    /// Delay (trading days) before entering after signal. 0=enter next day.
    /// Set >0 to skip the "dip period" where signal takes time to materialize.
    pub entry_delay_days: usize,
    /// Minimum average daily trading amount in CNY. Stocks below this are excluded.
    /// e.g. 50_000_000.0 = 50M CNY/day. None = no filter.
    pub min_daily_amount_cny: Option<f64>,
    /// Max position weight per stock (e.g. 0.10 = 10%)
    pub max_position_pct: Decimal,
    /// Skip top N% of ranked stocks to avoid value traps (extreme reversal = junk).
    /// e.g. 0.15 = skip top 15%, pick from the 15th-100th percentile.
    /// Default: 0.0 (pick from top)
    pub skip_top_pct: f64,
    /// Optional max absolute pairwise correlation among selected holdings.
    pub max_pairwise_correlation: Option<f64>,
    /// Trailing trading-day returns used for correlation estimates.
    pub correlation_lookback_days: usize,
    /// Fractional Kelly multiplier. 0 disables Kelly and uses equal weights.
    pub kelly_fraction: f64,
    /// Trailing trading-day returns used for Kelly edge/variance estimates.
    pub kelly_lookback_days: usize,
    /// Max long gross exposure across generated target weights.
    pub max_gross_exposure: f64,
    /// Score direction: "descending" picks high scores first; "ascending" picks low scores first.
    pub score_direction: ScoreDirection,
    /// Portfolio construction method. Defaults to the legacy heuristic path.
    pub portfolio_method: PortfolioConstructionMethod,
    /// Trailing trading-day returns used for risk-budget covariance estimates.
    pub risk_budget_lookback_days: usize,
    /// Capacity penalty strength for risk-budget weights.
    pub capacity_penalty_strength: f64,
}

/// Signal generation parameters for persisted model predictions.
#[derive(Debug, Clone)]
pub struct PredictionSignalConfig {
    /// Prediction set id in model_prediction.
    pub prediction_set_id: String,
    /// Number of stocks to hold.
    pub top_n: usize,
    /// Rebalance frequency in trading days (1=daily, 5=weekly, 20=monthly).
    pub rebalance_freq_days: usize,
    /// Delay (trading days) before entering after signal. 0=enter next day.
    pub entry_delay_days: usize,
    /// Minimum average daily trading amount in CNY. Stocks below this are excluded.
    pub min_daily_amount_cny: Option<f64>,
    /// Max position weight per stock.
    pub max_position_pct: Decimal,
    /// Skip top N% of ranked stocks.
    pub skip_top_pct: f64,
    /// Optional max absolute pairwise correlation among selected holdings.
    pub max_pairwise_correlation: Option<f64>,
    /// Trailing trading-day returns used for correlation estimates.
    pub correlation_lookback_days: usize,
    /// Fractional Kelly multiplier. 0 disables Kelly and uses equal weights.
    pub kelly_fraction: f64,
    /// Trailing trading-day returns used for Kelly edge/variance estimates.
    pub kelly_lookback_days: usize,
    /// Max long gross exposure across generated target weights.
    pub max_gross_exposure: f64,
    /// Score direction: "descending" picks high scores first; "ascending" picks low scores first.
    pub score_direction: ScoreDirection,
    /// Portfolio construction method. Defaults to the legacy heuristic path.
    pub portfolio_method: PortfolioConstructionMethod,
    /// Trailing trading-day returns used for risk-budget covariance estimates.
    pub risk_budget_lookback_days: usize,
    /// Capacity penalty strength for risk-budget weights.
    pub capacity_penalty_strength: f64,
}

impl Default for PredictionSignalConfig {
    fn default() -> Self {
        Self {
            prediction_set_id: String::new(),
            top_n: 20,
            rebalance_freq_days: 20,
            entry_delay_days: 0,
            min_daily_amount_cny: None,
            max_position_pct: Decimal::new(10, 2),
            skip_top_pct: 0.0,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct PredictionScoreRow {
    symbol: String,
    trade_date: NaiveDate,
    score: f64,
    rank: Option<i32>,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            combo_name: "icir_weighted_3f".into(),
            version: "1.0.0".into(),
            top_n: 20,
            rebalance_freq_days: 20,    // monthly default
            entry_delay_days: 0,        // no delay by default
            min_daily_amount_cny: None, // no filter by default
            max_position_pct: Decimal::new(10, 2),
            skip_top_pct: 0.0,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    Descending,
    Ascending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioConstructionMethod {
    Heuristic,
    RiskBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Bull,
    Bear,
    HighVolatility,
    Sideways,
    Mixed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegimeSignalRule {
    pub top_n: Option<usize>,
    pub rebalance_freq_days: Option<usize>,
    pub max_gross_exposure: Option<f64>,
    pub score_direction: Option<ScoreDirection>,
    pub skip_top_pct: Option<f64>,
    pub max_position_pct: Option<Decimal>,
}

impl RegimeSignalRule {
    fn apply_to(&self, base: &SignalConfig) -> SignalConfig {
        let mut config = base.clone();
        if let Some(top_n) = self.top_n {
            config.top_n = top_n.max(1);
        }
        if let Some(rebalance_freq_days) = self.rebalance_freq_days {
            config.rebalance_freq_days = rebalance_freq_days.max(1);
        }
        if let Some(max_gross_exposure) = self.max_gross_exposure {
            config.max_gross_exposure = max_gross_exposure.clamp(0.0, 1.0);
        }
        if let Some(score_direction) = self.score_direction {
            config.score_direction = score_direction;
        }
        if let Some(skip_top_pct) = self.skip_top_pct {
            config.skip_top_pct = skip_top_pct.clamp(0.0, 0.95);
        }
        if let Some(max_position_pct) = self.max_position_pct {
            config.max_position_pct = max_position_pct.clamp(Decimal::ZERO, Decimal::ONE);
        }
        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegimePolicy {
    pub benchmark: String,
    pub lookback_days: usize,
    pub min_observations: usize,
    pub high_volatility_threshold: f64,
    pub bear_return_threshold: f64,
    pub bear_drawdown_threshold: f64,
    pub bull_return_threshold: f64,
    pub bull_max_drawdown: f64,
    pub sideways_volatility_threshold: f64,
    pub sideways_abs_return_threshold: f64,
    pub rules: HashMap<MarketRegime, RegimeSignalRule>,
}

impl MarketRegimePolicy {
    pub fn professional_default(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.50),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.05),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.35),
                score_direction: Some(ScoreDirection::Ascending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 5,
            high_volatility_threshold: 0.30,
            bear_return_threshold: -0.02,
            bear_drawdown_threshold: 0.20,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.15,
            sideways_volatility_threshold: 0.12,
            sideways_abs_return_threshold: 0.05,
            rules,
        }
    }

    pub fn apply(&self, base: &SignalConfig, regime: MarketRegime) -> SignalConfig {
        self.rules
            .get(&regime)
            .or_else(|| self.rules.get(&MarketRegime::Mixed))
            .map(|rule| rule.apply_to(base))
            .unwrap_or_else(|| base.clone())
    }
}

#[derive(Debug, Clone)]
struct PortfolioConstructionConfig {
    top_n: usize,
    max_position_pct: Decimal,
    max_pairwise_correlation: Option<f64>,
    correlation_lookback_days: usize,
    kelly_fraction: f64,
    kelly_lookback_days: usize,
    max_gross_exposure: f64,
    portfolio_method: PortfolioConstructionMethod,
    risk_budget_lookback_days: usize,
    capacity_penalty_strength: f64,
}

impl Default for PortfolioConstructionConfig {
    fn default() -> Self {
        Self {
            top_n: 20,
            max_position_pct: Decimal::new(10, 2),
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
        }
    }
}

impl From<&SignalConfig> for PortfolioConstructionConfig {
    fn from(config: &SignalConfig) -> Self {
        Self {
            top_n: config.top_n,
            max_position_pct: config.max_position_pct,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
        }
    }
}

impl From<&PredictionSignalConfig> for PortfolioConstructionConfig {
    fn from(config: &PredictionSignalConfig) -> Self {
        Self {
            top_n: config.top_n,
            max_position_pct: config.max_position_pct,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
        }
    }
}

/// Generate daily strategy signals from factor combo scores.
///
/// For each rebalance date, ranks all stocks by combo score (higher = better),
/// selects top-N, and assigns equal weight.
///
/// **Delayed entry**: when `entry_delay_days > 0`, the signal uses scores from
/// `entry_delay_days` trading days earlier than normal. This skips the initial
/// "dip period" where newly-ranked stocks tend to decline before outperforming.
pub async fn generate_signals(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    // 1. Load combo scores
    let rows: Vec<(String, NaiveDate, Option<f64>)> = sqlx::query_as(
        "SELECT symbol, trade_date, raw_score
         FROM multi_factor_value
         WHERE combo_name = $1 AND version = $2
           AND trade_date >= $3 AND trade_date <= $4
           AND (available_at IS NULL OR available_at <= trade_date)
         ORDER BY trade_date, symbol",
    )
    .bind(&config.combo_name)
    .bind(&config.version)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load combo scores: {}", e))?;

    if rows.is_empty() {
        return Err("No combo scores found".into());
    }

    // Group by date
    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }

    // Sort each day's scores according to the configured alpha direction.
    for items in scores_by_date.values_mut() {
        sort_factor_scores(items, config.score_direction);
    }

    // 2. Liquidity filter: remove stocks with low average daily trading amount
    if let Some(min_amount) = config.min_daily_amount_cny {
        // Collect symbols and date range
        let all_symbols: Vec<String> = scores_by_date
            .values()
            .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if !all_symbols.is_empty() {
            // Note: tushare amount is in 千元 (thousands of CNY). Convert threshold.
            let min_amount_1k = min_amount / 1000.0;
            // Single efficient SQL: avg daily amount per symbol
            let liquid_rows: Vec<(String,)> = sqlx::query_as(
                "SELECT symbol FROM (
                    SELECT symbol, AVG(amount) as avg_amt
                    FROM market_stock_daily_bar
                    WHERE symbol = ANY($1)
                      AND trade_date >= $2 AND trade_date <= $3
                      AND amount > 0
                    GROUP BY symbol
                    HAVING AVG(amount) >= $4
                ) sub",
            )
            .bind(&all_symbols)
            .bind(start_date)
            .bind(end_date)
            .bind(min_amount_1k)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Failed to load liquidity data: {}", e))?;

            let liquid_set: HashSet<String> = liquid_rows.into_iter().map(|(s,)| s).collect();
            let before: usize = scores_by_date.values().flatten().count();
            for stocks in scores_by_date.values_mut() {
                stocks.retain(|(sym, _)| liquid_set.contains(sym));
            }
            let after: usize = scores_by_date.values().flatten().count();
            info!("Liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
                min_amount as u64, after, before, liquid_set.len());
        }
    }

    // 3. Load trading calendar
    let trading_days = load_open_trading_days(pool, start_date, end_date).await?;

    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let return_history = load_symbol_return_history(
        pool,
        &all_symbols,
        start_date,
        end_date,
        portfolio_history_lookback_days(&PortfolioConstructionConfig::from(config)),
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs(
        pool,
        &all_symbols,
        start_date,
        end_date,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;

    let signals = build_rebalance_factor_signals(
        &trading_days,
        &scores_by_date,
        config,
        &return_history,
        &average_amounts,
        |_day, base| base.clone(),
    )?;

    Ok(signals)
}

/// Generate factor signals with market-regime-aware parameter overlays.
pub async fn generate_regime_signals(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let rows: Vec<(String, NaiveDate, Option<f64>)> = sqlx::query_as(
        "SELECT symbol, trade_date, raw_score
         FROM multi_factor_value
         WHERE combo_name = $1 AND version = $2
           AND trade_date >= $3 AND trade_date <= $4
           AND (available_at IS NULL OR available_at <= trade_date)
         ORDER BY trade_date, symbol",
    )
    .bind(&config.combo_name)
    .bind(&config.version)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load combo scores: {}", e))?;

    if rows.is_empty() {
        return Err("No combo scores found".into());
    }

    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }

    if let Some(min_amount) = config.min_daily_amount_cny {
        let all_symbols: Vec<String> = scores_by_date
            .values()
            .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !all_symbols.is_empty() {
            let min_amount_1k = min_amount / 1000.0;
            let liquid_rows: Vec<(String,)> = sqlx::query_as(
                "SELECT symbol FROM (
                    SELECT symbol, AVG(amount) as avg_amt
                    FROM market_stock_daily_bar
                    WHERE symbol = ANY($1)
                      AND trade_date >= $2 AND trade_date <= $3
                      AND amount > 0
                    GROUP BY symbol
                    HAVING AVG(amount) >= $4
                ) sub",
            )
            .bind(&all_symbols)
            .bind(start_date)
            .bind(end_date)
            .bind(min_amount_1k)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Failed to load liquidity data: {}", e))?;
            let liquid_set: HashSet<String> = liquid_rows.into_iter().map(|(s,)| s).collect();
            for stocks in scores_by_date.values_mut() {
                stocks.retain(|(sym, _)| liquid_set.contains(sym));
            }
        }
    }

    let trading_days = load_open_trading_days(pool, start_date, end_date).await?;
    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let max_lookback = portfolio_history_lookback_days(&PortfolioConstructionConfig::from(config))
        .max(policy.lookback_days);
    let return_history =
        load_symbol_return_history(pool, &all_symbols, start_date, end_date, max_lookback).await?;
    let average_amounts = load_portfolio_capacity_inputs(
        pool,
        &all_symbols,
        start_date,
        end_date,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;
    let benchmark_returns =
        load_benchmark_return_history(pool, &policy.benchmark, start_date, end_date, max_lookback)
            .await?;

    build_rebalance_factor_signals(
        &trading_days,
        &scores_by_date,
        config,
        &return_history,
        &average_amounts,
        |day, base| {
            let returns = trailing_market_returns(&benchmark_returns, day, policy.lookback_days);
            let regime = classify_market_regime(&returns, policy);
            policy.apply(base, regime)
        },
    )
}

/// Generate daily strategy signals from persisted model predictions.
///
/// This mirrors factor-combo signal timing: a signal generated on day D uses
/// scores from the previous trading day by default, then the runner executes it
/// on the next trading day.
pub async fn generate_prediction_signals(
    pool: &PgPool,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let rows: Vec<(String, NaiveDate, f64, Option<i32>)> = sqlx::query_as(
        "SELECT symbol, trade_date, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND available_at <= trade_date
         ORDER BY trade_date, rank NULLS LAST, score DESC, symbol",
    )
    .bind(&config.prediction_set_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load model predictions: {}", e))?;

    if rows.is_empty() {
        return Err("No model predictions found".into());
    }

    let prediction_rows = rows
        .into_iter()
        .map(|(symbol, trade_date, score, rank)| PredictionScoreRow {
            symbol,
            trade_date,
            score,
            rank,
        })
        .collect();

    build_prediction_signals_from_rows(pool, config, start_date, end_date, prediction_rows).await
}

async fn build_prediction_signals_from_rows(
    pool: &PgPool,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    rows: Vec<PredictionScoreRow>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>> = HashMap::new();
    for row in rows {
        if row.score.is_finite() {
            scores_by_date
                .entry(row.trade_date)
                .or_default()
                .push((row.symbol, row.score, row.rank));
        }
    }

    sort_prediction_scores(&mut scores_by_date);
    apply_prediction_liquidity_filter(pool, &mut scores_by_date, config, start_date, end_date)
        .await?;

    let trading_days = load_open_trading_days(pool, start_date, end_date).await?;
    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let return_history = load_symbol_return_history(
        pool,
        &all_symbols,
        start_date,
        end_date,
        portfolio_history_lookback_days(&PortfolioConstructionConfig::from(config)),
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs(
        pool,
        &all_symbols,
        start_date,
        end_date,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;

    build_rebalance_prediction_signals(
        &trading_days,
        &scores_by_date,
        config,
        &return_history,
        &average_amounts,
    )
}

fn sort_prediction_scores(
    scores_by_date: &mut HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
) {
    for items in scores_by_date.values_mut() {
        items.sort_by(|left, right| {
            left.2
                .unwrap_or(i32::MAX)
                .cmp(&right.2.unwrap_or(i32::MAX))
                .then_with(|| {
                    right
                        .1
                        .partial_cmp(&left.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.0.cmp(&right.0))
        });
    }
}

fn sort_factor_scores(items: &mut [(String, f64)], direction: ScoreDirection) {
    items.sort_by(|a, b| {
        let score_order = match direction {
            ScoreDirection::Descending => b.1.partial_cmp(&a.1),
            ScoreDirection::Ascending => a.1.partial_cmp(&b.1),
        }
        .unwrap_or(std::cmp::Ordering::Equal);
        score_order.then_with(|| a.0.cmp(&b.0))
    });
}

fn build_rebalance_factor_signals<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();

    for (i, &day) in trading_days.iter().enumerate() {
        let active_config = active_config_for_day(day, base_config);
        let min_idx = 1 + active_config.entry_delay_days;
        if i < min_idx {
            continue;
        }
        if i % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }

        let score_day = match score_day_for_signal(trading_days, i, &active_config) {
            Some(day) => day,
            None => continue,
        };
        let mut prev_scores = match scores_by_date.get(&score_day) {
            Some(scores) => scores.clone(),
            None => continue,
        };
        sort_factor_scores(&mut prev_scores, active_config.score_direction);

        let skip_count = if active_config.skip_top_pct > 0.0 {
            (prev_scores.len() as f64 * active_config.skip_top_pct).ceil() as usize
        } else {
            0
        };
        let candidates: Vec<(String, f64)> = prev_scores
            .iter()
            .skip(skip_count)
            .map(|(symbol, score)| (symbol.clone(), *score))
            .collect();
        if candidates.len() < active_config.top_n.min(5) {
            continue;
        }

        let portfolio_config = PortfolioConstructionConfig::from(&active_config);
        let target_weights = build_portfolio_weights(
            score_day,
            &candidates,
            return_history,
            average_amounts,
            &portfolio_config,
        );
        if target_weights.len() < active_config.top_n.min(5) {
            continue;
        }

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights,
            },
        );
    }

    info!(
        "Generated {} factor signals (base top-{}, rebalance every {}d, entry_delay {}d)",
        signals.len(),
        base_config.top_n,
        base_config.rebalance_freq_days,
        base_config.entry_delay_days
    );

    Ok(signals)
}

async fn apply_prediction_liquidity_filter(
    pool: &PgPool,
    scores_by_date: &mut HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), String> {
    let Some(min_amount) = config.min_daily_amount_cny else {
        return Ok(());
    };

    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if all_symbols.is_empty() {
        return Ok(());
    }

    let min_amount_1k = min_amount / 1000.0;
    let liquid_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT symbol FROM (
            SELECT symbol, AVG(amount) as avg_amt
            FROM market_stock_daily_bar
            WHERE symbol = ANY($1)
              AND trade_date >= $2 AND trade_date <= $3
              AND amount > 0
            GROUP BY symbol
            HAVING AVG(amount) >= $4
        ) sub",
    )
    .bind(&all_symbols)
    .bind(start_date)
    .bind(end_date)
    .bind(min_amount_1k)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load liquidity data: {}", e))?;

    let liquid_set: HashSet<String> = liquid_rows.into_iter().map(|(s,)| s).collect();
    let before: usize = scores_by_date.values().flatten().count();
    for stocks in scores_by_date.values_mut() {
        stocks.retain(|(sym, _, _)| liquid_set.contains(sym));
    }
    let after: usize = scores_by_date.values().flatten().count();
    info!(
        "Prediction liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
        min_amount as u64,
        after,
        before,
        liquid_set.len()
    );

    Ok(())
}

async fn load_open_trading_days(
    pool: &PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<NaiveDate>, String> {
    sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT trade_date FROM market_trade_calendar
         WHERE exchange = 'SSE' AND is_open = true
           AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(|(d,)| d).collect())
    .map_err(|e| format!("Failed to load calendar: {}", e))
}

fn classify_market_regime(returns: &[f64], policy: &MarketRegimePolicy) -> MarketRegime {
    let returns = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < policy.min_observations.max(1) {
        return MarketRegime::Mixed;
    }

    let total_return = returns.iter().fold(1.0, |acc, ret| acc * (1.0 + ret)) - 1.0;
    let volatility = annualized_return_volatility(&returns);
    let drawdown = drawdown_from_return_path(&returns);

    if volatility >= policy.high_volatility_threshold {
        MarketRegime::HighVolatility
    } else if total_return <= policy.bear_return_threshold
        || drawdown >= policy.bear_drawdown_threshold
    {
        MarketRegime::Bear
    } else if total_return >= policy.bull_return_threshold && drawdown <= policy.bull_max_drawdown {
        MarketRegime::Bull
    } else if volatility <= policy.sideways_volatility_threshold
        && total_return.abs() <= policy.sideways_abs_return_threshold
    {
        MarketRegime::Sideways
    } else {
        MarketRegime::Mixed
    }
}

fn trailing_market_returns(
    returns: &[(NaiveDate, f64)],
    signal_day: NaiveDate,
    lookback_days: usize,
) -> Vec<f64> {
    let mut values = returns
        .iter()
        .filter(|(date, value)| *date < signal_day && value.is_finite())
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    if values.len() > lookback_days {
        values = values[values.len() - lookback_days..].to_vec();
    }
    values
}

fn annualized_return_volatility(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    variance.sqrt() * (252.0_f64).sqrt()
}

fn drawdown_from_return_path(returns: &[f64]) -> f64 {
    let mut nav = 1.0;
    let mut peak = 1.0;
    let mut max_drawdown = 0.0;
    for ret in returns {
        nav *= 1.0 + ret;
        if nav > peak {
            peak = nav;
        }
        if peak > 0.0 {
            let drawdown = 1.0 - nav / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }
    max_drawdown
}

fn build_rebalance_prediction_signals(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    config: &PredictionSignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let min_idx = 1 + config.entry_delay_days;
    let mut signals = HashMap::new();

    for (i, &day) in trading_days.iter().enumerate() {
        if i < min_idx || i % config.rebalance_freq_days != 0 {
            continue;
        }
        let score_day = match prediction_score_day_for_signal(trading_days, i, config) {
            Some(day) => day,
            None => continue,
        };
        let prev_scores = match scores_by_date.get(&score_day) {
            Some(scores) => scores,
            None => continue,
        };

        let skip_count = if config.skip_top_pct > 0.0 {
            (prev_scores.len() as f64 * config.skip_top_pct).ceil() as usize
        } else {
            0
        };
        let candidates: Vec<(String, f64)> = prev_scores
            .iter()
            .skip(skip_count)
            .map(|(symbol, score, _)| (symbol.clone(), *score))
            .collect();
        if candidates.len() < config.top_n.min(5) {
            continue;
        }

        let target_weights = build_portfolio_weights(
            score_day,
            &candidates,
            return_history,
            average_amounts,
            &PortfolioConstructionConfig::from(config),
        );
        if target_weights.len() < config.top_n.min(5) {
            continue;
        }

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights,
            },
        );
    }

    if signals.is_empty() {
        return Err("No prediction signals generated".into());
    }

    info!(
        "Generated {} prediction signals (prediction_set={}, top-{}, rebalance every {}d, entry_delay {}d)",
        signals.len(),
        config.prediction_set_id,
        config.top_n,
        config.rebalance_freq_days,
        config.entry_delay_days
    );

    Ok(signals)
}

fn score_day_for_signal(
    trading_days: &[NaiveDate],
    signal_day_idx: usize,
    config: &SignalConfig,
) -> Option<NaiveDate> {
    let score_idx = signal_day_idx.checked_sub(1 + config.entry_delay_days)?;
    trading_days.get(score_idx).copied()
}

fn prediction_score_day_for_signal(
    trading_days: &[NaiveDate],
    signal_day_idx: usize,
    config: &PredictionSignalConfig,
) -> Option<NaiveDate> {
    let score_idx = signal_day_idx.checked_sub(1 + config.entry_delay_days)?;
    trading_days.get(score_idx).copied()
}

fn portfolio_history_lookback_days(config: &PortfolioConstructionConfig) -> usize {
    config
        .correlation_lookback_days
        .max(config.kelly_lookback_days)
        .max(config.risk_budget_lookback_days)
        .max(1)
}

async fn load_symbol_return_history(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<HashMap<String, Vec<(NaiveDate, f64)>>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let query_start = start_date - Duration::days((lookback_days as i64).saturating_mul(3));
    let rows: Vec<(String, NaiveDate, Decimal, Option<Decimal>)> = sqlx::query_as(
        "SELECT symbol, trade_date, close, pre_close
         FROM market_stock_daily_bar
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND close IS NOT NULL AND close > 0
         ORDER BY symbol, trade_date",
    )
    .bind(symbols)
    .bind(query_start)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        format!(
            "Failed to load portfolio construction return history: {}",
            e
        )
    })?;

    let mut grouped_prices: HashMap<String, Vec<(NaiveDate, f64, Option<f64>)>> = HashMap::new();
    for (symbol, date, close, pre_close) in rows {
        if let Some(close) = close
            .to_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
        {
            let pre_close = pre_close.and_then(|value| value.to_f64());
            grouped_prices
                .entry(symbol)
                .or_default()
                .push((date, close, pre_close));
        }
    }

    let mut returns_by_symbol = HashMap::new();
    for (symbol, rows) in grouped_prices {
        let mut returns = Vec::with_capacity(rows.len());
        let mut previous_close: Option<f64> = None;
        for (date, close, pre_close) in rows {
            let base = pre_close.or(previous_close);
            if let Some(base) = base.filter(|value| value.is_finite() && *value > 0.0) {
                let daily_return = close / base - 1.0;
                if daily_return.is_finite() {
                    returns.push((date, daily_return));
                }
            }
            previous_close = Some(close);
        }
        returns_by_symbol.insert(symbol, returns);
    }

    Ok(returns_by_symbol)
}

async fn load_average_amounts(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<String, f64>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<(String, Option<Decimal>)> = sqlx::query_as(
        "SELECT symbol, AVG(amount) as avg_amount
         FROM market_stock_daily_bar
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND amount > 0
         GROUP BY symbol",
    )
    .bind(symbols)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load portfolio capacity data: {}", e))?;

    Ok(rows
        .into_iter()
        .filter_map(|(symbol, amount)| {
            amount
                .and_then(|value| value.to_f64())
                .filter(|value| value.is_finite() && *value > 0.0)
                .map(|value| (symbol, value))
        })
        .collect())
}

async fn load_portfolio_capacity_inputs(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    config: &PortfolioConstructionConfig,
) -> Result<HashMap<String, f64>, String> {
    if config.portfolio_method != PortfolioConstructionMethod::RiskBudget {
        Ok(HashMap::new())
    } else {
        load_average_amounts(pool, symbols, start_date, end_date).await
    }
}

async fn load_benchmark_return_history(
    pool: &PgPool,
    benchmark: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Vec<(NaiveDate, f64)>, String> {
    let query_start = start_date - Duration::days((lookback_days as i64).saturating_mul(3));
    let rows: Vec<(NaiveDate, Decimal, Option<Decimal>)> = sqlx::query_as(
        "SELECT trade_date, close, pre_close
         FROM market_index_daily_bar
         WHERE symbol = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND close IS NOT NULL AND close > 0
         ORDER BY trade_date",
    )
    .bind(benchmark)
    .bind(query_start)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load benchmark return history: {}", e))?;

    let mut returns = Vec::with_capacity(rows.len());
    let mut previous_close: Option<f64> = None;
    for (date, close, pre_close) in rows {
        let Some(close) = close
            .to_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        let base = pre_close
            .and_then(|value| value.to_f64())
            .or(previous_close);
        if let Some(base) = base.filter(|value| value.is_finite() && *value > 0.0) {
            let daily_return = close / base - 1.0;
            if daily_return.is_finite() {
                returns.push((date, daily_return));
            }
        }
        previous_close = Some(close);
    }

    Ok(returns)
}

fn build_portfolio_weights(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let selected = select_uncorrelated_candidates(score_day, candidates, return_history, config);
    if selected.is_empty() {
        return HashMap::new();
    }

    let raw_weights = match config.portfolio_method {
        PortfolioConstructionMethod::Heuristic => {
            if config.kelly_fraction > 0.0 {
                build_kelly_raw_weights(score_day, &selected, return_history, config)
            } else {
                vec![1.0; selected.len()]
            }
        }
        PortfolioConstructionMethod::RiskBudget => build_risk_budget_raw_weights(
            score_day,
            &selected,
            return_history,
            average_amounts,
            config,
        ),
    };

    normalize_and_cap_weights(&selected, &raw_weights, config)
}

fn select_uncorrelated_candidates(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    config: &PortfolioConstructionConfig,
) -> Vec<String> {
    let mut selected: Vec<String> = Vec::new();
    for (symbol, _) in candidates {
        if selected.len() >= config.top_n {
            break;
        }
        if let Some(limit) = config.max_pairwise_correlation {
            let candidate_returns = trailing_returns(
                return_history,
                symbol,
                score_day,
                config.correlation_lookback_days,
            );
            let too_correlated = selected.iter().any(|selected_symbol| {
                let selected_returns = trailing_returns(
                    return_history,
                    selected_symbol,
                    score_day,
                    config.correlation_lookback_days,
                );
                pearson_correlation(&candidate_returns, &selected_returns)
                    .map(|corr| corr.abs() > limit)
                    .unwrap_or(false)
            });
            if too_correlated {
                continue;
            }
        }
        selected.push(symbol.clone());
    }
    selected
}

fn build_kelly_raw_weights(
    score_day: NaiveDate,
    symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let returns = trailing_returns(
            return_history,
            symbol,
            score_day,
            config.kelly_lookback_days,
        );
        let kelly = fractional_kelly_weight(&returns, config.kelly_fraction).unwrap_or(0.0);
        raw_weights.push(kelly.max(0.0));
    }
    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

fn build_risk_budget_raw_weights(
    score_day: NaiveDate,
    symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let returns = trailing_returns(
            return_history,
            symbol,
            score_day,
            config.risk_budget_lookback_days,
        );
        let volatility = sample_volatility(&returns).unwrap_or(0.20);
        let concentration_penalty =
            covariance_concentration_penalty(symbol, symbols, return_history, score_day, config);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        let risk_denominator = volatility.max(0.01) * concentration_penalty.max(1.0);
        let raw = capacity_multiplier / risk_denominator;
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

fn normalize_and_cap_weights(
    symbols: &[String],
    raw_weights: &[f64],
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let positive_sum: f64 = raw_weights
        .iter()
        .copied()
        .filter(|weight| weight.is_finite() && *weight > 0.0)
        .sum();
    if positive_sum <= 0.0 {
        return HashMap::new();
    }

    let gross = config.max_gross_exposure.clamp(0.0, 1.0);
    let mut target_weights = HashMap::new();
    for (symbol, raw_weight) in symbols.iter().zip(raw_weights.iter()) {
        if !raw_weight.is_finite() || *raw_weight <= 0.0 {
            continue;
        }
        let normalized = (*raw_weight / positive_sum * gross).max(0.0);
        let weight = Decimal::from_f64(normalized)
            .unwrap_or(Decimal::zero())
            .min(config.max_position_pct);
        if !weight.is_zero() {
            target_weights.insert(symbol.clone(), weight);
        }
    }
    target_weights
}

fn sample_volatility(returns: &[f64]) -> Option<f64> {
    let returns = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    if variance <= f64::EPSILON {
        None
    } else {
        Some(variance.sqrt())
    }
}

fn covariance_concentration_penalty(
    symbol: &str,
    symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) -> f64 {
    let own_returns = trailing_returns(
        return_history,
        symbol,
        score_day,
        config.risk_budget_lookback_days,
    );
    if own_returns.len() < 3 {
        return 1.0;
    }

    let average_abs_corr = symbols
        .iter()
        .filter(|other| other.as_str() != symbol)
        .filter_map(|other| {
            let other_returns = trailing_returns(
                return_history,
                other,
                score_day,
                config.risk_budget_lookback_days,
            );
            pearson_correlation(&own_returns, &other_returns).map(f64::abs)
        })
        .collect::<Vec<_>>();

    if average_abs_corr.is_empty() {
        1.0
    } else {
        1.0 + average_abs_corr.iter().sum::<f64>() / average_abs_corr.len() as f64
    }
}

fn capacity_score(symbol: &str, average_amounts: &HashMap<String, f64>, max_amount: f64) -> f64 {
    if max_amount <= 0.0 {
        return 1.0;
    }
    average_amounts
        .get(symbol)
        .copied()
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .map(|amount| (amount / max_amount).clamp(0.05, 1.0))
        .unwrap_or(0.50)
}

fn trailing_returns(
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    symbol: &str,
    score_day: NaiveDate,
    lookback_days: usize,
) -> Vec<f64> {
    let mut values: Vec<f64> = return_history
        .get(symbol)
        .map(|rows| {
            rows.iter()
                .filter(|(date, value)| *date < score_day && value.is_finite())
                .map(|(_, value)| *value)
                .collect()
        })
        .unwrap_or_default();
    if values.len() > lookback_days {
        values = values[values.len() - lookback_days..].to_vec();
    }
    values
}

fn pearson_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    let len = left.len().min(right.len());
    if len < 3 {
        return None;
    }
    let left = &left[left.len() - len..];
    let right = &right[right.len() - len..];
    let left_mean = left.iter().sum::<f64>() / len as f64;
    let right_mean = right.iter().sum::<f64>() / len as f64;
    let mut covariance = 0.0;
    let mut left_var = 0.0;
    let mut right_var = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_diff = *left_value - left_mean;
        let right_diff = *right_value - right_mean;
        covariance += left_diff * right_diff;
        left_var += left_diff * left_diff;
        right_var += right_diff * right_diff;
    }
    if left_var <= f64::EPSILON || right_var <= f64::EPSILON {
        return None;
    }
    Some(covariance / (left_var.sqrt() * right_var.sqrt()))
}

fn fractional_kelly_weight(returns: &[f64], fraction: f64) -> Option<f64> {
    if returns.len() < 3 || fraction <= 0.0 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    if variance <= f64::EPSILON {
        return None;
    }
    Some((mean / variance * fraction).clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_day_uses_previous_trading_day_by_default() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2026, 4, 28).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 29).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
        ];
        let config = SignalConfig {
            rebalance_freq_days: 1,
            entry_delay_days: 0,
            ..Default::default()
        };

        let score_day = score_day_for_signal(&trading_days, 2, &config).unwrap();

        assert_eq!(score_day, NaiveDate::from_ymd_opt(2026, 4, 29).unwrap());
    }

    #[test]
    fn score_day_honors_extra_entry_delay() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2026, 4, 27).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 28).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 29).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
        ];
        let config = SignalConfig {
            rebalance_freq_days: 1,
            entry_delay_days: 1,
            ..Default::default()
        };

        let score_day = score_day_for_signal(&trading_days, 3, &config).unwrap();

        assert_eq!(score_day, NaiveDate::from_ymd_opt(2026, 4, 28).unwrap());
    }

    #[test]
    fn prediction_signals_use_previous_day_ranked_predictions() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
        ];
        let score_day = trading_days[1];
        let mut scores_by_date = HashMap::new();
        scores_by_date.insert(
            score_day,
            vec![
                ("000003.SZ".to_string(), 0.8, Some(3)),
                ("000001.SZ".to_string(), 1.0, Some(1)),
                ("000002.SZ".to_string(), 0.9, Some(2)),
                ("000004.SZ".to_string(), 0.7, Some(4)),
                ("000005.SZ".to_string(), 0.6, Some(5)),
            ],
        );
        sort_prediction_scores(&mut scores_by_date);

        let signals = build_rebalance_prediction_signals(
            &trading_days,
            &scores_by_date,
            &PredictionSignalConfig {
                prediction_set_id: "pred-v1".into(),
                top_n: 5,
                rebalance_freq_days: 1,
                entry_delay_days: 0,
                max_position_pct: Decimal::new(20, 2),
                ..Default::default()
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .expect("prediction signals");

        let signal = signals
            .get(&NaiveDate::from_ymd_opt(2025, 1, 13).unwrap())
            .expect("signal on next trading day");
        assert_eq!(signal.target_weights.len(), 5);
        assert!(signal.target_weights.contains_key("000001.SZ"));
        assert!(signal.target_weights.contains_key("000005.SZ"));
        assert_eq!(
            signal.target_weights["000001.SZ"],
            Decimal::from_f64(0.2).unwrap()
        );
    }

    #[test]
    fn sort_factor_scores_honors_ascending_direction() {
        let mut scores = vec![
            ("AAA".to_string(), 3.0),
            ("BBB".to_string(), 1.0),
            ("CCC".to_string(), 2.0),
        ];

        sort_factor_scores(&mut scores, ScoreDirection::Ascending);

        assert_eq!(scores[0].0, "BBB");
        assert_eq!(scores[1].0, "CCC");
        assert_eq!(scores[2].0, "AAA");
    }

    #[test]
    fn portfolio_construction_filters_highly_correlated_candidates() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("AAA".to_string(), 3.0),
            ("BBB".to_string(), 2.0),
            ("CCC".to_string(), 1.0),
        ];
        let return_history = HashMap::from([
            ("AAA".to_string(), dated_returns(&[0.01, 0.02, 0.03, 0.04])),
            (
                "BBB".to_string(),
                dated_returns(&[0.011, 0.021, 0.031, 0.041]),
            ),
            (
                "CCC".to_string(),
                dated_returns(&[0.02, -0.01, 0.01, -0.02]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 3,
            max_position_pct: Decimal::new(50, 2),
            max_pairwise_correlation: Some(0.8),
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("AAA"));
        assert!(!weights.contains_key("BBB"));
        assert!(weights.contains_key("CCC"));
    }

    #[test]
    fn portfolio_construction_uses_fractional_kelly_with_caps() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 2.0)];
        let return_history = HashMap::from([
            ("AAA".to_string(), dated_returns(&[0.03, 0.02, 0.01, 0.02])),
            (
                "BBB".to_string(),
                dated_returns(&[0.03, -0.03, 0.02, -0.019]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(60, 2),
            kelly_fraction: 0.5,
            max_gross_exposure: 1.0,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &config,
        );

        assert!(weights["AAA"] > weights["BBB"]);
        let gross: Decimal = weights.values().copied().sum();
        assert!(gross <= Decimal::ONE);
        assert!(weights
            .values()
            .all(|weight| *weight <= Decimal::new(60, 2)));
    }

    #[test]
    fn risk_budget_portfolio_penalizes_high_volatility_and_low_capacity() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("LOW_RISK".to_string(), 3.0),
            ("HIGH_RISK".to_string(), 2.9),
        ];
        let return_history = HashMap::from([
            (
                "LOW_RISK".to_string(),
                dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
            ),
            (
                "HIGH_RISK".to_string(),
                dated_returns(&[0.08, -0.07, 0.09, -0.08, 0.07]),
            ),
        ]);
        let average_amounts = HashMap::from([
            ("LOW_RISK".to_string(), 500_000_000.0),
            ("HIGH_RISK".to_string(), 50_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(80, 2),
            max_gross_exposure: 1.0,
            portfolio_method: PortfolioConstructionMethod::RiskBudget,
            risk_budget_lookback_days: 5,
            capacity_penalty_strength: 1.0,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &average_amounts,
            &config,
        );

        assert!(weights["LOW_RISK"] > weights["HIGH_RISK"]);
        assert!(weights["LOW_RISK"] <= Decimal::new(80, 2));
        let gross: Decimal = weights.values().copied().sum();
        assert!(gross <= Decimal::ONE);
    }

    #[test]
    fn market_regime_policy_classifies_and_applies_bear_defensive_rule() {
        let base = SignalConfig {
            top_n: 80,
            rebalance_freq_days: 20,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::professional_default("000300.SH");
        let returns = vec![-0.015, -0.02, 0.004, -0.012, -0.008, 0.003, -0.01];

        let regime = classify_market_regime(&returns, &policy);
        let active = policy.apply(&base, regime);

        assert_eq!(regime, MarketRegime::Bear);
        assert_eq!(active.top_n, 30);
        assert_eq!(active.rebalance_freq_days, 60);
        assert_eq!(active.max_gross_exposure, 0.50);
        assert_eq!(active.score_direction, ScoreDirection::Ascending);
    }

    #[test]
    fn regime_aware_factor_signals_use_dynamic_direction_and_exposure() {
        let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let trading_days = vec![d1, d2, d3];
        let mut scores_by_date = HashMap::new();
        scores_by_date.insert(d1, vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 1.0)]);
        scores_by_date.insert(d2, vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 1.0)]);
        let base = SignalConfig {
            top_n: 1,
            rebalance_freq_days: 1,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            ..Default::default()
        };

        let signals = build_rebalance_factor_signals(
            &trading_days,
            &scores_by_date,
            &base,
            &HashMap::new(),
            &HashMap::new(),
            |day, base| {
                if day == d3 {
                    let mut defensive = base.clone();
                    defensive.max_gross_exposure = 0.50;
                    defensive.score_direction = ScoreDirection::Ascending;
                    defensive
                } else {
                    base.clone()
                }
            },
        )
        .expect("regime-aware signals");

        let bull_signal = signals.get(&d2).expect("bull signal");
        assert_eq!(bull_signal.target_weights.get("AAA"), Some(&Decimal::ONE));
        let bear_signal = signals.get(&d3).expect("bear signal");
        assert_eq!(
            bear_signal.target_weights.get("BBB"),
            Some(&Decimal::from_f64(0.50).unwrap())
        );
        assert!(!bear_signal.target_weights.contains_key("AAA"));
    }

    fn dated_returns(values: &[f64]) -> Vec<(NaiveDate, f64)> {
        let start = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        values
            .iter()
            .enumerate()
            .map(|(idx, value)| (start + chrono::Duration::days(idx as i64), *value))
            .collect()
    }
}
