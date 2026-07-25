//! Factor-based signal generator
//!
//! Converts factor combination scores from `multi_factor_value` into daily
//! `StrategySignal` objects for the backtest engine.
//!
//! Strategy: rank all stocks by combo score each rebalance day, pick top-N,
//! assign equal weight.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use chrono::{Duration, NaiveDate};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::{info, warn};

use crate::engine::StrategySignal;

mod cache_keys;
mod capacity_budget;
mod combo_reuse;
mod generation;
mod market_feature;
mod pit_alpha;
mod portfolio_construction;
mod signal_data_cache;

pub use cache_keys::*;
pub use capacity_budget::*;
pub use combo_reuse::*;
pub use generation::*;
pub use market_feature::*;
pub use pit_alpha::*;
pub use portfolio_construction::*;
pub use signal_data_cache::*;

#[cfg(test)]
mod tests;

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
    /// Portfolio notional used to translate ADV/participation limits into target weight caps.
    pub portfolio_notional_cny: Option<f64>,
    /// Optional target-weight cap derived from average trading amount and participation rate.
    pub max_participation_rate: Option<f64>,
    /// Portfolio-level capacity budget applied after initial target-weight construction.
    pub capacity_risk_budget_profile: CapacityRiskBudgetProfile,
    /// Candidate expansion profile used to keep target gross exposure fillable under execution caps.
    pub cash_utilization_profile: CashUtilizationProfile,
    /// Rebalance-path execution budget applied against previous target weights.
    pub execution_impact_budget_profile: ExecutionImpactBudgetProfile,
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
    /// Optional max aggregate target weight for any single industry.
    pub industry_max_weight_pct: Option<f64>,
    /// In-memory style exposure budget applied after raw portfolio weights.
    pub style_risk_budget_profile: StyleRiskBudgetProfile,
    /// Candidate-pool risk filter applied before portfolio construction.
    pub candidate_risk_filter_profile: CandidateRiskFilterProfile,
    /// Candidate ordering overlay applied before portfolio construction.
    pub candidate_ranking_profile: CandidateRankingProfile,
    /// Portfolio-level risk-contribution control applied after weights are built.
    pub risk_contribution_control_profile: RiskContributionControlProfile,
    /// Stress-fill target exposure shaping from same-day prediction/confidence score strength.
    pub stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile,
    /// Minimum absolute target-weight delta required to move a position on rebalance.
    /// 0.01 = 1 percentage point. 0 disables hysteresis.
    pub rebalance_hysteresis_pct: f64,
    /// Fraction of the target-weight gap to apply on each rebalance.
    /// 1.0 = full rebalance; 0.5 = move halfway toward the new target.
    pub partial_rebalance_ratio: f64,
    /// Optional per-trade-date score preselection size before in-memory portfolio filtering.
    /// None or 0 keeps the legacy full-universe score load.
    pub score_candidate_pool_size: Option<usize>,
    /// Tradable universe pruning profile applied while loading combo scores.
    pub universe_profile: TradableUniverseProfile,
    /// Optional persisted prediction set to blend with factor combo scores.
    pub prediction_blend: Option<PredictionBlendConfig>,
    /// Optional event-window gate applied after base score loading.
    pub event_gate: Option<EventGateConfig>,
    /// Optional regime-conditioned overlay score blended into the base combo.
    pub score_overlay: Option<FactorScoreOverlayConfig>,
    /// Optional regime-conditioned portfolio sleeve blended after portfolio construction.
    pub portfolio_sleeve: Option<FactorPortfolioSleeveConfig>,
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
    /// Portfolio notional used to translate ADV/participation limits into target weight caps.
    pub portfolio_notional_cny: Option<f64>,
    /// Optional target-weight cap derived from average trading amount and participation rate.
    pub max_participation_rate: Option<f64>,
    /// Portfolio-level capacity budget applied after initial target-weight construction.
    pub capacity_risk_budget_profile: CapacityRiskBudgetProfile,
    /// Candidate expansion profile used to keep target gross exposure fillable under execution caps.
    pub cash_utilization_profile: CashUtilizationProfile,
    /// Rebalance-path execution budget applied against previous target weights.
    pub execution_impact_budget_profile: ExecutionImpactBudgetProfile,
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
    /// Optional max aggregate target weight for any single industry.
    pub industry_max_weight_pct: Option<f64>,
    /// In-memory style exposure budget applied after raw portfolio weights.
    pub style_risk_budget_profile: StyleRiskBudgetProfile,
    /// Candidate-pool risk filter applied before portfolio construction.
    pub candidate_risk_filter_profile: CandidateRiskFilterProfile,
    /// Candidate ordering overlay applied before portfolio construction.
    pub candidate_ranking_profile: CandidateRankingProfile,
    /// Portfolio-level risk-contribution control applied after weights are built.
    pub risk_contribution_control_profile: RiskContributionControlProfile,
    /// Stress-fill target exposure shaping from same-day prediction/confidence score strength.
    pub stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile,
    /// Optional market-regime policy. When set, prediction signals can adjust parameters
    /// (top_n, rebalance, cash buffer) based on the detected market state.
    pub market_regime: Option<String>,
    /// Minimum absolute target-weight delta required to move a position on rebalance.
    pub rebalance_hysteresis_pct: f64,
    /// Fraction of the target-weight gap to apply on each rebalance.
    /// 1.0 = full rebalance; 0.5 = move halfway toward the new target.
    pub partial_rebalance_ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictionBlendConfig {
    pub prediction_set_id: String,
    pub factor_weight: f64,
    pub prediction_weight: f64,
    /// Keep only stocks whose same-day prediction percentile is at least this threshold.
    /// Percentile is computed cross-sectionally per date, with higher prediction scores better.
    pub prediction_min_percentile: Option<f64>,
    /// Keep only stocks whose raw same-day prediction score is at least this threshold.
    pub prediction_min_score: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventGateConfig {
    pub combo_name: String,
    pub version: String,
    pub mode: EventGateMode,
    pub score_direction: ScoreDirection,
    pub min_score: f64,
    pub boost_weight: f64,
    pub active_regimes: Vec<MarketRegime>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactorScoreOverlayConfig {
    pub combo_name: String,
    pub version: String,
    pub weight: f64,
    pub score_direction: ScoreDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactorPortfolioSleeveConfig {
    pub combo_name: String,
    pub version: String,
    pub weight: f64,
    pub score_direction: ScoreDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventGateMode {
    BoostPositive,
    ExcludeNegative,
    RequirePositive,
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
            portfolio_notional_cny: None,
            max_participation_rate: None,
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::Off,
            cash_utilization_profile: CashUtilizationProfile::Off,
            execution_impact_budget_profile: ExecutionImpactBudgetProfile::Off,
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
            industry_max_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
            candidate_ranking_profile: CandidateRankingProfile::Off,
            risk_contribution_control_profile: RiskContributionControlProfile::Off,
            stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile::Off,
            market_regime: None,
            rebalance_hysteresis_pct: 0.0,
            partial_rebalance_ratio: 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PredictionScoreRow {
    pub(crate) symbol: String,
    pub(crate) trade_date: NaiveDate,
    pub(crate) score: f64,
    pub(crate) rank: Option<i32>,
}

pub(crate) type FactorScoresByDate = HashMap<NaiveDate, Vec<(String, f64)>>;
pub(crate) type PredictionScoresByDate = HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>;
pub(crate) type SymbolReturnHistory = HashMap<String, Vec<(NaiveDate, f64)>>;
pub(crate) type AverageAmounts = HashMap<String, f64>;
pub(crate) type AverageAmountHistory = HashMap<String, Vec<(NaiveDate, f64)>>;
pub(crate) type AverageAmountsByDate = HashMap<NaiveDate, AverageAmounts>;
pub(crate) type IndustryMap = HashMap<String, String>;
pub(crate) type BenchmarkReturns = Vec<(NaiveDate, f64)>;

pub(crate) const PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS: usize = 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FactorScoreSourceKey {
    pub(crate) combo_name: String,
    pub(crate) version: String,
    pub(crate) score_direction: ScoreDirection,
    pub(crate) score_candidate_pool_size: Option<usize>,
    pub(crate) universe_profile: TradableUniverseProfile,
}

impl FactorScoreSourceKey {
    pub(crate) fn from_config(config: &SignalConfig) -> Self {
        Self {
            combo_name: config.combo_name.clone(),
            version: config.version.clone(),
            score_direction: config.score_direction,
            score_candidate_pool_size: normalize_score_candidate_pool_size(
                config.score_candidate_pool_size,
            ),
            universe_profile: config.universe_profile,
        }
    }
}

/// Normalize a symbol slice by sorting and de-duplicating in place.
pub(crate) fn normalized_symbol_key(symbols: &[String]) -> Vec<String> {
    let mut symbols = symbols.to_vec();
    symbols.sort();
    symbols.dedup();
    symbols
}

/// Collapse a `score_candidate_pool_size` option to `None` when it is zero so
/// callers can treat `None` and `Some(0)` identically.
pub(crate) fn normalize_score_candidate_pool_size(value: Option<usize>) -> Option<usize> {
    value.filter(|size| *size > 0)
}
