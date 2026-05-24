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
use sqlx::PgPool;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
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
    /// Minimum absolute target-weight delta required to move a position on rebalance.
    pub rebalance_hysteresis_pct: f64,
    /// Fraction of the target-weight gap to apply on each rebalance.
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
            rebalance_hysteresis_pct: 0.0,
            partial_rebalance_ratio: 1.0,
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

type FactorScoresByDate = HashMap<NaiveDate, Vec<(String, f64)>>;
type PredictionScoresByDate = HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>;
type SymbolReturnHistory = HashMap<String, Vec<(NaiveDate, f64)>>;
type AverageAmounts = HashMap<String, f64>;
type AverageAmountHistory = HashMap<String, Vec<(NaiveDate, f64)>>;
type AverageAmountsByDate = HashMap<NaiveDate, AverageAmounts>;
type IndustryMap = HashMap<String, String>;
type BenchmarkReturns = Vec<(NaiveDate, f64)>;

const PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS: usize = 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FactorScoreSourceKey {
    combo_name: String,
    version: String,
    score_direction: ScoreDirection,
    score_candidate_pool_size: Option<usize>,
    universe_profile: TradableUniverseProfile,
}

impl FactorScoreSourceKey {
    fn from_config(config: &SignalConfig) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SignalDataCacheKey {
    ComboScores {
        combo_name: String,
        version: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
        score_direction: Option<ScoreDirection>,
        score_candidate_pool_size: Option<usize>,
        universe_profile: TradableUniverseProfile,
    },
    TradingDays {
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    ReturnHistorySymbol {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    },
    AverageAmountSymbol {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    AverageAmountHistorySymbol {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    },
    PredictionScores {
        prediction_set_id: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    IndustryClassifications {
        symbols: Vec<String>,
    },
    BenchmarkReturns {
        benchmark: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    },
}

impl SignalDataCacheKey {
    fn combo_scores(
        combo_name: &str,
        version: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        score_direction: ScoreDirection,
        score_candidate_pool_size: Option<usize>,
        universe_profile: TradableUniverseProfile,
    ) -> Self {
        let score_candidate_pool_size =
            normalize_score_candidate_pool_size(score_candidate_pool_size);
        Self::ComboScores {
            combo_name: combo_name.to_string(),
            version: version.to_string(),
            start_date,
            end_date,
            score_direction: score_candidate_pool_size.map(|_| score_direction),
            score_candidate_pool_size,
            universe_profile,
        }
    }

    fn trading_days(start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::TradingDays {
            start_date,
            end_date,
        }
    }

    fn return_history_symbol(
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Self {
        Self::ReturnHistorySymbol {
            symbol: symbol.to_string(),
            start_date,
            end_date,
            lookback_days,
        }
    }

    fn average_amount_symbol(symbol: &str, start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::AverageAmountSymbol {
            symbol: symbol.to_string(),
            start_date,
            end_date,
        }
    }

    fn average_amount_history_symbol(
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Self {
        Self::AverageAmountHistorySymbol {
            symbol: symbol.to_string(),
            start_date,
            end_date,
            lookback_days,
        }
    }

    fn prediction_scores(
        prediction_set_id: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Self {
        Self::PredictionScores {
            prediction_set_id: prediction_set_id.to_string(),
            start_date,
            end_date,
        }
    }

    fn industry_classifications(symbols: &[String]) -> Self {
        Self::IndustryClassifications {
            symbols: normalized_symbol_key(symbols),
        }
    }

    fn benchmark_returns(
        benchmark: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Self {
        Self::BenchmarkReturns {
            benchmark: benchmark.to_string(),
            start_date,
            end_date,
            lookback_days,
        }
    }
}

fn normalized_symbol_key(symbols: &[String]) -> Vec<String> {
    let mut symbols = symbols.to_vec();
    symbols.sort();
    symbols.dedup();
    symbols
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SignalDataCacheStats {
    pub combo_score_hits: usize,
    pub combo_score_misses: usize,
    pub trading_day_hits: usize,
    pub trading_day_misses: usize,
    pub return_history_hits: usize,
    pub return_history_covering_window_hits: usize,
    pub return_history_snapshot_hits: usize,
    pub return_history_misses: usize,
    pub average_amount_hits: usize,
    pub average_amount_symbol_hits: usize,
    pub average_amount_history_hits: usize,
    pub average_amount_history_covering_window_hits: usize,
    pub average_amount_history_snapshot_hits: usize,
    pub average_amount_misses: usize,
    pub average_amount_symbol_misses: usize,
    pub average_amount_history_misses: usize,
    pub prediction_score_hits: usize,
    pub prediction_score_misses: usize,
    pub industry_classification_hits: usize,
    pub industry_classification_misses: usize,
    pub benchmark_return_hits: usize,
    pub benchmark_return_misses: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketFeatureSnapshotKey {
    pub data_version_id: String,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub lookback_days: usize,
    pub universe_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeaturePrewarmReport {
    pub snapshot_key: MarketFeatureSnapshotKey,
    pub feature_start: NaiveDate,
    pub feature_end: NaiveDate,
    pub symbol_count: usize,
    pub lookback_days: usize,
    pub cache_delta: SignalDataCacheStats,
}

#[derive(Debug, Clone)]
pub struct FactorSignalFeaturePrewarmSpec {
    pub data_version_id: String,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub feature_start: NaiveDate,
    pub feature_end: NaiveDate,
    pub config: SignalConfig,
    pub regime_policy: Option<MarketRegimePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorSignalBatchPrewarmReport {
    pub requested_specs: usize,
    pub candidate_specs: usize,
    pub skipped_empty_specs: usize,
    pub unique_feature_groups: usize,
    pub total_symbol_count: usize,
    pub cache_delta: SignalDataCacheStats,
    pub groups: Vec<MarketFeaturePrewarmReport>,
}

#[derive(Debug, Clone)]
struct FactorSignalFeaturePrewarmCandidate {
    data_version_id: String,
    train_start: NaiveDate,
    train_end: NaiveDate,
    test_start: NaiveDate,
    test_end: NaiveDate,
    feature_start: NaiveDate,
    feature_end: NaiveDate,
    lookback_days: usize,
    symbols: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FactorSignalFeaturePrewarmGroupKey {
    data_version_id: String,
    train_start: NaiveDate,
    train_end: NaiveDate,
    test_start: NaiveDate,
    test_end: NaiveDate,
    feature_start: NaiveDate,
    feature_end: NaiveDate,
    lookback_days: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FactorSignalFeaturePrewarmGroup {
    key: FactorSignalFeaturePrewarmGroupKey,
    symbols: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MarketFeatureSnapshotScope {
    pub data_version_id: String,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
}

impl MarketFeatureSnapshotScope {
    pub fn new(
        data_version_id: impl AsRef<str>,
        train_start: NaiveDate,
        train_end: NaiveDate,
        test_start: NaiveDate,
        test_end: NaiveDate,
    ) -> Self {
        Self {
            data_version_id: data_version_id.as_ref().trim().to_string(),
            train_start,
            train_end,
            test_start,
            test_end,
        }
    }

    fn snapshot_key(&self, lookback_days: usize, symbols: &[String]) -> MarketFeatureSnapshotKey {
        MarketFeatureSnapshotKey::new(
            &self.data_version_id,
            self.train_start,
            self.train_end,
            self.test_start,
            self.test_end,
            lookback_days,
            symbols,
        )
    }
}

#[derive(Debug, Clone)]
struct MarketFeatureSnapshot {
    key: MarketFeatureSnapshotKey,
    return_lookback_days: usize,
    amount_lookback_days: usize,
    return_history: Arc<SymbolReturnHistory>,
    average_amount_history: Arc<AverageAmountHistory>,
}

impl MarketFeatureSnapshot {
    fn return_feature_start(&self) -> NaiveDate {
        return_history_query_start(self.key.train_start, self.return_lookback_days)
    }

    fn amount_feature_start(&self) -> NaiveDate {
        average_amount_history_query_start(self.key.train_start, self.amount_lookback_days)
    }

    fn feature_end(&self) -> NaiveDate {
        self.key.feature_end()
    }
}

impl MarketFeatureSnapshotKey {
    pub fn new(
        data_version_id: impl AsRef<str>,
        train_start: NaiveDate,
        train_end: NaiveDate,
        test_start: NaiveDate,
        test_end: NaiveDate,
        lookback_days: usize,
        symbols: &[String],
    ) -> Self {
        Self {
            data_version_id: data_version_id.as_ref().trim().to_string(),
            train_start,
            train_end,
            test_start,
            test_end,
            lookback_days: lookback_days.max(1),
            universe_hash: symbol_universe_hash(symbols),
        }
    }

    pub fn feature_start(&self) -> NaiveDate {
        return_history_query_start(self.train_start, self.lookback_days).min(
            average_amount_history_query_start(self.train_start, self.lookback_days),
        )
    }

    pub fn feature_end(&self) -> NaiveDate {
        self.test_end
    }
}

pub fn symbol_universe_hash(symbols: &[String]) -> String {
    let symbols = normalized_symbol_key(symbols);
    let mut hasher = DefaultHasher::new();
    symbols.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub fn signal_cache_stats_delta(
    before: SignalDataCacheStats,
    after: SignalDataCacheStats,
) -> SignalDataCacheStats {
    SignalDataCacheStats {
        combo_score_hits: after
            .combo_score_hits
            .saturating_sub(before.combo_score_hits),
        combo_score_misses: after
            .combo_score_misses
            .saturating_sub(before.combo_score_misses),
        trading_day_hits: after
            .trading_day_hits
            .saturating_sub(before.trading_day_hits),
        trading_day_misses: after
            .trading_day_misses
            .saturating_sub(before.trading_day_misses),
        return_history_hits: after
            .return_history_hits
            .saturating_sub(before.return_history_hits),
        return_history_covering_window_hits: after
            .return_history_covering_window_hits
            .saturating_sub(before.return_history_covering_window_hits),
        return_history_snapshot_hits: after
            .return_history_snapshot_hits
            .saturating_sub(before.return_history_snapshot_hits),
        return_history_misses: after
            .return_history_misses
            .saturating_sub(before.return_history_misses),
        average_amount_hits: after
            .average_amount_hits
            .saturating_sub(before.average_amount_hits),
        average_amount_symbol_hits: after
            .average_amount_symbol_hits
            .saturating_sub(before.average_amount_symbol_hits),
        average_amount_history_hits: after
            .average_amount_history_hits
            .saturating_sub(before.average_amount_history_hits),
        average_amount_history_covering_window_hits: after
            .average_amount_history_covering_window_hits
            .saturating_sub(before.average_amount_history_covering_window_hits),
        average_amount_history_snapshot_hits: after
            .average_amount_history_snapshot_hits
            .saturating_sub(before.average_amount_history_snapshot_hits),
        average_amount_misses: after
            .average_amount_misses
            .saturating_sub(before.average_amount_misses),
        average_amount_symbol_misses: after
            .average_amount_symbol_misses
            .saturating_sub(before.average_amount_symbol_misses),
        average_amount_history_misses: after
            .average_amount_history_misses
            .saturating_sub(before.average_amount_history_misses),
        prediction_score_hits: after
            .prediction_score_hits
            .saturating_sub(before.prediction_score_hits),
        prediction_score_misses: after
            .prediction_score_misses
            .saturating_sub(before.prediction_score_misses),
        industry_classification_hits: after
            .industry_classification_hits
            .saturating_sub(before.industry_classification_hits),
        industry_classification_misses: after
            .industry_classification_misses
            .saturating_sub(before.industry_classification_misses),
        benchmark_return_hits: after
            .benchmark_return_hits
            .saturating_sub(before.benchmark_return_hits),
        benchmark_return_misses: after
            .benchmark_return_misses
            .saturating_sub(before.benchmark_return_misses),
    }
}

pub async fn prewarm_market_feature_cache(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: &str,
    train_start: NaiveDate,
    train_end: NaiveDate,
    test_start: NaiveDate,
    test_end: NaiveDate,
    feature_start: NaiveDate,
    feature_end: NaiveDate,
    lookback_days: usize,
    symbols: &[String],
) -> Result<MarketFeaturePrewarmReport, String> {
    let symbols = normalized_symbol_key(symbols);
    let lookback_days = lookback_days.max(1);
    let snapshot_key = MarketFeatureSnapshotKey::new(
        data_version_id,
        train_start,
        train_end,
        test_start,
        test_end,
        lookback_days,
        &symbols,
    );
    let before = cache.stats();
    let _ = load_open_trading_days_cached(pool, cache, feature_start, feature_end).await?;
    let return_history = load_symbol_return_history_cached(
        pool,
        cache,
        &symbols,
        feature_start,
        feature_end,
        lookback_days,
    )
    .await?;
    let average_amount_history = load_average_amount_history_cached(
        pool,
        cache,
        &symbols,
        feature_start,
        feature_end,
        PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
    )
    .await?;
    let _ = load_average_amounts_cached(pool, cache, &symbols, feature_start, feature_end).await?;
    cache.insert_market_feature_snapshot(
        snapshot_key.clone(),
        lookback_days,
        PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        return_history.as_ref().clone(),
        average_amount_history.as_ref().clone(),
    );
    let after = cache.stats();

    Ok(MarketFeaturePrewarmReport {
        snapshot_key,
        feature_start,
        feature_end,
        symbol_count: symbols.len(),
        lookback_days,
        cache_delta: signal_cache_stats_delta(before, after),
    })
}

pub async fn prewarm_factor_signal_batch_feature_cache(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    specs: &[FactorSignalFeaturePrewarmSpec],
) -> Result<FactorSignalBatchPrewarmReport, String> {
    let before = cache.stats();
    let mut candidates = Vec::with_capacity(specs.len());
    for spec in specs {
        candidates.push(load_factor_signal_feature_prewarm_candidate(pool, cache, spec).await?);
    }
    let candidate_specs = candidates
        .iter()
        .filter(|candidate| !candidate.symbols.is_empty())
        .count();
    let skipped_empty_specs = specs.len().saturating_sub(candidate_specs);
    let groups = merge_factor_signal_feature_prewarm_groups(candidates);
    let mut reports = Vec::with_capacity(groups.len());
    for group in groups {
        let key = group.key;
        reports.push(
            prewarm_market_feature_cache(
                pool,
                cache,
                &key.data_version_id,
                key.train_start,
                key.train_end,
                key.test_start,
                key.test_end,
                key.feature_start,
                key.feature_end,
                key.lookback_days,
                &group.symbols,
            )
            .await?,
        );
    }
    let after = cache.stats();
    let total_symbol_count = reports.iter().map(|report| report.symbol_count).sum();

    Ok(FactorSignalBatchPrewarmReport {
        requested_specs: specs.len(),
        candidate_specs,
        skipped_empty_specs,
        unique_feature_groups: reports.len(),
        total_symbol_count,
        cache_delta: signal_cache_stats_delta(before, after),
        groups: reports,
    })
}

async fn load_factor_signal_feature_prewarm_candidate(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    spec: &FactorSignalFeaturePrewarmSpec,
) -> Result<FactorSignalFeaturePrewarmCandidate, String> {
    let trading_days =
        load_open_trading_days_cached(pool, cache, spec.feature_start, spec.feature_end).await?;
    let portfolio_config = PortfolioConstructionConfig::from(&spec.config);
    let (symbols, lookback_days) = if let Some(policy) = spec.regime_policy.as_ref() {
        let max_lookback =
            portfolio_history_lookback_days(&portfolio_config).max(policy.lookback_days);
        let benchmark_returns = load_benchmark_return_history_cached(
            pool,
            cache,
            &policy.benchmark,
            spec.feature_start,
            spec.feature_end,
            max_lookback,
        )
        .await?;
        let score_days = rebalance_score_days(trading_days.as_ref(), &spec.config, |day, base| {
            let returns =
                trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
            let regime = classify_market_regime(&returns, policy);
            policy.apply(base, regime)
        });
        let score_source_configs = regime_score_source_configs(&spec.config, policy);
        let mut score_sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
        for source_config in score_source_configs {
            let key = FactorScoreSourceKey::from_config(&source_config);
            if score_sources.contains_key(&key) {
                continue;
            }
            let scores =
                load_regime_base_scores_for_dates_cached(pool, cache, &source_config, &score_days)
                    .await?;
            score_sources.insert(key, scores);
        }
        if let Some(event_gate) = spec
            .config
            .event_gate
            .as_ref()
            .filter(|gate| !gate.active_regimes.is_empty())
        {
            let event_scores =
                load_event_gate_scores_for_dates_cached(pool, cache, event_gate, &score_days)
                    .await?;
            for scores in score_sources.values_mut() {
                apply_event_gate_scores_for_regime(
                    scores,
                    event_scores.as_ref(),
                    event_gate,
                    |day| {
                        let returns = trailing_market_returns(
                            benchmark_returns.as_ref(),
                            day,
                            policy.lookback_days,
                        );
                        classify_market_regime(&returns, policy)
                    },
                );
            }
        }
        let symbols = score_sources
            .values()
            .flat_map(|scores_by_date| symbols_from_factor_scores(scores_by_date))
            .collect::<Vec<_>>();
        (symbols, max_lookback)
    } else {
        let score_days = rebalance_score_days(trading_days.as_ref(), &spec.config, |_day, base| {
            base.clone()
        });
        let scores =
            load_regime_base_scores_for_dates_cached(pool, cache, &spec.config, &score_days)
                .await?;
        let lookback_days = portfolio_history_lookback_days(&portfolio_config);
        (symbols_from_factor_scores(&scores), lookback_days)
    };

    Ok(FactorSignalFeaturePrewarmCandidate {
        data_version_id: spec.data_version_id.clone(),
        train_start: spec.train_start,
        train_end: spec.train_end,
        test_start: spec.test_start,
        test_end: spec.test_end,
        feature_start: spec.feature_start,
        feature_end: spec.feature_end,
        lookback_days,
        symbols,
    })
}

fn symbols_from_factor_scores(scores: &FactorScoresByDate) -> Vec<String> {
    let symbols = scores
        .values()
        .flat_map(|rows| rows.iter().map(|(symbol, _)| symbol.clone()))
        .collect::<Vec<_>>();
    normalized_symbol_key(&symbols)
}

fn merge_factor_signal_feature_prewarm_groups(
    candidates: Vec<FactorSignalFeaturePrewarmCandidate>,
) -> Vec<FactorSignalFeaturePrewarmGroup> {
    let mut groups: BTreeMap<FactorSignalFeaturePrewarmGroupKey, BTreeSet<String>> =
        BTreeMap::new();
    for candidate in candidates {
        if candidate.symbols.is_empty() {
            continue;
        }
        let key = FactorSignalFeaturePrewarmGroupKey {
            data_version_id: candidate.data_version_id,
            train_start: candidate.train_start,
            train_end: candidate.train_end,
            test_start: candidate.test_start,
            test_end: candidate.test_end,
            feature_start: candidate.feature_start,
            feature_end: candidate.feature_end,
            lookback_days: candidate.lookback_days.max(1),
        };
        groups
            .entry(key)
            .or_default()
            .extend(candidate.symbols.into_iter());
    }

    groups
        .into_iter()
        .map(|(key, symbols)| FactorSignalFeaturePrewarmGroup {
            key,
            symbols: symbols.into_iter().collect(),
        })
        .collect()
}

#[derive(Debug, Default)]
pub struct SignalDataCache {
    combo_scores: HashMap<SignalDataCacheKey, Arc<FactorScoresByDate>>,
    trading_days: HashMap<SignalDataCacheKey, Arc<Vec<NaiveDate>>>,
    return_history: HashMap<SignalDataCacheKey, Arc<SymbolReturnHistory>>,
    average_amounts: HashMap<SignalDataCacheKey, Arc<AverageAmounts>>,
    average_amount_history: HashMap<SignalDataCacheKey, Arc<AverageAmountHistory>>,
    market_feature_snapshots: HashMap<MarketFeatureSnapshotKey, Arc<MarketFeatureSnapshot>>,
    prediction_scores: HashMap<SignalDataCacheKey, Arc<PredictionScoresByDate>>,
    industry_classifications: HashMap<SignalDataCacheKey, Arc<IndustryMap>>,
    benchmark_returns: HashMap<SignalDataCacheKey, Arc<BenchmarkReturns>>,
    stats: SignalDataCacheStats,
}

#[derive(Debug, Clone, Default)]
pub struct SignalDataCacheSnapshot {
    combo_scores: HashMap<SignalDataCacheKey, Arc<FactorScoresByDate>>,
    trading_days: HashMap<SignalDataCacheKey, Arc<Vec<NaiveDate>>>,
    return_history: HashMap<SignalDataCacheKey, Arc<SymbolReturnHistory>>,
    average_amounts: HashMap<SignalDataCacheKey, Arc<AverageAmounts>>,
    average_amount_history: HashMap<SignalDataCacheKey, Arc<AverageAmountHistory>>,
    market_feature_snapshots: HashMap<MarketFeatureSnapshotKey, Arc<MarketFeatureSnapshot>>,
    prediction_scores: HashMap<SignalDataCacheKey, Arc<PredictionScoresByDate>>,
    industry_classifications: HashMap<SignalDataCacheKey, Arc<IndustryMap>>,
    benchmark_returns: HashMap<SignalDataCacheKey, Arc<BenchmarkReturns>>,
}

impl SignalDataCache {
    pub fn stats(&self) -> SignalDataCacheStats {
        self.stats
    }

    pub fn snapshot(&self) -> SignalDataCacheSnapshot {
        SignalDataCacheSnapshot {
            combo_scores: self.combo_scores.clone(),
            trading_days: self.trading_days.clone(),
            return_history: self.return_history.clone(),
            average_amounts: self.average_amounts.clone(),
            average_amount_history: self.average_amount_history.clone(),
            market_feature_snapshots: self.market_feature_snapshots.clone(),
            prediction_scores: self.prediction_scores.clone(),
            industry_classifications: self.industry_classifications.clone(),
            benchmark_returns: self.benchmark_returns.clone(),
        }
    }

    pub fn from_snapshot(snapshot: &SignalDataCacheSnapshot) -> Self {
        Self {
            combo_scores: snapshot.combo_scores.clone(),
            trading_days: snapshot.trading_days.clone(),
            return_history: snapshot.return_history.clone(),
            average_amounts: snapshot.average_amounts.clone(),
            average_amount_history: snapshot.average_amount_history.clone(),
            market_feature_snapshots: snapshot.market_feature_snapshots.clone(),
            prediction_scores: snapshot.prediction_scores.clone(),
            industry_classifications: snapshot.industry_classifications.clone(),
            benchmark_returns: snapshot.benchmark_returns.clone(),
            stats: SignalDataCacheStats::default(),
        }
    }

    pub(crate) fn cached_combo_scores(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<FactorScoresByDate>> {
        match self.combo_scores.get(key) {
            Some(value) => {
                self.stats.combo_score_hits += 1;
                Some(Arc::clone(value))
            }
            None => self.cached_combo_scores_from_larger_candidate_pool(key),
        }
    }

    fn cached_combo_scores_from_larger_candidate_pool(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<FactorScoresByDate>> {
        let Some((reuse_plan, source_scores)) = self
            .combo_scores
            .iter()
            .filter_map(|(candidate_key, scores)| {
                reusable_combo_candidate_pool(candidate_key, key)
                    .map(|reuse_plan| (reuse_plan, Arc::clone(scores)))
            })
            .min_by_key(|(reuse_plan, _)| reuse_plan.source_span_days())
        else {
            self.stats.combo_score_misses += 1;
            return None;
        };

        let pruned = match reuse_plan {
            ComboScoreReusePlan::RankedPool {
                requested_size,
                requested_start,
                requested_end,
                ..
            } => {
                let windowed = filter_factor_scores_by_date(
                    source_scores.as_ref(),
                    requested_start,
                    requested_end,
                );
                prune_factor_scores_by_date(&windowed, requested_size)
            }
            ComboScoreReusePlan::UnboundedPool {
                requested_size,
                score_direction,
                requested_start,
                requested_end,
                ..
            } => {
                let windowed = filter_factor_scores_by_date(
                    source_scores.as_ref(),
                    requested_start,
                    requested_end,
                );
                rank_and_prune_factor_scores_by_date(&windowed, requested_size, score_direction)
            }
            ComboScoreReusePlan::FullPool {
                requested_start,
                requested_end,
                ..
            } => {
                filter_factor_scores_by_date(source_scores.as_ref(), requested_start, requested_end)
            }
        };
        let pruned = self.insert_combo_scores(key.clone(), pruned);
        self.stats.combo_score_hits += 1;
        Some(pruned)
    }

    fn insert_combo_scores(
        &mut self,
        key: SignalDataCacheKey,
        value: FactorScoresByDate,
    ) -> Arc<FactorScoresByDate> {
        let value = Arc::new(value);
        self.combo_scores.insert(key, Arc::clone(&value));
        value
    }

    fn cached_trading_days(&mut self, key: &SignalDataCacheKey) -> Option<Arc<Vec<NaiveDate>>> {
        match self.trading_days.get(key) {
            Some(value) => {
                self.stats.trading_day_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.trading_day_misses += 1;
                None
            }
        }
    }

    fn insert_trading_days(
        &mut self,
        key: SignalDataCacheKey,
        value: Vec<NaiveDate>,
    ) -> Arc<Vec<NaiveDate>> {
        let value = Arc::new(value);
        self.trading_days.insert(key, Arc::clone(&value));
        value
    }

    fn insert_market_feature_snapshot(
        &mut self,
        key: MarketFeatureSnapshotKey,
        return_lookback_days: usize,
        amount_lookback_days: usize,
        return_history: SymbolReturnHistory,
        average_amount_history: AverageAmountHistory,
    ) {
        self.market_feature_snapshots.insert(
            key.clone(),
            Arc::new(MarketFeatureSnapshot {
                key,
                return_lookback_days: return_lookback_days.max(1),
                amount_lookback_days: amount_lookback_days.max(1),
                return_history: Arc::new(return_history),
                average_amount_history: Arc::new(average_amount_history),
            }),
        );
    }

    fn insert_market_feature_snapshot_from_cached_histories(
        &mut self,
        key: MarketFeatureSnapshotKey,
        return_lookback_days: usize,
        amount_lookback_days: usize,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) {
        let mut return_history = HashMap::new();
        let mut average_amount_history = HashMap::new();

        for symbol in normalized_symbol_key(symbols) {
            let return_key = SignalDataCacheKey::return_history_symbol(
                &symbol,
                start_date,
                end_date,
                return_lookback_days,
            );
            if let Some(rows) = self
                .return_history
                .get(&return_key)
                .and_then(|history| history.get(&symbol))
                .cloned()
            {
                return_history.insert(symbol.clone(), rows);
            }

            let amount_key = SignalDataCacheKey::average_amount_history_symbol(
                &symbol,
                start_date,
                end_date,
                amount_lookback_days,
            );
            if let Some(rows) = self
                .average_amount_history
                .get(&amount_key)
                .and_then(|history| history.get(&symbol))
                .cloned()
            {
                average_amount_history.insert(symbol, rows);
            }
        }

        if return_history.is_empty() && average_amount_history.is_empty() {
            return;
        }

        self.insert_market_feature_snapshot(
            key,
            return_lookback_days,
            amount_lookback_days,
            return_history,
            average_amount_history,
        );
    }

    fn cached_return_history_symbols(
        &mut self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> (Vec<String>, SymbolReturnHistory) {
        let mut missing_symbols = Vec::new();
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(symbols) {
            let key = SignalDataCacheKey::return_history_symbol(
                &symbol,
                start_date,
                end_date,
                lookback_days,
            );
            match self.return_history.get(&key) {
                Some(value) => {
                    self.stats.return_history_hits += 1;
                    let rows = value.as_ref().get(&symbol).cloned().unwrap_or_default();
                    result.insert(symbol, rows);
                }
                None => {
                    if let Some(rows) = self.cached_return_history_from_covering_window(
                        &symbol,
                        start_date,
                        end_date,
                        lookback_days,
                    ) {
                        self.stats.return_history_hits += 1;
                        self.stats.return_history_covering_window_hits += 1;
                        result.insert(symbol, rows);
                    } else if let Some(rows) = self
                        .cached_return_history_from_market_feature_snapshot(
                            &symbol,
                            start_date,
                            end_date,
                            lookback_days,
                        )
                    {
                        self.stats.return_history_hits += 1;
                        self.stats.return_history_snapshot_hits += 1;
                        result.insert(symbol, rows);
                    } else {
                        self.stats.return_history_misses += 1;
                        missing_symbols.push(symbol);
                    }
                }
            }
        }
        (missing_symbols, result)
    }

    fn cached_return_history_from_covering_window(
        &mut self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Option<Vec<(NaiveDate, f64)>> {
        let source = self
            .return_history
            .iter()
            .filter_map(|(candidate_key, history)| {
                let SignalDataCacheKey::ReturnHistorySymbol {
                    symbol: candidate_symbol,
                    start_date: candidate_start,
                    end_date: candidate_end,
                    lookback_days: candidate_lookback_days,
                } = candidate_key
                else {
                    return None;
                };
                if candidate_symbol == symbol
                    && *candidate_lookback_days == lookback_days
                    && *candidate_start <= start_date
                    && *candidate_end >= end_date
                {
                    let span_days = candidate_end
                        .signed_duration_since(*candidate_start)
                        .num_days();
                    Some((span_days, Arc::clone(history)))
                } else {
                    None
                }
            })
            .min_by_key(|(span_days, _)| *span_days)
            .map(|(_, history)| history)?;

        let query_start = return_history_query_start(start_date, lookback_days);
        let rows = source
            .as_ref()
            .get(symbol)
            .map(|rows| filter_dated_values(rows, query_start, end_date))
            .unwrap_or_default();
        let mut symbol_history = HashMap::new();
        symbol_history.insert(symbol.to_string(), rows.clone());
        let key =
            SignalDataCacheKey::return_history_symbol(symbol, start_date, end_date, lookback_days);
        self.return_history.insert(key, Arc::new(symbol_history));
        Some(rows)
    }

    fn cached_return_history_from_market_feature_snapshot(
        &mut self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Option<Vec<(NaiveDate, f64)>> {
        let query_start = return_history_query_start(start_date, lookback_days);
        let source = self
            .market_feature_snapshots
            .values()
            .filter(|snapshot| {
                snapshot.return_lookback_days == lookback_days
                    && snapshot.return_feature_start() <= query_start
                    && snapshot.feature_end() >= end_date
                    && snapshot.return_history.contains_key(symbol)
            })
            .min_by_key(|snapshot| {
                snapshot
                    .feature_end()
                    .signed_duration_since(snapshot.return_feature_start())
                    .num_days()
            })
            .cloned()?;

        let rows = source
            .return_history
            .get(symbol)
            .map(|rows| filter_dated_values(rows, query_start, end_date))
            .unwrap_or_default();
        let mut symbol_history = HashMap::new();
        symbol_history.insert(symbol.to_string(), rows.clone());
        let key =
            SignalDataCacheKey::return_history_symbol(symbol, start_date, end_date, lookback_days);
        self.return_history.insert(key, Arc::new(symbol_history));
        Some(rows)
    }

    fn insert_return_history_symbols(
        &mut self,
        requested_symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        mut value: SymbolReturnHistory,
    ) -> SymbolReturnHistory {
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(requested_symbols) {
            let rows = value.remove(&symbol).unwrap_or_default();
            result.insert(symbol.clone(), rows.clone());
            let mut symbol_history = HashMap::new();
            symbol_history.insert(symbol.clone(), rows);
            let key = SignalDataCacheKey::return_history_symbol(
                &symbol,
                start_date,
                end_date,
                lookback_days,
            );
            self.return_history.insert(key, Arc::new(symbol_history));
        }
        result
    }

    fn cached_average_amount_symbols(
        &mut self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> (Vec<String>, AverageAmounts) {
        let mut missing_symbols = Vec::new();
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(symbols) {
            let key = SignalDataCacheKey::average_amount_symbol(&symbol, start_date, end_date);
            match self.average_amounts.get(&key) {
                Some(value) => {
                    self.stats.average_amount_hits += 1;
                    self.stats.average_amount_symbol_hits += 1;
                    if let Some(amount) = value.as_ref().get(&symbol).copied() {
                        result.insert(symbol, amount);
                    }
                }
                None => {
                    self.stats.average_amount_misses += 1;
                    self.stats.average_amount_symbol_misses += 1;
                    missing_symbols.push(symbol);
                }
            }
        }
        (missing_symbols, result)
    }

    fn insert_average_amount_symbols(
        &mut self,
        requested_symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        mut value: AverageAmounts,
    ) -> AverageAmounts {
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(requested_symbols) {
            let amount = value.remove(&symbol);
            if let Some(amount) = amount {
                result.insert(symbol.clone(), amount);
            }
            let mut symbol_amount = HashMap::new();
            if let Some(amount) = amount {
                symbol_amount.insert(symbol.clone(), amount);
            }
            let key = SignalDataCacheKey::average_amount_symbol(&symbol, start_date, end_date);
            self.average_amounts.insert(key, Arc::new(symbol_amount));
        }
        result
    }

    fn cached_average_amount_history_symbols(
        &mut self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> (Vec<String>, AverageAmountHistory) {
        let mut missing_symbols = Vec::new();
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(symbols) {
            let key = SignalDataCacheKey::average_amount_history_symbol(
                &symbol,
                start_date,
                end_date,
                lookback_days,
            );
            match self.average_amount_history.get(&key) {
                Some(value) => {
                    self.stats.average_amount_hits += 1;
                    self.stats.average_amount_history_hits += 1;
                    let rows = value.as_ref().get(&symbol).cloned().unwrap_or_default();
                    result.insert(symbol, rows);
                }
                None => {
                    if let Some(rows) = self.cached_average_amount_history_from_covering_window(
                        &symbol,
                        start_date,
                        end_date,
                        lookback_days,
                    ) {
                        self.stats.average_amount_hits += 1;
                        self.stats.average_amount_history_hits += 1;
                        self.stats.average_amount_history_covering_window_hits += 1;
                        result.insert(symbol, rows);
                    } else if let Some(rows) = self
                        .cached_average_amount_history_from_market_feature_snapshot(
                            &symbol,
                            start_date,
                            end_date,
                            lookback_days,
                        )
                    {
                        self.stats.average_amount_hits += 1;
                        self.stats.average_amount_history_hits += 1;
                        self.stats.average_amount_history_snapshot_hits += 1;
                        result.insert(symbol, rows);
                    } else {
                        self.stats.average_amount_misses += 1;
                        self.stats.average_amount_history_misses += 1;
                        missing_symbols.push(symbol);
                    }
                }
            }
        }
        (missing_symbols, result)
    }

    fn cached_average_amount_history_from_covering_window(
        &mut self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Option<Vec<(NaiveDate, f64)>> {
        let source = self
            .average_amount_history
            .iter()
            .filter_map(|(candidate_key, history)| {
                let SignalDataCacheKey::AverageAmountHistorySymbol {
                    symbol: candidate_symbol,
                    start_date: candidate_start,
                    end_date: candidate_end,
                    lookback_days: candidate_lookback_days,
                } = candidate_key
                else {
                    return None;
                };
                if candidate_symbol == symbol
                    && *candidate_lookback_days == lookback_days
                    && *candidate_start <= start_date
                    && *candidate_end >= end_date
                {
                    let span_days = candidate_end
                        .signed_duration_since(*candidate_start)
                        .num_days();
                    Some((span_days, Arc::clone(history)))
                } else {
                    None
                }
            })
            .min_by_key(|(span_days, _)| *span_days)
            .map(|(_, history)| history)?;

        let query_start = average_amount_history_query_start(start_date, lookback_days);
        let rows = source
            .as_ref()
            .get(symbol)
            .map(|rows| filter_dated_values(rows, query_start, end_date))
            .unwrap_or_default();
        let mut symbol_history = HashMap::new();
        symbol_history.insert(symbol.to_string(), rows.clone());
        let key = SignalDataCacheKey::average_amount_history_symbol(
            symbol,
            start_date,
            end_date,
            lookback_days,
        );
        self.average_amount_history
            .insert(key, Arc::new(symbol_history));
        Some(rows)
    }

    fn cached_average_amount_history_from_market_feature_snapshot(
        &mut self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Option<Vec<(NaiveDate, f64)>> {
        let query_start = average_amount_history_query_start(start_date, lookback_days);
        let source = self
            .market_feature_snapshots
            .values()
            .filter(|snapshot| {
                snapshot.amount_lookback_days == lookback_days
                    && snapshot.amount_feature_start() <= query_start
                    && snapshot.feature_end() >= end_date
                    && snapshot.average_amount_history.contains_key(symbol)
            })
            .min_by_key(|snapshot| {
                snapshot
                    .feature_end()
                    .signed_duration_since(snapshot.amount_feature_start())
                    .num_days()
            })
            .cloned()?;

        let rows = source
            .average_amount_history
            .get(symbol)
            .map(|rows| filter_dated_values(rows, query_start, end_date))
            .unwrap_or_default();
        let mut symbol_history = HashMap::new();
        symbol_history.insert(symbol.to_string(), rows.clone());
        let key = SignalDataCacheKey::average_amount_history_symbol(
            symbol,
            start_date,
            end_date,
            lookback_days,
        );
        self.average_amount_history
            .insert(key, Arc::new(symbol_history));
        Some(rows)
    }

    fn insert_average_amount_history_symbols(
        &mut self,
        requested_symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        mut value: AverageAmountHistory,
    ) -> AverageAmountHistory {
        let mut result = HashMap::new();
        for symbol in normalized_symbol_key(requested_symbols) {
            let rows = value.remove(&symbol).unwrap_or_default();
            result.insert(symbol.clone(), rows.clone());
            let mut symbol_history = HashMap::new();
            symbol_history.insert(symbol.clone(), rows);
            let key = SignalDataCacheKey::average_amount_history_symbol(
                &symbol,
                start_date,
                end_date,
                lookback_days,
            );
            self.average_amount_history
                .insert(key, Arc::new(symbol_history));
        }
        result
    }

    fn cached_prediction_scores(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<PredictionScoresByDate>> {
        match self.prediction_scores.get(key) {
            Some(value) => {
                self.stats.prediction_score_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.prediction_score_misses += 1;
                None
            }
        }
    }

    fn insert_prediction_scores(
        &mut self,
        key: SignalDataCacheKey,
        value: PredictionScoresByDate,
    ) -> Arc<PredictionScoresByDate> {
        let value = Arc::new(value);
        self.prediction_scores.insert(key, Arc::clone(&value));
        value
    }

    fn cached_industry_classifications(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<IndustryMap>> {
        match self.industry_classifications.get(key) {
            Some(value) => {
                self.stats.industry_classification_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.industry_classification_misses += 1;
                None
            }
        }
    }

    fn insert_industry_classifications(
        &mut self,
        key: SignalDataCacheKey,
        value: IndustryMap,
    ) -> Arc<IndustryMap> {
        let value = Arc::new(value);
        self.industry_classifications
            .insert(key, Arc::clone(&value));
        value
    }

    fn cached_benchmark_returns(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<BenchmarkReturns>> {
        match self.benchmark_returns.get(key) {
            Some(value) => {
                self.stats.benchmark_return_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.benchmark_return_misses += 1;
                None
            }
        }
    }

    fn insert_benchmark_returns(
        &mut self,
        key: SignalDataCacheKey,
        value: BenchmarkReturns,
    ) -> Arc<BenchmarkReturns> {
        let value = Arc::new(value);
        self.benchmark_returns.insert(key, Arc::clone(&value));
        value
    }

    #[cfg(test)]
    fn store_combo_scores_for_test(&mut self, key: SignalDataCacheKey, value: FactorScoresByDate) {
        self.combo_scores.insert(key, Arc::new(value));
    }

    #[cfg(test)]
    fn store_prediction_scores_for_test(
        &mut self,
        key: SignalDataCacheKey,
        value: PredictionScoresByDate,
    ) {
        self.prediction_scores.insert(key, Arc::new(value));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComboScoreReusePlan {
    RankedPool {
        requested_size: usize,
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
    UnboundedPool {
        requested_size: usize,
        score_direction: ScoreDirection,
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
    FullPool {
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
}

impl ComboScoreReusePlan {
    fn source_span_days(self) -> i64 {
        match self {
            Self::RankedPool {
                source_span_days, ..
            }
            | Self::UnboundedPool {
                source_span_days, ..
            }
            | Self::FullPool {
                source_span_days, ..
            } => source_span_days,
        }
    }
}

fn reusable_combo_candidate_pool(
    candidate: &SignalDataCacheKey,
    requested: &SignalDataCacheKey,
) -> Option<ComboScoreReusePlan> {
    let (
        SignalDataCacheKey::ComboScores {
            combo_name: candidate_combo_name,
            version: candidate_version,
            start_date: candidate_start_date,
            end_date: candidate_end_date,
            score_direction: candidate_score_direction,
            score_candidate_pool_size: candidate_pool_size,
            universe_profile: candidate_universe_profile,
        },
        SignalDataCacheKey::ComboScores {
            combo_name: requested_combo_name,
            version: requested_version,
            start_date: requested_start_date,
            end_date: requested_end_date,
            score_direction: requested_score_direction,
            score_candidate_pool_size: requested_pool_size,
            universe_profile: requested_universe_profile,
        },
    ) = (candidate, requested)
    else {
        return None;
    };

    let same_score_source = candidate_combo_name == requested_combo_name
        && candidate_version == requested_version
        && candidate_universe_profile == requested_universe_profile;
    if !same_score_source {
        return None;
    }

    let covers_requested_window =
        candidate_start_date <= requested_start_date && candidate_end_date >= requested_end_date;
    if !covers_requested_window {
        return None;
    }
    let source_span_days = candidate_end_date
        .signed_duration_since(*candidate_start_date)
        .num_days();

    let Some(requested_pool_size) = requested_pool_size else {
        if candidate_pool_size.is_none() {
            return Some(ComboScoreReusePlan::FullPool {
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
        return None;
    };

    if let Some(candidate_pool_size) = candidate_pool_size {
        if candidate_score_direction == requested_score_direction
            && candidate_pool_size >= requested_pool_size
        {
            return Some(ComboScoreReusePlan::RankedPool {
                requested_size: *requested_pool_size,
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
    }

    if candidate_pool_size.is_none() {
        if let Some(score_direction) = requested_score_direction {
            return Some(ComboScoreReusePlan::UnboundedPool {
                requested_size: *requested_pool_size,
                score_direction: *score_direction,
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
    }

    None
}

fn filter_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .filter_map(|(date, rows)| {
            if *date >= start_date && *date <= end_date {
                Some((*date, rows.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn prune_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    requested_size: usize,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .map(|(date, rows)| {
            (
                *date,
                rows.iter()
                    .take(requested_size)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

fn rank_and_prune_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    requested_size: usize,
    score_direction: ScoreDirection,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .map(|(date, rows)| {
            let mut ranked = rows.clone();
            ranked.sort_by(|left, right| match score_direction {
                ScoreDirection::Descending => right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0)),
                ScoreDirection::Ascending => left
                    .1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0)),
            });
            ranked.truncate(requested_size);
            (*date, ranked)
        })
        .collect()
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
            rebalance_hysteresis_pct: 0.0,
            partial_rebalance_ratio: 1.0,
            score_candidate_pool_size: None,
            universe_profile: TradableUniverseProfile::All,
            prediction_blend: None,
            event_gate: None,
            score_overlay: None,
            portfolio_sleeve: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    MinVariance,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityRiskBudgetProfile {
    #[default]
    Off,
    ParticipationBalancedV1,
    ParticipationStrictV1,
    StressParticipationSoftCapV1,
    StressParticipationTargetScaleV1,
    StressParticipationFloor35V1,
    StressParticipationFloor50V1,
    StressParticipationFloor60V1,
    StressParticipationFloor70V1,
    StressParticipationSoftFloor60V1,
    StressParticipationHeadroomFloor60V1,
    StressParticipationHeadroomFloor70V1,
    StressParticipationAlphaHeadroomFloor60V1,
    StressParticipationAlphaHeadroomFloor70V1,
    StressParticipationBlendedAlphaHeadroomFloor60V1,
    StressParticipationBlendedAlphaHeadroomFloor70V1,
}

impl CapacityRiskBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "participation_balanced_v1"
            | "participation-balanced-v1"
            | "capacity_participation_balanced_v1"
            | "capacity-participation-balanced-v1" => Ok(Self::ParticipationBalancedV1),
            "participation_strict_v1"
            | "participation-strict-v1"
            | "capacity_participation_strict_v1"
            | "capacity-participation-strict-v1" => Ok(Self::ParticipationStrictV1),
            "stress_participation_soft_cap_v1"
            | "stress-participation-soft-cap-v1"
            | "capacity_stress_participation_soft_cap_v1"
            | "capacity-stress-participation-soft-cap-v1" => Ok(Self::StressParticipationSoftCapV1),
            "stress_participation_target_scale_v1"
            | "stress-participation-target-scale-v1"
            | "capacity_stress_participation_target_scale_v1"
            | "capacity-stress-participation-target-scale-v1" => {
                Ok(Self::StressParticipationTargetScaleV1)
            }
            "stress_participation_floor_35_v1"
            | "stress-participation-floor-35-v1"
            | "capacity_stress_participation_floor_35_v1"
            | "capacity-stress-participation-floor-35-v1" => Ok(Self::StressParticipationFloor35V1),
            "stress_participation_floor_50_v1"
            | "stress-participation-floor-50-v1"
            | "capacity_stress_participation_floor_50_v1"
            | "capacity-stress-participation-floor-50-v1" => Ok(Self::StressParticipationFloor50V1),
            "stress_participation_floor_60_v1"
            | "stress-participation-floor-60-v1"
            | "capacity_stress_participation_floor_60_v1"
            | "capacity-stress-participation-floor-60-v1" => Ok(Self::StressParticipationFloor60V1),
            "stress_participation_floor_70_v1"
            | "stress-participation-floor-70-v1"
            | "capacity_stress_participation_floor_70_v1"
            | "capacity-stress-participation-floor-70-v1" => Ok(Self::StressParticipationFloor70V1),
            "stress_participation_soft_floor_60_v1"
            | "stress-participation-soft-floor-60-v1"
            | "capacity_stress_participation_soft_floor_60_v1"
            | "capacity-stress-participation-soft-floor-60-v1" => {
                Ok(Self::StressParticipationSoftFloor60V1)
            }
            "stress_participation_headroom_floor_60_v1"
            | "stress-participation-headroom-floor-60-v1"
            | "capacity_stress_participation_headroom_floor_60_v1"
            | "capacity-stress-participation-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationHeadroomFloor60V1)
            }
            "stress_participation_headroom_floor_70_v1"
            | "stress-participation-headroom-floor-70-v1"
            | "capacity_stress_participation_headroom_floor_70_v1"
            | "capacity-stress-participation-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationHeadroomFloor70V1)
            }
            "stress_participation_alpha_headroom_floor_60_v1"
            | "stress-participation-alpha-headroom-floor-60-v1"
            | "capacity_stress_participation_alpha_headroom_floor_60_v1"
            | "capacity-stress-participation-alpha-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationAlphaHeadroomFloor60V1)
            }
            "stress_participation_alpha_headroom_floor_70_v1"
            | "stress-participation-alpha-headroom-floor-70-v1"
            | "capacity_stress_participation_alpha_headroom_floor_70_v1"
            | "capacity-stress-participation-alpha-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationAlphaHeadroomFloor70V1)
            }
            "stress_participation_blended_alpha_headroom_floor_60_v1"
            | "stress-participation-blended-alpha-headroom-floor-60-v1"
            | "capacity_stress_participation_blended_alpha_headroom_floor_60_v1"
            | "capacity-stress-participation-blended-alpha-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationBlendedAlphaHeadroomFloor60V1)
            }
            "stress_participation_blended_alpha_headroom_floor_70_v1"
            | "stress-participation-blended-alpha-headroom-floor-70-v1"
            | "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
            | "capacity-stress-participation-blended-alpha-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationBlendedAlphaHeadroomFloor70V1)
            }
            other => Err(format!("unsupported capacity_risk_budget: {}", other)),
        }
    }

    fn params(self) -> Option<CapacityRiskBudgetParams> {
        match self {
            Self::Off => None,
            Self::ParticipationBalancedV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.30,
                low_capacity_max_weight_pct: 0.30,
                refill_gross_exposure: true,
                participation_cap_multiplier: 1.0,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::ParticipationStrictV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.40,
                low_capacity_max_weight_pct: 0.20,
                refill_gross_exposure: true,
                participation_cap_multiplier: 1.0,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationSoftCapV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.15,
                refill_gross_exposure: true,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationTargetScaleV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor35V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.35),
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor50V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.50),
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationSoftFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.15,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.65,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationHeadroomFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::Headroom,
            }),
            Self::StressParticipationHeadroomFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::Headroom,
            }),
            Self::StressParticipationAlphaHeadroomFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::AlphaHeadroom,
            }),
            Self::StressParticipationAlphaHeadroomFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::AlphaHeadroom,
            }),
            Self::StressParticipationBlendedAlphaHeadroomFloor60V1 => {
                Some(CapacityRiskBudgetParams {
                    low_capacity_quantile: 0.35,
                    low_capacity_max_weight_pct: 0.12,
                    refill_gross_exposure: false,
                    participation_cap_multiplier: 0.50,
                    min_target_gross_exposure_pct: Some(0.60),
                    floor_refill_cap_multiplier: 1.25,
                    floor_refill_mode: CapacityFloorRefillMode::BlendedAlphaHeadroom,
                })
            }
            Self::StressParticipationBlendedAlphaHeadroomFloor70V1 => {
                Some(CapacityRiskBudgetParams {
                    low_capacity_quantile: 0.35,
                    low_capacity_max_weight_pct: 0.12,
                    refill_gross_exposure: false,
                    participation_cap_multiplier: 0.50,
                    min_target_gross_exposure_pct: Some(0.70),
                    floor_refill_cap_multiplier: 1.50,
                    floor_refill_mode: CapacityFloorRefillMode::BlendedAlphaHeadroom,
                })
            }
        }
    }

    fn uses_capacity(self) -> bool {
        self.params().is_some()
    }
}

#[derive(Debug, Clone, Copy)]
struct CapacityRiskBudgetParams {
    low_capacity_quantile: f64,
    low_capacity_max_weight_pct: f64,
    refill_gross_exposure: bool,
    participation_cap_multiplier: f64,
    min_target_gross_exposure_pct: Option<f64>,
    floor_refill_cap_multiplier: f64,
    floor_refill_mode: CapacityFloorRefillMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityFloorRefillMode {
    ExistingWeight,
    Headroom,
    AlphaHeadroom,
    BlendedAlphaHeadroom,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CashUtilizationProfile {
    #[default]
    Off,
    FillableGross90V1,
    FillableGross95V1,
    StressFillGross98V1,
}

impl CashUtilizationProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "fillable_gross_90_v1"
            | "fillable-gross-90-v1"
            | "cash_utilization_90pct_v1"
            | "cash-utilization-90pct-v1"
            | "cash_utilization_fillable_gross_90_v1"
            | "cash-utilization-fillable-gross-90-v1" => Ok(Self::FillableGross90V1),
            "fillable_gross_95_v1"
            | "fillable-gross-95-v1"
            | "cash_utilization_95pct_v1"
            | "cash-utilization-95pct-v1"
            | "cash_utilization_fillable_gross_95_v1"
            | "cash-utilization-fillable-gross-95-v1" => Ok(Self::FillableGross95V1),
            "stress_fill_gross_98_v1"
            | "stress-fill-gross-98-v1"
            | "fillable_gross_98_v1"
            | "fillable-gross-98-v1"
            | "cash_utilization_stress_fill_gross_98_v1"
            | "cash-utilization-stress-fill-gross-98-v1" => Ok(Self::StressFillGross98V1),
            other => Err(format!("unsupported cash_utilization: {}", other)),
        }
    }

    fn params(self) -> Option<CashUtilizationParams> {
        match self {
            Self::Off => None,
            Self::FillableGross90V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.90,
                max_holdings: 50,
            }),
            Self::FillableGross95V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.95,
                max_holdings: 60,
            }),
            Self::StressFillGross98V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.98,
                max_holdings: 120,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CashUtilizationParams {
    min_gross_exposure_pct: f64,
    max_holdings: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionImpactBudgetProfile {
    #[default]
    Off,
    Turnover30PctV1,
    Turnover20PctV1,
    Turnover15PctV1,
}

impl ExecutionImpactBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "turnover_30pct_v1"
            | "turnover-30pct-v1"
            | "impact_turnover_30pct_v1"
            | "impact-turnover-30pct-v1"
            | "execution_impact_turnover_30pct_v1"
            | "execution-impact-turnover-30pct-v1" => Ok(Self::Turnover30PctV1),
            "turnover_20pct_v1"
            | "turnover-20pct-v1"
            | "impact_turnover_20pct_v1"
            | "impact-turnover-20pct-v1"
            | "execution_impact_turnover_20pct_v1"
            | "execution-impact-turnover-20pct-v1" => Ok(Self::Turnover20PctV1),
            "turnover_15pct_v1"
            | "turnover-15pct-v1"
            | "impact_turnover_15pct_v1"
            | "impact-turnover-15pct-v1"
            | "execution_impact_turnover_15pct_v1"
            | "execution-impact-turnover-15pct-v1" => Ok(Self::Turnover15PctV1),
            other => Err(format!("unsupported execution_impact_budget: {}", other)),
        }
    }

    fn params(self) -> Option<ExecutionImpactBudgetParams> {
        match self {
            Self::Off => None,
            Self::Turnover30PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.30,
                max_new_name_weight_pct: 0.06,
            }),
            Self::Turnover20PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.20,
                max_new_name_weight_pct: 0.04,
            }),
            Self::Turnover15PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.15,
                max_new_name_weight_pct: 0.03,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ExecutionImpactBudgetParams {
    max_rebalance_turnover_pct: f64,
    max_new_name_weight_pct: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StyleRiskBudgetProfile {
    #[default]
    Off,
    LiquidityVolatilityBalancedV1,
    DefensiveStyleBudgetV1,
}

impl StyleRiskBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "liquidity_volatility_balanced_v1" | "liquidity-volatility-balanced-v1" => {
                Ok(Self::LiquidityVolatilityBalancedV1)
            }
            "defensive_style_budget_v1" | "defensive-style-budget-v1" => {
                Ok(Self::DefensiveStyleBudgetV1)
            }
            other => Err(format!("unsupported style_risk_budget: {}", other)),
        }
    }

    fn params(self) -> Option<StyleRiskBudgetParams> {
        match self {
            Self::Off => None,
            Self::LiquidityVolatilityBalancedV1 => Some(StyleRiskBudgetParams {
                high_volatility_quantile: 0.70,
                high_volatility_max_weight_pct: 0.40,
                low_liquidity_quantile: 0.30,
                low_liquidity_max_weight_pct: 0.35,
            }),
            Self::DefensiveStyleBudgetV1 => Some(StyleRiskBudgetParams {
                high_volatility_quantile: 0.60,
                high_volatility_max_weight_pct: 0.30,
                low_liquidity_quantile: 0.35,
                low_liquidity_max_weight_pct: 0.30,
            }),
        }
    }

    fn uses_liquidity(self) -> bool {
        self.params()
            .map(|params| params.low_liquidity_max_weight_pct < 1.0)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Copy)]
struct StyleRiskBudgetParams {
    high_volatility_quantile: f64,
    high_volatility_max_weight_pct: f64,
    low_liquidity_quantile: f64,
    low_liquidity_max_weight_pct: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRiskFilterProfile {
    #[default]
    Off,
    LowVolatilityV1,
    LowVolatilityLowCorrelationV1,
    SoftLowVolatilityV1,
    SoftLowVolatilityLowCorrelationV1,
    SoftLiquidityLowVolatilityLowCorrelationV1,
}

impl CandidateRiskFilterProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "low_volatility_v1" | "low-volatility-v1" => Ok(Self::LowVolatilityV1),
            "low_volatility_low_correlation_v1" | "low-volatility-low-correlation-v1" => {
                Ok(Self::LowVolatilityLowCorrelationV1)
            }
            "soft_low_volatility_v1" | "soft-low-volatility-v1" => Ok(Self::SoftLowVolatilityV1),
            "soft_low_volatility_low_correlation_v1" | "soft-low-volatility-low-correlation-v1" => {
                Ok(Self::SoftLowVolatilityLowCorrelationV1)
            }
            "soft_liquidity_low_volatility_low_correlation_v1"
            | "soft-liquidity-low-volatility-low-correlation-v1" => {
                Ok(Self::SoftLiquidityLowVolatilityLowCorrelationV1)
            }
            other => Err(format!("unsupported candidate_risk_filter: {}", other)),
        }
    }

    fn params(self) -> Option<CandidateRiskFilterParams> {
        match self {
            Self::Off => None,
            Self::LowVolatilityV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.70,
                max_average_abs_correlation: None,
                correlation_reference_limit: 0,
                min_liquidity_quantile: None,
            }),
            Self::LowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.70,
                max_average_abs_correlation: Some(0.55),
                correlation_reference_limit: 120,
                min_liquidity_quantile: None,
            }),
            Self::SoftLowVolatilityV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: None,
                correlation_reference_limit: 0,
                min_liquidity_quantile: None,
            }),
            Self::SoftLowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: Some(0.70),
                correlation_reference_limit: 120,
                min_liquidity_quantile: None,
            }),
            Self::SoftLiquidityLowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: Some(0.70),
                correlation_reference_limit: 120,
                min_liquidity_quantile: Some(0.50),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateRiskFilterParams {
    max_volatility_quantile: f64,
    max_average_abs_correlation: Option<f64>,
    correlation_reference_limit: usize,
    min_liquidity_quantile: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRankingProfile {
    #[default]
    Off,
    CapacityAwareAlphaLiquidityV1,
    AlphaFirstLowImpactV1,
    RelativeStrengthAlphaLiquidityV1,
}

impl CandidateRankingProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "capacity_aware_alpha_liquidity_v1"
            | "capacity-aware-alpha-liquidity-v1"
            | "capacity_aware_candidate_ranking_v1"
            | "capacity-aware-candidate-ranking-v1" => Ok(Self::CapacityAwareAlphaLiquidityV1),
            "alpha_first_low_impact_v1"
            | "alpha-first-low-impact-v1"
            | "alpha_first_liquidity_v1"
            | "alpha-first-liquidity-v1" => Ok(Self::AlphaFirstLowImpactV1),
            "relative_strength_alpha_liquidity_v1"
            | "relative-strength-alpha-liquidity-v1"
            | "return_aware_alpha_liquidity_v1"
            | "return-aware-alpha-liquidity-v1"
            | "pit_relative_strength_alpha_liquidity_v1"
            | "pit-relative-strength-alpha-liquidity-v1" => {
                Ok(Self::RelativeStrengthAlphaLiquidityV1)
            }
            other => Err(format!("unsupported candidate_ranking: {}", other)),
        }
    }

    fn params(self) -> Option<CandidateRankingParams> {
        match self {
            Self::Off => None,
            Self::CapacityAwareAlphaLiquidityV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.30,
                liquidity_rank_weight: 0.70,
                relative_strength_rank_weight: 0.0,
            }),
            Self::AlphaFirstLowImpactV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.75,
                liquidity_rank_weight: 0.25,
                relative_strength_rank_weight: 0.0,
            }),
            Self::RelativeStrengthAlphaLiquidityV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.45,
                liquidity_rank_weight: 0.25,
                relative_strength_rank_weight: 0.30,
            }),
        }
    }

    fn uses_capacity(self) -> bool {
        self.params().is_some()
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateRankingParams {
    alpha_rank_weight: f64,
    liquidity_rank_weight: f64,
    relative_strength_rank_weight: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskContributionControlProfile {
    #[default]
    Off,
    SoftSingleName20PctV1,
    SoftSingleName15PctV1,
}

impl RiskContributionControlProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "soft_single_name_20pct_v1" | "soft-single-name-20pct-v1" => {
                Ok(Self::SoftSingleName20PctV1)
            }
            "soft_single_name_15pct_v1" | "soft-single-name-15pct-v1" => {
                Ok(Self::SoftSingleName15PctV1)
            }
            other => Err(format!("unsupported risk_contribution_control: {}", other)),
        }
    }

    fn params(self) -> Option<RiskContributionControlParams> {
        match self {
            Self::Off => None,
            Self::SoftSingleName20PctV1 => Some(RiskContributionControlParams {
                max_single_name_contribution_pct: 0.20,
                iterations: 6,
            }),
            Self::SoftSingleName15PctV1 => Some(RiskContributionControlParams {
                max_single_name_contribution_pct: 0.15,
                iterations: 6,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RiskContributionControlParams {
    max_single_name_contribution_pct: f64,
    iterations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradableUniverseProfile {
    All,
    ListedNonSt,
    MainBoardNonSt,
}

impl TradableUniverseProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "all" | "full" => Ok(Self::All),
            "listed_non_st" | "listed-non-st" => Ok(Self::ListedNonSt),
            "main_board_non_st" | "main-board-non-st" => Ok(Self::MainBoardNonSt),
            other => Err(format!("unsupported universe_profile: {}", other)),
        }
    }
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
    pub combo_name: Option<String>,
    pub version: Option<String>,
    pub top_n: Option<usize>,
    pub rebalance_freq_days: Option<usize>,
    pub max_gross_exposure: Option<f64>,
    pub score_direction: Option<ScoreDirection>,
    pub skip_top_pct: Option<f64>,
    pub max_pairwise_correlation: Option<f64>,
    pub max_position_pct: Option<Decimal>,
    pub score_overlay: Option<FactorScoreOverlayConfig>,
    pub portfolio_sleeve: Option<FactorPortfolioSleeveConfig>,
}

impl RegimeSignalRule {
    fn apply_to(&self, base: &SignalConfig) -> SignalConfig {
        let mut config = base.clone();
        if let Some(combo_name) = self.combo_name.as_ref() {
            config.combo_name = combo_name.clone();
        }
        if let Some(version) = self.version.as_ref() {
            config.version = version.clone();
        }
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
        if let Some(max_pairwise_correlation) = self.max_pairwise_correlation {
            config.max_pairwise_correlation = Some(max_pairwise_correlation.clamp(0.0, 1.0));
        }
        if let Some(max_position_pct) = self.max_position_pct {
            config.max_position_pct = max_position_pct.clamp(Decimal::ZERO, Decimal::ONE);
        }
        if let Some(score_overlay) = self.score_overlay.as_ref() {
            config.score_overlay = Some(score_overlay.clone());
        }
        if let Some(portfolio_sleeve) = self.portfolio_sleeve.as_ref() {
            config.portfolio_sleeve = Some(portfolio_sleeve.clone());
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

type StateAlphaSleeveSpec = (&'static str, f64, ScoreDirection);

struct StateAlphaSelectorSpec {
    bull_sleeve: StateAlphaSleeveSpec,
    bear_sleeve: StateAlphaSleeveSpec,
    high_volatility_sleeve: StateAlphaSleeveSpec,
    sideways_sleeve: StateAlphaSleeveSpec,
    mixed_sleeve: StateAlphaSleeveSpec,
    bear_exposure: f64,
    high_volatility_exposure: f64,
    bear_max_position_pct: Decimal,
    high_volatility_max_position_pct: Decimal,
}

#[allow(clippy::too_many_arguments)]
fn apply_state_alpha_rule(
    policy: &mut MarketRegimePolicy,
    regime: MarketRegime,
    top_n: Option<usize>,
    rebalance_freq_days: Option<usize>,
    max_gross_exposure: Option<f64>,
    max_pairwise_correlation: Option<f64>,
    max_position_pct: Option<Decimal>,
    sleeve: StateAlphaSleeveSpec,
) {
    let Some(rule) = policy.rules.get_mut(&regime) else {
        return;
    };
    if let Some(top_n) = top_n {
        rule.top_n = Some(top_n);
    }
    if let Some(rebalance_freq_days) = rebalance_freq_days {
        rule.rebalance_freq_days = Some(rebalance_freq_days);
    }
    if let Some(max_gross_exposure) = max_gross_exposure {
        rule.max_gross_exposure = Some(max_gross_exposure);
    }
    if let Some(max_pairwise_correlation) = max_pairwise_correlation {
        rule.max_pairwise_correlation = Some(max_pairwise_correlation);
    }
    if let Some(max_position_pct) = max_position_pct {
        rule.max_position_pct = Some(max_position_pct);
    }
    rule.portfolio_sleeve = Some(FactorPortfolioSleeveConfig {
        combo_name: sleeve.0.to_string(),
        version: "1.0.0".to_string(),
        weight: sleeve.1.clamp(0.0, 1.0),
        score_direction: sleeve.2,
    });
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

    pub fn drawdown_control_v1(benchmark: impl Into<String>) -> Self {
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
                top_n: Some(20),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.35),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.10),
                max_position_pct: Some(Decimal::new(5, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(20),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.25),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(5, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.24,
            bear_return_threshold: -0.015,
            bear_drawdown_threshold: 0.12,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    pub fn drawdown_control_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(15),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.25),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.15),
                max_position_pct: Some(Decimal::new(4, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(15),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.18),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.10),
                max_position_pct: Some(Decimal::new(4, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(40),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.55),
                max_position_pct: Some(Decimal::new(6, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(40),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.55),
                max_position_pct: Some(Decimal::new(6, 2)),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.20,
            bear_return_threshold: -0.01,
            bear_drawdown_threshold: 0.08,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.10,
            sideways_volatility_threshold: 0.09,
            sideways_abs_return_threshold: 0.035,
            rules,
        }
    }

    /// Risk-off overlay for quality/value style alpha where the score direction
    /// itself is the edge. Unlike the generic policies, this never flips
    /// `score_direction`; it only scales exposure and position caps in weak
    /// markets.
    pub fn quality_risk_off_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(12, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.24,
            bear_return_threshold: -0.015,
            bear_drawdown_threshold: 0.12,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Tail-risk guard for quality/value style alpha. It keeps the stock
    /// selection shape intact and only reduces exposure in severe market
    /// stress, so high-return quality candidates are not diluted in normal
    /// sideways or mild risk-off regimes.
    pub fn quality_crash_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.85),
                max_position_pct: Some(Decimal::new(12, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.75),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.50,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.25,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Stronger tail-risk guard for Phase 7-R bridge candidates. It keeps the
    /// quality alpha shape intact, but cuts exposure harder once benchmark
    /// drawdown or volatility confirms a deeper stress regime.
    pub fn quality_crash_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.75),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.60),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.45,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.22,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Mid-strength tail guard for bridge candidates that lost too much return
    /// under v2. It preserves v1's late trigger thresholds while using slightly
    /// stronger exposure caps once the tail regime is already confirmed.
    pub fn quality_crash_guard_v3(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.80),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.68),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.50,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.25,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Earlier bear-window guard for Phase 7-X attribution findings. It targets
    /// long weak windows by triggering earlier than crash guards, while keeping
    /// the quality alpha direction, holding count, and rebalance cadence intact.
    pub fn quality_bear_window_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.78),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.66),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.32,
            bear_return_threshold: -0.04,
            bear_drawdown_threshold: 0.16,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Regime-conditioned alpha router for Phase 7-AN. Normal regimes keep the
    /// high-return financial-quality anchor, while weak/high-volatility regimes
    /// switch to industry-residual quality as a defensive second alpha source.
    pub fn quality_regime_alpha_switch_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_industry_residual_quality_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_value_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_valuation_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_recovery_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_blend_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_value_05pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_valuation_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_value_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_valuation_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_blend_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_value_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_valuation_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_value_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_valuation_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_blend_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.075,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.125,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_all_regime_event_window_sleeve_05pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.05)
    }

    pub fn quality_all_regime_event_window_sleeve_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.10)
    }

    pub fn quality_all_regime_event_window_sleeve_15pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.15)
    }

    fn quality_event_window_sleeve_all_regimes(
        benchmark: impl Into<String>,
        sleeve_weight: f64,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            sleeve_weight,
            ScoreDirection::Descending,
            [
                MarketRegime::Bull,
                MarketRegime::Bear,
                MarketRegime::HighVolatility,
                MarketRegime::Sideways,
                MarketRegime::Mixed,
            ],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
            [MarketRegime::Bear],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
            [MarketRegime::HighVolatility],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_10d_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_40d_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_earnings_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_price_volume_expanded_v1",
            0.10,
            ScoreDirection::Ascending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_price_volume_expanded_v1",
            0.15,
            ScoreDirection::Ascending,
        )
    }

    fn quality_regime_alpha_switch(
        benchmark: impl Into<String>,
        stress_combo_name: &str,
        stress_score_direction: ScoreDirection,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v1(benchmark);
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.combo_name = Some(stress_combo_name.to_string());
                rule.version = Some("1.0.0".to_string());
                rule.score_direction = Some(stress_score_direction);
            }
        }
        policy
    }

    /// Regime-conditioned alpha overlay for Phase 7-AP. This preserves the
    /// quality anchor and only adds a small secondary score in bear or
    /// high-volatility regimes, avoiding the return dilution seen in hard
    /// alpha-source replacement.
    fn quality_regime_alpha_overlay(
        benchmark: impl Into<String>,
        overlay_combo_name: &str,
        overlay_weight: f64,
        overlay_score_direction: ScoreDirection,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        let overlay = FactorScoreOverlayConfig {
            combo_name: overlay_combo_name.to_string(),
            version: "1.0.0".to_string(),
            weight: overlay_weight.clamp(0.0, 1.0),
            score_direction: overlay_score_direction,
        };
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.score_overlay = Some(overlay.clone());
            }
        }
        policy
    }

    /// Regime-conditioned portfolio sleeve for Phase 7-AQ. It builds a small
    /// independent stress sleeve after portfolio construction and blends target
    /// weights, instead of perturbing the main quality ranking.
    fn quality_regime_alpha_portfolio_sleeve(
        benchmark: impl Into<String>,
        sleeve_combo_name: &str,
        sleeve_weight: f64,
        sleeve_score_direction: ScoreDirection,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            sleeve_combo_name,
            sleeve_weight,
            sleeve_score_direction,
            [MarketRegime::Bear, MarketRegime::HighVolatility],
        )
    }

    fn quality_regime_alpha_portfolio_sleeve_for_regimes(
        benchmark: impl Into<String>,
        sleeve_combo_name: &str,
        sleeve_weight: f64,
        sleeve_score_direction: ScoreDirection,
        regimes: impl IntoIterator<Item = MarketRegime>,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        let sleeve = FactorPortfolioSleeveConfig {
            combo_name: sleeve_combo_name.to_string(),
            version: "1.0.0".to_string(),
            weight: sleeve_weight.clamp(0.0, 1.0),
            score_direction: sleeve_score_direction,
        };
        for regime in regimes {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.portfolio_sleeve = Some(sleeve.clone());
            }
        }
        policy
    }

    /// Stronger early bear-window guard. This is still quality-shape preserving,
    /// but cuts tail regimes harder when U2 needs more Sharpe stabilization.
    pub fn quality_bear_window_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.72),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.58),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Position-aware bear-window guard for the U2 quality anchor. It keeps the
    /// quality score direction intact, but diversifies and slows the book when
    /// the benchmark enters a weak or high-volatility regime.
    pub fn quality_bear_position_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.74),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.58),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(75, 3)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Stronger position-aware guard for local Sharpe searches. Use as a
    /// stress-neighborhood candidate, not as a broad default.
    pub fn quality_bear_position_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.68),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.52),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(65, 3)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Mild position-aware guard for the current Phase 7 anchor. It uses the
    /// same generic bear/high-volatility triggers as v1/v2, but cuts less
    /// aggressively so the 15%+ return target has a better chance to survive.
    pub fn quality_bear_position_guard_v3(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.82),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.66),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    pub fn quality_event_window_position_guard_v1(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v1(benchmark))
    }

    pub fn quality_event_window_position_guard_v2(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v2(benchmark))
    }

    pub fn quality_event_window_position_guard_v3(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v3(benchmark))
    }

    fn with_event_window_position_sleeve(mut policy: Self) -> Self {
        let sleeve = FactorPortfolioSleeveConfig {
            combo_name: "phase7_event_window_earnings_v1".to_string(),
            version: "1.0.0".to_string(),
            weight: 0.15,
            score_direction: ScoreDirection::Descending,
        };
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.portfolio_sleeve = Some(sleeve.clone());
            }
        }
        policy
    }

    pub fn quality_event_window_return_sharpe_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.72,
            0.58,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.78,
            0.64,
            Decimal::new(11, 2),
            Decimal::new(9, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.75,
            0.62,
            Decimal::new(10, 2),
            Decimal::new(9, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v4(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.68,
            0.54,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
        )
    }

    pub fn quality_state_alpha_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                sideways_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                mixed_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                bear_exposure: 0.68,
                high_volatility_exposure: 0.54,
                bear_max_position_pct: Decimal::new(9, 2),
                high_volatility_max_position_pct: Decimal::new(7, 2),
            },
        )
    }

    pub fn quality_state_alpha_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.125,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.125,
                    ScoreDirection::Descending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.75,
                high_volatility_exposure: 0.62,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(9, 2),
            },
        )
    }

    pub fn quality_state_alpha_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                mixed_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.68,
                high_volatility_exposure: 0.54,
                bear_max_position_pct: Decimal::new(9, 2),
                high_volatility_max_position_pct: Decimal::new(7, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_overlay_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
            ],
        )
    }

    pub fn quality_mixed_event_state_overlay_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_mixed_event_state_selector_v2(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_residual_confirm_10pct_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    bear_exposure: 0.72,
                    high_volatility_exposure: 0.58,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [(
                MarketRegime::Mixed,
                "phase7_valuation_v1",
                0.03,
                ScoreDirection::Descending,
            )],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    mixed_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.10,
                        ScoreDirection::Descending,
                    ),
                    bear_exposure: 0.72,
                    high_volatility_exposure: 0.58,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [(
                MarketRegime::Mixed,
                "phase7_quality_residual_confirm_10pct_v1",
                0.03,
                ScoreDirection::Ascending,
            )],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_blend_defensive_rel_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_nonlinear_alpha_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_residual_confirm_10pct_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    bear_exposure: 0.74,
                    high_volatility_exposure: 0.60,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.04,
                    ScoreDirection::Descending,
                ),
            ],
        )
        .with_nonlinear_mixed_risk(0.98, Decimal::new(14, 2), 0.72)
    }

    pub fn quality_nonlinear_alpha_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.125,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.075,
                        ScoreDirection::Descending,
                    ),
                    bear_exposure: 0.78,
                    high_volatility_exposure: 0.62,
                    bear_max_position_pct: Decimal::new(11, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [
                (
                    MarketRegime::Mixed,
                    "phase7_quality_residual_confirm_10pct_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_event_window_earnings_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
            ],
        )
        .with_nonlinear_mixed_risk(1.0, Decimal::new(15, 2), 0.75)
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v1(benchmark).with_nonlinear_mixed_risk(
            0.94,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v1(benchmark).with_nonlinear_mixed_risk(
            0.97,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v2(benchmark).with_nonlinear_mixed_risk(
            0.98,
            Decimal::new(14, 2),
            0.75,
        )
    }

    fn with_nonlinear_mixed_risk(
        mut self,
        mixed_exposure: f64,
        mixed_max_position_pct: Decimal,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = self.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        self
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v2(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v3(benchmark),
            0.92,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.85,
            Decimal::new(12, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.75,
            Decimal::new(10, 2),
            0.65,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_overlay_selector_v1(benchmark),
            0.82,
            Decimal::new(11, 2),
            0.65,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v4(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v5(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.95,
            Decimal::new(14, 2),
            0.75,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v6(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_overlay_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v7(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.92,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v8(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.93,
            Decimal::new(13, 2),
            0.73,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v9(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.94,
            Decimal::new(14, 2),
            0.74,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v10(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.95,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v11(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.94,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v12(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.96,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v13(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v14(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v15(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v16(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(15, 2),
            0.75,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v17(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v18(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(15, 2),
            0.75,
        )
    }

    fn with_mixed_state_risk_memory(
        mut policy: Self,
        mixed_exposure: f64,
        mixed_max_position_pct: Decimal,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        policy
    }

    pub fn quality_state_alpha_overlay_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v3(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.05,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_state_alpha_overlay_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v2(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
            ],
        )
    }

    pub fn quality_state_alpha_overlay_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v3(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_state_sharpe_bridge_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.70,
            0.56,
            0.96,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_state_sharpe_bridge_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            0.98,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_state_sharpe_bridge_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.66,
            0.52,
            0.92,
            Decimal::new(8, 2),
            Decimal::new(7, 2),
            Decimal::new(12, 2),
            0.62,
            0.62,
            0.68,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.70,
            0.56,
            0.96,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
            Decimal::new(13, 2),
            0.62,
            0.62,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v4(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v5(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v6(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v7(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_state_sharpe_bridge_risk(
        mut policy: Self,
        bear_exposure: f64,
        high_volatility_exposure: f64,
        mixed_exposure: f64,
        bear_max_position_pct: Decimal,
        high_volatility_max_position_pct: Decimal,
        mixed_max_position_pct: Decimal,
        bear_max_pairwise_correlation: f64,
        high_volatility_max_pairwise_correlation: f64,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Bear) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(bear_exposure);
            rule.max_position_pct = Some(bear_max_position_pct);
            rule.max_pairwise_correlation = Some(bear_max_pairwise_correlation);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::HighVolatility) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(high_volatility_exposure);
            rule.max_position_pct = Some(high_volatility_max_position_pct);
            rule.max_pairwise_correlation = Some(high_volatility_max_pairwise_correlation);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        policy
    }

    fn quality_state_alpha_selector_with_overlay(
        mut policy: Self,
        overlays: impl IntoIterator<Item = (MarketRegime, &'static str, f64, ScoreDirection)>,
    ) -> Self {
        for (regime, combo_name, weight, score_direction) in overlays {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.score_overlay = Some(FactorScoreOverlayConfig {
                    combo_name: combo_name.to_string(),
                    version: "1.0.0".to_string(),
                    weight: weight.clamp(0.0, 1.0),
                    score_direction,
                });
            }
        }
        policy
    }

    fn quality_state_alpha_selector(
        benchmark: impl Into<String>,
        spec: StateAlphaSelectorSpec,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Bull,
            None,
            None,
            None,
            None,
            None,
            spec.bull_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Bear,
            Some(20),
            Some(60),
            Some(spec.bear_exposure),
            Some(0.65),
            Some(spec.bear_max_position_pct),
            spec.bear_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::HighVolatility,
            Some(20),
            Some(60),
            Some(spec.high_volatility_exposure),
            Some(0.65),
            Some(spec.high_volatility_max_position_pct),
            spec.high_volatility_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Sideways,
            None,
            None,
            Some(1.0),
            None,
            None,
            spec.sideways_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Mixed,
            None,
            None,
            Some(1.0),
            None,
            None,
            spec.mixed_sleeve,
        );
        policy
    }

    fn quality_event_window_return_sharpe_router(
        benchmark: impl Into<String>,
        bear_exposure: f64,
        high_vol_exposure: f64,
        bear_max_position_pct: Decimal,
        high_vol_max_position_pct: Decimal,
    ) -> Self {
        let mut policy =
            Self::quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(benchmark);
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Bear) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(bear_exposure);
            rule.max_pairwise_correlation = Some(0.65);
            rule.max_position_pct = Some(bear_max_position_pct);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::HighVolatility) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(high_vol_exposure);
            rule.max_pairwise_correlation = Some(0.65);
            rule.max_position_pct = Some(high_vol_max_position_pct);
        }
        policy
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
    portfolio_notional_cny: Option<f64>,
    max_participation_rate: Option<f64>,
    capacity_risk_budget_profile: CapacityRiskBudgetProfile,
    cash_utilization_profile: CashUtilizationProfile,
    max_pairwise_correlation: Option<f64>,
    correlation_lookback_days: usize,
    kelly_fraction: f64,
    kelly_lookback_days: usize,
    max_gross_exposure: f64,
    portfolio_method: PortfolioConstructionMethod,
    risk_budget_lookback_days: usize,
    capacity_penalty_strength: f64,
    max_industry_weight_pct: Option<f64>,
    style_risk_budget_profile: StyleRiskBudgetProfile,
    candidate_risk_filter_profile: CandidateRiskFilterProfile,
    candidate_ranking_profile: CandidateRankingProfile,
    risk_contribution_control_profile: RiskContributionControlProfile,
}

impl Default for PortfolioConstructionConfig {
    fn default() -> Self {
        Self {
            top_n: 20,
            max_position_pct: Decimal::new(10, 2),
            portfolio_notional_cny: None,
            max_participation_rate: None,
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::Off,
            cash_utilization_profile: CashUtilizationProfile::Off,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
            max_industry_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
            candidate_ranking_profile: CandidateRankingProfile::Off,
            risk_contribution_control_profile: RiskContributionControlProfile::Off,
        }
    }
}

impl From<&SignalConfig> for PortfolioConstructionConfig {
    fn from(config: &SignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
            portfolio_notional_cny: config.portfolio_notional_cny,
            max_participation_rate: config.max_participation_rate,
            capacity_risk_budget_profile: config.capacity_risk_budget_profile,
            cash_utilization_profile: config.cash_utilization_profile,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
            max_industry_weight_pct: config.industry_max_weight_pct,
            style_risk_budget_profile: config.style_risk_budget_profile,
            candidate_risk_filter_profile: config.candidate_risk_filter_profile,
            candidate_ranking_profile: config.candidate_ranking_profile,
            risk_contribution_control_profile: config.risk_contribution_control_profile,
        }
    }
}

impl From<&PredictionSignalConfig> for PortfolioConstructionConfig {
    fn from(config: &PredictionSignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
            portfolio_notional_cny: config.portfolio_notional_cny,
            max_participation_rate: config.max_participation_rate,
            capacity_risk_budget_profile: config.capacity_risk_budget_profile,
            cash_utilization_profile: config.cash_utilization_profile,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
            max_industry_weight_pct: config.industry_max_weight_pct,
            style_risk_budget_profile: config.style_risk_budget_profile,
            candidate_risk_filter_profile: config.candidate_risk_filter_profile,
            candidate_ranking_profile: config.candidate_ranking_profile,
            risk_contribution_control_profile: config.risk_contribution_control_profile,
        }
    }
}

impl PortfolioConstructionConfig {
    fn uses_capacity_inputs(&self) -> bool {
        self.portfolio_method == PortfolioConstructionMethod::RiskBudget
            || self.style_risk_budget_profile.uses_liquidity()
            || self.candidate_ranking_profile.uses_capacity()
            || self.capacity_risk_budget_profile.uses_capacity()
            || (self.max_participation_rate.is_some() && self.portfolio_notional_cny.is_some())
    }
}

fn capped_portfolio_top_n(top_n: usize, method: PortfolioConstructionMethod) -> usize {
    match method {
        PortfolioConstructionMethod::RiskBudget | PortfolioConstructionMethod::MinVariance => {
            top_n.min(50)
        }
        PortfolioConstructionMethod::Heuristic => top_n,
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
    let mut cache = SignalDataCache::default();
    generate_signals_with_cache(pool, config, start_date, end_date, &mut cache).await
}

/// Generate factor signals using a caller-owned cache for repeated trial batches.
pub async fn generate_signals_with_cache(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_signals_with_cache_internal(pool, config, start_date, end_date, cache, None).await
}

pub async fn generate_signals_with_cache_and_market_feature_snapshot(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: &MarketFeatureSnapshotScope,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_signals_with_cache_internal(
        pool,
        config,
        start_date,
        end_date,
        cache,
        Some(snapshot_scope),
    )
    .await
}

async fn generate_signals_with_cache_internal(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: Option<&MarketFeatureSnapshotScope>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let trading_days = load_open_trading_days_cached(pool, cache, start_date, end_date).await?;
    let score_days = rebalance_score_days(trading_days.as_ref(), config, |_day, base| base.clone());
    let score_cache = load_combo_scores_for_dates_cached(pool, cache, config, &score_days).await?;
    let mut adjusted_scores;
    let scores_by_date: &FactorScoresByDate = if config.min_daily_amount_cny.is_some()
        || config.prediction_blend.is_some()
        || config.event_gate.is_some()
    {
        adjusted_scores = Arc::as_ref(&score_cache).clone();
        if config.min_daily_amount_cny.is_some() {
            apply_factor_liquidity_filter(
                pool,
                cache,
                &mut adjusted_scores,
                config,
                start_date,
                end_date,
            )
            .await?;
        }
        if let Some(blend) = config.prediction_blend.as_ref() {
            let prediction_scores = load_prediction_scores_by_date_cached(
                pool,
                cache,
                &blend.prediction_set_id,
                start_date,
                end_date,
            )
            .await?;
            blend_factor_prediction_scores(
                &mut adjusted_scores,
                prediction_scores.as_ref(),
                blend,
                config.score_direction,
            );
        }
        if let Some(event_gate) = config.event_gate.as_ref() {
            let event_scores =
                load_event_gate_scores_for_dates_cached(pool, cache, event_gate, &score_days)
                    .await?;
            apply_event_gate_scores(&mut adjusted_scores, event_scores.as_ref(), event_gate);
        }
        &adjusted_scores
    } else {
        score_cache.as_ref()
    };

    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let portfolio_config = PortfolioConstructionConfig::from(config);
    let return_lookback_days = portfolio_history_lookback_days(&portfolio_config);
    let return_history = load_symbol_return_history_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        return_lookback_days,
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        &score_days,
        &portfolio_config,
    )
    .await?;
    if let Some(snapshot_scope) = snapshot_scope {
        cache.insert_market_feature_snapshot_from_cached_histories(
            snapshot_scope.snapshot_key(return_lookback_days, &all_symbols),
            return_lookback_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
            &all_symbols,
            start_date,
            end_date,
        );
    }
    let industry_by_symbol =
        load_portfolio_industry_inputs_cached(pool, cache, &all_symbols, &portfolio_config).await?;

    build_rebalance_factor_signals(
        trading_days.as_ref(),
        scores_by_date,
        config,
        return_history.as_ref(),
        average_amounts.as_ref(),
        industry_by_symbol.as_ref(),
        |_day, base| base.clone(),
    )
}

/// Generate factor signals with market-regime-aware parameter overlays.
pub async fn generate_regime_signals(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let mut cache = SignalDataCache::default();
    generate_regime_signals_with_cache(pool, config, policy, start_date, end_date, &mut cache).await
}

/// Generate regime-aware factor signals using a caller-owned cache for repeated trial batches.
pub async fn generate_regime_signals_with_cache(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_regime_signals_with_cache_internal(
        pool, config, policy, start_date, end_date, cache, None,
    )
    .await
}

pub async fn generate_regime_signals_with_cache_and_market_feature_snapshot(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: &MarketFeatureSnapshotScope,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_regime_signals_with_cache_internal(
        pool,
        config,
        policy,
        start_date,
        end_date,
        cache,
        Some(snapshot_scope),
    )
    .await
}

async fn generate_regime_signals_with_cache_internal(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: Option<&MarketFeatureSnapshotScope>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let trading_days = load_open_trading_days_cached(pool, cache, start_date, end_date).await?;
    let portfolio_config = PortfolioConstructionConfig::from(config);
    let max_lookback = portfolio_history_lookback_days(&portfolio_config).max(policy.lookback_days);
    let benchmark_returns = load_benchmark_return_history_cached(
        pool,
        cache,
        &policy.benchmark,
        start_date,
        end_date,
        max_lookback,
    )
    .await?;

    let score_days = rebalance_score_days(trading_days.as_ref(), config, |day, base| {
        let returns =
            trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
        let regime = classify_market_regime(&returns, policy);
        policy.apply(base, regime)
    });
    let score_source_configs = regime_score_source_configs(config, policy);
    let mut score_sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
    for source_config in score_source_configs {
        let key = FactorScoreSourceKey::from_config(&source_config);
        if score_sources.contains_key(&key) {
            continue;
        }
        let scores =
            load_regime_base_scores_for_dates_cached(pool, cache, &source_config, &score_days)
                .await?;
        score_sources.insert(key, scores);
    }

    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| !gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_for_dates_cached(pool, cache, event_gate, &score_days).await?;
        for scores in score_sources.values_mut() {
            apply_event_gate_scores_for_regime(scores, event_scores.as_ref(), event_gate, |day| {
                let returns =
                    trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
                classify_market_regime(&returns, policy)
            });
        }
    }

    let all_symbols: Vec<String> = score_sources
        .values()
        .flat_map(|scores_by_date| scores_by_date.values())
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let return_history = load_symbol_return_history_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        max_lookback,
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        &score_days,
        &portfolio_config,
    )
    .await?;
    if let Some(snapshot_scope) = snapshot_scope {
        cache.insert_market_feature_snapshot_from_cached_histories(
            snapshot_scope.snapshot_key(max_lookback, &all_symbols),
            max_lookback,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
            &all_symbols,
            start_date,
            end_date,
        );
    }
    let industry_by_symbol =
        load_portfolio_industry_inputs_cached(pool, cache, &all_symbols, &portfolio_config).await?;

    build_rebalance_factor_signals_with_score_selector(
        trading_days.as_ref(),
        config,
        return_history.as_ref(),
        average_amounts.as_ref(),
        industry_by_symbol.as_ref(),
        |score_day, active_config| {
            score_rows_for_active_config(&score_sources, score_day, active_config)
        },
        |day, base| {
            let returns =
                trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
            let regime = classify_market_regime(&returns, policy);
            policy.apply(base, regime)
        },
    )
}

fn regime_score_source_configs(
    base_config: &SignalConfig,
    policy: &MarketRegimePolicy,
) -> Vec<SignalConfig> {
    let mut configs = Vec::new();
    let mut seen = HashSet::new();
    for config in std::iter::once(base_config.clone()).chain(
        [
            MarketRegime::Bull,
            MarketRegime::Bear,
            MarketRegime::HighVolatility,
            MarketRegime::Sideways,
            MarketRegime::Mixed,
        ]
        .into_iter()
        .map(|regime| policy.apply(base_config, regime)),
    ) {
        let key = FactorScoreSourceKey::from_config(&config);
        if seen.insert(key) {
            configs.push(config.clone());
        }
        if let Some(overlay) = config.score_overlay.as_ref() {
            let overlay_config = score_source_config_for_overlay(&config, overlay);
            let key = FactorScoreSourceKey::from_config(&overlay_config);
            if seen.insert(key) {
                configs.push(overlay_config);
            }
        }
        if let Some(sleeve) = config.portfolio_sleeve.as_ref() {
            let sleeve_config = score_source_config_for_portfolio_sleeve(&config, sleeve);
            let key = FactorScoreSourceKey::from_config(&sleeve_config);
            if seen.insert(key) {
                configs.push(sleeve_config);
            }
        }
    }
    configs
}

fn score_rows_for_active_config(
    score_sources: &HashMap<FactorScoreSourceKey, FactorScoresByDate>,
    score_day: NaiveDate,
    active_config: &SignalConfig,
) -> Option<Vec<(String, f64)>> {
    let base_key = FactorScoreSourceKey::from_config(active_config);
    let base_rows = score_sources.get(&base_key)?.get(&score_day)?.clone();
    let Some(overlay) = active_config.score_overlay.as_ref() else {
        return Some(base_rows);
    };
    let overlay_config = score_source_config_for_overlay(active_config, overlay);
    let overlay_key = FactorScoreSourceKey::from_config(&overlay_config);
    let Some(overlay_rows) = score_sources
        .get(&overlay_key)
        .and_then(|scores_by_date| scores_by_date.get(&score_day))
        .cloned()
    else {
        return Some(base_rows);
    };
    if overlay_rows.is_empty() {
        return Some(base_rows);
    }

    Some(blend_factor_overlay_scores(
        base_rows,
        overlay_rows,
        active_config.score_direction,
        overlay.score_direction,
        overlay.weight,
    ))
}

fn score_source_config_for_overlay(
    base_config: &SignalConfig,
    overlay: &FactorScoreOverlayConfig,
) -> SignalConfig {
    let mut config = base_config.clone();
    config.combo_name = overlay.combo_name.clone();
    config.version = overlay.version.clone();
    config.score_direction = overlay.score_direction;
    config.score_overlay = None;
    config.portfolio_sleeve = None;
    config
}

fn score_source_config_for_portfolio_sleeve(
    base_config: &SignalConfig,
    sleeve: &FactorPortfolioSleeveConfig,
) -> SignalConfig {
    let mut config = base_config.clone();
    config.combo_name = sleeve.combo_name.clone();
    config.version = sleeve.version.clone();
    config.score_direction = sleeve.score_direction;
    config.score_overlay = None;
    config.portfolio_sleeve = None;
    config
}

fn blend_factor_overlay_scores(
    base_rows: Vec<(String, f64)>,
    overlay_rows: Vec<(String, f64)>,
    base_direction: ScoreDirection,
    overlay_direction: ScoreDirection,
    overlay_weight: f64,
) -> Vec<(String, f64)> {
    let overlay_weight = overlay_weight.clamp(0.0, 1.0);
    if overlay_weight <= f64::EPSILON {
        return base_rows;
    }
    let base_weight = 1.0 - overlay_weight;
    let base_stats = score_stats(base_rows.iter().map(|(_, score)| *score));
    let overlay_stats = score_stats(overlay_rows.iter().map(|(_, score)| *score));
    let overlay_scores: HashMap<String, f64> = overlay_rows
        .into_iter()
        .filter(|(_, score)| score.is_finite())
        .map(|(symbol, score)| {
            (
                symbol,
                oriented_standard_score(score, overlay_stats, overlay_direction),
            )
        })
        .collect();

    base_rows
        .into_iter()
        .filter(|(_, score)| score.is_finite())
        .map(|(symbol, score)| {
            let base_good = oriented_standard_score(score, base_stats, base_direction);
            let overlay_good = overlay_scores.get(&symbol).copied().unwrap_or(0.0);
            let blended_good = base_weight * base_good + overlay_weight * overlay_good;
            let blended_score = match base_direction {
                ScoreDirection::Descending => blended_good,
                ScoreDirection::Ascending => -blended_good,
            };
            (symbol, blended_score)
        })
        .collect()
}

fn oriented_standard_score(value: f64, stats: (f64, f64), direction: ScoreDirection) -> f64 {
    let score = standard_score(value, stats);
    match direction {
        ScoreDirection::Descending => score,
        ScoreDirection::Ascending => -score,
    }
}

async fn load_regime_base_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
) -> Result<FactorScoresByDate, String> {
    let score_cache = load_combo_scores_for_dates_cached(pool, cache, config, score_days).await?;
    let mut adjusted_scores = score_cache.as_ref().clone();
    let score_span = date_span(score_days);

    if config.min_daily_amount_cny.is_some() {
        if let Some((start_date, end_date)) = score_span {
            apply_factor_liquidity_filter(
                pool,
                cache,
                &mut adjusted_scores,
                config,
                start_date,
                end_date,
            )
            .await?;
        }
    }
    if let Some(blend) = config.prediction_blend.as_ref() {
        if let Some((start_date, end_date)) = score_span {
            let prediction_scores = load_prediction_scores_by_date_cached(
                pool,
                cache,
                &blend.prediction_set_id,
                start_date,
                end_date,
            )
            .await?;
            blend_factor_prediction_scores(
                &mut adjusted_scores,
                prediction_scores.as_ref(),
                blend,
                config.score_direction,
            );
        }
    }
    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_for_dates_cached(pool, cache, event_gate, score_days).await?;
        apply_event_gate_scores(&mut adjusted_scores, event_scores.as_ref(), event_gate);
    }

    Ok(adjusted_scores)
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
    let prediction_start_date = prediction_load_start_date(start_date, config.entry_delay_days);
    let rows: Vec<(String, NaiveDate, f64, Option<i32>)> = sqlx::query_as(
        "SELECT symbol, trade_date, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND available_at <= trade_date
         ORDER BY trade_date, rank NULLS LAST, score DESC, symbol",
    )
    .bind(&config.prediction_set_id)
    .bind(prediction_start_date)
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

fn prediction_load_start_date(start_date: NaiveDate, entry_delay_days: usize) -> NaiveDate {
    let calendar_buffer_days = 30 + (entry_delay_days as i64 * 3);
    start_date - chrono::Duration::days(calendar_buffer_days)
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

    sort_prediction_scores(&mut scores_by_date, config.score_direction);
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
        &trading_days,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;
    let industry_by_symbol = load_portfolio_industry_inputs(
        pool,
        &all_symbols,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;

    build_rebalance_prediction_signals(
        &trading_days,
        &scores_by_date,
        config,
        &return_history,
        &average_amounts,
        &industry_by_symbol,
    )
}

fn sort_prediction_scores(
    scores_by_date: &mut HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    direction: ScoreDirection,
) {
    for items in scores_by_date.values_mut() {
        items.sort_by(|left, right| {
            let score_order = match direction {
                ScoreDirection::Descending => right.1.partial_cmp(&left.1),
                ScoreDirection::Ascending => left.1.partial_cmp(&right.1),
            }
            .unwrap_or(std::cmp::Ordering::Equal);
            score_order
                .then_with(|| left.2.unwrap_or(i32::MAX).cmp(&right.2.unwrap_or(i32::MAX)))
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

async fn load_prediction_scores_by_date(
    pool: &PgPool,
    prediction_set_id: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<PredictionScoresByDate, String> {
    let rows: Vec<(String, NaiveDate, f64, Option<i32>)> = sqlx::query_as(
        "SELECT symbol, trade_date, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND available_at <= trade_date
         ORDER BY trade_date, symbol",
    )
    .bind(prediction_set_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load model predictions for blend: {}", e))?;

    if rows.is_empty() {
        return Err(format!(
            "No model predictions found for blend prediction_set_id={}",
            prediction_set_id
        ));
    }

    let mut scores_by_date = HashMap::new();
    for (symbol, trade_date, score, rank) in rows {
        if score.is_finite() {
            scores_by_date
                .entry(trade_date)
                .or_insert_with(Vec::new)
                .push((symbol, score, rank));
        }
    }
    Ok(scores_by_date)
}

async fn load_prediction_scores_by_date_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    prediction_set_id: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<PredictionScoresByDate>, String> {
    let key = SignalDataCacheKey::prediction_scores(prediction_set_id, start_date, end_date);
    if let Some(scores) = cache.cached_prediction_scores(&key) {
        return Ok(scores);
    }

    let scores =
        load_prediction_scores_by_date(pool, prediction_set_id, start_date, end_date).await?;
    Ok(cache.insert_prediction_scores(key, scores))
}

fn blend_factor_prediction_scores(
    factor_scores: &mut FactorScoresByDate,
    prediction_scores: &PredictionScoresByDate,
    blend: &PredictionBlendConfig,
    score_direction: ScoreDirection,
) {
    let factor_weight = blend.factor_weight.max(0.0);
    let prediction_weight = blend.prediction_weight.max(0.0);
    let gross_weight = factor_weight + prediction_weight;
    if gross_weight <= f64::EPSILON {
        return;
    }
    let factor_weight = factor_weight / gross_weight;
    let prediction_weight = prediction_weight / gross_weight;

    factor_scores.retain(|date, rows| {
        let Some(predictions) = prediction_scores.get(date) else {
            return false;
        };
        let prediction_percentiles = prediction_percentiles_by_symbol(
            predictions
                .iter()
                .map(|(symbol, score, _)| (symbol.as_str(), *score)),
        );
        let min_prediction_percentile = blend
            .prediction_min_percentile
            .map(|value| value.clamp(0.0, 1.0));
        let paired = rows
            .iter()
            .filter_map(|(symbol, factor_score)| {
                let (prediction_score, prediction_percentile) =
                    prediction_percentiles.get(symbol.as_str())?;
                if min_prediction_percentile
                    .map(|threshold| *prediction_percentile < threshold)
                    .unwrap_or(false)
                {
                    return None;
                }
                if factor_score.is_finite() && prediction_score.is_finite() {
                    Some((symbol.clone(), *factor_score, *prediction_score))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if paired.is_empty() {
            return false;
        }

        let factor_stats = score_stats(paired.iter().map(|(_, score, _)| *score));
        let prediction_stats = score_stats(paired.iter().map(|(_, _, score)| *score));
        *rows = paired
            .into_iter()
            .map(|(symbol, factor_score, prediction_score)| {
                let prediction_score = standard_score(prediction_score, prediction_stats);
                let prediction_score = match score_direction {
                    ScoreDirection::Descending => prediction_score,
                    ScoreDirection::Ascending => -prediction_score,
                };
                let score = factor_weight * standard_score(factor_score, factor_stats)
                    + prediction_weight * prediction_score;
                (symbol, score)
            })
            .collect();
        true
    });
}

async fn load_event_gate_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    event_gate: &EventGateConfig,
    score_days: &[NaiveDate],
) -> Result<Arc<FactorScoresByDate>, String> {
    let mut gate_config = SignalConfig {
        combo_name: event_gate.combo_name.clone(),
        version: event_gate.version.clone(),
        score_direction: event_gate.score_direction,
        score_candidate_pool_size: None,
        universe_profile: TradableUniverseProfile::All,
        event_gate: None,
        prediction_blend: None,
        ..Default::default()
    };
    gate_config.min_daily_amount_cny = None;
    load_combo_scores_for_dates_cached(pool, cache, &gate_config, score_days).await
}

fn apply_event_gate_scores(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
) {
    apply_event_gate_scores_when(factor_scores, event_scores, gate, |_| true);
}

fn apply_event_gate_scores_for_regime<F>(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
    regime_for_date: F,
) where
    F: Fn(NaiveDate) -> MarketRegime,
{
    apply_event_gate_scores_when(factor_scores, event_scores, gate, |date| {
        gate.active_regimes.is_empty() || gate.active_regimes.contains(&regime_for_date(date))
    });
}

fn apply_event_gate_scores_when<F>(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
    active_for_date: F,
) where
    F: Fn(NaiveDate) -> bool,
{
    let min_score = if gate.min_score.is_finite() {
        gate.min_score
    } else {
        0.0
    };
    let boost_weight = gate.boost_weight.max(0.0);
    factor_scores.retain(|date, rows| {
        if !active_for_date(*date) {
            return true;
        }
        let event_by_symbol = event_scores
            .get(date)
            .map(|scores| {
                scores
                    .iter()
                    .filter(|(_, score)| score.is_finite())
                    .map(|(symbol, score)| (symbol.as_str(), *score))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        match gate.mode {
            EventGateMode::BoostPositive => {
                let event_stats = score_stats(event_by_symbol.values().copied());
                for (symbol, factor_score) in rows.iter_mut() {
                    let Some(event_score) = event_by_symbol.get(symbol.as_str()).copied() else {
                        continue;
                    };
                    if event_score > min_score && boost_weight > 0.0 {
                        *factor_score += boost_weight * standard_score(event_score, event_stats);
                    }
                }
            }
            EventGateMode::ExcludeNegative => {
                rows.retain(|(symbol, _)| {
                    event_by_symbol
                        .get(symbol.as_str())
                        .map(|score| *score >= min_score)
                        .unwrap_or(true)
                });
            }
            EventGateMode::RequirePositive => {
                rows.retain(|(symbol, _)| {
                    event_by_symbol
                        .get(symbol.as_str())
                        .map(|score| *score > min_score)
                        .unwrap_or(false)
                });
            }
        }
        !rows.is_empty()
    });
}

fn prediction_percentiles_by_symbol<'a>(
    predictions: impl Iterator<Item = (&'a str, f64)>,
) -> HashMap<&'a str, (f64, f64)> {
    let mut finite = predictions
        .filter(|(_, score)| score.is_finite())
        .collect::<Vec<_>>();
    finite.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let denominator = finite.len().saturating_sub(1).max(1) as f64;
    finite
        .into_iter()
        .enumerate()
        .map(|(index, (symbol, score))| (symbol, (score, index as f64 / denominator)))
        .collect()
}

fn score_stats(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return (0.0, 1.0);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len().max(1) as f64;
    let std_dev = variance.sqrt();
    (mean, if std_dev > f64::EPSILON { std_dev } else { 1.0 })
}

fn standard_score(value: f64, (mean, std_dev): (f64, f64)) -> f64 {
    (value - mean) / std_dev
}

async fn load_combo_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
) -> Result<Arc<FactorScoresByDate>, String> {
    let score_days = normalized_dates(score_days);
    if score_days.is_empty() {
        return Ok(Arc::new(HashMap::new()));
    }

    let score_candidate_pool_size =
        normalize_score_candidate_pool_size(config.score_candidate_pool_size);
    let mut scores_by_date: FactorScoresByDate = HashMap::new();
    let mut missing_days = Vec::new();
    for day in &score_days {
        let key = SignalDataCacheKey::combo_scores(
            &config.combo_name,
            &config.version,
            *day,
            *day,
            config.score_direction,
            score_candidate_pool_size,
            config.universe_profile,
        );
        if let Some(cached) = cache.cached_combo_scores(&key) {
            scores_by_date.extend(cached.as_ref().clone());
        } else {
            missing_days.push(*day);
        }
    }

    if !missing_days.is_empty() {
        let sql = combo_score_load_dates_sql(
            config.score_direction,
            score_candidate_pool_size,
            config.universe_profile,
        );
        let mut query = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(&sql)
            .bind(&config.combo_name)
            .bind(&config.version)
            .bind(&missing_days);
        if let Some(limit) = score_candidate_pool_size {
            query = query.bind(limit as i64);
        }
        let rows = query
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Failed to load combo scores for sparse dates: {}", e))?;
        let loaded_scores = factor_scores_by_date_from_rows(rows);

        for day in missing_days {
            let day_scores = loaded_scores.get(&day).cloned().unwrap_or_default();
            let day_map = if day_scores.is_empty() {
                HashMap::new()
            } else {
                HashMap::from([(day, day_scores.clone())])
            };
            let key = SignalDataCacheKey::combo_scores(
                &config.combo_name,
                &config.version,
                day,
                day,
                config.score_direction,
                score_candidate_pool_size,
                config.universe_profile,
            );
            cache.insert_combo_scores(key, day_map);
            if !day_scores.is_empty() {
                scores_by_date.insert(day, day_scores);
            }
        }
    }

    if scores_by_date.is_empty() {
        return Err("No combo scores found".into());
    }

    Ok(Arc::new(scores_by_date))
}

fn factor_scores_by_date_from_rows(
    rows: Vec<(String, NaiveDate, Option<f64>)>,
) -> FactorScoresByDate {
    let mut scores_by_date: FactorScoresByDate = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }
    scores_by_date
}

fn normalize_score_candidate_pool_size(value: Option<usize>) -> Option<usize> {
    value.filter(|size| *size > 0)
}

#[cfg(test)]
fn combo_score_load_sql(
    score_direction: ScoreDirection,
    score_candidate_pool_size: Option<usize>,
    universe_profile: TradableUniverseProfile,
) -> String {
    let universe_join = tradable_universe_join_sql(universe_profile);
    let universe_filter = tradable_universe_filter_sql(universe_profile)
        .map(|filter| format!("\n           AND {filter}"))
        .unwrap_or_default();
    let Some(_) = normalize_score_candidate_pool_size(score_candidate_pool_size) else {
        return format!(
            "SELECT mfv.symbol, mfv.trade_date, mfv.raw_score
         FROM multi_factor_value mfv{universe_join}
         WHERE mfv.combo_name = $1 AND mfv.version = $2
           AND mfv.trade_date >= $3 AND mfv.trade_date <= $4
           AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ORDER BY mfv.trade_date, mfv.symbol"
        );
    };

    let score_order = match score_direction {
        ScoreDirection::Descending => "COALESCE(mfv.raw_score, 0.0) DESC",
        ScoreDirection::Ascending => "COALESCE(mfv.raw_score, 0.0) ASC",
    };
    format!(
        "SELECT symbol, trade_date, raw_score
         FROM (
             SELECT mfv.symbol, mfv.trade_date, mfv.raw_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY mfv.trade_date
                        ORDER BY {score_order}, mfv.symbol ASC
                    ) AS score_rank
             FROM multi_factor_value mfv{universe_join}
             WHERE mfv.combo_name = $1 AND mfv.version = $2
               AND mfv.trade_date >= $3 AND mfv.trade_date <= $4
               AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ) ranked
         WHERE score_rank <= $5
         ORDER BY trade_date, score_rank, symbol"
    )
}

fn combo_score_load_dates_sql(
    score_direction: ScoreDirection,
    score_candidate_pool_size: Option<usize>,
    universe_profile: TradableUniverseProfile,
) -> String {
    let universe_join = tradable_universe_join_sql(universe_profile);
    let universe_filter = tradable_universe_filter_sql(universe_profile)
        .map(|filter| format!("\n           AND {filter}"))
        .unwrap_or_default();
    let Some(_) = normalize_score_candidate_pool_size(score_candidate_pool_size) else {
        return format!(
            "SELECT mfv.symbol, mfv.trade_date, mfv.raw_score
         FROM multi_factor_value mfv{universe_join}
         WHERE mfv.combo_name = $1 AND mfv.version = $2
           AND mfv.trade_date = ANY($3)
           AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ORDER BY mfv.trade_date, mfv.symbol"
        );
    };

    let score_order = match score_direction {
        ScoreDirection::Descending => "COALESCE(mfv.raw_score, 0.0) DESC",
        ScoreDirection::Ascending => "COALESCE(mfv.raw_score, 0.0) ASC",
    };
    format!(
        "SELECT symbol, trade_date, raw_score
         FROM (
             SELECT mfv.symbol, mfv.trade_date, mfv.raw_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY mfv.trade_date
                        ORDER BY {score_order}, mfv.symbol ASC
                    ) AS score_rank
             FROM multi_factor_value mfv{universe_join}
             WHERE mfv.combo_name = $1 AND mfv.version = $2
               AND mfv.trade_date = ANY($3)
               AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ) ranked
         WHERE score_rank <= $4
         ORDER BY trade_date, score_rank, symbol"
    )
}

fn tradable_universe_join_sql(profile: TradableUniverseProfile) -> &'static str {
    match profile {
        TradableUniverseProfile::All => "",
        TradableUniverseProfile::ListedNonSt | TradableUniverseProfile::MainBoardNonSt => {
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol"
        }
    }
}

fn tradable_universe_filter_sql(profile: TradableUniverseProfile) -> Option<&'static str> {
    match profile {
        TradableUniverseProfile::All => None,
        TradableUniverseProfile::ListedNonSt => {
            Some("ms.list_status = 'L' AND COALESCE(ms.is_st, false) = false")
        }
        TradableUniverseProfile::MainBoardNonSt => Some(
            "ms.list_status = 'L'
           AND COALESCE(ms.is_st, false) = false
           AND ms.exchange IN ('SSE', 'SZSE')
           AND COALESCE(ms.market, '') NOT ILIKE '%创业%'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'",
        ),
    }
}

async fn apply_factor_liquidity_filter(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    scores_by_date: &mut FactorScoresByDate,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), String> {
    let Some(min_amount) = config.min_daily_amount_cny else {
        return Ok(());
    };
    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if all_symbols.is_empty() {
        return Ok(());
    }

    let average_amounts =
        load_average_amounts_cached(pool, cache, &all_symbols, start_date, end_date).await?;
    let stats =
        retain_scores_with_min_average_amount(scores_by_date, min_amount, average_amounts.as_ref());

    info!(
        "Liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
        min_amount as u64, stats.after, stats.before, stats.liquid_symbols
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiquidityFilterStats {
    before: usize,
    after: usize,
    liquid_symbols: usize,
}

fn retain_scores_with_min_average_amount(
    scores_by_date: &mut FactorScoresByDate,
    min_amount_cny: f64,
    average_amounts: &AverageAmounts,
) -> LiquidityFilterStats {
    let min_amount_1k = min_amount_cny / 1000.0;
    let liquid_symbols: HashSet<&str> = average_amounts
        .iter()
        .filter_map(|(symbol, amount)| {
            if *amount >= min_amount_1k {
                Some(symbol.as_str())
            } else {
                None
            }
        })
        .collect();
    let before: usize = scores_by_date.values().flatten().count();
    for stocks in scores_by_date.values_mut() {
        stocks.retain(|(symbol, _)| liquid_symbols.contains(symbol.as_str()));
    }
    let after: usize = scores_by_date.values().flatten().count();

    LiquidityFilterStats {
        before,
        after,
        liquid_symbols: liquid_symbols.len(),
    }
}

fn build_pit_average_amounts_by_date(
    amount_history: &AverageAmountHistory,
    as_of_dates: &[NaiveDate],
    lookback_days: usize,
) -> AverageAmountsByDate {
    if amount_history.is_empty() || as_of_dates.is_empty() {
        return HashMap::new();
    }
    let mut dates = as_of_dates.to_vec();
    dates.sort_unstable();
    dates.dedup();
    let lookback_days = lookback_days.max(1);
    let mut amounts_by_date: AverageAmountsByDate =
        dates.iter().map(|date| (*date, HashMap::new())).collect();

    for (symbol, rows) in amount_history {
        let mut rows = rows
            .iter()
            .copied()
            .filter(|(_, amount)| amount.is_finite() && *amount > 0.0)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        rows.sort_unstable_by_key(|(date, _)| *date);

        let mut left = 0usize;
        let mut right = 0usize;
        let mut amount_sum = 0.0;
        for as_of in &dates {
            while right < rows.len() && rows[right].0 <= *as_of {
                amount_sum += rows[right].1;
                right += 1;
            }
            while right.saturating_sub(left) > lookback_days {
                amount_sum -= rows[left].1;
                left += 1;
            }
            let count = right.saturating_sub(left);
            if count == 0 {
                continue;
            }
            amounts_by_date
                .entry(*as_of)
                .or_default()
                .insert(symbol.clone(), amount_sum / count as f64);
        }
    }

    amounts_by_date.retain(|_, amounts| !amounts.is_empty());
    amounts_by_date
}

fn normalized_dates(dates: &[NaiveDate]) -> Vec<NaiveDate> {
    let mut dates = dates.to_vec();
    dates.sort_unstable();
    dates.dedup();
    dates
}

fn date_span(dates: &[NaiveDate]) -> Option<(NaiveDate, NaiveDate)> {
    let dates = normalized_dates(dates);
    Some((*dates.first()?, *dates.last()?))
}

fn average_amounts_for_score_day(
    average_amounts_by_date: &AverageAmountsByDate,
    score_day: NaiveDate,
) -> AverageAmounts {
    average_amounts_by_date
        .get(&score_day)
        .cloned()
        .unwrap_or_default()
}

fn build_rebalance_factor_signals<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    build_rebalance_factor_signals_with_score_selector(
        trading_days,
        base_config,
        return_history,
        average_amounts_by_date,
        industry_by_symbol,
        |score_day, _active_config| scores_by_date.get(&score_day).cloned(),
        active_config_for_day,
    )
}

fn build_rebalance_factor_signals_with_score_selector<F, S>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    scores_for_day: S,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();
    let mut previous_target_weights: Option<HashMap<String, Decimal>> = None;

    for (i, &day) in trading_days.iter().enumerate() {
        let active_config = active_config_for_day(day, base_config);
        let min_idx = 1 + active_config.entry_delay_days;
        if i < min_idx {
            continue;
        }
        if (i - min_idx) % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }

        let score_day = match score_day_for_signal(trading_days, i, &active_config) {
            Some(day) => day,
            None => continue,
        };
        let average_amounts = average_amounts_for_score_day(average_amounts_by_date, score_day);
        let mut target_weights = match build_portfolio_sleeve_target_weights(
            score_day,
            &active_config,
            return_history,
            &average_amounts,
            industry_by_symbol,
            &scores_for_day,
        ) {
            Some(weights) => weights,
            None => continue,
        };
        apply_rebalance_path_smoothing(
            &mut target_weights,
            previous_target_weights.as_ref(),
            active_config.rebalance_hysteresis_pct,
            active_config.partial_rebalance_ratio,
        );
        apply_execution_impact_budget(
            &mut target_weights,
            previous_target_weights.as_ref(),
            active_config.execution_impact_budget_profile,
        );

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights: target_weights.clone(),
            },
        );
        previous_target_weights = Some(target_weights);
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

fn build_portfolio_sleeve_target_weights<S>(
    score_day: NaiveDate,
    active_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    scores_for_day: &S,
) -> Option<HashMap<String, Decimal>>
where
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut base_config = active_config.clone();
    base_config.portfolio_sleeve = None;
    let base_weights = build_single_sleeve_target_weights(
        score_day,
        &base_config,
        return_history,
        average_amounts,
        industry_by_symbol,
        scores_for_day,
    )?;

    let Some(sleeve) = active_config.portfolio_sleeve.as_ref() else {
        return Some(base_weights);
    };
    let sleeve_weight = sleeve.weight.clamp(0.0, 1.0);
    if sleeve_weight <= f64::EPSILON {
        return Some(base_weights);
    }
    let sleeve_config = score_source_config_for_portfolio_sleeve(active_config, sleeve);
    let Some(sleeve_weights) = build_single_sleeve_target_weights(
        score_day,
        &sleeve_config,
        return_history,
        average_amounts,
        industry_by_symbol,
        scores_for_day,
    ) else {
        return Some(base_weights);
    };

    Some(blend_portfolio_sleeve_weights(
        base_weights,
        1.0 - sleeve_weight,
        sleeve_weights,
        sleeve_weight,
    ))
}

fn build_single_sleeve_target_weights<S>(
    score_day: NaiveDate,
    config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    scores_for_day: &S,
) -> Option<HashMap<String, Decimal>>
where
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut prev_scores = scores_for_day(score_day, config)?;
    sort_factor_scores(&mut prev_scores, config.score_direction);

    let skip_count = if config.skip_top_pct > 0.0 {
        (prev_scores.len() as f64 * config.skip_top_pct).ceil() as usize
    } else {
        0
    };
    let candidates: Vec<(String, f64)> = prev_scores
        .iter()
        .skip(skip_count)
        .map(|(symbol, score)| (symbol.clone(), *score))
        .collect();
    if candidates.len() < config.top_n.min(5) {
        return None;
    }

    let portfolio_config = PortfolioConstructionConfig::from(config);
    let target_weights = build_portfolio_weights(
        score_day,
        &candidates,
        return_history,
        average_amounts,
        industry_by_symbol,
        &portfolio_config,
    );
    if target_weights.len() < config.top_n.min(5) {
        return None;
    }
    Some(target_weights)
}

fn blend_portfolio_sleeve_weights(
    base_weights: HashMap<String, Decimal>,
    base_weight: f64,
    sleeve_weights: HashMap<String, Decimal>,
    sleeve_weight: f64,
) -> HashMap<String, Decimal> {
    let base_weight = decimal_from_unit_f64(base_weight);
    let sleeve_weight = decimal_from_unit_f64(sleeve_weight);
    let mut blended = HashMap::new();

    for (symbol, weight) in base_weights {
        let scaled = weight * base_weight;
        if scaled > Decimal::ZERO {
            blended.insert(symbol, scaled);
        }
    }
    for (symbol, weight) in sleeve_weights {
        let scaled = weight * sleeve_weight;
        if scaled > Decimal::ZERO {
            *blended.entry(symbol).or_insert(Decimal::ZERO) += scaled;
        }
    }
    blended.retain(|_, weight| *weight > Decimal::ZERO);
    blended
}

fn decimal_from_unit_f64(value: f64) -> Decimal {
    Decimal::from_f64(value.clamp(0.0, 1.0)).unwrap_or(Decimal::ZERO)
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

async fn load_open_trading_days_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<Vec<NaiveDate>>, String> {
    let key = SignalDataCacheKey::trading_days(start_date, end_date);
    if let Some(days) = cache.cached_trading_days(&key) {
        return Ok(days);
    }
    let days = load_open_trading_days(pool, start_date, end_date).await?;
    Ok(cache.insert_trading_days(key, days))
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
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let min_idx = 1 + config.entry_delay_days;
    let mut signals = HashMap::new();
    let mut previous_target_weights: Option<HashMap<String, Decimal>> = None;

    for (i, &day) in trading_days.iter().enumerate() {
        if i < min_idx || (i - min_idx) % config.rebalance_freq_days.max(1) != 0 {
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

        let average_amounts = average_amounts_for_score_day(average_amounts_by_date, score_day);
        let mut target_weights = build_portfolio_weights(
            score_day,
            &candidates,
            return_history,
            &average_amounts,
            industry_by_symbol,
            &PortfolioConstructionConfig::from(config),
        );
        if target_weights.len() < config.top_n.min(5) {
            continue;
        }
        apply_rebalance_path_smoothing(
            &mut target_weights,
            previous_target_weights.as_ref(),
            config.rebalance_hysteresis_pct,
            config.partial_rebalance_ratio,
        );
        apply_execution_impact_budget(
            &mut target_weights,
            previous_target_weights.as_ref(),
            config.execution_impact_budget_profile,
        );

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights: target_weights.clone(),
            },
        );
        previous_target_weights = Some(target_weights);
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

fn apply_rebalance_path_smoothing(
    target_weights: &mut HashMap<String, Decimal>,
    previous_target_weights: Option<&HashMap<String, Decimal>>,
    rebalance_hysteresis_pct: f64,
    partial_rebalance_ratio: f64,
) {
    let Some(previous_target_weights) = previous_target_weights else {
        return;
    };
    let hysteresis = finite_decimal(rebalance_hysteresis_pct, 0.0, 0.0, 1.0);
    let partial = finite_decimal(partial_rebalance_ratio, 1.0, 0.0, 1.0);
    if hysteresis.is_zero() && partial == Decimal::ONE {
        return;
    }
    if target_weights.is_empty() {
        return;
    }

    let target_gross = target_weights.values().copied().sum::<Decimal>();
    if target_gross.is_zero() {
        target_weights.clear();
        return;
    }

    let raw_target_weights = target_weights.clone();
    let mut symbols = previous_target_weights
        .keys()
        .chain(raw_target_weights.keys())
        .cloned()
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();

    target_weights.clear();
    for symbol in symbols {
        let previous = previous_target_weights
            .get(&symbol)
            .copied()
            .unwrap_or_default();
        let target = raw_target_weights.get(&symbol).copied().unwrap_or_default();
        let delta = target - previous;
        let adjusted = if delta.abs() <= hysteresis {
            previous
        } else {
            previous + delta * partial
        };
        if adjusted > Decimal::ZERO {
            target_weights.insert(symbol, adjusted);
        }
    }

    let adjusted_gross = target_weights.values().copied().sum::<Decimal>();
    if adjusted_gross > target_gross && !adjusted_gross.is_zero() {
        let scale = target_gross / adjusted_gross;
        for weight in target_weights.values_mut() {
            *weight *= scale;
        }
    }
    target_weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn apply_execution_impact_budget(
    target_weights: &mut HashMap<String, Decimal>,
    previous_target_weights: Option<&HashMap<String, Decimal>>,
    profile: ExecutionImpactBudgetProfile,
) {
    let Some(params) = profile.params() else {
        return;
    };
    let Some(previous_target_weights) = previous_target_weights else {
        return;
    };
    if target_weights.is_empty() || previous_target_weights.is_empty() {
        return;
    }

    let max_turnover =
        finite_decimal(params.max_rebalance_turnover_pct, 1.0, 0.0, 2.0).max(Decimal::ZERO);
    if max_turnover.is_zero() {
        target_weights.clear();
        return;
    }
    let max_new_name_weight =
        finite_decimal(params.max_new_name_weight_pct, 1.0, 0.0, 1.0).max(Decimal::ZERO);

    let mut desired = target_weights.clone();
    let mut released = Decimal::ZERO;
    let previous_symbols = previous_target_weights
        .iter()
        .filter(|(_, weight)| **weight > Decimal::ZERO)
        .map(|(symbol, _)| symbol.clone())
        .collect::<HashSet<_>>();

    for (symbol, weight) in desired.iter_mut() {
        let previous = previous_target_weights
            .get(symbol)
            .copied()
            .unwrap_or_default();
        if previous.is_zero() && *weight > max_new_name_weight {
            released += *weight - max_new_name_weight;
            *weight = max_new_name_weight;
        }
    }

    if released > Decimal::ZERO && !previous_symbols.is_empty() {
        redistribute_released_weight_to_existing_positions(
            &mut desired,
            previous_target_weights,
            &previous_symbols,
            released,
        );
    }

    let mut symbols = previous_target_weights
        .keys()
        .chain(desired.keys())
        .cloned()
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();

    let gross_turnover = symbols.iter().fold(Decimal::ZERO, |acc, symbol| {
        let previous = previous_target_weights
            .get(symbol)
            .copied()
            .unwrap_or_default();
        let target = desired.get(symbol).copied().unwrap_or_default();
        acc + (target - previous).abs()
    });

    if gross_turnover.is_zero() {
        target_weights.clear();
        for (symbol, weight) in previous_target_weights {
            if *weight > Decimal::ZERO {
                target_weights.insert(symbol.clone(), *weight);
            }
        }
        return;
    }

    let scale = if gross_turnover > max_turnover {
        max_turnover / gross_turnover
    } else {
        Decimal::ONE
    };

    target_weights.clear();
    for symbol in symbols {
        let previous = previous_target_weights
            .get(&symbol)
            .copied()
            .unwrap_or_default();
        let target = desired.get(&symbol).copied().unwrap_or_default();
        let adjusted = previous + (target - previous) * scale;
        if adjusted > Decimal::ZERO {
            target_weights.insert(symbol, adjusted);
        }
    }
    target_weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn redistribute_released_weight_to_existing_positions(
    weights: &mut HashMap<String, Decimal>,
    previous_target_weights: &HashMap<String, Decimal>,
    previous_symbols: &HashSet<String>,
    released: Decimal,
) {
    let mut rooms = previous_symbols
        .iter()
        .filter_map(|symbol| {
            let previous = previous_target_weights
                .get(symbol)
                .copied()
                .unwrap_or_default();
            let current = weights.get(symbol).copied().unwrap_or_default();
            let room = (previous - current).max(Decimal::ZERO);
            if room > Decimal::ZERO {
                Some((symbol.clone(), room))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    rooms.sort_by(|(left, _), (right, _)| left.cmp(right));

    let total_room = rooms
        .iter()
        .map(|(_, room)| *room)
        .fold(Decimal::ZERO, |acc, room| acc + room);
    if total_room.is_zero() {
        return;
    }

    let allocation = released.min(total_room);
    for (symbol, room) in rooms {
        let add = allocation * room / total_room;
        if add > Decimal::ZERO {
            *weights.entry(symbol).or_insert(Decimal::ZERO) += add.min(room);
        }
    }
}

fn finite_decimal(value: f64, default: f64, min: f64, max: f64) -> Decimal {
    let bounded = if value.is_finite() {
        value.clamp(min, max)
    } else {
        default
    };
    Decimal::from_f64(bounded).unwrap_or_else(|| Decimal::from_f64(default).unwrap_or_default())
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

fn rebalance_score_days<F>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    active_config_for_day: F,
) -> Vec<NaiveDate>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    let mut score_days = Vec::new();
    for (i, &day) in trading_days.iter().enumerate() {
        let active_config = active_config_for_day(day, base_config);
        let min_idx = 1 + active_config.entry_delay_days;
        if i < min_idx {
            continue;
        }
        if (i - min_idx) % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }
        if let Some(score_day) = score_day_for_signal(trading_days, i, &active_config) {
            score_days.push(score_day);
        }
    }
    normalized_dates(&score_days)
}

fn portfolio_history_lookback_days(config: &PortfolioConstructionConfig) -> usize {
    config
        .correlation_lookback_days
        .max(config.kelly_lookback_days)
        .max(config.risk_budget_lookback_days)
        .max(1)
}

fn return_history_query_start(start_date: NaiveDate, lookback_days: usize) -> NaiveDate {
    start_date - Duration::days((lookback_days as i64).saturating_mul(3))
}

fn average_amount_history_query_start(start_date: NaiveDate, lookback_days: usize) -> NaiveDate {
    start_date - Duration::days((lookback_days as i64).saturating_mul(3).max(1))
}

fn filter_dated_values(
    rows: &[(NaiveDate, f64)],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<(NaiveDate, f64)> {
    rows.iter()
        .copied()
        .filter(|(date, _)| *date >= start_date && *date <= end_date)
        .collect()
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

    let query_start = return_history_query_start(start_date, lookback_days);
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

async fn load_symbol_return_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<SymbolReturnHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_return_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }

    let loaded_history =
        load_symbol_return_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    let loaded_history = cache.insert_return_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
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

async fn load_average_amounts_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<AverageAmounts>, String> {
    let (missing_symbols, mut amounts) =
        cache.cached_average_amount_symbols(symbols, start_date, end_date);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(amounts));
    }
    let loaded_amounts = load_average_amounts(pool, &missing_symbols, start_date, end_date).await?;
    let loaded_amounts =
        cache.insert_average_amount_symbols(&missing_symbols, start_date, end_date, loaded_amounts);
    amounts.extend(loaded_amounts);
    Ok(Arc::new(amounts))
}

async fn load_average_amount_history(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<AverageAmountHistory, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let query_start = average_amount_history_query_start(start_date, lookback_days);
    let rows: Vec<(String, NaiveDate, Option<Decimal>)> = sqlx::query_as(
        "SELECT symbol, trade_date, amount
         FROM market_stock_daily_bar
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND amount > 0
         ORDER BY symbol, trade_date",
    )
    .bind(symbols)
    .bind(query_start)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load PIT portfolio capacity history: {}", e))?;

    let mut history = HashMap::new();
    for (symbol, trade_date, amount) in rows {
        let Some(amount) = amount
            .and_then(|value| value.to_f64())
            .filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        history
            .entry(symbol)
            .or_insert_with(Vec::new)
            .push((trade_date, amount));
    }
    Ok(history)
}

async fn load_average_amount_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<AverageAmountHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_average_amount_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }
    let loaded_history =
        load_average_amount_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    let loaded_history = cache.insert_average_amount_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
}

async fn load_industry_classifications(
    pool: &PgPool,
    symbols: &[String],
) -> Result<HashMap<String, String>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT symbol, industry
         FROM market_stock
         WHERE symbol = ANY($1)
           AND industry IS NOT NULL
           AND trim(industry) <> ''",
    )
    .bind(symbols)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load industry classifications: {}", e))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, industry)| (symbol, industry.trim().to_string()))
        .collect())
}

async fn load_industry_classifications_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
) -> Result<Arc<IndustryMap>, String> {
    let key = SignalDataCacheKey::industry_classifications(symbols);
    if let Some(industries) = cache.cached_industry_classifications(&key) {
        return Ok(industries);
    }
    let industries = load_industry_classifications(pool, symbols).await?;
    Ok(cache.insert_industry_classifications(key, industries))
}

async fn load_portfolio_capacity_inputs(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    trading_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
) -> Result<AverageAmountsByDate, String> {
    if config.uses_capacity_inputs() {
        let history = load_average_amount_history(
            pool,
            symbols,
            start_date,
            end_date,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await?;
        Ok(build_pit_average_amounts_by_date(
            &history,
            trading_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        ))
    } else {
        Ok(HashMap::new())
    }
}

async fn load_portfolio_capacity_inputs_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    trading_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
) -> Result<Arc<AverageAmountsByDate>, String> {
    if config.uses_capacity_inputs() {
        let history = load_average_amount_history_cached(
            pool,
            cache,
            symbols,
            start_date,
            end_date,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await?;
        Ok(Arc::new(build_pit_average_amounts_by_date(
            history.as_ref(),
            trading_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )))
    } else {
        Ok(Arc::new(HashMap::new()))
    }
}

async fn load_portfolio_industry_inputs(
    pool: &PgPool,
    symbols: &[String],
    config: &PortfolioConstructionConfig,
) -> Result<HashMap<String, String>, String> {
    if config.max_industry_weight_pct.is_none() {
        Ok(HashMap::new())
    } else {
        load_industry_classifications(pool, symbols).await
    }
}

async fn load_portfolio_industry_inputs_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    config: &PortfolioConstructionConfig,
) -> Result<Arc<IndustryMap>, String> {
    if config.max_industry_weight_pct.is_none() {
        Ok(Arc::new(HashMap::new()))
    } else {
        load_industry_classifications_cached(pool, cache, symbols).await
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

async fn load_benchmark_return_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    benchmark: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<BenchmarkReturns>, String> {
    let key = SignalDataCacheKey::benchmark_returns(benchmark, start_date, end_date, lookback_days);
    if let Some(returns) = cache.cached_benchmark_returns(&key) {
        return Ok(returns);
    }
    let returns =
        load_benchmark_return_history(pool, benchmark, start_date, end_date, lookback_days).await?;
    Ok(cache.insert_benchmark_returns(key, returns))
}

fn build_portfolio_weights(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let ranked_candidates = rank_candidates_for_capacity(
        score_day,
        candidates,
        return_history,
        average_amounts,
        config.candidate_ranking_profile,
        config.risk_budget_lookback_days,
    );
    let risk_filtered_candidates = filter_candidate_risk_pool(
        score_day,
        &ranked_candidates,
        return_history,
        average_amounts,
        config,
    );
    let selected = select_uncorrelated_candidates(
        score_day,
        &risk_filtered_candidates,
        return_history,
        config,
        cash_utilization_selection_limit(&risk_filtered_candidates, average_amounts, config),
    );
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
        PortfolioConstructionMethod::MinVariance => build_min_variance_raw_weights(
            score_day,
            &selected,
            return_history,
            average_amounts,
            config,
        ),
    };

    let mut weights = normalize_and_cap_weights(&selected, &raw_weights, average_amounts, config);
    apply_capacity_risk_budget(&mut weights, average_amounts, config);
    apply_style_risk_budget(
        &mut weights,
        return_history,
        average_amounts,
        score_day,
        config,
    );
    apply_industry_cap(&mut weights, industry_by_symbol, config);
    apply_risk_contribution_control(&mut weights, return_history, score_day, config);
    weights
}

fn rank_candidates_for_capacity(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    profile: CandidateRankingProfile,
    lookback_days: usize,
) -> Vec<(String, f64)> {
    let Some(params) = profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= 1 {
        return candidates.to_vec();
    }

    let amount_ranks = liquidity_rank_scores(candidates, average_amounts);
    let relative_strength_ranks =
        relative_strength_rank_scores(candidates, return_history, score_day, lookback_days);
    let denominator = candidates.len().saturating_sub(1).max(1) as f64;
    let alpha_weight = params.alpha_rank_weight.max(0.0);
    let liquidity_weight = params.liquidity_rank_weight.max(0.0);
    let relative_strength_weight = params.relative_strength_rank_weight.max(0.0);
    let weight_sum = (alpha_weight + liquidity_weight + relative_strength_weight).max(f64::EPSILON);
    let mut ranked = candidates
        .iter()
        .enumerate()
        .map(|(idx, (symbol, score))| {
            let alpha_rank = 1.0 - (idx as f64 / denominator);
            let liquidity_rank = amount_ranks.get(symbol).copied().unwrap_or(0.5);
            let relative_strength_rank =
                relative_strength_ranks.get(symbol).copied().unwrap_or(0.5);
            let blended_rank = (alpha_weight * alpha_rank
                + liquidity_weight * liquidity_rank
                + relative_strength_weight * relative_strength_rank)
                / weight_sum;
            (idx, symbol.clone(), *score, blended_rank)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .map(|(_, symbol, score, _)| (symbol, score))
        .collect()
}

fn relative_strength_rank_scores(
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    lookback_days: usize,
) -> HashMap<String, f64> {
    let mut ranked_returns = candidates
        .iter()
        .enumerate()
        .filter_map(|(idx, (symbol, _))| {
            let returns = trailing_returns(return_history, symbol, score_day, lookback_days);
            trailing_total_return(&returns).map(|total_return| (idx, symbol.clone(), total_return))
        })
        .collect::<Vec<_>>();
    if ranked_returns.is_empty() {
        return HashMap::new();
    }
    ranked_returns.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    let denominator = ranked_returns.len().saturating_sub(1).max(1) as f64;
    ranked_returns
        .into_iter()
        .enumerate()
        .map(|(rank, (_, symbol, _))| (symbol, 1.0 - (rank as f64 / denominator)))
        .collect()
}

fn trailing_total_return(returns: &[f64]) -> Option<f64> {
    let mut seen = false;
    let total_return = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > -1.0)
        .fold(1.0, |acc, value| {
            seen = true;
            acc * (1.0 + value)
        })
        - 1.0;
    seen.then_some(total_return)
}

fn liquidity_rank_scores(
    candidates: &[(String, f64)],
    average_amounts: &HashMap<String, f64>,
) -> HashMap<String, f64> {
    let mut ranked_amounts = candidates
        .iter()
        .enumerate()
        .filter_map(|(idx, (symbol, _))| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (idx, symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    ranked_amounts.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    let denominator = ranked_amounts.len().saturating_sub(1).max(1) as f64;
    ranked_amounts
        .into_iter()
        .enumerate()
        .map(|(rank, (_, symbol, _))| (symbol, 1.0 - (rank as f64 / denominator)))
        .collect()
}

fn select_uncorrelated_candidates(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    config: &PortfolioConstructionConfig,
    selection_limit: usize,
) -> Vec<String> {
    let mut selected: Vec<String> = Vec::new();
    let selection_limit = selection_limit.max(config.top_n).min(candidates.len());
    for (symbol, _) in candidates {
        if selected.len() >= selection_limit {
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

fn cash_utilization_selection_limit(
    candidates: &[(String, f64)],
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> usize {
    let base_limit = config.top_n.min(candidates.len());
    let Some(params) = config.cash_utilization_profile.params() else {
        return base_limit;
    };
    if candidates.is_empty() || average_amounts.is_empty() {
        return base_limit;
    }

    let target_gross = config
        .max_gross_exposure
        .clamp(0.0, params.min_gross_exposure_pct.clamp(0.0, 1.0));
    if target_gross <= f64::EPSILON {
        return base_limit;
    }

    let hard_limit = params.max_holdings.max(config.top_n).min(candidates.len());
    let max_position_cap = config
        .max_position_pct
        .to_f64()
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    let mut cumulative_cap = 0.0;
    let mut count = 0usize;
    for (symbol, _) in candidates.iter().take(hard_limit) {
        let participation_cap_multiplier = config
            .capacity_risk_budget_profile
            .params()
            .map(|params| params.participation_cap_multiplier)
            .unwrap_or(1.0);
        let symbol_cap = participation_weight_cap_with_multiplier(
            symbol,
            average_amounts,
            config,
            participation_cap_multiplier,
        )
        .and_then(|value| value.to_f64())
        .unwrap_or(max_position_cap)
        .min(max_position_cap)
        .clamp(0.0, 1.0);
        cumulative_cap += symbol_cap;
        count += 1;
        if count >= config.top_n && cumulative_cap >= target_gross {
            return count;
        }
    }

    hard_limit
}

fn filter_candidate_risk_pool(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<(String, f64)> {
    let Some(params) = config.candidate_risk_filter_profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= config.top_n || candidates.is_empty() {
        return candidates.to_vec();
    }

    let volatility_scores = candidates
        .iter()
        .filter_map(|(symbol, _)| {
            let returns = trailing_returns(
                return_history,
                symbol,
                score_day,
                config.risk_budget_lookback_days,
            );
            sample_volatility(&returns).map(|volatility| (symbol.as_str(), volatility))
        })
        .collect::<Vec<_>>();

    let Some(volatility_threshold) = quantile_value(
        volatility_scores.iter().map(|(_, volatility)| *volatility),
        params.max_volatility_quantile,
    ) else {
        return candidates.to_vec();
    };

    let low_volatility_symbols = volatility_scores
        .iter()
        .filter(|(_, volatility)| *volatility <= volatility_threshold)
        .map(|(symbol, _)| *symbol)
        .collect::<HashSet<_>>();

    let mut filtered = candidates
        .iter()
        .filter(|(symbol, _)| {
            if volatility_scores
                .iter()
                .any(|(known, _)| *known == symbol.as_str())
            {
                low_volatility_symbols.contains(symbol.as_str())
            } else {
                true
            }
        })
        .cloned()
        .collect::<Vec<_>>();

    if let Some(max_average_corr) = params.max_average_abs_correlation {
        let reference_limit = params
            .correlation_reference_limit
            .max(config.top_n.saturating_mul(4))
            .max(20);
        let reference_symbols = filtered
            .iter()
            .take(reference_limit)
            .map(|(symbol, _)| symbol.clone())
            .collect::<Vec<_>>();
        filtered.retain(|(symbol, _)| {
            average_abs_correlation_to_reference(
                symbol,
                &reference_symbols,
                return_history,
                score_day,
                config.risk_budget_lookback_days,
            )
            .map(|corr| corr <= max_average_corr)
            .unwrap_or(true)
        });
    }

    if let Some(min_liquidity_quantile) = params.min_liquidity_quantile {
        if let Some(liquidity_threshold) = quantile_value(
            filtered
                .iter()
                .filter_map(|(symbol, _)| average_amounts.get(symbol).copied()),
            min_liquidity_quantile,
        ) {
            let liquidity_filtered = filtered
                .iter()
                .filter(|(symbol, _)| {
                    average_amounts
                        .get(symbol)
                        .map(|amount| *amount >= liquidity_threshold)
                        .unwrap_or(false)
                })
                .cloned()
                .collect::<Vec<_>>();
            if liquidity_filtered.len() >= config.top_n {
                filtered = liquidity_filtered;
            }
        }
    }

    if filtered.len() >= config.top_n {
        filtered
    } else {
        candidates.to_vec()
    }
}

fn quantile_value(values: impl Iterator<Item = f64>, quantile: f64) -> Option<f64> {
    let mut values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((values.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).floor() as usize;
    values.get(idx.min(values.len() - 1)).copied()
}

fn average_abs_correlation_to_reference(
    symbol: &str,
    reference_symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    lookback_days: usize,
) -> Option<f64> {
    let own_returns = trailing_returns(return_history, symbol, score_day, lookback_days);
    if own_returns.len() < 3 {
        return None;
    }
    let correlations = reference_symbols
        .iter()
        .filter(|other| other.as_str() != symbol)
        .filter_map(|other| {
            let other_returns =
                trailing_returns(return_history, other.as_str(), score_day, lookback_days);
            pearson_correlation(&own_returns, &other_returns).map(f64::abs)
        })
        .collect::<Vec<_>>();
    if correlations.is_empty() {
        None
    } else {
        Some(correlations.iter().sum::<f64>() / correlations.len() as f64)
    }
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

fn build_min_variance_raw_weights(
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
        let volatility = sample_volatility(&returns).unwrap_or(0.20).max(0.01);
        let concentration_penalty =
            covariance_concentration_penalty(symbol, symbols, return_history, score_day, config);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        let variance = volatility * volatility;
        let covariance_penalty = concentration_penalty.max(1.0).powi(2);
        let raw = capacity_multiplier / (variance.max(0.0001) * covariance_penalty);
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
    average_amounts: &HashMap<String, f64>,
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
        let symbol_cap = participation_weight_cap(symbol, average_amounts, config)
            .unwrap_or(config.max_position_pct);
        let weight = Decimal::from_f64(normalized)
            .unwrap_or(Decimal::zero())
            .min(config.max_position_pct)
            .min(symbol_cap);
        if !weight.is_zero() {
            target_weights.insert(symbol.clone(), weight);
        }
    }
    target_weights
}

fn participation_weight_cap(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Option<Decimal> {
    participation_weight_cap_with_multiplier(symbol, average_amounts, config, 1.0)
}

fn participation_weight_cap_with_multiplier(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
    multiplier: f64,
) -> Option<Decimal> {
    let participation_rate = config.max_participation_rate?;
    let notional = config.portfolio_notional_cny?;
    let multiplier = multiplier.clamp(0.0, 1.0);
    if !participation_rate.is_finite()
        || !notional.is_finite()
        || !multiplier.is_finite()
        || participation_rate <= 0.0
        || notional <= 0.0
    {
        return None;
    }
    let average_amount = average_amounts.get(symbol).copied()?;
    if !average_amount.is_finite() || average_amount <= 0.0 {
        return None;
    }
    let cap = (average_amount * participation_rate * multiplier / notional).clamp(0.0, 1.0);
    Decimal::from_f64(cap)
}

fn target_weight_cap_with_multiplier(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
    multiplier: f64,
) -> Decimal {
    participation_weight_cap_with_multiplier(symbol, average_amounts, config, multiplier)
        .unwrap_or(config.max_position_pct)
        .min(config.max_position_pct)
        .max(Decimal::ZERO)
}

fn apply_capacity_risk_budget(
    weights: &mut HashMap<String, Decimal>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.capacity_risk_budget_profile.params() else {
        return;
    };
    if weights.is_empty() {
        return;
    }

    let liquidity_scores = weights
        .keys()
        .filter_map(|symbol| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    if liquidity_scores.is_empty() {
        return;
    }

    let mut low_capacity_symbols =
        low_style_bucket_symbols(&liquidity_scores, params.low_capacity_quantile);
    for symbol in weights.keys() {
        let has_valid_amount = average_amounts
            .get(symbol)
            .copied()
            .map(|amount| amount.is_finite() && amount > 0.0)
            .unwrap_or(false);
        if !has_valid_amount {
            low_capacity_symbols.insert(symbol.clone());
        }
    }

    let caps = weights
        .keys()
        .map(|symbol| {
            (
                symbol.clone(),
                target_weight_cap_with_multiplier(
                    symbol,
                    average_amounts,
                    config,
                    params.participation_cap_multiplier,
                ),
            )
        })
        .collect::<HashMap<_, _>>();
    cap_weights_to_symbol_caps(weights, &caps, 8);

    let low_capacity_cap = Decimal::from_f64(params.low_capacity_max_weight_pct.clamp(0.0, 1.0))
        .unwrap_or(Decimal::ONE);
    cap_bucket_and_redistribute(weights, &low_capacity_symbols, low_capacity_cap, &caps, 8);

    if let Some(min_target_gross_exposure_pct) = params.min_target_gross_exposure_pct {
        let target_gross = Decimal::from_f64(
            config
                .max_gross_exposure
                .clamp(0.0, 1.0)
                .min(min_target_gross_exposure_pct.clamp(0.0, 1.0)),
        )
        .unwrap_or(Decimal::ZERO);
        let current_gross = weights.values().copied().sum::<Decimal>();
        if target_gross > current_gross {
            let floor_caps = weights
                .keys()
                .map(|symbol| {
                    (
                        symbol.clone(),
                        target_weight_cap_with_multiplier(
                            symbol,
                            average_amounts,
                            config,
                            params.floor_refill_cap_multiplier,
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();
            match params.floor_refill_mode {
                CapacityFloorRefillMode::ExistingWeight => {
                    redistribute_weight(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::Headroom => {
                    redistribute_weight_by_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::AlphaHeadroom => {
                    redistribute_weight_by_alpha_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::BlendedAlphaHeadroom => {
                    redistribute_weight_by_blended_alpha_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
            }
        }
    }

    if params.refill_gross_exposure {
        let target_gross =
            Decimal::from_f64(config.max_gross_exposure.clamp(0.0, 1.0)).unwrap_or(Decimal::ONE);
        let current_gross = weights.values().copied().sum::<Decimal>();
        if target_gross > current_gross {
            redistribute_weight(
                weights,
                &low_capacity_symbols,
                target_gross - current_gross,
                &caps,
                8,
            );
        }
    }
    weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn cap_weights_to_symbol_caps(
    weights: &mut HashMap<String, Decimal>,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    if weights.is_empty() || caps.is_empty() {
        return Decimal::ZERO;
    }

    let mut excess = Decimal::ZERO;
    for (symbol, weight) in weights.iter_mut() {
        let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
        if *weight > cap {
            excess += *weight - cap;
            *weight = cap;
        }
    }

    if excess <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    redistribute_weight(weights, &HashSet::new(), excess, caps, iterations)
}

fn cap_bucket_and_redistribute(
    weights: &mut HashMap<String, Decimal>,
    bucket_symbols: &HashSet<String>,
    cap: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) {
    if weights.is_empty() || bucket_symbols.is_empty() || cap >= Decimal::ONE {
        return;
    }

    let bucket_total = weights
        .iter()
        .filter(|(symbol, _)| bucket_symbols.contains(*symbol))
        .map(|(_, weight)| *weight)
        .sum::<Decimal>();
    if bucket_total <= cap || bucket_total.is_zero() {
        return;
    }

    let scale = cap / bucket_total;
    for (symbol, weight) in weights.iter_mut() {
        if bucket_symbols.contains(symbol) {
            *weight *= scale;
        }
    }
    redistribute_weight(
        weights,
        bucket_symbols,
        bucket_total - cap,
        caps,
        iterations,
    );
    weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn redistribute_weight(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), *weight, headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let base_sum = eligible
            .iter()
            .map(|(_, weight, _)| *weight)
            .sum::<Decimal>();
        let equal_share = Decimal::ONE / Decimal::from(eligible.len() as u64);
        let mut allocated = Decimal::ZERO;
        for (symbol, weight, headroom) in eligible {
            let share = if base_sum > Decimal::ZERO {
                weight / base_sum
            } else {
                equal_share
            };
            let addition = (remaining * share).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

fn redistribute_weight_by_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let headroom_sum = eligible
            .iter()
            .map(|(_, headroom)| *headroom)
            .sum::<Decimal>();
        if headroom_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, headroom) in eligible {
            let addition = (remaining * headroom / headroom_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

fn redistribute_weight_by_alpha_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    let alpha_weight = (*weight).max(epsilon);
                    Some((symbol.clone(), headroom, alpha_weight * headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let score_sum = eligible.iter().map(|(_, _, score)| *score).sum::<Decimal>();
        if score_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, headroom, score) in eligible {
            let addition = (remaining * score / score_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

fn redistribute_weight_by_blended_alpha_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), *weight, headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let max_weight = eligible
            .iter()
            .map(|(_, weight, _)| *weight)
            .max()
            .unwrap_or(Decimal::ZERO);
        let score_sum = eligible
            .iter()
            .map(|(_, weight, headroom)| {
                let alpha_boost = if max_weight > epsilon {
                    Decimal::ONE + (*weight / max_weight)
                } else {
                    Decimal::ONE
                };
                *headroom * alpha_boost
            })
            .sum::<Decimal>();
        if score_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, weight, headroom) in eligible {
            let alpha_boost = if max_weight > epsilon {
                Decimal::ONE + (weight / max_weight)
            } else {
                Decimal::ONE
            };
            let score = headroom * alpha_boost;
            let addition = (remaining * score / score_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

fn apply_industry_cap(
    weights: &mut HashMap<String, Decimal>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) {
    let Some(cap) = config.max_industry_weight_pct else {
        return;
    };
    if weights.is_empty() || industry_by_symbol.is_empty() || !cap.is_finite() {
        return;
    }
    let cap = cap.clamp(0.0, 1.0);
    if cap >= 1.0 {
        return;
    }
    let cap = Decimal::from_f64(cap).unwrap_or(Decimal::ONE);

    let mut industry_totals: HashMap<&str, Decimal> = HashMap::new();
    for (symbol, weight) in weights.iter() {
        if let Some(industry) = industry_by_symbol
            .get(symbol)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            *industry_totals.entry(industry).or_default() += *weight;
        }
    }

    let industry_scales: HashMap<&str, Decimal> = industry_totals
        .into_iter()
        .filter_map(|(industry, total)| {
            if total > cap && !total.is_zero() {
                Some((industry, cap / total))
            } else {
                None
            }
        })
        .collect();
    if industry_scales.is_empty() {
        return;
    }

    for (symbol, weight) in weights.iter_mut() {
        if let Some(scale) = industry_by_symbol
            .get(symbol)
            .map(|value| value.trim())
            .and_then(|industry| industry_scales.get(industry))
        {
            *weight *= *scale;
        }
    }
    weights.retain(|_, weight| !weight.is_zero());
}

fn apply_style_risk_budget(
    weights: &mut HashMap<String, Decimal>,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.style_risk_budget_profile.params() else {
        return;
    };
    if weights.is_empty() {
        return;
    }

    let volatility_scores = weights
        .keys()
        .filter_map(|symbol| {
            let returns = trailing_returns(
                return_history,
                symbol,
                score_day,
                config.risk_budget_lookback_days,
            );
            sample_volatility(&returns).map(|volatility| (symbol.clone(), volatility))
        })
        .collect::<Vec<_>>();
    let high_volatility_symbols =
        high_style_bucket_symbols(&volatility_scores, params.high_volatility_quantile);
    cap_style_bucket(
        weights,
        &high_volatility_symbols,
        params.high_volatility_max_weight_pct,
    );

    let liquidity_scores = weights
        .keys()
        .filter_map(|symbol| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    let low_liquidity_symbols =
        low_style_bucket_symbols(&liquidity_scores, params.low_liquidity_quantile);
    cap_style_bucket(
        weights,
        &low_liquidity_symbols,
        params.low_liquidity_max_weight_pct,
    );
}

fn high_style_bucket_symbols(scores: &[(String, f64)], quantile: f64) -> HashSet<String> {
    if scores.is_empty() {
        return HashSet::new();
    }
    let mut sorted = scores
        .iter()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return HashSet::new();
    }
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let threshold_index =
        ((sorted.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).ceil() as usize;
    let threshold = sorted[threshold_index.min(sorted.len() - 1)].1;
    sorted
        .into_iter()
        .filter(|(_, value)| *value >= threshold)
        .map(|(symbol, _)| symbol.clone())
        .collect()
}

fn low_style_bucket_symbols(scores: &[(String, f64)], quantile: f64) -> HashSet<String> {
    if scores.is_empty() {
        return HashSet::new();
    }
    let mut sorted = scores
        .iter()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return HashSet::new();
    }
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let threshold_index =
        ((sorted.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).floor() as usize;
    let threshold = sorted[threshold_index.min(sorted.len() - 1)].1;
    sorted
        .into_iter()
        .filter(|(_, value)| *value <= threshold)
        .map(|(symbol, _)| symbol.clone())
        .collect()
}

fn cap_style_bucket(
    weights: &mut HashMap<String, Decimal>,
    bucket_symbols: &HashSet<String>,
    max_weight_pct: f64,
) {
    if weights.is_empty() || bucket_symbols.is_empty() || !max_weight_pct.is_finite() {
        return;
    }
    let cap = max_weight_pct.clamp(0.0, 1.0);
    if cap >= 1.0 {
        return;
    }
    let cap = Decimal::from_f64(cap).unwrap_or(Decimal::ONE);
    let total = weights
        .iter()
        .filter(|(symbol, _)| bucket_symbols.contains(*symbol))
        .map(|(_, weight)| *weight)
        .sum::<Decimal>();
    if total <= cap || total.is_zero() {
        return;
    }
    let scale = cap / total;
    for (symbol, weight) in weights.iter_mut() {
        if bucket_symbols.contains(symbol) {
            *weight *= scale;
        }
    }
    weights.retain(|_, weight| !weight.is_zero());
}

fn apply_risk_contribution_control(
    weights: &mut HashMap<String, Decimal>,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.risk_contribution_control_profile.params() else {
        return;
    };
    if weights.len() < 2 {
        return;
    }

    let cap = params.max_single_name_contribution_pct.clamp(0.01, 1.0);
    if cap >= 1.0 {
        return;
    }

    for _ in 0..params.iterations.max(1) {
        let symbols = weights.keys().cloned().collect::<Vec<_>>();
        let contributions = symbols
            .iter()
            .filter_map(|symbol| {
                let weight = weights.get(symbol)?.to_f64()?;
                if !weight.is_finite() || weight <= 0.0 {
                    return None;
                }
                let volatility = sample_volatility(&trailing_returns(
                    return_history,
                    symbol,
                    score_day,
                    config.risk_budget_lookback_days,
                ))
                .unwrap_or(0.20)
                .max(0.01);
                let concentration_penalty = covariance_concentration_penalty(
                    symbol,
                    &symbols,
                    return_history,
                    score_day,
                    config,
                );
                let risk_score = weight * volatility * concentration_penalty.max(1.0);
                if risk_score.is_finite() && risk_score > 0.0 {
                    Some((symbol.clone(), risk_score))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let total_risk = contributions.iter().map(|(_, risk)| *risk).sum::<f64>();
        if total_risk <= 0.0 {
            return;
        }

        let mut changed = false;
        for (symbol, risk_score) in contributions {
            let contribution_pct = risk_score / total_risk;
            if contribution_pct <= cap {
                continue;
            }
            if let Some(weight) = weights.get_mut(&symbol) {
                let scale = Decimal::from_f64((cap / contribution_pct).clamp(0.0, 1.0))
                    .unwrap_or(Decimal::ONE);
                *weight *= scale;
                changed = true;
            }
        }
        weights.retain(|_, weight| !weight.is_zero());
        if !changed {
            break;
        }
    }
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
    fn pit_average_amounts_by_date_never_uses_future_amount_rows() {
        let as_of = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let future_day = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
        let amount_history = HashMap::from([
            (
                "FUTURE_LIQUID".to_string(),
                vec![(as_of, 1_000_000.0), (future_day, 1_000_000_000.0)],
            ),
            (
                "LIQUID_NOW".to_string(),
                vec![
                    (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 900_000_000.0),
                    (as_of, 800_000_000.0),
                ],
            ),
        ]);

        let average_amounts_by_date =
            build_pit_average_amounts_by_date(&amount_history, &[as_of], 2);
        let average_amounts = average_amounts_by_date
            .get(&as_of)
            .expect("as-of liquidity snapshot");

        assert_eq!(average_amounts["FUTURE_LIQUID"], 1_000_000.0);
        assert!(average_amounts["LIQUID_NOW"] > average_amounts["FUTURE_LIQUID"]);
    }

    #[test]
    fn factor_signals_use_score_day_pit_capacity_snapshot() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
        ];
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let mut scores_by_date = HashMap::new();
        scores_by_date.insert(
            score_day,
            vec![
                ("FUTURE_LIQUID_ALPHA".to_string(), 100.0),
                ("LIQUID_NEAR_ALPHA".to_string(), 99.0),
                ("LIQUID_BACKUP".to_string(), 98.0),
                ("THIN_BACKUP".to_string(), 97.0),
            ],
        );
        let average_amounts_by_date = HashMap::from([
            (
                score_day,
                HashMap::from([
                    ("FUTURE_LIQUID_ALPHA".to_string(), 1_000_000.0),
                    ("LIQUID_NEAR_ALPHA".to_string(), 900_000_000.0),
                    ("LIQUID_BACKUP".to_string(), 800_000_000.0),
                    ("THIN_BACKUP".to_string(), 1_000_000.0),
                ]),
            ),
            (
                NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
                HashMap::from([("FUTURE_LIQUID_ALPHA".to_string(), 1_000_000_000.0)]),
            ),
        ]);
        let config = SignalConfig {
            top_n: 2,
            rebalance_freq_days: 1,
            max_position_pct: Decimal::new(50, 2),
            candidate_ranking_profile: CandidateRankingProfile::CapacityAwareAlphaLiquidityV1,
            ..Default::default()
        };

        let signals = build_rebalance_factor_signals(
            &trading_days,
            &scores_by_date,
            &config,
            &HashMap::new(),
            &average_amounts_by_date,
            &HashMap::new(),
            |_day, base| base.clone(),
        )
        .expect("factor signals");
        let signal = signals
            .get(&NaiveDate::from_ymd_opt(2026, 1, 7).unwrap())
            .expect("signal from score day");

        assert!(!signal.target_weights.contains_key("FUTURE_LIQUID_ALPHA"));
        assert!(signal.target_weights.contains_key("LIQUID_NEAR_ALPHA"));
        assert!(signal.target_weights.contains_key("LIQUID_BACKUP"));
    }

    #[test]
    fn factor_signals_emit_first_available_rebalance_inside_short_oos_window() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
        ];
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let scores_by_date = HashMap::from([(
            score_day,
            vec![
                ("AAA".to_string(), 3.0),
                ("BBB".to_string(), 2.0),
                ("CCC".to_string(), 1.0),
            ],
        )]);
        let config = SignalConfig {
            top_n: 1,
            rebalance_freq_days: 60,
            max_position_pct: Decimal::ONE,
            ..Default::default()
        };

        let signals = build_rebalance_factor_signals(
            &trading_days,
            &scores_by_date,
            &config,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            |_day, base| base.clone(),
        )
        .expect("factor signals");

        let first_signal_day = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let signal = signals
            .get(&first_signal_day)
            .expect("first eligible rebalance should not wait 60 trading days");
        assert_eq!(signal.target_weights.get("AAA"), Some(&Decimal::ONE));
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
        sort_prediction_scores(&mut scores_by_date, ScoreDirection::Descending);

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
    fn prediction_signals_emit_first_available_rebalance_inside_short_oos_window() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
        ];
        let score_day = trading_days[0];
        let mut scores_by_date = HashMap::new();
        scores_by_date.insert(
            score_day,
            vec![
                ("000001.SZ".to_string(), 1.0, Some(1)),
                ("000002.SZ".to_string(), 0.9, Some(2)),
                ("000003.SZ".to_string(), 0.8, Some(3)),
                ("000004.SZ".to_string(), 0.7, Some(4)),
                ("000005.SZ".to_string(), 0.6, Some(5)),
            ],
        );
        sort_prediction_scores(&mut scores_by_date, ScoreDirection::Descending);

        let signals = build_rebalance_prediction_signals(
            &trading_days,
            &scores_by_date,
            &PredictionSignalConfig {
                prediction_set_id: "pred-v1".into(),
                top_n: 5,
                rebalance_freq_days: 60,
                entry_delay_days: 0,
                max_position_pct: Decimal::new(20, 2),
                ..Default::default()
            },
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .expect("prediction signals");

        let signal = signals
            .get(&NaiveDate::from_ymd_opt(2025, 1, 10).unwrap())
            .expect("first eligible rebalance should not wait 60 trading days");
        assert_eq!(signal.target_weights.len(), 5);
    }

    #[test]
    fn factor_sparse_score_days_follow_actual_rebalance_schedule() {
        let trading_days = vec![
            NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
        ];
        let config = SignalConfig {
            rebalance_freq_days: 60,
            entry_delay_days: 0,
            ..Default::default()
        };

        let score_days = rebalance_score_days(&trading_days, &config, |_day, base| base.clone());

        assert_eq!(
            score_days,
            vec![NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()]
        );
    }

    #[test]
    fn combo_score_dates_query_filters_to_requested_score_days() {
        let sql = combo_score_load_dates_sql(
            ScoreDirection::Descending,
            Some(200),
            TradableUniverseProfile::All,
        );

        assert!(sql.contains("mfv.trade_date = ANY($3)"));
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("score_rank <= $4"));
        assert!(!sql.contains("mfv.trade_date >= $3"));
    }

    #[test]
    fn prediction_query_loads_prior_scores_for_first_signal_day() {
        let start_date = NaiveDate::from_ymd_opt(2025, 1, 21).unwrap();

        let load_start = prediction_load_start_date(start_date, 1);

        assert!(load_start < start_date);
        assert_eq!(load_start, NaiveDate::from_ymd_opt(2024, 12, 19).unwrap());
    }

    #[test]
    fn sort_prediction_scores_honors_ascending_direction() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let mut scores_by_date = HashMap::from([(
            day,
            vec![
                ("AAA".to_string(), 3.0, Some(1)),
                ("BBB".to_string(), 1.0, Some(3)),
                ("CCC".to_string(), 2.0, Some(2)),
            ],
        )]);

        sort_prediction_scores(&mut scores_by_date, ScoreDirection::Ascending);

        let sorted = scores_by_date.get(&day).unwrap();
        assert_eq!(sorted[0].0, "BBB");
        assert_eq!(sorted[1].0, "CCC");
        assert_eq!(sorted[2].0, "AAA");
    }

    #[test]
    fn prediction_blend_keeps_intersection_and_combines_normalized_scores() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let mut factor_scores = HashMap::from([(
            day,
            vec![
                ("AAA".to_string(), 10.0),
                ("BBB".to_string(), 20.0),
                ("CCC".to_string(), 30.0),
            ],
        )]);
        let prediction_scores = HashMap::from([(
            day,
            vec![
                ("AAA".to_string(), 0.1, None),
                ("BBB".to_string(), 0.2, None),
            ],
        )]);
        let blend = PredictionBlendConfig {
            prediction_set_id: "pred-quality-growth".to_string(),
            factor_weight: 0.5,
            prediction_weight: 0.5,
            prediction_min_percentile: None,
        };

        blend_factor_prediction_scores(
            &mut factor_scores,
            &prediction_scores,
            &blend,
            ScoreDirection::Descending,
        );

        let blended = factor_scores.get(&day).unwrap();
        assert_eq!(blended.len(), 2);
        assert_eq!(blended[0].0, "AAA");
        assert_eq!(blended[1].0, "BBB");
        assert!(blended[1].1 > blended[0].1);
    }

    #[test]
    fn prediction_blend_aligns_prediction_direction_for_ascending_factor_scores() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let mut factor_scores = HashMap::from([(
            day,
            vec![
                ("LOW_PRED".to_string(), 10.0),
                ("HIGH_PRED".to_string(), 10.0),
            ],
        )]);
        let prediction_scores = HashMap::from([(
            day,
            vec![
                ("LOW_PRED".to_string(), 0.1, None),
                ("HIGH_PRED".to_string(), 0.9, None),
            ],
        )]);
        let blend = PredictionBlendConfig {
            prediction_set_id: "pred-quality-growth".to_string(),
            factor_weight: 0.0,
            prediction_weight: 1.0,
            prediction_min_percentile: None,
        };

        blend_factor_prediction_scores(
            &mut factor_scores,
            &prediction_scores,
            &blend,
            ScoreDirection::Ascending,
        );
        sort_factor_scores(
            factor_scores.get_mut(&day).unwrap(),
            ScoreDirection::Ascending,
        );

        let blended = factor_scores.get(&day).unwrap();
        assert_eq!(blended[0].0, "HIGH_PRED");
        assert!(blended[0].1 < blended[1].1);
    }

    #[test]
    fn prediction_blend_can_filter_low_prediction_percentiles_without_reweighting() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let mut factor_scores = HashMap::from([(
            day,
            vec![
                ("LOW_FACTOR_BAD_PRED".to_string(), 1.0),
                ("MID_FACTOR_GOOD_PRED".to_string(), 2.0),
                ("HIGH_FACTOR_GOOD_PRED".to_string(), 3.0),
            ],
        )]);
        let prediction_scores = HashMap::from([(
            day,
            vec![
                ("LOW_FACTOR_BAD_PRED".to_string(), 0.1, None),
                ("MID_FACTOR_GOOD_PRED".to_string(), 0.8, None),
                ("HIGH_FACTOR_GOOD_PRED".to_string(), 0.9, None),
            ],
        )]);
        let blend = PredictionBlendConfig {
            prediction_set_id: "pred-quality-growth".to_string(),
            factor_weight: 1.0,
            prediction_weight: 0.0,
            prediction_min_percentile: Some(0.5),
        };

        blend_factor_prediction_scores(
            &mut factor_scores,
            &prediction_scores,
            &blend,
            ScoreDirection::Ascending,
        );
        sort_factor_scores(
            factor_scores.get_mut(&day).unwrap(),
            ScoreDirection::Ascending,
        );

        let blended = factor_scores.get(&day).unwrap();
        assert_eq!(blended.len(), 2);
        assert_eq!(blended[0].0, "MID_FACTOR_GOOD_PRED");
        assert_eq!(blended[1].0, "HIGH_FACTOR_GOOD_PRED");
    }

    #[test]
    fn event_gate_boosts_or_filters_without_replacing_base_ranking() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let base_scores = HashMap::from([(
            day,
            vec![
                ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
                ("BASE_MID_POSITIVE_EVENT".to_string(), 9.0),
                ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
            ],
        )]);
        let event_scores = HashMap::from([(
            day,
            vec![
                ("BASE_MID_POSITIVE_EVENT".to_string(), 1.2),
                ("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7),
            ],
        )]);

        let mut boosted = base_scores.clone();
        apply_event_gate_scores(
            &mut boosted,
            &event_scores,
            &EventGateConfig {
                combo_name: "phase7_event_window_earnings_v1".to_string(),
                version: "1.0.0".to_string(),
                mode: EventGateMode::BoostPositive,
                score_direction: ScoreDirection::Descending,
                min_score: 0.0,
                boost_weight: 0.05,
                active_regimes: vec![],
            },
        );
        let boosted = boosted.get(&day).unwrap();
        assert_eq!(
            boosted.len(),
            3,
            "boost mode must not create sparse deletion"
        );
        assert_eq!(boosted[0].0, "BASE_LEADER_NO_EVENT");
        assert!(boosted[1].1 > base_scores.get(&day).unwrap()[1].1);
        assert_eq!(boosted[2].1, base_scores.get(&day).unwrap()[2].1);

        let mut exclude_negative = base_scores.clone();
        apply_event_gate_scores(
            &mut exclude_negative,
            &event_scores,
            &EventGateConfig {
                combo_name: "phase7_event_window_earnings_v1".to_string(),
                version: "1.0.0".to_string(),
                mode: EventGateMode::ExcludeNegative,
                score_direction: ScoreDirection::Descending,
                min_score: 0.0,
                boost_weight: 0.0,
                active_regimes: vec![],
            },
        );
        let exclude_negative = exclude_negative.get(&day).unwrap();
        assert_eq!(
            exclude_negative
                .iter()
                .map(|(symbol, _)| symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["BASE_LEADER_NO_EVENT", "BASE_MID_POSITIVE_EVENT"]
        );

        let mut require_positive = base_scores.clone();
        apply_event_gate_scores(
            &mut require_positive,
            &event_scores,
            &EventGateConfig {
                combo_name: "phase7_event_window_earnings_v1".to_string(),
                version: "1.0.0".to_string(),
                mode: EventGateMode::RequirePositive,
                score_direction: ScoreDirection::Descending,
                min_score: 0.0,
                boost_weight: 0.0,
                active_regimes: vec![],
            },
        );
        let require_positive = require_positive.get(&day).unwrap();
        assert_eq!(require_positive.len(), 1);
        assert_eq!(require_positive[0].0, "BASE_MID_POSITIVE_EVENT");
    }

    #[test]
    fn event_gate_can_be_limited_to_stress_regimes() {
        let bear_day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let bull_day = NaiveDate::from_ymd_opt(2025, 1, 13).unwrap();
        let base_scores = HashMap::from([
            (
                bear_day,
                vec![
                    ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
                    ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
                ],
            ),
            (
                bull_day,
                vec![
                    ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
                    ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
                ],
            ),
        ]);
        let event_scores = HashMap::from([
            (
                bear_day,
                vec![("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7)],
            ),
            (
                bull_day,
                vec![("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7)],
            ),
        ]);

        let mut regime_limited = base_scores.clone();
        apply_event_gate_scores_for_regime(
            &mut regime_limited,
            &event_scores,
            &EventGateConfig {
                combo_name: "phase7_valuation_v1".to_string(),
                version: "1.0.0".to_string(),
                mode: EventGateMode::ExcludeNegative,
                score_direction: ScoreDirection::Descending,
                min_score: 0.0,
                boost_weight: 0.0,
                active_regimes: vec![MarketRegime::Bear, MarketRegime::HighVolatility],
            },
            |date| {
                if date == bear_day {
                    MarketRegime::Bear
                } else {
                    MarketRegime::Bull
                }
            },
        );

        let bear_symbols = regime_limited
            .get(&bear_day)
            .unwrap()
            .iter()
            .map(|(symbol, _)| symbol.as_str())
            .collect::<Vec<_>>();
        let bull_symbols = regime_limited
            .get(&bull_day)
            .unwrap()
            .iter()
            .map(|(symbol, _)| symbol.as_str())
            .collect::<Vec<_>>();

        assert_eq!(bear_symbols, vec!["BASE_LEADER_NO_EVENT"]);
        assert_eq!(
            bull_symbols,
            vec!["BASE_LEADER_NO_EVENT", "BASE_LOW_NEGATIVE_EVENT"],
            "the valuation guard should stay inactive outside stress regimes"
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
    fn risk_model_portfolio_config_caps_large_top_n_for_local_search() {
        let config = SignalConfig {
            top_n: 80,
            portfolio_method: PortfolioConstructionMethod::RiskBudget,
            ..Default::default()
        };
        let portfolio_config = PortfolioConstructionConfig::from(&config);

        assert_eq!(portfolio_config.top_n, 50);

        let min_variance_config = SignalConfig {
            top_n: 80,
            portfolio_method: PortfolioConstructionMethod::MinVariance,
            ..Default::default()
        };
        let portfolio_config = PortfolioConstructionConfig::from(&min_variance_config);

        assert_eq!(portfolio_config.top_n, 50);

        let heuristic_config = SignalConfig {
            top_n: 80,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            ..Default::default()
        };
        let portfolio_config = PortfolioConstructionConfig::from(&heuristic_config);

        assert_eq!(portfolio_config.top_n, 80);
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
            &HashMap::new(),
            &config,
        );

        assert!(weights["LOW_RISK"] > weights["HIGH_RISK"]);
        assert!(weights["LOW_RISK"] <= Decimal::new(80, 2));
        let gross: Decimal = weights.values().copied().sum();
        assert!(gross <= Decimal::ONE);
    }

    #[test]
    fn portfolio_construction_caps_target_weight_by_participation_capacity() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![("LIQUID".to_string(), 3.0), ("THIN".to_string(), 2.9)];
        let average_amounts = HashMap::from([
            ("LIQUID".to_string(), 5_000_000.0),
            ("THIN".to_string(), 1_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(80, 2),
            max_gross_exposure: 1.0,
            portfolio_notional_cny: Some(1_000_000.0),
            max_participation_rate: Some(0.05),
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert_eq!(weights["LIQUID"], Decimal::new(25, 2));
        assert_eq!(weights["THIN"], Decimal::new(5, 2));
    }

    #[test]
    fn liquidity_candidate_risk_filter_prefers_tradable_low_volatility_names() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("THIN_HIGH_SCORE".to_string(), 100.0),
            ("LIQUID_RISKY".to_string(), 99.0),
            ("LIQUID_STABLE".to_string(), 98.0),
            ("MID_LIQUID_STABLE".to_string(), 97.0),
        ];
        let return_history = HashMap::from([
            (
                "THIN_HIGH_SCORE".to_string(),
                dated_returns(&[0.003, 0.004, 0.002, 0.003, 0.004]),
            ),
            (
                "LIQUID_RISKY".to_string(),
                dated_returns(&[0.12, -0.10, 0.11, -0.09, 0.10]),
            ),
            (
                "LIQUID_STABLE".to_string(),
                dated_returns(&[0.004, 0.003, 0.004, 0.003, 0.004]),
            ),
            (
                "MID_LIQUID_STABLE".to_string(),
                dated_returns(&[0.005, 0.004, 0.005, 0.004, 0.005]),
            ),
        ]);
        let average_amounts = HashMap::from([
            ("THIN_HIGH_SCORE".to_string(), 1_000_000.0),
            ("LIQUID_RISKY".to_string(), 120_000_000.0),
            ("LIQUID_STABLE".to_string(), 100_000_000.0),
            ("MID_LIQUID_STABLE".to_string(), 80_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 1,
            candidate_risk_filter_profile:
                CandidateRiskFilterProfile::SoftLiquidityLowVolatilityLowCorrelationV1,
            risk_budget_lookback_days: 5,
            max_position_pct: Decimal::ONE,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("LIQUID_STABLE"));
        assert!(!weights.contains_key("THIN_HIGH_SCORE"));
        assert!(!weights.contains_key("LIQUID_RISKY"));
    }

    #[test]
    fn capacity_aware_candidate_ranking_prefers_liquid_near_alpha_candidates() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("THIN_ALPHA_1".to_string(), 100.0),
            ("THIN_ALPHA_2".to_string(), 99.0),
            ("LIQUID_NEAR_ALPHA_1".to_string(), 98.0),
            ("LIQUID_NEAR_ALPHA_2".to_string(), 97.0),
        ];
        let average_amounts = HashMap::from([
            ("THIN_ALPHA_1".to_string(), 1_000_000.0),
            ("THIN_ALPHA_2".to_string(), 1_200_000.0),
            ("LIQUID_NEAR_ALPHA_1".to_string(), 1_000_000_000.0),
            ("LIQUID_NEAR_ALPHA_2".to_string(), 800_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(50, 2),
            candidate_ranking_profile: CandidateRankingProfile::CapacityAwareAlphaLiquidityV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("LIQUID_NEAR_ALPHA_1"));
        assert!(weights.contains_key("LIQUID_NEAR_ALPHA_2"));
        assert!(!weights.contains_key("THIN_ALPHA_1"));
        assert!(!weights.contains_key("THIN_ALPHA_2"));
    }

    #[test]
    fn alpha_first_low_impact_ranking_preserves_alpha_with_liquidity_bias() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("ALPHA_LEADER".to_string(), 100.0),
            ("LIQUID_NEAR_ALPHA".to_string(), 99.0),
            ("LIQUID_BACKUP".to_string(), 98.0),
            ("THIN_BACKUP".to_string(), 97.0),
        ];
        let average_amounts = HashMap::from([
            ("ALPHA_LEADER".to_string(), 80_000_000.0),
            ("LIQUID_NEAR_ALPHA".to_string(), 1_000_000_000.0),
            ("LIQUID_BACKUP".to_string(), 900_000_000.0),
            ("THIN_BACKUP".to_string(), 1_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(50, 2),
            candidate_ranking_profile: CandidateRankingProfile::AlphaFirstLowImpactV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("ALPHA_LEADER"));
        assert!(weights.contains_key("LIQUID_NEAR_ALPHA"));
        assert!(!weights.contains_key("LIQUID_BACKUP"));
        assert!(!weights.contains_key("THIN_BACKUP"));
    }

    #[test]
    fn relative_strength_alpha_liquidity_ranking_uses_only_pit_trailing_returns() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
        let candidates = vec![
            ("ALPHA_LEADER".to_string(), 100.0),
            ("RELATIVE_STRENGTH".to_string(), 99.0),
            ("FUTURE_SPIKE".to_string(), 98.0),
            ("LIQUID_BACKUP".to_string(), 97.0),
        ];
        let return_history = HashMap::from([
            (
                "ALPHA_LEADER".to_string(),
                dated_returns(&[0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
            (
                "RELATIVE_STRENGTH".to_string(),
                dated_returns(&[0.02, 0.03, 0.01, 0.02, 0.03]),
            ),
            (
                "FUTURE_SPIKE".to_string(),
                vec![
                    (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.02),
                    (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.02),
                    (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.01),
                    (future_day, 0.30),
                ],
            ),
            (
                "LIQUID_BACKUP".to_string(),
                dated_returns(&[0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
        ]);
        let average_amounts = HashMap::from([
            ("ALPHA_LEADER".to_string(), 200_000_000.0),
            ("RELATIVE_STRENGTH".to_string(), 1_000_000_000.0),
            ("FUTURE_SPIKE".to_string(), 900_000_000.0),
            ("LIQUID_BACKUP".to_string(), 800_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(50, 2),
            risk_budget_lookback_days: 5,
            candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("RELATIVE_STRENGTH"));
        assert!(weights.contains_key("ALPHA_LEADER"));
        assert!(!weights.contains_key("FUTURE_SPIKE"));
        assert!(!weights.contains_key("LIQUID_BACKUP"));
    }

    #[test]
    fn cash_utilization_profile_expands_holdings_to_restore_fillable_gross() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = (0..20)
            .map(|idx| (format!("S{:02}", idx + 1), 100.0 - idx as f64))
            .collect::<Vec<_>>();
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| (symbol.clone(), 50_000_000.0))
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 5,
            max_position_pct: Decimal::new(20, 2),
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.10),
            max_gross_exposure: 1.0,
            cash_utilization_profile: CashUtilizationProfile::FillableGross90V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();

        assert!(weights.len() > 5);
        assert!(gross >= Decimal::new(90, 2));
        assert!(weights.values().all(|weight| *weight <= Decimal::new(5, 2)));
    }

    #[test]
    fn stress_fill_cash_utilization_expands_deeper_to_restore_strict_fillable_gross() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = (0..150)
            .map(|idx| (format!("S{:03}", idx + 1), 100.0 - idx as f64))
            .collect::<Vec<_>>();
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| (symbol.clone(), 25_000_000.0))
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 60,
            max_position_pct: Decimal::new(10, 2),
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            max_gross_exposure: 1.0,
            cash_utilization_profile: CashUtilizationProfile::StressFillGross98V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();

        assert!(weights.len() > 60);
        assert!(gross >= Decimal::new(98, 2));
        assert!(weights
            .values()
            .all(|weight| *weight <= Decimal::new(125, 4)));
    }

    #[test]
    fn capacity_risk_budget_caps_low_capacity_bucket_and_redistributes() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("DEEP_A".to_string(), 4.0),
            ("THIN_A".to_string(), 3.0),
            ("DEEP_B".to_string(), 2.0),
            ("THIN_B".to_string(), 1.0),
        ];
        let average_amounts = HashMap::from([
            ("DEEP_A".to_string(), 900_000_000.0),
            ("DEEP_B".to_string(), 800_000_000.0),
            ("THIN_A".to_string(), 20_000_000.0),
            ("THIN_B".to_string(), 10_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 4,
            max_position_pct: Decimal::new(80, 2),
            max_gross_exposure: 1.0,
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::ParticipationStrictV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        let low_capacity_weight = weights["THIN_A"] + weights["THIN_B"];
        let deep_capacity_weight = weights["DEEP_A"] + weights["DEEP_B"];

        assert!(low_capacity_weight <= Decimal::new(20, 2));
        assert!(deep_capacity_weight >= Decimal::new(80, 2));
        assert!(weights["DEEP_A"] > Decimal::new(25, 2));
        assert!(weights["DEEP_B"] > Decimal::new(25, 2));
    }

    #[test]
    fn stress_participation_budget_soft_caps_names_under_tight_capacity() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("DEEP_A".to_string(), 6.0),
            ("THIN_A".to_string(), 5.0),
            ("DEEP_B".to_string(), 4.0),
            ("THIN_B".to_string(), 3.0),
            ("DEEP_C".to_string(), 2.0),
            ("DEEP_D".to_string(), 1.0),
        ];
        let average_amounts = HashMap::from([
            ("DEEP_A".to_string(), 500_000_000.0),
            ("DEEP_B".to_string(), 500_000_000.0),
            ("DEEP_C".to_string(), 500_000_000.0),
            ("DEEP_D".to_string(), 500_000_000.0),
            ("THIN_A".to_string(), 20_000_000.0),
            ("THIN_B".to_string(), 20_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 6,
            max_position_pct: Decimal::new(40, 2),
            max_gross_exposure: 1.0,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.10),
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationSoftCapV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights["THIN_A"] <= Decimal::new(1, 2));
        assert!(weights["THIN_B"] <= Decimal::new(1, 2));
        assert!(weights["DEEP_A"] > Decimal::new(1660, 4));
        assert!(weights["DEEP_B"] > Decimal::new(1660, 4));
    }

    #[test]
    fn stress_participation_target_scale_reduces_target_gross_to_capacity() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = (0..24)
            .map(|idx| (format!("STRESS_{idx:02}"), (24 - idx) as f64))
            .collect::<Vec<_>>();
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| (symbol.clone(), 25_000_000.0))
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 24,
            max_position_pct: Decimal::new(8, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile:
                CapacityRiskBudgetProfile::StressParticipationTargetScaleV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();

        assert!(gross < Decimal::new(90, 2));
        assert!(gross <= Decimal::new(15, 2));
        assert!(weights.len() >= 12);
        assert!(weights
            .values()
            .all(|weight| *weight <= Decimal::new(625, 5)));
    }

    #[test]
    fn stress_participation_floor_scale_preserves_minimum_investable_gross() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let mut candidates = (0..10)
            .map(|idx| (format!("DEEP_{idx:02}"), (20 - idx) as f64))
            .collect::<Vec<_>>();
        candidates.extend((0..2).map(|idx| (format!("THIN_{idx:02}"), (2 - idx) as f64)));
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| {
                let amount = if symbol.starts_with("DEEP_") {
                    let suffix = symbol
                        .rsplit_once('_')
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    60_000_000.0 + suffix as f64 * 5_000_000.0
                } else {
                    10_000_000.0
                };
                (symbol.clone(), amount)
            })
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 12,
            max_position_pct: Decimal::new(8, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationFloor35V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();
        let deep_max = (0..10)
            .filter_map(|idx| weights.get(&format!("DEEP_{idx:02}")).copied())
            .max()
            .unwrap_or(Decimal::ZERO);
        let thin_weight = (0..2)
            .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
            .sum::<Decimal>();

        assert!(gross >= Decimal::new(35, 2));
        assert!(gross < Decimal::new(90, 2));
        assert!(deep_max > Decimal::new(2, 2));
        assert!(deep_max <= Decimal::new(6, 2));
        assert!(thin_weight <= Decimal::new(1, 2));
    }

    #[test]
    fn stress_participation_return_recovery_floor_keeps_higher_investable_gross() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let mut candidates = (0..20)
            .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
            .collect::<Vec<_>>();
        candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| {
                let amount = if symbol.starts_with("DEEP_") {
                    let suffix = symbol
                        .rsplit_once('_')
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    120_000_000.0 + suffix as f64 * 5_000_000.0
                } else {
                    10_000_000.0
                };
                (symbol.clone(), amount)
            })
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 24,
            max_position_pct: Decimal::new(6, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationFloor70V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();
        let thin_weight = (0..4)
            .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
            .sum::<Decimal>();

        assert!(gross >= Decimal::new(70, 2));
        assert!(gross < Decimal::new(90, 2));
        assert!(thin_weight <= Decimal::new(12, 2));
    }

    #[test]
    fn headroom_refill_allocates_more_weight_to_symbols_with_larger_pressure_headroom() {
        let mut weights = HashMap::from([
            ("LOW_HEADROOM".to_string(), Decimal::new(10, 2)),
            ("HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
        ]);
        let caps = HashMap::from([
            ("LOW_HEADROOM".to_string(), Decimal::new(20, 2)),
            ("HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
        ]);

        let remaining = redistribute_weight_by_headroom(
            &mut weights,
            &HashSet::new(),
            Decimal::new(20, 2),
            &caps,
            4,
        );

        assert!(remaining <= Decimal::new(1, 8));
        assert!(weights["HIGH_HEADROOM"] > Decimal::new(25, 2));
        assert!(weights["LOW_HEADROOM"] < Decimal::new(15, 2));
    }

    #[test]
    fn alpha_headroom_refill_balances_existing_alpha_weight_and_pressure_headroom() {
        let mut weights = HashMap::from([
            ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(30, 2)),
            ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
        ]);
        let caps = HashMap::from([
            ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(50, 2)),
            ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
        ]);

        let remaining = redistribute_weight_by_alpha_headroom(
            &mut weights,
            &HashSet::new(),
            Decimal::new(20, 2),
            &caps,
            4,
        );

        assert!(remaining <= Decimal::new(1, 8));
        assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] > Decimal::new(38, 2));
        assert!(weights["LOW_ALPHA_HIGH_HEADROOM"] > Decimal::new(18, 2));
        assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] < Decimal::new(50, 2));
    }

    #[test]
    fn blended_alpha_headroom_refill_preserves_alpha_without_starving_headroom() {
        let mut weights = HashMap::from([
            ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(30, 2)),
            ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
        ]);
        let caps = HashMap::from([
            ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(50, 2)),
            ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
        ]);

        let remaining = redistribute_weight_by_blended_alpha_headroom(
            &mut weights,
            &HashSet::new(),
            Decimal::new(20, 2),
            &caps,
            4,
        );

        assert!(remaining <= Decimal::new(1, 8));
        assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] > Decimal::new(35, 2));
        assert!(weights["LOW_ALPHA_HIGH_HEADROOM"] > Decimal::new(23, 2));
        assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] < Decimal::new(50, 2));
    }

    #[test]
    fn stress_participation_headroom_floor_keeps_target_gross_with_capacity_weighted_refill() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let mut candidates = (0..20)
            .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
            .collect::<Vec<_>>();
        candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| {
                let amount = if symbol.starts_with("DEEP_") {
                    let suffix = symbol
                        .rsplit_once('_')
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    120_000_000.0 + suffix as f64 * 5_000_000.0
                } else {
                    10_000_000.0
                };
                (symbol.clone(), amount)
            })
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 24,
            max_position_pct: Decimal::new(6, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile:
                CapacityRiskBudgetProfile::StressParticipationHeadroomFloor70V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();
        let thin_weight = (0..4)
            .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
            .sum::<Decimal>();

        assert!(gross >= Decimal::new(70, 2));
        assert!(gross < Decimal::new(90, 2));
        assert!(thin_weight <= Decimal::new(12, 2));
    }

    #[test]
    fn stress_participation_alpha_headroom_floor_keeps_target_gross_with_dual_objective_refill() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let mut candidates = (0..20)
            .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
            .collect::<Vec<_>>();
        candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| {
                let amount = if symbol.starts_with("DEEP_") {
                    let suffix = symbol
                        .rsplit_once('_')
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    120_000_000.0 + suffix as f64 * 5_000_000.0
                } else {
                    10_000_000.0
                };
                (symbol.clone(), amount)
            })
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 24,
            max_position_pct: Decimal::new(6, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile:
                CapacityRiskBudgetProfile::StressParticipationAlphaHeadroomFloor70V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();
        let thin_weight = (0..4)
            .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
            .sum::<Decimal>();

        assert!(gross >= Decimal::new(70, 2));
        assert!(gross < Decimal::new(90, 2));
        assert!(thin_weight <= Decimal::new(12, 2));
    }

    #[test]
    fn stress_participation_blended_alpha_headroom_floor_keeps_capacity_floor() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let mut candidates = (0..20)
            .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
            .collect::<Vec<_>>();
        candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
        let average_amounts = candidates
            .iter()
            .map(|(symbol, _)| {
                let amount = if symbol.starts_with("DEEP_") {
                    let suffix = symbol
                        .rsplit_once('_')
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    120_000_000.0 + suffix as f64 * 5_000_000.0
                } else {
                    10_000_000.0
                };
                (symbol.clone(), amount)
            })
            .collect::<HashMap<_, _>>();
        let config = PortfolioConstructionConfig {
            top_n: 24,
            max_position_pct: Decimal::new(6, 2),
            max_gross_exposure: 0.90,
            portfolio_notional_cny: Some(100_000_000.0),
            max_participation_rate: Some(0.05),
            capacity_risk_budget_profile:
                CapacityRiskBudgetProfile::StressParticipationBlendedAlphaHeadroomFloor70V1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &average_amounts,
            &HashMap::new(),
            &config,
        );
        let gross = weights.values().copied().sum::<Decimal>();
        let thin_weight = (0..4)
            .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
            .sum::<Decimal>();

        assert!(gross >= Decimal::new(70, 2));
        assert!(gross < Decimal::new(90, 2));
        assert!(thin_weight <= Decimal::new(12, 2));
    }

    #[test]
    fn min_variance_portfolio_penalizes_variance_more_aggressively() {
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
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(95, 2),
            max_gross_exposure: 1.0,
            portfolio_method: PortfolioConstructionMethod::MinVariance,
            risk_budget_lookback_days: 5,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &HashMap::new(),
            &config,
        );

        assert!(weights["LOW_RISK"] > Decimal::new(90, 2));
        assert!(weights["HIGH_RISK"] < Decimal::new(10, 2));
    }

    #[test]
    fn industry_cap_scales_overweight_industry_exposure() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("BANK_A".to_string(), 3.0),
            ("BANK_B".to_string(), 2.0),
            ("TECH_A".to_string(), 1.0),
        ];
        let industries = HashMap::from([
            ("BANK_A".to_string(), "bank".to_string()),
            ("BANK_B".to_string(), "bank".to_string()),
            ("TECH_A".to_string(), "tech".to_string()),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 3,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            max_industry_weight_pct: Some(0.50),
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &HashMap::new(),
            &HashMap::new(),
            &industries,
            &config,
        );

        let bank_weight = weights["BANK_A"] + weights["BANK_B"];

        assert!(bank_weight <= Decimal::new(50, 2));
        assert!(weights["BANK_A"] < weights["TECH_A"]);
        assert!(weights["BANK_B"] < weights["TECH_A"]);
    }

    #[test]
    fn style_risk_budget_caps_high_volatility_and_low_liquidity_exposures() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("STABLE_LIQUID".to_string(), 3.0),
            ("HIGH_VOL".to_string(), 2.0),
            ("LOW_LIQUIDITY".to_string(), 1.0),
        ];
        let return_history = HashMap::from([
            (
                "STABLE_LIQUID".to_string(),
                dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
            ),
            (
                "HIGH_VOL".to_string(),
                dated_returns(&[0.08, -0.07, 0.09, -0.08, 0.07]),
            ),
            (
                "LOW_LIQUIDITY".to_string(),
                dated_returns(&[0.005, 0.004, 0.004, 0.006, 0.005]),
            ),
        ]);
        let average_amounts = HashMap::from([
            ("STABLE_LIQUID".to_string(), 800_000_000.0),
            ("HIGH_VOL".to_string(), 600_000_000.0),
            ("LOW_LIQUIDITY".to_string(), 20_000_000.0),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 3,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            style_risk_budget_profile: StyleRiskBudgetProfile::DefensiveStyleBudgetV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &average_amounts,
            &HashMap::new(),
            &config,
        );

        assert!(weights["HIGH_VOL"] <= Decimal::new(30, 2));
        assert!(weights["LOW_LIQUIDITY"] <= Decimal::new(30, 2));
        assert!(weights["STABLE_LIQUID"] > weights["HIGH_VOL"]);
        assert!(weights["STABLE_LIQUID"] > weights["LOW_LIQUIDITY"]);
    }

    #[test]
    fn candidate_risk_filter_removes_high_volatility_candidates_before_selection() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("HIGH_VOL_LEADER".to_string(), 4.0),
            ("LOW_VOL_A".to_string(), 3.0),
            ("LOW_VOL_B".to_string(), 2.0),
        ];
        let return_history = HashMap::from([
            (
                "HIGH_VOL_LEADER".to_string(),
                dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
            ),
            (
                "LOW_VOL_A".to_string(),
                dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
            ),
            (
                "LOW_VOL_B".to_string(),
                dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 2,
            max_position_pct: Decimal::new(60, 2),
            risk_budget_lookback_days: 5,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &HashMap::new(),
            &config,
        );

        assert!(!weights.contains_key("HIGH_VOL_LEADER"));
        assert!(weights.contains_key("LOW_VOL_A"));
        assert!(weights.contains_key("LOW_VOL_B"));
    }

    #[test]
    fn candidate_risk_filter_can_prefer_low_correlation_candidate_over_cluster() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("CLUSTER_A".to_string(), 4.0),
            ("CLUSTER_B".to_string(), 3.0),
            ("CLUSTER_C".to_string(), 2.0),
            ("DIVERSIFIER".to_string(), 1.0),
        ];
        let return_history = HashMap::from([
            (
                "CLUSTER_A".to_string(),
                dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
            ),
            (
                "CLUSTER_B".to_string(),
                dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
            ),
            (
                "CLUSTER_C".to_string(),
                dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
            ),
            (
                "DIVERSIFIER".to_string(),
                dated_returns(&[0.010, 0.010, -0.010, -0.010, 0.010]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 1,
            max_position_pct: Decimal::ONE,
            risk_budget_lookback_days: 5,
            candidate_risk_filter_profile:
                CandidateRiskFilterProfile::LowVolatilityLowCorrelationV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &HashMap::new(),
            &config,
        );

        assert!(weights.contains_key("DIVERSIFIER"));
        assert_eq!(weights.len(), 1);
    }

    #[test]
    fn soft_candidate_risk_filter_keeps_more_return_candidates() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("VERY_HIGH_VOL".to_string(), 5.0),
            ("MID_HIGH_VOL".to_string(), 4.0),
            ("LOW_VOL_A".to_string(), 3.0),
            ("LOW_VOL_B".to_string(), 2.0),
            ("LOW_VOL_C".to_string(), 1.0),
        ];
        let return_history = HashMap::from([
            (
                "VERY_HIGH_VOL".to_string(),
                dated_returns(&[0.15, -0.14, 0.13, -0.12, 0.11]),
            ),
            (
                "MID_HIGH_VOL".to_string(),
                dated_returns(&[0.07, -0.06, 0.06, -0.05, 0.05]),
            ),
            (
                "LOW_VOL_A".to_string(),
                dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
            ),
            (
                "LOW_VOL_B".to_string(),
                dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
            ),
            (
                "LOW_VOL_C".to_string(),
                dated_returns(&[0.006, 0.005, 0.004, 0.005, 0.006]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 4,
            max_position_pct: Decimal::new(40, 2),
            risk_budget_lookback_days: 5,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::SoftLowVolatilityV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &HashMap::new(),
            &config,
        );

        assert!(!weights.contains_key("VERY_HIGH_VOL"));
        assert!(weights.contains_key("MID_HIGH_VOL"));
        assert!(weights.contains_key("LOW_VOL_A"));
        assert!(weights.len() >= 4);
    }

    #[test]
    fn risk_contribution_control_scales_dominant_risk_name() {
        let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
        let candidates = vec![
            ("HIGH_RISK".to_string(), 3.0),
            ("LOW_RISK_A".to_string(), 2.0),
            ("LOW_RISK_B".to_string(), 1.0),
        ];
        let return_history = HashMap::from([
            (
                "HIGH_RISK".to_string(),
                dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
            ),
            (
                "LOW_RISK_A".to_string(),
                dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
            ),
            (
                "LOW_RISK_B".to_string(),
                dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
            ),
        ]);
        let config = PortfolioConstructionConfig {
            top_n: 3,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            risk_budget_lookback_days: 5,
            risk_contribution_control_profile:
                RiskContributionControlProfile::SoftSingleName20PctV1,
            ..Default::default()
        };

        let weights = build_portfolio_weights(
            score_day,
            &candidates,
            &return_history,
            &HashMap::new(),
            &HashMap::new(),
            &config,
        );

        assert!(weights["HIGH_RISK"] < weights["LOW_RISK_A"]);
        assert!(weights["HIGH_RISK"] < weights["LOW_RISK_B"]);
    }

    #[test]
    fn signal_data_cache_normalizes_symbol_order_for_portfolio_inputs() {
        let symbols = vec!["BBB".to_string(), "AAA".to_string(), "AAA".to_string()];

        assert_eq!(
            normalized_symbol_key(&symbols),
            vec!["AAA".to_string(), "BBB".to_string()]
        );
    }

    #[test]
    fn market_feature_snapshot_key_is_window_data_and_universe_scoped() {
        let train_start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let train_end = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let test_start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let test_end = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        let symbols = vec![
            "000002.SZ".to_string(),
            "000001.SZ".to_string(),
            "000001.SZ".to_string(),
        ];

        let key = MarketFeatureSnapshotKey::new(
            "full-market-2016-v1",
            train_start,
            train_end,
            test_start,
            test_end,
            120,
            &symbols,
        );
        let reordered = MarketFeatureSnapshotKey::new(
            "full-market-2016-v1",
            train_start,
            train_end,
            test_start,
            test_end,
            120,
            &["000001.SZ".to_string(), "000002.SZ".to_string()],
        );
        let different_data = MarketFeatureSnapshotKey::new(
            "full-market-2016-v2",
            train_start,
            train_end,
            test_start,
            test_end,
            120,
            &symbols,
        );

        assert_eq!(key.universe_hash, reordered.universe_hash);
        assert_ne!(key, different_data);
        assert!(key.feature_start() < train_start);
        assert_eq!(key.feature_end(), test_end);
    }

    #[test]
    fn return_history_cache_reuses_overlapping_symbols_incrementally() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let lookback_days = 60;
        let mut cache = SignalDataCache::default();
        let cached_symbols = vec!["AAA".to_string(), "BBB".to_string()];
        let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();

        cache.insert_return_history_symbols(
            &cached_symbols,
            start,
            end,
            lookback_days,
            HashMap::from([
                ("AAA".to_string(), vec![(trade_date, 0.01)]),
                ("BBB".to_string(), vec![(trade_date, -0.02)]),
            ]),
        );

        let requested_symbols = vec![
            "BBB".to_string(),
            "CCC".to_string(),
            "AAA".to_string(),
            "AAA".to_string(),
        ];
        let (missing, cached_history) =
            cache.cached_return_history_symbols(&requested_symbols, start, end, lookback_days);

        assert_eq!(missing, vec!["CCC".to_string()]);
        assert_eq!(cached_history["AAA"], vec![(trade_date, 0.01)]);
        assert_eq!(cached_history["BBB"], vec![(trade_date, -0.02)]);
        assert_eq!(cache.stats().return_history_hits, 2);
        assert_eq!(cache.stats().return_history_misses, 1);

        cache.insert_return_history_symbols(&missing, start, end, lookback_days, HashMap::new());
        let (missing_again, cached_again) =
            cache.cached_return_history_symbols(&requested_symbols, start, end, lookback_days);

        assert!(missing_again.is_empty());
        assert_eq!(cached_again["CCC"], Vec::<(NaiveDate, f64)>::new());
        assert_eq!(cache.stats().return_history_hits, 5);
        assert_eq!(cache.stats().return_history_misses, 1);
    }

    #[test]
    fn signal_data_cache_snapshot_forks_prewarmed_market_features_without_copying_stats() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let lookback_days = 60;
        let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let symbols = vec!["AAA".to_string()];
        let mut base = SignalDataCache::default();
        base.insert_return_history_symbols(
            &symbols,
            start,
            end,
            lookback_days,
            HashMap::from([("AAA".to_string(), vec![(trade_date, 0.01)])]),
        );
        base.insert_average_amount_history_symbols(
            &symbols,
            start,
            end,
            lookback_days,
            HashMap::from([("AAA".to_string(), vec![(trade_date, 1_000.0)])]),
        );

        let snapshot = base.snapshot();
        let mut fork = SignalDataCache::from_snapshot(&snapshot);
        let (missing_returns, returns) =
            fork.cached_return_history_symbols(&symbols, start, end, lookback_days);
        let (missing_amounts, amounts) =
            fork.cached_average_amount_history_symbols(&symbols, start, end, lookback_days);

        assert!(missing_returns.is_empty());
        assert!(missing_amounts.is_empty());
        assert_eq!(returns["AAA"], vec![(trade_date, 0.01)]);
        assert_eq!(amounts["AAA"], vec![(trade_date, 1_000.0)]);
        assert_eq!(base.stats().return_history_hits, 0);
        assert_eq!(fork.stats().return_history_hits, 1);
        assert_eq!(fork.stats().average_amount_hits, 1);
        assert_eq!(fork.stats().average_amount_history_hits, 1);
    }

    #[test]
    fn return_history_cache_reuses_covering_symbol_window() {
        let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let lookback_days = 60;
        let mut cache = SignalDataCache::default();
        let cached_symbols = vec!["AAA".to_string()];

        cache.insert_return_history_symbols(
            &cached_symbols,
            cached_start,
            cached_end,
            lookback_days,
            HashMap::from([(
                "AAA".to_string(),
                vec![
                    (before_request, 0.01),
                    (inside_request, 0.02),
                    (after_request, 0.03),
                ],
            )]),
        );

        let (missing, cached_history) = cache.cached_return_history_symbols(
            &cached_symbols,
            requested_start,
            requested_end,
            lookback_days,
        );

        assert!(missing.is_empty());
        assert_eq!(
            cached_history["AAA"],
            vec![(before_request, 0.01), (inside_request, 0.02)]
        );
        assert_eq!(cache.stats().return_history_hits, 1);
        assert_eq!(cache.stats().return_history_covering_window_hits, 1);
        assert_eq!(cache.stats().return_history_snapshot_hits, 0);
        assert_eq!(cache.stats().return_history_misses, 0);
    }

    #[test]
    fn average_amount_history_cache_reuses_covering_symbol_window() {
        let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let lookback_days = 60;
        let mut cache = SignalDataCache::default();
        let cached_symbols = vec!["AAA".to_string()];

        cache.insert_average_amount_history_symbols(
            &cached_symbols,
            cached_start,
            cached_end,
            lookback_days,
            HashMap::from([(
                "AAA".to_string(),
                vec![
                    (before_request, 100.0),
                    (inside_request, 200.0),
                    (after_request, 300.0),
                ],
            )]),
        );

        let (missing, cached_history) = cache.cached_average_amount_history_symbols(
            &cached_symbols,
            requested_start,
            requested_end,
            lookback_days,
        );

        assert!(missing.is_empty());
        assert_eq!(
            cached_history["AAA"],
            vec![(before_request, 100.0), (inside_request, 200.0)]
        );
        assert_eq!(cache.stats().average_amount_hits, 1);
        assert_eq!(cache.stats().average_amount_history_hits, 1);
        assert_eq!(cache.stats().average_amount_history_covering_window_hits, 1);
        assert_eq!(cache.stats().average_amount_history_snapshot_hits, 0);
        assert_eq!(cache.stats().average_amount_history_misses, 0);
        assert_eq!(cache.stats().average_amount_misses, 0);
    }

    #[test]
    fn market_feature_snapshot_reuses_subset_windows_for_return_and_capacity_history() {
        let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let symbols = vec!["AAA".to_string(), "BBB".to_string()];
        let requested_symbols = vec!["BBB".to_string()];
        let return_lookback_days = 60;
        let amount_lookback_days = PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS;
        let mut cache = SignalDataCache::default();
        let key = MarketFeatureSnapshotKey::new(
            "full-market-2016-v1",
            train_start,
            train_end,
            train_start,
            train_end,
            return_lookback_days,
            &symbols,
        );

        cache.insert_market_feature_snapshot(
            key,
            return_lookback_days,
            amount_lookback_days,
            HashMap::from([
                (
                    "AAA".to_string(),
                    vec![(before_request, 0.01), (inside_request, 0.02)],
                ),
                (
                    "BBB".to_string(),
                    vec![
                        (before_request, -0.01),
                        (inside_request, -0.02),
                        (after_request, -0.03),
                    ],
                ),
            ]),
            HashMap::from([
                (
                    "AAA".to_string(),
                    vec![(before_request, 100.0), (inside_request, 110.0)],
                ),
                (
                    "BBB".to_string(),
                    vec![
                        (before_request, 200.0),
                        (inside_request, 210.0),
                        (after_request, 220.0),
                    ],
                ),
            ]),
        );

        let (missing_returns, return_history) = cache.cached_return_history_symbols(
            &requested_symbols,
            requested_start,
            requested_end,
            return_lookback_days,
        );
        let (missing_amounts, amount_history) = cache.cached_average_amount_history_symbols(
            &requested_symbols,
            requested_start,
            requested_end,
            amount_lookback_days,
        );

        assert!(missing_returns.is_empty());
        assert!(missing_amounts.is_empty());
        assert_eq!(
            return_history["BBB"],
            vec![(before_request, -0.01), (inside_request, -0.02)]
        );
        assert_eq!(
            amount_history["BBB"],
            vec![(before_request, 200.0), (inside_request, 210.0)]
        );
        assert_eq!(cache.stats().return_history_hits, 1);
        assert_eq!(cache.stats().average_amount_hits, 1);
        assert_eq!(cache.stats().average_amount_history_hits, 1);
        assert_eq!(cache.stats().return_history_covering_window_hits, 0);
        assert_eq!(cache.stats().return_history_snapshot_hits, 1);
        assert_eq!(cache.stats().average_amount_history_covering_window_hits, 0);
        assert_eq!(cache.stats().average_amount_history_snapshot_hits, 1);
        assert_eq!(cache.stats().return_history_misses, 0);
        assert_eq!(cache.stats().average_amount_history_misses, 0);
        assert_eq!(cache.stats().average_amount_misses, 0);
    }

    #[test]
    fn market_feature_snapshot_registers_full_score_universe_from_cached_histories() {
        let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let full_universe = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
        let requested_symbols = vec!["BBB".to_string()];
        let return_lookback_days = 60;
        let amount_lookback_days = PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS;
        let mut cache = SignalDataCache::default();
        let key = MarketFeatureSnapshotKey::new(
            "full-market-2016-v1",
            train_start,
            train_end,
            train_start,
            train_end,
            return_lookback_days,
            &full_universe,
        );

        cache.insert_return_history_symbols(
            &full_universe,
            train_start,
            train_end,
            return_lookback_days,
            HashMap::from([
                (
                    "AAA".to_string(),
                    vec![(before_request, 0.01), (inside_request, 0.02)],
                ),
                (
                    "BBB".to_string(),
                    vec![
                        (before_request, -0.01),
                        (inside_request, -0.02),
                        (after_request, -0.03),
                    ],
                ),
                (
                    "CCC".to_string(),
                    vec![(inside_request, 0.03), (after_request, 0.04)],
                ),
            ]),
        );
        cache.insert_average_amount_history_symbols(
            &full_universe,
            train_start,
            train_end,
            amount_lookback_days,
            HashMap::from([
                (
                    "AAA".to_string(),
                    vec![(before_request, 100.0), (inside_request, 110.0)],
                ),
                (
                    "BBB".to_string(),
                    vec![
                        (before_request, 200.0),
                        (inside_request, 210.0),
                        (after_request, 220.0),
                    ],
                ),
                (
                    "CCC".to_string(),
                    vec![(inside_request, 300.0), (after_request, 310.0)],
                ),
            ]),
        );

        cache.insert_market_feature_snapshot_from_cached_histories(
            key,
            return_lookback_days,
            amount_lookback_days,
            &full_universe,
            train_start,
            train_end,
        );
        let snapshot = cache.snapshot();
        let mut fork = SignalDataCache {
            market_feature_snapshots: snapshot.market_feature_snapshots.clone(),
            ..Default::default()
        };

        let (missing_returns, return_history) = fork.cached_return_history_symbols(
            &requested_symbols,
            requested_start,
            requested_end,
            return_lookback_days,
        );
        let (missing_amounts, amount_history) = fork.cached_average_amount_history_symbols(
            &requested_symbols,
            requested_start,
            requested_end,
            amount_lookback_days,
        );

        assert!(missing_returns.is_empty());
        assert!(missing_amounts.is_empty());
        assert_eq!(
            return_history["BBB"],
            vec![(before_request, -0.01), (inside_request, -0.02)]
        );
        assert_eq!(
            amount_history["BBB"],
            vec![(before_request, 200.0), (inside_request, 210.0)]
        );
        assert_eq!(fork.stats().return_history_snapshot_hits, 1);
        assert_eq!(fork.stats().average_amount_history_snapshot_hits, 1);
    }

    #[test]
    fn factor_signal_prewarm_groups_union_symbols_by_exact_window_and_lookback() {
        let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let candidates = vec![
            FactorSignalFeaturePrewarmCandidate {
                data_version_id: "full-market-2016-v1".to_string(),
                train_start,
                train_end,
                test_start: train_start,
                test_end: train_end,
                feature_start: train_start,
                feature_end: train_end,
                lookback_days: 60,
                symbols: vec!["BBB".to_string(), "AAA".to_string()],
            },
            FactorSignalFeaturePrewarmCandidate {
                data_version_id: "full-market-2016-v1".to_string(),
                train_start,
                train_end,
                test_start: train_start,
                test_end: train_end,
                feature_start: train_start,
                feature_end: train_end,
                lookback_days: 60,
                symbols: vec!["CCC".to_string(), "AAA".to_string()],
            },
            FactorSignalFeaturePrewarmCandidate {
                data_version_id: "full-market-2016-v1".to_string(),
                train_start,
                train_end,
                test_start: train_start,
                test_end: train_end,
                feature_start: train_start,
                feature_end: train_end,
                lookback_days: 120,
                symbols: vec!["AAA".to_string()],
            },
        ];

        let groups = merge_factor_signal_feature_prewarm_groups(candidates);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].key.lookback_days, 60);
        assert_eq!(
            groups[0].symbols,
            vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()]
        );
        assert_eq!(groups[1].key.lookback_days, 120);
        assert_eq!(groups[1].symbols, vec!["AAA".to_string()]);
    }

    #[test]
    fn average_amount_cache_reuses_overlapping_symbols_incrementally() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let mut cache = SignalDataCache::default();
        let cached_symbols = vec!["AAA".to_string(), "BBB".to_string()];

        cache.insert_average_amount_symbols(
            &cached_symbols,
            start,
            end,
            HashMap::from([("AAA".to_string(), 5_000.0), ("BBB".to_string(), 3_000.0)]),
        );

        let requested_symbols = vec![
            "BBB".to_string(),
            "CCC".to_string(),
            "AAA".to_string(),
            "AAA".to_string(),
        ];
        let (missing, cached_amounts) =
            cache.cached_average_amount_symbols(&requested_symbols, start, end);

        assert_eq!(missing, vec!["CCC".to_string()]);
        assert_eq!(cached_amounts["AAA"], 5_000.0);
        assert_eq!(cached_amounts["BBB"], 3_000.0);
        assert_eq!(cache.stats().average_amount_hits, 2);
        assert_eq!(cache.stats().average_amount_symbol_hits, 2);
        assert_eq!(cache.stats().average_amount_history_hits, 0);
        assert_eq!(cache.stats().average_amount_misses, 1);
        assert_eq!(cache.stats().average_amount_symbol_misses, 1);
        assert_eq!(cache.stats().average_amount_history_misses, 0);

        cache.insert_average_amount_symbols(&missing, start, end, HashMap::new());
        let (missing_again, cached_again) =
            cache.cached_average_amount_symbols(&requested_symbols, start, end);

        assert!(missing_again.is_empty());
        assert!(!cached_again.contains_key("CCC"));
        assert_eq!(cache.stats().average_amount_hits, 5);
        assert_eq!(cache.stats().average_amount_symbol_hits, 5);
        assert_eq!(cache.stats().average_amount_misses, 1);
        assert_eq!(cache.stats().average_amount_symbol_misses, 1);
    }

    #[test]
    fn average_amount_cache_does_not_reuse_covering_aggregate_window() {
        let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let mut cache = SignalDataCache::default();
        let cached_symbols = vec!["AAA".to_string()];

        cache.insert_average_amount_symbols(
            &cached_symbols,
            cached_start,
            cached_end,
            HashMap::from([("AAA".to_string(), 5_000.0)]),
        );

        let (missing, cached_amounts) =
            cache.cached_average_amount_symbols(&cached_symbols, requested_start, requested_end);

        assert_eq!(missing, vec!["AAA".to_string()]);
        assert!(cached_amounts.is_empty());
        assert_eq!(cache.stats().average_amount_hits, 0);
        assert_eq!(cache.stats().average_amount_misses, 1);
    }

    #[test]
    fn combo_score_cache_key_is_direction_aware_only_when_candidate_pool_is_pruned() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

        let full_desc = SignalDataCacheKey::combo_scores(
            "phase7_price_volume_expanded_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            None,
            TradableUniverseProfile::All,
        );
        let full_asc = SignalDataCacheKey::combo_scores(
            "phase7_price_volume_expanded_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Ascending,
            None,
            TradableUniverseProfile::All,
        );
        let pruned_desc = SignalDataCacheKey::combo_scores(
            "phase7_price_volume_expanded_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(200),
            TradableUniverseProfile::All,
        );
        let pruned_asc = SignalDataCacheKey::combo_scores(
            "phase7_price_volume_expanded_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Ascending,
            Some(200),
            TradableUniverseProfile::All,
        );

        assert_eq!(full_desc, full_asc);
        assert_ne!(pruned_desc, pruned_asc);
    }

    #[test]
    fn combo_score_pruned_query_uses_directional_daily_window_rank() {
        let descending = combo_score_load_sql(
            ScoreDirection::Descending,
            Some(200),
            TradableUniverseProfile::All,
        );
        let ascending = combo_score_load_sql(
            ScoreDirection::Ascending,
            Some(200),
            TradableUniverseProfile::All,
        );

        assert!(descending.contains("ROW_NUMBER() OVER"));
        assert!(descending.contains("PARTITION BY mfv.trade_date"));
        assert!(descending.contains("COALESCE(mfv.raw_score, 0.0) DESC"));
        assert!(descending.contains("score_rank <= $5"));
        assert!(ascending.contains("COALESCE(mfv.raw_score, 0.0) ASC"));
    }

    #[test]
    fn combo_score_query_filters_tradable_universe_before_daily_ranking() {
        let sql = combo_score_load_sql(
            ScoreDirection::Descending,
            Some(200),
            TradableUniverseProfile::ListedNonSt,
        );

        assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
        assert!(sql.contains("ms.list_status = 'L'"));
        assert!(sql.contains("COALESCE(ms.is_st, false) = false"));
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("score_rank <= $5"));
    }

    #[test]
    fn combo_score_cache_key_separates_universe_profiles() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

        let all = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(500),
            TradableUniverseProfile::All,
        );
        let listed_non_st = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(500),
            TradableUniverseProfile::ListedNonSt,
        );

        assert_ne!(all, listed_non_st);
    }

    #[test]
    fn signal_data_cache_tracks_hits_for_reused_combo_scores() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            None,
            TradableUniverseProfile::All,
        );
        let mut cache = SignalDataCache::default();
        let mut scores = HashMap::new();
        scores.insert(
            start,
            vec![("AAA".to_string(), 0.1), ("BBB".to_string(), 0.2)],
        );

        cache.store_combo_scores_for_test(key.clone(), scores);
        let first = cache.cached_combo_scores(&key).expect("first hit");
        let second = cache.cached_combo_scores(&key).expect("second hit");

        assert_eq!(first.len(), second.len());
        assert_eq!(cache.stats().combo_score_hits, 2);
        assert_eq!(cache.stats().combo_score_misses, 0);
    }

    #[test]
    fn signal_data_cache_reuses_larger_combo_candidate_pool_for_smaller_request() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let cached_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(3),
            TradableUniverseProfile::All,
        );
        let requested_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(2),
            TradableUniverseProfile::All,
        );
        let mut scores = HashMap::new();
        scores.insert(
            start,
            vec![
                ("AAA".to_string(), 0.9),
                ("BBB".to_string(), 0.8),
                ("CCC".to_string(), 0.7),
            ],
        );
        let mut cache = SignalDataCache::default();
        cache.store_combo_scores_for_test(cached_key, scores);

        let reused = cache
            .cached_combo_scores(&requested_key)
            .expect("larger candidate pool should satisfy smaller request");

        assert_eq!(
            reused.get(&start).unwrap(),
            &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
        );
        assert_eq!(cache.stats().combo_score_hits, 1);
        assert_eq!(cache.stats().combo_score_misses, 0);
    }

    #[test]
    fn signal_data_cache_reuses_covering_combo_score_window() {
        let wide_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let narrow_day = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let outside_day = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let cached_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            wide_start,
            wide_end,
            ScoreDirection::Descending,
            Some(3),
            TradableUniverseProfile::ListedNonSt,
        );
        let requested_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            narrow_start,
            narrow_end,
            ScoreDirection::Descending,
            Some(2),
            TradableUniverseProfile::ListedNonSt,
        );
        let scores = HashMap::from([
            (
                narrow_day,
                vec![
                    ("AAA".to_string(), 0.9),
                    ("BBB".to_string(), 0.8),
                    ("CCC".to_string(), 0.7),
                ],
            ),
            (outside_day, vec![("DDD".to_string(), 0.6)]),
        ]);
        let mut cache = SignalDataCache::default();
        cache.store_combo_scores_for_test(cached_key, scores);

        let reused = cache
            .cached_combo_scores(&requested_key)
            .expect("wider combo score window should satisfy narrower request");

        assert_eq!(reused.len(), 1);
        assert_eq!(
            reused.get(&narrow_day).unwrap(),
            &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
        );
        assert!(!reused.contains_key(&outside_day));
        assert_eq!(cache.stats().combo_score_hits, 1);
        assert_eq!(cache.stats().combo_score_misses, 0);
    }

    #[test]
    fn signal_data_cache_ranks_unbounded_covering_combo_window_for_directional_request() {
        let wide_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let narrow_day = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
        let cached_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            wide_start,
            wide_end,
            ScoreDirection::Descending,
            None,
            TradableUniverseProfile::ListedNonSt,
        );
        let requested_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            narrow_start,
            narrow_end,
            ScoreDirection::Ascending,
            Some(2),
            TradableUniverseProfile::ListedNonSt,
        );
        let scores = HashMap::from([(
            narrow_day,
            vec![
                ("AAA".to_string(), 0.9),
                ("BBB".to_string(), 0.7),
                ("CCC".to_string(), 0.8),
            ],
        )]);
        let mut cache = SignalDataCache::default();
        cache.store_combo_scores_for_test(cached_key, scores);

        let reused = cache
            .cached_combo_scores(&requested_key)
            .expect("unbounded covering window should be ranked for requested direction");

        assert_eq!(
            reused.get(&narrow_day).unwrap(),
            &vec![("BBB".to_string(), 0.7), ("CCC".to_string(), 0.8)]
        );
        assert_eq!(cache.stats().combo_score_hits, 1);
        assert_eq!(cache.stats().combo_score_misses, 0);
    }

    #[test]
    fn signal_data_cache_reuses_unbounded_combo_scores_for_directional_pool_request() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let cached_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            None,
            TradableUniverseProfile::ListedNonSt,
        );
        let requested_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(2),
            TradableUniverseProfile::ListedNonSt,
        );
        let mut scores = HashMap::new();
        scores.insert(
            start,
            vec![
                ("CCC".to_string(), 0.7),
                ("AAA".to_string(), 0.9),
                ("BBB".to_string(), 0.8),
            ],
        );
        let mut cache = SignalDataCache::default();
        cache.store_combo_scores_for_test(cached_key, scores);

        let reused = cache
            .cached_combo_scores(&requested_key)
            .expect("unbounded cache should satisfy directional pool request");

        assert_eq!(
            reused.get(&start).unwrap(),
            &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
        );
        assert_eq!(cache.stats().combo_score_hits, 1);
        assert_eq!(cache.stats().combo_score_misses, 0);
    }

    #[test]
    fn signal_data_cache_does_not_reuse_combo_candidate_pool_across_direction_or_universe() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let cached_key = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(500),
            TradableUniverseProfile::All,
        );
        let ascending_request = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Ascending,
            Some(400),
            TradableUniverseProfile::All,
        );
        let universe_request = SignalDataCacheKey::combo_scores(
            "phase7_financial_quality_v1",
            "1.0.0",
            start,
            end,
            ScoreDirection::Descending,
            Some(400),
            TradableUniverseProfile::ListedNonSt,
        );
        let mut cache = SignalDataCache::default();
        cache.store_combo_scores_for_test(cached_key, HashMap::from([(start, Vec::new())]));

        assert!(cache.cached_combo_scores(&ascending_request).is_none());
        assert!(cache.cached_combo_scores(&universe_request).is_none());
        assert_eq!(cache.stats().combo_score_hits, 0);
        assert_eq!(cache.stats().combo_score_misses, 2);
    }

    #[test]
    fn signal_data_cache_tracks_hits_for_reused_prediction_scores() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let key = SignalDataCacheKey::prediction_scores("pred-phase7-wide", start, end);
        let mut cache = SignalDataCache::default();
        let scores = HashMap::from([(
            start,
            vec![
                ("AAA".to_string(), 0.1, Some(1)),
                ("BBB".to_string(), 0.2, Some(2)),
            ],
        )]);

        cache.store_prediction_scores_for_test(key.clone(), scores);
        let first = cache.cached_prediction_scores(&key).expect("first hit");
        let second = cache.cached_prediction_scores(&key).expect("second hit");

        assert_eq!(first.len(), second.len());
        assert_eq!(cache.stats().prediction_score_hits, 2);
        assert_eq!(cache.stats().prediction_score_misses, 0);
    }

    #[test]
    fn liquidity_filter_uses_cached_average_amount_inputs() {
        let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let mut scores_by_date = HashMap::from([(
            trade_date,
            vec![
                ("LIQUID".to_string(), 0.9),
                ("THIN".to_string(), 0.8),
                ("MISSING".to_string(), 0.7),
            ],
        )]);
        let average_amounts = HashMap::from([
            ("LIQUID".to_string(), 5_000.0),
            ("THIN".to_string(), 1_000.0),
        ]);

        let stats = retain_scores_with_min_average_amount(
            &mut scores_by_date,
            3_000_000.0,
            &average_amounts,
        );

        assert_eq!(stats.before, 3);
        assert_eq!(stats.after, 1);
        assert_eq!(stats.liquid_symbols, 1);
        assert_eq!(
            scores_by_date[&trade_date],
            vec![("LIQUID".to_string(), 0.9)]
        );
    }

    #[test]
    fn market_regime_policy_classifies_and_applies_bear_defensive_rule() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
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
    fn market_regime_rule_can_route_to_regime_specific_alpha_combo() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_regime_alpha_switch_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.combo_name, "phase7_industry_residual_quality_v1");
        assert_eq!(bear.version, "1.0.0");
        assert_eq!(bear.score_direction, ScoreDirection::Descending);
        assert_eq!(bear.max_gross_exposure, 0.78);
        assert_eq!(
            high_volatility.combo_name,
            "phase7_industry_residual_quality_v1"
        );
        assert_eq!(high_volatility.score_direction, ScoreDirection::Descending);
        assert_eq!(high_volatility.max_gross_exposure, 0.66);
    }

    #[test]
    fn market_regime_alpha_switch_variants_route_to_distinct_stress_sleeves() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };

        let value = MarketRegimePolicy::quality_regime_alpha_switch_value_v1("000300.SH")
            .apply(&base, MarketRegime::Bear);
        let recovery = MarketRegimePolicy::quality_regime_alpha_switch_recovery_v1("000300.SH")
            .apply(&base, MarketRegime::Bear);
        let blend = MarketRegimePolicy::quality_regime_alpha_switch_blend_v1("000300.SH")
            .apply(&base, MarketRegime::HighVolatility);

        assert_eq!(value.combo_name, "phase7_valuation_v1");
        assert_eq!(value.score_direction, ScoreDirection::Descending);
        assert_eq!(recovery.combo_name, "phase7_growth_recovery_v1");
        assert_eq!(recovery.score_direction, ScoreDirection::Descending);
        assert_eq!(blend.combo_name, "phase7_quality_value_recovery_confirm_v1");
        assert_eq!(blend.score_direction, ScoreDirection::Descending);
    }

    #[test]
    fn drawdown_control_policy_deleverages_bear_and_high_volatility() {
        let base = SignalConfig {
            top_n: 80,
            rebalance_freq_days: 20,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::drawdown_control_v1("000300.SH");

        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.bear_drawdown_threshold, 0.12);
        assert_eq!(policy.high_volatility_threshold, 0.24);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.rebalance_freq_days, 60);
        assert_eq!(bear.max_gross_exposure, 0.35);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.skip_top_pct, 0.10);
        assert_eq!(high_volatility.max_gross_exposure, 0.25);
    }

    #[test]
    fn drawdown_control_v2_is_stricter_than_v1() {
        let base = SignalConfig {
            top_n: 80,
            rebalance_freq_days: 20,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(10, 2),
            score_direction: ScoreDirection::Descending,
            ..Default::default()
        };
        let v1 = MarketRegimePolicy::drawdown_control_v1("000300.SH");
        let v2 = MarketRegimePolicy::drawdown_control_v2("000300.SH");

        let bear_v1 = v1.apply(&base, MarketRegime::Bear);
        let bear_v2 = v2.apply(&base, MarketRegime::Bear);
        let high_volatility_v1 = v1.apply(&base, MarketRegime::HighVolatility);
        let high_volatility_v2 = v2.apply(&base, MarketRegime::HighVolatility);
        let sideways_v2 = v2.apply(&base, MarketRegime::Sideways);

        assert!(v2.bear_drawdown_threshold < v1.bear_drawdown_threshold);
        assert!(v2.high_volatility_threshold < v1.high_volatility_threshold);
        assert_eq!(v2.min_observations, 10);
        assert_eq!(bear_v2.top_n, 15);
        assert_eq!(bear_v2.max_gross_exposure, 0.25);
        assert_eq!(bear_v2.max_position_pct, Decimal::new(4, 2));
        assert_eq!(bear_v2.skip_top_pct, 0.15);
        assert_eq!(bear_v2.score_direction, ScoreDirection::Ascending);
        assert!(bear_v2.max_gross_exposure < bear_v1.max_gross_exposure);
        assert_eq!(high_volatility_v2.top_n, 15);
        assert_eq!(high_volatility_v2.max_gross_exposure, 0.18);
        assert_eq!(high_volatility_v2.max_position_pct, Decimal::new(4, 2));
        assert!(high_volatility_v2.max_gross_exposure < high_volatility_v1.max_gross_exposure);
        assert_eq!(sideways_v2.max_gross_exposure, 0.55);
        assert_eq!(sideways_v2.max_position_pct, Decimal::new(6, 2));
    }

    #[test]
    fn quality_risk_off_keeps_alpha_direction_while_reducing_exposure() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_risk_off_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bull.rebalance_freq_days, 60);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.max_gross_exposure, 0.80);
        assert_eq!(bear.max_position_pct, Decimal::new(15, 2));
        assert_eq!(high_volatility.score_direction, ScoreDirection::Ascending);
        assert_eq!(high_volatility.max_gross_exposure, 0.65);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(12, 2));
    }

    #[test]
    fn quality_crash_guard_preserves_alpha_shape_and_only_scales_tail_regimes() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_crash_guard_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let sideways = policy.apply(&base, MarketRegime::Sideways);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.bear_drawdown_threshold, 0.25);
        assert_eq!(policy.high_volatility_threshold, 0.50);
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bull.rebalance_freq_days, 60);
        assert_eq!(bull.skip_top_pct, 0.10);
        assert_eq!(sideways.max_gross_exposure, 1.0);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.rebalance_freq_days, 60);
        assert_eq!(bear.skip_top_pct, 0.10);
        assert_eq!(bear.max_gross_exposure, 0.85);
        assert_eq!(bear.max_position_pct, Decimal::new(12, 2));
        assert_eq!(high_volatility.max_gross_exposure, 0.75);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(10, 2));
    }

    #[test]
    fn quality_crash_guard_v2_preserves_alpha_shape_with_stronger_tail_scaling() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_crash_guard_v2("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let sideways = policy.apply(&base, MarketRegime::Sideways);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.bear_drawdown_threshold, 0.22);
        assert_eq!(policy.high_volatility_threshold, 0.45);
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bull.skip_top_pct, 0.10);
        assert_eq!(sideways.max_gross_exposure, 1.0);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.rebalance_freq_days, 60);
        assert_eq!(bear.skip_top_pct, 0.10);
        assert_eq!(bear.max_gross_exposure, 0.75);
        assert_eq!(bear.max_position_pct, Decimal::new(10, 2));
        assert_eq!(high_volatility.max_gross_exposure, 0.60);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(8, 2));
    }

    #[test]
    fn quality_crash_guard_v3_keeps_late_trigger_with_mid_tail_scaling() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_crash_guard_v3("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.bear_drawdown_threshold, 0.25);
        assert_eq!(policy.high_volatility_threshold, 0.50);
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.skip_top_pct, 0.10);
        assert_eq!(bear.max_gross_exposure, 0.80);
        assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
        assert_eq!(high_volatility.max_gross_exposure, 0.68);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
    }

    #[test]
    fn quality_bear_window_guard_triggers_earlier_without_flipping_quality_alpha() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_bear_window_guard_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.lookback_days, 126);
        assert_eq!(policy.bear_drawdown_threshold, 0.16);
        assert_eq!(policy.high_volatility_threshold, 0.32);
        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.rebalance_freq_days, 60);
        assert_eq!(bear.skip_top_pct, 0.10);
        assert_eq!(bear.max_gross_exposure, 0.78);
        assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
        assert_eq!(high_volatility.score_direction, ScoreDirection::Ascending);
        assert_eq!(high_volatility.max_gross_exposure, 0.66);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
    }

    #[test]
    fn quality_bear_position_guard_changes_only_position_risk_shape_in_stress_regimes() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_bear_position_guard_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.lookback_days, 126);
        assert_eq!(policy.bear_drawdown_threshold, 0.14);
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 25);
        assert_eq!(bear.rebalance_freq_days, 80);
        assert_eq!(bear.skip_top_pct, 0.05);
        assert_eq!(bear.max_gross_exposure, 0.74);
        assert_eq!(bear.max_position_pct, Decimal::new(9, 2));
        assert_eq!(high_volatility.top_n, 25);
        assert_eq!(high_volatility.rebalance_freq_days, 40);
        assert_eq!(high_volatility.max_gross_exposure, 0.58);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(75, 3));
    }

    #[test]
    fn quality_bear_position_guard_v3_is_milder_for_return_preservation() {
        let base = SignalConfig {
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_bear_position_guard_v3("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(policy.lookback_days, 126);
        assert_eq!(policy.bear_drawdown_threshold, 0.14);
        assert_eq!(bull.score_direction, ScoreDirection::Ascending);
        assert_eq!(bull.top_n, 20);
        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.top_n, 25);
        assert_eq!(bear.rebalance_freq_days, 80);
        assert_eq!(bear.skip_top_pct, 0.05);
        assert_eq!(bear.max_gross_exposure, 0.82);
        assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
        assert_eq!(high_volatility.top_n, 25);
        assert_eq!(high_volatility.rebalance_freq_days, 40);
        assert_eq!(high_volatility.max_gross_exposure, 0.66);
        assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
    }

    #[test]
    fn quality_event_window_position_guard_keeps_event_sleeve_and_position_shape() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_event_window_position_guard_v3("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert!(bull.portfolio_sleeve.is_none());
        assert_eq!(bear.top_n, 25);
        assert_eq!(bear.rebalance_freq_days, 80);
        assert_eq!(bear.max_gross_exposure, 0.82);
        assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(bear_sleeve.version, "1.0.0");
        assert_eq!(bear_sleeve.score_direction, ScoreDirection::Descending);
        assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
        assert_eq!(high_volatility.top_n, 25);
        assert_eq!(high_volatility.max_gross_exposure, 0.66);
        assert!(high_volatility.portfolio_sleeve.is_some());
    }

    #[test]
    fn quality_event_window_return_sharpe_router_tightens_correlation_in_stress_only() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_event_window_return_sharpe_router_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(bull.max_pairwise_correlation, Some(0.75));
        assert!(bull.portfolio_sleeve.is_none());
        assert_eq!(bear.max_pairwise_correlation, Some(0.65));
        assert_eq!(bear.top_n, 20);
        assert_eq!(bear.max_gross_exposure, 0.72);
        assert_eq!(bear.max_position_pct, Decimal::new(10, 2));
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
        assert_eq!(high_volatility.max_pairwise_correlation, Some(0.65));
        assert_eq!(high_volatility.max_gross_exposure, 0.58);
        assert!(high_volatility.portfolio_sleeve.is_some());
    }

    #[test]
    fn quality_event_window_return_sharpe_router_frontier_interpolates_exposure() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let balanced =
            MarketRegimePolicy::quality_event_window_return_sharpe_router_v3("000300.SH");
        let tighter = MarketRegimePolicy::quality_event_window_return_sharpe_router_v4("000300.SH");

        let balanced_bear = balanced.apply(&base, MarketRegime::Bear);
        let tighter_bear = tighter.apply(&base, MarketRegime::Bear);
        let balanced_high_vol = balanced.apply(&base, MarketRegime::HighVolatility);
        let tighter_high_vol = tighter.apply(&base, MarketRegime::HighVolatility);

        assert_eq!(balanced_bear.max_gross_exposure, 0.75);
        assert_eq!(balanced_bear.max_position_pct, Decimal::new(10, 2));
        assert_eq!(balanced_high_vol.max_gross_exposure, 0.62);
        assert_eq!(balanced_high_vol.max_position_pct, Decimal::new(9, 2));
        assert_eq!(tighter_bear.max_gross_exposure, 0.68);
        assert_eq!(tighter_bear.max_position_pct, Decimal::new(9, 2));
        assert_eq!(tighter_high_vol.max_gross_exposure, 0.54);
        assert_eq!(tighter_high_vol.max_position_pct, Decimal::new(7, 2));
        assert!(balanced_bear.portfolio_sleeve.is_some());
        assert!(tighter_bear.portfolio_sleeve.is_some());
    }

    #[test]
    fn quality_state_alpha_selector_routes_distinct_sleeves_by_regime() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_state_alpha_selector_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let sideways = policy.apply(&base, MarketRegime::Sideways);
        let mixed = policy.apply(&base, MarketRegime::Mixed);

        let bull_sleeve = bull.portfolio_sleeve.expect("bull quality/value sleeve");
        assert_eq!(
            bull_sleeve.combo_name,
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(bull_sleeve.score_direction, ScoreDirection::Descending);
        assert!((bull_sleeve.weight - 0.05).abs() < 1e-9);
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        assert_eq!(bear.max_pairwise_correlation, Some(0.65));
        assert_eq!(bear.max_gross_exposure, 0.68);
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
        let sideways_sleeve = sideways.portfolio_sleeve.expect("sideways low-risk sleeve");
        assert_eq!(
            sideways_sleeve.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(sideways_sleeve.score_direction, ScoreDirection::Ascending);
        assert!((sideways_sleeve.weight - 0.10).abs() < 1e-9);
        let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value sleeve");
        assert_eq!(mixed_sleeve.combo_name, "phase7_valuation_v1");
        assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Descending);
        assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
    }

    #[test]
    fn quality_state_alpha_overlay_selector_adds_small_orthogonal_overlay() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_state_alpha_overlay_selector_v1("000300.SH");

        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        let bear_overlay = bear.score_overlay.expect("bear valuation overlay");
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(bear_overlay.combo_name, "phase7_valuation_v1");
        assert_eq!(bear_overlay.score_direction, ScoreDirection::Descending);
        assert!((bear_overlay.weight - 0.05).abs() < 1e-9);
        let high_volatility_sleeve = high_volatility
            .portfolio_sleeve
            .expect("high-volatility low-risk sleeve");
        let high_volatility_overlay = high_volatility
            .score_overlay
            .expect("high-volatility low-risk overlay");
        assert_eq!(
            high_volatility_sleeve.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(
            high_volatility_overlay.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(
            high_volatility_overlay.score_direction,
            ScoreDirection::Ascending
        );
        assert!((high_volatility_overlay.weight - 0.05).abs() < 1e-9);
    }

    #[test]
    fn quality_mixed_event_state_selector_routes_event_flow_in_mixed_state_only() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_mixed_event_state_overlay_selector_v1("000300.SH");

        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let sideways = policy.apply(&base, MarketRegime::Sideways);
        let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed event-window sleeve");
        let mixed_overlay = mixed.score_overlay.expect("mixed valuation overlay");
        let sideways_sleeve = sideways
            .portfolio_sleeve
            .expect("sideways valuation sleeve");

        assert_eq!(mixed_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Descending);
        assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
        assert_eq!(mixed_overlay.combo_name, "phase7_valuation_v1");
        assert!((mixed_overlay.weight - 0.05).abs() < 1e-9);
        assert_eq!(sideways_sleeve.combo_name, "phase7_valuation_v1");
        assert!(sideways.score_overlay.is_none());
    }

    #[test]
    fn quality_mixed_state_risk_memory_router_only_tightens_mixed_state() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v4("000300.SH");

        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let bull = policy.apply(&base, MarketRegime::Bull);
        let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed event-window sleeve");

        assert_eq!(mixed_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(mixed.max_gross_exposure, 0.90);
        assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bull.max_position_pct, Decimal::new(15, 2));
        assert_eq!(bull.max_pairwise_correlation, Some(0.75));
    }

    #[test]
    fn quality_mixed_state_risk_memory_router_frontier_relaxes_mixed_risk_only() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let strict = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v14("000300.SH");
        let relaxed = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v16("000300.SH");

        let strict_mixed = strict.apply(&base, MarketRegime::Mixed);
        let strict_bear = strict.apply(&base, MarketRegime::Bear);
        let strict_bull = strict.apply(&base, MarketRegime::Bull);
        let relaxed_mixed = relaxed.apply(&base, MarketRegime::Mixed);
        let relaxed_bear = relaxed.apply(&base, MarketRegime::Bear);
        let relaxed_bull = relaxed.apply(&base, MarketRegime::Bull);

        assert_eq!(strict_mixed.max_gross_exposure, 1.0);
        assert_eq!(strict_mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(strict_mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(relaxed_mixed.max_gross_exposure, 1.0);
        assert_eq!(relaxed_mixed.max_position_pct, Decimal::new(15, 2));
        assert_eq!(relaxed_mixed.max_pairwise_correlation, Some(0.75));
        assert_eq!(relaxed_bear.max_position_pct, strict_bear.max_position_pct);
        assert_eq!(
            relaxed_bear.max_pairwise_correlation,
            strict_bear.max_pairwise_correlation
        );
        assert_eq!(relaxed_bull.max_position_pct, strict_bull.max_position_pct);
        assert_eq!(
            relaxed_bull.max_pairwise_correlation,
            strict_bull.max_pairwise_correlation
        );
    }

    #[test]
    fn quality_mixed_orthogonal_alpha_routes_residual_confirmation_in_mixed_state() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_mixed_orthogonal_alpha_selector_v1("000300.SH");

        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let mixed_sleeve = mixed
            .portfolio_sleeve
            .expect("mixed residual confirmation sleeve");
        let mixed_overlay = mixed
            .score_overlay
            .expect("mixed valuation confirmation overlay");
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event-window sleeve");

        assert_eq!(
            mixed_sleeve.combo_name,
            "phase7_quality_residual_confirm_10pct_v1"
        );
        assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Ascending);
        assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
        assert_eq!(mixed_overlay.combo_name, "phase7_valuation_v1");
        assert_eq!(mixed_overlay.score_direction, ScoreDirection::Descending);
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    }

    #[test]
    fn quality_mixed_orthogonal_risk_memory_reuses_cc_risk_shell() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v2("000300.SH");

        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let bull = policy.apply(&base, MarketRegime::Bull);
        let mixed_sleeve = mixed
            .portfolio_sleeve
            .expect("mixed value/recovery/event sleeve");
        let mixed_overlay = mixed
            .score_overlay
            .expect("mixed residual confirmation overlay");

        assert_eq!(
            mixed_sleeve.combo_name,
            "phase7_quality_value_recovery_event_confirm_v1"
        );
        assert_eq!(
            mixed_overlay.combo_name,
            "phase7_quality_residual_confirm_10pct_v1"
        );
        assert_eq!(mixed.max_gross_exposure, 0.90);
        assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bull.max_pairwise_correlation, Some(0.75));
    }

    #[test]
    fn quality_state_sharpe_bridge_router_preserves_bx_return_sleeves_with_tighter_stress_risk() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_state_sharpe_bridge_router_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);
        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event-window sleeve");
        let high_volatility_sleeve = high_volatility
            .portfolio_sleeve
            .expect("high-volatility low-risk sleeve");
        let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value/recovery sleeve");

        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(
            high_volatility_sleeve.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(
            mixed_sleeve.combo_name,
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(bear.max_gross_exposure, 0.70);
        assert_eq!(high_volatility.max_gross_exposure, 0.56);
        assert_eq!(mixed.max_gross_exposure, 0.96);
        assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
    }

    #[test]
    fn quality_nonlinear_alpha_router_uses_distinct_state_alpha_without_handpicked_dates() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_nonlinear_alpha_router_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        let bull_sleeve = bull
            .portfolio_sleeve
            .expect("bull valuation/recovery sleeve");
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        let bear_overlay = bear.score_overlay.expect("bear valuation overlay");
        let mixed_sleeve = mixed
            .portfolio_sleeve
            .expect("mixed residual confirmation sleeve");
        let mixed_overlay = mixed.score_overlay.expect("mixed quality/recovery overlay");
        let high_vol_sleeve = high_volatility
            .portfolio_sleeve
            .expect("high-volatility low-risk sleeve");

        assert_eq!(
            bull_sleeve.combo_name,
            "phase7_quality_value_recovery_event_confirm_v1"
        );
        assert_eq!(bull_sleeve.score_direction, ScoreDirection::Descending);
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(bear_overlay.combo_name, "phase7_valuation_v1");
        assert_eq!(
            mixed_sleeve.combo_name,
            "phase7_quality_residual_confirm_10pct_v1"
        );
        assert_eq!(
            mixed_overlay.combo_name,
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(mixed.max_gross_exposure, 0.98);
        assert_eq!(mixed.max_position_pct, Decimal::new(14, 2));
        assert_eq!(mixed.max_pairwise_correlation, Some(0.72));
        assert_eq!(
            high_vol_sleeve.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(high_volatility.max_gross_exposure, 0.60);
    }

    #[test]
    fn quality_nonlinear_alpha_risk_memory_relaxed_routers_bridge_return_without_date_rules() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let strict = MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v1("000300.SH");
        let relaxed =
            MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v2("000300.SH");
        let overlay_relaxed =
            MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v3("000300.SH");

        let strict_mixed = strict.apply(&base, MarketRegime::Mixed);
        let relaxed_mixed = relaxed.apply(&base, MarketRegime::Mixed);
        let overlay_mixed = overlay_relaxed.apply(&base, MarketRegime::Mixed);

        assert_eq!(strict_mixed.max_gross_exposure, 0.94);
        assert_eq!(strict_mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(strict_mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(relaxed_mixed.max_gross_exposure, 0.97);
        assert_eq!(relaxed_mixed.max_position_pct, Decimal::new(14, 2));
        assert_eq!(relaxed_mixed.max_pairwise_correlation, Some(0.72));
        assert_eq!(overlay_mixed.max_gross_exposure, 0.98);
        assert_eq!(overlay_mixed.max_pairwise_correlation, Some(0.75));
        assert!(relaxed_mixed.portfolio_sleeve.is_some());
        assert!(overlay_mixed.score_overlay.is_some());
    }

    #[test]
    fn quality_frontier_regime_bridge_keeps_return_sleeves_and_v14_mixed_risk_memory() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_frontier_regime_bridge_router_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);
        let mixed = policy.apply(&base, MarketRegime::Mixed);
        let sideways = policy.apply(&base, MarketRegime::Sideways);
        let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
        let high_vol_sleeve = high_volatility
            .portfolio_sleeve
            .expect("high-volatility price/volume sleeve");
        let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value/recovery sleeve");
        let sideways_sleeve = sideways
            .portfolio_sleeve
            .expect("sideways valuation sleeve");

        assert_eq!(bull.max_gross_exposure, 1.0);
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(
            high_vol_sleeve.combo_name,
            "phase7_price_volume_expanded_v1"
        );
        assert_eq!(
            mixed_sleeve.combo_name,
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(sideways_sleeve.combo_name, "phase7_valuation_v1");
        assert_eq!(bear.max_gross_exposure, 0.74);
        assert_eq!(high_volatility.max_gross_exposure, 0.60);
        assert_eq!(mixed.max_gross_exposure, 1.0);
        assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
    }

    #[test]
    fn quality_frontier_regime_bridge_decomposition_splits_stress_and_mixed_risk_axes() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 20,
            rebalance_freq_days: 60,
            max_gross_exposure: 1.0,
            max_position_pct: Decimal::new(15, 2),
            max_pairwise_correlation: Some(0.75),
            skip_top_pct: 0.10,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };

        let stress_lift_strict_mixed =
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v4("000300.SH");
        let base_stress_relaxed_mixed =
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v5("000300.SH");
        let stress_lift_position_only =
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v6("000300.SH");
        let stress_lift_correlation_only =
            MarketRegimePolicy::quality_frontier_regime_bridge_router_v7("000300.SH");

        let v4_bear = stress_lift_strict_mixed.apply(&base, MarketRegime::Bear);
        let v4_mixed = stress_lift_strict_mixed.apply(&base, MarketRegime::Mixed);
        let v5_bear = base_stress_relaxed_mixed.apply(&base, MarketRegime::Bear);
        let v5_mixed = base_stress_relaxed_mixed.apply(&base, MarketRegime::Mixed);
        let v6_mixed = stress_lift_position_only.apply(&base, MarketRegime::Mixed);
        let v7_mixed = stress_lift_correlation_only.apply(&base, MarketRegime::Mixed);

        assert_eq!(v4_bear.max_gross_exposure, 0.76);
        assert_eq!(v4_mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(v4_mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(v5_bear.max_gross_exposure, 0.74);
        assert_eq!(v5_mixed.max_position_pct, Decimal::new(14, 2));
        assert_eq!(v5_mixed.max_pairwise_correlation, Some(0.72));
        assert_eq!(v6_mixed.max_position_pct, Decimal::new(14, 2));
        assert_eq!(v6_mixed.max_pairwise_correlation, Some(0.70));
        assert_eq!(v7_mixed.max_position_pct, Decimal::new(13, 2));
        assert_eq!(v7_mixed.max_pairwise_correlation, Some(0.72));
        assert!(v4_mixed.portfolio_sleeve.is_some());
        assert!(v5_mixed.portfolio_sleeve.is_some());
    }

    #[test]
    fn rebalance_smoothing_keeps_small_changes_and_partially_moves_large_ones() {
        let previous = HashMap::from([
            ("AAA".to_string(), Decimal::new(10, 2)),
            ("BBB".to_string(), Decimal::new(10, 2)),
        ]);
        let mut next = HashMap::from([
            ("AAA".to_string(), Decimal::new(14, 2)),
            ("BBB".to_string(), Decimal::new(6, 2)),
        ]);

        apply_rebalance_path_smoothing(&mut next, Some(&previous), 0.01, 0.50);

        assert_eq!(next.get("AAA"), Some(&Decimal::new(12, 2)));
        assert_eq!(next.get("BBB"), Some(&Decimal::new(8, 2)));
    }

    #[test]
    fn rebalance_smoothing_leaves_small_deltas_unchanged() {
        let previous = HashMap::from([
            ("AAA".to_string(), Decimal::new(10, 2)),
            ("BBB".to_string(), Decimal::new(10, 2)),
        ]);
        let mut next = HashMap::from([
            ("AAA".to_string(), Decimal::new(105, 3)),
            ("BBB".to_string(), Decimal::new(95, 3)),
        ]);

        apply_rebalance_path_smoothing(&mut next, Some(&previous), 0.01, 0.50);

        assert_eq!(next.get("AAA"), Some(&Decimal::new(10, 2)));
        assert_eq!(next.get("BBB"), Some(&Decimal::new(10, 2)));
    }

    #[test]
    fn impact_risk_budget_limits_aggregate_rebalance_turnover() {
        let previous = HashMap::from([
            ("OLD_A".to_string(), Decimal::new(50, 2)),
            ("OLD_B".to_string(), Decimal::new(50, 2)),
        ]);
        let mut next = HashMap::from([
            ("NEW_A".to_string(), Decimal::new(50, 2)),
            ("NEW_B".to_string(), Decimal::new(50, 2)),
        ]);

        apply_execution_impact_budget(
            &mut next,
            Some(&previous),
            ExecutionImpactBudgetProfile::Turnover20PctV1,
        );

        let mut symbols = previous
            .keys()
            .chain(next.keys())
            .cloned()
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        let turnover = symbols.iter().fold(Decimal::ZERO, |acc, symbol| {
            let prev = previous.get(symbol).copied().unwrap_or_default();
            let target = next.get(symbol).copied().unwrap_or_default();
            acc + (target - prev).abs()
        });

        assert!(turnover <= Decimal::new(20, 2));
        assert!(next.get("OLD_A").copied().unwrap_or_default() > Decimal::new(40, 2));
        assert!(next.get("OLD_B").copied().unwrap_or_default() > Decimal::new(40, 2));
        assert!(next.get("NEW_A").copied().unwrap_or_default() <= Decimal::new(4, 2));
        assert!(next.get("NEW_B").copied().unwrap_or_default() <= Decimal::new(4, 2));
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

    #[test]
    fn regime_aware_factor_signals_can_switch_score_source_by_active_combo() {
        let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let trading_days = vec![d1, d2, d3];
        let quality_scores = HashMap::from([
            (
                d1,
                vec![
                    ("QUALITY_WINNER".to_string(), 3.0),
                    ("RESIDUAL_WINNER".to_string(), 1.0),
                ],
            ),
            (
                d2,
                vec![
                    ("QUALITY_WINNER".to_string(), 3.0),
                    ("RESIDUAL_WINNER".to_string(), 1.0),
                ],
            ),
        ]);
        let residual_scores = HashMap::from([
            (
                d1,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("RESIDUAL_WINNER".to_string(), 3.0),
                ],
            ),
            (
                d2,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("RESIDUAL_WINNER".to_string(), 3.0),
                ],
            ),
        ]);
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            top_n: 1,
            rebalance_freq_days: 1,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            ..Default::default()
        };

        let signals = build_rebalance_factor_signals_with_score_selector(
            &trading_days,
            &base,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            |score_day, active_config| match active_config.combo_name.as_str() {
                "phase7_financial_quality_v1" => quality_scores.get(&score_day).cloned(),
                "phase7_industry_residual_quality_v1" => residual_scores.get(&score_day).cloned(),
                _ => None,
            },
            |day, base| {
                if day == d3 {
                    let mut residual = base.clone();
                    residual.combo_name = "phase7_industry_residual_quality_v1".to_string();
                    residual
                } else {
                    base.clone()
                }
            },
        )
        .expect("regime-aware alpha-routed signals");

        let quality_signal = signals.get(&d2).expect("quality signal");
        assert_eq!(
            quality_signal.target_weights.get("QUALITY_WINNER"),
            Some(&Decimal::ONE)
        );
        let residual_signal = signals.get(&d3).expect("residual signal");
        assert_eq!(
            residual_signal.target_weights.get("RESIDUAL_WINNER"),
            Some(&Decimal::ONE)
        );
        assert!(!residual_signal
            .target_weights
            .contains_key("QUALITY_WINNER"));
    }

    #[test]
    fn quality_regime_alpha_overlay_keeps_quality_anchor_and_adds_small_stress_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy = MarketRegimePolicy::quality_regime_alpha_overlay_value_10pct_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);

        assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
        assert!(bull.score_overlay.is_none());
        assert_eq!(bear.combo_name, "phase7_financial_quality_v1");
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        assert_eq!(bear.max_gross_exposure, 0.72);
        let overlay = bear.score_overlay.expect("stress overlay");
        assert_eq!(overlay.combo_name, "phase7_valuation_v1");
        assert_eq!(overlay.version, "1.0.0");
        assert_eq!(overlay.score_direction, ScoreDirection::Descending);
        assert!((overlay.weight - 0.10).abs() < 1e-9);
    }

    #[test]
    fn regime_score_selector_blends_overlay_scores_without_replacing_base_source() {
        let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let mut base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };
        base.score_overlay = Some(FactorScoreOverlayConfig {
            combo_name: "phase7_valuation_v1".to_string(),
            version: "1.0.0".to_string(),
            weight: 0.25,
            score_direction: ScoreDirection::Descending,
        });

        let mut score_sources = HashMap::new();
        let mut quality_scores = HashMap::new();
        quality_scores.insert(
            d1,
            vec![
                ("QUALITY_BEST".to_string(), 1.0),
                ("VALUATION_BEST".to_string(), 2.0),
            ],
        );
        let mut valuation_scores = HashMap::new();
        valuation_scores.insert(
            d1,
            vec![
                ("QUALITY_BEST".to_string(), 1.0),
                ("VALUATION_BEST".to_string(), 5.0),
            ],
        );
        score_sources.insert(FactorScoreSourceKey::from_config(&base), quality_scores);
        let overlay_config = score_source_config_for_overlay(
            &base,
            base.score_overlay.as_ref().expect("overlay config"),
        );
        score_sources.insert(
            FactorScoreSourceKey::from_config(&overlay_config),
            valuation_scores,
        );

        let rows = score_rows_for_active_config(&score_sources, d1, &base).expect("blended rows");
        let mut sorted = rows.clone();
        sort_factor_scores(&mut sorted, base.score_direction);

        assert_eq!(sorted[0].0, "QUALITY_BEST");
        assert_eq!(sorted.len(), 2);
        assert_ne!(rows[0].1, 1.0);
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_keeps_anchor_and_allocates_stress_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_value_15pct_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);

        assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
        assert!(bull.portfolio_sleeve.is_none());
        assert_eq!(bear.combo_name, "phase7_financial_quality_v1");
        assert_eq!(bear.score_direction, ScoreDirection::Ascending);
        let sleeve = bear.portfolio_sleeve.expect("stress portfolio sleeve");
        assert_eq!(sleeve.combo_name, "phase7_valuation_v1");
        assert_eq!(sleeve.version, "1.0.0");
        assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_allocate_low_risk_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1("000300.SH");

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);

        assert!(bull.portfolio_sleeve.is_none());
        let sleeve = bear.portfolio_sleeve.expect("low-risk portfolio sleeve");
        assert_eq!(sleeve.combo_name, "phase7_price_volume_expanded_v1");
        assert_eq!(sleeve.version, "1.0.0");
        assert_eq!(sleeve.score_direction, ScoreDirection::Ascending);
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_allocate_event_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1(
                "000300.SH",
            );

        let bull = policy.apply(&base, MarketRegime::Bull);
        let bear = policy.apply(&base, MarketRegime::Bear);
        let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

        assert!(bull.portfolio_sleeve.is_none());
        let bear_sleeve = bear.portfolio_sleeve.expect("event portfolio sleeve");
        assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(bear_sleeve.version, "1.0.0");
        assert_eq!(bear_sleeve.score_direction, ScoreDirection::Descending);
        assert!((bear_sleeve.weight - 0.10).abs() < 1e-9);
        assert_eq!(
            high_volatility
                .portfolio_sleeve
                .expect("high-vol event sleeve")
                .combo_name,
            "phase7_event_window_earnings_v1"
        );
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_allocate_fractional_event_window_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1(
                "000300.SH",
            );

        let bear = policy.apply(&base, MarketRegime::Bear);

        let sleeve = bear.portfolio_sleeve.expect("event window sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
        assert!((sleeve.weight - 0.125).abs() < 1e-9);
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_allocate_upper_bound_event_window_sleeve() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(
                "000300.SH",
            );

        let bear = policy.apply(&base, MarketRegime::Bear);

        let sleeve = bear.portfolio_sleeve.expect("event window sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
        assert!((sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn quality_all_regime_event_window_sleeve_allocates_event_flow_in_every_state() {
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            score_direction: ScoreDirection::Ascending,
            max_gross_exposure: 1.0,
            ..Default::default()
        };
        let policy =
            MarketRegimePolicy::quality_all_regime_event_window_sleeve_10pct_v1("000300.SH");

        for regime in [
            MarketRegime::Bull,
            MarketRegime::Bear,
            MarketRegime::HighVolatility,
            MarketRegime::Sideways,
            MarketRegime::Mixed,
        ] {
            let active = policy.apply(&base, regime);
            let sleeve = active.portfolio_sleeve.expect("all-regime event sleeve");
            assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
            assert_eq!(sleeve.version, "1.0.0");
            assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
            assert!((sleeve.weight - 0.10).abs() < 1e-9);
        }
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_route_event_window_by_regime() {
        let bear_only =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1(
                "000300.SH",
            );
        let highvol_only =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1(
                "000300.SH",
            );

        assert!(bear_only
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .is_some());
        assert!(bear_only
            .rules
            .get(&MarketRegime::HighVolatility)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .is_none());
        assert!(highvol_only
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .is_none());
        assert!(highvol_only
            .rules
            .get(&MarketRegime::HighVolatility)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .is_some());
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_select_event_window_decay_variant() {
        let short_decay =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1(
                "000300.SH",
            );
        let long_decay =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1(
                "000300.SH",
            );

        let short_sleeve = short_decay
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("short-decay event sleeve");
        let long_sleeve = long_decay
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("long-decay event sleeve");

        assert_eq!(
            short_sleeve.combo_name,
            "phase7_event_window_earnings_10d_v1"
        );
        assert_eq!(short_sleeve.score_direction, ScoreDirection::Descending);
        assert!((short_sleeve.weight - 0.15).abs() < 1e-9);
        assert_eq!(
            long_sleeve.combo_name,
            "phase7_event_window_earnings_40d_v1"
        );
        assert_eq!(long_sleeve.score_direction, ScoreDirection::Descending);
        assert!((long_sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn quality_regime_alpha_portfolio_sleeve_can_select_event_quality_segment() {
        let surprise =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1(
                "000300.SH",
            );
        let confirm =
            MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1(
                "000300.SH",
            );

        let surprise_sleeve = surprise
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("event-surprise sleeve");
        let confirm_sleeve = confirm
            .rules
            .get(&MarketRegime::Bear)
            .and_then(|rule| rule.portfolio_sleeve.as_ref())
            .expect("event-confirm sleeve");

        assert_eq!(surprise_sleeve.combo_name, "phase7_event_surprise_v1");
        assert_eq!(surprise_sleeve.score_direction, ScoreDirection::Descending);
        assert!((surprise_sleeve.weight - 0.15).abs() < 1e-9);
        assert_eq!(confirm_sleeve.combo_name, "phase7_event_earnings_v1");
        assert_eq!(confirm_sleeve.score_direction, ScoreDirection::Descending);
        assert!((confirm_sleeve.weight - 0.15).abs() < 1e-9);
    }

    #[test]
    fn regime_aware_factor_signals_can_allocate_portfolio_sleeve_weights() {
        let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let trading_days = vec![d1, d2, d3];
        let quality_scores = HashMap::from([
            (
                d1,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("SLEEVE_WINNER".to_string(), 3.0),
                ],
            ),
            (
                d2,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("SLEEVE_WINNER".to_string(), 3.0),
                ],
            ),
        ]);
        let sleeve_scores = HashMap::from([
            (
                d1,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("SLEEVE_WINNER".to_string(), 3.0),
                ],
            ),
            (
                d2,
                vec![
                    ("QUALITY_WINNER".to_string(), 1.0),
                    ("SLEEVE_WINNER".to_string(), 3.0),
                ],
            ),
        ]);
        let base = SignalConfig {
            combo_name: "phase7_financial_quality_v1".to_string(),
            version: "1.0.0".to_string(),
            top_n: 1,
            rebalance_freq_days: 1,
            max_position_pct: Decimal::ONE,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Ascending,
            ..Default::default()
        };

        let signals = build_rebalance_factor_signals_with_score_selector(
            &trading_days,
            &base,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            |score_day, active_config| match active_config.combo_name.as_str() {
                "phase7_financial_quality_v1" => quality_scores.get(&score_day).cloned(),
                "phase7_valuation_v1" => sleeve_scores.get(&score_day).cloned(),
                _ => None,
            },
            |day, base| {
                if day == d3 {
                    let mut stressed = base.clone();
                    stressed.portfolio_sleeve = Some(FactorPortfolioSleeveConfig {
                        combo_name: "phase7_valuation_v1".to_string(),
                        version: "1.0.0".to_string(),
                        weight: 0.25,
                        score_direction: ScoreDirection::Descending,
                    });
                    stressed
                } else {
                    base.clone()
                }
            },
        )
        .expect("portfolio-sleeve signals");

        let normal_signal = signals.get(&d2).expect("normal signal");
        assert_eq!(
            normal_signal.target_weights.get("QUALITY_WINNER"),
            Some(&Decimal::ONE)
        );
        let sleeve_signal = signals.get(&d3).expect("sleeve signal");
        assert_eq!(
            sleeve_signal.target_weights.get("QUALITY_WINNER"),
            Some(&Decimal::new(75, 2))
        );
        assert_eq!(
            sleeve_signal.target_weights.get("SLEEVE_WINNER"),
            Some(&Decimal::new(25, 2))
        );
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
