//! Factor-based signal generator
//!
//! Converts factor combination scores from `multi_factor_value` into daily
//! `StrategySignal` objects for the backtest engine.
//!
//! Strategy: rank all stocks by combo score each rebalance day, pick top-N,
//! assign equal weight.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    /// Optional max aggregate target weight for any single industry.
    pub industry_max_weight_pct: Option<f64>,
    /// In-memory style exposure budget applied after raw portfolio weights.
    pub style_risk_budget_profile: StyleRiskBudgetProfile,
    /// Candidate-pool risk filter applied before portfolio construction.
    pub candidate_risk_filter_profile: CandidateRiskFilterProfile,
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
type SymbolReturnHistory = HashMap<String, Vec<(NaiveDate, f64)>>;
type AverageAmounts = HashMap<String, f64>;
type IndustryMap = HashMap<String, String>;
type BenchmarkReturns = Vec<(NaiveDate, f64)>;

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
    pub return_history_misses: usize,
    pub average_amount_hits: usize,
    pub average_amount_misses: usize,
    pub industry_classification_hits: usize,
    pub industry_classification_misses: usize,
    pub benchmark_return_hits: usize,
    pub benchmark_return_misses: usize,
}

#[derive(Debug, Default)]
pub struct SignalDataCache {
    combo_scores: HashMap<SignalDataCacheKey, Arc<FactorScoresByDate>>,
    trading_days: HashMap<SignalDataCacheKey, Arc<Vec<NaiveDate>>>,
    return_history: HashMap<SignalDataCacheKey, Arc<SymbolReturnHistory>>,
    average_amounts: HashMap<SignalDataCacheKey, Arc<AverageAmounts>>,
    industry_classifications: HashMap<SignalDataCacheKey, Arc<IndustryMap>>,
    benchmark_returns: HashMap<SignalDataCacheKey, Arc<BenchmarkReturns>>,
    stats: SignalDataCacheStats,
}

impl SignalDataCache {
    pub fn stats(&self) -> SignalDataCacheStats {
        self.stats
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
            None => {
                self.stats.combo_score_misses += 1;
                None
            }
        }
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
                    self.stats.return_history_misses += 1;
                    missing_symbols.push(symbol);
                }
            }
        }
        (missing_symbols, result)
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
                    if let Some(amount) = value.as_ref().get(&symbol).copied() {
                        result.insert(symbol, amount);
                    }
                }
                None => {
                    self.stats.average_amount_misses += 1;
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
            industry_max_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
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
}

impl CandidateRiskFilterProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "low_volatility_v1" | "low-volatility-v1" => Ok(Self::LowVolatilityV1),
            "low_volatility_low_correlation_v1" | "low-volatility-low-correlation-v1" => {
                Ok(Self::LowVolatilityLowCorrelationV1)
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
            }),
            Self::LowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.70,
                max_average_abs_correlation: Some(0.55),
                correlation_reference_limit: 120,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateRiskFilterParams {
    max_volatility_quantile: f64,
    max_average_abs_correlation: Option<f64>,
    correlation_reference_limit: usize,
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
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        let sleeve = FactorPortfolioSleeveConfig {
            combo_name: sleeve_combo_name.to_string(),
            version: "1.0.0".to_string(),
            weight: sleeve_weight.clamp(0.0, 1.0),
            score_direction: sleeve_score_direction,
        };
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
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
    max_industry_weight_pct: Option<f64>,
    style_risk_budget_profile: StyleRiskBudgetProfile,
    candidate_risk_filter_profile: CandidateRiskFilterProfile,
    risk_contribution_control_profile: RiskContributionControlProfile,
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
            max_industry_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
            risk_contribution_control_profile: RiskContributionControlProfile::Off,
        }
    }
}

impl From<&SignalConfig> for PortfolioConstructionConfig {
    fn from(config: &SignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
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
            risk_contribution_control_profile: config.risk_contribution_control_profile,
        }
    }
}

impl From<&PredictionSignalConfig> for PortfolioConstructionConfig {
    fn from(config: &PredictionSignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
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
            risk_contribution_control_profile: config.risk_contribution_control_profile,
        }
    }
}

fn capped_portfolio_top_n(top_n: usize, method: PortfolioConstructionMethod) -> usize {
    match method {
        PortfolioConstructionMethod::RiskBudget => top_n.min(50),
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
    let score_cache = load_combo_scores_cached(pool, cache, config, start_date, end_date).await?;
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
            let prediction_scores = load_prediction_scores_by_date(
                pool,
                &blend.prediction_set_id,
                start_date,
                end_date,
            )
            .await?;
            blend_factor_prediction_scores(
                &mut adjusted_scores,
                &prediction_scores,
                blend,
                config.score_direction,
            );
        }
        if let Some(event_gate) = config.event_gate.as_ref() {
            let event_scores =
                load_event_gate_scores_cached(pool, cache, event_gate, start_date, end_date)
                    .await?;
            apply_event_gate_scores(&mut adjusted_scores, event_scores.as_ref(), event_gate);
        }
        &adjusted_scores
    } else {
        score_cache.as_ref()
    };

    let trading_days = load_open_trading_days_cached(pool, cache, start_date, end_date).await?;

    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let portfolio_config = PortfolioConstructionConfig::from(config);
    let return_history = load_symbol_return_history_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        portfolio_history_lookback_days(&portfolio_config),
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs_cached(
        pool,
        cache,
        &all_symbols,
        start_date,
        end_date,
        &portfolio_config,
    )
    .await?;
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

    let score_source_configs = regime_score_source_configs(config, policy);
    let mut score_sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
    for source_config in score_source_configs {
        let key = FactorScoreSourceKey::from_config(&source_config);
        if score_sources.contains_key(&key) {
            continue;
        }
        let scores =
            load_regime_base_scores_cached(pool, cache, &source_config, start_date, end_date)
                .await?;
        score_sources.insert(key, scores);
    }

    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| !gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_cached(pool, cache, event_gate, start_date, end_date).await?;
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
        &portfolio_config,
    )
    .await?;
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

async fn load_regime_base_scores_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<FactorScoresByDate, String> {
    let score_cache = load_combo_scores_cached(pool, cache, config, start_date, end_date).await?;
    let mut adjusted_scores = score_cache.as_ref().clone();

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
        let prediction_scores =
            load_prediction_scores_by_date(pool, &blend.prediction_set_id, start_date, end_date)
                .await?;
        blend_factor_prediction_scores(
            &mut adjusted_scores,
            &prediction_scores,
            blend,
            config.score_direction,
        );
    }
    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_cached(pool, cache, event_gate, start_date, end_date).await?;
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
) -> Result<HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>, String> {
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

fn blend_factor_prediction_scores(
    factor_scores: &mut FactorScoresByDate,
    prediction_scores: &HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
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

async fn load_event_gate_scores_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    event_gate: &EventGateConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
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
    load_combo_scores_cached(pool, cache, &gate_config, start_date, end_date).await
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

async fn load_combo_scores_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<FactorScoresByDate>, String> {
    let score_candidate_pool_size =
        normalize_score_candidate_pool_size(config.score_candidate_pool_size);
    let key = SignalDataCacheKey::combo_scores(
        &config.combo_name,
        &config.version,
        start_date,
        end_date,
        config.score_direction,
        score_candidate_pool_size,
        config.universe_profile,
    );
    if let Some(scores) = cache.cached_combo_scores(&key) {
        return Ok(scores);
    }

    let sql = combo_score_load_sql(
        config.score_direction,
        score_candidate_pool_size,
        config.universe_profile,
    );
    let mut query = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(&sql)
        .bind(&config.combo_name)
        .bind(&config.version)
        .bind(start_date)
        .bind(end_date);
    if let Some(limit) = score_candidate_pool_size {
        query = query.bind(limit as i64);
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Failed to load combo scores: {}", e))?;

    if rows.is_empty() {
        return Err("No combo scores found".into());
    }

    let mut scores_by_date: FactorScoresByDate = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }

    Ok(cache.insert_combo_scores(key, scores_by_date))
}

fn normalize_score_candidate_pool_size(value: Option<usize>) -> Option<usize> {
    value.filter(|size| *size > 0)
}

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

fn build_rebalance_factor_signals<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
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
        average_amounts,
        industry_by_symbol,
        |score_day, _active_config| scores_by_date.get(&score_day).cloned(),
        active_config_for_day,
    )
}

fn build_rebalance_factor_signals_with_score_selector<F, S>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
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
        if i % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }

        let score_day = match score_day_for_signal(trading_days, i, &active_config) {
            Some(day) => day,
            None => continue,
        };
        let mut target_weights = match build_portfolio_sleeve_target_weights(
            score_day,
            &active_config,
            return_history,
            average_amounts,
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
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let min_idx = 1 + config.entry_delay_days;
    let mut signals = HashMap::new();
    let mut previous_target_weights: Option<HashMap<String, Decimal>> = None;

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

        let mut target_weights = build_portfolio_weights(
            score_day,
            &candidates,
            return_history,
            average_amounts,
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
    config: &PortfolioConstructionConfig,
) -> Result<HashMap<String, f64>, String> {
    if config.portfolio_method == PortfolioConstructionMethod::RiskBudget
        || config.style_risk_budget_profile.uses_liquidity()
    {
        load_average_amounts(pool, symbols, start_date, end_date).await
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
    config: &PortfolioConstructionConfig,
) -> Result<Arc<AverageAmounts>, String> {
    if config.portfolio_method == PortfolioConstructionMethod::RiskBudget
        || config.style_risk_budget_profile.uses_liquidity()
    {
        load_average_amounts_cached(pool, cache, symbols, start_date, end_date).await
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
    let risk_filtered_candidates =
        filter_candidate_risk_pool(score_day, candidates, return_history, config);
    let selected = select_uncorrelated_candidates(
        score_day,
        &risk_filtered_candidates,
        return_history,
        config,
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
    };

    let mut weights = normalize_and_cap_weights(&selected, &raw_weights, config);
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

fn filter_candidate_risk_pool(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
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
    fn risk_budget_portfolio_config_caps_large_top_n_for_local_search() {
        let config = SignalConfig {
            top_n: 80,
            portfolio_method: PortfolioConstructionMethod::RiskBudget,
            ..Default::default()
        };
        let portfolio_config = PortfolioConstructionConfig::from(&config);

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
        assert_eq!(cache.stats().average_amount_misses, 1);

        cache.insert_average_amount_symbols(&missing, start, end, HashMap::new());
        let (missing_again, cached_again) =
            cache.cached_average_amount_symbols(&requested_symbols, start, end);

        assert!(missing_again.is_empty());
        assert!(!cached_again.contains_key("CCC"));
        assert_eq!(cache.stats().average_amount_hits, 5);
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
