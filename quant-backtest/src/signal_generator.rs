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
    /// Optional per-trade-date score preselection size before in-memory portfolio filtering.
    /// None or 0 keeps the legacy full-universe score load.
    pub score_candidate_pool_size: Option<usize>,
    /// Tradable universe pruning profile applied while loading combo scores.
    pub universe_profile: TradableUniverseProfile,
    /// Optional persisted prediction set to blend with factor combo scores.
    pub prediction_blend: Option<PredictionBlendConfig>,
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
    ReturnHistory {
        symbols: Vec<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    },
    AverageAmounts {
        symbols: Vec<String>,
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

    fn return_history(
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
    ) -> Self {
        Self::ReturnHistory {
            symbols: normalized_symbol_key(symbols),
            start_date,
            end_date,
            lookback_days,
        }
    }

    fn average_amounts(symbols: &[String], start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::AverageAmounts {
            symbols: normalized_symbol_key(symbols),
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

    fn cached_return_history(
        &mut self,
        key: &SignalDataCacheKey,
    ) -> Option<Arc<SymbolReturnHistory>> {
        match self.return_history.get(key) {
            Some(value) => {
                self.stats.return_history_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.return_history_misses += 1;
                None
            }
        }
    }

    fn insert_return_history(
        &mut self,
        key: SignalDataCacheKey,
        value: SymbolReturnHistory,
    ) -> Arc<SymbolReturnHistory> {
        let value = Arc::new(value);
        self.return_history.insert(key, Arc::clone(&value));
        value
    }

    fn cached_average_amounts(&mut self, key: &SignalDataCacheKey) -> Option<Arc<AverageAmounts>> {
        match self.average_amounts.get(key) {
            Some(value) => {
                self.stats.average_amount_hits += 1;
                Some(Arc::clone(value))
            }
            None => {
                self.stats.average_amount_misses += 1;
                None
            }
        }
    }

    fn insert_average_amounts(
        &mut self,
        key: SignalDataCacheKey,
        value: AverageAmounts,
    ) -> Arc<AverageAmounts> {
        let value = Arc::new(value);
        self.average_amounts.insert(key, Arc::clone(&value));
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
            score_candidate_pool_size: None,
            universe_profile: TradableUniverseProfile::All,
            prediction_blend: None,
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
    let scores_by_date: &FactorScoresByDate =
        if config.min_daily_amount_cny.is_some() || config.prediction_blend.is_some() {
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
    let score_cache = load_combo_scores_cached(pool, cache, config, start_date, end_date).await?;
    let mut adjusted_scores;
    let scores_by_date: &FactorScoresByDate =
        if config.min_daily_amount_cny.is_some() || config.prediction_blend.is_some() {
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
    let max_lookback = portfolio_history_lookback_days(&portfolio_config).max(policy.lookback_days);
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
    let benchmark_returns = load_benchmark_return_history_cached(
        pool,
        cache,
        &policy.benchmark,
        start_date,
        end_date,
        max_lookback,
    )
    .await?;

    build_rebalance_factor_signals(
        trading_days.as_ref(),
        scores_by_date,
        config,
        return_history.as_ref(),
        average_amounts.as_ref(),
        industry_by_symbol.as_ref(),
        |day, base| {
            let returns =
                trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
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
            industry_by_symbol,
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
            industry_by_symbol,
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

async fn load_symbol_return_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<SymbolReturnHistory>, String> {
    let key = SignalDataCacheKey::return_history(symbols, start_date, end_date, lookback_days);
    if let Some(history) = cache.cached_return_history(&key) {
        return Ok(history);
    }
    let history =
        load_symbol_return_history(pool, symbols, start_date, end_date, lookback_days).await?;
    Ok(cache.insert_return_history(key, history))
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
    let key = SignalDataCacheKey::average_amounts(symbols, start_date, end_date);
    if let Some(amounts) = cache.cached_average_amounts(&key) {
        return Ok(amounts);
    }
    let amounts = load_average_amounts(pool, symbols, start_date, end_date).await?;
    Ok(cache.insert_average_amounts(key, amounts))
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
    if config.portfolio_method != PortfolioConstructionMethod::RiskBudget {
        Ok(HashMap::new())
    } else {
        load_average_amounts(pool, symbols, start_date, end_date).await
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
    if config.portfolio_method != PortfolioConstructionMethod::RiskBudget {
        Ok(Arc::new(HashMap::new()))
    } else {
        load_average_amounts_cached(pool, cache, symbols, start_date, end_date).await
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

    let mut weights = normalize_and_cap_weights(&selected, &raw_weights, config);
    apply_industry_cap(&mut weights, industry_by_symbol, config);
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
    fn signal_data_cache_normalizes_symbol_order_for_portfolio_inputs() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let left_symbols = vec!["BBB".to_string(), "AAA".to_string()];
        let right_symbols = vec!["AAA".to_string(), "BBB".to_string()];

        let left_key = SignalDataCacheKey::return_history(&left_symbols, start, end, 60);
        let right_key = SignalDataCacheKey::return_history(&right_symbols, start, end, 60);

        assert_eq!(left_key, right_key);
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

    fn dated_returns(values: &[f64]) -> Vec<(NaiveDate, f64)> {
        let start = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        values
            .iter()
            .enumerate()
            .map(|(idx, value)| (start + chrono::Duration::days(idx as i64), *value))
            .collect()
    }
}
