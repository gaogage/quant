//! Cache key types for the signal data cache.
use super::*;


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
    pub(crate) fn combo_scores(
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

    pub(crate) fn trading_days(start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::TradingDays {
            start_date,
            end_date,
        }
    }

    pub(crate) fn return_history_symbol(
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

    pub(crate) fn average_amount_symbol(symbol: &str, start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self::AverageAmountSymbol {
            symbol: symbol.to_string(),
            start_date,
            end_date,
        }
    }

    pub(crate) fn average_amount_history_symbol(
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

    pub(crate) fn prediction_scores(
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

    pub(crate) fn industry_classifications(symbols: &[String]) -> Self {
        Self::IndustryClassifications {
            symbols: normalized_symbol_key(symbols),
        }
    }

    pub(crate) fn benchmark_returns(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PersistentMarketFeatureKind {
    ReturnHistory,
    AverageAmountHistory,
    PitAverageAmountMatrix,
    ReturnRiskFeatureMatrix,
    ReturnRiskStatsFeatureMatrix,
}

impl PersistentMarketFeatureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReturnHistory => "return_history",
            Self::AverageAmountHistory => "average_amount_history",
            Self::PitAverageAmountMatrix => "pit_average_amount_matrix",
            Self::ReturnRiskFeatureMatrix => "return_risk_feature_matrix",
            Self::ReturnRiskStatsFeatureMatrix => "return_risk_stats_feature_matrix",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistentMarketFeatureCacheKey {
    pub feature_kind: PersistentMarketFeatureKind,
    pub data_version_id: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub lookback_days: usize,
    pub universe_hash: String,
    pub symbol_count: usize,
    pub cache_key: String,
}

impl PersistentMarketFeatureCacheKey {
    pub fn new(
        feature_kind: PersistentMarketFeatureKind,
        data_version_id: impl AsRef<str>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        symbols: &[String],
    ) -> Self {
        let data_version_id = data_version_id.as_ref().trim().to_string();
        let normalized_symbols = normalized_symbol_key(symbols);
        let universe_hash = persistent_market_feature_universe_hash(&normalized_symbols);
        let symbol_count = normalized_symbols.len();
        let lookback_days = lookback_days.max(1);
        let cache_key = [
            "market_feature",
            feature_kind.as_str(),
            data_version_id.as_str(),
            &start_date.to_string(),
            &end_date.to_string(),
            &lookback_days.to_string(),
            universe_hash.as_str(),
            &symbol_count.to_string(),
        ]
        .join(":");

        Self {
            feature_kind,
            data_version_id,
            start_date,
            end_date,
            lookback_days,
            universe_hash,
            symbol_count,
            cache_key,
        }
    }

    pub fn new_for_dates(
        feature_kind: PersistentMarketFeatureKind,
        data_version_id: impl AsRef<str>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        symbols: &[String],
        as_of_dates: &[NaiveDate],
    ) -> Self {
        let mut key = Self::new(
            feature_kind,
            data_version_id,
            start_date,
            end_date,
            lookback_days,
            symbols,
        );
        let dates = normalized_dates(as_of_dates);
        let date_hash = persistent_market_feature_date_hash(&dates);
        key.cache_key = format!("{}:dates:{}:{}", key.cache_key, date_hash, dates.len());
        key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PitAverageAmountMatrixCacheKey {
    pub(crate) start_date: NaiveDate,
    end_date: NaiveDate,
    pub(crate) lookback_days: usize,
    pub(crate) symbols: Vec<String>,
    pub(crate) universe_hash: String,
    pub(crate) symbol_count: usize,
    pub(crate) score_dates: Vec<NaiveDate>,
    pub(crate) score_date_hash: String,
    pub(crate) score_date_count: usize,
}

impl PitAverageAmountMatrixCacheKey {
    pub(crate) fn new(
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        symbols: &[String],
        score_dates: &[NaiveDate],
    ) -> Self {
        let symbols = normalized_symbol_key(symbols);
        let score_dates = normalized_dates(score_dates);
        Self {
            start_date,
            end_date,
            lookback_days: lookback_days.max(1),
            symbols: symbols.clone(),
            universe_hash: persistent_market_feature_universe_hash(&symbols),
            symbol_count: symbols.len(),
            score_dates: score_dates.clone(),
            score_date_hash: persistent_market_feature_date_hash(&score_dates),
            score_date_count: score_dates.len(),
        }
    }
}

fn sorted_strings_cover(covering: &[String], requested: &[String]) -> bool {
    requested
        .iter()
        .all(|symbol| covering.binary_search(symbol).is_ok())
}

fn sorted_dates_cover(covering: &[NaiveDate], requested: &[NaiveDate]) -> bool {
    requested
        .iter()
        .all(|date| covering.binary_search(date).is_ok())
}

pub(crate) fn pit_average_amount_matrix_key_covers(
    covering: &PitAverageAmountMatrixCacheKey,
    requested: &PitAverageAmountMatrixCacheKey,
) -> bool {
    covering.start_date == requested.start_date
        && covering.end_date == requested.end_date
        && covering.lookback_days == requested.lookback_days
        && covering.symbol_count >= requested.symbol_count
        && covering.score_date_count >= requested.score_date_count
        && sorted_strings_cover(&covering.symbols, &requested.symbols)
        && sorted_dates_cover(&covering.score_dates, &requested.score_dates)
}

pub(crate) fn subset_pit_average_amount_matrix(
    matrix: &AverageAmountsByDate,
    symbols: &[String],
    score_dates: &[NaiveDate],
) -> AverageAmountsByDate {
    score_dates
        .iter()
        .map(|date| {
            let row = matrix
                .get(date)
                .map(|amounts| {
                    symbols
                        .iter()
                        .filter_map(|symbol| {
                            amounts
                                .get(symbol)
                                .copied()
                                .map(|amount| (symbol.clone(), amount))
                        })
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default();
            (*date, row)
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ReturnRiskFeatureMatrixCacheKey {
    pub(crate) start_date: NaiveDate,
    end_date: NaiveDate,
    pub(crate) lookback_days: usize,
    pub(crate) symbols: Vec<String>,
    pub(crate) universe_hash: String,
    pub(crate) symbol_count: usize,
    pub(crate) score_dates: Vec<NaiveDate>,
    pub(crate) score_date_hash: String,
    pub(crate) score_date_count: usize,
}

impl ReturnRiskFeatureMatrixCacheKey {
    pub(crate) fn new(
        start_date: NaiveDate,
        end_date: NaiveDate,
        lookback_days: usize,
        symbols: &[String],
        score_dates: &[NaiveDate],
    ) -> Self {
        let symbols = normalized_symbol_key(symbols);
        let score_dates = normalized_dates(score_dates);
        Self {
            start_date,
            end_date,
            lookback_days: lookback_days.max(1),
            symbols: symbols.clone(),
            universe_hash: persistent_market_feature_universe_hash(&symbols),
            symbol_count: symbols.len(),
            score_dates: score_dates.clone(),
            score_date_hash: persistent_market_feature_date_hash(&score_dates),
            score_date_count: score_dates.len(),
        }
    }
}

pub(crate) fn return_risk_feature_matrix_key_covers(
    covering: &ReturnRiskFeatureMatrixCacheKey,
    requested: &ReturnRiskFeatureMatrixCacheKey,
) -> bool {
    covering.start_date == requested.start_date
        && covering.end_date == requested.end_date
        && covering.lookback_days == requested.lookback_days
        && covering.symbol_count >= requested.symbol_count
        && covering.score_date_count >= requested.score_date_count
        && sorted_strings_cover(&covering.symbols, &requested.symbols)
        && sorted_dates_cover(&covering.score_dates, &requested.score_dates)
}

pub(crate) fn subset_return_risk_feature_matrix(
    matrix: &ScoreDateReturnRiskMatrix,
    symbols: &[String],
    score_dates: &[NaiveDate],
) -> ScoreDateReturnRiskMatrix {
    let mut returns_by_score_symbol = HashMap::new();
    for score_day in score_dates {
        for symbol in symbols {
            let returns = matrix.returns(*score_day, symbol);
            if !returns.is_empty() {
                returns_by_score_symbol.insert((*score_day, symbol.clone()), returns.to_vec());
            }
        }
    }
    ScoreDateReturnRiskMatrix {
        returns_by_score_symbol,
    }
}

