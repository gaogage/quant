//! Signal data cache and statistics.
use super::*;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SignalDataCacheStats {
    pub combo_score_hits: usize,
    pub combo_score_misses: usize,
    pub trading_day_hits: usize,
    pub trading_day_misses: usize,
    pub return_history_hits: usize,
    pub return_history_covering_window_hits: usize,
    pub return_history_snapshot_hits: usize,
    pub return_history_misses: usize,
    pub persistent_return_history_hits: usize,
    pub persistent_return_history_misses: usize,
    pub persistent_return_history_writes: usize,
    pub average_amount_hits: usize,
    pub average_amount_symbol_hits: usize,
    pub average_amount_history_hits: usize,
    pub average_amount_history_covering_window_hits: usize,
    pub average_amount_history_snapshot_hits: usize,
    pub average_amount_misses: usize,
    pub average_amount_symbol_misses: usize,
    pub average_amount_history_misses: usize,
    pub persistent_average_amount_history_hits: usize,
    pub persistent_average_amount_history_misses: usize,
    pub persistent_average_amount_history_writes: usize,
    pub persistent_pit_average_amount_matrix_hits: usize,
    pub persistent_pit_average_amount_matrix_misses: usize,
    pub persistent_pit_average_amount_matrix_writes: usize,
    pub persistent_return_risk_feature_matrix_hits: usize,
    pub persistent_return_risk_feature_matrix_misses: usize,
    pub persistent_return_risk_feature_matrix_writes: usize,
    pub persistent_return_risk_stats_feature_matrix_hits: usize,
    pub persistent_return_risk_stats_feature_matrix_misses: usize,
    pub persistent_return_risk_stats_feature_matrix_writes: usize,
    pub persistent_return_risk_feature_matrix_rows_loaded: usize,
    pub persistent_return_risk_feature_matrix_return_values_loaded: usize,
    pub persistent_return_risk_feature_matrix_rows_written: usize,
    pub persistent_return_risk_feature_matrix_return_values_written: usize,
    pub persistent_return_risk_stats_feature_matrix_stats_rows_loaded: usize,
    pub persistent_return_risk_stats_feature_matrix_pair_rows_loaded: usize,
    pub persistent_return_risk_stats_feature_matrix_stats_rows_written: usize,
    pub persistent_return_risk_stats_feature_matrix_pair_rows_written: usize,
    pub prediction_score_hits: usize,
    pub prediction_score_misses: usize,
    pub industry_classification_hits: usize,
    pub industry_classification_misses: usize,
    pub benchmark_return_hits: usize,
    pub benchmark_return_misses: usize,
}

pub const DEFAULT_RETURN_RISK_STATS_PAYLOAD_PREFERENCE_RATIO: f64 = 0.80;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReturnRiskCacheEconomicsRecommendation {
    PreferStatsMatrix,
    PreferRawMatrix,
    Inconclusive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnRiskCacheEconomicsProfile {
    pub recommendation: ReturnRiskCacheEconomicsRecommendation,
    pub reason: String,
    pub raw_matrix_hits: usize,
    pub stats_matrix_hits: usize,
    pub stats_matrix_steady_state: bool,
    pub stats_matrix_raw_fallback_hits: usize,
    pub raw_matrix_rows_loaded: usize,
    pub raw_matrix_return_values_loaded: usize,
    pub stats_matrix_raw_fallback_return_values_loaded: usize,
    pub stats_matrix_stats_rows_loaded: usize,
    pub stats_matrix_pair_rows_loaded: usize,
    pub stats_matrix_payload_rows_loaded: usize,
    pub stats_matrix_adjusted_payload_rows_loaded: usize,
    pub stats_to_raw_return_value_ratio: Option<f64>,
    pub stats_adjusted_to_raw_return_value_ratio: Option<f64>,
    pub stats_pair_to_stats_row_ratio: Option<f64>,
}

pub fn compare_return_risk_cache_economics(
    raw_matrix_stats: SignalDataCacheStats,
    stats_matrix_stats: SignalDataCacheStats,
) -> ReturnRiskCacheEconomicsProfile {
    let raw_return_values =
        raw_matrix_stats.persistent_return_risk_feature_matrix_return_values_loaded;
    let stats_payload_rows = stats_matrix_stats
        .persistent_return_risk_stats_feature_matrix_stats_rows_loaded
        .saturating_add(
            stats_matrix_stats.persistent_return_risk_stats_feature_matrix_pair_rows_loaded,
        );
    let stats_raw_fallback_return_values =
        stats_matrix_stats.persistent_return_risk_feature_matrix_return_values_loaded;
    let stats_adjusted_payload_rows =
        stats_payload_rows.saturating_add(stats_raw_fallback_return_values);
    let stats_to_raw_return_value_ratio = ratio(stats_payload_rows, raw_return_values);
    let stats_adjusted_to_raw_return_value_ratio =
        ratio(stats_adjusted_payload_rows, raw_return_values);
    let stats_pair_to_stats_row_ratio = ratio(
        stats_matrix_stats.persistent_return_risk_stats_feature_matrix_pair_rows_loaded,
        stats_matrix_stats.persistent_return_risk_stats_feature_matrix_stats_rows_loaded,
    );
    let stats_matrix_steady_state =
        stats_matrix_stats.persistent_return_risk_stats_feature_matrix_hits > 0
            && stats_matrix_stats.persistent_return_risk_stats_feature_matrix_misses == 0
            && stats_matrix_stats.persistent_return_risk_stats_feature_matrix_writes == 0;

    let (recommendation, reason) = if raw_matrix_stats.persistent_return_risk_feature_matrix_hits
        == 0
        || raw_return_values == 0
    {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "raw_matrix_payload_missing",
        )
    } else if stats_matrix_stats.persistent_return_risk_stats_feature_matrix_hits == 0 {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "stats_matrix_hits_missing",
        )
    } else if !stats_matrix_steady_state {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "stats_matrix_warmup_not_steady_state",
        )
    } else if stats_payload_rows == 0 {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "stats_matrix_payload_missing",
        )
    } else if stats_matrix_stats.persistent_return_risk_feature_matrix_hits > 0
        || stats_raw_fallback_return_values > 0
    {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "stats_matrix_raw_fallback_present",
        )
    } else if stats_to_raw_return_value_ratio
        .map(|ratio| ratio <= DEFAULT_RETURN_RISK_STATS_PAYLOAD_PREFERENCE_RATIO)
        .unwrap_or(false)
    {
        (
            ReturnRiskCacheEconomicsRecommendation::PreferStatsMatrix,
            "stats_payload_below_raw_return_values",
        )
    } else if stats_to_raw_return_value_ratio
        .map(|ratio| ratio >= 1.0)
        .unwrap_or(false)
    {
        (
            ReturnRiskCacheEconomicsRecommendation::PreferRawMatrix,
            "stats_payload_not_smaller_than_raw_return_values",
        )
    } else {
        (
            ReturnRiskCacheEconomicsRecommendation::Inconclusive,
            "stats_payload_savings_too_small",
        )
    };

    ReturnRiskCacheEconomicsProfile {
        recommendation,
        reason: reason.to_string(),
        raw_matrix_hits: raw_matrix_stats.persistent_return_risk_feature_matrix_hits,
        stats_matrix_hits: stats_matrix_stats.persistent_return_risk_stats_feature_matrix_hits,
        stats_matrix_steady_state,
        stats_matrix_raw_fallback_hits: stats_matrix_stats
            .persistent_return_risk_feature_matrix_hits,
        raw_matrix_rows_loaded: raw_matrix_stats.persistent_return_risk_feature_matrix_rows_loaded,
        raw_matrix_return_values_loaded: raw_return_values,
        stats_matrix_raw_fallback_return_values_loaded: stats_raw_fallback_return_values,
        stats_matrix_stats_rows_loaded: stats_matrix_stats
            .persistent_return_risk_stats_feature_matrix_stats_rows_loaded,
        stats_matrix_pair_rows_loaded: stats_matrix_stats
            .persistent_return_risk_stats_feature_matrix_pair_rows_loaded,
        stats_matrix_payload_rows_loaded: stats_payload_rows,
        stats_matrix_adjusted_payload_rows_loaded: stats_adjusted_payload_rows,
        stats_to_raw_return_value_ratio,
        stats_adjusted_to_raw_return_value_ratio,
        stats_pair_to_stats_row_ratio,
    }
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}


#[derive(Debug, Default)]
pub struct SignalDataCache {
    pub(crate) combo_scores: HashMap<SignalDataCacheKey, Arc<FactorScoresByDate>>,
    pub(crate) trading_days: HashMap<SignalDataCacheKey, Arc<Vec<NaiveDate>>>,
    pub(crate) return_history: HashMap<SignalDataCacheKey, Arc<SymbolReturnHistory>>,
    pub(crate) average_amounts: HashMap<SignalDataCacheKey, Arc<AverageAmounts>>,
    pub(crate) average_amount_history: HashMap<SignalDataCacheKey, Arc<AverageAmountHistory>>,
    pub(crate) pit_average_amount_matrices: HashMap<PitAverageAmountMatrixCacheKey, Arc<AverageAmountsByDate>>,
    pub(crate) return_risk_feature_matrices:
        HashMap<ReturnRiskFeatureMatrixCacheKey, Arc<ScoreDateReturnRiskMatrix>>,
    pub(crate) market_feature_snapshots: HashMap<MarketFeatureSnapshotKey, Arc<MarketFeatureSnapshot>>,
    pub(crate) prediction_scores: HashMap<SignalDataCacheKey, Arc<PredictionScoresByDate>>,
    pub(crate) industry_classifications: HashMap<SignalDataCacheKey, Arc<IndustryMap>>,
    pub(crate) benchmark_returns: HashMap<SignalDataCacheKey, Arc<BenchmarkReturns>>,
    pub(crate) stats: SignalDataCacheStats,
}

#[derive(Debug, Clone, Default)]
pub struct SignalDataCacheSnapshot {
    pub(crate) combo_scores: HashMap<SignalDataCacheKey, Arc<FactorScoresByDate>>,
    pub(crate) trading_days: HashMap<SignalDataCacheKey, Arc<Vec<NaiveDate>>>,
    pub(crate) return_history: HashMap<SignalDataCacheKey, Arc<SymbolReturnHistory>>,
    pub(crate) average_amounts: HashMap<SignalDataCacheKey, Arc<AverageAmounts>>,
    pub(crate) average_amount_history: HashMap<SignalDataCacheKey, Arc<AverageAmountHistory>>,
    pub(crate) pit_average_amount_matrices: HashMap<PitAverageAmountMatrixCacheKey, Arc<AverageAmountsByDate>>,
    pub(crate) return_risk_feature_matrices:
        HashMap<ReturnRiskFeatureMatrixCacheKey, Arc<ScoreDateReturnRiskMatrix>>,
    pub(crate) market_feature_snapshots: HashMap<MarketFeatureSnapshotKey, Arc<MarketFeatureSnapshot>>,
    pub(crate) prediction_scores: HashMap<SignalDataCacheKey, Arc<PredictionScoresByDate>>,
    pub(crate) industry_classifications: HashMap<SignalDataCacheKey, Arc<IndustryMap>>,
    pub(crate) benchmark_returns: HashMap<SignalDataCacheKey, Arc<BenchmarkReturns>>,
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
            pit_average_amount_matrices: self.pit_average_amount_matrices.clone(),
            return_risk_feature_matrices: self.return_risk_feature_matrices.clone(),
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
            pit_average_amount_matrices: snapshot.pit_average_amount_matrices.clone(),
            return_risk_feature_matrices: snapshot.return_risk_feature_matrices.clone(),
            market_feature_snapshots: snapshot.market_feature_snapshots.clone(),
            prediction_scores: snapshot.prediction_scores.clone(),
            industry_classifications: snapshot.industry_classifications.clone(),
            benchmark_returns: snapshot.benchmark_returns.clone(),
            stats: SignalDataCacheStats::default(),
        }
    }

    pub(crate) fn cached_pit_average_amount_matrix(
        &self,
        key: &PitAverageAmountMatrixCacheKey,
    ) -> Option<Arc<AverageAmountsByDate>> {
        if let Some(matrix) = self.pit_average_amount_matrices.get(key).cloned() {
            return Some(matrix);
        }
        self.pit_average_amount_matrices
            .iter()
            .find_map(|(covering_key, matrix)| {
                if !pit_average_amount_matrix_key_covers(covering_key, key) {
                    return None;
                }
                Some(Arc::new(subset_pit_average_amount_matrix(
                    matrix.as_ref(),
                    &key.symbols,
                    &key.score_dates,
                )))
            })
    }

    pub(crate) fn insert_pit_average_amount_matrix(
        &mut self,
        key: PitAverageAmountMatrixCacheKey,
        matrix: AverageAmountsByDate,
    ) -> Arc<AverageAmountsByDate> {
        let matrix = Arc::new(matrix);
        self.pit_average_amount_matrices
            .insert(key, Arc::clone(&matrix));
        matrix
    }

    pub(crate) fn cached_return_risk_feature_matrix(
        &self,
        key: &ReturnRiskFeatureMatrixCacheKey,
    ) -> Option<Arc<ScoreDateReturnRiskMatrix>> {
        self.return_risk_feature_matrices
            .get(key)
            .cloned()
            .or_else(|| {
                self.return_risk_feature_matrices
                    .iter()
                    .find_map(|(covering_key, matrix)| {
                        if !return_risk_feature_matrix_key_covers(covering_key, key) {
                            return None;
                        }
                        Some(Arc::new(subset_return_risk_feature_matrix(
                            matrix.as_ref(),
                            &key.symbols,
                            &key.score_dates,
                        )))
                    })
            })
    }

    pub(crate) fn insert_return_risk_feature_matrix(
        &mut self,
        key: ReturnRiskFeatureMatrixCacheKey,
        matrix: ScoreDateReturnRiskMatrix,
    ) -> Arc<ScoreDateReturnRiskMatrix> {
        let matrix = Arc::new(matrix);
        self.return_risk_feature_matrices
            .insert(key, Arc::clone(&matrix));
        matrix
    }

    pub(crate) fn record_persistent_market_feature_hit(&mut self, kind: PersistentMarketFeatureKind) {
        match kind {
            PersistentMarketFeatureKind::ReturnHistory => {
                self.stats.persistent_return_history_hits += 1;
            }
            PersistentMarketFeatureKind::AverageAmountHistory => {
                self.stats.persistent_average_amount_history_hits += 1;
            }
            PersistentMarketFeatureKind::PitAverageAmountMatrix => {
                self.stats.persistent_pit_average_amount_matrix_hits += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskFeatureMatrix => {
                self.stats.persistent_return_risk_feature_matrix_hits += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix => {
                self.stats.persistent_return_risk_stats_feature_matrix_hits += 1;
            }
        }
    }

    pub(crate) fn record_persistent_market_feature_miss(&mut self, kind: PersistentMarketFeatureKind) {
        match kind {
            PersistentMarketFeatureKind::ReturnHistory => {
                self.stats.persistent_return_history_misses += 1;
            }
            PersistentMarketFeatureKind::AverageAmountHistory => {
                self.stats.persistent_average_amount_history_misses += 1;
            }
            PersistentMarketFeatureKind::PitAverageAmountMatrix => {
                self.stats.persistent_pit_average_amount_matrix_misses += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskFeatureMatrix => {
                self.stats.persistent_return_risk_feature_matrix_misses += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix => {
                self.stats
                    .persistent_return_risk_stats_feature_matrix_misses += 1;
            }
        }
    }

    pub(crate) fn record_persistent_market_feature_write(&mut self, kind: PersistentMarketFeatureKind) {
        match kind {
            PersistentMarketFeatureKind::ReturnHistory => {
                self.stats.persistent_return_history_writes += 1;
            }
            PersistentMarketFeatureKind::AverageAmountHistory => {
                self.stats.persistent_average_amount_history_writes += 1;
            }
            PersistentMarketFeatureKind::PitAverageAmountMatrix => {
                self.stats.persistent_pit_average_amount_matrix_writes += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskFeatureMatrix => {
                self.stats.persistent_return_risk_feature_matrix_writes += 1;
            }
            PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix => {
                self.stats
                    .persistent_return_risk_stats_feature_matrix_writes += 1;
            }
        }
    }

    pub(crate) fn record_persistent_return_risk_feature_matrix_payload_loaded(
        &mut self,
        rows: usize,
        return_values: usize,
    ) {
        self.stats.persistent_return_risk_feature_matrix_rows_loaded += rows;
        self.stats
            .persistent_return_risk_feature_matrix_return_values_loaded += return_values;
    }

    pub(crate) fn record_persistent_return_risk_feature_matrix_payload_written(
        &mut self,
        rows: usize,
        return_values: usize,
    ) {
        self.stats
            .persistent_return_risk_feature_matrix_rows_written += rows;
        self.stats
            .persistent_return_risk_feature_matrix_return_values_written += return_values;
    }

    #[allow(dead_code)]
    pub(crate) fn record_persistent_return_risk_stats_feature_matrix_payload_loaded(
        &mut self,
        stats_rows: usize,
        pair_rows: usize,
    ) {
        self.stats
            .persistent_return_risk_stats_feature_matrix_stats_rows_loaded += stats_rows;
        self.stats
            .persistent_return_risk_stats_feature_matrix_pair_rows_loaded += pair_rows;
    }

    #[allow(dead_code)]
    pub(crate) fn record_persistent_return_risk_stats_feature_matrix_payload_written(
        &mut self,
        stats_rows: usize,
        pair_rows: usize,
    ) {
        self.stats
            .persistent_return_risk_stats_feature_matrix_stats_rows_written += stats_rows;
        self.stats
            .persistent_return_risk_stats_feature_matrix_pair_rows_written += pair_rows;
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

    pub(crate) fn cached_combo_scores_from_larger_candidate_pool(
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

    pub(crate) fn insert_combo_scores(
        &mut self,
        key: SignalDataCacheKey,
        value: FactorScoresByDate,
    ) -> Arc<FactorScoresByDate> {
        let value = Arc::new(value);
        self.combo_scores.insert(key, Arc::clone(&value));
        value
    }

    pub(crate) fn cached_trading_days(&mut self, key: &SignalDataCacheKey) -> Option<Arc<Vec<NaiveDate>>> {
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

    pub(crate) fn insert_trading_days(
        &mut self,
        key: SignalDataCacheKey,
        value: Vec<NaiveDate>,
    ) -> Arc<Vec<NaiveDate>> {
        let value = Arc::new(value);
        self.trading_days.insert(key, Arc::clone(&value));
        value
    }

    pub(crate) fn insert_market_feature_snapshot(
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

    pub(crate) fn insert_market_feature_snapshot_from_cached_histories(
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

    pub(crate) fn cached_return_history_symbols(
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

    pub(crate) fn cached_return_history_from_covering_window(
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

    pub(crate) fn cached_return_history_from_market_feature_snapshot(
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

    pub(crate) fn insert_return_history_symbols(
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

    pub(crate) fn cached_average_amount_symbols(
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

    pub(crate) fn insert_average_amount_symbols(
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

    pub(crate) fn cached_average_amount_history_symbols(
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

    pub(crate) fn cached_average_amount_history_from_covering_window(
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

    pub(crate) fn cached_average_amount_history_from_market_feature_snapshot(
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

    pub(crate) fn insert_average_amount_history_symbols(
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

    pub(crate) fn cached_prediction_scores(
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

    pub(crate) fn insert_prediction_scores(
        &mut self,
        key: SignalDataCacheKey,
        value: PredictionScoresByDate,
    ) -> Arc<PredictionScoresByDate> {
        let value = Arc::new(value);
        self.prediction_scores.insert(key, Arc::clone(&value));
        value
    }

    pub(crate) fn cached_industry_classifications(
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

    pub(crate) fn insert_industry_classifications(
        &mut self,
        key: SignalDataCacheKey,
        value: IndustryMap,
    ) -> Arc<IndustryMap> {
        let value = Arc::new(value);
        self.industry_classifications
            .insert(key, Arc::clone(&value));
        value
    }

    pub(crate) fn cached_benchmark_returns(
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

    pub(crate) fn insert_benchmark_returns(
        &mut self,
        key: SignalDataCacheKey,
        value: BenchmarkReturns,
    ) -> Arc<BenchmarkReturns> {
        let value = Arc::new(value);
        self.benchmark_returns.insert(key, Arc::clone(&value));
        value
    }

    #[cfg(test)]
    pub(crate) fn store_combo_scores_for_test(&mut self, key: SignalDataCacheKey, value: FactorScoresByDate) {
        self.combo_scores.insert(key, Arc::new(value));
    }

    #[cfg(test)]
    pub(crate) fn store_prediction_scores_for_test(
        &mut self,
        key: SignalDataCacheKey,
        value: PredictionScoresByDate,
    ) {
        self.prediction_scores.insert(key, Arc::new(value));
    }
}
