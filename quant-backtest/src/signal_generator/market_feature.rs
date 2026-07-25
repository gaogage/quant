//! Market feature cache prewarm/load/store.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeaturePrewarmReport {
    pub snapshot_key: MarketFeatureSnapshotKey,
    pub feature_start: NaiveDate,
    pub feature_end: NaiveDate,
    pub symbol_count: usize,
    pub lookback_days: usize,
    pub return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
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
    pub return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
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
pub(crate) struct FactorSignalFeaturePrewarmCandidate {
    pub(crate) data_version_id: String,
    pub(crate) train_start: NaiveDate,
    pub(crate) train_end: NaiveDate,
    pub(crate) test_start: NaiveDate,
    pub(crate) test_end: NaiveDate,
    pub(crate) feature_start: NaiveDate,
    pub(crate) feature_end: NaiveDate,
    pub(crate) lookback_days: usize,
    pub(crate) return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
    pub(crate) symbols: Vec<String>,
    pub(crate) score_days: Vec<NaiveDate>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FactorSignalFeaturePrewarmGroupKey {
    pub(crate) data_version_id: String,
    pub(crate) train_start: NaiveDate,
    pub(crate) train_end: NaiveDate,
    pub(crate) test_start: NaiveDate,
    pub(crate) test_end: NaiveDate,
    pub(crate) feature_start: NaiveDate,
    pub(crate) feature_end: NaiveDate,
    pub(crate) lookback_days: usize,
    pub(crate) return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FactorSignalFeaturePrewarmGroup {
    pub(crate) key: FactorSignalFeaturePrewarmGroupKey,
    pub(crate) symbols: Vec<String>,
    pub(crate) score_days: Vec<NaiveDate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnRiskFeatureCacheMode {
    RawMatrix,
    StatsMatrixExperimental,
}

impl Default for ReturnRiskFeatureCacheMode {
    fn default() -> Self {
        Self::RawMatrix
    }
}

#[derive(Debug, Clone)]
pub struct MarketFeatureSnapshotScope {
    pub data_version_id: String,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
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
            return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::RawMatrix,
        }
    }

    pub fn with_return_risk_feature_cache_mode(mut self, mode: ReturnRiskFeatureCacheMode) -> Self {
        self.return_risk_feature_cache_mode = mode;
        self
    }

    pub fn with_return_risk_stats_cache_experiment(self) -> Self {
        self.with_return_risk_feature_cache_mode(
            ReturnRiskFeatureCacheMode::StatsMatrixExperimental,
        )
    }

    pub fn prefer_return_risk_stats_cache(&self) -> bool {
        matches!(
            self.return_risk_feature_cache_mode,
            ReturnRiskFeatureCacheMode::StatsMatrixExperimental
        )
    }

    pub(crate) fn snapshot_key(&self, lookback_days: usize, symbols: &[String]) -> MarketFeatureSnapshotKey {
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
pub(crate) struct MarketFeatureSnapshot {
    pub(crate) key: MarketFeatureSnapshotKey,
    pub(crate) return_lookback_days: usize,
    pub(crate) amount_lookback_days: usize,
    pub(crate) return_history: Arc<SymbolReturnHistory>,
    pub(crate) average_amount_history: Arc<AverageAmountHistory>,
}

impl MarketFeatureSnapshot {
    pub(crate) fn return_feature_start(&self) -> NaiveDate {
        return_history_query_start(self.key.train_start, self.return_lookback_days)
    }

    pub(crate) fn amount_feature_start(&self) -> NaiveDate {
        average_amount_history_query_start(self.key.train_start, self.amount_lookback_days)
    }

    pub(crate) fn feature_end(&self) -> NaiveDate {
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

pub fn persistent_market_feature_universe_hash(symbols: &[String]) -> String {
    let symbols = normalized_symbol_key(symbols);
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for symbol in symbols {
        for byte in symbol.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

pub(crate) fn persistent_market_feature_date_hash(dates: &[NaiveDate]) -> String {
    let dates = normalized_dates(dates);
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for date in dates {
        for byte in date.to_string().as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

pub(crate) fn pit_average_amount_matrix_to_symbol_history(
    matrix: &AverageAmountsByDate,
) -> AverageAmountHistory {
    let mut history: AverageAmountHistory = HashMap::new();
    let dates = normalized_dates(&matrix.keys().copied().collect::<Vec<_>>());
    for date in dates {
        let Some(amounts) = matrix.get(&date) else {
            continue;
        };
        let mut symbols = amounts.keys().cloned().collect::<Vec<_>>();
        symbols.sort();
        for symbol in symbols {
            let Some(amount) = amounts.get(&symbol).copied() else {
                continue;
            };
            if amount.is_finite() && amount > 0.0 {
                history.entry(symbol).or_default().push((date, amount));
            }
        }
    }
    history
}

pub(crate) fn average_amount_symbol_history_to_matrix(
    history: &AverageAmountHistory,
    as_of_dates: &[NaiveDate],
) -> AverageAmountsByDate {
    let dates = normalized_dates(as_of_dates);
    let mut matrix: AverageAmountsByDate =
        dates.iter().map(|date| (*date, HashMap::new())).collect();
    let date_set = dates.into_iter().collect::<HashSet<_>>();
    for (symbol, rows) in history {
        for (date, amount) in rows {
            if date_set.contains(date) && amount.is_finite() && *amount > 0.0 {
                matrix
                    .entry(*date)
                    .or_default()
                    .insert(symbol.clone(), *amount);
            }
        }
    }
    matrix
}

pub(crate) fn persistent_market_feature_manifest_is_usable(
    status: &str,
    symbol_count: i32,
    cached_symbols: &[String],
    requested_symbols: &[String],
) -> bool {
    if status != "ready" {
        return false;
    }
    let requested_symbols = normalized_symbol_key(requested_symbols);
    if symbol_count != requested_symbols.len() as i32 {
        return false;
    }
    normalized_symbol_key(cached_symbols) == requested_symbols
}

pub(crate) fn persistent_market_feature_grouped_rows_to_history(
    requested_symbols: &[String],
    row_count: i64,
    grouped_rows: Vec<(String, Vec<NaiveDate>, Vec<f64>)>,
) -> Option<SymbolReturnHistory> {
    if row_count < 0 {
        return None;
    }
    let requested_symbols = normalized_symbol_key(requested_symbols);
    let requested_set = requested_symbols.iter().cloned().collect::<HashSet<_>>();
    let mut history = requested_symbols
        .iter()
        .map(|symbol| (symbol.clone(), Vec::new()))
        .collect::<HashMap<_, _>>();
    let mut seen_symbols = HashSet::new();
    let mut actual_row_count = 0_i64;

    for (symbol, trade_dates, values) in grouped_rows {
        if !requested_set.contains(&symbol) || !seen_symbols.insert(symbol.clone()) {
            return None;
        }
        if trade_dates.len() != values.len() {
            return None;
        }

        let mut rows = Vec::with_capacity(trade_dates.len());
        for (trade_date, value) in trade_dates.into_iter().zip(values.into_iter()) {
            if !value.is_finite() {
                return None;
            }
            rows.push((trade_date, value));
            actual_row_count += 1;
        }
        history.insert(symbol, rows);
    }

    if actual_row_count != row_count {
        return None;
    }
    Some(history)
}

pub(crate) fn is_missing_persistent_market_feature_cache_table(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .as_deref()
        == Some("42P01")
}

pub(crate) async fn load_persistent_market_feature_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
) -> Result<Option<HashMap<String, Vec<(NaiveDate, f64)>>>, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    if requested_symbols.is_empty() {
        return Ok(Some(HashMap::new()));
    }

    let manifest: Option<(String, i32, i64)> = match sqlx::query_as(
        "SELECT status, symbol_count, row_count
         FROM market_feature_cache_manifest
         WHERE cache_key = $1
           AND feature_kind = $2
           AND data_version_id = $3
           AND start_date = $4
           AND end_date = $5
           AND lookback_days = $6
           AND universe_hash = $7
           AND symbol_count = $8",
    )
    .bind(&key.cache_key)
    .bind(key.feature_kind.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .fetch_optional(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent market feature cache manifest {}: {}",
                key.cache_key, error
            ));
        }
    };

    let Some((status, symbol_count, row_count)) = manifest else {
        return Ok(None);
    };

    let cached_symbols: Vec<(String,)> =
        sqlx::query_as("SELECT symbol FROM market_feature_cache_symbol WHERE cache_key = $1")
            .bind(&key.cache_key)
            .fetch_all(pool)
            .await
            .map_err(|error| {
                format!(
                    "Failed to load persistent market feature cache symbols {}: {}",
                    key.cache_key, error
                )
            })?;
    let cached_symbols = cached_symbols
        .into_iter()
        .map(|(symbol,)| symbol)
        .collect::<Vec<_>>();
    if !persistent_market_feature_manifest_is_usable(
        &status,
        symbol_count,
        &cached_symbols,
        &requested_symbols,
    ) {
        return Ok(None);
    }

    let grouped_rows: Vec<(String, Vec<NaiveDate>, Vec<f64>)> = sqlx::query_as(
        "SELECT symbol,
                ARRAY_AGG(trade_date ORDER BY trade_date)::DATE[] AS trade_dates,
                ARRAY_AGG(value ORDER BY trade_date)::DOUBLE PRECISION[] AS values
         FROM market_feature_cache_value
         WHERE cache_key = $1
         GROUP BY symbol
         ORDER BY symbol",
    )
    .bind(&key.cache_key)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        format!(
            "Failed to load persistent market feature cache values {}: {}",
            key.cache_key, error
        )
    })?;

    Ok(persistent_market_feature_grouped_rows_to_history(
        &requested_symbols,
        row_count,
        grouped_rows,
    ))
}

pub(crate) async fn store_persistent_market_feature_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
    history: &HashMap<String, Vec<(NaiveDate, f64)>>,
) -> Result<bool, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    if requested_symbols.is_empty() {
        return Ok(false);
    }

    let manifest_result = sqlx::query(
        "INSERT INTO market_feature_cache_manifest (
             cache_key, feature_kind, data_version_id, start_date, end_date,
             lookback_days, universe_hash, symbol_count, row_count, status, metadata
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'building',
                 jsonb_build_object('writer', 'quant-backtest'))
         ON CONFLICT (cache_key) DO UPDATE SET
             feature_kind = EXCLUDED.feature_kind,
             data_version_id = EXCLUDED.data_version_id,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             lookback_days = EXCLUDED.lookback_days,
             universe_hash = EXCLUDED.universe_hash,
             symbol_count = EXCLUDED.symbol_count,
             row_count = 0,
             status = 'building',
             metadata = EXCLUDED.metadata,
             updated_at = now()",
    )
    .bind(&key.cache_key)
    .bind(key.feature_kind.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .execute(pool)
    .await;

    match manifest_result {
        Ok(_) => {}
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => {
            return Ok(false);
        }
        Err(error) => {
            return Err(format!(
                "Failed to upsert persistent market feature cache manifest {}: {}",
                key.cache_key, error
            ));
        }
    }

    sqlx::query("DELETE FROM market_feature_cache_value WHERE cache_key = $1")
        .bind(&key.cache_key)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to clear persistent market feature cache values {}: {}",
                key.cache_key, error
            )
        })?;
    sqlx::query("DELETE FROM market_feature_cache_symbol WHERE cache_key = $1")
        .bind(&key.cache_key)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to clear persistent market feature cache symbols {}: {}",
                key.cache_key, error
            )
        })?;

    for chunk in requested_symbols.chunks(5_000) {
        let chunk_symbols = chunk.to_vec();
        sqlx::query(
            "INSERT INTO market_feature_cache_symbol (cache_key, symbol)
             SELECT $1, symbol
             FROM UNNEST($2::TEXT[]) AS t(symbol)
             ON CONFLICT (cache_key, symbol) DO NOTHING",
        )
        .bind(&key.cache_key)
        .bind(&chunk_symbols)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to store persistent market feature cache symbols {}: {}",
                key.cache_key, error
            )
        })?;
    }

    let mut row_count = 0_i64;
    let mut value_symbols = Vec::with_capacity(5_000);
    let mut value_dates = Vec::with_capacity(5_000);
    let mut values = Vec::with_capacity(5_000);
    for symbol in &requested_symbols {
        for (trade_date, value) in history.get(symbol).into_iter().flatten() {
            if !value.is_finite() {
                continue;
            }
            value_symbols.push(symbol.clone());
            value_dates.push(*trade_date);
            values.push(*value);
            if value_symbols.len() >= 5_000 {
                row_count += insert_persistent_market_feature_value_chunk(
                    pool,
                    &key.cache_key,
                    &value_symbols,
                    &value_dates,
                    &values,
                )
                .await?;
                value_symbols.clear();
                value_dates.clear();
                values.clear();
            }
        }
    }
    if !value_symbols.is_empty() {
        row_count += insert_persistent_market_feature_value_chunk(
            pool,
            &key.cache_key,
            &value_symbols,
            &value_dates,
            &values,
        )
        .await?;
    }

    sqlx::query(
        "UPDATE market_feature_cache_manifest
         SET row_count = $2,
             status = 'ready',
             updated_at = now()
         WHERE cache_key = $1",
    )
    .bind(&key.cache_key)
    .bind(row_count)
    .execute(pool)
    .await
    .map_err(|error| {
        format!(
            "Failed to mark persistent market feature cache ready {}: {}",
            key.cache_key, error
        )
    })?;

    Ok(true)
}

async fn insert_persistent_market_feature_value_chunk(
    pool: &PgPool,
    cache_key: &str,
    symbols: &[String],
    trade_dates: &[NaiveDate],
    values: &[f64],
) -> Result<i64, String> {
    let result = sqlx::query(
        "INSERT INTO market_feature_cache_value (cache_key, symbol, trade_date, value)
         SELECT $1, symbol, trade_date, value
         FROM UNNEST($2::TEXT[], $3::DATE[], $4::DOUBLE PRECISION[])
             AS t(symbol, trade_date, value)
         ON CONFLICT (cache_key, symbol, trade_date) DO UPDATE SET
             value = EXCLUDED.value",
    )
    .bind(cache_key)
    .bind(symbols)
    .bind(trade_dates)
    .bind(values)
    .execute(pool)
    .await
    .map_err(|error| {
        format!(
            "Failed to store persistent market feature cache values {}: {}",
            cache_key, error
        )
    })?;
    Ok(result.rows_affected() as i64)
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
        persistent_return_history_hits: after
            .persistent_return_history_hits
            .saturating_sub(before.persistent_return_history_hits),
        persistent_return_history_misses: after
            .persistent_return_history_misses
            .saturating_sub(before.persistent_return_history_misses),
        persistent_return_history_writes: after
            .persistent_return_history_writes
            .saturating_sub(before.persistent_return_history_writes),
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
        persistent_average_amount_history_hits: after
            .persistent_average_amount_history_hits
            .saturating_sub(before.persistent_average_amount_history_hits),
        persistent_average_amount_history_misses: after
            .persistent_average_amount_history_misses
            .saturating_sub(before.persistent_average_amount_history_misses),
        persistent_average_amount_history_writes: after
            .persistent_average_amount_history_writes
            .saturating_sub(before.persistent_average_amount_history_writes),
        persistent_pit_average_amount_matrix_hits: after
            .persistent_pit_average_amount_matrix_hits
            .saturating_sub(before.persistent_pit_average_amount_matrix_hits),
        persistent_pit_average_amount_matrix_misses: after
            .persistent_pit_average_amount_matrix_misses
            .saturating_sub(before.persistent_pit_average_amount_matrix_misses),
        persistent_pit_average_amount_matrix_writes: after
            .persistent_pit_average_amount_matrix_writes
            .saturating_sub(before.persistent_pit_average_amount_matrix_writes),
        persistent_return_risk_feature_matrix_hits: after
            .persistent_return_risk_feature_matrix_hits
            .saturating_sub(before.persistent_return_risk_feature_matrix_hits),
        persistent_return_risk_feature_matrix_misses: after
            .persistent_return_risk_feature_matrix_misses
            .saturating_sub(before.persistent_return_risk_feature_matrix_misses),
        persistent_return_risk_feature_matrix_writes: after
            .persistent_return_risk_feature_matrix_writes
            .saturating_sub(before.persistent_return_risk_feature_matrix_writes),
        persistent_return_risk_stats_feature_matrix_hits: after
            .persistent_return_risk_stats_feature_matrix_hits
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_hits),
        persistent_return_risk_stats_feature_matrix_misses: after
            .persistent_return_risk_stats_feature_matrix_misses
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_misses),
        persistent_return_risk_stats_feature_matrix_writes: after
            .persistent_return_risk_stats_feature_matrix_writes
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_writes),
        persistent_return_risk_feature_matrix_rows_loaded: after
            .persistent_return_risk_feature_matrix_rows_loaded
            .saturating_sub(before.persistent_return_risk_feature_matrix_rows_loaded),
        persistent_return_risk_feature_matrix_return_values_loaded: after
            .persistent_return_risk_feature_matrix_return_values_loaded
            .saturating_sub(before.persistent_return_risk_feature_matrix_return_values_loaded),
        persistent_return_risk_feature_matrix_rows_written: after
            .persistent_return_risk_feature_matrix_rows_written
            .saturating_sub(before.persistent_return_risk_feature_matrix_rows_written),
        persistent_return_risk_feature_matrix_return_values_written: after
            .persistent_return_risk_feature_matrix_return_values_written
            .saturating_sub(before.persistent_return_risk_feature_matrix_return_values_written),
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: after
            .persistent_return_risk_stats_feature_matrix_stats_rows_loaded
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_stats_rows_loaded),
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: after
            .persistent_return_risk_stats_feature_matrix_pair_rows_loaded
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_pair_rows_loaded),
        persistent_return_risk_stats_feature_matrix_stats_rows_written: after
            .persistent_return_risk_stats_feature_matrix_stats_rows_written
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_stats_rows_written),
        persistent_return_risk_stats_feature_matrix_pair_rows_written: after
            .persistent_return_risk_stats_feature_matrix_pair_rows_written
            .saturating_sub(before.persistent_return_risk_stats_feature_matrix_pair_rows_written),
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
    score_days: &[NaiveDate],
    return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
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
    if should_prewarm_raw_return_risk_matrix(return_risk_feature_cache_mode, !score_days.is_empty())
    {
        let _ = load_return_risk_feature_matrix_persistent_cached(
            pool,
            cache,
            Some(data_version_id),
            &symbols,
            feature_start,
            feature_end,
            score_days,
            lookback_days,
            None,
        )
        .await?;
    }
    let should_load_return_history = !should_prewarm_raw_return_risk_matrix(
        return_risk_feature_cache_mode,
        !score_days.is_empty(),
    );
    if should_load_return_history {
        let _ = load_symbol_return_history_persistent_cached(
            pool,
            cache,
            data_version_id,
            &symbols,
            feature_start,
            feature_end,
            lookback_days,
        )
        .await?;
    }
    if score_days.is_empty() {
        let _ = load_average_amount_history_persistent_cached(
            pool,
            cache,
            data_version_id,
            &symbols,
            feature_start,
            feature_end,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await?;
    } else {
        let _ = load_pit_average_amount_matrix_persistent_cached(
            pool,
            cache,
            Some(data_version_id),
            &symbols,
            feature_start,
            feature_end,
            score_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await?;
    };
    let _ = load_average_amounts_cached(pool, cache, &symbols, feature_start, feature_end).await?;
    cache.insert_market_feature_snapshot_from_cached_histories(
        snapshot_key.clone(),
        lookback_days,
        PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        &symbols,
        feature_start,
        feature_end,
    );
    let after = cache.stats();

    Ok(MarketFeaturePrewarmReport {
        snapshot_key,
        feature_start,
        feature_end,
        symbol_count: symbols.len(),
        lookback_days,
        return_risk_feature_cache_mode,
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
                &group.score_days,
                key.return_risk_feature_cache_mode,
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
    let (symbols, lookback_days, score_days) = if let Some(policy) = spec.regime_policy.as_ref() {
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
        (symbols, max_lookback, score_days)
    } else {
        let score_days = rebalance_score_days(trading_days.as_ref(), &spec.config, |_day, base| {
            base.clone()
        });
        let scores =
            load_regime_base_scores_for_dates_cached(pool, cache, &spec.config, &score_days)
                .await?;
        let lookback_days = portfolio_history_lookback_days(&portfolio_config);
        (
            symbols_from_factor_scores(&scores),
            lookback_days,
            score_days,
        )
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
        return_risk_feature_cache_mode: spec.return_risk_feature_cache_mode,
        symbols,
        score_days,
    })
}

fn symbols_from_factor_scores(scores: &FactorScoresByDate) -> Vec<String> {
    let symbols = scores
        .values()
        .flat_map(|rows| rows.iter().map(|(symbol, _)| symbol.clone()))
        .collect::<Vec<_>>();
    normalized_symbol_key(&symbols)
}

pub(crate) fn merge_factor_signal_feature_prewarm_groups(
    candidates: Vec<FactorSignalFeaturePrewarmCandidate>,
) -> Vec<FactorSignalFeaturePrewarmGroup> {
    let mut groups: BTreeMap<
        FactorSignalFeaturePrewarmGroupKey,
        (BTreeSet<String>, BTreeSet<NaiveDate>),
    > = BTreeMap::new();
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
            return_risk_feature_cache_mode: candidate.return_risk_feature_cache_mode,
        };
        let group = groups.entry(key).or_default();
        group.0.extend(candidate.symbols.into_iter());
        group.1.extend(candidate.score_days.into_iter());
    }

    groups
        .into_iter()
        .map(
            |(key, (symbols, score_days))| FactorSignalFeaturePrewarmGroup {
                key,
                symbols: symbols.into_iter().collect(),
                score_days: score_days.into_iter().collect(),
            },
        )
        .collect()
}
