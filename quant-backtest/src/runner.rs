//! 回测运行器 — 数据加载、引擎驱动、结果持久化

use chrono::NaiveDate;
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use super::engine::{
    BacktestConfig, BacktestEngine, BacktestMode, BacktestOutput, BacktestPersistenceMode,
    MarketDay, StrategySignal,
};

pub fn schedule_signals_for_execution(
    trading_days: &[NaiveDate],
    signals: &HashMap<NaiveDate, StrategySignal>,
) -> HashMap<NaiveDate, StrategySignal> {
    let mut scheduled = HashMap::new();
    for pair in trading_days.windows(2) {
        let signal_day = pair[0];
        let execution_day = pair[1];
        if let Some(signal) = signals.get(&signal_day) {
            scheduled.insert(execution_day, signal.clone());
        }
    }
    scheduled
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTaskInsert {
    pub task_id: String,
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub prediction_set_id: Option<String>,
    pub benchmark_symbol: String,
    pub symbols: Vec<String>,
    pub rebalance_frequency: String,
    pub mode: String,
}

#[derive(Debug, Clone, Default)]
struct TradingProfile {
    exchange: Option<String>,
    market: Option<String>,
    is_st: bool,
    /// 品种类型(market_stock.instrument_type):'etf'/'stock'/None(未回填)。
    /// 用于 ETF 涨跌幅规则(跨境 QDII ±20%、货基无限制)与股票区分。
    instrument_type: Option<String>,
}

type DailyBarsByDate = HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>>;

/// 基准行情缓存：键 (symbol, start, end)，值为日期→(close, pre_close)。
type BenchmarkCache =
    HashMap<(String, NaiveDate, NaiveDate), Arc<HashMap<NaiveDate, (Decimal, Decimal)>>>;

/// 日线记录缓存：键 (dv_key, start, end)，值为 symbol→DailyBarRecord 列表。
type DailyBarsCache =
    HashMap<(String, NaiveDate, NaiveDate), HashMap<String, Arc<Vec<DailyBarRecord>>>>;

/// market_stock 交易特征原始行：(symbol, exchange, market, is_st, instrument_type)。
type TradingProfileRow = (
    String,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<String>,
);

/// 日线行情原始行：(trade_date, symbol, open, close, pre_close, amount, data_version_id)。
type DailyBarRow = (
    NaiveDate,
    String,
    Option<Decimal>,
    Decimal,
    Option<Decimal>,
    Option<Decimal>,
    String,
);

/// 价格映射：键 (trade_date, symbol)。
type PriceMapByDateSymbol = HashMap<(NaiveDate, String), Decimal>;

#[derive(Debug, Clone)]
struct DailyBarRecord {
    trade_date: NaiveDate,
    symbol: String,
    open: Decimal,
    close: Decimal,
    pre_close: Decimal,
    amount: Decimal,
    /// 该 bar 所属数据版本（PIT 追溯印记，Step 5b 接入）。
    ///
    /// 行情表 (symbol, trade_date) 天然唯一（无跨 dv_id 重复），故此字段仅作
    /// 追溯印记，不参与 WHERE 过滤——避免单一 dv_id 过滤丢失历史批次数据。
    ///
    /// 当前为骨架：从 SQL `data_version_id` 列填充，但下游 `DailyBarsByDate`
    /// 暂不消费（待 build_market_day 接入 dv_id 时启用）。缓存键已纳入 dv_id
    /// 维度（防跨版本污染），字段读取留待下游消费时打开。
    #[allow(dead_code)]
    data_version_id: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BacktestDataCacheStats {
    pub trading_day_hits: usize,
    pub trading_day_misses: usize,
    pub benchmark_data_hits: usize,
    pub benchmark_data_misses: usize,
    pub daily_bar_symbol_hits: usize,
    pub daily_bar_covering_window_hits: usize,
    pub daily_bar_snapshot_hits: usize,
    pub daily_bar_symbol_misses: usize,
    pub trading_profile_symbol_hits: usize,
    pub trading_profile_symbol_misses: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestMarketDataPrewarmReport {
    pub snapshot_key: BacktestMarketDataSnapshotKey,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub benchmark: String,
    pub symbol_count: usize,
    pub cache_delta: BacktestDataCacheStats,
}

pub fn backtest_cache_stats_delta(
    before: BacktestDataCacheStats,
    after: BacktestDataCacheStats,
) -> BacktestDataCacheStats {
    BacktestDataCacheStats {
        trading_day_hits: after
            .trading_day_hits
            .saturating_sub(before.trading_day_hits),
        trading_day_misses: after
            .trading_day_misses
            .saturating_sub(before.trading_day_misses),
        benchmark_data_hits: after
            .benchmark_data_hits
            .saturating_sub(before.benchmark_data_hits),
        benchmark_data_misses: after
            .benchmark_data_misses
            .saturating_sub(before.benchmark_data_misses),
        daily_bar_symbol_hits: after
            .daily_bar_symbol_hits
            .saturating_sub(before.daily_bar_symbol_hits),
        daily_bar_covering_window_hits: after
            .daily_bar_covering_window_hits
            .saturating_sub(before.daily_bar_covering_window_hits),
        daily_bar_snapshot_hits: after
            .daily_bar_snapshot_hits
            .saturating_sub(before.daily_bar_snapshot_hits),
        daily_bar_symbol_misses: after
            .daily_bar_symbol_misses
            .saturating_sub(before.daily_bar_symbol_misses),
        trading_profile_symbol_hits: after
            .trading_profile_symbol_hits
            .saturating_sub(before.trading_profile_symbol_hits),
        trading_profile_symbol_misses: after
            .trading_profile_symbol_misses
            .saturating_sub(before.trading_profile_symbol_misses),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct BacktestMarketDataSnapshotKey {
    pub data_version_id: String,
    pub benchmark: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub universe_hash: String,
}

#[derive(Debug, Clone)]
struct BacktestMarketDataSnapshot {
    key: BacktestMarketDataSnapshotKey,
    daily_bars: HashMap<String, Arc<Vec<DailyBarRecord>>>,
}

impl BacktestMarketDataSnapshotKey {
    pub fn new(
        data_version_id: impl AsRef<str>,
        benchmark: impl AsRef<str>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        symbols: &[String],
    ) -> Self {
        Self {
            data_version_id: data_version_id.as_ref().trim().to_string(),
            benchmark: benchmark.as_ref().trim().to_string(),
            start_date,
            end_date,
            universe_hash: symbol_universe_hash(symbols),
        }
    }
}

#[derive(Debug, Default)]
pub struct BacktestDataCache {
    trading_days: HashMap<(NaiveDate, NaiveDate), Arc<Vec<NaiveDate>>>,
    benchmark_data: BenchmarkCache,
    /// 行情缓存键：(data_version_id, start, end) —— Step 5b 纳入 dv_id 维度，
    /// 防止不同 dv_id 的回测共享缓存（snapshot_key 声称按 dv_id 区分，底层
    /// 缓存键须与之一致，否则跨版本命中污染数据）。
    daily_bars: DailyBarsCache,
    market_data_snapshots: HashMap<BacktestMarketDataSnapshotKey, Arc<BacktestMarketDataSnapshot>>,
    trading_profiles: HashMap<String, Arc<Option<TradingProfile>>>,
    stats: BacktestDataCacheStats,
}

#[derive(Debug, Clone, Default)]
pub struct BacktestDataCacheSnapshot {
    trading_days: HashMap<(NaiveDate, NaiveDate), Arc<Vec<NaiveDate>>>,
    benchmark_data: BenchmarkCache,
    daily_bars: DailyBarsCache,
    market_data_snapshots: HashMap<BacktestMarketDataSnapshotKey, Arc<BacktestMarketDataSnapshot>>,
    trading_profiles: HashMap<String, Arc<Option<TradingProfile>>>,
}

impl BacktestDataCache {
    pub fn stats(&self) -> BacktestDataCacheStats {
        self.stats
    }

    pub fn snapshot(&self) -> BacktestDataCacheSnapshot {
        BacktestDataCacheSnapshot {
            trading_days: self.trading_days.clone(),
            benchmark_data: self.benchmark_data.clone(),
            daily_bars: self.daily_bars.clone(),
            market_data_snapshots: self.market_data_snapshots.clone(),
            trading_profiles: self.trading_profiles.clone(),
        }
    }

    pub fn from_snapshot(snapshot: &BacktestDataCacheSnapshot) -> Self {
        Self {
            trading_days: snapshot.trading_days.clone(),
            benchmark_data: snapshot.benchmark_data.clone(),
            daily_bars: snapshot.daily_bars.clone(),
            market_data_snapshots: snapshot.market_data_snapshots.clone(),
            trading_profiles: snapshot.trading_profiles.clone(),
            stats: BacktestDataCacheStats::default(),
        }
    }

    fn cached_trading_days(&mut self, start: NaiveDate, end: NaiveDate) -> Option<Vec<NaiveDate>> {
        match self.trading_days.get(&(start, end)) {
            Some(days) => {
                self.stats.trading_day_hits += 1;
                Some(days.as_ref().clone())
            }
            None => {
                if let Some(days) = self.cached_trading_days_from_covering_window(start, end) {
                    self.stats.trading_day_hits += 1;
                    self.trading_days
                        .insert((start, end), Arc::new(days.clone()));
                    Some(days)
                } else {
                    self.stats.trading_day_misses += 1;
                    None
                }
            }
        }
    }

    fn cached_trading_days_from_covering_window(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<Vec<NaiveDate>> {
        self.trading_days
            .iter()
            .filter(|((cached_start, cached_end), _)| *cached_start <= start && *cached_end >= end)
            .min_by_key(|((cached_start, cached_end), _)| (*cached_end - *cached_start).num_days())
            .map(|(_, days)| {
                days.as_ref()
                    .iter()
                    .copied()
                    .filter(|day| *day >= start && *day <= end)
                    .collect()
            })
    }

    fn insert_trading_days(
        &mut self,
        start: NaiveDate,
        end: NaiveDate,
        days: Vec<NaiveDate>,
    ) -> Vec<NaiveDate> {
        self.trading_days
            .insert((start, end), Arc::new(days.clone()));
        days
    }

    fn cached_benchmark_data(
        &mut self,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<HashMap<NaiveDate, (Decimal, Decimal)>> {
        let key = (benchmark.to_string(), start, end);
        match self.benchmark_data.get(&key) {
            Some(data) => {
                self.stats.benchmark_data_hits += 1;
                Some(data.as_ref().clone())
            }
            None => {
                if let Some(data) =
                    self.cached_benchmark_data_from_covering_window(benchmark, start, end)
                {
                    self.stats.benchmark_data_hits += 1;
                    self.benchmark_data.insert(key, Arc::new(data.clone()));
                    Some(data)
                } else {
                    self.stats.benchmark_data_misses += 1;
                    None
                }
            }
        }
    }

    fn cached_benchmark_data_from_covering_window(
        &self,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<HashMap<NaiveDate, (Decimal, Decimal)>> {
        self.benchmark_data
            .iter()
            .filter(|((cached_benchmark, cached_start, cached_end), _)| {
                cached_benchmark == benchmark && *cached_start <= start && *cached_end >= end
            })
            .min_by_key(|((_, cached_start, cached_end), _)| {
                (*cached_end - *cached_start).num_days()
            })
            .map(|(_, data)| {
                data.as_ref()
                    .iter()
                    .filter(|(day, _)| **day >= start && **day <= end)
                    .map(|(day, values)| (*day, *values))
                    .collect()
            })
    }

    fn insert_benchmark_data(
        &mut self,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
        data: HashMap<NaiveDate, (Decimal, Decimal)>,
    ) -> HashMap<NaiveDate, (Decimal, Decimal)> {
        self.benchmark_data
            .insert((benchmark.to_string(), start, end), Arc::new(data.clone()));
        data
    }

    fn cached_daily_bar_symbols(
        &mut self,
        data_version_id: &str,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> (Vec<String>, DailyBarsByDate) {
        let symbols = normalized_symbol_key(symbols);
        let mut missing = Vec::new();
        let mut cached = HashMap::new();
        let mut reusable_records: Vec<(String, Vec<DailyBarRecord>)> = Vec::new();

        for symbol in symbols {
            if let Some(records) = self
                .daily_bars
                .get(&(data_version_id.to_string(), start, end))
                .and_then(|bucket| bucket.get(&symbol))
            {
                self.stats.daily_bar_symbol_hits += 1;
                merge_daily_bar_records(&mut cached, records.as_ref());
            } else if let Some(records) = self.cached_daily_bar_records_from_covering_window(
                data_version_id,
                &symbol,
                start,
                end,
            ) {
                self.stats.daily_bar_symbol_hits += 1;
                self.stats.daily_bar_covering_window_hits += 1;
                merge_daily_bar_records(&mut cached, &records);
                reusable_records.push((symbol, records));
            } else if let Some(records) = self.cached_daily_bar_records_from_market_data_snapshot(
                data_version_id,
                &symbol,
                start,
                end,
            ) {
                self.stats.daily_bar_symbol_hits += 1;
                self.stats.daily_bar_snapshot_hits += 1;
                merge_daily_bar_records(&mut cached, &records);
                reusable_records.push((symbol, records));
            } else {
                self.stats.daily_bar_symbol_misses += 1;
                missing.push(symbol);
            }
        }

        if !reusable_records.is_empty() {
            let bucket = self
                .daily_bars
                .entry((data_version_id.to_string(), start, end))
                .or_default();
            for (symbol, records) in reusable_records {
                bucket.insert(symbol, Arc::new(records));
            }
        }

        (missing, cached)
    }

    fn cached_daily_bar_records_from_covering_window(
        &self,
        data_version_id: &str,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<Vec<DailyBarRecord>> {
        self.daily_bars
            .iter()
            .filter(|((cached_dv_id, cached_start, cached_end), bucket)| {
                cached_dv_id == data_version_id
                    && *cached_start <= start
                    && *cached_end >= end
                    && bucket.contains_key(symbol)
            })
            .min_by_key(|((_, cached_start, cached_end), _)| {
                (*cached_end - *cached_start).num_days()
            })
            .and_then(|(_, bucket)| bucket.get(symbol))
            .map(|records| {
                records
                    .as_ref()
                    .iter()
                    .filter(|record| record.trade_date >= start && record.trade_date <= end)
                    .cloned()
                    .collect()
            })
    }

    fn cached_daily_bar_records_from_market_data_snapshot(
        &self,
        data_version_id: &str,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<Vec<DailyBarRecord>> {
        self.market_data_snapshots
            .values()
            .filter(|snapshot| {
                snapshot.key.data_version_id == data_version_id
                    && snapshot.key.start_date <= start
                    && snapshot.key.end_date >= end
                    && snapshot.daily_bars.contains_key(symbol)
            })
            .min_by_key(|snapshot| {
                snapshot
                    .key
                    .end_date
                    .signed_duration_since(snapshot.key.start_date)
                    .num_days()
            })
            .and_then(|snapshot| snapshot.daily_bars.get(symbol))
            .map(|records| {
                records
                    .as_ref()
                    .iter()
                    .filter(|record| record.trade_date >= start && record.trade_date <= end)
                    .cloned()
                    .collect()
            })
    }

    fn insert_daily_bar_rows(
        &mut self,
        data_version_id: &str,
        start: NaiveDate,
        end: NaiveDate,
        requested_symbols: &[String],
        rows: Vec<DailyBarRecord>,
    ) -> DailyBarsByDate {
        let requested_symbols = normalized_symbol_key(requested_symbols);
        let mut by_symbol: HashMap<String, Vec<DailyBarRecord>> = requested_symbols
            .iter()
            .map(|symbol| (symbol.clone(), Vec::new()))
            .collect();

        for row in rows {
            by_symbol.entry(row.symbol.clone()).or_default().push(row);
        }

        let bucket = self
            .daily_bars
            .entry((data_version_id.to_string(), start, end))
            .or_default();
        let mut inserted = HashMap::new();
        for symbol in requested_symbols {
            let mut records = by_symbol.remove(&symbol).unwrap_or_default();
            records.sort_by_key(|record| record.trade_date);
            merge_daily_bar_records(&mut inserted, &records);
            bucket.insert(symbol, Arc::new(records));
        }

        inserted
    }

    #[cfg(test)]
    fn insert_market_data_snapshot(
        &mut self,
        key: BacktestMarketDataSnapshotKey,
        daily_bars: DailyBarsByDate,
    ) {
        let data_version_id = key.data_version_id.clone();
        let mut by_symbol: HashMap<String, Vec<DailyBarRecord>> = HashMap::new();
        for (trade_date, day) in daily_bars {
            for (symbol, (open, close, pre_close, amount)) in day {
                by_symbol
                    .entry(symbol.clone())
                    .or_default()
                    .push(DailyBarRecord {
                        trade_date,
                        symbol,
                        open,
                        close,
                        pre_close,
                        amount,
                        data_version_id: data_version_id.clone(),
                    });
            }
        }
        let daily_bars = by_symbol
            .into_iter()
            .map(|(symbol, mut records)| {
                records.sort_by_key(|record| record.trade_date);
                (symbol, Arc::new(records))
            })
            .collect();
        self.market_data_snapshots.insert(
            key.clone(),
            Arc::new(BacktestMarketDataSnapshot { key, daily_bars }),
        );
    }

    fn insert_market_data_snapshot_from_cache(
        &mut self,
        key: BacktestMarketDataSnapshotKey,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) {
        let mut daily_bars = HashMap::new();
        if let Some(bucket) = self
            .daily_bars
            .get(&(key.data_version_id.clone(), start, end))
        {
            for symbol in normalized_symbol_key(symbols) {
                if let Some(records) = bucket.get(&symbol) {
                    daily_bars.insert(symbol, Arc::clone(records));
                }
            }
        }
        self.market_data_snapshots.insert(
            key.clone(),
            Arc::new(BacktestMarketDataSnapshot { key, daily_bars }),
        );
    }

    fn cached_trading_profiles(
        &mut self,
        symbols: &[String],
    ) -> (Vec<String>, HashMap<String, TradingProfile>) {
        let mut missing = Vec::new();
        let mut cached = HashMap::new();

        for symbol in normalized_symbol_key(symbols) {
            match self.trading_profiles.get(&symbol).map(Arc::as_ref) {
                Some(Some(profile)) => {
                    self.stats.trading_profile_symbol_hits += 1;
                    cached.insert(symbol, profile.clone());
                }
                Some(None) => {
                    self.stats.trading_profile_symbol_hits += 1;
                }
                None => {
                    self.stats.trading_profile_symbol_misses += 1;
                    missing.push(symbol);
                }
            }
        }

        (missing, cached)
    }

    fn insert_trading_profiles(
        &mut self,
        requested_symbols: &[String],
        rows: Vec<TradingProfileRow>,
    ) -> HashMap<String, TradingProfile> {
        let requested_symbols = normalized_symbol_key(requested_symbols);
        let mut by_symbol: HashMap<String, Option<TradingProfile>> = requested_symbols
            .iter()
            .map(|symbol| (symbol.clone(), None))
            .collect();

        for (symbol, exchange, market, is_st, instrument_type) in rows {
            by_symbol.insert(
                symbol,
                Some(TradingProfile {
                    exchange,
                    market,
                    is_st: is_st.unwrap_or(false),
                    instrument_type,
                }),
            );
        }

        let mut inserted = HashMap::new();
        for symbol in requested_symbols {
            let profile = by_symbol.remove(&symbol).unwrap_or(None);
            if let Some(profile) = profile.as_ref() {
                inserted.insert(symbol.clone(), profile.clone());
            }
            self.trading_profiles.insert(symbol, Arc::new(profile));
        }

        inserted
    }
}

fn normalized_symbol_key(symbols: &[String]) -> Vec<String> {
    let mut symbols = symbols.to_vec();
    symbols.sort();
    symbols.dedup();
    symbols
}

fn symbol_universe_hash(symbols: &[String]) -> String {
    let symbols = normalized_symbol_key(symbols);
    let mut hasher = DefaultHasher::new();
    symbols.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn merge_daily_bars(target: &mut DailyBarsByDate, source: DailyBarsByDate) {
    for (date, day) in source {
        target.entry(date).or_default().extend(day);
    }
}

fn merge_daily_bar_records(target: &mut DailyBarsByDate, records: &[DailyBarRecord]) {
    for record in records {
        target.entry(record.trade_date).or_default().insert(
            record.symbol.clone(),
            (record.open, record.close, record.pre_close, record.amount),
        );
    }
}

impl BacktestTaskInsert {
    pub fn from_config(task_id: &str, config: &BacktestConfig) -> Self {
        Self {
            task_id: task_id.to_string(),
            strategy_version_id: config.strategy_version_id.clone(),
            data_version_id: config.data_version_id.clone(),
            prediction_set_id: config.prediction_set_id.clone(),
            benchmark_symbol: config.benchmark.clone(),
            symbols: config.symbols.clone(),
            rebalance_frequency: config.rebalance_frequency.clone(),
            mode: match config.mode {
                BacktestMode::Fast => "fast".into(),
                BacktestMode::Standard => "standard".into(),
                BacktestMode::Audit => "audit".into(),
            },
        }
    }
}

pub fn backtest_task_parameters(config: &BacktestConfig) -> Value {
    let mut parameters = config.parameters.clone();
    if !parameters.is_object() {
        parameters = json!({});
    }
    if let Some(object) = parameters.as_object_mut() {
        object
            .entry("research_dataset_id")
            .or_insert_with(|| json!(config.research_dataset_id.as_deref()));
        object
            .entry("feature_set_version_id")
            .or_insert_with(|| json!(config.feature_set_version_id.as_deref()));
        object
            .entry("prediction_set_id")
            .or_insert_with(|| json!(config.prediction_set_id.as_deref()));
        object
            .entry("portfolio_policy_id")
            .or_insert_with(|| json!(config.portfolio_policy_id.as_deref()));
    }
    parameters
}

// ─── Runner ───────────────────────────────────────────────────────

pub struct BacktestRunner {
    pool: PgPool,
}

impl BacktestRunner {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 执行回测
    pub async fn run(
        &self,
        task_id: &str,
        config: BacktestConfig,
        signals: &HashMap<NaiveDate, StrategySignal>,
    ) -> Result<BacktestOutput, Box<dyn std::error::Error>> {
        self.run_with_cache(task_id, config, signals, None).await
    }

    pub async fn prewarm_market_data_cache(
        &self,
        cache: &mut BacktestDataCache,
        data_version_id: &str,
        benchmark: &str,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<BacktestMarketDataPrewarmReport, sqlx::Error> {
        let symbols = normalized_symbol_key(symbols);
        let snapshot_key =
            BacktestMarketDataSnapshotKey::new(data_version_id, benchmark, start, end, &symbols);
        let before = cache.stats();
        let _ = self.load_trading_days_cached(cache, start, end).await?;
        let _ = self
            .load_benchmark_data_cached(cache, benchmark, start, end)
            .await?;
        if !symbols.is_empty() {
            let _ = self
                .load_daily_bars_cached(cache, data_version_id, &symbols, start, end)
                .await?;
            cache.insert_market_data_snapshot_from_cache(
                snapshot_key.clone(),
                &symbols,
                start,
                end,
            );
            let _ = self.load_trading_profiles_cached(cache, &symbols).await?;
        }
        let after = cache.stats();

        Ok(BacktestMarketDataPrewarmReport {
            snapshot_key,
            start_date: start,
            end_date: end,
            benchmark: benchmark.to_string(),
            symbol_count: symbols.len(),
            cache_delta: backtest_cache_stats_delta(before, after),
        })
    }

    /// 执行回测，并在批量 trial 场景复用调用方持有的数据缓存。
    pub async fn run_with_cache(
        &self,
        task_id: &str,
        config: BacktestConfig,
        signals: &HashMap<NaiveDate, StrategySignal>,
        mut data_cache: Option<&mut BacktestDataCache>,
    ) -> Result<BacktestOutput, Box<dyn std::error::Error>> {
        info!(
            task_id,
            "回测开始: {} -> {}", config.start_date, config.end_date
        );

        // 1. 创建任务记录
        self.create_task(task_id, &config).await?;

        // 2. 加载交易日历
        let trading_days = match data_cache.as_mut() {
            Some(cache) => {
                self.load_trading_days_cached(cache, config.start_date, config.end_date)
                    .await?
            }
            None => {
                self.load_trading_days(config.start_date, config.end_date)
                    .await?
            }
        };
        info!(task_id, days = trading_days.len(), "交易日已加载");

        // 3. 加载基准数据
        let benchmark_data = match data_cache.as_mut() {
            Some(cache) => {
                self.load_benchmark_data_cached(
                    cache,
                    &config.benchmark,
                    config.start_date,
                    config.end_date,
                )
                .await?
            }
            None => {
                self.load_benchmark_data(&config.benchmark, config.start_date, config.end_date)
                    .await?
            }
        };
        info!(task_id, bm_points = benchmark_data.len(), "基准数据已加载");

        // 4. 加载所需股票日线
        let all_symbols: Vec<String> = signals
            .values()
            .flat_map(|s| s.target_weights.keys().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let (daily_data, trading_profiles) = if all_symbols.is_empty() {
            warn!(task_id, "无股票持仓信号，按现金曲线完成回测");
            (HashMap::new(), HashMap::new())
        } else {
            let daily_data = match data_cache.as_mut() {
                Some(cache) => {
                    self.load_daily_bars_cached(
                        cache,
                        &config.data_version_id,
                        &all_symbols,
                        config.start_date,
                        config.end_date,
                    )
                    .await?
                }
                None => {
                    self.load_daily_bars(
                        &config.data_version_id,
                        &all_symbols,
                        config.start_date,
                        config.end_date,
                    )
                    .await?
                }
            };
            let trading_profiles = match data_cache.as_mut() {
                Some(cache) => {
                    self.load_trading_profiles_cached(cache, &all_symbols)
                        .await?
                }
                None => self.load_trading_profiles(&all_symbols).await?,
            };
            (daily_data, trading_profiles)
        };
        info!(
            task_id,
            symbols = all_symbols.len(),
            dates = daily_data.len(),
            "日线已加载"
        );

        // DEBUG: check first day
        if let Some(first) = trading_days.first() {
            if let Some(day_data) = daily_data.get(first) {
                info!(task_id, date = %first, stocks_in_day = day_data.len(), "首日数据");
                for (sym, (o, c, pc, amount)) in day_data.iter().take(3) {
                    info!(task_id, symbol = %sym, open = %o, close = %c, pre_close = %pc, amount = %amount, "首日行情");
                }
            } else {
                warn!(task_id, date = %first, "首日无行情数据!");
            }
        }

        // 5. 运行回测引擎
        let mut engine = BacktestEngine::new(config.clone());
        let execution_signals = schedule_signals_for_execution(&trading_days, signals);

        for (idx, day) in trading_days.iter().enumerate() {
            let prev_day = idx
                .checked_sub(1)
                .and_then(|i| trading_days.get(i).copied());
            let market = self.build_market_day(
                *day,
                prev_day,
                &daily_data,
                &benchmark_data,
                &trading_profiles,
            );
            let signal = execution_signals.get(day);
            engine.process_day(&market, signal);
        }

        // 6. 计算指标 + 持久化
        let output = engine.finalize();
        self.persist_results(task_id, &output).await?;

        info!(task_id,
            total_return = %output.metrics.total_return_pct,
            sharpe = %output.metrics.sharpe_ratio,
            trades = output.trades.len(),
            "回测完成"
        );

        Ok(output)
    }

    // ─── Data loading ──────────────────────────────────────────

    async fn load_trading_days(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<NaiveDate>, sqlx::Error> {
        let rows: Vec<(NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE exchange = 'SSE' AND is_open = true
             AND trade_date >= $1 AND trade_date <= $2
             ORDER BY trade_date",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(d,)| d).collect())
    }

    async fn load_trading_days_cached(
        &self,
        cache: &mut BacktestDataCache,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<NaiveDate>, sqlx::Error> {
        if let Some(days) = cache.cached_trading_days(start, end) {
            return Ok(days);
        }

        let days = self.load_trading_days(start, end).await?;
        Ok(cache.insert_trading_days(start, end, days))
    }

    async fn load_benchmark_data(
        &self,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<HashMap<NaiveDate, (Decimal, Decimal)>, sqlx::Error> {
        let rows: Vec<(NaiveDate, Decimal, Option<Decimal>)> = sqlx::query_as(
            "SELECT trade_date, close, pre_close
             FROM market_index_daily_bar
             WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date",
        )
        .bind(benchmark)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(d, c, pc)| (d, (c, pc.unwrap_or(c))))
            .collect())
    }

    async fn load_benchmark_data_cached(
        &self,
        cache: &mut BacktestDataCache,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<HashMap<NaiveDate, (Decimal, Decimal)>, sqlx::Error> {
        if let Some(data) = cache.cached_benchmark_data(benchmark, start, end) {
            return Ok(data);
        }

        let data = self.load_benchmark_data(benchmark, start, end).await?;
        Ok(cache.insert_benchmark_data(benchmark, start, end, data))
    }

    async fn load_daily_bars(
        &self,
        _data_version_id: &str,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<DailyBarsByDate, sqlx::Error> {
        // Step 5b：SELECT 暴露 data_version_id 列（视图已补，见 migration
        // 20260725000002），承载 PIT 追溯印记。不加 WHERE dv_id 过滤——行情表
        // (symbol, trade_date) 天然唯一（无跨 dv_id 重复），单一 dv_id 过滤会
        // 丢失历史批次数据（dv_id 是"数据批次"非"数据版本"）。
        //
        // `_data_version_id` 当前仅用于与 cached 版签名对称（cached 版用它做
        // 缓存键隔离）。裸版返回 DailyBarsByDate 不含 dv_id，待下游（build_market_day）
        // 消费 dv_id 时再启用。
        let rows: Vec<DailyBarRow> = sqlx::query_as(
            "SELECT trade_date, symbol, open, close, pre_close, amount, data_version_id
             FROM market_stock_daily_bar_adj
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date, symbol",
        )
        .bind(symbols)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        let mut result: DailyBarsByDate = HashMap::new();
        for (d, sym, o, c, pc, amount, _dv_id) in rows {
            result.entry(d).or_default().insert(
                sym,
                (
                    o.unwrap_or(c),
                    c,
                    pc.unwrap_or(c),
                    amount.unwrap_or_default(),
                ),
            );
        }
        Ok(result)
    }

    async fn load_daily_bars_cached(
        &self,
        cache: &mut BacktestDataCache,
        data_version_id: &str,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<DailyBarsByDate, sqlx::Error> {
        let (missing_symbols, mut result) =
            cache.cached_daily_bar_symbols(data_version_id, symbols, start, end);
        if missing_symbols.is_empty() {
            return Ok(result);
        }

        let rows: Vec<DailyBarRow> = sqlx::query_as(
            "SELECT trade_date, symbol, open, close, pre_close, amount, data_version_id
             FROM market_stock_daily_bar_adj
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date, symbol",
        )
        .bind(&missing_symbols)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        let rows = rows
            .into_iter()
            .map(
                |(trade_date, symbol, open, close, pre_close, amount, row_dv_id)| DailyBarRecord {
                    trade_date,
                    symbol,
                    open: open.unwrap_or(close),
                    close,
                    pre_close: pre_close.unwrap_or(close),
                    amount: amount.unwrap_or_default(),
                    data_version_id: row_dv_id,
                },
            )
            .collect();
        let inserted =
            cache.insert_daily_bar_rows(data_version_id, start, end, &missing_symbols, rows);
        merge_daily_bars(&mut result, inserted);
        Ok(result)
    }

    async fn load_trading_profiles(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, TradingProfile>, sqlx::Error> {
        let rows: Vec<TradingProfileRow> = sqlx::query_as(
            "SELECT symbol, exchange, market, is_st, instrument_type FROM market_stock WHERE symbol = ANY($1)",
        )
        .bind(symbols)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(symbol, exchange, market, is_st, instrument_type)| {
                (
                    symbol,
                    TradingProfile {
                        exchange,
                        market,
                        is_st: is_st.unwrap_or(false),
                        instrument_type,
                    },
                )
            })
            .collect())
    }

    async fn load_trading_profiles_cached(
        &self,
        cache: &mut BacktestDataCache,
        symbols: &[String],
    ) -> Result<HashMap<String, TradingProfile>, sqlx::Error> {
        let (missing_symbols, mut result) = cache.cached_trading_profiles(symbols);
        if missing_symbols.is_empty() {
            return Ok(result);
        }

        let rows: Vec<TradingProfileRow> = sqlx::query_as(
            "SELECT symbol, exchange, market, is_st, instrument_type FROM market_stock WHERE symbol = ANY($1)",
        )
        .bind(&missing_symbols)
        .fetch_all(&self.pool)
        .await?;
        result.extend(cache.insert_trading_profiles(&missing_symbols, rows));
        Ok(result)
    }

    // ─── Market day builder ────────────────────────────────────

    fn build_market_day(
        &self,
        date: NaiveDate,
        prev_trading_day: Option<NaiveDate>,
        daily_data: &DailyBarsByDate,
        benchmark_data: &HashMap<NaiveDate, (Decimal, Decimal)>,
        trading_profiles: &HashMap<String, TradingProfile>,
    ) -> MarketDay {
        let day_data = daily_data.get(&date);
        let bm = benchmark_data
            .get(&date)
            .copied()
            .unwrap_or((Decimal::zero(), Decimal::zero()));

        let mut open = HashMap::new();
        let mut close = HashMap::new();
        let mut pre_close = HashMap::new();
        let mut amount = HashMap::new();
        let mut suspended = HashSet::new();
        let mut up_limit = HashMap::new();
        let mut down_limit = HashMap::new();

        if let Some(data) = day_data {
            for (sym, (o, c, pc, amt)) in data {
                open.insert(sym.clone(), *o);
                close.insert(sym.clone(), *c);
                pre_close.insert(sym.clone(), *pc);
                amount.insert(sym.clone(), *amt);
                if !pc.is_zero() {
                    let limit_rate = Self::limit_rate_for(sym, trading_profiles.get(sym));
                    up_limit.insert(sym.clone(), *pc * (Decimal::ONE + limit_rate));
                    down_limit.insert(sym.clone(), *pc * (Decimal::ONE - limit_rate));
                }
            }
        }

        // 标记停牌：上一交易日有数据但今天没有，避免自然日前一天穿过节假日。
        if let Some(prev_data) = prev_trading_day.and_then(|prev| daily_data.get(&prev)) {
            for sym in prev_data.keys() {
                if !close.contains_key(sym) {
                    suspended.insert(sym.clone());
                }
            }
        }

        MarketDay {
            date,
            open,
            close,
            pre_close,
            amount,
            suspended,
            up_limit,
            down_limit,
            benchmark_close: bm.0,
            benchmark_pre_close: bm.1,
        }
    }

    fn limit_rate_for(symbol: &str, profile: Option<&TradingProfile>) -> Decimal {
        if profile.is_some_and(|p| p.is_st) {
            return Decimal::new(5, 2);
        }

        // ETF 涨跌幅:优先用 instrument_type 字段(权威),回退 symbol 代码段。
        // 跨境 QDII(513xxx 沪/159xxx 深部分)±20%,货币基金(511880/511990)无限制,
        // 其余 ETF ±10%。简化:QDII 513xxx →20%,货基 511880/511990 →不限(用1.0),
        // 其他 ETF →10%。
        let is_etf = profile.is_some_and(|p| p.instrument_type.as_deref() == Some("etf"))
            || quant_common::trading_rules::is_etf_symbol(symbol);
        if is_etf {
            let code = symbol.split('.').next().unwrap_or("");
            if code.starts_with("513") {
                return Decimal::new(20, 2); // 跨境 QDII ±20%
            }
            if code == "511880" || code == "511990" {
                return Decimal::ONE; // 货币基金无涨跌幅限制
            }
            return Decimal::new(10, 2); // 普通 ETF ±10%
        }

        let market = profile
            .and_then(|p| p.market.as_deref())
            .unwrap_or_default();
        let exchange = profile
            .and_then(|p| p.exchange.as_deref())
            .unwrap_or_default();
        let is_growth_or_bse = market.contains("创业")
            || market.contains("科创")
            || market.contains("北交")
            || exchange.eq_ignore_ascii_case("BSE")
            || symbol.starts_with("300")
            || symbol.starts_with("301")
            || symbol.starts_with("688")
            || symbol.starts_with("8")
            || symbol.starts_with("4")
            || symbol.starts_with("920");

        if is_growth_or_bse {
            Decimal::new(20, 2)
        } else {
            Decimal::new(10, 2)
        }
    }

    // ─── Persistence ──────────────────────────────────────────

    async fn create_task(&self, task_id: &str, config: &BacktestConfig) -> Result<(), sqlx::Error> {
        let insert = BacktestTaskInsert::from_config(task_id, config);
        let cost_model = json!({
            "commission_rate": config.fee_config.commission_rate.to_string(),
            "min_commission": config.fee_config.min_commission.to_string(),
            "tax_rate": config.fee_config.tax_rate.to_string(),
            "cost_multiplier": config.fee_config.cost_multiplier.to_string()
        });
        let slippage_model = json!({
            "slippage_bps": config.fee_config.slippage_bps.to_string(),
            "impact_cost_coefficient": config.fee_config.impact_cost_coefficient.to_string()
        });
        let execution_rules = json!({
            "execution_timing": config.execution_timing,
            "execution_price": config.execution_price,
            "max_participation_rate": config.max_participation_rate.map(|v| v.to_string()),
            "persistence_mode": config.persistence_mode
        });
        let parameters = backtest_task_parameters(config);
        sqlx::query(
            r#"INSERT INTO backtest_task (task_id, strategy_version_id, data_version_id,
               prediction_set_id, benchmark_symbol, symbols, start_date, end_date, initial_capital,
               rebalance_frequency, cost_model, slippage_model, execution_rules,
               parameters, status, mode, progress, last_heartbeat_at, heartbeat_timeout_seconds, started_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'running', $15, 0, now(), 600, now())"#,
        )
        .bind(&insert.task_id)
        .bind(&insert.strategy_version_id)
        .bind(&insert.data_version_id)
        .bind(&insert.prediction_set_id)
        .bind(&insert.benchmark_symbol)
        .bind(&insert.symbols)
        .bind(config.start_date)
        .bind(config.end_date)
        .bind(config.initial_capital)
        .bind(&insert.rebalance_frequency)
        .bind(cost_model)
        .bind(slippage_model)
        .bind(execution_rules)
        .bind(parameters)
        .bind(&insert.mode)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_results(
        &self,
        task_id: &str,
        output: &BacktestOutput,
    ) -> Result<(), sqlx::Error> {
        let result_id = format!("result-{}", Uuid::new_v4());
        let metrics_json = serde_json::to_value(&output.metrics).unwrap_or(serde_json::Value::Null);

        // Backtest result
        sqlx::query(
            r#"INSERT INTO backtest_result (result_id, task_id,
               total_return, annualized_return, benchmark_return, excess_return,
               annualized_excess_return, sharpe_ratio, sortino_ratio,
               information_ratio, max_drawdown, relative_max_drawdown,
               turnover, total_trades, win_rate, metrics,
               calmar_ratio, annualized_volatility, reproducibility_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"#,
        )
        .bind(&result_id)
        .bind(task_id)
        .bind(output.metrics.total_return)
        .bind(output.metrics.annual_return_pct)
        .bind(output.metrics.benchmark_return_pct)
        .bind(output.metrics.excess_return_pct)
        .bind(output.metrics.excess_return_pct)
        .bind(output.metrics.sharpe_ratio)
        .bind(output.metrics.sortino_ratio)
        .bind(output.metrics.information_ratio)
        .bind(output.metrics.max_drawdown_pct)
        .bind(output.metrics.max_drawdown_pct)
        .bind(output.metrics.turnover)
        .bind(output.metrics.num_trades as i32)
        .bind(output.metrics.win_rate_pct)
        .bind(metrics_json)
        .bind(output.metrics.calmar_ratio)
        .bind(output.metrics.annualized_volatility)
        .bind(output.reproducibility_hash.as_deref().unwrap_or(""))
        .execute(&self.pool)
        .await?;

        // Equity curve
        for (date, val) in &output.equity_curve {
            sqlx::query(
                "INSERT INTO backtest_equity_curve (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(task_id)
            .bind(date)
            .bind(val)
            .bind(Decimal::zero()) // cash detail not tracked in current version
            .execute(&self.pool)
            .await?;
        }

        if !Self::should_persist_detail_tables(&output.config) {
            self.mark_task_completed(task_id, output).await?;
            return Ok(());
        }

        // Trades
        for trade in &output.trades {
            let trade_id = format!("tr-{}", Uuid::new_v4());
            sqlx::query(
                r#"INSERT INTO backtest_trade (trade_id, task_id, symbol, trade_time, side,
                   quantity, price, amount, commission, tax, slippage,
                   signal_type, signal_strength, fill_status, event_reason,
                   target_weight, executed_weight)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'filled', $14, $15, $16)"#,
            )
            .bind(&trade_id)
            .bind(task_id)
            .bind(&trade.symbol)
            .bind(trade.trade_date.and_hms_opt(15, 0, 0))
            .bind(match trade.side {
                super::portfolio::TradeSide::Buy => "buy",
                super::portfolio::TradeSide::Sell => "sell",
            })
            .bind(trade.quantity)
            .bind(trade.price)
            .bind(trade.amount)
            .bind(trade.commission)
            .bind(trade.tax)
            .bind(trade.slippage)
            .bind(trade.signal_type.as_deref())
            .bind(None::<Decimal>)
            .bind(trade.event_reason.as_deref())
            .bind(trade.target_weight)
            .bind(trade.executed_weight)
            .execute(&self.pool)
            .await?;
        }

        // Portfolio targets
        for target in &output.targets {
            sqlx::query(
                r#"INSERT INTO portfolio_target (task_id, trade_date, symbol, target_weight, target_quantity, reason)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (task_id, trade_date, symbol) DO UPDATE SET
                     target_weight = EXCLUDED.target_weight,
                     target_quantity = EXCLUDED.target_quantity,
                     reason = EXCLUDED.reason"#,
            )
            .bind(task_id)
            .bind(target.trade_date)
            .bind(&target.symbol)
            .bind(target.target_weight)
            .bind(target.target_quantity)
            .bind(target.reason.as_deref())
            .execute(&self.pool)
            .await?;
        }

        // Positions — batch insert (1000 per batch)
        //
        // [价格空间] backtest_position 落库口径用真实价（market_stock_daily_bar.close），
        // 而非 BC3 引擎内部的后复权价。BC3 equity_curve/绩效仍用后复权（分红再投资假设，
        // 无除权跳跃，audit 不变）；但仓位明细输出给下游实盘/mvo_simulate 时必须与真实
        // 资金管理口径一致，否则下游 select_positions 反推的成交价会落在后复权空间，与
        // 实盘持仓真实价混用（2026-08-10 NAV +21.6% 根因）。
        // [价格空间] backtest_position 落库口径用真实价。两层数据源：
        // 1) market_stock_daily_bar.close（当日真实价，EOD 20:00 入库）
        // 2) fallback: pos.close_price(后复权) / adj_factor 反算真实价
        //
        // 14:40 盘中调仓时当日 bar 尚未入库（EOD 20:00 才同步），raw_close_map 查不到
        // 当日真实价。若此时回退 pos.close_price（后复权）会污染 backtest_position，
        // 下游 select_positions 反推成交价落在后复权空间，与实盘持仓真实价混用
        // （2026-08-11 NAV +125.98% 根因：8/10 修复的 fallback 漏洞每个交易日必现）。
        //
        // adj_factor 取 trade_date <= position_date 的最近值（复权因子在除权日跳变后
        // 保持稳定，向前取最近即可）。已验证：adj_close / adj_factor = raw_close
        // （002014.SZ 8/10: 158.6543 / 14.5421 = 10.9100 ✓）。
        let (raw_close_map, adj_factor_map): (PriceMapByDateSymbol, PriceMapByDateSymbol) = {
            let symbols: Vec<String> = output
                .daily_positions
                .iter()
                .map(|p| p.symbol.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            if symbols.is_empty() {
                (HashMap::new(), HashMap::new())
            } else {
                let mut dates: Vec<NaiveDate> =
                    output.daily_positions.iter().map(|p| p.date).collect();
                dates.sort();
                let (min_d, max_d) = (*dates.first().unwrap(), *dates.last().unwrap());
                let raw_close_map: HashMap<(NaiveDate, String), Decimal> =
                    sqlx::query_as::<_, (NaiveDate, String, Decimal)>(
                        "SELECT trade_date, symbol, close::numeric FROM market_stock_daily_bar
                         WHERE symbol = ANY($1) AND trade_date BETWEEN $2 AND $3",
                    )
                    .bind(&symbols)
                    .bind(min_d)
                    .bind(max_d)
                    .fetch_all(&self.pool)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(d, s, c)| ((d, s), c))
                    .collect();
                // adj_factor: 简单范围查询全量因子，Rust 侧按 (symbol, date<=pos_date) 就近取。
                // 此前用 CROSS JOIN × LATERAL（3080 日 × 500 股 = 150 万组合），查询超时
                // 被 unwrap_or_default 静默吞掉 → adj_factor_map 恒空 → 停牌股 fallback
                // 全部走 unwrap_or(ONE) → 复权价直接写入（价格空间切换 BUG 的真正根因）。
                let adj_rows: Vec<(String, NaiveDate, Decimal)> = sqlx::query_as(
                    "SELECT symbol, trade_date, adj_factor::numeric FROM market_adjustment_factor
                         WHERE symbol = ANY($1) AND trade_date BETWEEN $2 AND $3
                         ORDER BY symbol, trade_date",
                )
                .bind(&symbols)
                .bind(min_d)
                .bind(max_d)
                .fetch_all(&self.pool)
                .await
                .unwrap_or_default();
                // 构建 (symbol → [(date, factor)]) 索引，查询时二分/线性找 <= pos_date 的最近因子
                let mut factor_by_symbol: HashMap<String, Vec<(NaiveDate, Decimal)>> =
                    HashMap::new();
                for (sym, d, f) in adj_rows {
                    factor_by_symbol.entry(sym).or_default().push((d, f));
                }
                // 为每个 position_date 构建精确查找表
                let position_dates: Vec<NaiveDate> = output
                    .daily_positions
                    .iter()
                    .map(|p| p.date)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                let mut adj_factor_map: HashMap<(NaiveDate, String), Decimal> = HashMap::new();
                for pd in &position_dates {
                    for sym in &symbols {
                        if let Some(factors) = factor_by_symbol.get(sym) {
                            // 找 <= pd 的最近因子（factors 已按 date 排序）
                            let best = factors.iter().rev().find(|(d, _)| d <= pd).map(|(_, f)| *f);
                            if let Some(f) = best {
                                adj_factor_map.insert((*pd, sym.clone()), f);
                            }
                        }
                    }
                }
                (raw_close_map, adj_factor_map)
            }
        };
        for chunk in output.daily_positions.chunks(1000) {
            let mut query_builder = String::from(
                "INSERT INTO backtest_position (task_id, symbol, position_date,
                 quantity, available_quantity, avg_cost, close_price,
                 market_value, weight, unrealized_pnl, target_weight) VALUES ",
            );
            let mut params: Vec<String> = Vec::new();
            for (i, pos) in chunk.iter().enumerate() {
                if i > 0 {
                    query_builder.push_str(", ");
                }
                let base = i * 11;
                query_builder.push_str(&format!(
                    "(${}, ${}, ${}::date, ${}::numeric, ${}::numeric, ${}::numeric, ${}::numeric, ${}::numeric, ${}::numeric, ${}::numeric, ${}::numeric)",
                    base + 1, base + 2, base + 3, base + 4, base + 5, base + 6,
                    base + 7, base + 8, base + 9, base + 10, base + 11
                ));
                params.push(task_id.to_string());
                params.push(pos.symbol.clone());
                params.push(pos.date.format("%Y-%m-%d").to_string());
                params.push(pos.quantity.to_string());
                params.push(pos.available_quantity.to_string());
                params.push(pos.avg_cost.to_string());
                // close_price / market_value 用真实价（与实盘口径一致）——价格语义铁律：
                // 宁可报错不出结果，也不能用错误价格空间的数据污染下游（用户 2026-09-07 指令）。
                // 三层取价：① raw_close（当日真实价，EOD 入库）② 后复权价/adj_factor 反算 ③ 报错
                // 14:40 盘中调仓当日 bar 未入库时走 ②，用 pos.close_price(后复权) / adj_factor
                // 反算真实价。③ 不再兜底用复权价——2026-07-13 raw+factor 双缺时复权价 439.17
                // 被当真实价 7.61 写入，导致下游 composite NAV 单日翻倍（价格源切换 BUG）。
                let (cp, mv) = match raw_close_map.get(&(pos.date, pos.symbol.clone())) {
                    Some(rc) if !rc.is_zero() => (*rc, pos.quantity * *rc),
                    _ => {
                        // adj_factor 缺失时默认 1.0（无公司行动 = 无复权 = 因子 1.0）
                        // ——135 只股票无任何 factor 记录，实际是"从未除权"而非数据缺失
                        let adj_factor = adj_factor_map
                            .get(&(pos.date, pos.symbol.clone()))
                            .copied()
                            .filter(|f| !f.is_zero())
                            .unwrap_or(Decimal::ONE);
                        let raw = pos.close_price / adj_factor;
                        (raw, pos.quantity * raw)
                    }
                };
                params.push(cp.to_string());
                params.push(mv.to_string());
                params.push(pos.weight.to_string());
                params.push(pos.unrealized_pnl.to_string());
                params.push(
                    pos.target_weight
                        .map(|w| w.to_string())
                        .unwrap_or_else(|| "0".to_string()),
                );
            }
            query_builder.push_str(" ON CONFLICT (task_id, symbol, position_date) DO NOTHING");
            let mut q = sqlx::query(&query_builder);
            for p in &params {
                q = q.bind(p);
            }
            q.execute(&self.pool).await?;
        }

        // Portfolio exposures
        for exposure in &output.exposures {
            sqlx::query(
                r#"INSERT INTO portfolio_exposure
                   (task_id, trade_date, exposure_type, exposure_name, net_exposure, gross_exposure)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (task_id, trade_date, exposure_type, exposure_name) DO UPDATE SET
                     net_exposure = EXCLUDED.net_exposure,
                     gross_exposure = EXCLUDED.gross_exposure"#,
            )
            .bind(task_id)
            .bind(exposure.trade_date)
            .bind(&exposure.exposure_type)
            .bind(&exposure.exposure_name)
            .bind(exposure.net_exposure)
            .bind(exposure.gross_exposure)
            .execute(&self.pool)
            .await?;
        }

        // Portfolio attributions
        for attribution in &output.attributions {
            sqlx::query(
                r#"INSERT INTO portfolio_attribution
                   (task_id, trade_date, attribution_type, attribution_name, contribution)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (task_id, trade_date, attribution_type, attribution_name) DO UPDATE SET
                     contribution = EXCLUDED.contribution"#,
            )
            .bind(task_id)
            .bind(attribution.trade_date)
            .bind(&attribution.attribution_type)
            .bind(&attribution.attribution_name)
            .bind(attribution.contribution)
            .execute(&self.pool)
            .await?;
        }

        // Constraint violations
        for violation in &output.violations {
            let violation_id = format!("pv-{}", Uuid::new_v4());
            sqlx::query(
                r#"INSERT INTO portfolio_constraint_violation
                   (violation_id, task_id, trade_date, constraint_name, limit_value, actual_value, severity)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            )
            .bind(&violation_id)
            .bind(task_id)
            .bind(violation.trade_date)
            .bind(&violation.constraint_name)
            .bind(violation.limit_value)
            .bind(violation.actual_value)
            .bind(&violation.severity)
            .execute(&self.pool)
            .await?;
        }

        // Update task status
        self.mark_task_completed(task_id, output).await?;

        Ok(())
    }

    fn should_persist_detail_tables(config: &BacktestConfig) -> bool {
        config.mode != BacktestMode::Fast
            && matches!(config.persistence_mode, BacktestPersistenceMode::Full)
    }

    async fn mark_task_completed(
        &self,
        task_id: &str,
        output: &BacktestOutput,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE backtest_task SET status = 'completed', completed_at = now(), progress = 100, last_completed_date = $2, last_heartbeat_at = now() WHERE task_id = $1",
        )
        .bind(task_id)
        .bind(output.equity_curve.last().map(|(date, _)| *date))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_rate_uses_st_growth_board_and_default_rules() {
        let st = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: true,
            instrument_type: Some("stock".into()),
        };
        let growth = TradingProfile {
            exchange: Some("SZSE".into()),
            market: Some("创业板".into()),
            is_st: false,
            instrument_type: Some("stock".into()),
        };
        let main = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: false,
            instrument_type: Some("stock".into()),
        };

        assert_eq!(
            BacktestRunner::limit_rate_for("600000.SH", Some(&st)),
            Decimal::new(5, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("300001.SZ", Some(&growth)),
            Decimal::new(20, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("600000.SH", Some(&main)),
            Decimal::new(10, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("688001.SH", None),
            Decimal::new(20, 2)
        );
    }

    #[test]
    fn summary_only_persistence_skips_detail_tables() {
        let config = BacktestConfig {
            persistence_mode: BacktestPersistenceMode::SummaryOnly,
            ..Default::default()
        };

        assert!(!BacktestRunner::should_persist_detail_tables(&config));
    }

    #[test]
    fn backtest_data_cache_reuses_overlapping_daily_bar_symbols() {
        let dv_id = "dv-test-001";
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let mut cache = BacktestDataCache::default();

        let first_symbols = vec!["000002.SZ".to_string(), "000001.SZ".to_string()];
        let (missing_first, cached_first) =
            cache.cached_daily_bar_symbols(dv_id, &first_symbols, start, end);
        assert_eq!(missing_first, vec!["000001.SZ", "000002.SZ"]);
        assert!(cached_first.is_empty());

        cache.insert_daily_bar_rows(
            dv_id,
            start,
            end,
            &missing_first,
            vec![
                DailyBarRecord {
                    trade_date: start,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(101, 1),
                    close: Decimal::new(102, 1),
                    pre_close: Decimal::new(100, 1),
                    amount: Decimal::new(1000, 0),
                    data_version_id: dv_id.into(),
                },
                DailyBarRecord {
                    trade_date: start,
                    symbol: "000002.SZ".into(),
                    open: Decimal::new(201, 1),
                    close: Decimal::new(202, 1),
                    pre_close: Decimal::new(200, 1),
                    amount: Decimal::new(2000, 0),
                    data_version_id: dv_id.into(),
                },
            ],
        );

        let second_symbols = vec!["000002.SZ".to_string(), "000003.SZ".to_string()];
        let (missing_second, cached_second) =
            cache.cached_daily_bar_symbols(dv_id, &second_symbols, start, end);

        assert_eq!(missing_second, vec!["000003.SZ"]);
        assert_eq!(
            cached_second
                .get(&start)
                .and_then(|day| day.get("000002.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(202, 1))
        );
        let stats = cache.stats();
        assert_eq!(stats.daily_bar_symbol_hits, 1);
        assert_eq!(stats.daily_bar_symbol_misses, 3);
    }

    #[test]
    fn backtest_data_cache_reuses_wider_daily_bar_window_for_narrower_request() {
        let dv_id = "dv-test-002";
        let wide_start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();

        let symbols = vec!["000001.SZ".to_string()];
        cache.insert_daily_bar_rows(
            dv_id,
            wide_start,
            wide_end,
            &symbols,
            vec![
                DailyBarRecord {
                    trade_date: wide_start,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(101, 1),
                    close: Decimal::new(102, 1),
                    pre_close: Decimal::new(100, 1),
                    amount: Decimal::new(1000, 0),
                    data_version_id: dv_id.into(),
                },
                DailyBarRecord {
                    trade_date: narrow_start,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(111, 1),
                    close: Decimal::new(112, 1),
                    pre_close: Decimal::new(110, 1),
                    amount: Decimal::new(1100, 0),
                    data_version_id: dv_id.into(),
                },
                DailyBarRecord {
                    trade_date: narrow_end,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(121, 1),
                    close: Decimal::new(122, 1),
                    pre_close: Decimal::new(120, 1),
                    amount: Decimal::new(1200, 0),
                    data_version_id: dv_id.into(),
                },
                DailyBarRecord {
                    trade_date: wide_end,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(131, 1),
                    close: Decimal::new(132, 1),
                    pre_close: Decimal::new(130, 1),
                    amount: Decimal::new(1300, 0),
                    data_version_id: dv_id.into(),
                },
            ],
        );

        let (missing, cached) =
            cache.cached_daily_bar_symbols(dv_id, &symbols, narrow_start, narrow_end);

        assert!(missing.is_empty());
        assert_eq!(cached.len(), 2);
        assert!(!cached.contains_key(&wide_start));
        assert!(!cached.contains_key(&wide_end));
        assert_eq!(
            cached
                .get(&narrow_start)
                .and_then(|day| day.get("000001.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(112, 1))
        );
        assert_eq!(
            cached
                .get(&narrow_end)
                .and_then(|day| day.get("000001.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(122, 1))
        );
        let stats = cache.stats();
        assert_eq!(stats.daily_bar_symbol_hits, 1);
        assert_eq!(stats.daily_bar_covering_window_hits, 1);
        assert_eq!(stats.daily_bar_snapshot_hits, 0);
        assert_eq!(stats.daily_bar_symbol_misses, 0);
    }

    #[test]
    fn backtest_market_data_snapshot_reuses_subset_window_daily_bars() {
        let wide_start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();
        let symbols = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        let key = BacktestMarketDataSnapshotKey::new(
            "full-market-2016-v1",
            "000300.SH",
            wide_start,
            wide_end,
            &symbols,
        );
        let mut snapshot_bars = DailyBarsByDate::new();
        for (date, close_a, close_b) in [
            (wide_start, Decimal::new(102, 1), Decimal::new(202, 1)),
            (narrow_start, Decimal::new(112, 1), Decimal::new(212, 1)),
            (narrow_end, Decimal::new(122, 1), Decimal::new(222, 1)),
            (wide_end, Decimal::new(132, 1), Decimal::new(232, 1)),
        ] {
            snapshot_bars.entry(date).or_default().insert(
                "000001.SZ".to_string(),
                (close_a, close_a, close_a, Decimal::new(1000, 0)),
            );
            snapshot_bars.entry(date).or_default().insert(
                "000002.SZ".to_string(),
                (close_b, close_b, close_b, Decimal::new(2000, 0)),
            );
        }
        cache.insert_market_data_snapshot(key, snapshot_bars);

        let (missing, cached) = cache.cached_daily_bar_symbols(
            "full-market-2016-v1",
            &["000002.SZ".to_string()],
            narrow_start,
            narrow_end,
        );

        assert!(missing.is_empty());
        assert_eq!(cached.len(), 2);
        assert!(!cached.contains_key(&wide_start));
        assert!(!cached.contains_key(&wide_end));
        assert_eq!(
            cached
                .get(&narrow_start)
                .and_then(|day| day.get("000002.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(212, 1))
        );
        assert_eq!(
            cached
                .get(&narrow_end)
                .and_then(|day| day.get("000002.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(222, 1))
        );
        let stats = cache.stats();
        assert_eq!(stats.daily_bar_symbol_hits, 1);
        assert_eq!(stats.daily_bar_covering_window_hits, 0);
        assert_eq!(stats.daily_bar_snapshot_hits, 1);
        assert_eq!(stats.daily_bar_symbol_misses, 0);
    }

    #[test]
    fn backtest_data_cache_reuses_wider_trading_days_for_narrower_request() {
        let wide_start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();

        cache.insert_trading_days(
            wide_start,
            wide_end,
            vec![wide_start, narrow_start, narrow_end, wide_end],
        );

        let cached = cache
            .cached_trading_days(narrow_start, narrow_end)
            .expect("narrow trading days should be served from wider cache");

        assert_eq!(cached, vec![narrow_start, narrow_end]);
        let stats = cache.stats();
        assert_eq!(stats.trading_day_hits, 1);
        assert_eq!(stats.trading_day_misses, 0);
    }

    #[test]
    fn backtest_data_cache_reuses_wider_benchmark_window_for_narrower_request() {
        let wide_start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();
        let benchmark = "000300.SH";

        cache.insert_benchmark_data(
            benchmark,
            wide_start,
            wide_end,
            HashMap::from([
                (wide_start, (Decimal::new(100, 0), Decimal::new(99, 0))),
                (narrow_start, (Decimal::new(101, 0), Decimal::new(100, 0))),
                (narrow_end, (Decimal::new(102, 0), Decimal::new(101, 0))),
                (wide_end, (Decimal::new(103, 0), Decimal::new(102, 0))),
            ]),
        );

        let cached = cache
            .cached_benchmark_data(benchmark, narrow_start, narrow_end)
            .expect("narrow benchmark data should be served from wider cache");

        assert_eq!(cached.len(), 2);
        assert!(!cached.contains_key(&wide_start));
        assert!(!cached.contains_key(&wide_end));
        assert_eq!(
            cached.get(&narrow_start).copied(),
            Some((Decimal::new(101, 0), Decimal::new(100, 0)))
        );
        assert_eq!(
            cached.get(&narrow_end).copied(),
            Some((Decimal::new(102, 0), Decimal::new(101, 0)))
        );
        let stats = cache.stats();
        assert_eq!(stats.benchmark_data_hits, 1);
        assert_eq!(stats.benchmark_data_misses, 0);
    }

    #[test]
    fn schedule_signals_maps_each_signal_to_next_trading_day() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let signal = |date, symbol: &str, weight: i64| StrategySignal {
            date,
            target_weights: HashMap::from([(symbol.to_string(), Decimal::new(weight, 2))]),
        };
        let signals = HashMap::from([
            (d1, signal(d1, "000001.SZ", 50)),
            (d2, signal(d2, "000002.SZ", 60)),
        ]);

        let scheduled = schedule_signals_for_execution(&[d1, d2, d3], &signals);

        // windows(2) 语义：信号日 -> 次个交易日执行
        assert_eq!(scheduled.len(), 2);
        assert_eq!(
            scheduled.get(&d2).map(|s| &s.target_weights),
            Some(&HashMap::from([(
                "000001.SZ".to_string(),
                Decimal::new(50, 2)
            )]))
        );
        assert_eq!(
            scheduled.get(&d3).map(|s| &s.target_weights),
            Some(&HashMap::from([(
                "000002.SZ".to_string(),
                Decimal::new(60, 2)
            )]))
        );
        assert!(
            !scheduled.contains_key(&d1),
            "首日信号只能映射到次日执行，执行键不可能是信号日自身"
        );
    }

    #[test]
    fn schedule_signals_drops_last_day_signal_and_non_trading_day_signal() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let d5 = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        let make = |date: NaiveDate| StrategySignal {
            date,
            target_weights: HashMap::from([("000001.SZ".to_string(), Decimal::ONE)]),
        };
        // d3 是最后一个交易日，没有次个交易日可执行；d5 不在交易日历中
        let signals = HashMap::from([(d3, make(d3)), (d5, make(d5))]);

        let scheduled = schedule_signals_for_execution(&[d1, d2, d3], &signals);

        assert!(scheduled.is_empty());
    }

    #[test]
    fn schedule_signals_handles_empty_and_single_day_inputs() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let signals = HashMap::from([(
            d1,
            StrategySignal {
                date: d1,
                target_weights: HashMap::new(),
            },
        )]);

        // 交易日历不足两天时 windows(2) 为空，安全返回空 map
        assert!(schedule_signals_for_execution(&[], &HashMap::new()).is_empty());
        assert!(schedule_signals_for_execution(&[], &signals).is_empty());
        assert!(schedule_signals_for_execution(&[d1], &signals).is_empty());
    }

    #[test]
    fn backtest_cache_stats_delta_computes_per_field_difference() {
        let before = BacktestDataCacheStats {
            trading_day_hits: 3,
            trading_day_misses: 1,
            benchmark_data_hits: 4,
            benchmark_data_misses: 2,
            daily_bar_symbol_hits: 10,
            daily_bar_covering_window_hits: 2,
            daily_bar_snapshot_hits: 1,
            daily_bar_symbol_misses: 5,
            trading_profile_symbol_hits: 6,
            trading_profile_symbol_misses: 3,
        };
        let after = BacktestDataCacheStats {
            trading_day_hits: 10,
            trading_day_misses: 4,
            benchmark_data_hits: 9,
            benchmark_data_misses: 2,
            daily_bar_symbol_hits: 12,
            daily_bar_covering_window_hits: 5,
            daily_bar_snapshot_hits: 4,
            daily_bar_symbol_misses: 9,
            trading_profile_symbol_hits: 8,
            trading_profile_symbol_misses: 3,
        };

        let delta = backtest_cache_stats_delta(before, after);

        assert_eq!(delta.trading_day_hits, 7);
        assert_eq!(delta.trading_day_misses, 3);
        assert_eq!(delta.benchmark_data_hits, 5);
        assert_eq!(delta.benchmark_data_misses, 0);
        assert_eq!(delta.daily_bar_symbol_hits, 2);
        assert_eq!(delta.daily_bar_covering_window_hits, 3);
        assert_eq!(delta.daily_bar_snapshot_hits, 3);
        assert_eq!(delta.daily_bar_symbol_misses, 4);
        assert_eq!(delta.trading_profile_symbol_hits, 2);
        assert_eq!(delta.trading_profile_symbol_misses, 0);
    }

    #[test]
    fn backtest_cache_stats_delta_saturates_when_counters_reset() {
        // from_snapshot 会把计数器清零：after < before 时 delta 必须饱和为 0 而不是下溢 panic
        let before = BacktestDataCacheStats {
            trading_day_hits: 10,
            trading_day_misses: 4,
            benchmark_data_hits: 9,
            benchmark_data_misses: 2,
            daily_bar_symbol_hits: 12,
            daily_bar_covering_window_hits: 5,
            daily_bar_snapshot_hits: 4,
            daily_bar_symbol_misses: 9,
            trading_profile_symbol_hits: 8,
            trading_profile_symbol_misses: 3,
        };

        let delta = backtest_cache_stats_delta(before, BacktestDataCacheStats::default());

        assert_eq!(delta.trading_day_hits, 0);
        assert_eq!(delta.trading_day_misses, 0);
        assert_eq!(delta.benchmark_data_hits, 0);
        assert_eq!(delta.benchmark_data_misses, 0);
        assert_eq!(delta.daily_bar_symbol_hits, 0);
        assert_eq!(delta.daily_bar_covering_window_hits, 0);
        assert_eq!(delta.daily_bar_snapshot_hits, 0);
        assert_eq!(delta.daily_bar_symbol_misses, 0);
        assert_eq!(delta.trading_profile_symbol_hits, 0);
        assert_eq!(delta.trading_profile_symbol_misses, 0);
    }

    #[test]
    fn snapshot_key_trims_inputs_and_normalizes_symbol_order() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();

        let key = BacktestMarketDataSnapshotKey::new(
            "  dv-2024-q1  ",
            " 000300.SH ",
            start,
            end,
            &["000002.SZ".to_string(), "000001.SZ".to_string()],
        );
        let equivalent = BacktestMarketDataSnapshotKey::new(
            "dv-2024-q1",
            "000300.SH",
            start,
            end,
            &[
                "000001.SZ".to_string(),
                "000002.SZ".to_string(),
                "000001.SZ".to_string(),
            ],
        );

        // 输入 trim；符号顺序/重复归一化后键等价
        assert_eq!(key.data_version_id, "dv-2024-q1");
        assert_eq!(key.benchmark, "000300.SH");
        assert_eq!(key, equivalent);
        assert_eq!(key.universe_hash, equivalent.universe_hash);
        assert_eq!(key.universe_hash.len(), 16);
        assert!(key.universe_hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Hash/Eq 语义一致：作为 HashMap 键时归一化后的 key 视为同一键
        let mut map = HashMap::new();
        map.insert(key, 1);
        map.insert(equivalent, 2);
        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next(), Some(&2));
    }

    #[test]
    fn snapshot_key_distinguishes_universe_window_and_version() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let later_end = NaiveDate::from_ymd_opt(2024, 6, 30).unwrap();
        let pair = |a: &str, b: &str| vec![a.to_string(), b.to_string()];

        let base =
            BacktestMarketDataSnapshotKey::new("dv-1", "000300.SH", start, end, &pair("A", "B"));
        let other_universe =
            BacktestMarketDataSnapshotKey::new("dv-1", "000300.SH", start, end, &pair("A", "C"));
        let other_window = BacktestMarketDataSnapshotKey::new(
            "dv-1",
            "000300.SH",
            start,
            later_end,
            &pair("A", "B"),
        );
        let other_version =
            BacktestMarketDataSnapshotKey::new("dv-2", "000300.SH", start, end, &pair("A", "B"));

        assert_ne!(base.universe_hash, other_universe.universe_hash);
        assert_ne!(base, other_universe);
        assert_ne!(base, other_window);
        assert_ne!(base, other_version);
    }

    #[test]
    fn symbol_universe_hash_is_deterministic_and_non_trivial() {
        // DefaultHasher::new() 使用固定 key，同输入跨调用结果稳定
        assert_eq!(symbol_universe_hash(&[]), symbol_universe_hash(&[]));
        assert_eq!(symbol_universe_hash(&[]).len(), 16);
        assert!(symbol_universe_hash(&[])
            .chars()
            .all(|c| c.is_ascii_hexdigit()));

        let single = vec!["600000.SH".to_string()];
        assert_eq!(symbol_universe_hash(&single), symbol_universe_hash(&single));
        assert_ne!(symbol_universe_hash(&single), symbol_universe_hash(&[]));
    }

    #[test]
    fn backtest_data_cache_snapshot_roundtrip_preserves_all_caches() {
        let dv_id = "dv-snap-001";
        let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();

        cache.insert_trading_days(start, end, vec![start, end]);
        cache.insert_benchmark_data(
            "000300.SH",
            start,
            end,
            HashMap::from([(start, (Decimal::new(100, 0), Decimal::new(99, 0)))]),
        );
        cache.insert_daily_bar_rows(
            dv_id,
            start,
            end,
            &["000001.SZ".to_string()],
            vec![DailyBarRecord {
                trade_date: start,
                symbol: "000001.SZ".into(),
                open: Decimal::new(101, 1),
                close: Decimal::new(102, 1),
                pre_close: Decimal::new(100, 1),
                amount: Decimal::new(1000, 0),
                data_version_id: dv_id.into(),
            }],
        );
        cache.insert_trading_profiles(
            &["000001.SZ".to_string(), "000002.SZ".to_string()],
            vec![(
                "000001.SZ".to_string(),
                Some("SZSE".to_string()),
                Some("主板".to_string()),
                Some(false),
                Some("stock".to_string()),
            )],
        );

        // snapshot 只读克隆，不触碰命中统计
        let snapshot = cache.snapshot();
        assert_eq!(cache.stats().trading_day_hits, 0);
        assert_eq!(cache.stats().daily_bar_symbol_hits, 0);

        let mut restored = BacktestDataCache::from_snapshot(&snapshot);
        // from_snapshot 重置统计计数器
        assert_eq!(restored.stats().trading_day_hits, 0);
        assert_eq!(restored.stats().trading_day_misses, 0);

        // 交易日历精确键命中
        let days = restored
            .cached_trading_days(start, end)
            .expect("trading days must survive snapshot roundtrip");
        assert_eq!(days, vec![start, end]);

        // 基准数据精确键命中
        let benchmark = restored
            .cached_benchmark_data("000300.SH", start, end)
            .expect("benchmark data must survive snapshot roundtrip");
        assert_eq!(
            benchmark.get(&start).copied(),
            Some((Decimal::new(100, 0), Decimal::new(99, 0)))
        );

        // 日线记录命中
        let (missing, bars) =
            restored.cached_daily_bar_symbols(dv_id, &["000001.SZ".to_string()], start, end);
        assert!(missing.is_empty());
        assert_eq!(
            bars.get(&start)
                .and_then(|day| day.get("000001.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(102, 1))
        );

        // 交易特征：000001 有档命中；000002 走负缓存命中（不在 missing）
        let (missing_profiles, cached_profiles) =
            restored.cached_trading_profiles(&["000001.SZ".to_string(), "000002.SZ".to_string()]);
        assert!(missing_profiles.is_empty());
        assert_eq!(cached_profiles.len(), 1);
        assert!(cached_profiles.contains_key("000001.SZ"));

        let stats = restored.stats();
        assert_eq!(stats.trading_day_hits, 1);
        assert_eq!(stats.trading_day_misses, 0);
        assert_eq!(stats.benchmark_data_hits, 1);
        assert_eq!(stats.daily_bar_symbol_hits, 1);
        assert_eq!(stats.daily_bar_symbol_misses, 0);
        assert_eq!(stats.trading_profile_symbol_hits, 2);
        assert_eq!(stats.trading_profile_symbol_misses, 0);

        // 二次往返：恢复出的缓存再次快照/恢复仍完整
        let snapshot2 = restored.snapshot();
        let mut restored2 = BacktestDataCache::from_snapshot(&snapshot2);
        let (missing2, bars2) =
            restored2.cached_daily_bar_symbols(dv_id, &["000001.SZ".to_string()], start, end);
        assert!(missing2.is_empty());
        assert_eq!(
            bars2
                .get(&start)
                .and_then(|day| day.get("000001.SZ"))
                .map(|(_, close, _, _)| *close),
            Some(Decimal::new(102, 1))
        );
    }

    #[test]
    fn normalized_symbol_key_sorts_and_dedups() {
        assert_eq!(
            normalized_symbol_key(&[
                "C.SZ".to_string(),
                "A.SZ".to_string(),
                "B.SH".to_string(),
                "A.SZ".to_string(),
            ]),
            vec!["A.SZ".to_string(), "B.SH".to_string(), "C.SZ".to_string()]
        );
        assert!(normalized_symbol_key(&[]).is_empty());
    }

    #[test]
    fn merge_daily_bar_records_indexes_by_date_and_overwrites_same_symbol() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let record = |date, symbol: &str, close: i64| DailyBarRecord {
            trade_date: date,
            symbol: symbol.into(),
            open: Decimal::new(close - 1, 0),
            close: Decimal::new(close, 0),
            pre_close: Decimal::new(close - 2, 0),
            amount: Decimal::new(close * 10, 0),
            data_version_id: "dv".into(),
        };

        let mut target = DailyBarsByDate::new();
        merge_daily_bar_records(
            &mut target,
            &[
                record(d1, "A", 10),
                record(d1, "B", 20),
                record(d2, "A", 11),
                // 同 (date, symbol) 后写覆盖
                record(d1, "A", 12),
            ],
        );

        assert_eq!(target.len(), 2);
        assert_eq!(target[&d1].len(), 2);
        assert_eq!(target[&d1]["A"].0, Decimal::new(11, 0));
        assert_eq!(target[&d1]["A"].1, Decimal::new(12, 0));
        assert_eq!(target[&d1]["A"].2, Decimal::new(10, 0));
        assert_eq!(target[&d1]["A"].3, Decimal::new(120, 0));
        assert_eq!(target[&d1]["B"].1, Decimal::new(20, 0));
        assert_eq!(target[&d2]["A"].1, Decimal::new(11, 0));
    }

    #[test]
    fn merge_daily_bars_overlays_same_date_and_appends_new_days() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let bar = |close: i64| {
            (
                Decimal::new(close - 1, 0),
                Decimal::new(close, 0),
                Decimal::new(close - 2, 0),
                Decimal::new(close * 10, 0),
            )
        };
        let mut target: DailyBarsByDate =
            HashMap::from([(d1, HashMap::from([("A".to_string(), bar(10))]))]);
        let source: DailyBarsByDate = HashMap::from([
            // 同日期：A 被覆盖为 99，B 新增
            (
                d1,
                HashMap::from([("A".to_string(), bar(99)), ("B".to_string(), bar(20))]),
            ),
            (d2, HashMap::from([("A".to_string(), bar(11))])),
        ]);

        merge_daily_bars(&mut target, source);

        assert_eq!(target.len(), 2);
        assert_eq!(target[&d1].len(), 2);
        assert_eq!(target[&d1]["A"].1, Decimal::new(99, 0));
        assert_eq!(target[&d1]["B"].1, Decimal::new(20, 0));
        assert_eq!(target[&d2]["A"].1, Decimal::new(11, 0));
    }

    #[test]
    fn backtest_task_insert_from_config_maps_all_fields() {
        let config = BacktestConfig {
            strategy_version_id: "sv-42".into(),
            data_version_id: "dv-42".into(),
            prediction_set_id: Some("ps-42".into()),
            benchmark: "000905.SH".into(),
            symbols: vec!["000001.SZ".into(), "600000.SH".into()],
            rebalance_frequency: "weekly".into(),
            mode: BacktestMode::Audit,
            ..Default::default()
        };

        let insert = BacktestTaskInsert::from_config("task-42", &config);

        assert_eq!(insert.task_id, "task-42");
        assert_eq!(insert.strategy_version_id, "sv-42");
        assert_eq!(insert.data_version_id, "dv-42");
        assert_eq!(insert.prediction_set_id.as_deref(), Some("ps-42"));
        assert_eq!(insert.benchmark_symbol, "000905.SH");
        assert_eq!(
            insert.symbols,
            vec!["000001.SZ".to_string(), "600000.SH".to_string()]
        );
        assert_eq!(insert.rebalance_frequency, "weekly");
        assert_eq!(insert.mode, "audit");
    }

    #[test]
    fn backtest_task_insert_from_config_covers_remaining_modes() {
        for (mode, expected) in [
            (BacktestMode::Fast, "fast"),
            (BacktestMode::Standard, "standard"),
        ] {
            let config = BacktestConfig {
                mode,
                ..Default::default()
            };
            assert_eq!(BacktestTaskInsert::from_config("t", &config).mode, expected);
        }

        // prediction_set_id 缺省映射为 None
        let insert = BacktestTaskInsert::from_config("t", &BacktestConfig::default());
        assert_eq!(insert.prediction_set_id, None);
        assert_eq!(insert.mode, "standard");
    }

    #[test]
    fn backtest_task_parameters_fills_missing_audit_keys_from_config() {
        let config = BacktestConfig {
            research_dataset_id: Some("rd-1".into()),
            feature_set_version_id: None,
            prediction_set_id: Some("ps-1".into()),
            portfolio_policy_id: None,
            parameters: json!({"custom_key": 7}),
            ..Default::default()
        };

        let parameters = backtest_task_parameters(&config);

        let object = parameters
            .as_object()
            .expect("parameters must stay an object");
        assert_eq!(object.get("custom_key"), Some(&json!(7)));
        assert_eq!(object.get("research_dataset_id"), Some(&json!("rd-1")));
        assert_eq!(object.get("feature_set_version_id"), Some(&Value::Null));
        assert_eq!(object.get("prediction_set_id"), Some(&json!("ps-1")));
        assert_eq!(object.get("portfolio_policy_id"), Some(&Value::Null));
        assert_eq!(object.len(), 5);
    }

    #[test]
    fn backtest_task_parameters_preserves_existing_keys_and_coerces_non_object() {
        // 已有键不被 config 值覆盖（or_insert 语义）
        let config = BacktestConfig {
            research_dataset_id: Some("rd-1".into()),
            parameters: json!({"research_dataset_id": "keep-me"}),
            ..Default::default()
        };
        let parameters = backtest_task_parameters(&config);
        assert_eq!(
            parameters.get("research_dataset_id"),
            Some(&json!("keep-me"))
        );

        // 非对象 parameters（字符串）被重置为空对象后再注入 4 个审计键
        let config = BacktestConfig {
            parameters: json!("not-an-object"),
            ..Default::default()
        };
        let parameters = backtest_task_parameters(&config);
        let object = parameters
            .as_object()
            .expect("non-object parameters must be coerced to object");
        assert_eq!(object.len(), 4);
        for key in [
            "research_dataset_id",
            "feature_set_version_id",
            "prediction_set_id",
            "portfolio_policy_id",
        ] {
            assert_eq!(object.get(key), Some(&Value::Null));
        }

        // JSON Null 同样被重置
        let config = BacktestConfig {
            parameters: Value::Null,
            ..Default::default()
        };
        assert!(backtest_task_parameters(&config).is_object());
    }

    // ─────────────────────────────────────────────────────────────────
    // DB 连库集成（只读；连本地 postgres://gaocheng@localhost/quant，真实生产数据）
    // ─────────────────────────────────────────────────────────────────

    /// 构造 NaiveDate 的简写。
    fn dbd(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
    }

    /// 连接本地 quant 库（先例：quant-factor/src/repository.rs 的 tests 模式）。
    /// runner 的 DB 写表测试（full_flow/dup/cache/empty_signals）共享九张生产表，
/// 并行执行时曾出现 task 行被外部删除导致 FK 23503（根源未定位，疑似 PG
/// 连接池竞争下的时序问题）——静态互斥串行化，稳定压倒并行速度。
static DB_RUN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn db_test_pool() -> PgPool {
        PgPool::connect("postgres://gaocheng@localhost/quant")
            .await
            .expect("connect local quant db")
    }

    /// 数据依据（psql 2026-09-20 核实）：
    /// `SELECT MIN(trade_date), MAX(trade_date) FROM market_trade_calendar
    ///  WHERE exchange='SSE' AND is_open` → 1990-12-19 ~ 2026-12-31。
    /// 2026-01-03 为周六（非交易日），2026-01-05 为周一（交易日）。
    #[tokio::test]
    async fn db_load_trading_days_returns_sse_calendar_and_cache_hits() {
        let runner = BacktestRunner::new(db_test_pool().await);

        let days = runner
            .load_trading_days(dbd(2026, 1, 1), dbd(2026, 1, 31))
            .await
            .expect("load SSE trading calendar");
        assert!(!days.is_empty());
        assert!(
            days.windows(2).all(|pair| pair[0] < pair[1]),
            "ascending order"
        );
        assert!(days
            .iter()
            .all(|day| *day >= dbd(2026, 1, 1) && *day <= dbd(2026, 1, 31)));
        assert!(days.contains(&dbd(2026, 1, 5)), "Monday 2026-01-05 is open");
        assert!(
            !days.contains(&dbd(2026, 1, 3)),
            "Saturday 2026-01-03 is closed"
        );

        // cached 版本：首调用 miss，二次调用命中内存缓存。
        let mut cache = BacktestDataCache::default();
        let first = runner
            .load_trading_days_cached(&mut cache, dbd(2026, 1, 1), dbd(2026, 1, 31))
            .await
            .expect("first cached load from DB");
        let mid = cache.stats();
        assert_eq!(mid.trading_day_misses, 1);
        assert_eq!(first, days);

        let second = runner
            .load_trading_days_cached(&mut cache, dbd(2026, 1, 1), dbd(2026, 1, 31))
            .await
            .expect("second cached load from memory");
        let after = cache.stats();
        assert_eq!(after.trading_day_hits, mid.trading_day_hits + 1);
        assert_eq!(after.trading_day_misses, mid.trading_day_misses);
        assert_eq!(second, days);
    }

    /// 数据依据（psql 2026-09-20 核实）：
    /// `SELECT symbol, MIN(trade_date), MAX(trade_date), COUNT(*) FROM market_index_daily_bar
    ///  WHERE symbol='000300.SH' GROUP BY 1` → 2009-01-05 ~ 2026-09-18 / 4,304 行。
    #[tokio::test]
    async fn db_load_benchmark_data_returns_real_index_bars_and_cache_hits() {
        let runner = BacktestRunner::new(db_test_pool().await);

        let data = runner
            .load_benchmark_data("000300.SH", dbd(2026, 9, 1), dbd(2026, 9, 18))
            .await
            .expect("load real benchmark bars");
        assert!(!data.is_empty());
        assert!(data
            .keys()
            .all(|day| *day >= dbd(2026, 9, 1) && *day <= dbd(2026, 9, 18)));
        assert!(data.contains_key(&dbd(2026, 9, 18)));
        for (close, pre_close) in data.values() {
            assert!(*close > Decimal::zero());
            assert!(*pre_close > Decimal::zero());
        }

        // cached 版本：首调用 miss，二次调用命中内存缓存。
        let mut cache = BacktestDataCache::default();
        let first = runner
            .load_benchmark_data_cached(&mut cache, "000300.SH", dbd(2026, 9, 1), dbd(2026, 9, 18))
            .await
            .expect("first cached benchmark load from DB");
        let mid = cache.stats();
        assert_eq!(mid.benchmark_data_misses, 1);
        assert_eq!(first.len(), data.len());

        let second = runner
            .load_benchmark_data_cached(&mut cache, "000300.SH", dbd(2026, 9, 1), dbd(2026, 9, 18))
            .await
            .expect("second cached benchmark load from memory");
        let after = cache.stats();
        assert_eq!(after.benchmark_data_hits, mid.benchmark_data_hits + 1);
        assert_eq!(after.benchmark_data_misses, mid.benchmark_data_misses);
        assert_eq!(second.len(), data.len());
    }

    /// 数据依据（psql 2026-09-20 核实）：
    /// `SELECT symbol, MIN(trade_date), MAX(trade_date), COUNT(*) FROM market_stock_daily_bar_adj
    ///  WHERE symbol IN ('601857.SH','000001.SZ') GROUP BY 1`
    /// → 两 symbol 均覆盖 2006-01-04 ~ 2026-09-18（601857.SH 4,772 行）。
    #[tokio::test]
    async fn db_load_daily_bars_returns_real_stock_bars_and_cache_hits() {
        let runner = BacktestRunner::new(db_test_pool().await);
        let symbols = vec!["601857.SH".to_string(), "000001.SZ".to_string()];

        let bars = runner
            .load_daily_bars(
                "zzz_test_runner_dv",
                &symbols,
                dbd(2026, 9, 1),
                dbd(2026, 9, 18),
            )
            .await
            .expect("load real stock daily bars");
        assert!(!bars.is_empty());
        let last_day = bars
            .keys()
            .max()
            .expect("daily bars must span at least one day");
        assert_eq!(*last_day, dbd(2026, 9, 18));
        let last_day_bars = &bars[last_day];
        assert!(last_day_bars.contains_key("601857.SH"));
        assert!(last_day_bars.contains_key("000001.SZ"));
        for (open, close, pre_close, amount) in last_day_bars.values() {
            assert!(*open > Decimal::zero());
            assert!(*close > Decimal::zero());
            assert!(*pre_close > Decimal::zero());
            assert!(*amount >= Decimal::zero());
        }

        // cached 版本：data_version_id 仅作内存缓存键（无 DB 写），首调用 2 symbol miss，
        // 二次调用 2 symbol 命中。
        let mut cache = BacktestDataCache::default();
        let first = runner
            .load_daily_bars_cached(
                &mut cache,
                "zzz_test_runner_dv",
                &symbols,
                dbd(2026, 9, 1),
                dbd(2026, 9, 18),
            )
            .await
            .expect("first cached daily bars load from DB");
        let mid = cache.stats();
        assert_eq!(mid.daily_bar_symbol_misses, 2);
        assert_eq!(first.len(), bars.len());

        let second = runner
            .load_daily_bars_cached(
                &mut cache,
                "zzz_test_runner_dv",
                &symbols,
                dbd(2026, 9, 1),
                dbd(2026, 9, 18),
            )
            .await
            .expect("second cached daily bars load from memory");
        let after = cache.stats();
        assert_eq!(after.daily_bar_symbol_hits, mid.daily_bar_symbol_hits + 2);
        assert_eq!(after.daily_bar_symbol_misses, mid.daily_bar_symbol_misses);
        assert_eq!(second.len(), bars.len());
    }

    /// 数据依据（psql 2026-09-20 核实）：
    /// `SELECT symbol, exchange, market, is_st FROM market_stock
    ///  WHERE symbol IN ('601857.SH','000001.SZ')`
    /// → 601857.SH: SSE/主板/非ST；000001.SZ: SZSE/主板/非ST。
    #[tokio::test]
    async fn db_load_trading_profiles_returns_real_profiles_and_cache_hits() {
        let runner = BacktestRunner::new(db_test_pool().await);
        let symbols = vec!["601857.SH".to_string(), "000001.SZ".to_string()];

        let profiles = runner
            .load_trading_profiles(&symbols)
            .await
            .expect("load real trading profiles");
        let maotai = profiles
            .get("601857.SH")
            .expect("601857.SH trading profile");
        assert_eq!(maotai.exchange.as_deref(), Some("SSE"));
        assert_eq!(maotai.market.as_deref(), Some("主板"));
        assert!(!maotai.is_st);
        let pingan = profiles
            .get("000001.SZ")
            .expect("000001.SZ trading profile");
        assert_eq!(pingan.exchange.as_deref(), Some("SZSE"));
        assert!(!pingan.is_st);

        // cached 版本：首调用 2 symbol miss，二次调用 2 symbol 命中。
        let mut cache = BacktestDataCache::default();
        let first = runner
            .load_trading_profiles_cached(&mut cache, &symbols)
            .await
            .expect("first cached trading profiles load from DB");
        let mid = cache.stats();
        assert_eq!(mid.trading_profile_symbol_misses, 2);
        assert_eq!(first.len(), 2);

        let second = runner
            .load_trading_profiles_cached(&mut cache, &symbols)
            .await
            .expect("second cached trading profiles load from memory");
        let after = cache.stats();
        assert_eq!(
            after.trading_profile_symbol_hits,
            mid.trading_profile_symbol_hits + 2
        );
        assert_eq!(
            after.trading_profile_symbol_misses,
            mid.trading_profile_symbol_misses
        );
        assert_eq!(second.len(), 2);
    }

    // ─────────────────────────────────────────────────────────────────
    // DB 全流程连库（BacktestRunner::run / run_with_cache 端到端落库）
    //
    // run 的 signals 参数由调用方注入（不依赖因子数据链），手工构造信号即可
    // 跑通「建任务 → 加载行情 → 引擎回测 → 九表落库」全流程。
    // 写路径一律 zzz_test_runner* 前缀 task_id + 前置/结尾九表清理。
    //
    // 数据依据（psql 2026-09-20 核实）：
    // - `SELECT data_version_id FROM market_stock_daily_bar ORDER BY trade_date DESC LIMIT 1`
    //   → dv-t1-20260918（EOD 最新批次）；
    // - market_trade_calendar SSE 2026-08-03 ~ 2026-09-11 共 30 个交易日
    //   （08-03/08-04/09-07/09-08 均 is_open）；
    // - market_stock_daily_bar_adj 两 symbol 各 30 行；market_index_daily_bar
    //   000300.SH 30 行（窗口内逐日齐全）。
    // ─────────────────────────────────────────────────────────────────

    /// 清理 zzz_test_runner 前缀 task 的全部回测落库行（九表，前置+结尾双清）。
    async fn cleanup_zzz_test_backtest_rows(pool: &PgPool, task_id: &str) {
        for table in [
            "backtest_task",
            "backtest_result",
            "backtest_equity_curve",
            "backtest_trade",
            "portfolio_target",
            "backtest_position",
            "portfolio_exposure",
            "portfolio_attribution",
            "portfolio_constraint_violation",
        ] {
            sqlx::query(&format!("DELETE FROM {table} WHERE task_id = $1"))
                .bind(task_id)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("cleanup zzz_test_runner rows in {table}: {e}"));
        }
    }

    /// 统计指定表内某 task_id 的落库行数。
    async fn db_task_row_count(pool: &PgPool, table: &str, task_id: &str) -> i64 {
        let (count,): (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {table} WHERE task_id = $1"))
                .bind(task_id)
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("count {table} rows for {task_id}: {e}"));
        count
    }

    /// 全流程主测试：真实行情 + 手工信号跑 run()，断言引擎输出与九表落库。
    ///
    /// config 要点：601857.SH/000001.SZ 两只主板股、30 个交易日窗口
    /// （2026-08-03 ~ 2026-09-11）、dv-t1-20260918、基准 000300.SH、
    /// 初始资金 100 万、Standard + Full 持久化（明细表全写）。
    /// 信号设计：08-03（周一）等权建仓 → 08-04 开盘执行；
    /// 09-07（周一）调权 60/40 → 09-08 开盘执行（NextOpen 时序）。
    #[tokio::test]
    async fn db_run_full_flow_persists_all_detail_tables() {
        let _db_run_guard = DB_RUN_TEST_LOCK.lock().await;
        let pool = db_test_pool().await;
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_full_flow").await;

        let task_id = "zzz_test_runner_full_flow";
        let config = BacktestConfig {
            initial_capital: Decimal::new(1_000_000, 0),
            benchmark: "000300.SH".into(),
            start_date: dbd(2026, 8, 3),
            end_date: dbd(2026, 9, 11),
            // strategy_version_id 受 backtest_task FK 约束（fk_backtest_task_strategy_version），
            // 默认值 "debug-strategy" 不在 strategy_version 表中，必须用真实行。
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "dv-t1-20260918".into(),
            symbols: vec!["601857.SH".into(), "000001.SZ".into()],
            ..Default::default()
        };
        let signals = HashMap::from([
            (
                dbd(2026, 8, 3),
                StrategySignal {
                    date: dbd(2026, 8, 3),
                    target_weights: HashMap::from([
                        ("601857.SH".to_string(), Decimal::new(50, 2)),
                        ("000001.SZ".to_string(), Decimal::new(50, 2)),
                    ]),
                },
            ),
            (
                dbd(2026, 9, 7),
                StrategySignal {
                    date: dbd(2026, 9, 7),
                    target_weights: HashMap::from([
                        ("601857.SH".to_string(), Decimal::new(60, 2)),
                        ("000001.SZ".to_string(), Decimal::new(40, 2)),
                    ]),
                },
            ),
        ]);

        let runner = BacktestRunner::new(pool.clone());
        let output = runner
            .run(task_id, config, &signals)
            .await
            .expect("run full backtest against real market data");

        // ── 引擎输出断言 ──
        // 30 个交易日逐日一个净值点；首日（08-03，信号次日才执行）纯现金 = 初始资金。
        assert_eq!(output.equity_curve.len(), 30, "每日一个净值点");
        assert_eq!(
            output.equity_curve.first().map(|(d, _)| *d),
            Some(dbd(2026, 8, 3)),
            "净值曲线首日 = 窗口首个交易日"
        );
        assert_eq!(
            output.equity_curve.last().map(|(d, _)| *d),
            Some(dbd(2026, 9, 11)),
            "净值曲线末日 = 窗口最后一个交易日"
        );
        assert_eq!(
            output.equity_curve.first().expect("first equity point").1,
            Decimal::new(1_000_000, 0),
            "首日无持仓无成交，权益应等于初始资金"
        );
        assert!(
            output.equity_curve.last().expect("last equity point").1 > Decimal::ZERO,
            "期末权益必须为正"
        );

        // 成交与信号对应：NextOpen 时序下信号日 t 的成交落在 t+1 交易日。
        assert!(!output.trades.is_empty(), "两次调仓必产生成交");
        assert_eq!(
            output.metrics.num_trades,
            output.trades.len(),
            "metrics 成交数与明细一致"
        );
        let trade_days: HashSet<NaiveDate> = output.trades.iter().map(|t| t.trade_date).collect();
        assert!(
            trade_days.contains(&dbd(2026, 8, 4)),
            "08-03 信号应在 08-04 开盘执行，实际成交日 {trade_days:?}"
        );
        assert!(
            trade_days.contains(&dbd(2026, 9, 8)),
            "09-07 信号应在 09-08 开盘执行，实际成交日 {trade_days:?}"
        );
        let first_day_buys: Vec<_> = output
            .trades
            .iter()
            .filter(|t| t.trade_date == dbd(2026, 8, 4))
            .collect();
        assert!(!first_day_buys.is_empty(), "建仓日必有成交");
        assert!(
            first_day_buys.iter().all(|t| t.quantity > Decimal::ZERO),
            "建仓成交数量必须为正"
        );

        // 持仓明细覆盖两只股票，数量非负。
        let held: HashSet<&String> = output.daily_positions.iter().map(|p| &p.symbol).collect();
        for symbol in ["601857.SH", "000001.SZ"] {
            assert!(
                held.contains(&symbol.to_string()),
                "{symbol} 应出现在持仓明细中"
            );
        }
        assert!(output
            .daily_positions
            .iter()
            .all(|p| p.quantity >= Decimal::ZERO));

        // ── 九表落库断言 ──
        let (status, mode, progress): (String, String, String) = sqlx::query_as(
            "SELECT status::text, mode::text, progress::text FROM backtest_task WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("backtest_task row must exist after run");
        assert_eq!(status, "completed", "run 结束后任务应标记 completed");
        assert_eq!(mode, "standard");
        assert_eq!(progress, "100");

        assert_eq!(
            db_task_row_count(&pool, "backtest_result", task_id).await,
            1,
            "唯一一行汇总结果"
        );
        assert_eq!(
            db_task_row_count(&pool, "backtest_equity_curve", task_id).await,
            30,
            "净值曲线逐日落库"
        );
        assert_eq!(
            db_task_row_count(&pool, "backtest_trade", task_id).await as usize,
            output.trades.len(),
            "成交逐笔落库，行数与引擎输出一致"
        );
        let (off_schedule,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM backtest_trade \
             WHERE task_id = $1 AND trade_time::date NOT IN ('2026-08-04', '2026-09-08')",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("check trade dates");
        assert_eq!(off_schedule, 0, "成交只应发生在两个计划执行日");

        for table in [
            "portfolio_target",
            "backtest_position",
            "portfolio_exposure",
            "portfolio_attribution",
        ] {
            assert!(
                db_task_row_count(&pool, table, task_id).await > 0,
                "Standard+Full 模式下 {table} 必须有明细行"
            );
        }

        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_full_flow").await;
    }

    /// Err 路径：同一 task_id 二次 run，create_task 裸 INSERT 主键冲突必须报错。
    /// （run 内部无显式 config 校验分支，数据库唯一约束回传是实际的 Err 路径。）
    #[tokio::test]
    async fn db_run_duplicate_task_id_returns_error() {
        let _db_run_guard = DB_RUN_TEST_LOCK.lock().await;
        let pool = db_test_pool().await;
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_dup").await;

        let task_id = "zzz_test_runner_dup";
        // 短窗口（5 个交易日）+ 单信号，降低测试成本。
        let config = BacktestConfig {
            initial_capital: Decimal::new(1_000_000, 0),
            benchmark: "000300.SH".into(),
            start_date: dbd(2026, 9, 7),
            end_date: dbd(2026, 9, 11),
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "dv-t1-20260918".into(),
            symbols: vec!["601857.SH".into()],
            ..Default::default()
        };
        let signals = HashMap::from([(
            dbd(2026, 9, 7),
            StrategySignal {
                date: dbd(2026, 9, 7),
                target_weights: HashMap::from([("601857.SH".to_string(), Decimal::ONE)]),
            },
        )]);

        let runner = BacktestRunner::new(pool.clone());
        runner
            .run(task_id, config.clone(), &signals)
            .await
            .expect("first run must succeed");

        let err = runner
            .run(task_id, config, &signals)
            .await
            .expect_err("duplicate task_id must fail on create_task insert");
        assert!(
            err.to_string().to_lowercase().contains("duplicate"),
            "应为唯一约束冲突错误，实际: {err}"
        );

        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_cache_a").await;
    }

    /// run_with_cache 变体：批量 trial 场景下复用调用方缓存——
    /// 第二次 run 同窗口/同 dv/同 symbols 时市场数据全部命中内存缓存，
    /// 且两次回测结果完全一致。
    #[tokio::test]
    async fn db_run_with_cache_reuses_market_data_across_trials() {
        let _db_run_guard = DB_RUN_TEST_LOCK.lock().await;
        let pool = db_test_pool().await;
        // 前置清理 a+b：若上一轮 panic 跳过结尾清理，cache_b 残留行会与本轮互扰
        // （12:40 轮实证残留→12:57 轮 FK 偶发的唯一环境差异）。
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_cache_a").await;
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_cache_b").await;

        let config = BacktestConfig {
            initial_capital: Decimal::new(1_000_000, 0),
            benchmark: "000300.SH".into(),
            start_date: dbd(2026, 8, 3),
            end_date: dbd(2026, 9, 11),
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "dv-t1-20260918".into(),
            symbols: vec!["601857.SH".into(), "000001.SZ".into()],
            ..Default::default()
        };
        let signals = HashMap::from([(
            dbd(2026, 8, 3),
            StrategySignal {
                date: dbd(2026, 8, 3),
                target_weights: HashMap::from([
                    ("601857.SH".to_string(), Decimal::new(50, 2)),
                    ("000001.SZ".to_string(), Decimal::new(50, 2)),
                ]),
            },
        )]);

        let runner = BacktestRunner::new(pool.clone());
        let mut cache = BacktestDataCache::default();

        // 第一遍（task A）：空缓存起步，四类市场数据各一次 miss。
        let out_a = runner
            .run_with_cache(
                "zzz_test_runner_cache_a",
                config.clone(),
                &signals,
                Some(&mut cache),
            )
            .await
            .expect("first trial with cold cache");
        let after_a = cache.stats();
        assert_eq!(after_a.trading_day_misses, 1, "交易日历一次 miss");
        assert_eq!(after_a.benchmark_data_misses, 1, "基准行情一次 miss");
        assert_eq!(
            after_a.daily_bar_symbol_misses, 2,
            "两只股票日线各一次 miss"
        );
        assert_eq!(
            after_a.trading_profile_symbol_misses, 2,
            "两只股票交易画像各一次 miss"
        );

        // 第二遍（task B）：同参数换 task_id，四类数据全部命中，不再触发 DB miss。
        let out_b = runner
            .run_with_cache(
                "zzz_test_runner_cache_b",
                config,
                &signals,
                Some(&mut cache),
            )
            .await
            .expect("second trial with warm cache");
        let after_b = cache.stats();
        assert_eq!(after_b.trading_day_hits, after_a.trading_day_hits + 1);
        assert_eq!(after_b.trading_day_misses, after_a.trading_day_misses);
        assert_eq!(after_b.benchmark_data_hits, after_a.benchmark_data_hits + 1);
        assert_eq!(after_b.benchmark_data_misses, after_a.benchmark_data_misses);
        assert_eq!(
            after_b.daily_bar_symbol_hits,
            after_a.daily_bar_symbol_hits + 2
        );
        assert_eq!(
            after_b.daily_bar_symbol_misses,
            after_a.daily_bar_symbol_misses
        );
        assert_eq!(
            after_b.trading_profile_symbol_hits,
            after_a.trading_profile_symbol_hits + 2
        );
        assert_eq!(
            after_b.trading_profile_symbol_misses,
            after_a.trading_profile_symbol_misses
        );

        // 同数据 + 同信号 + 同引擎 → 两次 trial 结果完全一致（回测可复现性）。
        assert_eq!(
            out_b.equity_curve, out_a.equity_curve,
            "缓存复用不得改变回测结果"
        );
        assert_eq!(out_b.metrics.num_trades, out_a.metrics.num_trades);

        // 两个 task 均完整落库（缓存只影响读取路径，不影响持久化）。
        for task_id in ["zzz_test_runner_cache_a", "zzz_test_runner_cache_b"] {
            let (status,): (String,) =
                sqlx::query_as("SELECT status::text FROM backtest_task WHERE task_id = $1")
                    .bind(task_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or_else(|e| panic!("task {task_id} missing: {e}"));
            assert_eq!(status, "completed", "task {task_id} 应正常完成");
            assert_eq!(
                db_task_row_count(&pool, "backtest_equity_curve", task_id).await,
                30,
                "task {task_id} 净值曲线应完整落库"
            );
        }

        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_cache_a").await;
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_cache_b").await;
    }

    // ─────────────────────────────────────────────────────────────────
    // 第六批覆盖率收尾补缺（ETF 涨跌幅家族 / prewarm 深路径 / 空信号现金曲线）
    // ─────────────────────────────────────────────────────────────────

    /// limit_rate_for 的 ETF 家族分支：跨境 QDII(513xxx) ±20%、
    /// 货币基金(511880/511990) 不设限(1.0)、普通 ETF ±10%。
    /// profile=None 走 symbol 代码段判 ETF；instrument_type="etf" 走权威字段。
    #[test]
    fn limit_rate_covers_etf_qdii_money_fund_and_plain_families() {
        let etf = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: false,
            instrument_type: Some("etf".into()),
        };

        // 跨境 QDII：513100 → 20%
        assert_eq!(
            BacktestRunner::limit_rate_for("513100.SH", None),
            Decimal::new(20, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("513100.SH", Some(&etf)),
            Decimal::new(20, 2)
        );
        // 货币基金：511880/511990 → 1.0（无涨跌幅限制）
        for code in ["511880.SH", "511990.SH"] {
            assert_eq!(
                BacktestRunner::limit_rate_for(code, None),
                Decimal::ONE,
                "{code} 货币基金不应设涨跌幅限制"
            );
        }
        // 普通 ETF：510300 → 10%（代码段与 instrument_type 双路一致）
        assert_eq!(
            BacktestRunner::limit_rate_for("510300.SH", None),
            Decimal::new(10, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("510300.SH", Some(&etf)),
            Decimal::new(10, 2)
        );
    }

    /// prewarm_market_data_cache 连库主路径：真实 dv/benchmark/symbol 窗口
    /// 预热交易日历、基准与日线到共享缓存并物化 snapshot；
    /// 二次预热全部命中缓存（无新增 DB miss）。
    #[tokio::test]
    async fn db_prewarm_market_data_cache_materializes_snapshot_from_real_bars() {
        let pool = db_test_pool().await;
        let runner = BacktestRunner::new(pool.clone());
        let mut cache = BacktestDataCache::default();
        let symbols = vec!["600519.SH".to_string(), "000001.SZ".to_string()];

        let report = runner
            .prewarm_market_data_cache(
                &mut cache,
                "dv-t1-20260918",
                "000300.SH",
                &symbols,
                NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 9, 11).unwrap(),
            )
            .await
            .expect("prewarm market data cache from real bars");

        assert_eq!(report.symbol_count, 2);
        assert_eq!(report.benchmark, "000300.SH");
        assert_eq!(
            report.start_date,
            NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()
        );
        assert_eq!(
            report.end_date,
            NaiveDate::from_ymd_opt(2026, 9, 11).unwrap()
        );
        // 首次预热必须产生日线 miss（从 DB 拉取）
        assert!(report.cache_delta.daily_bar_symbol_misses >= 2);

        // 二次预热：全部命中内存缓存（daily bar 无新 miss）
        let second = runner
            .prewarm_market_data_cache(
                &mut cache,
                "dv-t1-20260918",
                "000300.SH",
                &symbols,
                NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 9, 11).unwrap(),
            )
            .await
            .expect("second prewarm fully served by cache");
        assert_eq!(
            second.cache_delta.daily_bar_symbol_misses, 0,
            "二次预热不应再触发日线 DB miss"
        );
        assert!(second.cache_delta.daily_bar_symbol_hits >= 2);
    }

    /// run_with_cache 的空信号分支：无持仓信号时按现金曲线完成回测，
    /// 落库后 equity 点数 = 窗口交易日数且零成交。
    #[tokio::test]
    async fn db_run_with_cache_empty_signals_finishes_as_cash_curve() {
        let _db_run_guard = DB_RUN_TEST_LOCK.lock().await;
        let pool = db_test_pool().await;
        cleanup_zzz_test_backtest_rows(&pool, "zzz_test_runner_empty_signals").await;

        let task_id = "zzz_test_runner_empty_signals";
        let config = BacktestConfig {
            initial_capital: Decimal::new(1_000_000, 0),
            benchmark: "000300.SH".into(),
            start_date: NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 9, 4).unwrap(),
            data_version_id: "dv-t1-20260918".into(),
            // FK 坑（第五批已证）：默认 debug-strategy 不在 strategy_version 表，必拒。
            strategy_version_id: "factor-combo-v1".into(),
            ..Default::default()
        };
        let runner = BacktestRunner::new(pool.clone());
        let mut cache = BacktestDataCache::default();
        let empty_signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();

        let output = runner
            .run_with_cache(task_id, config, &empty_signals, Some(&mut cache))
            .await
            .expect("empty-signal run must finish on the cash curve");

        assert!(output.trades.is_empty(), "无信号不应有成交");
        assert_eq!(
            output.equity_curve.len(),
            4,
            "2026-09-01 ~ 09-04 共 4 个交易日，现金曲线逐日一格"
        );
        assert_eq!(
            db_task_row_count(&pool, "backtest_task", task_id).await,
            1,
            "任务行必须落库"
        );

        cleanup_zzz_test_backtest_rows(&pool, task_id).await;
    }
}
