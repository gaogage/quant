//! 回测运行器 — 数据加载、引擎驱动、结果持久化

use chrono::NaiveDate;
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
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
}

type DailyBarsByDate = HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>>;

#[derive(Debug, Clone)]
struct DailyBarRecord {
    trade_date: NaiveDate,
    symbol: String,
    open: Decimal,
    close: Decimal,
    pre_close: Decimal,
    amount: Decimal,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BacktestDataCacheStats {
    pub trading_day_hits: usize,
    pub trading_day_misses: usize,
    pub benchmark_data_hits: usize,
    pub benchmark_data_misses: usize,
    pub daily_bar_symbol_hits: usize,
    pub daily_bar_symbol_misses: usize,
    pub trading_profile_symbol_hits: usize,
    pub trading_profile_symbol_misses: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestMarketDataPrewarmReport {
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

#[derive(Debug, Default)]
pub struct BacktestDataCache {
    trading_days: HashMap<(NaiveDate, NaiveDate), Arc<Vec<NaiveDate>>>,
    benchmark_data:
        HashMap<(String, NaiveDate, NaiveDate), Arc<HashMap<NaiveDate, (Decimal, Decimal)>>>,
    daily_bars: HashMap<(NaiveDate, NaiveDate), HashMap<String, Arc<Vec<DailyBarRecord>>>>,
    trading_profiles: HashMap<String, Arc<Option<TradingProfile>>>,
    stats: BacktestDataCacheStats,
}

#[derive(Debug, Clone, Default)]
pub struct BacktestDataCacheSnapshot {
    trading_days: HashMap<(NaiveDate, NaiveDate), Arc<Vec<NaiveDate>>>,
    benchmark_data:
        HashMap<(String, NaiveDate, NaiveDate), Arc<HashMap<NaiveDate, (Decimal, Decimal)>>>,
    daily_bars: HashMap<(NaiveDate, NaiveDate), HashMap<String, Arc<Vec<DailyBarRecord>>>>,
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
            trading_profiles: self.trading_profiles.clone(),
        }
    }

    pub fn from_snapshot(snapshot: &BacktestDataCacheSnapshot) -> Self {
        Self {
            trading_days: snapshot.trading_days.clone(),
            benchmark_data: snapshot.benchmark_data.clone(),
            daily_bars: snapshot.daily_bars.clone(),
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
                .get(&(start, end))
                .and_then(|bucket| bucket.get(&symbol))
            {
                self.stats.daily_bar_symbol_hits += 1;
                merge_daily_bar_records(&mut cached, records.as_ref());
            } else if let Some(records) =
                self.cached_daily_bar_records_from_covering_window(&symbol, start, end)
            {
                self.stats.daily_bar_symbol_hits += 1;
                merge_daily_bar_records(&mut cached, &records);
                reusable_records.push((symbol, records));
            } else {
                self.stats.daily_bar_symbol_misses += 1;
                missing.push(symbol);
            }
        }

        if !reusable_records.is_empty() {
            let bucket = self.daily_bars.entry((start, end)).or_default();
            for (symbol, records) in reusable_records {
                bucket.insert(symbol, Arc::new(records));
            }
        }

        (missing, cached)
    }

    fn cached_daily_bar_records_from_covering_window(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<Vec<DailyBarRecord>> {
        self.daily_bars
            .iter()
            .filter(|((cached_start, cached_end), bucket)| {
                *cached_start <= start && *cached_end >= end && bucket.contains_key(symbol)
            })
            .min_by_key(|((cached_start, cached_end), _)| (*cached_end - *cached_start).num_days())
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

    fn insert_daily_bar_rows(
        &mut self,
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

        let bucket = self.daily_bars.entry((start, end)).or_default();
        let mut inserted = HashMap::new();
        for symbol in requested_symbols {
            let mut records = by_symbol.remove(&symbol).unwrap_or_default();
            records.sort_by_key(|record| record.trade_date);
            merge_daily_bar_records(&mut inserted, &records);
            bucket.insert(symbol, Arc::new(records));
        }

        inserted
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
        rows: Vec<(String, Option<String>, Option<String>, Option<bool>)>,
    ) -> HashMap<String, TradingProfile> {
        let requested_symbols = normalized_symbol_key(requested_symbols);
        let mut by_symbol: HashMap<String, Option<TradingProfile>> = requested_symbols
            .iter()
            .map(|symbol| (symbol.clone(), None))
            .collect();

        for (symbol, exchange, market, is_st) in rows {
            by_symbol.insert(
                symbol,
                Some(TradingProfile {
                    exchange,
                    market,
                    is_st: is_st.unwrap_or(false),
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
        benchmark: &str,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<BacktestMarketDataPrewarmReport, sqlx::Error> {
        let symbols = normalized_symbol_key(symbols);
        let before = cache.stats();
        let _ = self.load_trading_days_cached(cache, start, end).await?;
        let _ = self
            .load_benchmark_data_cached(cache, benchmark, start, end)
            .await?;
        if !symbols.is_empty() {
            let _ = self
                .load_daily_bars_cached(cache, &symbols, start, end)
                .await?;
            let _ = self.load_trading_profiles_cached(cache, &symbols).await?;
        }
        let after = cache.stats();

        Ok(BacktestMarketDataPrewarmReport {
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
                        &all_symbols,
                        config.start_date,
                        config.end_date,
                    )
                    .await?
                }
                None => {
                    self.load_daily_bars(&all_symbols, config.start_date, config.end_date)
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
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<DailyBarsByDate, sqlx::Error> {
        let rows: Vec<(
            NaiveDate,
            String,
            Option<Decimal>,
            Decimal,
            Option<Decimal>,
            Option<Decimal>,
        )> = sqlx::query_as(
            "SELECT trade_date, symbol, open, close, pre_close, amount
             FROM market_stock_daily_bar
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date, symbol",
        )
        .bind(symbols)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        let mut result: DailyBarsByDate = HashMap::new();
        for (d, sym, o, c, pc, amount) in rows {
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
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<DailyBarsByDate, sqlx::Error> {
        let (missing_symbols, mut result) = cache.cached_daily_bar_symbols(symbols, start, end);
        if missing_symbols.is_empty() {
            return Ok(result);
        }

        let rows: Vec<(
            NaiveDate,
            String,
            Option<Decimal>,
            Decimal,
            Option<Decimal>,
            Option<Decimal>,
        )> = sqlx::query_as(
            "SELECT trade_date, symbol, open, close, pre_close, amount
             FROM market_stock_daily_bar
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
                |(trade_date, symbol, open, close, pre_close, amount)| DailyBarRecord {
                    trade_date,
                    symbol,
                    open: open.unwrap_or(close),
                    close,
                    pre_close: pre_close.unwrap_or(close),
                    amount: amount.unwrap_or_default(),
                },
            )
            .collect();
        let inserted = cache.insert_daily_bar_rows(start, end, &missing_symbols, rows);
        merge_daily_bars(&mut result, inserted);
        Ok(result)
    }

    async fn load_trading_profiles(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, TradingProfile>, sqlx::Error> {
        let rows: Vec<(String, Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
            "SELECT symbol, exchange, market, is_st FROM market_stock WHERE symbol = ANY($1)",
        )
        .bind(symbols)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(symbol, exchange, market, is_st)| {
                (
                    symbol,
                    TradingProfile {
                        exchange,
                        market,
                        is_st: is_st.unwrap_or(false),
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

        let rows: Vec<(String, Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
            "SELECT symbol, exchange, market, is_st FROM market_stock WHERE symbol = ANY($1)",
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
        daily_data: &HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>>,
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
        let parameters = json!({
            "research_dataset_id": config.research_dataset_id.as_deref(),
            "feature_set_version_id": config.feature_set_version_id.as_deref(),
            "prediction_set_id": config.prediction_set_id.as_deref(),
            "portfolio_policy_id": config.portfolio_policy_id.as_deref()
        });
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

        // Positions
        for pos in &output.daily_positions {
            sqlx::query(
                r#"INSERT INTO backtest_position (task_id, symbol, position_date,
                   quantity, available_quantity, avg_cost, close_price,
                   market_value, weight, unrealized_pnl, target_weight)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (task_id, symbol, position_date) DO NOTHING"#,
            )
            .bind(task_id)
            .bind(&pos.symbol)
            .bind(pos.date)
            .bind(pos.quantity)
            .bind(pos.available_quantity)
            .bind(pos.avg_cost)
            .bind(pos.close_price)
            .bind(pos.market_value)
            .bind(pos.weight)
            .bind(pos.unrealized_pnl)
            .bind(pos.target_weight)
            .execute(&self.pool)
            .await?;
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
        matches!(config.persistence_mode, BacktestPersistenceMode::Full)
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
        };
        let growth = TradingProfile {
            exchange: Some("SZSE".into()),
            market: Some("创业板".into()),
            is_st: false,
        };
        let main = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: false,
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
        let mut config = BacktestConfig::default();
        config.persistence_mode = BacktestPersistenceMode::SummaryOnly;

        assert!(!BacktestRunner::should_persist_detail_tables(&config));
    }

    #[test]
    fn backtest_data_cache_reuses_overlapping_daily_bar_symbols() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let mut cache = BacktestDataCache::default();

        let first_symbols = vec!["000002.SZ".to_string(), "000001.SZ".to_string()];
        let (missing_first, cached_first) =
            cache.cached_daily_bar_symbols(&first_symbols, start, end);
        assert_eq!(missing_first, vec!["000001.SZ", "000002.SZ"]);
        assert!(cached_first.is_empty());

        cache.insert_daily_bar_rows(
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
                },
                DailyBarRecord {
                    trade_date: start,
                    symbol: "000002.SZ".into(),
                    open: Decimal::new(201, 1),
                    close: Decimal::new(202, 1),
                    pre_close: Decimal::new(200, 1),
                    amount: Decimal::new(2000, 0),
                },
            ],
        );

        let second_symbols = vec!["000002.SZ".to_string(), "000003.SZ".to_string()];
        let (missing_second, cached_second) =
            cache.cached_daily_bar_symbols(&second_symbols, start, end);

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
        let wide_start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let narrow_start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let narrow_end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let wide_end = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let mut cache = BacktestDataCache::default();

        let symbols = vec!["000001.SZ".to_string()];
        cache.insert_daily_bar_rows(
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
                },
                DailyBarRecord {
                    trade_date: narrow_start,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(111, 1),
                    close: Decimal::new(112, 1),
                    pre_close: Decimal::new(110, 1),
                    amount: Decimal::new(1100, 0),
                },
                DailyBarRecord {
                    trade_date: narrow_end,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(121, 1),
                    close: Decimal::new(122, 1),
                    pre_close: Decimal::new(120, 1),
                    amount: Decimal::new(1200, 0),
                },
                DailyBarRecord {
                    trade_date: wide_end,
                    symbol: "000001.SZ".into(),
                    open: Decimal::new(131, 1),
                    close: Decimal::new(132, 1),
                    pre_close: Decimal::new(130, 1),
                    amount: Decimal::new(1300, 0),
                },
            ],
        );

        let (missing, cached) = cache.cached_daily_bar_symbols(&symbols, narrow_start, narrow_end);

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
}
