//! Database-backed Phase 2 baseline using `BacktestRunner`.

use crate::engine::{BacktestConfig, BacktestMode, StrategySignal};
use crate::runner::BacktestRunner;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DbPerfBaselineConfig {
    pub database_url: String,
    pub trading_days: usize,
    pub symbols: usize,
    pub rebalance_every_n_days: usize,
    pub basket_size: usize,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub benchmark: String,
    pub task_prefix: String,
}

impl Default for DbPerfBaselineConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://gaocheng@localhost/quant".to_string(),
            trading_days: 40,
            symbols: 25,
            rebalance_every_n_days: 10,
            basket_size: 8,
            start_date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            benchmark: "000300.SH".to_string(),
            task_prefix: "perf-db-smoke".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPerfBaselineReport {
    pub scenario: String,
    pub task_id: String,
    pub database_url: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub requested_trading_days: usize,
    pub actual_trading_days: usize,
    pub requested_symbols: usize,
    pub actual_symbols: usize,
    pub rebalance_every_n_days: usize,
    pub basket_size: usize,
    pub signal_count: usize,
    pub equity_points: usize,
    pub benchmark_points: usize,
    pub trade_count: usize,
    pub daily_position_count: usize,
    pub target_count: usize,
    pub exposure_count: usize,
    pub attribution_count: usize,
    pub violation_count: usize,
    pub elapsed_ms: u128,
    pub total_return: Decimal,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
}

pub fn parse_db_perf_args<I, S>(args: I) -> Result<DbPerfBaselineConfig, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut config = DbPerfBaselineConfig::default();
    let mut iter = args.into_iter();
    let _program = iter.next();

    while let Some(arg) = iter.next() {
        let arg = arg.as_ref();
        match arg {
            "--database-url" => config.database_url = parse_next_string(&mut iter, arg)?,
            "--days" => config.trading_days = parse_next_usize(&mut iter, arg)?,
            "--symbols" => config.symbols = parse_next_usize(&mut iter, arg)?,
            "--rebalance-days" => config.rebalance_every_n_days = parse_next_usize(&mut iter, arg)?,
            "--basket-size" => config.basket_size = parse_next_usize(&mut iter, arg)?,
            "--start-date" => config.start_date = parse_next_date(&mut iter, arg)?,
            "--end-date" => config.end_date = parse_next_date(&mut iter, arg)?,
            "--benchmark" => config.benchmark = parse_next_string(&mut iter, arg)?,
            "--task-prefix" => config.task_prefix = parse_next_string(&mut iter, arg)?,
            "--help" | "-h" => return Err(db_perf_usage()),
            unknown => return Err(format!("unknown argument `{}`\n{}", unknown, db_perf_usage())),
        }
    }

    Ok(normalize_config(config))
}

pub async fn run_db_perf_baseline(
    config: DbPerfBaselineConfig,
) -> Result<DbPerfBaselineReport, Box<dyn std::error::Error>> {
    let config = normalize_config(config);
    let pool = PgPool::connect(&config.database_url).await?;
    ensure_perf_metadata(&pool, &config).await?;

    let trading_days = load_trading_days(&pool, config.start_date, config.end_date).await?;
    let trading_days: Vec<NaiveDate> = trading_days.into_iter().take(config.trading_days).collect();
    if trading_days.len() < 2 {
        return Err("database baseline needs at least two trading days".into());
    }

    let end_date = *trading_days.last().expect("checked above");
    let symbols = load_candidate_symbols(
        &pool,
        config.start_date,
        end_date,
        config.trading_days.saturating_sub(1),
        config.symbols,
        &config.benchmark,
    )
    .await?;
    if symbols.is_empty() {
        return Err("database baseline found no candidate symbols".into());
    }

    let signals = generate_rebalance_signals(
        &trading_days,
        &symbols,
        config.rebalance_every_n_days,
        config.basket_size.min(symbols.len()),
    );
    let task_id = format!("{}-{}", config.task_prefix, Uuid::new_v4());
    let runner = BacktestRunner::new(pool);
    let backtest_config = BacktestConfig {
        benchmark: config.benchmark.clone(),
        start_date: config.start_date,
        end_date,
        mode: BacktestMode::Standard,
        max_position_pct: Decimal::ONE,
        strategy_version_id: perf_strategy_version_id().to_string(),
        data_version_id: perf_data_version_id().to_string(),
        symbols: symbols.clone(),
        rebalance_frequency: format!("{}d", config.rebalance_every_n_days),
        max_participation_rate: Some(Decimal::new(10, 2)),
        ..BacktestConfig::default()
    };

    let started_at = Instant::now();
    let output = runner.run(&task_id, backtest_config, &signals).await?;
    let elapsed_ms = started_at.elapsed().as_millis();

    Ok(DbPerfBaselineReport {
        scenario: "phase2_db_runner_smoke".to_string(),
        task_id,
        database_url: redact_database_url(&config.database_url),
        start_date: config.start_date,
        end_date,
        requested_trading_days: config.trading_days,
        actual_trading_days: output.equity_curve.len(),
        requested_symbols: config.symbols,
        actual_symbols: symbols.len(),
        rebalance_every_n_days: config.rebalance_every_n_days,
        basket_size: config.basket_size.min(symbols.len()),
        signal_count: signals.len(),
        equity_points: output.equity_curve.len(),
        benchmark_points: output.benchmark_curve.len(),
        trade_count: output.trades.len(),
        daily_position_count: output.daily_positions.len(),
        target_count: output.targets.len(),
        exposure_count: output.exposures.len(),
        attribution_count: output.attributions.len(),
        violation_count: output.violations.len(),
        elapsed_ms,
        total_return: output.metrics.total_return_pct,
        sharpe_ratio: output.metrics.sharpe_ratio,
        max_drawdown: output.metrics.max_drawdown_pct,
        turnover: output.metrics.turnover,
    })
}

fn normalize_config(mut config: DbPerfBaselineConfig) -> DbPerfBaselineConfig {
    config.trading_days = config.trading_days.max(2);
    config.symbols = config.symbols.max(1);
    config.rebalance_every_n_days = config.rebalance_every_n_days.max(1);
    config.basket_size = config.basket_size.max(1).min(config.symbols);
    if config.start_date > config.end_date {
        std::mem::swap(&mut config.start_date, &mut config.end_date);
    }
    config
}

async fn ensure_perf_metadata(
    pool: &PgPool,
    config: &DbPerfBaselineConfig,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO strategy_definition (strategy_code, name, strategy_type, description, status)
           VALUES ($1, 'Phase 2 DB performance smoke', 'benchmark', 'Synthetic metadata for BacktestRunner DB performance baselines', 'active')
           ON CONFLICT (strategy_code) DO UPDATE SET
             name = EXCLUDED.name,
             strategy_type = EXCLUDED.strategy_type,
             description = EXCLUDED.description,
             status = EXCLUDED.status,
             updated_at = now()"#,
    )
    .bind(perf_strategy_code())
    .execute(pool)
    .await?;

    sqlx::query(
        r#"INSERT INTO strategy_version
           (strategy_version_id, strategy_code, version, parameter_schema, default_parameters, required_factors, required_models, risk_constraints, status)
           VALUES ($1, $2, 'v1', '{}'::jsonb, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{}'::jsonb, 'active')
           ON CONFLICT (strategy_version_id) DO UPDATE SET
             parameter_schema = EXCLUDED.parameter_schema,
             default_parameters = EXCLUDED.default_parameters,
             required_factors = EXCLUDED.required_factors,
             required_models = EXCLUDED.required_models,
             risk_constraints = EXCLUDED.risk_constraints,
             status = EXCLUDED.status"#,
    )
    .bind(perf_strategy_version_id())
    .bind(perf_strategy_code())
    .execute(pool)
    .await?;

    sqlx::query(
        r#"INSERT INTO data_version
           (data_version_id, name, source, start_date, end_date, tables, snapshot_hash, metadata)
           VALUES ($1, 'Phase 2 DB performance smoke data', 'perf_baseline', $2, $3,
             ARRAY['market_trade_calendar','market_stock','market_stock_daily_bar'],
             'perf-db-smoke', $4)
           ON CONFLICT (data_version_id) DO UPDATE SET
             name = EXCLUDED.name,
             source = EXCLUDED.source,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             tables = EXCLUDED.tables,
             snapshot_hash = EXCLUDED.snapshot_hash,
             metadata = EXCLUDED.metadata"#,
    )
    .bind(perf_data_version_id())
    .bind(config.start_date)
    .bind(config.end_date)
    .bind(serde_json::json!({
        "scenario": "phase2_db_runner_smoke",
        "benchmark": config.benchmark,
        "symbols": config.symbols,
        "trading_days": config.trading_days
    }))
    .execute(pool)
    .await?;

    Ok(())
}

async fn load_trading_days(
    pool: &PgPool,
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
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(date,)| date).collect())
}

async fn load_candidate_symbols(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
    min_points: usize,
    limit: usize,
    benchmark: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT symbol FROM market_stock_daily_bar
         WHERE trade_date >= $1 AND trade_date <= $2 AND symbol <> $3
         GROUP BY symbol
         HAVING count(*) >= $4
         ORDER BY symbol
         LIMIT $5",
    )
    .bind(start)
    .bind(end)
    .bind(benchmark)
    .bind(min_points as i64)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(symbol,)| symbol).collect())
}

fn generate_rebalance_signals(
    trading_days: &[NaiveDate],
    symbols: &[String],
    rebalance_every_n_days: usize,
    basket_size: usize,
) -> HashMap<NaiveDate, StrategySignal> {
    let mut signals = HashMap::new();
    let basket_size = basket_size.max(1).min(symbols.len());
    let weight = Decimal::new(95, 2) / Decimal::from(basket_size as u64);

    for (day_idx, date) in trading_days.iter().enumerate().step_by(rebalance_every_n_days) {
        let offset = day_idx % symbols.len();
        let mut target_weights = HashMap::with_capacity(basket_size);
        for basket_idx in 0..basket_size {
            let symbol = symbols[(offset + basket_idx) % symbols.len()].clone();
            target_weights.insert(symbol, weight);
        }
        signals.insert(
            *date,
            StrategySignal {
                date: *date,
                target_weights,
            },
        );
    }

    signals
}

fn parse_next_string<I, S>(iter: &mut I, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    iter.next()
        .map(|value| value.as_ref().to_string())
        .ok_or_else(|| format!("missing value for `{}`\n{}", flag, db_perf_usage()))
}

fn parse_next_usize<I, S>(iter: &mut I, flag: &str) -> Result<usize, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let value = parse_next_string(iter, flag)?;
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value `{}` for `{}`", value, flag))
}

fn parse_next_date<I, S>(iter: &mut I, flag: &str) -> Result<NaiveDate, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let value = parse_next_string(iter, flag)?;
    NaiveDate::parse_from_str(&value, "%Y%m%d")
        .map_err(|_| format!("invalid date `{}` for `{}`; expected YYYYMMDD", value, flag))
}

pub fn db_perf_usage() -> String {
    [
        "Usage: db_perf_baseline [--database-url URL] [--days N] [--symbols N]",
        "                        [--rebalance-days N] [--basket-size N]",
        "                        [--start-date YYYYMMDD] [--end-date YYYYMMDD]",
        "                        [--benchmark SYMBOL] [--task-prefix PREFIX]",
        "",
        "Defaults: --database-url postgres://gaocheng@localhost/quant --days 40 --symbols 25 --rebalance-days 10 --basket-size 8 --start-date 20240102 --end-date 20241231",
    ]
    .join("\n")
}

fn perf_strategy_code() -> &'static str {
    "PERF_DB_SMOKE"
}

fn perf_strategy_version_id() -> &'static str {
    "perf-db-smoke-v1"
}

fn perf_data_version_id() -> &'static str {
    "perf-db-smoke-data-v1"
}

fn redact_database_url(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://") {
        if let Some((userinfo, host)) = rest.split_once('@') {
            if userinfo.contains(':') {
                return format!("{}://***@{}", scheme, host);
            }
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_swaps_reversed_date_range() {
        let config = normalize_config(DbPerfBaselineConfig {
            start_date: NaiveDate::from_ymd_opt(2024, 6, 30).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ..DbPerfBaselineConfig::default()
        });

        assert_eq!(config.start_date, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap());
        assert_eq!(config.end_date, NaiveDate::from_ymd_opt(2024, 6, 30).unwrap());
    }

    #[test]
    fn redact_database_url_hides_password() {
        assert_eq!(
            redact_database_url("postgres://user:secret@localhost/quant"),
            "postgres://***@localhost/quant"
        );
    }
}
