//! Deterministic in-memory workloads for Phase 2 backtest performance baselines.

use crate::engine::{BacktestConfig, BacktestEngine, BacktestMode, MarketDay, StrategySignal};
use crate::runner::schedule_signals_for_execution;
use chrono::{Datelike, Duration, NaiveDate, Weekday};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase2PerfBaselineConfig {
    pub trading_days: usize,
    pub symbols: usize,
    pub rebalance_every_n_days: usize,
    pub basket_size: usize,
    pub initial_capital: Decimal,
    pub mode: BacktestMode,
    pub start_date: NaiveDate,
}

impl Default for Phase2PerfBaselineConfig {
    fn default() -> Self {
        Self {
            trading_days: 252,
            symbols: 300,
            rebalance_every_n_days: 20,
            basket_size: 50,
            initial_capital: Decimal::new(100_000_000, 0),
            mode: BacktestMode::Standard,
            start_date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase2PerfBaselineReport {
    pub scenario: String,
    pub trading_days: usize,
    pub symbols: usize,
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

pub fn parse_phase2_perf_args<I, S>(args: I) -> Result<Phase2PerfBaselineConfig, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut config = Phase2PerfBaselineConfig::default();
    let mut iter = args.into_iter();
    let _program = iter.next();

    while let Some(arg) = iter.next() {
        let arg = arg.as_ref();
        match arg {
            "--days" => config.trading_days = parse_next_usize(&mut iter, arg)?,
            "--symbols" => config.symbols = parse_next_usize(&mut iter, arg)?,
            "--rebalance-days" => config.rebalance_every_n_days = parse_next_usize(&mut iter, arg)?,
            "--basket-size" => config.basket_size = parse_next_usize(&mut iter, arg)?,
            "--help" | "-h" => return Err(phase2_perf_usage()),
            unknown => {
                return Err(format!(
                    "unknown argument `{}`\n{}",
                    unknown,
                    phase2_perf_usage()
                ))
            }
        }
    }

    Ok(normalize_config(config))
}

pub fn run_phase2_smoke_baseline(config: Phase2PerfBaselineConfig) -> Phase2PerfBaselineReport {
    let config = normalize_config(config);
    let trading_days = generate_trading_days(config.start_date, config.trading_days);
    let symbols = generate_symbols(config.symbols);
    let signals = generate_rebalance_signals(
        &trading_days,
        &symbols,
        config.rebalance_every_n_days,
        config.basket_size,
    );
    let execution_signals = schedule_signals_for_execution(&trading_days, &signals);
    let mut engine = BacktestEngine::new(backtest_config(&config, &trading_days, &symbols));

    let started_at = Instant::now();
    for (idx, date) in trading_days.iter().enumerate() {
        let market = synthetic_market_day(*date, idx, &symbols);
        engine.process_day(&market, execution_signals.get(date));
    }
    let elapsed_ms = started_at.elapsed().as_millis();
    let output = engine.finalize();

    Phase2PerfBaselineReport {
        scenario: "phase2_smoke_in_memory".to_string(),
        trading_days: config.trading_days,
        symbols: config.symbols,
        rebalance_every_n_days: config.rebalance_every_n_days,
        basket_size: config.basket_size,
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
    }
}

fn parse_next_usize<I, S>(iter: &mut I, flag: &str) -> Result<usize, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let value = iter
        .next()
        .ok_or_else(|| format!("missing value for `{}`\n{}", flag, phase2_perf_usage()))?;
    value
        .as_ref()
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value `{}` for `{}`", value.as_ref(), flag))
}

pub fn phase2_perf_usage() -> String {
    [
        "Usage: phase2_perf_baseline [--days N] [--symbols N] [--rebalance-days N] [--basket-size N]",
        "",
        "Defaults: --days 252 --symbols 300 --rebalance-days 20 --basket-size 50",
    ]
    .join("\n")
}

fn normalize_config(mut config: Phase2PerfBaselineConfig) -> Phase2PerfBaselineConfig {
    config.trading_days = config.trading_days.max(2);
    config.symbols = config.symbols.max(1);
    config.rebalance_every_n_days = config.rebalance_every_n_days.max(1);
    config.basket_size = config.basket_size.max(1).min(config.symbols);
    config
}

fn backtest_config(
    config: &Phase2PerfBaselineConfig,
    trading_days: &[NaiveDate],
    symbols: &[String],
) -> BacktestConfig {
    BacktestConfig {
        initial_capital: config.initial_capital,
        benchmark: "000300.SH".to_string(),
        start_date: *trading_days.first().expect("normalized config has days"),
        end_date: *trading_days.last().expect("normalized config has days"),
        mode: config.mode,
        max_position_pct: Decimal::ONE,
        strategy_version_id: "phase2-perf-smoke-v1".to_string(),
        data_version_id: "phase2-perf-smoke-data-v1".to_string(),
        symbols: symbols.to_vec(),
        rebalance_frequency: format!("{}d", config.rebalance_every_n_days),
        max_participation_rate: Some(Decimal::new(10, 2)),
        ..BacktestConfig::default()
    }
}

fn generate_trading_days(start: NaiveDate, count: usize) -> Vec<NaiveDate> {
    let mut days = Vec::with_capacity(count);
    let mut date = start;
    while days.len() < count {
        if !matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            days.push(date);
        }
        date += Duration::days(1);
    }
    days
}

fn generate_symbols(count: usize) -> Vec<String> {
    (0..count)
        .map(|idx| {
            if idx % 2 == 0 {
                format!("{:06}.SZ", idx + 1)
            } else {
                format!("{:06}.SH", idx + 1)
            }
        })
        .collect()
}

fn generate_rebalance_signals(
    trading_days: &[NaiveDate],
    symbols: &[String],
    rebalance_every_n_days: usize,
    basket_size: usize,
) -> HashMap<NaiveDate, StrategySignal> {
    let mut signals = HashMap::new();
    let weight = Decimal::new(95, 2) / Decimal::from(basket_size as u64);

    for (day_idx, date) in trading_days
        .iter()
        .enumerate()
        .step_by(rebalance_every_n_days)
    {
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

fn synthetic_market_day(date: NaiveDate, day_idx: usize, symbols: &[String]) -> MarketDay {
    let mut open = HashMap::with_capacity(symbols.len());
    let mut close = HashMap::with_capacity(symbols.len());
    let mut pre_close = HashMap::with_capacity(symbols.len());
    let mut amount = HashMap::with_capacity(symbols.len());
    let mut up_limit = HashMap::with_capacity(symbols.len());
    let mut down_limit = HashMap::with_capacity(symbols.len());

    for (symbol_idx, symbol) in symbols.iter().enumerate() {
        let close_price = synthetic_price(symbol_idx, day_idx);
        let previous_close = if day_idx == 0 {
            close_price
        } else {
            synthetic_price(symbol_idx, day_idx - 1)
        };
        let open_price = (previous_close + close_price) / Decimal::from(2u64);

        open.insert(symbol.clone(), open_price);
        close.insert(symbol.clone(), close_price);
        pre_close.insert(symbol.clone(), previous_close);
        amount.insert(symbol.clone(), Decimal::new(1_000_000_000, 0));
        up_limit.insert(symbol.clone(), previous_close * Decimal::new(2, 0));
        down_limit.insert(symbol.clone(), previous_close / Decimal::new(2, 0));
    }

    let benchmark_close = Decimal::new(4_000_000 + day_idx as i64 * 120, 3);
    let benchmark_pre_close = if day_idx == 0 {
        benchmark_close
    } else {
        Decimal::new(4_000_000 + (day_idx as i64 - 1) * 120, 3)
    };

    MarketDay {
        date,
        open,
        close,
        pre_close,
        amount,
        suspended: HashSet::new(),
        up_limit,
        down_limit,
        benchmark_close,
        benchmark_pre_close,
    }
}

fn synthetic_price(symbol_idx: usize, day_idx: usize) -> Decimal {
    let base_cents = 900 + (symbol_idx as i64 % 90) * 17;
    let drift_cents = day_idx as i64 * ((symbol_idx as i64 % 5) + 1);
    let cycle_cents = ((day_idx as i64 + symbol_idx as i64) % 11) - 5;
    Decimal::new(base_cents + drift_cents + cycle_cents, 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_caps_basket_size_to_available_symbols() {
        let report = run_phase2_smoke_baseline(Phase2PerfBaselineConfig {
            trading_days: 1,
            symbols: 3,
            rebalance_every_n_days: 0,
            basket_size: 10,
            ..Phase2PerfBaselineConfig::default()
        });

        assert_eq!(report.trading_days, 2);
        assert_eq!(report.symbols, 3);
        assert_eq!(report.rebalance_every_n_days, 1);
        assert_eq!(report.basket_size, 3);
        assert_eq!(report.equity_points, 2);
    }

    #[test]
    fn generated_trading_days_skip_weekends() {
        let days = generate_trading_days(NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(), 3);

        assert_eq!(
            days,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 8).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 9).unwrap(),
            ]
        );
    }
}
