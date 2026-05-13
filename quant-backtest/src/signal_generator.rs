//! Factor-based signal generator
//!
//! Converts factor combination scores from `multi_factor_value` into daily
//! `StrategySignal` objects for the backtest engine.
//!
//! Strategy: rank all stocks by combo score each rebalance day, pick top-N,
//! assign equal weight.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
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
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            combo_name: "icir_weighted_3f".into(),
            version: "1.0.0".into(),
            top_n: 20,
            rebalance_freq_days: 20, // monthly default
            entry_delay_days: 0,     // no delay by default
            min_daily_amount_cny: None, // no filter by default
            max_position_pct: Decimal::new(10, 2),
        }
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
    // 1. Load combo scores
    let rows: Vec<(String, NaiveDate, Option<f64>)> = sqlx::query_as(
        "SELECT symbol, trade_date, raw_score
         FROM multi_factor_value
         WHERE combo_name = $1 AND version = $2
           AND trade_date >= $3 AND trade_date <= $4
         ORDER BY trade_date, symbol"
    )
    .bind(&config.combo_name)
    .bind(&config.version)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load combo scores: {}", e))?;

    if rows.is_empty() {
        return Err("No combo scores found".into());
    }

    // Group by date
    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }

    // Sort each day's scores descending
    for items in scores_by_date.values_mut() {
        items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    // 2. Liquidity filter: remove stocks with low average daily trading amount
    if let Some(min_amount) = config.min_daily_amount_cny {
        // Collect symbols and date range
        let all_symbols: Vec<String> = scores_by_date.values()
            .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        
        if !all_symbols.is_empty() {
            // Note: tushare amount is in 千元 (thousands of CNY). Convert threshold.
            let min_amount_1k = min_amount / 1000.0;
            // Single efficient SQL: avg daily amount per symbol
            let liquid_rows: Vec<(String,)> = sqlx::query_as(
                "SELECT symbol FROM (
                    SELECT symbol, AVG(amount) as avg_amt
                    FROM market_stock_daily_bar
                    WHERE symbol = ANY($1)
                      AND trade_date >= $2 AND trade_date <= $3
                      AND amount > 0
                    GROUP BY symbol
                    HAVING AVG(amount) >= $4
                ) sub"
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
                stocks.retain(|(sym, _)| liquid_set.contains(sym));
            }
            let after: usize = scores_by_date.values().flatten().count();
            info!("Liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
                min_amount as u64, after, before, liquid_set.len());
        }
    }

    // 3. Load trading calendar
    let trading_days: Vec<NaiveDate> = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT trade_date FROM market_trade_calendar
         WHERE exchange = 'SSE' AND is_open = true
           AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date"
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load calendar: {}", e))?
    .into_iter()
    .map(|(d,)| d)
    .collect();

    let min_idx = 1 + config.entry_delay_days; // need at least this many days of history

    // 3. Generate signals on rebalance schedule
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();
    let n = config.top_n;

    for (i, &day) in trading_days.iter().enumerate() {
        if i < min_idx {
            continue; // not enough history for delayed entry
        }

        // Only rebalance on schedule
        if i % config.rebalance_freq_days != 0 {
            continue;
        }

        // Use scores from entry_delay_days trading days before
        // i - 1 = previous trading day; i - 1 - delay = delayed score day
        let score_idx = i - 1 - config.entry_delay_days;
        let score_day = trading_days[score_idx];
        let prev_scores = match scores_by_date.get(&score_day) {
            Some(s) => s,
            None => continue,
        };

        // Pick top-N
        let top: Vec<&(String, f64)> = prev_scores.iter().take(n).collect();
        if top.len() < n.min(5) {
            continue; // not enough candidates
        }

        let weight = Decimal::from_f64(1.0 / top.len() as f64)
            .unwrap_or(Decimal::new(1, 1))
            .min(config.max_position_pct);

        let mut target_weights = HashMap::new();
        for (sym, _score) in &top {
            target_weights.insert((*sym).clone(), weight);
        }

        signals.insert(day, StrategySignal {
            date: day,
            target_weights,
        });
    }

    info!(
        "Generated {} signals (top-{}, rebalance every {}d, entry_delay {}d)",
        signals.len(),
        n,
        config.rebalance_freq_days,
        config.entry_delay_days
    );

    Ok(signals)
}
