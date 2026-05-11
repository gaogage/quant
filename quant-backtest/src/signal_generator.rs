//! Factor-based signal generator
//!
//! Converts factor combination scores from `multi_factor_value` into daily
//! `StrategySignal` objects for the backtest engine.
//!
//! Strategy: rank all stocks by combo score each rebalance day, pick top-N,
//! assign equal weight.

use std::collections::HashMap;

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
            max_position_pct: Decimal::new(10, 2),
        }
    }
}

/// Generate daily strategy signals from factor combo scores.
///
/// For each rebalance date, ranks all stocks by combo score (higher = better),
/// selects top-N, and assigns equal weight.
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

    // 2. Load trading calendar
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

    // 3. Generate signals on rebalance schedule
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();
    let n = config.top_n;

    for (i, &day) in trading_days.iter().enumerate() {
        if i == 0 {
            continue; // skip first day (need previous day's scores)
        }

        // Only rebalance on schedule
        if i % config.rebalance_freq_days != 0 {
            continue;
        }

        // Use previous trading day's scores
        let prev_day = trading_days[i - 1];
        let prev_scores = match scores_by_date.get(&prev_day) {
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
        "Generated {} signals (top-{}, rebalance every {}d)",
        signals.len(),
        n,
        config.rebalance_freq_days
    );

    Ok(signals)
}
