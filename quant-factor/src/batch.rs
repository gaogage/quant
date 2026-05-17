//! Batch factor computation for large symbol universes.
//!
//! Progress is tracked via a simple callback so that API endpoints
//! can report live status without coupling to a specific database schema.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;

use crate::factors::price_volume::*;
use crate::standardize::standardize;
use crate::types::*;

/// Progress callback signature: (completed_chunks, total_chunks, symbols_processed, last_symbol)
pub type ProgressFn = Arc<dyn Fn(usize, usize, usize, String) + Send + Sync>;

/// Configuration for batch computation
pub struct BatchConfig {
    pub factor: String,
    pub version: String,
    pub symbols: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub standardize: Option<StandardizeMethod>,
    pub chunk_size: usize,
    pub progress: Option<ProgressFn>,
}

/// Result of batch computation
pub struct BatchResult {
    pub factor_name: String,
    pub version: String,
    pub total_values: usize,
    pub inserted: usize,
    pub errors: Vec<String>,
    pub standardized: bool,
}

/// Compute and persist factor values for a list of symbols in chunks.
///
/// The `save_fn` callback is called per chunk to persist values.
pub async fn batch_compute_factors(
    config: BatchConfig,
    bar_loader: Arc<
        dyn Fn(&[String], NaiveDate, NaiveDate) -> Result<HashMap<String, Vec<DailyBar>>, String>
            + Send
            + Sync,
    >,
    save_fn: Arc<
        dyn Fn(&str, &str, &[(String, NaiveDate, f64, bool)]) -> Result<usize, String>
            + Send
            + Sync,
    >,
) -> BatchResult {
    let mut total_values = 0usize;
    let mut inserted = 0usize;
    let mut errors: Vec<String> = Vec::new();

    let (factor_type, period) = parse_factor(&config.factor);
    let n_chunks = (config.symbols.len() + config.chunk_size - 1) / config.chunk_size;

    for (chunk_idx, chunk) in config.symbols.chunks(config.chunk_size).enumerate() {
        let syms: Vec<String> = chunk.to_vec();

        // Report progress
        if let Some(ref progress) = config.progress {
            let last = syms.last().cloned().unwrap_or_default();
            progress(chunk_idx + 1, n_chunks, config.symbols.len(), last);
        }

        // Load bars
        let bars = match bar_loader(&syms, config.start_date, config.end_date) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("chunk {} load failed: {}", chunk_idx, e));
                continue;
            }
        };

        if bars.is_empty() {
            continue;
        }

        let input = FactorInput {
            bars,
            trade_dates: vec![],
        };

        // Compute factor
        let mut output = match compute_price_volume_factor(factor_type, period, &input) {
            Some(output) => output,
            None => {
                errors.push(format!("Unknown factor: {}", config.factor));
                break;
            }
        };

        // Standardize if requested
        let is_std = config.standardize.is_some();
        if let Some(method) = config.standardize {
            output = standardize(&output, method);
        }

        total_values += output.values.len();

        // Build save rows: (symbol, date, value, is_standardized)
        let rows: Vec<(String, NaiveDate, f64, bool)> = output
            .values
            .iter()
            .map(|fv| (fv.symbol.clone(), fv.date, fv.value, is_std))
            .collect();

        match save_fn(&output.name, &config.version, &rows) {
            Ok(n) => inserted += n,
            Err(e) => errors.push(format!("chunk {} save failed: {}", chunk_idx, e)),
        }
    }

    BatchResult {
        factor_name: format!(
            "{}_{}d{}",
            factor_type,
            period,
            if config.standardize.is_some() {
                "_std"
            } else {
                ""
            }
        ),
        version: config.version,
        total_values,
        inserted,
        errors,
        standardized: config.standardize.is_some(),
    }
}

fn parse_factor(name: &str) -> (&'static str, usize) {
    if let Some(rest) = name.strip_prefix("mom_") {
        ("momentum", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("vol_") {
        (
            "volatility",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("downvol_") {
        (
            "downside_volatility",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("rev_") {
        ("reversal", rest.trim_end_matches('d').parse().unwrap_or(5))
    } else if let Some(rest) = name.strip_prefix("turn_") {
        ("turnover", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("amihud_") {
        (
            "amihud_illiquidity",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("amt_intensity_") {
        (
            "amount_intensity",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("rsi_") {
        ("rsi", rest.trim_end_matches('d').parse().unwrap_or(14))
    } else if let Some(rest) = name.strip_prefix("bb_pos_") {
        (
            "bb_position",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("atr_") {
        ("atr", rest.trim_end_matches('d').parse().unwrap_or(14))
    } else if let Some(rest) = name.strip_prefix("amp_") {
        (
            "amplitude",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("vp_corr_") {
        (
            "vol_price_corr",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("skew_") {
        ("skewness", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("maxdd_") {
        (
            "max_drawdown",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else {
        ("momentum", 20) // default fallback
    }
}
