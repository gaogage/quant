//! Factor neutralization — strip sector and size effects
//!
//! Two-step orthogonalization (standard quant approach):
//!   1. Subtract industry mean → industry-neutral
//!   2. Regress on log_amount, take residual → industry+size neutral

use std::collections::HashMap;

use chrono::NaiveDate;

use crate::types::*;

/// Configuration for neutralization
#[derive(Debug, Clone)]
pub struct NeutralizeConfig {
    /// Industry mapping: symbol → industry name
    pub industries: HashMap<String, String>,
    /// Size proxy: (symbol, date) → size value (e.g. log_amount)
    pub size_proxy: HashMap<String, Vec<(NaiveDate, f64)>>,
}

/// Result of neutralization
#[derive(Debug, Clone)]
pub struct NeutralizeResult {
    pub factor_name: String,
    pub original_count: usize,
    pub neutralized_count: usize,
    pub industry_neutral: bool,
    pub size_neutral: bool,
}

/// Neutralize factor values: industry de-mean + size orthogonalization
pub fn neutralize(
    output: &FactorOutput,
    config: &NeutralizeConfig,
    do_industry: bool,
    do_size: bool,
) -> (FactorOutput, NeutralizeResult) {
    let mut values = output.values.clone();
    let original_count = values.len();

    // Step 1: Industry neutralization (de-mean within industry×date)
    if do_industry {
        // Build lookup: (date, industry) → list of (value index)
        let mut groups: HashMap<(NaiveDate, String), Vec<usize>> = HashMap::new();
        for (idx, fv) in values.iter().enumerate() {
            if let Some(ind) = config.industries.get(&fv.symbol) {
                groups.entry((fv.date, ind.clone())).or_default().push(idx);
            }
        }

        // Subtract group mean
        for ((_date, _ind), indices) in &groups {
            if indices.len() < 3 {
                continue;
            }
            let (sum_val, count) = indices
                .iter()
                .filter_map(|&i| {
                    let v = values[i].value;
                    if v.is_finite() {
                        Some(v)
                    } else {
                        None
                    }
                })
                .fold((0.0f64, 0usize), |(sum, n), v| (sum + v, n + 1));
            if count == 0 {
                continue;
            }
            let avg = sum_val / count as f64;
            for &i in indices {
                if values[i].value.is_finite() {
                    values[i].value -= avg;
                }
            }
        }
    }

    // Step 2: Size neutralization (cross-sectional regression per date)
    if do_size {
        // Convert size_proxy to a lookup: (symbol, date) → f64
        let size_map: HashMap<(String, NaiveDate), f64> = config
            .size_proxy
            .iter()
            .flat_map(|(sym, entries)| entries.iter().map(move |(d, v)| ((sym.clone(), *d), *v)))
            .collect();

        // Group indices by date
        let mut date_groups: HashMap<NaiveDate, Vec<usize>> = HashMap::new();
        for (idx, fv) in values.iter().enumerate() {
            let key = (fv.symbol.clone(), fv.date);
            if size_map.contains_key(&key) && fv.value.is_finite() {
                date_groups.entry(fv.date).or_default().push(idx);
            }
        }

        // Per-date OLS: factor_residual = factor - (alpha + beta * size)
        for (_date, indices) in &date_groups {
            if indices.len() < 20 {
                continue;
            }

            let pairs: Vec<(f64, f64)> = indices
                .iter()
                .filter_map(|&i| {
                    let key = (values[i].symbol.clone(), values[i].date);
                    let sz = size_map.get(&key).copied()?;
                    if sz.is_finite() && sz > 0.0 {
                        Some((values[i].value, (sz + 1.0).ln()))
                    } else {
                        None
                    }
                })
                .collect();

            if pairs.len() < 20 {
                continue;
            }

            // Simple OLS: y = a + b * size
            let sx: f64 = pairs.iter().map(|(_, x)| x).sum();
            let sy: f64 = pairs.iter().map(|(y, _)| y).sum();
            let sxx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
            let sxy: f64 = pairs.iter().map(|(y, x)| y * x).sum();
            let m = pairs.len() as f64;

            let denominator = m * sxx - sx * sx;
            if denominator.abs() < 1e-12 {
                continue;
            }

            let beta = (m * sxy - sx * sy) / denominator;
            let alpha = (sy - beta * sx) / m;

            // Apply residuals back to original indices
            // We need to match pairs back to indices
            let mut pair_idx = 0usize;
            for &i in indices {
                let key = (values[i].symbol.clone(), values[i].date);
                if let Some(&sz) = size_map.get(&key) {
                    if sz.is_finite() && sz > 0.0 && pair_idx < pairs.len() {
                        let predicted = alpha + beta * (sz + 1.0).ln();
                        values[i].value -= predicted;
                        pair_idx += 1;
                    }
                }
            }
        }
    }

    let neutralized_count = values.iter().filter(|v| v.value.is_finite()).count();

    let mut meta = output.metadata.clone();
    meta.computed_at = chrono::Utc::now();
    let neutral_tag = match (do_industry, do_size) {
        (true, true) => "_ind_sz",
        (true, false) => "_ind",
        (false, true) => "_sz",
        _ => "",
    };

    let result = NeutralizeResult {
        factor_name: output.name.clone(),
        original_count,
        neutralized_count,
        industry_neutral: do_industry,
        size_neutral: do_size,
    };

    (
        FactorOutput {
            name: format!("{}{}", output.name, neutral_tag),
            values,
            metadata: meta,
        },
        result,
    )
}
