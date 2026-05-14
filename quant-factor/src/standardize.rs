//! Cross-sectional standardization for factor values
//!
//! Standardization is applied per date (cross-section), transforming
//! raw factor values into comparable z-scores, ranks, or winsorized values.

use std::collections::BTreeMap;
use tracing::debug;

use crate::types::*;

/// Apply cross-sectional standardization to factor values.
///
/// For each date, this computes the chosen normalization across all symbols.
/// Missing values (NaN/Inf) are excluded from the computation and remain NaN.
pub fn standardize(output: &FactorOutput, method: StandardizeMethod) -> FactorOutput {
    let mut grouped: BTreeMap<chrono::NaiveDate, Vec<usize>> = BTreeMap::new();
    for (idx, fv) in output.values.iter().enumerate() {
        if fv.value.is_finite() {
            grouped.entry(fv.date).or_default().push(idx);
        }
    }

    let mut new_values = output.values.clone();

    for (date, indices) in &grouped {
        if indices.len() < 3 {
            debug!(
                "date={}, only {} valid symbols, skipping standardization",
                date,
                indices.len()
            );
            continue;
        }

        let raw: Vec<f64> = indices.iter().map(|&i| new_values[i].value).collect();

        match method {
            StandardizeMethod::ZScore => {
                let (mean, std) = mean_std(&raw);
                if std > 0.0 {
                    for &i in indices {
                        new_values[i].value =
                            (raw[indices.iter().position(|&j| j == i).unwrap()] - mean) / std;
                    }
                }
            }
            StandardizeMethod::Rank => {
                let n = raw.len() as f64;
                let mut sorted: Vec<(usize, f64)> = indices
                    .iter()
                    .enumerate()
                    .map(|(pos, &_idx)| (pos, raw[pos]))
                    .collect();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

                for (rank, (pos, _)) in sorted.iter().enumerate() {
                    let idx = indices[*pos];
                    new_values[idx].value = (rank as f64) / (n - 1.0);
                }
            }
            StandardizeMethod::Winsorized(sigma) => {
                // Iterative Winsorize: clip at ±Nσ, recompute mean/std from
                // cleaned distribution, repeat until stable (no new clips).
                // Handles multiple extreme outliers that would otherwise
                // keep inflating the clipping window after a fixed pass count.
                let mut current = raw.clone();
                for _pass in 0..10 {
                    let (m, s) = mean_std(&current);
                    if s <= 0.0 {
                        break;
                    }
                    let lower = m - sigma * s;
                    let upper = m + sigma * s;
                    let mut clipped = false;
                    for x in &mut current {
                        let old = *x;
                        *x = x.clamp(lower, upper);
                        if old != *x {
                            clipped = true;
                        }
                    }
                    if !clipped {
                        break;
                    } // converged
                }
                let (cm, cs) = mean_std(&current);
                if cs > 0.0 {
                    for (pos, &i) in indices.iter().enumerate() {
                        new_values[i].value = (current[pos] - cm) / cs;
                    }
                }
            }
        }
    }

    // Rebuild metadata
    let meta = crate::factors::price_volume::build_metadata_inner(
        &new_values,
        &output.name,
        output.metadata.category,
        &output.metadata.version,
        output.metadata.params.clone(),
    );

    FactorOutput {
        name: format!("{}_std", output.name),
        values: new_values,
        metadata: meta,
    }
}

/// Compute mean and standard deviation
fn mean_std(data: &[f64]) -> (f64, f64) {
    let n = data.len() as f64;
    if n < 2.0 {
        return (data.iter().sum::<f64>() / n, 0.0);
    }
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, variance.sqrt())
}

// Re-export build_metadata for internal use
#[doc(hidden)]
pub mod helpers {
    use crate::types::*;

    pub fn build_metadata_inner(
        values: &[FactorValue],
        name: &str,
        category: FactorCategory,
        version: &str,
        params: serde_json::Value,
    ) -> FactorMetadata {
        let n = values.len() as f64;
        if n == 0.0 {
            return FactorMetadata {
                factor_name: name.to_string(),
                category,
                version: version.to_string(),
                params,
                computed_at: chrono::Utc::now(),
                symbol_count: 0,
                date_count: 0,
                coverage_ratio: 0.0,
                mean: f64::NAN,
                std: f64::NAN,
                min: f64::NAN,
                max: f64::NAN,
            };
        }

        let valid: Vec<f64> = values
            .iter()
            .map(|v| v.value)
            .filter(|x| x.is_finite())
            .collect();
        let m = valid.len() as f64;
        let mean = valid.iter().sum::<f64>() / m;
        let variance = valid.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / m;
        let std = variance.sqrt();
        let min = valid.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = valid.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let symbols: std::collections::HashSet<&str> =
            values.iter().map(|v| v.symbol.as_str()).collect();
        let dates: std::collections::HashSet<chrono::NaiveDate> =
            values.iter().map(|v| v.date).collect();

        FactorMetadata {
            factor_name: name.to_string(),
            category,
            version: version.to_string(),
            params,
            computed_at: chrono::Utc::now(),
            symbol_count: symbols.len(),
            date_count: dates.len(),
            coverage_ratio: m / n,
            mean,
            std,
            min,
            max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_output(name: &str, values: Vec<(f64, &str)>) -> FactorOutput {
        let date = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
        FactorOutput {
            name: name.to_string(),
            values: values
                .iter()
                .enumerate()
                .map(|(i, (v, s))| FactorValue {
                    symbol: s.to_string(),
                    date,
                    value: *v,
                    available_at: None,
                })
                .collect(),
            metadata: FactorMetadata {
                factor_name: name.to_string(),
                category: FactorCategory::PriceVolume,
                version: "1.0.0".to_string(),
                params: serde_json::json!({}),
                computed_at: chrono::Utc::now(),
                symbol_count: values.len(),
                date_count: 1,
                coverage_ratio: 1.0,
                mean: 0.0,
                std: 0.0,
                min: 0.0,
                max: 0.0,
            },
        }
    }

    #[test]
    fn test_zscore_standardization() {
        let output = make_output(
            "test",
            vec![(1.0, "A"), (2.0, "B"), (3.0, "C"), (4.0, "D"), (5.0, "E")],
        );
        let result = standardize(&output, StandardizeMethod::ZScore);
        let vals: Vec<f64> = result.values.iter().map(|v| v.value).collect();
        // mean=3.0, std≈1.581 → z-scores: [-1.265, -0.632, 0, 0.632, 1.265]
        let expected = vec![-1.2649, -0.6325, 0.0, 0.6325, 1.2649];
        for (a, e) in vals.iter().zip(expected.iter()) {
            assert!((a - e).abs() < 0.01, "got {}, expected {}", a, e);
        }
    }

    #[test]
    fn test_rank_standardization() {
        let output = make_output(
            "test",
            vec![(5.0, "A"), (1.0, "B"), (3.0, "C"), (4.0, "D"), (2.0, "E")],
        );
        let result = standardize(&output, StandardizeMethod::Rank);
        let vals: Vec<f64> = result.values.iter().map(|v| v.value).collect();
        // sorted: B=1(0), E=2(1), C=3(2), D=4(3), A=5(4) → ranks: [1.0, 0.0, 0.5, 0.75, 0.25]
        assert!((vals[0] - 1.0).abs() < 0.01); // A: rank 4/4 = 1.0
        assert!((vals[1] - 0.0).abs() < 0.01); // B: rank 0/4 = 0.0
    }

    #[test]
    fn test_winsorized_clips_extreme() {
        // Normal values + one extreme outlier → should be clipped to ≤ 5σ after re-zscore
        let output = make_output(
            "test",
            vec![
                (1.0, "A"),
                (2.0, "B"),
                (3.0, "C"),
                (4.0, "D"),
                (100.0, "E"), // extreme outlier — should be winsorized
            ],
        );
        let result = standardize(&output, StandardizeMethod::Winsorized(3.0));
        let vals: Vec<f64> = result.values.iter().map(|v| v.value).collect();
        eprintln!("winsorized values: {:?}", vals);
        // All values should be within [-5, 5] after winsorize+re-zscore
        for v in &vals {
            assert!(v.abs() < 5.0, "value {} exceeds 5σ after winsorize", v);
        }
    }

    #[test]
    fn test_winsorized_extreme_outlier() {
        // Simulate a realistic cross-section: 4999 normal + 1 127σ outlier
        let mut values: Vec<(f64, &str)> = (0..4999)
            .map(|i| (i as f64 * 0.001 - 2.5, "N")) // spread from -2.5 to 2.5
            .collect();
        values.push((127.0, "OUTLIER"));

        let output = make_output("large", values);
        let result = standardize(&output, StandardizeMethod::Winsorized(5.0));
        let vals: Vec<f64> = result.values.iter().map(|v| v.value).collect();
        let outlier_val = vals.last().unwrap();
        eprintln!("outlier after winsorize(5): {}", outlier_val);
        assert!(
            outlier_val.abs() <= 6.0,
            "127σ outlier should be clipped near 5σ, got {}",
            outlier_val
        );
    }

    #[test]
    fn test_winsorized_double_outlier() {
        // Two extreme outliers on same date — iterative Winsorize should converge
        let mut values: Vec<(f64, &str)> =
            (0..2998).map(|i| (i as f64 * 0.001 - 1.5, "N")).collect();
        values.push((774.0, "OUTLIER1"));
        values.push((550.0, "OUTLIER2"));
        let output = make_output("double", values);
        let result = standardize(&output, StandardizeMethod::Winsorized(5.0));
        let vals: Vec<f64> = result.values.iter().map(|v| v.value).collect();
        let o1 = vals[vals.len() - 2];
        let o2 = vals[vals.len() - 1];
        eprintln!("double outliers: {} and {}", o1, o2);
        assert!(o1.abs() <= 6.0, "outlier1 {} > 6σ", o1);
        assert!(o2.abs() <= 6.0, "outlier2 {} > 6σ", o2);
    }
}
