//! Factor evaluation: IC, RankIC, quantile returns
//!
//! Evaluates how well a factor predicts future returns using:
//! - Information Coefficient (IC): Pearson correlation between factor and forward returns
//! - Rank IC: Spearman rank correlation
//! - Quantile analysis: forward returns by factor quintile

use std::collections::{BTreeMap, HashMap};

use crate::types::*;

/// Evaluate a factor against forward returns.
///
/// # Arguments
/// * `factor` - Factor values (already standardized or raw)
/// * `forward_returns` - Forward 1-period returns, keyed by (symbol, date)
/// * `n_quantiles` - Number of quantile groups for return spread (default: 5)
pub fn evaluate(
    factor: &FactorOutput,
    forward_returns: &HashMap<(String, chrono::NaiveDate), f64>,
    n_quantiles: usize,
) -> FactorEvaluation {
    // Align factor values with forward returns
    let mut aligned: Vec<(String, chrono::NaiveDate, f64, f64)> = Vec::new();

    for fv in &factor.values {
        if !fv.value.is_finite() {
            continue;
        }
        if fv
            .available_at
            .is_some_and(|available_at| available_at > fv.date)
        {
            continue;
        }
        if let Some(&fw_ret) = forward_returns.get(&(fv.symbol.clone(), fv.date)) {
            if fw_ret.is_finite() {
                aligned.push((fv.symbol.clone(), fv.date, fv.value, fw_ret));
            }
        }
    }

    if aligned.len() < 10 {
        return FactorEvaluation {
            factor_name: factor.name.clone(),
            date_range: (
                factor.values.first().map(|v| v.date).unwrap_or_default(),
                factor.values.last().map(|v| v.date).unwrap_or_default(),
            ),
            mean_ic: 0.0,
            ic_ir: 0.0,
            mean_rank_ic: 0.0,
            rank_ic_ir: 0.0,
            ic_series: vec![],
            quantile_spread: 0.0,
            quantile_returns: vec![0.0; n_quantiles],
            period_count: 0,
        };
    }

    // Group by date for cross-sectional IC
    let mut by_date: BTreeMap<chrono::NaiveDate, Vec<(f64, f64)>> = BTreeMap::new();
    for (_, date, factor_val, fw_ret) in &aligned {
        by_date
            .entry(*date)
            .or_default()
            .push((*factor_val, *fw_ret));
    }

    let mut ic_series = Vec::new();
    let mut rank_ic_series = Vec::new();

    for (date, pairs) in &by_date {
        if pairs.len() < 3 {
            continue;
        }

        let fs: Vec<f64> = pairs.iter().map(|(f, _)| *f).collect();
        let rs: Vec<f64> = pairs.iter().map(|(_, r)| *r).collect();

        // Pearson IC
        if let Some(ic) = pearson_corr(&fs, &rs) {
            ic_series.push((*date, ic));
        }

        // Rank IC (convert to ranks first)
        let f_ranks = to_ranks(&fs);
        let r_ranks = to_ranks(&rs);
        if let Some(ric) = pearson_corr(&f_ranks, &r_ranks) {
            rank_ic_series.push((*date, ric));
        }
    }

    let mean_ic = mean_of(&ic_series.iter().map(|(_, v)| *v).collect::<Vec<_>>());
    let mean_rank_ic = mean_of(&rank_ic_series.iter().map(|(_, v)| *v).collect::<Vec<_>>());
    let ic_std = std_of(&ic_series.iter().map(|(_, v)| *v).collect::<Vec<_>>());
    let rank_ic_std = std_of(&rank_ic_series.iter().map(|(_, v)| *v).collect::<Vec<_>>());

    // Quantile analysis: sort all aligned pairs by factor value, split into quantiles
    let mut sorted = aligned.clone();
    sorted.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));

    let total = sorted.len();
    let chunk_size = total / n_quantiles;
    let mut quantile_returns = vec![0.0; n_quantiles];

    for q in 0..n_quantiles {
        let start = q * chunk_size;
        let end = if q == n_quantiles - 1 {
            total
        } else {
            (q + 1) * chunk_size
        };
        let slice = &sorted[start..end];
        let rets: Vec<f64> = slice.iter().map(|(_, _, _, r)| *r).collect();
        quantile_returns[q] = mean_of(&rets);
    }

    let quantile_spread =
        quantile_returns.last().unwrap_or(&0.0) - quantile_returns.first().unwrap_or(&0.0);

    FactorEvaluation {
        factor_name: factor.name.clone(),
        date_range: (
            factor.values.first().map(|v| v.date).unwrap_or_default(),
            factor.values.last().map(|v| v.date).unwrap_or_default(),
        ),
        mean_ic,
        ic_ir: if ic_std > 0.0 { mean_ic / ic_std } else { 0.0 },
        mean_rank_ic,
        rank_ic_ir: if rank_ic_std > 0.0 {
            mean_rank_ic / rank_ic_std
        } else {
            0.0
        },
        ic_series,
        quantile_spread,
        quantile_returns,
        period_count: by_date.len(),
    }
}

/// Pearson correlation between two equal-length slices
fn pearson_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 3 {
        return None;
    }
    let n = x.len() as f64;
    let mx = x.iter().sum::<f64>() / n;
    let my = y.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut sx2 = 0.0;
    let mut sy2 = 0.0;

    for i in 0..x.len() {
        let dx = x[i] - mx;
        let dy = y[i] - my;
        cov += dx * dy;
        sx2 += dx * dx;
        sy2 += dy * dy;
    }

    let denom = (sx2 * sy2).sqrt();
    if denom == 0.0 {
        None
    } else {
        Some(cov / denom)
    }
}

/// Convert values to percentile ranks [0, 1]
fn to_ranks(values: &[f64]) -> Vec<f64> {
    let n = values.len() as f64;
    if n < 2.0 {
        return vec![0.5; values.len()];
    }

    let mut indexed: Vec<(usize, f64)> = values.iter().enumerate().map(|(i, v)| (i, *v)).collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut ranks = vec![0.0; values.len()];
    for (rank, (orig_idx, _)) in indexed.iter().enumerate() {
        ranks[*orig_idx] = (rank as f64) / (n - 1.0);
    }
    ranks
}

fn mean_of(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_of(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = mean_of(values);
    let variance =
        values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pearson_perfect_positive() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.0, 6.0, 8.0, 10.0];
        let corr = pearson_corr(&x, &y).unwrap();
        assert!((corr - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_pearson_perfect_negative() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![10.0, 8.0, 6.0, 4.0, 2.0];
        let corr = pearson_corr(&x, &y).unwrap();
        assert!((corr + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_rank_ic_same() {
        let fs = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let rs = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let rank_fs = to_ranks(&fs);
        let rank_rs = to_ranks(&rs);
        let corr = pearson_corr(&rank_fs, &rank_rs).unwrap();
        assert!((corr - 1.0).abs() < 0.001);
    }
}

#[cfg(test)]
mod pit_tests {
    use super::*;
    use crate::types::{FactorCategory, FactorMetadata, FactorOutput, FactorValue};
    use chrono::{NaiveDate, Utc};
    use std::collections::HashMap;

    fn factor_value(
        symbol: &str,
        date: &str,
        value: f64,
        available_at: Option<&str>,
    ) -> FactorValue {
        FactorValue {
            symbol: symbol.to_string(),
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            value,
            available_at: available_at.map(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").unwrap()),
        }
    }

    fn output(values: Vec<FactorValue>) -> FactorOutput {
        FactorOutput {
            name: "pit_factor".into(),
            values,
            metadata: FactorMetadata {
                factor_name: "pit_factor".into(),
                category: FactorCategory::Fundamental,
                version: "1.0.0".into(),
                params: serde_json::json!({}),
                computed_at: Utc::now(),
                symbol_count: 0,
                date_count: 0,
                coverage_ratio: 0.0,
                mean: 0.0,
                std: 0.0,
                min: 0.0,
                max: 0.0,
            },
        }
    }

    #[test]
    fn evaluate_excludes_values_unavailable_on_factor_date() {
        let factor = output(vec![
            factor_value("A", "2024-01-02", 1.0, Some("2024-01-02")),
            factor_value("B", "2024-01-02", 2.0, Some("2024-01-03")),
            factor_value("C", "2024-01-02", 3.0, Some("2024-01-02")),
        ]);
        let mut returns = HashMap::new();
        returns.insert(
            (
                "A".to_string(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ),
            0.01,
        );
        returns.insert(
            (
                "B".to_string(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ),
            0.02,
        );
        returns.insert(
            (
                "C".to_string(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ),
            0.03,
        );

        let evaluation = evaluate(&factor, &returns, 2);

        assert_eq!(evaluation.period_count, 0);
    }
}
