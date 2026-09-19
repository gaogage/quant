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
                .map(|(v, s)| FactorValue {
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
        let expected = [-1.2649, -0.6325, 0.0, 0.6325, 1.2649];
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

    // ─── 分支与边界补充 ─────────────────────────────────────────

    fn fv(symbol: &str, date: NaiveDate, value: f64) -> FactorValue {
        FactorValue {
            symbol: symbol.to_string(),
            date,
            value,
            available_at: None,
        }
    }

    fn multi_date_output(values: Vec<FactorValue>) -> FactorOutput {
        FactorOutput {
            name: "multi".to_string(),
            values,
            metadata: FactorMetadata {
                factor_name: "multi".to_string(),
                category: FactorCategory::PriceVolume,
                version: "1.0.0".to_string(),
                params: serde_json::json!({}),
                computed_at: chrono::Utc::now(),
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
    fn test_zscore_zero_std_cross_section_left_unchanged() {
        // 同日 3 个等值 → std=0 → 不做除法，值保持原样（防除零）
        let output = make_output("flat", vec![(5.0, "A"), (5.0, "B"), (5.0, "C")]);
        let result = standardize(&output, StandardizeMethod::ZScore);
        for v in &result.values {
            assert!(
                (v.value - 5.0).abs() < 1e-12,
                "零方差截面应保持原值，实际 {}",
                v.value
            );
        }
        assert_eq!(result.name, "flat_std", "标准化后因子名应带 _std 后缀");
    }

    #[test]
    fn test_standardize_skips_dates_with_fewer_than_three_symbols() {
        // d1 有 3 个符号（正常 z-score）；d2 仅 2 个（不足 3，跳过保持原值）
        let d1 = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        let output = multi_date_output(vec![
            fv("A", d1, 1.0),
            fv("B", d1, 2.0),
            fv("C", d1, 3.0),
            fv("D", d2, 7.0),
            fv("E", d2, 9.0),
        ]);
        let result = standardize(&output, StandardizeMethod::ZScore);

        let d1_vals: Vec<f64> = result
            .values
            .iter()
            .filter(|v| v.date == d1)
            .map(|v| v.value)
            .collect();
        let d1_mean = d1_vals.iter().sum::<f64>() / 3.0;
        assert!(
            d1_mean.abs() < 1e-9,
            "3 符号截面应被标准化（均值≈0），实际 {}",
            d1_mean
        );
        for v in result.values.iter().filter(|v| v.date == d2) {
            assert!(
                (v.value - 7.0).abs() < 1e-12 || (v.value - 9.0).abs() < 1e-12,
                "不足 3 符号的日期应保持原值，实际 {}",
                v.value
            );
        }
    }

    #[test]
    fn test_standardize_excludes_nonfinite_values_from_groups() {
        // NaN 值不参与截面统计（不拉偏均值/方差），且自身保持 NaN
        let output = make_output(
            "mixed",
            vec![(1.0, "A"), (2.0, "B"), (3.0, "C"), (f64::NAN, "N")],
        );
        let result = standardize(&output, StandardizeMethod::ZScore);

        let nan_row = result.values.iter().find(|v| v.symbol == "N").unwrap();
        assert!(nan_row.value.is_nan(), "NaN 输入应保持 NaN");
        // 有效 3 值 [1,2,3] 标准化后：z(2)=0
        let b = result.values.iter().find(|v| v.symbol == "B").unwrap();
        assert!(
            b.value.abs() < 1e-9,
            "B 的 z 值应为 0（等于有效值均值），实际 {}",
            b.value
        );
        // metadata 的 coverage_ratio 应只计有效值
        assert!(
            (result.metadata.coverage_ratio - 0.75).abs() < 1e-12,
            "覆盖率 3/4，实际 {}",
            result.metadata.coverage_ratio
        );
    }

    #[test]
    fn test_winsorized_zero_std_breaks_early_without_change() {
        // 3 个等值 → 首轮 s<=0 即 break，cs=0 不做 z-score，值保持原样
        let output = make_output("wflat", vec![(4.2, "A"), (4.2, "B"), (4.2, "C")]);
        let result = standardize(&output, StandardizeMethod::Winsorized(2.0));
        for v in &result.values {
            assert!(
                (v.value - 4.2).abs() < 1e-12,
                "零方差截面 winsorize 应保持原值，实际 {}",
                v.value
            );
        }
    }

    #[test]
    fn test_mean_std_single_element_returns_value_with_zero_std() {
        // 单元素截面：均值为自身，std 记 0（无离散）
        let (m, s) = mean_std(&[4.2]);
        assert!((m - 4.2).abs() < 1e-12);
        assert_eq!(s, 0.0);
    }

    #[test]
    fn test_build_metadata_inner_empty_values_returns_nan_metadata() {
        let meta = helpers::build_metadata_inner(
            &[],
            "none",
            FactorCategory::Sentiment,
            "1.0.0",
            serde_json::json!({}),
        );
        assert_eq!(meta.factor_name, "none");
        assert_eq!(meta.symbol_count, 0);
        assert_eq!(meta.date_count, 0);
        assert_eq!(meta.coverage_ratio, 0.0);
        assert!(meta.mean.is_nan(), "空序列统计量应为 NaN");
        assert!(meta.std.is_nan());
        assert!(meta.min.is_nan());
        assert!(meta.max.is_nan());
    }

    #[test]
    fn test_build_metadata_inner_stats_exclude_nonfinite() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 1).unwrap();
        let values = vec![
            fv("A", d, 1.0),
            fv("B", d, f64::NAN),
            fv("C", d, 2.0),
            fv("A", NaiveDate::from_ymd_opt(2025, 6, 2).unwrap(), 4.0),
        ];
        let meta = helpers::build_metadata_inner(
            &values,
            "stats",
            FactorCategory::PriceVolume,
            "1.0.0",
            serde_json::json!({}),
        );
        assert_eq!(meta.symbol_count, 3, "A/B/C 三个标的");
        assert_eq!(meta.date_count, 2, "两个交易日");
        assert!(
            (meta.coverage_ratio - 0.75).abs() < 1e-12,
            "有效值 3/4，实际 {}",
            meta.coverage_ratio
        );
        assert!((meta.min - 1.0).abs() < 1e-12, "min 只计有效值");
        assert!((meta.max - 4.0).abs() < 1e-12);
        assert!((meta.mean - 7.0 / 3.0).abs() < 1e-12);
    }
}
