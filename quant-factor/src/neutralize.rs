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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_output(values: Vec<(&str, f64)>) -> FactorOutput {
        let date = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        FactorOutput {
            name: "test_factor".to_string(),
            values: values
                .iter()
                .map(|(s, v)| FactorValue {
                    symbol: s.to_string(),
                    date,
                    value: *v,
                    available_at: None,
                })
                .collect(),
            metadata: FactorMetadata {
                factor_name: "test_factor".to_string(),
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

    fn industries(map: &[(&str, &str)]) -> HashMap<String, String> {
        map.iter().map(|(s, i)| (s.to_string(), i.to_string())).collect()
    }

    #[test]
    fn industry_neutral_demeans_within_groups() {
        // 两行业各 3 只：组内减均值后均值≈0，组间差异保留
        let output = make_output(vec![
            ("A1", 1.0), ("A2", 2.0), ("A3", 3.0),   // 行业 X 均值 2
            ("B1", 10.0), ("B2", 20.0), ("B3", 30.0), // 行业 Y 均值 20
        ]);
        let config = NeutralizeConfig {
            industries: industries(&[("A1", "X"), ("A2", "X"), ("A3", "X"), ("B1", "Y"), ("B2", "Y"), ("B3", "Y")]),
            size_proxy: HashMap::new(),
        };
        let (out, res) = neutralize(&output, &config, true, false);
        assert_eq!(out.name, "test_factor_ind");
        assert!(res.industry_neutral);
        assert!(!res.size_neutral);
        assert_eq!(res.original_count, 6);
        // 组内均值≈0
        let mean_a = (out.values[0].value + out.values[1].value + out.values[2].value) / 3.0;
        let mean_b = (out.values[3].value + out.values[4].value + out.values[5].value) / 3.0;
        assert!(mean_a.abs() < 1e-12, "A 组均值 {} 应≈0", mean_a);
        assert!(mean_b.abs() < 1e-12, "B 组均值 {} 应≈0", mean_b);
        // 组间离散度保留（B 组残差绝对值大于 A 组）
        let spread_b = (out.values[3].value - out.values[5].value).abs();
        assert!(spread_b > 10.0);
    }

    #[test]
    fn industry_neutral_small_group_skipped() {
        // 组内 <3 只：不中性化，值不变
        let output = make_output(vec![("A1", 1.0), ("A2", 2.0)]);
        let config = NeutralizeConfig {
            industries: industries(&[("A1", "X"), ("A2", "X")]),
            size_proxy: HashMap::new(),
        };
        let (out, _) = neutralize(&output, &config, true, false);
        assert_eq!(out.values[0].value, 1.0);
        assert_eq!(out.values[1].value, 2.0);
    }

    #[test]
    fn industry_neutral_unmapped_symbols_untouched() {
        // 无行业映射的 symbol 不参与分组，值不变
        let output = make_output(vec![("A1", 1.0), ("A2", 2.0), ("A3", 3.0), ("Z", 99.0)]);
        let config = NeutralizeConfig {
            industries: industries(&[("A1", "X"), ("A2", "X"), ("A3", "X")]),
            size_proxy: HashMap::new(),
        };
        let (out, _) = neutralize(&output, &config, true, false);
        assert_eq!(out.values[3].value, 99.0);
        // A 组被中性化
        let mean_a = out.values[..3].iter().map(|v| v.value).sum::<f64>() / 3.0;
        assert!(mean_a.abs() < 1e-12);
    }

    #[test]
    fn size_neutral_residual_for_perfect_linear_relation() {
        // 20 只、factor 与 ln(size) 完全线性 → 残差≈0
        let n = 20;
        let values: Vec<(&str, f64)> = (0..n)
            .map(|i| {
                let sz = 100.0 + i as f64 * 10.0;
                let x = (sz + 1.0).ln();
                (LEAK[i], 2.0 * x + 1.0) // y = 2x + 1
            })
            .collect();
        let output = make_output(values);
        let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
        let date = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        for i in 0..n {
            size_proxy.insert(
                LEAK[i].to_string(),
                vec![(date, 100.0 + i as f64 * 10.0)],
            );
        }
        let config = NeutralizeConfig {
            industries: HashMap::new(),
            size_proxy,
        };
        let (out, res) = neutralize(&output, &config, false, true);
        assert_eq!(out.name, "test_factor_sz");
        assert!(res.size_neutral);
        for v in &out.values {
            assert!(
                v.value.abs() < 1e-9,
                "完全线性关系残差应≈0，实际 {}",
                v.value
            );
        }
    }

    #[test]
    fn size_neutral_insufficient_sample_skipped() {
        // <20 只：不回归，值不变
        let output = make_output(vec![("A", 1.0), ("B", 2.0), ("C", 3.0)]);
        let date = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        let mut size_proxy = HashMap::new();
        for (i, s) in ["A", "B", "C"].iter().enumerate() {
            size_proxy.insert(s.to_string(), vec![(date, 100.0 + i as f64)]);
        }
        let config = NeutralizeConfig {
            industries: HashMap::new(),
            size_proxy,
        };
        let (out, _) = neutralize(&output, &config, false, true);
        assert_eq!(out.values[0].value, 1.0);
        assert_eq!(out.values[2].value, 3.0);
    }

    #[test]
    fn neutralize_both_flags_appends_combined_tag() {
        let output = make_output(vec![("A", 1.0), ("B", 2.0)]);
        let (out, res) = neutralize(&output, &NeutralizeConfig::default_config(), true, true);
        assert_eq!(out.name, "test_factor_ind_sz");
        assert!(res.industry_neutral && res.size_neutral);
    }

    #[test]
    fn neutralize_no_flags_returns_original_values() {
        let output = make_output(vec![("A", 1.0), ("B", f64::NAN)]);
        let (out, res) = neutralize(&output, &NeutralizeConfig::default_config(), false, false);
        assert_eq!(out.name, "test_factor");
        assert_eq!(out.values[0].value, 1.0);
        assert!(out.values[1].value.is_nan());
        // neutralized_count 只统计有限值
        assert_eq!(res.neutralized_count, 1);
        assert_eq!(res.original_count, 2);
    }

    /// 测试专用 symbol 池（size_neutral 需 ≥20 只）
    const LEAK: [&str; 20] = [
        "S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09", "S10",
        "S11", "S12", "S13", "S14", "S15", "S16", "S17", "S18", "S19", "S20",
    ];

    impl NeutralizeConfig {
        fn default_config() -> Self {
            Self {
                industries: HashMap::new(),
                size_proxy: HashMap::new(),
            }
        }
    }
}
