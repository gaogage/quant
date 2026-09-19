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
            rank_ic_series: vec![],
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

    let quantile_returns: Vec<f64> = (0..n_quantiles)
        .map(|q| {
            let start = q * chunk_size;
            let end = if q == n_quantiles - 1 {
                total
            } else {
                (q + 1) * chunk_size
            };
            let slice = &sorted[start..end];
            let rets: Vec<f64> = slice.iter().map(|(_, _, _, r)| *r).collect();
            mean_of(&rets)
        })
        .collect();

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
        rank_ic_series,
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

    #[test]
    fn test_pearson_length_mismatch_and_too_short_rejected() {
        // 长度不等 → None
        assert!(pearson_corr(&[1.0, 2.0, 3.0], &[1.0, 2.0]).is_none());
        // 样本 <3 → None(两点相关无意义)
        assert!(pearson_corr(&[1.0, 2.0], &[2.0, 1.0]).is_none());
        assert!(pearson_corr(&[], &[]).is_none());
    }

    #[test]
    fn test_pearson_zero_variance_returns_none() {
        // 一侧恒定(零方差) → 分母为 0 → None, 不产生 NaN
        let x = vec![1.0, 2.0, 3.0, 4.0];
        let flat = vec![5.0, 5.0, 5.0, 5.0];
        assert!(pearson_corr(&x, &flat).is_none());
        assert!(pearson_corr(&flat, &x).is_none());
    }

    #[test]
    fn test_pearson_partial_negative_correlation() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let y = vec![6.0, 5.0, 5.0, 3.0, 2.0, 2.0];
        let corr = pearson_corr(&x, &y).unwrap();
        assert!(
            corr < -0.9 && corr > -1.0,
            "强负相关应在 (-1, -0.9), 实际 {}",
            corr
        );
    }

    #[test]
    fn test_to_ranks_single_and_empty_degrade_to_neutral() {
        // n<2 → 全部 0.5(中性秩, 避免除零)
        assert_eq!(to_ranks(&[42.0]), vec![0.5]);
        assert_eq!(to_ranks(&[]), Vec::<f64>::new());
    }

    #[test]
    fn test_to_ranks_maps_to_zero_one_with_ties_ordered() {
        let ranks = to_ranks(&[30.0, 10.0, 20.0]);
        // 最小值→0.0, 最大值→1.0, 中位→0.5
        assert!((ranks[0] - 1.0).abs() < 1e-9);
        assert!((ranks[1] - 0.0).abs() < 1e-9);
        assert!((ranks[2] - 0.5).abs() < 1e-9);
        // 重复值按 stable 排序获得相邻的不同秩(非平均秩——记录行为):
        // [1.0,1.0,2.0] → 先出现的 1.0 得秩 0/2, 后出现的得 1/2, 2.0 得 2/2
        let tied = to_ranks(&[1.0, 1.0, 2.0]);
        assert!((tied[0] - 0.0).abs() < 1e-9);
        assert!((tied[1] - 0.5).abs() < 1e-9);
        assert!((tied[2] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_mean_and_std_edge_cases() {
        // 空集 → 0(防除零约定)
        assert_eq!(mean_of(&[]), 0.0);
        assert_eq!(std_of(&[]), 0.0);
        // 单元素 → std 0(无离散)
        assert_eq!(mean_of(&[7.0]), 7.0);
        assert_eq!(std_of(&[7.0]), 0.0);
        // 样本标准差(n-1 分母): [1,2,3] 均值 2, 方差 = (1+0+1)/2 = 1
        assert!((mean_of(&[1.0, 2.0, 3.0]) - 2.0).abs() < 1e-12);
        assert!((std_of(&[1.0, 2.0, 3.0]) - 1.0).abs() < 1e-12);
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

    /// 对齐样本 < 10 → 整体判无效，返回全零评估（防小样本噪声）
    #[test]
    fn evaluate_below_ten_aligned_pairs_returns_zero_evaluation() {
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        for i in 0..9 {
            let sym = format!("S{}", i);
            vals.push(factor_value(&sym, "2024-01-02", i as f64, None));
            rets.insert(
                (sym, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
                0.01 * (i + 1) as f64,
            );
        }

        let ev = evaluate(&output(vals), &rets, 3);

        assert_eq!(ev.period_count, 0, "不足 10 对直接短路");
        assert_eq!(ev.mean_ic, 0.0);
        assert_eq!(ev.ic_ir, 0.0);
        assert_eq!(ev.mean_rank_ic, 0.0);
        assert_eq!(ev.rank_ic_ir, 0.0);
        assert!(ev.ic_series.is_empty());
        assert!(ev.rank_ic_series.is_empty());
        assert_eq!(
            ev.quantile_returns,
            vec![0.0; 3],
            "quantile_returns 用 n_quantiles 个 0 占位"
        );
        assert_eq!(ev.quantile_spread, 0.0);
    }

    /// 主路径手算：2 日 × 5 标的，因子值与前瞻收益完全正相关 → 每日 IC/rankIC = 1；
    /// 排序后 2 分位：低组收益均值 0.03、高组 0.08，spread = 0.05
    #[test]
    fn evaluate_perfect_correlation_computes_ic_and_quantiles_by_hand() {
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        for (day, base) in [("2024-01-02", 1.0f64), ("2024-01-03", 6.0f64)] {
            for i in 0..5usize {
                let sym = format!("S{}", i);
                let f = base + i as f64; // 1..5 / 6..10
                let r = f / 100.0; // 0.01..0.05 / 0.06..0.10
                vals.push(factor_value(&sym, day, f, None));
                rets.insert(
                    (sym, NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap()),
                    r,
                );
            }
        }

        let ev = evaluate(&output(vals), &rets, 2);

        assert_eq!(ev.period_count, 2, "2 个交易日截面");
        assert_eq!(ev.ic_series.len(), 2);
        assert_eq!(ev.rank_ic_series.len(), 2);
        assert!(
            (ev.mean_ic - 1.0).abs() < 1e-9,
            "完全正相关 IC=1，实际 {}",
            ev.mean_ic
        );
        assert!(
            (ev.mean_rank_ic - 1.0).abs() < 1e-9,
            "秩相关同为 1，实际 {}",
            ev.mean_rank_ic
        );
        // 分位手算：因子 1..5 → 收益均值 0.03；因子 6..10 → 均值 0.08
        assert_eq!(ev.quantile_returns.len(), 2);
        assert!(
            (ev.quantile_returns[0] - 0.03).abs() < 1e-12,
            "底组收益均值应 0.03，实际 {}",
            ev.quantile_returns[0]
        );
        assert!(
            (ev.quantile_returns[1] - 0.08).abs() < 1e-12,
            "顶组收益均值应 0.08，实际 {}",
            ev.quantile_returns[1]
        );
        assert!(
            (ev.quantile_spread - 0.05).abs() < 1e-12,
            "顶组-底组收益差应 0.05，实际 {}",
            ev.quantile_spread
        );
        // date_range 取因子值首末日期
        assert_eq!(
            ev.date_range.0,
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap()
        );
        assert_eq!(
            ev.date_range.1,
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap()
        );
    }

    /// 混合 IC 符号：两日完全正相关、一日完全反相关 → mean_ic=1/3、std>0 → ic_ir≠0
    #[test]
    fn evaluate_mixed_ic_signs_produce_nonzero_ir() {
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        let days = ["2024-01-02", "2024-01-03", "2024-01-04"];
        for (di, day) in days.iter().enumerate() {
            for i in 0..5usize {
                let sym = format!("S{}", i);
                let f = 1.0 + i as f64;
                // 前两日收益与因子同序，第三日反序
                let r = if di < 2 {
                    0.01 * (i + 1) as f64
                } else {
                    0.01 * (5 - i) as f64
                };
                vals.push(factor_value(&sym, day, f, None));
                rets.insert(
                    (sym, NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap()),
                    r,
                );
            }
        }

        let ev = evaluate(&output(vals), &rets, 2);

        // mean = (1 + 1 - 1)/3
        assert!(
            (ev.mean_ic - 1.0 / 3.0).abs() < 1e-9,
            "混合三日 mean_ic 应 1/3，实际 {}",
            ev.mean_ic
        );
        // 样本 std = 2/√3 → IR = (1/3)/(2/√3) = √3/6
        let expected_ir = (1.0f64 / 3.0) / (2.0f64 / 3.0f64.sqrt());
        assert!(
            (ev.ic_ir - expected_ir).abs() < 1e-9,
            "ic_ir 应 {}，实际 {}",
            expected_ir,
            ev.ic_ir
        );
        assert!(
            (ev.mean_rank_ic - 1.0 / 3.0).abs() < 1e-9,
            "rank_ic 同构造应 1/3，实际 {}",
            ev.mean_rank_ic
        );
        // 正相关占优 → 顶组收益高于底组
        assert!(
            ev.quantile_spread > 0.0,
            "正相关主导下 spread 应为正，实际 {}",
            ev.quantile_spread
        );
    }

    /// 单日截面 pairs < 3 跳过该日 IC，但 period_count 仍按 by_date 口径计数
    #[test]
    fn evaluate_skips_dates_with_fewer_than_three_pairs() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        // d1：8 对（>=3，正常计算 IC）
        for i in 0..8usize {
            let sym = format!("A{}", i);
            let f = 1.0 + i as f64;
            vals.push(factor_value(&sym, "2024-01-02", f, None));
            rets.insert((sym, d1), f / 100.0);
        }
        // d2：仅 2 对（<3，跳过）→ aligned 合计 10 恰好过门槛
        for i in 0..2usize {
            let sym = format!("B{}", i);
            vals.push(factor_value(&sym, "2024-01-03", i as f64, None));
            rets.insert((sym, d2), 0.5 * i as f64);
        }

        let ev = evaluate(&output(vals), &rets, 2);

        assert_eq!(ev.ic_series.len(), 1, "仅 d1 产出截面 IC");
        assert_eq!(ev.ic_series[0].0, d1);
        assert!(
            ev.rank_ic_series.iter().all(|(d, _)| *d == d1),
            "rank_ic 也应只有 d1"
        );
        // period_count = by_date.len()（含被跳过的 d2）——记录当前口径
        assert_eq!(ev.period_count, 2);
    }

    /// 脏数据过滤：NaN 因子值 / NaN 前瞻收益 / 缺失前瞻收益 / PIT 违规（available_at > date）
    /// 全部剔除后，干净样本仍走主路径且不受污染
    #[test]
    fn evaluate_drops_nonfinite_and_missing_and_future_available_pairs() {
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        // d1：5 个干净对
        for i in 0..5usize {
            let sym = format!("A{}", i);
            vals.push(factor_value(&sym, "2024-01-02", 1.0 + i as f64, None));
            rets.insert(
                (sym, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
                0.01 + 0.01 * i as f64,
            );
        }
        // d2：5 个干净对 + 4 类脏数据各 1
        for i in 0..5usize {
            let sym = format!("B{}", i);
            vals.push(factor_value(&sym, "2024-01-03", 10.0 + i as f64, None));
            rets.insert((sym, d2), 0.10 + 0.01 * i as f64);
        }
        // 脏 1：因子值 NaN
        vals.push(factor_value("DX", "2024-01-03", f64::NAN, None));
        rets.insert(("DX".to_string(), d2), 0.99);
        // 脏 2：前瞻收益 NaN
        vals.push(factor_value("DY", "2024-01-03", 20.0, None));
        rets.insert(("DY".to_string(), d2), f64::NAN);
        // 脏 3：缺失前瞻收益（rets 无该键）
        vals.push(factor_value("DZ", "2024-01-03", 21.0, None));
        // 脏 4：PIT 违规——available_at > date 视为未来函数
        vals.push(factor_value("DP", "2024-01-03", 22.0, Some("2024-01-10")));
        rets.insert(("DP".to_string(), d2), 0.55);

        let ev = evaluate(&output(vals), &rets, 2);

        assert_eq!(ev.period_count, 2);
        assert_eq!(ev.ic_series.len(), 2, "两天各 5 个干净对（>=3）");
        // d2 干净对完全正相关 → IC=1，脏数据若混入会偏离
        assert!(
            (ev.ic_series[1].1 - 1.0).abs() < 1e-9,
            "脏数据过滤后 d2 截面应完全正相关，实际 {}",
            ev.ic_series[1].1
        );
        assert!(
            (ev.rank_ic_series[1].1 - 1.0).abs() < 1e-9,
            "rank IC 同样不受脏数据影响"
        );
    }

    /// 单日截面：ic_series 仅 1 个值 → std_of 单元素返回 0 → ic_ir/rank_ic_ir 记 0
    /// （std=0 防除零分支的精确覆盖——单元素序列无离散，std 恰为 0.0）
    #[test]
    fn evaluate_single_period_zero_ic_std_yields_zero_ir() {
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        for i in 0..10usize {
            let sym = format!("S{}", i);
            let f = 1.0 + i as f64;
            vals.push(factor_value(&sym, "2024-01-02", f, None));
            rets.insert(
                (sym, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
                f / 100.0,
            );
        }

        let ev = evaluate(&output(vals), &rets, 2);

        assert_eq!(ev.ic_series.len(), 1, "单日只有一个截面 IC");
        assert_eq!(ev.ic_ir, 0.0, "std(IC) 对单元素恒为 0 → IR 记 0");
        assert_eq!(ev.rank_ic_ir, 0.0);
        assert_eq!(ev.period_count, 1);
        // 5 分位 × 10 对：每分位恰 2 个 → 分位收益手算
        assert_eq!(ev.quantile_returns.len(), 2);
    }

    /// rank IC 与 Pearson IC 的分离场景：单调但非线性关系 → rank IC=1 而 IC<1
    #[test]
    fn evaluate_rank_ic_survives_nonlinear_monotone_relation() {
        let mut vals = Vec::new();
        let mut rets = HashMap::new();
        for i in 0..10usize {
            let sym = format!("S{}", i);
            let f = i as f64;
            // 指数型收益：单调但非线性 → Pearson IC < 1，Spearman rank IC = 1
            let r = (f * f) / 1000.0;
            vals.push(factor_value(&sym, "2024-01-02", f, None));
            rets.insert((sym, NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()), r);
        }

        let ev = evaluate(&output(vals), &rets, 2);

        assert!((ev.mean_rank_ic - 1.0).abs() < 1e-9, "单调关系 rank IC=1");
        assert!(
            ev.mean_ic < 0.999 && ev.mean_ic > 0.9,
            "非线性关系 Pearson IC 应 <1 但仍强正相关，实际 {}",
            ev.mean_ic
        );
    }
}
