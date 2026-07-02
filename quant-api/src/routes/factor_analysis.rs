//! 因子强度分桶分析:按 regime/行业/市值/流动性分解因子 IC/IR。
//! 蓝图§4.2 最大化已知 alpha:深挖 financial_quality 信号最强子域。

use std::collections::HashMap;
use chrono::NaiveDate;
use serde_json::{json, Value};
use axum::{extract::State, response::IntoResponse, Json};
use std::sync::Arc;

use crate::AppState;

/// 单组 IC 评估结果(分桶或全市场)。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IcResult {
    pub mean_ic: f64,
    pub ic_ir: f64,
    pub mean_rank_ic: f64,
    pub rank_ic_ir: f64,
    pub n_obs: usize,
}

/// 算一组 aligned(factor_value, forward_return) 数据的 IC/IR。
/// mean_ic = 截面 IC 的均值(每日 IC 的平均);ic_ir = mean_ic / std_ic。
/// 简化:按日分组算截面 rank IC,再聚合(与 evaluate.rs 口径一致)。
pub fn compute_ic(
    factor_values: &[(String, NaiveDate, f64)], // (symbol, date, factor_value)
    forward_returns: &HashMap<(String, NaiveDate), f64>,
) -> IcResult {
    // 1. 按日分组,对齐 factor_value + forward_return
    let mut by_date: HashMap<NaiveDate, Vec<(String, f64, f64)>> = HashMap::new();
    for (symbol, date, fv) in factor_values {
        if !fv.is_finite() {
            continue;
        }
        if let Some(&fw_ret) = forward_returns.get(&(symbol.clone(), *date)) {
            if fw_ret.is_finite() {
                by_date.entry(*date).or_default().push((symbol.clone(), *fv, fw_ret));
            }
        }
    }
    if by_date.is_empty() {
        return IcResult::default();
    }

    // 2. 逐日算 rank IC(Spearman),收集序列
    let mut rank_ic_series: Vec<f64> = Vec::new();
    let mut n_total: usize = 0;
    for (_date, rows) in &by_date {
        if rows.len() < 5 {
            continue; // 截面样本太少,跳过
        }
        let ic = spearman_rank_ic(&rows.iter().map(|(s, fv, fr)| (fv.clone(), fr.clone())).collect::<Vec<_>>());
        if ic.is_finite() {
            rank_ic_series.push(ic);
        }
        n_total += rows.len();
    }
    if rank_ic_series.is_empty() {
        return IcResult { n_obs: n_total, ..Default::default() };
    }

    // 3. 聚合:mean/std/IR
    let mean_rank_ic = mean(&rank_ic_series);
    let std_ic = std(&rank_ic_series);
    let rank_ic_ir = if std_ic > 1e-12 { mean_rank_ic / std_ic } else { 0.0 };

    // mean_ic 用 Pearson IC(同样逐日算)
    let mut pearson_series: Vec<f64> = Vec::new();
    for (_date, rows) in &by_date {
        if rows.len() < 5 {
            continue;
        }
        let ic = pearson_ic(&rows.iter().map(|(_, fv, fr)| (fv.clone(), fr.clone())).collect::<Vec<_>>());
        if ic.is_finite() {
            pearson_series.push(ic);
        }
    }
    let mean_ic = mean(&pearson_series);
    let std_p = std(&pearson_series);
    let ic_ir = if std_p > 1e-12 { mean_ic / std_p } else { 0.0 };

    IcResult { mean_ic, ic_ir, mean_rank_ic, rank_ic_ir, n_obs: n_total }
}

/// 按分桶函数对 aligned 数据分组算 IC。
/// bucket_key_fn: (symbol, date) -> Option<String>(分桶名),None 则跳过。
pub fn compute_ic_by_bucket<F>(
    factor_values: &[(String, NaiveDate, f64)],
    forward_returns: &HashMap<(String, NaiveDate), f64>,
    bucket_key_fn: F,
) -> HashMap<String, IcResult>
where
    F: Fn(&str, NaiveDate) -> Option<String>,
{
    let mut buckets: HashMap<String, Vec<(String, NaiveDate, f64)>> = HashMap::new();
    for (symbol, date, fv) in factor_values {
        if let Some(key) = bucket_key_fn(symbol, *date) {
            buckets.entry(key).or_default().push((symbol.clone(), *date, *fv));
        }
    }
    buckets
        .into_iter()
        .map(|(key, fvs)| (key, compute_ic(&fvs, forward_returns)))
        .collect()
}

/// Spearman rank IC:对 factor_value 和 forward_return 分别排名后算 Pearson。
fn spearman_rank_ic(pairs: &[(f64, f64)]) -> f64 {
    let ranks_fv = rank(pairs.iter().map(|(fv, _)| *fv).collect::<Vec<_>>().as_slice());
    let ranks_fr = rank(pairs.iter().map(|(_, fr)| *fr).collect::<Vec<_>>().as_slice());
    pearson_ic(&ranks_fv.iter().zip(ranks_fr.iter()).map(|(a, b)| (*a, *b)).collect::<Vec<_>>())
}

/// Pearson 相关系数。
fn pearson_ic(pairs: &[(f64, f64)]) -> f64 {
    let n = pairs.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let mean_x = mean(&pairs.iter().map(|(x, _)| *x).collect::<Vec<_>>());
    let mean_y = mean(&pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>());
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (x, y) in pairs {
        cov += (x - mean_x) * (y - mean_y);
        var_x += (x - mean_x).powi(2);
        var_y += (y - mean_y).powi(2);
    }
    let denom = (var_x * var_y).sqrt();
    if denom > 1e-12 { cov / denom } else { 0.0 }
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() { 0.0 } else { v.iter().sum::<f64>() / v.len() as f64 }
}

fn std(v: &[f64]) -> f64 {
    if v.len() < 2 { return 0.0; }
    let m = mean(v);
    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
}

/// 简单排名(平均秩处理 ties)。
fn rank(values: &[f64]) -> Vec<f64> {
    let mut indexed: Vec<(usize, f64)> = values.iter().enumerate().map(|(i, &v)| (i, v)).collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0; values.len()];
    let mut i = 0;
    while i < indexed.len() {
        let mut j = i;
        while j + 1 < indexed.len() && indexed[j + 1].1 == indexed[i].1 {
            j += 1;
        }
        let avg_rank = (i + j) as f64 / 2.0 + 1.0;
        for k in i..=j {
            ranks[indexed[k].0] = avg_rank;
        }
        i = j + 1;
    }
    ranks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pearson_ic_perfect_positive() {
        let pairs = vec![(1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)];
        let ic = pearson_ic(&pairs);
        assert!((ic - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_pearson_ic_perfect_negative() {
        let pairs = vec![(1.0, 8.0), (2.0, 6.0), (3.0, 4.0), (4.0, 2.0)];
        let ic = pearson_ic(&pairs);
        assert!((ic - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn test_rank_ties() {
        let r = rank(&[3.0, 1.0, 2.0, 1.0]);
        // 1.0 出现两次,平均秩 (1+2)/2=1.5;2.0 秩 3;3.0 秩 4
        assert!((r[0] - 4.0).abs() < 1e-9);
        assert!((r[1] - 1.5).abs() < 1e-9);
        assert!((r[2] - 3.0).abs() < 1e-9);
        assert!((r[3] - 1.5).abs() < 1e-9);
    }

    #[test]
    fn test_compute_ic_basic() {
        let fvs = vec![
            ("A".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 1.0),
            ("B".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 2.0),
            ("C".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 3.0),
            ("D".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 4.0),
            ("E".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 5.0),
        ];
        let mut fr = HashMap::new();
        fr.insert(("A".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.01);
        fr.insert(("B".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.02);
        fr.insert(("C".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.03);
        fr.insert(("D".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.04);
        fr.insert(("E".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.05);
        let r = compute_ic(&fvs, &fr);
        assert!((r.mean_ic - 1.0).abs() < 1e-9); // 完美正相关
        assert_eq!(r.n_obs, 5);
    }
}
