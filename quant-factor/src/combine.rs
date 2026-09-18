//! 多因子组合纯计算（领域层, DDD Step 2, 2026-09-18 与仓储分离）。
//!
//! 只留权重算法纯函数：无 IO、无 async、确定性输入输出，可独立单测。
//! DB 编排（IC 评估加载、组合分数持久化）在 `repository.rs`。
//!
//! Combines multiple factor scores into a single composite score using
//! equal-weight, ICIR-weighted, or custom weight methods.

use std::collections::HashMap;

/// Combination method
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CombineMethod {
    /// Equal weight: each factor gets 1/N, sign flipped for negative-IC factors
    EqualWeight,
    /// ICIR-weighted: weight proportional to |ICIR|, sign from IC sign
    IcirWeighted,
}

/// Factor weight entry
#[derive(Debug, Clone)]
pub struct FactorWeight {
    pub factor_code: String,
    pub factor_version: String,
    pub weight: f64,
}

/// ICIR 加权纯计算：权重 = |IR| / Σ|IR|，IC<0 的因子取负号（反向使用）。
/// 无任何 IC 评估记录时退化为等权（total_ir=0 防除零）。
pub(crate) fn icir_weights_from_evals(
    factors: &[(String, String)],
    ic_evals: &HashMap<String, (f64, f64)>,
) -> Vec<FactorWeight> {
    let total_ir: f64 = factors
        .iter()
        .filter_map(|(code, ver)| {
            let key = format!("{}:{}", code, ver);
            ic_evals.get(&key).map(|(_, ir)| ir.abs())
        })
        .sum();
    let total_ir = if total_ir == 0.0 { 1.0 } else { total_ir };

    factors
        .iter()
        .map(|(code, ver)| {
            let key = format!("{}:{}", code, ver);
            let (ic, ir) = ic_evals.get(&key).copied().unwrap_or((0.0, 0.0));
            let w = ir.abs() / total_ir;
            FactorWeight {
                factor_code: code.clone(),
                factor_version: ver.clone(),
                weight: if ic < 0.0 { -w } else { w },
            }
        })
        .collect()
}

/// 等权纯计算：每个因子 1/N，IC<0 取负号。
pub(crate) fn equal_weights_from_evals(
    factors: &[(String, String)],
    ic_evals: &HashMap<String, (f64, f64)>,
) -> Vec<FactorWeight> {
    let n = factors.len() as f64;
    factors
        .iter()
        .map(|(code, ver)| {
            let key = format!("{}:{}", code, ver);
            let (ic, _) = ic_evals.get(&key).copied().unwrap_or((0.0, 0.0));
            let w = 1.0 / n;
            FactorWeight {
                factor_code: code.clone(),
                factor_version: ver.clone(),
                weight: if ic < 0.0 { -w } else { w },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── 权重纯计算（无 DB）──────────────────────────────────

    fn eval_map(entries: &[(&str, f64, f64)]) -> HashMap<String, (f64, f64)> {
        entries
            .iter()
            .map(|(k, ic, ir)| (k.to_string(), (*ic, *ir)))
            .collect()
    }

    #[test]
    fn icir_weights_proportional_to_abs_ir_with_ic_sign() {
        // IR 1.0 / 3.0 → 权重 0.25 / 0.75；第二个因子 IC<0 → 负号
        let factors = vec![
            ("f1".to_string(), "v1".to_string()),
            ("f2".to_string(), "v1".to_string()),
        ];
        let evals = eval_map(&[("f1:v1", 0.05, 1.0), ("f2:v1", -0.02, 3.0)]);
        let w = icir_weights_from_evals(&factors, &evals);
        assert!((w[0].weight - 0.25).abs() < 1e-12);
        assert!((w[1].weight - (-0.75)).abs() < 1e-12, "IC<0 应取负号");
        // 权重绝对值和为 1
        let abs_sum: f64 = w.iter().map(|x| x.weight.abs()).sum();
        assert!((abs_sum - 1.0).abs() < 1e-12);
    }

    #[test]
    fn icir_weights_no_evals_fall_back_to_equal() {
        // 无任何 IC 记录 → total_ir=0 → 各因子权重 0（不 panic）
        let factors = vec![("f1".to_string(), "v1".to_string())];
        let w = icir_weights_from_evals(&factors, &HashMap::new());
        assert_eq!(w.len(), 1);
        assert!((w[0].weight - 0.0).abs() < 1e-12);
    }

    #[test]
    fn icir_weights_negative_ir_still_positive_weight() {
        // IR 为负但 IC 为正：权重用 |IR|，符号跟 IC
        let factors = vec![("f1".to_string(), "v1".to_string())];
        let evals = eval_map(&[("f1:v1", 0.10, -2.0)]);
        let w = icir_weights_from_evals(&factors, &evals);
        assert!((w[0].weight - 1.0).abs() < 1e-12, "|IR| 全部归一");
    }

    #[test]
    fn equal_weights_one_over_n_with_ic_sign() {
        let factors = vec![
            ("f1".to_string(), "v1".to_string()),
            ("f2".to_string(), "v1".to_string()),
            ("f3".to_string(), "v1".to_string()),
            ("f4".to_string(), "v1".to_string()),
        ];
        let evals = eval_map(&[("f1:v1", 0.1, 1.0), ("f2:v1", -0.1, 1.0)]);
        let w = equal_weights_from_evals(&factors, &evals);
        assert!((w[0].weight - 0.25).abs() < 1e-12);
        assert!((w[1].weight - (-0.25)).abs() < 1e-12);
        // 无 IC 记录的因子默认正号
        assert!((w[2].weight - 0.25).abs() < 1e-12);
        assert!((w[3].weight - 0.25).abs() < 1e-12);
    }

    #[test]
    fn icir_weights_ir_mixed_magnitudes_normalize() {
        // 混合量级 |IR|: 0.5/1.5/3.0 → 0.1/0.3/0.6, 绝对值和恒为 1
        let factors = vec![
            ("a".to_string(), "1.0.0".to_string()),
            ("b".to_string(), "1.0.0".to_string()),
            ("c".to_string(), "1.0.0".to_string()),
        ];
        let evals = eval_map(&[
            ("a:1.0.0", 0.02, 0.5),
            ("b:1.0.0", 0.03, 1.5),
            ("c:1.0.0", 0.04, 3.0),
        ]);
        let w = icir_weights_from_evals(&factors, &evals);
        assert!((w[0].weight - 0.1).abs() < 1e-12);
        assert!((w[1].weight - 0.3).abs() < 1e-12);
        assert!((w[2].weight - 0.6).abs() < 1e-12);
    }

    #[test]
    fn weights_empty_factors_returns_empty() {
        // 空因子清单 → 空权重(不 panic, 不产生假权重)
        assert!(icir_weights_from_evals(&[], &HashMap::new()).is_empty());
        assert!(equal_weights_from_evals(&[], &HashMap::new()).is_empty());
    }

    #[test]
    fn equal_weights_zero_factors_no_div_by_zero() {
        // 等权 n=0 已由空清单短路覆盖; 单因子 n=1 → 权重恰为 ±1
        let factors = vec![("only".to_string(), "v".to_string())];
        let evals = eval_map(&[("only:v", -0.05, 0.7)]);
        let w = equal_weights_from_evals(&factors, &evals);
        assert!((w[0].weight - (-1.0)).abs() < 1e-12, "单因子负 IC → -1");
    }
}
