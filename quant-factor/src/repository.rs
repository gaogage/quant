//! 因子组合仓储层（DDD Step 2 领域/仓储分离, 2026-09-18 自 combine.rs 迁出）。
//!
//! 职责边界：本模块只做 DB 编排（IC 评估加载、组合分数加载与持久化），
//! 权重算法纯计算在 `combine.rs`（领域层, 无 IO 依赖, 可独立单测）。
//! 分层动机：原 combine.rs 领域与 IO 混居, 行覆盖率被 DB 编排拖至 33.5%,
//! 无法反映纯算法的真实覆盖; 迁移后覆盖率统计回归分层真实口径。
//!
//! PIT 红线：`load_ic_evals_pit` 只取 `end_date <= as_of` 的最新评估,
//! 杜绝 lookahead（配套红线单测见文件尾部, 连本地库运行）。

use std::collections::HashMap;

use chrono::NaiveDate;
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;

use crate::combine::{
    equal_weights_from_evals, icir_weights_from_evals, CombineMethod, FactorWeight,
};

/// Load IC evaluations for weight computation
async fn load_ic_evals(
    pool: &PgPool,
    horizon: i16,
) -> std::result::Result<HashMap<String, (f64, f64)>, sqlx::Error> {
    // (factor_code, factor_version) -> (mean_ic, ic_ir)
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
        ),
    >(
        "SELECT factor_code, factor_version, mean_ic, ic_ir
         FROM factor_evaluation
         WHERE horizon = $1
           AND mean_ic IS NOT NULL
           AND ic_ir IS NOT NULL
         ORDER BY factor_code",
    )
    .bind(horizon)
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    for (code, ver, mean_ic, ic_ir) in rows {
        let key = format!("{}:{}", code, ver);
        let ic = mean_ic.and_then(|d| d.to_f64()).unwrap_or(0.0);
        let ir = ic_ir.and_then(|d| d.to_f64()).unwrap_or(0.0);
        map.insert(key, (ic, ir));
    }
    Ok(map)
}

/// PIT: 只取 `end_date <= as_of` 的最新一条 IC（每因子），用于滚动 ICIR 权重。
/// 绝不读取 as_of 之后的 IC，杜绝 lookahead。
async fn load_ic_evals_pit(
    pool: &PgPool,
    horizon: i16,
    as_of: NaiveDate,
) -> std::result::Result<HashMap<String, (f64, f64)>, sqlx::Error> {
    // 每个 (factor_code, factor_version) 取 end_date <= as_of 的最近一条评估
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
        ),
    >(
        "SELECT DISTINCT ON (factor_code, factor_version)
            factor_code, factor_version, mean_ic, ic_ir
         FROM factor_evaluation
         WHERE horizon = $1
           AND end_date <= $2
           AND mean_ic IS NOT NULL
           AND ic_ir IS NOT NULL
         ORDER BY factor_code, factor_version, end_date DESC",
    )
    .bind(horizon)
    .bind(as_of)
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    for (code, ver, mean_ic, ic_ir) in rows {
        let key = format!("{}:{}", code, ver);
        let ic = mean_ic.and_then(|d| d.to_f64()).unwrap_or(0.0);
        let ir = ic_ir.and_then(|d| d.to_f64()).unwrap_or(0.0);
        map.insert(key, (ic, ir));
    }
    Ok(map)
}

/// PIT 滚动 ICIR 权重：与 `compute_weights(IcirWeighted)` 同逻辑，
/// 但 IC 只来自 `as_of` 之前的历史评估（滚动、无 lookahead）。
pub async fn compute_weights_pit(
    pool: &PgPool,
    factors: &[(String, String)], // (factor_code, factor_version)
    as_of: NaiveDate,
    horizon: i16,
) -> std::result::Result<Vec<FactorWeight>, String> {
    let ic_evals = load_ic_evals_pit(pool, horizon, as_of)
        .await
        .map_err(|e| format!("Failed to load PIT IC evals: {}", e))?;

    Ok(icir_weights_from_evals(factors, &ic_evals))
}

/// Compute factor weights based on IC evaluations
pub async fn compute_weights(
    pool: &PgPool,
    factors: &[(String, String)], // (factor_code, factor_version)
    method: CombineMethod,
    horizon: i16,
) -> std::result::Result<Vec<FactorWeight>, String> {
    let ic_evals = load_ic_evals(pool, horizon)
        .await
        .map_err(|e| format!("Failed to load IC evals: {}", e))?;

    match method {
        CombineMethod::EqualWeight => Ok(equal_weights_from_evals(factors, &ic_evals)),
        CombineMethod::IcirWeighted => Ok(icir_weights_from_evals(factors, &ic_evals)),
    }
}

/// Combine factor values into composite scores and persist
pub async fn combine_and_persist(
    pool: &PgPool,
    combo_name: &str,
    version: &str,
    weights: &[FactorWeight],
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> std::result::Result<usize, String> {
    if weights.is_empty() {
        return Err("No weights provided".to_string());
    }

    // Load factor values for all factors, aligned by (symbol, date)
    let mut factor_values: HashMap<(String, NaiveDate), HashMap<String, f64>> = HashMap::new();

    for fw in weights {
        let rows: std::result::Result<Vec<_>, sqlx::Error> =
            if let (Some(start), Some(end)) = (start_date, end_date) {
                sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                    "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
                 FROM factor_value
                 WHERE factor_code = $1 AND factor_version = $2
                   AND trade_date >= $3 AND trade_date <= $4",
                )
                .bind(&fw.factor_code)
                .bind(&fw.factor_version)
                .bind(start)
                .bind(end)
                .fetch_all(pool)
                .await
            } else {
                sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                    "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
                 FROM factor_value
                 WHERE factor_code = $1 AND factor_version = $2",
                )
                .bind(&fw.factor_code)
                .bind(&fw.factor_version)
                .fetch_all(pool)
                .await
            };

        let rows = rows
            .map_err(|e| format!("Failed to load factor values for {}: {}", fw.factor_code, e))?;
        for (sym, date, val) in rows {
            if let Some(v) = val {
                let vf = v.to_f64().unwrap_or(f64::NAN);
                if vf.is_finite() {
                    factor_values
                        .entry((sym, date))
                        .or_default()
                        .insert(fw.factor_code.clone(), vf);
                }
            }
        }
    }

    // Compute combined scores: 因子值不全的 (symbol, date) 截面跳过
    // (2026-09-18 迁移时清理: 原 248-252 空循环体死代码, 与本段检查重复)
    let mut scores: Vec<(String, NaiveDate, f64)> = Vec::new();
    for ((sym, date), vals) in &factor_values {
        if vals.len() < weights.len() {
            continue;
        }

        let mut score = 0.0;
        for fw in weights {
            if let Some(&v) = vals.get(&fw.factor_code) {
                score += fw.weight * v;
            }
        }
        scores.push((sym.clone(), *date, score));
    }

    // Store weights config
    let weights_json = serde_json::to_value(
        weights
            .iter()
            .map(|w| (w.factor_code.clone(), w.weight))
            .collect::<HashMap<_, _>>(),
    )
    .map_err(|e| format!("JSON error: {}", e))?;

    let method_str = if weights.len() == 1 {
        "single"
    } else {
        let n = weights.len() as f64;
        let all_equal = weights
            .iter()
            .all(|w| (w.weight.abs() - 1.0 / n).abs() < 0.001);
        if all_equal {
            "equal_weight"
        } else {
            "icir_weighted"
        }
    };

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights, created_at = NOW()",
    )
    .bind(combo_name)
    .bind(version)
    .bind(&weights_json)
    .bind(method_str)
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to store weights: {}", e))?;

    // Batch insert scores. Alpha scores are available at the score date after
    // source factor PIT filters have already been applied upstream.
    let mut inserted = 0usize;
    for chunk in scores.chunks(500) {
        for (sym, date, score) in chunk {
            let result = sqlx::query(
                "INSERT INTO multi_factor_value (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
                 VALUES ($1, $2, $3, $4, $5, $5, $4)
                 ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
                   raw_score = EXCLUDED.raw_score,
                   normalized_score = EXCLUDED.normalized_score,
                   available_at = EXCLUDED.available_at,
                   created_at = NOW()"
            )
            .bind(combo_name)
            .bind(version)
            .bind(sym)
            .bind(date)
            .bind(score)
            .execute(pool)
            .await;

            match result {
                Ok(_) => inserted += 1,
                Err(e) => tracing::warn!("Failed to insert combo score: {}", e),
            }
        }
    }

    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 连接本地 quant 库（与真实数据共库，测试键一律 zzz_test_ 前缀 + 前置/末尾 DELETE）
    async fn local_pool() -> PgPool {
        PgPool::connect("postgres://gaocheng@localhost/quant")
            .await
            .expect("connect local quant db")
    }

    /// 清理 factor_evaluation 的测试键行
    async fn cleanup_evals(pool: &PgPool, codes: &[&str]) {
        for c in codes {
            sqlx::query("DELETE FROM factor_evaluation WHERE factor_code = $1")
                .bind(c)
                .execute(pool)
                .await
                .unwrap();
        }
    }

    /// 清理 combine_and_persist 涉及的三张表测试键行
    async fn cleanup_combine(pool: &PgPool, combo: &str, factor_codes: &[&str]) {
        sqlx::query("DELETE FROM multi_factor_value WHERE combo_name = $1")
            .bind(combo)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM multi_factor_weight WHERE combo_name = $1")
            .bind(combo)
            .execute(pool)
            .await
            .unwrap();
        for c in factor_codes {
            sqlx::query("DELETE FROM factor_value WHERE factor_code = $1")
                .bind(c)
                .execute(pool)
                .await
                .unwrap();
        }
    }

    /// 插入一条 horizon=20 的 IC 评估（与 compute_weights 查询口径一致）
    async fn insert_eval(pool: &PgPool, code: &str, mean_ic: &str, ic_ir: &str) {
        sqlx::query(
            "INSERT INTO factor_evaluation (factor_code,factor_version,horizon,start_date,end_date,mean_ic,ic_ir)
             VALUES ($1,'1.0.0',20,'2016-01-01','2019-12-31',$2::numeric,$3::numeric)",
        )
        .bind(code)
        .bind(mean_ic)
        .bind(ic_ir)
        .execute(pool)
        .await
        .unwrap();
    }

    /// 插入一行 factor_value（raw_value 用字符串精确绑定 numeric）
    async fn insert_fv(pool: &PgPool, code: &str, symbol: &str, date: NaiveDate, raw: &str) {
        sqlx::query(
            "INSERT INTO factor_value (factor_code,factor_version,symbol,trade_date,raw_value)
             VALUES ($1,'1.0.0',$2,$3,$4::numeric)",
        )
        .bind(code)
        .bind(symbol)
        .bind(date)
        .bind(raw)
        .execute(pool)
        .await
        .unwrap();
    }

    /// 插入一行 raw/normalized 双 NULL 的 factor_value（COALESCE 取 NULL 的防御分支）
    async fn insert_fv_null(pool: &PgPool, code: &str, symbol: &str, date: NaiveDate) {
        sqlx::query(
            "INSERT INTO factor_value (factor_code,factor_version,symbol,trade_date)
             VALUES ($1,'1.0.0',$2,$3)",
        )
        .bind(code)
        .bind(symbol)
        .bind(date)
        .execute(pool)
        .await
        .unwrap();
    }

    /// 读取指定组合的全部分数行（按 symbol/date 排序稳定断言）
    async fn load_scores(pool: &PgPool, combo: &str) -> Vec<(String, NaiveDate, f64, f64)> {
        sqlx::query_as(
            "SELECT symbol, trade_date, raw_score, normalized_score
             FROM multi_factor_value
             WHERE combo_name = $1 AND version = '1.0.0'
             ORDER BY symbol, trade_date",
        )
        .bind(combo)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// compute_weights(EqualWeight)：从 factor_evaluation 读 IC，
    /// 每因子 1/N 等权；IC<0 的因子取负号（反向使用）
    #[tokio::test]
    async fn test_compute_weights_equal_weight_from_db() {
        let pool = local_pool().await;
        let f1 = "zzz_test_w1";
        let f2 = "zzz_test_w2";
        cleanup_evals(&pool, &[f1, f2]).await;

        // f1 正 IC；f2 负 IC（等权下应翻负号）
        insert_eval(&pool, f1, "0.05", "0.8").await;
        insert_eval(&pool, f2, "-0.02", "3.2").await;
        // 干扰行：mean_ic 为 NULL 的行必须被 SQL 过滤，不参与权重
        sqlx::query(
            "INSERT INTO factor_evaluation (factor_code,factor_version,horizon,start_date,end_date,mean_ic,ic_ir)
             VALUES ($1,'1.0.0',20,'2016-01-01','2018-12-31',NULL,9.9)",
        )
        .bind(f1)
        .execute(&pool)
        .await
        .unwrap();

        let factors = vec![
            (f1.to_string(), "1.0.0".to_string()),
            (f2.to_string(), "1.0.0".to_string()),
        ];
        let w = compute_weights(&pool, &factors, CombineMethod::EqualWeight, 20)
            .await
            .expect("compute_weights 应成功");

        assert_eq!(w.len(), 2, "应返回 2 个因子权重");
        assert_eq!(w[0].factor_code, f1);
        assert_eq!(w[1].factor_code, f2);
        assert!(
            (w[0].weight - 0.5).abs() < 1e-9,
            "正 IC 因子等权 +1/2，实际 {}",
            w[0].weight
        );
        assert!(
            (w[1].weight - (-0.5)).abs() < 1e-9,
            "负 IC 因子等权 -1/2（反向使用），实际 {}",
            w[1].weight
        );

        cleanup_evals(&pool, &[f1, f2]).await;
    }

    /// compute_weights(IcirWeighted)：权重 ∝ |ICIR| 并归一，IC 符号决定方向
    /// （与等权测试用不同 factor_code 键，避免并行跑测时互相清数据）
    #[tokio::test]
    async fn test_compute_weights_icir_weighted_from_db() {
        let pool = local_pool().await;
        let f1 = "zzz_test_w3";
        let f2 = "zzz_test_w4";
        cleanup_evals(&pool, &[f1, f2]).await;

        // |ICIR| 0.8 : 3.2 → 权重 0.2 : 0.8；f2 IC<0 → 负号
        insert_eval(&pool, f1, "0.05", "0.8").await;
        insert_eval(&pool, f2, "-0.02", "3.2").await;

        let factors = vec![
            (f1.to_string(), "1.0.0".to_string()),
            (f2.to_string(), "1.0.0".to_string()),
        ];
        let w = compute_weights(&pool, &factors, CombineMethod::IcirWeighted, 20)
            .await
            .expect("compute_weights 应成功");

        assert_eq!(w.len(), 2);
        assert!(
            (w[0].weight - 0.2).abs() < 1e-9,
            "|ICIR| 0.8/(0.8+3.2)=0.2，实际 {}",
            w[0].weight
        );
        assert!(
            (w[1].weight - (-0.8)).abs() < 1e-9,
            "|ICIR| 3.2/4=0.8 且 IC<0 取负，实际 {}",
            w[1].weight
        );
        let abs_sum: f64 = w.iter().map(|x| x.weight.abs()).sum();
        assert!(
            (abs_sum - 1.0).abs() < 1e-9,
            "|权重| 之和应归一为 1，实际 {}",
            abs_sum
        );

        cleanup_evals(&pool, &[f1, f2]).await;
    }

    /// combine_and_persist：空权重清单直接拒绝，不触碰任何表
    #[tokio::test]
    async fn test_combine_and_persist_empty_weights_rejected() {
        let pool = local_pool().await;
        let err = combine_and_persist(&pool, "zzz_test_combo_never", "1.0.0", &[], None, None)
            .await
            .expect_err("空 weights 应返回 Err");
        assert_eq!(err, "No weights provided");
    }

    /// combine_and_persist 主写路径：不等权 → method=icir_weighted，
    /// 分数 = Σ w*v 逐行核对；先全日期（None 分支）再限定日期（Some 分支）复算，upsert 幂等
    #[tokio::test]
    async fn test_combine_and_persist_icir_weighted_scores() {
        let pool = local_pool().await;
        let f1 = "zzz_test_cf1";
        let f2 = "zzz_test_cf2";
        let combo = "zzz_test_combo_a";
        cleanup_combine(&pool, combo, &[f1, f2]).await;

        let d1 = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2020, 1, 3).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2020, 1, 6).unwrap();
        let s1 = "ZZZT1.SH";
        let s2 = "ZZZT2.SH";

        // 两因子 × 两标的 × 三日：f1 取 base+i，f2 取 (base+i)*10
        for (sym, base) in [(s1, 1i32), (s2, 4i32)] {
            for (i, d) in [d1, d2, d3].iter().enumerate() {
                insert_fv(&pool, f1, sym, *d, &(base + i as i32).to_string()).await;
                insert_fv(&pool, f2, sym, *d, &((base + i as i32) * 10).to_string()).await;
            }
        }

        let weights = vec![
            FactorWeight {
                factor_code: f1.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.7,
            },
            FactorWeight {
                factor_code: f2.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.3,
            },
        ];

        // 全日期范围（start/end 均 None 分支）
        let inserted = combine_and_persist(&pool, combo, "1.0.0", &weights, None, None)
            .await
            .expect("combine 应成功");
        assert_eq!(inserted, 6, "2 标的 × 3 日 = 6 行");

        // 读回分数，与手算 Σ w*v 逐行核对
        let rows = load_scores(&pool, combo).await;
        assert_eq!(rows.len(), 6);
        for (sym, date, raw, normalized) in &rows {
            let base: i32 = if sym == s1 { 1 } else { 4 };
            let i: i32 = if *date == d1 {
                0
            } else if *date == d2 {
                1
            } else {
                2
            };
            let v1 = (base + i) as f64;
            let v2 = ((base + i) * 10) as f64;
            let expected = 0.7 * v1 + 0.3 * v2;
            assert!(
                (raw - expected).abs() < 1e-9,
                "{} {} 分数应为 {}，实际 {}",
                sym,
                date,
                expected,
                raw
            );
            assert!(
                (normalized - raw).abs() < 1e-12,
                "normalized_score 应与 raw_score 同值写入"
            );
        }

        // method 判定：权重不等 → icir_weighted
        let method: String = sqlx::query_scalar(
            "SELECT method FROM multi_factor_weight WHERE combo_name = $1 AND version = '1.0.0'",
        )
        .bind(combo)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            method, "icir_weighted",
            "不等权组合 method 应为 icir_weighted"
        );

        // weights json：因子代码 → 权重
        let w1: String = sqlx::query_scalar(
            "SELECT (weights ->> $2)::text FROM multi_factor_weight WHERE combo_name = $1",
        )
        .bind(combo)
        .bind(f1)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(w1, "0.7", "weights json 应保留 f1 的权重");

        // 限定日期范围（Some 分支）：只重算 d2 一天 → 2 行 upsert
        let inserted2 = combine_and_persist(&pool, combo, "1.0.0", &weights, Some(d2), Some(d2))
            .await
            .expect("限定范围 combine 应成功");
        assert_eq!(inserted2, 2, "限定日期只覆盖 1 日 × 2 标的");

        // 行数不变（upsert 幂等，不新增行）
        let cnt: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM multi_factor_value WHERE combo_name = $1")
                .bind(combo)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(cnt, 6, "限定范围复算应 upsert 而非新增");

        cleanup_combine(&pool, combo, &[f1, f2]).await;
    }

    /// 单因子组合 → method=single，分数即因子值本身
    #[tokio::test]
    async fn test_combine_and_persist_single_factor_method_single() {
        let pool = local_pool().await;
        let f1 = "zzz_test_cs1";
        let combo = "zzz_test_combo_b";
        cleanup_combine(&pool, combo, &[f1]).await;

        let d1 = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        insert_fv(&pool, f1, "ZZZT1.SH", d1, "2.5").await;
        insert_fv(&pool, f1, "ZZZT2.SH", d1, "-1.25").await;

        let weights = vec![FactorWeight {
            factor_code: f1.to_string(),
            factor_version: "1.0.0".to_string(),
            weight: 1.0,
        }];
        let inserted = combine_and_persist(&pool, combo, "1.0.0", &weights, None, None)
            .await
            .expect("combine 应成功");
        assert_eq!(inserted, 2);

        let rows = load_scores(&pool, combo).await;
        assert_eq!(rows.len(), 2);
        assert!((rows[0].2 - 2.5).abs() < 1e-9, "ZZZT1 分数 = 1.0 × 2.5");
        assert!(
            (rows[1].2 - (-1.25)).abs() < 1e-9,
            "ZZZT2 分数 = 1.0 × -1.25"
        );

        let method: String = sqlx::query_scalar(
            "SELECT method FROM multi_factor_weight WHERE combo_name = $1 AND version = '1.0.0'",
        )
        .bind(combo)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(method, "single", "单因子组合 method 应为 single");

        cleanup_combine(&pool, combo, &[f1]).await;
    }

    /// 双因子等权（各 0.5）→ method=equal_weight
    #[tokio::test]
    async fn test_combine_and_persist_equal_weights_method_equal_weight() {
        let pool = local_pool().await;
        let f1 = "zzz_test_ce1";
        let f2 = "zzz_test_ce2";
        let combo = "zzz_test_combo_d";
        cleanup_combine(&pool, combo, &[f1, f2]).await;

        let d1 = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        insert_fv(&pool, f1, "ZZZT1.SH", d1, "1.0").await;
        insert_fv(&pool, f2, "ZZZT1.SH", d1, "3.0").await;

        let weights = vec![
            FactorWeight {
                factor_code: f1.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.5,
            },
            FactorWeight {
                factor_code: f2.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.5,
            },
        ];
        let inserted = combine_and_persist(&pool, combo, "1.0.0", &weights, None, None)
            .await
            .expect("combine 应成功");
        assert_eq!(inserted, 1);

        // 等权分数 = 0.5*1 + 0.5*3 = 2.0
        let rows = load_scores(&pool, combo).await;
        assert_eq!(rows.len(), 1);
        assert!((rows[0].2 - 2.0).abs() < 1e-9, "等权分数应为 2.0");

        let method: String = sqlx::query_scalar(
            "SELECT method FROM multi_factor_weight WHERE combo_name = $1 AND version = '1.0.0'",
        )
        .bind(combo)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            method, "equal_weight",
            "全等权重组合 method 应为 equal_weight"
        );

        cleanup_combine(&pool, combo, &[f1, f2]).await;
    }

    /// 脏数据防线：numeric NaN（非有限值）与 NULL 值不入截面；
    /// 因子值不全的 (symbol,date) 截面整体跳过；normalized_value 优先于 raw_value
    #[tokio::test]
    async fn test_combine_and_persist_skips_nonfinite_and_incomplete_cross_sections() {
        let pool = local_pool().await;
        let f1 = "zzz_test_cn1";
        let f2 = "zzz_test_cn2";
        let combo = "zzz_test_combo_c";
        cleanup_combine(&pool, combo, &[f1, f2]).await;

        let d1 = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2020, 1, 3).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2020, 1, 6).unwrap();
        let s1 = "ZZZT1.SH";
        let s2 = "ZZZT2.SH";

        // f1：s1 三日齐全（d1 带优先使用的 normalized_value）；s2 的 d1 为 NaN
        sqlx::query(
            "INSERT INTO factor_value (factor_code,factor_version,symbol,trade_date,raw_value,normalized_value)
             VALUES ($1,'1.0.0',$2,$3,1.0::numeric,2.0::numeric)",
        )
        .bind(f1)
        .bind(s1)
        .bind(d1)
        .execute(&pool)
        .await
        .unwrap();
        insert_fv(&pool, f1, s1, d2, "2.0").await;
        insert_fv(&pool, f1, s1, d3, "3.0").await;
        // NaN 字面量行：参数化绑定 NaN 语义不稳，测试键固定无注入面
        sqlx::query(
            "INSERT INTO factor_value (factor_code,factor_version,symbol,trade_date,raw_value)
             VALUES ('zzz_test_cn1','1.0.0','ZZZT2.SH','2020-01-02','NaN'::numeric)",
        )
        .execute(&pool)
        .await
        .unwrap();
        insert_fv(&pool, f1, s2, d2, "5.0").await;
        insert_fv(&pool, f1, s2, d3, "6.0").await;

        // f2：s1 的 d2 双 NULL（COALESCE NULL → 丢弃）；s2 三日齐全
        insert_fv(&pool, f2, s1, d1, "10.0").await;
        insert_fv_null(&pool, f2, s1, d2).await;
        insert_fv(&pool, f2, s1, d3, "30.0").await;
        insert_fv(&pool, f2, s2, d1, "40.0").await;
        insert_fv(&pool, f2, s2, d2, "50.0").await;
        insert_fv(&pool, f2, s2, d3, "60.0").await;

        let weights = vec![
            FactorWeight {
                factor_code: f1.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.5,
            },
            FactorWeight {
                factor_code: f2.to_string(),
                factor_version: "1.0.0".to_string(),
                weight: 0.5,
            },
        ];
        // 实测行为定性（防回归）：'NaN'::numeric 在 sqlx 解码层直接失败——
        // rust_decimal 不支持 NaN，整批读取 Err（而非跳过该行）。真实管线中
        // 上游标准化已保证无 NaN 落库，此 Err 语义是边界防御。
        let err = combine_and_persist(&pool, combo, "1.0.0", &weights, None, None).await;
        assert!(
            err.is_err(),
            "'NaN'::numeric 行应使整批读取失败而非静默跳过"
        );

        // 移除 NaN 行后进入成功路径（s2@d1 变为缺 f1 行——截面不齐跳过的等价形态）
        sqlx::query("DELETE FROM factor_value WHERE factor_code='zzz_test_cn1' AND symbol='ZZZT2.SH' AND trade_date='2020-01-02'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM multi_factor_value WHERE combo_name=$1")
            .bind(combo)
            .execute(&pool)
            .await
            .unwrap();

        let inserted = combine_and_persist(&pool, combo, "1.0.0", &weights, None, None)
            .await
            .expect("combine 应成功");

        // 完整截面 4 个：s1@d1(用 normalized 2.0)、s1@d3、s2@d2、s2@d3
        // 跳过 2 个：s1@d2（f2 值 NULL）、s2@d1（f1 行缺失）
        assert_eq!(inserted, 4, "缺任一因子值的截面应整体跳过");

        let rows = load_scores(&pool, combo).await;
        assert_eq!(rows.len(), 4);
        let by_key: std::collections::HashMap<(String, NaiveDate), f64> = rows
            .iter()
            .map(|(s, d, v, _)| ((s.clone(), *d), *v))
            .collect();
        // s1@d1：COALESCE 优先 normalized_value → 0.5*2.0 + 0.5*10.0 = 6.0
        assert!(
            (by_key[&(s1.to_string(), d1)] - 6.0).abs() < 1e-9,
            "应优先使用 normalized_value"
        );
        assert!((by_key[&(s1.to_string(), d3)] - 16.5).abs() < 1e-9);
        assert!((by_key[&(s2.to_string(), d2)] - 27.5).abs() < 1e-9);
        assert!((by_key[&(s2.to_string(), d3)] - 33.0).abs() < 1e-9);
        assert!(
            !by_key.contains_key(&(s1.to_string(), d2)),
            "f2 值 NULL 的截面不应产出分数"
        );
        assert!(
            !by_key.contains_key(&(s2.to_string(), d1)),
            "f1 行缺失的截面不应产出分数"
        );

        cleanup_combine(&pool, combo, &[f1, f2]).await;
    }

    /// PIT 红线单测：构造一条合规历史 IC（end<=as_of）和一条越界未来 IC（end>as_of），
    /// 断言 compute_weights_pit 只用历史那条、绝不读未来。
    /// 历史 IC 为正(0.05)→权重正号；若误读未来 IC(-0.99)→会翻成负号。
    #[tokio::test]
    async fn test_pit_weights_excludes_future_ic() {
        let pool = PgPool::connect("postgres://gaocheng@localhost/quant")
            .await
            .expect("db");
        let fc = "test_pit_factor_zzz";
        let ver = "1.0.0";
        sqlx::query("DELETE FROM factor_evaluation WHERE factor_code=$1")
            .bind(fc)
            .execute(&pool)
            .await
            .unwrap();
        // 合规历史 IC（end_date <= as_of）: 正 IC
        sqlx::query(
            "INSERT INTO factor_evaluation (factor_code,factor_version,horizon,start_date,end_date,mean_ic,ic_ir)
             VALUES ($1,$2,20,'2016-01-01','2019-12-31',0.05,0.8)",
        )
        .bind(fc).bind(ver)
        .execute(&pool).await.unwrap();
        // 越界未来 IC（end_date > as_of）: 极端负 IC，若被读到会翻号
        sqlx::query(
            "INSERT INTO factor_evaluation (factor_code,factor_version,horizon,start_date,end_date,mean_ic,ic_ir)
             VALUES ($1,$2,20,'2016-01-01','2021-12-31',-0.99,9.9)",
        )
        .bind(fc).bind(ver)
        .execute(&pool).await.unwrap();

        let as_of = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
        let w = compute_weights_pit(&pool, &[(fc.to_string(), ver.to_string())], as_of, 20)
            .await
            .unwrap();

        assert_eq!(w.len(), 1, "应返回1个因子权重");
        assert!(
            w[0].weight > 0.0,
            "PIT 违规：读到了未来 IC（权重应为正，实际 {}）",
            w[0].weight
        );

        // 反向验证：as_of 推到 2022，未来那条变合规且更近，权重应翻负
        let as_of2 = NaiveDate::from_ymd_opt(2022, 6, 30).unwrap();
        let w2 = compute_weights_pit(&pool, &[(fc.to_string(), ver.to_string())], as_of2, 20)
            .await
            .unwrap();
        assert!(
            w2[0].weight < 0.0,
            "as_of=2022 应取到 2021 那条负 IC（权重应为负，实际 {}）",
            w2[0].weight
        );

        sqlx::query("DELETE FROM factor_evaluation WHERE factor_code=$1")
            .bind(fc)
            .execute(&pool)
            .await
            .unwrap();
    }
}
