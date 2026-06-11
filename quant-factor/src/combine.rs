//! Multi-factor combination engine
//!
//! Combines multiple factor scores into a single composite score using
//! equal-weight, ICIR-weighted, or custom weight methods.

use std::collections::HashMap;

use chrono::NaiveDate;
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;

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

    let total_ir: f64 = factors
        .iter()
        .filter_map(|(code, ver)| {
            let key = format!("{}:{}", code, ver);
            ic_evals.get(&key).map(|(_, ir)| ir.abs())
        })
        .sum();
    let total_ir = if total_ir == 0.0 { 1.0 } else { total_ir };

    let weights: Vec<FactorWeight> = factors
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
        .collect();
    Ok(weights)
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
    let n = factors.len() as f64;

    match method {
        CombineMethod::EqualWeight => {
            let weights: Vec<FactorWeight> = factors
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
                .collect();
            Ok(weights)
        }
        CombineMethod::IcirWeighted => {
            let total_ir: f64 = factors
                .iter()
                .filter_map(|(code, ver)| {
                    let key = format!("{}:{}", code, ver);
                    ic_evals.get(&key).map(|(_, ir)| ir.abs())
                })
                .sum();

            let total_ir = if total_ir == 0.0 { 1.0 } else { total_ir };

            let weights: Vec<FactorWeight> = factors
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
                .collect();
            Ok(weights)
        }
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
            if start_date.is_some() && end_date.is_some() {
                sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                    "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
                 FROM factor_value
                 WHERE factor_code = $1 AND factor_version = $2
                   AND trade_date >= $3 AND trade_date <= $4",
                )
                .bind(&fw.factor_code)
                .bind(&fw.factor_version)
                .bind(start_date.unwrap())
                .bind(end_date.unwrap())
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

    // Compute combined scores
    for ((_sym, _date), vals) in &factor_values {
        if vals.len() < weights.len() {
            continue;
        }
    }

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

    /// PIT 红线单测：构造一条合规历史 IC（end<=as_of）和一条越界未来 IC（end>as_of），
    /// 断言 compute_weights_pit 只用历史那条、绝不读未来。
    /// 历史 IC 为正(0.05)→权重正号；若误读未来 IC(-0.99)→会翻成负号。
    #[tokio::test]
    #[ignore] // 连本地库: cargo test -p quant-factor test_pit_weights_excludes_future_ic -- --ignored
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
