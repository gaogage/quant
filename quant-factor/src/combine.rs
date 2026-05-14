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
