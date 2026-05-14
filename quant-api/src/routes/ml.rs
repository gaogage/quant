//! ML API routes — minimal Phase 5 prediction-set smoke path

use axum::{extract::State, response::IntoResponse, Json};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct LinearPredictionSetRequest {
    pub model_code: String,
    pub model_version: String,
    pub model_version_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub data_version_id: String,
    pub feature_set_version_id: String,
    pub training_dataset_id: String,
    pub start_date: String,
    pub end_date: String,
    pub factors: Vec<LinearFactorWeight>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LinearFactorWeight {
    pub factor_code: String,
    pub factor_version: String,
    pub weight: f64,
}

struct NormalizedLinearPredictionSetRequest {
    model_code: String,
    model_version: String,
    model_version_id: String,
    prediction_set_id: String,
    data_version_id: String,
    feature_set_version_id: String,
    training_dataset_id: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    factors: Vec<LinearFactorWeight>,
}

#[derive(Debug, Clone)]
struct PredictionRow {
    prediction_set_id: String,
    symbol: String,
    trade_date: NaiveDate,
    score: f64,
    probability: Option<f64>,
    rank: i32,
    available_at: NaiveDate,
}

pub async fn create_linear_prediction_set(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LinearPredictionSetRequest>,
) -> impl IntoResponse {
    match create_linear_prediction_set_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn create_linear_prediction_set_inner(
    db: &sqlx::PgPool,
    req: LinearPredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_linear_prediction_request(&req)?;
    let metadata = json!({
        "model_type": "linear_factor_smoke",
        "factors": req.factors,
        "point_in_time_policy": "model_prediction.available_at = trade_date",
    });
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": req.model_version_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "factors": metadata["factors"],
    }));

    let rows = build_linear_prediction_rows(db, &req).await?;
    if rows.is_empty() {
        return Err("linear prediction smoke found no factor values".into());
    }

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin ML prediction transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO training_dataset
           (training_dataset_id, data_version_id, feature_set_version_id, label_definition,
            train_window, validation_window, test_window, sample_filter, split_policy,
            dataset_hash, status, metadata)
         VALUES ($1, $2, $3, '{}'::jsonb, $4, $5, $6, '{}'::jsonb, $7, $8, 'frozen', $9)
         ON CONFLICT (training_dataset_id) DO UPDATE SET
            data_version_id = EXCLUDED.data_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            train_window = EXCLUDED.train_window,
            validation_window = EXCLUDED.validation_window,
            test_window = EXCLUDED.test_window,
            split_policy = EXCLUDED.split_policy,
            dataset_hash = EXCLUDED.dataset_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.training_dataset_id)
    .bind(&req.data_version_id)
    .bind(&req.feature_set_version_id)
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"type": "phase5a_smoke_same_window"}))
    .bind(stable_metadata_hash(&metadata))
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'linear_factor_smoke', $3, $4, '{}'::jsonb, $5,
                 $6, $7, $8, $9, 'active', $10)
         ON CONFLICT (model_version_id) DO UPDATE SET
            feature_version_id = EXCLUDED.feature_version_id,
            validation_metrics = EXCLUDED.validation_metrics,
            test_metrics = EXCLUDED.test_metrics,
            artifact_hash = EXCLUDED.artifact_hash,
            status = EXCLUDED.status,
            training_dataset_id = EXCLUDED.training_dataset_id",
    )
    .bind(&req.model_version_id)
    .bind(&req.model_code)
    .bind(&req.model_version)
    .bind(&req.feature_set_version_id)
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"row_count": rows.len()}))
    .bind(json!({"row_count": rows.len()}))
    .bind(format!("artifact://{}", req.model_version_id))
    .bind(&artifact_hash)
    .bind(&req.training_dataset_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert model_registry: {}", error))?;

    sqlx::query(
        "INSERT INTO prediction_set
           (prediction_set_id, model_version_id, feature_set_version_id, data_version_id,
            start_date, end_date, prediction_hash, status, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', $8)
         ON CONFLICT (prediction_set_id) DO UPDATE SET
            model_version_id = EXCLUDED.model_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            data_version_id = EXCLUDED.data_version_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            prediction_hash = EXCLUDED.prediction_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.prediction_set_id)
    .bind(&req.model_version_id)
    .bind(&req.feature_set_version_id)
    .bind(&req.data_version_id)
    .bind(req.start_date)
    .bind(req.end_date)
    .bind(&prediction_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert prediction_set: {}", error))?;

    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(&req.prediction_set_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to clear model_prediction: {}", error))?;

    for row in &rows {
        sqlx::query(
            "INSERT INTO model_prediction
               (prediction_set_id, trade_date, symbol, score, probability, rank, available_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&row.prediction_set_id)
        .bind(row.trade_date)
        .bind(&row.symbol)
        .bind(row.score)
        .bind(row.probability)
        .bind(row.rank)
        .bind(row.available_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to insert model_prediction: {}", error))?;
    }

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit ML prediction transaction: {}", error))?;

    Ok(json!({
        "model_version_id": req.model_version_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "prediction_hash": prediction_hash,
        "inserted": rows.len(),
        "min_trade_date": rows.iter().map(|row| row.trade_date).min(),
        "max_trade_date": rows.iter().map(|row| row.trade_date).max(),
        "point_in_time": "available_at = trade_date",
    }))
}

fn normalize_linear_prediction_request(
    req: &LinearPredictionSetRequest,
) -> Result<NormalizedLinearPredictionSetRequest, String> {
    if req.factors.is_empty() {
        return Err("factors must not be empty".into());
    }
    let model_code = req.model_code.trim().to_string();
    let model_version = req.model_version.trim().to_string();
    let data_version_id = req.data_version_id.trim().to_string();
    let feature_set_version_id = req.feature_set_version_id.trim().to_string();
    let training_dataset_id = req.training_dataset_id.trim().to_string();
    if model_code.is_empty()
        || model_version.is_empty()
        || data_version_id.is_empty()
        || feature_set_version_id.is_empty()
        || training_dataset_id.is_empty()
    {
        return Err("model_code/model_version/data_version_id/feature_set_version_id/training_dataset_id must not be empty".into());
    }
    let start_date = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end_date = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end_date < start_date {
        return Err("end_date must be greater than or equal to start_date".into());
    }

    Ok(NormalizedLinearPredictionSetRequest {
        model_version_id: req
            .model_version_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}@{}", model_code, model_version)),
        prediction_set_id: req
            .prediction_set_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                format!(
                    "pred-{}-{}-{}-{}",
                    model_code,
                    model_version,
                    req.start_date.trim(),
                    req.end_date.trim()
                )
            }),
        model_code,
        model_version,
        data_version_id,
        feature_set_version_id,
        training_dataset_id,
        start_date,
        end_date,
        factors: req.factors.clone(),
    })
}

async fn build_linear_prediction_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
) -> Result<Vec<PredictionRow>, String> {
    let mut scores: HashMap<(NaiveDate, String), f64> = HashMap::new();
    for factor in &req.factors {
        let rows = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(
            "SELECT symbol, trade_date, normalized_value::double precision
             FROM factor_value
             WHERE factor_code = $1
               AND factor_version = $2
               AND trade_date >= $3
               AND trade_date <= $4
               AND (available_at IS NULL OR available_at <= trade_date)",
        )
        .bind(&factor.factor_code)
        .bind(&factor.factor_version)
        .bind(req.start_date)
        .bind(req.end_date)
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load factor values: {}", error))?;

        for (symbol, trade_date, value) in rows {
            if let Some(value) = value {
                *scores.entry((trade_date, symbol)).or_insert(0.0) += value * factor.weight;
            }
        }
    }

    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for ((trade_date, symbol), score) in scores {
        by_date.entry(trade_date).or_default().push((symbol, score));
    }

    let mut predictions = Vec::new();
    for (trade_date, mut rows) in by_date {
        rows.sort_by(|left, right| right.1.total_cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (idx, (symbol, score)) in rows.into_iter().enumerate() {
            predictions.push(build_prediction_row(
                &req.prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }

    Ok(predictions)
}

fn build_prediction_row(
    prediction_set_id: &str,
    symbol: &str,
    trade_date: &str,
    score: f64,
    rank: i32,
) -> Result<PredictionRow, String> {
    let trade_date = NaiveDate::parse_from_str(trade_date, "%Y-%m-%d")
        .map_err(|_| "trade_date must use YYYY-MM-DD format".to_string())?;
    Ok(PredictionRow {
        prediction_set_id: prediction_set_id.to_string(),
        symbol: symbol.to_string(),
        trade_date,
        score,
        probability: Some(1.0 / (1.0 + (-score).exp())),
        rank,
        available_at: trade_date,
    })
}

fn stable_metadata_hash(value: &Value) -> String {
    let canonical = canonical_json(value);
    let mut hash = 14695981039346656037u64;
    for byte in canonical.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("hash-{:016x}", hash)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let body = entries
                .into_iter()
                .map(|(key, value)| format!("\"{}\":{}", key, canonical_json(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{}}}", body)
        }
        Value::Array(values) => {
            let body = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{}]", body)
        }
        _ => value.to_string(),
    }
}

fn parse_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .map_err(|_| format!("{} must use YYYYMMDD format", field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn linear_prediction_request_defaults_model_and_prediction_ids() {
        let req = LinearPredictionSetRequest {
            model_code: "linear_alpha_smoke".into(),
            model_version: "phase5a-v1".into(),
            model_version_id: None,
            prediction_set_id: None,
            data_version_id: "perf-db-smoke-data-v1".into(),
            feature_set_version_id: "phase5a-feature-smoke-v1".into(),
            training_dataset_id: "phase5a-training-smoke-v1".into(),
            start_date: "20250109".into(),
            end_date: "20250131".into(),
            factors: vec![LinearFactorWeight {
                factor_code: "mom_5d_std".into(),
                factor_version: "1.0.0".into(),
                weight: 0.7,
            }],
        };

        let normalized = normalize_linear_prediction_request(&req).expect("normalized request");

        assert_eq!(normalized.model_version_id, "linear_alpha_smoke@phase5a-v1");
        assert_eq!(
            normalized.prediction_set_id,
            "pred-linear_alpha_smoke-phase5a-v1-20250109-20250131"
        );
    }

    #[test]
    fn model_prediction_rows_use_trade_date_as_available_at() {
        let row = build_prediction_row("pred-v1", "000001.SZ", "2025-01-09", 0.42, 3)
            .expect("prediction row");

        assert_eq!(row.prediction_set_id, "pred-v1");
        assert_eq!(row.trade_date, row.available_at);
        assert_eq!(row.rank, 3);
    }

    #[test]
    fn model_metadata_hash_is_stable_for_same_inputs() {
        let left = stable_metadata_hash(&json!({
            "model": "linear",
            "weights": [{"factor": "mom", "weight": 0.7}, {"factor": "turn", "weight": 0.3}]
        }));
        let right = stable_metadata_hash(&json!({
            "weights": [{"weight": 0.7, "factor": "mom"}, {"weight": 0.3, "factor": "turn"}],
            "model": "linear"
        }));

        assert_eq!(left, right);
        assert!(left.starts_with("hash-"));
    }
}
