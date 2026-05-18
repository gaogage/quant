//! ML API routes — minimal Phase 5 prediction-set smoke path

use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LinearFactorRef {
    pub factor_code: String,
    pub factor_version: String,
}

#[derive(Debug, Deserialize)]
pub struct TrainLinearModelRequest {
    pub model_code: String,
    pub model_version: String,
    pub model_version_id: Option<String>,
    pub training_task_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub data_version_id: String,
    pub feature_set_version_id: String,
    pub training_dataset_id: String,
    pub train_start_date: String,
    pub train_end_date: String,
    pub prediction_start_date: String,
    pub prediction_end_date: String,
    pub label_horizon_days: Option<i64>,
    pub factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Deserialize)]
pub struct WalkForwardLinearPredictionSetRequest {
    pub model_code: String,
    pub model_version: String,
    pub model_version_id: Option<String>,
    pub training_task_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub data_version_id: String,
    pub feature_set_version_id: String,
    pub training_dataset_id: String,
    pub prediction_start_date: String,
    pub prediction_end_date: String,
    pub train_lookback_days: Option<i64>,
    pub prediction_step_days: Option<i64>,
    pub label_horizon_days: Option<i64>,
    pub min_training_samples: Option<usize>,
    pub max_windows: Option<usize>,
    pub factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Deserialize)]
pub struct EvaluatePredictionSetRequest {
    pub prediction_set_id: String,
    pub backtest_task_id: String,
    pub min_trade_count: Option<i64>,
    pub max_drawdown: Option<f64>,
    pub min_excess_return: Option<f64>,
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

struct NormalizedLinearTrainingRequest {
    model_code: String,
    model_version: String,
    model_version_id: String,
    training_task_id: String,
    prediction_set_id: String,
    data_version_id: String,
    feature_set_version_id: String,
    training_dataset_id: String,
    train_start_date: NaiveDate,
    train_end_date: NaiveDate,
    prediction_start_date: NaiveDate,
    prediction_end_date: NaiveDate,
    label_horizon_days: i64,
    factors: Vec<LinearFactorRef>,
}

struct NormalizedWalkForwardLinearPredictionSetRequest {
    model_code: String,
    model_version: String,
    model_version_id: String,
    training_task_id: String,
    prediction_set_id: String,
    data_version_id: String,
    feature_set_version_id: String,
    training_dataset_id: String,
    prediction_start_date: NaiveDate,
    prediction_end_date: NaiveDate,
    train_lookback_days: i64,
    prediction_step_days: i64,
    label_horizon_days: i64,
    min_training_samples: usize,
    max_windows: Option<usize>,
    factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Clone, Serialize)]
struct WalkForwardWindow {
    window_index: usize,
    train_start_date: NaiveDate,
    train_end_date: NaiveDate,
    prediction_start_date: NaiveDate,
    prediction_end_date: NaiveDate,
}

#[derive(Debug, Clone, Serialize)]
struct WalkForwardWindowSummary {
    window_index: usize,
    train_start_date: NaiveDate,
    train_end_date: NaiveDate,
    prediction_start_date: NaiveDate,
    prediction_end_date: NaiveDate,
    sample_count: usize,
    prediction_rows: usize,
    skipped: bool,
    skip_reason: Option<String>,
    weights: Vec<LinearFactorWeight>,
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

#[derive(Debug, Clone)]
struct TrainingSample {
    features: Vec<f64>,
    label: f64,
}

struct NormalizedPredictionSetEvaluationRequest {
    prediction_set_id: String,
    backtest_task_id: String,
    min_trade_count: i64,
    max_drawdown: f64,
    min_excess_return: f64,
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

pub async fn train_linear_model(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TrainLinearModelRequest>,
) -> impl IntoResponse {
    match train_linear_model_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn create_walk_forward_linear_prediction_set(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WalkForwardLinearPredictionSetRequest>,
) -> impl IntoResponse {
    match create_walk_forward_linear_prediction_set_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn evaluate_prediction_set(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluatePredictionSetRequest>,
) -> impl IntoResponse {
    match evaluate_prediction_set_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn create_walk_forward_linear_prediction_set_inner(
    db: &sqlx::PgPool,
    req: WalkForwardLinearPredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_walk_forward_linear_prediction_request(&req)?;
    let windows = build_walk_forward_windows(&req)?;
    if windows.is_empty() {
        return Err("walk-forward request produced no windows".into());
    }

    let mut all_rows = Vec::new();
    let mut summaries = Vec::new();
    for window in &windows {
        let training_req = NormalizedLinearTrainingRequest {
            model_code: req.model_code.clone(),
            model_version: req.model_version.clone(),
            model_version_id: req.model_version_id.clone(),
            training_task_id: req.training_task_id.clone(),
            prediction_set_id: req.prediction_set_id.clone(),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: req.feature_set_version_id.clone(),
            training_dataset_id: req.training_dataset_id.clone(),
            train_start_date: window.train_start_date,
            train_end_date: window.train_end_date,
            prediction_start_date: window.prediction_start_date,
            prediction_end_date: window.prediction_end_date,
            label_horizon_days: req.label_horizon_days,
            factors: req.factors.clone(),
        };
        let samples = load_training_samples(db, &training_req).await?;
        if samples.len() < req.min_training_samples {
            summaries.push(WalkForwardWindowSummary {
                window_index: window.window_index,
                train_start_date: window.train_start_date,
                train_end_date: window.train_end_date,
                prediction_start_date: window.prediction_start_date,
                prediction_end_date: window.prediction_end_date,
                sample_count: samples.len(),
                prediction_rows: 0,
                skipped: true,
                skip_reason: Some(format!(
                    "sample_count {} < min_training_samples {}",
                    samples.len(),
                    req.min_training_samples
                )),
                weights: Vec::new(),
            });
            continue;
        }

        let weights = fit_linear_weights(&samples, req.factors.len());
        let weighted_factors = req
            .factors
            .iter()
            .zip(weights.iter())
            .map(|(factor, weight)| LinearFactorWeight {
                factor_code: factor.factor_code.clone(),
                factor_version: factor.factor_version.clone(),
                weight: *weight,
            })
            .collect::<Vec<_>>();
        let prediction_req = NormalizedLinearPredictionSetRequest {
            model_code: req.model_code.clone(),
            model_version: req.model_version.clone(),
            model_version_id: req.model_version_id.clone(),
            prediction_set_id: req.prediction_set_id.clone(),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: req.feature_set_version_id.clone(),
            training_dataset_id: req.training_dataset_id.clone(),
            start_date: window.prediction_start_date,
            end_date: window.prediction_end_date,
            factors: weighted_factors.clone(),
        };
        let mut rows = build_linear_prediction_rows(db, &prediction_req).await?;
        let prediction_rows = rows.len();
        all_rows.append(&mut rows);
        summaries.push(WalkForwardWindowSummary {
            window_index: window.window_index,
            train_start_date: window.train_start_date,
            train_end_date: window.train_end_date,
            prediction_start_date: window.prediction_start_date,
            prediction_end_date: window.prediction_end_date,
            sample_count: samples.len(),
            prediction_rows,
            skipped: false,
            skip_reason: None,
            weights: weighted_factors,
        });
    }

    if all_rows.is_empty() {
        return Err("walk-forward linear model produced no prediction rows".into());
    }

    let skipped_windows = summaries.iter().filter(|summary| summary.skipped).count();
    let completed_windows = summaries.len().saturating_sub(skipped_windows);
    let status = if skipped_windows == 0 {
        "completed"
    } else {
        "partial"
    };
    let label_definition = json!({
        "label": "future_return",
        "horizon_trading_days": req.label_horizon_days,
        "price": "close",
    });
    let feature_config = json!({
        "feature_set_version_id": req.feature_set_version_id,
        "factors": req.factors,
        "point_in_time_policy": "factor_value.available_at <= trade_date",
    });
    let hyperparameters = json!({
        "trainer": "walk_forward_covariance_linear_v1",
        "train_lookback_days": req.train_lookback_days,
        "prediction_step_days": req.prediction_step_days,
        "min_training_samples": req.min_training_samples,
    });
    let window_json = serde_json::to_value(&summaries)
        .map_err(|error| format!("Failed to serialize walk-forward windows: {}", error))?;
    let dataset_hash = stable_metadata_hash(&json!({
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "prediction_window": {"start": req.prediction_start_date, "end": req.prediction_end_date},
        "train_lookback_days": req.train_lookback_days,
        "prediction_step_days": req.prediction_step_days,
        "label_definition": label_definition,
        "factors": feature_config["factors"],
    }));
    let metadata = json!({
        "model_type": "walk_forward_linear_factor",
        "training_task_id": req.training_task_id,
        "prediction_set_id": req.prediction_set_id,
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "prediction_rows": all_rows.len(),
        "windows": window_json,
        "point_in_time_policy": "each window trains on dates <= prediction_start_date - label_horizon_days",
    });
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": req.model_version_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "prediction_window": {"start": req.prediction_start_date, "end": req.prediction_end_date},
        "windows": metadata["windows"],
    }));
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin walk-forward ML transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO training_dataset
           (training_dataset_id, data_version_id, feature_set_version_id, label_definition,
            train_window, validation_window, test_window, sample_filter, split_policy,
            dataset_hash, status, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'frozen', $11)
         ON CONFLICT (training_dataset_id) DO UPDATE SET
            data_version_id = EXCLUDED.data_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            label_definition = EXCLUDED.label_definition,
            train_window = EXCLUDED.train_window,
            validation_window = EXCLUDED.validation_window,
            test_window = EXCLUDED.test_window,
            sample_filter = EXCLUDED.sample_filter,
            split_policy = EXCLUDED.split_policy,
            dataset_hash = EXCLUDED.dataset_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.training_dataset_id)
    .bind(&req.data_version_id)
    .bind(&req.feature_set_version_id)
    .bind(&label_definition)
    .bind(json!({
        "lookback_days": req.train_lookback_days,
        "first_train_start": summaries.first().map(|summary| summary.train_start_date),
        "last_train_end": summaries.iter().rev().find(|summary| !summary.skipped).map(|summary| summary.train_end_date),
    }))
    .bind(json!({"walk_forward_validation": "windowed"}))
    .bind(json!({"prediction_start": req.prediction_start_date, "prediction_end": req.prediction_end_date}))
    .bind(json!({"min_training_samples": req.min_training_samples}))
    .bind(json!({"type": "walk_forward", "prediction_step_days": req.prediction_step_days}))
    .bind(&dataset_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert walk-forward training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_training_task
           (training_task_id, model_code, model_type, data_version_id, training_dataset_id,
            feature_config, label_definition, hyperparameters, status, progress,
            last_heartbeat_at, started_at, completed_at)
         VALUES ($1, $2, 'walk_forward_linear_factor', $3, $4, $5, $6, $7, $8, 100,
                 now(), now(), now())
         ON CONFLICT (training_task_id) DO UPDATE SET
            training_dataset_id = EXCLUDED.training_dataset_id,
            feature_config = EXCLUDED.feature_config,
            label_definition = EXCLUDED.label_definition,
            hyperparameters = EXCLUDED.hyperparameters,
            status = EXCLUDED.status,
            progress = 100,
            last_heartbeat_at = now(),
            completed_at = now(),
            error_message = NULL",
    )
    .bind(&req.training_task_id)
    .bind(&req.model_code)
    .bind(&req.data_version_id)
    .bind(&req.training_dataset_id)
    .bind(&feature_config)
    .bind(&label_definition)
    .bind(&hyperparameters)
    .bind(status)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert walk-forward model_training_task: {}",
            error
        )
    })?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'walk_forward_linear_factor', $3, $4, $5, $6,
                 $7, $8, $9, $10, 'active', $11)
         ON CONFLICT (model_version_id) DO UPDATE SET
            feature_version_id = EXCLUDED.feature_version_id,
            label_definition = EXCLUDED.label_definition,
            training_window = EXCLUDED.training_window,
            validation_metrics = EXCLUDED.validation_metrics,
            test_metrics = EXCLUDED.test_metrics,
            artifact_path = EXCLUDED.artifact_path,
            artifact_hash = EXCLUDED.artifact_hash,
            status = EXCLUDED.status,
            training_dataset_id = EXCLUDED.training_dataset_id",
    )
    .bind(&req.model_version_id)
    .bind(&req.model_code)
    .bind(&req.model_version)
    .bind(&req.feature_set_version_id)
    .bind(&label_definition)
    .bind(
        json!({"walk_forward_windows": summaries.len(), "lookback_days": req.train_lookback_days}),
    )
    .bind(json!({"completed_windows": completed_windows, "skipped_windows": skipped_windows}))
    .bind(json!({"prediction_row_count": all_rows.len()}))
    .bind(format!("memory://{}", req.training_task_id))
    .bind(&artifact_hash)
    .bind(&req.training_dataset_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert walk-forward model_registry: {}", error))?;

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
    .bind(req.prediction_start_date)
    .bind(req.prediction_end_date)
    .bind(&prediction_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert walk-forward prediction_set: {}", error))?;

    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(&req.prediction_set_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to clear walk-forward model_prediction: {}", error))?;
    insert_prediction_rows(&mut tx, &all_rows).await?;

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status)
         VALUES ($1, 'ml_walk_forward_linear', 'prediction_set', $2, $3, $4, $5)",
    )
    .bind(&experiment_run_id)
    .bind(&req.prediction_set_id)
    .bind(&hyperparameters)
    .bind(json!({
        "prediction_rows": all_rows.len(),
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "prediction_hash": prediction_hash,
        "artifact_hash": artifact_hash,
    }))
    .bind(status)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to insert walk-forward experiment_run: {}", error))?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit walk-forward ML transaction: {}", error))?;

    Ok(json!({
        "training_task_id": req.training_task_id,
        "model_version_id": req.model_version_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "prediction_rows": all_rows.len(),
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "status": status,
        "prediction_hash": prediction_hash,
        "experiment_run_id": experiment_run_id,
        "windows": summaries,
    }))
}

async fn train_linear_model_inner(
    db: &sqlx::PgPool,
    req: TrainLinearModelRequest,
) -> Result<Value, String> {
    let req = normalize_linear_training_request(&req)?;
    let samples = load_training_samples(db, &req).await?;
    if samples.len() < req.factors.len().max(5) {
        return Err(format!(
            "linear training found too few complete samples: {}",
            samples.len()
        ));
    }

    let weights = fit_linear_weights(&samples, req.factors.len());
    let weighted_factors = req
        .factors
        .iter()
        .zip(weights.iter())
        .map(|(factor, weight)| LinearFactorWeight {
            factor_code: factor.factor_code.clone(),
            factor_version: factor.factor_version.clone(),
            weight: *weight,
        })
        .collect::<Vec<_>>();

    let prediction_req = NormalizedLinearPredictionSetRequest {
        model_code: req.model_code.clone(),
        model_version: req.model_version.clone(),
        model_version_id: req.model_version_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        data_version_id: req.data_version_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        training_dataset_id: req.training_dataset_id.clone(),
        start_date: req.prediction_start_date,
        end_date: req.prediction_end_date,
        factors: weighted_factors.clone(),
    };
    let rows = build_linear_prediction_rows(db, &prediction_req).await?;
    if rows.is_empty() {
        return Err("trained linear model found no prediction factor values".into());
    }

    let label_definition = json!({
        "label": "future_return",
        "horizon_trading_days": req.label_horizon_days,
        "price": "close",
    });
    let feature_config = json!({
        "feature_set_version_id": req.feature_set_version_id,
        "factors": req.factors,
        "point_in_time_policy": "factor_value.available_at <= trade_date",
    });
    let hyperparameters = json!({
        "trainer": "covariance_linear_v1",
        "normalization": "absolute_weight_sum",
    });
    let metadata = json!({
        "model_type": "trained_linear_factor",
        "training_task_id": req.training_task_id,
        "sample_count": samples.len(),
        "factors": weighted_factors,
        "label_definition": label_definition,
        "point_in_time_policy": "model_prediction.available_at = trade_date",
    });
    let dataset_hash = stable_metadata_hash(&json!({
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "training_window": {"start": req.train_start_date, "end": req.train_end_date},
        "label_definition": label_definition,
        "factors": feature_config["factors"],
    }));
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": req.model_version_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "start_date": req.prediction_start_date,
        "end_date": req.prediction_end_date,
        "weights": metadata["factors"],
    }));
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let experiment_config = linear_training_experiment_config(&req);
    let experiment_metrics = linear_training_experiment_metrics(
        samples.len(),
        rows.len(),
        &metadata["factors"],
        &artifact_hash,
        &prediction_hash,
    );

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin ML training transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO training_dataset
           (training_dataset_id, data_version_id, feature_set_version_id, label_definition,
            train_window, validation_window, test_window, sample_filter, split_policy,
            dataset_hash, status, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'frozen', $11)
         ON CONFLICT (training_dataset_id) DO UPDATE SET
            data_version_id = EXCLUDED.data_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            label_definition = EXCLUDED.label_definition,
            train_window = EXCLUDED.train_window,
            validation_window = EXCLUDED.validation_window,
            test_window = EXCLUDED.test_window,
            sample_filter = EXCLUDED.sample_filter,
            split_policy = EXCLUDED.split_policy,
            dataset_hash = EXCLUDED.dataset_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.training_dataset_id)
    .bind(&req.data_version_id)
    .bind(&req.feature_set_version_id)
    .bind(&label_definition)
    .bind(json!({"start": req.train_start_date, "end": req.train_end_date}))
    .bind(json!({"start": req.train_start_date, "end": req.train_end_date}))
    .bind(json!({"start": req.prediction_start_date, "end": req.prediction_end_date}))
    .bind(json!({"complete_features": true, "finite_label": true}))
    .bind(json!({"type": "phase5c_train_predict_split"}))
    .bind(&dataset_hash)
    .bind(json!({"training_task_id": req.training_task_id, "sample_count": samples.len()}))
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_training_task
           (training_task_id, model_code, model_type, data_version_id, training_dataset_id,
            feature_config, label_definition, hyperparameters, status, progress,
            last_heartbeat_at, heartbeat_timeout_seconds, started_at, completed_at)
         VALUES ($1, $2, 'trained_linear_factor', $3, $4, $5, $6, $7, 'completed', 100,
                 now(), 600, now(), now())
         ON CONFLICT (training_task_id) DO UPDATE SET
            training_dataset_id = EXCLUDED.training_dataset_id,
            feature_config = EXCLUDED.feature_config,
            label_definition = EXCLUDED.label_definition,
            hyperparameters = EXCLUDED.hyperparameters,
            status = EXCLUDED.status,
            progress = EXCLUDED.progress,
            last_heartbeat_at = EXCLUDED.last_heartbeat_at,
            completed_at = EXCLUDED.completed_at",
    )
    .bind(&req.training_task_id)
    .bind(&req.model_code)
    .bind(&req.data_version_id)
    .bind(&req.training_dataset_id)
    .bind(&feature_config)
    .bind(&label_definition)
    .bind(&hyperparameters)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert model_training_task: {}", error))?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'trained_linear_factor', $3, $4, $5, $6,
                 $7, $8, $9, $10, 'active', $11)
         ON CONFLICT (model_version_id) DO UPDATE SET
            feature_version_id = EXCLUDED.feature_version_id,
            label_definition = EXCLUDED.label_definition,
            training_window = EXCLUDED.training_window,
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
    .bind(&label_definition)
    .bind(json!({"start": req.train_start_date, "end": req.train_end_date}))
    .bind(json!({"sample_count": samples.len(), "weights": metadata["factors"]}))
    .bind(json!({"prediction_row_count": rows.len()}))
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
    .bind(req.prediction_start_date)
    .bind(req.prediction_end_date)
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

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'ml_training_linear', 'model_training_task', $2, $3, $4,
                 'completed', now(), now())
         ON CONFLICT (experiment_run_id) DO UPDATE SET
            config = EXCLUDED.config,
            metrics = EXCLUDED.metrics,
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at",
    )
    .bind(&experiment_run_id)
    .bind(&req.training_task_id)
    .bind(&experiment_config)
    .bind(&experiment_metrics)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to insert experiment_run: {}", error))?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit ML training transaction: {}", error))?;

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "training_task_id": req.training_task_id,
        "model_version_id": req.model_version_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "sample_count": samples.len(),
        "prediction_rows": rows.len(),
        "artifact_hash": artifact_hash,
        "prediction_hash": prediction_hash,
        "weights": metadata["factors"],
    }))
}

async fn evaluate_prediction_set_inner(
    db: &sqlx::PgPool,
    req: EvaluatePredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_prediction_set_evaluation_request(&req)?;

    let prediction = sqlx::query_as::<
        _,
        (
            Option<i64>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<i64>,
        ),
    >(
        "SELECT COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
         FROM model_prediction
         WHERE prediction_set_id = $1",
    )
    .bind(&req.prediction_set_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize model_prediction: {}", error))?;

    let backtest = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, prediction_set_id, error_message
         FROM backtest_task
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_task: {}", error))?
    .ok_or_else(|| "backtest_task not found".to_string())?;
    if backtest.1.as_deref() != Some(req.prediction_set_id.as_str()) {
        return Err("backtest_task.prediction_set_id does not match request".into());
    }

    let result = sqlx::query_as::<
        _,
        (
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<i32>,
            Option<f64>,
        ),
    >(
        "SELECT total_return::double precision,
                benchmark_return::double precision,
                excess_return::double precision,
                max_drawdown::double precision,
                total_trades,
                turnover::double precision
         FROM backtest_result
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_result: {}", error))?
    .unwrap_or((None, None, None, None, None, None));

    let target_count = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
        "SELECT COUNT(*)::bigint, COUNT(DISTINCT symbol)::bigint
         FROM portfolio_target
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize portfolio_target: {}", error))?;

    let prediction_rows = prediction.0.unwrap_or(0);
    let target_rows = target_count.0.unwrap_or(0);
    let trade_count = i64::from(result.4.unwrap_or(0));
    let max_drawdown = result.3.unwrap_or(0.0);
    let excess_return = result.2.unwrap_or(0.0);
    let gates = evaluate_prediction_gates(
        trade_count,
        max_drawdown,
        excess_return,
        req.min_trade_count,
        req.max_drawdown,
        req.min_excess_return,
    );
    let status = prediction_evaluation_status(&gates);
    let metrics = json!({
        "prediction_rows": prediction_rows,
        "prediction_symbol_count": prediction.3.unwrap_or(0),
        "prediction_start_date": prediction.1,
        "prediction_end_date": prediction.2,
        "target_rows": target_rows,
        "target_symbol_count": target_count.1.unwrap_or(0),
        "trade_count": trade_count,
        "total_return": result.0,
        "benchmark_return": result.1,
        "excess_return": result.2,
        "max_drawdown": result.3,
        "turnover": result.5,
        "backtest_status": backtest.0,
        "backtest_error": backtest.2,
        "gates": gates,
    });
    let config = json!({
        "prediction_set_id": req.prediction_set_id,
        "backtest_task_id": req.backtest_task_id,
        "gate_policy": {
            "min_trade_count": req.min_trade_count,
            "max_drawdown": req.max_drawdown,
            "min_excess_return": req.min_excess_return,
        }
    });
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'ml_prediction_backtest_gate', 'prediction_set', $2, $3, $4,
                 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(&req.prediction_set_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert prediction evaluation experiment_run: {}",
            error
        )
    })?;

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "prediction_set_id": req.prediction_set_id,
        "backtest_task_id": req.backtest_task_id,
        "status": status,
        "metrics": metrics,
    }))
}

fn linear_training_experiment_config(req: &NormalizedLinearTrainingRequest) -> Value {
    json!({
        "model_code": req.model_code,
        "model_version": req.model_version,
        "model_version_id": req.model_version_id,
        "training_task_id": req.training_task_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "train_window": {"start": req.train_start_date, "end": req.train_end_date},
        "prediction_window": {"start": req.prediction_start_date, "end": req.prediction_end_date},
        "label": {
            "type": "future_return",
            "horizon_trading_days": req.label_horizon_days,
            "price": "close"
        },
        "factors": req.factors,
        "trainer": "covariance_linear_v1",
        "point_in_time_policy": "factor_value.available_at <= trade_date"
    })
}

fn linear_training_experiment_metrics(
    sample_count: usize,
    prediction_rows: usize,
    weights: &Value,
    artifact_hash: &str,
    prediction_hash: &str,
) -> Value {
    json!({
        "sample_count": sample_count,
        "prediction_rows": prediction_rows,
        "weights": weights,
        "artifact_hash": artifact_hash,
        "prediction_hash": prediction_hash,
        "prediction_point_in_time_policy": "model_prediction.available_at = trade_date",
        "status": "training_and_prediction_completed"
    })
}

fn normalize_prediction_set_evaluation_request(
    req: &EvaluatePredictionSetRequest,
) -> Result<NormalizedPredictionSetEvaluationRequest, String> {
    let prediction_set_id = req.prediction_set_id.trim().to_string();
    let backtest_task_id = req.backtest_task_id.trim().to_string();
    if prediction_set_id.is_empty() || backtest_task_id.is_empty() {
        return Err("prediction_set_id/backtest_task_id must not be empty".into());
    }
    let min_trade_count = req.min_trade_count.unwrap_or(1);
    if min_trade_count < 0 {
        return Err("min_trade_count must be non-negative".into());
    }
    let max_drawdown = req.max_drawdown.unwrap_or(0.20);
    if !max_drawdown.is_finite() || max_drawdown < 0.0 {
        return Err("max_drawdown must be a non-negative finite number".into());
    }
    let min_excess_return = req.min_excess_return.unwrap_or(0.0);
    if !min_excess_return.is_finite() {
        return Err("min_excess_return must be finite".into());
    }

    Ok(NormalizedPredictionSetEvaluationRequest {
        prediction_set_id,
        backtest_task_id,
        min_trade_count,
        max_drawdown,
        min_excess_return,
    })
}

fn evaluate_prediction_gates(
    trade_count: i64,
    max_drawdown: f64,
    excess_return: f64,
    min_trade_count: i64,
    max_drawdown_limit: f64,
    min_excess_return: f64,
) -> Value {
    json!([
        {
            "gate": "min_trade_count",
            "passed": trade_count >= min_trade_count,
            "limit": min_trade_count,
            "actual": trade_count
        },
        {
            "gate": "max_drawdown",
            "passed": max_drawdown <= max_drawdown_limit,
            "limit": max_drawdown_limit,
            "actual": max_drawdown
        },
        {
            "gate": "min_excess_return",
            "passed": excess_return >= min_excess_return,
            "limit": min_excess_return,
            "actual": excess_return
        }
    ])
}

fn prediction_evaluation_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item.get("passed").and_then(Value::as_bool).unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "approved_candidate"
    } else {
        "review_required"
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

fn normalize_linear_training_request(
    req: &TrainLinearModelRequest,
) -> Result<NormalizedLinearTrainingRequest, String> {
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

    let train_start_date = parse_yyyymmdd(&req.train_start_date, "train_start_date")?;
    let train_end_date = parse_yyyymmdd(&req.train_end_date, "train_end_date")?;
    let prediction_start_date =
        parse_yyyymmdd(&req.prediction_start_date, "prediction_start_date")?;
    let prediction_end_date = parse_yyyymmdd(&req.prediction_end_date, "prediction_end_date")?;
    if train_end_date < train_start_date {
        return Err("train_end_date must be greater than or equal to train_start_date".into());
    }
    if prediction_end_date < prediction_start_date {
        return Err(
            "prediction_end_date must be greater than or equal to prediction_start_date".into(),
        );
    }
    let label_horizon_days = req.label_horizon_days.unwrap_or(1);
    if label_horizon_days <= 0 {
        return Err("label_horizon_days must be positive".into());
    }

    Ok(NormalizedLinearTrainingRequest {
        model_version_id: req
            .model_version_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}@{}", model_code, model_version)),
        training_task_id: req
            .training_task_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("train-{}-{}", model_code, model_version)),
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
                    req.prediction_start_date.trim(),
                    req.prediction_end_date.trim()
                )
            }),
        model_code,
        model_version,
        data_version_id,
        feature_set_version_id,
        training_dataset_id,
        train_start_date,
        train_end_date,
        prediction_start_date,
        prediction_end_date,
        label_horizon_days,
        factors: req.factors.clone(),
    })
}

fn normalize_walk_forward_linear_prediction_request(
    req: &WalkForwardLinearPredictionSetRequest,
) -> Result<NormalizedWalkForwardLinearPredictionSetRequest, String> {
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

    let prediction_start_date =
        parse_yyyymmdd(&req.prediction_start_date, "prediction_start_date")?;
    let prediction_end_date = parse_yyyymmdd(&req.prediction_end_date, "prediction_end_date")?;
    if prediction_end_date < prediction_start_date {
        return Err(
            "prediction_end_date must be greater than or equal to prediction_start_date".into(),
        );
    }
    let train_lookback_days = req.train_lookback_days.unwrap_or(756);
    let prediction_step_days = req.prediction_step_days.unwrap_or(63);
    let label_horizon_days = req.label_horizon_days.unwrap_or(5);
    if train_lookback_days <= 0 {
        return Err("train_lookback_days must be positive".into());
    }
    if prediction_step_days <= 0 {
        return Err("prediction_step_days must be positive".into());
    }
    if label_horizon_days <= 0 {
        return Err("label_horizon_days must be positive".into());
    }
    let min_training_samples = req
        .min_training_samples
        .unwrap_or_else(|| req.factors.len().max(100));
    if min_training_samples == 0 {
        return Err("min_training_samples must be positive".into());
    }

    Ok(NormalizedWalkForwardLinearPredictionSetRequest {
        model_version_id: req
            .model_version_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}@{}", model_code, model_version)),
        training_task_id: req
            .training_task_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("train-{}-{}-walk-forward", model_code, model_version)),
        prediction_set_id: req
            .prediction_set_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                format!(
                    "pred-{}-{}-wf-{}-{}",
                    model_code,
                    model_version,
                    req.prediction_start_date.trim(),
                    req.prediction_end_date.trim()
                )
            }),
        model_code,
        model_version,
        data_version_id,
        feature_set_version_id,
        training_dataset_id,
        prediction_start_date,
        prediction_end_date,
        train_lookback_days,
        prediction_step_days,
        label_horizon_days,
        min_training_samples,
        max_windows: req.max_windows,
        factors: req.factors.clone(),
    })
}

fn build_walk_forward_windows(
    req: &NormalizedWalkForwardLinearPredictionSetRequest,
) -> Result<Vec<WalkForwardWindow>, String> {
    let mut windows = Vec::new();
    let mut prediction_start = req.prediction_start_date;
    while prediction_start <= req.prediction_end_date {
        if let Some(max_windows) = req.max_windows {
            if windows.len() >= max_windows {
                break;
            }
        }
        let prediction_end = (prediction_start + Duration::days(req.prediction_step_days - 1))
            .min(req.prediction_end_date);
        let train_end = prediction_start - Duration::days(req.label_horizon_days);
        let train_start = train_end - Duration::days(req.train_lookback_days - 1);
        if train_end < train_start {
            return Err("walk-forward train window is invalid".into());
        }
        windows.push(WalkForwardWindow {
            window_index: windows.len() + 1,
            train_start_date: train_start,
            train_end_date: train_end,
            prediction_start_date: prediction_start,
            prediction_end_date: prediction_end,
        });
        prediction_start = prediction_end + Duration::days(1);
    }
    Ok(windows)
}

async fn load_training_samples(
    db: &sqlx::PgPool,
    req: &NormalizedLinearTrainingRequest,
) -> Result<Vec<TrainingSample>, String> {
    let mut features_by_key: BTreeMap<(NaiveDate, String), Vec<Option<f64>>> = BTreeMap::new();
    for (factor_idx, factor) in req.factors.iter().enumerate() {
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
        .bind(req.train_start_date)
        .bind(req.train_end_date)
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load training factor values: {}", error))?;

        for (symbol, trade_date, value) in rows {
            let entry = features_by_key
                .entry((trade_date, symbol))
                .or_insert_with(|| vec![None; req.factors.len()]);
            entry[factor_idx] = value;
        }
    }

    let label_end_date = req.train_end_date + Duration::days(req.label_horizon_days + 7);
    let price_rows = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(
        "SELECT symbol, trade_date, close::double precision
         FROM market_stock_daily_bar
         WHERE trade_date >= $1
           AND trade_date <= $2
           AND close IS NOT NULL
         ORDER BY symbol, trade_date",
    )
    .bind(req.train_start_date)
    .bind(label_end_date)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load training labels: {}", error))?;

    let mut closes_by_symbol: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    for (symbol, trade_date, close) in price_rows {
        if let Some(close) = close {
            if close.is_finite() && close > 0.0 {
                closes_by_symbol
                    .entry(symbol)
                    .or_default()
                    .push((trade_date, close));
            }
        }
    }

    let mut samples = Vec::new();
    for ((trade_date, symbol), maybe_features) in features_by_key {
        let Some(features) = complete_features(maybe_features) else {
            continue;
        };
        let Some(label) = future_return_label(
            closes_by_symbol.get(&symbol),
            trade_date,
            req.label_horizon_days,
        ) else {
            continue;
        };
        if label.is_finite() {
            samples.push(TrainingSample { features, label });
        }
    }

    Ok(samples)
}

fn complete_features(values: Vec<Option<f64>>) -> Option<Vec<f64>> {
    values
        .into_iter()
        .map(|value| value.filter(|v| v.is_finite()))
        .collect()
}

fn future_return_label(
    closes: Option<&Vec<(NaiveDate, f64)>>,
    trade_date: NaiveDate,
    horizon_days: i64,
) -> Option<f64> {
    let closes = closes?;
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let target_idx = current_idx.checked_add(horizon_days as usize)?;
    let (_, current_close) = closes.get(current_idx)?;
    let (_, future_close) = closes.get(target_idx)?;
    if *future_close > 0.0 {
        Some((future_close / current_close) - 1.0)
    } else {
        None
    }
}

fn fit_linear_weights(samples: &[TrainingSample], factor_count: usize) -> Vec<f64> {
    if factor_count == 0 {
        return Vec::new();
    }

    let mut weights = vec![0.0; factor_count];
    for sample in samples {
        for (idx, feature) in sample.features.iter().take(factor_count).enumerate() {
            if feature.is_finite() && sample.label.is_finite() {
                weights[idx] += feature * sample.label;
            }
        }
    }

    normalize_weights(weights)
}

fn normalize_weights(mut weights: Vec<f64>) -> Vec<f64> {
    let gross: f64 = weights.iter().map(|value| value.abs()).sum();
    if gross > 0.0 {
        for weight in &mut weights {
            *weight /= gross;
        }
    } else if !weights.is_empty() {
        let equal = 1.0 / weights.len() as f64;
        weights.fill(equal);
    }
    weights
}

async fn build_linear_prediction_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
) -> Result<Vec<PredictionRow>, String> {
    let mut scores: HashMap<(NaiveDate, String), f64> = HashMap::new();
    let mut feature_counts: HashMap<(NaiveDate, String), usize> = HashMap::new();
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
                let key = (trade_date, symbol);
                *scores.entry(key.clone()).or_insert(0.0) += value * factor.weight;
                *feature_counts.entry(key).or_insert(0) += 1;
            }
        }
    }

    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for ((trade_date, symbol), score) in scores {
        if feature_counts
            .get(&(trade_date, symbol.clone()))
            .copied()
            .unwrap_or_default()
            < req.factors.len()
        {
            continue;
        }
        by_date.entry(trade_date).or_default().push((symbol, score));
    }

    let mut predictions = Vec::new();
    for (trade_date, mut rows) in by_date {
        rows.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
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

async fn insert_prediction_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[PredictionRow],
) -> Result<(), String> {
    for chunk in rows.chunks(5_000) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO model_prediction
               (prediction_set_id, trade_date, symbol, score, probability, rank, available_at) ",
        );
        builder.push_values(chunk, |mut row_builder, row| {
            row_builder
                .push_bind(&row.prediction_set_id)
                .push_bind(row.trade_date)
                .push_bind(&row.symbol)
                .push_bind(row.score)
                .push_bind(row.probability)
                .push_bind(row.rank)
                .push_bind(row.available_at);
        });
        builder
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|error| format!("Failed to insert model_prediction rows: {}", error))?;
    }
    Ok(())
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

    #[test]
    fn linear_training_request_defaults_ids_and_label_horizon() {
        let req = TrainLinearModelRequest {
            model_code: "trained_linear_alpha".into(),
            model_version: "phase5c-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "perf-db-smoke-data-v1".into(),
            feature_set_version_id: "phase5c-feature-smoke-v1".into(),
            training_dataset_id: "phase5c-training-smoke-v1".into(),
            train_start_date: "20250109".into(),
            train_end_date: "20250120".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250131".into(),
            label_horizon_days: None,
            factors: vec![LinearFactorRef {
                factor_code: "mom_5d_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };

        let normalized = normalize_linear_training_request(&req).expect("training request");

        assert_eq!(
            normalized.model_version_id,
            "trained_linear_alpha@phase5c-v1"
        );
        assert_eq!(
            normalized.training_task_id,
            "train-trained_linear_alpha-phase5c-v1"
        );
        assert_eq!(
            normalized.prediction_set_id,
            "pred-trained_linear_alpha-phase5c-v1-20250121-20250131"
        );
        assert_eq!(normalized.label_horizon_days, 1);
    }

    #[test]
    fn walk_forward_linear_request_defaults_full_history_ids() {
        let req = WalkForwardLinearPredictionSetRequest {
            model_code: "phase7_wf_linear_alpha".into(),
            model_version: "phase7-wf-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-a-v1".into(),
            feature_set_version_id: "phase7-alpha-v1".into(),
            training_dataset_id: "phase7-wf-training-v1".into(),
            prediction_start_date: "20160201".into(),
            prediction_end_date: "20260515".into(),
            train_lookback_days: None,
            prediction_step_days: None,
            label_horizon_days: None,
            min_training_samples: None,
            max_windows: Some(2),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };

        let normalized =
            normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

        assert_eq!(
            normalized.model_version_id,
            "phase7_wf_linear_alpha@phase7-wf-v1"
        );
        assert_eq!(
            normalized.training_task_id,
            "train-phase7_wf_linear_alpha-phase7-wf-v1-walk-forward"
        );
        assert_eq!(
            normalized.prediction_set_id,
            "pred-phase7_wf_linear_alpha-phase7-wf-v1-wf-20160201-20260515"
        );
        assert_eq!(normalized.train_lookback_days, 756);
        assert_eq!(normalized.prediction_step_days, 63);
        assert_eq!(normalized.label_horizon_days, 5);
        assert_eq!(normalized.min_training_samples, 100);
    }

    #[test]
    fn walk_forward_windows_leave_label_gap_before_prediction() {
        let req = WalkForwardLinearPredictionSetRequest {
            model_code: "phase7_wf_linear_alpha".into(),
            model_version: "phase7-wf-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-a-v1".into(),
            feature_set_version_id: "phase7-alpha-v1".into(),
            training_dataset_id: "phase7-wf-training-v1".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250210".into(),
            train_lookback_days: Some(10),
            prediction_step_days: Some(5),
            label_horizon_days: Some(2),
            min_training_samples: Some(1),
            max_windows: Some(2),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };
        let normalized =
            normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

        let windows = build_walk_forward_windows(&normalized).expect("windows");

        assert_eq!(windows.len(), 2);
        assert_eq!(
            windows[0].prediction_start_date,
            NaiveDate::from_ymd_opt(2025, 1, 21).unwrap()
        );
        assert_eq!(
            windows[0].prediction_end_date,
            NaiveDate::from_ymd_opt(2025, 1, 25).unwrap()
        );
        assert_eq!(
            windows[0].train_end_date,
            NaiveDate::from_ymd_opt(2025, 1, 19).unwrap()
        );
        assert_eq!(
            windows[0].train_start_date,
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap()
        );
        assert_eq!(
            windows[1].prediction_start_date,
            NaiveDate::from_ymd_opt(2025, 1, 26).unwrap()
        );
    }

    #[test]
    fn fit_linear_weights_normalizes_covariance_scores() {
        let samples = vec![
            TrainingSample {
                features: vec![1.0, 0.0],
                label: 0.10,
            },
            TrainingSample {
                features: vec![0.0, 1.0],
                label: -0.05,
            },
        ];

        let weights = fit_linear_weights(&samples, 2);

        assert_eq!(weights.len(), 2);
        assert!((weights[0] - 0.6666666667).abs() < 1e-6);
        assert!((weights[1] + 0.3333333333).abs() < 1e-6);
        assert!((weights.iter().map(|value| value.abs()).sum::<f64>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn future_return_label_uses_later_close_only() {
        let closes = vec![
            (NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(), 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 11.0),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 12.1),
        ];

        let label = future_return_label(
            Some(&closes),
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            2,
        )
        .expect("label");

        assert!((label - 0.21).abs() < 1e-9);
    }

    #[test]
    fn linear_training_experiment_records_lineage_and_metrics() {
        let req = TrainLinearModelRequest {
            model_code: "trained_linear_alpha".into(),
            model_version: "phase5c-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "perf-db-smoke-data-v1".into(),
            feature_set_version_id: "phase5c-feature-smoke-v1".into(),
            training_dataset_id: "phase5c-training-smoke-v1".into(),
            train_start_date: "20250109".into(),
            train_end_date: "20250120".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250131".into(),
            label_horizon_days: Some(1),
            factors: vec![LinearFactorRef {
                factor_code: "mom_5d_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };
        let normalized = normalize_linear_training_request(&req).expect("training request");

        let config = linear_training_experiment_config(&normalized);
        let metrics = linear_training_experiment_metrics(
            40367,
            25418,
            &json!([{"factor_code":"mom_5d_std","factor_version":"1.0.0","weight":1.0}]),
            "hash-artifact",
            "hash-prediction",
        );

        assert_eq!(
            config["training_task_id"],
            "train-trained_linear_alpha-phase5c-v1"
        );
        assert_eq!(
            config["prediction_set_id"],
            "pred-trained_linear_alpha-phase5c-v1-20250121-20250131"
        );
        assert_eq!(config["label"]["horizon_trading_days"], 1);
        assert_eq!(metrics["sample_count"], 40367);
        assert_eq!(metrics["prediction_rows"], 25418);
        assert_eq!(metrics["artifact_hash"], "hash-artifact");
        assert_eq!(metrics["prediction_hash"], "hash-prediction");
    }

    #[test]
    fn prediction_evaluation_gates_require_trades_drawdown_and_excess_return() {
        let gates = evaluate_prediction_gates(0, 0.05, -0.01, 1, 0.20, 0.0);

        assert_eq!(prediction_evaluation_status(&gates), "review_required");
        assert_eq!(gates[0]["gate"], "min_trade_count");
        assert_eq!(gates[0]["passed"], false);
        assert_eq!(gates[1]["passed"], true);
        assert_eq!(gates[2]["passed"], false);

        let passed = evaluate_prediction_gates(3, 0.05, 0.01, 1, 0.20, 0.0);
        assert_eq!(prediction_evaluation_status(&passed), "approved_candidate");
    }

    #[test]
    fn prediction_evaluation_request_defaults_gate_policy() {
        let req = EvaluatePredictionSetRequest {
            prediction_set_id: "pred-v1".into(),
            backtest_task_id: "pbt-v1".into(),
            min_trade_count: None,
            max_drawdown: None,
            min_excess_return: None,
        };

        let normalized =
            normalize_prediction_set_evaluation_request(&req).expect("evaluation request");

        assert_eq!(normalized.prediction_set_id, "pred-v1");
        assert_eq!(normalized.backtest_task_id, "pbt-v1");
        assert_eq!(normalized.min_trade_count, 1);
        assert!((normalized.max_drawdown - 0.20).abs() < 1e-9);
        assert_eq!(normalized.min_excess_return, 0.0);
    }
}
