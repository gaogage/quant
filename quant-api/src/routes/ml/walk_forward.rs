use super::*;
use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;
use uuid::Uuid;

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
    pub label_objective: Option<String>,
    pub min_training_samples: Option<usize>,
    pub max_windows: Option<usize>,
    pub factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Deserialize)]
pub struct WalkForwardNonlinearQuantileRankerRequest {
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
    pub label_objective: Option<String>,
    pub min_training_samples: Option<usize>,
    pub max_windows: Option<usize>,
    pub bucket_count: Option<usize>,
    pub min_samples_per_bucket: Option<usize>,
    pub factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
pub(crate) struct NormalizedWalkForwardLinearPredictionSetRequest {
    pub(crate) model_code: String,
    pub(crate) model_version: String,
    pub(crate) model_version_id: String,
    pub(crate) training_task_id: String,
    pub(crate) prediction_set_id: String,
    pub(crate) data_version_id: String,
    pub(crate) feature_set_version_id: String,
    pub(crate) training_dataset_id: String,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
    pub(crate) train_lookback_days: i64,
    pub(crate) prediction_step_days: i64,
    pub(crate) label_horizon_days: i64,
    pub(crate) label_objective: LabelObjective,
    pub(crate) min_training_samples: usize,
    pub(crate) max_windows: Option<usize>,
    pub(crate) factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
pub(crate) struct NormalizedWalkForwardNonlinearQuantileRankerRequest {
    pub(crate) linear: NormalizedWalkForwardLinearPredictionSetRequest,
    pub(crate) bucket_count: usize,
    pub(crate) min_samples_per_bucket: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalkForwardWindow {
    pub(crate) window_index: usize,
    pub(crate) train_start_date: NaiveDate,
    pub(crate) train_end_date: NaiveDate,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalkForwardWindowSummary {
    pub(crate) window_index: usize,
    pub(crate) train_start_date: NaiveDate,
    pub(crate) train_end_date: NaiveDate,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
    pub(crate) sample_count: usize,
    pub(crate) prediction_rows: usize,
    pub(crate) skipped: bool,
    pub(crate) skip_reason: Option<String>,
    pub(crate) weights: Vec<LinearFactorWeight>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalkForwardNonlinearWindowSummary {
    pub(crate) window_index: usize,
    pub(crate) train_start_date: NaiveDate,
    pub(crate) train_end_date: NaiveDate,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
    pub(crate) sample_count: usize,
    pub(crate) prediction_rows: usize,
    pub(crate) skipped: bool,
    pub(crate) skip_reason: Option<String>,
    pub(crate) model: Option<NonlinearQuantileRanker>,
}

pub(crate) async fn create_walk_forward_linear_prediction_set_inner(
    db: &sqlx::PgPool,
    req: WalkForwardLinearPredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_walk_forward_linear_prediction_request(&req)?;
    ensure_data_version_exists(db, &req.data_version_id).await?;
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
            label_objective: req.label_objective,
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
    let label_definition = label_definition_json(req.label_objective, req.label_horizon_days);
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
    let experiment_config = walk_forward_linear_experiment_config(&req, &label_definition);
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

    let training_end = req.prediction_start_date - chrono::Duration::days(1);
    sqlx::query(
        "INSERT INTO prediction_set
           (prediction_set_id, model_version_id, feature_set_version_id, data_version_id,
            start_date, end_date, prediction_hash, status, metadata,
            training_end_date)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', $8, $9)
         ON CONFLICT (prediction_set_id) DO UPDATE SET
            model_version_id = EXCLUDED.model_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            data_version_id = EXCLUDED.data_version_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            prediction_hash = EXCLUDED.prediction_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata,
            training_end_date = COALESCE(prediction_set.training_end_date, EXCLUDED.training_end_date)",
    )
    .bind(&req.prediction_set_id)
    .bind(&req.model_version_id)
    .bind(&req.feature_set_version_id)
    .bind(&req.data_version_id)
    .bind(req.prediction_start_date)
    .bind(req.prediction_end_date)
    .bind(&prediction_hash)
    .bind(&metadata)
    .bind(training_end)
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
    .bind(&experiment_config)
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

pub(crate) async fn create_walk_forward_nonlinear_quantile_ranker_inner(
    db: &sqlx::PgPool,
    req: WalkForwardNonlinearQuantileRankerRequest,
) -> Result<Value, String> {
    let started_at = Instant::now();
    let req = normalize_walk_forward_nonlinear_quantile_ranker_request(&req)?;
    let linear = &req.linear;
    ensure_data_version_exists(db, &linear.data_version_id).await?;
    let windows = build_walk_forward_windows(linear)?;
    if windows.is_empty() {
        return Err("walk-forward nonlinear request produced no windows".into());
    }

    let matrix_start_date = windows
        .iter()
        .map(|window| window.train_start_date.min(window.prediction_start_date))
        .min()
        .unwrap_or(linear.prediction_start_date);
    let matrix_end_date = windows
        .iter()
        .map(|window| window.train_end_date.max(window.prediction_end_date))
        .max()
        .unwrap_or(linear.prediction_end_date);
    let matrix_req = NormalizedLinearPredictionSetRequest {
        model_code: linear.model_code.clone(),
        model_version: linear.model_version.clone(),
        model_version_id: linear.model_version_id.clone(),
        prediction_set_id: linear.prediction_set_id.clone(),
        data_version_id: linear.data_version_id.clone(),
        feature_set_version_id: linear.feature_set_version_id.clone(),
        training_dataset_id: linear.training_dataset_id.clone(),
        start_date: matrix_start_date,
        end_date: matrix_end_date,
        factors: linear
            .factors
            .iter()
            .map(|factor| LinearFactorWeight {
                factor_code: factor.factor_code.clone(),
                factor_version: factor.factor_version.clone(),
                weight: 1.0,
            })
            .collect(),
    };
    let load_started_at = Instant::now();
    let feature_matrix_cache =
        FeatureMatrixWindowCache::new(load_prediction_feature_matrix_rows(db, &matrix_req).await?);
    let load_elapsed = load_started_at.elapsed();

    let scoring_started_at = Instant::now();
    let mut all_rows = Vec::new();
    let mut summaries = Vec::new();
    for window in &windows {
        let training_req = NormalizedLinearTrainingRequest {
            model_code: linear.model_code.clone(),
            model_version: linear.model_version.clone(),
            model_version_id: linear.model_version_id.clone(),
            training_task_id: linear.training_task_id.clone(),
            prediction_set_id: linear.prediction_set_id.clone(),
            data_version_id: linear.data_version_id.clone(),
            feature_set_version_id: linear.feature_set_version_id.clone(),
            training_dataset_id: linear.training_dataset_id.clone(),
            train_start_date: window.train_start_date,
            train_end_date: window.train_end_date,
            prediction_start_date: window.prediction_start_date,
            prediction_end_date: window.prediction_end_date,
            label_horizon_days: linear.label_horizon_days,
            label_objective: linear.label_objective,
            factors: linear.factors.clone(),
        };
        let training_feature_rows =
            feature_matrix_cache.slice(window.train_start_date, window.train_end_date);
        let samples =
            load_training_samples_from_feature_rows(db, &training_req, training_feature_rows)
                .await?;
        let required_samples = linear
            .min_training_samples
            .max(req.bucket_count * req.min_samples_per_bucket);
        if samples.len() < required_samples {
            summaries.push(WalkForwardNonlinearWindowSummary {
                window_index: window.window_index,
                train_start_date: window.train_start_date,
                train_end_date: window.train_end_date,
                prediction_start_date: window.prediction_start_date,
                prediction_end_date: window.prediction_end_date,
                sample_count: samples.len(),
                prediction_rows: 0,
                skipped: true,
                skip_reason: Some(format!(
                    "sample_count {} < required_samples {}",
                    samples.len(),
                    required_samples
                )),
                model: None,
            });
            continue;
        }

        let model = match fit_nonlinear_quantile_ranker(
            &samples,
            linear.factors.len(),
            req.bucket_count,
            req.min_samples_per_bucket,
        ) {
            Ok(model) => model,
            Err(message) => {
                summaries.push(WalkForwardNonlinearWindowSummary {
                    window_index: window.window_index,
                    train_start_date: window.train_start_date,
                    train_end_date: window.train_end_date,
                    prediction_start_date: window.prediction_start_date,
                    prediction_end_date: window.prediction_end_date,
                    sample_count: samples.len(),
                    prediction_rows: 0,
                    skipped: true,
                    skip_reason: Some(message),
                    model: None,
                });
                continue;
            }
        };
        let feature_rows =
            feature_matrix_cache.slice(window.prediction_start_date, window.prediction_end_date);
        let mut rows = nonlinear_prediction_rows_from_feature_matrix_rows(
            &linear.prediction_set_id,
            feature_rows,
            &model,
        )?;
        let prediction_rows = rows.len();
        all_rows.append(&mut rows);
        summaries.push(WalkForwardNonlinearWindowSummary {
            window_index: window.window_index,
            train_start_date: window.train_start_date,
            train_end_date: window.train_end_date,
            prediction_start_date: window.prediction_start_date,
            prediction_end_date: window.prediction_end_date,
            sample_count: samples.len(),
            prediction_rows,
            skipped: false,
            skip_reason: None,
            model: Some(model),
        });
    }
    let scoring_elapsed = scoring_started_at.elapsed();

    if all_rows.is_empty() {
        return Err(walk_forward_nonlinear_no_prediction_rows_error(&summaries));
    }

    let skipped_windows = summaries.iter().filter(|summary| summary.skipped).count();
    let completed_windows = summaries.len().saturating_sub(skipped_windows);
    let status = if skipped_windows == 0 {
        "completed"
    } else {
        "partial"
    };
    let label_definition = label_definition_json(linear.label_objective, linear.label_horizon_days);
    let feature_config = json!({
        "feature_set_version_id": linear.feature_set_version_id,
        "factors": linear.factors,
        "point_in_time_policy": "factor_value.available_at <= trade_date",
    });
    let hyperparameters = json!({
        "trainer": "walk_forward_nonlinear_quantile_ranker_v1",
        "train_lookback_days": linear.train_lookback_days,
        "prediction_step_days": linear.prediction_step_days,
        "min_training_samples": linear.min_training_samples,
        "bucket_count": req.bucket_count,
        "min_samples_per_bucket": req.min_samples_per_bucket,
    });
    let experiment_config = walk_forward_nonlinear_quantile_ranker_experiment_config(&req);
    let window_json = serde_json::to_value(&summaries).map_err(|error| {
        format!(
            "Failed to serialize nonlinear walk-forward windows: {}",
            error
        )
    })?;
    let dataset_hash = stable_metadata_hash(&json!({
        "data_version_id": linear.data_version_id,
        "feature_set_version_id": linear.feature_set_version_id,
        "prediction_window": {"start": linear.prediction_start_date, "end": linear.prediction_end_date},
        "train_lookback_days": linear.train_lookback_days,
        "prediction_step_days": linear.prediction_step_days,
        "label_definition": label_definition,
        "factors": feature_config["factors"],
        "trainer": hyperparameters,
    }));
    let metadata = json!({
        "model_type": "walk_forward_nonlinear_quantile_ranker",
        "training_task_id": linear.training_task_id,
        "prediction_set_id": linear.prediction_set_id,
        "feature_matrix_cache": feature_matrix_cache_metadata(
            "request_window",
            matrix_start_date,
            matrix_end_date,
            feature_matrix_cache.row_count(),
        ),
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "prediction_rows": all_rows.len(),
        "prediction_insert_telemetry": prediction_insert_telemetry(&all_rows),
        "prediction_generation_telemetry": prediction_generation_telemetry(
            "walk_forward_nonlinear_quantile_ranker",
            summaries.len(),
            completed_windows,
            skipped_windows,
            all_rows.len(),
            started_at.elapsed(),
            vec![
                prediction_progress_stage(
                    "load_feature_matrix_cache",
                    1,
                    1,
                    feature_matrix_cache.row_count(),
                    load_elapsed,
                ),
                prediction_progress_stage(
                    "fit_and_score_windows",
                    summaries.len(),
                    summaries.len(),
                    all_rows.len(),
                    scoring_elapsed,
                ),
            ],
        ),
        "windows": window_json,
        "point_in_time_policy": "each window trains on dates <= prediction_start_date - label_horizon_days; model_prediction.available_at = trade_date",
    });
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": linear.model_version_id,
        "prediction_set_id": linear.prediction_set_id,
        "data_version_id": linear.data_version_id,
        "feature_set_version_id": linear.feature_set_version_id,
        "prediction_window": {"start": linear.prediction_start_date, "end": linear.prediction_end_date},
        "windows": metadata["windows"],
    }));
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());

    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to begin nonlinear walk-forward ML transaction: {}",
            error
        )
    })?;

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
    .bind(&linear.training_dataset_id)
    .bind(&linear.data_version_id)
    .bind(&linear.feature_set_version_id)
    .bind(&label_definition)
    .bind(json!({
        "lookback_days": linear.train_lookback_days,
        "first_train_start": summaries.first().map(|summary| summary.train_start_date),
        "last_train_end": summaries.iter().rev().find(|summary| !summary.skipped).map(|summary| summary.train_end_date),
    }))
    .bind(json!({"walk_forward_validation": "windowed"}))
    .bind(json!({"prediction_start": linear.prediction_start_date, "prediction_end": linear.prediction_end_date}))
    .bind(json!({
        "min_training_samples": linear.min_training_samples,
        "bucket_count": req.bucket_count,
        "min_samples_per_bucket": req.min_samples_per_bucket,
    }))
    .bind(json!({"type": "walk_forward_nonlinear_quantile_ranker", "prediction_step_days": linear.prediction_step_days}))
    .bind(&dataset_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert nonlinear walk-forward training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_training_task
           (training_task_id, model_code, model_type, data_version_id, training_dataset_id,
            feature_config, label_definition, hyperparameters, status, progress,
            last_heartbeat_at, heartbeat_timeout_seconds, started_at, completed_at)
         VALUES ($1, $2, 'walk_forward_nonlinear_quantile_ranker', $3, $4, $5, $6, $7, $8, 100,
                 now(), 600, now(), now())
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
    .bind(&linear.training_task_id)
    .bind(&linear.model_code)
    .bind(&linear.data_version_id)
    .bind(&linear.training_dataset_id)
    .bind(&feature_config)
    .bind(&label_definition)
    .bind(&hyperparameters)
    .bind(status)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert nonlinear walk-forward model_training_task: {}",
            error
        )
    })?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'walk_forward_nonlinear_quantile_ranker', $3, $4, $5, $6,
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
    .bind(&linear.model_version_id)
    .bind(&linear.model_code)
    .bind(&linear.model_version)
    .bind(&linear.feature_set_version_id)
    .bind(&label_definition)
    .bind(json!({"walk_forward_windows": summaries.len(), "lookback_days": linear.train_lookback_days}))
    .bind(json!({
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "bucket_count": req.bucket_count,
    }))
    .bind(json!({"prediction_row_count": all_rows.len()}))
    .bind(format!("memory://{}", linear.training_task_id))
    .bind(&artifact_hash)
    .bind(&linear.training_dataset_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert nonlinear walk-forward model_registry: {}", error))?;

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
    .bind(&linear.prediction_set_id)
    .bind(&linear.model_version_id)
    .bind(&linear.feature_set_version_id)
    .bind(&linear.data_version_id)
    .bind(linear.prediction_start_date)
    .bind(linear.prediction_end_date)
    .bind(&prediction_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert nonlinear walk-forward prediction_set: {}",
            error
        )
    })?;

    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(&linear.prediction_set_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to clear nonlinear walk-forward model_prediction: {}",
                error
            )
        })?;
    insert_prediction_rows(&mut tx, &all_rows).await?;

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status)
         VALUES ($1, 'ml_walk_forward_nonlinear_quantile_ranker', 'prediction_set', $2, $3, $4, $5)",
    )
    .bind(&experiment_run_id)
    .bind(&linear.prediction_set_id)
    .bind(&experiment_config)
    .bind(json!({
        "prediction_rows": all_rows.len(),
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "bucket_count": req.bucket_count,
        "feature_matrix_cache": metadata["feature_matrix_cache"],
        "prediction_insert_telemetry": metadata["prediction_insert_telemetry"],
        "prediction_generation_telemetry": metadata["prediction_generation_telemetry"],
        "prediction_hash": prediction_hash,
        "artifact_hash": artifact_hash,
    }))
    .bind(status)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to insert nonlinear walk-forward experiment_run: {}", error))?;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit nonlinear walk-forward ML transaction: {}",
            error
        )
    })?;

    Ok(json!({
        "training_task_id": linear.training_task_id,
        "model_version_id": linear.model_version_id,
        "training_dataset_id": linear.training_dataset_id,
        "prediction_set_id": linear.prediction_set_id,
        "prediction_rows": all_rows.len(),
        "window_count": summaries.len(),
        "completed_windows": completed_windows,
        "skipped_windows": skipped_windows,
        "bucket_count": req.bucket_count,
        "status": status,
        "prediction_hash": prediction_hash,
        "experiment_run_id": experiment_run_id,
        "prediction_generation_telemetry": metadata["prediction_generation_telemetry"],
        "windows": summaries,
    }))
}

pub(crate) fn walk_forward_nonlinear_no_prediction_rows_error(
    summaries: &[WalkForwardNonlinearWindowSummary],
) -> String {
    let diagnostics = summaries
        .iter()
        .take(10)
        .map(|summary| {
            format!(
                "window={} train={}..{} predict={}..{} sample_count={} prediction_rows={} skipped={} skip_reason={}",
                summary.window_index,
                summary.train_start_date,
                summary.train_end_date,
                summary.prediction_start_date,
                summary.prediction_end_date,
                summary.sample_count,
                summary.prediction_rows,
                summary.skipped,
                summary
                    .skip_reason
                    .as_deref()
                    .unwrap_or("none")
            )
        })
        .collect::<Vec<_>>();
    format!(
        "walk-forward nonlinear ranker produced no prediction rows; window_diagnostics=[{}]",
        diagnostics.join("; ")
    )
}

pub(crate) fn walk_forward_linear_experiment_config(
    req: &NormalizedWalkForwardLinearPredictionSetRequest,
    label_definition: &Value,
) -> Value {
    json!({
        "model_code": req.model_code,
        "model_version": req.model_version,
        "model_version_id": req.model_version_id,
        "training_task_id": req.training_task_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "prediction_window": {"start": req.prediction_start_date, "end": req.prediction_end_date},
        "label": label_definition,
        "factors": req.factors,
        "trainer": "walk_forward_covariance_linear_v1",
        "train_lookback_days": req.train_lookback_days,
        "prediction_step_days": req.prediction_step_days,
        "min_training_samples": req.min_training_samples,
        "point_in_time_policy": "each window trains on dates <= train_end_date and labels are capped at train_end_date"
    })
}

pub(crate) fn walk_forward_nonlinear_quantile_ranker_experiment_config(
    req: &NormalizedWalkForwardNonlinearQuantileRankerRequest,
) -> Value {
    let linear = &req.linear;
    json!({
        "model_code": linear.model_code,
        "model_version": linear.model_version,
        "model_version_id": linear.model_version_id,
        "training_task_id": linear.training_task_id,
        "training_dataset_id": linear.training_dataset_id,
        "prediction_set_id": linear.prediction_set_id,
        "data_version_id": linear.data_version_id,
        "feature_set_version_id": linear.feature_set_version_id,
        "prediction_window": {"start": linear.prediction_start_date, "end": linear.prediction_end_date},
        "label": label_definition_json(linear.label_objective, linear.label_horizon_days),
        "factors": linear.factors,
        "trainer": "walk_forward_nonlinear_quantile_ranker_v1",
        "train_lookback_days": linear.train_lookback_days,
        "prediction_step_days": linear.prediction_step_days,
        "min_training_samples": linear.min_training_samples,
        "bucket_count": req.bucket_count,
        "min_samples_per_bucket": req.min_samples_per_bucket,
        "point_in_time_policy": "each window trains on dates <= prediction_start_date - label_horizon_days and labels are capped at train_end_date"
    })
}

pub(crate) fn normalize_walk_forward_linear_prediction_request(
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
    let label_objective = LabelObjective::parse(req.label_objective.as_deref())?;
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
        label_objective,
        min_training_samples,
        max_windows: req.max_windows,
        factors: req.factors.clone(),
    })
}

pub(crate) fn normalize_walk_forward_nonlinear_quantile_ranker_request(
    req: &WalkForwardNonlinearQuantileRankerRequest,
) -> Result<NormalizedWalkForwardNonlinearQuantileRankerRequest, String> {
    let linear_req = WalkForwardLinearPredictionSetRequest {
        model_code: req.model_code.clone(),
        model_version: req.model_version.clone(),
        model_version_id: req.model_version_id.clone(),
        training_task_id: req.training_task_id.clone(),
        prediction_set_id: req.prediction_set_id.clone().or_else(|| {
            let model_code = req.model_code.trim();
            let model_version = req.model_version.trim();
            (!model_code.is_empty() && !model_version.is_empty()).then(|| {
                format!(
                    "pred-{}-{}-nlq-wf-{}-{}",
                    model_code,
                    model_version,
                    req.prediction_start_date.trim(),
                    req.prediction_end_date.trim()
                )
            })
        }),
        data_version_id: req.data_version_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        training_dataset_id: req.training_dataset_id.clone(),
        prediction_start_date: req.prediction_start_date.clone(),
        prediction_end_date: req.prediction_end_date.clone(),
        train_lookback_days: req.train_lookback_days,
        prediction_step_days: req.prediction_step_days,
        label_horizon_days: req.label_horizon_days,
        label_objective: req.label_objective.clone(),
        min_training_samples: req.min_training_samples,
        max_windows: req.max_windows,
        factors: req.factors.clone(),
    };
    let linear = normalize_walk_forward_linear_prediction_request(&linear_req)?;
    let bucket_count = req.bucket_count.unwrap_or(5);
    if !(2..=20).contains(&bucket_count) {
        return Err("bucket_count must be between 2 and 20".into());
    }
    let min_samples_per_bucket = req.min_samples_per_bucket.unwrap_or(100);
    if min_samples_per_bucket == 0 {
        return Err("min_samples_per_bucket must be positive".into());
    }

    Ok(NormalizedWalkForwardNonlinearQuantileRankerRequest {
        linear,
        bucket_count,
        min_samples_per_bucket,
    })
}

pub(crate) fn build_walk_forward_windows(
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
