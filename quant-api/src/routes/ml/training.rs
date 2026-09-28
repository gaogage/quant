use super::*;
use chrono::{Duration, NaiveDate};
use rayon::prelude::*;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct NormalizedLinearTrainingRequest {
    pub(crate) model_code: String,
    pub(crate) model_version: String,
    pub(crate) model_version_id: String,
    pub(crate) training_task_id: String,
    pub(crate) prediction_set_id: String,
    pub(crate) data_version_id: String,
    pub(crate) feature_set_version_id: String,
    pub(crate) training_dataset_id: String,
    pub(crate) train_start_date: NaiveDate,
    pub(crate) train_end_date: NaiveDate,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
    pub(crate) label_horizon_days: i64,
    pub(crate) label_objective: LabelObjective,
    pub(crate) factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
pub(crate) struct NormalizedNonlinearQuantileRankerRequest {
    pub(crate) model_code: String,
    pub(crate) model_version: String,
    pub(crate) model_version_id: String,
    pub(crate) training_task_id: String,
    pub(crate) prediction_set_id: String,
    pub(crate) data_version_id: String,
    pub(crate) feature_set_version_id: String,
    pub(crate) training_dataset_id: String,
    pub(crate) train_start_date: NaiveDate,
    pub(crate) train_end_date: NaiveDate,
    pub(crate) prediction_start_date: NaiveDate,
    pub(crate) prediction_end_date: NaiveDate,
    pub(crate) label_horizon_days: i64,
    pub(crate) label_objective: LabelObjective,
    pub(crate) bucket_count: usize,
    pub(crate) min_samples_per_bucket: usize,
    pub(crate) factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelObjective {
    FutureReturn,
    FutureExcessReturn,
    RiskAdjustedExcessReturn,
    QualityAdjustedExcessReturn,
    QualityAdjustedRiskAdjustedExcessReturn,
    FundamentalQualityAdjustedExcessReturn,
    RegimeConditionalExcessReturn,
    GradientBoostingExcessReturn,
    MlpExcessReturn,
    AsymmetricExcessReturn,
}

impl LabelObjective {
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("future_return") => Ok(Self::FutureReturn),
            Some("future_excess_return") => Ok(Self::FutureExcessReturn),
            Some("risk_adjusted_excess_return") => Ok(Self::RiskAdjustedExcessReturn),
            Some("quality_adjusted_excess_return") => Ok(Self::QualityAdjustedExcessReturn),
            Some("quality_adjusted_risk_adjusted_excess_return") => {
                Ok(Self::QualityAdjustedRiskAdjustedExcessReturn)
            }
            Some("fundamental_quality_adjusted_excess_return") => {
                Ok(Self::FundamentalQualityAdjustedExcessReturn)
            }
            Some("regime_conditional_excess_return") => Ok(Self::RegimeConditionalExcessReturn),
            Some("gradient_boosting_excess_return") => Ok(Self::GradientBoostingExcessReturn),
            Some("mlp_excess_return") => Ok(Self::MlpExcessReturn),
            Some("asymmetric_excess_return") => Ok(Self::AsymmetricExcessReturn),
            Some(other) => Err(format!(
                "label_objective must be one of future_return, future_excess_return, risk_adjusted_excess_return, quality_adjusted_excess_return, quality_adjusted_risk_adjusted_excess_return, fundamental_quality_adjusted_excess_return, regime_conditional_excess_return, gradient_boosting_excess_return, asymmetric_excess_return; got {}",
                other
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::FutureReturn => "future_return",
            Self::FutureExcessReturn => "future_excess_return",
            Self::RiskAdjustedExcessReturn => "risk_adjusted_excess_return",
            Self::QualityAdjustedExcessReturn => "quality_adjusted_excess_return",
            Self::QualityAdjustedRiskAdjustedExcessReturn => {
                "quality_adjusted_risk_adjusted_excess_return"
            }
            Self::FundamentalQualityAdjustedExcessReturn => {
                "fundamental_quality_adjusted_excess_return"
            }
            Self::RegimeConditionalExcessReturn => "regime_conditional_excess_return",
            Self::GradientBoostingExcessReturn => "gradient_boosting_excess_return",
            Self::MlpExcessReturn => "mlp_excess_return",
            Self::AsymmetricExcessReturn => "asymmetric_excess_return",
        }
    }

    pub(crate) fn requires_benchmark(self) -> bool {
        matches!(
            self,
            Self::FutureExcessReturn
                | Self::RiskAdjustedExcessReturn
                | Self::QualityAdjustedExcessReturn
                | Self::QualityAdjustedRiskAdjustedExcessReturn
                | Self::FundamentalQualityAdjustedExcessReturn
                | Self::RegimeConditionalExcessReturn
                | Self::GradientBoostingExcessReturn
                | Self::MlpExcessReturn
                | Self::AsymmetricExcessReturn
        )
    }

    pub(crate) fn is_quality_adjusted(self) -> bool {
        matches!(
            self,
            Self::QualityAdjustedExcessReturn
                | Self::QualityAdjustedRiskAdjustedExcessReturn
                | Self::FundamentalQualityAdjustedExcessReturn
                | Self::RegimeConditionalExcessReturn
                | Self::GradientBoostingExcessReturn
        )
    }
    pub(crate) fn uses_fundamental_quality(self) -> bool {
        matches!(self, Self::FundamentalQualityAdjustedExcessReturn)
    }
    pub(crate) fn uses_regime_conditioning(self) -> bool {
        matches!(self, Self::RegimeConditionalExcessReturn)
    }
    pub(crate) fn uses_gradient_boosting(self) -> bool {
        matches!(self, Self::GradientBoostingExcessReturn)
    }
    pub(crate) fn uses_mlp(self) -> bool {
        matches!(self, Self::MlpExcessReturn)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TrainingFeatureMatrixRow {
    pub(crate) symbol: String,
    pub(crate) trade_date: NaiveDate,
    pub(crate) features: Vec<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct FeatureMatrixWindowCache {
    pub(crate) rows: Vec<TrainingFeatureMatrixRow>,
}

impl FeatureMatrixWindowCache {
    pub(crate) fn new(mut rows: Vec<TrainingFeatureMatrixRow>) -> Self {
        rows.sort_by(|left, right| {
            left.trade_date
                .cmp(&right.trade_date)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Self { rows }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn slice(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Vec<TrainingFeatureMatrixRow> {
        self.rows
            .iter()
            .filter(|row| row.trade_date >= start_date && row.trade_date <= end_date)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TrainingSample {
    pub(crate) features: Vec<f64>,
    pub(crate) label: f64,
    pub(crate) regime_tag: Option<MarketRegimeTag>,
}

pub(crate) async fn train_linear_model_inner(
    db: &sqlx::PgPool,
    req: TrainLinearModelRequest,
) -> Result<Value, String> {
    let req = normalize_linear_training_request(&req)?;
    ensure_data_version_exists(db, &req.data_version_id).await?;
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

    let label_definition = label_definition_json(req.label_objective, req.label_horizon_days);
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

pub(crate) async fn train_nonlinear_quantile_ranker_inner(
    db: &sqlx::PgPool,
    req: TrainNonlinearQuantileRankerRequest,
) -> Result<Value, String> {
    let started_at = Instant::now();
    let req = normalize_nonlinear_quantile_ranker_request(&req)?;
    ensure_data_version_exists(db, &req.data_version_id).await?;
    let training_req = NormalizedLinearTrainingRequest {
        model_code: req.model_code.clone(),
        model_version: req.model_version.clone(),
        model_version_id: req.model_version_id.clone(),
        training_task_id: req.training_task_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        data_version_id: req.data_version_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        training_dataset_id: req.training_dataset_id.clone(),
        train_start_date: req.train_start_date,
        train_end_date: req.train_end_date,
        prediction_start_date: req.prediction_start_date,
        prediction_end_date: req.prediction_end_date,
        label_horizon_days: req.label_horizon_days,
        label_objective: req.label_objective,
        factors: req.factors.clone(),
    };
    let samples = load_training_samples(db, &training_req).await?;
    let regime_split = req.label_objective.uses_regime_conditioning();
    let use_gb = req.label_objective.uses_gradient_boosting();
    let mut gb_model: Option<GradientBoostingModel> = None;
    let mut mlp_model: Option<MlpModel> = None;
    if use_gb {
        gb_model = Some(fit_gradient_boosting(
            &samples,
            req.factors.len(),
            100,
            4,
            0.05,
        )?);
    }
    if req.label_objective.uses_mlp() {
        mlp_model = Some(fit_mlp(
            &samples,
            req.factors.len(),
            128,
            64,
            100,
            256,
            0.001,
            0.0001,
        )?);
    }
    let model = fit_nonlinear_quantile_ranker(
        &samples,
        req.factors.len(),
        req.bucket_count,
        req.min_samples_per_bucket,
    )?;
    let mut regime_split_model: Option<RegimeSplitModel> = None;
    if regime_split {
        let (bull, bear, sideways) = split_samples_by_regime(&samples);
        let factor_count = req.factors.len();
        let bull_model = if bull.len() >= req.min_samples_per_bucket * 2 {
            Some(fit_nonlinear_quantile_ranker(
                &bull,
                factor_count,
                req.bucket_count,
                req.min_samples_per_bucket,
            )?)
        } else {
            None
        };
        let bear_model = if bear.len() >= req.min_samples_per_bucket * 2 {
            Some(fit_nonlinear_quantile_ranker(
                &bear,
                factor_count,
                req.bucket_count,
                req.min_samples_per_bucket,
            )?)
        } else {
            None
        };
        let sideways_model = if sideways.len() >= req.min_samples_per_bucket * 2 {
            Some(fit_nonlinear_quantile_ranker(
                &sideways,
                factor_count,
                req.bucket_count,
                req.min_samples_per_bucket,
            )?)
        } else {
            None
        };
        regime_split_model = Some(RegimeSplitModel {
            bull: bull_model,
            bear: bear_model,
            sideways: sideways_model,
            bull_samples: bull.len(),
            bear_samples: bear.len(),
            sideways_samples: sideways.len(),
            factor_count,
        });
    }
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
        factors: req
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
    let feature_rows = load_prediction_feature_matrix_rows(db, &prediction_req).await?;
    let load_elapsed = load_started_at.elapsed();
    let feature_matrix_cache = feature_matrix_cache_metadata(
        "prediction_window",
        req.prediction_start_date,
        req.prediction_end_date,
        feature_rows.len(),
    );
    let scoring_started_at = Instant::now();
    let rows = if let Some(ref rsm) = regime_split_model {
        if rsm.any_model() {
            let benchmark_closes = load_benchmark_closes(
                db,
                "000300.SH",
                req.prediction_start_date - Duration::days(120),
                req.prediction_end_date,
            )
            .await?;
            nonlinear_prediction_rows_with_regime_split(
                &req.prediction_set_id,
                feature_rows,
                rsm,
                &benchmark_closes,
            )?
        } else {
            nonlinear_prediction_rows_from_feature_matrix_rows(
                &req.prediction_set_id,
                feature_rows,
                &model,
            )?
        }
    } else if let Some(ref gb) = gb_model {
        nonlinear_prediction_rows_with_gb(&req.prediction_set_id, feature_rows, gb)?
    } else if let Some(ref mlp) = mlp_model {
        nonlinear_prediction_rows_with_mlp(&req.prediction_set_id, feature_rows, mlp)?
    } else {
        nonlinear_prediction_rows_from_feature_matrix_rows(
            &req.prediction_set_id,
            feature_rows,
            &model,
        )?
    };
    let scoring_elapsed = scoring_started_at.elapsed();
    if rows.is_empty() {
        return Err("nonlinear quantile ranker found no prediction factor values".into());
    }

    let label_definition = label_definition_json(req.label_objective, req.label_horizon_days);
    let feature_config = json!({
        "feature_set_version_id": req.feature_set_version_id,
        "factors": req.factors,
        "point_in_time_policy": "factor_value.available_at <= trade_date",
    });
    let hyperparameters = if let Some(ref gb) = gb_model {
        json!({
            "trainer": "gradient_boosting_v1",
            "num_trees": gb.trees.len(),
            "learning_rate": gb.learning_rate,
            "init_value": gb.init_value,
            "scoring": "gradient_boosting_sum",
        })
    } else if regime_split_model.is_some() {
        json!({
            "trainer": "nonlinear_quantile_ranker_regime_split_v1",
            "bucket_count": req.bucket_count,
            "min_samples_per_bucket": req.min_samples_per_bucket,
            "scoring": "sum_train_window_bucket_mean_label",
        })
    } else {
        json!({
            "trainer": "nonlinear_quantile_ranker_v1",
            "bucket_count": req.bucket_count,
            "min_samples_per_bucket": req.min_samples_per_bucket,
            "scoring": "sum_train_window_bucket_mean_label",
        })
    };
    let model_json = if let Some(ref gb) = gb_model {
        serde_json::to_value(gb)
            .map_err(|error| format!("Failed to serialize GB model: {}", error))?
    } else if let Some(ref rsm) = regime_split_model {
        serde_json::to_value(rsm)
            .map_err(|error| format!("Failed to serialize regime split model: {}", error))?
    } else {
        serde_json::to_value(&model)
            .map_err(|error| format!("Failed to serialize nonlinear ranker model: {}", error))?
    };
    let insert_telemetry = prediction_insert_telemetry(&rows);
    let metadata = nonlinear_quantile_ranker_prediction_set_metadata(
        &req.training_task_id,
        samples.len(),
        &label_definition,
        model_json.clone(),
        feature_matrix_cache,
        insert_telemetry.clone(),
        prediction_generation_telemetry(
            "train_only_nonlinear_quantile_ranker",
            1,
            1,
            0,
            rows.len(),
            started_at.elapsed(),
            vec![
                prediction_progress_stage(
                    "load_prediction_feature_matrix",
                    1,
                    1,
                    rows.len(),
                    load_elapsed,
                ),
                prediction_progress_stage(
                    "score_prediction_window",
                    1,
                    1,
                    rows.len(),
                    scoring_elapsed,
                ),
            ],
        ),
    );
    let dataset_hash = stable_metadata_hash(&json!({
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "training_window": {"start": req.train_start_date, "end": req.train_end_date},
        "label_definition": label_definition,
        "factors": feature_config["factors"],
        "trainer": hyperparameters,
    }));
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": req.model_version_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "start_date": req.prediction_start_date,
        "end_date": req.prediction_end_date,
        "model": metadata["model"],
    }));
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let experiment_config = nonlinear_quantile_ranker_experiment_config(&req);
    let experiment_metrics = nonlinear_quantile_ranker_experiment_metrics(
        samples.len(),
        rows.len(),
        &metadata["model"],
        &artifact_hash,
        &prediction_hash,
        &metadata["feature_matrix_cache"],
        &insert_telemetry,
        &metadata["prediction_generation_telemetry"],
    );

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin nonlinear ranker transaction: {}", error))?;

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
    .bind(json!({"type": "train_predict_split", "selection_scope": "train_window_only"}))
    .bind(&dataset_hash)
    .bind(json!({"training_task_id": req.training_task_id, "sample_count": samples.len()}))
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert nonlinear training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_training_task
           (training_task_id, model_code, model_type, data_version_id, training_dataset_id,
            feature_config, label_definition, hyperparameters, status, progress,
            last_heartbeat_at, heartbeat_timeout_seconds, started_at, completed_at)
         VALUES ($1, $2, 'nonlinear_quantile_ranker', $3, $4, $5, $6, $7, 'completed', 100,
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
    .map_err(|error| format!("Failed to upsert nonlinear model_training_task: {}", error))?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'nonlinear_quantile_ranker', $3, $4, $5, $6,
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
    .bind(json!({"sample_count": samples.len(), "bucket_count": req.bucket_count}))
    .bind(json!({"prediction_row_count": rows.len()}))
    .bind(format!("artifact://{}", req.model_version_id))
    .bind(&artifact_hash)
    .bind(&req.training_dataset_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert nonlinear model_registry: {}", error))?;

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
    .map_err(|error| format!("Failed to upsert nonlinear prediction_set: {}", error))?;

    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(&req.prediction_set_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to clear nonlinear model_prediction: {}", error))?;
    insert_prediction_rows(&mut tx, &rows).await?;

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'ml_nonlinear_quantile_ranker', 'model_training_task', $2, $3, $4,
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
    .map_err(|error| format!("Failed to insert nonlinear experiment_run: {}", error))?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit nonlinear ranker transaction: {}", error))?;

    Ok(json!({
        "training_task_id": req.training_task_id,
        "model_version_id": req.model_version_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "sample_count": samples.len(),
        "prediction_rows": rows.len(),
        "bucket_count": req.bucket_count,
        "feature_matrix_cache": metadata["feature_matrix_cache"],
        "prediction_insert_telemetry": metadata["prediction_insert_telemetry"],
        "prediction_generation_telemetry": metadata["prediction_generation_telemetry"],
        "prediction_hash": prediction_hash,
        "experiment_run_id": experiment_run_id,
        "status": "completed",
    }))
}

pub(crate) fn linear_training_experiment_config(req: &NormalizedLinearTrainingRequest) -> Value {
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
            "type": req.label_objective.as_str(),
            "horizon_trading_days": req.label_horizon_days,
            "price": "close",
            "benchmark": if req.label_objective.requires_benchmark() { Some("000300.SH") } else { None::<&str> },
            "risk_adjustment": if req.label_objective == LabelObjective::RiskAdjustedExcessReturn {
                Some("forward_downside_volatility_floor_1pct")
            } else {
                None::<&str>
            }
        },
        "factors": req.factors,
        "trainer": "covariance_linear_v1",
        "point_in_time_policy": "factor_value.available_at <= trade_date"
    })
}

pub(crate) fn nonlinear_quantile_ranker_experiment_config(
    req: &NormalizedNonlinearQuantileRankerRequest,
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
        "train_window": {"start": req.train_start_date, "end": req.train_end_date},
        "prediction_window": {"start": req.prediction_start_date, "end": req.prediction_end_date},
        "label": {
            "type": req.label_objective.as_str(),
            "horizon_trading_days": req.label_horizon_days,
            "price": "close",
            "benchmark": if req.label_objective.requires_benchmark() { Some("000300.SH") } else { None::<&str> },
            "risk_adjustment": if req.label_objective == LabelObjective::RiskAdjustedExcessReturn {
                Some("forward_downside_volatility_floor_1pct")
            } else {
                None::<&str>
            }
        },
        "factors": req.factors,
        "trainer": "nonlinear_quantile_ranker_v1",
        "bucket_count": req.bucket_count,
        "min_samples_per_bucket": req.min_samples_per_bucket,
        "point_in_time_policy": "factor_value.available_at <= trade_date; labels capped at train_end_date"
    })
}

pub(crate) fn nonlinear_quantile_ranker_prediction_set_metadata(
    training_task_id: &str,
    sample_count: usize,
    label_definition: &Value,
    model: Value,
    feature_matrix_cache: Value,
    prediction_insert_telemetry: Value,
    prediction_generation_telemetry: Value,
) -> Value {
    json!({
        "model_type": "nonlinear_quantile_ranker",
        "training_task_id": training_task_id,
        "sample_count": sample_count,
        "label_definition": label_definition,
        "feature_matrix_cache": feature_matrix_cache,
        "prediction_insert_telemetry": prediction_insert_telemetry,
        "prediction_generation_telemetry": prediction_generation_telemetry,
        "point_in_time_policy": "model_prediction.available_at = trade_date",
        "model": model,
    })
}

pub(crate) fn nonlinear_quantile_ranker_experiment_metrics(
    sample_count: usize,
    prediction_rows: usize,
    model: &Value,
    artifact_hash: &str,
    prediction_hash: &str,
    feature_matrix_cache: &Value,
    prediction_insert_telemetry: &Value,
    prediction_generation_telemetry: &Value,
) -> Value {
    json!({
        "sample_count": sample_count,
        "prediction_rows": prediction_rows,
        "model": model,
        "artifact_hash": artifact_hash,
        "prediction_hash": prediction_hash,
        "feature_matrix_cache": feature_matrix_cache,
        "prediction_insert_telemetry": prediction_insert_telemetry,
        "prediction_generation_telemetry": prediction_generation_telemetry,
        "prediction_point_in_time_policy": "model_prediction.available_at = trade_date",
        "status": "training_and_prediction_completed"
    })
}

pub(crate) fn linear_training_experiment_metrics(
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

pub(crate) fn label_definition_json(label_objective: LabelObjective, horizon_days: i64) -> Value {
    let is_quality_adjusted = label_objective.is_quality_adjusted();
    let quality_adjustment_value = if is_quality_adjusted {
        json!({
            "method": "trailing_volatility_and_max_drawdown_penalty",
            "vol_lookback_days": 60,
            "dd_lookback_days": 120,
            "vol_floor": 0.25,
            "dd_floor": 0.15,
            "vol_weight": 1.5,
            "dd_weight": 1.0
        })
    } else {
        Value::Null
    };
    json!({
        "label": label_objective.as_str(),
        "horizon_trading_days": horizon_days,
        "price": "close",
        "benchmark": if label_objective.requires_benchmark() { Some("000300.SH") } else { None::<&str> },
        "risk_adjustment": if label_objective == LabelObjective::RiskAdjustedExcessReturn || label_objective == LabelObjective::QualityAdjustedRiskAdjustedExcessReturn {
            Some("forward_downside_volatility_floor_1pct")
        } else {
            None::<&str>
        },
        "quality_adjustment": quality_adjustment_value,
        "point_in_time_policy": "labels are computed only inside the training window; prediction windows never contribute labels"
    })
}

pub(crate) fn normalize_linear_training_request(
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
    let label_objective = LabelObjective::parse(req.label_objective.as_deref())?;

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
        label_objective,
        factors: req.factors.clone(),
    })
}

pub(crate) fn normalize_nonlinear_quantile_ranker_request(
    req: &TrainNonlinearQuantileRankerRequest,
) -> Result<NormalizedNonlinearQuantileRankerRequest, String> {
    let linear_req = TrainLinearModelRequest {
        model_code: req.model_code.clone(),
        model_version: req.model_version.clone(),
        model_version_id: req.model_version_id.clone(),
        training_task_id: req.training_task_id.clone(),
        prediction_set_id: req.prediction_set_id.clone(),
        data_version_id: req.data_version_id.clone(),
        feature_set_version_id: req.feature_set_version_id.clone(),
        training_dataset_id: req.training_dataset_id.clone(),
        train_start_date: req.train_start_date.clone(),
        train_end_date: req.train_end_date.clone(),
        prediction_start_date: req.prediction_start_date.clone(),
        prediction_end_date: req.prediction_end_date.clone(),
        label_horizon_days: req.label_horizon_days,
        label_objective: req.label_objective.clone(),
        factors: req.factors.clone(),
    };
    let normalized = normalize_linear_training_request(&linear_req)?;
    let bucket_count = req.bucket_count.unwrap_or(5);
    if !(2..=20).contains(&bucket_count) {
        return Err("bucket_count must be between 2 and 20".into());
    }
    let min_samples_per_bucket = req.min_samples_per_bucket.unwrap_or(100);
    if min_samples_per_bucket == 0 {
        return Err("min_samples_per_bucket must be positive".into());
    }

    Ok(NormalizedNonlinearQuantileRankerRequest {
        model_code: normalized.model_code,
        model_version: normalized.model_version,
        model_version_id: normalized.model_version_id,
        training_task_id: normalized.training_task_id,
        prediction_set_id: normalized.prediction_set_id,
        data_version_id: normalized.data_version_id,
        feature_set_version_id: normalized.feature_set_version_id,
        training_dataset_id: normalized.training_dataset_id,
        train_start_date: normalized.train_start_date,
        train_end_date: normalized.train_end_date,
        prediction_start_date: normalized.prediction_start_date,
        prediction_end_date: normalized.prediction_end_date,
        label_horizon_days: normalized.label_horizon_days,
        label_objective: normalized.label_objective,
        bucket_count,
        min_samples_per_bucket,
        factors: normalized.factors,
    })
}

pub(crate) async fn load_training_samples(
    db: &sqlx::PgPool,
    req: &NormalizedLinearTrainingRequest,
) -> Result<Vec<TrainingSample>, String> {
    let feature_rows = load_training_feature_matrix_rows(db, req).await?;
    load_training_samples_from_feature_rows(db, req, feature_rows).await
}

pub(crate) async fn load_training_samples_from_feature_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearTrainingRequest,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
) -> Result<Vec<TrainingSample>, String> {
    let price_start_date = training_label_price_start_date(req);
    let label_end_date = req.train_end_date + Duration::days(req.label_horizon_days + 7);
    let price_rows = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(
        "SELECT symbol, trade_date, close::double precision
         FROM market_stock_daily_bar_adj
         WHERE trade_date >= $1
           AND trade_date <= $2
           AND close IS NOT NULL
         ORDER BY symbol, trade_date",
    )
    .bind(price_start_date)
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

    let benchmark_closes = if req.label_objective.requires_benchmark() {
        load_benchmark_closes(db, "000300.SH", price_start_date, label_end_date).await?
    } else {
        Vec::new()
    };

    Ok(training_samples_from_feature_matrix_rows(
        feature_rows,
        &closes_by_symbol,
        &benchmark_closes,
        req.label_objective,
        Some(req.train_end_date),
        req.label_horizon_days,
        req.factors.len(),
    ))
}

pub(crate) fn training_label_price_start_date(req: &NormalizedLinearTrainingRequest) -> NaiveDate {
    req.train_start_date
        - Duration::days(training_label_price_history_lookback_days(
            req.label_objective,
        ))
}

pub(crate) fn training_label_price_history_lookback_days(label_objective: LabelObjective) -> i64 {
    match label_objective {
        LabelObjective::QualityAdjustedExcessReturn
        | LabelObjective::QualityAdjustedRiskAdjustedExcessReturn
        | LabelObjective::FundamentalQualityAdjustedExcessReturn => 252,
        LabelObjective::RegimeConditionalExcessReturn => 126,
        _ => 0,
    }
}

pub(crate) async fn load_benchmark_closes(
    db: &sqlx::PgPool,
    benchmark_symbol: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<(NaiveDate, f64)>, String> {
    let rows = sqlx::query_as::<_, (NaiveDate, Option<f64>)>(
        "SELECT trade_date, close::double precision
         FROM market_index_daily_bar
         WHERE symbol = $1
           AND trade_date >= $2
           AND trade_date <= $3
           AND close IS NOT NULL
         ORDER BY trade_date",
    )
    .bind(benchmark_symbol)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load benchmark labels: {}", error))?;

    Ok(rows
        .into_iter()
        .filter_map(|(trade_date, close)| {
            close
                .filter(|value| value.is_finite() && *value > 0.0)
                .map(|close| (trade_date, close))
        })
        .collect())
}

pub(crate) async fn load_training_feature_matrix_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearTrainingRequest,
) -> Result<Vec<TrainingFeatureMatrixRow>, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "WITH requested(factor_code, factor_version, factor_idx) AS (",
    );
    builder.push_values(
        req.factors.iter().enumerate(),
        |mut row, (factor_idx, factor)| {
            row.push_bind(&factor.factor_code)
                .push_bind(&factor.factor_version)
                .push_bind(factor_idx as i32);
        },
    );
    builder.push(
        ")
         SELECT fv.symbol,
                fv.trade_date,
                array_agg(fv.normalized_value::double precision ORDER BY requested.factor_idx)::double precision[] AS features
         FROM factor_value fv
         JOIN requested
           ON requested.factor_code = fv.factor_code
          AND requested.factor_version = fv.factor_version
         WHERE fv.trade_date >= ",
    );
    builder.push_bind(req.train_start_date);
    builder.push(
        "
           AND fv.trade_date <= ",
    );
    builder.push_bind(req.train_end_date);
    builder.push(
        "
           AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
         GROUP BY fv.symbol, fv.trade_date
         HAVING COUNT(*) = ",
    );
    builder.push_bind(req.factors.len() as i64);
    builder.push(
        "
            AND bool_and(fv.normalized_value IS NOT NULL)
         ORDER BY fv.trade_date, fv.symbol",
    );

    let rows = builder
        .build_query_as::<(String, NaiveDate, Vec<f64>)>()
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load training factor matrix: {}", error))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, trade_date, features)| TrainingFeatureMatrixRow {
            symbol,
            trade_date,
            features,
        })
        .collect())
}

pub(crate) fn training_samples_from_feature_matrix_rows(
    rows: Vec<TrainingFeatureMatrixRow>,
    closes_by_symbol: &HashMap<String, Vec<(NaiveDate, f64)>>,
    benchmark_closes: &Vec<(NaiveDate, f64)>,
    label_objective: LabelObjective,
    max_label_date: Option<NaiveDate>,
    horizon_days: i64,
    factor_count: usize,
) -> Vec<TrainingSample> {
    let mut samples = Vec::new();
    for row in rows {
        if row.features.len() != factor_count || row.features.iter().any(|value| !value.is_finite())
        {
            continue;
        }
        let quality_features = label_objective
            .uses_fundamental_quality()
            .then_some(row.features.as_slice());
        let Some(label) = label_for_objective_with_features(
            label_objective,
            closes_by_symbol.get(&row.symbol),
            Some(benchmark_closes).filter(|closes| !closes.is_empty()),
            row.trade_date,
            max_label_date,
            horizon_days,
            quality_features,
        ) else {
            continue;
        };
        if label.is_finite() {
            let regime_tag =
                if label_objective.uses_regime_conditioning() && !benchmark_closes.is_empty() {
                    Some(MarketRegimeTag::from_benchmark_trailing(
                        benchmark_closes,
                        row.trade_date,
                    ))
                } else {
                    None
                };
            samples.push(TrainingSample {
                features: row.features,
                label,
                regime_tag,
            });
        }
    }
    samples
}

pub(crate) fn future_return_label_until(
    closes: Option<&Vec<(NaiveDate, f64)>>,
    trade_date: NaiveDate,
    max_label_date: Option<NaiveDate>,
    horizon_days: i64,
) -> Option<f64> {
    let closes = closes?;
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let target_idx = current_idx.checked_add(horizon_days as usize)?;
    let (_, current_close) = closes.get(current_idx)?;
    let (future_date, future_close) = closes.get(target_idx)?;
    if max_label_date.is_some_and(|max_date| *future_date > max_date) {
        return None;
    }
    if *future_close > 0.0 {
        Some((future_close / current_close) - 1.0)
    } else {
        None
    }
}

pub(crate) fn label_for_objective(
    label_objective: LabelObjective,
    closes: Option<&Vec<(NaiveDate, f64)>>,
    benchmark_closes: Option<&Vec<(NaiveDate, f64)>>,
    trade_date: NaiveDate,
    max_label_date: Option<NaiveDate>,
    horizon_days: i64,
) -> Option<f64> {
    let stock_return = future_return_label_until(closes, trade_date, max_label_date, horizon_days)?;
    match label_objective {
        LabelObjective::FutureReturn => Some(stock_return),
        LabelObjective::FutureExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            Some(stock_return - benchmark_return)
        }
        LabelObjective::RiskAdjustedExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            let downside_volatility =
                forward_downside_volatility(closes?, trade_date, max_label_date, horizon_days)?;
            Some((stock_return - benchmark_return) / downside_volatility.max(0.01))
        }
        LabelObjective::QualityAdjustedExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            let quality = quality_adjustment(closes?, trade_date, 60, 120, 0.25, 0.15, 1.5, 1.0)?;
            Some((stock_return - benchmark_return) * quality)
        }
        LabelObjective::QualityAdjustedRiskAdjustedExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            let downside_volatility =
                forward_downside_volatility(closes?, trade_date, max_label_date, horizon_days)?;
            let quality = quality_adjustment(closes?, trade_date, 60, 120, 0.25, 0.15, 1.5, 1.0)?;
            Some((stock_return - benchmark_return) / downside_volatility.max(0.01) * quality)
        }
        LabelObjective::FundamentalQualityAdjustedExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            let price_quality =
                quality_adjustment(closes?, trade_date, 60, 120, 0.25, 0.15, 1.5, 1.0)?;
            Some((stock_return - benchmark_return) * price_quality)
        }
        LabelObjective::RegimeConditionalExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            Some(stock_return - benchmark_return)
        }
        LabelObjective::GradientBoostingExcessReturn | LabelObjective::MlpExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            Some(stock_return - benchmark_return)
        }
        LabelObjective::AsymmetricExcessReturn => {
            let benchmark_return = future_return_label_until(
                benchmark_closes,
                trade_date,
                max_label_date,
                horizon_days,
            )?;
            let excess = stock_return - benchmark_return;
            // Asymmetric penalty: negative returns penalized 2x, positive returns unchanged
            if excess > 0.0 {
                Some(excess)
            } else {
                Some(excess * 2.0)
            }
        }
    }
}

/// Compute a fundamental quality score from a slice of PIT feature values.
///
/// The quality score is the average of the first `quality_feature_count` features
/// after applying a sigmoid normalization to each. Features are expected to be
/// standardized (z-score), so sigmoid maps roughly [-3,3] → [0.05, 0.95].
///
/// The quality score is in (0, 1], where higher values indicate higher quality.
/// This ensures the multiplier is always ≤ 1.0.
pub(crate) fn fundamental_quality_score(features: &[f64], quality_feature_count: usize) -> f64 {
    let count = quality_feature_count.min(features.len());
    if count == 0 {
        return 1.0;
    }
    let sum: f64 = features[..count]
        .iter()
        .map(|&v| {
            if v.is_finite() {
                1.0 / (1.0 + (-v).exp())
            } else {
                0.5
            }
        })
        .sum();
    sum / count as f64
}

/// Like `label_for_objective` but accepts PIT feature values for fundamental-quality-aware labels.
pub(crate) fn label_for_objective_with_features(
    label_objective: LabelObjective,
    closes: Option<&Vec<(NaiveDate, f64)>>,
    benchmark_closes: Option<&Vec<(NaiveDate, f64)>>,
    trade_date: NaiveDate,
    max_label_date: Option<NaiveDate>,
    horizon_days: i64,
    quality_features: Option<&[f64]>,
) -> Option<f64> {
    let stock_return = future_return_label_until(closes, trade_date, max_label_date, horizon_days)?;
    if label_objective.uses_fundamental_quality() {
        let benchmark_return =
            future_return_label_until(benchmark_closes, trade_date, max_label_date, horizon_days)?;
        let price_quality = quality_adjustment(closes?, trade_date, 60, 120, 0.25, 0.15, 1.5, 1.5)?;
        let fund_quality = quality_features
            .map(|features| fundamental_quality_score(features, 12))
            .unwrap_or(1.0);
        let blended_quality = price_quality * 0.4 + fund_quality * 0.6;
        return Some((stock_return - benchmark_return) * blended_quality);
    }
    label_for_objective(
        label_objective,
        closes,
        benchmark_closes,
        trade_date,
        max_label_date,
        horizon_days,
    )
}

pub(crate) fn forward_downside_volatility(
    closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
    max_label_date: Option<NaiveDate>,
    horizon_days: i64,
) -> Option<f64> {
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let target_idx = current_idx.checked_add(horizon_days as usize)?;
    if target_idx >= closes.len() {
        return None;
    }
    if max_label_date.is_some_and(|max_date| closes[target_idx].0 > max_date) {
        return None;
    }

    let mut downside_squares = Vec::new();
    for pair in closes[current_idx..=target_idx].windows(2) {
        let previous = pair[0].1;
        let current = pair[1].1;
        if !previous.is_finite() || !current.is_finite() || previous <= 0.0 || current <= 0.0 {
            return None;
        }
        let daily_return = (current / previous) - 1.0;
        if daily_return < 0.0 {
            downside_squares.push(daily_return * daily_return);
        }
    }

    if downside_squares.is_empty() {
        return Some(0.01);
    }
    let mean = downside_squares.iter().sum::<f64>() / downside_squares.len() as f64;
    Some(mean.sqrt())
}

/// Trailing annualized volatility computed from daily returns over `lookback_days` prior to `trade_date`.
/// Uses only data available at trade_date and requires a minimum of 20 valid daily returns.
pub(crate) fn trailing_volatility(
    closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
    lookback_days: i64,
) -> Option<f64> {
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let start_idx = current_idx.checked_sub(lookback_days as usize)?;
    let window = &closes[start_idx..=current_idx];
    let mut daily_returns = Vec::new();
    for pair in window.windows(2) {
        let prev = pair[0].1;
        let curr = pair[1].1;
        if !prev.is_finite() || !curr.is_finite() || prev <= 0.0 || curr <= 0.0 {
            continue;
        }
        daily_returns.push((curr / prev) - 1.0);
    }
    if daily_returns.len() < 20 {
        return None;
    }
    let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns
        .iter()
        .map(|r| (r - mean) * (r - mean))
        .sum::<f64>()
        / (daily_returns.len() - 1) as f64;
    Some(variance.sqrt() * (252_f64).sqrt())
}

/// Trailing maximum drawdown from peak over `lookback_days` prior to `trade_date`.
/// Returns the drawdown as a positive decimal (e.g. 0.15 = 15% drawdown).
pub(crate) fn trailing_max_drawdown(
    closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
    lookback_days: i64,
) -> Option<f64> {
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let start_idx = current_idx.checked_sub(lookback_days as usize)?;
    let window = &closes[start_idx..=current_idx];
    let mut peak: f64 = 0.0;
    let mut max_dd: f64 = 0.0;
    for (_date, close) in window {
        if !close.is_finite() || *close <= 0.0 {
            continue;
        }
        if *close > peak {
            peak = *close;
        }
        if peak > 0.0 {
            let dd = (peak - *close) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    if peak <= 0.0 {
        return None;
    }
    Some(max_dd)
}

/// Quality adjustment multiplier for label computation.
/// Returns a value in (0.0, 1.0] where lower values indicate higher quality penalty.
///
/// The penalty is driven by two signals available PIT from price data:
/// - Trailing 60-day annualized volatility (higher vol → larger penalty)
/// - Trailing 120-day maximum drawdown (larger dd → larger penalty)
///
/// The adjustment formula:
///   vol_penalty = max(0, trailing_vol - vol_floor) * vol_weight
///   dd_penalty  = max(0, trailing_max_dd - dd_floor) * dd_weight
///   multiplier  = 1.0 / (1.0 + vol_penalty + dd_penalty)
///
/// This ensures the multiplier is ≤ 1.0, so quality-adjusted labels are always
/// conservative relative to raw excess returns.
pub(crate) fn quality_adjustment(
    closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
    vol_lookback_days: i64,
    dd_lookback_days: i64,
    vol_floor: f64,
    dd_floor: f64,
    vol_weight: f64,
    dd_weight: f64,
) -> Option<f64> {
    let vol = trailing_volatility(closes, trade_date, vol_lookback_days)?;
    let max_dd = trailing_max_drawdown(closes, trade_date, dd_lookback_days)?;
    let vol_penalty = (vol - vol_floor).max(0.0) * vol_weight;
    let dd_penalty = (max_dd - dd_floor).max(0.0) * dd_weight;
    let multiplier = 1.0 / (1.0 + vol_penalty + dd_penalty);
    Some(multiplier)
}

pub(crate) fn fit_linear_weights(samples: &[TrainingSample], factor_count: usize) -> Vec<f64> {
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

pub(crate) fn normalize_weights(mut weights: Vec<f64>) -> Vec<f64> {
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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NonlinearQuantileRanker {
    pub(crate) factor_count: usize,
    pub(crate) bucket_count: usize,
    pub(crate) min_samples_per_bucket: usize,
    pub(crate) tables: Vec<NonlinearQuantileFactorTable>,
    pub(crate) pairwise_tables: Vec<NonlinearQuantilePairwiseTable>,
    pub(crate) global_label_mean: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NonlinearQuantileFactorTable {
    pub(crate) factor_idx: usize,
    pub(crate) cutpoints: Vec<f64>,
    pub(crate) bucket_scores: Vec<f64>,
    bucket_counts: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NonlinearQuantilePairwiseTable {
    pub(crate) factor_i: usize,
    pub(crate) factor_j: usize,
    /// Flat 2D array: scores[bucket_i * bucket_count + bucket_j]
    pub(crate) scores: Vec<f64>,
    counts: Vec<usize>,
}

pub(crate) fn fit_nonlinear_quantile_ranker(
    samples: &[TrainingSample],
    factor_count: usize,
    bucket_count: usize,
    min_samples_per_bucket: usize,
) -> Result<NonlinearQuantileRanker, String> {
    if factor_count == 0 {
        return Err("nonlinear ranker factor_count must be positive".into());
    }
    let bucket_count = bucket_count.clamp(2, 20);
    let min_samples_per_bucket = min_samples_per_bucket.max(1);
    let usable_labels = samples
        .iter()
        .filter(|sample| {
            sample.features.len() == factor_count
                && sample.label.is_finite()
                && sample.features.iter().all(|value| value.is_finite())
        })
        .map(|sample| sample.label)
        .collect::<Vec<_>>();
    if usable_labels.len() < bucket_count * min_samples_per_bucket {
        return Err(format!(
            "nonlinear ranker found too few complete samples: {}",
            usable_labels.len()
        ));
    }
    let global_label_mean = usable_labels.iter().sum::<f64>() / usable_labels.len() as f64;
    let mut tables = Vec::with_capacity(factor_count);

    for factor_idx in 0..factor_count {
        let mut pairs = samples
            .iter()
            .filter_map(|sample| {
                if sample.features.len() != factor_count || !sample.label.is_finite() {
                    return None;
                }
                let feature = sample.features[factor_idx];
                (feature.is_finite()).then_some((feature, sample.label))
            })
            .collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.total_cmp(&right.0));
        if pairs.len() < bucket_count * min_samples_per_bucket {
            return Err(format!(
                "nonlinear ranker factor {} found too few complete samples: {}",
                factor_idx,
                pairs.len()
            ));
        }

        let mut bucket_scores = Vec::with_capacity(bucket_count);
        let mut bucket_counts = Vec::with_capacity(bucket_count);
        for bucket_idx in 0..bucket_count {
            let start = bucket_idx * pairs.len() / bucket_count;
            let end = ((bucket_idx + 1) * pairs.len() / bucket_count).max(start + 1);
            let slice = &pairs[start..end.min(pairs.len())];
            let count = slice.len();
            let score = if count >= min_samples_per_bucket {
                slice.iter().map(|(_, label)| *label).sum::<f64>() / count as f64
            } else {
                global_label_mean
            };
            bucket_scores.push(score);
            bucket_counts.push(count);
        }

        let cutpoints = (1..bucket_count)
            .map(|bucket_idx| {
                let idx = (bucket_idx * pairs.len() / bucket_count).min(pairs.len() - 1);
                pairs[idx].0
            })
            .collect::<Vec<_>>();
        tables.push(NonlinearQuantileFactorTable {
            factor_idx,
            cutpoints,
            bucket_scores,
            bucket_counts,
        });
    }

    // Pairwise feature interactions: select top features by label variance,
    // compute 2D bucket tables for all pairs among them.
    let pairwise_feature_count = 15usize.min(factor_count);
    let pairwise_min_samples = min_samples_per_bucket; // unified: 1x global min
    let mut pairwise_tables = Vec::new();
    if factor_count >= 2 && samples.len() >= bucket_count * min_samples_per_bucket * 4 {
        // Rank features by label-weighted variance
        let mut factor_variances: Vec<(usize, f64)> = (0..factor_count)
            .map(|fi| {
                let values: Vec<f64> = samples
                    .iter()
                    .filter(|s| {
                        s.features.len() == factor_count
                            && s.label.is_finite()
                            && s.features[fi].is_finite()
                    })
                    .map(|s| s.features[fi] * s.label)
                    .collect();
                let mean = values.iter().sum::<f64>() / values.len().max(1) as f64;
                let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / values.len().max(1) as f64;
                (fi, var)
            })
            .collect();
        factor_variances.sort_by(|a, b| b.1.total_cmp(&a.1));
        let top_features: Vec<usize> = factor_variances
            .iter()
            .take(pairwise_feature_count)
            .map(|(fi, _)| *fi)
            .collect();

        // Build pair indices for parallel processing
        let pair_indices: Vec<(usize, usize)> = (0..top_features.len())
            .flat_map(|i| ((i + 1)..top_features.len()).map(move |j| (i, j)))
            .collect();
        let parallel_tables: Vec<NonlinearQuantilePairwiseTable> = pair_indices
            .par_iter()
            .filter_map(|&(i, j)| {
                let fi = top_features[i];
                let fj = top_features[j];
                let mut grid: Vec<Vec<Vec<f64>>> =
                    vec![vec![Vec::new(); bucket_count]; bucket_count];
                for sample in samples {
                    if sample.features.len() != factor_count || !sample.label.is_finite() {
                        continue;
                    }
                    let vi = sample.features[fi];
                    let vj = sample.features[fj];
                    if !vi.is_finite() || !vj.is_finite() {
                        continue;
                    }
                    let bi = tables[fi]
                        .cutpoints
                        .partition_point(|c| vi >= *c)
                        .min(bucket_count - 1);
                    let bj = tables[fj]
                        .cutpoints
                        .partition_point(|c| vj >= *c)
                        .min(bucket_count - 1);
                    grid[bi][bj].push(sample.label);
                }
                let total_cells = bucket_count * bucket_count;
                let mut scores = vec![global_label_mean; total_cells];
                let mut counts = vec![0usize; total_cells];
                for bi in 0..bucket_count {
                    for bj in 0..bucket_count {
                        let cell_labels = &grid[bi][bj];
                        let idx = bi * bucket_count + bj;
                        counts[idx] = cell_labels.len();
                        if cell_labels.len() >= pairwise_min_samples {
                            scores[idx] =
                                cell_labels.iter().sum::<f64>() / cell_labels.len() as f64;
                        }
                    }
                }
                Some(NonlinearQuantilePairwiseTable {
                    factor_i: fi,
                    factor_j: fj,
                    scores,
                    counts,
                })
            })
            .collect();
        pairwise_tables = parallel_tables;
    }

    Ok(NonlinearQuantileRanker {
        factor_count,
        bucket_count,
        min_samples_per_bucket,
        tables,
        pairwise_tables,
        global_label_mean,
    })
}

// ── Gradient Boosting Model ──

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GbTreeNode {
    pub(crate) feature_idx: usize,
    pub(crate) split_value: f64,
    pub(crate) left_value: f64,
    pub(crate) right_value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GbTree {
    pub(crate) nodes: Vec<GbTreeNode>,
}

impl GbTree {
    pub(crate) fn predict(&self, features: &[f64]) -> f64 {
        let mut idx = 0usize;
        let mut prediction = 0.0;
        for _depth in 0..4 {
            if idx >= self.nodes.len() {
                break;
            }
            let node = &self.nodes[idx];
            if features[node.feature_idx] < node.split_value {
                prediction = node.left_value;
                idx = idx * 2 + 1;
            } else {
                prediction = node.right_value;
                idx = idx * 2 + 2;
            }
        }
        prediction
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GradientBoostingModel {
    pub(crate) trees: Vec<GbTree>,
    pub(crate) init_value: f64,
    pub(crate) learning_rate: f64,
    pub(crate) factor_count: usize,
}

impl GradientBoostingModel {
    pub(crate) fn predict(&self, features: &[f64]) -> f64 {
        if features.len() != self.factor_count || features.iter().any(|v| !v.is_finite()) {
            return self.init_value;
        }
        let mut score = self.init_value;
        for tree in &self.trees {
            score += self.learning_rate * tree.predict(features);
        }
        score
    }
}

// ── 2-Layer MLP with ReLU + Adam ──

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MlpModel {
    pub(crate) w1: Vec<Vec<f64>>, // factor_count × hidden1
    pub(crate) b1: Vec<f64>,
    pub(crate) w2: Vec<Vec<f64>>, // hidden1 × hidden2
    pub(crate) b2: Vec<f64>,
    pub(crate) w3: Vec<f64>, // hidden2 → 1
    pub(crate) b3: f64,
    pub(crate) factor_count: usize,
    pub(crate) hidden1: usize,
    pub(crate) hidden2: usize,
}

impl MlpModel {
    pub(crate) fn predict(&self, features: &[f64]) -> f64 {
        if features.len() != self.factor_count || features.iter().any(|v| !v.is_finite()) {
            return 0.0;
        }
        // Layer 1: ReLU(W1·x + b1)
        let mut h1 = vec![0.0; self.hidden1];
        for i in 0..self.hidden1 {
            let mut s = self.b1[i];
            for j in 0..self.factor_count {
                s += self.w1[i][j] * features[j];
            }
            h1[i] = s.max(0.0);
        }
        // Layer 2: ReLU(W2·h1 + b2)
        let mut h2 = vec![0.0; self.hidden2];
        for i in 0..self.hidden2 {
            let mut s = self.b2[i];
            for j in 0..self.hidden1 {
                s += self.w2[i][j] * h1[j];
            }
            h2[i] = s.max(0.0);
        }
        // Output: W3·h2 + b3
        let mut out = self.b3;
        for i in 0..self.hidden2 {
            out += self.w3[i] * h2[i];
        }
        out
    }
}

pub(crate) fn fit_mlp(
    samples: &[TrainingSample],
    factor_count: usize,
    hidden1: usize,
    hidden2: usize,
    epochs: usize,
    batch_size: usize,
    learning_rate: f64,
    l2_reg: f64,
) -> Result<MlpModel, String> {
    if factor_count == 0 || samples.len() < 100 {
        return Err("MLP: need factor_count>0 and >=100 samples".into());
    }
    let valid: Vec<&TrainingSample> = samples
        .iter()
        .filter(|s| {
            s.features.len() == factor_count
                && s.label.is_finite()
                && s.features.iter().all(|v| v.is_finite())
        })
        .collect();
    let n = valid.len();
    let val_n = (n as f64 * 0.15).ceil() as usize;
    let train_n = n - val_n;

    // Xavier init
    let rng = || {
        let x = (train_n as u64)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (x as f64 / u64::MAX as f64) * 2.0 - 1.0
    };
    let scale1 = (2.0 / factor_count as f64).sqrt();
    let scale2 = (2.0 / hidden1 as f64).sqrt();
    let scale3 = (2.0 / hidden2 as f64).sqrt();

    let mut w1: Vec<Vec<f64>> = (0..hidden1)
        .map(|_| (0..factor_count).map(|_| rng() * scale1).collect())
        .collect();
    let mut b1 = vec![0.0; hidden1];
    let mut w2: Vec<Vec<f64>> = (0..hidden2)
        .map(|_| (0..hidden1).map(|_| rng() * scale2).collect())
        .collect();
    let mut b2 = vec![0.0; hidden2];
    let mut w3: Vec<f64> = (0..hidden2).map(|_| rng() * scale3).collect();
    let mut b3 = 0.0;

    // Adam state
    let beta1 = 0.9;
    let beta2 = 0.999;
    let eps = 1e-8;
    let adam_m = |grad: &mut [f64], m: &mut [f64], v: &mut [f64], t: f64| {
        for i in 0..grad.len() {
            m[i] = beta1 * m[i] + (1.0 - beta1) * grad[i];
            v[i] = beta2 * v[i] + (1.0 - beta2) * grad[i] * grad[i];
            let m_hat = m[i] / (1.0 - beta1.powf(t));
            let v_hat = v[i] / (1.0 - beta2.powf(t));
            grad[i] = learning_rate * m_hat / (v_hat.sqrt() + eps);
        }
    };
    let mut m_w1: Vec<Vec<f64>> = w1.iter().map(|r| vec![0.0; r.len()]).collect();
    let mut v_w1: Vec<Vec<f64>> = w1.iter().map(|r| vec![0.0; r.len()]).collect();
    let _m_b1 = vec![0.0; hidden1];
    let _v_b1 = vec![0.0; hidden1];
    let mut m_w2: Vec<Vec<f64>> = w2.iter().map(|r| vec![0.0; r.len()]).collect();
    let mut v_w2: Vec<Vec<f64>> = w2.iter().map(|r| vec![0.0; r.len()]).collect();
    let _m_b2 = vec![0.0; hidden2];
    let _v_b2 = vec![0.0; hidden2];
    let mut m_w3 = vec![0.0; hidden2];
    let mut v_w3 = vec![0.0; hidden2];
    let _m_b3 = 0.0;
    let _v_b3 = 0.0;
    let mut t = 0.0;

    let mut best_val_loss = f64::MAX;
    let mut best_weights: Option<(
        Vec<Vec<f64>>,
        Vec<f64>,
        Vec<Vec<f64>>,
        Vec<f64>,
        Vec<f64>,
        f64,
    )> = None;
    let mut patience_left = 10i32;

    for _epoch in 0..epochs {
        if patience_left <= 0 {
            break;
        }
        t += 1.0;
        // Shuffle train indices
        let mut indices: Vec<usize> = (0..train_n).collect();
        for i in (1..train_n).rev() {
            let j = (i * 2654435761 + _epoch) % (i + 1);
            indices.swap(i, j);
        }

        for batch_start in (0..train_n).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(train_n);
            // Accumulate gradients
            let mut dw1: Vec<Vec<f64>> = w1.iter().map(|r| vec![0.0; r.len()]).collect();
            let mut db1 = vec![0.0; hidden1];
            let mut dw2: Vec<Vec<f64>> = w2.iter().map(|r| vec![0.0; r.len()]).collect();
            let mut db2 = vec![0.0; hidden2];
            let mut dw3 = vec![0.0; hidden2];
            let mut db3_grad = 0.0;
            let batch_sz = (batch_end - batch_start) as f64;

            for &idx in &indices[batch_start..batch_end] {
                let s = valid[idx];
                // Forward
                let mut h1 = vec![0.0; hidden1];
                for i in 0..hidden1 {
                    let mut sum = b1[i];
                    for j in 0..factor_count {
                        sum += w1[i][j] * s.features[j];
                    }
                    h1[i] = sum.max(0.0);
                }
                let mut h2 = vec![0.0; hidden2];
                for i in 0..hidden2 {
                    let mut sum = b2[i];
                    for j in 0..hidden1 {
                        sum += w2[i][j] * h1[j];
                    }
                    h2[i] = sum.max(0.0);
                }
                let mut pred = b3;
                for i in 0..hidden2 {
                    pred += w3[i] * h2[i];
                }
                let error = pred - s.label;
                // Backward
                let dout = error;
                db3_grad += dout;
                for i in 0..hidden2 {
                    dw3[i] += dout * h2[i];
                }
                let mut dh2 = vec![0.0; hidden2];
                for i in 0..hidden2 {
                    dh2[i] = if h2[i] > 0.0 { dout * w3[i] } else { 0.0 };
                }
                for i in 0..hidden2 {
                    db2[i] += dh2[i];
                    for j in 0..hidden1 {
                        dw2[i][j] += dh2[i] * h1[j];
                    }
                }
                let mut dh1 = vec![0.0; hidden1];
                for i in 0..hidden1 {
                    let mut s = 0.0;
                    for j in 0..hidden2 {
                        s += dh2[j] * w2[j][i];
                    }
                    dh1[i] = if h1[i] > 0.0 { s } else { 0.0 };
                }
                for i in 0..hidden1 {
                    db1[i] += dh1[i];
                    for j in 0..factor_count {
                        dw1[i][j] += dh1[i] * s.features[j];
                    }
                }
            }
            // Apply gradients with Adam + L2 reg
            for i in 0..hidden1 {
                for j in 0..factor_count {
                    dw1[i][j] = dw1[i][j] / batch_sz + l2_reg * w1[i][j];
                }
                adam_m(&mut dw1[i], &mut m_w1[i], &mut v_w1[i], t);
                for j in 0..factor_count {
                    w1[i][j] -= dw1[i][j];
                }
                db1[i] /= batch_sz;
                b1[i] -= learning_rate * db1[i];
            }
            for i in 0..hidden2 {
                for j in 0..hidden1 {
                    dw2[i][j] = dw2[i][j] / batch_sz + l2_reg * w2[i][j];
                }
                adam_m(&mut dw2[i], &mut m_w2[i], &mut v_w2[i], t);
                for j in 0..hidden1 {
                    w2[i][j] -= dw2[i][j];
                }
                db2[i] /= batch_sz;
                b2[i] -= learning_rate * db2[i];
            }
            for i in 0..hidden2 {
                dw3[i] = dw3[i] / batch_sz + l2_reg * w3[i];
            }
            adam_m(&mut dw3, &mut m_w3, &mut v_w3, t);
            for i in 0..hidden2 {
                w3[i] -= dw3[i];
            }
            b3 -= learning_rate * db3_grad / batch_sz;
        }

        // Validation
        if val_n >= 20 {
            let val_loss: f64 = (train_n..n)
                .map(|i| {
                    let s = valid[i];
                    let mut h1 = vec![0.0; hidden1];
                    for k in 0..hidden1 {
                        let mut sum = b1[k];
                        for j in 0..factor_count {
                            sum += w1[k][j] * s.features[j];
                        }
                        h1[k] = sum.max(0.0);
                    }
                    let mut h2 = vec![0.0; hidden2];
                    for k in 0..hidden2 {
                        let mut sum = b2[k];
                        for j in 0..hidden1 {
                            sum += w2[k][j] * h1[j];
                        }
                        h2[k] = sum.max(0.0);
                    }
                    let mut pred = b3;
                    for k in 0..hidden2 {
                        pred += w3[k] * h2[k];
                    }
                    let e = pred - s.label;
                    e * e
                })
                .sum::<f64>()
                / val_n as f64;
            if val_loss < best_val_loss {
                best_val_loss = val_loss;
                best_weights = Some((
                    w1.clone(),
                    b1.clone(),
                    w2.clone(),
                    b2.clone(),
                    w3.clone(),
                    b3,
                ));
                patience_left = 10;
            } else {
                patience_left -= 1;
            }
        }
    }

    let (w1, b1, w2, b2, w3, b3) = best_weights.unwrap_or((w1, b1, w2, b2, w3, b3));
    Ok(MlpModel {
        w1,
        b1,
        w2,
        b2,
        w3,
        b3,
        factor_count,
        hidden1,
        hidden2,
    })
}

pub(crate) fn fit_gradient_boosting(
    samples: &[TrainingSample],
    factor_count: usize,
    num_trees: usize,
    max_depth: usize,
    learning_rate: f64,
) -> Result<GradientBoostingModel, String> {
    if factor_count == 0 || samples.is_empty() {
        return Err("GB: factor_count must be positive with samples".into());
    }
    let valid: Vec<&TrainingSample> = samples
        .iter()
        .filter(|s| {
            s.features.len() == factor_count
                && s.label.is_finite()
                && s.features.iter().all(|v| v.is_finite())
        })
        .collect();
    let min_samples = 100usize;
    if valid.len() < min_samples {
        return Err(format!("GB: need >=100 valid samples, got {}", valid.len()));
    }
    let init_value = valid.iter().map(|s| s.label).sum::<f64>() / valid.len() as f64;
    let mut residuals: Vec<f64> = valid.iter().map(|s| s.label - init_value).collect();
    let mut trees = Vec::with_capacity(num_trees);
    let n = valid.len();
    let n_features_sample = (factor_count as f64).sqrt().ceil() as usize;
    let bagging_fraction = 0.8;
    let bag_size = ((n as f64) * bagging_fraction).ceil() as usize;

    // Early stopping: reserve 15% for validation
    let val_size = (n as f64 * 0.15).ceil() as usize;
    let val_start = n - val_size;
    let mut best_val_loss = f64::MAX;
    let mut trees_since_best = 0usize;
    let patience = 10usize;

    for _tree_idx in 0..num_trees {
        if trees_since_best >= patience {
            break;
        }
        // Bagging: random sample of rows (from training portion only)
        let train_n = n - val_size;
        let mut bag_indices: Vec<usize> = (0..train_n).collect();
        for i in (1..train_n).rev() {
            let j = (i * 2654435761 + _tree_idx) % (i + 1);
            bag_indices.swap(i, j);
        }
        bag_indices.truncate(bag_size.min(train_n));

        let mut node_queue: Vec<(usize, Vec<usize>, usize)> = vec![(0, bag_indices, 0)];
        let mut tree_nodes: Vec<Option<GbTreeNode>> = vec![None; (1 << (max_depth + 1)) - 1];
        while let Some((ni, indices, depth)) = node_queue.pop() {
            if ni >= tree_nodes.len() || depth >= max_depth || indices.len() < min_samples {
                continue;
            }
            // Total mean for gain calculation
            let total_mean =
                indices.iter().map(|&i| residuals[i]).sum::<f64>() / indices.len() as f64;

            // MSE gain via sufficient statistics
            let total_sq = total_mean * total_mean * indices.len() as f64;
            // Rayon-parallel split search across features
            let feature_results: Vec<(f64, usize, f64, f64, f64)> = (0..n_features_sample
                .min(factor_count))
                .into_par_iter()
                .filter_map(|_| {
                    let fi = ((tree_nodes.len() + ni * 7 + _tree_idx * 13) % factor_count) as usize;
                    let mut sorted = indices.clone();
                    sorted.sort_by(|&a, &b| {
                        valid[a].features[fi]
                            .partial_cmp(&valid[b].features[fi])
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    let total_sum: f64 = sorted.iter().map(|&i| residuals[i]).sum();
                    let total_n = sorted.len();
                    let mut best_gain = 0.0f64;
                    let mut best_split = 0.0f64;
                    let mut best_left_mean = total_mean;
                    let mut best_right_mean = total_mean;
                    let mut left_sum = 0.0;
                    let mut left_cnt = 0usize;
                    for (pos, &si) in sorted.iter().enumerate() {
                        if pos < min_samples / 2 || pos >= total_n - min_samples / 2 {
                            left_sum += residuals[si];
                            left_cnt += 1;
                            continue;
                        }
                        left_sum += residuals[si];
                        left_cnt += 1;
                        let right_cnt = total_n - left_cnt;
                        if left_cnt < min_samples / 2 || right_cnt < min_samples / 2 {
                            continue;
                        }
                        let right_sum = total_sum - left_sum;
                        let left_mean = left_sum / left_cnt as f64;
                        let right_mean = right_sum / right_cnt as f64;
                        let gain = left_cnt as f64 * left_mean * left_mean
                            + right_cnt as f64 * right_mean * right_mean
                            - total_sq;
                        if gain > best_gain {
                            best_gain = gain;
                            best_split = valid[si].features[fi];
                            best_left_mean = left_mean;
                            best_right_mean = right_mean;
                        }
                        if pos % 5 != 0 {
                            continue;
                        }
                    }
                    if best_gain > 0.0 {
                        Some((best_gain, fi, best_split, best_left_mean, best_right_mean))
                    } else {
                        None
                    }
                })
                .collect();
            let best = feature_results
                .iter()
                .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let (_best_gain, best_fi, best_split, best_left_mean, best_right_mean) = match best {
                Some(b) => (b.0, b.1, b.2, b.3, b.4),
                None => continue,
            };
            tree_nodes[ni] = Some(GbTreeNode {
                feature_idx: best_fi,
                split_value: best_split,
                left_value: best_left_mean,
                right_value: best_right_mean,
            });
            // Split indices
            let (left_indices, right_indices): (Vec<usize>, Vec<usize>) = indices
                .iter()
                .partition(|&&i| valid[i].features[best_fi] < best_split);
            let left_ni = ni * 2 + 1;
            let right_ni = ni * 2 + 2;
            if !left_indices.is_empty() && left_ni < tree_nodes.len() {
                node_queue.push((left_ni, left_indices, depth + 1));
            }
            if !right_indices.is_empty() && right_ni < tree_nodes.len() {
                node_queue.push((right_ni, right_indices, depth + 1));
            }
        }
        let nodes: Vec<GbTreeNode> = tree_nodes.into_iter().flatten().collect();
        if nodes.is_empty() {
            continue;
        }
        let tree = GbTree { nodes };
        // Update residuals
        for i in 0..n {
            let pred = tree.predict(&valid[i].features);
            residuals[i] -= learning_rate * pred;
        }
        trees.push(tree);

        // Early stopping: compute validation loss
        if val_size >= 20 {
            let val_loss: f64 = (val_start..n)
                .map(|i| {
                    let mut pred = init_value;
                    for t in &trees {
                        pred += learning_rate * t.predict(&valid[i].features);
                    }
                    let err = valid[i].label - pred;
                    err * err
                })
                .sum::<f64>()
                / val_size as f64;
            if val_loss < best_val_loss {
                best_val_loss = val_loss;
                trees_since_best = 0;
            } else {
                trees_since_best += 1;
            }
        }
    }
    if trees.is_empty() {
        return Err("GB: no trees could be fitted".into());
    }
    Ok(GradientBoostingModel {
        trees,
        init_value,
        learning_rate,
        factor_count,
    })
}

pub(crate) fn nonlinear_prediction_rows_from_feature_matrix_rows(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    model: &NonlinearQuantileRanker,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for row in feature_rows {
        if row.features.len() != model.factor_count
            || row.features.iter().any(|value| !value.is_finite())
        {
            continue;
        }
        let mut score = 0.0;
        for table in &model.tables {
            let feature = row.features[table.factor_idx];
            let bucket_idx = table
                .cutpoints
                .partition_point(|cutpoint| feature >= *cutpoint);
            let bucket_score = table
                .bucket_scores
                .get(bucket_idx)
                .copied()
                .unwrap_or(model.global_label_mean);
            score += bucket_score;
        }
        // Pairwise interaction scoring
        for ptable in &model.pairwise_tables {
            let bucket_i = model.tables[ptable.factor_i]
                .cutpoints
                .partition_point(|c| row.features[ptable.factor_i] >= *c)
                .min(model.bucket_count - 1);
            let bucket_j = model.tables[ptable.factor_j]
                .cutpoints
                .partition_point(|c| row.features[ptable.factor_j] >= *c)
                .min(model.bucket_count - 1);
            let pscore = ptable.scores[bucket_i * model.bucket_count + bucket_j];
            if pscore.is_finite() {
                score += pscore;
            }
        }
        if !score.is_finite() {
            continue;
        }
        by_date
            .entry(row.trade_date)
            .or_default()
            .push((row.symbol, score));
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
                prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }

    Ok(predictions)
}

pub(crate) fn nonlinear_prediction_rows_with_regime_split(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    split_model: &RegimeSplitModel,
    benchmark_closes: &[(NaiveDate, f64)],
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for row in feature_rows {
        let regime = MarketRegimeTag::from_benchmark_trailing(benchmark_closes, row.trade_date);
        let Some(score) = split_model.score_row(&row.features, regime) else {
            continue;
        };
        by_date
            .entry(row.trade_date)
            .or_default()
            .push((row.symbol, score));
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
                prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }
    Ok(predictions)
}

pub(crate) fn nonlinear_prediction_rows_with_mlp(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    mlp: &MlpModel,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for row in feature_rows {
        let score = mlp.predict(&row.features);
        if !score.is_finite() {
            continue;
        }
        by_date
            .entry(row.trade_date)
            .or_default()
            .push((row.symbol, score));
    }
    let mut predictions = Vec::new();
    for (trade_date, mut rows) in by_date {
        rows.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for (idx, (symbol, score)) in rows.into_iter().enumerate() {
            predictions.push(build_prediction_row(
                prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }
    Ok(predictions)
}

pub(crate) fn nonlinear_prediction_rows_with_gb(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    gb: &GradientBoostingModel,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for row in feature_rows {
        if row.features.len() != gb.factor_count || row.features.iter().any(|v| !v.is_finite()) {
            continue;
        }
        let score = gb.predict(&row.features);
        if !score.is_finite() {
            continue;
        }
        by_date
            .entry(row.trade_date)
            .or_default()
            .push((row.symbol, score));
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
                prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }
    Ok(predictions)
}
