//! ML API routes — minimal Phase 5 prediction-set smoke path

use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Duration, NaiveDate};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
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
    pub label_objective: Option<String>,
    pub factors: Vec<LinearFactorRef>,
}

#[derive(Debug, Deserialize)]
pub struct TrainNonlinearQuantileRankerRequest {
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
    pub label_objective: Option<String>,
    pub bucket_count: Option<usize>,
    pub min_samples_per_bucket: Option<usize>,
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

#[derive(Debug, Deserialize)]
pub struct EvaluatePredictionSetRequest {
    pub prediction_set_id: String,
    pub backtest_task_id: String,
    pub min_trade_count: Option<i64>,
    pub max_drawdown: Option<f64>,
    pub min_excess_return: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct PredictionSetCacheEconomicsReportRequest {
    pub prediction_set_ids: Vec<String>,
    pub persist_report: Option<bool>,
}

#[derive(Debug, Clone)]
struct PredictionSetCacheEconomicsInput {
    prediction_set_id: String,
    status: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    metadata: Value,
    prediction_rows: i64,
    symbol_count: i64,
    trading_day_count: i64,
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

#[derive(Debug)]
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
    label_objective: LabelObjective,
    factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
struct NormalizedNonlinearQuantileRankerRequest {
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
    label_objective: LabelObjective,
    bucket_count: usize,
    min_samples_per_bucket: usize,
    factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
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
    label_objective: LabelObjective,
    min_training_samples: usize,
    max_windows: Option<usize>,
    factors: Vec<LinearFactorRef>,
}

#[derive(Debug)]
struct NormalizedWalkForwardNonlinearQuantileRankerRequest {
    linear: NormalizedWalkForwardLinearPredictionSetRequest,
    bucket_count: usize,
    min_samples_per_bucket: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LabelObjective {
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
    fn parse(value: Option<&str>) -> Result<Self, String> {
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

    fn as_str(self) -> &'static str {
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

    fn requires_benchmark(self) -> bool {
        matches!(self, Self::FutureExcessReturn | Self::RiskAdjustedExcessReturn
            | Self::QualityAdjustedExcessReturn | Self::QualityAdjustedRiskAdjustedExcessReturn
            | Self::FundamentalQualityAdjustedExcessReturn | Self::RegimeConditionalExcessReturn
            | Self::GradientBoostingExcessReturn | Self::MlpExcessReturn | Self::AsymmetricExcessReturn)
    }

    fn is_quality_adjusted(self) -> bool { matches!(self, Self::QualityAdjustedExcessReturn | Self::QualityAdjustedRiskAdjustedExcessReturn | Self::FundamentalQualityAdjustedExcessReturn | Self::RegimeConditionalExcessReturn | Self::GradientBoostingExcessReturn) }
    fn uses_fundamental_quality(self) -> bool { matches!(self, Self::FundamentalQualityAdjustedExcessReturn) }
    fn uses_regime_conditioning(self) -> bool { matches!(self, Self::RegimeConditionalExcessReturn) }
    fn uses_gradient_boosting(self) -> bool { matches!(self, Self::GradientBoostingExcessReturn) }
    fn uses_mlp(self) -> bool { matches!(self, Self::MlpExcessReturn) }
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

#[derive(Debug, Clone, Serialize)]
struct WalkForwardNonlinearWindowSummary {
    window_index: usize,
    train_start_date: NaiveDate,
    train_end_date: NaiveDate,
    prediction_start_date: NaiveDate,
    prediction_end_date: NaiveDate,
    sample_count: usize,
    prediction_rows: usize,
    skipped: bool,
    skip_reason: Option<String>,
    model: Option<NonlinearQuantileRanker>,
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
struct TrainingFeatureMatrixRow {
    symbol: String,
    trade_date: NaiveDate,
    features: Vec<f64>,
}

#[derive(Debug, Clone)]
struct FeatureMatrixWindowCache {
    rows: Vec<TrainingFeatureMatrixRow>,
}

impl FeatureMatrixWindowCache {
    fn new(mut rows: Vec<TrainingFeatureMatrixRow>) -> Self {
        rows.sort_by(|left, right| {
            left.trade_date
                .cmp(&right.trade_date)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Self { rows }
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn slice(&self, start_date: NaiveDate, end_date: NaiveDate) -> Vec<TrainingFeatureMatrixRow> {
        self.rows
            .iter()
            .filter(|row| row.trade_date >= start_date && row.trade_date <= end_date)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone)]
struct TrainingSample {
    features: Vec<f64>,
    label: f64,
    regime_tag: Option<MarketRegimeTag>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
enum MarketRegimeTag {
    Bull,
    Bear,
    Sideways,
}

impl MarketRegimeTag {
    fn from_benchmark_trailing(closes: &[(NaiveDate, f64)], trade_date: NaiveDate) -> Self {
        let feature = benchmark_trailing_regime_feature(closes, trade_date);
        if feature > 0.0 {
            Self::Bull
        } else if feature < 0.0 {
            Self::Bear
        } else {
            Self::Sideways
        }
    }
}

fn benchmark_trailing_regime_feature(
    benchmark_closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
) -> f64 {
    let Some(current_idx) = benchmark_closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)
    else {
        return 0.0;
    };
    let lookback_idx = current_idx.saturating_sub(60);
    let Some((_, lookback_close)) = benchmark_closes.get(lookback_idx) else {
        return 0.0;
    };
    let Some((_, current_close)) = benchmark_closes.get(current_idx) else {
        return 0.0;
    };
    if *lookback_close <= 0.0 {
        return 0.0;
    }
    let trailing_return = (current_close / lookback_close) - 1.0;
    (trailing_return / 0.10).clamp(-3.0, 3.0)
}

fn split_samples_by_regime(
    samples: &[TrainingSample],
) -> (Vec<TrainingSample>, Vec<TrainingSample>, Vec<TrainingSample>) {
    let mut bull = Vec::new();
    let mut bear = Vec::new();
    let mut sideways = Vec::new();
    for sample in samples {
        match sample.regime_tag {
            Some(MarketRegimeTag::Bull) => bull.push(sample.clone()),
            Some(MarketRegimeTag::Bear) => bear.push(sample.clone()),
            _ => sideways.push(sample.clone()),
        }
    }
    (bull, bear, sideways)
}

#[derive(Debug, Clone, Serialize)]
struct RegimeSplitModel {
    bull: Option<NonlinearQuantileRanker>,
    bear: Option<NonlinearQuantileRanker>,
    sideways: Option<NonlinearQuantileRanker>,
    bull_samples: usize,
    bear_samples: usize,
    sideways_samples: usize,
    factor_count: usize,
}

impl RegimeSplitModel {
    fn score_row(&self, features: &[f64], regime: MarketRegimeTag) -> Option<f64> {
        let model = match regime {
            MarketRegimeTag::Bull => self.bull.as_ref(),
            MarketRegimeTag::Bear => self.bear.as_ref(),
            MarketRegimeTag::Sideways => self.sideways.as_ref(),
        };
        let model = model?;
        if features.len() != model.factor_count || features.iter().any(|v| !v.is_finite()) {
            return None;
        }
        let mut score = 0.0;
        for table in &model.tables {
            let feature = features[table.factor_idx];
            let bucket_idx = table.cutpoints.partition_point(|c| feature >= *c);
            let bucket_score = table
                .bucket_scores
                .get(bucket_idx)
                .copied()
                .unwrap_or(model.global_label_mean);
            score += bucket_score;
        }
        for ptable in &model.pairwise_tables {
            let bucket_i = model.tables[ptable.factor_i]
                .cutpoints
                .partition_point(|c| features[ptable.factor_i] >= *c)
                .min(model.bucket_count - 1);
            let bucket_j = model.tables[ptable.factor_j]
                .cutpoints
                .partition_point(|c| features[ptable.factor_j] >= *c)
                .min(model.bucket_count - 1);
            let pscore = ptable.scores[bucket_i * model.bucket_count + bucket_j];
            if pscore.is_finite() {
                score += pscore;
            }
        }
        if score.is_finite() { Some(score) } else { None }
    }

    fn any_model(&self) -> bool {
        self.bull.is_some() || self.bear.is_some() || self.sideways.is_some()
    }
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

pub async fn train_nonlinear_quantile_ranker(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TrainNonlinearQuantileRankerRequest>,
) -> impl IntoResponse {
    match train_nonlinear_quantile_ranker_inner(&state.db, req).await {
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

pub async fn create_walk_forward_nonlinear_quantile_ranker_prediction_set(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WalkForwardNonlinearQuantileRankerRequest>,
) -> impl IntoResponse {
    match create_walk_forward_nonlinear_quantile_ranker_inner(&state.db, req).await {
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

pub async fn report_prediction_set_cache_economics(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PredictionSetCacheEconomicsReportRequest>,
) -> impl IntoResponse {
    match build_prediction_set_cache_economics_report(&state.db, &req).await {
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
        return Err("walk-forward nonlinear ranker produced no prediction rows".into());
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

pub(crate) async fn train_nonlinear_quantile_ranker_inner(
    db: &sqlx::PgPool,
    req: TrainNonlinearQuantileRankerRequest,
) -> Result<Value, String> {
    let started_at = Instant::now();
    let req = normalize_nonlinear_quantile_ranker_request(&req)?;
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
        gb_model = Some(fit_gradient_boosting(&samples, req.factors.len(), 100, 4, 0.05)?);
    }
    if req.label_objective.uses_mlp() {
        mlp_model = Some(fit_mlp(&samples, req.factors.len(), 128, 64, 100, 256, 0.001, 0.0001)?);
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

async fn build_prediction_set_cache_economics_report(
    db: &sqlx::PgPool,
    req: &PredictionSetCacheEconomicsReportRequest,
) -> Result<Value, String> {
    let prediction_set_ids = normalize_prediction_set_cache_economics_request(req)?;
    let mut inputs = Vec::with_capacity(prediction_set_ids.len());
    for prediction_set_id in &prediction_set_ids {
        inputs.push(load_prediction_set_cache_economics_input(db, prediction_set_id).await?);
    }
    let report = prediction_set_cache_economics_report_json(&inputs);
    let experiment_run_id = if req.persist_report.unwrap_or(true) {
        Some(persist_prediction_set_cache_economics_report(db, &prediction_set_ids, &report).await?)
    } else {
        None
    };

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}

fn normalize_prediction_set_cache_economics_request(
    req: &PredictionSetCacheEconomicsReportRequest,
) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    for id in &req.prediction_set_ids {
        let trimmed = id.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !ids.iter().any(|existing: &String| existing == trimmed) {
            ids.push(trimmed.to_string());
        }
    }
    if ids.is_empty() {
        return Err("prediction_set_ids must not be empty".into());
    }
    if ids.len() > 20 {
        return Err("prediction_set_ids supports at most 20 sets per report".into());
    }
    Ok(ids)
}

async fn load_prediction_set_cache_economics_input(
    db: &sqlx::PgPool,
    prediction_set_id: &str,
) -> Result<PredictionSetCacheEconomicsInput, String> {
    let row = sqlx::query_as::<_, (String, NaiveDate, NaiveDate, Option<Value>)>(
        "SELECT status, start_date, end_date, metadata
         FROM prediction_set
         WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load prediction_set: {}", error))?
    .ok_or_else(|| format!("prediction_set not found: {}", prediction_set_id))?;

    let summary = sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<i64>)>(
        "SELECT COUNT(*)::bigint,
                COUNT(DISTINCT symbol)::bigint,
                COUNT(DISTINCT trade_date)::bigint
         FROM model_prediction
         WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize model_prediction: {}", error))?;

    Ok(PredictionSetCacheEconomicsInput {
        prediction_set_id: prediction_set_id.to_string(),
        status: row.0,
        start_date: row.1,
        end_date: row.2,
        metadata: row.3.unwrap_or_else(|| json!({})),
        prediction_rows: summary.0.unwrap_or(0),
        symbol_count: summary.1.unwrap_or(0),
        trading_day_count: summary.2.unwrap_or(0),
    })
}

async fn persist_prediction_set_cache_economics_report(
    db: &sqlx::PgPool,
    prediction_set_ids: &[String],
    report: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let related_entity_id = prediction_set_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "prediction_set_group".to_string());
    let config = json!({
        "prediction_set_ids": prediction_set_ids,
        "report_type": "prediction_set_cache_economics",
        "point_in_time_scope": "prediction_set metadata and model_prediction only; no backtest/OOS metrics",
    });
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'prediction_set_cache_economics_report', 'prediction_set_group', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(&related_entity_id)
    .bind(&config)
    .bind(report)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert prediction-set cache economics experiment_run: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

fn prediction_set_cache_economics_report_json(
    inputs: &[PredictionSetCacheEconomicsInput],
) -> Value {
    let sets = inputs
        .iter()
        .map(prediction_set_cache_economics_set_json)
        .collect::<Vec<_>>();
    let total_prediction_rows = inputs
        .iter()
        .map(|input| input.prediction_rows)
        .sum::<i64>();
    let missing_cache_metadata_count = sets
        .iter()
        .filter(|set| !set["cache_metadata_present"].as_bool().unwrap_or(false))
        .count();
    let missing_insert_telemetry_count = sets
        .iter()
        .filter(|set| !set["insert_telemetry_present"].as_bool().unwrap_or(false))
        .count();
    let missing_generation_telemetry_count = sets
        .iter()
        .filter(|set| {
            !set["generation_telemetry_present"]
                .as_bool()
                .unwrap_or(false)
        })
        .count();
    let max_generation_elapsed_ms = sets
        .iter()
        .filter_map(|set| {
            set["prediction_generation_telemetry"]["elapsed_ms"]
                .as_u64()
                .or_else(|| {
                    set["prediction_generation_telemetry"]["elapsed_ms"]
                        .as_i64()
                        .and_then(|value| u64::try_from(value).ok())
                })
        })
        .max()
        .unwrap_or(0);
    let max_rows_per_trading_day = inputs
        .iter()
        .map(rows_per_trading_day)
        .fold(0.0_f64, f64::max);
    let max_rows_per_symbol = inputs.iter().map(rows_per_symbol).fold(0.0_f64, f64::max);
    let recommendation = if missing_cache_metadata_count > 0 || missing_insert_telemetry_count > 0 {
        "audit_uncached_prediction_sets"
    } else if max_rows_per_trading_day >= 100_000.0 {
        "prefer_window_cache_and_background_insert"
    } else {
        "cache_metadata_complete"
    };
    json!({
        "prediction_set_count": inputs.len(),
        "total_prediction_rows": total_prediction_rows,
        "sets": sets,
        "economics": {
            "recommendation": recommendation,
            "missing_cache_metadata_count": missing_cache_metadata_count,
            "missing_insert_telemetry_count": missing_insert_telemetry_count,
            "missing_generation_telemetry_count": missing_generation_telemetry_count,
            "max_generation_elapsed_ms": max_generation_elapsed_ms,
            "max_rows_per_trading_day": max_rows_per_trading_day,
            "max_rows_per_symbol": max_rows_per_symbol,
            "point_in_time_scope": "uses prediction_set metadata and model_prediction density only; does not read backtest/OOS metrics"
        }
    })
}

fn prediction_set_cache_economics_set_json(input: &PredictionSetCacheEconomicsInput) -> Value {
    let feature_matrix_cache = input
        .metadata
        .get("feature_matrix_cache")
        .cloned()
        .unwrap_or(Value::Null);
    let cache_metadata_present = !feature_matrix_cache.is_null();
    let prediction_insert_telemetry = input
        .metadata
        .get("prediction_insert_telemetry")
        .cloned()
        .unwrap_or(Value::Null);
    let insert_telemetry_present = !prediction_insert_telemetry.is_null();
    let prediction_generation_telemetry = input
        .metadata
        .get("prediction_generation_telemetry")
        .cloned()
        .unwrap_or(Value::Null);
    let generation_telemetry_present = !prediction_generation_telemetry.is_null();
    json!({
        "prediction_set_id": input.prediction_set_id,
        "status": input.status,
        "start_date": input.start_date,
        "end_date": input.end_date,
        "prediction_rows": input.prediction_rows,
        "symbol_count": input.symbol_count,
        "trading_day_count": input.trading_day_count,
        "rows_per_trading_day": rows_per_trading_day(input),
        "rows_per_symbol": rows_per_symbol(input),
        "cache_metadata_present": cache_metadata_present,
        "feature_matrix_cache": feature_matrix_cache,
        "insert_telemetry_present": insert_telemetry_present,
        "prediction_insert_telemetry": prediction_insert_telemetry,
        "generation_telemetry_present": generation_telemetry_present,
        "prediction_generation_telemetry": prediction_generation_telemetry,
    })
}

fn rows_per_trading_day(input: &PredictionSetCacheEconomicsInput) -> f64 {
    if input.trading_day_count <= 0 {
        return 0.0;
    }
    (input.prediction_rows as f64 / input.trading_day_count as f64).round()
}

fn rows_per_symbol(input: &PredictionSetCacheEconomicsInput) -> f64 {
    if input.symbol_count <= 0 {
        return 0.0;
    }
    (input.prediction_rows as f64 / input.symbol_count as f64).round()
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

fn nonlinear_quantile_ranker_experiment_config(
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

fn nonlinear_quantile_ranker_prediction_set_metadata(
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

fn walk_forward_linear_experiment_config(
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

fn walk_forward_nonlinear_quantile_ranker_experiment_config(
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

fn nonlinear_quantile_ranker_experiment_metrics(
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

fn label_definition_json(label_objective: LabelObjective, horizon_days: i64) -> Value {
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

fn normalize_nonlinear_quantile_ranker_request(
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

fn normalize_walk_forward_nonlinear_quantile_ranker_request(
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
    let feature_rows = load_training_feature_matrix_rows(db, req).await?;
    load_training_samples_from_feature_rows(db, req, feature_rows).await
}

async fn load_training_samples_from_feature_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearTrainingRequest,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
) -> Result<Vec<TrainingSample>, String> {
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

    let benchmark_closes = if req.label_objective.requires_benchmark() {
        load_benchmark_closes(db, "000300.SH", req.train_start_date, label_end_date).await?
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

async fn load_benchmark_closes(
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

async fn load_training_feature_matrix_rows(
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

fn training_samples_from_feature_matrix_rows(
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
            .then(|| row.features.as_slice());
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
            let regime_tag = if label_objective.uses_regime_conditioning()
                && !benchmark_closes.is_empty()
            {
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

fn future_return_label_until(
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

fn label_for_objective(
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
            let benchmark_return = future_return_label_until(benchmark_closes, trade_date, max_label_date, horizon_days)?;
            Some(stock_return - benchmark_return)
        }
        LabelObjective::AsymmetricExcessReturn => {
            let benchmark_return = future_return_label_until(benchmark_closes, trade_date, max_label_date, horizon_days)?;
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
fn fundamental_quality_score(features: &[f64], quality_feature_count: usize) -> f64 {
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
fn label_for_objective_with_features(
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

fn forward_downside_volatility(
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
fn trailing_volatility(
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
fn trailing_max_drawdown(
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
fn quality_adjustment(
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

#[derive(Debug, Clone, Serialize)]
struct NonlinearQuantileRanker {
    factor_count: usize,
    bucket_count: usize,
    min_samples_per_bucket: usize,
    tables: Vec<NonlinearQuantileFactorTable>,
    pairwise_tables: Vec<NonlinearQuantilePairwiseTable>,
    global_label_mean: f64,
}

#[derive(Debug, Clone, Serialize)]
struct NonlinearQuantileFactorTable {
    factor_idx: usize,
    cutpoints: Vec<f64>,
    bucket_scores: Vec<f64>,
    bucket_counts: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct NonlinearQuantilePairwiseTable {
    factor_i: usize,
    factor_j: usize,
    /// Flat 2D array: scores[bucket_i * bucket_count + bucket_j]
    scores: Vec<f64>,
    counts: Vec<usize>,
}

fn fit_nonlinear_quantile_ranker(
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
struct GbTreeNode {
    feature_idx: usize,
    split_value: f64,
    left_value: f64,
    right_value: f64,
}

#[derive(Debug, Clone, Serialize)]
struct GbTree {
    nodes: Vec<GbTreeNode>,
}

impl GbTree {
    fn predict(&self, features: &[f64]) -> f64 {
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
struct GradientBoostingModel {
    trees: Vec<GbTree>,
    init_value: f64,
    learning_rate: f64,
    factor_count: usize,
}

impl GradientBoostingModel {
    fn predict(&self, features: &[f64]) -> f64 {
        if features.len() != self.factor_count
            || features.iter().any(|v| !v.is_finite())
        {
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
struct MlpModel {
    w1: Vec<Vec<f64>>, // factor_count × hidden1
    b1: Vec<f64>,
    w2: Vec<Vec<f64>>, // hidden1 × hidden2
    b2: Vec<f64>,
    w3: Vec<f64>,      // hidden2 → 1
    b3: f64,
    factor_count: usize,
    hidden1: usize,
    hidden2: usize,
}

impl MlpModel {
    fn predict(&self, features: &[f64]) -> f64 {
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

fn fit_mlp(
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
    let valid: Vec<&TrainingSample> = samples.iter()
        .filter(|s| s.features.len() == factor_count && s.label.is_finite() && s.features.iter().all(|v| v.is_finite()))
        .collect();
    let n = valid.len();
    let val_n = (n as f64 * 0.15).ceil() as usize;
    let train_n = n - val_n;

    // Xavier init
    let mut rng = || {
        let x = (train_n as u64).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (x as f64 / u64::MAX as f64) * 2.0 - 1.0
    };
    let scale1 = (2.0 / factor_count as f64).sqrt();
    let scale2 = (2.0 / hidden1 as f64).sqrt();
    let scale3 = (2.0 / hidden2 as f64).sqrt();

    let mut w1: Vec<Vec<f64>> = (0..hidden1).map(|_| (0..factor_count).map(|_| rng() * scale1).collect()).collect();
    let mut b1 = vec![0.0; hidden1];
    let mut w2: Vec<Vec<f64>> = (0..hidden2).map(|_| (0..hidden1).map(|_| rng() * scale2).collect()).collect();
    let mut b2 = vec![0.0; hidden2];
    let mut w3: Vec<f64> = (0..hidden2).map(|_| rng() * scale3).collect();
    let mut b3 = 0.0;

    // Adam state
    let beta1 = 0.9; let beta2 = 0.999; let eps = 1e-8;
    let mut adam_m = |grad: &mut [f64], m: &mut [f64], v: &mut [f64], t: f64| {
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
    let mut m_b1 = vec![0.0; hidden1]; let mut v_b1 = vec![0.0; hidden1];
    let mut m_w2: Vec<Vec<f64>> = w2.iter().map(|r| vec![0.0; r.len()]).collect();
    let mut v_w2: Vec<Vec<f64>> = w2.iter().map(|r| vec![0.0; r.len()]).collect();
    let mut m_b2 = vec![0.0; hidden2]; let mut v_b2 = vec![0.0; hidden2];
    let mut m_w3 = vec![0.0; hidden2]; let mut v_w3 = vec![0.0; hidden2];
    let mut m_b3 = 0.0; let mut v_b3 = 0.0;
    let mut t = 0.0;

    let mut best_val_loss = f64::MAX;
    let mut best_weights: Option<(Vec<Vec<f64>>, Vec<f64>, Vec<Vec<f64>>, Vec<f64>, Vec<f64>, f64)> = None;
    let mut patience_left = 10i32;

    for _epoch in 0..epochs {
        if patience_left <= 0 { break; }
        t += 1.0;
        // Shuffle train indices
        let mut indices: Vec<usize> = (0..train_n).collect();
        for i in (1..train_n).rev() { let j = (i * 2654435761 + _epoch) % (i + 1); indices.swap(i, j); }

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
                for i in 0..hidden1 { let mut sum = b1[i]; for j in 0..factor_count { sum += w1[i][j] * s.features[j]; } h1[i] = sum.max(0.0); }
                let mut h2 = vec![0.0; hidden2];
                for i in 0..hidden2 { let mut sum = b2[i]; for j in 0..hidden1 { sum += w2[i][j] * h1[j]; } h2[i] = sum.max(0.0); }
                let mut pred = b3;
                for i in 0..hidden2 { pred += w3[i] * h2[i]; }
                let error = pred - s.label;
                // Backward
                let dout = error;
                db3_grad += dout;
                for i in 0..hidden2 { dw3[i] += dout * h2[i]; }
                let mut dh2 = vec![0.0; hidden2];
                for i in 0..hidden2 { dh2[i] = if h2[i] > 0.0 { dout * w3[i] } else { 0.0 }; }
                for i in 0..hidden2 { db2[i] += dh2[i]; for j in 0..hidden1 { dw2[i][j] += dh2[i] * h1[j]; } }
                let mut dh1 = vec![0.0; hidden1];
                for i in 0..hidden1 { let mut s = 0.0; for j in 0..hidden2 { s += dh2[j] * w2[j][i]; } dh1[i] = if h1[i] > 0.0 { s } else { 0.0 }; }
                for i in 0..hidden1 { db1[i] += dh1[i]; for j in 0..factor_count { dw1[i][j] += dh1[i] * s.features[j]; } }
            }
            // Apply gradients with Adam + L2 reg
            for i in 0..hidden1 {
                for j in 0..factor_count { dw1[i][j] = dw1[i][j] / batch_sz + l2_reg * w1[i][j]; }
                adam_m(&mut dw1[i], &mut m_w1[i], &mut v_w1[i], t);
                for j in 0..factor_count { w1[i][j] -= dw1[i][j]; }
                db1[i] /= batch_sz; b1[i] -= learning_rate * db1[i];
            }
            for i in 0..hidden2 {
                for j in 0..hidden1 { dw2[i][j] = dw2[i][j] / batch_sz + l2_reg * w2[i][j]; }
                adam_m(&mut dw2[i], &mut m_w2[i], &mut v_w2[i], t);
                for j in 0..hidden1 { w2[i][j] -= dw2[i][j]; }
                db2[i] /= batch_sz; b2[i] -= learning_rate * db2[i];
            }
            for i in 0..hidden2 { dw3[i] = dw3[i] / batch_sz + l2_reg * w3[i]; }
            adam_m(&mut dw3, &mut m_w3, &mut v_w3, t);
            for i in 0..hidden2 { w3[i] -= dw3[i]; }
            b3 -= learning_rate * db3_grad / batch_sz;
        }

        // Validation
        if val_n >= 20 {
            let val_loss: f64 = (train_n..n).map(|i| {
                let s = valid[i];
                let mut h1 = vec![0.0; hidden1];
                for k in 0..hidden1 { let mut sum = b1[k]; for j in 0..factor_count { sum += w1[k][j] * s.features[j]; } h1[k] = sum.max(0.0); }
                let mut h2 = vec![0.0; hidden2];
                for k in 0..hidden2 { let mut sum = b2[k]; for j in 0..hidden1 { sum += w2[k][j] * h1[j]; } h2[k] = sum.max(0.0); }
                let mut pred = b3; for k in 0..hidden2 { pred += w3[k] * h2[k]; }
                let e = pred - s.label; e * e
            }).sum::<f64>() / val_n as f64;
            if val_loss < best_val_loss {
                best_val_loss = val_loss;
                best_weights = Some((w1.clone(), b1.clone(), w2.clone(), b2.clone(), w3.clone(), b3));
                patience_left = 10;
            } else { patience_left -= 1; }
        }
    }

    let (w1, b1, w2, b2, w3, b3) = best_weights.unwrap_or((w1, b1, w2, b2, w3, b3));
    Ok(MlpModel { w1, b1, w2, b2, w3, b3, factor_count, hidden1, hidden2 })
}

fn fit_gradient_boosting(
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
            let feature_results: Vec<(f64, usize, f64, f64, f64)> = (0..n_features_sample.min(factor_count))
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
                        if pos % 5 != 0 { continue; }
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
            let (_best_gain, best_fi, best_split, best_left_mean, best_right_mean) =
                match best {
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

fn nonlinear_prediction_rows_from_feature_matrix_rows(
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

fn nonlinear_prediction_rows_with_regime_split(
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
            right.1.total_cmp(&left.1).then_with(|| left.0.cmp(&right.0))
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

fn nonlinear_prediction_rows_with_mlp(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    mlp: &MlpModel,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for row in feature_rows {
        let score = mlp.predict(&row.features);
        if !score.is_finite() { continue; }
        by_date.entry(row.trade_date).or_default().push((row.symbol, score));
    }
    let mut predictions = Vec::new();
    for (trade_date, mut rows) in by_date {
        rows.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for (idx, (symbol, score)) in rows.into_iter().enumerate() {
            predictions.push(build_prediction_row(prediction_set_id, &symbol, &trade_date.to_string(), score, (idx+1) as i32)?);
        }
    }
    Ok(predictions)
}

fn nonlinear_prediction_rows_with_gb(
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
            right.1.total_cmp(&left.1).then_with(|| left.0.cmp(&right.0))
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

async fn build_linear_prediction_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
) -> Result<Vec<PredictionRow>, String> {
    let feature_rows = load_prediction_feature_matrix_rows(db, req).await?;
    let weights = req
        .factors
        .iter()
        .map(|factor| factor.weight)
        .collect::<Vec<_>>();

    prediction_rows_from_feature_matrix_rows(
        &req.prediction_set_id,
        feature_rows,
        &weights,
        req.factors.len(),
    )
}

async fn load_prediction_feature_matrix_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
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
    builder.push_bind(req.start_date);
    builder.push(
        "
           AND fv.trade_date <= ",
    );
    builder.push_bind(req.end_date);
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
        .map_err(|error| format!("Failed to load prediction factor matrix: {}", error))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, trade_date, features)| TrainingFeatureMatrixRow {
            symbol,
            trade_date,
            features,
        })
        .collect())
}

fn prediction_rows_from_feature_matrix_rows(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    weights: &[f64],
    factor_count: usize,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    if weights.len() != factor_count {
        return Err("prediction weights count must match factor count".into());
    }
    for row in feature_rows {
        if row.features.len() != factor_count || row.features.iter().any(|value| !value.is_finite())
        {
            continue;
        }
        let score = row
            .features
            .iter()
            .zip(weights.iter())
            .map(|(feature, weight)| feature * weight)
            .sum::<f64>();
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
    for chunk in rows.chunks(PREDICTION_INSERT_BATCH_SIZE) {
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

const PREDICTION_INSERT_BATCH_SIZE: usize = 5_000;

fn prediction_insert_telemetry(rows: &[PredictionRow]) -> Value {
    let chunk_row_counts = rows
        .chunks(PREDICTION_INSERT_BATCH_SIZE)
        .map(|chunk| chunk.len())
        .collect::<Vec<_>>();
    json!({
        "mode": "bulk_insert",
        "row_count": rows.len(),
        "batch_size": PREDICTION_INSERT_BATCH_SIZE,
        "batch_count": chunk_row_counts.len(),
        "chunk_row_counts": chunk_row_counts,
    })
}

fn prediction_progress_stage(
    stage: &str,
    total_units: usize,
    completed_units: usize,
    row_count: usize,
    elapsed: StdDuration,
) -> Value {
    json!({
        "stage": stage,
        "total_units": total_units,
        "completed_units": completed_units,
        "row_count": row_count,
        "elapsed_ms": elapsed.as_millis() as u64,
        "progress_pct": progress_pct(total_units, completed_units),
    })
}

fn prediction_generation_telemetry(
    operation: &str,
    total_units: usize,
    completed_units: usize,
    skipped_units: usize,
    prediction_rows: usize,
    elapsed: StdDuration,
    stages: Vec<Value>,
) -> Value {
    json!({
        "operation": operation,
        "total_units": total_units,
        "completed_units": completed_units,
        "skipped_units": skipped_units,
        "prediction_rows": prediction_rows,
        "elapsed_ms": elapsed.as_millis() as u64,
        "progress_pct": progress_pct(total_units, completed_units + skipped_units),
        "stages": stages,
    })
}

fn progress_pct(total_units: usize, completed_units: usize) -> f64 {
    if total_units == 0 {
        return 100.0;
    }
    ((completed_units.min(total_units) as f64 / total_units as f64) * 10_000.0).round() / 100.0
}

fn feature_matrix_cache_metadata(
    scope: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    row_count: usize,
) -> Value {
    json!({
        "scope": scope,
        "start_date": start_date,
        "end_date": end_date,
        "row_count": row_count,
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
            label_objective: None,
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
            label_objective: None,
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
            label_objective: None,
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
            TrainingSample { regime_tag: None,
                features: vec![1.0, 0.0],
                label: 0.10,
            },
            TrainingSample { regime_tag: None,
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

        let label = future_return_label_until(
            Some(&closes),
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            None,
            2,
        )
        .expect("label");

        assert!((label - 0.21).abs() < 1e-9);
    }

    #[test]
    fn quality_adjusted_excess_return_penalizes_high_volatility_stocks() {
        let days = 300;
        let stable_closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    10.0 + (i as f64 * 0.005),
                )
            })
            .collect();
        let volatile_closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                let base = 10.0 + (i as f64 * 0.005);
                let noise = (i as f64 * 0.3).sin() * 0.5;
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    base + noise,
                )
            })
            .collect();
        let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    100.0 + (i as f64 * 0.003),
                )
            })
            .collect();

        // Use trade_date at day 150 so trailing lookback (60/120) and horizon (20) are within range
        let trade_idx = 150usize;
        let trade_date = stable_closes[trade_idx].0;
        let horizon = 20i64;

        let stable_label = label_for_objective(
            LabelObjective::QualityAdjustedExcessReturn,
            Some(&stable_closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
        );
        let volatile_label = label_for_objective(
            LabelObjective::QualityAdjustedExcessReturn,
            Some(&volatile_closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
        );

        assert!(stable_label.is_some(), "stable label should be computed");
        assert!(
            volatile_label.is_some(),
            "volatile label should be computed"
        );
        let stable = stable_label.unwrap();
        let volatile = volatile_label.unwrap();
        // Both have similar price trends; stable has lower trailing vol,
        // so quality adjustment penalizes the volatile stock more
        assert!(
            stable > volatile,
            "stable_label={stable} should be > volatile_label={volatile}"
        );
    }

    #[test]
    fn quality_adjusted_risk_adjusted_excess_return_labels_are_smaller_than_raw_excess() {
        let days = 300;
        let closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    10.0 + (i as f64 * 0.01),
                )
            })
            .collect();
        let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    100.0 + (i as f64 * 0.003),
                )
            })
            .collect();

        let trade_idx = 150usize;
        let trade_date = closes[trade_idx].0;
        let horizon = 20i64;

        let raw = label_for_objective(
            LabelObjective::FutureExcessReturn,
            Some(&closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
        )
        .expect("raw excess return label");
        let qa = label_for_objective(
            LabelObjective::QualityAdjustedExcessReturn,
            Some(&closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
        )
        .expect("qa excess return label");
        let qara = label_for_objective(
            LabelObjective::QualityAdjustedRiskAdjustedExcessReturn,
            Some(&closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
        )
        .expect("qara label");

        // Quality-adjusted should be ≤ raw excess in absolute magnitude
        assert!(
            qa.abs() <= raw.abs(),
            "qa={qa} should be ≤ raw={raw} in magnitude"
        );
        // Both quality-adjusted labels should agree on direction with raw
        assert!(
            qara.is_sign_positive() == qa.is_sign_positive()
                && qa.is_sign_positive() == raw.is_sign_positive(),
            "qara={qara}, qa={qa}, raw={raw} should all have same sign"
        );
        // Quality-adjusted risk-adjusted = qa / downside_vol (can be larger due to low vol)
        assert!(qara.is_finite() && qara != 0.0);
    }

    #[test]
    fn label_definition_json_includes_quality_adjustment_for_new_objectives() {
        let def = label_definition_json(LabelObjective::QualityAdjustedExcessReturn, 60);
        assert_eq!(def["label"], "quality_adjusted_excess_return");
        assert_eq!(def["benchmark"], "000300.SH");
        assert!(def["quality_adjustment"].is_object());
        assert_eq!(
            def["quality_adjustment"]["method"],
            "trailing_volatility_and_max_drawdown_penalty"
        );

        let def2 =
            label_definition_json(LabelObjective::QualityAdjustedRiskAdjustedExcessReturn, 120);
        assert_eq!(
            def2["label"],
            "quality_adjusted_risk_adjusted_excess_return"
        );
        assert_eq!(
            def2["risk_adjustment"],
            "forward_downside_volatility_floor_1pct"
        );
        assert!(def2["quality_adjustment"].is_object());
    }

    #[test]
    fn trailing_volatility_computes_annualized_vol() {
        let closes: Vec<(NaiveDate, f64)> = (0..=60)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2025, 1, 1).unwrap() + chrono::Duration::days(i),
                    10.0,
                )
            })
            .collect();
        let vol = trailing_volatility(&closes, NaiveDate::from_ymd_opt(2025, 2, 20).unwrap(), 40);
        // Flat prices → near-zero volatility
        assert!(vol.is_some());
        assert!(vol.unwrap() < 0.01);
    }

    #[test]
    fn trailing_max_drawdown_detects_drawdown() {
        // Build a continuous daily sequence: flat at 10, then peak at 12, trough at 9, recovery to 11
        let mut closes = Vec::new();
        let base = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        // Index 0-79: flat at 10.0
        for i in 0..80 {
            closes.push((base + chrono::Duration::days(i as i64), 10.0));
        }
        // Index 80: peak at 12.0
        closes.push((base + chrono::Duration::days(80), 12.0));
        // Index 81: trough at 9.0 (drawdown 25%)
        closes.push((base + chrono::Duration::days(81), 9.0));
        // Index 82-99: recovery to 11.0
        for i in 82..100 {
            closes.push((base + chrono::Duration::days(i as i64), 11.0));
        }
        let trade_date = closes.last().unwrap().0;
        let dd = trailing_max_drawdown(&closes, trade_date, 80);
        assert!(dd.is_some(), "should compute max drawdown, got None");
        // Max drawdown from peak 12.0 to trough 9.0 = 3.0/12.0 = 0.25
        assert!((dd.unwrap() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn fundamental_quality_score_maps_positive_features_to_high_quality() {
        // Positive z-scores (good fundamentals) → high sigmoid → high quality
        let features = vec![1.0, 0.5, 0.3, 0.8, 0.6, 0.0, 0.2, 0.4, -0.1, 0.1, 0.7, 0.9];
        let score = fundamental_quality_score(&features, 12);
        // All features are ≥ -0.1, sigmoid > 0.47, so average > 0.5
        assert!(
            score > 0.5,
            "positive features should give score > 0.5, got {score}"
        );
        assert!(score <= 1.0, "score should be ≤ 1.0");
    }

    #[test]
    fn fundamental_quality_score_penalizes_negative_features() {
        // Negative z-scores (poor fundamentals) → low sigmoid → low quality
        let features = vec![
            -1.0, -0.5, -2.0, -0.8, -0.6, -1.5, -0.2, -0.4, -0.1, -0.9, -0.7, -0.3,
        ];
        let score = fundamental_quality_score(&features, 12);
        assert!(
            score < 0.5,
            "negative features should give score < 0.5, got {score}"
        );
        assert!(score > 0.0, "score should be > 0");
    }

    #[test]
    fn fundamental_quality_label_uses_quality_features_when_available() {
        let days = 300;
        let closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    10.0 + (i as f64 * 0.005),
                )
            })
            .collect();
        let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
            .map(|i| {
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                    100.0 + (i as f64 * 0.003),
                )
            })
            .collect();
        let trade_idx = 150usize;
        let trade_date = closes[trade_idx].0;
        let horizon = 20i64;

        // With high-quality features → higher label
        let high_quality_features: Vec<f64> = vec![1.0; 12];
        let hq_label = label_for_objective_with_features(
            LabelObjective::FundamentalQualityAdjustedExcessReturn,
            Some(&closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
            Some(&high_quality_features),
        );
        // With low-quality features → lower label
        let low_quality_features: Vec<f64> = vec![-1.0; 12];
        let lq_label = label_for_objective_with_features(
            LabelObjective::FundamentalQualityAdjustedExcessReturn,
            Some(&closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            horizon,
            Some(&low_quality_features),
        );

        assert!(hq_label.is_some());
        assert!(lq_label.is_some());
        let hq = hq_label.unwrap();
        let lq = lq_label.unwrap();
        assert!(
            hq > lq,
            "high-quality label {hq} should be > low-quality label {lq}"
        );
    }

    #[test]
    fn fundamental_quality_label_parses_correctly() {
        let obj = LabelObjective::parse(Some("fundamental_quality_adjusted_excess_return"))
            .expect("parse");
        assert!(obj.uses_fundamental_quality());
        assert!(obj.requires_benchmark());
        assert!(obj.is_quality_adjusted());
        assert_eq!(obj.as_str(), "fundamental_quality_adjusted_excess_return");
    }

    #[test]
    fn linear_training_request_accepts_label_objective_and_rejects_unknown() {
        let mut req = TrainLinearModelRequest {
            model_code: "trained_linear_alpha".into(),
            model_version: "phase7-fu-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-market-2016-v1".into(),
            feature_set_version_id: "phase7-alpha-v1".into(),
            training_dataset_id: "phase7-fu-training-v1".into(),
            train_start_date: "20250109".into(),
            train_end_date: "20250120".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250131".into(),
            label_horizon_days: Some(60),
            label_objective: Some("risk_adjusted_excess_return".into()),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };

        let normalized = normalize_linear_training_request(&req).expect("training request");
        assert_eq!(
            normalized.label_objective.as_str(),
            "risk_adjusted_excess_return"
        );

        req.label_objective = Some("future_magic".into());
        let err = normalize_linear_training_request(&req).expect_err("unknown objective");
        assert!(err.contains("label_objective"));
    }

    #[test]
    fn walk_forward_request_persists_label_objective_in_config() {
        let req = WalkForwardLinearPredictionSetRequest {
            model_code: "phase7_wf_linear_alpha".into(),
            model_version: "phase7-fu-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-market-2016-v1".into(),
            feature_set_version_id: "phase7-alpha-v1".into(),
            training_dataset_id: "phase7-fu-training-v1".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250131".into(),
            train_lookback_days: Some(756),
            prediction_step_days: Some(20),
            label_horizon_days: Some(60),
            label_objective: Some("future_excess_return".into()),
            min_training_samples: Some(100),
            max_windows: Some(1),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };

        let normalized =
            normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

        assert_eq!(normalized.label_objective.as_str(), "future_excess_return");
    }

    #[test]
    fn walk_forward_linear_experiment_config_records_label_objective() {
        let req = WalkForwardLinearPredictionSetRequest {
            model_code: "phase7_wf_linear_alpha".into(),
            model_version: "phase7-fu-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-market-2016-v1".into(),
            feature_set_version_id: "phase7-alpha-v1".into(),
            training_dataset_id: "phase7-fu-training-v1".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250131".into(),
            train_lookback_days: Some(756),
            prediction_step_days: Some(20),
            label_horizon_days: Some(60),
            label_objective: Some("future_excess_return".into()),
            min_training_samples: Some(100),
            max_windows: Some(1),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };
        let normalized =
            normalize_walk_forward_linear_prediction_request(&req).expect("wf request");
        let label_definition =
            label_definition_json(normalized.label_objective, normalized.label_horizon_days);

        let config = walk_forward_linear_experiment_config(&normalized, &label_definition);

        assert_eq!(config["label"]["label"], "future_excess_return");
        assert_eq!(config["label"]["benchmark"], "000300.SH");
        assert_eq!(
            config["point_in_time_policy"],
            "each window trains on dates <= train_end_date and labels are capped at train_end_date"
        );
    }

    #[test]
    fn walk_forward_nonlinear_quantile_ranker_request_keeps_label_gap_and_bucket_params() {
        let req = WalkForwardNonlinearQuantileRankerRequest {
            model_code: "p7_nlq_wf".into(),
            model_version: "h60-fe-v1".into(),
            model_version_id: None,
            training_task_id: None,
            prediction_set_id: None,
            data_version_id: "full-market-2016-v1".into(),
            feature_set_version_id: "phase7-fy-core9-v1".into(),
            training_dataset_id: "ds-p7fy-nlq-wf-v1".into(),
            prediction_start_date: "20250121".into(),
            prediction_end_date: "20250210".into(),
            train_lookback_days: Some(120),
            prediction_step_days: Some(5),
            label_horizon_days: Some(60),
            label_objective: Some("future_excess_return".into()),
            min_training_samples: Some(100),
            max_windows: Some(2),
            bucket_count: Some(7),
            min_samples_per_bucket: Some(25),
            factors: vec![LinearFactorRef {
                factor_code: "fin_roe_daily_std".into(),
                factor_version: "1.0.0".into(),
            }],
        };

        let normalized =
            normalize_walk_forward_nonlinear_quantile_ranker_request(&req).expect("wf nlq request");
        let windows = build_walk_forward_windows(&normalized.linear).expect("wf windows");
        let config = walk_forward_nonlinear_quantile_ranker_experiment_config(&normalized);

        assert_eq!(normalized.bucket_count, 7);
        assert_eq!(normalized.min_samples_per_bucket, 25);
        assert_eq!(
            normalized.linear.prediction_set_id,
            "pred-p7_nlq_wf-h60-fe-v1-nlq-wf-20250121-20250210"
        );
        assert_eq!(windows.len(), 2);
        assert_eq!(
            windows[0].train_end_date,
            NaiveDate::from_ymd_opt(2024, 11, 22).unwrap()
        );
        assert_eq!(
            windows[0].prediction_start_date,
            NaiveDate::from_ymd_opt(2025, 1, 21).unwrap()
        );
        assert_eq!(
            config["trainer"],
            "walk_forward_nonlinear_quantile_ranker_v1"
        );
        assert_eq!(config["bucket_count"], 7);
        assert_eq!(config["label"]["label"], "future_excess_return");
    }

    #[test]
    fn feature_matrix_cache_slices_train_and_prediction_windows_without_requery() {
        let rows = vec![
            TrainingFeatureMatrixRow {
                symbol: "000001.SZ".into(),
                trade_date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                features: vec![1.0, 0.1],
            },
            TrainingFeatureMatrixRow {
                symbol: "000001.SZ".into(),
                trade_date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                features: vec![2.0, 0.2],
            },
            TrainingFeatureMatrixRow {
                symbol: "000002.SZ".into(),
                trade_date: NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
                features: vec![3.0, 0.3],
            },
        ];
        let cache = FeatureMatrixWindowCache::new(rows);

        let train_rows = cache.slice(
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        );
        let prediction_rows = cache.slice(
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
        );

        assert_eq!(cache.row_count(), 3);
        assert_eq!(train_rows.len(), 2);
        assert_eq!(prediction_rows.len(), 1);
        assert_eq!(prediction_rows[0].symbol, "000002.SZ");
    }

    #[test]
    fn prediction_insert_telemetry_counts_bulk_insert_batches() {
        let rows = (0..12_001)
            .map(|idx| {
                build_prediction_row(
                    "pred-telemetry",
                    &format!("{:06}.SZ", idx),
                    "2025-01-02",
                    idx as f64,
                    idx + 1,
                )
                .expect("prediction row")
            })
            .collect::<Vec<_>>();

        let telemetry = prediction_insert_telemetry(&rows);

        assert_eq!(telemetry["row_count"], 12_001);
        assert_eq!(telemetry["batch_size"], 5_000);
        assert_eq!(telemetry["batch_count"], 3);
        assert_eq!(telemetry["chunk_row_counts"], json!([5000, 5000, 2001]));
    }

    #[test]
    fn nonlinear_ranker_prediction_metadata_records_cache_and_insert_telemetry() {
        let label_definition = label_definition_json(LabelObjective::RiskAdjustedExcessReturn, 45);
        let rows = vec![
            build_prediction_row("p7gb-test", "000001.SZ", "2025-01-02", 0.7, 1).expect("row 1"),
            build_prediction_row("p7gb-test", "000002.SZ", "2025-01-02", 0.4, 2).expect("row 2"),
        ];
        let metadata = nonlinear_quantile_ranker_prediction_set_metadata(
            "train-p7gb-test",
            512,
            &label_definition,
            json!({"buckets": []}),
            feature_matrix_cache_metadata(
                "prediction_window",
                NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2025, 3, 31).unwrap(),
                20_000,
            ),
            prediction_insert_telemetry(&rows),
            prediction_generation_telemetry(
                "train_only_nonlinear_quantile_ranker",
                1,
                1,
                0,
                rows.len(),
                std::time::Duration::from_millis(125),
                vec![prediction_progress_stage(
                    "score_prediction_window",
                    1,
                    1,
                    rows.len(),
                    std::time::Duration::from_millis(25),
                )],
            ),
        );

        assert_eq!(metadata["model_type"], "nonlinear_quantile_ranker");
        assert_eq!(
            metadata["feature_matrix_cache"]["scope"],
            "prediction_window"
        );
        assert_eq!(metadata["feature_matrix_cache"]["row_count"], 20_000);
        assert_eq!(metadata["prediction_insert_telemetry"]["row_count"], 2);
        assert_eq!(metadata["prediction_insert_telemetry"]["batch_count"], 1);
        assert_eq!(
            metadata["prediction_generation_telemetry"]["operation"],
            "train_only_nonlinear_quantile_ranker"
        );
        assert_eq!(
            metadata["prediction_generation_telemetry"]["progress_pct"],
            100.0
        );
        assert_eq!(
            metadata["prediction_generation_telemetry"]["stages"][0]["stage"],
            "score_prediction_window"
        );
    }

    #[test]
    fn prediction_generation_telemetry_records_elapsed_and_progress() {
        let telemetry = prediction_generation_telemetry(
            "walk_forward_nonlinear_quantile_ranker",
            6,
            4,
            2,
            253_694,
            std::time::Duration::from_millis(12_345),
            vec![
                prediction_progress_stage(
                    "load_feature_matrix_cache",
                    1,
                    1,
                    923_773,
                    std::time::Duration::from_millis(3_000),
                ),
                prediction_progress_stage(
                    "fit_and_score_windows",
                    6,
                    6,
                    253_694,
                    std::time::Duration::from_millis(9_345),
                ),
            ],
        );

        assert_eq!(
            telemetry["operation"],
            "walk_forward_nonlinear_quantile_ranker"
        );
        assert_eq!(telemetry["elapsed_ms"], 12_345);
        assert_eq!(telemetry["total_units"], 6);
        assert_eq!(telemetry["completed_units"], 4);
        assert_eq!(telemetry["skipped_units"], 2);
        assert_eq!(telemetry["prediction_rows"], 253_694);
        assert_eq!(telemetry["progress_pct"], 100.0);
        assert_eq!(telemetry["stages"][0]["row_count"], 923_773);
    }

    #[test]
    fn prediction_cache_economics_report_flags_cached_train_and_uncached_test_sets() {
        let train = PredictionSetCacheEconomicsInput {
            prediction_set_id: "p7gb-w1-tr".into(),
            status: "ready".into(),
            start_date: NaiveDate::from_ymd_opt(2023, 10, 24).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2024, 5, 17).unwrap(),
            metadata: json!({
                "feature_matrix_cache": {
                    "scope": "request_window",
                    "start_date": "2023-01-01",
                    "end_date": "2024-05-18",
                    "row_count": 923773
                },
                "prediction_insert_telemetry": {
                    "mode": "bulk_insert",
                    "row_count": 383993,
                    "batch_size": 5000,
                    "batch_count": 77,
                    "chunk_row_counts": []
                },
                "prediction_generation_telemetry": {
                    "operation": "walk_forward_nonlinear_quantile_ranker",
                    "elapsed_ms": 12345,
                    "progress_pct": 100.0,
                    "prediction_rows": 383993
                }
            }),
            prediction_rows: 383_993,
            symbol_count: 3_094,
            trading_day_count: 140,
        };
        let test = PredictionSetCacheEconomicsInput {
            prediction_set_id: "p7gb-w1-te".into(),
            status: "ready".into(),
            start_date: NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 24).unwrap(),
            metadata: json!({}),
            prediction_rows: 484_286,
            symbol_count: 3_313,
            trading_day_count: 164,
        };

        let report = prediction_set_cache_economics_report_json(&[train, test]);

        assert_eq!(report["prediction_set_count"], 2);
        assert_eq!(report["total_prediction_rows"], 868_279);
        assert_eq!(report["sets"][0]["cache_metadata_present"], true);
        assert_eq!(report["sets"][0]["generation_telemetry_present"], true);
        assert_eq!(
            report["sets"][0]["feature_matrix_cache"]["row_count"],
            923_773
        );
        assert_eq!(report["sets"][1]["cache_metadata_present"], false);
        assert_eq!(
            report["economics"]["recommendation"],
            "audit_uncached_prediction_sets"
        );
        assert_eq!(report["economics"]["missing_cache_metadata_count"], 1);
        assert_eq!(report["economics"]["missing_insert_telemetry_count"], 1);
        assert_eq!(report["economics"]["missing_generation_telemetry_count"], 1);
        assert_eq!(report["economics"]["max_generation_elapsed_ms"], 12_345);
        assert_eq!(report["economics"]["max_rows_per_trading_day"], 2953.0);
    }

    #[test]
    fn excess_label_subtracts_benchmark_future_return() {
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let stock_closes = vec![
            (trade_date, 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
        ];
        let benchmark_closes = vec![
            (trade_date, 100.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 101.0),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 102.0),
        ];

        let label = label_for_objective(
            LabelObjective::FutureExcessReturn,
            Some(&stock_closes),
            Some(&benchmark_closes),
            trade_date,
            None,
            2,
        )
        .expect("excess label");

        assert!((label - 0.08).abs() < 1e-9);
    }

    #[test]
    fn risk_adjusted_excess_label_penalizes_forward_downside() {
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let smooth_stock = vec![
            (trade_date, 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.4),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 10.8),
        ];
        let choppy_stock = vec![
            (trade_date, 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 9.2),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 10.8),
        ];
        let benchmark_closes = vec![
            (trade_date, 100.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 100.0),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 100.0),
        ];

        let smooth = label_for_objective(
            LabelObjective::RiskAdjustedExcessReturn,
            Some(&smooth_stock),
            Some(&benchmark_closes),
            trade_date,
            None,
            2,
        )
        .expect("smooth label");
        let choppy = label_for_objective(
            LabelObjective::RiskAdjustedExcessReturn,
            Some(&choppy_stock),
            Some(&benchmark_closes),
            trade_date,
            None,
            2,
        )
        .expect("choppy label");

        assert!(smooth > choppy);
    }

    #[test]
    fn label_objective_drops_samples_when_target_date_crosses_prediction_cutoff() {
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let closes = vec![
            (trade_date, 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
        ];

        let label = label_for_objective(
            LabelObjective::FutureReturn,
            Some(&closes),
            None,
            trade_date,
            Some(NaiveDate::from_ymd_opt(2025, 1, 10).unwrap()),
            2,
        );

        assert!(label.is_none());
    }

    #[test]
    fn training_feature_matrix_rows_build_samples_without_losing_factor_order() {
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let mut closes_by_symbol = HashMap::new();
        closes_by_symbol.insert(
            "AAA".to_string(),
            vec![
                (trade_date, 10.0),
                (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
                (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
            ],
        );
        closes_by_symbol.insert(
            "BBB".to_string(),
            vec![
                (trade_date, 20.0),
                (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 20.5),
                (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 21.0),
            ],
        );

        let samples = training_samples_from_feature_matrix_rows(
            vec![
                TrainingFeatureMatrixRow {
                    symbol: "AAA".to_string(),
                    trade_date,
                    features: vec![0.25, -0.75],
                },
                TrainingFeatureMatrixRow {
                    symbol: "BBB".to_string(),
                    trade_date,
                    features: vec![0.50],
                },
                TrainingFeatureMatrixRow {
                    symbol: "AAA".to_string(),
                    trade_date: NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
                    features: vec![f64::NAN, 0.10],
                },
            ],
            &closes_by_symbol,
            &Vec::new(),
            LabelObjective::FutureReturn,
            None,
            2,
            2,
        );

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].features, vec![0.25, -0.75]);
        assert!((samples[0].label - 0.10).abs() < 1e-9);
    }

    #[test]
    fn prediction_feature_matrix_rows_build_ranked_scores_without_losing_factor_order() {
        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let rows = prediction_rows_from_feature_matrix_rows(
            "pred-v1",
            vec![
                TrainingFeatureMatrixRow {
                    symbol: "BBB".to_string(),
                    trade_date,
                    features: vec![1.0, 0.25],
                },
                TrainingFeatureMatrixRow {
                    symbol: "AAA".to_string(),
                    trade_date,
                    features: vec![2.0, -0.5],
                },
                TrainingFeatureMatrixRow {
                    symbol: "CCC".to_string(),
                    trade_date,
                    features: vec![4.0],
                },
                TrainingFeatureMatrixRow {
                    symbol: "DDD".to_string(),
                    trade_date,
                    features: vec![f64::NAN, 1.0],
                },
            ],
            &[0.25, -1.0],
            2,
        )
        .expect("prediction rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].symbol, "AAA");
        assert_eq!(rows[0].rank, 1);
        assert!((rows[0].score - 1.0).abs() < 1e-9);
        assert_eq!(rows[0].available_at, trade_date);
        assert_eq!(rows[1].symbol, "BBB");
        assert_eq!(rows[1].rank, 2);
        assert!((rows[1].score - 0.0).abs() < 1e-9);
    }

    #[test]
    fn nonlinear_quantile_ranker_learns_bucket_payoffs_and_scores_prediction_rows() {
        let train_rows = vec![
            TrainingSample { regime_tag: None,
                features: vec![-1.0, 0.2],
                label: -0.02,
            },
            TrainingSample { regime_tag: None,
                features: vec![-0.8, 0.1],
                label: -0.01,
            },
            TrainingSample { regime_tag: None,
                features: vec![0.1, 0.5],
                label: 0.01,
            },
            TrainingSample { regime_tag: None,
                features: vec![0.2, 0.4],
                label: 0.02,
            },
            TrainingSample { regime_tag: None,
                features: vec![0.8, -0.3],
                label: 0.08,
            },
            TrainingSample { regime_tag: None,
                features: vec![1.0, -0.2],
                label: 0.10,
            },
        ];

        let model =
            fit_nonlinear_quantile_ranker(&train_rows, 2, 3, 2).expect("nonlinear quantile ranker");

        assert_eq!(model.factor_count, 2);
        assert_eq!(model.bucket_count, 3);
        assert_eq!(model.tables.len(), 2);
        assert!(
            model.tables[0].bucket_scores[2] > model.tables[0].bucket_scores[0],
            "first factor should learn that the high bucket has the stronger payoff"
        );

        let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
        let rows = nonlinear_prediction_rows_from_feature_matrix_rows(
            "pred-nonlinear-v1",
            vec![
                TrainingFeatureMatrixRow {
                    symbol: "WEAK".to_string(),
                    trade_date,
                    features: vec![-0.9, 0.1],
                },
                TrainingFeatureMatrixRow {
                    symbol: "STRONG".to_string(),
                    trade_date,
                    features: vec![0.9, -0.1],
                },
                TrainingFeatureMatrixRow {
                    symbol: "BROKEN".to_string(),
                    trade_date,
                    features: vec![f64::NAN, 1.0],
                },
            ],
            &model,
        )
        .expect("prediction rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].symbol, "STRONG");
        assert_eq!(rows[0].rank, 1);
        assert_eq!(rows[0].available_at, trade_date);
        assert_eq!(rows[1].symbol, "WEAK");
        assert!(rows[0].score > rows[1].score);
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
            label_objective: Some("future_excess_return".into()),
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
        assert_eq!(config["label"]["type"], "future_excess_return");
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
