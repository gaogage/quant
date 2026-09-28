//! ML API routes — minimal Phase 5 prediction-set smoke path
// 矩阵乘法索引循环（MLP 前向/反向传播、grid 分桶）保持索引形式：数值求和
// 顺序敏感且迭代器化损害可读性（科学计算惯例），显式豁免。
// 枚举变体统一 Return 后缀是标签目标语义命名，同样豁免。
#![allow(clippy::needless_range_loop)]
#![allow(clippy::enum_variant_names)]

mod internal;
mod prediction_set;
mod training;
mod walk_forward;

pub(crate) use internal::*;
pub(crate) use prediction_set::*;
pub(crate) use training::*;
pub(crate) use walk_forward::*;
pub use walk_forward::{
    WalkForwardLinearPredictionSetRequest, WalkForwardNonlinearQuantileRankerRequest,
};

use crate::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

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

#[derive(Debug, Deserialize)]
pub struct PredictionSetReadinessRequest {
    pub prediction_set_id: String,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
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

pub async fn report_prediction_set_readiness(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PredictionSetReadinessRequest>,
) -> impl IntoResponse {
    match build_prediction_set_readiness_report_from_request(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ninth_batch;
