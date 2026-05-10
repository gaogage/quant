/// API 路由
pub mod sync;

use axum::{extract::{Path, State}, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use crate::AppState;

pub async fn create_backtest(State(_s): State<Arc<AppState>>, Json(_b): Json<serde_json::Value>) -> impl IntoResponse {
    Json(json!({"code":0,"data":{"task_id":"placeholder","status":"pending"}}))
}
pub async fn get_backtest(Path(id): Path<String>) -> impl IntoResponse {
    Json(json!({"code":0,"data":{"task_id":id,"status":"pending","progress":0}}))
}
pub async fn get_report(Path(_id): Path<String>) -> impl IntoResponse {
    Json(json!({"code":0,"data":{}}))
}
