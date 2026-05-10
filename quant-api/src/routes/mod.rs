/// API 路由处理

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

/// POST /api/v1/quant/backtests — 创建回测任务
pub async fn create_backtest(
    State(_state): State<Arc<AppState>>,
    Json(_body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // TODO: Phase 2 实现
    Json(json!({
        "code": 0,
        "data": {
            "task_id": "placeholder",
            "status": "pending",
            "message": "backtest task accepted"
        }
    }))
}

/// GET /api/v1/quant/backtests/:id — 回测状态
pub async fn get_backtest(
    Path(id): Path<String>,
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": {
            "task_id": id,
            "status": "pending",
            "progress": 0
        }
    }))
}

/// GET /api/v1/quant/backtests/:id/report — 回测报告
pub async fn get_report(
    Path(_id): Path<String>,
) -> impl IntoResponse {
    Json(json!({"code": 0, "data": {}}))
}

/// GET /api/v1/quant/backtests/:id/equity — 净值曲线
pub async fn get_equity(
    Path(_id): Path<String>,
) -> impl IntoResponse {
    Json(json!({"code": 0, "data": {"equity_curve": [], "benchmark_curve": []}}))
}
