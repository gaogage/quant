/// 数据同步路由

use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::info;

use crate::AppState;

/// POST /api/v1/quant/data/sync/stock-basic
#[derive(Debug, Deserialize)]
pub struct SyncStockBasicReq {
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_stock_basic(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncStockBasicReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S").to_string()
    });

    info!(data_version_id = %dv_id, "开始同步 A 股基本信息");

    match quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &dv_id).await {
        Ok(count) => Json(json!({
            "code": 0,
            "data": {
                "task_id": dv_id,
                "status": "completed",
                "count": count
            }
        })),
        Err(e) => Json(json!({
            "code": 1,
            "message": e.to_string()
        })),
    }
}

/// POST /api/v1/quant/data/sync/daily
#[derive(Debug, Deserialize)]
pub struct SyncDailyReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S").to_string()
    });

    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "开始同步日线");

    match quant_data::sync::sync_daily_bars(
        &state.db, &state.tushare,
        &req.symbols, &req.start_date, &req.end_date, &dv_id,
    ).await {
        Ok(count) => Json(json!({
            "code": 0,
            "data": {
                "task_id": dv_id,
                "status": "completed",
                "count": count
            }
        })),
        Err(e) => Json(json!({
            "code": 1,
            "message": e.to_string()
        })),
    }
}

/// GET /api/v1/quant/data/stats
pub async fn data_stats(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let stock_count = quant_data::repository::count_stocks(&state.db).await.unwrap_or(0);
    Json(json!({
        "code": 0,
        "data": {
            "stock_count": stock_count,
        }
    }))
}
