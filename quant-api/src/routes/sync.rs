/// 数据同步路由

use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
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
        Ok(count) => Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
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
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步日线");
    match quant_data::sync::sync_daily_bars(
        &state.db, &state.tushare, &req.symbols, &req.start_date, &req.end_date, &dv_id,
    ).await {
        Ok(count) => Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/adj-factor
#[derive(Debug, Deserialize)]
pub struct SyncAdjFactorReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_adj_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S").to_string()
    });
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步复权因子");
    match quant_data::sync::sync_adj_factor(
        &state.db, &state.tushare, &req.symbols, &req.start_date, &req.end_date, &dv_id,
    ).await {
        Ok(count) => Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/index-daily
#[derive(Debug, Deserialize)]
pub struct SyncIndexDailyReq {
    pub index_codes: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_index_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncIndexDailyReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S").to_string()
    });
    info!(data_version_id = %dv_id, indexes = req.index_codes.len(), "同步指数日线");
    match quant_data::sync::sync_index_daily(
        &state.db, &state.tushare, &req.index_codes, &req.start_date, &req.end_date, &dv_id,
    ).await {
        Ok(count) => Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/trade-cal
pub async fn sync_trade_cal(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    info!("同步交易日历");
    let mut count = 0usize;
    for ex in &["SSE", "SZSE"] {
        match quant_data::sync::sync_trade_calendar(&state.db, &state.tushare, ex).await {
            Ok(c) => count += c,
            Err(e) => return Json(json!({"code": 1, "message": format!("{}: {}", ex, e)})),
        }
    }
    Json(json!({"code": 0, "data": {"status": "completed", "count": count}}))
}

/// POST /api/v1/quant/data/quality-check
#[derive(Debug, Deserialize)]
pub struct QualityCheckReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
}

pub async fn quality_check(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QualityCheckReq>,
) -> impl IntoResponse {
    info!(symbols = req.symbols.len(), "数据质量检查");
    match quant_data::sync::run_quality_check(
        &state.db, &req.symbols, &req.start_date, &req.end_date,
    ).await {
        Ok(result) => Json(json!({"code": 0, "data": result})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// GET /api/v1/quant/data/stats
pub async fn data_stats(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let stock_count = quant_data::repository::count_stocks(&state.db).await.unwrap_or(0);
    let bar_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_bar")
        .fetch_one(&state.db).await.unwrap_or(0);
    let adj_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_adjustment_factor")
        .fetch_one(&state.db).await.unwrap_or(0);
    let fin_stmt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_statement")
        .fetch_one(&state.db).await.unwrap_or(0);
    let fin_ind: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_indicator")
        .fetch_one(&state.db).await.unwrap_or(0);
    Json(json!({"code": 0, "data": {
        "stock_count": stock_count, "bar_count": bar_count, "adj_factor_count": adj_count,
        "fin_statement_count": fin_stmt, "fin_indicator_count": fin_ind
    }}))
}

/// POST /api/v1/quant/data/sync/financial
#[derive(Debug, Deserialize)]
pub struct SyncFinancialReq {
    pub symbols: Vec<String>,
}

pub async fn sync_financial(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFinancialReq>,
) -> impl IntoResponse {
    info!(symbols = req.symbols.len(), "同步财务数据");
    match quant_data::sync::sync_financial_data(
        &state.db, &state.tushare, &req.symbols,
    ).await {
        Ok((stmt, ind)) => Json(json!({"code": 0, "data": {"statements": stmt, "indicators": ind}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}
