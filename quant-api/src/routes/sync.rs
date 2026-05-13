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
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
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
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
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
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
    });
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步复权因子");
    match quant_data::sync::sync_adj_factor(
        &state.db, &state.tushare, &req.symbols, &req.start_date, &req.end_date, &dv_id,
    ).await {
        Ok(count) => Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/adj-factor/background
///
/// 大批量后台同步复权因子，立即返回 task_id。
pub async fn sync_adj_factor_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
    });
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "后台同步复权因子");

    let state = state.clone();
    let symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    tokio::spawn(async move {
        match quant_data::sync::sync_adj_factor(
            &state.db, &state.tushare, &symbols, &start, &end, &task_id,
        ).await {
            Ok(count) => info!(task_id = %task_id, count = count, "后台同步复权因子完成"),
            Err(e) => tracing::error!(task_id = %task_id, error = %e.to_string(), "后台同步复权因子失败"),
        }
    });

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
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
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
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

/// POST /api/v1/quant/data/sync/daily/background
///
/// 大批量后台同步日线行情，立即返回 task_id。
/// 通过 GET /api/v1/quant/data/sync/tasks/:task_id 查询进度。
pub async fn sync_daily_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req.data_version_id.unwrap_or_else(|| {
        chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
    });
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "后台同步日线");

    let state = state.clone();
    let symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    tokio::spawn(async move {
        match quant_data::sync::sync_daily_bars(
            &state.db, &state.tushare, &symbols, &start, &end, &task_id,
        )
        .await
        {
            Ok(count) => {
                info!(task_id = %task_id, count = count, "后台同步日线完成");
            }
            Err(e) => {
                tracing::error!(task_id = %task_id, error = %e.to_string(), "后台同步日线失败");
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
}

/// GET /api/v1/quant/data/sync/tasks/:task_id
///
/// 查询数据同步任务状态（同步/后台均适用）。
pub async fn sync_task_status(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let row: Option<(String, String, Option<i32>, Option<i32>, Option<i32>, Option<i32>)> =
        sqlx::query_as(
            "SELECT task_type, status, total_count, success_count, failed_count, progress
             FROM data_sync_task WHERE task_id = $1",
        )
        .bind(&task_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();

    match row {
        Some((task_type, status, total, success, failed, progress)) => {
            Json(json!({"code": 0, "data": {
                "task_id": task_id,
                "task_type": task_type,
                "status": status,
                "total": total,
                "success": success,
                "failed": failed,
                "progress": progress,
            }}))
        }
        None => Json(json!({"code": 1, "message": "task not found"})),
    }
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
