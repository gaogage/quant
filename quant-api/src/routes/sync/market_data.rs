/// 数据同步路由
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    path::Path,
    sync::Arc,
    time::Duration as StdDuration,
};
use tokio::{process::Command, time::timeout};
use tracing::info;
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

use crate::phase7_alpha_admission::{
    industry_prosperity_alpha_admission_policy, industry_prosperity_alpha_admission_policy_static,
    INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD, INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
    SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
};
use crate::AppState;

use super::*;

#[derive(Debug, Deserialize)]
pub struct SyncStockBasicReq {
    #[serde(default)]
    pub data_version_id: Option<String>,
}



pub async fn sync_stock_basic(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncStockBasicReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, "开始同步 A 股基本信息");
    match quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &dv_id).await {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
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
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步日线");
    match quant_data::sync::sync_daily_bars(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
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
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步复权因子");
    match quant_data::sync::sync_adj_factor(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/fund-adj — 同步 ETF/基金复权因子（Tushare fund_adj）


pub async fn sync_fund_adj(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步基金复权因子");
    match quant_data::sync::sync_fund_adj(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
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
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);

    let state = state.clone();
    let mut symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    // symbols 为空表示按区间修复全 A 股。必须按 PIT 上市/退市区间展开，
    // 不能只取当前仍上市股票，否则历史日线缺口会被退市/状态变更掩盖。
    if symbols.is_empty() {
        symbols = sqlx::query_as::<_, (String,)>(
            "SELECT symbol FROM market_stock
             WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
               AND list_date IS NOT NULL
               AND list_date <= $1::date
               AND (delist_date IS NULL OR delist_date >= $2::date)
             ORDER BY symbol",
        )
        .bind(&end)
        .bind(&start)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(s,)| s)
        .collect();
    }
    info!(data_version_id = %dv_id, symbols = symbols.len(), "后台同步复权因子");

    crate::sync_task_registry::spawn_sync_task(
        state.sync_tasks.clone(),
        task_id.clone(),
        async move {
            match quant_data::sync::sync_adj_factor(
                &state.db,
                &state.tushare,
                &symbols,
                &start,
                &end,
                &task_id,
            )
            .await
            {
                Ok(count) => info!(task_id = %task_id, count = count, "后台同步复权因子完成"),
                Err(e) => {
                    tracing::error!(task_id = %task_id, error = %e.to_string(), "后台同步复权因子失败")
                }
            }
        },
    )
    .await;

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
}

/// POST /api/v1/quant/data/sync/adj-factor/backfill
/// 手动触发复权因子前向填充兜底,修复任意日期的复权因子缺失(不必重跑整个 EOD)。
/// 用于事后修复如 7/20-7/21 EOD 卡死导致的复权因子缺失,避免 adj 视图退化为 raw 价。
#[derive(Debug, Deserialize)]


pub struct BackfillAdjFactorReq {
    /// 目标日期 YYYYMMDD(必填)
    pub date: String,
    /// 向前回溯几天(默认 5),对每个日期逐日前向填充
    #[serde(default)]
    pub days: Option<i64>,
}



pub async fn sync_adj_factor_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillAdjFactorReq>,
) -> impl IntoResponse {
    let target = match NaiveDate::parse_from_str(&req.date, "%Y%m%d") {
        Ok(d) => d,
        Err(e) => {
            return Json(
                json!({"code": 1, "message": format!("date 格式错误(需 YYYYMMDD): {}", e)}),
            )
        }
    };
    let days = req.days.unwrap_or(5).max(0);
    let dv_id = format!("dv-adj-backfill-{}", req.date);
    info!(date = %target, days, %dv_id, "手动触发复权因子前向填充兜底");

    // 逐日调用 backfill_adj_factor_for_date(幂等,非交易日/无 bar 自动跳过)
    let mut processed: Vec<Value> = Vec::new();
    for i in 0..=days {
        let d = target - Duration::days(i);
        let bar_cnt: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT symbol) FROM market_stock_daily_bar WHERE trade_date = $1",
        )
        .bind(d)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
        crate::routes::scheduler::backfill_adj_factor_for_date(&state.db, d, &dv_id).await;
        let adj_cnt: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT symbol) FROM market_adjustment_factor WHERE trade_date = $1",
        )
        .bind(d)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
        let pct = if bar_cnt > 0 { adj_cnt * 100 / bar_cnt } else { 100 };
        processed.push(json!({
            "date": d.format("%Y-%m-%d").to_string(),
            "bar_count": bar_cnt,
            "adj_count": adj_cnt,
            "coverage_pct": pct,
        }));
    }
    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "processed": processed}}))
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

#[derive(Debug, Deserialize)]


pub struct SyncHsgtRequest {
    start_date: String,
    end_date: String,
}



pub async fn sync_moneyflow_hsgt(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncHsgtRequest>,
) -> impl IntoResponse {
    let client =
        quant_data::tushare::client::TushareClient::from_env().expect("Tushare client init failed");
    match quant_data::sync::sync_moneyflow_hsgt(&state.db, &client, &req.start_date, &req.end_date)
        .await
    {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

#[derive(Debug, Deserialize)]


pub struct SyncMarginRequest {
    start_date: String,
    end_date: String,
}



pub async fn sync_margin(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncMarginRequest>,
) -> impl IntoResponse {
    let client =
        quant_data::tushare::client::TushareClient::from_env().expect("Tushare client init failed");
    match quant_data::sync::sync_margin(&state.db, &client, &req.start_date, &req.end_date).await {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

#[derive(Debug, Deserialize)]


pub struct SyncFundDailyReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    pub data_version_id: Option<String>,
}



pub async fn sync_fund_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFundDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步基金日线");
    match quant_data::sync::sync_fund_daily(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}



pub async fn sync_index_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncIndexDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, indexes = req.index_codes.len(), "同步指数日线");
    match quant_data::sync::sync_index_daily(
        &state.db,
        &state.tushare,
        &req.index_codes,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/trade-cal


pub async fn sync_trade_cal(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
        &state.db,
        &req.symbols,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(result) => Json(json!({"code": 0, "data": result})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// GET /api/v1/quant/data/stats


pub async fn data_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stock_count = quant_data::repository::count_stocks(&state.db)
        .await
        .unwrap_or(0);
    let bar_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_bar_adj")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let adj_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_adjustment_factor")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let fin_stmt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_statement")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let fin_ind: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_indicator")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    Json(json!({"code": 0, "data": {
        "stock_count": stock_count, "bar_count": bar_count, "adj_factor_count": adj_count,
        "fin_statement_count": fin_stmt, "fin_indicator_count": fin_ind
    }}))
}

/// GET /api/v1/quant/data/phase7-feasibility-audit


pub async fn sync_daily_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);

    let state = state.clone();
    let mut symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    // symbols 为空表示按区间修复全 A 股。必须按 PIT 上市/退市区间展开，
    // 不能只取当前仍上市股票，否则历史日线缺口会被状态变更或 ETF/REIT 混入掩盖。
    if symbols.is_empty() {
        symbols = sqlx::query_as::<_, (String,)>(
            "SELECT symbol FROM market_stock
             WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
               AND list_date IS NOT NULL
               AND list_date <= $1::date
               AND (delist_date IS NULL OR delist_date >= $2::date)
             ORDER BY symbol",
        )
        .bind(&end)
        .bind(&start)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(s,)| s)
        .collect();
    }
    info!(data_version_id = %dv_id, symbols = symbols.len(), "后台同步日线");

    crate::sync_task_registry::spawn_sync_task(
        state.sync_tasks.clone(),
        task_id.clone(),
        async move {
            match quant_data::sync::sync_daily_bars(
                &state.db,
                &state.tushare,
                &symbols,
                &start,
                &end,
                &task_id,
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
        },
    )
    .await;

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
}

/// GET /api/v1/quant/data/sync/tasks/:task_id
///
/// 查询数据同步任务状态（同步/后台均适用）。


pub async fn sync_fund_basic(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match quant_data::sync::sync_fund_basic(&state.db, &state.tushare).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

