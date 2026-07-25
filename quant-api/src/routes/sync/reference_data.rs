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

pub async fn sync_namechange(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match quant_data::sync::sync_namechange(&state.db, &state.tushare).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension
///
/// 同步当日停牌股票数据
#[derive(Debug, serde::Deserialize)]


pub struct SyncSuspensionRequest {
    pub trade_date: String, // YYYYMMDD
}



pub async fn sync_suspension(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncSuspensionRequest>,
) -> impl IntoResponse {
    match quant_data::sync::sync_suspension(&state.db, &state.tushare, &req.trade_date).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/limit
#[derive(Debug, serde::Deserialize)]


pub struct SyncLimitListRequest {
    pub trade_date: String, // YYYYMMDD
}



pub async fn sync_limit_list(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncLimitListRequest>,
) -> impl IntoResponse {
    match quant_data::sync::sync_limit_list(&state.db, &state.tushare, &req.trade_date).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension/backfill
///
/// 批量回填停牌历史数据（按交易日历逐日同步）
#[derive(Debug, serde::Deserialize)]


pub struct BackfillRequest {
    pub start_date: String, // YYYYMMDD
    pub end_date: String,   // YYYYMMDD
    #[serde(default)]
    pub force_tushare: bool,
}



pub async fn sync_suspension_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    if !req.force_tushare {
        return match quant_data::sync::backfill_suspension_completion_markers(
            &state.db,
            &req.start_date,
            &req.end_date,
        )
        .await
        {
            Ok(markers) => Json(json!({
                "code": 0,
                "data": {
                    "mode": "completion_marker_backfill",
                    "markers": markers,
                    "source": "derived:susp_existing"
                }
            })),
            Err(e) => Json(json!({"code": 1, "message": e})),
        };
    }

    match quant_data::sync::sync_suspension_range(
        &state.db,
        &state.tushare,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(total) => Json(json!({
            "code": 0,
            "data": {
                "mode": "tushare_range",
                "total_records": total,
                "source": "tushare:suspend_d"
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension/derive-from-daily
///
/// 基于已同步 A 股日线缺失派生历史停牌事实；不补价格。


pub async fn derive_suspension_from_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    match quant_data::sync::derive_suspension_from_daily_absence(
        &state.db,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(inserted) => Json(json!({
            "code": 0,
            "data": {
                "mode": "derived_from_daily_absence",
                "inserted_records": inserted,
                "source": "derived:daily_absence_suspension"
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/limit/backfill


pub async fn sync_limit_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    let start = match parse_health_date(&req.start_date) {
        Ok(date) => date,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    let end = match parse_health_date(&req.end_date) {
        Ok(date) => date,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    if start > end {
        return Json(json!({"code": 1, "message": "start_date 不能晚于 end_date"}));
    }

    let earliest = proven_limit_list_earliest_date();
    let mut derived_records = 0u64;
    let mut tushare_records = 0usize;
    let mut marker_rows = 0u64;

    if start < earliest {
        let derive_end = end.min(earliest - Duration::days(1));
        if derive_end >= start {
            match quant_data::sync::derive_limit_list_from_daily_bars(
                &state.db,
                &yyyymmdd(start),
                &yyyymmdd(derive_end),
            )
            .await
            {
                Ok(n) => derived_records += n,
                Err(e) => return Json(json!({"code": 1, "message": e})),
            }
        }
    }

    if end >= earliest {
        let tushare_start = start.max(earliest);
        if req.force_tushare {
            match quant_data::sync::sync_limit_list_range(
                &state.db,
                &state.tushare,
                &yyyymmdd(tushare_start),
                &yyyymmdd(end),
            )
            .await
            {
                Ok(n) => tushare_records += n,
                Err(e) => return Json(json!({"code": 1, "message": e})),
            }
        }
        match quant_data::sync::backfill_limit_completion_markers(
            &state.db,
            &yyyymmdd(tushare_start),
            &yyyymmdd(end),
            if req.force_tushare {
                "tushare:limit_list_d_range"
            } else {
                "derived:limit_existing"
            },
        )
        .await
        {
            Ok(n) => marker_rows += n,
            Err(e) => return Json(json!({"code": 1, "message": e})),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "mode": if req.force_tushare { "derive_then_tushare_range" } else { "derive_then_marker_backfill" },
            "derived_records": derived_records,
            "tushare_records": tushare_records,
            "marker_rows": marker_rows,
            "derived_until": if start < earliest { Some(yyyymmdd(end.min(earliest - Duration::days(1)))) } else { None::<String> },
        }
    }))
}

/// POST /api/v1/quant/data/sync/historical
///
/// 补齐历史数据（2006-2015），参数：start_date、end_date
#[derive(Debug, serde::Deserialize)]


pub struct SyncHistoricalRequest {
    pub start_date: String, // YYYYMMDD
    pub end_date: String,   // YYYYMMDD
}



pub async fn sync_historical(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncHistoricalRequest>,
) -> impl IntoResponse {
    let db = &state.db;
    let client = &state.tushare;
    let empty_symbols: Vec<String> = vec![];

    let mut results = Vec::new();

    // 1. 日线行情
    info!(
        "[sync-hist] 同步日线行情 {} → {}",
        req.start_date, req.end_date
    );
    match quant_data::sync::sync_daily_bars(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "daily-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("daily_bar: {} 条", n)),
        Err(e) => results.push(format!("daily_bar 失败: {}", e)),
    }

    // 2. 复权因子
    info!("[sync-hist] 同步复权因子");
    match quant_data::sync::sync_adj_factor(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "adj-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("adj_factor: {} 条", n)),
        Err(e) => results.push(format!("adj_factor 失败: {}", e)),
    }

    // 3. 日线基础
    info!("[sync-hist] 同步日线基础指标");
    match quant_data::sync::sync_daily_basic(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "basic-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("daily_basic: {} 条", n)),
        Err(e) => results.push(format!("daily_basic 失败: {}", e)),
    }

    Json(json!({"code": 0, "data": {"results": results}}))
}


