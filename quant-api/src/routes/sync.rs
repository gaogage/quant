/// 数据同步路由
use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Duration, NaiveDate};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tracing::info;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct DataSyncTaskReq {
    pub dataset: String,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub index_codes: Vec<String>,
    #[serde(default)]
    pub exchanges: Vec<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub quality_check: bool,
    #[serde(default)]
    pub create_data_version: bool,
    #[serde(default)]
    pub retry_of_task_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TusharePermissionSmokeReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

fn default_source() -> String {
    "tushare".into()
}

fn generated_data_version_id() -> String {
    chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
}

fn parse_optional_date(value: Option<&str>) -> Result<Option<NaiveDate>, String> {
    value
        .map(|raw| {
            NaiveDate::parse_from_str(raw, "%Y%m%d")
                .map_err(|_| format!("date must use YYYYMMDD format: {}", raw))
        })
        .transpose()
}

const PHASE7_FEASIBILITY_COMBOS: &[&str] = &[
    "phase7_financial_quality_v1",
    "phase7_valuation_v1",
    "phase7_moneyflow_v1",
    "phase7_industry_residual_quality_v1",
    "phase7_growth_recovery_v1",
    "phase7_quality_relative_strength_v1",
    "phase7_quality_event_window_overlay_v1",
    "phase7_event_surprise_v1",
    "phase7_event_window_earnings_v1",
    "phase7_quality_recovery_acceleration_v1",
];

const PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS: &[&str] = &["cashflow", "dividend", "repurchase"];
const PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS: usize = 3;
const PHASE7_PERMISSION_SMOKE_MAX_ROWS: usize = 5;

fn phase7_permission_smoke_sources(requested: &[String]) -> Vec<String> {
    let raw_sources: Vec<String> = if requested.is_empty() {
        PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS
            .iter()
            .map(|source| (*source).to_string())
            .collect()
    } else {
        requested
            .iter()
            .map(|source| source.trim().to_ascii_lowercase())
            .filter(|source| !source.is_empty())
            .collect()
    };

    let mut seen = BTreeSet::new();
    raw_sources
        .into_iter()
        .filter(|source| seen.insert(source.clone()))
        .collect()
}

fn phase7_permission_smoke_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(1)
        .clamp(1, PHASE7_PERMISSION_SMOKE_MAX_ROWS)
}

fn classify_tushare_permission_error(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if error.contains("权限")
        || error.contains("积分")
        || lower.contains("permission")
        || lower.contains("privilege")
        || lower.contains("2002")
    {
        "permission_denied"
    } else if lower.contains("token") || error.contains("TUSHARE_TOKEN") {
        "auth_error"
    } else {
        "error"
    }
}

fn phase7_tushare_probe_json(
    source: &str,
    symbol: Option<&str>,
    scope: &str,
    result: quant_common::QuantResult<
        quant_data::model::tushare_dto::TushareResponse<Vec<serde_json::Value>>,
    >,
) -> Value {
    match result {
        Ok(response) => {
            let row_count = response
                .data
                .as_ref()
                .map(|data| data.items.len())
                .unwrap_or_default();
            let fields = response
                .data
                .as_ref()
                .map(|data| data.fields.clone())
                .unwrap_or_default();
            let sample_rows: Vec<Value> = response
                .data
                .as_ref()
                .map(|data| {
                    data.to_maps()
                        .into_iter()
                        .take(2)
                        .map(Value::Object)
                        .collect()
                })
                .unwrap_or_default();

            json!({
                "source": source,
                "scope": scope,
                "symbol": symbol,
                "status": if row_count > 0 { "ok" } else { "ok_empty" },
                "permission": "available",
                "row_count": row_count,
                "fields": fields,
                "sample_rows": sample_rows,
            })
        }
        Err(error) => {
            let message = error.to_string();
            json!({
                "source": source,
                "scope": scope,
                "symbol": symbol,
                "status": classify_tushare_permission_error(&message),
                "permission": "unknown_or_unavailable",
                "error": message,
            })
        }
    }
}

fn phase7_tushare_source_status(probes: &[Value]) -> &'static str {
    if probes.iter().any(|probe| {
        matches!(
            probe.get("status").and_then(|status| status.as_str()),
            Some("ok" | "ok_empty")
        )
    }) {
        "available"
    } else if probes.iter().any(|probe| {
        probe.get("status").and_then(|status| status.as_str()) == Some("permission_denied")
    }) {
        "permission_denied"
    } else {
        "error"
    }
}

fn phase7_coverage_grade(symbols: i64, reference_symbols: i64) -> &'static str {
    if symbols <= 0 {
        "missing"
    } else if reference_symbols <= 0 {
        "unknown_reference"
    } else {
        let ratio = symbols as f64 / reference_symbols as f64;
        if ratio >= 0.80 {
            "broad"
        } else if ratio >= 0.30 {
            "partial"
        } else {
            "undercovered"
        }
    }
}

fn phase7_coverage_json(
    name: String,
    rows: i64,
    min_date: Option<NaiveDate>,
    max_date: Option<NaiveDate>,
    symbols: i64,
    reference_symbols: i64,
) -> Value {
    json!({
        "name": name,
        "rows": rows,
        "symbols": symbols,
        "reference_symbols": reference_symbols,
        "symbol_coverage_ratio": if reference_symbols > 0 {
            Some(symbols as f64 / reference_symbols as f64)
        } else {
            None
        },
        "min_date": min_date,
        "max_date": max_date,
        "coverage_grade": phase7_coverage_grade(symbols, reference_symbols),
    })
}

fn phase7_coverage_rows_to_json(
    rows: Vec<(String, i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    reference_symbols: i64,
) -> Vec<Value> {
    rows.into_iter()
        .map(|(name, rows, min_date, max_date, symbols)| {
            phase7_coverage_json(name, rows, min_date, max_date, symbols, reference_symbols)
        })
        .collect()
}

async fn register_sync_task(
    state: &AppState,
    task_id: &str,
    req: &DataSyncTaskReq,
    status: &str,
) -> Result<(), String> {
    let start_date = parse_optional_date(req.start_date.as_deref())?;
    let end_date = parse_optional_date(req.end_date.as_deref())?;
    let symbols = match req.dataset.as_str() {
        "index_daily" if !req.index_codes.is_empty() => req.index_codes.as_slice(),
        "trade_cal" if !req.exchanges.is_empty() => req.exchanges.as_slice(),
        _ => req.symbols.as_slice(),
    };
    let symbols_opt = if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    };

    quant_data::repository::create_sync_task_with_context(
        &state.db,
        task_id,
        &req.dataset,
        &req.source,
        symbols_opt,
        start_date,
        end_date,
        status,
        req.retry_of_task_id.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())
}

fn require_range(req: &DataSyncTaskReq) -> Result<(&str, &str), String> {
    let start = req
        .start_date
        .as_deref()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = req
        .end_date
        .as_deref()
        .ok_or_else(|| "end_date is required".to_string())?;
    Ok((start, end))
}

async fn execute_sync_task(
    state: Arc<AppState>,
    task_id: String,
    req: DataSyncTaskReq,
) -> Result<serde_json::Value, String> {
    match req.dataset.as_str() {
        "stock_basic" => {
            let count = quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &task_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "daily" | "stock_daily" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_bars(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily", "status": "completed", "count": count}),
            )
        }
        "daily_basic" | "stock_daily_basic" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_basic(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily_basic", "status": "completed", "count": count}),
            )
        }
        "moneyflow" | "stock_moneyflow" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_moneyflow(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "moneyflow", "status": "completed", "count": count}),
            )
        }
        "forecast" | "stock_forecast" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_forecast(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "forecast", "status": "completed", "count": count}),
            )
        }
        "express" | "stock_express" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_express(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "express", "status": "completed", "count": count}),
            )
        }
        "disclosure_date" | "stock_disclosure_date" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_disclosure_date(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "disclosure_date", "status": "completed", "count": count}),
            )
        }
        "adj_factor" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for adj_factor sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_adj_factor(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "index_daily" => {
            let codes = if req.index_codes.is_empty() {
                &req.symbols
            } else {
                &req.index_codes
            };
            if codes.is_empty() {
                return Err("index_codes must not be empty for index_daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_index_daily(
                &state.db,
                &state.tushare,
                codes,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "trade_cal" => {
            let exchanges = if req.exchanges.is_empty() {
                vec!["SSE".to_string(), "SZSE".to_string()]
            } else {
                req.exchanges.clone()
            };
            let mut total = 0usize;
            for exchange in &exchanges {
                let child_task_id = format!("{}-{}", task_id, exchange.to_lowercase());
                total += quant_data::sync::sync_trade_calendar_with_task(
                    &state.db,
                    &state.tushare,
                    exchange,
                    &child_task_id,
                )
                .await
                .map_err(|e| e.to_string())?;
            }
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                total as i32,
                total as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": total}),
            )
        }
        "financial" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for financial sync".into());
            }
            let (statements, indicators) = quant_data::sync::sync_financial_data_with_task(
                &state.db,
                &state.tushare,
                &req.symbols,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(json!({
                "task_id": task_id,
                "dataset": req.dataset,
                "status": "completed",
                "statements": statements,
                "indicators": indicators
            }))
        }
        other => Err(format!("unsupported dataset: {}", other)),
    }
}

/// POST /api/v1/quant/data/sync-tasks
///
/// 统一数据同步任务入口，专用 sync 接口保留为兼容层。
pub async fn create_sync_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DataSyncTaskReq>,
) -> impl IntoResponse {
    let task_id = req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("data-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if req.background {
        let state_for_task = state.clone();
        let task_id_for_task = task_id.clone();
        let req_for_task = req.clone();
        tokio::spawn(async move {
            if let Err(message) = execute_sync_task(
                state_for_task.clone(),
                task_id_for_task.clone(),
                req_for_task,
            )
            .await
            {
                let _ = quant_data::repository::fail_sync_task(
                    &state_for_task.db,
                    &task_id_for_task,
                    &message,
                )
                .await;
                tracing::error!(task_id = %task_id_for_task, error = %message, "统一同步任务失败");
            }
        });

        return Json(json!({"code": 0, "data": {
            "task_id": task_id,
            "status": "running",
            "dataset": req.dataset,
            "mode": req.mode,
            "quality_check": req.quality_check,
            "create_data_version": req.create_data_version,
            "reason": req.reason
        }}));
    }

    match execute_sync_task(state, task_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

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
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "后台同步复权因子");

    let state = state.clone();
    let symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    tokio::spawn(async move {
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
    let bar_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_bar")
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
pub async fn phase7_feasibility_audit(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match build_phase7_feasibility_audit(&state).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/tushare/permission-smoke
pub async fn tushare_permission_smoke(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TusharePermissionSmokeReq>,
) -> impl IntoResponse {
    match build_tushare_permission_smoke(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

async fn build_tushare_permission_smoke(
    state: &AppState,
    req: TusharePermissionSmokeReq,
) -> Result<Value, String> {
    parse_optional_date(req.start_date.as_deref())?;
    parse_optional_date(req.end_date.as_deref())?;

    let sources = phase7_permission_smoke_sources(&req.sources);
    let symbols = resolve_phase7_permission_smoke_symbols(state, &req.symbols).await?;
    let row_limit = phase7_permission_smoke_limit(req.limit);
    let today = chrono::Utc::now().date_naive();
    let default_start = (today - Duration::days(365 * 3))
        .format("%Y%m%d")
        .to_string();
    let default_end = today.format("%Y%m%d").to_string();
    let start_date = req.start_date.unwrap_or(default_start);
    let end_date = req.end_date.unwrap_or(default_end);

    let mut source_results = Vec::new();
    for source in sources {
        source_results.push(
            run_tushare_permission_source_smoke(
                state,
                &source,
                &symbols,
                &start_date,
                &end_date,
                row_limit,
            )
            .await,
        );
    }

    Ok(json!({
        "audit_version": "phase7-fe-v1",
        "mode": "read_only_permission_smoke",
        "max_symbols": PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS,
        "row_limit_per_probe": row_limit,
        "sample_symbols": symbols,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "sources": source_results,
        "notes": [
            "This endpoint performs only small read-only Tushare API probes; it does not create tables or sync full-market data.",
            "cashflow and dividend are probed by sample ts_code; repurchase is probed by announcement date range because the Tushare repurchase API has no ts_code input parameter.",
            "Use this result to decide whether Phase 7-FE should proceed to Rust schema/repository/sync implementation or mark a source as blocked."
        ],
    }))
}

async fn resolve_phase7_permission_smoke_symbols(
    state: &AppState,
    requested: &[String],
) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let symbols: Vec<String> = requested
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .take(PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS)
        .collect();
    if !symbols.is_empty() {
        return Ok(symbols);
    }

    let preferred = sqlx::query_scalar::<_, String>(
        r#"
        SELECT symbol
        FROM market_stock
        WHERE list_status = 'L'
          AND symbol IN ('000001.SZ', '600000.SH', '000333.SZ')
        ORDER BY CASE symbol
            WHEN '000001.SZ' THEN 1
            WHEN '600000.SH' THEN 2
            WHEN '000333.SZ' THEN 3
            ELSE 99
        END
        LIMIT 3
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for symbol in preferred {
        if seen.insert(symbol.clone()) {
            resolved.push(symbol);
        }
    }

    if resolved.len() < PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS {
        let fallback = sqlx::query_scalar::<_, String>(
            r#"
            SELECT symbol
            FROM market_stock
            WHERE list_status = 'L'
            ORDER BY symbol
            LIMIT 3
            "#,
        )
        .fetch_all(&state.db)
        .await
        .map_err(|error| error.to_string())?;

        for symbol in fallback {
            if seen.insert(symbol.clone()) {
                resolved.push(symbol);
            }
            if resolved.len() >= PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS {
                break;
            }
        }
    }

    if resolved.is_empty() {
        Err("no sample symbols available for Tushare permission smoke".to_string())
    } else {
        Ok(resolved)
    }
}

async fn run_tushare_permission_source_smoke(
    state: &AppState,
    source: &str,
    symbols: &[String],
    start_date: &str,
    end_date: &str,
    row_limit: usize,
) -> Value {
    match source {
        "cashflow" => {
            let mut probes = Vec::new();
            for symbol in symbols {
                let result = state
                    .tushare
                    .cashflow(
                        symbol,
                        Some(start_date),
                        Some(end_date),
                        Some(row_limit),
                        Some(0),
                    )
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "symbol",
                    result,
                ));
            }
            json!({
                "source": source,
                "query_scope": "symbol",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
            })
        }
        "dividend" => {
            let mut probes = Vec::new();
            for symbol in symbols {
                let result = state
                    .tushare
                    .dividend(symbol, None, None, None, None, Some(row_limit), Some(0))
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "symbol",
                    result,
                ));
            }
            json!({
                "source": source,
                "query_scope": "symbol",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
            })
        }
        "repurchase" => {
            let result = state
                .tushare
                .repurchase(
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let probe = phase7_tushare_probe_json(source, None, "announcement_date_range", result);
            let probes = vec![probe];
            json!({
                "source": source,
                "query_scope": "announcement_date_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": false,
            })
        }
        unsupported => json!({
            "source": unsupported,
            "status": "unsupported_source",
            "supported_sources": PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS,
        }),
    }
}

async fn build_phase7_feasibility_audit(state: &AppState) -> Result<Value, String> {
    let listed_stock_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock")
        .fetch_one(&state.db)
        .await
        .map_err(|error| error.to_string())?;

    let market_rows = sqlx::query_as::<
        _,
        (String, i64, Option<NaiveDate>, Option<NaiveDate>, i64),
    >(
        r#"
        SELECT 'market_stock_daily_bar'::text, COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_daily_bar
        UNION ALL
        SELECT 'market_stock_daily_basic'::text, COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_daily_basic
        UNION ALL
        SELECT 'market_stock_moneyflow'::text, COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_moneyflow
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let financial_rows = sqlx::query_as::<
        _,
        (String, i64, Option<NaiveDate>, Option<NaiveDate>, i64),
    >(
        r#"
        SELECT 'market_financial_statement'::text, COUNT(*)::bigint, MIN(ann_date), MAX(ann_date), COUNT(DISTINCT ts_code)::bigint
        FROM market_financial_statement
        UNION ALL
        SELECT 'market_financial_indicator'::text, COUNT(*)::bigint, MIN(ann_date), MAX(ann_date), COUNT(DISTINCT ts_code)::bigint
        FROM market_financial_indicator
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let event_rows = sqlx::query_as::<
        _,
        (String, i64, Option<NaiveDate>, Option<NaiveDate>, i64),
    >(
        r#"
        SELECT 'market_stock_forecast'::text, COUNT(*)::bigint, MIN(ann_date), MAX(ann_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_forecast
        UNION ALL
        SELECT 'market_stock_express'::text, COUNT(*)::bigint, MIN(ann_date), MAX(ann_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_express
        UNION ALL
        SELECT 'market_stock_disclosure_date'::text, COUNT(*)::bigint, MIN(ann_date), MAX(ann_date), COUNT(DISTINCT symbol)::bigint
        FROM market_stock_disclosure_date
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let combo_rows = sqlx::query_as::<
        _,
        (String, i64, Option<NaiveDate>, Option<NaiveDate>, i64),
    >(
        r#"
        SELECT combo_name::text, COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
        FROM multi_factor_value
        WHERE combo_name IN (
            'phase7_financial_quality_v1',
            'phase7_valuation_v1',
            'phase7_moneyflow_v1',
            'phase7_industry_residual_quality_v1',
            'phase7_growth_recovery_v1',
            'phase7_quality_relative_strength_v1',
            'phase7_quality_event_window_overlay_v1',
            'phase7_event_surprise_v1',
            'phase7_event_window_earnings_v1',
            'phase7_quality_recovery_acceleration_v1'
        )
        GROUP BY combo_name
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let mut combo_by_name = BTreeMap::new();
    for value in phase7_coverage_rows_to_json(combo_rows, listed_stock_count) {
        if let Some(name) = value.get("name").and_then(|name| name.as_str()) {
            combo_by_name.insert(name.to_string(), value);
        }
    }
    let combo_coverage: Vec<Value> = PHASE7_FEASIBILITY_COMBOS
        .iter()
        .map(|name| {
            combo_by_name.get(*name).cloned().unwrap_or_else(|| {
                phase7_coverage_json((*name).to_string(), 0, None, None, 0, listed_stock_count)
            })
        })
        .collect();

    let available_optional_tables: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT table_name::text
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
              'market_stock_cashflow',
              'market_stock_dividend',
              'market_stock_repurchase'
          )
        ORDER BY table_name
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;
    let available_optional_tables: BTreeSet<String> =
        available_optional_tables.into_iter().collect();

    let optional_data_sources: Vec<Value> = [
        (
            "cashflow",
            "market_stock_cashflow",
            "permission_smoke_required",
        ),
        (
            "dividend",
            "market_stock_dividend",
            "permission_smoke_required",
        ),
        (
            "repurchase",
            "market_stock_repurchase",
            "permission_smoke_required",
        ),
    ]
    .into_iter()
    .map(|(source, table, next_step)| {
        json!({
            "source": source,
            "table": table,
            "table_exists": available_optional_tables.contains(table),
            "next_step": next_step,
        })
    })
    .collect();

    Ok(json!({
        "audit_version": "phase7-fd-v1",
        "listed_stock_count": listed_stock_count,
        "status": "needs_data_expansion_before_new_alpha_discovery",
        "market_coverage": phase7_coverage_rows_to_json(market_rows, listed_stock_count),
        "financial_coverage": phase7_coverage_rows_to_json(financial_rows, listed_stock_count),
        "event_coverage": phase7_coverage_rows_to_json(event_rows, listed_stock_count),
        "phase7_combo_coverage": combo_coverage,
        "optional_data_sources": optional_data_sources,
        "tushare_permission_notes": {
            "forecast": "2000-point interface is usable by symbol; full-market quarterly forecast_vip requires higher permission.",
            "express": "2000-point interface is usable by symbol; full-market quarterly express_vip requires higher permission.",
            "cashflow_dividend_repurchase": "Run 2-3 symbol permission smoke before schema or full sync."
        },
        "recommended_next_steps": [
            "Do not expand Phase 7-FB/FC narrow post-event return searches before data expansion.",
            "Run read-only permission smoke for cashflow, dividend, and repurchase on 2-3 symbols.",
            "Expand financial/fina_indicator coverage from the current narrow symbol set before treating financial quality as full-market alpha.",
            "Only after data coverage passes, build PIT feature factory and strict OOS/WFA automatic discovery profiles."
        ],
    }))
}

/// POST /api/v1/quant/data/sync/daily/background
///
/// 大批量后台同步日线行情，立即返回 task_id。
/// 通过 GET /api/v1/quant/data/sync/tasks/:task_id 查询进度。
pub async fn sync_daily_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "后台同步日线");

    let state = state.clone();
    let symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    tokio::spawn(async move {
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
    let row: Option<(
        String,
        String,
        Option<Vec<String>>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT task_type, status, symbols, start_date, end_date,
                    total_count, success_count, failed_count, progress,
                    last_heartbeat_at, heartbeat_timeout_seconds,
                    retry_of_task_id, error_message
             FROM data_sync_task WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    match row {
        Some((
            task_type,
            status,
            symbols,
            start_date,
            end_date,
            total,
            success,
            failed,
            progress,
            last_heartbeat_at,
            heartbeat_timeout_seconds,
            retry_of_task_id,
            error_message,
        )) => {
            let stale = match (last_heartbeat_at, heartbeat_timeout_seconds) {
                (Some(last), Some(timeout)) if status == "running" => {
                    chrono::Utc::now().signed_duration_since(last).num_seconds() > timeout as i64
                }
                _ => false,
            };
            Json(json!({"code": 0, "data": {
                "task_id": task_id,
                "task_type": task_type,
                "status": status,
                "symbols": symbols.unwrap_or_default(),
                "start_date": start_date.map(|date| date.to_string()),
                "end_date": end_date.map(|date| date.to_string()),
                "total": total,
                "success": success,
                "failed": failed,
                "progress": progress,
                "last_heartbeat_at": last_heartbeat_at.map(|ts| ts.to_rfc3339()),
                "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
                "stale": stale,
                "retry_of_task_id": retry_of_task_id,
                "error_message": error_message,
            }}))
        }
        None => Json(json!({"code": 1, "message": "task not found"})),
    }
}

fn sync_task_cancel_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" => Some("cancelled"),
        "running" => Some("cancel_requested"),
        "cancel_requested" => Some("cancel_requested"),
        _ => None,
    }
}

/// POST /api/v1/quant/data/sync-tasks/:task_id/cancel
///
/// 请求取消数据同步类后台任务。running 任务进入 cancel_requested，
/// 由 worker 在批次边界安全停止；pending 任务直接进入 cancelled。
pub async fn cancel_sync_task(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status
         FROM data_sync_task
         WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(status) = status else {
        return Json(json!({"code": 1, "message": "task not found"}));
    };

    let Some(next_status) = sync_task_cancel_transition(&status) else {
        return Json(json!({
            "code": 1,
            "message": format!("task cannot be cancelled from status {}", status),
            "data": {
                "task_id": task_id,
                "status": status,
            }
        }));
    };

    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = $2,
             error_message = COALESCE(error_message, 'cancel requested by user'),
             last_heartbeat_at = now(),
             completed_at = CASE WHEN $2 = 'cancelled' THEN now() ELSE completed_at END
         WHERE task_id = $1 AND status = $3",
    )
    .bind(&task_id)
    .bind(next_status)
    .bind(&status)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) if result.rows_affected() == 1 => Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "previous_status": status,
                "status": next_status,
            }
        })),
        Ok(_) => Json(json!({
            "code": 1,
            "message": "task status changed before cancel request was applied",
            "data": {
                "task_id": task_id,
                "previous_status": status,
            }
        })),
        Err(error) => Json(json!({
            "code": 1,
            "message": format!("Failed to cancel sync task: {}", error)
        })),
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
    match quant_data::sync::sync_financial_data(&state.db, &state.tushare, &req.symbols).await {
        Ok((stmt, ind)) => {
            Json(json!({"code": 0, "data": {"statements": stmt, "indicators": ind}}))
        }
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase7_coverage_grade_classifies_symbol_breadth() {
        assert_eq!(phase7_coverage_grade(0, 5_000), "missing");
        assert_eq!(phase7_coverage_grade(400, 5_000), "undercovered");
        assert_eq!(phase7_coverage_grade(2_000, 5_000), "partial");
        assert_eq!(phase7_coverage_grade(4_200, 5_000), "broad");
    }

    #[test]
    fn phase7_permission_smoke_sources_default_and_deduped() {
        assert_eq!(
            phase7_permission_smoke_sources(&[]),
            vec!["cashflow", "dividend", "repurchase"]
        );

        let requested = vec![
            " CashFlow ".to_string(),
            "dividend".to_string(),
            "cashflow".to_string(),
            "repurchase".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["cashflow", "dividend", "repurchase"]
        );
    }

    #[test]
    fn phase7_permission_smoke_limit_is_bounded() {
        assert_eq!(phase7_permission_smoke_limit(None), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(0)), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(3)), 3);
        assert_eq!(
            phase7_permission_smoke_limit(Some(100)),
            PHASE7_PERMISSION_SMOKE_MAX_ROWS
        );
    }

    #[test]
    fn tushare_permission_error_classifier_recognizes_permission_and_auth() {
        assert_eq!(
            classify_tushare_permission_error("API error (code=2002): 没有权限"),
            "permission_denied"
        );
        assert_eq!(
            classify_tushare_permission_error("Authentication error: TUSHARE_TOKEN 未设置"),
            "auth_error"
        );
        assert_eq!(classify_tushare_permission_error("timeout"), "error");
    }

    #[test]
    fn cancel_transition_matches_task_lifecycle() {
        assert_eq!(sync_task_cancel_transition("pending"), Some("cancelled"));
        assert_eq!(
            sync_task_cancel_transition("running"),
            Some("cancel_requested")
        );
        assert_eq!(
            sync_task_cancel_transition("cancel_requested"),
            Some("cancel_requested")
        );
        assert_eq!(sync_task_cancel_transition("completed"), None);
        assert_eq!(sync_task_cancel_transition("failed"), None);
        assert_eq!(sync_task_cancel_transition("cancelled"), None);
    }
}
