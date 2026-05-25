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
const PHASE7_OPTIONAL_SOURCE_SYNC_DEFAULT_SYMBOLS: usize = 20;
const PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS: usize = 200;
const PHASE7_OPTIONAL_SOURCE_BATCH_MAX_COUNT: usize = 10;
const PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_SIZE: usize = 50;
const PHASE7_COVERAGE_RUNNER_MAX_BATCH_SIZE: usize = 100;
const PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_COUNT: usize = 4;
const PHASE7_COVERAGE_RUNNER_MAX_BATCH_COUNT: usize = 10;
const PHASE7_COVERAGE_RUNNER_DEFAULT_PROFILE: &str = "local_mac_safe";
const PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES: &[&str] = &["cashflow", "dividend"];

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OptionalSourceCoverageSyncReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub max_symbols: Option<usize>,
    #[serde(default)]
    pub offset_symbols: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OptionalSourceCoverageBatchReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub start_offset: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7CoverageExpansionRunnerReq {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
    #[serde(default)]
    pub stop_when_readiness_at_least_partial: Option<bool>,
}

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

fn phase7_optional_source_sync_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(PHASE7_OPTIONAL_SOURCE_SYNC_DEFAULT_SYMBOLS)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS)
}

fn phase7_optional_source_sync_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

fn phase7_optional_source_batch_size(batch_size: Option<usize>) -> usize {
    phase7_optional_source_sync_limit(batch_size)
}

fn phase7_optional_source_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(1)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_BATCH_MAX_COUNT)
}

fn phase7_optional_source_batch_offsets(
    start_offset: usize,
    batch_size: usize,
    batch_count: usize,
) -> Vec<usize> {
    (0..batch_count)
        .map(|index| start_offset + index * batch_size)
        .collect()
}

fn phase7_optional_source_batch_next_offset(offsets: &[usize], batch_size: usize) -> usize {
    offsets
        .last()
        .map(|offset| offset + batch_size)
        .unwrap_or_default()
}

fn phase7_optional_source_batch_recommended_resume_offset(
    plan_only: bool,
    offsets: &[usize],
    batch_size: usize,
) -> usize {
    if plan_only {
        phase7_optional_source_batch_next_offset(offsets, batch_size)
    } else {
        0
    }
}

fn phase7_optional_source_batch_child_background(plan_only: bool) -> bool {
    !plan_only
}

fn phase7_coverage_runner_profile(profile: Option<&str>) -> String {
    profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_PROFILE)
        .to_string()
}

fn phase7_coverage_runner_batch_size(batch_size: Option<usize>) -> usize {
    batch_size
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_SIZE)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_SIZE)
}

fn phase7_coverage_runner_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_COUNT)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_COUNT)
}

fn phase7_coverage_runner_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

fn phase7_coverage_runner_sources(requested: &[String]) -> Result<Vec<String>, String> {
    let raw_sources: Vec<String> = if requested.is_empty() {
        PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES
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
    let mut sources = Vec::new();
    for source in raw_sources {
        if !PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES.contains(&source.as_str()) {
            return Err(format!(
                "unsupported coverage runner source: {}; supported sources are cashflow, dividend",
                source
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

fn phase7_coverage_runner_should_plan_source(readiness: &str) -> bool {
    !matches!(
        readiness,
        "partial_feature_candidate" | "ready_for_feature_factory"
    )
}

fn phase7_optional_source_sync_sources(requested: &[String]) -> Result<Vec<String>, String> {
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
    let mut sources = Vec::new();
    for source in raw_sources {
        if !PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&source.as_str()) {
            return Err(format!(
                "unsupported optional source: {}; supported sources are cashflow, dividend, repurchase",
                source
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

fn phase7_optional_source_table(source: &str) -> Option<&'static str> {
    match source {
        "cashflow" => Some("market_stock_cashflow"),
        "dividend" => Some("market_stock_dividend"),
        "repurchase" => Some("market_stock_repurchase"),
        _ => None,
    }
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

fn phase7_optional_source_readiness(
    table_exists: bool,
    rows: i64,
    symbols: i64,
    reference_symbols: i64,
) -> &'static str {
    if !table_exists {
        "schema_missing"
    } else if rows <= 0 {
        "needs_sync"
    } else {
        match phase7_coverage_grade(symbols, reference_symbols) {
            "broad" => "ready_for_feature_factory",
            "partial" => "partial_feature_candidate",
            "unknown_reference" => "coverage_reference_missing",
            _ => "sample_only_do_not_train",
        }
    }
}

fn phase7_optional_source_next_step(readiness: &str) -> &'static str {
    match readiness {
        "ready_for_feature_factory" => "build_pit_feature_factory",
        "partial_feature_candidate" => "run_feature_smoke_then_expand_coverage",
        "sample_only_do_not_train" => "expand_sync_before_training",
        "needs_sync" => "run_bounded_sync_smoke_then_coverage_audit",
        "schema_missing" => "apply_phase7_optional_financial_sources_schema",
        _ => "repair_reference_coverage_before_feature_factory",
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

fn phase7_optional_source_json(
    source: &str,
    table: &str,
    table_exists: bool,
    stats: Option<&(i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    reference_symbols: i64,
    next_feature: &str,
) -> Value {
    let (rows, min_date, max_date, symbols) = stats.copied().unwrap_or((0, None, None, 0));
    let readiness =
        phase7_optional_source_readiness(table_exists, rows, symbols, reference_symbols);
    let mut value = phase7_coverage_json(
        source.to_string(),
        rows,
        min_date,
        max_date,
        symbols,
        reference_symbols,
    );
    if let Value::Object(ref mut object) = value {
        object.insert("source".to_string(), json!(source));
        object.insert("table".to_string(), json!(table));
        object.insert("table_exists".to_string(), json!(table_exists));
        object.insert("feature_readiness".to_string(), json!(readiness));
        object.insert("next_feature".to_string(), json!(next_feature));
        object.insert(
            "next_step".to_string(),
            json!(phase7_optional_source_next_step(readiness)),
        );
    }
    value
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

fn optional_source_all_symbols_allowed(mode: Option<&str>) -> bool {
    mode == Some("full_market")
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
        "cashflow" | "stock_cashflow" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for cashflow sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_cashflow(
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
                json!({"task_id": task_id, "dataset": "cashflow", "status": "completed", "count": count}),
            )
        }
        "dividend" | "stock_dividend" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for dividend sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_dividend(
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
                json!({"task_id": task_id, "dataset": "dividend", "status": "completed", "count": count}),
            )
        }
        "repurchase" | "stock_repurchase" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for repurchase sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_repurchase(
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
                json!({"task_id": task_id, "dataset": "repurchase", "status": "completed", "count": count}),
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

/// POST /api/v1/quant/data/phase7-optional-source-coverage-sync
pub async fn phase7_optional_source_coverage_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OptionalSourceCoverageSyncReq>,
) -> impl IntoResponse {
    match build_phase7_optional_source_coverage_sync(state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/phase7-optional-source-coverage-batches
pub async fn phase7_optional_source_coverage_batches(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OptionalSourceCoverageBatchReq>,
) -> impl IntoResponse {
    match build_phase7_optional_source_coverage_batches(state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/phase7-coverage-expansion-runner
pub async fn phase7_coverage_expansion_runner(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7CoverageExpansionRunnerReq>,
) -> impl IntoResponse {
    match build_phase7_coverage_expansion_runner(state, req).await {
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

async fn build_phase7_optional_source_coverage_sync(
    state: Arc<AppState>,
    req: Phase7OptionalSourceCoverageSyncReq,
) -> Result<Value, String> {
    let start_date = req
        .start_date
        .clone()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end_date = req
        .end_date
        .clone()
        .ok_or_else(|| "end_date is required".to_string())?;
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let sources = phase7_optional_source_sync_sources(&req.sources)?;
    let max_symbols = phase7_optional_source_sync_limit(req.max_symbols);
    let offset_symbols = req.offset_symbols.unwrap_or_default();
    let plan_only = phase7_optional_source_sync_plan_only(req.plan_only);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-optional-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut source_results = Vec::new();
    for source in sources {
        let table = phase7_optional_source_table(&source)
            .ok_or_else(|| format!("unsupported optional source: {}", source))?;
        let selected_symbols = resolve_phase7_optional_source_sync_symbols(
            &state,
            &source,
            &req.symbols,
            max_symbols,
            offset_symbols,
        )
        .await?;
        if selected_symbols.is_empty() {
            source_results.push(json!({
                "source": source,
                "table": table,
                "status": "skipped_no_symbols",
                "selected_count": 0,
                "selected_symbols": selected_symbols,
            }));
            continue;
        }

        let task_id = format!("{}-{}", data_version_prefix, source);
        let sync_req = DataSyncTaskReq {
            dataset: source.clone(),
            source: "tushare".to_string(),
            mode: Some("bounded_symbols".to_string()),
            symbols: selected_symbols.clone(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            data_version_id: Some(task_id.clone()),
            background: req.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 optional source bounded coverage expansion".to_string()),
        };

        if plan_only {
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "planned",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
            }));
        } else if req.background {
            register_sync_task(&state, &task_id, &sync_req, "running").await?;
            let state_for_task = state.clone();
            let task_id_for_task = task_id.clone();
            let req_for_task = sync_req.clone();
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
                    tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 可选源 bounded 补数失败");
                }
            });
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "running",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
            }));
        } else {
            let execution = execute_sync_task(state.clone(), task_id.clone(), sync_req).await?;
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "completed",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
                "execution": execution,
            }));
        }
    }

    Ok(json!({
        "audit_version": "phase7-ff-bounded-sync-v1",
        "mode": if plan_only {
            "plan_only"
        } else if req.background {
            "background"
        } else {
            "synchronous"
        },
        "plan_only": plan_only,
        "max_symbols": max_symbols,
        "offset_symbols": offset_symbols,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "data_version_prefix": data_version_prefix,
        "sources": source_results,
        "notes": [
            "This endpoint never performs full-market sync through empty symbols; it always resolves a bounded symbol list first.",
            "plan_only defaults to true. Set plan_only=false only for bounded smoke or controlled background expansion.",
            "Run phase7-feasibility-audit after completion and do not build optional-source factors while readiness remains sample_only_do_not_train."
        ],
    }))
}

async fn build_phase7_optional_source_coverage_batches(
    state: Arc<AppState>,
    req: Phase7OptionalSourceCoverageBatchReq,
) -> Result<Value, String> {
    let start_date = req
        .start_date
        .clone()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end_date = req
        .end_date
        .clone()
        .ok_or_else(|| "end_date is required".to_string())?;
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let sources = phase7_optional_source_sync_sources(&req.sources)?;
    let batch_size = phase7_optional_source_batch_size(req.batch_size);
    let batch_count = phase7_optional_source_batch_count(req.batch_count);
    let start_offset = req.start_offset.unwrap_or_default();
    let plan_only = phase7_optional_source_sync_plan_only(req.plan_only);
    let child_background = phase7_optional_source_batch_child_background(plan_only);
    let offsets = phase7_optional_source_batch_offsets(start_offset, batch_size, batch_count);
    let planned_next_offset = phase7_optional_source_batch_next_offset(&offsets, batch_size);
    let recommended_resume_offset =
        phase7_optional_source_batch_recommended_resume_offset(plan_only, &offsets, batch_size);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-optional-batch-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut batches = Vec::new();
    for (batch_index, offset_symbols) in offsets.iter().copied().enumerate() {
        let batch_prefix = format!("{}-b{:03}", data_version_prefix, batch_index + 1);
        let child_req = Phase7OptionalSourceCoverageSyncReq {
            sources: sources.clone(),
            symbols: Vec::new(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            max_symbols: Some(batch_size),
            offset_symbols: Some(offset_symbols),
            plan_only: Some(plan_only),
            background: child_background,
            data_version_prefix: Some(batch_prefix.clone()),
        };
        let batch_result =
            build_phase7_optional_source_coverage_sync(state.clone(), child_req).await?;
        batches.push(json!({
            "batch_index": batch_index + 1,
            "offset_symbols": offset_symbols,
            "batch_size": batch_size,
            "data_version_prefix": batch_prefix,
            "status": if plan_only { "planned" } else { "launched" },
            "result": batch_result,
        }));
    }

    Ok(json!({
        "audit_version": "phase7-ff-bounded-batch-sync-v1",
        "mode": if plan_only { "plan_only" } else { "background" },
        "plan_only": plan_only,
        "child_background": child_background,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "start_offset": start_offset,
        "next_offset": recommended_resume_offset,
        "planned_next_offset": planned_next_offset,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "sources": sources,
        "data_version_prefix": data_version_prefix,
        "batches": batches,
        "notes": [
            "This endpoint orchestrates bounded optional-source sync batches only; it never trains or backfills factors.",
            "plan_only defaults to true. Set plan_only=false to launch child bounded sync tasks in background mode.",
            "For plan-only pagination, next_offset advances through the planned uncovered set.",
            "After a real execution, next_offset resets to 0 because the uncovered set changes as rows are written.",
            "Rerun phase7-feasibility-audit after tasks complete before feature training."
        ],
    }))
}

async fn build_phase7_coverage_expansion_runner(
    state: Arc<AppState>,
    req: Phase7CoverageExpansionRunnerReq,
) -> Result<Value, String> {
    let start_date = req
        .start_date
        .clone()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end_date = req
        .end_date
        .clone()
        .ok_or_else(|| "end_date is required".to_string())?;
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let profile = phase7_coverage_runner_profile(req.profile.as_deref());
    let requested_sources = phase7_coverage_runner_sources(&req.sources)?;
    let batch_size = phase7_coverage_runner_batch_size(req.batch_size);
    let batch_count = phase7_coverage_runner_batch_count(req.batch_count);
    let plan_only = phase7_coverage_runner_plan_only(req.plan_only);
    let stop_when_ready = req.stop_when_readiness_at_least_partial.unwrap_or(true);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-ff-coverage-runner-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let audit = build_phase7_feasibility_audit(&state).await?;
    let mut optional_readiness = BTreeMap::new();
    if let Some(sources) = audit
        .get("optional_data_sources")
        .and_then(|sources| sources.as_array())
    {
        for source in sources {
            if let (Some(name), Some(readiness)) = (
                source.get("source").and_then(|name| name.as_str()),
                source
                    .get("feature_readiness")
                    .and_then(|readiness| readiness.as_str()),
            ) {
                optional_readiness.insert(name.to_string(), readiness.to_string());
            }
        }
    }

    let planned_sources: Vec<String> = requested_sources
        .iter()
        .filter(|source| {
            if !stop_when_ready {
                return true;
            }
            optional_readiness
                .get(*source)
                .map(|readiness| phase7_coverage_runner_should_plan_source(readiness))
                .unwrap_or(true)
        })
        .cloned()
        .collect();

    let skipped_sources: Vec<Value> = requested_sources
        .iter()
        .filter(|source| !planned_sources.contains(source))
        .map(|source| {
            json!({
                "source": source,
                "reason": "readiness_at_least_partial",
                "feature_readiness": optional_readiness.get(source).cloned().unwrap_or_else(|| "unknown".to_string()),
            })
        })
        .collect();

    let batch_result = if planned_sources.is_empty() {
        None
    } else {
        let batch_req = Phase7OptionalSourceCoverageBatchReq {
            sources: planned_sources.clone(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            batch_size: Some(batch_size),
            batch_count: Some(batch_count),
            start_offset: Some(0),
            plan_only: Some(plan_only),
            data_version_prefix: Some(data_version_prefix.clone()),
        };
        Some(build_phase7_optional_source_coverage_batches(state, batch_req).await?)
    };

    Ok(json!({
        "audit_version": "phase7-ff-coverage-runner-v1",
        "profile": profile,
        "mode": if plan_only { "plan_only" } else { "background" },
        "plan_only": plan_only,
        "stop_when_readiness_at_least_partial": stop_when_ready,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "max_child_tasks": planned_sources.len() * batch_count,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "requested_sources": requested_sources,
        "planned_sources": planned_sources,
        "skipped_sources": skipped_sources,
        "optional_source_readiness": optional_readiness,
        "data_version_prefix": data_version_prefix,
        "batch_plan": batch_result,
        "notes": [
            "This runner orchestrates bounded optional-source coverage expansion only; it never builds features or runs strategy discovery.",
            "Default profile is local_mac_safe and defaults to plan_only=true.",
            "The runner only supports symbol-filtered optional sources; repurchase is excluded because the Tushare API is announcement-date-range based.",
            "Feature factories remain blocked while readiness is sample_only_do_not_train."
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

async fn resolve_phase7_optional_source_sync_symbols(
    state: &AppState,
    source: &str,
    requested: &[String],
    max_symbols: usize,
    offset_symbols: usize,
) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let symbols: Vec<String> = requested
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .take(max_symbols)
        .collect();
    if !symbols.is_empty() {
        return Ok(symbols);
    }

    let limit = max_symbols as i64;
    let offset = offset_symbols as i64;
    let table = phase7_optional_source_table(source)
        .ok_or_else(|| format!("unsupported optional source: {}", source))?;
    let table_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = $1
        )
        "#,
    )
    .bind(table)
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let uncovered = if table_exists {
        match source {
            "cashflow" => sqlx::query_scalar::<_, String>(
                r#"
                    SELECT stock.symbol
                    FROM market_stock stock
                    WHERE stock.list_status = 'L'
                      AND NOT EXISTS (
                          SELECT 1 FROM market_stock_cashflow data
                          WHERE data.symbol = stock.symbol
                      )
                    ORDER BY stock.symbol
                    OFFSET $1 LIMIT $2
                    "#,
            )
            .bind(offset)
            .bind(limit)
            .fetch_all(&state.db)
            .await
            .map_err(|error| error.to_string())?,
            "dividend" => sqlx::query_scalar::<_, String>(
                r#"
                    SELECT stock.symbol
                    FROM market_stock stock
                    WHERE stock.list_status = 'L'
                      AND NOT EXISTS (
                          SELECT 1 FROM market_stock_dividend data
                          WHERE data.symbol = stock.symbol
                      )
                    ORDER BY stock.symbol
                    OFFSET $1 LIMIT $2
                    "#,
            )
            .bind(offset)
            .bind(limit)
            .fetch_all(&state.db)
            .await
            .map_err(|error| error.to_string())?,
            "repurchase" => sqlx::query_scalar::<_, String>(
                r#"
                    SELECT stock.symbol
                    FROM market_stock stock
                    WHERE stock.list_status = 'L'
                      AND NOT EXISTS (
                          SELECT 1 FROM market_stock_repurchase data
                          WHERE data.symbol = stock.symbol
                      )
                    ORDER BY stock.symbol
                    OFFSET $1 LIMIT $2
                    "#,
            )
            .bind(offset)
            .bind(limit)
            .fetch_all(&state.db)
            .await
            .map_err(|error| error.to_string())?,
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    if !uncovered.is_empty() {
        return Ok(uncovered);
    }

    sqlx::query_scalar::<_, String>(
        r#"
        SELECT symbol
        FROM market_stock
        WHERE list_status = 'L'
        ORDER BY symbol
        OFFSET $1 LIMIT $2
        "#,
    )
    .bind(offset)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())
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

    let optional_source_specs = [
        (
            "cashflow",
            "market_stock_cashflow",
            "cashflow_quality_pit_features",
        ),
        (
            "dividend",
            "market_stock_dividend",
            "dividend_stability_quality_pit_features",
        ),
        (
            "repurchase",
            "market_stock_repurchase",
            "repurchase_event_capital_return_pit_features",
        ),
    ];
    let optional_query_parts: Vec<&str> = optional_source_specs
        .iter()
        .filter_map(|(_, table, _)| {
            if !available_optional_tables.contains(*table) {
                return None;
            }
            match *table {
                "market_stock_cashflow" => Some(
                    "SELECT 'cashflow'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_cashflow",
                ),
                "market_stock_dividend" => Some(
                    "SELECT 'dividend'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_dividend",
                ),
                "market_stock_repurchase" => Some(
                    "SELECT 'repurchase'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_repurchase",
                ),
                _ => None,
            }
        })
        .collect();
    let optional_rows: Vec<(String, i64, Option<NaiveDate>, Option<NaiveDate>, i64)> =
        if optional_query_parts.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as(&optional_query_parts.join("\nUNION ALL\n"))
                .fetch_all(&state.db)
                .await
                .map_err(|error| error.to_string())?
        };
    let mut optional_stats_by_source = BTreeMap::new();
    for (source, rows, min_date, max_date, symbols) in optional_rows {
        optional_stats_by_source.insert(source, (rows, min_date, max_date, symbols));
    }

    let optional_data_sources: Vec<Value> = optional_source_specs
        .into_iter()
        .map(|(source, table, next_feature)| {
            phase7_optional_source_json(
                source,
                table,
                available_optional_tables.contains(table),
                optional_stats_by_source.get(source),
                listed_stock_count,
                next_feature,
            )
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
            "cashflow_dividend_repurchase": "Current Pro 2000 permission passed bounded smoke; use optional_data_sources.feature_readiness before feature backfill or training."
        },
        "recommended_next_steps": [
            "Do not expand Phase 7-FB/FC narrow post-event return searches before data expansion.",
            "Expand optional source sync only through bounded, resumable Rust tasks; never use empty symbols without mode=full_market.",
            "Only build PIT feature factory for optional sources after coverage is at least partial_feature_candidate.",
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
    fn phase7_optional_source_readiness_separates_smoke_from_trainable_coverage() {
        assert_eq!(
            phase7_optional_source_readiness(false, 0, 0, 5_000),
            "schema_missing"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 0, 0, 5_000),
            "needs_sync"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 19, 3, 5_000),
            "sample_only_do_not_train"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 5_000, 2_000, 5_000),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 50_000, 4_200, 5_000),
            "ready_for_feature_factory"
        );
    }

    #[test]
    fn phase7_optional_source_sync_limit_is_bounded_for_local_runs() {
        assert_eq!(phase7_optional_source_sync_limit(None), 20);
        assert_eq!(phase7_optional_source_sync_limit(Some(0)), 1);
        assert_eq!(phase7_optional_source_sync_limit(Some(30)), 30);
        assert_eq!(
            phase7_optional_source_sync_limit(Some(10_000)),
            PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS
        );
    }

    #[test]
    fn phase7_optional_source_sync_sources_are_known_deduped_and_defaulted() {
        assert_eq!(
            phase7_optional_source_sync_sources(&[]).expect("default sources"),
            vec!["cashflow", "dividend", "repurchase"]
        );
        assert_eq!(
            phase7_optional_source_sync_sources(&[
                " CashFlow ".to_string(),
                "dividend".to_string(),
                "cashflow".to_string(),
            ])
            .expect("deduped sources"),
            vec!["cashflow", "dividend"]
        );
        assert!(phase7_optional_source_sync_sources(&["forecast".to_string()]).is_err());
    }

    #[test]
    fn phase7_optional_source_sync_plan_only_defaults_to_safe_mode() {
        assert!(phase7_optional_source_sync_plan_only(None));
        assert!(phase7_optional_source_sync_plan_only(Some(true)));
        assert!(!phase7_optional_source_sync_plan_only(Some(false)));
    }

    #[test]
    fn phase7_optional_source_batch_size_and_count_are_bounded() {
        assert_eq!(phase7_optional_source_batch_size(None), 20);
        assert_eq!(phase7_optional_source_batch_size(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_size(Some(30)), 30);
        assert_eq!(phase7_optional_source_batch_size(Some(10_000)), 200);

        assert_eq!(phase7_optional_source_batch_count(None), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(3)), 3);
        assert_eq!(phase7_optional_source_batch_count(Some(10_000)), 10);
    }

    #[test]
    fn phase7_optional_source_batch_offsets_are_resumable() {
        let offsets = phase7_optional_source_batch_offsets(5, 20, 3);
        assert_eq!(offsets, vec![5, 25, 45]);
        assert_eq!(phase7_optional_source_batch_next_offset(&offsets, 20), 65);
    }

    #[test]
    fn phase7_optional_source_batch_resume_offset_resets_after_execution() {
        let offsets = phase7_optional_source_batch_offsets(0, 20, 3);
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(true, &offsets, 20),
            60
        );
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(false, &offsets, 20),
            0
        );
    }

    #[test]
    fn phase7_optional_source_batch_launches_background_only_when_executing() {
        assert!(!phase7_optional_source_batch_child_background(true));
        assert!(phase7_optional_source_batch_child_background(false));
    }

    #[test]
    fn phase7_coverage_runner_profile_is_bounded_and_plan_only_by_default() {
        assert_eq!(phase7_coverage_runner_profile(None), "local_mac_safe");
        assert_eq!(phase7_coverage_runner_batch_size(None), 50);
        assert_eq!(phase7_coverage_runner_batch_size(Some(10_000)), 100);
        assert_eq!(phase7_coverage_runner_batch_count(None), 4);
        assert_eq!(phase7_coverage_runner_batch_count(Some(10_000)), 10);
        assert_eq!(phase7_coverage_runner_plan_only(None), true);
        assert_eq!(
            phase7_coverage_runner_sources(&[]).unwrap(),
            vec!["cashflow", "dividend"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
    }

    #[test]
    fn phase7_coverage_runner_stops_when_source_is_feature_ready() {
        assert!(phase7_coverage_runner_should_plan_source(
            "sample_only_do_not_train"
        ));
        assert!(phase7_coverage_runner_should_plan_source(
            "coverage_reference_missing"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "partial_feature_candidate"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "ready_for_feature_factory"
        ));
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
    fn optional_source_full_market_sync_requires_explicit_mode() {
        assert!(!optional_source_all_symbols_allowed(None));
        assert!(!optional_source_all_symbols_allowed(Some("")));
        assert!(!optional_source_all_symbols_allowed(Some("full")));
        assert!(optional_source_all_symbols_allowed(Some("full_market")));
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
