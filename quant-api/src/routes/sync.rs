/// 数据同步路由
use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Duration, NaiveDate};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
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
pub struct CleanupStaleSyncTasksReq {
    #[serde(default)]
    pub dry_run: bool,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
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

fn stale_cleanup_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(3600).clamp(60, 86_400)
}

fn stale_cleanup_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}

fn generated_data_version_id() -> String {
    chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
}

fn bounded_phase7_task_id(parts: &[&str]) -> String {
    const MAX_ID_LEN: usize = 64;
    let raw = parts
        .iter()
        .map(|part| {
            part.trim()
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                        ch
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        })
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if raw.len() <= MAX_ID_LEN {
        return raw;
    }

    let mut hasher = DefaultHasher::new();
    raw.hash(&mut hasher);
    let suffix = format!("-{:016x}", hasher.finish());
    let prefix_len = MAX_ID_LEN.saturating_sub(suffix.len());
    let prefix = raw
        .chars()
        .take(prefix_len)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string();
    format!("{}{}", prefix, suffix)
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
const PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES: &[&str] = &["cashflow", "dividend", "financial"];
const PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO: f64 = 1.0;
const PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO: f64 = 0.30;
const PHASE7_COVERAGE_RUNNER_DEFAULT_MAX_ROUNDS: usize = PHASE7_COVERAGE_RUNNER_MAX_ROUNDS;
const PHASE7_COVERAGE_RUNNER_MAX_ROUNDS: usize = 20;

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
    #[serde(default)]
    pub auto_continue: Option<bool>,
    #[serde(default)]
    pub max_rounds: Option<usize>,
    #[serde(default)]
    pub target_coverage_ratio: Option<f64>,
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

fn phase7_coverage_runner_auto_continue(auto_continue: Option<bool>) -> bool {
    auto_continue.unwrap_or(false)
}

fn phase7_coverage_runner_max_rounds(max_rounds: Option<usize>, auto_continue: bool) -> usize {
    if auto_continue {
        max_rounds
            .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_MAX_ROUNDS)
            .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_ROUNDS)
    } else {
        1
    }
}

fn phase7_coverage_runner_should_build_immediate_batches(
    plan_only: bool,
    auto_continue: bool,
) -> bool {
    plan_only || !auto_continue
}

fn phase7_coverage_autopilot_batch_offsets(batch_count: usize) -> Vec<usize> {
    vec![0; batch_count]
}

fn phase7_coverage_runner_target_ratio(target: Option<f64>) -> f64 {
    target
        .filter(|value| value.is_finite())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO)
        .clamp(
            PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO,
            PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO,
        )
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
                "unsupported coverage runner source: {}; supported sources are cashflow, dividend, financial",
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

fn phase7_coverage_runner_should_plan_source_for_target(
    readiness: Option<&str>,
    coverage_ratio: Option<f64>,
    target_ratio: f64,
    stop_at_partial_gate: bool,
) -> bool {
    if stop_at_partial_gate {
        return readiness
            .map(phase7_coverage_runner_should_plan_source)
            .unwrap_or(true);
    }
    let ratio = coverage_ratio.unwrap_or(0.0);
    ratio < target_ratio
}

fn phase7_attempt_coverage_readiness(
    source: &str,
    attempted_symbols: i64,
    reference_symbols: i64,
) -> &'static str {
    let grade = phase7_coverage_grade(attempted_symbols, reference_symbols);
    if source == "financial" {
        phase7_financial_source_readiness(grade)
    } else {
        match grade {
            "broad" => "ready_for_feature_factory",
            "partial" => "partial_feature_candidate",
            "unknown_reference" => "coverage_reference_missing",
            "missing" => "needs_sync",
            _ => "sample_only_do_not_train",
        }
    }
}

fn phase7_coverage_runner_source_state_for_window(
    audit: &Value,
    window_attempts: Option<&BTreeMap<String, i64>>,
) -> (BTreeMap<String, String>, BTreeMap<String, f64>) {
    let mut readiness_by_source = BTreeMap::new();
    let mut coverage_by_source = BTreeMap::new();
    let reference_symbols = audit
        .get("listed_stock_count")
        .and_then(|count| count.as_i64())
        .unwrap_or_default();
    if let Some(sources) = audit
        .get("optional_data_sources")
        .and_then(|sources| sources.as_array())
    {
        for source in sources {
            if let Some(name) = source.get("source").and_then(|name| name.as_str()) {
                if let Some(attempted_symbols) = window_attempts
                    .and_then(|attempts| attempts.get(name))
                    .copied()
                {
                    readiness_by_source.insert(
                        name.to_string(),
                        phase7_attempt_coverage_readiness(
                            name,
                            attempted_symbols,
                            reference_symbols,
                        )
                        .to_string(),
                    );
                    coverage_by_source.insert(
                        name.to_string(),
                        if reference_symbols > 0 {
                            attempted_symbols as f64 / reference_symbols as f64
                        } else {
                            0.0
                        },
                    );
                    continue;
                }
                if let Some(readiness) = source
                    .get("feature_readiness")
                    .and_then(|readiness| readiness.as_str())
                {
                    readiness_by_source.insert(name.to_string(), readiness.to_string());
                }
                if let Some(ratio) = source
                    .get("symbol_coverage_ratio")
                    .and_then(|ratio| ratio.as_f64())
                {
                    coverage_by_source.insert(name.to_string(), ratio);
                }
            }
        }
    }

    if let Some(rows) = audit
        .get("financial_coverage")
        .and_then(|sources| sources.as_array())
    {
        let mut financial_ready = "ready_for_feature_factory".to_string();
        let mut financial_ratio = 1.0_f64;
        let mut has_financial_rows = false;
        for row in rows {
            if let (Some(name), Some(coverage_grade)) = (
                row.get("name").and_then(|name| name.as_str()),
                row.get("coverage_grade").and_then(|grade| grade.as_str()),
            ) {
                if matches!(
                    name,
                    "market_financial_statement" | "market_financial_indicator"
                ) {
                    has_financial_rows = true;
                    if let Some(ratio) = row
                        .get("symbol_coverage_ratio")
                        .and_then(|ratio| ratio.as_f64())
                    {
                        financial_ratio = financial_ratio.min(ratio);
                    }
                    let readiness = phase7_financial_source_readiness(coverage_grade).to_string();
                    if phase7_coverage_runner_should_plan_source(&readiness) {
                        financial_ready = readiness;
                    }
                }
            }
        }
        if has_financial_rows {
            if let Some(attempted_symbols) = window_attempts
                .and_then(|attempts| attempts.get("financial"))
                .copied()
            {
                readiness_by_source.insert(
                    "financial".to_string(),
                    phase7_attempt_coverage_readiness(
                        "financial",
                        attempted_symbols,
                        reference_symbols,
                    )
                    .to_string(),
                );
                coverage_by_source.insert(
                    "financial".to_string(),
                    if reference_symbols > 0 {
                        attempted_symbols as f64 / reference_symbols as f64
                    } else {
                        0.0
                    },
                );
            } else {
                readiness_by_source.insert("financial".to_string(), financial_ready);
                coverage_by_source.insert("financial".to_string(), financial_ratio);
            }
        }
    }

    (readiness_by_source, coverage_by_source)
}

fn phase7_financial_source_readiness(coverage_grade: &str) -> &'static str {
    match coverage_grade {
        "broad" => "ready_for_feature_factory",
        "partial" => "partial_feature_candidate",
        "unknown_reference" => "coverage_reference_missing",
        "missing" => "needs_sync",
        _ => "undercovered_do_not_train",
    }
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
    attempted_symbols: i64,
    attempted_zero_row_symbols: i64,
    reference_symbols: i64,
    next_feature: &str,
) -> Value {
    let (rows, min_date, max_date, symbols) = stats.copied().unwrap_or((0, None, None, 0));
    let available_or_attempted_symbols = symbols.max(attempted_symbols);
    let readiness = phase7_optional_source_readiness(
        table_exists,
        rows,
        available_or_attempted_symbols,
        reference_symbols,
    );
    let mut value = phase7_coverage_json(
        source.to_string(),
        rows,
        min_date,
        max_date,
        available_or_attempted_symbols,
        reference_symbols,
    );
    if let Value::Object(ref mut object) = value {
        object.insert("source".to_string(), json!(source));
        object.insert("table".to_string(), json!(table));
        object.insert("table_exists".to_string(), json!(table_exists));
        object.insert("data_row_symbols".to_string(), json!(symbols));
        object.insert("attempted_symbols".to_string(), json!(attempted_symbols));
        object.insert(
            "attempted_zero_row_symbols".to_string(),
            json!(attempted_zero_row_symbols),
        );
        object.insert(
            "coverage_basis".to_string(),
            json!("data_rows_or_successful_attempts"),
        );
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
            start,
            end,
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

        let task_id = bounded_phase7_task_id(&[data_version_prefix.as_str(), source.as_str()]);
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
        let batch_label = format!("b{:03}", batch_index + 1);
        let batch_prefix =
            bounded_phase7_task_id(&[data_version_prefix.as_str(), batch_label.as_str()]);
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

async fn build_phase7_financial_coverage_batches(
    state: Arc<AppState>,
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    plan_only: bool,
    data_version_prefix: &str,
) -> Result<Value, String> {
    let start = parse_optional_date(Some(start_date))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end =
        parse_optional_date(Some(end_date))?.ok_or_else(|| "end_date is required".to_string())?;
    let child_background = phase7_optional_source_batch_child_background(plan_only);
    let offsets = phase7_optional_source_batch_offsets(0, batch_size, batch_count);
    let planned_next_offset = phase7_optional_source_batch_next_offset(&offsets, batch_size);
    let recommended_resume_offset =
        phase7_optional_source_batch_recommended_resume_offset(plan_only, &offsets, batch_size);

    let mut batches = Vec::new();
    for (batch_index, offset_symbols) in offsets.iter().copied().enumerate() {
        let symbols =
            resolve_phase7_financial_sync_symbols(&state, start, end, batch_size, offset_symbols)
                .await?;
        let batch_label = format!("b{:03}", batch_index + 1);
        let task_id =
            bounded_phase7_task_id(&[data_version_prefix, "financial", batch_label.as_str()]);
        let sync_req = DataSyncTaskReq {
            dataset: "financial".to_string(),
            source: "tushare".to_string(),
            mode: Some("bounded_symbols".to_string()),
            symbols: symbols.clone(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(start_date.to_string()),
            end_date: Some(end_date.to_string()),
            data_version_id: Some(task_id.clone()),
            background: child_background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 financial bounded coverage expansion".to_string()),
        };

        let status;
        let mut execution = None;
        if symbols.is_empty() {
            status = "skipped_no_symbols";
        } else if plan_only {
            status = "planned";
        } else if child_background {
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
                    tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 financial bounded 补数失败");
                }
            });
            status = "running";
        } else {
            execution = Some(execute_sync_task(state.clone(), task_id.clone(), sync_req).await?);
            status = "completed";
        }

        batches.push(json!({
            "batch_index": batch_index + 1,
            "offset_symbols": offset_symbols,
            "batch_size": batch_size,
            "task_id": task_id,
            "status": status,
            "selected_count": symbols.len(),
            "selected_symbols": symbols,
            "execution": execution,
        }));
    }

    Ok(json!({
        "audit_version": "phase7-ff-financial-bounded-batch-sync-v1",
        "mode": if plan_only { "plan_only" } else { "background" },
        "plan_only": plan_only,
        "child_background": child_background,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "next_offset": recommended_resume_offset,
        "planned_next_offset": planned_next_offset,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "source": "financial",
        "data_version_prefix": data_version_prefix,
        "batches": batches,
        "notes": [
            "This financial runner resolves a bounded stock list before syncing; it never uses an empty symbols full-market request.",
            "It reuses the existing financial dataset sync for market_financial_statement and market_financial_indicator.",
            "Run phase7-feasibility-audit after completion and keep financial alpha blocked until coverage reaches partial_feature_candidate."
        ],
    }))
}

async fn phase7_completed_attempts_by_source_for_window(
    state: &AppState,
    sources: &[String],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<BTreeMap<String, i64>, String> {
    let mut attempts = BTreeMap::new();
    if sources.is_empty() {
        return Ok(attempts);
    }
    for source in sources {
        attempts.insert(source.clone(), 0);
    }

    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT source::text, COUNT(DISTINCT symbol)::bigint
        FROM data_sync_attempt
        WHERE source = ANY($1)
          AND status = 'completed'
          AND start_date <= $2
          AND end_date >= $3
        GROUP BY source
        "#,
    )
    .bind(sources)
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    for (source, symbols) in rows {
        attempts.insert(source, symbols);
    }
    Ok(attempts)
}

fn build_phase7_bounded_sync_req(
    dataset: &str,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    task_id: &str,
    reason: &str,
) -> DataSyncTaskReq {
    DataSyncTaskReq {
        dataset: dataset.to_string(),
        source: "tushare".to_string(),
        mode: Some("bounded_symbols".to_string()),
        symbols,
        index_codes: Vec::new(),
        exchanges: Vec::new(),
        start_date: Some(start_date.to_string()),
        end_date: Some(end_date.to_string()),
        data_version_id: Some(task_id.to_string()),
        background: false,
        quality_check: false,
        create_data_version: true,
        retry_of_task_id: None,
        reason: Some(reason.to_string()),
    }
}

async fn run_phase7_autopilot_bounded_sync(
    state: Arc<AppState>,
    dataset: &str,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    task_id: String,
    reason: &str,
) -> Result<Value, String> {
    let sync_req =
        build_phase7_bounded_sync_req(dataset, symbols, start_date, end_date, &task_id, reason);
    execute_sync_task(state, task_id, sync_req).await
}

async fn run_phase7_autopilot_optional_round(
    state: Arc<AppState>,
    sources: &[String],
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    round_prefix: &str,
) -> Result<(), String> {
    for (batch_index, offset_symbols) in phase7_coverage_autopilot_batch_offsets(batch_count)
        .into_iter()
        .enumerate()
    {
        for source in sources {
            let symbols = resolve_phase7_optional_source_sync_symbols(
                &state,
                source,
                &[],
                parse_optional_date(Some(start_date))?
                    .ok_or_else(|| "start_date is required".to_string())?,
                parse_optional_date(Some(end_date))?
                    .ok_or_else(|| "end_date is required".to_string())?,
                batch_size,
                offset_symbols,
            )
            .await?;
            if symbols.is_empty() {
                tracing::info!(
                    source,
                    round_prefix,
                    "Phase 7 coverage autopilot optional source has no selected symbols"
                );
                continue;
            }
            let batch_label = format!("b{:03}", batch_index + 1);
            let task_id =
                bounded_phase7_task_id(&[round_prefix, source.as_str(), batch_label.as_str()]);
            run_phase7_autopilot_bounded_sync(
                state.clone(),
                source,
                symbols,
                start_date,
                end_date,
                task_id,
                "phase7 coverage autopilot bounded optional source expansion",
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_phase7_autopilot_financial_round(
    state: Arc<AppState>,
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    round_prefix: &str,
) -> Result<(), String> {
    for (batch_index, offset_symbols) in phase7_coverage_autopilot_batch_offsets(batch_count)
        .into_iter()
        .enumerate()
    {
        let symbols = resolve_phase7_financial_sync_symbols(
            &state,
            parse_optional_date(Some(start_date))?
                .ok_or_else(|| "start_date is required".to_string())?,
            parse_optional_date(Some(end_date))?
                .ok_or_else(|| "end_date is required".to_string())?,
            batch_size,
            offset_symbols,
        )
        .await?;
        if symbols.is_empty() {
            tracing::info!(
                round_prefix,
                "Phase 7 coverage autopilot financial source has no selected symbols"
            );
            continue;
        }
        let batch_label = format!("b{:03}", batch_index + 1);
        let task_id = bounded_phase7_task_id(&[round_prefix, "financial", batch_label.as_str()]);
        run_phase7_autopilot_bounded_sync(
            state.clone(),
            "financial",
            symbols,
            start_date,
            end_date,
            task_id,
            "phase7 coverage autopilot bounded financial expansion",
        )
        .await?;
    }
    Ok(())
}

async fn run_phase7_coverage_autopilot_background(
    state: Arc<AppState>,
    start_date: String,
    end_date: String,
    requested_sources: Vec<String>,
    batch_size: usize,
    batch_count: usize,
    max_rounds: usize,
    target_ratio: f64,
    data_version_prefix: String,
) {
    for round_index in 0..max_rounds {
        let round_audit = match build_phase7_feasibility_audit(&state).await {
            Ok(audit) => audit,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot audit failed");
                break;
            }
        };
        let start = match parse_optional_date(Some(&start_date))
            .and_then(|value| value.ok_or_else(|| "start_date is required".to_string()))
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot start date parse failed");
                break;
            }
        };
        let end = match parse_optional_date(Some(&end_date))
            .and_then(|value| value.ok_or_else(|| "end_date is required".to_string()))
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot end date parse failed");
                break;
            }
        };
        let window_attempts = match phase7_completed_attempts_by_source_for_window(
            &state,
            &requested_sources,
            start,
            end,
        )
        .await
        {
            Ok(attempts) => attempts,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot attempt audit failed");
                break;
            }
        };
        let (readiness, coverage) =
            phase7_coverage_runner_source_state_for_window(&round_audit, Some(&window_attempts));
        let planned_sources: Vec<String> = requested_sources
            .iter()
            .filter(|source| {
                phase7_coverage_runner_should_plan_source_for_target(
                    readiness.get(*source).map(String::as_str),
                    coverage.get(*source).copied(),
                    target_ratio,
                    false,
                )
            })
            .cloned()
            .collect();
        if planned_sources.is_empty() {
            tracing::info!(
                round = round_index + 1,
                target_ratio,
                "Phase 7 coverage autopilot reached target"
            );
            break;
        }

        let round_label = format!("r{:03}", round_index + 1);
        let round_prefix =
            bounded_phase7_task_id(&[data_version_prefix.as_str(), round_label.as_str()]);
        let optional_sources: Vec<String> = planned_sources
            .iter()
            .filter(|source| source.as_str() != "financial")
            .cloned()
            .collect();
        if !optional_sources.is_empty() {
            if let Err(error) = run_phase7_autopilot_optional_round(
                state.clone(),
                &optional_sources,
                &start_date,
                &end_date,
                batch_size,
                batch_count,
                &bounded_phase7_task_id(&[round_prefix.as_str(), "optional"]),
            )
            .await
            {
                tracing::error!(round = round_index + 1, error = %error, "Phase 7 optional coverage autopilot round failed");
                break;
            }
        }

        if planned_sources.iter().any(|source| source == "financial") {
            if let Err(error) = run_phase7_autopilot_financial_round(
                state.clone(),
                &start_date,
                &end_date,
                batch_size,
                batch_count,
                &bounded_phase7_task_id(&[round_prefix.as_str(), "financial"]),
            )
            .await
            {
                tracing::error!(round = round_index + 1, error = %error, "Phase 7 financial coverage autopilot round failed");
                break;
            }
        }
    }
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
    let auto_continue = phase7_coverage_runner_auto_continue(req.auto_continue);
    let max_rounds = phase7_coverage_runner_max_rounds(req.max_rounds, auto_continue);
    let target_ratio = phase7_coverage_runner_target_ratio(req.target_coverage_ratio);
    let stop_when_ready = req.stop_when_readiness_at_least_partial.unwrap_or(false);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-ff-coverage-runner-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let audit = build_phase7_feasibility_audit(&state).await?;
    let window_attempts =
        phase7_completed_attempts_by_source_for_window(&state, &requested_sources, start, end)
            .await?;
    let (source_readiness, source_coverage_ratio) =
        phase7_coverage_runner_source_state_for_window(&audit, Some(&window_attempts));

    let planned_sources: Vec<String> = requested_sources
        .iter()
        .filter(|source| {
            phase7_coverage_runner_should_plan_source_for_target(
                source_readiness.get(*source).map(String::as_str),
                source_coverage_ratio.get(*source).copied(),
                target_ratio,
                stop_when_ready,
            )
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
                "feature_readiness": source_readiness.get(source).cloned().unwrap_or_else(|| "unknown".to_string()),
            })
        })
        .collect();

    let optional_sources: Vec<String> = planned_sources
        .iter()
        .filter(|source| source.as_str() != "financial")
        .cloned()
        .collect();
    let build_immediate_batches =
        phase7_coverage_runner_should_build_immediate_batches(plan_only, auto_continue);
    let optional_batch_result = if optional_sources.is_empty() || !build_immediate_batches {
        None
    } else {
        let batch_req = Phase7OptionalSourceCoverageBatchReq {
            sources: optional_sources.clone(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            batch_size: Some(batch_size),
            batch_count: Some(batch_count),
            start_offset: Some(0),
            plan_only: Some(plan_only),
            data_version_prefix: Some(data_version_prefix.clone()),
        };
        Some(build_phase7_optional_source_coverage_batches(state.clone(), batch_req).await?)
    };
    let financial_batch_result =
        if planned_sources.iter().any(|source| source == "financial") && build_immediate_batches {
            Some(
                build_phase7_financial_coverage_batches(
                    state.clone(),
                    &start_date,
                    &end_date,
                    batch_size,
                    batch_count,
                    plan_only,
                    &data_version_prefix,
                )
                .await?,
            )
        } else {
            None
        };
    if auto_continue && !plan_only && !planned_sources.is_empty() {
        let state_for_task = state.clone();
        let start_date_for_task = start_date.clone();
        let end_date_for_task = end_date.clone();
        let requested_sources_for_task = requested_sources.clone();
        let data_version_prefix_for_task = data_version_prefix.clone();
        tokio::spawn(async move {
            run_phase7_coverage_autopilot_background(
                state_for_task,
                start_date_for_task,
                end_date_for_task,
                requested_sources_for_task,
                batch_size,
                batch_count,
                max_rounds,
                target_ratio,
                data_version_prefix_for_task,
            )
            .await;
        });
    }

    Ok(json!({
        "audit_version": "phase7-ff-coverage-runner-v1",
        "profile": profile,
        "mode": if plan_only { "plan_only" } else if auto_continue { "autopilot_background" } else { "background" },
        "plan_only": plan_only,
        "auto_continue": auto_continue,
        "max_rounds": max_rounds,
        "target_coverage_ratio": target_ratio,
        "partial_feature_candidate_gate_ratio": PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO,
        "coverage_target_policy": if stop_when_ready { "legacy_stop_at_partial_gate" } else { "full_available_coverage_by_default" },
        "stop_when_readiness_at_least_partial": stop_when_ready,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "max_child_tasks": planned_sources.len() * batch_count * max_rounds,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "requested_sources": requested_sources,
        "planned_sources": planned_sources,
        "skipped_sources": skipped_sources,
        "optional_source_readiness": source_readiness.clone(),
        "source_readiness": source_readiness,
        "source_coverage_ratio": source_coverage_ratio,
        "window_completed_attempts": window_attempts,
        "data_version_prefix": data_version_prefix,
        "optional_batch_plan": optional_batch_result,
        "financial_batch_plan": financial_batch_result,
        "notes": [
            "This runner orchestrates bounded coverage expansion only; it never builds features or runs strategy discovery.",
            "Default profile is local_mac_safe and defaults to plan_only=true.",
            "The runner supports symbol-filtered cashflow/dividend and bounded financial coverage sync; repurchase is excluded because the Tushare API is announcement-date-range based.",
            "The 30% partial_feature_candidate threshold is only a research unlock gate; the default target_coverage_ratio is 1.0 for full available coverage.",
            "When auto_continue=true and plan_only=false, one background autopilot task runs bounded child syncs synchronously round by round, then re-audits before selecting the next uncovered batch.",
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
    start: NaiveDate,
    end: NaiveDate,
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
        let sql = phase7_optional_source_uncovered_symbols_sql(source)
            .ok_or_else(|| format!("unsupported optional source: {}", source))?;
        sqlx::query_scalar::<_, String>(sql)
            .bind(start)
            .bind(end)
            .bind(offset)
            .bind(limit)
            .fetch_all(&state.db)
            .await
            .map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };

    if !uncovered.is_empty() {
        return Ok(uncovered);
    }
    if table_exists {
        return Ok(Vec::new());
    }

    Ok(Vec::new())
}

fn phase7_optional_source_uncovered_symbols_sql(source: &str) -> Option<&'static str> {
    match source {
        "cashflow" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'cashflow'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "dividend" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'dividend'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "repurchase" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'repurchase'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        _ => None,
    }
}

async fn resolve_phase7_financial_sync_symbols(
    state: &AppState,
    start: NaiveDate,
    end: NaiveDate,
    max_symbols: usize,
    offset_symbols: usize,
) -> Result<Vec<String>, String> {
    let limit = max_symbols as i64;
    let offset = offset_symbols as i64;
    let symbols = sqlx::query_scalar::<_, String>(phase7_financial_uncovered_symbols_sql())
        .bind(start)
        .bind(end)
        .bind(offset)
        .bind(limit)
        .fetch_all(&state.db)
        .await
        .map_err(|error| error.to_string())?;
    Ok(symbols)
}

fn phase7_financial_uncovered_symbols_sql() -> &'static str {
    r#"
    SELECT stock.symbol
    FROM market_stock stock
    WHERE stock.list_status = 'L'
      AND NOT EXISTS (
          SELECT 1 FROM data_sync_attempt attempt
          WHERE attempt.source = 'financial'
            AND attempt.symbol = stock.symbol
            AND attempt.status = 'completed'
            AND attempt.start_date <= $1
            AND attempt.end_date >= $2
      )
    ORDER BY stock.symbol
    OFFSET $3 LIMIT $4
    "#
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
        FROM market_stock_daily_bar_adj
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
    let sync_attempt_table_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'data_sync_attempt'
        )
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;

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
    let optional_attempt_rows: Vec<(String, i64, i64)> = if sync_attempt_table_exists {
        sqlx::query_as(
            r#"
            SELECT source::text,
                   COUNT(DISTINCT symbol)::bigint AS attempted_symbols,
                   COUNT(DISTINCT CASE WHEN row_count = 0 THEN symbol END)::bigint AS zero_row_symbols
            FROM data_sync_attempt
            WHERE source IN ('cashflow', 'dividend', 'repurchase')
              AND status = 'completed'
            GROUP BY source
            "#,
        )
        .fetch_all(&state.db)
        .await
        .map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };
    let mut optional_attempts_by_source = BTreeMap::new();
    for (source, attempted_symbols, zero_row_symbols) in optional_attempt_rows {
        optional_attempts_by_source.insert(source, (attempted_symbols, zero_row_symbols));
    }

    let optional_data_sources: Vec<Value> = optional_source_specs
        .into_iter()
        .map(|(source, table, next_feature)| {
            let (attempted_symbols, zero_row_symbols) = optional_attempts_by_source
                .get(source)
                .copied()
                .unwrap_or_default();
            phase7_optional_source_json(
                source,
                table,
                available_optional_tables.contains(table),
                optional_stats_by_source.get(source),
                attempted_symbols,
                zero_row_symbols,
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

/// POST /api/v1/quant/data/sync-tasks/cleanup-stale
///
/// 将 heartbeat 超时的 running 同步任务标记为 failed。默认 dry-run=false；
/// 可用 dry_run=true 先查看候选任务，避免误伤仍在正常推进的后台任务。
pub async fn cleanup_stale_sync_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleSyncTasksReq>,
) -> impl IntoResponse {
    let default_timeout_seconds =
        stale_cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = stale_cleanup_limit(req.limit);

    let rows: Vec<(
        String,
        String,
        String,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT task_id, task_type, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, started_at, created_at
         FROM data_sync_task
         WHERE status = 'running'
           AND COALESCE(last_heartbeat_at, started_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                task_type,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                started_at,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.or(*started_at).or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "task_id": task_id,
                    "task_type": task_type,
                    "status": status,
                    "progress": progress,
                    "last_heartbeat_at": last_heartbeat_at.map(|ts| ts.to_rfc3339()),
                    "started_at": started_at.map(|ts| ts.to_rfc3339()),
                    "created_at": created_at.map(|ts| ts.to_rfc3339()),
                    "observed_at": observed_at.map(|ts| ts.to_rfc3339()),
                    "heartbeat_timeout_seconds": timeout_seconds
                })
            },
        )
        .collect::<Vec<_>>();

    if req.dry_run || rows.is_empty() {
        return Json(json!({"code": 0, "data": {
            "dry_run": true,
            "candidate_count": candidates.len(),
            "updated_count": 0,
            "candidates": candidates
        }}));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = 'failed',
             failed_count = GREATEST(COALESCE(failed_count, 0), 1),
             completed_at = now(),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 'stale running task timed out by cleanup-stale: no heartbeat within configured timeout'
             )
         WHERE task_id = ANY($1) AND status = 'running'",
    )
    .bind(&task_ids)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) => Json(json!({"code": 0, "data": {
            "dry_run": false,
            "candidate_count": candidates.len(),
            "updated_count": result.rows_affected(),
            "candidates": candidates
        }})),
        Err(error) => Json(
            json!({"code": 1, "message": format!("cleanup stale sync tasks failed: {}", error)}),
        ),
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
    fn parse_health_date_accepts_html_and_compact_dates() {
        assert_eq!(
            parse_health_date("2026-05-31").expect("html date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert_eq!(
            parse_health_date("20260531").expect("compact date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert!(parse_health_date("2026/05/31").is_err());
    }

    #[test]
    fn coverage_level_requires_full_range_coverage() {
        assert_eq!(coverage_level(0, 0), "green");
        assert_eq!(coverage_level(10, 10), "green");
        assert_eq!(coverage_level(10, 9), "red");
    }

    #[test]
    fn data_readiness_gate_blocks_required_red_only() {
        let checks = vec![
            json!({"item": "A股日线", "level": "green", "required": true}),
            json!({"item": "涨跌停历史", "level": "red", "required": false}),
            json!({"item": "ML预测", "level": "red", "required": true}),
        ];

        let blocked = data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredRed);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("ML预测"));
    }

    #[test]
    fn data_readiness_strict_gate_blocks_required_yellow() {
        let checks = vec![
            json!({"item": "A股日线", "level": "yellow", "required": true}),
            json!({"item": "可观测性标记", "level": "yellow", "required": false}),
        ];

        let blocked =
            data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredYellow);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("A股日线"));
    }

    #[test]
    fn stale_cleanup_parameters_are_bounded() {
        assert_eq!(stale_cleanup_default_timeout_seconds(None), 3600);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(10)), 60);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(100_000)), 86_400);
        assert_eq!(stale_cleanup_limit(None), 100);
        assert_eq!(stale_cleanup_limit(Some(0)), 1);
        assert_eq!(stale_cleanup_limit(Some(10_000)), 1000);
    }

    #[test]
    fn event_sync_source_quality_requires_official_source_after_limit_api_earliest_date() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("tushare:limit_list_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:limit_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:limit_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_allows_pre_api_limit_derivation_only_before_earliest_date() {
        let pre_api_date = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:daily_limit"), pre_api_date),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:daily_limit"), pre_api_date),
            "green"
        );
    }

    #[test]
    fn event_sync_source_quality_flags_suspension_markers_as_unverified() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("tushare:suspend_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("derived:susp_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("suspension_daily", Some("derived:susp_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_accepts_daily_absence_suspension_derivation() {
        let date = NaiveDate::from_ymd_opt(2015, 7, 9).unwrap();

        assert_eq!(
            event_sync_source_quality(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            "green"
        );
    }

    #[test]
    fn parse_etf_symbols_defaults_when_strategy_field_is_empty() {
        assert_eq!(parse_etf_symbols(None), default_mvo_etfs());
        assert_eq!(
            parse_etf_symbols(Some(json!(["518880.SH", "", " 511010.SH "]))),
            vec!["518880.SH".to_string(), "511010.SH".to_string()]
        );
    }

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
            vec!["cashflow", "dividend", "financial"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
    }

    #[test]
    fn phase7_bounded_task_id_keeps_database_varchar64_contract() {
        let short = bounded_phase7_task_id(&["dv-phase7", "cashflow", "b001"]);
        assert_eq!(short, "dv-phase7-cashflow-b001");
        assert!(short.len() <= 64);

        let long = bounded_phase7_task_id(&[
            "dv-phase7-ff-full-auto-2016-20260526a",
            "r001",
            "optional",
            "cashflow",
            "b001",
        ]);
        assert!(long.len() <= 64, "task id length was {}", long.len());
        assert!(long.starts_with("dv-phase7-ff-full-auto-2016-20260526a-r001"));
        assert_ne!(
            long,
            "dv-phase7-ff-full-auto-2016-20260526a-r001-optional-cashflow-b001"
        );
    }

    #[test]
    fn phase7_coverage_runner_sources_include_bounded_financial_sync() {
        assert_eq!(
            phase7_coverage_runner_sources(&[
                " financial ".to_string(),
                "cashflow".to_string(),
                "financial".to_string(),
            ])
            .expect("financial source accepted"),
            vec!["financial", "cashflow"]
        );
    }

    #[test]
    fn phase7_coverage_runner_auto_continue_targets_full_coverage_with_bounds() {
        assert!(!phase7_coverage_runner_auto_continue(None));
        assert!(phase7_coverage_runner_auto_continue(Some(true)));

        assert_eq!(phase7_coverage_runner_max_rounds(None, false), 1);
        assert_eq!(
            phase7_coverage_runner_max_rounds(None, true),
            PHASE7_COVERAGE_RUNNER_MAX_ROUNDS
        );
        assert_eq!(phase7_coverage_runner_max_rounds(Some(0), true), 1);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(12), true), 12);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(1_000), true), 20);

        assert_eq!(phase7_coverage_runner_target_ratio(None), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.10)), 0.30);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.75)), 0.75);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(2.0)), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(f64::NAN)), 1.0);
    }

    #[test]
    fn phase7_coverage_runner_autopilot_uses_round_budget_not_manual_batches() {
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, true
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, false
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            false, false
        ));
        assert!(!phase7_coverage_runner_should_build_immediate_batches(
            false, true
        ));
        assert_eq!(phase7_coverage_autopilot_batch_offsets(3), vec![0, 0, 0]);
    }

    #[test]
    fn phase7_optional_source_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_optional_source_uncovered_symbols_sql("cashflow").expect("cashflow sql");

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'cashflow'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_financial_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_financial_uncovered_symbols_sql();

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'financial'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_coverage_runner_source_state_prefers_requested_window_attempts() {
        let audit = json!({
            "listed_stock_count": 100,
            "optional_data_sources": [
                {
                    "source": "cashflow",
                    "feature_readiness": "ready_for_feature_factory",
                    "symbol_coverage_ratio": 0.99
                }
            ],
            "financial_coverage": [
                {
                    "name": "market_financial_statement",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.98
                },
                {
                    "name": "market_financial_indicator",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.96
                }
            ]
        });
        let mut window_attempts = BTreeMap::new();
        window_attempts.insert("cashflow".to_string(), 25);
        window_attempts.insert("financial".to_string(), 40);

        let (_, coverage) =
            phase7_coverage_runner_source_state_for_window(&audit, Some(&window_attempts));

        assert_eq!(coverage.get("cashflow"), Some(&0.25));
        assert_eq!(coverage.get("financial"), Some(&0.40));
    }

    #[test]
    fn phase7_coverage_runner_partial_gate_does_not_block_full_coverage_target() {
        assert!(phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            0.30,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            true,
        ));
    }

    #[test]
    fn phase7_financial_source_readiness_maps_coverage_to_training_gate() {
        assert_eq!(phase7_financial_source_readiness("missing"), "needs_sync");
        assert_eq!(
            phase7_financial_source_readiness("thin"),
            "undercovered_do_not_train"
        );
        assert_eq!(
            phase7_financial_source_readiness("partial"),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_financial_source_readiness("broad"),
            "ready_for_feature_factory"
        );
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

/// POST /api/v1/quant/data/sync/fund-basic
///
/// 同步 ETF/LOF 基金基本信息（名称、类型、管理人）到 market_stock 表。
pub async fn sync_fund_basic(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match quant_data::sync::sync_fund_basic(&state.db, &state.tushare).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/namechange
///
/// 同步股票名称变更历史（ST 状态 PIT 合规数据）
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

/// 组件4: 账号依赖加工数据健康检查请求。
/// 无 start_date/end_date = 轻量新鲜度检查；带时间段 = 逐年深度红黄绿扫描。
#[derive(Debug, serde::Deserialize)]
pub struct AccountDataHealthReq {
    pub user_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]
struct StrategyHealthConfig {
    combo_name: String,
    equity_curve_task_id: String,
    prediction_set_id: Option<String>,
    etf_symbols: Vec<String>,
    signal_source: String,
    prediction_blend_weight: f64,
}

const DEFAULT_MVO_ETFS: &[&str] = &[
    "518880.SH",
    "511010.SH",
    "513500.SH",
    "513100.SH",
    "159980.SZ",
    "159985.SZ",
    "501018.SH",
];

fn proven_limit_list_earliest_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2019, 11, 28).expect("valid limit_list_d earliest date")
}

fn parse_health_date(value: &str) -> Result<NaiveDate, String> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .map_err(|_| format!("日期格式无效: {}，需要 YYYY-MM-DD 或 YYYYMMDD", value))
}

fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn default_mvo_etfs() -> Vec<String> {
    DEFAULT_MVO_ETFS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect()
}

fn parse_etf_symbols(value: Option<Value>) -> Vec<String> {
    let parsed = value
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(str::trim).map(str::to_string))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        default_mvo_etfs()
    } else {
        parsed
    }
}

fn coverage_level(expected: i64, actual: i64) -> &'static str {
    if expected <= 0 || actual >= expected {
        "green"
    } else {
        "red"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventSyncSourceQuality {
    Official,
    AcceptedDerived,
    UnverifiedDerived,
    Missing,
    Unknown,
}

fn event_sync_source_quality(
    task_type: &str,
    source: Option<&str>,
    trade_date: NaiveDate,
) -> EventSyncSourceQuality {
    let Some(source) = source.map(str::trim).filter(|source| !source.is_empty()) else {
        return EventSyncSourceQuality::Missing;
    };
    match task_type {
        "limit_daily" => {
            if source.starts_with("tushare:limit_list_d") {
                EventSyncSourceQuality::Official
            } else if source == "derived:daily_limit"
                && trade_date < proven_limit_list_earliest_date()
            {
                EventSyncSourceQuality::AcceptedDerived
            } else if source.starts_with("derived:") {
                EventSyncSourceQuality::UnverifiedDerived
            } else {
                EventSyncSourceQuality::Unknown
            }
        }
        "suspension_daily" => {
            if source.starts_with("tushare:suspend_d") {
                EventSyncSourceQuality::Official
            } else if source == "derived:daily_absence_suspension" {
                EventSyncSourceQuality::AcceptedDerived
            } else if source.starts_with("derived:") {
                EventSyncSourceQuality::UnverifiedDerived
            } else {
                EventSyncSourceQuality::Unknown
            }
        }
        _ => {
            if source.starts_with("tushare:") {
                EventSyncSourceQuality::Official
            } else if source.starts_with("derived:") {
                EventSyncSourceQuality::UnverifiedDerived
            } else {
                EventSyncSourceQuality::Unknown
            }
        }
    }
}

fn event_sync_source_level(
    task_type: &str,
    source: Option<&str>,
    trade_date: NaiveDate,
) -> &'static str {
    match event_sync_source_quality(task_type, source, trade_date) {
        EventSyncSourceQuality::Official | EventSyncSourceQuality::AcceptedDerived => "green",
        EventSyncSourceQuality::UnverifiedDerived | EventSyncSourceQuality::Unknown => "yellow",
        EventSyncSourceQuality::Missing => "red",
    }
}

fn event_sync_source_label(
    task_type: &str,
    source: Option<&str>,
    trade_date: NaiveDate,
) -> &'static str {
    match event_sync_source_quality(task_type, source, trade_date) {
        EventSyncSourceQuality::Official => "official",
        EventSyncSourceQuality::AcceptedDerived => "accepted_derived",
        EventSyncSourceQuality::UnverifiedDerived => "unverified_derived",
        EventSyncSourceQuality::Missing => "missing",
        EventSyncSourceQuality::Unknown => "unknown",
    }
}

fn lag_level(lag_days: i64, yellow_after_days: i64, red_after_days: i64) -> &'static str {
    if lag_days > red_after_days {
        "red"
    } else if lag_days > yellow_after_days {
        "yellow"
    } else {
        "green"
    }
}

fn check_item(
    account: &str,
    strategy: &str,
    item: impl Into<String>,
    level: &'static str,
    detail: impl Into<String>,
    fix_endpoint: Option<&str>,
    fix_params: Option<Value>,
    fix_reason: Option<String>,
) -> Value {
    let mut value = json!({
        "account": account,
        "strategy": strategy,
        "item": item.into(),
        "level": level,
        "detail": detail.into(),
        "required": true,
        "repairable": fix_endpoint.is_some(),
    });
    if let Some(endpoint) = fix_endpoint {
        value["fix_endpoint"] = json!(endpoint);
    }
    if let Some(params) = fix_params {
        value["fix_params"] = params;
    }
    if let Some(reason) = fix_reason {
        value["fix_reason"] = json!(reason);
    }
    value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataReadinessGate {
    BlockRequiredRed,
    BlockRequiredYellow,
}

fn data_readiness_required(check: &Value) -> bool {
    check
        .get("required")
        .and_then(|value| value.as_bool())
        .unwrap_or(true)
}

fn data_readiness_blocking_checks<'a>(
    checks: &'a [Value],
    gate: DataReadinessGate,
) -> Vec<&'a Value> {
    checks
        .iter()
        .filter(|check| data_readiness_required(check))
        .filter(|check| {
            let level = check.get("level").and_then(|value| value.as_str());
            match gate {
                DataReadinessGate::BlockRequiredRed => level == Some("red"),
                DataReadinessGate::BlockRequiredYellow => {
                    level == Some("red") || level == Some("yellow")
                }
            }
        })
        .collect()
}

fn data_readiness_failure_message(
    operation: &str,
    account_name: &str,
    strategy_id: &str,
    blocked: &[&Value],
) -> String {
    let summary = blocked
        .iter()
        .take(6)
        .map(|check| {
            let item = check
                .get("item")
                .and_then(|value| value.as_str())
                .unwrap_or("未知数据项");
            let level = check
                .get("level")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let detail = check
                .get("detail")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            format!("{}={}({})", item, level, detail)
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{} 数据门禁失败: account={} strategy={} blocking_items={}{}",
        operation,
        account_name,
        strategy_id,
        blocked.len(),
        if summary.is_empty() {
            String::new()
        } else {
            format!("; {}", summary)
        }
    )
}

async fn write_data_readiness_audit_event(
    db: &sqlx::PgPool,
    account_id: &str,
    operation: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, $2, 'paper_account', $3, 'system', $4, $5)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(format!("data_readiness.{}", status))
    .bind(account_id)
    .bind(format!("{} data readiness {}", operation, status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据门禁审计失败: {}", error))
}

async fn write_data_health_check_audit_event(
    db: &sqlx::PgPool,
    entity_id: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, 'data_readiness.checked', 'data_health_check', $2, 'system', $3, $4)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(entity_id)
    .bind(format!("account data health check {}", status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据健康检查审计失败: {}", error))
}

pub async fn check_paper_account_data_readiness(
    db: &sqlx::PgPool,
    account_id: &str,
    range: Option<(NaiveDate, NaiveDate)>,
    gate: DataReadinessGate,
    operation: &str,
) -> Result<Value, String> {
    let account: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id
         FROM paper_account WHERE paper_account_id=$1 AND status='active'",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询账号数据门禁: {}", e))?;
    let (account_id, account_name, strategy_id) =
        account.ok_or_else(|| format!("账号不存在或未激活: {}", account_id))?;
    let sid = strategy_id.unwrap_or_else(|| "v19".to_string());

    let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> = sqlx::query_as(
        "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                signal_source, prediction_blend_weight
         FROM strategy_config WHERE strategy_id=$1 AND status='active'",
    )
    .bind(&sid)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询策略数据门禁: {}", e))?;

    let checks = if let Some((
        combo_name,
        equity_curve_task_id,
        prediction_set_id,
        etf_symbols,
        signal_source,
        prediction_blend_weight,
    )) = cfg
    {
        let cfg = StrategyHealthConfig {
            combo_name,
            equity_curve_task_id,
            prediction_set_id,
            etf_symbols: parse_etf_symbols(etf_symbols),
            signal_source,
            prediction_blend_weight,
        };
        check_account_deps(db, &account_name, &sid, &cfg, range, true, true).await
    } else {
        vec![check_item(
            &account_name,
            &sid,
            "策略配置",
            "red",
            "strategy_config 未找到激活配置",
            None,
            None,
            Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
        )]
    };

    let red = checks
        .iter()
        .filter(|check| check["level"] == "red")
        .count();
    let yellow = checks
        .iter()
        .filter(|check| check["level"] == "yellow")
        .count();
    let blocked = data_readiness_blocking_checks(&checks, gate);
    let blocking_items = blocked.len();
    let failure_message = if blocking_items == 0 {
        None
    } else {
        Some(data_readiness_failure_message(
            operation,
            &account_name,
            &sid,
            &blocked,
        ))
    };
    let passed = blocking_items == 0;
    let report = json!({
        "operation": operation,
        "mode": if range.is_some() { "range_coverage" } else { "freshness" },
        "account_id": account_id,
        "account": account_name,
        "strategy": sid,
        "gate": match gate {
            DataReadinessGate::BlockRequiredRed => "block_required_red",
            DataReadinessGate::BlockRequiredYellow => "block_required_yellow",
        },
        "passed": passed,
        "red": red,
        "yellow": yellow,
        "blocking_items": blocking_items,
        "checks": checks,
    });

    let audit_status = if passed { "passed" } else { "failed" };
    if let Err(error) =
        write_data_readiness_audit_event(db, &account_id, operation, audit_status, &report).await
    {
        let base_message = failure_message
            .clone()
            .unwrap_or_else(|| format!("{} 数据门禁审计失败", operation));
        return Err(format!("{}; {}", base_message, error));
    }

    if passed {
        Ok(report)
    } else {
        Err(failure_message.unwrap_or_else(|| format!("{} 数据门禁失败", operation)))
    }
}

/// POST /api/v1/quant/data/account-data-health
/// 遍历激活账号(模拟+实盘) → 其策略依赖的加工数据(combo因子/PIT combo/权益曲线/滚动IC) → 红黄绿。
pub async fn account_data_health(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AccountDataHealthReq>,
) -> impl IntoResponse {
    let db = &state.db;
    let range = match (req.start_date.as_deref(), req.end_date.as_deref()) {
        (Some(start), Some(end)) => match (parse_health_date(start), parse_health_date(end)) {
            (Ok(start), Ok(end)) if start <= end => Some((start, end)),
            (Ok(_), Ok(_)) => {
                return Json(json!({"code": 1, "message": "start_date 不能晚于 end_date"}));
            }
            (Err(message), _) | (_, Err(message)) => {
                return Json(json!({"code": 1, "message": message}));
            }
        },
        _ => None,
    };
    let deep = range.is_some();
    let mut checks: Vec<serde_json::Value> = Vec::new();

    // 活跃账号(可选按 user 过滤)
    let accounts: Vec<(String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id, COALESCE(leverage_enabled,false)
         FROM paper_account WHERE status='active'
           AND ($1::text IS NULL OR user_id = $1)
         ORDER BY name",
    )
    .bind(req.user_id.as_deref())
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let mut cfg_cache: BTreeMap<String, Option<StrategyHealthConfig>> = BTreeMap::new();
    let mut deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut common_deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for (_acct_id, acct_name, strat_id, _lev) in &accounts {
        let sid = strat_id.as_deref().unwrap_or("v19");
        if !cfg_cache.contains_key(sid) {
            let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> =
                sqlx::query_as(
                    "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                        signal_source, prediction_blend_weight
                 FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                )
                .bind(sid)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
            cfg_cache.insert(
                sid.to_string(),
                cfg.map(
                    |(
                        combo_name,
                        equity_curve_task_id,
                        prediction_set_id,
                        etf_symbols,
                        signal_source,
                        prediction_blend_weight,
                    )| {
                        StrategyHealthConfig {
                            combo_name,
                            equity_curve_task_id,
                            prediction_set_id,
                            etf_symbols: parse_etf_symbols(etf_symbols),
                            signal_source,
                            prediction_blend_weight,
                        }
                    },
                ),
            );
        }

        let Some(Some(cfg)) = cfg_cache.get(sid) else {
            checks.push(check_item(
                acct_name,
                sid,
                "策略配置",
                "red",
                "strategy_config 未找到激活配置",
                None,
                None,
                Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
            ));
            continue;
        };

        let mut cached_checks = Vec::new();
        if let Some((start, end)) = range {
            let common_key = format!("{}|{}|{}", start, end, cfg.etf_symbols.join(","));
            if !common_deps_cache.contains_key(&common_key) {
                common_deps_cache.insert(
                    common_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, false)
                        .await,
                );
            }
            if let Some(common_checks) = common_deps_cache.get(&common_key) {
                cached_checks.extend(common_checks.iter().cloned());
            }

            let deps_key = format!(
                "{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or("")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, false, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        } else {
            let deps_key = format!(
                "{}|{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or(""),
                cfg.etf_symbols.join(",")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        }

        checks.extend(cached_checks.into_iter().map(|mut check| {
            check["account"] = json!(acct_name);
            check["strategy"] = json!(sid);
            if let Some(params) = check.get_mut("fix_params") {
                if params.get("strategy_id").and_then(|v| v.as_str()).is_some() {
                    params["strategy_id"] = json!(sid);
                }
            }
            check
        }));
    }

    let red = checks.iter().filter(|c| c["level"] == "red").count();
    let yellow = checks.iter().filter(|c| c["level"] == "yellow").count();
    let status = if red > 0 {
        "red"
    } else if yellow > 0 {
        "yellow"
    } else {
        "green"
    };
    let mut data = serde_json::json!({
        "mode": if deep {"range_coverage"} else {"freshness"},
        "accounts_checked": accounts.len(),
        "red": red,
        "yellow": yellow,
        "checks": checks
    });
    let audit_report = json!({
        "operation": "account_data_health",
        "status": status,
        "user_id": req.user_id,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "data": data.clone()
    });
    let audit_entity_id = audit_report
        .get("user_id")
        .and_then(|value| value.as_str())
        .unwrap_or("all_active_accounts");
    match write_data_health_check_audit_event(db, audit_entity_id, status, &audit_report).await {
        Ok(()) => data["audit_persisted"] = json!(true),
        Err(error) => {
            data["audit_persisted"] = json!(false);
            data["audit_error"] = json!(error);
        }
    }

    Json(serde_json::json!({
        "code": 0,
        "data": data
    }))
}

async fn latest_market_date(db: &sqlx::PgPool) -> NaiveDate {
    sqlx::query_scalar::<_, Option<NaiveDate>>(
        "SELECT GREATEST(
            (SELECT MAX(trade_date) FROM market_stock_daily_bar_adj),
            (SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH')
        )",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .flatten()
    .unwrap_or_else(|| chrono::Utc::now().date_naive())
}

fn strategy_needs_prediction(cfg: &StrategyHealthConfig) -> bool {
    matches!(
        cfg.signal_source.as_str(),
        "prediction" | "prediction_blend"
    ) && (cfg.signal_source == "prediction" || cfg.prediction_blend_weight > f64::EPSILON)
}

async fn resolve_live_prediction_set(
    db: &sqlx::PgPool,
    cfg: &StrategyHealthConfig,
    date: NaiveDate,
) -> Option<String> {
    if !strategy_needs_prediction(cfg) {
        return None;
    }
    if let Some(prediction_set_id) = cfg.prediction_set_id.as_ref() {
        return Some(prediction_set_id.clone());
    }
    sqlx::query_scalar(
        "SELECT ps.prediction_set_id FROM prediction_set ps
         WHERE ps.status = 'ready'
           AND ps.training_end_date IS NOT NULL
           AND ps.training_end_date < $1
           AND ps.start_date <= $1 AND ps.end_date >= $1
         ORDER BY ps.training_end_date DESC, ps.created_at DESC
         LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

async fn expected_open_day_count(db: &sqlx::PgPool, start: NaiveDate, end: NaiveDate) -> i64 {
    let calendar_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_trade_calendar
         WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true",
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0);
    if calendar_count > 0 {
        calendar_count
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .unwrap_or(0)
    }
}

async fn event_completion_latest_source(
    db: &sqlx::PgPool,
    table: &str,
    task_type: &str,
) -> (Option<NaiveDate>, Option<String>, i64) {
    let table_sql = format!(
        "SELECT MAX(trade_date), COUNT(*)::int8
         FROM {table}
         WHERE trade_date = (SELECT MAX(trade_date) FROM {table})"
    );
    let (table_date, table_rows): (Option<NaiveDate>, i64) = sqlx::query_as(&table_sql)
        .fetch_one(db)
        .await
        .unwrap_or((None, 0));

    let task_row: Option<(NaiveDate, Option<String>, i64)> = sqlx::query_as(
        "SELECT end_date, source, COALESCE(success_count, total_count, 0)::int8
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
         ORDER BY end_date DESC, completed_at DESC NULLS LAST
         LIMIT 1",
    )
    .bind(task_type)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    match (table_date, task_row) {
        (Some(td), Some((task_date, source, rows))) if task_date >= td => {
            (Some(task_date), source, rows)
        }
        (Some(td), _) => (Some(td), None, table_rows),
        (None, Some((task_date, source, rows))) => (Some(task_date), source, rows),
        (None, None) => (None, None, 0),
    }
}

async fn event_completed_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT end_date)::int8 FROM data_sync_task
         WHERE task_type = $1 AND status = 'completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0)
}

async fn event_verified_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    let rows: Vec<(NaiveDate, Option<String>)> = sqlx::query_as(
        "SELECT DISTINCT end_date, source
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    rows.into_iter()
        .filter(|(date, source)| {
            matches!(
                event_sync_source_quality(task_type, source.as_deref(), *date),
                EventSyncSourceQuality::Official | EventSyncSourceQuality::AcceptedDerived
            )
        })
        .map(|(date, _)| date)
        .collect::<std::collections::BTreeSet<_>>()
        .len() as i64
}

async fn rolling_pit_ic_quarter_coverage(
    db: &sqlx::PgPool,
    horizon: i16,
    start: NaiveDate,
    end: NaiveDate,
) -> (i64, i64, Option<NaiveDate>, Option<NaiveDate>) {
    sqlx::query_as(
        "WITH quarters AS (
           SELECT MIN(trade_date) AS as_of
           FROM (SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
                 WHERE trade_date >= $2 AND trade_date <= $3) d
           GROUP BY date_trunc('quarter', trade_date)
         ),
         covered AS (
           SELECT q.as_of
           FROM quarters q
           WHERE EXISTS (
             SELECT 1
             FROM factor_evaluation fe
             WHERE fe.horizon = $1
               AND fe.end_date <= q.as_of
               AND fe.mean_ic IS NOT NULL
               AND fe.ic_ir IS NOT NULL
               AND fe.factor_code !~ '^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)'
           )
         )
         SELECT COUNT(q.as_of)::int8,
                COUNT(c.as_of)::int8,
                MIN(q.as_of) FILTER (WHERE c.as_of IS NULL),
                MAX(q.as_of) FILTER (WHERE c.as_of IS NULL)
         FROM quarters q
         LEFT JOIN covered c USING (as_of)",
    )
    .bind(horizon)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or((0, 0, None, None))
}

/// 检查单个账号策略依赖的数据。range=None 查新鲜度；range=Some 查区间覆盖率。
async fn check_account_deps(
    db: &sqlx::PgPool,
    acct: &str,
    sid: &str,
    cfg: &StrategyHealthConfig,
    range: Option<(NaiveDate, NaiveDate)>,
    include_common: bool,
    include_strategy: bool,
) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let last_mkt = latest_market_date(db).await;
    let repair_start = yyyymmdd(last_mkt - Duration::days(30));
    let repair_end = yyyymmdd(last_mkt);

    if include_strategy {
        out.push(check_item(
            acct,
            sid,
            "策略配置",
            "green",
            format!(
                "combo={}, curve={}, signal_source={}, prediction_set={}, blend_weight={}, ETF{}只",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.signal_source,
                cfg.prediction_set_id.as_deref().unwrap_or("自动选择"),
                cfg.prediction_blend_weight,
                cfg.etf_symbols.len()
            ),
            None,
            None,
            None,
        ));
    }

    if let Some((start, end)) = range {
        let expected_days = expected_open_day_count(db, start, end).await;

        if include_common {
            let (a_expected_days, a_days, a_min_expected, a_min_symbols, a_weak_days): (
                i64,
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               LEFT JOIN market_stock_suspension susp
                 ON susp.symbol = s.symbol
                AND susp.trade_date = c.trade_date
                AND COALESCE(susp.suspend_type, 'S') = 'S'
               WHERE susp.symbol IS NULL
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_stock_daily_bar_adj
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_level = if a_days < a_expected_days || a_expected_days < expected_days {
                "red"
            } else if a_weak_days > 0 {
                "yellow"
            } else {
                "green"
            };
            let (a_fix_endpoint, a_fix_params) =
                if a_days < a_expected_days || a_expected_days < expected_days {
                    (
                        Some("/api/v1/quant/data/sync/daily/background"),
                        Some(json!({
                            "symbols": [],
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "data_version_id": format!("health-repair-daily-{}", yyyymmdd(end))
                        })),
                    )
                } else if a_weak_days > 0 {
                    (
                        Some("/api/v1/quant/data/sync/suspension/derive-from-daily"),
                        Some(json!({
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "force_tushare": false
                        })),
                    )
                } else {
                    (None, None)
                };
            out.push(check_item(
                acct,
                sid,
                "A股日线区间覆盖",
                a_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市/停牌口径单日最少{}/{}只，低于90%天数{}",
                    start, end, expected_days, a_days, a_min_symbols, a_min_expected, a_weak_days
                ),
                a_fix_endpoint,
                a_fix_params,
                None,
            ));

            let (
                a_adj_expected_days,
                a_adj_days,
                a_adj_min_expected,
                a_adj_min_symbols,
                a_adj_weak_days,
            ): (i64, i64, i64, i64, i64) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_adjustment_factor
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_adj_level =
                if a_adj_days < a_adj_expected_days || a_adj_expected_days < expected_days {
                    "red"
                } else if a_adj_weak_days > 0 {
                    "yellow"
                } else {
                    "green"
                };
            out.push(check_item(
                acct,
                sid,
                "A股复权因子区间覆盖",
                a_adj_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市口径单日最少{}/{}只，低于90%天数{}",
                    start,
                    end,
                    expected_days,
                    a_adj_days,
                    a_adj_min_symbols,
                    a_adj_min_expected,
                    a_adj_weak_days
                ),
                Some("/api/v1/quant/data/sync/adj-factor/background"),
                Some(json!({
                    "symbols": [],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let (etf_expected, etf_actual, etf_bad, etf_confirmed_absent): (
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
                 firsts AS (
                   SELECT symbol, MIN(trade_date) AS first_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                   GROUP BY symbol
                 ),
                 calendar AS (
                   SELECT DISTINCT trade_date
                   FROM market_trade_calendar
                   WHERE is_open = true
                     AND trade_date >= $2::date
                     AND trade_date <= $3::date
                 ),
                 expected AS (
                   SELECT s.symbol, c.trade_date
                   FROM symbols s
                   LEFT JOIN firsts f ON f.symbol = s.symbol
                   JOIN calendar c
                     ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
                 ),
                 actual AS (
                   SELECT DISTINCT symbol, trade_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                     AND trade_date >= $2 AND trade_date <= $3
                 ),
                 confirmed_absent AS (
                   SELECT e.symbol, e.trade_date
                   FROM expected e
                   JOIN data_sync_attempt attempt
                     ON attempt.source = 'fund_daily'
                    AND attempt.symbol = e.symbol
                    AND attempt.status = 'completed'
                    AND attempt.row_count = 0
                    AND attempt.start_date = e.trade_date
                    AND attempt.end_date = e.trade_date
                 )
                 SELECT COUNT(*)::int8,
                        COUNT(a.trade_date)::int8,
                        COUNT(DISTINCT e.symbol) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NULL
                        )::int8,
                        COUNT(*) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NOT NULL
                        )::int8
                 FROM expected e
                 LEFT JOIN actual a ON a.symbol = e.symbol AND a.trade_date = e.trade_date
                 LEFT JOIN confirmed_absent ca ON ca.symbol = e.symbol AND ca.trade_date = e.trade_date",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0));
            let etf_missing = (etf_expected - etf_actual).max(0);
            let etf_level = if etf_bad == 0 {
                "green"
            } else if etf_missing <= 5 {
                "yellow"
            } else {
                "red"
            };
            let (etf_fix_endpoint, etf_fix_params, etf_fix_reason) = if etf_level == "red" {
                (
                    Some("/api/v1/quant/data/sync/fund-daily"),
                    Some(json!({
                        "symbols": cfg.etf_symbols,
                        "start_date": yyyymmdd(start),
                        "end_date": yyyymmdd(end),
                        "data_version_id": format!("health-repair-etf-daily-{}", yyyymmdd(end))
                    })),
                    None,
                )
            } else if etf_level == "yellow" {
                (
                    None,
                    None,
                    Some(
                        "少量 ETF/QDII 日线缺口通常来自基金非交易日或上游空值；已尝试同步仍为空时不应静默补假价格"
                            .to_string(),
                    ),
                )
            } else {
                (None, None, None)
            };
            out.push(check_item(
                acct,
                sid,
                "ETF日线区间覆盖(MVO)",
                etf_level,
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口{}个 symbol-day/{}只ETF，其中{}个 symbol-day 已确认源端无行情",
                    cfg.etf_symbols.len(),
                    etf_expected,
                    etf_actual,
                    etf_missing,
                    etf_bad,
                    etf_confirmed_absent
                ),
                etf_fix_endpoint,
                etf_fix_params,
                etf_fix_reason,
            ));

            let (etf_adj_expected, etf_adj_actual, etf_adj_bad): (i64, i64, i64) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
             firsts AS (
               SELECT symbol, MIN(trade_date) AS first_date
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
               GROUP BY symbol
             ),
             calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true
                 AND trade_date >= $2::date
                 AND trade_date <= $3::date
             ),
             expected AS (
               SELECT s.symbol, COUNT(DISTINCT c.trade_date)::int8 AS expected_days
               FROM symbols s
               LEFT JOIN firsts f ON f.symbol = s.symbol
               LEFT JOIN calendar c
                 ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
               GROUP BY s.symbol
             ),
             actual AS (
               SELECT symbol, COUNT(DISTINCT trade_date)::int8 AS actual_days
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
                 AND trade_date >= $2 AND trade_date <= $3
               GROUP BY symbol
             )
             SELECT COALESCE(SUM(e.expected_days), 0)::int8,
                    COALESCE(SUM(COALESCE(a.actual_days, 0)), 0)::int8,
                    COUNT(*) FILTER (WHERE COALESCE(a.actual_days, 0) < e.expected_days)::int8
             FROM expected e
             LEFT JOIN actual a ON a.symbol = e.symbol",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0));
            out.push(check_item(
                acct,
                sid,
                "ETF复权因子区间覆盖(MVO)",
                if etf_adj_bad == 0 { "green" } else { "red" },
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口ETF数{}",
                    cfg.etf_symbols.len(),
                    etf_adj_expected,
                    etf_adj_actual,
                    etf_adj_bad
                ),
                Some("/api/v1/quant/data/sync/fund-adj"),
                Some(json!({
                    "symbols": cfg.etf_symbols,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-etf-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let stock_basic_days: i64 = sqlx::query_scalar(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             )
             SELECT COUNT(*)::int8 FROM expected WHERE expected_symbols > 0",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
            acct,
            sid,
            "股票基础信息区间覆盖",
            coverage_level(expected_days, stock_basic_days),
            format!(
                "market_stock 按 list_date/delist_date 可覆盖{}个交易日，期望{}天",
                stock_basic_days, expected_days
            ),
            Some("/api/v1/quant/data/sync/stock-basic"),
            Some(
                json!({"data_version_id": format!("health-repair-stock-basic-{}", yyyymmdd(end))}),
            ),
            None,
        ));

            let csi_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "CSI300区间覆盖",
                coverage_level(expected_days, csi_days),
                format!("期望{}个交易日，覆盖{}天", expected_days, csi_days),
                Some("/api/v1/quant/data/sync/index-daily"),
                Some(json!({
                    "index_codes": ["000300.SH"],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-csi300-{}", yyyymmdd(end))
                })),
                None,
            ));
        }

        if include_strategy {
            let (ic_quarters, ic_covered, ic_missing_min, ic_missing_max) =
                rolling_pit_ic_quarter_coverage(db, 20, start, end).await;
            let ic_level = coverage_level(ic_quarters, ic_covered);
            out.push(check_item(
                acct,
                sid,
                "滚动IC区间覆盖(PIT权重)",
                ic_level,
                format!(
                    "{}~{} 期望{}个季度as-of，PIT IC可用{}个；缺失as-of范围{}~{}",
                    start,
                    end,
                    ic_quarters,
                    ic_covered,
                    ic_missing_min
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string()),
                    ic_missing_max
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string())
                ),
                Some("/api/v1/quant/factors/evaluate-rolling-pit/background"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "version": "1.0.0",
                    "horizon": 20,
                    "train_lookback_days": 756
                })),
                None,
            ));

            let combo_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM multi_factor_value
             WHERE combo_name=$1 AND version='1.0.0'
               AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            let combo_future_leak: bool = sqlx::query_scalar(
                "SELECT EXISTS(
               SELECT 1 FROM multi_factor_value
               WHERE combo_name=$1 AND version='1.0.0'
                 AND trade_date >= $2 AND trade_date <= $3
                 AND available_at > trade_date
               LIMIT 1
             )",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(false);
            let combo_level = if combo_future_leak {
                "red"
            } else {
                coverage_level(expected_days, combo_days)
            };
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo区间覆盖({})", cfg.combo_name),
                combo_level,
                format!(
                    "期望{}个交易日，PIT覆盖{}天，future available_at={}",
                    expected_days, combo_days, combo_future_leak
                ),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({
                    "combo_name": cfg.combo_name,
                    "version": "1.0.0",
                    "horizon": 20,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            let curve_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM backtest_equity_curve
             WHERE task_id=$1 AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.equity_curve_task_id)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线区间覆盖",
                coverage_level(expected_days, curve_days),
                format!(
                    "期望{}个交易日，覆盖{}天，task={}",
                    expected_days, curve_days, cfg.equity_curve_task_id
                ),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({
                    "name": "权益曲线",
                    "strategy_id": sid,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            if strategy_needs_prediction(cfg) && cfg.prediction_set_id.is_none() {
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    "red",
                    format!(
                        "strategy signal_source={} 需要 ML，但未固定 prediction_set_id；历史回放不可复现",
                        cfg.signal_source
                    ),
                    None,
                    None,
                    Some(
                        "历史回放必须绑定 PIT prediction_set_id；不能用运行时最新预测集替代"
                            .to_string(),
                    ),
                ));
            } else if let Some(pred_set) = cfg
                .prediction_set_id
                .as_deref()
                .filter(|_| strategy_needs_prediction(cfg))
            {
                let ps_ready: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $3
                   AND training_end_date IS NOT NULL
                   AND training_end_date < $2
                 )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                let ml_days: i64 = sqlx::query_scalar(
                    "SELECT COUNT(DISTINCT trade_date)::int8 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND COALESCE(available_at, trade_date) <= trade_date",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(0);
                let ml_future_leak: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND available_at > trade_date
                 LIMIT 1
                )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    if !ps_ready || ml_future_leak {
                        "red"
                    } else {
                        coverage_level(expected_days, ml_days)
                    },
                    format!(
                        "期望{}个交易日，覆盖{}天，prediction_set_ready={}，future available_at={}，set={}",
                        expected_days, ml_days, ps_ready, ml_future_leak, pred_set
                    ),
                    None,
                    None,
                    Some(
                        "预测集由训练/预测流水线生成，不能用单点补数安全修复；需重建 PIT 预测集"
                            .to_string(),
                    ),
                ));
            }
        }

        if include_common {
            let suspension_completed =
                event_completed_day_count(db, "suspension_daily", start, end).await;
            let suspension_verified =
                event_verified_day_count(db, "suspension_daily", start, end).await;
            out.push(check_item(
                acct,
                sid,
                "停牌同步完成标记",
                coverage_level(expected_days, suspension_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；事件表空行和 derived 标记不能证明零停牌",
                    expected_days, suspension_verified, suspension_completed
                ),
                Some("/api/v1/quant/data/sync/suspension/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let limit_earliest = proven_limit_list_earliest_date();
            let limit_completed = event_completed_day_count(db, "limit_daily", start, end).await;
            let limit_verified = event_verified_day_count(db, "limit_daily", start, end).await;
            let limit_missing_source_note = if start < limit_earliest {
                format!(
                    "；{} 前 Tushare 不提供 limit_list_d，系统需用日线 close/pre_close 按交易规则派生",
                    limit_earliest
                )
            } else {
                String::new()
            };
            out.push(check_item(
                acct,
                sid,
                "涨跌停同步完成标记",
                coverage_level(expected_days, limit_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；2019-11-28后必须有 Tushare 来源，事件表空行不能证明零涨跌停{}",
                    expected_days, limit_verified, limit_completed, limit_missing_source_note
                ),
                Some("/api/v1/quant/data/sync/limit/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
            out.push(check_item(
                acct,
                sid,
                "数据同步调度任务",
                if scheduler_issues.is_empty() {
                    "green"
                } else {
                    "red"
                },
                if scheduler_issues.is_empty() {
                    "启用调度任务 CRON/依赖顺序检查通过".to_string()
                } else {
                    format!(
                        "调度任务问题{}项: {}",
                        scheduler_issues.len(),
                        scheduler_issues.join("；")
                    )
                },
                None,
                None,
                if scheduler_issues.is_empty() {
                    None
                } else {
                    Some(
                        "请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效"
                            .to_string(),
                    )
                },
            ));

            let st_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::int8 FROM market_stock_name_history
             WHERE start_date <= $1 AND (end_date IS NULL OR end_date >= $2)",
            )
            .bind(end)
            .bind(start)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "ST/名称历史",
                if st_count > 0 { "green" } else { "red" },
                format!("区间相交名称历史/ST记录{}条", st_count),
                Some("/api/v1/quant/data/sync/namechange"),
                Some(json!({})),
                None,
            ));
        }

        return out;
    }

    let active_stock_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::int8
         FROM market_stock s
         LEFT JOIN market_stock_suspension susp
           ON susp.symbol = s.symbol
          AND susp.trade_date = $1
          AND COALESCE(susp.suspend_type, 'S') = 'S'
         WHERE s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
           AND s.list_date IS NOT NULL
           AND s.list_date <= $1
           AND (s.delist_date IS NULL OR s.delist_date >= $1)
           AND susp.symbol IS NULL",
    )
    .bind(last_mkt)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    let (a_last, a_symbols): (Option<NaiveDate>, i64) = sqlx::query_as(
        "WITH latest AS (
           SELECT MAX(trade_date) AS trade_date FROM market_stock_daily_bar_adj
           WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         )
         SELECT latest.trade_date,
                COUNT(DISTINCT b.symbol)::int8
         FROM latest
         LEFT JOIN market_stock_daily_bar_adj b
           ON b.trade_date = latest.trade_date
          AND b.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         GROUP BY latest.trade_date",
    )
    .fetch_one(db)
    .await
    .unwrap_or((None, 0));
    let a_lag = a_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let a_partial = active_stock_count > 0 && a_symbols * 100 < active_stock_count * 80;
    out.push(check_item(
        acct,
        sid,
        "A股日线",
        if a_partial {
            "red"
        } else {
            lag_level(a_lag, 2, 7)
        },
        format!(
            "最新{}，落后{}天，最新日{}只/按上市退市停牌口径应有{}只",
            a_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            a_lag,
            a_symbols,
            active_stock_count
        ),
        Some("/api/v1/quant/data/sync/daily/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-daily-{}", repair_end)
        })),
        None,
    ));

    let (etf_present, etf_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_stock_daily_bar_adj
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_lag = etf_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF日线(MVO)",
        if etf_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_lag, 2, 7)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_lag
        ),
        Some("/api/v1/quant/data/sync/fund-daily"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-daily-{}", repair_end)
        })),
        None,
    ));

    let adj_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_adjustment_factor
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let adj_lag = adj_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "A股复权因子",
        lag_level(adj_lag, 30, 90),
        format!(
            "最新{}，落后{}天",
            adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            adj_lag
        ),
        Some("/api/v1/quant/data/sync/adj-factor/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-adj-{}", repair_end)
        })),
        None,
    ));

    let (etf_adj_present, etf_adj_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_adjustment_factor
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_adj_lag = etf_adj_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF复权因子(MVO)",
        if etf_adj_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_adj_lag, 30, 90)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_adj_lag
        ),
        Some("/api/v1/quant/data/sync/fund-adj"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-adj-{}", repair_end)
        })),
        None,
    ));

    let csi_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let csi_lag = csi_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "CSI300",
        lag_level(csi_lag, 2, 7),
        format!(
            "最新{}，落后{}天",
            csi_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            csi_lag
        ),
        Some("/api/v1/quant/data/sync/index-daily"),
        Some(json!({
            "index_codes": ["000300.SH"],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-csi300-{}", repair_end)
        })),
        None,
    ));

    let stock_basic_level = if active_stock_count >= 3000 {
        "green"
    } else {
        "red"
    };
    out.push(check_item(
        acct,
        sid,
        "股票基础信息",
        stock_basic_level,
        format!("当前上市A股{}只", active_stock_count),
        Some("/api/v1/quant/data/sync/stock-basic"),
        Some(json!({"data_version_id": format!("health-repair-stock-basic-{}", repair_end)})),
        None,
    ));

    let st_count: i64 = sqlx::query_scalar("SELECT COUNT(*)::int8 FROM market_stock_name_history")
        .fetch_one(db)
        .await
        .unwrap_or(0);
    out.push(check_item(
        acct,
        sid,
        "ST/名称历史",
        if st_count > 0 { "green" } else { "red" },
        format!("名称历史/ST记录{}条", st_count),
        Some("/api/v1/quant/data/sync/namechange"),
        Some(json!({})),
        None,
    ));

    let (suspension_last, suspension_source, suspension_rows) =
        event_completion_latest_source(db, "market_stock_suspension", "suspension_daily").await;
    let suspension_lag = suspension_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    let suspension_source_level = suspension_last
        .map(|d| event_sync_source_level("suspension_daily", suspension_source.as_deref(), d))
        .unwrap_or("red");
    let suspension_level = if suspension_source_level == "green" {
        lag_level(suspension_lag, 2, 7)
    } else {
        suspension_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "停牌",
        suspension_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}",
            suspension_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            suspension_lag,
            suspension_source.as_deref().unwrap_or("无"),
            suspension_last
                .map(|d| event_sync_source_label(
                    "suspension_daily",
                    suspension_source.as_deref(),
                    d
                ))
                .unwrap_or("missing"),
            suspension_rows
        ),
        Some("/api/v1/quant/data/sync/suspension"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let (limit_last, limit_source, limit_rows) =
        event_completion_latest_source(db, "market_stock_limit", "limit_daily").await;
    let limit_lag = limit_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let limit_source_level = limit_last
        .map(|d| event_sync_source_level("limit_daily", limit_source.as_deref(), d))
        .unwrap_or("red");
    let limit_level = if limit_source_level == "green" {
        lag_level(limit_lag, 2, 7)
    } else {
        limit_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "涨跌停",
        limit_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}；2019-11-28前历史需由日线按交易规则派生",
            limit_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            limit_lag,
            limit_source.as_deref().unwrap_or("无"),
            limit_last
                .map(|d| event_sync_source_label("limit_daily", limit_source.as_deref(), d))
                .unwrap_or("missing"),
            limit_rows
        ),
        Some("/api/v1/quant/data/sync/limit"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
    out.push(check_item(
        acct,
        sid,
        "数据同步调度任务",
        if scheduler_issues.is_empty() {
            "green"
        } else {
            "red"
        },
        if scheduler_issues.is_empty() {
            "启用调度任务 CRON/依赖顺序检查通过".to_string()
        } else {
            format!(
                "调度任务问题{}项: {}",
                scheduler_issues.len(),
                scheduler_issues.join("；")
            )
        },
        None,
        None,
        if scheduler_issues.is_empty() {
            None
        } else {
            Some("请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效".to_string())
        },
    ));

    // PIT combo 物化新鲜度
    let combo_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name=$1 AND version='1.0.0'
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(&cfg.combo_name)
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    match combo_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo物化({})", cfg.combo_name),
                lag_level(lag, 2, 7),
                format!("最新 {} (落后行情 {} 天)", d, lag),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            format!("PIT combo物化({})", cfg.combo_name),
            "red",
            "combo 无任何 PIT 合规物化数据",
            Some("/api/v1/quant/factors/materialize-pit-combo/background"),
            Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 权益曲线新鲜度
    let curve_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM backtest_equity_curve WHERE task_id=$1")
            .bind(&cfg.equity_curve_task_id)
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    match curve_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线",
                lag_level(lag, 4, 10),
                format!("最新 {} (落后 {} 天, task={})", d, lag, cfg.equity_curve_task_id),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            "A股权益曲线",
            "red",
            format!("曲线 {} 无数据", cfg.equity_curve_task_id),
            Some("/api/v1/admin/sync/repair"),
            Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 滚动 IC 新鲜度
    let ic_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(end_date) FROM factor_evaluation WHERE horizon=20")
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    if let Some(d) = ic_last {
        let lag = (last_mkt - d).num_days();
        out.push(check_item(
            acct,
            sid,
            "滚动IC窗口",
            if lag > 100 { "yellow" } else { "green" },
            format!("最新IC窗口 {} (距今 {} 天)", d, lag),
            Some("/api/v1/quant/factors/evaluate-all/background"),
            Some(json!({})),
            None,
        ));
    }

    // ML 预测集新鲜度(策略使用 prediction / prediction_blend 时为必需项)
    if strategy_needs_prediction(cfg) {
        let resolved_prediction_set = resolve_live_prediction_set(db, cfg, last_mkt).await;
        let Some(ps) = resolved_prediction_set.as_deref() else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!(
                    "strategy signal_source={} 需要 ML，但没有 PIT 合规预测集覆盖 {}",
                    cfg.signal_source, last_mkt
                ),
                None,
                None,
                Some("需要先运行 PIT 训练/预测流水线，不能降级成纯因子交易".to_string()),
            ));
            return out;
        };
        let ps_ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(
               SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $2
                 AND training_end_date IS NOT NULL
                 AND training_end_date < $2
             )",
        )
        .bind(ps)
        .bind(last_mkt)
        .fetch_one(db)
        .await
        .unwrap_or(false);
        if !ps_ready {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 未 ready 或未 PIT 覆盖 {}", ps, last_mkt),
                None,
                None,
                Some("需要重建或切换到覆盖当前交易日的 PIT 预测集".to_string()),
            ));
            return out;
        }
        let ml_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(trade_date) FROM model_prediction
             WHERE prediction_set_id=$1 AND COALESCE(available_at, trade_date) <= trade_date",
        )
        .bind(ps)
        .fetch_one(db)
        .await
        .ok()
        .flatten();
        if let Some(d) = ml_last {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                lag_level(lag, 2, 7),
                format!("最新 {} (落后 {} 天, set={})", d, lag, ps),
                None,
                None,
                Some(
                    "预测集由 PIT 训练/预测流水线生成；单点页面修复不能保证模型正确性".to_string(),
                ),
            ));
        } else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 无 PIT 合规预测行", ps),
                None,
                None,
                Some("需要重建 PIT 预测集".to_string()),
            ));
        }
    }
    out
}
