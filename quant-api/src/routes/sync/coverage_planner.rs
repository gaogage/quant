//! phase7 可选源同步/覆盖率规划：参数规范化、批次编排与 readiness 计划函数。
use super::*;

pub(crate) fn phase7_optional_source_sync_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(PHASE7_OPTIONAL_SOURCE_SYNC_DEFAULT_SYMBOLS)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS)
}

pub(crate) fn phase7_optional_source_sync_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

pub(crate) fn phase7_optional_source_batch_size(batch_size: Option<usize>) -> usize {
    phase7_optional_source_sync_limit(batch_size)
}

pub(crate) fn phase7_optional_source_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(1)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_BATCH_MAX_COUNT)
}

pub(crate) fn phase7_optional_source_batch_offsets(
    start_offset: usize,
    batch_size: usize,
    batch_count: usize,
) -> Vec<usize> {
    (0..batch_count)
        .map(|index| start_offset + index * batch_size)
        .collect()
}

pub(crate) fn phase7_optional_source_batch_next_offset(
    offsets: &[usize],
    batch_size: usize,
) -> usize {
    offsets
        .last()
        .map(|offset| offset + batch_size)
        .unwrap_or_default()
}

pub(crate) fn phase7_optional_source_batch_recommended_resume_offset(
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

pub(crate) fn phase7_optional_source_batch_child_background(plan_only: bool) -> bool {
    !plan_only
}

pub(crate) fn phase7_coverage_runner_profile(profile: Option<&str>) -> String {
    profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_PROFILE)
        .to_string()
}

pub(crate) fn phase7_coverage_runner_batch_size(batch_size: Option<usize>) -> usize {
    batch_size
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_SIZE)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_SIZE)
}

pub(crate) fn phase7_coverage_runner_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_COUNT)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_COUNT)
}

pub(crate) fn phase7_coverage_runner_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

pub(crate) fn phase7_coverage_runner_auto_continue(auto_continue: Option<bool>) -> bool {
    auto_continue.unwrap_or(false)
}

pub(crate) fn phase7_coverage_runner_max_rounds(
    max_rounds: Option<usize>,
    auto_continue: bool,
) -> usize {
    if auto_continue {
        max_rounds
            .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_MAX_ROUNDS)
            .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_ROUNDS)
    } else {
        1
    }
}

pub(crate) fn phase7_coverage_runner_should_build_immediate_batches(
    plan_only: bool,
    auto_continue: bool,
) -> bool {
    plan_only || !auto_continue
}

pub(crate) fn phase7_coverage_autopilot_batch_offsets(batch_count: usize) -> Vec<usize> {
    vec![0; batch_count]
}

pub(crate) fn phase7_coverage_runner_target_ratio(target: Option<f64>) -> f64 {
    target
        .filter(|value| value.is_finite())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO)
        .clamp(
            PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO,
            PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO,
        )
}

pub(crate) fn phase7_share_float_chunk_granularity(granularity: Option<&str>) -> &'static str {
    match granularity
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("month") | Some("monthly") => "month",
        Some("quarter") | Some("quarterly") => "quarter",
        Some("year") | Some("yearly") => "year",
        _ => "year",
    }
}

pub(crate) fn phase7_share_float_coverage_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

pub(crate) fn phase7_share_float_coverage_max_chunks(max_chunks: Option<usize>) -> usize {
    max_chunks
        .unwrap_or(PHASE7_SHARE_FLOAT_COVERAGE_DEFAULT_MAX_CHUNKS)
        .clamp(1, PHASE7_SHARE_FLOAT_COVERAGE_MAX_CHUNKS)
}

pub(crate) fn add_months_clamped(date: NaiveDate, months: u32) -> NaiveDate {
    let month0 = date.month0() + months;
    let year = date.year() + (month0 / 12) as i32;
    let month = (month0 % 12) + 1;
    NaiveDate::from_ymd_opt(year, month, 1).expect("valid first day")
}

pub(crate) fn phase7_share_float_date_chunks(
    start: NaiveDate,
    end: NaiveDate,
    granularity: &str,
    max_chunks: usize,
) -> Vec<(NaiveDate, NaiveDate)> {
    let mut chunks = Vec::new();
    let mut cursor = start;
    while cursor <= end && chunks.len() < max_chunks {
        let next_start = phase7_share_float_next_chunk_start(cursor, granularity);
        let chunk_end = (next_start - Duration::days(1)).min(end);
        chunks.push((cursor, chunk_end));
        cursor = next_start;
    }
    chunks
}

pub(crate) fn phase7_share_float_readiness_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT symbol)::bigint AS symbol_count,
        MIN(float_date) AS min_float_date,
        MAX(float_date) AS max_float_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at > float_date)::bigint AS late_or_invalid_count,
        COUNT(DISTINCT float_date)::bigint AS distinct_float_dates
    FROM market_stock_share_float
    WHERE float_date BETWEEN $1 AND $2
      AND available_at <= $2
    "#
}

pub(crate) fn phase7_share_float_expected_days(start: NaiveDate, end: NaiveDate) -> i64 {
    end.signed_duration_since(start)
        .num_days()
        .saturating_add(1)
}

pub(crate) fn phase7_share_float_covered_days_from_windows(
    mut windows: Vec<(NaiveDate, NaiveDate)>,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    windows.sort_by_key(|(window_start, window_end)| (*window_start, *window_end));
    let mut covered_days = 0i64;
    let mut current_start: Option<NaiveDate> = None;
    let mut current_end: Option<NaiveDate> = None;

    for (window_start, window_end) in windows {
        let clipped_start = window_start.max(start);
        let clipped_end = window_end.min(end);
        if clipped_start > clipped_end {
            continue;
        }

        match (current_start, current_end) {
            (Some(active_start), Some(active_end))
                if clipped_start <= active_end + Duration::days(1) =>
            {
                current_start = Some(active_start);
                current_end = Some(active_end.max(clipped_end));
            }
            (Some(active_start), Some(active_end)) => {
                covered_days += phase7_share_float_expected_days(active_start, active_end);
                current_start = Some(clipped_start);
                current_end = Some(clipped_end);
            }
            _ => {
                current_start = Some(clipped_start);
                current_end = Some(clipped_end);
            }
        }
    }

    if let (Some(active_start), Some(active_end)) = (current_start, current_end) {
        covered_days += phase7_share_float_expected_days(active_start, active_end);
    }

    covered_days
}

pub(crate) fn phase7_share_float_feature_readiness(
    row_count: i64,
    covered_days: i64,
    expected_days: i64,
    late_or_invalid_count: i64,
) -> &'static str {
    if expected_days > 0 && covered_days < expected_days {
        "needs_float_date_backfill"
    } else if row_count <= 0 {
        "full_sync_empty_review"
    } else if late_or_invalid_count > 0 {
        "ready_for_pit_feature_factory_with_late_exclusion"
    } else {
        "ready_for_pit_feature_factory"
    }
}

pub(crate) fn phase7_coverage_runner_sources(requested: &[String]) -> Result<Vec<String>, String> {
    let raw_sources: Vec<String> = if requested.is_empty() {
        PHASE7_COVERAGE_RUNNER_DEFAULT_SOURCES
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
                "unsupported coverage runner source: {}; supported sources are {}",
                source,
                PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES.join(", ")
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

pub(crate) fn phase7_coverage_runner_should_plan_source(readiness: &str) -> bool {
    !matches!(
        readiness,
        "partial_feature_candidate" | "ready_for_feature_factory"
    )
}

pub(crate) fn phase7_coverage_runner_should_plan_source_for_target(
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

pub(crate) fn phase7_coverage_runner_source_state_for_window(
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

pub(crate) fn phase7_financial_source_readiness(coverage_grade: &str) -> &'static str {
    match coverage_grade {
        "broad" => "ready_for_feature_factory",
        "partial" => "partial_feature_candidate",
        "unknown_reference" => "coverage_reference_missing",
        "missing" => "needs_sync",
        _ => "undercovered_do_not_train",
    }
}

pub(crate) fn phase7_optional_source_sync_sources(
    requested: &[String],
) -> Result<Vec<String>, String> {
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
        if !PHASE7_OPTIONAL_SOURCE_SYNC_ALLOWED.contains(&source.as_str()) {
            return Err(format!(
                "unsupported optional source: {}; supported sources are {}",
                source,
                PHASE7_OPTIONAL_SOURCE_SYNC_ALLOWED.join(", ")
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

pub(crate) fn phase7_optional_source_table(source: &str) -> Option<&'static str> {
    match source {
        "cashflow" => Some("market_stock_cashflow"),
        "dividend" => Some("market_stock_dividend"),
        "repurchase" => Some("market_stock_repurchase"),
        "forecast" => Some("market_stock_forecast"),
        "express" => Some("market_stock_express"),
        "disclosure_date" => Some("market_stock_disclosure_date"),
        "share_float" => Some("market_stock_share_float"),
        _ => None,
    }
}

pub(crate) fn classify_tushare_permission_error(error: &str) -> &'static str {
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

pub(crate) fn exchange_announcement_order_capacity_is_excluded_unsupported_category_attempt(
    attempt_symbol: &str,
    error: &str,
) -> bool {
    attempt_symbol.ends_with(":重大事项") && error.trim_matches('"') == "'重大事项'"
}

pub(crate) fn normalize_exchange_announcement_order_capacity_probe_payload(
    payload: Value,
) -> Value {
    let status = payload
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if status != "error" {
        return payload;
    }

    let error_type = payload
        .get("error_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let error = payload
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if error_type == "KeyError"
        && exchange_announcement_order_capacity_is_akshare_empty_dataframe_key_error(error)
    {
        return json!({
            "status": "ok_empty",
            "permission": "available",
            "row_count": 0,
            "fields": [],
            "sample_rows": [],
            "normalized_from_error": {
                "error_type": error_type,
                "error": error,
                "reason": "akshare_empty_dataframe_missing_expected_columns"
            }
        });
    }

    if error_type == "KeyError" {
        let mut normalized = payload;
        if let Some(object) = normalized.as_object_mut() {
            object.insert("status".to_string(), json!("category_parser_error"));
            object.insert("parser_reliability".to_string(), json!("blocked"));
        }
        return normalized;
    }

    if error_type == "ValueError"
        && error.contains("Length mismatch")
        && error.contains("Expected axis has 0 elements")
    {
        return json!({
            "status": "ok_empty",
            "permission": "available",
            "row_count": 0,
            "fields": [],
            "sample_rows": [],
            "normalized_from_error": {
                "error_type": error_type,
                "error": error,
                "reason": "akshare_empty_dataframe_length_mismatch"
            }
        });
    }

    payload
}

pub(crate) fn exchange_announcement_order_capacity_pdf_parser_readiness_report(
    python: &str,
    checks: Vec<Value>,
) -> Value {
    let available_count = checks
        .iter()
        .filter(|check| {
            check
                .get("available")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();
    let parser_status = if available_count > 0 {
        "available"
    } else {
        "missing"
    };
    let admission_decision = if parser_status == "available" {
        "pdf_parser_available_detail_timestamp_audit_required_next"
    } else {
        "blocked_pdf_parser_missing"
    };

    json!({
        "audit_version": "p3.24d-exchange-announcement-order-capacity-pdf-parser-readiness-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24D",
        "mode": "read_only_pdf_parser_source_timestamp_readiness_no_write",
        "write_enabled": false,
        "vendor": "cninfo",
        "source_shape": "cninfo_detail_redirects_to_pdf",
        "python": python,
        "parser_status": parser_status,
        "available_parser_count": available_count,
        "checks": checks,
        "admission_decision": admission_decision,
        "promotion_gate": {
            "schema_apply": if parser_status == "available" {
                "blocked_until_pdf_text_and_source_published_at_audit_passes"
            } else {
                "blocked_until_pdf_parser_available"
            },
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "required_evidence_after_parser": [
            "pdf_http_success",
            "deterministic_pdf_text_extract",
            "stable_text_hash",
            "source_published_at_timestamp_or_explicit_next_session_policy",
            "evidence_span_precision_for_order_capacity_contract_price_production_capacity_events"
        ],
        "next_step": if parser_status == "available" {
            "run_pdf_text_source_published_at_detail_audit_on_bounded_samples"
        } else {
            "install_or_configure_reviewed_pdf_text_parser_in_isolated_runtime_then_rerun_readiness"
        },
        "guardrails": [
            "this endpoint never writes raw tables, sync attempts, data versions, factors, WFA tasks, or strategy configs",
            "PDF parser availability alone does not prove PIT safety; source_published_at and next-session availability must be audited separately",
            "do not use PDF bytes, redirected URLs, or date-only announcementTime as text evidence",
            "P3.10, WFA and v19 remain blocked until full coverage/PIT/text-evidence/correlation gates pass"
        ],
    })
}

pub(crate) fn normalize_akshare_analyst_revision_full_fetch_payload(payload: Value) -> Value {
    if !akshare_analyst_revision_is_empty_dataframe_length_mismatch(&payload) {
        return payload;
    }

    json!({
        "status": "ok_empty",
        "permission": "available",
        "row_count": 0,
        "fields": [],
        "records": [],
        "normalized_from_error": {
            "error_type": payload.get("error_type").cloned().unwrap_or(json!(null)),
            "error": payload.get("error").cloned().unwrap_or(json!(null)),
            "reason": "akshare_empty_dataframe_length_mismatch"
        }
    })
}

pub(crate) fn phase7_coverage_grade(symbols: i64, reference_symbols: i64) -> &'static str {
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

pub(crate) fn phase7_optional_source_readiness(
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

pub(crate) fn phase7_coverage_json(
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

pub(crate) fn phase7_coverage_rows_to_json(
    rows: Vec<(String, i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    reference_symbols: i64,
) -> Vec<Value> {
    rows.into_iter()
        .map(|(name, rows, min_date, max_date, symbols)| {
            phase7_coverage_json(name, rows, min_date, max_date, symbols, reference_symbols)
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]

pub(crate) struct Phase7OptionalSourceSpec {
    pub(crate) source: &'static str,
    pub(crate) table: &'static str,
    pub(crate) next_feature: &'static str,
}

pub(crate) fn phase7_optional_source_specs() -> &'static [Phase7OptionalSourceSpec] {
    &[
        Phase7OptionalSourceSpec {
            source: "cashflow",
            table: "market_stock_cashflow",
            next_feature: "cashflow_quality_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "dividend",
            table: "market_stock_dividend",
            next_feature: "dividend_stability_quality_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "repurchase",
            table: "market_stock_repurchase",
            next_feature: "repurchase_event_capital_return_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "forecast",
            table: "market_stock_forecast",
            next_feature: "event_post_announcement_return_curve_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "express",
            table: "market_stock_express",
            next_feature: "event_post_announcement_return_curve_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "disclosure_date",
            table: "market_stock_disclosure_date",
            next_feature: "event_disclosure_schedule_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "share_float",
            table: "market_stock_share_float",
            next_feature: "unlock_supply_pressure_pit_features",
        },
    ]
}

#[derive(Debug, Clone, Copy)]

pub(crate) struct Phase7MarketLevelSourceAudit {
    pub(crate) data_rows: i64,
    pub(crate) min_trade_date: Option<NaiveDate>,
    pub(crate) latest_trade_date: Option<NaiveDate>,
    pub(crate) open_day_lag: Option<i64>,
}

#[derive(Debug, Clone)]

pub(crate) struct Phase7MarketLevelSyncAudit {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) start_date: Option<NaiveDate>,
    pub(crate) end_date: Option<NaiveDate>,
    pub(crate) status: String,
    pub(crate) total_count: i32,
    pub(crate) success_count: i32,
    pub(crate) failed_count: i32,
    pub(crate) error_message: Option<String>,
    pub(crate) completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy)]

pub(crate) struct Phase7BlockTradeSourceAudit {
    pub(crate) data_rows: i64,
    pub(crate) symbols: i64,
    pub(crate) covered_trade_days: i64,
    pub(crate) open_days_in_range: i64,
    pub(crate) min_trade_date: Option<NaiveDate>,
    pub(crate) latest_trade_date: Option<NaiveDate>,
    pub(crate) min_available_at: Option<NaiveDate>,
    pub(crate) latest_available_at: Option<NaiveDate>,
    pub(crate) pit_violation_rows: i64,
}

#[derive(Debug, Clone, Copy)]

pub(crate) struct Phase7IndustryMembershipSourceAudit {
    pub(crate) data_rows: i64,
    pub(crate) symbols: i64,
    pub(crate) index_codes: i64,
    pub(crate) current_active_stock_symbols: i64,
    pub(crate) current_covered_stock_symbols: i64,
    pub(crate) min_in_date: Option<NaiveDate>,
    pub(crate) latest_in_date: Option<NaiveDate>,
    pub(crate) min_out_date: Option<NaiveDate>,
    pub(crate) latest_out_date: Option<NaiveDate>,
    pub(crate) min_available_at: Option<NaiveDate>,
    pub(crate) latest_available_at: Option<NaiveDate>,
    pub(crate) pit_violation_rows: i64,
    pub(crate) invalid_interval_rows: i64,
    pub(crate) duplicate_key_rows: i64,
}

pub(crate) fn phase7_market_level_sync_dataset(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("moneyflow_hsgt"),
        _ => None,
    }
}

pub(crate) fn phase7_date_json(date: Option<NaiveDate>) -> Value {
    date.map(|date| json!(date.to_string()))
        .unwrap_or(Value::Null)
}

pub(crate) fn phase7_datetime_json(date: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    json!(fmt_rfc3339_local(date))
}

pub(crate) fn phase7_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    if denominator <= 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

pub(crate) fn phase7_p315_sync_start_date(
    stats: &Phase7MarketLevelSourceAudit,
    today: NaiveDate,
) -> NaiveDate {
    stats
        .latest_trade_date
        .map(|date| (date + Duration::days(1)).min(today))
        .unwrap_or(today)
}
