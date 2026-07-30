/// 数据同步路由
use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tracing::info;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

use crate::phase7_alpha_admission::{
    industry_prosperity_alpha_admission_policy, industry_prosperity_alpha_admission_policy_static,
    INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD, INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
    SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
};
use crate::AppState;

pub mod account_health;
pub mod analyst_revision;
pub mod bounded_raw;
pub mod exchange_announcement;
pub mod market_data;
pub mod phase7_audit;
pub mod reference_data;
pub mod task_lifecycle;

#[cfg(test)]
mod tests;

// Re-export all public items to preserve routes::sync::X path
pub use account_health::*;
pub use analyst_revision::*;
pub use bounded_raw::*;
pub use exchange_announcement::*;
pub use market_data::*;
pub use phase7_audit::*;
pub use reference_data::*;
pub use task_lifecycle::*;

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



pub(crate) fn default_source() -> String {
    "tushare".into()
}



pub(crate) fn stale_cleanup_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(3600).clamp(60, 86_400)
}



pub(crate) fn stale_cleanup_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}



pub(crate) fn stale_sync_task_cleanup_terminal_status(status: &str) -> Option<&'static str> {
    match status {
        "running" => Some("failed"),
        "cancel_requested" => Some("cancelled"),
        _ => None,
    }
}



pub(crate) fn stale_sync_task_cleanup_action(status: &str) -> &'static str {
    match stale_sync_task_cleanup_terminal_status(status) {
        Some("cancelled") => "finalize_cancel_requested",
        Some("failed") => "mark_running_failed",
        _ => "ignore",
    }
}



pub(crate) fn generated_data_version_id() -> String {
    chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
}



pub(crate) fn bounded_phase7_task_id(parts: &[&str]) -> String {
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



pub(crate) fn parse_optional_date(value: Option<&str>) -> Result<Option<NaiveDate>, String> {
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
pub(crate) const PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED: &[&str] = &[
    "cashflow",
    "dividend",
    "repurchase",
    "share_float",
    "industry_membership",
    "main_business",
    "report_rc",
    "futures_price_chain",
    "equity_pledge_pressure",
    "shareholder_structure",
    "margin_detail",
];
const AKSHARE_ANALYST_REVISION_SMOKE_DEFAULTS: &[&str] = &["stock_rank_forecast_cninfo"];
pub(crate) const AKSHARE_ANALYST_REVISION_SMOKE_ALLOWED: &[&str] = &[
    "stock_rank_forecast_cninfo",
    "stock_research_report_em",
    "stock_profit_forecast_em",
    "stock_institute_recommend",
    "stock_institute_recommend_detail",
    "stock_profit_forecast_ths",
];
const PHASE7_OPTIONAL_SOURCE_SYNC_ALLOWED: &[&str] = &[
    "cashflow",
    "dividend",
    "repurchase",
    "forecast",
    "express",
    "disclosure_date",
    "share_float",
];
pub(crate) const PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS: usize = 3;
pub(crate) const PHASE7_PERMISSION_SMOKE_MAX_ROWS: usize = 5;
pub(crate) const AKSHARE_ANALYST_REVISION_MAX_DATES: usize = 8;
pub(crate) const AKSHARE_ANALYST_REVISION_HISTORY_MAX_DATES: usize = 16;
pub(crate) const AKSHARE_ANALYST_REVISION_MAX_SYMBOLS: usize = 5;
pub(crate) const AKSHARE_ANALYST_REVISION_MAX_ROWS: usize = 50;
const AKSHARE_ANALYST_REVISION_SMOKE_TIMEOUT_SECONDS: u64 = 60;
pub(crate) const AKSHARE_ANALYST_REVISION_SYNC_PLAN_DEFAULT_BATCH: &str = "quarter";
const AKSHARE_ANALYST_REVISION_SYNC_PLAN_MAX_BATCHES: usize = 80;
const AKSHARE_ANALYST_REVISION_SYNC_MAX_CALENDAR_DAYS: i64 = 100;
pub(crate) const AKSHARE_ANALYST_REVISION_SYNC_MAX_RETRIES: usize = 2;
pub(crate) const AKSHARE_ANALYST_REVISION_TASK_SOURCE: &str = "akshare_cninfo_revision";
pub(crate) const AKSHARE_ANALYST_REVISION_ATTEMPT_SOURCE: &str = "akshare:stock_rank_forecast_cninfo";
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_TASK_SOURCE: &str = "ak_cninfo_exann_oc";
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE: &str =
    "akshare:stock_zh_a_disclosure_report_cninfo";
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_SYMBOLS: usize = 5;
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_CATEGORIES: usize = 6;
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS: usize = 20;
const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_RAW_SYNC_MAX_ROWS: usize = 30;
const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_OCR_AUDIT_MAX_ROWS: usize = 8;
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS: i64 = 3;
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS: usize = 120;
pub(crate) const EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_DEFAULT_CATEGORIES: &[&str] =
    &["日常经营", "重大事项", "股权激励", "并购重组"];
pub(crate) const MAIN_BUSINESS_AVAILABLE_AT_AUDIT_MAX_PERIODS: usize = 100;
pub(crate) const MAIN_BUSINESS_READINESS_BREAKDOWN_MAX_PERIODS: usize = 200;
pub(crate) const BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT: usize = 32;
pub(crate) const BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE: f64 = 0.50;
pub(crate) const BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE: f64 = 0.30;
pub(crate) const BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE: f64 = 0.30;
pub(crate) const BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO: f64 = 0.15;
const PHASE7_OPTIONAL_SOURCE_SYNC_DEFAULT_SYMBOLS: usize = 20;
const PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS: usize = 200;
const PHASE7_OPTIONAL_SOURCE_BATCH_MAX_COUNT: usize = 10;
const PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_SIZE: usize = 50;
const PHASE7_COVERAGE_RUNNER_MAX_BATCH_SIZE: usize = 100;
const PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_COUNT: usize = 4;
const PHASE7_COVERAGE_RUNNER_MAX_BATCH_COUNT: usize = 10;
const PHASE7_COVERAGE_RUNNER_DEFAULT_PROFILE: &str = "local_mac_safe";
const PHASE7_COVERAGE_RUNNER_DEFAULT_SOURCES: &[&str] = &["cashflow", "dividend", "financial"];
const PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES: &[&str] = &[
    "cashflow",
    "dividend",
    "financial",
    "forecast",
    "express",
    "disclosure_date",
];
const PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO: f64 = 1.0;
const PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO: f64 = 0.30;
const PHASE7_COVERAGE_RUNNER_DEFAULT_MAX_ROUNDS: usize = PHASE7_COVERAGE_RUNNER_MAX_ROUNDS;
const PHASE7_COVERAGE_RUNNER_MAX_ROUNDS: usize = 20;
const PHASE7_SHARE_FLOAT_COVERAGE_DEFAULT_MAX_CHUNKS: usize = 16;
const PHASE7_SHARE_FLOAT_COVERAGE_MAX_CHUNKS: usize = 64;

#[derive(Debug, Clone, Deserialize)]


pub struct Phase7ShareFloatReadinessAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct Phase7IndustryMembershipCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}



pub(crate) fn phase7_permission_smoke_sources(requested: &[String]) -> Vec<String> {
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



pub(crate) fn phase7_permission_smoke_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(1)
        .clamp(1, PHASE7_PERMISSION_SMOKE_MAX_ROWS)
}



pub(crate) fn akshare_analyst_revision_smoke_sources(requested: &[String]) -> Vec<String> {
    let raw_sources: Vec<String> = if requested.is_empty() {
        AKSHARE_ANALYST_REVISION_SMOKE_DEFAULTS
            .iter()
            .map(|source| (*source).to_string())
            .collect()
    } else {
        requested
            .iter()
            .map(|source| source.trim().to_ascii_lowercase())
            .filter(|source| !source.is_empty())
            .filter(|source| AKSHARE_ANALYST_REVISION_SMOKE_ALLOWED.contains(&source.as_str()))
            .collect()
    };

    let mut seen = BTreeSet::new();
    raw_sources
        .into_iter()
        .filter(|source| seen.insert(source.clone()))
        .collect()
}



pub(crate) fn akshare_analyst_revision_python_path(requested: Option<String>) -> String {
    requested
        .filter(|path| !path.trim().is_empty())
        .or_else(|| env::var("AKSHARE_PYTHON").ok())
        .unwrap_or_else(|| "/tmp/akshare-smoke/bin/python".to_string())
}



pub(crate) fn akshare_analyst_revision_timeout_seconds() -> u64 {
    env::var("AKSHARE_SMOKE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(AKSHARE_ANALYST_REVISION_SMOKE_TIMEOUT_SECONDS)
        .clamp(5, 300)
}



pub(crate) fn command_exists_on_path(command: &str) -> bool {
    env::var_os("PATH")
        .map(|paths| {
            env::split_paths(&paths).any(|path| {
                let candidate = path.join(command);
                candidate.is_file()
            })
        })
        .unwrap_or(false)
}



pub(crate) fn exchange_announcement_order_capacity_raw_sync_row_limit() -> usize {
    EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_RAW_SYNC_MAX_ROWS
}



pub(crate) fn exchange_announcement_order_capacity_smoke_plan(
    req: &ExchangeAnnouncementOrderCapacitySmokeReq,
) -> Result<Value, String> {
    let symbols = exchange_announcement_order_capacity_symbols(&req.symbols);
    if symbols.is_empty() {
        return Err("exchange announcement smoke requires at least one symbol".into());
    }
    let categories = exchange_announcement_order_capacity_categories(&req.categories);
    if categories.is_empty() {
        return Err("exchange announcement smoke requires at least one category".into());
    }
    let (start_date, end_date) = exchange_announcement_order_capacity_date_range(req)?;
    let market = req
        .market
        .clone()
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let row_limit = exchange_announcement_order_capacity_row_limit(req.limit);
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let query_count = symbols.len() * categories.len();

    Ok(json!({
        "audit_version": "p3.24b-exchange-announcement-order-capacity-permission-history-category-smoke-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24B",
        "mode": "read_only_permission_history_category_smoke_no_write",
        "write_enabled": false,
        "vendor": "akshare",
        "upstream": "cninfo",
        "vendor_endpoint": "stock_zh_a_disclosure_report_cninfo",
        "python": python,
        "market": market,
        "symbols": symbols,
        "categories": categories,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "row_limit_per_probe": row_limit,
        "query_count": query_count,
        "required_fields_for_schema_audit": ["代码", "简称", "公告标题", "公告时间", "公告链接"],
        "required_link_metadata": ["announcementId", "orgId", "stockCode", "announcementTime"],
        "quality_dimensions": [
            "symbol_category_history_availability",
            "category_parser_error_classification",
            "announcement_time_presence",
            "announcement_link_presence",
            "announcement_link_metadata_completeness",
            "duplicate_announcement_id_detection"
        ],
        "promotion_gate": {
            "schema_apply": "blocked",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_this_read_only_smoke_then_audit_cninfo_detail_text_timestamp_and_text_hash_before_schema_apply",
        "guardrails": [
            "this plan and smoke endpoint never create raw tables, sync attempts, data versions, factors, WFA tasks, or strategy configs",
            "ok_empty is not a pass; empty categories must be separated from parser/runtime errors before full-history sync design",
            "date-only announcement_time cannot be used for same-session intraday decisions",
            "schema apply is blocked until official detail text, source_published_at timestamp, text hash and manual evidence-span audit are proven"
        ],
    }))
}



pub(crate) fn parse_cninfo_announcement_link_metadata(link: &str) -> Value {
    let announcement_id = cninfo_query_param(link, "announcementId")
        .or_else(|| cninfo_announcement_id_from_path(link));
    let org_id = cninfo_query_param(link, "orgId");
    let stock_code = cninfo_query_param(link, "stockCode");
    let announcement_time = cninfo_query_param(link, "announcementTime");

    let required = [
        ("announcementId", announcement_id.clone()),
        ("orgId", org_id.clone()),
        ("stockCode", stock_code.clone()),
        ("announcementTime", announcement_time.clone()),
    ];
    let missing_fields = required
        .iter()
        .filter_map(|(field, value)| {
            if value
                .as_ref()
                .map(|value| value.trim().is_empty())
                .unwrap_or(true)
            {
                Some(*field)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    json!({
        "raw_link": link,
        "announcement_id": announcement_id,
        "org_id": org_id,
        "stock_code": stock_code,
        "announcement_time": announcement_time,
        "metadata_complete": missing_fields.is_empty(),
        "missing_fields": missing_fields,
    })
}



pub(crate) fn exchange_announcement_order_capacity_detail_audit_plan(
    req: &ExchangeAnnouncementOrderCapacityDetailAuditReq,
) -> Result<Value, String> {
    let links =
        exchange_announcement_order_capacity_detail_links(&req.announcement_links, req.limit);
    if links.is_empty() {
        return Err(
            "exchange announcement detail audit requires at least one announcement link".into(),
        );
    }
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let link_metadata = links
        .iter()
        .map(|link| parse_cninfo_announcement_link_metadata(link))
        .collect::<Vec<_>>();
    let incomplete_metadata_count = link_metadata
        .iter()
        .filter(|metadata| {
            !metadata
                .get("metadata_complete")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();

    Ok(json!({
        "audit_version": "p3.24c-exchange-announcement-order-capacity-detail-text-timestamp-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24C",
        "mode": "read_only_cninfo_detail_text_timestamp_hash_audit_no_write",
        "write_enabled": false,
        "vendor": "cninfo",
        "vendor_endpoint": "announcement_detail_page",
        "python": python,
        "link_count": links.len(),
        "announcement_links": links,
        "link_metadata": link_metadata,
        "incomplete_link_metadata_count": incomplete_metadata_count,
        "required_evidence": [
            "detail_page_http_success",
            "nonempty_text_content",
            "stable_text_hash",
            "source_published_at_timestamp_or_explicit_date_only_block",
            "announcement_id_org_id_stock_code_announcement_time"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_detail_audit_passes",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_read_only_detail_fetch_then_manual_schema_review_if_text_timestamp_hash_pass",
        "guardrails": [
            "this endpoint never writes raw tables, sync attempts, data versions, factors, WFA tasks, or strategy configs",
            "detail text availability does not prove event taxonomy precision or alpha economics",
            "date-only announcementTime remains insufficient for same-session intraday decisions",
            "schema apply still requires manual review of source_published_at semantics and evidence span policy"
        ],
    }))
}



pub(crate) fn exchange_announcement_order_capacity_pdf_detail_audit_plan(
    req: &ExchangeAnnouncementOrderCapacityPdfDetailAuditReq,
) -> Result<Value, String> {
    let links =
        exchange_announcement_order_capacity_detail_links(&req.announcement_links, req.limit);
    if links.is_empty() {
        return Err(
            "exchange announcement PDF detail audit requires at least one announcement link".into(),
        );
    }
    let python = exchange_announcement_order_capacity_pdf_audit_python_path(req.python.clone());
    let link_metadata = links
        .iter()
        .map(|link| parse_cninfo_announcement_link_metadata(link))
        .collect::<Vec<_>>();
    let incomplete_link_metadata_count = link_metadata
        .iter()
        .filter(|metadata| {
            !metadata
                .get("metadata_complete")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();

    Ok(json!({
        "audit_version": "p3.24e-exchange-announcement-order-capacity-pdf-detail-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24E",
        "mode": "read_only_pdf_detail_text_timestamp_span_audit_no_write",
        "write_enabled": false,
        "vendor": "cninfo",
        "vendor_endpoint": "announcement_pdf_detail",
        "python": python,
        "link_count": links.len(),
        "announcement_links": links,
        "link_metadata": link_metadata,
        "incomplete_link_metadata_count": incomplete_link_metadata_count,
        "required_evidence": [
            "pdf_http_success",
            "nonempty_pdf_text_content",
            "stable_text_hash",
            "source_published_at_timestamp_or_explicit_next_session_policy",
            "relevant_evidence_spans_for_order_capacity_contract_price_production_capacity_events"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_pdf_detail_audit_passes",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_read_only_pdf_detail_audit_then_manual_schema_review_if_pdf_text_availability_and_span_gates_pass",
        "guardrails": [
            "this endpoint never writes raw tables, sync attempts, data versions, factors, WFA tasks, or strategy configs",
            "pdf parser output is admission evidence only; it does not classify alpha or unlock training",
            "date-only source availability may support next-session policy but never same-session intraday use",
            "scanned pdfs requiring OCR remain blocked in this stage"
        ],
    }))
}



pub(crate) fn exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(
    req: &ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq,
) -> Result<Value, String> {
    let today = Utc::now().date_naive();
    let start_date = req
        .start_date
        .clone()
        .unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .clone()
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("OCR blocked-row audit start_date cannot be after end_date".into());
    }

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols.clone());
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    let row_limit = req
        .limit
        .unwrap_or(4)
        .clamp(1, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_OCR_AUDIT_MAX_ROWS);
    let python = exchange_announcement_order_capacity_pdf_audit_python_path(req.python.clone());

    Ok(json!({
        "audit_version": "p3.24t-exchange-announcement-order-capacity-scanned-pdf-ocr-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24T",
        "mode": "read_only_scanned_pdf_ocr_candidate_audit_no_write",
        "write_enabled": false,
        "vendor": "cninfo",
        "source_table": "market_exchange_announcement_text_raw",
        "python": python,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "symbols": symbols,
        "row_limit": row_limit,
        "row_selector": {
            "pdf_parse_status": "scanned_pdf_ocr_required",
            "order_by": "announcement_time, symbol, announcement_id",
        },
        "required_runtime": [
            "tesseract_binary",
            "pytesseract_python_module",
            "pymupdf_or_pdf2image_or_poppler_renderer"
        ],
        "required_evidence": [
            "ocr_text_nonempty",
            "stable_ocr_text_hash",
            "source_published_at_or_date_only_next_session_policy_preserved_from_raw",
            "ocr_quality_score_or_minimum_text_length",
            "evidence_spans_for_order_capacity_contract_price_production_capacity_events",
            "manual_taxonomy_precision_review_before_any_trainable_row_unblock"
        ],
        "promotion_gate": {
            "raw_backfill": "blocked_until_ocr_quality_and_manual_taxonomy_review_pass",
            "coverage_quality_audit": "blocked_until_ocr_quality_and_manual_taxonomy_review_pass",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint reads blocked raw rows and candidate PDF URLs but never updates market_exchange_announcement_text_raw",
            "OCR text is not automatically trainable data; it is manual-review evidence until quality and taxonomy gates pass",
            "raw available_at and source_published_at policy must be preserved; OCR runtime execution time is never used as PIT availability",
            "scanned PDF exclusion remains a separate pre-registered coverage-scope decision, not an implicit fallback"
        ],
    }))
}



pub(crate) fn decide_exchange_announcement_detail_audit(
    total_links: usize,
    fetched_text_count: usize,
    text_hash_count: usize,
    source_published_at_count: usize,
    incomplete_link_metadata_count: usize,
) -> Value {
    let admission_decision = if total_links == 0 {
        "blocked_no_detail_links"
    } else if incomplete_link_metadata_count > 0 {
        "blocked_incomplete_announcement_link_metadata"
    } else if fetched_text_count < total_links {
        "blocked_detail_text_fetch_incomplete"
    } else if text_hash_count < total_links {
        "blocked_missing_text_hash"
    } else if source_published_at_count < total_links {
        "blocked_missing_source_published_at_timestamp"
    } else {
        "detail_text_timestamp_hash_audit_passed_schema_review_allowed_next"
    };

    json!({
        "admission_decision": admission_decision,
        "total_links": total_links,
        "fetched_text_count": fetched_text_count,
        "text_hash_count": text_hash_count,
        "source_published_at_count": source_published_at_count,
        "incomplete_link_metadata_count": incomplete_link_metadata_count,
        "promotion_gate": {
            "schema_apply": if admission_decision == "detail_text_timestamp_hash_audit_passed_schema_review_allowed_next" {
                "manual_review_required"
            } else {
                "blocked"
            },
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
    })
}



pub(crate) fn decide_exchange_announcement_order_capacity_pdf_detail_audit(
    total_links: usize,
    parsed_pdf_count: usize,
    stable_hash_count: usize,
    availability_count: usize,
    evidence_span_count: usize,
    scanned_pdf_count: usize,
    incomplete_link_metadata_count: usize,
) -> Value {
    let admission_decision = if total_links == 0 {
        "blocked_no_pdf_detail_links"
    } else if incomplete_link_metadata_count > 0 {
        "blocked_incomplete_announcement_link_metadata"
    } else if scanned_pdf_count > 0 {
        "blocked_scanned_pdf_ocr_required"
    } else if parsed_pdf_count < total_links {
        "blocked_pdf_fetch_or_parse_incomplete"
    } else if stable_hash_count < total_links {
        "blocked_unstable_pdf_text_hash"
    } else if availability_count < total_links {
        "blocked_missing_pdf_source_published_at_or_next_session_policy"
    } else if evidence_span_count < total_links {
        "blocked_missing_relevant_evidence_spans"
    } else {
        "pdf_detail_audit_passed_manual_schema_review_allowed_next"
    };

    json!({
        "admission_decision": admission_decision,
        "total_links": total_links,
        "parsed_pdf_count": parsed_pdf_count,
        "stable_hash_count": stable_hash_count,
        "availability_count": availability_count,
        "evidence_span_count": evidence_span_count,
        "scanned_pdf_count": scanned_pdf_count,
        "incomplete_link_metadata_count": incomplete_link_metadata_count,
        "promotion_gate": {
            "schema_apply": if admission_decision == "pdf_detail_audit_passed_manual_schema_review_allowed_next" {
                "manual_review_required"
            } else {
                "blocked"
            },
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
    })
}



pub(crate) fn decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
    total_rows: usize,
    ocr_text_count: usize,
    stable_hash_count: usize,
    availability_count: usize,
    quality_pass_count: usize,
    evidence_span_count: usize,
    runtime_missing_count: usize,
    ocr_error_count: usize,
    no_target_span_count: usize,
    incomplete_raw_link_count: usize,
) -> Value {
    let admission_decision = if total_rows == 0 {
        "blocked_no_scanned_pdf_rows_in_scope"
    } else if incomplete_raw_link_count > 0 {
        "blocked_incomplete_raw_pdf_link_metadata"
    } else if runtime_missing_count > 0 || ocr_text_count < total_rows {
        "blocked_ocr_runtime_missing_or_incomplete"
    } else if ocr_error_count > 0 {
        "blocked_ocr_runtime_errors"
    } else if stable_hash_count < total_rows {
        "blocked_unstable_ocr_text_hash"
    } else if availability_count < total_rows {
        "blocked_missing_raw_source_published_at_or_available_at_policy"
    } else if quality_pass_count < total_rows {
        "blocked_low_ocr_text_quality"
    } else if evidence_span_count == 0 {
        "blocked_ocr_text_has_no_order_capacity_evidence_spans"
    } else {
        "ocr_text_quality_audit_passed_manual_taxonomy_review_required"
    };
    let ocr_quality_status =
        if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
            "passed_for_manual_review_only"
        } else {
            "blocked"
        };

    json!({
        "admission_decision": admission_decision,
        "total_rows": total_rows,
        "ocr_text_count": ocr_text_count,
        "stable_hash_count": stable_hash_count,
        "availability_count": availability_count,
        "quality_pass_count": quality_pass_count,
        "evidence_span_count": evidence_span_count,
        "runtime_missing_count": runtime_missing_count,
        "ocr_error_count": ocr_error_count,
        "no_target_span_count": no_target_span_count,
        "incomplete_raw_link_count": incomplete_raw_link_count,
        "ocr_quality_gate": {
            "status": ocr_quality_status,
            "policy": "OCR output is admission evidence only; it cannot unblock trainable rows until manual taxonomy precision review passes"
        },
        "promotion_gate": {
            "raw_backfill": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
                "manual_review_required"
            } else {
                "blocked"
            },
            "coverage_quality_audit": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
                "manual_review_required_before_unblocking_scanned_pdf_rows"
            } else {
                "blocked"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
            "manual_review_ocr_text_and_evidence_spans_then_design_auditable_raw_backfill_or_exclusion_policy"
        } else if runtime_missing_count > 0 {
            "install_or_configure_isolated_ocr_runtime_then_rerun_this_read_only_audit"
        } else {
            "repair_ocr_quality_or_pre_register_scanned_pdf_exclusion_scope_then_rerun"
        }
    })
}



pub(crate) fn summarize_exchange_announcement_detail_probes(
    probes: &[Value],
) -> ExchangeAnnouncementDetailProbeSummary {
    let fetched_text_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("text_content_type")
                    .and_then(Value::as_str)
                    .map(|content_type| content_type != "pdf")
                    .unwrap_or(true)
                && probe
                    .get("text_length")
                    .and_then(Value::as_u64)
                    .map(|length| length > 0)
                    .unwrap_or(false)
        })
        .count();
    let text_hash_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("text_content_type")
                    .and_then(Value::as_str)
                    .map(|content_type| content_type != "pdf")
                    .unwrap_or(true)
                && probe.get("text_hash").and_then(Value::as_str).is_some()
        })
        .count();
    let source_published_at_count = probes
        .iter()
        .filter(|probe| {
            probe
                .get("source_published_at_quality")
                .and_then(Value::as_str)
                == Some("timestamp")
        })
        .count();
    let incomplete_link_metadata_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("incomplete_link_metadata")
                || probe
                    .get("link_metadata")
                    .and_then(|metadata| metadata.get("metadata_complete"))
                    .and_then(Value::as_bool)
                    .map(|complete| !complete)
                    .unwrap_or(false)
        })
        .count();
    let pdf_parser_required_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("pdf_text_parser_required")
                || probe.get("text_content_type").and_then(Value::as_str) == Some("pdf")
        })
        .count();

    ExchangeAnnouncementDetailProbeSummary {
        fetched_text_count,
        text_hash_count,
        source_published_at_count,
        incomplete_link_metadata_count,
        pdf_parser_required_count,
    }
}



pub(crate) fn validate_exchange_announcement_order_capacity_sync_request(
    req: &ExchangeAnnouncementOrderCapacitySyncReq,
) -> Result<ExchangeAnnouncementOrderCapacityValidatedSyncRequest, String> {
    if req.background {
        return Err(
            "exchange announcement order capacity P3.24I sync requires background=false".into(),
        );
    }

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
        return Err("exchange announcement sync start_date cannot be after end_date".into());
    }
    let calendar_day_count = (end - start).num_days() + 1;
    if calendar_day_count > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS {
        return Err(format!(
            "exchange announcement bounded sync resolved {} calendar days, above max {}. Use one tiny manually reviewed batch first.",
            calendar_day_count, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS
        ));
    }

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols.clone());
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err("exchange announcement sync requires at least one symbol".into());
    }
    let requested_categories =
        exchange_announcement_order_capacity_csv_values(req.categories.clone());
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement sync requires at least one category".into());
    }
    let market = req
        .market
        .clone()
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let data_version_id = req.data_version_id.clone().unwrap_or_else(|| {
        format!(
            "exchange-announcement-order-capacity-{}-{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        )
    });
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let pdf_python =
        exchange_announcement_order_capacity_pdf_audit_python_path(req.pdf_python.clone());
    let query_count = symbols.len() * categories.len();

    Ok(ExchangeAnnouncementOrderCapacityValidatedSyncRequest {
        symbols,
        categories,
        market,
        start,
        end,
        calendar_day_count,
        query_count,
        data_version_id,
        python,
        pdf_python,
    })
}



pub(crate) fn quarter_index(date: NaiveDate) -> u32 {
    ((date.month() - 1) / 3) + 1
}



pub(crate) fn validate_exchange_announcement_order_capacity_bounded_sync_request(
    req: &ExchangeAnnouncementOrderCapacityBoundedSyncReq,
) -> Result<ExchangeAnnouncementOrderCapacityValidatedBoundedSyncRequest, String> {
    if req.background {
        return Err(
            "exchange announcement order capacity P3.24J bounded sync requires background=false"
                .into(),
        );
    }

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
        return Err(
            "exchange announcement bounded sync start_date cannot be after end_date".into(),
        );
    }
    let batch_mode = req
        .batch
        .clone()
        .unwrap_or_else(|| "month".to_string())
        .trim()
        .to_ascii_lowercase();
    exchange_announcement_order_capacity_validate_bounded_window(start, end, &batch_mode)?;

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols.clone());
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err("exchange announcement bounded sync requires at least one symbol".into());
    }
    let requested_categories =
        exchange_announcement_order_capacity_csv_values(req.categories.clone());
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement bounded sync requires at least one category".into());
    }
    let slices = exchange_announcement_order_capacity_tiny_slices(start, end);
    let total_query_units = slices.len() * symbols.len() * categories.len();
    if total_query_units > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS {
        return Err(format!(
            "exchange announcement bounded sync resolved {} query units, above max {}. Narrow symbols/categories or run smaller month/quarter batches.",
            total_query_units, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS
        ));
    }

    let market = req
        .market
        .clone()
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let data_version_id = req.data_version_id.clone().unwrap_or_else(|| {
        format!(
            "exann-oc-{}-{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        )
    });
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let pdf_python =
        exchange_announcement_order_capacity_pdf_audit_python_path(req.pdf_python.clone());
    let stop_on_audit_failure = req.stop_on_audit_failure.unwrap_or(true);

    Ok(
        ExchangeAnnouncementOrderCapacityValidatedBoundedSyncRequest {
            symbols,
            categories,
            market,
            start,
            end,
            calendar_day_count: (end - start).num_days() + 1,
            batch_mode,
            slices,
            total_query_units,
            data_version_id,
            python,
            pdf_python,
            stop_on_audit_failure,
        },
    )
}



fn exchange_announcement_event_type_from_title_and_spans(
    title: &str,
    spans: &Value,
) -> Option<String> {
    if exchange_announcement_order_capacity_taxonomy_risk_title_reason("日常经营", title).is_some()
    {
        return None;
    }
    exchange_announcement_event_type_from_spans(spans)
}



fn exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
    category: &str,
    title: &str,
) -> Option<&'static str> {
    let text = format!("{category}{title}");
    if text.contains("控股股东及其他关联方资金占用")
        || text.contains("非经营性资金占用")
        || text.contains("关联资金往来情况汇总表")
    {
        return Some("special_report_related_party_funds");
    }
    if text.contains("财务公司关联交易")
        || text.contains("存款、贷款等金融业务")
        || text.contains("金融业务的专项说明")
    {
        return Some("special_report_related_party_finance");
    }
    if text.contains("专项说明")
        && (text.contains("审计")
            || text.contains("资金占用")
            || text.contains("关联方")
            || text.contains("关联交易")
            || text.contains("财务公司"))
    {
        return Some("special_report_audit_or_related_party");
    }
    if text.contains("募集资金")
        && (text.contains("存放与使用") || text.contains("存放和使用"))
        && (text.contains("鉴证报告") || text.contains("专项核查报告") || text.contains("专项报告"))
    {
        return Some("special_report_fundraising_use_assurance");
    }
    if text.contains("审计报告") {
        return Some("audit_report");
    }
    if text.contains("法律意见书") {
        return Some("legal_opinion");
    }
    if text.contains("财务顾问报告") {
        return Some("financial_advisor_report");
    }
    None
}



pub(crate) fn exchange_announcement_order_capacity_taxonomy_risk_title_reason(
    category: &str,
    title: &str,
) -> Option<&'static str> {
    let text = format!("{category}{title}");
    let is_true_operating_target = text.contains("签署《关于进一步加强和深化合作的协议》")
        || text.contains("合资建厂")
        || text.contains("签订日常经营重大合同")
        || text.contains("投资建设高效电池产能")
        || text.contains("投资建设产能项目");
    if is_true_operating_target {
        return None;
    }

    if text.contains("计提减值准备")
        || text.contains("募投项目")
        || text.contains("募集资金")
        || text.contains("关联交易")
        || text.contains("授信额度")
        || text.contains("注册资本")
        || text.contains("工商变更")
        || text.contains("实际控制人")
        || text.contains("控制权")
        || text.contains("股份质押")
        || text.contains("财务报告")
        || text.contains("年度报告")
        || text.contains("半年度报告")
        || text.contains("季度报告")
        || text.contains("主要经营数据")
        || text.contains("股权投资基金")
        || text.contains("投资基金")
        || text.contains("风险评估报告")
        || text.contains("H股发行")
        || text.contains("发行H股")
        || text.contains("H股股票")
        || text.contains("上市审计机构")
        || text.contains("章程")
        || text.contains("审计报告")
        || text.contains("审计机构")
        || text.contains("资产减值")
        || text.contains("公募REITs")
        || text.contains("REITs")
        || text.contains("收购控股子公司")
        || text.contains("董事会工作报告")
        || text.contains("监事会工作报告")
        || text.contains("内部控制")
        || text.contains("会计师事务所")
        || text.contains("审计委员会")
        || text.contains("委托理财")
        || text.contains("套期保值")
        || text.contains("担保额度")
        || text.contains("担保的进展")
        || text.contains("提供担保")
        || text.contains("发行债券")
        || text.contains("公司章程")
        || text.contains("公司制度")
        || text.contains("制定及修订")
        || text.contains("独立董事")
        || text.contains("会计政策变更")
        || text.contains("社会责任报告")
        || text.contains("可持续发展报告")
        || text.contains("可持续发展")
        || text.contains("环境、社会及治理")
        || text.contains("ESG")
        || text.contains("估值提升计划")
        || text.contains("市值管理")
        || text.contains("质量回报双提升")
        || text.contains("履职情况")
        || text.contains("履行监督职责")
    {
        return Some("admin_finance_governance_false_positive");
    }

    None
}



pub(crate) fn exchange_announcement_order_capacity_probe_is_truncated(probe: &Value) -> bool {
    let row_count = probe.get("row_count").and_then(Value::as_i64).unwrap_or(0);
    let sample_count = probe
        .get("sample_rows")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as i64)
        .unwrap_or(0);
    row_count > sample_count
}



pub(crate) fn exchange_announcement_raw_row_from_list_and_pdf_probe(
    list_row: &Value,
    category: &str,
    request_key: &str,
    pdf_probe: &Value,
    open_dates: &[NaiveDate],
    data_version_id: &str,
) -> Result<ExchangeAnnouncementOrderCapacityRawRow, String> {
    let announcement_url = list_row
        .get("公告链接")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing_announcement_url".to_string())?
        .trim()
        .to_string();
    let metadata = parse_cninfo_announcement_link_metadata(&announcement_url);
    if !metadata
        .get("metadata_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err("incomplete_link_metadata".to_string());
    }
    let announcement_id = metadata
        .get("announcement_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_announcement_id".to_string())?
        .to_string();
    let org_id = metadata
        .get("org_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let symbol = metadata
        .get("stock_code")
        .and_then(Value::as_str)
        .or_else(|| list_row.get("代码").and_then(Value::as_str))
        .ok_or_else(|| "missing_symbol".to_string())?
        .to_string();
    let announcement_time_raw = metadata
        .get("announcement_time")
        .and_then(Value::as_str)
        .or_else(|| list_row.get("公告时间").and_then(Value::as_str))
        .ok_or_else(|| "missing_announcement_time".to_string())?;
    let announcement_time = parse_exchange_announcement_date(announcement_time_raw)
        .ok_or_else(|| format!("invalid_announcement_time:{announcement_time_raw}"))?;
    let available_at = exchange_announcement_next_open_date(announcement_time, open_dates);

    let raw_payload = list_row.clone();
    let raw_payload_hash = akshare_stable_hash(&[
        "akshare".to_string(),
        "stock_zh_a_disclosure_report_cninfo".to_string(),
        announcement_id.clone(),
        symbol.clone(),
        serde_json::to_string(&raw_payload).unwrap_or_default(),
    ]);
    let evidence_spans = pdf_probe
        .get("evidence_spans")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let announcement_title = list_row
        .get("公告标题")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let event_type =
        exchange_announcement_event_type_from_title_and_spans(&announcement_title, &evidence_spans);
    let parser_errors = pdf_probe
        .get("parser_errors")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let timestamp_candidates = pdf_probe
        .get("timestamp_candidates")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let pdf_metadata_keys = pdf_probe
        .get("pdf_metadata_keys")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let source_published_at = pdf_probe
        .get("source_published_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(announcement_time_raw)
        .to_string();
    let parsed_source_published_at_ts = parse_exchange_announcement_timestamp(&source_published_at);
    let declared_source_published_at_quality = pdf_probe
        .get("source_published_at_quality")
        .and_then(Value::as_str)
        .filter(|quality| matches!(*quality, "timestamp" | "date_only_next_session"))
        .unwrap_or("date_only_next_session");
    let source_published_at_quality = if parsed_source_published_at_ts.is_some() {
        "timestamp".to_string()
    } else {
        declared_source_published_at_quality.to_string()
    };
    let (source_published_at_ts, source_published_date) =
        if source_published_at_quality == "timestamp" {
            (parsed_source_published_at_ts, None)
        } else {
            (None, Some(announcement_time))
        };
    let text_content = pdf_probe
        .get("text_sample")
        .and_then(Value::as_str)
        .map(str::to_string);
    let text_hash = pdf_probe
        .get("text_hash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let parser_used = pdf_probe
        .get("parser_used")
        .and_then(Value::as_str)
        .map(str::to_string);
    let pdf_final_url = pdf_probe
        .get("final_url")
        .and_then(Value::as_str)
        .map(str::to_string);

    Ok(ExchangeAnnouncementOrderCapacityRawRow {
        vendor: "akshare".to_string(),
        vendor_endpoint: "stock_zh_a_disclosure_report_cninfo".to_string(),
        request_key: request_key.to_string(),
        symbol,
        symbol_name: list_row
            .get("简称")
            .and_then(Value::as_str)
            .map(str::to_string),
        announcement_id,
        org_id,
        announcement_category: category.to_string(),
        announcement_title,
        announcement_time,
        source_published_at,
        source_published_at_ts,
        source_published_date,
        source_published_at_quality,
        available_at,
        announcement_url,
        pdf_final_url,
        text_content,
        text_hash,
        timestamp_candidates,
        pdf_metadata_keys,
        raw_payload,
        raw_payload_hash,
        parser_used,
        parser_version: None,
        parser_errors,
        pdf_parse_status: exchange_announcement_pdf_parse_status(pdf_probe),
        event_type,
        evidence_spans,
    })
    .map(|mut row| {
        row.raw_payload = json!({
            "list_row": row.raw_payload,
            "pdf_probe_status": pdf_probe.get("status").cloned().unwrap_or(Value::Null),
            "data_version_id": data_version_id,
        });
        row
    })
}



pub(crate) fn decide_exchange_announcement_order_capacity_coverage_quality_audit(
    metrics: ExchangeAnnouncementOrderCapacityCoverageQualityMetrics,
) -> Value {
    let raw_quality_failed = metrics.pit_violation_rows > 0
        || metrics.missing_available_at_rows > 0
        || metrics.missing_source_published_at_quality_rows > 0
        || metrics.duplicate_announcement_id_rows > 0
        || metrics.duplicate_raw_payload_hash_groups > 0;
    let admissible_target_event_rows =
        (metrics.target_event_rows - metrics.taxonomy_blocked_target_event_rows).max(0);

    let (status, admission_decision, next_step) = if !metrics.table_exists {
        (
            "blocked_raw_schema_not_applied",
            "blocked_raw_schema_not_applied_no_coverage_to_audit",
            "apply_sql_phase7_exchange_announcement_order_capacity_source_then_rerun_audit",
        )
    } else if metrics.failed_attempts > 0 {
        (
            "blocked_failed_sync_attempts_present",
            "blocked_until_failed_small_batch_attempts_are_repaired",
            "repair_failed_request_keys_then_rerun_coverage_quality_audit",
        )
    } else if metrics.row_count <= 0 && metrics.completed_attempts <= 0 {
        (
            "raw_table_present_bounded_sync_required",
            "bounded_sync_required_before_coverage_quality_audit",
            "run_one_tiny_exchange_announcement_raw_sync_then_rerun_audit",
        )
    } else if metrics.row_count <= 0 {
        (
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only",
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only",
            "continue_next_tiny_slice_or_batch_then_rerun_full_window_audit",
        )
    } else if raw_quality_failed {
        (
            "blocked_raw_pit_or_quality_failed",
            "blocked_until_pit_duplicate_or_source_quality_is_repaired",
            "repair_or_exclude_bad_raw_rows_before_expanding_sync",
        )
    } else if metrics.trainable_scanned_pdf_blocking_rows > 0 {
        (
            "blocked_scanned_pdf_ocr_required_rows_present",
            "blocked_until_scanned_pdf_ocr_runtime_and_audit_pass",
            "run_separate_ocr_runtime_quality_audit_or_exclude_scanned_pdf_rows",
        )
    } else if metrics.taxonomy_blocked_target_event_rows > 0 {
        (
            "blocked_event_taxonomy_precision_gate_failed",
            "blocked_until_category_aware_taxonomy_precision_manual_review_passes",
            "manually_review_or_exclude_taxonomy_risk_category_target_events_before_expansion",
        )
    } else if metrics.target_event_missing_evidence_span_rows > 0 {
        (
            "blocked_target_event_rows_missing_text_evidence_spans",
            "blocked_until_target_event_evidence_spans_are_repaired",
            "repair_or_exclude_target_event_rows_without_evidence_spans",
        )
    } else if metrics.target_event_rows <= 0 {
        (
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only",
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only",
            "continue_bounded_sync_and_track_target_event_yield",
        )
    } else {
        (
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
            "expand_by_month_or_quarter_then_rerun_coverage_quality_audit",
        )
    };

    json!({
        "status": status,
        "admission_decision": admission_decision,
        "next_step": next_step,
        "summary": {
            "table_exists": metrics.table_exists,
            "row_count": metrics.row_count,
            "distinct_symbol_count": metrics.distinct_symbol_count,
            "distinct_category_count": metrics.distinct_category_count,
            "pit_violation_rows": metrics.pit_violation_rows,
            "missing_available_at_rows": metrics.missing_available_at_rows,
            "missing_source_published_at_quality_rows": metrics.missing_source_published_at_quality_rows,
            "duplicate_announcement_id_rows": metrics.duplicate_announcement_id_rows,
            "duplicate_raw_payload_hash_groups": metrics.duplicate_raw_payload_hash_groups,
            "evidence_span_rows": metrics.evidence_span_rows,
            "target_event_rows": metrics.target_event_rows,
            "target_event_missing_evidence_span_rows": metrics.target_event_missing_evidence_span_rows,
            "scanned_pdf_ocr_required_rows": metrics.scanned_pdf_ocr_required_rows,
            "ocr_taxonomy_excluded_rows": metrics.ocr_taxonomy_excluded_rows,
            "trainable_scanned_pdf_blocking_rows": metrics.trainable_scanned_pdf_blocking_rows,
            "taxonomy_blocked_target_event_rows": metrics.taxonomy_blocked_target_event_rows,
            "taxonomy_risk_category_rows": metrics.taxonomy_risk_category_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "completed_attempts": metrics.completed_attempts,
            "completed_empty_attempts": metrics.completed_empty_attempts,
            "failed_attempts": metrics.failed_attempts,
            "total_failed_attempts": metrics.total_failed_attempts,
            "excluded_unsupported_category_failed_attempts": metrics.excluded_unsupported_category_failed_attempts,
        },
        "attempt_failure_gate": {
            "status": if metrics.failed_attempts > 0 {
                "blocked"
            } else if metrics.excluded_unsupported_category_failed_attempts > 0 {
                "passed_with_excluded_unsupported_category_parser_failures"
            } else {
                "passed_or_not_observed"
            },
            "blocking_failed_attempts": metrics.failed_attempts,
            "total_failed_attempts": metrics.total_failed_attempts,
            "excluded_unsupported_category_failed_attempts": metrics.excluded_unsupported_category_failed_attempts,
            "policy": "failed attempts in the admitted symbol/category/date scope block admission; pre-registered unsupported category parser failures are retained as evidence but do not block the current admitted scope"
        },
        "ocr_quality_gate": {
            "status": if metrics.trainable_scanned_pdf_blocking_rows > 0 {
                "blocked"
            } else if metrics.ocr_taxonomy_excluded_rows > 0 {
                "passed_with_taxonomy_exclusions_only"
            } else {
                "passed_or_not_observed"
            },
            "scanned_pdf_ocr_required_rows": metrics.scanned_pdf_ocr_required_rows,
            "ocr_taxonomy_excluded_rows": metrics.ocr_taxonomy_excluded_rows,
            "trainable_scanned_pdf_blocking_rows": metrics.trainable_scanned_pdf_blocking_rows,
            "policy": "scanned_pdf rows are retained as raw evidence; only narrow, audited non-target OCR taxonomy exclusions stop blocking coverage, unresolved scanned PDFs remain blocked"
        },
        "taxonomy_precision_gate": {
            "status": if metrics.taxonomy_blocked_target_event_rows > 0 { "blocked" } else { "passed_or_not_observed" },
            "taxonomy_risk_category_rows": metrics.taxonomy_risk_category_rows,
            "taxonomy_blocked_target_event_rows": metrics.taxonomy_blocked_target_event_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "policy": "category-risk target events are taxonomy precision audit material only and cannot be counted as trainable positive labels"
        },
        "bounded_sync": if admission_decision == "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only" {
            "expand_bounded_sync_by_month_or_quarter_only"
        } else if admission_decision == "synced_empty_no_event_rows_passed_for_coverage_accounting_only" {
            "continue_bounded_sync_for_coverage_accounting_only"
        } else if admission_decision == "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only" {
            "continue_bounded_sync_and_track_target_event_yield"
        } else {
            "blocked_until_small_batch_audit_passes"
        },
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
    })
}



pub(crate) fn exchange_announcement_order_capacity_target_event_yield_report(
    raw_row_count: i64,
    target_event_rows: i64,
    target_event_with_evidence_span_rows: i64,
    target_event_missing_evidence_span_rows: i64,
    taxonomy_blocked_target_event_rows: i64,
    scanned_pdf_ocr_required_rows: i64,
    ocr_taxonomy_excluded_rows: i64,
    trainable_scanned_pdf_blocking_rows: i64,
) -> Value {
    let admissible_target_event_rows =
        (target_event_rows - taxonomy_blocked_target_event_rows).max(0);
    json!({
        "raw_row_count": raw_row_count,
        "target_event_rows": target_event_rows,
        "admissible_target_event_rows": admissible_target_event_rows,
        "taxonomy_blocked_target_event_rows": taxonomy_blocked_target_event_rows,
        "scanned_pdf_ocr_required_rows": scanned_pdf_ocr_required_rows,
        "ocr_taxonomy_excluded_rows": ocr_taxonomy_excluded_rows,
        "trainable_scanned_pdf_blocking_rows": trainable_scanned_pdf_blocking_rows,
        "non_target_event_rows": (raw_row_count - target_event_rows).max(0),
        "target_event_with_evidence_span_rows": target_event_with_evidence_span_rows,
        "target_event_missing_evidence_span_rows": target_event_missing_evidence_span_rows,
        "target_event_yield_ratio": phase7_ratio(target_event_rows, raw_row_count),
        "admissible_target_event_yield_ratio": phase7_ratio(
            admissible_target_event_rows,
            raw_row_count,
        ),
        "target_event_evidence_span_coverage_ratio": phase7_ratio(
            target_event_with_evidence_span_rows,
            target_event_rows,
        ),
        "admission_scope": "raw_coverage_taxonomy_accounting_only",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
    })
}



pub(crate) fn exchange_announcement_order_capacity_admission_readiness_report(coverage: &Value) -> Value {
    let row_count =
        exchange_announcement_order_capacity_json_path_i64(coverage, &["summary", "row_count"]);
    let target_event_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "target_event_rows"],
    );
    let admissible_target_event_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "admissible_target_event_rows"],
    );
    let pit_violation_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "pit_violation_rows"],
    );
    let duplicate_announcement_id_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "duplicate_announcement_id_rows"],
    );
    let duplicate_raw_payload_hash_groups = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "duplicate_raw_payload_hash_groups"],
    );
    let failed_attempts = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "failed_attempts"],
    );
    let trainable_scanned_pdf_blocking_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "trainable_scanned_pdf_blocking_rows"],
    );
    let missing_evidence_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "target_event_missing_evidence_span_rows"],
    );
    let evidence_span_coverage = exchange_announcement_order_capacity_json_path_f64(
        coverage,
        &[
            "target_event_yield",
            "target_event_evidence_span_coverage_ratio",
        ],
    )
    .unwrap_or(0.0);
    let year_category_count =
        exchange_announcement_order_capacity_array_len(coverage, "year_category_breakdown");
    let symbol_breakdown_count =
        exchange_announcement_order_capacity_array_len(coverage, "symbol_event_breakdown");

    let raw_gate_passed = row_count > 0
        && target_event_rows > 0
        && admissible_target_event_rows > 0
        && pit_violation_rows == 0
        && duplicate_announcement_id_rows == 0
        && duplicate_raw_payload_hash_groups == 0
        && failed_attempts == 0
        && trainable_scanned_pdf_blocking_rows == 0
        && missing_evidence_rows == 0
        && evidence_span_coverage >= 1.0;

    let effective_coverage_status =
        if raw_gate_passed && year_category_count >= 1 && symbol_breakdown_count >= 4 {
            "pilot_scope_only_not_full_history"
        } else {
            "blocked_until_broader_pre_registered_coverage_passes"
        };

    json!({
        "audit_version": "p3.24w-exchange-announcement-order-capacity-admission-readiness-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24W",
        "mode": "read_only_admission_readiness_no_factor_no_p310_no_wfa",
        "coverage_pit_quality_gate": {
            "status": if raw_gate_passed {
                "passed_pilot_scope"
            } else {
                "blocked_until_coverage_pit_quality_passes"
            },
            "row_count": row_count,
            "target_event_rows": target_event_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "pit_violation_rows": pit_violation_rows,
            "duplicate_announcement_id_rows": duplicate_announcement_id_rows,
            "duplicate_raw_payload_hash_groups": duplicate_raw_payload_hash_groups,
            "failed_attempts": failed_attempts,
            "trainable_scanned_pdf_blocking_rows": trainable_scanned_pdf_blocking_rows,
            "target_event_missing_evidence_span_rows": missing_evidence_rows,
            "target_event_evidence_span_coverage_ratio": evidence_span_coverage,
        },
        "effective_coverage_gate": {
            "status": effective_coverage_status,
            "year_category_breakdown_count": year_category_count,
            "symbol_breakdown_count": symbol_breakdown_count,
            "current_scope": "bounded pilot scope; not yet full-history or formally pre-registered broad coverage",
            "required_before_p310": "pre-register target universe/date range/category scope and prove coverage/readiness across that scope"
        },
        "manual_evidence_span_precision_gate": {
            "status": "blocked_manual_review_required",
            "required_precision_min": 0.80,
            "required_sample_size_min": 50,
            "current_machine_evidence_coverage": evidence_span_coverage,
            "policy": "machine spans prove text anchoring, not human semantic precision"
        },
        "event_taxonomy_precision_gate": {
            "status": "blocked_manual_review_required",
            "required_precision_min": 0.80,
            "required_sample_size_min": 50,
            "policy": "manual review must confirm order/capacity/price/commissioning labels and negative exclusions before trainable rows"
        },
        "correlation_gate": {
            "status": "blocked_correlation_audit_required",
            "max_abs_correlation_threshold": 0.30,
            "reference_families": [
                "moneyflow_congestion",
                "liquidity",
                "price_volume",
                "financial_quality_change",
                "earnings_recovery_persistence",
                "event_overlay",
                "shareholder_structure"
            ],
            "pit_alignment": "must join raw event features by conservative available_at, not announcement_time"
        },
        "promotion_gate": {
            "factor_builder": "blocked_until_admission_readiness_passes",
            "p310_status": "blocked_until_manual_precision_effective_coverage_and_correlation_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if raw_gate_passed {
            "run_manual_evidence_span_and_taxonomy_precision_review_then_low_correlation_audit_before_p310"
        } else {
            "repair_or_extend_raw_coverage_pit_quality_before_admission_readiness"
        },
        "coverage_audit": coverage,
    })
}



pub(crate) fn exchange_announcement_order_capacity_manual_precision_sample_report(
    admissible_target_event_rows: i64,
    required_target_sample_size: i64,
    target_sample_rows: i64,
    negative_sample_rows: i64,
    review_items: Vec<Value>,
) -> Value {
    let target_sample_shortfall = (required_target_sample_size - target_sample_rows).max(0);
    let status = if target_sample_shortfall > 0 {
        "blocked_insufficient_target_review_sample"
    } else {
        "manual_review_sample_ready"
    };

    json!({
        "audit_version": "p3.24x-exchange-announcement-order-capacity-manual-precision-sample-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24X",
        "mode": "read_only_manual_review_sample_no_labels_no_factor_no_p310",
        "status": status,
        "admissible_target_event_rows": admissible_target_event_rows,
        "required_target_sample_size_min": required_target_sample_size,
        "target_sample_rows": target_sample_rows,
        "negative_sample_rows": negative_sample_rows,
        "target_sample_shortfall": target_sample_shortfall,
        "manual_evidence_span_precision_gate": {
            "status": "blocked_until_human_labels_are_recorded",
            "required_precision_min": 0.80,
            "required_sample_size_min": required_target_sample_size,
            "review_labels_required": [
                "evidence_span_correct",
                "evidence_span_wrong_or_too_broad",
                "insufficient_context"
            ],
            "policy": "this endpoint creates a deterministic review sample only; it cannot certify precision without persisted human labels"
        },
        "event_taxonomy_precision_gate": {
            "status": "blocked_until_human_labels_are_recorded",
            "required_precision_min": 0.80,
            "required_sample_size_min": required_target_sample_size,
            "review_labels_required": [
                "taxonomy_correct_target_event",
                "taxonomy_false_positive",
                "taxonomy_uncertain"
            ],
            "policy": "target-event labels and negative exclusions require human review before trainable rows"
        },
        "promotion_gate": {
            "factor_builder": "blocked_until_manual_precision_review_passes",
            "p310_status": "blocked_until_manual_precision_review_passes",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if status == "manual_review_sample_ready" {
            "record_human_labels_for_sample_then_compute_precision_before_correlation_audit"
        } else {
            "expand_pre_registered_coverage_until_minimum_target_review_sample_is_available"
        },
        "review_items": review_items,
    })
}



pub(crate) fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap() - Duration::days(1)
}



pub(crate) fn akshare_analyst_revision_sync_plan_batches(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
) -> Result<Vec<AkshareAnalystRevisionSyncPlanBatch>, String> {
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }
    if !matches!(batch_mode, "year" | "quarter" | "month") {
        return Err(
            "AkShare analyst revision sync-plan batch must be year, quarter, or month".to_string(),
        );
    }

    let mut batches = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let batch_end = akshare_analyst_revision_batch_end(cursor, batch_mode).min(end);
        batches.push(AkshareAnalystRevisionSyncPlanBatch {
            label: akshare_analyst_revision_batch_label(cursor, batch_mode),
            start_date: cursor,
            end_date: batch_end,
            calendar_day_count: (batch_end - cursor).num_days() + 1,
        });
        cursor = batch_end + Duration::days(1);
    }

    if batches.len() > AKSHARE_ANALYST_REVISION_SYNC_PLAN_MAX_BATCHES {
        return Err(format!(
            "AkShare analyst revision sync-plan resolved {} batches, above max {}",
            batches.len(),
            AKSHARE_ANALYST_REVISION_SYNC_PLAN_MAX_BATCHES
        ));
    }
    Ok(batches)
}



pub(crate) fn akshare_stable_hash(parts: &[String]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}



pub(crate) fn akshare_analyst_revision_raw_row_from_record(
    record: &serde_json::Map<String, Value>,
    request_key: &str,
    open_dates: &[NaiveDate],
) -> Result<AkshareAnalystRevisionRawRow, String> {
    let symbol = akshare_value_key_part(record, "证券代码");
    if symbol.is_empty() {
        return Err("missing_symbol".to_string());
    }
    let publication_date =
        parse_akshare_publication_date(&akshare_value_key_part(record, "发布日期"))
            .ok_or_else(|| "missing_or_invalid_publication_date".to_string())?;
    if publication_date.format("%Y%m%d").to_string() != request_key {
        return Err(format!(
            "publication_date_mismatch:{}",
            publication_date.format("%Y%m%d")
        ));
    }
    let available_at = akshare_next_open_date(publication_date, open_dates);
    let raw_payload = Value::Object(record.clone());
    let raw_payload_string = serde_json::to_string(&raw_payload).unwrap_or_default();
    let raw_payload_hash = akshare_stable_hash(&[
        "akshare".to_string(),
        "stock_rank_forecast_cninfo".to_string(),
        request_key.to_string(),
        symbol.clone(),
        raw_payload_string,
    ]);

    Ok(AkshareAnalystRevisionRawRow {
        vendor: "akshare".to_string(),
        vendor_source: "akshare".to_string(),
        vendor_endpoint: "stock_rank_forecast_cninfo".to_string(),
        request_key: request_key.to_string(),
        symbol,
        symbol_name: akshare_optional_string(record, "证券简称"),
        publication_date,
        source_published_at: akshare_source_published_at(available_at),
        available_at,
        institution_name: akshare_optional_string(record, "研究机构简称"),
        analyst_name: akshare_optional_string(record, "研究员名称"),
        rating_current: akshare_optional_string(record, "投资评级"),
        rating_previous: akshare_optional_string(record, "前一次投资评级"),
        rating_change: akshare_optional_string(record, "评级变化"),
        is_first_rating: akshare_optional_string(record, "是否首次评级"),
        target_price_min: akshare_optional_decimal(record, "目标价格-下限"),
        target_price_max: akshare_optional_decimal(record, "目标价格-上限"),
        raw_payload,
        raw_payload_hash,
    })
}



pub(crate) fn validate_akshare_analyst_revision_sync_range(
    start: NaiveDate,
    end: NaiveDate,
) -> Result<i64, String> {
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }
    let calendar_day_count = (end - start).num_days() + 1;
    if calendar_day_count > AKSHARE_ANALYST_REVISION_SYNC_MAX_CALENDAR_DAYS {
        return Err(format!(
            "AkShare analyst revision bounded sync resolved {} calendar days, above max {}. Use monthly or <=100-day batches.",
            calendar_day_count, AKSHARE_ANALYST_REVISION_SYNC_MAX_CALENDAR_DAYS
        ));
    }
    Ok(calendar_day_count)
}



pub(crate) fn akshare_analyst_revision_should_retry_fetch_status(status: &str) -> bool {
    matches!(status, "timeout" | "error")
}



pub(crate) fn safe_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}



pub(crate) fn decide_broad_analyst_revision_audit(
    available_at_rule_violations: i64,
    union_symbol_coverage_ratio: f64,
    forecast_symbol_coverage_ratio: f64,
    revised_symbol_coverage_ratio: f64,
    revised_symbol_period_ratio: f64,
) -> BroadAnalystRevisionAuditDecision {
    if available_at_rule_violations > 0 {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_available_at_rule_violation",
            readiness: "blocked_pit_available_at_repair_required",
            admission_decision: "blocked_broad_analyst_revision_available_at_rule_failed",
            p310_status: "not_started",
            blocked_reason:
                "one_or_more_event_source_rows_do_not_follow_the_registered_available_at_policy",
        }
    } else if union_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_undercovered_for_broad_base_revision",
            readiness: "blocked_full_history_coverage_not_broad_enough",
            admission_decision: "blocked_broad_analyst_revision_after_full_history_coverage_audit",
            p310_status: "not_started",
            blocked_reason: "forecast_express_disclosure_sources_do_not_cover_enough_symbols_for_broad_base_revision",
        }
    } else if forecast_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE
        || revised_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE
        || revised_symbol_period_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO
    {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_sparse_revision_semantics",
            readiness: "stopped_current_raw_bundle_revision_semantics_too_sparse",
            admission_decision: "stopped_broad_analyst_revision_current_raw_bundle_after_audit_sparse_revision_semantics",
            p310_status: "not_started",
            blocked_reason: "true_forecast_revision_events_are_too_sparse_and_would_degenerate_into_event_overlay",
        }
    } else {
        BroadAnalystRevisionAuditDecision {
            passed: true,
            status: "full_history_revision_semantics_audit_passed",
            readiness: "ready_for_p310_diagnostics_only",
            admission_decision:
                "coverage_available_at_revision_semantics_passed_p310_required_next",
            p310_status: "not_started",
            blocked_reason: "",
        }
    }
}



pub(crate) fn decide_akshare_analyst_revision_history_replay_audit(
    requested_date_count: usize,
    available_date_count: usize,
    error_date_count: usize,
    empty_date_count: usize,
    row_count: i64,
    publication_date_mismatch_rows: i64,
    missing_publication_date_rows: i64,
    missing_revision_semantics_rows: i64,
) -> Value {
    let (passed, status, admission_decision, blocked_reason) = if requested_date_count == 0 {
        (
            false,
            "blocked_no_history_dates_requested",
            "blocked_no_history_dates_requested",
            "history replay needs explicit dates or a market-calendar year range",
        )
    } else if error_date_count > 0 {
        (
            false,
            "blocked_history_replay_probe_failed",
            "blocked_history_replay_probe_failed",
            "one or more AkShare history-date probes failed or timed out",
        )
    } else if empty_date_count > 0 || available_date_count < requested_date_count {
        (
            false,
            "blocked_history_replay_empty_dates",
            "blocked_history_replay_empty_dates",
            "one or more representative history dates returned no analyst revision rows",
        )
    } else if row_count <= 0 {
        (
            false,
            "blocked_history_replay_no_rows",
            "blocked_history_replay_no_rows",
            "history replay returned no rows",
        )
    } else if publication_date_mismatch_rows > 0 || missing_publication_date_rows > 0 {
        (
            false,
            "blocked_publication_date_mismatch_or_missing",
            "blocked_publication_date_mismatch_or_missing",
            "source publication date must equal the requested history date and be non-null",
        )
    } else if missing_revision_semantics_rows > 0 {
        (
            false,
            "blocked_revision_semantics_missing_fields",
            "blocked_revision_semantics_missing_fields",
            "rating_change and previous_rating fields must be populated before schema review",
        )
    } else {
        (
            true,
            "history_replay_available_at_sample_passed",
            "history_replay_available_at_sample_passed_schema_review_next",
            "",
        )
    };

    json!({
        "passed": passed,
        "status": status,
        "admission_decision": admission_decision,
        "blocked_reason": if blocked_reason.is_empty() { Value::Null } else { json!(blocked_reason) },
        "promotion_gate": {
            "schema_apply": if passed {
                "schema_review_allowed_next"
            } else {
                "blocked_until_history_replay_audit_passes"
            },
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}



pub(crate) fn decide_akshare_analyst_revision_readiness(
    schema_exists: bool,
    row_count: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    missing_current_rating_rows: i64,
    missing_revision_semantics_rows: i64,
    duplicate_key_rows: i64,
) -> Value {
    let (passed, status, admission_decision, blocked_reason) = if !schema_exists {
        (
            false,
            "schema_not_applied",
            "schema_review_apply_required_before_bounded_sync",
            "market_vendor_analyst_revision_raw does not exist",
        )
    } else if row_count <= 0 {
        (
            false,
            "schema_created_sync_not_started",
            "bounded_sync_required_before_coverage_audit",
            "raw schema exists but contains no analyst revision rows",
        )
    } else if pit_violation_rows > 0 || missing_source_published_at_rows > 0 {
        (
            false,
            "raw_pit_failed",
            "raw_pit_or_source_published_at_failed",
            "raw rows must have publication_date/source_published_at and available_at >= publication_date",
        )
    } else if missing_revision_semantics_rows > 0 {
        (
            false,
            "raw_revision_semantics_failed",
            "raw_revision_semantics_failed",
            "rating_previous/rating_change must be present before coverage admission; missing current ratings are audited separately and must be excluded or downweighted before current-rating factor use",
        )
    } else if duplicate_key_rows > 0 {
        (
            false,
            "raw_duplicate_key_failed",
            "raw_duplicate_key_failed",
            "natural key plus raw_payload_hash must not produce duplicate rows",
        )
    } else {
        (
            true,
            "raw_readiness_passed_coverage_audit_required_next",
            "raw_schema_and_pit_ready_for_coverage_audit_only",
            "",
        )
    };

    json!({
        "passed": passed,
        "status": status,
        "admission_decision": admission_decision,
        "blocked_reason": if blocked_reason.is_empty() { Value::Null } else { json!(blocked_reason) },
        "row_quality": {
            "missing_current_rating_rows": missing_current_rating_rows,
            "current_rating_usage": if missing_current_rating_rows > 0 {
                "exclude_or_downweight_rows_before_current_rating_factor_use"
            } else {
                "fully_populated"
            },
            "revision_semantics_required_fields": ["rating_previous", "rating_change"]
        },
        "promotion_gate": {
            "bounded_sync": if schema_exists {
                "schema_exists_bounded_sync_can_be_considered"
            } else {
                "blocked_until_schema_review_and_apply"
            },
            "coverage_audit": if passed {
                "coverage_audit_required_next"
            } else {
                "blocked_until_readiness_passes"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}



pub(crate) fn decide_akshare_analyst_revision_coverage_audit(
    table_exists: bool,
    row_count: i64,
    coverage_ratio: f64,
    failed_attempt_dates: i64,
    missing_year_count: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    missing_revision_semantics_rows: i64,
    duplicate_key_rows: i64,
    duplicate_payload_hash_rows: i64,
    correlation_decision: &str,
) -> Value {
    let raw_quality_failed = pit_violation_rows > 0
        || missing_source_published_at_rows > 0
        || missing_revision_semantics_rows > 0
        || duplicate_key_rows > 0
        || duplicate_payload_hash_rows > 0;
    let (status, admission_decision, p310_status, next_step) = if !table_exists {
        (
            "blocked_no_raw_schema_or_full_history_sync",
            "blocked_raw_schema_not_applied_no_coverage_to_audit",
            "blocked",
            "apply_sql_phase7_akshare_analyst_revision_source_then_rerun_coverage_audit",
        )
    } else if row_count <= 0 {
        (
            "raw_table_present_bounded_sync_required",
            "bounded_sync_required_before_coverage_audit",
            "blocked",
            "run_bounded_calendar_day_raw_sync_before_coverage_audit",
        )
    } else if failed_attempt_dates > 0 {
        (
            "blocked_failed_sync_attempts_present",
            "blocked_until_failed_dates_are_repaired_and_rerun",
            "blocked",
            "repair_failed_dates_with_same_sync_endpoint_then_rerun_coverage_audit",
        )
    } else if coverage_ratio + f64::EPSILON < 1.0 || missing_year_count > 0 {
        (
            "blocked_incomplete_calendar_coverage",
            "blocked_until_full_history_calendar_coverage_passes",
            "blocked",
            "continue_month_or_quarter_bounded_raw_sync_then_rerun_coverage_audit",
        )
    } else if raw_quality_failed {
        (
            "blocked_raw_pit_source_revision_or_duplicate_quality_failed",
            "blocked_until_pit_source_published_at_revision_semantics_and_duplicate_hash_audit_passes",
            "blocked",
            "repair_or_exclude_bad_raw_rows_before_p310_diagnostics",
        )
    } else if correlation_decision != "passed_low_linear_correlation_screen" {
        (
            "blocked_correlation_screen_not_passed_or_needs_review",
            "blocked_until_moneyflow_liquidity_price_volume_correlation_audit_passes",
            "blocked",
            "complete_low_correlation_review_before_p310_diagnostics",
        )
    } else {
        (
            "coverage_pit_quality_correlation_ready_for_p310_diagnostics",
            "coverage_pit_quality_correlation_passed_p310_diagnostics_required_next",
            "ready_for_p310_diagnostics_only",
            "run_p310a_d_rankic_group_decay_turnover_capacity_regime_exposure_diagnostics",
        )
    };

    json!({
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": p310_status,
        "next_step": next_step,
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "factor_builder": "blocked_until_p310_diagnostics_passes",
    })
}



pub(crate) fn main_business_raw_source_readiness(
    row_count: i64,
    expected_periods: usize,
    completed_periods: usize,
    failed_periods: usize,
    pit_violation_rows: i64,
) -> &'static str {
    if row_count <= 0 {
        return "raw_source_missing";
    }
    if pit_violation_rows > 0 {
        return "raw_source_pit_failed";
    }
    if completed_periods < expected_periods {
        return "period_sync_incomplete";
    }
    if failed_periods > 0 {
        return "period_sync_failed";
    }
    "raw_source_ready_for_full_history_coverage_audit"
}



pub(crate) fn main_business_readiness_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT symbol)::bigint AS symbol_count,
        COUNT(DISTINCT end_date)::bigint AS distinct_periods,
        MIN(end_date) AS min_end_date,
        MAX(end_date) AS max_end_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < end_date)::bigint AS pit_violation_rows
    FROM market_stock_main_business
    WHERE end_date BETWEEN $1 AND $2
      AND business_type = $3
    "#
}



pub(crate) fn main_business_missing_available_at_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "missing_available_at_rows=")
}



pub(crate) fn main_business_out_of_universe_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "out_of_universe_rows=")
}



pub(crate) fn decide_main_business_available_at_join_audit(
    total_periods: usize,
    mappings: &[MainBusinessPeriodMapping],
) -> MainBusinessAvailableAtJoinDecision {
    let explicit_missing = mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_none())
        .count();
    let implicit_missing = total_periods.saturating_sub(mappings.len());
    let missing_mapping_count = explicit_missing + implicit_missing;
    let pit_violation_count = mappings
        .iter()
        .filter(|mapping| {
            mapping
                .available_at
                .map(|available_at| available_at < mapping.end_date)
                .unwrap_or(false)
        })
        .count();
    let mut source_counts = BTreeMap::new();
    for mapping in mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_some())
    {
        let source = mapping
            .source
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        *source_counts.entry(source).or_insert(0) += 1;
    }

    let (passed, status, readiness) = if total_periods == 0 {
        (
            false,
            "blocked_no_sample_periods",
            "blocked_available_at_join_audit_required",
        )
    } else if pit_violation_count > 0 {
        (
            false,
            "blocked_pit_available_at_violations",
            "blocked_available_at_join_audit_required",
        )
    } else if missing_mapping_count > 0 {
        (
            false,
            "blocked_available_at_join_gaps",
            "blocked_available_at_join_audit_required",
        )
    } else {
        (true, "passed", "available_at_join_ready_for_schema_design")
    };

    MainBusinessAvailableAtJoinDecision {
        passed,
        status,
        readiness,
        missing_mapping_count,
        pit_violation_count,
        source_counts,
    }
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



fn phase7_share_float_chunk_granularity(granularity: Option<&str>) -> &'static str {
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



fn phase7_share_float_coverage_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}



fn phase7_share_float_coverage_max_chunks(max_chunks: Option<usize>) -> usize {
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



fn phase7_share_float_date_chunks(
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



fn phase7_share_float_readiness_sql() -> &'static str {
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



fn phase7_share_float_expected_days(start: NaiveDate, end: NaiveDate) -> i64 {
    end.signed_duration_since(start)
        .num_days()
        .saturating_add(1)
}



fn phase7_share_float_covered_days_from_windows(
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



fn phase7_share_float_feature_readiness(
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



fn phase7_coverage_runner_sources(requested: &[String]) -> Result<Vec<String>, String> {
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



pub(crate) fn phase7_financial_source_readiness(coverage_grade: &str) -> &'static str {
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



fn phase7_optional_source_table(source: &str) -> Option<&'static str> {
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



pub(crate) fn normalize_exchange_announcement_order_capacity_probe_payload(payload: Value) -> Value {
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
            "error_type": payload.get("error_type").cloned().unwrap_or_else(|| json!(null)),
            "error": payload.get("error").cloned().unwrap_or_else(|| json!(null)),
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

#[derive(Debug, Clone, Copy)]


struct Phase7OptionalSourceSpec {
    source: &'static str,
    table: &'static str,
    next_feature: &'static str,
}



fn phase7_optional_source_specs() -> &'static [Phase7OptionalSourceSpec] {
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
    pub(crate) data_rows:i64,
    pub(crate) min_trade_date:Option<NaiveDate>,
    pub(crate) latest_trade_date:Option<NaiveDate>,
    pub(crate) open_day_lag:Option<i64>,
}

#[derive(Debug, Clone)]


pub(crate) struct Phase7MarketLevelSyncAudit {
    pub(crate) task_id:String,
    pub(crate) task_type:String,
    pub(crate) start_date:Option<NaiveDate>,
    pub(crate) end_date:Option<NaiveDate>,
    pub(crate) status:String,
    pub(crate) total_count:i32,
    pub(crate) success_count:i32,
    pub(crate) failed_count:i32,
    error_message: Option<String>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy)]


pub(crate) struct Phase7BlockTradeSourceAudit {
    pub(crate) data_rows:i64,
    pub(crate) symbols:i64,
    pub(crate) covered_trade_days:i64,
    pub(crate) open_days_in_range:i64,
    pub(crate) min_trade_date:Option<NaiveDate>,
    pub(crate) latest_trade_date:Option<NaiveDate>,
    pub(crate) min_available_at:Option<NaiveDate>,
    pub(crate) latest_available_at:Option<NaiveDate>,
    pit_violation_rows: i64,
}

#[derive(Debug, Clone, Copy)]


pub(crate) struct Phase7IndustryMembershipSourceAudit {
    pub(crate) data_rows:i64,
    pub(crate) symbols:i64,
    pub(crate) index_codes:i64,
    pub(crate) current_active_stock_symbols:i64,
    pub(crate) current_covered_stock_symbols:i64,
    pub(crate) min_in_date:Option<NaiveDate>,
    pub(crate) latest_in_date:Option<NaiveDate>,
    pub(crate) min_out_date:Option<NaiveDate>,
    latest_out_date: Option<NaiveDate>,
    min_available_at: Option<NaiveDate>,
    latest_available_at: Option<NaiveDate>,
    pit_violation_rows: i64,
    invalid_interval_rows: i64,
    duplicate_key_rows: i64,
}



fn phase7_market_level_sync_dataset(source: &str) -> Option<&'static str> {
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



fn phase7_datetime_json(date: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    json!(fmt_rfc3339_local(date))
}



pub(crate) fn phase7_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    if denominator <= 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}



fn phase7_p315_sync_start_date(
    stats: &Phase7MarketLevelSourceAudit,
    today: NaiveDate,
) -> NaiveDate {
    stats
        .latest_trade_date
        .map(|date| (date + Duration::days(1)).min(today))
        .unwrap_or(today)
}



fn phase7_new_alpha_candidate_sources() -> Vec<Value> {
    vec![
        json!({
            "source": "market_margin_regime",
            "current_tables": ["market_margin"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "融资融券汇总是交易所级时间序列，不能直接形成股票横截面排序 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "market_moneyflow_hsgt_regime",
            "current_tables": ["market_moneyflow_hsgt"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "北向资金汇总是市场级资金流，适合 regime/risk budget，不适合作为单股横截面 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "industry_prosperity_proxy",
            "current_tables": ["market_stock", "market_financial_indicator", "market_stock_daily_bar", "market_stock_moneyflow"],
            "candidate_raw_sources": ["tushare:index_classify", "tushare:index_member"],
            "admission_scope": "broad_base_proxy_candidate",
            "alpha_admission_gate": industry_prosperity_alpha_admission_policy_static(),
            "readiness": "industry_membership_permission_probe_required",
            "pit_boundary": "current market_stock.industry is static and cannot be treated as historical PIT industry classification",
            "why_not_trainable_now": "行业 PIT 原始源接入后仍需通过全历史 membership snapshot/coverage 审计和 P3.10 诊断，不能直接进入训练",
            "next_step": "run_industry_membership_permission_smoke_then_schema_available_at_audit"
        }),
        json!({
            "source": "block_trade_supply_demand",
            "current_tables": ["market_stock_block_trade"],
            "admission_scope": "stopped_same_family_after_p310",
            "readiness": "stopped_after_p310_economics_weak",
            "pit_boundary": "must persist announcement/trade publication date as available_at before any event-window feature",
            "why_not_trainable_now": "大宗交易供需源数据/PIT 可用，但 P3.10 显示覆盖偏窄、alpha economics 弱；不得继续同族扩参或直接 WFA",
            "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
        }),
        json!({
            "source": "equity_pledge_pressure",
            "current_tables": [],
            "candidate_raw_sources": ["tushare:pledge_stat", "tushare:pledge_detail"],
            "admission_scope": "new_p320_schema_contract_candidate",
            "readiness": "permission_smoke_passed_schema_contract_ready",
            "pit_boundary": "pledge_detail.ann_date is native available_at candidate; pledge_stat.end_date is a measurement date and cannot be used alone as availability",
            "why_not_trainable_now": "新候选源已完成生产 permission-smoke 和 schema contract，但尚未人工审查/应用 schema、全历史 bounded sync、coverage/readiness 或 P3.10",
            "next_step": "review_apply_equity_pledge_schema_then_bounded_sync_plan"
        }),
        json!({
            "source": "equity_incentive_execution_quality",
            "current_tables": [],
            "admission_scope": "raw_source_onboarding_required",
            "readiness": "schema_and_client_missing",
            "pit_boundary": "must persist disclosure/announcement date as available_at and execution periods as event attributes",
            "why_not_trainable_now": "当前没有股权激励原始表、Tushare client、同步账本或 PIT 可得日审计",
            "next_step": "add_permission_smoke_then_schema_and_bounded_sync"
        }),
    ]
}



pub(crate) fn phase7_futures_price_chain_schema_contract() -> Value {
    json!({
        "source_id": "futures_price_chain",
        "stage": "P3.19J",
        "mode": "read_only_schema_mapping_pit_contract",
        "source_status": "permission_smoke_available_schema_contract_defined",
        "admission_decision": "schema_mapping_available_at_audit_required_before_sync",
        "raw_sources": [
            {
                "api": "fut_daily",
                "doc": "https://tushare.pro/wctapi/documents/138.md",
                "semantics": "daily futures OHLC settlement volume and open-interest",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_wsr",
                "doc": "https://tushare.pro/wctapi/documents/140.md",
                "semantics": "warehouse receipt inventory and daily inventory change",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_holding",
                "doc": "https://tushare.pro/wctapi/documents/139.md",
                "semantics": "broker-level daily volume long and short holding ranking",
                "native_time_key": "trade_date"
            }
        ],
        "raw_tables": [
            {
                "table": "market_futures_daily",
                "natural_key": ["ts_code", "trade_date"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["close", "settle", "vol", "amount", "oi", "oi_chg"],
                "pit_rule": "available_at must be >= trade_date and downstream features must filter available_at <= stock_trade_date"
            },
            {
                "table": "market_futures_warehouse_receipt",
                "natural_key": ["trade_date", "symbol", "exchange", "warehouse"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["pre_vol", "vol", "vol_chg", "unit"],
                "pit_rule": "warehouse inventory changes are usable only after source publication"
            },
            {
                "table": "market_futures_holding_rank",
                "natural_key": ["trade_date", "symbol", "exchange", "broker"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg"],
                "pit_rule": "broker position changes are usable only after source publication"
            }
        ],
        "mapping_tables": [
            {
                "table": "market_futures_product_exposure_mapping_pit",
                "natural_key": ["product_symbol", "exposure_type", "exposure_code", "valid_from", "mapping_version"],
                "required_fields": ["product_symbol", "exposure_type", "exposure_code", "direction", "weight", "valid_from", "valid_to", "available_at", "source", "mapping_version"],
                "allowed_exposure_types": ["sw_industry", "stock_symbol"],
                "pit_rule": "mapping.available_at <= stock_trade_date; mapping rows must be versioned and must not be derived from future stock returns or future factor performance",
                "preferred_first_pass": "product-to-sw-industry mapping joined to market_stock_industry_membership_pit; direct stock_symbol mapping requires stronger evidence"
            },
            {
                "table": "market_futures_product_exclusion_gate_pit",
                "natural_key": ["product_symbol", "gate_scope", "valid_from", "gate_version"],
                "required_fields": ["product_symbol", "gate_scope", "reason_code", "valid_from", "valid_to", "available_at", "source", "gate_version", "evidence"],
                "allowed_reason_codes": ["financial_index_future", "interest_rate_future", "non_industry_derivative", "ambiguous_product_symbol", "insufficient_industry_evidence"],
                "pit_rule": "exclusion gates are admission controls: excluded products must not be forced into product-to-industry mappings or downstream factors",
                "preferred_first_pass": "pre-register non-industry derivatives such as equity index and treasury bond futures as excluded before product-to-SW-industry review"
            }
        ],
        "pit_policy": {
            "native_available_at_candidate": "trade_date_after_market_close",
            "source_published_at_required": true,
            "intraday_stock_decision_rule": "use_previous_available_futures_trade_date_until_source_published_at_is_audited",
            "prohibited": [
                "using same-day futures close or warehouse data in an intraday stock rebalance before publication",
                "using static hindsight product-to-stock mapping without available_at",
                "backfilling exposure weights from later performance or later industry reclassification"
            ]
        },
        "coverage_audit_required": {
            "full_history_range": "2014-01-01_to_latest_complete_trade_date",
            "required_breakdowns": ["year", "endpoint", "product_symbol", "exchange", "mapped_industry", "stock_market_scope"],
            "minimum_before_p310": "coverage/readiness green or explicitly gated market/date scope"
        },
        "promotion_gate": {
            "schema_status": "not_created",
            "sync_status": "not_started",
            "coverage_status": "not_started",
            "p310_status": "not_started",
            "wfa_status": "blocked_until_p310_passes",
            "v19_train_selection": "blocked"
        },
        "ddl_path": "sql/phase7_futures_price_chain_source.sql",
        "next_step": "create_schema_then_run_bounded_full_history_sync_and_coverage_readiness_audit"
    })
}



pub(crate) fn phase7_equity_pledge_schema_contract() -> Value {
    json!({
        "audit_version": "p3.20b-equity-pledge-pressure-schema-contract-v1",
        "source_id": "equity_pledge_pressure",
        "stage": "P3.20B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_equity_pledge_source.sql",
        "raw_sources": [
            {
                "api": "pledge_stat",
                "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                "semantics": "stock_equity_pledge_stat_snapshot",
                "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                "required_fields": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                "minimum_points": 2000
            },
            {
                "api": "pledge_detail",
                "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                "semantics": "stock_equity_pledge_detail_events",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_pledge_stat",
                "natural_key": ["symbol", "end_date"],
                "required_fields": ["symbol", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio", "available_at", "source_published_at", "raw_payload", "source", "data_version_id"],
                "pit_rule": "available_at must be >= end_date. Default sync may only use conservative end_date+1day unless a source_published_at audit proves earlier availability.",
                "training_gate": "stat snapshots cannot enter factor construction until joined to detail announcements or audited with a conservative availability lag."
            },
            {
                "table": "market_stock_pledge_detail",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "pledge_amount", "pledge_start_date", "pledge_end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date. Downstream features must use available_at <= stock_trade_date and must preserve multiple pledge rows on the same announcement date.",
                "nullable_source_fields": ["pledge_start_date", "pledge_end_date", "release_date", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio"],
                "duplicate_policy": "preserve source rows by source_row_hash; do not collapse same-day multiple pledges before audit. Nullable source fields must remain nullable and must not be promoted into the primary key."
            }
        ],
        "available_at_policy": {
            "pledge_detail": "ann_date is native available_at candidate and must be persisted as available_at.",
            "pledge_stat": "end_date is a measurement date, not disclosure availability; use only after conservative lag or detail-derived audit.",
            "intraday_trading": "without verified source publication timestamps, same-day pledge updates are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_symbol_ann_date_breakdown",
            "detail_available_at_null_or_future_leak_count",
            "detail_release_before_start_count",
            "detail_ratio_out_of_range_count",
            "stat_pledge_ratio_out_of_range_count",
            "stat_end_date_coverage_and_lag_policy",
            "symbol_breadth_vs_main_chinext_non_st",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}



pub(crate) fn phase7_margin_detail_schema_contract() -> Value {
    json!({
        "audit_version": "p3.22c-margin-detail-schema-contract-v1",
        "source_id": "margin_detail_leverage_crowding",
        "stage": "P3.22C",
        "status": "permission_smoke_passed_schema_created_sync_smoke_passed",
        "mode": "read_only_schema_available_at_quality_contract",
        "ddl_path": "sql/phase7_margin_detail_source.sql",
        "raw_sources": [
            {
                "api": "margin_detail",
                "official_doc": "https://tushare.pro/document/2?doc_id=59",
                "semantics": "security_level_margin_financing_and_short_selling_detail",
                "native_time_key": "trade_date",
                "official_publication_hint": "previous trading day data updates around next trading day 08:30",
                "required_fields": ["trade_date", "ts_code", "name", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"]
            }
        ],
        "tables": [
            {
                "table": "market_stock_margin_detail",
                "natural_key": ["symbol", "trade_date"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"],
                "pit_rule": "available_at must be the next open trading date after trade_date; downstream features and intraday trading must additionally require source_published_at <= decision timestamp",
                "quality_rule": "rzche and rqchl may be negative vendor adjustment fields and must be preserved; balances/core activity fields must be nonnegative",
                "raw_landing_policy": "raw sync preserves vendor rows and reports anomalies; admission gates decide factor eligibility"
            }
        ],
        "available_at_policy": {
            "default": "conservative next-session availability",
            "source_published_at": "next open trading day 08:30 China time when native timestamp is unavailable",
            "intraday_trading": "same-day margin_detail must not be used for intraday rebalance; only rows with source_published_at <= decision timestamp are usable"
        },
        "coverage_audit_required": [
            "year_market_symbol_trade_date_breakdown",
            "open_trade_day_coverage_ratio",
            "available_at_pit_violation_rows",
            "missing_source_published_at_rows",
            "rzche_rqchl_negative_adjustment_breakdown",
            "core_nonnegative_field_violation_rows",
            "sync_attempt_success_failure_breakdown",
            "correlation_vs_moneyflow_liquidity_price_volume"
        ],
        "promotion_gate": {
            "schema_status": "created_or_review_required",
            "bounded_sync": "allowed_only_as_raw_admission_sync",
            "factor_builder": "blocked_until_full_history_coverage_pit_quality_and_correlation_pass",
            "p310_status": "blocked_until_coverage_pit_quality_and_correlation_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_full_history_bounded_sync_by_year_or_quarter_then_rerun_coverage_pit_quality_correlation_audit"
    })
}



pub(crate) fn phase7_shareholder_structure_schema_contract() -> Value {
    json!({
        "audit_version": "p3.21b-shareholder-structure-schema-contract-v1",
        "source_id": "shareholder_structure",
        "stage": "P3.21B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_shareholder_structure_source.sql",
        "raw_sources": [
            {
                "api": "stk_holdernumber",
                "semantics": "stock_shareholder_count_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_num"],
                "minimum_points": 2000
            },
            {
                "api": "top10_holders",
                "semantics": "top10_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "top10_floatholders",
                "semantics": "top10_float_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "stk_holdertrade",
                "semantics": "major_holder_or_insider_increase_decrease_event",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "begin_date", "close_date"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_holder_number",
                "natural_key": ["symbol", "ann_date", "end_date"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_num", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; ann_date before end_date is retained as a raw source anomaly and blocks admission until repaired, excluded, or gated."
            },
            {
                "table": "market_stock_top10_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; downstream features must filter available_at <= stock_trade_date and exclude or gate ann_date before end_date anomalies."
            },
            {
                "table": "market_stock_top10_float_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; do not infer float-holder concentration before disclosure, and treat ann_date before end_date as a blocking raw anomaly."
            },
            {
                "table": "market_stock_holder_trade",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share", "begin_date", "close_date", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; begin_date/close_date are event attributes and must not move availability earlier."
            }
        ],
        "available_at_policy": {
            "default": "available_at equals native ann_date for all four shareholder_structure raw tables; end_date is a measurement period only.",
            "period_snapshot_rule": "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated.",
            "raw_landing": "raw tables accept native source rows for auditability; admission gates, not insert constraints, decide whether rows can feed factor work.",
            "intraday_trading": "without verified source publication timestamps, same-day shareholder disclosures are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_source_symbol_ann_date_breakdown",
            "symbol_breadth_vs_main_chinext_non_st",
            "ann_date_before_end_date_count",
            "available_at_before_ann_date_count",
            "holder_ratio_out_of_range_count",
            "holder_trade_interval_violation_count",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}



pub(crate) fn phase7_exchange_announcement_order_capacity_schema_contract() -> Value {
    json!({
        "audit_version": "p3.24a-exchange-announcement-order-capacity-source-contract-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24A",
        "status": "schema_contract_defined_pdf_detail_admission_required_before_manual_review",
        "mode": "read_only_source_admission_contract_no_sync",
        "ddl_path": "sql/phase7_exchange_announcement_order_capacity_source.sql",
        "raw_sources": [
            {
                "vendor": "akshare",
                "upstream": "cninfo",
                "vendor_endpoint": "stock_zh_a_disclosure_report_cninfo",
                "source_semantics": "CNInfo-listed company disclosure reports by symbol, market, category and date range",
                "request_key": "symbol+market+category+start_date+end_date",
                "native_available_at_candidate": "公告时间",
                "source_published_at_candidate": "announcement detail page publish timestamp if available; otherwise date-only announcement time",
                "observed_smoke": {
                    "as_of": "2026-06-25",
                    "akshare_version": "1.18.64",
                    "symbol": "000001",
                    "market": "沪深京",
                    "category": "日常经营",
                    "date_range": "20230101..20231231",
                    "row_count": 31,
                    "fields": ["代码", "简称", "公告标题", "公告时间", "公告链接"]
                },
                "known_risks": [
                    "symbol fanout can be expensive; bounded history sync must be batched by symbol and date range",
                    "some categories may return empty or parser errors and need category-level reliability audit before full sync",
                    "date-only announcement time is insufficient for same-day intraday decisions"
                ],
                "admission_gate": "permission_history_category_smoke_and_available_at_text_parse_audit_required_before_schema_apply"
            },
            {
                "vendor": "cninfo",
                "vendor_endpoint": "announcement_detail_page",
                "source_semantics": "official disclosure detail page referenced by announcement link",
                "request_key": "announcement_id+org_id+stock_code",
                "native_available_at_candidate": "detail page disclosure timestamp when present",
                "required_from_link": ["announcementId", "orgId", "stockCode", "announcementTime"],
                "admission_gate": "official_page_fetch_text_hash_and_publication_timestamp_audit_required"
            },
            {
                "vendor": "licensed_vendor",
                "vendor_endpoint": "broad_base_announcement_text_feed",
                "source_semantics": "licensed exchange/CNInfo disclosure feed with timestamped announcement text",
                "native_available_at_candidate": "vendor source publication timestamp",
                "admission_gate": "permission_and_schema_contract_required_if_public_feed_is_not_reliable_enough"
            }
        ],
        "event_taxonomy": [
            {
                "event_type": "order_or_contract_signed",
                "positive_evidence": ["中标", "签订合同", "重大合同", "订单", "框架协议"],
                "required_evidence": ["counterparty", "contract_amount_or_capacity", "time_window_or_delivery_schedule"]
            },
            {
                "event_type": "capacity_expansion_or_commissioning",
                "positive_evidence": ["扩产", "产能", "投产", "试生产", "达产"],
                "required_evidence": ["project_name", "capacity_or_capex", "expected_start_or_completion_date"]
            },
            {
                "event_type": "product_price_adjustment",
                "positive_evidence": ["价格调整", "上调", "下调", "产品价格"],
                "required_evidence": ["product", "price_direction", "effective_date"]
            },
            {
                "event_type": "major_supply_or_customer_agreement",
                "positive_evidence": ["供货协议", "采购协议", "长期协议", "战略合作"],
                "required_evidence": ["customer_or_supplier", "covered_product", "duration_or_amount"]
            }
        ],
        "tables": [
            {
                "table": "market_exchange_announcement_text_raw",
                "natural_key": ["vendor", "vendor_endpoint", "announcement_id", "symbol"],
                "required_fields": [
                    "vendor",
                    "vendor_endpoint",
                    "request_key",
                    "symbol",
                    "symbol_name",
                    "announcement_id",
                    "org_id",
                    "announcement_category",
                    "announcement_title",
                    "announcement_time",
                    "source_published_at",
                    "source_published_at_quality",
                    "available_at",
                    "announcement_url",
                    "pdf_final_url",
                    "text_content",
                    "text_hash",
                    "text_hash_algorithm",
                    "timestamp_candidates",
                    "pdf_metadata_keys",
                    "raw_payload",
                    "raw_payload_hash",
                    "parser_used",
                    "parser_version",
                    "event_type",
                    "evidence_spans",
                    "ingested_at",
                    "data_version_id"
                ],
                "pit_rule": "available_at must be no earlier than source_published_at/date-only announcement_time. If source_published_at_quality is date_only_next_session, downstream trading must promote availability to the next open session. Intraday trading must additionally require a trusted timestamp with source_published_at <= decision timestamp.",
                "text_evidence_rule": "event_type is invalid without evidence_spans that quote the exact announcement text supporting order, capacity, contract, price-adjustment or commissioning semantics. PDF-only rows without evidence_spans remain blocked.",
                "raw_landing_policy": "preserve full raw payload, source URL, pdf_final_url, text hash and parser identity; category/parser errors and scanned_pdf_ocr_required cases must be audited, not silently dropped."
            }
        ],
        "available_at_policy": {
            "preferred": "use official source_published_at timestamp from the announcement detail/feed when available",
            "date_only_policy": "if only announcement date is available, set available_at to next open session for trading decisions until source_published_at timestamp is audited",
            "intraday_trading": "same-day announcement events are forbidden for intraday rebalance unless source_published_at <= decision timestamp is proven",
            "weekend_or_holiday_publications": "bounded sync must scan calendar days and map date-only announcements to the next open session rather than dropping non-trading-day disclosures"
        },
        "pdf_admission": {
            "runtime_default_python": "~/.local/share/quant-pdf-audit/venv/bin/python",
            "pdf_parser_readiness_endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/pdf-parser-readiness",
            "pdf_detail_audit_endpoint": "POST /api/v1/quant/data/exchange-announcement-order-capacity/pdf-detail-audit",
            "required_audit_version": "p3.24e-exchange-announcement-order-capacity-pdf-detail-audit-v1",
            "required_detail_fields": [
                "pdf_final_url",
                "parser_used",
                "text_hash",
                "text_hash_algorithm",
                "timestamp_candidates",
                "source_published_at",
                "source_published_at_quality",
                "evidence_spans",
                "pdf_metadata_keys"
            ],
            "next_session_policy": "date_only_next_session is admissible only for next-open-session daily PIT usage; same-session and intraday usage remain blocked",
            "ocr_policy": "scanned_pdf_ocr_required stays blocked until a separate OCR runtime and audit path are reviewed"
        },
        "manual_schema_review": {
            "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/manual-schema-review",
            "audit_version": "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
            "mode": "read_only_schema_contract_ddl_review_no_apply",
            "decision_scope": "ddl_contract_review_only_bounded_sync_design_allowed_next",
            "requires": [
                "pdf_detail_audit_passed_manual_schema_review_allowed_next",
                "date_only_next_session_policy_encoded",
                "raw_failure_samples_preserved",
                "evidence_span_jsonb_preserved"
            ]
        },
        "sync_plan": {
            "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/sync-plan",
            "audit_version": "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1",
            "mode": "read_only_plan_only_calendar_day_symbol_category_sync_design",
            "sync_endpoint_status": "disabled_plan_only_design_until_operator_schema_apply_and_small_batch_review"
        },
        "coverage_audit_required": [
            "year_market_symbol_category_breakdown",
            "calendar_day_and_open_day_publication_coverage",
            "announcement_id_duplicate_or_missing_count",
            "source_published_at_null_or_date_only_count",
            "text_fetch_success_rate",
            "text_hash_duplicate_count",
            "category_parser_error_breakdown",
            "event_taxonomy_precision_manual_sample",
            "evidence_span_presence_rate",
            "correlation_vs_existing_event_moneyflow_liquidity_price_volume_quality_sources"
        ],
        "promotion_gate": {
            "permission_smoke": "required",
            "history_category_replay": "required_before_schema_apply",
            "schema_apply": "blocked_until_pdf_detail_audit_passes_and_manual_review",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "do not build generic post-announcement return overlay from this source",
            "do not use full-period keyword sign or horizon mining",
            "do not merge announcement categories before category-level coverage and parser quality pass",
            "do not use date-only same-day announcements for intraday or same-session decisions",
            "do not enter P3.10 until coverage/PIT/text-evidence/correlation audits pass"
        ],
        "next_step": "use permission_smoke_detail_audit_pdf_parser_readiness_and_pdf_detail_audit_evidence_to_finish_manual_schema_review_before_any_bounded_sync_design"
    })
}



pub(crate) fn phase7_exchange_announcement_order_capacity_next_source_admission_plan() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25a-exchange-announcement-order-capacity-next-source-admission-plan-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.25A",
        "mode": "read_only_next_source_admission_plan_no_sync_no_factor_no_p310",
        "current_pilot_decision": "stopped_current_4_symbol_daily_operation_pilot_after_clean_target_recall_failed",
        "current_pilot_evidence": {
            "scope": "002459.SZ,600519.SH,300750.SZ,000001.SZ / 日常经营 / 2023-10-01..2025-03-31",
            "raw_rows": 249,
            "target_event_rows_before_title_risk_gate": 119,
            "taxonomy_blocked_target_event_rows": 113,
            "admissible_target_event_rows": 6,
            "manual_review_min_clean_target_samples": 50,
            "manual_review_sample_shortfall": 44,
            "pit_violation_rows": 0,
            "duplicate_key_rows": 0,
            "blocking_failed_attempts": 0,
            "raw_sync_quality": "passed_after_high_density_landing_cap_fix",
            "interpretation": "engineering raw landing and PIT controls are healthy; blocker is insufficient clean target-event recall and noisy vendor category semantics"
        },
        "stop_rules": [
            "do_not_continue_month_or_quarter_raw_sync_for_current_4_symbol_daily_operation_pilot",
            "do_not_relax_title_taxonomy_or_manual_precision_gate_to_create_false_ready_status",
            "do_not_enter_factor_builder_p310_bounded_wfa_or_v19_from_current_pilot",
            "do_not_use_oos_feedback_full_period_sign_flip_or_horizon_mining_to_rescue_this_source"
        ],
        "candidate_routes": [
            {
                "priority": 1,
                "route_id": "structured_order_capacity_contract_price_chain_source",
                "source_family": "licensed_or_structured_real_operations_feed",
                "economic_hypothesis": "structured order, capacity, contract, price-adjustment or commissioning data may provide cleaner operational information than noisy category-level disclosure text",
                "candidate_sources": [
                    "licensed_exchange_or_cninfo_timestamped_announcement_feed",
                    "licensed_structured_order_contract_capacity_event_feed",
                    "authorized_industry_price_capacity_order_chain_feed"
                ],
                "universe_policy": "broad_base_main_chinext_non_st_or_pre_registered_market_scope_gate; any excluded market/date/symbol scope must be declared before diagnostics",
                "required_gates": [
                    "vendor_permission_and_legal_usage_audit",
                    "raw_schema_contract_with_stable_natural_key",
                    "available_at_source_published_at_audit",
                    "full_history_bounded_sync_plan",
                    "coverage_readiness_pit_duplicate_hash_audit",
                    "manual_precision_ge_0_80_with_min_50_clean_target_samples",
                    "correlation_vs_existing_moneyflow_liquidity_price_volume_quality_event_sources"
                ],
                "stop_rule": "stop_before_factor_builder_if_coverage_pit_precision_or_correlation_gate_fails",
                "promotion_gate": blocked_promotion_gate.clone(),
                "next_step": "source_discovery_permission_schema_available_at_contract_before_any_raw_sync"
            },
            {
                "priority": 2,
                "route_id": "announcement_text_broader_universe",
                "source_family": "public_or_licensed_announcement_text_with_pre_registered_broader_universe",
                "economic_hypothesis": "if announcement text remains the source, recall must be improved by pre-registering a broader universe and category/taxonomy scope rather than extending the stopped 4-symbol pilot",
                "candidate_sources": [
                    "akshare_cninfo_disclosure_feed_with_broader_symbol_universe",
                    "official_cninfo_or_exchange_feed_with_timestamped_detail_pages",
                    "licensed_timestamped_announcement_text_feed"
                ],
                "universe_policy": "pre_register_symbols_markets_categories_and_date_range; no post-hoc symbol/category selection based on return performance",
                "required_gates": [
                    "permission_history_category_smoke",
                    "detail_text_pdf_ocr_timestamp_hash_audit",
                    "available_at_source_published_at_audit",
                    "bounded_calendar_day_symbol_category_sync_plan",
                    "coverage_readiness_pit_failed_attempt_duplicate_hash_audit",
                    "manual_precision_ge_0_80_with_min_50_clean_target_samples",
                    "taxonomy_precision_false_positive_review",
                    "correlation_vs_existing_event_moneyflow_liquidity_price_volume_quality_sources"
                ],
                "stop_rule": "stop_if_broader_pre_registered_scope_still_cannot_produce_50_clean_target_review_samples_or_precision_below_0_80",
                "promotion_gate": blocked_promotion_gate.clone(),
                "next_step": "write_pre_registered_broader_universe_plan_then_run_permission_and_available_at_smoke_only"
            }
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_new_route_permission_schema_available_at_contract_passes",
            "bounded_sync": "blocked_until_new_route_manual_schema_review_and_plan_pass",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_required_steps": [
            "choose_route_1_structured_source_if_usable_vendor_exists_otherwise_route_2_broader_universe",
            "create_read_only_permission_schema_available_at_admission_for_selected_route",
            "keep_current_4_symbol_daily_operation_pilot_stopped"
        ]
    })
}



fn phase7_structured_order_capacity_price_chain_source_contract() -> Value {
    json!({
        "audit_version": "p3.25b-structured-order-capacity-contract-price-chain-source-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25B",
        "mode": "read_only_structured_source_admission_contract_no_sync",
        "admission_decision": "blocked_vendor_permission_and_available_at_contract_required",
        "source_priority": 1,
        "why_now": "current public CNInfo/AkShare 4-symbol daily-operation pilot is stopped for clean target-event recall and taxonomy precision; next source must prefer structured, timestamped, legally usable real-operation events",
        "candidate_source_families": [
            {
                "family": "licensed_structured_order_contract_capacity_event_feed",
                "examples": [
                    "vendor-normalized listed-company contract/order/capacity/commissioning events",
                    "licensed exchange or CNInfo feed with event tags and publication timestamps"
                ],
                "minimum_admission_state": "vendor_permission_and_schema_sample_required"
            },
            {
                "family": "authorized_industry_price_capacity_order_chain_feed",
                "examples": [
                    "industry product price adjustment feed",
                    "capacity commissioning or production schedule feed",
                    "order backlog or contract award feed with listed-company identifiers"
                ],
                "minimum_admission_state": "legal_usage_and_symbol_mapping_required"
            }
        ],
        "legal_and_vendor_gate": {
            "status": "required_before_schema_or_sync",
            "required_evidence": [
                "license_or_terms_allow_research_and_internal_trading_use",
                "redistribution_and_storage_rights_reviewed",
                "historical_access_range_confirmed",
                "api_rate_limit_and_cost_estimated",
                "vendor_field_dictionary_or_sample_payload_collected"
            ],
            "blocked_if": [
                "no_storage_rights",
                "no_historical_access",
                "no_source_publication_timestamp_or_conservative_availability_rule",
                "event_labels_are_derived_from_future_returns"
            ]
        },
        "required_time_fields": [
            "event_date",
            "source_published_at",
            "available_at",
            "ingested_at"
        ],
        "raw_schema_contract": {
            "table_candidate": "market_structured_operation_event_raw",
            "natural_key": [
                "vendor",
                "vendor_endpoint",
                "vendor_event_id",
                "symbol",
                "event_type",
                "source_published_at",
                "raw_payload_hash"
            ],
            "required_identity_fields": [
                "vendor",
                "vendor_endpoint",
                "request_key",
                "vendor_event_id",
                "symbol",
                "symbol_name",
                "event_type"
            ],
            "required_evidence_fields": [
                "source_url",
                "source_title",
                "source_document_id",
                "source_excerpt_or_structured_payload",
                "evidence_hash",
                "raw_payload",
                "raw_payload_hash",
                "parser_or_vendor_model_version"
            ],
            "pit_rule": "downstream features must filter available_at <= trade_date; intraday decisions must additionally require source_published_at <= decision_timestamp"
        },
        "event_schema": [
            {
                "event_type": "order_or_contract_signed",
                "required_fields": [
                    "counterparty",
                    "contract_amount",
                    "covered_product_or_service",
                    "delivery_or_execution_window",
                    "contract_status"
                ],
                "quality_checks": [
                    "contract_amount_nonnegative_or_null_with_reason",
                    "counterparty_not_empty",
                    "execution_window_not_before_source_published_at"
                ]
            },
            {
                "event_type": "capacity_expansion_or_commissioning",
                "required_fields": [
                    "project_name",
                    "capacity_or_capex",
                    "product_or_line",
                    "expected_start_or_completion_date",
                    "project_location"
                ],
                "quality_checks": [
                    "capacity_or_capex_nonnegative_or_null_with_reason",
                    "project_timeline_not_backfilled_from_later_reports",
                    "location_or_product_scope_reviewed"
                ]
            },
            {
                "event_type": "product_price_adjustment",
                "required_fields": [
                    "product",
                    "price_direction",
                    "effective_date",
                    "price_change_magnitude_or_bucket",
                    "scope"
                ],
                "quality_checks": [
                    "price_direction_increase_decrease_or_mixed",
                    "effective_date_available_only_after_source_published_at",
                    "scope_not_market_return_derived"
                ]
            },
            {
                "event_type": "supply_customer_agreement_or_order_backlog",
                "required_fields": [
                    "customer_or_supplier",
                    "covered_product",
                    "duration_or_amount",
                    "agreement_type",
                    "execution_status"
                ],
                "quality_checks": [
                    "customer_supplier_not_empty",
                    "duration_or_amount_not_future_filled",
                    "agreement_type_from_vendor_or_source_text_not_return_label"
                ]
            }
        ],
        "available_at_policy": {
            "daily_rule": "if source has only date-level publication, available_at must map to the next open trading session",
            "intraday_rule": "decision_timestamp must be >= source_published_at; date-only events are daily next-session only",
            "weekend_holiday_rule": "weekend or holiday publications map to the next open trading session",
            "forbidden": [
                "using event effective_date as available_at",
                "using vendor ingestion time as source publication time",
                "same-session trading from date-only publications",
                "labels or event directions inferred from future stock returns"
            ]
        },
        "coverage_audit_required": [
            "vendor_endpoint_year_month_breakdown",
            "market_scope_symbol_coverage_vs_main_chinext_non_st",
            "event_type_distribution_and_target_yield",
            "source_published_at_null_or_date_only_count",
            "available_at_pit_violation_rows",
            "duplicate_vendor_event_or_payload_hash_rows",
            "manual_precision_ge_0_80_with_min_50_clean_target_samples",
            "field_null_rate_and_range_checks_by_event_type",
            "correlation_vs_existing_moneyflow_liquidity_price_volume_quality_event_sources",
            "cost_rate_limit_and_refresh_latency_budget"
        ],
        "promotion_gate": {
            "permission_smoke": "blocked_until_vendor_candidate_selected",
            "schema_apply": "blocked",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "do not proceed without legal/vendor storage and usage review",
            "do not map event_type from post-event returns or OOS performance",
            "do not merge public noisy text pilot rows into structured-source positives",
            "do not enter P3.10 before full coverage/PIT/precision/correlation gates pass"
        ],
        "next_step": "identify_vendor_or_authorized_structured_source_then_run_permission_and_sample_payload_smoke"
    })
}



fn phase7_structured_order_capacity_price_chain_vendor_admission_plan() -> Value {
    let blocked_promotion_gate = json!({
        "permission_smoke": "blocked_until_candidate_vendor_and_endpoint_selected",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    let sample_payload_evidence_required = json!([
        "sample_request_and_response_captured_read_only",
        "vendor_field_dictionary_or_payload_schema",
        "source_published_at_or_conservative_available_at",
        "event_date_and_event_type_semantics",
        "stable_vendor_event_id_or_deterministic_natural_key",
        "raw_payload_hash",
        "symbol_mapping_evidence",
        "license_storage_and_internal_use_note"
    ]);

    json!({
        "audit_version": "p3.25c-structured-order-capacity-price-chain-vendor-admission-plan-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25C",
        "mode": "read_only_vendor_source_candidate_discovery_plan_no_schema_no_sync",
        "admission_decision": "blocked_until_vendor_permission_history_available_at_and_sample_payload_smoke_pass",
        "why_now": "P3.25B defined the structured source contract; P3.25C must discover a legally usable vendor or source and prove payload/PIT semantics before any schema or sync work",
        "candidate_sources": [
            {
                "priority": 1,
                "vendor": "licensed_structured_financial_data_vendor",
                "source_name": "listed_company_order_contract_capacity_event_feed",
                "source_family": "licensed_structured_order_contract_capacity_event_feed",
                "legal_storage_use_status": "unknown_requires_terms_or_contract_review",
                "endpoint_payload_availability": "unknown_requires_permission_and_sample_payload_smoke",
                "historical_coverage_range": "unknown_requires_history_date_probe_covering_2014_to_present_or_declared_start_date",
                "source_published_at_semantics": "must_provide_publication_timestamp_or_auditable_date_level_publication",
                "symbol_mapping_requirement": "must_map_vendor_company_identifier_to_ts_code_or_exchange_symbol_with_effective_date_scope",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_event_labels_are_return_derived_or_publication_time_is_missing_without_conservative_available_at"
            },
            {
                "priority": 2,
                "vendor": "licensed_exchange_or_cninfo_metadata_vendor",
                "source_name": "timestamped_announcement_metadata_with_structured_event_tags",
                "source_family": "licensed_timestamped_disclosure_metadata_feed",
                "legal_storage_use_status": "unknown_requires_license_storage_redistribution_and_internal_trading_use_review",
                "endpoint_payload_availability": "unknown_requires_endpoint_smoke_for_event_tags_detail_url_and_payload_hash",
                "historical_coverage_range": "unknown_requires_replay_probe_by_publication_date_and_category",
                "source_published_at_semantics": "must_distinguish_source_publication_time_from_vendor_ingestion_time",
                "symbol_mapping_requirement": "must_preserve exchange_symbol and normalized ts_code mapping without current_snapshot_backfill",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_tags_reproduce_the_noisy_current_daily_operation_category_without_clean_target_precision"
            },
            {
                "priority": 3,
                "vendor": "authorized_industry_chain_data_vendor",
                "source_name": "industry_price_capacity_order_chain_feed",
                "source_family": "authorized_industry_price_capacity_order_chain_feed",
                "legal_storage_use_status": "unknown_requires_data_use_storage_and_symbol_mapping_terms",
                "endpoint_payload_availability": "unknown_requires_sample_for_product_price_capacity_order_records",
                "historical_coverage_range": "unknown_requires_product_or_company_history_probe_and_market_scope_declaration",
                "source_published_at_semantics": "must_provide_observation_publication_or_release_time_not_future_revised_series_only",
                "symbol_mapping_requirement": "must map product/industry/company exposure using pre_registered PIT mapping or exclusion gate",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_mapping_to_listed_company_or_sw_industry_requires_future_performance_or_subjective_post_hoc_weights"
            }
        ],
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id",
            "symbol_or_company_identifier",
            "event_type",
            "event_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_id",
            "source_title_or_structured_payload_excerpt",
            "raw_payload",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "permission_and_history_smoke_plan": {
            "write_enabled": false,
            "db_write_enabled": false,
            "minimum_smoke": [
                "terms_or_license_storage_use_review",
                "single_endpoint_permission_probe",
                "history_date_probe_for_old_mid_recent_periods",
                "sample_payload_capture_without_persistence",
                "source_published_at_available_at_semantics_review",
                "symbol_mapping_and_market_scope_review"
            ],
            "representative_history_dates": [
                "2014-01-02",
                "2017-01-03",
                "2020-07-01",
                "2024-01-02",
                "latest_completed_trading_or_publication_date"
            ],
            "blocked_outputs": [
                "ddl_generation",
                "schema_apply",
                "bounded_raw_sync",
                "factor_backfill",
                "p310_diagnostics",
                "bounded_wfa",
                "v19_train_selection"
            ]
        },
        "stop_rules": [
            "stop_if_vendor_terms_do_not_allow_storage_research_and_internal_trading_use",
            "stop_if_vendor_cannot_provide_historical_payload_samples_with_source_published_at_or_auditable_availability",
            "stop_if_only_current_snapshot_or_forward_revised_series_is_available",
            "stop_if_event_type_or_direction_is_derived_from_future_returns",
            "stop_if_symbol_mapping_requires_post_hoc_performance_weights",
            "stop_if_sample_payload_cannot_preserve_stable_natural_key_and_raw_payload_hash",
            "stop_if_sample_precision_or_event_semantics_are_equivalent_to_the_stopped_noisy_cninfo_daily_operation_pilot"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_if_no_usable_vendor": "switch_to_announcement_text_broader_universe_pre_registered_plan_without_rescuing_current_4_symbol_pilot",
        "next_step": "collect_candidate_vendor_terms_endpoint_dictionary_and_read_only_sample_payload_evidence_before_any_schema_or_sync_design"
    })
}



fn phase7_structured_order_capacity_price_chain_source_evidence_inventory() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });
    let missing_core_evidence = json!([
        "license_or_terms_allow_storage_research_and_internal_trading_use",
        "read_only_sample_payload_with_source_published_at",
        "historical_access_range_covering_2014_to_present_or_declared_start",
        "stable_vendor_event_id_or_natural_key",
        "raw_payload_hash_and_field_dictionary",
        "symbol_mapping_effective_date_scope",
        "rate_limit_cost_and_refresh_latency_budget"
    ]);

    json!({
        "audit_version": "p3.25d-structured-order-capacity-price-chain-source-evidence-inventory-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25D",
        "mode": "read_only_source_evidence_inventory_no_permission_probe_no_schema_no_sync",
        "admission_decision": "blocked_no_candidate_has_complete_vendor_terms_history_payload_and_available_at_evidence",
        "permission_smoke": "blocked_until_candidate_access_configured",
        "candidate_evidence": [
            {
                "candidate_id": "cninfo_data_service",
                "vendor": "CNINFO Data Service / Shenzhen Securities Information",
                "source_url": "https://webapi.cninfo.com.cn/",
                "source_family": "licensed_timestamped_disclosure_metadata_feed",
                "observed_relevance": "official data service site advertises listed-company announcements, thematic statistics, data browser, quantitative data service, and industry-chain entry points",
                "candidate_strength": "official_channel_for_cninfo_disclosure_and_data_service",
                "admission_status": "candidate_permission_sample_smoke_required",
                "pit_risk": "public site confirms product family but not sample payload timestamp semantics or storage rights",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "contact_or_configure_authorized_access_then_read_only_endpoint_dictionary_and_sample_payload_smoke"
            },
            {
                "candidate_id": "eastmoney_major_contracts_public_page",
                "vendor": "Eastmoney Data Center",
                "source_url": "https://data.eastmoney.com/zdht/",
                "source_family": "public_major_contracts_web_page",
                "observed_relevance": "public page lists major-contract fields such as stock code, contract type, contract name, contract amount, sign date and announcement date",
                "candidate_strength": "confirms_major_contract_event_taxonomy_exists_publicly",
                "admission_status": "blocked_public_web_page_not_licensed_api",
                "pit_risk": "public web page is not evidence of licensed API use, storage rights, stable payload contract, or source publication timestamp",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "use_only_as_taxonomy_hint_until_choice_or_other_licensed_api_terms_and_sample_payload_are_available"
            },
            {
                "candidate_id": "cnopendata_major_contracts_dataset",
                "vendor": "CnOpenData",
                "source_url": "https://m.cnopendata.com/pages/data?module=listedcompany-basic&dataKey=listedco-zdht",
                "source_family": "licensed_or_paid_major_contracts_dataset",
                "observed_relevance": "dataset description advertises A-share listed-company major-contract fields including announcement date, sign date, contract name, contract type, amount, content and impact",
                "candidate_strength": "field_semantics_close_to_order_contract_source",
                "admission_status": "candidate_permission_sample_smoke_required",
                "pit_risk": "mobile catalog snippet does not prove API access, historical coverage, source_published_at, storage rights or raw payload stability",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "request_terms_field_dictionary_history_range_and_read_only_sample_payload_before_schema_design"
            },
            {
                "candidate_id": "wind_client_api_platform",
                "vendor": "Wind",
                "source_url": "https://www.wind.com.cn/mobile/ClientApi/zh.html",
                "source_family": "licensed_financial_terminal_or_client_api",
                "observed_relevance": "official ClientApi page advertises secure and consistent access to Wind data for internal or third-party applications",
                "candidate_strength": "mature_licensed_data_platform_candidate",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "platform availability alone does not prove the needed structured operation-event endpoint, entitlement, source timestamp, or storage rights",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "review_contract_entitlement_and_catalog_for_contract_capacity_price_chain_events_then_sample_payload_smoke"
            },
            {
                "candidate_id": "choice_dataservice_platform",
                "vendor": "Eastmoney Choice",
                "source_url": "https://choice.eastmoney.com/dataservice",
                "source_family": "licensed_financial_dataservice_platform",
                "observed_relevance": "Choice data-service page advertises data interface delivery across assets and macro/industry datasets into enterprise data warehouses",
                "candidate_strength": "possible_licensed_path_for_eastmoney_major_contracts_or_related_event_data",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "data-service page does not prove major-contract endpoint, payload schema, source_published_at, or allowed research/trading storage",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "verify_whether_choice_entitlement_exposes_major_contracts_or_announcement_event_dataset_with_payload_samples"
            },
            {
                "candidate_id": "juyuan_gildata_platform",
                "vendor": "Gildata / Hundsun Juyuan",
                "source_url": "https://www.gildata.com/",
                "source_family": "licensed_financial_data_platform",
                "observed_relevance": "public site describes broad financial market data, applied databases and information terminal products",
                "candidate_strength": "possible_licensed_structured_event_or_announcement_dataset_provider",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "public homepage does not prove specific order/capacity/contract/price-chain endpoint or PIT timestamp semantics",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "request_product_catalog_and_sample_payload_for_listed_company_operation_event_or_announcement_structuring_dataset"
            }
        ],
        "hard_stop_if_missing": [
            "vendor_terms_allowing_storage_research_internal_trading_use",
            "source_published_at_or_conservative_available_at_semantics",
            "historical_payload_samples",
            "stable_natural_key_and_raw_payload_hash",
            "symbol_mapping_with_effective_date_or_pre_registered_scope",
            "event_labels_not_derived_from_future_returns"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_rule": "if_no_candidate_can_supply_terms_history_payload_and_available_at_evidence_then_prepare_announcement_text_broader_universe_pre_registration_instead",
        "next_step": "select_one_candidate_with_legal_access_then_run_read_only_permission_and_sample_payload_smoke"
    })
}



pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_access_smoke_contract() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25e-cninfo-data-service-access-smoke-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25E",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_access_and_sample_payload_contract_no_network_no_schema_no_sync",
        "access_status": "not_configured_or_not_reviewed",
        "admission_decision": "blocked_cninfo_terms_credentials_endpoint_dictionary_and_sample_payload_required",
        "why_selected_first": "CNINFO Data Service is the official disclosure/data-service candidate and is closer to source publication semantics than public scraped pages",
        "selected_candidate": {
            "vendor": "CNINFO Data Service / Shenzhen Securities Information",
            "source_url": "https://webapi.cninfo.com.cn/",
            "source_family": "licensed_timestamped_disclosure_metadata_feed",
            "target_dataset_candidates": [
                "listed_company_announcements",
                "announcement_customization",
                "thematic_statistics_for_major_contracts_or_operation_events",
                "industry_chain_or_quantitative_data_service_if_contract_capacity_price_chain_fields_exist"
            ],
            "explicit_non_goals": [
                "do_not_use_public_cninfo_or_eastmoney_web_scraping_as_licensed_source",
                "do_not_import_current_4_symbol_daily_operation_pilot_rows",
                "do_not_accept_html_page_timestamp_as_source_published_at_without_payload_proof"
            ]
        },
        "required_local_evidence": [
            "CNINFO authorized account or API token configured outside source control",
            "license or terms file reviewed and stored outside repository secrets",
            "terms explicitly allow local storage for research and internal trading use",
            "endpoint dictionary identifies operation-event, major-contract, announcement metadata, or industry-chain dataset",
            "history access range covers 2014-present or declares audited start date",
            "sample request plan uses read-only calls only and persists no raw rows",
            "operator records rate limit, cost, and refresh latency budget"
        ],
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id_or_document_id",
            "symbol_or_company_identifier",
            "event_type_or_announcement_category",
            "event_date_or_announcement_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_url",
            "source_title_or_payload_excerpt",
            "raw_payload",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "permission_smoke": {
            "status": "blocked_no_authorized_access_or_endpoint_dictionary",
            "network_enabled": false,
            "db_write_enabled": false,
            "allowed_after_evidence": [
                "single_endpoint_read_only_permission_probe",
                "representative_history_date_probe_2014_2017_2020_2024_latest",
                "sample_payload_hash_and_timestamp_audit",
                "symbol_mapping_effective_date_scope_review"
            ],
            "forbidden_outputs": [
                "schema_apply",
                "raw_sync",
                "factor_backfill",
                "p310_diagnostics",
                "wfa",
                "v19_train_selection"
            ]
        },
        "available_at_contract": {
            "daily_rule": "date-only CNINFO announcements or metadata must map to next open trading session until timestamp precision is proven",
            "intraday_rule": "intraday use requires source_published_at timestamp and decision_timestamp >= source_published_at",
            "forbidden": [
                "using vendor ingestion time as source_published_at",
                "using announcement effective date as available_at",
                "same-session trading from date-only publication",
                "event labels inferred from future returns"
            ]
        },
        "stop_rules": [
            "stop_if_cninfo_terms_do_not_allow_local_storage_research_and_internal_trading_use",
            "stop_if_no_endpoint_dictionary_for_operation_event_major_contract_or_announcement_metadata",
            "stop_if_history_access_cannot_cover_2014_to_present_or_declared_start_date",
            "stop_if_sample_payload_lacks_source_published_at_or_auditable_date_only_publication",
            "stop_if_payload_lacks_stable_document_or_event_id_and_raw_payload_hash",
            "stop_if_event_tags_are_equivalent_to_noisy_public_daily_operation_category_without_precision_evidence"
        ],
        "promotion_gate": blocked_promotion_gate,
        "next_step": "obtain_cninfo_terms_endpoint_dictionary_and_authorized_read_only_sample_payload_before_any_network_probe"
    })
}



pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_contract() -> Value {
    let blocked_promotion_gate = json!({
        "permission_smoke": "blocked_until_manifest_exists_and_manual_review_passes",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25f-cninfo-operator-evidence-manifest-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25F",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_operator_evidence_manifest_contract_no_network_no_secret_read_no_schema_no_sync",
        "admission_decision": "blocked_until_redacted_operator_evidence_manifest_is_reviewed",
        "why_now": "P3.25E selected CNINFO as the first candidate but correctly blocks network probes until legal, endpoint, history, PIT and sample-payload evidence exists; P3.25F formalizes the external evidence manifest without storing secrets or vendor raw data in the repository",
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "evidence_manifest_contract": {
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "manifest_default_location": "operator_controlled_path_outside_git_repository",
            "repository_storage_policy": "forbid_secrets_raw_payloads_and_vendor_documents_in_repo",
            "accepted_evidence_categories": [
                "terms_review_attestation",
                "credential_presence_attestation_without_secret_value",
                "endpoint_dictionary_reference",
                "history_range_attestation",
                "sample_payload_redacted_hash_evidence",
                "available_at_source_published_at_semantics_note",
                "symbol_mapping_scope_note",
                "rate_limit_cost_refresh_latency_budget"
            ],
            "required_manifest_fields": [
                "artifact_id",
                "artifact_type",
                "owner",
                "review_status",
                "reviewed_at",
                "storage_location_type",
                "content_hash",
                "redaction_status",
                "source_effective_start_date",
                "source_effective_end_date",
                "pit_relevance",
                "notes"
            ],
            "review_status_allowed_values": [
                "missing",
                "pending_review",
                "reviewed_pass",
                "reviewed_blocked"
            ],
            "minimum_pass_conditions": [
                "terms_review_attestation_reviewed_pass",
                "credential_presence_attestation_reviewed_pass_without_secret_value",
                "endpoint_dictionary_reference_reviewed_pass",
                "history_range_attestation_covers_2014_to_present_or_declared_start",
                "sample_payload_redacted_hash_evidence_contains_stable_id_source_published_at_available_at_rule_and_raw_hash",
                "symbol_mapping_scope_note_reviewed_pass",
                "rate_limit_cost_refresh_latency_budget_reviewed_pass"
            ]
        },
        "forbidden_manifest_contents": [
            "api_token_or_password",
            "raw_vendor_payload_or_full_vendor_document",
            "unredacted_license_contract",
            "cookie_session_or_authorization_header",
            "private_endpoint_secret",
            "material_non_public_information",
            "post_event_return_label_or_oos_performance_based_event_direction"
        ],
        "manual_review_required": [
            "legal_or_operator_attestation_terms_allow_local_storage_research_and_internal_trading_use",
            "endpoint_dictionary_contains_operation_event_major_contract_announcement_metadata_or_industry_chain_dataset",
            "history_range_covers_2014_to_present_or_has_pre_registered_audited_start_date",
            "sample_payload_has_source_published_at_or_auditable_date_only_publication_rule",
            "sample_payload_has_stable_document_or_event_id_and_raw_payload_hash",
            "available_at_rule_is_next_open_session_for_date_only_publications",
            "intraday_use_requires_minute_level_source_published_at",
            "symbol_mapping_has_effective_date_scope_or_pre_registered_market_scope_exclusion",
            "event_tags_are_not_equivalent_to_noisy_daily_operation_category_without_precision_evidence"
        ],
        "allowed_after_manual_review_passes": [
            "design_read_only_permission_sample_smoke_without_persisting_raw_vendor_rows",
            "run_single_endpoint_permission_probe_with_operator_supplied_credentials_outside_source_control",
            "probe_representative_history_dates_2014_2017_2020_2024_latest",
            "audit_sample_payload_hash_timestamp_available_at_and_symbol_mapping",
            "decide_schema_contract_only_after_smoke_payload_semantics_pass"
        ],
        "stop_rules": [
            "stop_if_manifest_missing_or_not_operator_reviewed",
            "stop_if_manifest_contains_secret_or_raw_vendor_payload",
            "stop_if_terms_do_not_allow_storage_research_and_internal_trading_use",
            "stop_if_endpoint_dictionary_lacks_target_operation_event_or_metadata_dataset",
            "stop_if_sample_payload_cannot_prove_source_published_at_or_conservative_available_at",
            "stop_if_history_access_is_current_snapshot_only_or_forward_revised",
            "stop_if_symbol_mapping_requires_post_hoc_performance_weights",
            "stop_if_event_tags_are_return_derived_or_oos_tuned"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_if_evidence_cannot_be_supplied": "return_to_p3.25d_other_candidates_or_prepare_announcement_text_broader_universe_pre_registration_without_rescuing_current_4_symbol_pilot",
        "next_step": "prepare_redacted_external_cninfo_evidence_manifest_then_manual_review_before_any_read_only_network_probe"
    })
}



pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
    manifest_path: Option<&str>,
    manifest: Option<&Value>,
    read_error: Option<&str>,
) -> Value {
    let base = |manifest_status: &str, admission_decision: &str, permission_smoke: &str| {
        json!({
            "audit_version": "p3.25g-cninfo-operator-evidence-manifest-structure-audit-v1",
            "source_id": "structured_order_capacity_contract_price_chain_source",
            "stage": "P3.25G",
            "candidate_id": "cninfo_data_service",
            "mode": "read_only_cninfo_operator_evidence_manifest_structure_audit_no_network_no_secret_read_no_schema_no_sync",
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "manifest_path_configured": manifest_path
                .map(|path| !path.trim().is_empty())
                .unwrap_or(false),
            "manifest_status": manifest_status,
            "admission_decision": admission_decision,
            "runtime_actions": phase7_cninfo_operator_evidence_runtime_actions(),
            "privacy_guards": phase7_cninfo_operator_evidence_privacy_guards(),
            "promotion_gate": phase7_cninfo_operator_evidence_promotion_gate(permission_smoke)
        })
    };

    let configured_path = manifest_path.map(str::trim).filter(|path| !path.is_empty());
    if configured_path.is_none() {
        let mut response = base(
            "missing_env_var",
            "blocked_manifest_env_var_not_configured",
            "blocked_until_manifest_audit_passes",
        );
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] =
            json!("configure_quant_cninfo_evidence_manifest_path_outside_git_repository");
        return response;
    }

    if read_error.is_some() {
        let mut response = base(
            "manifest_file_unreadable_or_invalid_json",
            "blocked_manifest_file_unreadable_or_invalid_json",
            "blocked_until_manifest_audit_passes",
        );
        response["read_error_redacted"] = json!(true);
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] =
            json!("fix_external_manifest_readability_or_json_structure_without_committing_secrets");
        return response;
    }

    let Some(manifest) = manifest else {
        let mut response = base(
            "manifest_json_missing",
            "blocked_manifest_json_missing",
            "blocked_until_manifest_audit_passes",
        );
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] = json!("provide_external_redacted_json_manifest");
        return response;
    };

    let artifacts = manifest
        .get("artifacts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let artifact_count = artifacts.len();
    let mut present_categories = BTreeSet::new();
    let mut missing_required_fields = BTreeSet::new();
    let mut reviewed_pass_count = 0usize;
    let mut redaction_failure_count = 0usize;

    for artifact in &artifacts {
        for field in cninfo_operator_evidence_required_fields() {
            if !cninfo_operator_evidence_string_present(artifact.get(field)) {
                missing_required_fields.insert(field.to_string());
            }
        }
        if let Some(artifact_type) = artifact.get("artifact_type").and_then(Value::as_str) {
            present_categories.insert(artifact_type.to_string());
        }
        if artifact
            .get("review_status")
            .and_then(Value::as_str)
            .map(|status| status == "reviewed_pass")
            .unwrap_or(false)
        {
            reviewed_pass_count += 1;
        }
        let redacted = artifact
            .get("redaction_status")
            .and_then(Value::as_str)
            .map(|status| status.contains("redacted") && !status.contains("unredacted"))
            .unwrap_or(false);
        if !redacted {
            redaction_failure_count += 1;
        }
    }

    let missing_required_categories: Vec<String> = cninfo_operator_evidence_required_categories()
        .into_iter()
        .filter(|category| !present_categories.contains(*category))
        .map(ToString::to_string)
        .collect();
    let mut forbidden_manifest_keys = BTreeSet::new();
    collect_cninfo_operator_forbidden_manifest_keys(manifest, &mut forbidden_manifest_keys);
    let forbidden_manifest_keys: Vec<String> = forbidden_manifest_keys.into_iter().collect();
    let missing_required_fields: Vec<String> = missing_required_fields.into_iter().collect();

    let top_level_valid = manifest
        .get("source_id")
        .and_then(Value::as_str)
        .map(|source_id| source_id == "structured_order_capacity_contract_price_chain_source")
        .unwrap_or(false)
        && manifest
            .get("candidate_id")
            .and_then(Value::as_str)
            .map(|candidate_id| candidate_id == "cninfo_data_service")
            .unwrap_or(false);
    let structure_passed = top_level_valid
        && artifact_count > 0
        && reviewed_pass_count == artifact_count
        && redaction_failure_count == 0
        && missing_required_categories.is_empty()
        && missing_required_fields.is_empty()
        && forbidden_manifest_keys.is_empty();

    let (manifest_status, admission_decision, permission_smoke) =
        if !forbidden_manifest_keys.is_empty() {
            (
                "forbidden_content_detected",
                "blocked_manifest_contains_forbidden_secret_or_raw_payload_fields",
                "blocked_until_manifest_audit_passes",
            )
        } else if structure_passed {
            (
                "structure_passed",
                "passed_for_read_only_permission_sample_smoke_design_only",
                "allowed_read_only_sample_smoke_design_only",
            )
        } else {
            (
                "structure_incomplete_or_not_reviewed",
                "blocked_manifest_structure_or_review_incomplete",
                "blocked_until_manifest_audit_passes",
            )
        };

    let mut response = base(manifest_status, admission_decision, permission_smoke);
    response["audit_summary"] = json!({
        "artifact_count": artifact_count,
        "reviewed_pass_count": reviewed_pass_count,
        "missing_required_category_count": missing_required_categories.len(),
        "forbidden_manifest_key_count": forbidden_manifest_keys.len(),
        "missing_required_field_count": missing_required_fields.len(),
        "redaction_failure_count": redaction_failure_count,
        "top_level_source_candidate_valid": top_level_valid
    });
    response["missing_required_categories"] = json!(missing_required_categories);
    response["missing_required_fields"] = json!(missing_required_fields);
    response["forbidden_manifest_keys"] = json!(forbidden_manifest_keys);
    response["allowed_after_pass"] = json!([
        "design_read_only_permission_sample_smoke_without_persisting_raw_vendor_rows",
        "run_single_endpoint_permission_probe_only_with_operator_supplied_credentials",
        "probe_representative_history_dates_and_sample_payload_hash_timestamp_semantics"
    ]);
    response["still_forbidden_after_pass"] = json!([
        "schema_apply",
        "bounded_sync",
        "factor_builder",
        "p310_diagnostics",
        "bounded_wfa",
        "v19_train_selection"
    ]);
    response["next_step"] = if structure_passed {
        json!("design_cninfo_read_only_permission_sample_smoke_without_raw_payload_persistence")
    } else {
        json!("fix_external_redacted_manifest_then_repeat_structure_audit_before_any_network_probe")
    };
    response
}



pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan() -> Value {
    let promotion_gate = json!({
        "permission_smoke": "blocked_until_p3_25g_manifest_audit_passes",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25h-cninfo-permission-sample-smoke-plan-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25H",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_permission_sample_smoke_plan_no_network_no_secret_read_no_db_write_no_schema_no_sync",
        "admission_decision": "blocked_until_p3_25g_manifest_audit_passes",
        "why_now": "P3.25G can verify that external redacted CNINFO evidence is structurally reviewed; P3.25H defines the next read-only single-endpoint smoke plan but still performs no network call, credential read, DB write, schema apply, raw sync, factor build or training",
        "preconditions": {
            "required_previous_gate": "P3.25G",
            "required_previous_gate_endpoint": "GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-audit",
            "required_previous_gate_decision": "passed_for_read_only_permission_sample_smoke_design_only",
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "credential_source": "operator_supplied_outside_source_control_only_after_manifest_audit_passes",
            "blocked_current_production_reason": "manifest_audit_is_not_passed_or_not_configured"
        },
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "smoke_plan": {
            "endpoint_scope": "single_endpoint_only",
            "max_endpoints_per_run": 1,
            "max_sample_rows_per_probe": 20,
            "persist_raw_rows": false,
            "persist_credentials": false,
            "capture_raw_payload_in_repo": false,
            "sample_payload_handling": "hash_and_redacted_shape_only_no_raw_payload_return",
            "representative_history_dates": [
                "2014-01-02",
                "2017-01-03",
                "2020-07-01",
                "2024-01-02",
                "latest_completed_publication_or_trading_date"
            ],
            "target_endpoint_candidates_from_manifest_only": [
                "operation_event_or_major_contract_endpoint",
                "timestamped_announcement_metadata_endpoint",
                "industry_chain_or_price_capacity_order_dataset_if_manifest_reviewed"
            ],
            "expected_probe_outputs": [
                "permission_status",
                "endpoint_name_or_id_hash",
                "history_date_status_by_probe_date",
                "sample_row_count_by_probe_date",
                "schema_field_presence_summary",
                "source_published_at_quality_summary",
                "available_at_rule_summary",
                "stable_id_and_raw_hash_presence_summary",
                "symbol_mapping_presence_summary"
            ]
        },
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id_or_document_id",
            "symbol_or_company_identifier",
            "event_type_or_announcement_category",
            "event_date_or_announcement_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_url",
            "source_title_or_payload_excerpt",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "pit_and_available_at_rules": {
            "date_only_publication": "available_at must be next open trading session",
            "intraday_publication": "decision_timestamp must be >= source_published_at",
            "weekend_or_holiday_publication": "available_at maps to next open trading session",
            "forbidden": [
                "using vendor ingestion time as source_published_at",
                "using effective date as available_at",
                "same_session_trading_from_date_only_publication",
                "event_direction_from_future_returns_or_oos_performance"
            ]
        },
        "forbidden_outputs": [
            "raw_vendor_payload_persistence",
            "credential_or_token_echo",
            "schema_apply",
            "bounded_sync",
            "factor_backfill",
            "p310_diagnostics",
            "bounded_wfa",
            "v19_train_selection",
            "full_history_pull",
            "multi_endpoint_probe"
        ],
        "stop_rules": [
            "stop_if_p3_25g_manifest_audit_not_passed",
            "stop_if_endpoint_selected_outside_reviewed_manifest",
            "stop_if_probe_would_persist_raw_vendor_payload",
            "stop_if_probe_requires_more_than_one_endpoint",
            "stop_if_sample_payload_lacks_source_published_at_or_conservative_available_at_rule",
            "stop_if_history_probe_cannot_cover_2014_2017_2020_2024_latest_or_declared_start",
            "stop_if_symbol_mapping_is_current_snapshot_only_or_post_hoc",
            "stop_if_event_tags_are_equivalent_to_noisy_daily_operation_category_without_precision_evidence"
        ],
        "promotion_gate": promotion_gate,
        "allowed_next_implementation_after_precondition_passes": "implement_cninfo_single_endpoint_read_only_permission_sample_smoke_executor_no_raw_persistence",
        "next_step": "wait_for_p3_25g_manifest_audit_pass_then_implement_single_endpoint_read_only_permission_sample_smoke_executor"
    })
}



pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template() -> Value
{
    let artifacts: Vec<Value> = cninfo_operator_evidence_required_categories()
        .into_iter()
        .enumerate()
        .map(|(idx, artifact_type)| {
            json!({
                "artifact_id": format!("replace_me_cninfo_evidence_{:02}", idx + 1),
                "artifact_type": artifact_type,
                "owner": "replace_me_operator_or_reviewer",
                "review_status": "missing",
                "reviewed_at": "replace_me_iso8601_after_manual_review",
                "storage_location_type": "external_operator_controlled_redacted_reference",
                "content_hash": "replace_me_sha256_of_redacted_evidence_metadata_or_document_reference",
                "redaction_status": "missing",
                "source_effective_start_date": "replace_me_yyyy_mm_dd_or_declared_start",
                "source_effective_end_date": "replace_me_yyyy_mm_dd_or_present",
                "pit_relevance": "replace_me_why_this_artifact_supports_cninfo_pit_source_admission",
                "notes": "replace_me_redacted_summary_no_secret_no_raw_payload_no_vendor_document"
            })
        })
        .collect();

    json!({
        "audit_version": "p3.25i-cninfo-operator-evidence-manifest-template-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25I",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_operator_evidence_manifest_template_no_network_no_secret_no_db_write",
        "admission_decision": "template_only_not_evidence_blocked_until_operator_review_replaces_placeholders",
        "why_now": "P3.25G/H correctly block real CNINFO probes until an external redacted evidence manifest exists; P3.25I provides a machine-auditable template so operators can prepare evidence without committing secrets, raw payloads or vendor documents",
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "template_policy": {
            "template_can_pass_p3_25g_without_operator_review": false,
            "must_be_stored_outside_git_repository": true,
            "must_replace_all_placeholders": true,
            "must_set_review_status_to_reviewed_pass_only_after_manual_review": true,
            "must_not_include_secret_values_raw_vendor_payloads_or_full_vendor_documents": true
        },
        "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
        "suggested_external_path": "operator_controlled_path_outside_git_repository/cninfo_manifest.redacted.json",
        "manifest_template": {
            "manifest_version": "p3.25i-cninfo-operator-evidence-manifest-template-v1",
            "source_id": "structured_order_capacity_contract_price_chain_source",
            "candidate_id": "cninfo_data_service",
            "artifacts": artifacts
        },
        "operator_fill_instructions": [
            "copy_manifest_template_to_operator_controlled_path_outside_git_repository",
            "replace_every_replace_me_placeholder_with_redacted_metadata_or_hash_only",
            "keep_credential_values_contract_text_raw_payloads_and_vendor_documents_out_of_manifest",
            "set_review_status_reviewed_pass_only_after_legal_operator_manual_review",
            "configure_quant_cninfo_evidence_manifest_path_to_the_external_manifest",
            "rerun_p3_25g_operator_evidence_audit"
        ],
        "forbidden_manifest_contents": [
            "api_token",
            "password",
            "cookie",
            "authorization",
            "authorization_header",
            "raw_payload",
            "raw_vendor_payload",
            "full_vendor_document",
            "unredacted_license_contract",
            "private_endpoint_secret",
            "material_non_public_information",
            "oos_performance_label"
        ],
        "promotion_gate": {
            "manifest_audit": "blocked_until_operator_replaces_template_and_manual_review_passes",
            "permission_smoke": "blocked_until_p3_25g_manifest_audit_passes",
            "schema_apply": "blocked",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "operator_prepares_external_redacted_manifest_then_reruns_p3_25g_audit"
    })
}



fn ddl_contains_all(ddl: &str, required: &[&str]) -> (Vec<String>, Vec<String>) {
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for item in required {
        if ddl.contains(item) {
            present.push((*item).to_string());
        } else {
            missing.push((*item).to_string());
        }
    }
    (present, missing)
}



pub(crate) fn phase7_exchange_announcement_order_capacity_manual_schema_review() -> Value {
    const DDL_PATH: &str = "sql/phase7_exchange_announcement_order_capacity_source.sql";
    let ddl = include_str!("../../../../sql/phase7_exchange_announcement_order_capacity_source.sql");

    let required_fields = [
        "vendor",
        "vendor_endpoint",
        "request_key",
        "symbol",
        "announcement_id",
        "announcement_time",
        "source_published_at",
        "source_published_at_ts",
        "source_published_date",
        "source_published_at_quality",
        "available_at",
        "announcement_url",
        "pdf_final_url",
        "text_content",
        "text_hash",
        "text_hash_algorithm",
        "timestamp_candidates",
        "pdf_metadata_keys",
        "raw_payload",
        "raw_payload_hash",
        "parser_used",
        "parser_version",
        "parser_errors",
        "pdf_parse_status",
        "event_type",
        "evidence_spans",
        "ingested_at",
        "data_version_id",
    ];
    let required_constraints = [
        "PRIMARY KEY (vendor, vendor_endpoint, announcement_id, symbol, raw_payload_hash)",
        "CHECK (available_at >= announcement_time)",
        "source_published_at_quality IN ('timestamp', 'date_only_next_session', 'missing')",
        "text_hash_algorithm IN ('sha256')",
        "source_published_at_quality = 'timestamp' AND source_published_at_ts IS NOT NULL",
        "source_published_at_quality = 'date_only_next_session' AND source_published_date IS NOT NULL",
        "order_or_contract_signed",
        "capacity_expansion_or_commissioning",
        "product_price_adjustment",
        "major_supply_or_customer_agreement",
        "scanned_pdf_ocr_required",
    ];
    let required_indexes = [
        "idx_market_exchange_announcement_available_at",
        "idx_market_exchange_announcement_symbol_available_at",
        "idx_market_exchange_announcement_quality",
        "idx_market_exchange_announcement_hash",
        "idx_market_exchange_announcement_parser_status",
    ];

    let (present_fields, missing_fields) = ddl_contains_all(ddl, &required_fields);
    let (present_constraints, missing_constraints) = ddl_contains_all(ddl, &required_constraints);
    let (present_indexes, missing_indexes) = ddl_contains_all(ddl, &required_indexes);

    let passed = missing_fields.is_empty()
        && missing_constraints.is_empty()
        && missing_indexes.is_empty()
        && !ddl.contains("CREATE TABLE IF NOT EXISTS factor_")
        && !ddl.contains("model_prediction")
        && !ddl.contains("experiment_run");

    json!({
        "audit_version": "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24F",
        "mode": "read_only_schema_contract_ddl_review_no_apply",
        "write_enabled": false,
        "ddl_path": DDL_PATH,
        "schema_review_decision": if passed {
            "passed_for_bounded_sync_design_only"
        } else {
            "blocked_schema_contract_or_pit_ddl_gap"
        },
        "pit_review": {
            "timestamp_policy": "timestamp requires source_published_at_ts; intraday remains blocked unless decision-time ordering is proven",
            "date_only_next_session_policy": if ddl.contains("date_only_next_session") && ddl.contains("source_published_date") {
                "encoded"
            } else {
                "missing"
            },
            "available_at_policy": "available_at >= announcement_time is necessary but not sufficient; bounded sync must map date_only_next_session to the next open session",
            "future_data_policy": "features must still require available_at <= trade_date; this review does not admit factor generation"
        },
        "ddl_checks": {
            "required_fields": present_fields,
            "missing_required_fields": missing_fields,
            "missing_required_field_count": missing_fields.len(),
            "required_constraints": present_constraints,
            "missing_required_constraints": missing_constraints,
            "missing_required_constraint_count": missing_constraints.len(),
            "required_indexes": present_indexes,
            "missing_required_indexes": missing_indexes,
            "missing_required_index_count": missing_indexes.len(),
            "raw_failure_sample_preservation": if ddl.contains("parser_errors") && ddl.contains("pdf_parse_status") {
                "encoded"
            } else {
                "missing"
            },
            "text_evidence_preservation": if ddl.contains("evidence_spans JSONB") && ddl.contains("text_content TEXT") {
                "encoded"
            } else {
                "missing"
            }
        },
        "promotion_gate": {
            "schema_apply": if passed {
                "manual_review_passed_apply_still_requires_explicit_operator_action"
            } else {
                "blocked_until_schema_review_passes"
            },
            "bounded_sync": if passed {
                "blocked_until_plan_only_bounded_sync_review"
            } else {
                "blocked"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint does not execute DDL or write raw tables",
            "manual schema review passing is not data coverage readiness",
            "date-only announcementTime may only be mapped to next open session",
            "P3.10, WFA and v19 remain blocked until bounded sync, coverage, PIT, evidence precision and correlation audits pass"
        ],
        "next_required_steps": if passed {
            json!([
                "build_plan_only_calendar_day_symbol_category_bounded_sync_design",
                "review_calendar_day_to_open_session_mapping",
                "review_idempotent_raw_landing_and_failure_sample_policy",
                "run_one_small_batch_after_operator_schema_apply"
            ])
        } else {
            json!([
                "fix_schema_contract_or_ddl_gaps",
                "rerun_manual_schema_review_before_bounded_sync_design"
            ])
        }
    })
}



pub(crate) fn phase7_exchange_announcement_order_capacity_coverage_quality_audit_contract() -> Value {
    json!({
        "audit_version": "p3.24h-exchange-announcement-order-capacity-coverage-quality-audit-contract-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24H",
        "status": "coverage_pit_quality_contract_defined_small_batch_audit_required_next",
        "mode": "read_only_coverage_pit_quality_contract_no_raw_sync",
        "write_enabled": false,
        "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit-contract",
        "depends_on": [
            "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
            "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1"
        ],
        "small_batch_preconditions": [
            "operator_applies_sql_phase7_exchange_announcement_order_capacity_source_explicitly",
            "run_one_small_calendar_day_symbol_category_raw_sync_after_schema_apply",
            "retain ok_empty, parser_error, scanned_pdf_ocr_required and incomplete_metadata rows as auditable outcomes",
            "do not start full-history raw sync before this contract is implemented as a real audit endpoint"
        ],
        "coverage_breakdowns": [
            "year_market_category_symbol",
            "calendar_day_publication_and_next_open_session",
            "announcement_category_by_year_market",
            "event_type_by_year_market_category",
            "source_published_at_quality_by_year_category",
            "pdf_parse_status_by_year_category",
            "ok_empty_and_parser_error_by_request_key"
        ],
        "required_small_batch_audit_fields": [
            "raw_row_count",
            "request_key_count",
            "covered_calendar_day_count",
            "covered_open_session_count",
            "missing_calendar_day_count",
            "missing_next_open_session_count",
            "year_market_category_symbol_breakdown",
            "source_published_at_quality_distribution",
            "announcement_time_to_available_at_mapping_sample",
            "duplicate_announcement_id_rows",
            "duplicate_raw_payload_hash_groups",
            "missing_announcement_link_metadata_rows",
            "missing_pdf_final_url_rows",
            "parser_error_rows",
            "scanned_pdf_ocr_required_rows",
            "ocr_taxonomy_excluded_rows",
            "trainable_scanned_pdf_blocking_rows",
            "ok_empty_request_count",
            "missing_text_hash_rows",
            "missing_evidence_span_rows",
            "event_taxonomy_manual_precision_sample",
            "correlation_vs_existing_sources"
        ],
        "pit_audit": {
            "announcement_time_source": "CNInfo link announcementTime plus PDF/detail timestamp candidates when available",
            "date_only_next_session_mapping": "required_for_every_date_only_or_weekend_holiday_publication",
            "timestamp_mapping": "trusted source_published_at_ts may use same daily session only when source_published_at_ts <= decision timestamp is proven; intraday remains blocked otherwise",
            "open_session_calendar": "market_trade_calendar distinct open_date mapping; non-trading-day announcements map to the next open trading session",
            "no_future_data_rule": "every feature row must require available_at <= feature_trade_date and raw ingested_at must never be used as source publication time",
            "required_violation_checks": [
                "available_at_before_source_published_at",
                "available_at_after_feature_trade_date",
                "date_only_mapped_to_previous_open_session",
                "weekend_or_holiday_publication_dropped_or_backfilled_to_previous_session",
                "source_published_at_quality_missing_used_for_training"
            ]
        },
        "quality_thresholds": {
            "pit_violation_rows": 0,
            "missing_available_at_rows": 0,
            "missing_source_published_at_quality_rows": 0,
            "duplicate_announcement_id_rows": 0,
            "duplicate_raw_payload_hash_groups": 0,
            "missing_text_hash_rows_for_parsed_pdf": 0,
            "missing_evidence_span_rows_for_event_rows": 0,
            "missing_link_metadata_rows": 0,
            "parser_error_rows_policy": "allowed_only_as_retained_raw_failures_not_as_trainable_rows",
            "scanned_pdf_ocr_required_policy": "blocked_until_separate_ocr_runtime_and_audit_or_narrow_taxonomy_exclusion",
            "ocr_taxonomy_exclusion_policy": "only pre-registered non-target special reports such as related-party funds, finance-company transaction reports, audit reports, legal opinions and financial-advisor reports may be excluded from trainable OCR blockers",
            "trainable_scanned_pdf_blocking_rows": 0,
            "ok_empty_policy": "allowed_as_request_coverage_outcome_but_not_as_positive_event_evidence",
            "evidence_span_precision_manual_sample_min": "0.80",
            "event_taxonomy_precision_manual_sample_min": "0.80",
            "event_taxonomy_precision_sample_size_min": 50,
            "max_abs_correlation_vs_existing_source_daily_score": "0.30"
        },
        "correlation_audit": {
            "required_before_p310": true,
            "compare_against": [
                "moneyflow_congestion",
                "liquidity",
                "price_volume",
                "financial_quality_change",
                "earnings_recovery_persistence",
                "event_post_return_overlay",
                "multi_vendor_analyst_revision"
            ],
            "alignment_rule": "use conservative available_at aligned daily feature dates only; never align by announcement title date alone",
            "decision_rule": "high correlation does not imply failure alone, but requires orthogonalization or source stop before P3.10"
        },
        "diagnostics_after_contract_and_small_batch_pass": [
            "implement real coverage-quality-audit endpoint against market_exchange_announcement_text_raw",
            "expand bounded raw sync by month or quarter only after small-batch audit passes",
            "run full-history coverage/PIT/evidence/correlation audit before P3.10",
            "run P3.10A-D diagnostics only after raw source audit passes"
        ],
        "promotion_gate": {
            "schema_apply": "manual_review_passed_apply_still_requires_explicit_operator_action",
            "bounded_sync": "blocked_until_operator_schema_apply_and_one_small_batch_raw_sync_review",
            "coverage_quality_audit": "required_after_each_small_batch_and_before_any_full_history_sync",
            "full_history_sync": "blocked_until_small_batch_coverage_quality_audit_passes",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint is a read-only contract and never reads or writes raw data",
            "do not promote raw source readiness to trainable readiness",
            "do not mine keywords, event signs, horizons, or gates on the full OOS period",
            "do not use same-session or intraday date-only announcements",
            "do not enter P3.10, WFA or v19 until bounded sync, coverage, PIT, evidence precision and low-correlation audits pass"
        ],
        "next_step": "operator_schema_apply_then_one_small_batch_raw_sync_then_implement_real_coverage_quality_audit_endpoint"
    })
}



pub(crate) fn build_exchange_announcement_order_capacity_sync_plan(
    req: ExchangeAnnouncementOrderCapacitySyncPlanReq,
) -> Result<Value, String> {
    let today = Utc::now().date_naive();
    let start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("exchange announcement sync-plan start_date cannot be after end_date".into());
    }

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols);
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err(
            "exchange announcement sync-plan requires at least one comma-separated symbol".into(),
        );
    }
    let requested_categories = exchange_announcement_order_capacity_csv_values(req.categories);
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement sync-plan requires at least one category".into());
    }
    let market = req
        .market
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let batch_mode = req
        .batch
        .unwrap_or_else(|| "quarter".to_string())
        .trim()
        .to_ascii_lowercase();
    let batches = exchange_announcement_order_capacity_sync_plan_batches(start, end, &batch_mode)?;

    Ok(exchange_announcement_order_capacity_sync_plan_response(
        start,
        end,
        &batch_mode,
        market,
        symbols,
        categories,
        batches,
    ))
}



pub(crate) fn phase7_akshare_analyst_revision_schema_contract() -> Value {
    json!({
        "audit_version": "p3.23c-akshare-analyst-revision-schema-contract-v1",
        "source_id": "multi_vendor_analyst_revision",
        "stage": "P3.23C",
        "status": "history_replay_passed_schema_review_allowed",
        "mode": "read_only_vendor_schema_available_at_contract",
        "ddl_path": "sql/phase7_akshare_analyst_revision_source.sql",
        "raw_sources": [
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_rank_forecast_cninfo",
                "source_semantics": "cninfo analyst stock rating and target-price forecast by publication date",
                "request_key": "date",
                "native_available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "observed_smoke": {
                    "akshare_version": "1.18.64",
                    "date": "20260623",
                    "row_count": 26,
                    "fields": ["证券代码", "发布日期", "研究机构简称", "研究员名称", "投资评级", "是否首次评级", "评级变化", "前一次投资评级", "目标价格-上限", "目标价格-下限"]
                },
                "admission_gate": "permission_history_date_available_at_and_full_history_coverage_audit_required_before_raw_sync"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_research_report_em",
                "source_semantics": "eastmoney single-stock research report list with rating, institution, earnings forecast, report date and pdf link",
                "request_key": "symbol",
                "native_available_at_candidate": "日期",
                "source_published_at_candidate": "日期",
                "observed_smoke": {
                    "akshare_version": "1.18.64",
                    "symbol": "000001",
                    "row_count": 225
                },
                "admission_gate": "low_fanout_evidence_layer_only_until_full_symbol_fanout_coverage_cost_and_history_stability_pass"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_profit_forecast_em",
                "source_semantics": "current consensus profit forecast snapshot",
                "request_key": "snapshot",
                "native_available_at_candidate": "none_observed",
                "admission_gate": "blocked_snapshot_not_pit_ready_without_vendor_snapshot_archive"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_institute_recommend_or_ths_family",
                "source_semantics": "institution recommendation/profit forecast pages observed as parser-unstable in smoke",
                "request_key": "varies",
                "native_available_at_candidate": "not_admitted",
                "admission_gate": "blocked_parse_unreliable_do_not_sync"
            },
            {
                "vendor": "tushare",
                "vendor_endpoint": "report_rc",
                "source_semantics": "sell-side research report earnings forecast daily since 2010",
                "request_key": "report_date_range",
                "native_available_at_candidate": "report_date",
                "admission_gate": "blocked_current_api_unknown_source_after_40101_permission_smoke"
            }
        ],
        "tables": [
            {
                "table": "market_vendor_analyst_revision_raw",
                "natural_key": ["vendor", "vendor_endpoint", "request_key", "symbol", "source_published_at", "raw_payload_hash"],
                "required_fields": [
                    "vendor",
                    "vendor_source",
                    "vendor_endpoint",
                    "request_key",
                    "symbol",
                    "publication_date",
                    "source_published_at",
                    "available_at",
                    "ingested_at",
                    "institution_name",
                    "analyst_name",
                    "rating_current",
                    "rating_previous",
                    "rating_change",
                    "is_first_rating",
                    "target_price_min",
                    "target_price_max",
                    "report_title",
                    "report_url",
                    "raw_payload",
                    "raw_payload_hash",
                    "data_version_id"
                ],
                "pit_rule": "available_at must be no earlier than the native source publication date; without audited intraday timestamp, intraday trading must use next-session availability only.",
                "raw_landing_policy": "preserve vendor rows and raw payload hash; source admission gates decide whether rows can feed diagnostics."
            }
        ],
        "available_at_policy": {
            "stock_rank_forecast_cninfo": "发布日期 is a date-level available_at/source_published_at candidate; source must pass historical date replay and same-day publication timing audit before intraday use.",
            "stock_research_report_em": "日期 is a candidate only for evidence-layer reports; full-market fanout and pagination/history stability must pass before factor use.",
            "stock_profit_forecast_em": "current snapshot has no historical snapshot date, so it is blocked for 2014-2026 PIT revision unless a vendor snapshot archive is built prospectively.",
            "blocked_parse_unreliable": "parser-unstable endpoints must not create raw tables until smoke becomes repeatable."
        },
        "coverage_audit_required": [
            "history_date_replay_by_year",
            "trading_day_and_calendar_day_breakdown",
            "vendor_endpoint_symbol_breadth",
            "publication_date_null_or_future_leak_count",
            "source_published_at_null_count",
            "revision_semantics_rating_change_and_previous_rating_coverage",
            "duplicate_raw_payload_hash_count",
            "cross_vendor_overlap_vs_tushare_report_rc_if_available"
        ],
        "promotion_gate": {
            "schema_apply": "schema_review_allowed_after_history_replay_passed",
            "bounded_sync": "blocked_until_schema_review_and_manual_apply",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "sync_plan_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/sync-plan",
        "readiness_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/readiness-audit",
        "next_step": "manual_schema_review_apply_then_bounded_calendar_day_sync"
    })
}



pub(crate) fn phase7_akshare_analyst_revision_available_at_contract() -> Value {
    json!({
        "audit_version": "p3.23a-akshare-analyst-revision-available-at-contract-v1",
        "source_id": "multi_vendor_analyst_revision",
        "mode": "read_only_vendor_available_at_source_published_at_semantics_audit",
        "endpoint_semantics": [
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_rank_forecast_cninfo",
                "available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "revision_fields": ["评级变化", "前一次投资评级", "投资评级", "是否首次评级", "目标价格-上限", "目标价格-下限"],
                "verdict": "history_replay_required_before_raw_sync",
                "blocked_until": ["permission_smoke_passed", "multiple_history_dates_return_replayable_rows", "publication_date_parse_rate_audited", "source_published_at_lag_policy_registered"]
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_research_report_em",
                "available_at_candidate": "日期",
                "source_published_at_candidate": "日期",
                "revision_fields": ["评级", "机构", "盈利预测", "报告名称", "PDF链接"],
                "verdict": "low_fanout_evidence_layer_only_until_full_symbol_fanout_coverage_passes",
                "blocked_until": ["symbol_fanout_cost_audited", "pagination_history_stability_audited", "report_date_parse_rate_audited"]
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_profit_forecast_em",
                "available_at_candidate": "none_observed",
                "source_published_at_candidate": "none_observed",
                "revision_fields": [],
                "verdict": "blocked_snapshot_not_pit_ready",
                "blocked_reason": "current snapshot without historical snapshot date cannot reconstruct 2014-2026 PIT revision history"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_institute_recommend_or_ths_family",
                "available_at_candidate": "not_admitted",
                "source_published_at_candidate": "not_admitted",
                "revision_fields": [],
                "verdict": "blocked_parse_unreliable",
                "blocked_reason": "prior smoke observed parser/XML failures; do not create schema or sync until repeatability is proven"
            },
            {
                "vendor": "tushare",
                "vendor_endpoint": "report_rc",
                "available_at_candidate": "report_date",
                "source_published_at_candidate": "report_date_or_create_time",
                "revision_fields": ["rating", "eps", "max_price", "min_price", "quarter", "org_name", "author_name"],
                "verdict": "blocked_permission_unknown_source",
                "blocked_reason": "production smoke returned Tushare 40101 unknown data source"
            }
        ],
        "pit_policy": {
            "daily_research": "date-level publication fields are allowed only as end-of-day/next-session availability until intraday source timestamps are audited.",
            "downstream_rule": "factor and diagnostics must filter source.available_at <= equity_trade_date; intraday simulation must additionally require source_published_at <= decision_timestamp.",
            "prohibited": [
                "using current snapshot fields to reconstruct past consensus",
                "using ingestion time as historical source publication time",
                "using full-period coverage or OOS results to choose endpoint sign or endpoint inclusion"
            ]
        },
        "promotion_gate": {
            "schema_apply": "blocked_until_permission_history_available_at_review",
            "bounded_sync": "blocked_until_schema_review_and_history_smoke_pass",
            "coverage_status": "blocked_until_raw_sync_exists",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "permission_history_date_smoke_for_stock_rank_forecast_cninfo_across_multiple_years"
    })
}



fn phase7_multi_vendor_analyst_revision_candidate() -> Value {
    json!({
        "source_id": "multi_vendor_analyst_revision",
        "source_family": "broad_base_analyst_expectation_revision",
        "economic_hypothesis": "真正的评级/目标价/盈利预期修正如果能做到 broad-base、PIT、低相关，可能比已证伪的公开低频经营代理更接近可交易的信息增量；供应商替换不能绕过 source admission、coverage、P3.10 或 bounded WFA。",
        "candidate_raw_sources": [
            "akshare:stock_rank_forecast_cninfo",
            "akshare:stock_research_report_em",
            "akshare:stock_profit_forecast_em_blocked_snapshot",
            "akshare:stock_institute_recommend_blocked_parse_unreliable",
            "tushare:report_rc_blocked_40101"
        ],
        "source_discovery_evidence": [
            {
                "candidate": "akshare:stock_rank_forecast_cninfo",
                "status": "stopped_after_p310_economics_failed",
                "akshare_version": "1.18.64",
                "observed_smoke": {
                    "date": "20260623",
                    "row_count": 26,
                    "fields": ["证券代码", "发布日期", "研究机构简称", "研究员名称", "投资评级", "是否首次评级", "评级变化", "前一次投资评级", "目标价格-上限", "目标价格-下限"]
                },
                "history_date_smoke": "passed",
                "native_available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "full_history_summary": {
                    "raw_rows": 1222450,
                    "raw_date_range": "2014-01-01..2026-06-24",
                    "factor_task_id": "fs-20260624-155815497-05f24316",
                    "combo_rows": 1025751,
                    "combo_date_range": "2014-01-03..2026-06-23",
                    "future_leak_rows": 0,
                    "null_available_at_rows": 0,
                    "null_score_rows": 0
                },
                "diagnostics_summary": {
                    "latest_report_id": "exp-0930e5fa-f125-4f22-b9d2-5041da2c44c3",
                    "level": "red",
                    "passed": false,
                    "mean_rankic_20_45_60_120": [-0.00216, -0.00190, -0.00298, 0.00588],
                    "high_minus_low_spread_20_45_60_120": [0.00372, 0.00269, 0.00616, 0.00824],
                    "passed_horizon_count": 0,
                    "daily_weak_day_count": 1316,
                    "daily_weak_day_ratio": 0.4345,
                    "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                },
                "decision": "stop_current_akshare_cninfo_revision_expression_after_p310_economics_failed"
            },
            {
                "candidate": "akshare:stock_research_report_em",
                "status": "low_fanout_evidence_layer_candidate",
                "akshare_version": "1.18.64",
                "observed_smoke": {
                    "symbol": "000001",
                    "row_count": 225
                },
                "native_available_at_candidate": "日期",
                "decision": "do_not_treat_as_broad_base_until_full_symbol_fanout_coverage_and_pagination_history_stability_pass"
            },
            {
                "candidate": "akshare:stock_profit_forecast_em",
                "status": "blocked_snapshot_not_pit_ready",
                "blocked_reason": "current snapshot has no historical snapshot date and cannot reconstruct 2014-2026 PIT consensus revisions"
            },
            {
                "candidate": "akshare:stock_institute_recommend_or_ths_family",
                "status": "blocked_parse_unreliable",
                "blocked_reason": "prior smoke observed parser/XML failures"
            },
            {
                "candidate": "tushare:report_rc",
                "status": "blocked_current_api_unknown_source_after_production_smoke",
                "production_smoke": {
                    "as_of": "2026-06-21",
                    "error_code": "40101",
                    "error": "未知的数据源"
                }
            }
        ],
        "current_tables": ["market_vendor_analyst_revision_raw", "factor_value", "multi_factor_value"],
        "schema_status": "completed",
        "client_status": "read_only_and_sync_client_completed",
        "sync_status": "full_history_raw_sync_completed",
        "coverage_status": "coverage_pit_quality_correlation_green",
        "p310_status": "completed_failed_economics",
        "diagnostics_summary": {
            "latest_report_id": "exp-0930e5fa-f125-4f22-b9d2-5041da2c44c3",
            "level": "red",
            "passed": false,
            "mean_rankic_20_45_60_120": [-0.00216, -0.00190, -0.00298, 0.00588],
            "high_minus_low_spread_20_45_60_120": [0.00372, 0.00269, 0.00616, 0.00824],
            "passed_horizon_count": 0,
            "daily_weak_day_count": 1316,
            "daily_weak_day_ratio": 0.4345,
            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
        },
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "pit_required": true,
        "available_at_policy": "vendor publication date or report date must be persisted as source_published_at/available_at; without intraday timestamp, same-day use is blocked for intraday simulation and only next-session availability is allowed",
        "schema_contract_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/schema-contract",
        "permission_smoke_endpoint": "POST /api/v1/quant/data/akshare/analyst-revision/permission-smoke",
        "available_at_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/available-at-audit",
        "coverage_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/coverage-audit",
        "admission_decision": "stopped_after_p310_economics_failed",
        "blocked_reason": "raw_pit_coverage_and_factor_combo_completed_but_daily_symbol_cliff_and_p310_economics_failed; no_same_family_event_window_or_horizon_rescue",
        "next_step": "search_licensed_broad_base_consensus_revision_or_exchange_announcement_order_capacity_source"
    })
}



pub(crate) fn shareholder_structure_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            "market_stock_holder_number",
            vec![
                "market_stock_holder_number_pkey",
                "market_stock_holder_number_available_at_check",
            ],
        ),
        (
            "market_stock_top10_holders",
            vec![
                "market_stock_top10_holders_pkey",
                "market_stock_top10_holders_available_at_check",
            ],
        ),
        (
            "market_stock_top10_float_holders",
            vec![
                "market_stock_top10_float_holders_pkey",
                "market_stock_top10_float_holders_available_at_check",
            ],
        ),
        (
            "market_stock_holder_trade",
            vec![
                "market_stock_holder_trade_pkey",
                "market_stock_holder_trade_available_at_check",
                "market_stock_holder_trade_interval_check",
            ],
        ),
    ]
}



pub(crate) async fn table_exists(db: &sqlx::PgPool, table: &str) -> Result<bool, String> {
    let regclass_name = format!("public.{table}");
    sqlx::query_scalar("SELECT to_regclass($1)::text IS NOT NULL")
        .bind(&regclass_name)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to inspect table {table}: {error}"))
}



pub(crate) fn decide_equity_pledge_readiness(
    schema_passed: bool,
    stat_rows: i64,
    detail_rows: i64,
    pit_violation_rows: i64,
) -> Value {
    let raw_rows = stat_rows + detail_rows;
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_readiness_audit_required_before_p310",
            "run_year_symbol_ann_date_coverage_and_duplicate_audit_before_factor_builder",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_readiness_passes",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "stat_rows": stat_rows,
        "detail_rows": detail_rows,
        "raw_rows": raw_rows,
        "pit_violation_rows": pit_violation_rows,
        "next_step": next_step,
    })
}



pub(crate) fn decide_margin_detail_readiness(
    schema_passed: bool,
    raw_rows: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    core_negative_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_margin_detail_source_then_rerun_readiness_audit",
        )
    } else if raw_rows <= 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_margin_detail_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "repair_available_at_or_delete_bad_margin_detail_rows_then_rerun_sync_and_audit",
        )
    } else if missing_source_published_at_rows > 0 {
        (
            "created",
            "raw_synced_publication_time_incomplete",
            "raw_publication_time_failed",
            "repair_margin_detail_source_published_at_before_intraday_or_p310_use",
        )
    } else if core_negative_rows > 0 {
        (
            "created",
            "raw_synced_quality_failed",
            "raw_core_nonnegative_failed",
            "inspect_core_negative_margin_detail_rows_then_repair_exclude_or_gate",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_correlation_readiness_audit_required_before_p310",
            "run_year_market_symbol_negative_adjustment_and_correlation_audit_before_p310",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_pit_quality_and_correlation_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "pit_violation_rows": pit_violation_rows,
        "missing_source_published_at_rows": missing_source_published_at_rows,
        "core_negative_rows": core_negative_rows,
        "negative_adjustment_policy": {
            "rzche": "allowed_as_raw_vendor_adjustment_and_must_be_reported",
            "rqchl": "allowed_as_raw_vendor_adjustment_and_must_be_reported",
            "core_nonnegative_fields": ["rzye", "rqye", "rzmre", "rqyl", "rqmcl", "rzrqye"]
        },
        "next_step": next_step,
    })
}



pub(crate) fn margin_detail_correlation_decision(max_abs_correlation: Option<f64>) -> &'static str {
    match max_abs_correlation {
        Some(value) if value >= 0.70 => "blocked_same_family_high_correlation",
        Some(value) if value >= 0.50 => {
            "caution_same_family_medium_correlation_requires_manual_review"
        }
        Some(_) => "passed_low_linear_correlation_screen",
        None => "blocked_until_correlation_sample_available",
    }
}



pub(crate) fn decide_margin_detail_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    covered_trade_days: i64,
    open_trade_days: i64,
    covered_trade_day_ratio: f64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    core_negative_rows: i64,
    missing_year_count: i64,
    correlation_decision: &str,
) -> Value {
    const MIN_MARGINABLE_SYMBOL_COVERAGE: f64 = 0.20;
    const MIN_RAW_ROWS_FOR_P310: i64 = 1_000_000;
    let missing_open_trade_day_count = (open_trade_days - covered_trade_days).max(0);

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_margin_detail_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_margin_detail_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "repair_available_at_or_delete_bad_margin_detail_rows_then_rerun_audit",
        )
    } else if missing_source_published_at_rows > 0 {
        (
            "source_publication_time_failed",
            "source_publication_time_failed",
            "backfill_conservative_source_published_at_before_intraday_or_p310_use",
        )
    } else if core_negative_rows > 0 {
        (
            "raw_core_quality_failed",
            "raw_core_quality_failed",
            "inspect_core_negative_margin_detail_rows_then_repair_exclude_or_gate",
        )
    } else if open_trade_days <= 0 || missing_year_count > 0 || missing_open_trade_day_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_quarter_or_trade_day_margin_detail_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_MARGINABLE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_margin_detail_sync_then_rerun_coverage_audit",
        )
    } else if correlation_decision != "passed_low_linear_correlation_screen" {
        (
            "correlation_gate_failed_or_requires_review",
            "correlation_readiness_not_passed",
            "complete_moneyflow_liquidity_price_volume_correlation_review_before_p310",
        )
    } else {
        (
            "coverage_correlation_readiness_ready_for_p310_diagnostics",
            "coverage_correlation_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_correlation_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_pit_quality_and_correlation_readiness_pass"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "covered_trade_days": covered_trade_days,
        "open_trade_days": open_trade_days,
        "missing_open_trade_day_count": missing_open_trade_day_count,
        "covered_trade_day_ratio": covered_trade_day_ratio,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "missing_source_published_at_rows": missing_source_published_at_rows,
        "core_negative_rows": core_negative_rows,
        "missing_year_count": missing_year_count,
        "correlation_decision": correlation_decision,
        "next_step": next_step,
    })
}



pub(crate) fn decide_shareholder_structure_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
    data_quality_violation_rows: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_RAW_ROWS_FOR_P310: i64 = 250_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "review_period_snapshot_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if data_quality_violation_rows > 0 {
        (
            "raw_quality_failed",
            "raw_quality_failed",
            "review_ratio_and_interval_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_shareholder_structure_source_row_hash_duplicates_before_feature_builder",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "data_quality_violation_rows": data_quality_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
        "next_step": next_step,
    })
}



pub(crate) fn decide_shareholder_structure_strict_low_fanout_gate(
    schema_passed: bool,
    admissible_rows: i64,
    symbol_coverage_ratio: f64,
    duplicate_source_row_hash_count: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_ADMISSIBLE_ROWS_FOR_P310: i64 = 250_000;

    let (status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_strict_low_fanout_gate",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if admissible_rows <= 0 {
        (
            "admissible_rows_missing",
            "bounded_sync_required_before_strict_low_fanout_gate",
            "run_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_admissible_rows_failed",
            "duplicate_admissible_rows_failed",
            "inspect_strict_low_fanout_duplicate_source_hashes_before_p310",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || admissible_rows < MIN_ADMISSIBLE_ROWS_FOR_P310
    {
        (
            "undercovered_or_too_sparse",
            "bounded_sample_passed_needs_more_admissible_history",
            "increase_strict_low_fanout_admissible_coverage_before_p310",
        )
    } else {
        (
            "strict_low_fanout_ready_for_p310_diagnostics",
            "strict_low_fanout_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics_with_shareholder_structure_gate",
        )
    };

    json!({
        "gate_id": SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
        "source_scope": "low_fanout_holder_number_holder_trade",
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "strict_low_fanout_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_strict_low_fanout_gate_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "admissible_rows": admissible_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
        "next_step": next_step,
    })
}



pub(crate) fn shareholder_structure_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batches: Vec<ShareholderStructureSyncPlanBatch>,
) -> Value {
    const SAFE_FULL_RANGE_UNIT_LIMIT: i64 = 50_000;

    let batch_values = batches
        .iter()
        .map(|batch| {
            let global_ann_date_units = batch.quarter_count * 2;
            let symbol_quarter_units = batch.symbol_count * batch.quarter_count * 2;
            let estimated_units = global_ann_date_units + symbol_quarter_units;
            json!({
                "year": batch.year,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "symbol_count": batch.symbol_count,
                "quarter_count": batch.quarter_count,
                "global_ann_date_units": global_ann_date_units,
                "symbol_quarter_units": symbol_quarter_units,
                "estimated_units": estimated_units,
                "recommended_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("shareholder-structure-{}", batch.year),
                    "background": true
                },
                "recommended_low_fanout_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["holder_number", "holder_trade"],
                    "data_version_id": format!("shareholder-structure-{}-low-fanout", batch.year),
                    "background": true
                },
                "recommended_top10_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["top10_holders", "top10_float_holders"],
                    "data_version_id": format!("shareholder-structure-{}-top10", batch.year),
                    "background": true
                }
            })
        })
        .collect::<Vec<_>>();

    let estimated_total_units = batches
        .iter()
        .map(|batch| batch.quarter_count * 2 + batch.symbol_count * batch.quarter_count * 2)
        .sum::<i64>();
    let safe_to_run_full_range = estimated_total_units <= SAFE_FULL_RANGE_UNIT_LIMIT;

    json!({
        "audit_version": "p3.21c-shareholder-structure-sync-plan-v1",
        "source_id": "shareholder_structure",
        "mode": "read_only_bounded_sync_plan",
        "date_range": {
            "start_date": start.format("%Y-%m-%d").to_string(),
            "end_date": end.format("%Y-%m-%d").to_string(),
        },
        "batch_count": batch_values.len(),
        "estimated_total_units": estimated_total_units,
        "safe_full_range_unit_limit": SAFE_FULL_RANGE_UNIT_LIMIT,
        "safe_to_run_full_range": safe_to_run_full_range,
        "recommended_batch_granularity": if safe_to_run_full_range { "full_range" } else { "year" },
        "unit_model": {
            "global_ann_date_units": "quarter_count * 2 for holder_number and holder_trade",
            "symbol_quarter_units": "symbol_count * quarter_count * 2 for top10_holders and top10_floatholders",
            "warning": "plan is read-only and does not call Tushare"
        },
        "batches": batch_values,
        "prohibited": [
            "do_not_run_2014_2026_full_range_without_reviewing_estimated_units",
            "do_not_enter_factor_p310_wfa_until_coverage_readiness_and_anomaly_gates_pass"
        ]
    })
}



pub(crate) fn decide_equity_pledge_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.30;
    const MIN_RAW_ROWS_FOR_P310: i64 = 10_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_pledge_detail_source_row_hash_duplicates_before_feature_builder",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_equity_pledge_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "next_step": next_step,
    })
}



pub(crate) fn decide_futures_price_chain_readiness(
    schema_passed: bool,
    raw_rows: i64,
    mapping_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "schema_created_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_then_coverage_readiness_audit",
        )
    } else if mapping_rows == 0 {
        (
            "created",
            "raw_synced_mapping_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_before_factor_backfill",
        )
    } else {
        (
            "created",
            "raw_and_mapping_present",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_readiness_passes",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "mapping_rows": mapping_rows,
        "next_step": next_step,
    })
}

#[cfg(test)]


fn futures_price_chain_product_symbol_from_daily_ts_code(ts_code: &str) -> Option<String> {
    let value = ts_code.trim();
    let product = value
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>();
    let month_code = value
        .chars()
        .skip(product.len())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if month_code.len() != 4 {
        return None;
    }
    if value.chars().nth(product.len() + month_code.len()) != Some('.') {
        return None;
    }
    futures_price_chain_normalize_raw_product_symbol(&product)
}



pub(crate) fn decide_futures_price_chain_mapping_audit(
    schema_passed: bool,
    raw_product_count: i64,
    mapped_product_count: i64,
    excluded_product_count: i64,
    missing_product_count: i64,
    invalid_interval_rows: i64,
    mapping_pit_violation_rows: i64,
    unsupported_exposure_rows: i64,
    invalid_exclusion_interval_rows: i64,
    exclusion_pit_violation_rows: i64,
) -> Value {
    let covered_product_count = mapped_product_count + excluded_product_count;
    let (mapping_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_mapping_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_mapping_audit",
        )
    } else if raw_product_count == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_mapping_audit",
            "run_bounded_futures_price_chain_sync_before_mapping_audit",
        )
    } else if covered_product_count == 0 {
        (
            "mapping_and_exclusion_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_or_exclusion_gate_before_factor_backfill",
        )
    } else if unsupported_exposure_rows > 0
        || invalid_interval_rows > 0
        || mapping_pit_violation_rows > 0
        || invalid_exclusion_interval_rows > 0
        || exclusion_pit_violation_rows > 0
    {
        (
            "mapping_integrity_failed",
            "mapping_integrity_failed_before_feature_or_p310",
            "repair_mapping_or_exclusion_type_interval_and_available_at_before_coverage_audit",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else if mapped_product_count == 0 {
        (
            "all_raw_products_excluded",
            "all_products_excluded_no_trainable_price_chain_source",
            "stop_futures_price_chain_factor_source_or_add_evidence_backed_industry_mappings",
        )
    } else {
        (
            "mapping_coverage_ready",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
        )
    };

    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "schema_status": if schema_passed { "created" } else { "missing_or_invalid" },
        "sync_status": if raw_product_count > 0 { "raw_synced" } else { "not_started" },
        "mapping_status": mapping_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_mapping_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_product_count": raw_product_count,
        "mapped_product_count": mapped_product_count,
        "excluded_product_count": excluded_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "invalid_interval_rows": invalid_interval_rows,
        "mapping_pit_violation_rows": mapping_pit_violation_rows,
        "unsupported_exposure_rows": unsupported_exposure_rows,
        "invalid_exclusion_interval_rows": invalid_exclusion_interval_rows,
        "exclusion_pit_violation_rows": exclusion_pit_violation_rows,
        "next_step": next_step,
    })
}



pub(crate) fn decide_futures_price_chain_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    raw_product_count: i64,
    covered_product_count: i64,
    missing_product_count: i64,
    raw_pit_violation_rows: i64,
    failed_sync_attempt_count: i64,
) -> Value {
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };
    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_coverage_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_coverage_audit",
        )
    } else if raw_rows == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_before_coverage_audit",
        )
    } else if raw_pit_violation_rows > 0 {
        (
            "raw_pit_integrity_failed",
            "raw_pit_integrity_failed_before_feature_or_p310",
            "repair_raw_available_at_before_mapping_or_p310",
        )
    } else if failed_sync_attempt_count > 0 {
        (
            "sync_attempt_failures_present",
            "sync_attempt_failures_require_retry_before_feature_or_p310",
            "retry_or_explain_failed_futures_price_chain_sync_attempts_before_p310",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "coverage_mapping_ready",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "raw_rows": raw_rows,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "raw_pit_violation_rows": raw_pit_violation_rows,
        "failed_sync_attempt_count": failed_sync_attempt_count,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_mapping_and_coverage_readiness_pass"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
}



pub(crate) fn futures_price_chain_coverage_promotion_gate(decision: &Value) -> Value {
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_mapping_and_coverage_readiness_pass");
    let p310_ready = p310_status == "ready_for_p310_diagnostics";

    json!({
        "factor_builder": if p310_ready {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_coverage_mapping_and_pit_pass"
        },
        "p310_status": p310_status,
        "wfa_status": "blocked",
        "v19_train_selection": "blocked"
    })
}



pub(crate) fn futures_price_chain_raw_product_summary_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        product_symbol,
        COUNT(*)::bigint AS raw_rows,
        string_agg(DISTINCT endpoint, ',' ORDER BY endpoint) AS endpoints,
        string_agg(DISTINCT exchange_key, ',' ORDER BY exchange_key) AS exchanges,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY product_symbol
    ORDER BY product_symbol
    "#
}



pub(crate) fn futures_price_chain_coverage_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    raw AS (
        SELECT
            endpoint,
            EXTRACT(YEAR FROM trade_date)::int AS trade_year,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        trade_year,
        product_symbol,
        exchange_key,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint, trade_year, product_symbol, exchange_key
    ORDER BY trade_year, endpoint, product_symbol, exchange_key
    "#
}



pub(crate) fn futures_price_chain_sync_attempt_breakdown_sql() -> &'static str {
    r#"
    WITH futures_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND exchange IN ('SHFE', 'DCE', 'CZCE', 'CFFEX', 'INE')
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    fallback_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    open_calendar AS (
        SELECT trade_date FROM futures_calendar
        UNION
        SELECT trade_date FROM fallback_calendar
        WHERE NOT EXISTS (SELECT 1 FROM futures_calendar)
    ),
    attempts AS (
        SELECT
            attempt.*,
            open_calendar.trade_date IS NOT NULL AS is_open_trade_date
        FROM data_sync_attempt attempt
        LEFT JOIN open_calendar
          ON attempt.start_date = open_calendar.trade_date
         AND attempt.end_date = open_calendar.trade_date
        WHERE source IN (
            'futures_price_chain_daily',
            'futures_price_chain_wsr',
            'futures_price_chain_holding'
        )
          AND ($1::date IS NULL OR end_date >= $1::date)
          AND ($2::date IS NULL OR start_date <= $2::date)
    )
    SELECT
        source,
        status,
        COUNT(*) FILTER (WHERE is_open_trade_date)::bigint AS attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE is_open_trade_date), 0)::bigint AS row_count,
        MIN(start_date) FILTER (WHERE is_open_trade_date) AS min_start_date,
        MAX(end_date) FILTER (WHERE is_open_trade_date) AS max_end_date,
        COUNT(*) FILTER (WHERE is_open_trade_date AND error_message IS NOT NULL)::bigint AS error_attempt_count,
        COUNT(*) FILTER (WHERE NOT is_open_trade_date)::bigint AS non_open_attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE NOT is_open_trade_date), 0)::bigint AS non_open_row_count
    FROM attempts
    GROUP BY source, status
    ORDER BY source, status
    "#
}



pub(crate) fn futures_price_chain_raw_endpoint_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT product_symbol)::bigint AS product_count,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint
    ORDER BY endpoint
    "#
}



pub(crate) fn futures_price_chain_mapping_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS mapping_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS mapping_table_product_count,
        COUNT(*) FILTER (
            WHERE exposure_type NOT IN ('sw_industry', 'stock_symbol')
        )::bigint AS unsupported_exposure_rows,
        COUNT(*) FILTER (
            WHERE valid_to IS NOT NULL AND valid_to < valid_from
        )::bigint AS invalid_interval_rows,
        COUNT(*) FILTER (
            WHERE available_at < valid_from
        )::bigint AS mapping_pit_violation_rows,
        COUNT(*) FILTER (
            WHERE NULLIF(trim(source), '') IS NULL OR evidence = '{}'::jsonb
        )::bigint AS weak_evidence_rows
    FROM market_futures_product_exposure_mapping_pit
    "#
}



pub(crate) fn futures_price_chain_exclusion_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS exclusion_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS exclusion_table_product_count,
        COUNT(*) FILTER (
            WHERE valid_to IS NOT NULL AND valid_to < valid_from
        )::bigint AS invalid_interval_rows,
        COUNT(*) FILTER (
            WHERE available_at < valid_from
        )::bigint AS exclusion_pit_violation_rows,
        COUNT(*) FILTER (
            WHERE NULLIF(trim(source), '') IS NULL OR evidence = '{}'::jsonb
        )::bigint AS weak_evidence_rows
    FROM market_futures_product_exclusion_gate_pit
    WHERE gate_scope = 'futures_price_chain_factor'
    "#
}



pub(crate) fn futures_price_chain_industry_targets_sql() -> &'static str {
    r#"
    SELECT
        index_code,
        index_name,
        industry_code,
        industry_name,
        COUNT(DISTINCT symbol)::bigint AS current_or_historical_symbols,
        MIN(in_date) AS min_in_date,
        MAX(in_date) AS max_in_date,
        MAX(available_at) AS latest_available_at
    FROM market_stock_industry_membership_pit
    WHERE classification_source = 'SW2021'
      AND industry_level = 'L1'
    GROUP BY index_code, index_name, industry_code, industry_name
    ORDER BY index_code
    "#
}



pub(crate) fn validate_futures_price_chain_mapping_candidate(
    candidate: &FuturesPriceChainMappingCandidate,
    raw_products: &BTreeSet<String>,
    sw2021_l1_targets: &BTreeSet<String>,
) -> FuturesPriceChainMappingCandidateValidation {
    let product_symbol = candidate.product_symbol.trim().to_ascii_uppercase();
    let exposure_type = candidate.exposure_type.trim().to_string();
    let exposure_code = candidate.exposure_code.trim().to_ascii_uppercase();
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    if product_symbol.is_empty() {
        errors.push("product_symbol_required".to_string());
    } else if !raw_products.contains(&product_symbol) {
        errors.push("unknown_raw_product_symbol".to_string());
    }

    if exposure_type != "sw_industry" {
        if exposure_type == "stock_symbol" {
            errors.push("direct_stock_mapping_requires_separate_evidence_gate".to_string());
        } else {
            errors.push("unsupported_exposure_type".to_string());
        }
    } else if !sw2021_l1_targets.contains(&exposure_code) {
        errors.push("unknown_sw2021_l1_exposure_code".to_string());
    }

    if candidate.direction != -1 && candidate.direction != 1 {
        errors.push("direction_must_be_minus_one_or_one".to_string());
    }
    if !(candidate.weight > 0.0 && candidate.weight <= 1.0) {
        errors.push("weight_must_be_gt_0_and_lte_1".to_string());
    }

    let valid_from = parse_futures_price_chain_mapping_date(&candidate.valid_from);
    let valid_to = candidate
        .valid_to
        .as_deref()
        .map(parse_futures_price_chain_mapping_date)
        .transpose();
    let available_at = parse_futures_price_chain_mapping_date(&candidate.available_at);

    match (&valid_from, &valid_to) {
        (Ok(start), Ok(Some(end))) if end < start => {
            errors.push("valid_to_before_valid_from".to_string())
        }
        _ => {}
    }
    match (&valid_from, &available_at) {
        (Ok(start), Ok(available)) if available < start => {
            errors.push("available_at_before_valid_from".to_string())
        }
        _ => {}
    }
    if valid_from.is_err() {
        errors.push("valid_from_invalid".to_string());
    }
    if valid_to.is_err() {
        errors.push("valid_to_invalid".to_string());
    }
    if available_at.is_err() {
        errors.push("available_at_invalid".to_string());
    }

    if candidate.source.trim().is_empty() {
        errors.push("source_required".to_string());
    }
    if candidate.mapping_version.trim().is_empty() {
        errors.push("mapping_version_required".to_string());
    }
    match &candidate.evidence {
        Value::Object(object) if !object.is_empty() => {}
        _ => errors.push("evidence_required".to_string()),
    }
    if candidate.evidence.get("source_url").is_none()
        && candidate.evidence.get("source_document").is_none()
        && candidate.evidence.get("review_note").is_none()
    {
        warnings.push("evidence_should_include_source_url_or_review_note".to_string());
    }

    FuturesPriceChainMappingCandidateValidation {
        product_symbol,
        exposure_type,
        exposure_code,
        passed: errors.is_empty(),
        errors,
        warnings,
    }
}



pub(crate) fn decide_futures_price_chain_mapping_candidate_validation(
    raw_product_count: i64,
    covered_product_count: i64,
    invalid_row_count: i64,
    missing_product_count: i64,
) -> Value {
    let (admission_decision, next_step) = if raw_product_count == 0 {
        (
            "raw_sync_required_before_mapping_candidate_validation",
            "run_bounded_futures_price_chain_sync_before_mapping_template",
        )
    } else if invalid_row_count > 0 {
        (
            "mapping_candidate_validation_failed",
            "repair_candidate_rows_before_insert_or_review",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_candidate_coverage_incomplete_before_insert",
            "add_candidate_rows_for_all_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "mapping_candidate_ready_for_manual_review_before_insert",
            "manual_review_then_insert_versioned_mapping_rows",
        )
    };
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "admission_decision": admission_decision,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "invalid_row_count": invalid_row_count,
        "mapping_candidate_coverage_ratio": coverage_ratio,
        "write_enabled": false,
        "p310_status": "blocked_until_mapping_insert_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
}



fn phase7_exchange_announcement_order_capacity_candidate() -> Value {
    json!({
        "source_id": "exchange_announcement_order_capacity_text",
        "source_family": "regulatory_exchange_announcement_real_operations_text",
        "economic_hypothesis": "订单、合同、产能、投产、价格调整和重大供货协议等公告文本比常规财务/价量/资金流更接近真实经营边际变化；若能证明公告可得时间、文本证据和 broad-base 覆盖，可能提供低相关 PIT alpha source。",
        "candidate_raw_sources": [
            "akshare:stock_zh_a_disclosure_report_cninfo",
            "cninfo:announcement_detail_page",
            "licensed_vendor:broad_base_announcement_text_feed"
        ],
        "source_discovery_evidence": [
            {
                "candidate": "akshare:stock_zh_a_disclosure_report_cninfo",
                "status": "read_only_sample_smoke_available_source_discovery_only",
                "upstream": "cninfo",
                "observed_smoke": {
                    "as_of": "2026-06-25",
                    "akshare_version": "1.18.64",
                    "symbol": "000001",
                    "market": "沪深京",
                    "category": "日常经营",
                    "date_range": "20230101..20231231",
                    "row_count": 31,
                    "fields": ["代码", "简称", "公告标题", "公告时间", "公告链接"]
                },
                "native_available_at_candidate": "公告时间",
                "risk": "current smoke is one symbol/category only; category parser reliability, announcement detail text fetch and source_published_at timestamp are not audited",
                "decision": "permission_history_category_smoke_required_before_schema_or_sync"
            },
            {
                "candidate": "cninfo:announcement_detail_page",
                "status": "source_discovery_required",
                "required_from_link": ["announcementId", "orgId", "stockCode", "announcementTime"],
                "required_audit": ["text_fetch_success_rate", "source_published_at_timestamp", "text_hash_stability", "manual_evidence_span_precision_sample"],
                "decision": "do_not_sync_until_text_and_timestamp_audit_is_proven"
            },
            {
                "candidate": "licensed_vendor:broad_base_announcement_text_feed",
                "status": "source_discovery_required",
                "reason": "if public CNInfo/AkShare feed cannot provide reliable timestamps, full history, text and category stability, a licensed timestamped announcement feed is required",
                "decision": "restart_from_permission_schema_available_at_admission_if_vendor_is_available"
            }
        ],
        "current_tables": [],
        "schema_status": "schema_contract_defined_source_discovery_required",
        "client_status": "read_only_manual_smoke_only_no_rust_client",
        "sync_status": "not_started",
        "coverage_status": "not_started",
        "p310_status": "blocked",
        "factor_builder": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "pit_required": true,
        "available_at_policy": "use source_published_at from official announcement detail/feed when available; if only announcement date exists, map to next open session and forbid same-session intraday use",
        "schema_contract_endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/schema-contract",
        "admission_decision": "source_discovery_required_before_schema_or_sync",
        "blocked_reason": "sample feed proves candidate existence only; no full-history category replay, source_published_at timestamp audit, text fetch audit, evidence span precision audit, coverage audit, or correlation audit yet",
        "next_step": "implement_read_only_permission_history_category_smoke_for_akshare_cninfo_disclosure_then_cninfo_detail_text_fetch_audit"
    })
}



fn phase7_p319_candidate_admission_sources(futures_price_chain_readiness: Option<&Value>) -> Value {
    let mut admission = json!({
        "stage": "P3.24",
        "objective": "discover lower-correlation broad-base PIT alpha sources before any factor build, ML training, WFA admission, or v19 train selection",
        "hard_gate": "permission_schema_available_at_first",
        "global_policy": {
            "pit_required": true,
            "no_oos_reverse_tuning": true,
            "no_same_family_parameter_expansion": true,
            "model_algorithm_policy": {
                "algorithm_is_secondary_to_source_economics": true,
                "allowed_after": "candidate source passes data/PIT coverage and P3.10A-D economics gates",
                "forbidden_use": "do not use a new ML algorithm, full-period sign flip, label mining, or OOS feedback to rescue a source that failed RankIC, group return, decay, turnover/capacity, or bounded train robustness",
                "allowed_use": "after source admission, compare linear, tree/boosting and calibrated ensemble models only inside rolling train windows with net-of-cost objectives and unchanged test-window evaluation"
            },
            "required_sequence": [
                "permission_smoke",
                "schema_and_available_at_audit",
                "bounded_history_sync",
                "coverage_readiness_audit",
                "p310_rankic_group_decay_turnover_capacity",
                "bounded_wfa_only_after_diagnostics_pass"
            ],
            "promotion_rule": "only candidates passing data/PIT, RankIC, group return, decay, turnover/capacity and regime exposure gates may enter bounded WFA"
        },
        "stopped_same_family_sources": [
            "industry_prosperity_proxy",
            "market_residual_risk",
            "liquidity_regime",
            "event_surprise",
            "event_post_return_overlay",
            "moneyflow_congestion",
            "repurchase",
            "supply_float",
            "unlock_pressure",
            "block_trade_supply_demand",
            "main_business_fina_mainbz",
            "broad_analyst_revision_current_raw_bundle",
            "futures_price_chain_current_version",
            "equity_pledge_pressure_current_low_ratio_atom",
            "shareholder_structure_current_low_fanout_sleeve",
            "multi_vendor_analyst_revision_current_akshare_cninfo_revision"
        ],
        "candidates": [
            {
                "source_id": "p322_source_inventory",
                "source_family": "new_low_correlation_pit_broad_base_source_discovery",
                "economic_hypothesis": "蓝图达标需要新的信息增量，而不是继续压榨已证伪的公开低频同族源；优先寻找更接近经营兑现、订单、产能、价格链、真实预期修正或股权激励执行质量的 PIT broad-base 数据。",
                "candidate_raw_sources": [
                    "licensed_broad_base_analyst_revision_or_consensus_estimate_feed",
                    "regulatory_or_exchange_equity_incentive_employee_stock_plan_execution_feed",
                    "regulated_disclosure_or_exchange_feed_for_orders_capacity_price_chain",
                    "akshare:stock_rank_forecast_cninfo_stopped_p323f",
                    "akshare:stock_research_report_em_low_fanout_evidence",
                    "tushare:report_rc_blocked_40101"
                ],
                "ranked_candidates": [
                    {
                        "rank": 1,
                        "source_id": "licensed_broad_base_consensus_revision",
                        "status": "source_discovery_required",
                        "reason": "best aligned with true expectation-revision economics, but no licensed callable source is configured; current AkShare CNInfo rating-change expression failed P3.10 economics"
                    },
                    {
                        "rank": 2,
                        "source_id": "exchange_announcement_order_capacity_text",
                        "status": "source_discovery_required",
                        "reason": "closest to real operations/order/capacity information, but requires reliable announcement feed, parsing schema and available_at audit"
                    },
                    {
                        "rank": 3,
                        "source_id": "multi_vendor_analyst_revision",
                        "status": "stopped_after_p310_economics_failed",
                        "reason": "AkShare stock_rank_forecast_cninfo passed raw/PIT/correlation and factor/combo construction, but daily symbol cliff and 0/4 P3.10 horizons block WFA/v19; do not rescue current expression"
                    },
                    {
                        "rank": 4,
                        "source_id": "margin_detail_leverage_crowding",
                        "status": "stopped_after_p310_economics_failed",
                        "reason": "daily security-level margin financing/short data passed raw coverage/PIT but failed P3.10 economics, so do not rescue current version"
                    }
                ],
                "schema_status": "source_discovery_required",
                "client_status": "candidate_source_discovery_required",
                "sync_status": "not_started",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "native announcement/report/publication timestamp required; conservative next-session availability is allowed only when source publication time cannot be audited",
                "admission_decision": "multi_vendor_analyst_revision_stopped_after_p310_shift_to_next_low_correlation_source",
                "blocked_reason": "akshare_stock_rank_forecast_cninfo_current_expression_passed_data_pit_but_failed_daily_breadth_and_p310_economics",
                "next_step": "search_licensed_consensus_revision_or_exchange_announcement_order_capacity_source"
            },
            {
                "source_id": "margin_detail_leverage_crowding",
                "source_family": "security_level_leverage_crowding_and_short_pressure",
                "economic_hypothesis": "个股融资买入、偿还、融资余额、融券余量和融券卖出可刻画杠杆资金拥挤和去杠杆压力；当前版本已通过 raw/PIT 覆盖但 P3.10 经济性为负，不应继续救该同族表达。",
                "candidate_raw_sources": [
                    "tushare:margin_detail"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:margin_detail",
                        "status": "stopped_after_p310_economics_failed",
                        "official_doc": "https://tushare.pro/document/2?doc_id=59",
                        "official_semantics": "security_level_margin_trading_detail_updated_next_day_around_0830",
                        "observed_fields_from_doc": ["trade_date", "ts_code", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"],
                        "native_available_at_candidate": "next_session_after_exchange_publication",
                        "full_history_summary": {
                            "rows": 4604635,
                            "date_range": "2014-02-07..2026-06-23",
                            "pit_violations": 0
                        },
                        "diagnostics_summary": {
                            "latest_report_id": "exp-3812df52-2786-4a9a-b0a3-b5fc475e8ca4",
                            "mean_rankic_20_45_60_120": [-0.0332, -0.0320, -0.0288, -0.0256],
                            "passed_horizon_count": 0,
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_margin_detail_current_version_after_negative_p310_economics"
                    }
                ],
                "current_tables": ["market_stock_margin_detail"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_green",
                "correlation_status": "completed_but_economics_failed",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "official source says prior-day data is updated around next trading day 08:30; intraday decisions must use only records whose source_published_at or conservative next-session available_at is <= decision time",
                "admission_decision": "stopped_after_p310_economics_failed",
                "blocked_reason": "full_history_raw_coverage_pit_passed_but_rankic_negative_across_horizons; no_sign_flip_no_same_family_rescue_no_oos_reverse_tuning",
                "next_step": "do_not_rescue_margin_detail_current_version_shift_to_multi_vendor_analyst_revision_source_admission"
            },
            {
                "source_id": "futures_price_chain",
                "source_family": "real_operations_and_order_price_chain",
                "economic_hypothesis": "真实经营、订单、产能、价格链变化比价格成交同族特征更接近基本面边际变化，若可 PIT 化且覆盖 broad-base，可能提供低相关横截面信息。",
                "candidate_raw_sources": [
                    "tushare:fina_mainbz",
                    "tushare:fut_daily",
                    "tushare:fut_wsr",
                    "tushare:fut_holding",
                    "source_discovery_required_for_order_price_chain"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:fina_mainbz",
                        "status": "stopped_after_p310_economics_weak",
                        "official_semantics": "main_business_composition_by_product_region_or_industry",
                        "observed_fields": ["ts_code", "end_date", "bz_item", "bz_code", "bz_sales", "bz_profit", "bz_cost", "curr_type", "update_flag"],
                        "missing_pit_fields": ["ann_date", "f_ann_date", "disclosure_date"],
                        "available_at_join_candidates": [
                            "market_financial_statement.ann_date_by_ts_code_end_date",
                            "market_stock_disclosure_date.actual_date_by_ts_code_period"
                        ],
                        "audit_endpoint": "POST /api/v1/quant/data/main-business/available-at-audit",
                        "readiness_endpoint": "GET /api/v1/quant/data/main-business/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/main-business/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-f6374904-1d5d-4bfc-afaf-64f95ce24040",
                        "diagnostics_summary": {
                            "data_pit_coverage": "green",
                            "effective_start_date": "2014-08-29",
                            "raw_rows": 500223,
                            "pit_violation_rows": 0,
                            "period_universe_mismatch_rows": 0,
                            "rankic_verdict": "weak_or_negative_across_most_profiles_horizons",
                            "best_profile": "segment_concentration_inverse",
                            "best_profile_mean_rankic_range": "0.0022..0.0038",
                            "best_profile_spread_range": "-0.12%..0.17%",
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_fina_mainbz_main_business_source_after_p310_economics_failed"
                    },
                    {
                        "candidate": "tushare:futures_price_chain",
                        "status": "stopped_after_p310_component_economics_failed",
                        "smoke_source": "futures_price_chain",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "schema_contract_endpoint": "GET /api/v1/quant/data/futures-price-chain/schema-contract",
                        "readiness_endpoint": "GET /api/v1/quant/data/futures-price-chain/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                        "official_docs": [
                            "https://tushare.pro/wctapi/documents/138.md",
                            "https://tushare.pro/wctapi/documents/139.md",
                            "https://tushare.pro/wctapi/documents/140.md"
                        ],
                        "raw_endpoints": [
                            {
                                "api": "fut_daily",
                                "semantics": "daily futures OHLC/settlement/volume/open-interest",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_wsr",
                                "semantics": "warehouse receipt daily inventory changes",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_holding",
                                "semantics": "daily broker volume/long/short holding ranking",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            }
                        ],
                        "pit_policy": "trade_date may be used only after the futures market publication point; intraday stock decisions must use previous available futures trade_date unless a source_published_at audit proves earlier availability",
                        "mapping_gate": "must design product-to-industry/stock exposure mapping before factor backfill; no static hindsight mapping may revise prior samples",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "trade_date": "20181113",
                            "fut_daily_rows": 5,
                            "fut_wsr_rows": 5,
                            "fut_holding_rows": 5,
                            "status": "available"
                        },
                        "full_history_summary": {
                            "raw_rows": 28375979,
                            "mapped_products": 84,
                            "excluded_products": 10,
                            "missing_products": 0,
                            "factor_rows": 2770856,
                            "factor_start": "2014-04-03",
                            "factor_end": "2026-06-18",
                            "raw_mapping_exclusion_pit_violations": 0
                        },
                        "diagnostics_summary": {
                            "combo_report_id": "exp-e0e525c6-612d-4b12-b11e-91088dbc8150",
                            "component_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                            "coverage_pit_market_scope": "green",
                            "research_economic_admission": "blocked_by_p310_economics",
                            "component_passed_horizon_count": 0,
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_futures_price_chain_after_p310_component_economics_failed"
                    }
                ],
                "current_tables": ["market_stock_main_business", "market_futures_daily", "market_futures_warehouse_receipt", "market_futures_holding_rank", "market_futures_product_exposure_mapping_pit"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_mapping_exclusion_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "source_publication_or_disclosure_date_required_before_effective_period; futures trade_date is usable only after source publication/market close",
                "admission_decision": "stopped_after_p310_component_economics_failed",
                "blocked_reason": "data_pit_coverage_mapping_green_but_combo_and_component_p310_economics_failed; no_full_period_sign_flip_no_same_family_weight_rescue_no_oos_reverse_tuning",
                "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
            },
            {
                "source_id": "equity_pledge_pressure",
                "source_family": "shareholder_financing_pressure_and_governance_risk",
                "economic_hypothesis": "股权质押压力可能刻画控股股东融资约束、治理风险和潜在被动减持压力；若能用公告日 PIT 化并覆盖足够广的股票池，可能提供与价格成交、事件后收益、期货价格链较低相关的横截面信息。",
                "candidate_raw_sources": [
                    "tushare:pledge_stat",
                    "tushare:pledge_detail"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:pledge_stat",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                        "official_semantics": "stock_equity_pledge_stat_snapshot",
                        "observed_fields_from_doc": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "symbol_probe",
                            "sample_symbol_count": 3,
                            "status": "available"
                        },
                        "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                        "decision": "do_not_use_pledge_stat_alone_until_available_at_policy_is_joined_or_conservatively_derived"
                    },
                    {
                        "candidate": "tushare:pledge_detail",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                        "official_semantics": "stock_equity_pledge_detail_events",
                        "observed_fields_from_doc": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "announcement_date_range_probe",
                            "status": "available"
                        },
                        "native_available_at_candidate": "ann_date",
                        "decision": "schema_contract_ready_but_full_history_coverage_and_pit_audit_required_before_factor_design"
                    }
                ],
                "current_tables": ["market_stock_pledge_stat", "market_stock_pledge_detail"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "pledge_detail.ann_date is the native available_at candidate; pledge_stat.end_date is only a snapshot measurement date and must be joined to detail announcements or shifted conservatively before any feature use",
                "diagnostics_summary": {
                    "data_pit_coverage": "green_after_2015_coverage_cliff_check",
                    "latest_report_id": "exp-0dc8dfd6-c43b-4c01-a42b-99339fc5ef25",
                    "mean_rankic_20_45_60_120": [0.00353, 0.00505, 0.00580, 0.00895],
                    "high_minus_low_spread": [-0.00127, -0.00340, -0.00394, -0.00296],
                    "passed_horizon_count": 0,
                    "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                },
                "admission_decision": "stopped_after_p310_economics_failed",
                "blocked_reason": "coverage_and_pit_passed_but_low_pledge_ratio_atom_failed_rankic_group_spread_and_monotonicity; no_same_family_event_or_sign_flip_rescue",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "shareholder_structure",
                "source_family": "ownership_structure_and_governance_breadth",
                "economic_hypothesis": "股东户数和交易事件可刻画筹码扩散、低 fanout 持有人变化与治理压力，但当前 low-fanout 表达在 v19 control 上未能转化为可交易净收益。",
                "candidate_raw_sources": [
                    "tushare:stk_holdernumber",
                    "tushare:stk_holdertrade",
                    "tushare:top10_holders",
                    "tushare:top10_floatholders"
                ],
                "current_tables": [
                    "market_stock_holder_number",
                    "market_stock_holder_trade",
                    "market_stock_top10_holders",
                    "market_stock_top10_float_holders"
                ],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_low_fanout_raw_sync_completed",
                "coverage_status": "strict_low_fanout_coverage_pit_green_top10_high_fanout_not_admitted",
                "p310_status": "completed_passed_single_factor_diagnostics",
                "wfa_status": "completed_skipped_all_windows_by_train_robustness_and_cost_capacity_gate",
                "pit_required": true,
                "available_at_policy": "holder_number requires available_at >= end_date and holder_num > 0; holder_trade requires ann_date/native available_at and ratio/interval quality gates; top10 raw cannot bypass separate high-fanout coverage admission",
                "admission_decision": "stopped_after_bounded_wfa_train_robustness_failed",
                "diagnostics_summary": {
                    "p310_rankic_5_10_20d": [0.0187, 0.0285, 0.0361],
                    "p310_monotonicity": 0.75,
                    "wfa_experiment_id": "exp-39d6d820-0073-4a22-889f-e9eb67962785",
                    "wfa_windows": 13,
                    "skipped_windows": 13,
                    "stitched_oos": false,
                    "control_train_annual_return": -0.0677,
                    "sleeve_train_annual_return_5_10_15pct": [-0.0764, -0.0691, -0.0668],
                    "decision": "do_not_enter_v19_train_selection_or_expand_same_family_sleeve"
                },
                "blocked_reason": "raw_pit_and_p310_single_factor_passed_but_bounded_sleeve_wfa_failed_train_robustness_and_cost_capacity_gate",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "equity_incentive_execution_quality",
                "source_family": "equity_incentive_and_employee_stock_plan_execution",
                "economic_hypothesis": "股权激励、员工持股与执行进度可能代表治理层对未来经营兑现的约束和信号，但必须使用公告可得日与执行窗口，不能用事后完成状态回填。",
                "candidate_raw_sources": [
                    "tushare:stk_rewards_rejected_semantic_mismatch",
                    "tushare:stk_reward_rejected_invalid_endpoint",
                    "source_discovery_required"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:stk_rewards",
                        "status": "rejected_semantic_mismatch",
                        "official_semantics": "management_compensation_and_shareholding",
                        "observed_fields": ["ts_code", "ann_date", "end_date", "name", "title", "reward", "hold_vol"],
                        "missing_required_execution_fields": [
                            "plan_id",
                            "grant_date",
                            "grant_price_or_exercise_price",
                            "vesting_or_unlock_schedule",
                            "participant_scope",
                            "execution_progress",
                            "cancellation_or_adjustment_events"
                        ],
                        "decision": "do_not_build_schema_or_factor_from_stk_rewards_for_equity_incentive_execution_quality"
                    },
                    {
                        "candidate": "tushare:stk_reward",
                        "status": "rejected_invalid_endpoint",
                        "decision": "do_not_retry_without_official_endpoint_evidence"
                    }
                ],
                "current_tables": [],
                "schema_status": "blocked_until_valid_source_identified",
                "client_status": "do_not_add_stk_rewards_client_for_equity_incentive",
                "sync_status": "missing",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "announcement_or_disclosure_date_required_before_event_effective_date",
                "admission_decision": "blocked_no_valid_equity_incentive_source",
                "blocked_reason": "stk_rewards_is_management_compensation_shareholding_not_equity_incentive_execution_and_stk_reward_is_invalid",
                "next_step": "search_regulatory_disclosure_or_licensed_vendor_source_for_equity_incentive_employee_stock_plan_execution"
            },
            {
                "source_id": "broad_analyst_revision",
                "source_family": "broad_base_analyst_expectation_revision",
                "economic_hypothesis": "更 broad-base 的业绩预告/快报/披露日历修正可以刻画一致预期边际变化，但必须避免退化为稀疏公告后收益曲线 overlay。",
                "candidate_raw_sources": [
                    "tushare:forecast_stopped_sparse_revision_bundle",
                    "tushare:express_stopped_sparse_revision_bundle",
                    "tushare:disclosure_date_stopped_sparse_revision_bundle",
                    "tushare:report_rc"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:forecast/express/disclosure_date",
                        "status": "stopped_after_full_history_audit_sparse_revision_semantics",
                        "audit_endpoint": "GET /api/v1/quant/data/broad-analyst-revision/audit",
                        "available_at_policy": {
                            "forecast": "available_at equals ann_date; available_at can be before end_date because forecasts may be published before period end",
                            "express": "available_at equals ann_date",
                            "disclosure_date": "available_at equals max(ann_date, actual_date, modify_date); pre_date is not true availability"
                        },
                        "audit_summary": {
                            "union_symbols": 4074,
                            "union_symbol_coverage_ratio": 0.5650,
                            "forecast_symbols": 1682,
                            "forecast_symbol_coverage_ratio": 0.2333,
                            "forecast_symbol_periods": 27681,
                            "multi_announcement_symbol_periods": 1825,
                            "multi_announcement_symbol_period_ratio": 0.0659,
                            "revision_event_symbols": 832,
                            "revision_event_symbol_coverage_ratio": 0.1154,
                            "available_at_rule_violations": 0,
                            "decision": "do_not_enter_p310_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_current_forecast_express_disclosure_bundle_search_replacement_revision_source"
                    },
                    {
                        "candidate": "tushare:report_rc",
                        "status": "blocked_current_api_unknown_source_after_production_smoke",
                        "official_doc": "https://tushare.pro/wctapi/documents/292.md",
                        "official_semantics": "sell_side_research_report_earnings_forecast_daily_since_2010",
                        "observed_fields_from_doc": ["ts_code", "report_date", "report_title", "report_type", "classify", "org_name", "author_name", "quarter", "op_rt", "op_pr", "tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "rating", "max_price", "min_price", "imp_dg", "create_time"],
                        "native_available_at_candidate": "report_date",
                        "permission_note": "120 points can trial 10 requests/day; formal permission requires 8000 points according to official doc",
                        "smoke_source": "report_rc",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "request": {"sources": ["report_rc"], "start_date": "20260401", "end_date": "20260621", "limit": 5},
                            "status": "error",
                            "error_code": "40101",
                            "error": "未知的数据源"
                        },
                        "decision": "do_not_build_schema_sync_factor_or_p310_from_report_rc_until_the_callable_api_name_or_permission_path_is_verified"
                    }
                ],
                "current_tables": [
                    "market_stock_forecast",
                    "market_stock_express",
                    "market_stock_disclosure_date"
                ],
                "schema_status": "current_event_bundle_present_report_rc_blocked_current_api_unknown_source",
                "client_status": "report_rc_read_only_smoke_available_but_current_api_returns_unknown_source",
                "sync_status": "current_event_bundle_bounded_sync_available_report_rc_blocked_not_synced",
                "coverage_status": "current_event_bundle_full_history_audit_completed_report_rc_blocked_current_api_unknown_source",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "ann_date_or_latest_required_disclosure_date_as_available_at",
                "admission_decision": "blocked_report_rc_current_api_unknown_source_after_permission_smoke",
                "blocked_reason": "current_forecast_express_disclosure_bundle_stopped_and_report_rc_production_smoke_returned_tushare_40101_unknown_data_source",
                "guardrail": "must_be_broad_base_revision_not_sparse_event_post_return_overlay",
                "next_step": "search_licensed_or_alternative_broad_pit_expectation_revision_source; do_not_shift_back_to_stopped_futures_price_chain_proxy"
            }
        ]
    });
    if let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(|candidates| candidates.as_array_mut())
    {
        candidates.push(phase7_exchange_announcement_order_capacity_candidate());
        candidates.push(phase7_multi_vendor_analyst_revision_candidate());
    }
    if let Some(readiness) = futures_price_chain_readiness {
        apply_p319_futures_price_chain_readiness(&mut admission, readiness);
    }
    admission
}



fn apply_p320_equity_pledge_readiness(admission: &mut Value, readiness: &Value) {
    let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let decision = readiness
        .get("decision")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let sync_status = decision
        .get("sync_status")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let schema_status = decision
        .get("schema_status")
        .and_then(Value::as_str)
        .unwrap_or("schema_contract_ready_review_required");
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_coverage_readiness_passes");

    for source in candidates.iter_mut() {
        let Some("equity_pledge_pressure") =
            source.get("source_id").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            let source_is_stopped_by_research = object
                .get("admission_decision")
                .and_then(Value::as_str)
                .map(|decision| decision.starts_with("stopped_"))
                .unwrap_or(false)
                || object
                    .get("p310_status")
                    .and_then(Value::as_str)
                    .map(|status| status == "completed_failed_economics")
                    .unwrap_or(false);

            object.insert("equity_pledge_readiness".to_string(), readiness.clone());
            if source_is_stopped_by_research {
                object.insert("latest_raw_readiness".to_string(), readiness.clone());
                continue;
            }

            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("schema_status".to_string(), json!(schema_status));
            object.insert("sync_status".to_string(), json!(sync_status));
            object.insert(
                "coverage_status".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "raw_pit_ready_coverage_audit_not_started",
                    "raw_pit_failed" => "blocked_by_raw_pit_violations",
                    "bounded_sync_required_before_coverage_audit" => "not_started",
                    "apply_schema_before_sync" => "blocked_until_schema_applied",
                    _ => "not_started",
                }),
            );
            object.insert("p310_status".to_string(), json!(p310_status));
            object.insert("admission_decision".to_string(), json!(admission_decision));
            object.insert(
                "blocked_reason".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "equity_pledge_raw_pit_passed_but_year_symbol_ann_date_coverage_duplicate_and_sync_attempt_audit_not_done",
                    "raw_pit_failed" =>
                        "equity_pledge_raw_rows_have_available_at_future_leak_or_schema_pit_violation",
                    "bounded_sync_required_before_coverage_audit" =>
                        "equity_pledge_schema_exists_but_no_raw_rows_synced",
                    "apply_schema_before_sync" =>
                        "equity_pledge_schema_contract_ready_but_database_schema_missing_or_invalid",
                    _ =>
                        "equity_pledge_candidate_waiting_for_schema_available_at_bounded_sync_and_p310",
                }),
            );
            object.insert("next_step".to_string(), json!(next_step));
        }
    }
}



fn phase7_new_alpha_candidate_sources_with_market_status(
    market_stats: &BTreeMap<String, Phase7MarketLevelSourceAudit>,
    market_sync_tasks: &BTreeMap<String, Phase7MarketLevelSyncAudit>,
    today: NaiveDate,
) -> Vec<Value> {
    let mut sources = phase7_new_alpha_candidate_sources();
    for source in sources.iter_mut() {
        let Some(source_name) = source.get("source").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some(stats) = market_stats.get(source_name) else {
            continue;
        };
        let readiness = phase7_market_level_source_readiness(stats);
        let sync_start = phase7_p315_sync_start_date(stats, today);
        let sync_dataset = phase7_market_level_sync_dataset(source_name);
        let last_sync = market_sync_tasks.get(source_name);
        let zero_row_sync_covers_gap =
            phase7_market_level_zero_row_sync_covers_gap(last_sync, sync_start, today);
        let effective_readiness =
            if readiness == "market_level_stale_needs_sync" && zero_row_sync_covers_gap {
                "market_level_upstream_zero_rows_unavailable"
            } else {
                readiness
            };
        let freshness_gate = if effective_readiness == "market_level_ready_for_regime_feature" {
            "passed"
        } else {
            "failed"
        };

        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(effective_readiness));
            object.insert(
                "market_data_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "open_day_lag": stats.open_day_lag,
                    "freshness_max_open_day_lag": 2,
                    "freshness_gate": freshness_gate,
                }),
            );
            if let Some(last_sync) = last_sync {
                object.insert(
                    "last_sync_task".to_string(),
                    json!({
                        "task_id": last_sync.task_id,
                        "task_type": last_sync.task_type,
                        "start_date": phase7_date_json(last_sync.start_date),
                        "end_date": phase7_date_json(last_sync.end_date),
                        "status": last_sync.status,
                        "total_count": last_sync.total_count,
                        "success_count": last_sync.success_count,
                        "failed_count": last_sync.failed_count,
                        "error_message": last_sync.error_message,
                        "completed_at": phase7_datetime_json(last_sync.completed_at),
                    }),
                );
            }
            if effective_readiness == "market_level_upstream_zero_rows_unavailable" {
                object.insert(
                    "sync_remediation".to_string(),
                    json!({
                        "status": "not_retriable_until_upstream_resolved",
                        "reason": "latest stale-gap sync completed successfully but returned zero rows; treat this source as live-unavailable until upstream endpoint, permission, fields, or replacement source is fixed",
                    }),
                );
            }
            if let Some(dataset) = sync_dataset {
                object.insert(
                    "sync_task_payload".to_string(),
                    json!({
                        "dataset": dataset,
                        "source": "tushare",
                        "start_date": sync_start.format("%Y%m%d").to_string(),
                        "end_date": today.format("%Y%m%d").to_string(),
                        "background": true,
                        "reason": "p315_market_level_regime_source_freshness"
                    }),
                );
            }
        }
    }
    sources
}



fn phase7_new_alpha_candidate_sources_with_block_trade_status(
    mut sources: Vec<Value>,
    stats: &Phase7BlockTradeSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("block_trade_supply_demand") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_block_trade_readiness(stats);
        if let Value::Object(object) = source {
            let final_readiness = if readiness == "raw_source_pit_failed" {
                readiness
            } else {
                "stopped_after_p310_economics_weak"
            };
            object.insert("readiness".to_string(), json!(final_readiness));
            object.insert("raw_source_readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "covered_trade_days": stats.covered_trade_days,
                    "open_days_in_range": stats.open_days_in_range,
                    "open_day_coverage_ratio": phase7_ratio(stats.covered_trade_days, stats.open_days_in_range),
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match final_readiness {
                    "stopped_after_p310_economics_weak" => {
                        "do_not_expand_same_family_shift_to_p320_new_source_admission"
                    }
                    "raw_source_pit_failed" => "repair_available_at_before_any_diagnostics",
                    _ => "do_not_expand_same_family_shift_to_p320_new_source_admission",
                }),
            );
        }
    }
    sources
}



fn phase7_new_alpha_candidate_sources_with_equity_pledge_status(
    mut sources: Vec<Value>,
    readiness: Option<&Value>,
) -> Vec<Value> {
    let Some(readiness) = readiness else {
        return sources;
    };
    let decision = readiness
        .get("decision")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let readiness_label = equity_pledge_readiness_label(admission_decision);
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");

    for source in sources.iter_mut() {
        let Some("equity_pledge_pressure") = source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness_label));
            object.insert("next_step".to_string(), json!(next_step));
            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("raw_source_status".to_string(), readiness.clone());
            object.insert(
                "why_not_trainable_now".to_string(),
                json!("股权质押源仍停在 schema/raw/coverage 准入层；只有全历史 coverage/readiness/PIT 与 P3.10A-D 通过后才允许进入 WFA/v19"),
            );
        }
    }
    sources
}



fn phase7_new_alpha_candidate_sources_with_industry_membership_status(
    mut sources: Vec<Value>,
    stats: &Phase7IndustryMembershipSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("industry_prosperity_proxy") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_industry_membership_readiness(stats);
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "table": "market_stock_industry_membership_pit",
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "index_codes": stats.index_codes,
                    "current_active_stock_symbols": stats.current_active_stock_symbols,
                    "current_covered_stock_symbols": stats.current_covered_stock_symbols,
                    "current_missing_stock_symbols": stats.current_active_stock_symbols.saturating_sub(stats.current_covered_stock_symbols),
                    "current_stock_coverage_ratio": phase7_ratio(stats.current_covered_stock_symbols, stats.current_active_stock_symbols),
                    "min_in_date": phase7_date_json(stats.min_in_date),
                    "latest_in_date": phase7_date_json(stats.latest_in_date),
                    "min_out_date": phase7_date_json(stats.min_out_date),
                    "latest_out_date": phase7_date_json(stats.latest_out_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                    "invalid_interval_rows": stats.invalid_interval_rows,
                    "duplicate_key_rows": stats.duplicate_key_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match readiness {
                    "industry_membership_raw_source_ready_for_coverage_audit" => {
                        "run_full_history_coverage_and_pit_membership_snapshot_audit"
                    }
                    "industry_membership_raw_source_pit_failed" => {
                        "repair_industry_membership_intervals_before_factor_design"
                    }
                    "industry_membership_current_coverage_undercovered" => {
                        "run_full_l1_membership_sync_or_repair_missing_symbols"
                    }
                    _ => "run_bounded_industry_membership_sync",
                }),
            );
        }
    }
    sources
}



fn phase7_industry_membership_snapshot_readiness(
    expected_symbol_days: i64,
    covered_symbol_days: i64,
    _missing_symbol_days: i64,
    multi_membership_symbol_days: i64,
    pit_violation_rows: i64,
    invalid_interval_rows: i64,
    duplicate_key_rows: i64,
    missing_exit_available_at_rows: i64,
) -> &'static str {
    if expected_symbol_days <= 0 {
        return "snapshot_no_universe_symbol_days";
    }
    if pit_violation_rows > 0
        || invalid_interval_rows > 0
        || duplicate_key_rows > 0
        || missing_exit_available_at_rows > 0
    {
        return "snapshot_raw_source_pit_failed";
    }
    if multi_membership_symbol_days > 0 {
        return "snapshot_multi_membership_blocked";
    }
    if phase7_ratio(covered_symbol_days, expected_symbol_days).unwrap_or(0.0) < 0.995 {
        return "snapshot_coverage_gaps_need_review";
    }
    "snapshot_ready_for_p310_diagnostics"
}



fn phase7_industry_membership_audit_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(20).clamp(1, 100)
}



fn phase7_industry_membership_market_scope_eligible(
    coverage_ratio: Option<f64>,
    multi_membership_symbol_days: i64,
) -> bool {
    multi_membership_symbol_days == 0 && coverage_ratio.unwrap_or(0.0) >= 0.995
}



fn phase7_industry_membership_snapshot_summary_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date, stock.symbol
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    ),
    raw_stats AS (
        SELECT COUNT(*) FILTER (WHERE available_at < in_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND out_date < in_date)::bigint
                   AS invalid_interval_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND exit_available_at IS NULL)::bigint
                   AS missing_exit_available_at_rows
        FROM market_stock_industry_membership_pit
        WHERE classification_source IN ('SW2014', 'SW2021')
          AND industry_level = 'L1'
    ),
    duplicate_keys AS (
        SELECT COALESCE(SUM(row_count - 1), 0)::bigint AS duplicate_key_rows
        FROM (
            SELECT classification_source, index_code, symbol, in_date, COUNT(*)::bigint AS row_count
            FROM market_stock_industry_membership_pit
            WHERE classification_source IN ('SW2014', 'SW2021')
              AND industry_level = 'L1'
            GROUP BY classification_source, index_code, symbol, in_date
            HAVING COUNT(*) > 1
        ) duplicate_groups
    )
    SELECT COUNT(DISTINCT joined.trade_date)::bigint AS trade_days,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           MIN(joined.trade_date) AS min_trade_date,
           MAX(joined.trade_date) AS max_trade_date,
           raw_stats.pit_violation_rows,
           raw_stats.invalid_interval_rows,
           duplicate_keys.duplicate_key_rows,
           raw_stats.missing_exit_available_at_rows
    FROM joined, raw_stats, duplicate_keys
    GROUP BY raw_stats.pit_violation_rows,
             raw_stats.invalid_interval_rows,
             duplicate_keys.duplicate_key_rows,
             raw_stats.missing_exit_available_at_rows
    "#
}



fn phase7_industry_membership_year_breakdown_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date, stock.symbol
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    )
    SELECT DATE_TRUNC('year', joined.trade_date)::date AS period_start,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY DATE_TRUNC('year', joined.trade_date)::date
    ORDER BY period_start
    "#
}



fn phase7_industry_membership_market_breakdown_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date,
               stock.symbol,
               COALESCE(stock.market, '') AS market
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               universe.market,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    )
    SELECT joined.market,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY joined.market
    ORDER BY missing_symbol_days DESC, joined.market
    "#
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



pub(crate) async fn register_sync_task(
    state: &AppState,
    task_id: &str,
    req: &DataSyncTaskReq,
    status: &str,
) -> Result<(), String> {
    let start_date = parse_optional_date(req.start_date.as_deref())?;
    let end_date = parse_optional_date(req.end_date.as_deref())?;
    let symbols = match req.dataset.as_str() {
        "index_daily" if !req.index_codes.is_empty() => req.index_codes.as_slice(),
        "industry_membership" | "market_stock_industry_membership_pit"
            if !req.index_codes.is_empty() =>
        {
            req.index_codes.as_slice()
        }
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



pub(crate) fn require_range(req: &DataSyncTaskReq) -> Result<(&str, &str), String> {
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



pub(crate) fn optional_source_all_symbols_allowed(mode: Option<&str>) -> bool {
    mode == Some("full_market")
}



pub async fn structured_order_capacity_price_chain_source_contract() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_source_contract()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/vendor-admission-plan


pub async fn structured_order_capacity_price_chain_vendor_admission_plan() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_vendor_admission_plan()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/source-evidence-inventory


pub async fn structured_order_capacity_price_chain_source_evidence_inventory() -> impl IntoResponse
{
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_source_evidence_inventory()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-access-smoke-contract


pub(crate) fn akshare_analyst_revision_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
    batches: Vec<AkshareAnalystRevisionSyncPlanBatch>,
) -> Value {
    const DEFAULT_ESTIMATED_ROWS_PER_CALENDAR_DAY: f64 = 350.0;
    let batch_values = batches
        .iter()
        .map(|batch| {
            let estimated_rows =
                (batch.calendar_day_count as f64 * DEFAULT_ESTIMATED_ROWS_PER_CALENDAR_DAY).round()
                    as i64;
            json!({
                "batch": batch.label,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "calendar_day_count": batch.calendar_day_count,
                "estimated_api_calls": batch.calendar_day_count,
                "estimated_rows": estimated_rows,
                "estimated_rows_basis": "conservative_350_rows_per_calendar_day_from_p3_23b_history_replay_samples",
                "future_bounded_sync_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("akshare-analyst-revision-{}", batch.label),
                    "background": false
                }
            })
        })
        .collect::<Vec<_>>();

    let calendar_day_count = (end - start).num_days() + 1;
    let estimated_total_rows = batch_values
        .iter()
        .filter_map(|batch| batch.get("estimated_rows").and_then(Value::as_i64))
        .sum::<i64>();

    json!({
        "audit_version": "p3.23c-akshare-analyst-revision-sync-plan-v1",
        "source_id": "multi_vendor_analyst_revision",
        "vendor": "akshare",
        "vendor_endpoint": "stock_rank_forecast_cninfo",
        "mode": "read_only_bounded_calendar_day_sync_plan",
        "date_range": {
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
            "calendar_day_count": calendar_day_count,
        },
        "request_key_policy": "calendar publication date; do not restrict to market open days or weekend/holiday analyst reports may be missed",
        "batch_mode": batch_mode,
        "recommended_batch_granularity": AKSHARE_ANALYST_REVISION_SYNC_PLAN_DEFAULT_BATCH,
        "batch_count": batch_values.len(),
        "estimated_api_calls": calendar_day_count,
        "estimated_total_rows": estimated_total_rows,
        "safe_to_run_full_range": calendar_day_count <= 100,
        "sync_endpoint_status": "enabled_for_p3_23d_bounded_calendar_day_raw_sync_max_100_days_background_false",
        "batches": batch_values,
        "promotion_gate": {
            "bounded_sync": "enabled_for_small_batches_after_schema_apply",
            "coverage_audit": "blocked_until_raw_sync_completes",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "notes": [
            "This plan does not create data versions, data_sync_task rows, raw tables, or factors. Use the sync endpoint with background=false for each <=100-day batch.",
            "The bounded sync iterates calendar publication dates and treats empty dates as auditable source outcomes, not silent success.",
            "Without audited intraday publication timestamp, downstream intraday trading must use conservative next-session available_at."
        ]
    })
}



pub(crate) async fn build_phase7_optional_source_coverage_sync(
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
            source_filters: Vec::new(),
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
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
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
                },
            )
            .await;
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



pub(crate) async fn build_phase7_optional_source_coverage_batches(
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



pub(crate) async fn build_phase7_share_float_coverage_batches(
    state: Arc<AppState>,
    req: Phase7ShareFloatCoverageReq,
) -> Result<Value, String> {
    let today = chrono::Utc::now().date_naive();
    let start_date = req
        .start_date
        .clone()
        .unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .clone()
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let granularity = phase7_share_float_chunk_granularity(req.chunk_granularity.as_deref());
    let max_chunks = phase7_share_float_coverage_max_chunks(req.max_chunks);
    let plan_only = phase7_share_float_coverage_plan_only(req.plan_only);
    let chunks = phase7_share_float_date_chunks(start, end, granularity, max_chunks);
    let truncated = chunks
        .last()
        .map(|(_, chunk_end)| *chunk_end < end)
        .unwrap_or(false);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-p7-share-float-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut batch_results = Vec::new();
    for (index, (chunk_start, chunk_end)) in chunks.iter().copied().enumerate() {
        let batch_label = format!("f{:03}", index + 1);
        let task_id = bounded_phase7_task_id(&[data_version_prefix.as_str(), batch_label.as_str()]);
        let chunk_start_s = chunk_start.format("%Y%m%d").to_string();
        let chunk_end_s = chunk_end.format("%Y%m%d").to_string();
        let sync_req = DataSyncTaskReq {
            dataset: "share_float".to_string(),
            source: "tushare".to_string(),
            mode: Some("full_market".to_string()),
            symbols: Vec::new(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(chunk_start_s.clone()),
            end_date: Some(chunk_end_s.clone()),
            data_version_id: Some(task_id.clone()),
            background: req.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 share_float float_date coverage expansion".to_string()),
        };

        if plan_only {
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "planned",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                }
            }));
        } else if req.background {
            register_sync_task(&state, &task_id, &sync_req, "running").await?;
            let state_for_task = state.clone();
            let task_id_for_task = task_id.clone();
            let req_for_task = sync_req.clone();
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
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
                        tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 share_float float_date补数失败");
                    }
                },
            )
            .await;
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "running",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                }
            }));
        } else {
            let execution = execute_sync_task(state.clone(), task_id.clone(), sync_req).await?;
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "completed",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                },
                "execution": execution,
            }));
        }
    }

    Ok(json!({
        "audit_version": "phase7-share-float-float-date-coverage-v1",
        "mode": if plan_only {
            "plan_only"
        } else if req.background {
            "background"
        } else {
            "synchronous"
        },
        "plan_only": plan_only,
        "background": req.background,
        "dataset": "share_float",
        "query_basis": "float_date",
        "pit_available_at": "ann_date",
        "chunk_granularity": granularity,
        "max_chunks": max_chunks,
        "chunk_count": chunks.len(),
        "truncated_by_max_chunks": truncated,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "data_version_prefix": data_version_prefix,
        "batches": batch_results,
        "notes": [
            "share_float must be expanded by unlock float_date windows because the Tushare interface does not support reliable symbol-filtered full-history expansion.",
            "ann_date is persisted as available_at; downstream PIT features must require available_at <= trade_date.",
            "plan_only defaults to true. Set plan_only=false only for controlled historical repair runs.",
            "Run phase7-share-float-readiness-audit after completion before building unlock pressure factors."
        ],
    }))
}



pub(crate) async fn build_phase7_share_float_readiness_audit(
    state: &AppState,
    req: Phase7ShareFloatReadinessAuditReq,
) -> Result<Value, String> {
    let today = chrono::Utc::now().date_naive();
    let start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let row = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
        ),
    >(phase7_share_float_readiness_sql())
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to audit share_float readiness: {}", error))?;

    let (
        row_count,
        symbol_count,
        min_float_date,
        max_float_date,
        min_available_at,
        max_available_at,
        late_or_invalid_count,
        distinct_float_dates,
    ) = row;

    let completed_windows = sqlx::query_as::<_, (NaiveDate, NaiveDate)>(
        r#"
        SELECT start_date, end_date
        FROM data_sync_task
        WHERE task_type = 'share_float'
          AND status = 'completed'
          AND start_date IS NOT NULL
          AND end_date IS NOT NULL
          AND start_date <= $2
          AND end_date >= $1
        ORDER BY start_date, end_date
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to audit share_float sync windows: {}", error))?;
    let expected_days = phase7_share_float_expected_days(start, end);
    let covered_days =
        phase7_share_float_covered_days_from_windows(completed_windows.clone(), start, end);
    let sync_coverage_ratio = if expected_days > 0 {
        Some(covered_days as f64 / expected_days as f64)
    } else {
        None
    };
    let coverage_grade = if covered_days >= expected_days {
        phase7_coverage_grade(symbol_count, 1.max(symbol_count))
    } else {
        "incomplete_range"
    };
    let readiness = phase7_share_float_feature_readiness(
        row_count,
        covered_days,
        expected_days,
        late_or_invalid_count,
    );
    let completed_windows_json: Vec<Value> = completed_windows
        .into_iter()
        .map(|(window_start, window_end)| {
            json!({
                "start_date": window_start,
                "end_date": window_end,
            })
        })
        .collect();

    Ok(json!({
        "audit_version": "phase7-share-float-readiness-v1",
        "dataset": "share_float",
        "query_basis": "float_date",
        "pit_available_at": "ann_date",
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "row_count": row_count,
        "symbol_count": symbol_count,
        "distinct_float_dates": distinct_float_dates,
        "expected_days": expected_days,
        "covered_days": covered_days,
        "sync_coverage_ratio": sync_coverage_ratio,
        "completed_sync_windows": completed_windows_json,
        "min_float_date": min_float_date,
        "max_float_date": max_float_date,
        "min_available_at": min_available_at,
        "max_available_at": max_available_at,
        "late_or_invalid_count": late_or_invalid_count,
        "coverage_grade": coverage_grade,
        "feature_readiness": readiness,
        "pit_contract": {
            "source_event_date": "float_date",
            "source_available_at": "ann_date",
            "feature_filter": "available_at <= trade_date AND float_date >= trade_date"
        },
        "late_announcement_policy": {
            "late_or_invalid_count": late_or_invalid_count,
            "feature_handling": "exclude_from_pre_unlock_pressure",
            "rationale": "late source announcements are not PIT-available before unlock and must not be backdated"
        },
        "notes": [
            "Rows with available_at after float_date are retained as raw source records but excluded from pre-unlock pressure by the PIT feature filter.",
            "This audit is source readiness only; RankIC/group return/turnover/capacity still require alpha-source diagnostics after factor backfill."
        ]
    }))
}



pub(crate) async fn build_phase7_industry_membership_coverage_audit(
    state: &AppState,
    req: Phase7IndustryMembershipCoverageAuditReq,
) -> Result<Value, String> {
    let latest_open_date: Option<NaiveDate> = sqlx::query_scalar(
        r#"
        SELECT MAX(trade_date)
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open
          AND trade_date <= CURRENT_DATE
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to resolve latest open trade date: {}", error))?;
    let default_end = latest_open_date.unwrap_or_else(|| chrono::Utc::now().date_naive());
    let requested_start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let requested_end_date = req
        .end_date
        .unwrap_or_else(|| default_end.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(requested_start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let requested_end = parse_optional_date(Some(requested_end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    let end = requested_end.min(default_end);
    if start > end {
        return Err("start_date must be <= effective end_date".to_string());
    }
    let limit = phase7_industry_membership_audit_limit(req.limit);

    let summary = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
            i64,
            i64,
        ),
    >(phase7_industry_membership_snapshot_summary_sql())
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to audit industry membership snapshot coverage: {}",
            error
        )
    })?;
    let (
        trade_days,
        expected_symbol_days,
        covered_symbol_days,
        missing_symbol_days,
        multi_membership_symbol_days,
        min_trade_date,
        max_trade_date,
        pit_violation_rows,
        invalid_interval_rows,
        duplicate_key_rows,
        missing_exit_available_at_rows,
    ) = summary;
    let coverage_ratio = phase7_ratio(covered_symbol_days, expected_symbol_days);
    let readiness = phase7_industry_membership_snapshot_readiness(
        expected_symbol_days,
        covered_symbol_days,
        missing_symbol_days,
        multi_membership_symbol_days,
        pit_violation_rows,
        invalid_interval_rows,
        duplicate_key_rows,
        missing_exit_available_at_rows,
    );

    let missing_symbol_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            bool,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
        ),
    >(
        r#"
        WITH params AS (
            SELECT $1::date AS start_date, $2::date AS end_date
        ),
        days AS (
            SELECT cal.trade_date
            FROM market_trade_calendar cal
            JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
            WHERE cal.exchange = 'SSE'
              AND cal.is_open
        ),
        universe AS (
            SELECT days.trade_date, stock.symbol
            FROM days
            JOIN market_stock stock
              ON COALESCE(stock.market, '') <> ''
             AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
             AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
        ),
        membership_counts AS (
            SELECT days.trade_date,
                   membership.symbol,
                   COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
            FROM days
            JOIN market_stock_industry_membership_pit membership
              ON membership.classification_source = CASE
                     WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                     ELSE 'SW2021'
                 END
             AND membership.industry_level = 'L1'
             AND membership.available_at <= days.trade_date
             AND membership.in_date <= days.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > days.trade_date
             )
            GROUP BY days.trade_date, membership.symbol
        ),
        joined AS (
            SELECT universe.trade_date,
                   universe.symbol,
                   COALESCE(membership_counts.active_index_count, 0) AS active_index_count
            FROM universe
            LEFT JOIN membership_counts
              ON membership_counts.trade_date = universe.trade_date
             AND membership_counts.symbol = universe.symbol
        )
        SELECT joined.symbol,
               COALESCE(stock.name, '') AS name,
               COALESCE(stock.list_status, '') AS list_status,
               COALESCE(stock.market, '') AS market,
               COALESCE(stock.is_st, false) AS is_st,
               MIN(joined.trade_date) AS first_missing_date,
               MAX(joined.trade_date) AS latest_missing_date,
               COUNT(*)::bigint AS missing_days
        FROM joined
        LEFT JOIN market_stock stock ON stock.symbol = joined.symbol
        WHERE joined.active_index_count = 0
        GROUP BY joined.symbol, stock.name, stock.list_status, stock.market, stock.is_st
        ORDER BY missing_days DESC, joined.symbol
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample missing industry memberships: {}", error))?;
    let missing_symbols: Vec<Value> = missing_symbol_rows
        .into_iter()
        .map(
            |(
                symbol,
                name,
                list_status,
                market,
                is_st,
                first_missing_date,
                latest_missing_date,
                missing_days,
            )| {
                json!({
                    "symbol": symbol,
                    "name": name,
                    "list_status": list_status,
                    "market": market,
                    "is_st": is_st,
                    "first_missing_date": first_missing_date,
                    "latest_missing_date": latest_missing_date,
                    "missing_days": missing_days,
                })
            },
        )
        .collect();

    let multi_membership_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
            Option<String>,
        ),
    >(
        r#"
        WITH params AS (
            SELECT $1::date AS start_date, $2::date AS end_date
        ),
        days AS (
            SELECT cal.trade_date
            FROM market_trade_calendar cal
            JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
            WHERE cal.exchange = 'SSE'
              AND cal.is_open
        ),
        membership_counts AS (
            SELECT days.trade_date,
                   membership.symbol,
                   COUNT(DISTINCT membership.index_code)::bigint AS active_index_count,
                   STRING_AGG(DISTINCT membership.index_code, ',' ORDER BY membership.index_code)
                       AS index_codes
            FROM days
            JOIN market_stock_industry_membership_pit membership
              ON membership.classification_source = CASE
                     WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                     ELSE 'SW2021'
                 END
             AND membership.industry_level = 'L1'
             AND membership.available_at <= days.trade_date
             AND membership.in_date <= days.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > days.trade_date
             )
            GROUP BY days.trade_date, membership.symbol
            HAVING COUNT(DISTINCT membership.index_code) > 1
        )
        SELECT membership_counts.symbol,
               COALESCE(stock.name, '') AS name,
               MIN(membership_counts.trade_date) AS first_multi_date,
               MAX(membership_counts.trade_date) AS latest_multi_date,
               COUNT(*)::bigint AS multi_days,
               MAX(membership_counts.active_index_count)::bigint AS max_active_index_count,
               MIN(membership_counts.index_codes) AS sample_index_codes
        FROM membership_counts
        LEFT JOIN market_stock stock ON stock.symbol = membership_counts.symbol
        GROUP BY membership_counts.symbol, stock.name
        ORDER BY multi_days DESC, membership_counts.symbol
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample multi-membership symbols: {}", error))?;
    let multi_membership_symbols: Vec<Value> = multi_membership_rows
        .into_iter()
        .map(
            |(
                symbol,
                name,
                first_multi_date,
                latest_multi_date,
                multi_days,
                max_active_index_count,
                sample_index_codes,
            )| {
                json!({
                    "symbol": symbol,
                    "name": name,
                    "first_multi_date": first_multi_date,
                    "latest_multi_date": latest_multi_date,
                    "multi_days": multi_days,
                    "max_active_index_count": max_active_index_count,
                    "sample_index_codes": sample_index_codes,
                })
            },
        )
        .collect();

    let worst_day_rows = sqlx::query_as::<_, (NaiveDate, i64, i64, i64, Option<f64>)>(
        r#"
        WITH params AS (
            SELECT $1::date AS start_date, $2::date AS end_date
        ),
        days AS (
            SELECT cal.trade_date
            FROM market_trade_calendar cal
            JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
            WHERE cal.exchange = 'SSE'
              AND cal.is_open
        ),
        universe AS (
            SELECT days.trade_date, stock.symbol
            FROM days
            JOIN market_stock stock
              ON COALESCE(stock.market, '') <> ''
             AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
             AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
        ),
        membership_counts AS (
            SELECT days.trade_date,
                   membership.symbol,
                   COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
            FROM days
            JOIN market_stock_industry_membership_pit membership
              ON membership.classification_source = CASE
                     WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                     ELSE 'SW2021'
                 END
             AND membership.industry_level = 'L1'
             AND membership.available_at <= days.trade_date
             AND membership.in_date <= days.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > days.trade_date
             )
            GROUP BY days.trade_date, membership.symbol
        ),
        joined AS (
            SELECT universe.trade_date,
                   universe.symbol,
                   COALESCE(membership_counts.active_index_count, 0) AS active_index_count
            FROM universe
            LEFT JOIN membership_counts
              ON membership_counts.trade_date = universe.trade_date
             AND membership_counts.symbol = universe.symbol
        )
        SELECT joined.trade_date,
               COUNT(*)::bigint AS expected_symbols,
               COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbols,
               COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint AS multi_membership_symbols,
               (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
                   / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
        FROM joined
        GROUP BY joined.trade_date
        ORDER BY (COUNT(*) FILTER (WHERE joined.active_index_count = 0)
                  + COUNT(*) FILTER (WHERE joined.active_index_count > 1)) DESC,
                 joined.trade_date
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample worst industry membership days: {}", error))?;
    let worst_days: Vec<Value> = worst_day_rows
        .into_iter()
        .map(
            |(
                trade_date,
                expected_symbols,
                missing_symbols,
                multi_membership_symbols,
                daily_coverage_ratio,
            )| {
                json!({
                    "trade_date": trade_date,
                    "expected_symbols": expected_symbols,
                    "missing_symbols": missing_symbols,
                    "multi_membership_symbols": multi_membership_symbols,
                    "coverage_ratio": daily_coverage_ratio,
                })
            },
        )
        .collect();

    let year_breakdown_rows = sqlx::query_as::<_, (NaiveDate, i64, i64, i64, i64, Option<f64>)>(
        phase7_industry_membership_year_breakdown_sql(),
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to build industry membership year breakdown: {}",
            error
        )
    })?;
    let by_year: Vec<Value> = year_breakdown_rows
        .into_iter()
        .map(
            |(
                period_start,
                expected_symbol_days,
                covered_symbol_days,
                missing_symbol_days,
                multi_membership_symbol_days,
                period_coverage_ratio,
            )| {
                json!({
                    "year": period_start.year(),
                    "period_start": period_start,
                    "expected_symbol_days": expected_symbol_days,
                    "covered_symbol_days": covered_symbol_days,
                    "missing_symbol_days": missing_symbol_days,
                    "multi_membership_symbol_days": multi_membership_symbol_days,
                    "coverage_ratio": period_coverage_ratio,
                })
            },
        )
        .collect();

    let market_breakdown_rows = sqlx::query_as::<_, (String, i64, i64, i64, i64, Option<f64>)>(
        phase7_industry_membership_market_breakdown_sql(),
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to build industry membership market breakdown: {}",
            error
        )
    })?;
    let mut eligible_markets = Vec::new();
    let mut excluded_markets = Vec::new();
    let by_market: Vec<Value> = market_breakdown_rows
        .into_iter()
        .map(
            |(
                market,
                expected_symbol_days,
                covered_symbol_days,
                missing_symbol_days,
                multi_membership_symbol_days,
                market_coverage_ratio,
            )| {
                if phase7_industry_membership_market_scope_eligible(
                    market_coverage_ratio,
                    multi_membership_symbol_days,
                ) {
                    eligible_markets.push(market.clone());
                } else {
                    excluded_markets.push(market.clone());
                }
                json!({
                    "market": market,
                    "expected_symbol_days": expected_symbol_days,
                    "covered_symbol_days": covered_symbol_days,
                    "missing_symbol_days": missing_symbol_days,
                    "multi_membership_symbol_days": multi_membership_symbol_days,
                    "coverage_ratio": market_coverage_ratio,
                })
            },
        )
        .collect();
    let alpha_admission_gate = industry_prosperity_alpha_admission_policy(
        eligible_markets.clone(),
        excluded_markets.clone(),
    );

    Ok(json!({
        "audit_version": "phase7-industry-membership-coverage-v1",
        "dataset": "market_stock_industry_membership_pit",
        "classification_source": "SW2014_until_2021_12_12_then_SW2021",
        "source_version_gate": {
            "SW2014": "trade_date < 2021-12-13",
            "SW2021": "trade_date >= 2021-12-13"
        },
        "industry_level": "L1",
        "date_range": {
            "requested_start_date": requested_start_date,
            "requested_end_date": requested_end_date,
            "start_date": start,
            "end_date": end,
            "latest_open_trade_date": latest_open_date,
            "capped_by_latest_open_trade_date": requested_end > end,
        },
        "trade_days": trade_days,
        "expected_symbol_days": expected_symbol_days,
        "covered_symbol_days": covered_symbol_days,
        "missing_symbol_days": missing_symbol_days,
        "multi_membership_symbol_days": multi_membership_symbol_days,
        "coverage_ratio": coverage_ratio,
        "min_trade_date": min_trade_date,
        "max_trade_date": max_trade_date,
        "raw_source_checks": {
            "pit_violation_rows": pit_violation_rows,
            "invalid_interval_rows": invalid_interval_rows,
            "duplicate_key_rows": duplicate_key_rows,
            "missing_exit_available_at_rows": missing_exit_available_at_rows,
        },
        "breakdown": {
            "by_year": by_year,
            "by_market": by_market,
        },
        "market_scope_gate_candidate": {
            "coverage_threshold": INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD,
            "eligible_markets": eligible_markets,
            "excluded_markets": excluded_markets,
            "required_universe_profile": INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
            "gate_rule": "Only evaluate industry prosperity proxy for markets with coverage_ratio >= 0.995 and multi_membership_symbol_days = 0; excluded markets must not be statically backfilled."
        },
        "alpha_admission_gate": alpha_admission_gate,
        "readiness": readiness,
        "next_step": match readiness {
            "snapshot_ready_for_p310_diagnostics" => {
                "run_p310_rankic_group_decay_turnover_capacity_diagnostics"
            }
            "snapshot_multi_membership_blocked" => {
                "repair_sw2021_retroactive_current_memberships_or_add_source_version_gate"
            }
            "snapshot_raw_source_pit_failed" => "repair_raw_interval_available_at_contract",
            "snapshot_coverage_gaps_need_review" => "classify_missing_symbol_days_before_factor_design",
            _ => "repair_universe_or_calendar_inputs",
        },
        "samples": {
            "limit": limit,
            "top_missing_symbols": missing_symbols,
            "top_multi_membership_symbols": multi_membership_symbols,
            "worst_days": worst_days,
        },
        "pit_contract": {
            "entry_filter": "available_at <= trade_date AND in_date <= trade_date",
            "exit_filter": "exit_available_at IS NULL OR exit_available_at > trade_date",
            "forbidden_inputs": ["market_stock.industry static snapshot"]
        },
        "notes": [
            "This audit is source coverage/readiness only; it does not construct an alpha factor and does not unlock bounded WFA.",
            "Multi-membership symbol-days are blocking because a cross-sectional industry proxy cannot choose between overlapping L1 memberships without an explicit PIT source-version rule.",
            "Coverage gaps may be acceptable only after they are classified as ST/special shares or otherwise outside the intended tradable universe."
        ]
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
            source_filters: Vec::new(),
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
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
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
                },
            )
            .await;
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
        source_filters: Vec::new(),
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



pub(crate) async fn build_phase7_coverage_expansion_runner(
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
        // 合成 task_id 用于 registry 取消(phase7 autopilot 不注册 data_sync_task)
        let autopilot_task_id =
            format!("phase7-autopilot-{}", data_version_prefix);
        crate::sync_task_registry::spawn_sync_task(
            state.sync_tasks.clone(),
            autopilot_task_id.clone(),
            async move {
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
            },
        )
        .await;
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



pub(crate) async fn resolve_phase7_permission_smoke_symbols(
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
        "forecast" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'forecast'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "express" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'express'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "disclosure_date" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'disclosure_date'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "share_float" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'share_float'
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



pub(crate) async fn build_phase7_feasibility_audit(state: &AppState) -> Result<Value, String> {
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
              'market_stock_repurchase',
              'market_stock_forecast',
              'market_stock_express',
              'market_stock_disclosure_date',
              'market_stock_share_float'
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

    let optional_source_specs = phase7_optional_source_specs();
    let optional_query_parts: Vec<&str> = optional_source_specs
        .iter()
        .filter_map(|spec| {
            if !available_optional_tables.contains(spec.table) {
                return None;
            }
            match spec.table {
                "market_stock_cashflow" => Some(
                    "SELECT 'cashflow'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_cashflow",
                ),
                "market_stock_dividend" => Some(
                    "SELECT 'dividend'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_dividend",
                ),
                "market_stock_repurchase" => Some(
                    "SELECT 'repurchase'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_repurchase",
                ),
                "market_stock_forecast" => Some(
                    "SELECT 'forecast'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_forecast",
                ),
                "market_stock_express" => Some(
                    "SELECT 'express'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_express",
                ),
                "market_stock_disclosure_date" => Some(
                    "SELECT 'disclosure_date'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_disclosure_date",
                ),
                "market_stock_share_float" => Some(
                    "SELECT 'share_float'::text, COUNT(*)::bigint, MIN(available_at), MAX(available_at), COUNT(DISTINCT symbol)::bigint FROM market_stock_share_float",
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
            WHERE source IN ('cashflow', 'dividend', 'repurchase', 'forecast', 'express', 'disclosure_date')
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
        .iter()
        .map(|spec| {
            let (attempted_symbols, zero_row_symbols) = optional_attempts_by_source
                .get(spec.source)
                .copied()
                .unwrap_or_default();
            phase7_optional_source_json(
                spec.source,
                spec.table,
                available_optional_tables.contains(spec.table),
                optional_stats_by_source.get(spec.source),
                attempted_symbols,
                zero_row_symbols,
                listed_stock_count,
                spec.next_feature,
            )
        })
        .collect();

    let market_level_source_rows: Vec<(
        String,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<i64>,
    )> = sqlx::query_as(
        r#"
        WITH stats AS (
            SELECT 'market_margin_regime'::text AS source,
                   COUNT(*)::bigint AS data_rows,
                   MIN(trade_date) AS min_trade_date,
                   MAX(trade_date) AS latest_trade_date
            FROM market_margin
            UNION ALL
            SELECT 'market_moneyflow_hsgt_regime'::text AS source,
                   COUNT(*)::bigint AS data_rows,
                   MIN(trade_date) AS min_trade_date,
                   MAX(trade_date) AS latest_trade_date
            FROM market_moneyflow_hsgt
        )
        SELECT source,
               data_rows,
               min_trade_date,
               latest_trade_date,
               CASE
                   WHEN latest_trade_date IS NULL THEN NULL
                   ELSE (
                       SELECT COUNT(DISTINCT cal.trade_date)::bigint
                       FROM market_trade_calendar cal
                       WHERE cal.is_open
                         AND cal.trade_date > stats.latest_trade_date
                         AND cal.trade_date <= CURRENT_DATE
                   )
               END AS open_day_lag
        FROM stats
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;
    let mut market_level_source_stats = BTreeMap::new();
    for (source, data_rows, min_trade_date, latest_trade_date, open_day_lag) in
        market_level_source_rows
    {
        market_level_source_stats.insert(
            source,
            Phase7MarketLevelSourceAudit {
                data_rows,
                min_trade_date,
                latest_trade_date,
                open_day_lag,
            },
        );
    }
    let market_level_sync_task_rows: Vec<(
        String,
        String,
        String,
        Option<NaiveDate>,
        Option<NaiveDate>,
        String,
        i32,
        i32,
        i32,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        r#"
        WITH tasks AS (
            SELECT CASE
                       WHEN task_type = 'margin' THEN 'market_margin_regime'
                       WHEN task_type = 'moneyflow_hsgt' THEN 'market_moneyflow_hsgt_regime'
                   END AS source,
                   task_id,
                   task_type,
                   start_date,
                   end_date,
                   status,
                   total_count,
                   success_count,
                   failed_count,
                   error_message,
                   completed_at,
                   created_at
            FROM data_sync_task
            WHERE task_type IN ('margin', 'moneyflow_hsgt')
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY source ORDER BY created_at DESC) AS rn
            FROM tasks
            WHERE source IS NOT NULL
        )
        SELECT source,
               task_id,
               task_type,
               start_date,
               end_date,
               status,
               total_count,
               success_count,
               failed_count,
               error_message,
               completed_at
        FROM ranked
        WHERE rn = 1
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;
    let mut market_level_sync_tasks = BTreeMap::new();
    for (
        source,
        task_id,
        task_type,
        start_date,
        end_date,
        status,
        total_count,
        success_count,
        failed_count,
        error_message,
        completed_at,
    ) in market_level_sync_task_rows
    {
        market_level_sync_tasks.insert(
            source,
            Phase7MarketLevelSyncAudit {
                task_id,
                task_type,
                start_date,
                end_date,
                status,
                total_count,
                success_count,
                failed_count,
                error_message,
                completed_at,
            },
        );
    }
    let block_trade_source_stats: (
        i64,
        i64,
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
    ) = sqlx::query_as(
        r#"
        WITH stats AS (
            SELECT COUNT(*)::bigint AS data_rows,
                   COUNT(DISTINCT ts_code)::bigint AS symbols,
                   COUNT(DISTINCT trade_date)::bigint AS covered_trade_days,
                   MIN(trade_date) AS min_trade_date,
                   MAX(trade_date) AS latest_trade_date,
                   MIN(available_at) AS min_available_at,
                   MAX(available_at) AS latest_available_at,
                   COUNT(*) FILTER (WHERE available_at <= trade_date)::bigint AS pit_violation_rows
            FROM market_stock_block_trade
        )
        SELECT stats.data_rows,
               stats.symbols,
               stats.covered_trade_days,
               CASE
                   WHEN stats.min_trade_date IS NULL OR stats.latest_trade_date IS NULL THEN 0
                   ELSE (
                       SELECT COUNT(DISTINCT cal.trade_date)::bigint
                       FROM market_trade_calendar cal
                       WHERE cal.exchange = 'SSE'
                         AND cal.is_open
                         AND cal.trade_date BETWEEN stats.min_trade_date AND stats.latest_trade_date
                   )
               END AS open_days_in_range,
               stats.min_trade_date,
               stats.latest_trade_date,
               stats.min_available_at,
               stats.latest_available_at,
               stats.pit_violation_rows
        FROM stats
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;
    let block_trade_source_stats = Phase7BlockTradeSourceAudit {
        data_rows: block_trade_source_stats.0,
        symbols: block_trade_source_stats.1,
        covered_trade_days: block_trade_source_stats.2,
        open_days_in_range: block_trade_source_stats.3,
        min_trade_date: block_trade_source_stats.4,
        latest_trade_date: block_trade_source_stats.5,
        min_available_at: block_trade_source_stats.6,
        latest_available_at: block_trade_source_stats.7,
        pit_violation_rows: block_trade_source_stats.8,
    };
    let industry_membership_source_stats: (
        i64,
        i64,
        i64,
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"
        WITH stats AS (
            SELECT COUNT(*)::bigint AS data_rows,
                   COUNT(DISTINCT symbol)::bigint AS symbols,
                   COUNT(DISTINCT index_code)::bigint AS index_codes,
                   MIN(in_date) AS min_in_date,
                   MAX(in_date) AS latest_in_date,
                   MIN(out_date) AS min_out_date,
                   MAX(out_date) AS latest_out_date,
                   MIN(available_at) AS min_available_at,
                   MAX(available_at) AS latest_available_at,
                   COUNT(*) FILTER (WHERE available_at < in_date)::bigint AS pit_violation_rows,
                   COUNT(*) FILTER (WHERE out_date IS NOT NULL AND out_date < in_date)::bigint AS invalid_interval_rows
            FROM market_stock_industry_membership_pit
        ),
        duplicate_keys AS (
            SELECT COALESCE(SUM(row_count - 1), 0)::bigint AS duplicate_key_rows
            FROM (
                SELECT classification_source, index_code, symbol, in_date, COUNT(*)::bigint AS row_count
                FROM market_stock_industry_membership_pit
                GROUP BY classification_source, index_code, symbol, in_date
                HAVING COUNT(*) > 1
            ) duplicate_groups
        ),
        current_asof AS (
            SELECT MAX(trade_date) AS asof_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open
              AND trade_date <= CURRENT_DATE
        ),
        active_stocks AS (
            SELECT DISTINCT symbol
            FROM market_stock
            WHERE list_status = 'L'
              AND COALESCE(market, '') <> ''
        ),
        current_membership AS (
            SELECT DISTINCT membership.symbol
            FROM market_stock_industry_membership_pit membership
            CROSS JOIN current_asof
            WHERE current_asof.asof_date IS NOT NULL
              AND membership.classification_source = 'SW2021'
              AND membership.industry_level = 'L1'
              AND membership.available_at <= current_asof.asof_date
              AND membership.in_date <= current_asof.asof_date
              AND (
                  membership.exit_available_at IS NULL
                  OR membership.exit_available_at > current_asof.asof_date
              )
        ),
        current_coverage AS (
            SELECT COUNT(DISTINCT active_stocks.symbol)::bigint AS current_active_stock_symbols,
                   COUNT(DISTINCT current_membership.symbol)::bigint AS current_covered_stock_symbols
            FROM active_stocks
            LEFT JOIN current_membership ON current_membership.symbol = active_stocks.symbol
        )
        SELECT stats.data_rows,
               stats.symbols,
               stats.index_codes,
               current_coverage.current_active_stock_symbols,
               current_coverage.current_covered_stock_symbols,
               stats.min_in_date,
               stats.latest_in_date,
               stats.min_out_date,
               stats.latest_out_date,
               stats.min_available_at,
               stats.latest_available_at,
               stats.pit_violation_rows,
               stats.invalid_interval_rows,
               duplicate_keys.duplicate_key_rows
        FROM stats, duplicate_keys, current_coverage
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;
    let industry_membership_source_stats = Phase7IndustryMembershipSourceAudit {
        data_rows: industry_membership_source_stats.0,
        symbols: industry_membership_source_stats.1,
        index_codes: industry_membership_source_stats.2,
        current_active_stock_symbols: industry_membership_source_stats.3,
        current_covered_stock_symbols: industry_membership_source_stats.4,
        min_in_date: industry_membership_source_stats.5,
        latest_in_date: industry_membership_source_stats.6,
        min_out_date: industry_membership_source_stats.7,
        latest_out_date: industry_membership_source_stats.8,
        min_available_at: industry_membership_source_stats.9,
        latest_available_at: industry_membership_source_stats.10,
        pit_violation_rows: industry_membership_source_stats.11,
        invalid_interval_rows: industry_membership_source_stats.12,
        duplicate_key_rows: industry_membership_source_stats.13,
    };
    let equity_pledge_readiness = build_equity_pledge_pressure_readiness_audit(&state.db)
        .await
        .ok();
    let p315_new_alpha_candidate_sources =
        phase7_new_alpha_candidate_sources_with_equity_pledge_status(
            phase7_new_alpha_candidate_sources_with_block_trade_status(
                phase7_new_alpha_candidate_sources_with_industry_membership_status(
                    phase7_new_alpha_candidate_sources_with_market_status(
                        &market_level_source_stats,
                        &market_level_sync_tasks,
                        Utc::now().date_naive(),
                    ),
                    &industry_membership_source_stats,
                ),
                &block_trade_source_stats,
            ),
            equity_pledge_readiness.as_ref(),
        );
    let futures_price_chain_readiness =
        match build_futures_price_chain_mapping_audit(&state.db).await {
            Ok(audit) => Some(audit),
            Err(_) => build_futures_price_chain_readiness_audit(&state.db)
                .await
                .ok(),
        };
    let mut p319_candidate_admission =
        phase7_p319_candidate_admission_sources(futures_price_chain_readiness.as_ref());
    if let Some(readiness) = equity_pledge_readiness.as_ref() {
        apply_p320_equity_pledge_readiness(&mut p319_candidate_admission, readiness);
    }

    Ok(json!({
        "audit_version": "phase7-fd-v1",
        "listed_stock_count": listed_stock_count,
        "status": "needs_data_expansion_before_new_alpha_discovery",
        "market_coverage": phase7_coverage_rows_to_json(market_rows, listed_stock_count),
        "financial_coverage": phase7_coverage_rows_to_json(financial_rows, listed_stock_count),
        "event_coverage": phase7_coverage_rows_to_json(event_rows, listed_stock_count),
        "phase7_combo_coverage": combo_coverage,
        "optional_data_sources": optional_data_sources,
        "p315_new_alpha_candidate_sources": p315_new_alpha_candidate_sources,
        "p319_candidate_admission": p319_candidate_admission,
        "tushare_permission_notes": {
            "forecast": "2000-point interface is usable by symbol; full-market quarterly forecast_vip requires higher permission.",
            "express": "2000-point interface is usable by symbol; full-market quarterly express_vip requires higher permission.",
            "cashflow_dividend_repurchase": "Current Pro 2000 permission passed bounded smoke; use optional_data_sources.feature_readiness before feature backfill or training.",
            "main_business": "fina_mainbz is read-only smokeable as main_business, but it has no native announcement date; available_at join audit is required before schema, sync, factor backfill, P3.10, or WFA.",
            "report_rc": "report_rc official docs exist, but production smoke on 2026-06-21 returned Tushare 40101 unknown data source; do not build schema/sync/factors until the callable API path is verified.",
            "futures_price_chain": "fut_daily/fut_wsr/fut_holding data/PIT/mapping passed, but combo and component diagnostics failed P3.10 economics; do not expand same-family parameters or enter WFA/v19.",
            "equity_pledge_pressure": "pledge_stat/pledge_detail data/PIT/coverage passed, but the current low-pledge-ratio atom failed P3.10 economics; do not expand same-family parameters or enter WFA/v19.",
            "shareholder_structure": "shareholder low-fanout PIT factor passed single-factor P3.10 but bounded WFA skipped all windows by train robustness/cost-capacity gates; do not expand sleeve weights or enter v19."
        },
        "recommended_next_steps": [
            "For P3.24, prioritize licensed broad-base consensus revision, exchange/CNInfo announcement text for orders/capacity/contracts/price adjustments, and valid equity-incentive execution sources before any factor work.",
            "Do not expand stopped futures_price_chain, equity_pledge_pressure, shareholder_structure, event, moneyflow, unlock, liquidity, industry-prosperity, or main-business same-family variants.",
            "Treat ML algorithm changes as secondary expression/ensemble work only after a source passes P3.10A-D economics; do not use model changes to rescue failed sources.",
            "For any new source, enforce permission/schema/available_at -> bounded sync -> coverage/PIT -> P3.10A-D -> bounded WFA before factor builder, ML training, or v19 train selection.",
            "Only after a source passes bounded WFA should active-v19 incremental overlay and replay/paper-trading hardening restart."
        ],
    }))
}

/// POST /api/v1/quant/data/sync/daily/background
///
/// 大批量后台同步日线行情，立即返回 task_id。
/// 通过 GET /api/v1/quant/data/sync/tasks/:task_id 查询进度。


pub(crate) fn sync_task_cancel_transition(status: &str) -> Option<&'static str> {
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



pub(crate) fn proven_limit_list_earliest_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2019, 11, 28).expect("valid limit_list_d earliest date")
}



pub(crate) fn parse_health_date(value: &str) -> Result<NaiveDate, String> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .map_err(|_| format!("日期格式无效: {}，需要 YYYY-MM-DD 或 YYYYMMDD", value))
}



pub(crate) fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}



fn default_mvo_etfs() -> Vec<String> {
    DEFAULT_MVO_ETFS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect()
}



pub(crate) fn parse_etf_symbols(value: Option<Value>) -> Vec<String> {
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



pub(crate) fn coverage_level(expected: i64, actual: i64) -> &'static str {
    if expected <= 0 || actual >= expected {
        "green"
    } else {
        "red"
    }
}



pub(crate) fn event_sync_source_quality(
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



pub(crate) fn event_sync_source_level(
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



pub(crate) fn event_sync_source_label(
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



pub(crate) fn lag_level(lag_days: i64, yellow_after_days: i64, red_after_days: i64) -> &'static str {
    if lag_days > red_after_days {
        "red"
    } else if lag_days > yellow_after_days {
        "yellow"
    } else {
        "green"
    }
}



pub(crate) fn check_item(
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



fn data_readiness_required(check: &Value) -> bool {
    check
        .get("required")
        .and_then(|value| value.as_bool())
        .unwrap_or(true)
}



pub(crate) fn data_readiness_blocking_checks<'a>(
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



pub(crate) fn data_readiness_failure_message(
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



