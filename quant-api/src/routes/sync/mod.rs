/// 数据同步路由
use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
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
pub mod candidate_admission;
pub mod coverage_batches;
pub mod coverage_planner;
pub mod eod;
pub mod exchange_announcement;
pub mod market_data;
pub mod phase7_audit;
pub mod reference_data;
pub mod schema_contracts;
pub mod source_decisions;
pub mod task_lifecycle;

#[cfg(test)]
mod tests;

// Re-export all public items to preserve routes::sync::X path
pub use account_health::*;
pub use analyst_revision::*;
pub use bounded_raw::*;
pub(crate) use candidate_admission::*;
pub(crate) use coverage_batches::*;
pub(crate) use coverage_planner::*;
pub use eod::*;
pub use exchange_announcement::*;
pub use market_data::*;
pub use phase7_audit::*;
pub use reference_data::*;
pub(crate) use schema_contracts::*;
pub(crate) use source_decisions::*;
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
    // R11/R12: 转调 versioning 集中化（data_version ID 生成逻辑单一出口）。
    quant_data::versioning::generate_version_id().to_string()
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
pub(crate) const AKSHARE_ANALYST_REVISION_ATTEMPT_SOURCE: &str =
    "akshare:stock_rank_forecast_cninfo";
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

pub(crate) fn lag_level(
    lag_days: i64,
    yellow_after_days: i64,
    red_after_days: i64,
) -> &'static str {
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

pub(crate) fn data_readiness_blocking_checks(
    checks: &[Value],
    gate: DataReadinessGate,
) -> Vec<&Value> {
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

// ── 第六批覆盖率测试：sync 编排纯函数 / 公告审计计划与决策矩阵 / 注册与可行性审计连库路径 ──
// 模式沿用 coverage_batches.rs fourth_batch：inner 直调，跳过 axum HTTP 层与真实 Tushare；
// 连库只读测试走真实本机 PG；写库仅 zzz_test_ 前缀 task_id，前置与结尾精确清理。
#[cfg(test)]
mod sixth_batch {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
    }

    /// 构造真实本机 PG 连接（DATABASE_URL 缺省 postgres://gaocheng@localhost/quant）。
    async fn test_app_state() -> crate::AppState {
        dotenv::dotenv().ok();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("test db connect");
        crate::AppState {
            start_time: Utc::now(),
            db,
            tushare: quant_data::tushare::client::TushareClient::from_env()
                .expect("Tushare client init（dotenv 加载 quant/.env 后需 TUSHARE_TOKEN）"),
            sync_tasks: crate::sync_task_registry::new_registry(),
        }
    }

    /// 直调 handler 后取 JSON body（沿 market_data.rs resp_json 先例）。
    async fn resp_json(resp: impl IntoResponse) -> Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    /// DataSyncTaskReq 无 Default derive，手工列全字段构造测试请求。
    fn sync_task_req(dataset: &str) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: dataset.to_string(),
            source: default_source(),
            mode: None,
            symbols: Vec::new(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: None,
            end_date: None,
            data_version_id: None,
            background: false,
            quality_check: false,
            create_data_version: false,
            retry_of_task_id: None,
            reason: None,
        }
    }

    // ── 零依赖纯函数 ──

    #[test]
    fn default_source_is_tushare() {
        assert_eq!(default_source(), "tushare");
    }

    #[test]
    fn generated_version_id_is_nonempty_and_unique_per_call() {
        // data_version ID 生成单一出口（R11/R12 versioning 集中化）。
        // 唯一性：毫秒级时间戳，并行下两次调用可能同毫秒——单对 assert_ne 偶发
        // 脆弱（generate_version_id 同款教训），改快速连生成多次断言跨毫秒不同
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..2000 {
            distinct.insert(generated_data_version_id());
        }
        assert!(distinct.len() >= 2, "2000 次生成应跨毫秒产生不同 id");
    }

    #[test]
    fn require_range_checks_start_and_end_presence() {
        let mut req = sync_task_req("daily");
        assert_eq!(
            require_range(&req).unwrap_err(),
            "start_date is required",
            "缺 start_date 必须明确报错"
        );

        req.start_date = Some("20240101".into());
        assert_eq!(
            require_range(&req).unwrap_err(),
            "end_date is required",
            "缺 end_date 必须明确报错"
        );

        req.end_date = Some("20241231".into());
        assert_eq!(require_range(&req).unwrap(), ("20240101", "20241231"));
    }

    #[test]
    fn cninfo_link_metadata_parses_complete_query_params() {
        let link = "https://static.cninfo.com.cn/finalpage/2024-01-15/121939.PDF?announcementId=121939&orgId=gssz000600&stockCode=600000&announcementTime=2024-01-15";
        let metadata = parse_cninfo_announcement_link_metadata(link);
        assert_eq!(metadata["announcement_id"], "121939");
        assert_eq!(metadata["org_id"], "gssz000600");
        assert_eq!(metadata["stock_code"], "600000");
        assert_eq!(metadata["announcement_time"], "2024-01-15");
        assert_eq!(metadata["metadata_complete"], true);
        assert_eq!(metadata["missing_fields"].as_array().unwrap().len(), 0);
        assert_eq!(metadata["raw_link"], link);
    }

    #[test]
    fn cninfo_link_metadata_reports_missing_fields() {
        // 缺 orgId/stockCode/announcementTime → 计入 missing_fields，metadata_complete=false
        let partial =
            "https://static.cninfo.com.cn/finalpage/2024-01-15/121939.PDF?announcementId=121939";
        let metadata = parse_cninfo_announcement_link_metadata(partial);
        assert_eq!(metadata["metadata_complete"], false);
        let missing = metadata["missing_fields"].as_array().unwrap();
        assert!(missing.contains(&json!("orgId")));
        assert!(missing.contains(&json!("stockCode")));
        assert!(missing.contains(&json!("announcementTime")));
        assert!(!missing.contains(&json!("announcementId")));

        // 空 query 时 announcementId 从路径文件名回退提取，其余字段缺失
        let path_only = "https://static.cninfo.com.cn/finalpage/2024-01-15/987654.PDF";
        let fallback = parse_cninfo_announcement_link_metadata(path_only);
        assert_eq!(fallback["announcement_id"], "987654");
        assert_eq!(fallback["metadata_complete"], false);

        // 参数存在但值为空白同样视为缺失
        let blank = "https://x.cn/a.PDF?announcementId=1&orgId=&stockCode=  &announcementTime=2";
        let blank_metadata = parse_cninfo_announcement_link_metadata(blank);
        assert_eq!(blank_metadata["metadata_complete"], false);
        assert!(blank_metadata["missing_fields"]
            .as_array()
            .unwrap()
            .contains(&json!("orgId")));
    }

    #[test]
    fn exchange_smoke_plan_requires_symbols_and_categories() {
        let mut req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: Vec::new(),
            categories: Vec::new(),
            market: None,
            start_date: None,
            end_date: None,
            limit: None,
            python: None,
        };
        let error = exchange_announcement_order_capacity_smoke_plan(&req)
            .expect_err("symbols 为空必须拒绝");
        assert!(error.contains("at least one symbol"), "实际错误: {error}");

        // categories 全为空白：trim 后为空且不回退默认（默认仅作用于整个字段缺省）
        req.symbols = vec!["600000.SH".to_string()];
        req.categories = vec!["   ".to_string()];
        let error = exchange_announcement_order_capacity_smoke_plan(&req)
            .expect_err("categories 解析后为空必须拒绝");
        assert!(error.contains("at least one category"), "实际错误: {error}");
    }

    #[test]
    fn exchange_smoke_plan_builds_read_only_probe_plan() {
        let req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: vec![
                " 600000.SH ".to_string(),
                "600000.SH".to_string(),
                "000001.SZ".to_string(),
            ],
            categories: vec!["重大事项".to_string()],
            market: None,
            start_date: Some("20240101".into()),
            end_date: Some("20240630".into()),
            limit: Some(999),
            python: None,
        };
        let plan = exchange_announcement_order_capacity_smoke_plan(&req).expect("valid smoke plan");

        // symbol 取点号前部分、trim、去重；limit clamp 到 20
        let symbols = plan["symbols"].as_array().unwrap();
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0], "600000");
        assert_eq!(symbols[1], "000001");
        assert_eq!(plan["categories"][0], "重大事项");
        assert_eq!(plan["market"], "沪深京");
        assert_eq!(plan["date_range"]["start_date"], "20240101");
        assert_eq!(plan["date_range"]["end_date"], "20240630");
        assert_eq!(plan["row_limit_per_probe"], 20);
        assert_eq!(plan["query_count"], 2);
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["mode"],
            "read_only_permission_history_category_smoke_no_write"
        );
        assert_eq!(plan["vendor"], "akshare");
        assert_eq!(plan["upstream"], "cninfo");
        // python 解析可能受环境变量影响，仅断言非空
        assert!(!plan["python"].as_str().unwrap().is_empty());
        assert_eq!(plan["promotion_gate"]["schema_apply"], "blocked");
    }

    #[test]
    fn exchange_smoke_plan_rejects_inverted_date_range() {
        let req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: vec!["600000.SH".to_string()],
            categories: Vec::new(),
            market: None,
            start_date: Some("20240630".into()),
            end_date: Some("20240101".into()),
            limit: None,
            python: None,
        };
        let error =
            exchange_announcement_order_capacity_smoke_plan(&req).expect_err("start>end 必须拒绝");
        assert!(
            error.contains("cannot be after end_date"),
            "实际错误: {error}"
        );
    }

    #[test]
    fn exchange_detail_audit_plan_requires_links() {
        let req = ExchangeAnnouncementOrderCapacityDetailAuditReq {
            announcement_links: Vec::new(),
            limit: None,
            python: None,
        };
        let error = exchange_announcement_order_capacity_detail_audit_plan(&req)
            .expect_err("空链接必须拒绝");
        assert!(
            error.contains("at least one announcement link"),
            "实际错误: {error}"
        );
    }

    #[test]
    fn exchange_detail_audit_plan_limits_links_and_counts_incomplete_metadata() {
        let req = ExchangeAnnouncementOrderCapacityDetailAuditReq {
            announcement_links: vec![
                "https://static.cninfo.com.cn/finalpage/2024-01-15/121939.PDF?announcementId=121939&orgId=gssz000600&stockCode=600000&announcementTime=2024-01-15".to_string(),
                "https://static.cninfo.com.cn/finalpage/2024-02-20/222222.PDF".to_string(),
            ],
            limit: Some(1),
            python: None,
        };
        let plan =
            exchange_announcement_order_capacity_detail_audit_plan(&req).expect("detail plan");
        // limit=1 截断：只保留第一条（元数据完整的链接）
        assert_eq!(plan["link_count"], 1);
        assert_eq!(plan["incomplete_link_metadata_count"], 0);
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["mode"],
            "read_only_cninfo_detail_text_timestamp_hash_audit_no_write"
        );
        assert_eq!(plan["vendor"], "cninfo");
        assert_eq!(plan["vendor_endpoint"], "announcement_detail_page");
        assert_eq!(
            plan["promotion_gate"]["schema_apply"],
            "blocked_until_detail_audit_passes"
        );

        // 不截断时：第二条链接元数据不完整 → incomplete 计数为 1
        let full = ExchangeAnnouncementOrderCapacityDetailAuditReq { limit: None, ..req };
        let full_plan = exchange_announcement_order_capacity_detail_audit_plan(&full)
            .expect("detail plan without limit");
        assert_eq!(full_plan["link_count"], 2);
        assert_eq!(full_plan["incomplete_link_metadata_count"], 1);
    }

    #[test]
    fn exchange_pdf_detail_audit_plan_requires_links() {
        let req = ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
            announcement_links: Vec::new(),
            limit: None,
            python: None,
        };
        let error = exchange_announcement_order_capacity_pdf_detail_audit_plan(&req)
            .expect_err("空链接必须拒绝");
        assert!(
            error.contains("at least one announcement link"),
            "实际错误: {error}"
        );
    }

    #[test]
    fn exchange_pdf_detail_audit_plan_builds_pdf_readiness_audit() {
        let req = ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
            announcement_links: vec![
                "https://static.cninfo.com.cn/finalpage/2024-01-15/121939.PDF?announcementId=121939&orgId=gssz000600&stockCode=600000&announcementTime=2024-01-15".to_string(),
            ],
            limit: None,
            python: None,
        };
        let plan = exchange_announcement_order_capacity_pdf_detail_audit_plan(&req)
            .expect("pdf detail plan");
        assert_eq!(plan["link_count"], 1);
        assert_eq!(plan["incomplete_link_metadata_count"], 0);
        assert_eq!(plan["stage"], "P3.24E");
        assert_eq!(
            plan["mode"],
            "read_only_pdf_detail_text_timestamp_span_audit_no_write"
        );
        assert_eq!(plan["vendor_endpoint"], "announcement_pdf_detail");
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["promotion_gate"]["schema_apply"],
            "blocked_until_pdf_detail_audit_passes"
        );
        // 证据链必须包含 PDF 文本与证据区间两项
        let evidence = plan["required_evidence"].as_array().unwrap();
        assert!(evidence
            .iter()
            .any(|item| item == "nonempty_pdf_text_content"));
        assert!(evidence.iter().any(|item| item == "stable_text_hash"));
    }

    #[test]
    fn exchange_ocr_blocked_row_audit_plan_defaults_and_clamps() {
        let req = ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
            start_date: None,
            end_date: None,
            symbols: Some("600000.SH, 000001.SZ".to_string()),
            limit: Some(999),
            python: None,
        };
        let plan = exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(&req)
            .expect("ocr audit plan");
        // 默认起点 20140101，终点为当天（YYYYMMDD 8 位数字）
        assert_eq!(plan["date_range"]["start_date"], "20140101");
        let end_date = plan["date_range"]["end_date"].as_str().unwrap();
        assert_eq!(end_date.len(), 8, "默认 end_date 应为 YYYYMMDD");
        assert!(end_date.chars().all(|ch| ch.is_ascii_digit()));
        // CSV 解析后符号取点号前部分
        let symbols = plan["symbols"].as_array().unwrap();
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0], "600000");
        assert_eq!(symbols[1], "000001");
        // limit clamp 到 OCR 审计上限 8
        assert_eq!(plan["row_limit"], 8);
        assert_eq!(
            plan["row_selector"]["pdf_parse_status"],
            "scanned_pdf_ocr_required"
        );
        assert_eq!(
            plan["source_table"],
            "market_exchange_announcement_text_raw"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["promotion_gate"]["raw_backfill"],
            "blocked_until_ocr_quality_and_manual_taxonomy_review_pass"
        );
    }

    #[test]
    fn exchange_ocr_blocked_row_audit_plan_rejects_inverted_and_malformed_dates() {
        let inverted = ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
            start_date: Some("20240630".into()),
            end_date: Some("20240101".into()),
            symbols: None,
            limit: None,
            python: None,
        };
        let error = exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(&inverted)
            .expect_err("start>end 必须拒绝");
        assert!(
            error.contains("cannot be after end_date"),
            "实际错误: {error}"
        );

        let malformed = ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
            start_date: Some("2024/01/01".into()),
            end_date: Some("20240630".into()),
            symbols: None,
            limit: None,
            python: None,
        };
        let error = exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(&malformed)
            .expect_err("非法日期格式必须拒绝");
        assert!(error.contains("YYYYMMDD"), "实际错误: {error}");
    }

    #[test]
    fn exchange_detail_audit_decision_covers_full_gate_matrix() {
        // 无链接
        let none = decide_exchange_announcement_detail_audit(0, 0, 0, 0, 0);
        assert_eq!(none["admission_decision"], "blocked_no_detail_links");
        assert_eq!(none["promotion_gate"]["schema_apply"], "blocked");

        // 链接元数据不完整
        let incomplete = decide_exchange_announcement_detail_audit(2, 2, 2, 2, 1);
        assert_eq!(
            incomplete["admission_decision"],
            "blocked_incomplete_announcement_link_metadata"
        );

        // 正文抓取不完整
        let unfetched = decide_exchange_announcement_detail_audit(2, 1, 2, 2, 0);
        assert_eq!(
            unfetched["admission_decision"],
            "blocked_detail_text_fetch_incomplete"
        );

        // 缺文本哈希
        let unhashed = decide_exchange_announcement_detail_audit(2, 2, 1, 2, 0);
        assert_eq!(unhashed["admission_decision"], "blocked_missing_text_hash");

        // 缺 source_published_at 时间戳
        let untimed = decide_exchange_announcement_detail_audit(2, 2, 2, 1, 0);
        assert_eq!(
            untimed["admission_decision"],
            "blocked_missing_source_published_at_timestamp"
        );

        // 全部就绪 → 允许进入人工 schema 复核
        let passed = decide_exchange_announcement_detail_audit(2, 2, 2, 2, 0);
        assert_eq!(
            passed["admission_decision"],
            "detail_text_timestamp_hash_audit_passed_schema_review_allowed_next"
        );
        assert_eq!(
            passed["promotion_gate"]["schema_apply"],
            "manual_review_required"
        );
        assert_eq!(passed["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(passed["promotion_gate"]["v19_train_selection"], "blocked");
        assert_eq!(passed["total_links"], 2);
        assert_eq!(passed["fetched_text_count"], 2);
    }

    #[test]
    fn exchange_pdf_detail_audit_decision_covers_full_gate_matrix() {
        // 参数顺序：total/parsed/stable_hash/availability/evidence_span/scanned/incomplete
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(0, 0, 0, 0, 0, 0, 0)
                ["admission_decision"],
            "blocked_no_pdf_detail_links"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 2, 2, 0)
                ["admission_decision"],
            "blocked_scanned_pdf_ocr_required"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 2, 0, 1)
                ["admission_decision"],
            "blocked_incomplete_announcement_link_metadata"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 1, 2, 2, 2, 0, 0)
                ["admission_decision"],
            "blocked_pdf_fetch_or_parse_incomplete"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 1, 2, 2, 0, 0)
                ["admission_decision"],
            "blocked_unstable_pdf_text_hash"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 1, 2, 0, 0)
                ["admission_decision"],
            "blocked_missing_pdf_source_published_at_or_next_session_policy"
        );
        assert_eq!(
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 1, 0, 0)
                ["admission_decision"],
            "blocked_missing_relevant_evidence_spans"
        );

        // 全部就绪 → 允许进入人工 schema 复核
        let passed =
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 2, 0, 0);
        assert_eq!(
            passed["admission_decision"],
            "pdf_detail_audit_passed_manual_schema_review_allowed_next"
        );
        assert_eq!(
            passed["promotion_gate"]["schema_apply"],
            "manual_review_required"
        );
        assert_eq!(passed["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(passed["scanned_pdf_count"], 0);
    }

    #[test]
    fn akshare_sync_plan_response_marks_small_calendar_range_safe_to_run() {
        // 既有测试只覆盖 175 天（safe=false）；此处补 ≤100 天与空批次分支
        let start = date(2026, 1, 1);
        let end = date(2026, 3, 31); // 90 个日历日 ≤ 100
        let empty = akshare_analyst_revision_sync_plan_response(start, end, "quarter", Vec::new());
        assert_eq!(empty["safe_to_run_full_range"], true);
        assert_eq!(empty["batch_count"], 0);
        assert_eq!(empty["estimated_total_rows"], 0);
        assert_eq!(empty["estimated_api_calls"], 90);
        assert_eq!(empty["date_range"]["calendar_day_count"], 90);
        assert!(
            empty["sync_endpoint_status"]
                .as_str()
                .unwrap()
                .contains("background_false")
        );

        // 单批次估算：90 天 × 350 行/天 = 31500
        let batch = AkshareAnalystRevisionSyncPlanBatch {
            label: "2026Q1".to_string(),
            start_date: start,
            end_date: end,
            calendar_day_count: 90,
        };
        let planned =
            akshare_analyst_revision_sync_plan_response(start, end, "quarter", vec![batch]);
        assert_eq!(planned["batch_count"], 1);
        assert_eq!(planned["estimated_total_rows"], 31500);
        assert_eq!(planned["batches"][0]["estimated_api_calls"], 90);
        assert_eq!(
            planned["batches"][0]["future_bounded_sync_request"]["start_date"],
            "20260101"
        );
        assert_eq!(
            planned["batches"][0]["future_bounded_sync_request"]["data_version_id"],
            "akshare-analyst-revision-2026Q1"
        );
    }

    #[tokio::test]
    async fn structured_price_chain_handlers_wrap_contracts_with_code_zero() {
        // 三个只读契约 handler 均为无状态 Json 包装，直调并断言统一响应壳
        let contract =
            resp_json(structured_order_capacity_price_chain_source_contract().await).await;
        assert_eq!(contract["code"], 0);
        assert!(contract["data"].as_object().is_some());

        let admission =
            resp_json(structured_order_capacity_price_chain_vendor_admission_plan().await).await;
        assert_eq!(admission["code"], 0);
        assert!(admission["data"].as_object().is_some());

        let inventory =
            resp_json(structured_order_capacity_price_chain_source_evidence_inventory().await)
                .await;
        assert_eq!(inventory["code"], 0);
        assert!(inventory["data"].as_object().is_some());
    }

    // ── 连库路径：register_sync_task（写库 zzz_test_ 前缀 + 精确清理）──

    #[tokio::test]
    async fn register_sync_task_rejects_non_yyyymmdd_dates() {
        let state = test_app_state().await;
        let mut req = sync_task_req("daily");
        req.symbols = vec!["600000.SH".to_string()];
        // 连字符格式在写库前被拒（YYYYMMDD 强约束）
        req.start_date = Some("2024-01-01".into());
        let error = register_sync_task(&state, "zzz_test_sixth_batch_bad_date", &req, "pending")
            .await
            .expect_err("连字符日期必须拒绝");
        assert!(error.contains("YYYYMMDD"), "实际错误: {error}");

        req.start_date = Some("20240101".into());
        req.end_date = Some("31-12-2024".into());
        let error = register_sync_task(&state, "zzz_test_sixth_batch_bad_date", &req, "pending")
            .await
            .expect_err("非法 end_date 必须拒绝");
        assert!(error.contains("YYYYMMDD"), "实际错误: {error}");
    }

    #[tokio::test]
    async fn register_sync_task_persists_dataset_specific_symbols() {
        let state = test_app_state().await;
        // 前置精确清理，保证幂等
        sqlx::query("DELETE FROM data_sync_task WHERE task_id LIKE 'zzz_test_sixth_batch_reg_%'")
            .execute(&state.db)
            .await
            .expect("前置清理");

        // index_daily：symbols 取 index_codes 而非 symbols
        let mut index_req = sync_task_req("index_daily");
        index_req.symbols = vec!["600000.SH".to_string()];
        index_req.index_codes = vec!["000001.SH".to_string()];
        index_req.start_date = Some("20240101".into());
        index_req.end_date = Some("20240131".into());
        register_sync_task(
            &state,
            "zzz_test_sixth_batch_reg_index",
            &index_req,
            "completed",
        )
        .await
        .expect("register index_daily");

        // trade_cal：symbols 取 exchanges
        let mut cal_req = sync_task_req("trade_cal");
        cal_req.exchanges = vec!["SSE".to_string()];
        register_sync_task(
            &state,
            "zzz_test_sixth_batch_reg_cal",
            &cal_req,
            "completed",
        )
        .await
        .expect("register trade_cal");

        // 默认：symbols 为空时落 NULL
        let plain_req = sync_task_req("daily");
        register_sync_task(
            &state,
            "zzz_test_sixth_batch_reg_plain",
            &plain_req,
            "completed",
        )
        .await
        .expect("register daily without symbols");

        let index_row: (Option<Vec<String>>, Option<NaiveDate>, Option<NaiveDate>) =
            sqlx::query_as(
                "SELECT symbols, start_date, end_date FROM data_sync_task WHERE task_id = $1",
            )
            .bind("zzz_test_sixth_batch_reg_index")
            .fetch_one(&state.db)
            .await
            .expect("index task row");
        assert_eq!(
            index_row.0,
            Some(vec!["000001.SH".to_string()]),
            "index_daily 任务应落 index_codes"
        );
        assert_eq!(index_row.1, Some(date(2024, 1, 1)));
        assert_eq!(index_row.2, Some(date(2024, 1, 31)));

        let cal_row: (Option<Vec<String>>,) =
            sqlx::query_as("SELECT symbols FROM data_sync_task WHERE task_id = $1")
                .bind("zzz_test_sixth_batch_reg_cal")
                .fetch_one(&state.db)
                .await
                .expect("calendar task row");
        assert_eq!(cal_row.0, Some(vec!["SSE".to_string()]));

        let plain_row: (Option<Vec<String>>,) =
            sqlx::query_as("SELECT symbols FROM data_sync_task WHERE task_id = $1")
                .bind("zzz_test_sixth_batch_reg_plain")
                .fetch_one(&state.db)
                .await
                .expect("plain task row");
        assert!(plain_row.0.is_none(), "空 symbols 应落 NULL");

        // 结尾精确清理
        sqlx::query("DELETE FROM data_sync_task WHERE task_id LIKE 'zzz_test_sixth_batch_reg_%'")
            .execute(&state.db)
            .await
            .expect("结尾清理");
    }

    // ── 连库只读：phase7 可行性审计全量快照 ──

    #[tokio::test]
    async fn build_phase7_feasibility_audit_returns_readiness_snapshot() {
        let state = test_app_state().await;
        let audit = build_phase7_feasibility_audit(&state)
            .await
            .expect("本机库表齐全，可行性审计应成功返回");

        assert_eq!(audit["audit_version"], "phase7-fd-v1");
        assert_eq!(
            audit["status"],
            "needs_data_expansion_before_new_alpha_discovery"
        );
        // 本机 market_stock 有 7200+ 只股票
        assert!(audit["listed_stock_count"].as_i64().unwrap() > 0);

        // 行情/财务/事件三大覆盖块均为数组且表数正确（UNION 查询行数）
        assert_eq!(audit["market_coverage"].as_array().unwrap().len(), 3);
        assert_eq!(audit["financial_coverage"].as_array().unwrap().len(), 2);
        assert_eq!(audit["event_coverage"].as_array().unwrap().len(), 3);

        // 十个 phase7 combo 全量占位（库中无数据的 combo 也补 0 行快照）
        let combos = audit["phase7_combo_coverage"].as_array().unwrap();
        assert_eq!(combos.len(), PHASE7_FEASIBILITY_COMBOS.len());
        assert_eq!(combos.len(), 10);

        // 七个可选数据源（cashflow/dividend/repurchase/forecast/express/disclosure_date/share_float）
        assert_eq!(audit["optional_data_sources"].as_array().unwrap().len(), 7);

        // 新 alpha 候选与 P3.19 准入块存在且非空
        assert!(audit.get("p315_new_alpha_candidate_sources").is_some());
        assert!(audit.get("p319_candidate_admission").is_some());

        let next_steps = audit["recommended_next_steps"].as_array().unwrap();
        assert!(!next_steps.is_empty());
        // 权限备注必须覆盖关键源
        let notes = audit["tushare_permission_notes"].as_object().unwrap();
        assert!(notes.contains_key("forecast"));
        assert!(notes.contains_key("shareholder_structure"));
    }
}
