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
        let autopilot_task_id = format!("phase7-autopilot-{}", data_version_prefix);
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
