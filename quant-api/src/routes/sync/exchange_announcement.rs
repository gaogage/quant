/// 数据同步路由
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::env;
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration as StdDuration};
use tokio::{process::Command, time::timeout};

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeAnnouncementOrderCapacitySmokeReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityDetailAuditReq {
    #[serde(default)]
    pub announcement_links: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityPdfParserReadinessReq {
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
    #[serde(default)]
    pub announcement_links: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub symbols: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacitySyncPlanReq {
    #[serde(default)]
    pub symbols: Option<String>,
    #[serde(default)]
    pub categories: Option<String>,
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacitySyncReq {
    #[serde(default)]
    pub symbols: Option<String>,
    #[serde(default)]
    pub categories: Option<String>,
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub python: Option<String>,
    #[serde(default)]
    pub pdf_python: Option<String>,
    #[serde(default)]
    pub background: bool,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityAdmissionReadinessAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub include_negative_samples: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ExchangeAnnouncementOrderCapacityBoundedSyncReq {
    #[serde(default)]
    pub symbols: Option<String>,
    #[serde(default)]
    pub categories: Option<String>,
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub python: Option<String>,
    #[serde(default)]
    pub pdf_python: Option<String>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub stop_on_audit_failure: Option<bool>,
}

impl ExchangeAnnouncementOrderCapacitySyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        let symbols = exchange_announcement_order_capacity_csv_values(self.symbols);
        let categories = exchange_announcement_order_capacity_csv_values(self.categories);
        DataSyncTaskReq {
            dataset: "exchange_announcement_order_capacity".to_string(),
            source: EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_TASK_SOURCE.to_string(),
            mode: Some("tiny_calendar_day_symbol_category_raw_sync".to_string()),
            symbols,
            source_filters: categories,
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: self.start_date,
            end_date: self.end_date,
            data_version_id: self.data_version_id,
            background: self.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("p3.24 exchange announcement tiny raw sync smoke".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
    pub(crate) table_exists: bool,
    pub(crate) row_count: i64,
    pub(crate) distinct_symbol_count: i64,
    pub(crate) distinct_category_count: i64,
    pub(crate) pit_violation_rows: i64,
    pub(crate) missing_available_at_rows: i64,
    pub(crate) missing_source_published_at_quality_rows: i64,
    pub(crate) duplicate_announcement_id_rows: i64,
    pub(crate) duplicate_raw_payload_hash_groups: i64,
    pub(crate) evidence_span_rows: i64,
    pub(crate) target_event_rows: i64,
    pub(crate) target_event_missing_evidence_span_rows: i64,
    pub(crate) scanned_pdf_ocr_required_rows: i64,
    pub(crate) ocr_taxonomy_excluded_rows: i64,
    pub(crate) trainable_scanned_pdf_blocking_rows: i64,
    pub(crate) taxonomy_blocked_target_event_rows: i64,
    pub(crate) taxonomy_risk_category_rows: i64,
    pub(crate) completed_attempts: i64,
    pub(crate) completed_empty_attempts: i64,
    pub(crate) failed_attempts: i64,
    pub(crate) total_failed_attempts: i64,
    pub(crate) excluded_unsupported_category_failed_attempts: i64,
}

pub(crate) fn exchange_announcement_order_capacity_pdf_audit_python_path(
    requested: Option<String>,
) -> String {
    requested
        .filter(|path| !path.trim().is_empty())
        .or_else(|| env::var("QUANT_PDF_AUDIT_PYTHON").ok())
        .unwrap_or_else(|| {
            let home = env::var("HOME").unwrap_or_else(|_| "/Users/gaocheng".to_string());
            format!("{home}/.local/share/quant-pdf-audit/venv/bin/python")
        })
}

pub(crate) fn exchange_announcement_order_capacity_row_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(PHASE7_PERMISSION_SMOKE_MAX_ROWS)
        .clamp(1, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS)
}

fn exchange_announcement_order_capacity_symbol(value: &str) -> String {
    value
        .trim()
        .split('.')
        .next()
        .unwrap_or(value.trim())
        .to_string()
}

pub(crate) fn exchange_announcement_order_capacity_symbols(symbols: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    symbols
        .iter()
        .map(|symbol| exchange_announcement_order_capacity_symbol(symbol))
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .take(EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_SYMBOLS)
        .collect()
}

pub(crate) fn exchange_announcement_order_capacity_csv_values(
    value: Option<String>,
) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

pub(crate) fn exchange_announcement_order_capacity_categories(
    categories: &[String],
) -> Vec<String> {
    let raw_categories = if categories.is_empty() {
        EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_DEFAULT_CATEGORIES
            .iter()
            .map(|category| category.to_string())
            .collect::<Vec<_>>()
    } else {
        categories
            .iter()
            .map(|category| category.trim().to_string())
            .filter(|category| !category.is_empty())
            .collect::<Vec<_>>()
    };

    let mut seen = BTreeSet::new();
    raw_categories
        .into_iter()
        .filter(|category| seen.insert(category.clone()))
        .take(EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_CATEGORIES)
        .collect()
}

pub(crate) fn exchange_announcement_order_capacity_date_range(
    req: &ExchangeAnnouncementOrderCapacitySmokeReq,
) -> Result<(String, String), String> {
    let today = Utc::now().date_naive();
    let default_start = (today - Duration::days(365)).format("%Y%m%d").to_string();
    let default_end = today.format("%Y%m%d").to_string();
    let start_date = req.start_date.clone().unwrap_or(default_start);
    let end_date = req.end_date.clone().unwrap_or(default_end);
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("exchange announcement smoke start_date cannot be after end_date".into());
    }
    Ok((start_date, end_date))
}

fn cninfo_percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                decoded.push(byte);
                index += 3;
                continue;
            }
        }
        if bytes[index] == b'+' {
            decoded.push(b' ');
        } else {
            decoded.push(bytes[index]);
        }
        index += 1;
    }
    String::from_utf8(decoded).unwrap_or_else(|_| value.to_string())
}

pub(crate) fn cninfo_query_param(link: &str, key: &str) -> Option<String> {
    let query = link.split_once('?')?.1;
    for pair in query.split('&') {
        let (param_key, param_value) = pair.split_once('=').unwrap_or((pair, ""));
        if param_key == key && !param_value.trim().is_empty() {
            return Some(cninfo_percent_decode(param_value.trim()));
        }
    }
    None
}

pub(crate) fn cninfo_announcement_id_from_path(link: &str) -> Option<String> {
    let path = link.split('?').next().unwrap_or(link);
    let file_name = path.rsplit('/').next()?.trim();
    let id = file_name
        .strip_suffix(".PDF")
        .or_else(|| file_name.strip_suffix(".pdf"))
        .unwrap_or(file_name)
        .trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

pub(crate) fn exchange_announcement_order_capacity_detail_links(
    links: &[String],
    limit: Option<usize>,
) -> Vec<String> {
    let max_links = limit.unwrap_or(5).clamp(1, 20);
    let mut seen = BTreeSet::new();
    links
        .iter()
        .map(|link| link.trim().to_string())
        .filter(|link| !link.is_empty())
        .filter(|link| seen.insert(link.clone()))
        .take(max_links)
        .collect()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExchangeAnnouncementDetailProbeSummary {
    pub(crate) fetched_text_count: usize,
    pub(crate) text_hash_count: usize,
    pub(crate) source_published_at_count: usize,
    pub(crate) incomplete_link_metadata_count: usize,
    pub(crate) pdf_parser_required_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct ExchangeAnnouncementPdfDetailProbeSummary {
    parsed_pdf_count: usize,
    stable_hash_count: usize,
    availability_count: usize,
    timestamp_count: usize,
    next_session_policy_count: usize,
    evidence_span_count: usize,
    incomplete_link_metadata_count: usize,
    scanned_pdf_count: usize,
    runtime_not_configured_count: usize,
}

#[derive(Debug, Clone, Copy)]

struct ExchangeAnnouncementOcrBlockedRowProbeSummary {
    ocr_text_count: usize,
    stable_hash_count: usize,
    availability_count: usize,
    quality_pass_count: usize,
    evidence_span_count: usize,
    runtime_missing_count: usize,
    ocr_error_count: usize,
    no_target_span_count: usize,
    incomplete_raw_link_count: usize,
}

#[derive(Debug, Clone)]

pub(crate) struct ExchangeAnnouncementOrderCapacityValidatedSyncRequest {
    pub(crate) symbols: Vec<String>,
    pub(crate) categories: Vec<String>,
    pub(crate) market: String,
    pub(crate) start: NaiveDate,
    pub(crate) end: NaiveDate,
    pub(crate) calendar_day_count: i64,
    pub(crate) query_count: usize,
    pub(crate) data_version_id: String,
    pub(crate) python: String,
    pub(crate) pdf_python: String,
}

#[derive(Debug, Clone)]

pub(crate) struct ExchangeAnnouncementOrderCapacityTinySlice {
    pub(crate) label: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) calendar_day_count: i64,
}

#[derive(Debug, Clone)]

pub(crate) struct ExchangeAnnouncementOrderCapacityValidatedBoundedSyncRequest {
    pub(crate) symbols: Vec<String>,
    pub(crate) categories: Vec<String>,
    pub(crate) market: String,
    pub(crate) start: NaiveDate,
    pub(crate) end: NaiveDate,
    pub(crate) calendar_day_count: i64,
    pub(crate) batch_mode: String,
    pub(crate) slices: Vec<ExchangeAnnouncementOrderCapacityTinySlice>,
    pub(crate) total_query_units: usize,
    pub(crate) data_version_id: String,
    pub(crate) python: String,
    pub(crate) pdf_python: String,
    pub(crate) stop_on_audit_failure: bool,
}

#[derive(Debug, Clone)]

pub(crate) struct ExchangeAnnouncementOrderCapacityRawRow {
    pub(crate) vendor: String,
    pub(crate) vendor_endpoint: String,
    pub(crate) request_key: String,
    pub(crate) symbol: String,
    pub(crate) symbol_name: Option<String>,
    pub(crate) announcement_id: String,
    pub(crate) org_id: String,
    pub(crate) announcement_category: String,
    pub(crate) announcement_title: String,
    pub(crate) announcement_time: NaiveDate,
    pub(crate) source_published_at: String,
    pub(crate) source_published_at_ts: Option<DateTime<Utc>>,
    pub(crate) source_published_date: Option<NaiveDate>,
    pub(crate) source_published_at_quality: String,
    pub(crate) available_at: NaiveDate,
    pub(crate) announcement_url: String,
    pub(crate) pdf_final_url: Option<String>,
    pub(crate) text_content: Option<String>,
    pub(crate) text_hash: Option<String>,
    pub(crate) timestamp_candidates: Value,
    pub(crate) pdf_metadata_keys: Value,
    pub(crate) raw_payload: Value,
    pub(crate) raw_payload_hash: String,
    pub(crate) parser_used: Option<String>,
    pub(crate) parser_version: Option<String>,
    pub(crate) parser_errors: Value,
    pub(crate) pdf_parse_status: String,
    pub(crate) event_type: Option<String>,
    pub(crate) evidence_spans: Value,
}

pub(crate) fn exchange_announcement_order_capacity_tiny_slices(
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<ExchangeAnnouncementOrderCapacityTinySlice> {
    let mut slices = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let slice_end = (cursor
            + Duration::days(EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS - 1))
        .min(end);
        slices.push(ExchangeAnnouncementOrderCapacityTinySlice {
            label: format!("{}-{}", cursor.format("%Y%m%d"), slice_end.format("%Y%m%d")),
            start_date: cursor,
            end_date: slice_end,
            calendar_day_count: (slice_end - cursor).num_days() + 1,
        });
        cursor = slice_end + Duration::days(1);
    }
    slices
}

pub(crate) fn exchange_announcement_order_capacity_validate_bounded_window(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
) -> Result<(), String> {
    match batch_mode {
        "month" => {
            if start.year() != end.year() || start.month() != end.month() {
                return Err(
                    "exchange announcement bounded sync month batch must cover a single month"
                        .to_string(),
                );
            }
        }
        "quarter" => {
            if start.year() != end.year() || quarter_index(start) != quarter_index(end) {
                return Err(
                    "exchange announcement bounded sync quarter batch must cover a single quarter"
                        .to_string(),
                );
            }
        }
        _ => {
            return Err(
                "exchange announcement bounded sync batch must be 'month' or 'quarter'".to_string(),
            );
        }
    }
    Ok(())
}

/// 仅解 %XX 转义、不动 `+`（时间戳解析专用，2026-09-21 修复）。
/// 表单编码版 decode（cninfo_percent_decode）把 `+` 转空格——查询参数上下文
/// 正确，但喂给 RFC3339 解析会破坏正时区 offset（`+08:00` → ` 08:00`），
/// 导致带正 offset 的时间戳从未解析成功（中国主场景全部命中此 bug，
/// 由覆盖率第四批测试暴露）。
fn cninfo_percent_decode_strict(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                decoded.push(byte);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(decoded).unwrap_or_else(|_| value.to_string())
}

pub(crate) fn parse_exchange_announcement_date(value: &str) -> Option<NaiveDate> {
    let decoded = cninfo_percent_decode_strict(value);
    let trimmed = decoded.trim();
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y/%m/%d"))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc3339(trimmed)
                .ok()
                .map(|value| value.date_naive())
        })
        .or_else(|| parse_exchange_announcement_local_datetime(trimmed).map(|value| value.date()))
}

fn parse_exchange_announcement_local_datetime(value: &str) -> Option<NaiveDateTime> {
    let normalized = value.trim().replace('T', " ");
    [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
    ]
    .iter()
    .find_map(|format| NaiveDateTime::parse_from_str(&normalized, format).ok())
}

pub(crate) fn parse_exchange_announcement_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let decoded = cninfo_percent_decode_strict(value);
    let trimmed = decoded.trim();
    DateTime::parse_from_rfc3339(trimmed)
        .ok()
        .map(|value| value.with_timezone(&Utc))
        .or_else(|| {
            parse_exchange_announcement_local_datetime(trimmed)
                .map(|value| value.and_utc() - Duration::hours(8))
        })
}

async fn load_exchange_announcement_next_open_dates(
    db: &sqlx::PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND trade_date > $1
          AND trade_date <= $2
        ORDER BY trade_date
        "#,
    )
    .bind(start)
    .bind(end + Duration::days(14))
    .fetch_all(db)
    .await
}

pub(crate) fn exchange_announcement_next_open_date(
    announcement_time: NaiveDate,
    open_dates: &[NaiveDate],
) -> NaiveDate {
    open_dates
        .iter()
        .copied()
        .find(|date| *date > announcement_time)
        .unwrap_or_else(|| announcement_time + Duration::days(1))
}

pub(crate) fn exchange_announcement_event_type_from_spans(spans: &Value) -> Option<String> {
    let spans = spans.as_array()?;
    let mut has_order = false;
    let mut has_capacity = false;
    let mut has_price = false;
    for span in spans {
        match span.get("theme").and_then(Value::as_str) {
            Some("order_contract") => has_order = true,
            Some("capacity") => has_capacity = true,
            Some("price") => has_price = true,
            _ => {}
        }
    }
    if has_order {
        Some("order_or_contract_signed".to_string())
    } else if has_capacity {
        Some("capacity_expansion_or_commissioning".to_string())
    } else if has_price {
        Some("product_price_adjustment".to_string())
    } else {
        None
    }
}

pub(crate) fn exchange_announcement_pdf_parse_status(probe: &Value) -> String {
    match probe.get("status").and_then(Value::as_str) {
        Some("ok") => "ok",
        Some("scanned_pdf_ocr_required") => "scanned_pdf_ocr_required",
        Some("pdf_parse_empty") => "pdf_parse_empty",
        Some("not_pdf_after_redirect") => "error",
        Some("error" | "timeout" | "runtime_not_configured" | "incomplete_link_metadata") => {
            "error"
        }
        _ => "pending_manual_review",
    }
    .to_string()
}

pub(crate) fn exchange_announcement_order_capacity_json_path_i64(
    value: &Value,
    path: &[&str],
) -> i64 {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

pub(crate) fn exchange_announcement_order_capacity_json_path_f64(
    value: &Value,
    path: &[&str],
) -> Option<f64> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_f64)
}

pub(crate) fn exchange_announcement_order_capacity_array_len(value: &Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

async fn upsert_exchange_announcement_order_capacity_raw_rows(
    db: &sqlx::PgPool,
    rows: &[ExchangeAnnouncementOrderCapacityRawRow],
    data_version_id: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut saved = 0usize;
    for chunk in rows.chunks(500) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_exchange_announcement_text_raw \
             (vendor, vendor_endpoint, request_key, symbol, symbol_name, announcement_id, org_id, \
              announcement_category, announcement_title, announcement_time, source_published_at, \
              source_published_at_ts, source_published_date, source_published_at_quality, available_at, \
              announcement_url, pdf_final_url, text_content, text_hash, text_hash_algorithm, \
              timestamp_candidates, pdf_metadata_keys, raw_payload, raw_payload_hash, parser_used, \
              parser_version, parser_errors, pdf_parse_status, event_type, evidence_spans, data_version_id) ",
        );

        builder.push_values(chunk, |mut row_builder, row| {
            row_builder
                .push_bind(&row.vendor)
                .push_bind(&row.vendor_endpoint)
                .push_bind(&row.request_key)
                .push_bind(&row.symbol)
                .push_bind(&row.symbol_name)
                .push_bind(&row.announcement_id)
                .push_bind(&row.org_id)
                .push_bind(&row.announcement_category)
                .push_bind(&row.announcement_title)
                .push_bind(row.announcement_time)
                .push_bind(&row.source_published_at)
                .push_bind(row.source_published_at_ts)
                .push_bind(row.source_published_date)
                .push_bind(&row.source_published_at_quality)
                .push_bind(row.available_at)
                .push_bind(&row.announcement_url)
                .push_bind(&row.pdf_final_url)
                .push_bind(&row.text_content)
                .push_bind(&row.text_hash)
                .push_bind("sha256")
                .push_bind(&row.timestamp_candidates)
                .push_bind(&row.pdf_metadata_keys)
                .push_bind(&row.raw_payload)
                .push_bind(&row.raw_payload_hash)
                .push_bind(&row.parser_used)
                .push_bind(&row.parser_version)
                .push_bind(&row.parser_errors)
                .push_bind(&row.pdf_parse_status)
                .push_bind(&row.event_type)
                .push_bind(&row.evidence_spans)
                .push_bind(data_version_id);
        });

        builder.push(
            " ON CONFLICT (vendor, vendor_endpoint, announcement_id, symbol, raw_payload_hash) \
              DO UPDATE SET \
                request_key = EXCLUDED.request_key, \
                symbol_name = EXCLUDED.symbol_name, \
                org_id = EXCLUDED.org_id, \
                announcement_category = EXCLUDED.announcement_category, \
                announcement_title = EXCLUDED.announcement_title, \
                announcement_time = EXCLUDED.announcement_time, \
                source_published_at = EXCLUDED.source_published_at, \
                source_published_at_ts = EXCLUDED.source_published_at_ts, \
                source_published_date = EXCLUDED.source_published_date, \
                source_published_at_quality = EXCLUDED.source_published_at_quality, \
                available_at = EXCLUDED.available_at, \
                announcement_url = EXCLUDED.announcement_url, \
                pdf_final_url = EXCLUDED.pdf_final_url, \
                text_content = EXCLUDED.text_content, \
                text_hash = EXCLUDED.text_hash, \
                timestamp_candidates = EXCLUDED.timestamp_candidates, \
                pdf_metadata_keys = EXCLUDED.pdf_metadata_keys, \
                raw_payload = EXCLUDED.raw_payload, \
                parser_used = EXCLUDED.parser_used, \
                parser_version = EXCLUDED.parser_version, \
                parser_errors = EXCLUDED.parser_errors, \
                pdf_parse_status = EXCLUDED.pdf_parse_status, \
                event_type = EXCLUDED.event_type, \
                evidence_spans = EXCLUDED.evidence_spans, \
                data_version_id = EXCLUDED.data_version_id, \
                updated_at = now()",
        );

        let result = builder.build().execute(db).await?;
        saved += result.rows_affected() as usize;
    }

    Ok(saved)
}

async fn run_exchange_announcement_order_capacity_tiny_sync(
    state: &AppState,
    req: ExchangeAnnouncementOrderCapacitySyncReq,
) -> Result<Value, String> {
    let validated = validate_exchange_announcement_order_capacity_sync_request(&req)?;
    let table_exists = table_exists(&state.db, "market_exchange_announcement_text_raw").await?;
    if !table_exists {
        return Err(
            "market_exchange_announcement_text_raw does not exist; apply sql/phase7_exchange_announcement_order_capacity_source.sql before tiny sync"
                .to_string(),
        );
    }

    let task_id = bounded_phase7_task_id(&[validated.data_version_id.as_str(), "raw-sync"]);
    let sync_req = req.clone().into_sync_task_req();
    register_sync_task(state, &task_id, &sync_req, "running").await?;
    quant_data::repository::create_data_version(
        &state.db,
        &validated.data_version_id,
        "Exchange announcement order/capacity raw text sync",
        EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_TASK_SOURCE,
        &["market_exchange_announcement_text_raw"],
        validated.start,
        validated.end,
    )
    .await
    .map_err(|error| {
        format!(
            "Failed to create exchange announcement data_version {}: {error}",
            validated.data_version_id
        )
    })?;

    let total_units = validated.query_count as i32;
    quant_data::repository::heartbeat_sync_task(&state.db, &task_id, total_units, 0, 0, 0)
        .await
        .map_err(|error| format!("Failed to heartbeat exchange announcement task: {error}"))?;
    let open_dates =
        load_exchange_announcement_next_open_dates(&state.db, validated.start, validated.end)
            .await
            .map_err(|error| {
                format!("Failed to load next open dates for exchange announcement sync: {error}")
            })?;

    let start_key = validated.start.format("%Y%m%d").to_string();
    let end_key = validated.end.format("%Y%m%d").to_string();
    let mut completed_units = 0i32;
    let mut failed_units = 0i32;
    let mut fetched_rows = 0i64;
    let mut mapped_rows = 0i64;
    let mut upserted_rows = 0i64;
    let mut pdf_ok_rows = 0i64;
    let mut evidence_span_rows = 0i64;
    let mut malformed_rows = 0i64;
    let mut per_query = Vec::new();

    for symbol in &validated.symbols {
        for category in &validated.categories {
            let attempt_symbol = format!("{symbol}:{category}");
            let probe = run_exchange_announcement_order_capacity_probe(
                &validated.python,
                symbol,
                &validated.market,
                category,
                &start_key,
                &end_key,
                exchange_announcement_order_capacity_raw_sync_row_limit(),
            )
            .await;
            let status = probe
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("error");
            let row_count = probe.get("row_count").and_then(Value::as_i64).unwrap_or(0);
            fetched_rows += row_count;

            if status == "ok_empty" || (status == "ok" && row_count == 0) {
                completed_units += 1;
                quant_data::repository::upsert_sync_attempt(
                    &state.db,
                    EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE,
                    &attempt_symbol,
                    validated.start,
                    validated.end,
                    &task_id,
                    "completed",
                    0,
                    None,
                )
                .await
                .map_err(|error| {
                    format!("Failed to record exchange announcement empty sync attempt: {error}")
                })?;
                per_query.push(json!({
                    "symbol": symbol,
                    "category": category,
                    "status": "completed_empty",
                    "fetched_rows": row_count,
                    "mapped_rows": 0,
                    "upserted_rows": 0,
                }));
            } else if status == "ok" {
                let sample_rows = probe
                    .get("sample_rows")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if exchange_announcement_order_capacity_probe_is_truncated(&probe) {
                    failed_units += 1;
                    let message = format!(
                        "exchange_announcement_truncated_source_rows:row_count={row_count}:sample_rows={}:narrow_date_range_or_raise_reviewed_row_limit",
                        sample_rows.len()
                    );
                    quant_data::repository::upsert_sync_attempt(
                        &state.db,
                        EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE,
                        &attempt_symbol,
                        validated.start,
                        validated.end,
                        &task_id,
                        "failed",
                        0,
                        Some(&message),
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "Failed to record exchange announcement truncated sync attempt: {error}"
                        )
                    })?;
                    per_query.push(json!({
                        "symbol": symbol,
                        "category": category,
                        "status": "failed_truncated_source_rows",
                        "fetched_rows": row_count,
                        "mapped_rows": 0,
                        "upserted_rows": 0,
                        "sample_rows": sample_rows.len(),
                        "raw_sync_row_landing_cap": exchange_announcement_order_capacity_raw_sync_row_limit(),
                        "error": message,
                    }));
                    let finished_units = completed_units + failed_units;
                    let progress =
                        ((finished_units as f64 / total_units as f64) * 100.0).round() as i32;
                    quant_data::repository::heartbeat_sync_task(
                        &state.db,
                        &task_id,
                        total_units,
                        completed_units,
                        failed_units,
                        progress,
                    )
                    .await
                    .map_err(|error| {
                        format!("Failed to heartbeat exchange announcement sync progress: {error}")
                    })?;
                    continue;
                }
                let mut rows = Vec::new();
                let mut errors = Vec::new();
                for list_row in sample_rows {
                    let Some(link) = list_row.get("公告链接").and_then(Value::as_str) else {
                        errors.push("missing_announcement_link".to_string());
                        continue;
                    };
                    let pdf_probe =
                        run_exchange_announcement_pdf_detail_probe(&validated.pdf_python, link)
                            .await;
                    if pdf_probe.get("status").and_then(Value::as_str) == Some("ok") {
                        pdf_ok_rows += 1;
                    }
                    if pdf_probe
                        .get("evidence_spans")
                        .and_then(Value::as_array)
                        .map(|spans| !spans.is_empty())
                        .unwrap_or(false)
                    {
                        evidence_span_rows += 1;
                    }
                    match exchange_announcement_raw_row_from_list_and_pdf_probe(
                        &list_row,
                        category,
                        &format!("{}:{}:{}:{}", symbol, category, start_key, end_key),
                        &pdf_probe,
                        &open_dates,
                        &validated.data_version_id,
                    ) {
                        Ok(row) => rows.push(row),
                        Err(error) => errors.push(error),
                    }
                }

                if !errors.is_empty() {
                    failed_units += 1;
                    malformed_rows += errors.len() as i64;
                    let message = format!(
                        "exchange_announcement_malformed_rows:{}:{}",
                        errors.len(),
                        errors.iter().take(5).cloned().collect::<Vec<_>>().join("|")
                    );
                    quant_data::repository::upsert_sync_attempt(
                        &state.db,
                        EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE,
                        &attempt_symbol,
                        validated.start,
                        validated.end,
                        &task_id,
                        "failed",
                        0,
                        Some(&message),
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "Failed to record exchange announcement malformed sync attempt: {error}"
                        )
                    })?;
                    per_query.push(json!({
                        "symbol": symbol,
                        "category": category,
                        "status": "failed_malformed_rows",
                        "fetched_rows": row_count,
                        "mapped_rows": rows.len(),
                        "upserted_rows": 0,
                        "errors": errors.into_iter().take(10).collect::<Vec<_>>(),
                    }));
                } else {
                    let upserted = upsert_exchange_announcement_order_capacity_raw_rows(
                        &state.db,
                        &rows,
                        &validated.data_version_id,
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "Failed to upsert exchange announcement raw rows for {attempt_symbol}: {error}"
                        )
                    })?;
                    completed_units += 1;
                    mapped_rows += rows.len() as i64;
                    upserted_rows += upserted as i64;
                    quant_data::repository::upsert_sync_attempt(
                        &state.db,
                        EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE,
                        &attempt_symbol,
                        validated.start,
                        validated.end,
                        &task_id,
                        "completed",
                        rows.len() as i64,
                        None,
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "Failed to record exchange announcement completed sync attempt: {error}"
                        )
                    })?;
                    per_query.push(json!({
                        "symbol": symbol,
                        "category": category,
                        "status": "completed",
                        "fetched_rows": row_count,
                        "mapped_rows": rows.len(),
                        "upserted_rows": upserted,
                    }));
                }
            } else {
                failed_units += 1;
                let message = probe
                    .get("error")
                    .and_then(Value::as_str)
                    .or_else(|| probe.get("error_type").and_then(Value::as_str))
                    .unwrap_or(status)
                    .to_string();
                quant_data::repository::upsert_sync_attempt(
                    &state.db,
                    EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE,
                    &attempt_symbol,
                    validated.start,
                    validated.end,
                    &task_id,
                    "failed",
                    0,
                    Some(&message),
                )
                .await
                .map_err(|error| {
                    format!("Failed to record exchange announcement failed sync attempt: {error}")
                })?;
                per_query.push(json!({
                    "symbol": symbol,
                    "category": category,
                    "status": status,
                    "fetched_rows": row_count,
                    "mapped_rows": 0,
                    "upserted_rows": 0,
                    "error": message,
                }));
            }

            let finished_units = completed_units + failed_units;
            let progress = ((finished_units as f64 / total_units as f64) * 100.0).round() as i32;
            quant_data::repository::heartbeat_sync_task(
                &state.db,
                &task_id,
                total_units,
                completed_units,
                failed_units,
                progress,
            )
            .await
            .map_err(|error| {
                format!("Failed to heartbeat exchange announcement sync progress: {error}")
            })?;
        }
    }

    let final_status = if failed_units > 0 {
        "partial"
    } else {
        "completed"
    };
    quant_data::repository::update_sync_task(
        &state.db,
        &task_id,
        final_status,
        total_units,
        completed_units,
        failed_units,
    )
    .await
    .map_err(|error| format!("Failed to finalize exchange announcement sync task: {error}"))?;

    Ok(json!({
        "audit_version": "p3.24i-exchange-announcement-order-capacity-tiny-raw-sync-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24I",
        "mode": "tiny_calendar_day_symbol_category_raw_sync",
        "write_enabled": true,
        "task_id": task_id,
        "data_version_id": validated.data_version_id,
        "date_range": {
            "start_date": validated.start.format("%Y%m%d").to_string(),
            "end_date": validated.end.format("%Y%m%d").to_string(),
            "calendar_day_count": validated.calendar_day_count,
        },
        "symbols": validated.symbols,
        "categories": validated.categories,
        "market": validated.market,
        "summary": {
            "query_count": total_units,
            "permission_smoke_sample_cap": EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS,
            "raw_sync_row_landing_cap": exchange_announcement_order_capacity_raw_sync_row_limit(),
            "completed_units": completed_units,
            "failed_units": failed_units,
            "fetched_rows": fetched_rows,
            "mapped_rows": mapped_rows,
            "upserted_rows": upserted_rows,
            "pdf_ok_rows": pdf_ok_rows,
            "evidence_span_rows": evidence_span_rows,
            "malformed_rows": malformed_rows,
        },
        "per_query": per_query,
        "promotion_gate": {
            "coverage_quality_audit": "required_next",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_coverage_quality_audit_for_the_same_tiny_window_before_any_expansion"
    }))
}

async fn run_exchange_announcement_order_capacity_bounded_sync(
    state: &AppState,
    req: ExchangeAnnouncementOrderCapacityBoundedSyncReq,
) -> Result<Value, String> {
    let validated = validate_exchange_announcement_order_capacity_bounded_sync_request(&req)?;
    let table_exists = table_exists(&state.db, "market_exchange_announcement_text_raw").await?;
    if !table_exists {
        return Err(
            "market_exchange_announcement_text_raw does not exist; apply sql/phase7_exchange_announcement_order_capacity_source.sql before bounded sync"
                .to_string(),
        );
    }

    let mut completed_slices = 0usize;
    let mut failed_slices = 0usize;
    let mut stopped_by_audit = false;
    let mut slice_results = Vec::new();
    let mut aggregate_fetched_rows = 0i64;
    let mut aggregate_mapped_rows = 0i64;
    let mut aggregate_upserted_rows = 0i64;
    let mut aggregate_pdf_ok_rows = 0i64;
    let mut aggregate_evidence_span_rows = 0i64;
    let mut aggregate_failed_units = 0i64;

    for slice in &validated.slices {
        let slice_version_id =
            bounded_phase7_task_id(&[validated.data_version_id.as_str(), slice.label.as_str()]);
        let tiny_req = ExchangeAnnouncementOrderCapacitySyncReq {
            symbols: Some(validated.symbols.join(",")),
            categories: Some(validated.categories.join(",")),
            market: Some(validated.market.clone()),
            start_date: Some(slice.start_date.format("%Y%m%d").to_string()),
            end_date: Some(slice.end_date.format("%Y%m%d").to_string()),
            data_version_id: Some(slice_version_id.clone()),
            python: Some(validated.python.clone()),
            pdf_python: Some(validated.pdf_python.clone()),
            background: false,
        };
        let sync_result =
            run_exchange_announcement_order_capacity_tiny_sync(state, tiny_req).await?;
        let summary = sync_result
            .get("summary")
            .cloned()
            .unwrap_or_else(|| json!({}));
        aggregate_fetched_rows += summary
            .get("fetched_rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        aggregate_mapped_rows += summary
            .get("mapped_rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        aggregate_upserted_rows += summary
            .get("upserted_rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        aggregate_pdf_ok_rows += summary
            .get("pdf_ok_rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        aggregate_evidence_span_rows += summary
            .get("evidence_span_rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let failed_units = summary
            .get("failed_units")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        aggregate_failed_units += failed_units;

        let audit_req = ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
            start_date: Some(slice.start_date.format("%Y%m%d").to_string()),
            end_date: Some(slice.end_date.format("%Y%m%d").to_string()),
        };
        let audit_result =
            build_exchange_announcement_order_capacity_coverage_quality_audit(state, audit_req)
                .await?;
        let audit_decision = audit_result
            .get("decision")
            .and_then(|decision| decision.get("admission_decision"))
            .and_then(Value::as_str)
            .unwrap_or("missing_audit_decision")
            .to_string();
        let audit_passed_for_expansion =
            audit_decision == "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only";
        let audit_passed_for_coverage = audit_passed_for_expansion
            || audit_decision == "synced_empty_no_event_rows_passed_for_coverage_accounting_only"
            || audit_decision
                == "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only";
        if failed_units == 0 && audit_passed_for_coverage {
            completed_slices += 1;
        } else {
            failed_slices += 1;
        }

        slice_results.push(json!({
            "slice": slice.label,
            "start_date": slice.start_date.format("%Y%m%d").to_string(),
            "end_date": slice.end_date.format("%Y%m%d").to_string(),
            "calendar_day_count": slice.calendar_day_count,
            "data_version_id": slice_version_id,
            "sync_summary": summary,
            "audit_decision": audit_decision,
            "audit_status": audit_result
                .get("decision")
                .and_then(|decision| decision.get("status"))
                .cloned()
                .unwrap_or(Value::Null),
            "audit_passed_for_coverage": audit_passed_for_coverage,
            "audit_passed_for_expansion": audit_passed_for_expansion,
        }));

        if validated.stop_on_audit_failure && (failed_units > 0 || !audit_passed_for_coverage) {
            stopped_by_audit = true;
            break;
        }
    }

    Ok(json!({
        "audit_version": "p3.24j-exchange-announcement-order-capacity-bounded-raw-sync-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24J",
        "mode": "bounded_month_or_quarter_sync_composed_of_tiny_3day_slices",
        "write_enabled": true,
        "data_version_id": validated.data_version_id,
        "batch_mode": validated.batch_mode,
        "date_range": {
            "start_date": validated.start.format("%Y%m%d").to_string(),
            "end_date": validated.end.format("%Y%m%d").to_string(),
            "calendar_day_count": validated.calendar_day_count,
        },
        "symbols": validated.symbols,
        "categories": validated.categories,
        "market": validated.market,
        "slice_policy": {
            "max_calendar_days_per_internal_sync": EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS,
            "total_slices": validated.slices.len(),
            "total_query_units": validated.total_query_units,
            "permission_smoke_sample_cap": EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS,
            "raw_sync_row_landing_cap": exchange_announcement_order_capacity_raw_sync_row_limit(),
            "stop_on_audit_failure": validated.stop_on_audit_failure,
            "truncated_source_rows": "blocked_and_recorded_as_failed_attempt_no_partial_raw_landing"
        },
        "summary": {
            "completed_slices": completed_slices,
            "failed_slices": failed_slices,
            "stopped_by_audit": stopped_by_audit,
            "fetched_rows": aggregate_fetched_rows,
            "mapped_rows": aggregate_mapped_rows,
            "upserted_rows": aggregate_upserted_rows,
            "pdf_ok_rows": aggregate_pdf_ok_rows,
            "evidence_span_rows": aggregate_evidence_span_rows,
            "failed_units": aggregate_failed_units,
        },
        "slices": slice_results,
        "promotion_gate": {
            "coverage_quality_audit": "required_after_each_slice_and_after_batch",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if stopped_by_audit {
            "repair_failed_slice_or_reduce_symbol_category_date_scope_then_rerun"
        } else {
            "rerun_coverage_quality_audit_for_the_full_batch_then_expand_next_month_or_quarter"
        }
    }))
}

async fn build_exchange_announcement_order_capacity_coverage_quality_audit(
    state: &AppState,
    req: ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq,
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
        return Err("start_date must be <= end_date".to_string());
    }

    let table_exists = table_exists(&state.db, "market_exchange_announcement_text_raw").await?;
    let mut row_count = 0i64;
    let mut distinct_symbol_count = 0i64;
    let mut distinct_category_count = 0i64;
    let mut distinct_available_at_count = 0i64;
    let mut pit_violation_rows = 0i64;
    let mut missing_available_at_rows = 0i64;
    let mut missing_source_published_at_quality_rows = 0i64;
    let mut duplicate_announcement_id_rows = 0i64;
    let mut duplicate_raw_payload_hash_groups = 0i64;
    let mut evidence_span_rows = 0i64;
    let mut target_event_rows = 0i64;
    let mut target_event_missing_evidence_span_rows = 0i64;
    let mut target_event_with_evidence_span_rows = 0i64;
    let mut scanned_pdf_ocr_required_rows = 0i64;
    let mut ocr_taxonomy_excluded_rows = 0i64;
    let mut trainable_scanned_pdf_blocking_rows = 0i64;
    let mut taxonomy_blocked_target_event_rows = 0i64;
    let mut taxonomy_risk_category_rows = 0i64;
    let mut completed_attempts = 0i64;
    let mut completed_empty_attempts = 0i64;
    let mut failed_attempts = 0i64;
    let mut total_failed_attempts = 0i64;
    let mut excluded_unsupported_category_failed_attempts = 0i64;
    let mut attempt_row_count = 0i64;
    let mut source_published_at_quality_distribution = Vec::new();
    let mut parser_status_distribution = Vec::new();
    let mut category_breakdown = Vec::new();
    let mut event_type_distribution = Vec::new();
    let mut year_category_breakdown = Vec::new();
    let mut symbol_event_breakdown = Vec::new();

    if table_exists {
        let summary =
            sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64)>(
            r#"
            WITH scoped AS (
                SELECT *,
                       (
                           pdf_parse_status = 'scanned_pdf_ocr_required'
                           AND (
                               announcement_title LIKE '%控股股东及其他关联方资金占用%'
                               OR announcement_title LIKE '%非经营性资金占用%'
                               OR announcement_title LIKE '%关联资金往来情况汇总表%'
                               OR announcement_title LIKE '%财务公司关联交易%'
                               OR announcement_title LIKE '%存款、贷款等金融业务%'
                               OR announcement_title LIKE '%金融业务的专项说明%'
                               OR (
                                   announcement_title LIKE '%专项说明%'
                                   AND (
                                       announcement_title LIKE '%审计%'
                                       OR announcement_title LIKE '%资金占用%'
                                       OR announcement_title LIKE '%关联方%'
                                       OR announcement_title LIKE '%关联交易%'
                                       OR announcement_title LIKE '%财务公司%'
                                   )
                               )
                               OR (
                                   announcement_title LIKE '%募集资金%'
                                   AND (
                                       announcement_title LIKE '%存放与使用%'
                                       OR announcement_title LIKE '%存放和使用%'
                                   )
                                   AND (
                                       announcement_title LIKE '%鉴证报告%'
                                       OR announcement_title LIKE '%专项核查报告%'
                                       OR announcement_title LIKE '%专项报告%'
                                   )
                               )
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%法律意见书%'
                               OR announcement_title LIKE '%财务顾问报告%'
                           )
                       ) AS is_ocr_taxonomy_excluded,
                       (
                           NOT (
                               announcement_title LIKE '%签署《关于进一步加强和深化合作的协议》%'
                               OR announcement_title LIKE '%合资建厂%'
                               OR announcement_title LIKE '%签订日常经营重大合同%'
                               OR announcement_title LIKE '%投资建设高效电池产能%'
                               OR announcement_title LIKE '%投资建设产能项目%'
                           )
                           AND (
                               announcement_title LIKE '%计提减值准备%'
                               OR announcement_title LIKE '%募投项目%'
                               OR announcement_title LIKE '%募集资金%'
                               OR announcement_title LIKE '%关联交易%'
                               OR announcement_title LIKE '%授信额度%'
                               OR announcement_title LIKE '%注册资本%'
                               OR announcement_title LIKE '%工商变更%'
                               OR announcement_title LIKE '%实际控制人%'
                               OR announcement_title LIKE '%控制权%'
                               OR announcement_title LIKE '%股份质押%'
                               OR announcement_title LIKE '%财务报告%'
                               OR announcement_title LIKE '%年度报告%'
                               OR announcement_title LIKE '%半年度报告%'
                               OR announcement_title LIKE '%季度报告%'
                               OR announcement_title LIKE '%主要经营数据%'
                               OR announcement_title LIKE '%股权投资基金%'
                               OR announcement_title LIKE '%投资基金%'
                               OR announcement_title LIKE '%风险评估报告%'
                               OR announcement_title LIKE '%H股发行%'
                               OR announcement_title LIKE '%发行H股%'
                               OR announcement_title LIKE '%H股股票%'
                               OR announcement_title LIKE '%上市审计机构%'
                               OR announcement_title LIKE '%章程%'
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%审计机构%'
                               OR announcement_title LIKE '%资产减值%'
                               OR announcement_title LIKE '%公募REITs%'
                               OR announcement_title LIKE '%REITs%'
                               OR announcement_title LIKE '%收购控股子公司%'
                               OR announcement_title LIKE '%董事会工作报告%'
                               OR announcement_title LIKE '%监事会工作报告%'
                               OR announcement_title LIKE '%内部控制%'
                               OR announcement_title LIKE '%会计师事务所%'
                               OR announcement_title LIKE '%审计委员会%'
                               OR announcement_title LIKE '%委托理财%'
                               OR announcement_title LIKE '%套期保值%'
                               OR announcement_title LIKE '%担保额度%'
                               OR announcement_title LIKE '%担保的进展%'
                               OR announcement_title LIKE '%提供担保%'
                               OR announcement_title LIKE '%发行债券%'
                               OR announcement_title LIKE '%公司章程%'
                               OR announcement_title LIKE '%公司制度%'
                               OR announcement_title LIKE '%制定及修订%'
                               OR announcement_title LIKE '%独立董事%'
                               OR announcement_title LIKE '%会计政策变更%'
                               OR announcement_title LIKE '%社会责任报告%'
                               OR announcement_title LIKE '%可持续发展报告%'
                               OR announcement_title LIKE '%可持续发展%'
                               OR announcement_title LIKE '%环境、社会及治理%'
                               OR announcement_title LIKE '%ESG%'
                               OR announcement_title LIKE '%估值提升计划%'
                               OR announcement_title LIKE '%市值管理%'
                               OR announcement_title LIKE '%质量回报双提升%'
                               OR announcement_title LIKE '%履职情况%'
                               OR announcement_title LIKE '%履行监督职责%'
                           )
                       ) AS is_taxonomy_risk_title
                FROM market_exchange_announcement_text_raw
                WHERE announcement_time >= $1 AND announcement_time <= $2
            )
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(DISTINCT symbol)::bigint AS distinct_symbol_count,
                   COUNT(DISTINCT announcement_category)::bigint AS distinct_category_count,
                   COUNT(DISTINCT available_at)::bigint AS distinct_available_at_count,
                   COUNT(*) FILTER (
                       WHERE available_at < announcement_time
                          OR (source_published_at_quality = 'date_only_next_session' AND available_at <= announcement_time)
                   )::bigint AS pit_violation_rows,
                   COUNT(*) FILTER (WHERE available_at IS NULL)::bigint AS missing_available_at_rows,
                   COUNT(*) FILTER (WHERE source_published_at_quality IS NULL OR source_published_at_quality = 'missing')::bigint AS missing_source_published_at_quality_rows,
                   COUNT(*) FILTER (WHERE jsonb_array_length(evidence_spans) > 0)::bigint AS evidence_span_rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL)::bigint AS target_event_rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL AND jsonb_array_length(evidence_spans) = 0)::bigint AS target_event_missing_evidence_span_rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL AND jsonb_array_length(evidence_spans) > 0)::bigint AS target_event_with_evidence_span_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required')::bigint AS scanned_pdf_ocr_required_rows,
                   COUNT(*) FILTER (WHERE is_ocr_taxonomy_excluded)::bigint AS ocr_taxonomy_excluded_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required' AND NOT is_ocr_taxonomy_excluded)::bigint AS trainable_scanned_pdf_blocking_rows,
                   COUNT(*) FILTER (
                       WHERE event_type IS NOT NULL
                         AND (announcement_category IN ('股权激励') OR is_taxonomy_risk_title)
                   )::bigint AS taxonomy_blocked_target_event_rows,
                   COUNT(*) FILTER (
                       WHERE announcement_category IN ('股权激励') OR is_taxonomy_risk_title
                   )::bigint AS taxonomy_risk_category_rows
            FROM scoped
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize exchange announcement coverage quality: {error}")
        })?;
        row_count = summary.0;
        distinct_symbol_count = summary.1;
        distinct_category_count = summary.2;
        distinct_available_at_count = summary.3;
        pit_violation_rows = summary.4;
        missing_available_at_rows = summary.5;
        missing_source_published_at_quality_rows = summary.6;
        evidence_span_rows = summary.7;
        target_event_rows = summary.8;
        target_event_missing_evidence_span_rows = summary.9;
        target_event_with_evidence_span_rows = summary.10;
        scanned_pdf_ocr_required_rows = summary.11;
        ocr_taxonomy_excluded_rows = summary.12;
        trainable_scanned_pdf_blocking_rows = summary.13;
        taxonomy_blocked_target_event_rows = summary.14;
        taxonomy_risk_category_rows = summary.15;

        duplicate_announcement_id_rows = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(SUM(cnt - 1), 0)::bigint
            FROM (
                SELECT vendor, vendor_endpoint, announcement_id, symbol, COUNT(*)::bigint AS cnt
                FROM market_exchange_announcement_text_raw
                WHERE announcement_time >= $1 AND announcement_time <= $2
                GROUP BY vendor, vendor_endpoint, announcement_id, symbol
                HAVING COUNT(*) > 1
            ) d
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize exchange announcement duplicate IDs: {error}")
        })?;
        duplicate_raw_payload_hash_groups = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::bigint
            FROM (
                SELECT raw_payload_hash, COUNT(*)::bigint AS cnt
                FROM market_exchange_announcement_text_raw
                WHERE announcement_time >= $1 AND announcement_time <= $2
                GROUP BY raw_payload_hash
                HAVING COUNT(*) > 1
            ) d
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize exchange announcement duplicate hashes: {error}")
        })?;
        let attempt_rows: Vec<(String, Option<String>, String, i64)> = sqlx::query_as(
            r#"
            SELECT symbol,
                   error_message,
                   status,
                   row_count
            FROM data_sync_attempt
            WHERE source = $1
              AND start_date >= $2
              AND end_date <= $3
            "#,
        )
        .bind(EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE)
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize exchange announcement sync attempts: {error}")
        })?;
        for (attempt_symbol, error_message, status, row_count) in attempt_rows {
            match status.as_str() {
                "completed" => {
                    completed_attempts += 1;
                    attempt_row_count += row_count;
                    if row_count == 0 {
                        completed_empty_attempts += 1;
                    }
                }
                "failed" => {
                    total_failed_attempts += 1;
                    if exchange_announcement_order_capacity_is_excluded_unsupported_category_attempt(
                        &attempt_symbol,
                        error_message.as_deref().unwrap_or_default(),
                    ) {
                        excluded_unsupported_category_failed_attempts += 1;
                    } else {
                        failed_attempts += 1;
                    }
                }
                _ => {}
            }
        }

        let quality_rows: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT source_published_at_quality, COUNT(*)::bigint
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1 AND announcement_time <= $2
            GROUP BY source_published_at_quality
            ORDER BY source_published_at_quality
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to build source_published_at_quality distribution: {error}")
        })?;
        source_published_at_quality_distribution = quality_rows
            .into_iter()
            .map(|(quality, rows)| json!({"quality": quality, "rows": rows}))
            .collect();

        let parser_rows: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT pdf_parse_status, COUNT(*)::bigint
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1 AND announcement_time <= $2
            GROUP BY pdf_parse_status
            ORDER BY pdf_parse_status
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build parser status distribution: {error}"))?;
        parser_status_distribution = parser_rows
            .into_iter()
            .map(|(status, rows)| json!({"pdf_parse_status": status, "rows": rows}))
            .collect();

        let category_rows: Vec<(String, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(
            r#"
            WITH scoped AS (
                SELECT *,
                       (
                           pdf_parse_status = 'scanned_pdf_ocr_required'
                           AND (
                               announcement_title LIKE '%控股股东及其他关联方资金占用%'
                               OR announcement_title LIKE '%非经营性资金占用%'
                               OR announcement_title LIKE '%关联资金往来情况汇总表%'
                               OR announcement_title LIKE '%财务公司关联交易%'
                               OR announcement_title LIKE '%存款、贷款等金融业务%'
                               OR announcement_title LIKE '%金融业务的专项说明%'
                               OR (
                                   announcement_title LIKE '%专项说明%'
                                   AND (
                                       announcement_title LIKE '%审计%'
                                       OR announcement_title LIKE '%资金占用%'
                                       OR announcement_title LIKE '%关联方%'
                                       OR announcement_title LIKE '%关联交易%'
                                       OR announcement_title LIKE '%财务公司%'
                                   )
                               )
                               OR (
                                   announcement_title LIKE '%募集资金%'
                                   AND (
                                       announcement_title LIKE '%存放与使用%'
                                       OR announcement_title LIKE '%存放和使用%'
                                   )
                                   AND (
                                       announcement_title LIKE '%鉴证报告%'
                                       OR announcement_title LIKE '%专项核查报告%'
                                       OR announcement_title LIKE '%专项报告%'
                                   )
                               )
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%法律意见书%'
                               OR announcement_title LIKE '%财务顾问报告%'
                           )
                       ) AS is_ocr_taxonomy_excluded,
                       (
                           NOT (
                               announcement_title LIKE '%签署《关于进一步加强和深化合作的协议》%'
                               OR announcement_title LIKE '%合资建厂%'
                               OR announcement_title LIKE '%签订日常经营重大合同%'
                               OR announcement_title LIKE '%投资建设高效电池产能%'
                               OR announcement_title LIKE '%投资建设产能项目%'
                           )
                           AND (
                               announcement_title LIKE '%计提减值准备%'
                               OR announcement_title LIKE '%募投项目%'
                               OR announcement_title LIKE '%募集资金%'
                               OR announcement_title LIKE '%关联交易%'
                               OR announcement_title LIKE '%授信额度%'
                               OR announcement_title LIKE '%注册资本%'
                               OR announcement_title LIKE '%工商变更%'
                               OR announcement_title LIKE '%实际控制人%'
                               OR announcement_title LIKE '%控制权%'
                               OR announcement_title LIKE '%股份质押%'
                               OR announcement_title LIKE '%财务报告%'
                               OR announcement_title LIKE '%年度报告%'
                               OR announcement_title LIKE '%半年度报告%'
                               OR announcement_title LIKE '%季度报告%'
                               OR announcement_title LIKE '%主要经营数据%'
                               OR announcement_title LIKE '%股权投资基金%'
                               OR announcement_title LIKE '%投资基金%'
                               OR announcement_title LIKE '%风险评估报告%'
                               OR announcement_title LIKE '%H股发行%'
                               OR announcement_title LIKE '%发行H股%'
                               OR announcement_title LIKE '%H股股票%'
                               OR announcement_title LIKE '%上市审计机构%'
                               OR announcement_title LIKE '%章程%'
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%审计机构%'
                               OR announcement_title LIKE '%资产减值%'
                               OR announcement_title LIKE '%公募REITs%'
                               OR announcement_title LIKE '%REITs%'
                               OR announcement_title LIKE '%收购控股子公司%'
                               OR announcement_title LIKE '%董事会工作报告%'
                               OR announcement_title LIKE '%监事会工作报告%'
                               OR announcement_title LIKE '%内部控制%'
                               OR announcement_title LIKE '%会计师事务所%'
                               OR announcement_title LIKE '%审计委员会%'
                               OR announcement_title LIKE '%委托理财%'
                               OR announcement_title LIKE '%套期保值%'
                               OR announcement_title LIKE '%担保额度%'
                               OR announcement_title LIKE '%担保的进展%'
                               OR announcement_title LIKE '%提供担保%'
                               OR announcement_title LIKE '%发行债券%'
                               OR announcement_title LIKE '%公司章程%'
                               OR announcement_title LIKE '%公司制度%'
                               OR announcement_title LIKE '%制定及修订%'
                               OR announcement_title LIKE '%独立董事%'
                               OR announcement_title LIKE '%会计政策变更%'
                               OR announcement_title LIKE '%社会责任报告%'
                               OR announcement_title LIKE '%可持续发展报告%'
                               OR announcement_title LIKE '%可持续发展%'
                               OR announcement_title LIKE '%环境、社会及治理%'
                               OR announcement_title LIKE '%ESG%'
                               OR announcement_title LIKE '%估值提升计划%'
                               OR announcement_title LIKE '%市值管理%'
                               OR announcement_title LIKE '%质量回报双提升%'
                               OR announcement_title LIKE '%履职情况%'
                               OR announcement_title LIKE '%履行监督职责%'
                           )
                       ) AS is_taxonomy_risk_title
                FROM market_exchange_announcement_text_raw
                WHERE announcement_time >= $1 AND announcement_time <= $2
            )
            SELECT announcement_category,
                   COUNT(*)::bigint AS rows,
                   COUNT(DISTINCT symbol)::bigint AS symbols,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL)::bigint AS target_event_rows,
                   COUNT(*) FILTER (
                       WHERE event_type IS NOT NULL
                         AND (announcement_category IN ('股权激励') OR is_taxonomy_risk_title)
                   )::bigint AS taxonomy_blocked_target_event_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required')::bigint AS scanned_pdf_ocr_required_rows,
                   COUNT(*) FILTER (WHERE is_ocr_taxonomy_excluded)::bigint AS ocr_taxonomy_excluded_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required' AND NOT is_ocr_taxonomy_excluded)::bigint AS trainable_scanned_pdf_blocking_rows
            FROM scoped
            GROUP BY announcement_category
            ORDER BY announcement_category
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build category breakdown: {error}"))?;
        category_breakdown = category_rows
            .into_iter()
            .map(
                |(
                    category,
                    rows,
                    symbols,
                    target_rows,
                    taxonomy_blocked_rows,
                    ocr_rows,
                    ocr_taxonomy_excluded,
                    trainable_ocr_blocking,
                )| {
                json!({
                    "category": category,
                    "rows": rows,
                    "symbols": symbols,
                    "target_event_rows": target_rows,
                    "taxonomy_blocked_target_event_rows": taxonomy_blocked_rows,
                    "scanned_pdf_ocr_required_rows": ocr_rows,
                    "ocr_taxonomy_excluded_rows": ocr_taxonomy_excluded,
                    "trainable_scanned_pdf_blocking_rows": trainable_ocr_blocking,
                    "admissible_target_event_rows": (target_rows - taxonomy_blocked_rows).max(0),
                })
            },
            )
            .collect();

        let event_type_rows: Vec<(String, i64, i64)> = sqlx::query_as(
            r#"
            SELECT COALESCE(event_type, 'non_target_or_unclassified') AS event_type,
                   COUNT(*)::bigint AS rows,
                   COUNT(*) FILTER (WHERE jsonb_array_length(evidence_spans) > 0)::bigint AS evidence_span_rows
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1 AND announcement_time <= $2
            GROUP BY COALESCE(event_type, 'non_target_or_unclassified')
            ORDER BY rows DESC, event_type
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build event type distribution: {error}"))?;
        event_type_distribution = event_type_rows
            .into_iter()
            .map(|(event_type, rows, evidence_rows)| {
                let target_event = event_type != "non_target_or_unclassified";
                json!({
                    "event_type": event_type,
                    "rows": rows,
                    "evidence_span_rows": evidence_rows,
                    "target_event": target_event,
                })
            })
            .collect();

        let year_category_rows: Vec<(i32, String, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(
            r#"
            WITH scoped AS (
                SELECT *,
                       (
                           pdf_parse_status = 'scanned_pdf_ocr_required'
                           AND (
                               announcement_title LIKE '%控股股东及其他关联方资金占用%'
                               OR announcement_title LIKE '%非经营性资金占用%'
                               OR announcement_title LIKE '%关联资金往来情况汇总表%'
                               OR announcement_title LIKE '%财务公司关联交易%'
                               OR announcement_title LIKE '%存款、贷款等金融业务%'
                               OR announcement_title LIKE '%金融业务的专项说明%'
                               OR (
                                   announcement_title LIKE '%专项说明%'
                                   AND (
                                       announcement_title LIKE '%审计%'
                                       OR announcement_title LIKE '%资金占用%'
                                       OR announcement_title LIKE '%关联方%'
                                       OR announcement_title LIKE '%关联交易%'
                                       OR announcement_title LIKE '%财务公司%'
                                   )
                               )
                               OR (
                                   announcement_title LIKE '%募集资金%'
                                   AND (
                                       announcement_title LIKE '%存放与使用%'
                                       OR announcement_title LIKE '%存放和使用%'
                                   )
                                   AND (
                                       announcement_title LIKE '%鉴证报告%'
                                       OR announcement_title LIKE '%专项核查报告%'
                                       OR announcement_title LIKE '%专项报告%'
                                   )
                               )
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%法律意见书%'
                               OR announcement_title LIKE '%财务顾问报告%'
                           )
                       ) AS is_ocr_taxonomy_excluded,
                       (
                           NOT (
                               announcement_title LIKE '%签署《关于进一步加强和深化合作的协议》%'
                               OR announcement_title LIKE '%合资建厂%'
                               OR announcement_title LIKE '%签订日常经营重大合同%'
                               OR announcement_title LIKE '%投资建设高效电池产能%'
                               OR announcement_title LIKE '%投资建设产能项目%'
                           )
                           AND (
                               announcement_title LIKE '%计提减值准备%'
                               OR announcement_title LIKE '%募投项目%'
                               OR announcement_title LIKE '%募集资金%'
                               OR announcement_title LIKE '%关联交易%'
                               OR announcement_title LIKE '%授信额度%'
                               OR announcement_title LIKE '%注册资本%'
                               OR announcement_title LIKE '%工商变更%'
                               OR announcement_title LIKE '%实际控制人%'
                               OR announcement_title LIKE '%控制权%'
                               OR announcement_title LIKE '%股份质押%'
                               OR announcement_title LIKE '%财务报告%'
                               OR announcement_title LIKE '%年度报告%'
                               OR announcement_title LIKE '%半年度报告%'
                               OR announcement_title LIKE '%季度报告%'
                               OR announcement_title LIKE '%主要经营数据%'
                               OR announcement_title LIKE '%股权投资基金%'
                               OR announcement_title LIKE '%投资基金%'
                               OR announcement_title LIKE '%风险评估报告%'
                               OR announcement_title LIKE '%H股发行%'
                               OR announcement_title LIKE '%发行H股%'
                               OR announcement_title LIKE '%H股股票%'
                               OR announcement_title LIKE '%上市审计机构%'
                               OR announcement_title LIKE '%章程%'
                               OR announcement_title LIKE '%审计报告%'
                               OR announcement_title LIKE '%审计机构%'
                               OR announcement_title LIKE '%资产减值%'
                               OR announcement_title LIKE '%公募REITs%'
                               OR announcement_title LIKE '%REITs%'
                               OR announcement_title LIKE '%收购控股子公司%'
                               OR announcement_title LIKE '%董事会工作报告%'
                               OR announcement_title LIKE '%监事会工作报告%'
                               OR announcement_title LIKE '%内部控制%'
                               OR announcement_title LIKE '%会计师事务所%'
                               OR announcement_title LIKE '%审计委员会%'
                               OR announcement_title LIKE '%委托理财%'
                               OR announcement_title LIKE '%套期保值%'
                               OR announcement_title LIKE '%担保额度%'
                               OR announcement_title LIKE '%担保的进展%'
                               OR announcement_title LIKE '%提供担保%'
                               OR announcement_title LIKE '%发行债券%'
                               OR announcement_title LIKE '%公司章程%'
                               OR announcement_title LIKE '%公司制度%'
                               OR announcement_title LIKE '%制定及修订%'
                               OR announcement_title LIKE '%独立董事%'
                               OR announcement_title LIKE '%会计政策变更%'
                               OR announcement_title LIKE '%社会责任报告%'
                               OR announcement_title LIKE '%可持续发展报告%'
                               OR announcement_title LIKE '%可持续发展%'
                               OR announcement_title LIKE '%环境、社会及治理%'
                               OR announcement_title LIKE '%ESG%'
                               OR announcement_title LIKE '%估值提升计划%'
                               OR announcement_title LIKE '%市值管理%'
                               OR announcement_title LIKE '%质量回报双提升%'
                               OR announcement_title LIKE '%履职情况%'
                               OR announcement_title LIKE '%履行监督职责%'
                           )
                       ) AS is_taxonomy_risk_title
                FROM market_exchange_announcement_text_raw
                WHERE announcement_time >= $1 AND announcement_time <= $2
            )
            SELECT EXTRACT(YEAR FROM announcement_time)::int AS year,
                   announcement_category,
                   COUNT(*)::bigint AS rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL)::bigint AS target_event_rows,
                   COUNT(*) FILTER (
                       WHERE event_type IS NOT NULL
                         AND (announcement_category IN ('股权激励') OR is_taxonomy_risk_title)
                   )::bigint AS taxonomy_blocked_target_event_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required')::bigint AS scanned_pdf_ocr_required_rows,
                   COUNT(*) FILTER (WHERE is_ocr_taxonomy_excluded)::bigint AS ocr_taxonomy_excluded_rows,
                   COUNT(*) FILTER (WHERE pdf_parse_status = 'scanned_pdf_ocr_required' AND NOT is_ocr_taxonomy_excluded)::bigint AS trainable_scanned_pdf_blocking_rows,
                   COUNT(DISTINCT symbol)::bigint AS symbols
            FROM scoped
            GROUP BY EXTRACT(YEAR FROM announcement_time)::int, announcement_category
            ORDER BY year, announcement_category
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build year/category breakdown: {error}"))?;
        year_category_breakdown = year_category_rows
            .into_iter()
            .map(|(
                year,
                category,
                rows,
                target_rows,
                taxonomy_blocked_rows,
                ocr_rows,
                ocr_taxonomy_excluded,
                trainable_ocr_blocking,
                symbols,
            )| {
                let admissible_target_rows = (target_rows - taxonomy_blocked_rows).max(0);
                json!({
                    "year": year,
                    "category": category,
                    "rows": rows,
                    "target_event_rows": target_rows,
                    "taxonomy_blocked_target_event_rows": taxonomy_blocked_rows,
                    "scanned_pdf_ocr_required_rows": ocr_rows,
                    "ocr_taxonomy_excluded_rows": ocr_taxonomy_excluded,
                    "trainable_scanned_pdf_blocking_rows": trainable_ocr_blocking,
                    "admissible_target_event_rows": admissible_target_rows,
                    "target_event_yield_ratio": phase7_ratio(target_rows, rows),
                    "admissible_target_event_yield_ratio": phase7_ratio(admissible_target_rows, rows),
                    "symbols": symbols,
                })
            })
            .collect();

        let symbol_rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT symbol,
                   COUNT(*)::bigint AS rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL)::bigint AS target_event_rows,
                   COUNT(*) FILTER (WHERE event_type IS NOT NULL AND jsonb_array_length(evidence_spans) > 0)::bigint AS target_event_with_evidence_span_rows
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1 AND announcement_time <= $2
            GROUP BY symbol
            ORDER BY target_event_rows DESC, rows DESC, symbol
            LIMIT 200
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build symbol event breakdown: {error}"))?;
        symbol_event_breakdown = symbol_rows
            .into_iter()
            .map(|(symbol, rows, target_rows, target_span_rows)| {
                json!({
                    "symbol": symbol,
                    "rows": rows,
                    "target_event_rows": target_rows,
                    "target_event_with_evidence_span_rows": target_span_rows,
                    "target_event_yield_ratio": phase7_ratio(target_rows, rows),
                })
            })
            .collect();
    }

    let decision = decide_exchange_announcement_order_capacity_coverage_quality_audit(
        ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
            table_exists,
            row_count,
            distinct_symbol_count,
            distinct_category_count,
            pit_violation_rows,
            missing_available_at_rows,
            missing_source_published_at_quality_rows,
            duplicate_announcement_id_rows,
            duplicate_raw_payload_hash_groups,
            evidence_span_rows,
            target_event_rows,
            target_event_missing_evidence_span_rows,
            scanned_pdf_ocr_required_rows,
            ocr_taxonomy_excluded_rows,
            trainable_scanned_pdf_blocking_rows,
            taxonomy_blocked_target_event_rows,
            taxonomy_risk_category_rows,
            completed_attempts,
            completed_empty_attempts,
            failed_attempts,
            total_failed_attempts,
            excluded_unsupported_category_failed_attempts,
        },
    );

    Ok(json!({
        "audit_version": "p3.24i-exchange-announcement-order-capacity-coverage-quality-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24I",
        "mode": "read_only_db_backed_small_batch_coverage_pit_quality_audit",
        "table": "market_exchange_announcement_text_raw",
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "table_exists": table_exists,
        "summary": {
            "row_count": row_count,
            "distinct_symbol_count": distinct_symbol_count,
            "distinct_category_count": distinct_category_count,
            "distinct_available_at_count": distinct_available_at_count,
            "pit_violation_rows": pit_violation_rows,
            "missing_available_at_rows": missing_available_at_rows,
            "missing_source_published_at_quality_rows": missing_source_published_at_quality_rows,
            "duplicate_announcement_id_rows": duplicate_announcement_id_rows,
            "duplicate_raw_payload_hash_groups": duplicate_raw_payload_hash_groups,
            "evidence_span_rows": evidence_span_rows,
            "target_event_rows": target_event_rows,
            "target_event_missing_evidence_span_rows": target_event_missing_evidence_span_rows,
            "target_event_with_evidence_span_rows": target_event_with_evidence_span_rows,
            "scanned_pdf_ocr_required_rows": scanned_pdf_ocr_required_rows,
            "ocr_taxonomy_excluded_rows": ocr_taxonomy_excluded_rows,
            "trainable_scanned_pdf_blocking_rows": trainable_scanned_pdf_blocking_rows,
            "taxonomy_blocked_target_event_rows": taxonomy_blocked_target_event_rows,
            "taxonomy_risk_category_rows": taxonomy_risk_category_rows,
            "admissible_target_event_rows": (target_event_rows - taxonomy_blocked_target_event_rows).max(0),
            "completed_attempts": completed_attempts,
            "completed_empty_attempts": completed_empty_attempts,
            "failed_attempts": failed_attempts,
            "total_failed_attempts": total_failed_attempts,
            "excluded_unsupported_category_failed_attempts": excluded_unsupported_category_failed_attempts,
            "attempt_row_count": attempt_row_count,
        },
        "target_event_yield": exchange_announcement_order_capacity_target_event_yield_report(
            row_count,
            target_event_rows,
            target_event_with_evidence_span_rows,
            target_event_missing_evidence_span_rows,
            taxonomy_blocked_target_event_rows,
            scanned_pdf_ocr_required_rows,
            ocr_taxonomy_excluded_rows,
            trainable_scanned_pdf_blocking_rows,
        ),
        "source_published_at_quality_distribution": source_published_at_quality_distribution,
        "parser_status_distribution": parser_status_distribution,
        "event_type_distribution": event_type_distribution,
        "category_breakdown": category_breakdown,
        "year_category_breakdown": year_category_breakdown,
        "symbol_event_breakdown": symbol_event_breakdown,
        "decision": decision,
        "promotion_gate": {
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    }))
}

async fn build_exchange_announcement_order_capacity_manual_precision_sample_audit(
    state: &AppState,
    req: ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq,
) -> Result<Value, String> {
    let start_date = req.start_date.unwrap_or_else(|| "20240101".to_string());
    let end_date = req.end_date.unwrap_or_else(|| "20241231".to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date cannot be after end_date".into());
    }
    let required_target_sample_size = req.limit.unwrap_or(50).clamp(1, 200);
    let include_negative_samples = req.include_negative_samples.unwrap_or(true);
    let negative_limit = if include_negative_samples {
        (required_target_sample_size / 10).clamp(1, 20)
    } else {
        0
    };

    let table_exists = table_exists(&state.db, "market_exchange_announcement_text_raw").await?;
    if !table_exists {
        return Ok(json!({
            "audit_version": "p3.24x-exchange-announcement-order-capacity-manual-precision-sample-audit-v1",
            "source_id": "exchange_announcement_order_capacity_text",
            "stage": "P3.24X",
            "status": "blocked_raw_schema_not_applied",
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "promotion_gate": {
                "factor_builder": "blocked",
                "p310_status": "blocked_until_manual_precision_review_passes",
                "bounded_wfa": "blocked",
                "v19_train_selection": "blocked"
            }
        }));
    }

    let admissible_target_event_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM market_exchange_announcement_text_raw
        WHERE announcement_time >= $1
          AND announcement_time <= $2
          AND event_type IS NOT NULL
          AND announcement_category NOT IN ('股权激励')
          AND jsonb_array_length(evidence_spans) > 0
          AND pdf_parse_status <> 'scanned_pdf_ocr_required'
          AND NOT (
              NOT (
                  announcement_title LIKE '%签署《关于进一步加强和深化合作的协议》%'
                  OR announcement_title LIKE '%合资建厂%'
                  OR announcement_title LIKE '%签订日常经营重大合同%'
                  OR announcement_title LIKE '%投资建设高效电池产能%'
                  OR announcement_title LIKE '%投资建设产能项目%'
              )
              AND (
                  announcement_title LIKE '%计提减值准备%'
                  OR announcement_title LIKE '%募投项目%'
                  OR announcement_title LIKE '%募集资金%'
                  OR announcement_title LIKE '%关联交易%'
                  OR announcement_title LIKE '%授信额度%'
                  OR announcement_title LIKE '%注册资本%'
                  OR announcement_title LIKE '%工商变更%'
                  OR announcement_title LIKE '%实际控制人%'
                  OR announcement_title LIKE '%控制权%'
                  OR announcement_title LIKE '%股份质押%'
                  OR announcement_title LIKE '%财务报告%'
                  OR announcement_title LIKE '%年度报告%'
                  OR announcement_title LIKE '%半年度报告%'
                  OR announcement_title LIKE '%季度报告%'
                  OR announcement_title LIKE '%主要经营数据%'
                  OR announcement_title LIKE '%股权投资基金%'
                  OR announcement_title LIKE '%投资基金%'
                  OR announcement_title LIKE '%风险评估报告%'
                  OR announcement_title LIKE '%H股发行%'
                  OR announcement_title LIKE '%发行H股%'
                  OR announcement_title LIKE '%H股股票%'
                  OR announcement_title LIKE '%上市审计机构%'
                  OR announcement_title LIKE '%章程%'
                  OR announcement_title LIKE '%审计报告%'
                  OR announcement_title LIKE '%审计机构%'
                  OR announcement_title LIKE '%资产减值%'
                  OR announcement_title LIKE '%公募REITs%'
                  OR announcement_title LIKE '%REITs%'
                  OR announcement_title LIKE '%收购控股子公司%'
                  OR announcement_title LIKE '%董事会工作报告%'
                  OR announcement_title LIKE '%监事会工作报告%'
                  OR announcement_title LIKE '%内部控制%'
                  OR announcement_title LIKE '%会计师事务所%'
                  OR announcement_title LIKE '%审计委员会%'
                  OR announcement_title LIKE '%委托理财%'
                  OR announcement_title LIKE '%套期保值%'
                  OR announcement_title LIKE '%担保额度%'
                  OR announcement_title LIKE '%担保的进展%'
                  OR announcement_title LIKE '%提供担保%'
                  OR announcement_title LIKE '%发行债券%'
                  OR announcement_title LIKE '%公司章程%'
                  OR announcement_title LIKE '%公司制度%'
                  OR announcement_title LIKE '%制定及修订%'
                  OR announcement_title LIKE '%独立董事%'
                  OR announcement_title LIKE '%会计政策变更%'
                  OR announcement_title LIKE '%社会责任报告%'
                  OR announcement_title LIKE '%可持续发展报告%'
                  OR announcement_title LIKE '%可持续发展%'
                  OR announcement_title LIKE '%环境、社会及治理%'
                  OR announcement_title LIKE '%ESG%'
                  OR announcement_title LIKE '%估值提升计划%'
                  OR announcement_title LIKE '%市值管理%'
                  OR announcement_title LIKE '%质量回报双提升%'
                  OR announcement_title LIKE '%履职情况%'
                  OR announcement_title LIKE '%履行监督职责%'
              )
          )
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to count manual precision target sample universe: {error}"))?;

    let target_rows: Vec<(
        String,
        Option<String>,
        String,
        String,
        NaiveDate,
        NaiveDate,
        String,
        Option<String>,
        Option<String>,
        String,
        Value,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        r#"
        SELECT 'target'::text AS sample_kind,
               symbol_name,
               symbol,
               announcement_title,
               announcement_time,
               available_at,
               source_published_at_quality,
               event_type,
               text_hash,
               raw_payload_hash,
               evidence_spans,
               LEFT(COALESCE(text_content, ''), 800) AS text_excerpt,
               announcement_url,
               pdf_final_url
        FROM market_exchange_announcement_text_raw
        WHERE announcement_time >= $1
          AND announcement_time <= $2
          AND event_type IS NOT NULL
          AND announcement_category NOT IN ('股权激励')
          AND jsonb_array_length(evidence_spans) > 0
          AND pdf_parse_status <> 'scanned_pdf_ocr_required'
          AND NOT (
              NOT (
                  announcement_title LIKE '%签署《关于进一步加强和深化合作的协议》%'
                  OR announcement_title LIKE '%合资建厂%'
                  OR announcement_title LIKE '%签订日常经营重大合同%'
                  OR announcement_title LIKE '%投资建设高效电池产能%'
                  OR announcement_title LIKE '%投资建设产能项目%'
              )
              AND (
                  announcement_title LIKE '%计提减值准备%'
                  OR announcement_title LIKE '%募投项目%'
                  OR announcement_title LIKE '%募集资金%'
                  OR announcement_title LIKE '%关联交易%'
                  OR announcement_title LIKE '%授信额度%'
                  OR announcement_title LIKE '%注册资本%'
                  OR announcement_title LIKE '%工商变更%'
                  OR announcement_title LIKE '%实际控制人%'
                  OR announcement_title LIKE '%控制权%'
                  OR announcement_title LIKE '%股份质押%'
                  OR announcement_title LIKE '%财务报告%'
                  OR announcement_title LIKE '%年度报告%'
                  OR announcement_title LIKE '%半年度报告%'
                  OR announcement_title LIKE '%季度报告%'
                  OR announcement_title LIKE '%主要经营数据%'
                  OR announcement_title LIKE '%股权投资基金%'
                  OR announcement_title LIKE '%投资基金%'
                  OR announcement_title LIKE '%风险评估报告%'
                  OR announcement_title LIKE '%H股发行%'
                  OR announcement_title LIKE '%发行H股%'
                  OR announcement_title LIKE '%H股股票%'
                  OR announcement_title LIKE '%上市审计机构%'
                  OR announcement_title LIKE '%章程%'
                  OR announcement_title LIKE '%审计报告%'
                  OR announcement_title LIKE '%审计机构%'
                  OR announcement_title LIKE '%资产减值%'
                  OR announcement_title LIKE '%公募REITs%'
                  OR announcement_title LIKE '%REITs%'
                  OR announcement_title LIKE '%收购控股子公司%'
                  OR announcement_title LIKE '%董事会工作报告%'
                  OR announcement_title LIKE '%监事会工作报告%'
                  OR announcement_title LIKE '%内部控制%'
                  OR announcement_title LIKE '%会计师事务所%'
                  OR announcement_title LIKE '%审计委员会%'
                  OR announcement_title LIKE '%委托理财%'
                  OR announcement_title LIKE '%套期保值%'
                  OR announcement_title LIKE '%担保额度%'
                  OR announcement_title LIKE '%担保的进展%'
                  OR announcement_title LIKE '%提供担保%'
                  OR announcement_title LIKE '%发行债券%'
                  OR announcement_title LIKE '%公司章程%'
                  OR announcement_title LIKE '%公司制度%'
                  OR announcement_title LIKE '%制定及修订%'
                  OR announcement_title LIKE '%独立董事%'
                  OR announcement_title LIKE '%会计政策变更%'
                  OR announcement_title LIKE '%社会责任报告%'
                  OR announcement_title LIKE '%可持续发展报告%'
                  OR announcement_title LIKE '%可持续发展%'
                  OR announcement_title LIKE '%环境、社会及治理%'
                  OR announcement_title LIKE '%ESG%'
                  OR announcement_title LIKE '%估值提升计划%'
                  OR announcement_title LIKE '%市值管理%'
                  OR announcement_title LIKE '%质量回报双提升%'
                  OR announcement_title LIKE '%履职情况%'
                  OR announcement_title LIKE '%履行监督职责%'
              )
          )
        ORDER BY raw_payload_hash, announcement_time, symbol
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(required_target_sample_size)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to build manual precision target sample: {error}"))?;

    let negative_rows: Vec<(
        String,
        Option<String>,
        String,
        String,
        NaiveDate,
        NaiveDate,
        String,
        Option<String>,
        Option<String>,
        String,
        Value,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = if negative_limit > 0 {
        sqlx::query_as(
            r#"
            SELECT 'negative'::text AS sample_kind,
                   symbol_name,
                   symbol,
                   announcement_title,
                   announcement_time,
                   available_at,
                   source_published_at_quality,
                   event_type,
                   text_hash,
                   raw_payload_hash,
                   evidence_spans,
                   LEFT(COALESCE(text_content, ''), 800) AS text_excerpt,
                   announcement_url,
                   pdf_final_url
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1
              AND announcement_time <= $2
              AND event_type IS NULL
              AND pdf_parse_status <> 'scanned_pdf_ocr_required'
            ORDER BY raw_payload_hash, announcement_time, symbol
            LIMIT $3
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(negative_limit)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to build manual precision negative sample: {error}"))?
    } else {
        Vec::new()
    };

    let mut review_items = Vec::with_capacity(target_rows.len() + negative_rows.len());
    for (
        sample_kind,
        symbol_name,
        symbol,
        announcement_title,
        announcement_time,
        available_at,
        source_published_at_quality,
        event_type,
        text_hash,
        raw_payload_hash,
        evidence_spans,
        text_excerpt,
        announcement_url,
        pdf_final_url,
    ) in target_rows.into_iter().chain(negative_rows)
    {
        review_items.push(json!({
            "sample_kind": sample_kind,
            "symbol": symbol,
            "symbol_name": symbol_name,
            "announcement_title": announcement_title,
            "taxonomy_risk_reason": exchange_announcement_order_capacity_taxonomy_risk_title_reason(
                "日常经营",
                &announcement_title,
            ),
            "announcement_time": announcement_time.to_string(),
            "available_at": available_at.to_string(),
            "source_published_at_quality": source_published_at_quality,
            "event_type": event_type,
            "text_hash": text_hash,
            "raw_payload_hash": raw_payload_hash,
            "evidence_spans": evidence_spans,
            "text_excerpt": text_excerpt.unwrap_or_default(),
            "announcement_url": announcement_url,
            "pdf_final_url": pdf_final_url,
            "manual_review_fields": {
                "evidence_span_label": null,
                "taxonomy_label": null,
                "reviewer": null,
                "reviewed_at": null,
                "review_note": null
            }
        }));
    }

    let target_sample_rows = review_items
        .iter()
        .filter(|item| item.get("sample_kind").and_then(Value::as_str) == Some("target"))
        .count() as i64;
    let negative_sample_rows = review_items
        .iter()
        .filter(|item| item.get("sample_kind").and_then(Value::as_str) == Some("negative"))
        .count() as i64;

    let mut report = exchange_announcement_order_capacity_manual_precision_sample_report(
        admissible_target_event_rows,
        required_target_sample_size,
        target_sample_rows,
        negative_sample_rows,
        review_items,
    );
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "date_range".to_string(),
            json!({
                "start_date": start_date,
                "end_date": end_date,
            }),
        );
        object.insert(
            "sample_policy".to_string(),
            json!({
                "target_order": "raw_payload_hash, announcement_time, symbol",
                "negative_samples": include_negative_samples,
                "negative_sample_limit": negative_limit,
                "pit_policy": "review uses available_at and source_published_at_quality; no trading use is admitted"
            }),
        );
    }

    Ok(report)
}

fn summarize_exchange_announcement_pdf_detail_probes(
    probes: &[Value],
) -> ExchangeAnnouncementPdfDetailProbeSummary {
    let parsed_pdf_count = probes
        .iter()
        .filter(|probe| probe.get("status").and_then(Value::as_str) == Some("ok"))
        .count();
    let stable_hash_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("hash_stable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && probe.get("text_hash").and_then(Value::as_str).is_some()
        })
        .count();
    let timestamp_count = probes
        .iter()
        .filter(|probe| {
            probe
                .get("source_published_at_quality")
                .and_then(Value::as_str)
                == Some("timestamp")
        })
        .count();
    let next_session_policy_count = probes
        .iter()
        .filter(|probe| {
            probe
                .get("source_published_at_quality")
                .and_then(Value::as_str)
                == Some("date_only_next_session")
        })
        .count();
    let availability_count = timestamp_count + next_session_policy_count;
    let evidence_span_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("evidence_spans")
                    .and_then(Value::as_array)
                    .map(|spans| !spans.is_empty())
                    .unwrap_or(false)
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
    let scanned_pdf_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("scanned_pdf_ocr_required")
        })
        .count();
    let runtime_not_configured_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("runtime_not_configured")
        })
        .count();

    ExchangeAnnouncementPdfDetailProbeSummary {
        parsed_pdf_count,
        stable_hash_count,
        availability_count,
        timestamp_count,
        next_session_policy_count,
        evidence_span_count,
        incomplete_link_metadata_count,
        scanned_pdf_count,
        runtime_not_configured_count,
    }
}

fn summarize_exchange_announcement_ocr_blocked_row_probes(
    probes: &[Value],
) -> ExchangeAnnouncementOcrBlockedRowProbeSummary {
    let ocr_text_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("ocr_text_length")
                    .and_then(Value::as_u64)
                    .map(|length| length > 0)
                    .unwrap_or(false)
        })
        .count();
    let stable_hash_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("hash_stable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && probe.get("ocr_text_hash").and_then(Value::as_str).is_some()
        })
        .count();
    let availability_count = probes
        .iter()
        .filter(|probe| {
            matches!(
                probe
                    .get("source_published_at_quality")
                    .and_then(Value::as_str),
                Some("timestamp" | "date_only_next_session")
            ) && probe.get("available_at").and_then(Value::as_str).is_some()
        })
        .count();
    let quality_pass_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("ocr_quality")
                    .and_then(|quality| quality.get("status"))
                    .and_then(Value::as_str)
                    == Some("passed")
        })
        .count();
    let evidence_span_count = probes
        .iter()
        .filter(|probe| {
            probe
                .get("evidence_spans")
                .and_then(Value::as_array)
                .map(|spans| !spans.is_empty())
                .unwrap_or(false)
        })
        .count();
    let runtime_missing_count = probes
        .iter()
        .filter(|probe| {
            matches!(
                probe.get("status").and_then(Value::as_str),
                Some("ocr_runtime_not_configured" | "ocr_dependency_missing")
            )
        })
        .count();
    let ocr_error_count = probes
        .iter()
        .filter(|probe| {
            matches!(
                probe.get("status").and_then(Value::as_str),
                Some("error" | "timeout" | "ocr_error" | "pdf_fetch_error")
            )
        })
        .count();
    let no_target_span_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("evidence_spans")
                    .and_then(Value::as_array)
                    .map(|spans| spans.is_empty())
                    .unwrap_or(true)
        })
        .count();
    let incomplete_raw_link_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("incomplete_raw_pdf_link")
        })
        .count();

    ExchangeAnnouncementOcrBlockedRowProbeSummary {
        ocr_text_count,
        stable_hash_count,
        availability_count,
        quality_pass_count,
        evidence_span_count,
        runtime_missing_count,
        ocr_error_count,
        no_target_span_count,
        incomplete_raw_link_count,
    }
}

pub(crate) fn exchange_announcement_order_capacity_sync_plan_batches(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
) -> Result<Vec<AkshareAnalystRevisionSyncPlanBatch>, String> {
    akshare_analyst_revision_sync_plan_batches(start, end, batch_mode).map_err(|error| {
        error.replace(
            "AkShare analyst revision sync-plan",
            "exchange announcement order capacity sync-plan",
        )
    })
}

async fn run_exchange_announcement_order_capacity_probe(
    python: &str,
    symbol: &str,
    market: &str,
    category: &str,
    start_date: &str,
    end_date: &str,
    row_limit: usize,
) -> Value {
    let source = "stock_zh_a_disclosure_report_cninfo";
    if !Path::new(python).exists() {
        return json!({
            "source": source,
            "scope": "symbol_category_date_range",
            "symbol": symbol,
            "market": market,
            "category": category,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "status": "runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "python": python,
            "error": "AKSHARE_PYTHON is not configured and /tmp/akshare-smoke/bin/python is missing"
        });
    }

    let script = r#"
import json
import sys

symbol = sys.argv[1]
market = sys.argv[2]
category = sys.argv[3]
start_date = sys.argv[4]
end_date = sys.argv[5]
row_limit = int(sys.argv[6])

def scrub(value):
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)

def nonempty(value):
    try:
        import pandas as pd
        if pd.isna(value):
            return False
    except Exception:
        pass
    return str(value).strip() != ""

try:
    import akshare as ak
    import pandas as pd
    df = ak.stock_zh_a_disclosure_report_cninfo(
        symbol=symbol,
        market=market,
        category=category,
        start_date=start_date,
        end_date=end_date,
    )
    if df is None:
        df = pd.DataFrame()

    records = []
    if hasattr(df, "head"):
        for row in df.head(row_limit).to_dict(orient="records"):
            records.append({str(key): scrub(value) for key, value in row.items()})

    announcement_time_missing_rows = None
    announcement_link_missing_rows = None
    duplicate_link_rows = None
    if hasattr(df, "columns"):
        if "公告时间" in df.columns:
            announcement_time_missing_rows = int((~df["公告时间"].map(nonempty)).sum())
        if "公告链接" in df.columns:
            link_nonempty = df["公告链接"].map(nonempty)
            announcement_link_missing_rows = int((~link_nonempty).sum())
            duplicate_link_rows = int(df.loc[link_nonempty, "公告链接"].duplicated().sum())

    print(json.dumps({
        "status": "ok" if len(df) > 0 else "ok_empty",
        "permission": "available",
        "akshare_version": getattr(ak, "__version__", None),
        "row_count": int(len(df)),
        "fields": [str(field) for field in list(df.columns)] if hasattr(df, "columns") else [],
        "announcement_time_missing_rows": announcement_time_missing_rows,
        "announcement_link_missing_rows": announcement_link_missing_rows,
        "duplicate_link_rows": duplicate_link_rows,
        "sample_rows": records,
    }, ensure_ascii=False))
except Exception as error:
    print(json.dumps({
        "status": "error",
        "permission": "unknown_or_unavailable",
        "error_type": type(error).__name__,
        "error": str(error),
    }, ensure_ascii=False))
"#;

    let home = env::var("HOME").unwrap_or_else(|_| "/Users/gaocheng".to_string());
    let timeout_seconds = akshare_analyst_revision_timeout_seconds();
    let child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(symbol)
        .arg(market)
        .arg(category)
        .arg(start_date)
        .arg(end_date)
        .arg(row_limit.to_string())
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "source": source,
                "scope": "symbol_category_date_range",
                "symbol": symbol,
                "market": market,
                "category": category,
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "status": "error",
                "permission": "unknown_or_unavailable",
                "python": python,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "source": source,
                "scope": "symbol_category_date_range",
                "symbol": symbol,
                "market": market,
                "category": category,
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "status": "timeout",
                "permission": "unknown_or_unavailable",
                "python": python,
                "timeout_seconds": timeout_seconds,
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let mut payload: Value = serde_json::from_str(&stdout).unwrap_or_else(|_| {
        json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error": "failed_to_parse_exchange_announcement_probe_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });
    payload = normalize_exchange_announcement_order_capacity_probe_payload(payload);

    if let Some(object) = payload.as_object_mut() {
        object.insert("source".to_string(), json!(source));
        object.insert("scope".to_string(), json!("symbol_category_date_range"));
        object.insert("symbol".to_string(), json!(symbol));
        object.insert("market".to_string(), json!(market));
        object.insert("category".to_string(), json!(category));
        object.insert(
            "date_range".to_string(),
            json!({
                "start_date": start_date,
                "end_date": end_date,
            }),
        );
        object.insert("python".to_string(), json!(python));
        object.insert("exit_status".to_string(), json!(output.status.code()));
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    attach_exchange_announcement_link_metadata(payload)
}

pub(crate) fn exchange_announcement_order_capacity_is_akshare_empty_dataframe_key_error(
    error: &str,
) -> bool {
    error.contains("None of [Index([")
        && error.contains("'代码'")
        && error.contains("'简称'")
        && error.contains("'公告标题'")
        && error.contains("'公告时间'")
        && error.contains("'announcementId'")
        && error.contains("'orgId'")
        && error.contains("are in the [columns]")
}

fn attach_exchange_announcement_link_metadata(mut payload: Value) -> Value {
    let sample_rows = payload
        .get("sample_rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut link_metadata = Vec::new();
    for row in sample_rows {
        if let Some(link) = row.get("公告链接").and_then(Value::as_str) {
            link_metadata.push(parse_cninfo_announcement_link_metadata(link));
        }
    }
    let complete_link_metadata_rows = link_metadata
        .iter()
        .filter(|metadata| {
            metadata
                .get("metadata_complete")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();
    let incomplete_link_metadata_rows = link_metadata
        .len()
        .saturating_sub(complete_link_metadata_rows);

    if let Some(object) = payload.as_object_mut() {
        object.insert("link_metadata_sample".to_string(), json!(link_metadata));
        object.insert(
            "complete_link_metadata_sample_rows".to_string(),
            json!(complete_link_metadata_rows),
        );
        object.insert(
            "incomplete_link_metadata_sample_rows".to_string(),
            json!(incomplete_link_metadata_rows),
        );
    }
    payload
}

async fn run_exchange_announcement_detail_probe(python: &str, link: &str) -> Value {
    let metadata = parse_cninfo_announcement_link_metadata(link);
    if !metadata
        .get("metadata_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return json!({
            "status": "incomplete_link_metadata",
            "permission": "unknown_or_unavailable",
            "announcement_link": link,
            "link_metadata": metadata,
        });
    }

    if !Path::new(python).exists() {
        return json!({
            "status": "runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "announcement_link": link,
            "link_metadata": metadata,
            "python": python,
            "error": "AKSHARE_PYTHON is not configured and /tmp/akshare-smoke/bin/python is missing"
        });
    }

    let script = r#"
import json
import re
import sys
from html.parser import HTMLParser

url = sys.argv[1]

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
    def handle_data(self, data):
        text = data.strip()
        if text:
            self.parts.append(text)

def normalize_text(value):
    return re.sub(r"\s+", " ", value or "").strip()

try:
    import requests
    response = requests.get(url, timeout=15, headers={
        "User-Agent": "Mozilla/5.0 quant-source-admission-audit"
    })
    content_type = (response.headers.get("Content-Type") or "").lower()
    final_url = response.url or url
    is_pdf = "application/pdf" in content_type or final_url.lower().endswith(".pdf") or response.content[:5] == b"%PDF-"
    if is_pdf:
        print(json.dumps({
            "status": "pdf_text_parser_required",
            "permission": "available" if response.ok else "unknown_or_unavailable",
            "http_status": response.status_code,
            "final_url": final_url,
            "content_type": content_type,
            "text_content_type": "pdf",
            "text_length": 0,
            "text_sample": None,
            "source_published_at": None,
            "source_published_at_quality": "missing_or_date_only",
            "blocked_reason": "cninfo detail redirects to PDF; PDF parser and source timestamp audit are required before text evidence can be used",
        }, ensure_ascii=False))
        sys.exit(0)
    html = response.text or ""
    parser = TextExtractor()
    parser.feed(html)
    text = normalize_text(" ".join(parser.parts))
    timestamp_patterns = [
        r"(?:公告时间|披露时间|发布时间|发布日期)[:：\\s]*([0-9]{4}[-/年][0-9]{1,2}[-/月][0-9]{1,2}(?:日)?\\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)",
        r"([0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}\\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)",
    ]
    source_published_at = None
    for pattern in timestamp_patterns:
        match = re.search(pattern, html)
        if match:
            source_published_at = match.group(1)
            break
    print(json.dumps({
        "status": "ok" if response.ok and len(text) > 0 else "error",
        "permission": "available" if response.ok else "unknown_or_unavailable",
        "http_status": response.status_code,
        "final_url": final_url,
        "content_type": content_type,
        "text_content_type": "html",
        "text_length": len(text),
        "text_sample": text[:1000],
        "source_published_at": source_published_at,
        "source_published_at_quality": "timestamp" if source_published_at else "missing_or_date_only",
    }, ensure_ascii=False))
except Exception as error:
    print(json.dumps({
        "status": "error",
        "permission": "unknown_or_unavailable",
        "error_type": type(error).__name__,
        "error": str(error),
    }, ensure_ascii=False))
"#;

    let home = env::var("HOME").unwrap_or_else(|_| "/Users/gaocheng".to_string());
    let timeout_seconds = akshare_analyst_revision_timeout_seconds();
    let child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(link)
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "status": "error",
                "permission": "unknown_or_unavailable",
                "announcement_link": link,
                "link_metadata": metadata,
                "python": python,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "status": "timeout",
                "permission": "unknown_or_unavailable",
                "announcement_link": link,
                "link_metadata": metadata,
                "python": python,
                "timeout_seconds": timeout_seconds,
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let mut payload: Value = serde_json::from_str(&stdout).unwrap_or_else(|_| {
        json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error": "failed_to_parse_exchange_announcement_detail_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });
    let text_sample = payload
        .get("text_sample")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let text_hash = if text_sample.trim().is_empty() {
        None
    } else {
        Some(akshare_stable_hash(&[
            link.to_string(),
            text_sample.clone(),
        ]))
    };

    if let Some(object) = payload.as_object_mut() {
        object.insert("announcement_link".to_string(), json!(link));
        object.insert("link_metadata".to_string(), metadata);
        object.insert("python".to_string(), json!(python));
        object.insert("exit_status".to_string(), json!(output.status.code()));
        object.insert("text_hash".to_string(), json!(text_hash));
        object.insert(
            "text_hash_algorithm".to_string(),
            json!("fnv64_internal_admission_hash"),
        );
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    payload
}

async fn run_exchange_announcement_pdf_detail_probe(python: &str, link: &str) -> Value {
    let metadata = parse_cninfo_announcement_link_metadata(link);
    if !metadata
        .get("metadata_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return json!({
            "status": "incomplete_link_metadata",
            "permission": "unknown_or_unavailable",
            "announcement_link": link,
            "link_metadata": metadata,
        });
    }

    if !Path::new(python).exists() {
        return json!({
            "status": "runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "announcement_link": link,
            "link_metadata": metadata,
            "python": python,
            "error": "QUANT_PDF_AUDIT_PYTHON is not configured and the isolated PDF runtime is missing"
        });
    }

    let script = r#"
import hashlib
import json
import re
import sys
import tempfile
from email.utils import parsedate_to_datetime

url = sys.argv[1]
announcement_time = sys.argv[2]

def normalize_text(value):
    return re.sub(r"\s+", " ", value or "").strip()

def parse_pdf_with_pdfplumber(pdf_path):
    import pdfplumber
    with pdfplumber.open(pdf_path) as pdf:
        page_texts = [normalize_text(page.extract_text() or "") for page in pdf.pages]
        metadata = dict(pdf.metadata or {})
    return page_texts, metadata, "pdfplumber"

def parse_pdf_with_pypdf(pdf_path):
    from pypdf import PdfReader
    reader = PdfReader(pdf_path)
    page_texts = [normalize_text(page.extract_text() or "") for page in reader.pages]
    metadata = dict(reader.metadata or {})
    return page_texts, metadata, "pypdf"

def parse_pdf_with_pypdf2(pdf_path):
    from PyPDF2 import PdfReader
    reader = PdfReader(pdf_path)
    page_texts = [normalize_text(page.extract_text() or "") for page in reader.pages]
    metadata = dict(reader.metadata or {})
    return page_texts, metadata, "PyPDF2"

def parse_pdf_with_fitz(pdf_path):
    import fitz
    doc = fitz.open(pdf_path)
    page_texts = [normalize_text(page.get_text("text") or "") for page in doc]
    metadata = dict(doc.metadata or {})
    doc.close()
    return page_texts, metadata, "fitz"

def parse_pdf(pdf_path):
    errors = []
    for parser in (
        parse_pdf_with_pdfplumber,
        parse_pdf_with_pypdf,
        parse_pdf_with_pypdf2,
        parse_pdf_with_fitz,
    ):
        try:
            page_texts, metadata, parser_name = parser(pdf_path)
            return page_texts, metadata, parser_name, errors
        except Exception as error:
            errors.append({
                "parser": parser.__name__,
                "error_type": type(error).__name__,
                "error": str(error),
            })
    raise RuntimeError(json.dumps(errors, ensure_ascii=False))

def collect_timestamp_candidates(full_text, headers, metadata, announcement_time):
    candidates = []
    seen = set()

    patterns = [
        ("text_timestamp", "timestamp", r"(?:公告时间|披露时间|发布时间|发布日期|刊登时间)[:：\s]*([0-9]{4}[-/年][0-9]{1,2}[-/月][0-9]{1,2}(?:日)?\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)"),
        ("text_timestamp", "timestamp", r"([0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)"),
        ("text_date_only", "date_only_next_session", r"(?:公告时间|披露时间|发布时间|发布日期|刊登日期)[:：\s]*([0-9]{4}[-/年][0-9]{1,2}[-/月][0-9]{1,2}(?:日)?)"),
    ]
    for source, quality, pattern in patterns:
        for match in re.finditer(pattern, full_text):
            value = normalize_text(match.group(1))
            key = (source, value, quality)
            if value and key not in seen:
                candidates.append({
                    "source": source,
                    "value": value,
                    "quality": quality,
                })
                seen.add(key)
            if len(candidates) >= 6:
                return candidates

    if announcement_time:
        key = ("announcement_time_param", announcement_time, "date_only_next_session")
        if key not in seen:
            candidates.append({
                "source": "announcement_time_param",
                "value": announcement_time,
                "quality": "date_only_next_session",
            })
            seen.add(key)

    last_modified = headers.get("Last-Modified") or headers.get("last-modified")
    if last_modified:
        try:
            parsed = parsedate_to_datetime(last_modified)
            value = parsed.isoformat()
        except Exception:
            value = normalize_text(last_modified)
        key = ("http_last_modified", value, "transport_header_untrusted")
        if value and key not in seen:
            candidates.append({
                "source": "http_last_modified",
                "value": value,
                "quality": "transport_header_untrusted",
            })
            seen.add(key)

    for meta_key in ("/CreationDate", "/ModDate", "CreationDate", "ModDate", "creationDate", "modDate"):
        if meta_key in metadata and metadata[meta_key]:
            value = normalize_text(str(metadata[meta_key]))
            key = (f"pdf_metadata:{meta_key}", value, "metadata_untrusted")
            if value and key not in seen:
                candidates.append({
                    "source": f"pdf_metadata:{meta_key}",
                    "value": value,
                    "quality": "metadata_untrusted",
                })
                seen.add(key)

    return candidates[:8]

def evidence_spans(page_texts):
    themes = [
        ("order_contract", ["中标", "签订", "订单", "合同", "协议", "框架协议"]),
        ("capacity", ["产能", "扩产", "投产", "项目", "开工", "复产", "停产"]),
        ("price", ["调价", "提价", "降价", "价格调整", "售价"]),
    ]
    spans = []
    for page_index, page_text in enumerate(page_texts, start=1):
        if not page_text:
            continue
        for theme, keywords in themes:
            for keyword in keywords:
                position = page_text.find(keyword)
                if position < 0:
                    continue
                start = max(0, position - 50)
                end = min(len(page_text), position + len(keyword) + 80)
                spans.append({
                    "theme": theme,
                    "keyword": keyword,
                    "page": page_index,
                    "snippet": page_text[start:end],
                })
                break
        if len(spans) >= 12:
            break
    return spans[:12]

try:
    import requests
    response = requests.get(url, timeout=20, headers={
        "User-Agent": "Mozilla/5.0 quant-source-admission-pdf-audit"
    })
    content_type = (response.headers.get("Content-Type") or "").lower()
    final_url = response.url or url
    is_pdf = "application/pdf" in content_type or final_url.lower().endswith(".pdf") or response.content[:5] == b"%PDF-"
    if not is_pdf:
        print(json.dumps({
            "status": "not_pdf_after_redirect",
            "permission": "available" if response.ok else "unknown_or_unavailable",
            "http_status": response.status_code,
            "final_url": final_url,
            "content_type": content_type,
        }, ensure_ascii=False))
        sys.exit(0)

    with tempfile.NamedTemporaryFile(suffix=".pdf") as handle:
        handle.write(response.content)
        handle.flush()

        run_hashes = []
        selected_parser = None
        selected_page_texts = []
        selected_metadata = {}
        parser_errors = []
        for _ in range(2):
            page_texts, metadata, parser_name, errors = parse_pdf(handle.name)
            if selected_parser is None:
                selected_parser = parser_name
                selected_page_texts = page_texts
                selected_metadata = metadata
                parser_errors = errors
            canonical_text = normalize_text("\n".join(text for text in page_texts if text))
            run_hashes.append(hashlib.sha256(canonical_text.encode("utf-8")).hexdigest())

    page_count = len(selected_page_texts)
    nonempty_page_count = sum(1 for text in selected_page_texts if text)
    canonical_text = normalize_text("\n".join(text for text in selected_page_texts if text))
    text_sample = canonical_text[:1000] if canonical_text else None
    timestamp_candidates = collect_timestamp_candidates(
        canonical_text,
        dict(response.headers),
        selected_metadata,
        announcement_time,
    )
    quality = "missing"
    source_published_at = None
    for candidate in timestamp_candidates:
        if candidate["quality"] in {"timestamp", "date_only_next_session"}:
            source_published_at = candidate["value"]
            quality = candidate["quality"]
            break
    spans = evidence_spans(selected_page_texts)

    status = "ok"
    if page_count > 0 and nonempty_page_count == 0:
        status = "scanned_pdf_ocr_required"
    elif not canonical_text:
        status = "pdf_parse_empty"

    print(json.dumps({
        "status": status,
        "permission": "available" if response.ok else "unknown_or_unavailable",
        "http_status": response.status_code,
        "final_url": final_url,
        "content_type": content_type,
        "parser_used": selected_parser,
        "parser_errors": parser_errors,
        "page_count": page_count,
        "nonempty_page_count": nonempty_page_count,
        "text_length": len(canonical_text),
        "text_sample": text_sample,
        "text_hash": run_hashes[0] if run_hashes else None,
        "text_hash_algorithm": "sha256",
        "repeat_hashes": run_hashes,
        "hash_stable": len(set(run_hashes)) == 1 if run_hashes else False,
        "timestamp_candidates": timestamp_candidates,
        "source_published_at": source_published_at,
        "source_published_at_quality": quality,
        "evidence_spans": spans,
        "pdf_metadata_keys": sorted([str(key) for key in selected_metadata.keys()])[:20],
    }, ensure_ascii=False))
except Exception as error:
    print(json.dumps({
        "status": "error",
        "permission": "unknown_or_unavailable",
        "error_type": type(error).__name__,
        "error": str(error),
    }, ensure_ascii=False))
"#;

    let announcement_time = metadata
        .get("announcement_time")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let home = env::var("HOME").unwrap_or_else(|_| "/Users/gaocheng".to_string());
    let timeout_seconds = akshare_analyst_revision_timeout_seconds();
    let child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(link)
        .arg(announcement_time)
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "status": "error",
                "permission": "unknown_or_unavailable",
                "announcement_link": link,
                "link_metadata": metadata,
                "python": python,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "status": "timeout",
                "permission": "unknown_or_unavailable",
                "announcement_link": link,
                "link_metadata": metadata,
                "python": python,
                "timeout_seconds": timeout_seconds,
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let mut payload: Value = serde_json::from_str(&stdout).unwrap_or_else(|_| {
        json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error": "failed_to_parse_exchange_announcement_pdf_detail_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });

    if let Some(object) = payload.as_object_mut() {
        object.insert("announcement_link".to_string(), json!(link));
        object.insert("link_metadata".to_string(), metadata);
        object.insert("python".to_string(), json!(python));
        object.insert("exit_status".to_string(), json!(output.status.code()));
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    payload
}

async fn probe_exchange_announcement_order_capacity_pdf_parsers(python: &str) -> Vec<Value> {
    let mut checks = vec![json!({
        "tool": "pdftotext",
        "kind": "binary",
        "available": command_exists_on_path("pdftotext"),
    })];

    if !Path::new(python).exists() {
        checks.push(json!({
            "tool": "python_runtime",
            "kind": "python",
            "available": false,
            "python": python,
            "error": "configured python runtime is missing"
        }));
        return checks;
    }

    let script = r#"
import importlib.util
import json
import sys

modules = sys.argv[1:]
print(json.dumps([
    {
        "tool": module,
        "kind": "python_module",
        "available": importlib.util.find_spec(module) is not None,
    }
    for module in modules
], ensure_ascii=False))
"#;
    let child = Command::new(python)
        .arg("-c")
        .arg(script)
        .args(["pypdf", "PyPDF2", "pdfplumber", "fitz"])
        .env("PYTHONUNBUFFERED", "1")
        .output();
    let output = match timeout(StdDuration::from_secs(15), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            checks.push(json!({
                "tool": "python_module_probe",
                "kind": "python",
                "available": false,
                "python": python,
                "error": error.to_string()
            }));
            return checks;
        }
        Err(_) => {
            checks.push(json!({
                "tool": "python_module_probe",
                "kind": "python",
                "available": false,
                "python": python,
                "error": "python module probe timeout"
            }));
            return checks;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match serde_json::from_str::<Vec<Value>>(&stdout) {
        Ok(module_checks) => checks.extend(module_checks),
        Err(error) => checks.push(json!({
            "tool": "python_module_probe",
            "kind": "python",
            "available": false,
            "python": python,
            "exit_status": output.status.code(),
            "error": format!("failed_to_parse_python_module_probe_stdout: {error}"),
            "stdout": stdout,
            "stderr": stderr
        })),
    }
    checks
}

async fn run_exchange_announcement_ocr_blocked_row_probe(python: &str, row: Value) -> Value {
    let pdf_url = row
        .get("pdf_final_url")
        .and_then(Value::as_str)
        .filter(|url| !url.trim().is_empty())
        .or_else(|| row.get("announcement_url").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    if pdf_url.trim().is_empty() {
        return json!({
            "status": "incomplete_raw_pdf_link",
            "raw_row": row,
            "error": "raw scanned PDF row has neither pdf_final_url nor announcement_url"
        });
    }

    if !Path::new(python).exists() {
        return json!({
            "status": "ocr_runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "python": python,
            "pdf_url": pdf_url,
            "raw_row": row,
            "error": "QUANT_PDF_AUDIT_PYTHON is not configured and the isolated PDF/OCR runtime is missing"
        });
    }

    let script = r#"
import hashlib
import importlib.util
import json
import re
import shutil
import sys
import tempfile

url = sys.argv[1]

missing = []
if importlib.util.find_spec("requests") is None:
    missing.append("python_module:requests")
if importlib.util.find_spec("fitz") is None:
    missing.append("python_module:fitz")
if importlib.util.find_spec("pytesseract") is None:
    missing.append("python_module:pytesseract")
try:
    from PIL import Image  # noqa: F401
except Exception:
    missing.append("python_module:PIL")
if shutil.which("tesseract") is None:
    missing.append("binary:tesseract")

if missing:
    print(json.dumps({
        "status": "ocr_dependency_missing",
        "permission": "unknown_or_unavailable",
        "missing_dependencies": missing,
        "required_runtime": ["requests", "fitz", "pytesseract", "PIL", "tesseract"],
    }, ensure_ascii=False))
    sys.exit(0)

def normalize_text(value):
    return re.sub(r"\s+", " ", value or "").strip()

def evidence_spans(text):
    themes = [
        ("order_contract", ["中标", "签订", "订单", "合同", "协议", "框架协议"]),
        ("capacity", ["产能", "扩产", "投产", "项目", "开工", "复产", "停产"]),
        ("price", ["调价", "提价", "降价", "价格调整", "售价"]),
    ]
    spans = []
    for theme, keywords in themes:
        for keyword in keywords:
            position = text.find(keyword)
            if position < 0:
                continue
            start = max(0, position - 60)
            end = min(len(text), position + len(keyword) + 100)
            spans.append({
                "theme": theme,
                "keyword": keyword,
                "snippet": text[start:end],
            })
            break
    return spans[:12]

try:
    import fitz
    import pytesseract
    import requests

    response = requests.get(url, timeout=20, headers={
        "User-Agent": "Mozilla/5.0 quant-source-admission-ocr-audit"
    })
    if not response.ok or response.content[:5] != b"%PDF-":
        print(json.dumps({
            "status": "pdf_fetch_error",
            "permission": "available" if response.ok else "unknown_or_unavailable",
            "http_status": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "final_url": response.url or url,
        }, ensure_ascii=False))
        sys.exit(0)

    with tempfile.NamedTemporaryFile(suffix=".pdf") as handle:
        handle.write(response.content)
        handle.flush()

        run_hashes = []
        selected_text = ""
        selected_page_count = 0
        selected_ocr_page_count = 0
        for _ in range(2):
            doc = fitz.open(handle.name)
            selected_page_count = len(doc)
            page_texts = []
            for page_index, page in enumerate(doc):
                if page_index >= 3:
                    break
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                image = Image.open(__import__("io").BytesIO(pix.tobytes("png")))
                text = pytesseract.image_to_string(image, lang="chi_sim+eng")
                text = normalize_text(text)
                if text:
                    page_texts.append(text)
            doc.close()
            canonical = normalize_text("\n".join(page_texts))
            run_hashes.append(hashlib.sha256(canonical.encode("utf-8")).hexdigest())
            if not selected_text:
                selected_text = canonical
                selected_ocr_page_count = len(page_texts)

    spans = evidence_spans(selected_text)
    quality_passed = len(selected_text) >= 200
    print(json.dumps({
        "status": "ok" if selected_text else "ocr_empty_text",
        "permission": "available",
        "http_status": response.status_code,
        "final_url": response.url or url,
        "parser_used": "pymupdf+pytesseract",
        "ocr_page_limit": 3,
        "page_count": selected_page_count,
        "ocr_nonempty_page_count": selected_ocr_page_count,
        "ocr_text_length": len(selected_text),
        "ocr_text_sample": selected_text[:1000] if selected_text else None,
        "ocr_text_hash": run_hashes[0] if run_hashes else None,
        "text_hash_algorithm": "sha256",
        "repeat_hashes": run_hashes,
        "hash_stable": len(set(run_hashes)) == 1 if run_hashes else False,
        "ocr_quality": {
            "status": "passed" if quality_passed else "blocked",
            "min_text_length": 200,
            "observed_text_length": len(selected_text),
        },
        "evidence_spans": spans,
    }, ensure_ascii=False))
except Exception as error:
    print(json.dumps({
        "status": "ocr_error",
        "permission": "unknown_or_unavailable",
        "error_type": type(error).__name__,
        "error": str(error),
    }, ensure_ascii=False))
"#;

    let home = env::var("HOME").unwrap_or_else(|_| "/Users/gaocheng".to_string());
    let timeout_seconds = akshare_analyst_revision_timeout_seconds().max(120);
    let child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(&pdf_url)
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "status": "ocr_error",
                "permission": "unknown_or_unavailable",
                "python": python,
                "pdf_url": pdf_url,
                "raw_row": row,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "status": "timeout",
                "permission": "unknown_or_unavailable",
                "python": python,
                "pdf_url": pdf_url,
                "raw_row": row,
                "timeout_seconds": timeout_seconds,
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let mut payload: Value = serde_json::from_str(&stdout).unwrap_or_else(|_| {
        json!({
            "status": "ocr_error",
            "permission": "unknown_or_unavailable",
            "error": "failed_to_parse_exchange_announcement_ocr_probe_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });

    if let Some(object) = payload.as_object_mut() {
        object.insert("python".to_string(), json!(python));
        object.insert("pdf_url".to_string(), json!(pdf_url));
        object.insert("raw_row".to_string(), row.clone());
        object.insert("exit_status".to_string(), json!(output.status.code()));
        object.insert(
            "source_published_at_quality".to_string(),
            row.get("source_published_at_quality")
                .cloned()
                .unwrap_or(json!(null)),
        );
        object.insert(
            "source_published_at".to_string(),
            row.get("source_published_at")
                .cloned()
                .unwrap_or(json!(null)),
        );
        object.insert(
            "available_at".to_string(),
            row.get("available_at").cloned().unwrap_or(json!(null)),
        );
        object.insert(
            "pit_policy".to_string(),
            json!("preserve raw source_published_at and available_at; OCR execution timestamp is never used as data availability"),
        );
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    payload
}

pub(crate) fn cninfo_operator_evidence_required_categories() -> [&'static str; 7] {
    [
        "terms_review_attestation",
        "credential_presence_attestation_without_secret_value",
        "endpoint_dictionary_reference",
        "history_range_attestation",
        "sample_payload_redacted_hash_evidence",
        "symbol_mapping_scope_note",
        "rate_limit_cost_refresh_latency_budget",
    ]
}

pub(crate) fn cninfo_operator_evidence_required_fields() -> [&'static str; 12] {
    [
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
        "notes",
    ]
}

fn cninfo_operator_evidence_forbidden_keys() -> [&'static str; 13] {
    [
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
        "secret",
        "token",
        "oos_performance_label",
    ]
}

pub(crate) fn cninfo_operator_evidence_string_present(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .map(|item| !item.trim().is_empty())
        .unwrap_or(false)
}

pub(crate) fn collect_cninfo_operator_forbidden_manifest_keys(
    value: &Value,
    forbidden: &mut BTreeSet<String>,
) {
    match value {
        Value::Object(map) => {
            let forbidden_keys: BTreeSet<&str> = cninfo_operator_evidence_forbidden_keys()
                .into_iter()
                .collect();
            for (key, child) in map {
                let normalized = key.to_ascii_lowercase();
                if forbidden_keys.contains(normalized.as_str()) {
                    forbidden.insert(normalized);
                }
                collect_cninfo_operator_forbidden_manifest_keys(child, forbidden);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_cninfo_operator_forbidden_manifest_keys(item, forbidden);
            }
        }
        _ => {}
    }
}

pub(crate) fn phase7_cninfo_operator_evidence_promotion_gate(permission_smoke: &str) -> Value {
    json!({
        "permission_smoke": permission_smoke,
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    })
}

pub(crate) fn phase7_cninfo_operator_evidence_runtime_actions() -> Value {
    json!({
        "network_enabled": false,
        "credential_read_enabled": false,
        "db_write_enabled": false,
        "schema_apply_enabled": false,
        "raw_payload_persistence_enabled": false
    })
}

pub(crate) fn phase7_cninfo_operator_evidence_privacy_guards() -> Value {
    json!({
        "echo_manifest_content": false,
        "echo_secret_values": false,
        "echo_raw_payload": false,
        "echo_vendor_documents": false,
        "return_only_counts_missing_fields_and_forbidden_key_names": true
    })
}

pub(crate) fn exchange_announcement_order_capacity_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
    market: String,
    symbols: Vec<String>,
    categories: Vec<String>,
    batches: Vec<AkshareAnalystRevisionSyncPlanBatch>,
) -> Value {
    let symbol_count = symbols.len();
    let category_count = categories.len();
    let request_keys_per_batch = symbol_count * category_count;
    let batch_values = batches
        .iter()
        .map(|batch| {
            let tiny_slice_count =
                exchange_announcement_order_capacity_tiny_slices(batch.start_date, batch.end_date)
                    .len();
            let bounded_runner_query_units = tiny_slice_count * request_keys_per_batch;
            let would_exceed_limit =
                bounded_runner_query_units > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS;
            json!({
                "batch": batch.label,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "calendar_day_count": batch.calendar_day_count,
                "symbol_count": symbol_count,
                "category_count": category_count,
                "request_key_count": request_keys_per_batch,
                "tiny_slice_count": tiny_slice_count,
                "query_count": bounded_runner_query_units,
                "estimated_api_calls": bounded_runner_query_units,
                "would_exceed_limit": would_exceed_limit,
                "estimated_rows_basis": "unknown_before_small_batch; retain ok_empty and parser failures as auditable outcomes",
                "future_bounded_sync_request": {
                    "enabled": false,
                    "plan_only": true,
                    "market": market,
                    "symbols": symbols,
                    "categories": categories,
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("exchange-announcement-order-capacity-{}", batch.label),
                    "reason": "p3.24g plan-only design; actual sync endpoint intentionally disabled"
                }
            })
        })
        .collect::<Vec<_>>();

    let calendar_day_count = (end - start).num_days() + 1;
    let request_key_count = batches.len() * request_keys_per_batch;
    let estimated_api_calls = batch_values
        .iter()
        .filter_map(|batch| batch.get("estimated_api_calls").and_then(Value::as_u64))
        .sum::<u64>();
    let would_exceed_limit = batch_values
        .iter()
        .any(|batch| batch.get("would_exceed_limit").and_then(Value::as_bool) == Some(true));

    json!({
        "audit_version": "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24G",
        "mode": "read_only_plan_only_calendar_day_symbol_category_sync_design",
        "write_enabled": false,
        "vendor": "akshare",
        "upstream": "cninfo",
        "vendor_endpoint": "stock_zh_a_disclosure_report_cninfo",
        "market": market,
        "symbols": symbols,
        "categories": categories,
        "date_range": {
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
            "calendar_day_count": calendar_day_count,
        },
        "request_key_policy": "symbol+category+calendar_date_range; do not restrict to open trading days because weekend/holiday announcements must map to next open session",
        "batch_mode": batch_mode,
        "recommended_batch_granularity": "quarter",
        "batch_count": batch_values.len(),
        "request_key_count": request_key_count,
        "query_count": estimated_api_calls,
        "estimated_api_calls": estimated_api_calls,
        "bounded_runner_query_unit_policy": "query_count equals tiny_slice_count * symbol_count * category_count, matching bounded-sync runner enforcement",
        "bounded_sync_limit": {
            "max_query_units": EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS,
            "would_exceed_limit": would_exceed_limit,
        },
        "sync_endpoint_status": if would_exceed_limit {
            "blocked_plan_exceeds_bounded_runner_query_unit_limit"
        } else {
            "disabled_plan_only_design_until_operator_schema_apply_and_small_batch_review"
        },
        "pit_mapping_policy": {
            "date_only_next_session": "announcementTime/date-only source_published_at must be mapped to the next open session before any trading feature can use it",
            "timestamp": "if a trusted source_published_at_ts is later available, same-day daily use still requires available_at <= trade_date and intraday use requires source_published_at_ts <= decision timestamp",
            "weekend_or_holiday_publication": "calendar-day sync must retain the publication date and map available_at to the next open trading session"
        },
        "raw_landing_policy": {
            "idempotent_key": ["vendor", "vendor_endpoint", "announcement_id", "symbol", "raw_payload_hash"],
            "permission_smoke_sample_cap": EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS,
            "raw_sync_row_landing_cap": exchange_announcement_order_capacity_raw_sync_row_limit(),
            "failure_sample_retention": "retain parser errors, ok_empty category outcomes, scanned_pdf_ocr_required and incomplete metadata as auditable raw outcomes",
            "text_evidence_retention": "retain text_content, text_hash, timestamp_candidates, pdf_metadata_keys and evidence_spans for manual precision review"
        },
        "batches": batch_values,
        "promotion_gate": {
            "schema_apply": "operator_action_required_before_any_raw_sync",
            "bounded_sync": "blocked_until_operator_schema_apply_and_small_batch_review",
            "coverage_audit": "blocked_until_small_batch_raw_sync_completes",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint never writes data_sync_task, raw tables, data versions, factors, WFA tasks, or strategy configs",
            "future_bounded_sync_request is illustrative and disabled; no POST sync endpoint is exposed in P3.24G",
            "do not collapse weekend or holiday announcements into previous trading days",
            "do not enter P3.10 until bounded sync, coverage, PIT, evidence precision and low-correlation audits pass"
        ],
        "next_step": "after explicit operator schema apply, run one small manually reviewed batch and then build coverage_pit_quality_audit"
    })
}

pub async fn exchange_announcement_order_capacity_schema_contract() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_exchange_announcement_order_capacity_schema_contract()
    }))
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/next-source-admission-plan
pub async fn exchange_announcement_order_capacity_next_source_admission_plan() -> impl IntoResponse
{
    Json(json!({
        "code": 0,
        "data": phase7_exchange_announcement_order_capacity_next_source_admission_plan()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/source-contract
pub async fn structured_order_capacity_price_chain_cninfo_access_smoke_contract(
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_cninfo_access_smoke_contract()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-contract
pub async fn structured_order_capacity_price_chain_cninfo_operator_evidence_contract(
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_contract()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-audit
pub async fn structured_order_capacity_price_chain_cninfo_operator_evidence_audit(
) -> impl IntoResponse {
    let manifest_path = env::var("QUANT_CNINFO_EVIDENCE_MANIFEST_PATH").ok();
    let parsed_manifest = manifest_path.as_deref().and_then(|path| {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|content| serde_json::from_str::<Value>(&content).ok())
    });
    let read_error = match manifest_path.as_deref() {
        Some(path) if !path.trim().is_empty() && parsed_manifest.is_none() => {
            Some("manifest_file_unreadable_or_invalid_json")
        }
        _ => None,
    };

    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
            manifest_path.as_deref(),
            parsed_manifest.as_ref(),
            read_error,
        )
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-permission-sample-smoke-plan
pub async fn structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan(
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan()
    }))
}

/// GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-manifest-template
pub async fn structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template(
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template()
    }))
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/manual-schema-review
pub async fn exchange_announcement_order_capacity_manual_schema_review() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_exchange_announcement_order_capacity_manual_schema_review()
    }))
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/sync-plan
pub async fn exchange_announcement_order_capacity_sync_plan(
    Query(req): Query<ExchangeAnnouncementOrderCapacitySyncPlanReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_sync_plan(req) {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit-contract
pub async fn exchange_announcement_order_capacity_coverage_quality_audit_contract(
) -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_exchange_announcement_order_capacity_coverage_quality_audit_contract()
    }))
}

/// POST /api/v1/quant/data/exchange-announcement-order-capacity/permission-smoke
pub async fn exchange_announcement_order_capacity_permission_smoke(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeAnnouncementOrderCapacitySmokeReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_permission_smoke(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/exchange-announcement-order-capacity/detail-audit
pub async fn exchange_announcement_order_capacity_detail_audit(
    Json(req): Json<ExchangeAnnouncementOrderCapacityDetailAuditReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_detail_audit(req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/pdf-parser-readiness
pub async fn exchange_announcement_order_capacity_pdf_parser_readiness(
    Query(req): Query<ExchangeAnnouncementOrderCapacityPdfParserReadinessReq>,
) -> impl IntoResponse {
    let python = exchange_announcement_order_capacity_pdf_audit_python_path(req.python);
    let checks = probe_exchange_announcement_order_capacity_pdf_parsers(&python).await;
    Json(json!({
        "code": 0,
        "data": exchange_announcement_order_capacity_pdf_parser_readiness_report(&python, checks)
    }))
}

/// POST /api/v1/quant/data/exchange-announcement-order-capacity/pdf-detail-audit
pub async fn exchange_announcement_order_capacity_pdf_detail_audit(
    Json(req): Json<ExchangeAnnouncementOrderCapacityPdfDetailAuditReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_pdf_detail_audit(req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/ocr-blocked-row-audit
pub async fn exchange_announcement_order_capacity_ocr_blocked_row_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_ocr_blocked_row_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/exchange-announcement-order-capacity/sync
pub async fn exchange_announcement_order_capacity_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeAnnouncementOrderCapacitySyncReq>,
) -> impl IntoResponse {
    match run_exchange_announcement_order_capacity_tiny_sync(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/exchange-announcement-order-capacity/bounded-sync
pub async fn exchange_announcement_order_capacity_bounded_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeAnnouncementOrderCapacityBoundedSyncReq>,
) -> impl IntoResponse {
    match run_exchange_announcement_order_capacity_bounded_sync(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit
pub async fn exchange_announcement_order_capacity_coverage_quality_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_coverage_quality_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/admission-readiness-audit
pub async fn exchange_announcement_order_capacity_admission_readiness_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ExchangeAnnouncementOrderCapacityAdmissionReadinessAuditReq>,
) -> impl IntoResponse {
    let coverage_req = ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
        start_date: req.start_date,
        end_date: req.end_date,
    };
    match build_exchange_announcement_order_capacity_coverage_quality_audit(&state, coverage_req)
        .await
    {
        Ok(coverage) => Json(json!({
            "code": 0,
            "data": exchange_announcement_order_capacity_admission_readiness_report(&coverage)
        })),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/manual-precision-sample-audit
pub async fn exchange_announcement_order_capacity_manual_precision_sample_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq>,
) -> impl IntoResponse {
    match build_exchange_announcement_order_capacity_manual_precision_sample_audit(&state, req)
        .await
    {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/futures-price-chain/readiness-audit
async fn build_exchange_announcement_order_capacity_permission_smoke(
    state: &AppState,
    mut req: ExchangeAnnouncementOrderCapacitySmokeReq,
) -> Result<Value, String> {
    if req.symbols.is_empty() {
        req.symbols = resolve_phase7_permission_smoke_symbols(state, &[])
            .await?
            .into_iter()
            .collect();
    }

    let plan = exchange_announcement_order_capacity_smoke_plan(&req)?;
    let symbols = plan
        .get("symbols")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let categories = plan
        .get("categories")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let market = plan
        .get("market")
        .and_then(Value::as_str)
        .unwrap_or("沪深京")
        .to_string();
    let python = plan
        .get("python")
        .and_then(Value::as_str)
        .unwrap_or("/tmp/akshare-smoke/bin/python")
        .to_string();
    let start_date = plan
        .get("date_range")
        .and_then(|date_range| date_range.get("start_date"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let end_date = plan
        .get("date_range")
        .and_then(|date_range| date_range.get("end_date"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let row_limit = plan
        .get("row_limit_per_probe")
        .and_then(Value::as_u64)
        .unwrap_or(PHASE7_PERMISSION_SMOKE_MAX_ROWS as u64) as usize;

    let mut probes = Vec::new();
    for symbol in &symbols {
        for category in &categories {
            probes.push(
                run_exchange_announcement_order_capacity_probe(
                    &python,
                    symbol,
                    &market,
                    category,
                    &start_date,
                    &end_date,
                    row_limit,
                )
                .await,
            );
        }
    }

    let ok_probe_count = probes
        .iter()
        .filter(|probe| {
            matches!(
                probe.get("status").and_then(Value::as_str),
                Some("ok" | "ok_empty")
            )
        })
        .count();
    let nonempty_probe_count = probes
        .iter()
        .filter(|probe| probe.get("status").and_then(Value::as_str) == Some("ok"))
        .count();
    let category_parser_error_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("category_parser_error")
        })
        .count();
    let runtime_error_count = probes
        .len()
        .saturating_sub(ok_probe_count + category_parser_error_count);
    let row_count_total = probes
        .iter()
        .filter_map(|probe| probe.get("row_count").and_then(Value::as_i64))
        .sum::<i64>();
    let incomplete_link_metadata_sample_rows = probes
        .iter()
        .filter_map(|probe| {
            probe
                .get("incomplete_link_metadata_sample_rows")
                .and_then(Value::as_u64)
        })
        .sum::<u64>();

    let admission_decision = if runtime_error_count > 0 {
        "blocked_runtime_or_permission_errors_before_schema_apply"
    } else if category_parser_error_count > 0 {
        "blocked_category_parser_errors_before_schema_apply"
    } else if nonempty_probe_count == 0 {
        "blocked_no_nonempty_symbol_category_history_evidence"
    } else if incomplete_link_metadata_sample_rows > 0 {
        "blocked_incomplete_cninfo_link_metadata_before_detail_text_audit"
    } else {
        "permission_history_category_smoke_passed_detail_text_timestamp_audit_required_next"
    };

    Ok(json!({
        "audit_version": "p3.24b-exchange-announcement-order-capacity-permission-history-category-smoke-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24B",
        "mode": "read_only_permission_history_category_smoke_no_write",
        "write_enabled": false,
        "plan": plan,
        "summary": {
            "probe_count": probes.len(),
            "ok_probe_count": ok_probe_count,
            "nonempty_probe_count": nonempty_probe_count,
            "category_parser_error_count": category_parser_error_count,
            "runtime_error_count": runtime_error_count,
            "row_count_total": row_count_total,
            "incomplete_link_metadata_sample_rows": incomplete_link_metadata_sample_rows,
        },
        "probes": probes,
        "admission_decision": admission_decision,
        "promotion_gate": {
            "schema_apply": "blocked_until_detail_text_timestamp_hash_and_manual_evidence_audit",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "if_smoke_passes_build_read_only_cninfo_detail_text_timestamp_audit_before_schema_apply",
        "guardrails": [
            "this endpoint never writes market_exchange_announcement_text_raw or data_sync_attempt",
            "permission/category smoke does not prove trainable alpha; it only decides whether detail text/timestamp audit is worth building",
            "category_parser_error and ok_empty are separate states and must not be treated as full-history coverage",
            "P3.10, WFA and v19 remain blocked until full coverage/PIT/text-evidence/correlation gates pass"
        ],
    }))
}

async fn build_exchange_announcement_order_capacity_detail_audit(
    req: ExchangeAnnouncementOrderCapacityDetailAuditReq,
) -> Result<Value, String> {
    let plan = exchange_announcement_order_capacity_detail_audit_plan(&req)?;
    let python = plan
        .get("python")
        .and_then(Value::as_str)
        .unwrap_or("/tmp/akshare-smoke/bin/python")
        .to_string();
    let links = plan
        .get("announcement_links")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut probes = Vec::new();
    for link in &links {
        probes.push(run_exchange_announcement_detail_probe(&python, link).await);
    }

    let summary = summarize_exchange_announcement_detail_probes(&probes);

    let decision = decide_exchange_announcement_detail_audit(
        probes.len(),
        summary.fetched_text_count,
        summary.text_hash_count,
        summary.source_published_at_count,
        summary.incomplete_link_metadata_count,
    );

    Ok(json!({
        "audit_version": "p3.24c-exchange-announcement-order-capacity-detail-text-timestamp-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24C",
        "mode": "read_only_cninfo_detail_text_timestamp_hash_audit_no_write",
        "write_enabled": false,
        "plan": plan,
        "summary": {
            "probe_count": probes.len(),
            "fetched_text_count": summary.fetched_text_count,
            "text_hash_count": summary.text_hash_count,
            "source_published_at_count": summary.source_published_at_count,
            "incomplete_link_metadata_count": summary.incomplete_link_metadata_count,
            "pdf_parser_required_count": summary.pdf_parser_required_count,
        },
        "probes": probes,
        "decision": decision,
        "admission_decision": decision["admission_decision"].clone(),
        "promotion_gate": decision["promotion_gate"].clone(),
        "next_step": "if_detail_audit_passes_prepare_manual_schema_review_for_bounded_raw_sync_design",
        "guardrails": [
            "this detail audit never writes market_exchange_announcement_text_raw or data_sync_attempt",
            "missing source_published_at timestamp blocks same-session and intraday use even when text fetch succeeds",
            "manual evidence span precision and category taxonomy audits are still required before P3.10",
            "schema review may proceed only after detail text/timestamp/hash audit passes"
        ],
    }))
}

async fn build_exchange_announcement_order_capacity_pdf_detail_audit(
    req: ExchangeAnnouncementOrderCapacityPdfDetailAuditReq,
) -> Result<Value, String> {
    let plan = exchange_announcement_order_capacity_pdf_detail_audit_plan(&req)?;
    let python = plan
        .get("python")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let links = plan
        .get("announcement_links")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut probes = Vec::new();
    for link in &links {
        probes.push(run_exchange_announcement_pdf_detail_probe(&python, link).await);
    }

    let summary = summarize_exchange_announcement_pdf_detail_probes(&probes);
    let decision = decide_exchange_announcement_order_capacity_pdf_detail_audit(
        probes.len(),
        summary.parsed_pdf_count,
        summary.stable_hash_count,
        summary.availability_count,
        summary.evidence_span_count,
        summary.scanned_pdf_count,
        summary.incomplete_link_metadata_count,
    );

    Ok(json!({
        "audit_version": "p3.24e-exchange-announcement-order-capacity-pdf-detail-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24E",
        "mode": "read_only_pdf_detail_text_timestamp_span_audit_no_write",
        "write_enabled": false,
        "plan": plan,
        "summary": {
            "probe_count": probes.len(),
            "parsed_pdf_count": summary.parsed_pdf_count,
            "stable_hash_count": summary.stable_hash_count,
            "availability_count": summary.availability_count,
            "timestamp_count": summary.timestamp_count,
            "next_session_policy_count": summary.next_session_policy_count,
            "evidence_span_count": summary.evidence_span_count,
            "incomplete_link_metadata_count": summary.incomplete_link_metadata_count,
            "scanned_pdf_count": summary.scanned_pdf_count,
            "runtime_not_configured_count": summary.runtime_not_configured_count,
        },
        "probes": probes,
        "decision": decision,
        "admission_decision": decision["admission_decision"].clone(),
        "promotion_gate": decision["promotion_gate"].clone(),
        "next_step": "if_pdf_detail_audit_passes_prepare_manual_schema_review_for_bounded_raw_sync_design",
        "guardrails": [
            "this pdf detail audit never writes market_exchange_announcement_text_raw or data_sync_attempt",
            "date-only next-session availability can support daily PIT use but never same-session intraday use",
            "scanned pdfs requiring OCR are blocked until a separate OCR admission path is reviewed",
            "schema review may proceed only after pdf text, availability policy, stable hash and evidence span audits pass"
        ],
    }))
}

async fn build_exchange_announcement_order_capacity_ocr_blocked_row_audit(
    state: &AppState,
    req: ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq,
) -> Result<Value, String> {
    let plan = exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(&req)?;
    let python = plan
        .get("python")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let start_date = plan
        .get("date_range")
        .and_then(|range| range.get("start_date"))
        .and_then(Value::as_str)
        .ok_or_else(|| "OCR blocked-row audit plan missing start_date".to_string())?;
    let end_date = plan
        .get("date_range")
        .and_then(|range| range.get("end_date"))
        .and_then(Value::as_str)
        .ok_or_else(|| "OCR blocked-row audit plan missing end_date".to_string())?;
    let start = parse_optional_date(Some(start_date))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end =
        parse_optional_date(Some(end_date))?.ok_or_else(|| "end_date is required".to_string())?;
    let symbols = plan
        .get("symbols")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let row_limit = plan.get("row_limit").and_then(Value::as_u64).unwrap_or(4) as i64;

    let table_exists = table_exists(&state.db, "market_exchange_announcement_text_raw").await?;
    if !table_exists {
        let decision = decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        );
        return Ok(json!({
            "audit_version": "p3.24t-exchange-announcement-order-capacity-scanned-pdf-ocr-audit-v1",
            "source_id": "exchange_announcement_order_capacity_text",
            "stage": "P3.24T",
            "mode": "read_only_scanned_pdf_ocr_candidate_audit_no_write",
            "write_enabled": false,
            "table_exists": false,
            "plan": plan,
            "summary": {
                "candidate_row_count": 0,
                "table_missing": true,
            },
            "probes": [],
            "decision": decision,
            "admission_decision": "blocked_market_exchange_announcement_text_raw_missing",
            "promotion_gate": decision["promotion_gate"].clone(),
        }));
    }

    let raw_rows = if symbols.is_empty() {
        sqlx::query(
            r#"
            SELECT symbol, symbol_name, announcement_id, org_id, announcement_category,
                   announcement_title, announcement_time, source_published_at,
                   source_published_at_quality, available_at, announcement_url,
                   pdf_final_url, event_type
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1
              AND announcement_time <= $2
              AND pdf_parse_status = 'scanned_pdf_ocr_required'
            ORDER BY announcement_time, symbol, announcement_id
            LIMIT $3
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(row_limit)
        .fetch_all(&state.db)
        .await
        .map_err(|error| format!("Failed to load scanned PDF rows for OCR audit: {error}"))?
    } else {
        sqlx::query(
            r#"
            SELECT symbol, symbol_name, announcement_id, org_id, announcement_category,
                   announcement_title, announcement_time, source_published_at,
                   source_published_at_quality, available_at, announcement_url,
                   pdf_final_url, event_type
            FROM market_exchange_announcement_text_raw
            WHERE announcement_time >= $1
              AND announcement_time <= $2
              AND pdf_parse_status = 'scanned_pdf_ocr_required'
              AND symbol = ANY($3)
            ORDER BY announcement_time, symbol, announcement_id
            LIMIT $4
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(&symbols)
        .bind(row_limit)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to load symbol-filtered scanned PDF rows for OCR audit: {error}")
        })?
    };

    let rows = raw_rows
        .into_iter()
        .map(|row| {
            let announcement_time = row
                .try_get::<NaiveDate, _>("announcement_time")
                .map(|date| date.to_string())
                .unwrap_or_default();
            let available_at = row
                .try_get::<NaiveDate, _>("available_at")
                .map(|date| date.to_string())
                .unwrap_or_default();
            json!({
                "symbol": row.try_get::<String, _>("symbol").unwrap_or_default(),
                "symbol_name": row.try_get::<Option<String>, _>("symbol_name").ok().flatten(),
                "announcement_id": row.try_get::<String, _>("announcement_id").unwrap_or_default(),
                "org_id": row.try_get::<String, _>("org_id").unwrap_or_default(),
                "announcement_category": row.try_get::<String, _>("announcement_category").unwrap_or_default(),
                "announcement_title": row.try_get::<String, _>("announcement_title").unwrap_or_default(),
                "announcement_time": announcement_time,
                "source_published_at": row.try_get::<String, _>("source_published_at").unwrap_or_default(),
                "source_published_at_quality": row.try_get::<String, _>("source_published_at_quality").unwrap_or_default(),
                "available_at": available_at,
                "announcement_url": row.try_get::<String, _>("announcement_url").unwrap_or_default(),
                "pdf_final_url": row.try_get::<Option<String>, _>("pdf_final_url").ok().flatten(),
                "event_type": row.try_get::<Option<String>, _>("event_type").ok().flatten(),
            })
        })
        .collect::<Vec<_>>();

    let mut probes = Vec::new();
    for row in &rows {
        probes.push(run_exchange_announcement_ocr_blocked_row_probe(&python, row.clone()).await);
    }

    let summary = summarize_exchange_announcement_ocr_blocked_row_probes(&probes);
    let decision = decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
        probes.len(),
        summary.ocr_text_count,
        summary.stable_hash_count,
        summary.availability_count,
        summary.quality_pass_count,
        summary.evidence_span_count,
        summary.runtime_missing_count,
        summary.ocr_error_count,
        summary.no_target_span_count,
        summary.incomplete_raw_link_count,
    );

    Ok(json!({
        "audit_version": "p3.24t-exchange-announcement-order-capacity-scanned-pdf-ocr-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24T",
        "mode": "read_only_scanned_pdf_ocr_candidate_audit_no_write",
        "write_enabled": false,
        "table_exists": true,
        "plan": plan,
        "summary": {
            "candidate_row_count": rows.len(),
            "ocr_text_count": summary.ocr_text_count,
            "stable_hash_count": summary.stable_hash_count,
            "availability_count": summary.availability_count,
            "quality_pass_count": summary.quality_pass_count,
            "evidence_span_count": summary.evidence_span_count,
            "runtime_missing_count": summary.runtime_missing_count,
            "ocr_error_count": summary.ocr_error_count,
            "no_target_span_count": summary.no_target_span_count,
            "incomplete_raw_link_count": summary.incomplete_raw_link_count,
        },
        "candidate_rows": rows,
        "probes": probes,
        "decision": decision,
        "admission_decision": decision["admission_decision"].clone(),
        "promotion_gate": decision["promotion_gate"].clone(),
        "guardrails": [
            "this audit never writes OCR output back to raw tables",
            "passing OCR only allows manual taxonomy review and explicit backfill/exclusion design",
            "factor_builder, P3.10, bounded WFA and v19 remain blocked"
        ],
    }))
}

// ── 第四批覆盖率测试：公告 OCR/审计域纯函数、决策函数与只读审计路径 ──
// 模式沿用 phase7_audit.rs third_batch：inner 直调，跳过 axum HTTP 层；
// 外部 Python/OCR 运行时统一用不存在的路径触发 runtime_not_configured 早退，无网络无 spawn。
#[cfg(test)]
mod fourth_batch {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
    }

    /// 构造真实本机 PG 连接（DATABASE_URL 缺省 postgres://gaocheng@localhost/quant）。
    /// Tushare 客户端经 dotenv 加载 quant/.env 中的 TUSHARE_TOKEN（cargo test cwd=quant-api 向上查找）。
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

    /// 构造元数据完整的 cninfo 公告链接（四要素齐全，metadata_complete=true）。
    fn complete_cninfo_link(announcement_id: &str) -> String {
        format!(
            "http://static.cninfo.com.cn/finalpage/2024-01-02/{announcement_id}.PDF\
             ?announcementId={announcement_id}&orgId=gssz0600001&stockCode=600000\
             &announcementTime=2024-01-02"
        )
    }

    // ── 纯函数：符号/CSV/类别/行数限制归一化 ──

    #[test]
    fn order_capacity_symbol_strips_exchange_suffix_and_trims() {
        // split('.') 取第一段；先 trim
        assert_eq!(
            exchange_announcement_order_capacity_symbol(" 600000.SH "),
            "600000"
        );
        assert_eq!(
            exchange_announcement_order_capacity_symbol("000001"),
            "000001"
        );
        // 无小数点时 unwrap_or 兜底返回 trim 后原值
        assert_eq!(
            exchange_announcement_order_capacity_symbol(" 300750.SZ "),
            "300750"
        );
    }

    #[test]
    fn order_capacity_symbols_normalizes_dedupes_and_caps_at_five() {
        let normalized = exchange_announcement_order_capacity_symbols(&[
            "600000.SH".to_string(),
            "600000".to_string(),
            " 000001.SZ ".to_string(),
            "".to_string(),
            "   ".to_string(),
        ]);
        assert_eq!(normalized, vec!["600000".to_string(), "000001".to_string()]);

        // 上限 5 个（EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_SYMBOLS），超出截断
        let six: Vec<String> = ["1", "2", "3", "4", "5", "6"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(exchange_announcement_order_capacity_symbols(&six).len(), 5);
    }

    #[test]
    fn order_capacity_csv_values_splits_trims_and_drops_empty() {
        assert_eq!(
            exchange_announcement_order_capacity_csv_values(Some("600000, 000001 ,,300750".into())),
            vec![
                "600000".to_string(),
                "000001".to_string(),
                "300750".to_string()
            ]
        );
        assert!(exchange_announcement_order_capacity_csv_values(None).is_empty());
        assert!(exchange_announcement_order_capacity_csv_values(Some(" , ".into())).is_empty());
    }

    #[test]
    fn order_capacity_categories_defaults_to_four_and_dedupes() {
        // 空入参 → 默认四类
        assert_eq!(
            exchange_announcement_order_capacity_categories(&[]),
            vec![
                "日常经营".to_string(),
                "重大事项".to_string(),
                "股权激励".to_string(),
                "并购重组".to_string(),
            ]
        );
        // 非空入参：trim + 去重 + 去空
        assert_eq!(
            exchange_announcement_order_capacity_categories(&[
                "日常经营".to_string(),
                " 日常经营 ".to_string(),
                "".to_string(),
                "并购重组".to_string(),
            ]),
            vec!["日常经营".to_string(), "并购重组".to_string()]
        );
        // 上限 6 类
        let many: Vec<String> = ["a", "b", "c", "d", "e", "f", "g"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            exchange_announcement_order_capacity_categories(&many).len(),
            6
        );
    }

    #[test]
    fn order_capacity_row_limit_defaults_to_five_and_clamps_to_max_twenty() {
        assert_eq!(exchange_announcement_order_capacity_row_limit(None), 5);
        assert_eq!(exchange_announcement_order_capacity_row_limit(Some(0)), 1);
        assert_eq!(exchange_announcement_order_capacity_row_limit(Some(7)), 7);
        assert_eq!(
            exchange_announcement_order_capacity_row_limit(Some(999)),
            20
        );
    }

    #[test]
    fn order_capacity_detail_links_trims_dedupes_and_clamps() {
        assert_eq!(
            exchange_announcement_order_capacity_detail_links(
                &[
                    " http://a ".to_string(),
                    "http://a".to_string(),
                    "".to_string()
                ],
                None
            ),
            vec!["http://a".to_string()]
        );
        // limit clamp [1, 20]，默认 5
        let links: Vec<String> = (0..30).map(|i| format!("http://l{i}")).collect();
        assert_eq!(
            exchange_announcement_order_capacity_detail_links(&links, None).len(),
            5
        );
        assert_eq!(
            exchange_announcement_order_capacity_detail_links(&links, Some(0)).len(),
            1
        );
        assert_eq!(
            exchange_announcement_order_capacity_detail_links(&links, Some(50)).len(),
            20
        );
    }

    // ── 纯函数：cninfo 链接解析 / 百分号解码 ──

    #[test]
    fn cninfo_percent_decode_decodes_hex_and_plus_and_keeps_invalid() {
        // 标准 URL 编码的 UTF-8 中文
        assert_eq!(cninfo_percent_decode("%E4%B8%AD%E6%96%87"), "中文");
        // '+' 解码为空格
        assert_eq!(cninfo_percent_decode("a+b"), "a b");
        // 非法十六进制序列原样保留
        assert_eq!(cninfo_percent_decode("%ZZ"), "%ZZ");
        // 尾部截断的 % 不消费后续字符
        assert_eq!(cninfo_percent_decode("100%"), "100%");
    }

    #[test]
    fn cninfo_query_param_returns_decoded_first_match() {
        let link = "http://x/?announcementId=12345&orgId=gssz0001&note=a%20b";
        assert_eq!(
            cninfo_query_param(link, "announcementId"),
            Some("12345".to_string())
        );
        assert_eq!(
            cninfo_query_param(link, "orgId"),
            Some("gssz0001".to_string())
        );
        // 值做百分号解码
        assert_eq!(cninfo_query_param(link, "note"), Some("a b".to_string()));
        // 键不存在 / 值为空 → None
        assert_eq!(cninfo_query_param(link, "missing"), None);
        assert_eq!(
            cninfo_query_param("http://x/?k=", "k"),
            None,
            "空值 trim 后为空应返回 None"
        );
        // 无查询串 → None
        assert_eq!(cninfo_query_param("http://x/path", "k"), None);
    }

    #[test]
    fn cninfo_announcement_id_from_path_strips_pdf_suffix() {
        assert_eq!(
            cninfo_announcement_id_from_path(
                "http://static.cninfo.com.cn/finalpage/2024-01-02/121399.PDF"
            ),
            Some("121399".to_string())
        );
        assert_eq!(
            cninfo_announcement_id_from_path("http://x/abc.pdf"),
            Some("abc".to_string())
        );
        // 无后缀取文件名本身
        assert_eq!(
            cninfo_announcement_id_from_path("http://x/dir/xyz987"),
            Some("xyz987".to_string())
        );
        // 路径末尾为空文件名 → None
        assert_eq!(cninfo_announcement_id_from_path("http://x/dir/"), None);
    }

    // ── 纯函数：日期/时间戳解析 ──

    #[test]
    fn parse_announcement_date_accepts_common_and_urlencoded_formats() {
        assert_eq!(
            parse_exchange_announcement_date("2024-01-02"),
            Some(date(2024, 1, 2))
        );
        assert_eq!(
            parse_exchange_announcement_date("20240102"),
            Some(date(2024, 1, 2))
        );
        assert_eq!(
            parse_exchange_announcement_date("2024/01/02"),
            Some(date(2024, 1, 2))
        );
        // RFC3339
        assert_eq!(
            parse_exchange_announcement_date("2024-01-02T08:30:00+08:00"),
            Some(date(2024, 1, 2))
        );
        // URL 编码后的本地日期时间（%20 → 空格，再走本地 datetime 分支取 date）
        assert_eq!(
            parse_exchange_announcement_date("2024-01-02%2008:30:00"),
            Some(date(2024, 1, 2))
        );
        assert_eq!(parse_exchange_announcement_date("not-a-date"), None);
        assert_eq!(parse_exchange_announcement_date(""), None);
    }

    #[test]
    fn parse_announcement_local_datetime_supports_dash_slash_and_cn_formats() {
        let expected = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(8, 30, 0)
            .unwrap();
        assert_eq!(
            parse_exchange_announcement_local_datetime("2024-01-02 08:30:00"),
            Some(expected)
        );
        // 'T' 分隔符被替换为空格
        assert_eq!(
            parse_exchange_announcement_local_datetime("2024-01-02T08:30"),
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(8, 30, 0)
        );
        assert_eq!(
            parse_exchange_announcement_local_datetime("2024/01/02 08:30:00"),
            Some(expected)
        );
        assert_eq!(
            parse_exchange_announcement_local_datetime("2024年01月02日 08:30"),
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(8, 30, 0)
        );
        assert_eq!(
            parse_exchange_announcement_local_datetime("2024-01-02"),
            None
        );
    }

    #[test]
    fn parse_announcement_timestamp_converts_shanghai_local_to_utc() {
        // RFC3339 带时区优先：+08:00 → UTC 00:30
        let rfc = parse_exchange_announcement_timestamp("2024-01-02T08:30:00+08:00").unwrap();
        assert_eq!(rfc.to_rfc3339(), "2024-01-02T00:30:00+00:00");
        // 无时区本地时间按上海 -8h（2024-01-02 08:30 本地 → 00:30Z）
        let local = parse_exchange_announcement_timestamp("2024-01-02 08:30:00").unwrap();
        assert_eq!(local.to_rfc3339(), "2024-01-02T00:30:00+00:00");
        assert_eq!(parse_exchange_announcement_timestamp("invalid"), None);
    }

    #[test]
    fn next_open_date_picks_first_later_open_date_else_next_day() {
        let opens = vec![date(2024, 1, 8), date(2024, 1, 9)];
        assert_eq!(
            exchange_announcement_next_open_date(date(2024, 1, 5), &opens),
            date(2024, 1, 8)
        );
        // 无更晚开市日 → 公告日 +1
        assert_eq!(
            exchange_announcement_next_open_date(date(2024, 1, 10), &opens),
            date(2024, 1, 11)
        );
        // 空开市列表同样回退 +1
        assert_eq!(
            exchange_announcement_next_open_date(date(2024, 1, 10), &[]),
            date(2024, 1, 11)
        );
    }

    // ── 纯决策函数 ──

    #[test]
    fn event_type_from_spans_prefers_order_then_capacity_then_price() {
        let spans = |themes: &[&str]| {
            json!(themes
                .iter()
                .map(|theme| json!({ "theme": theme }))
                .collect::<Vec<_>>())
        };
        assert_eq!(
            exchange_announcement_event_type_from_spans(&spans(&["order_contract", "capacity"])),
            Some("order_or_contract_signed".to_string())
        );
        assert_eq!(
            exchange_announcement_event_type_from_spans(&spans(&["capacity"])),
            Some("capacity_expansion_or_commissioning".to_string())
        );
        assert_eq!(
            exchange_announcement_event_type_from_spans(&spans(&["price"])),
            Some("product_price_adjustment".to_string())
        );
        assert_eq!(
            exchange_announcement_event_type_from_spans(&spans(&["other"])),
            None
        );
        assert_eq!(
            exchange_announcement_event_type_from_spans(&json!([])),
            None
        );
        // 非数组 → None
        assert_eq!(
            exchange_announcement_event_type_from_spans(&json!({})),
            None
        );
    }

    #[test]
    fn pdf_parse_status_maps_probe_status_to_landing_status() {
        let status =
            |value: &str| exchange_announcement_pdf_parse_status(&json!({ "status": value }));
        assert_eq!(status("ok"), "ok");
        assert_eq!(
            status("scanned_pdf_ocr_required"),
            "scanned_pdf_ocr_required"
        );
        assert_eq!(status("pdf_parse_empty"), "pdf_parse_empty");
        assert_eq!(status("not_pdf_after_redirect"), "error");
        assert_eq!(status("error"), "error");
        assert_eq!(status("timeout"), "error");
        assert_eq!(status("runtime_not_configured"), "error");
        assert_eq!(status("incomplete_link_metadata"), "error");
        assert_eq!(status("anything_else"), "pending_manual_review");
        // 缺 status 字段 → pending_manual_review
        assert_eq!(
            exchange_announcement_pdf_parse_status(&json!({})),
            "pending_manual_review"
        );
    }

    #[test]
    fn json_path_accessors_extract_or_default() {
        let payload = json!({
            "summary": { "row_count": 7, "ratio": 1.5, "items": [1, 2, 3] }
        });
        assert_eq!(
            exchange_announcement_order_capacity_json_path_i64(&payload, &["summary", "row_count"]),
            7
        );
        assert_eq!(
            exchange_announcement_order_capacity_json_path_i64(&payload, &["missing", "x"]),
            0
        );
        assert_eq!(
            exchange_announcement_order_capacity_json_path_f64(&payload, &["summary", "ratio"]),
            Some(1.5)
        );
        // 字符串数字不做隐式转换
        assert_eq!(
            exchange_announcement_order_capacity_json_path_f64(&json!({ "v": "1.5" }), &["v"]),
            None
        );
        assert_eq!(
            exchange_announcement_order_capacity_array_len(&payload["summary"], "items"),
            3
        );
        assert_eq!(
            exchange_announcement_order_capacity_array_len(&payload["summary"], "missing"),
            0
        );
    }

    // ── 纯函数：切片与批次窗口 ──

    #[test]
    fn tiny_slices_chunk_calendar_days_by_max_three() {
        let slices =
            exchange_announcement_order_capacity_tiny_slices(date(2024, 1, 1), date(2024, 1, 7));
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].label, "20240101-20240103");
        assert_eq!(slices[0].calendar_day_count, 3);
        assert_eq!(slices[1].start_date, date(2024, 1, 4));
        assert_eq!(slices[1].end_date, date(2024, 1, 6));
        // 尾部不足 3 天单独成片
        assert_eq!(slices[2].start_date, date(2024, 1, 7));
        assert_eq!(slices[2].end_date, date(2024, 1, 7));
        assert_eq!(slices[2].calendar_day_count, 1);

        // 恰好 3 天 → 单片
        let single =
            exchange_announcement_order_capacity_tiny_slices(date(2024, 2, 1), date(2024, 2, 3));
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].calendar_day_count, 3);
    }

    #[test]
    fn validate_bounded_window_enforces_single_month_or_quarter() {
        assert!(
            exchange_announcement_order_capacity_validate_bounded_window(
                date(2024, 1, 1),
                date(2024, 1, 31),
                "month"
            )
            .is_ok()
        );
        // 跨月拒绝
        assert!(
            exchange_announcement_order_capacity_validate_bounded_window(
                date(2024, 1, 1),
                date(2024, 2, 1),
                "month"
            )
            .is_err()
        );
        assert!(
            exchange_announcement_order_capacity_validate_bounded_window(
                date(2024, 1, 1),
                date(2024, 3, 31),
                "quarter"
            )
            .is_ok()
        );
        // 跨季拒绝（Q1 → Q2）
        assert!(
            exchange_announcement_order_capacity_validate_bounded_window(
                date(2024, 1, 1),
                date(2024, 4, 30),
                "quarter"
            )
            .is_err()
        );
        // 未知 batch 模式拒绝
        assert!(
            exchange_announcement_order_capacity_validate_bounded_window(
                date(2024, 1, 1),
                date(2024, 1, 31),
                "week"
            )
            .is_err()
        );
    }

    #[test]
    fn sync_plan_batches_reuses_analyst_revision_batch_labels() {
        // 年批次：整年一片，label 为年份
        let year_batches = exchange_announcement_order_capacity_sync_plan_batches(
            date(2024, 1, 1),
            date(2024, 12, 31),
            "year",
        )
        .expect("year batches");
        assert_eq!(year_batches.len(), 1);
        assert_eq!(year_batches[0].label, "2024");
        assert_eq!(year_batches[0].calendar_day_count, 366);

        // 月批次：一年 12 片
        let month_batches = exchange_announcement_order_capacity_sync_plan_batches(
            date(2024, 1, 1),
            date(2024, 12, 31),
            "month",
        )
        .expect("month batches");
        assert_eq!(month_batches.len(), 12);
        assert_eq!(month_batches[0].label, "202401");

        // 错误文案做了前缀替换（AkShare → exchange announcement）
        let error = exchange_announcement_order_capacity_sync_plan_batches(
            date(2024, 1, 1),
            date(2024, 1, 31),
            "invalid",
        )
        .expect_err("invalid batch mode");
        assert!(
            error.contains("exchange announcement order capacity sync-plan"),
            "实际错误: {error}"
        );
    }

    #[test]
    fn pdf_audit_python_path_prefers_explicit_request() {
        // 显式请求优先于环境变量
        assert_eq!(
            exchange_announcement_order_capacity_pdf_audit_python_path(Some(
                "/custom/pdf-python".into()
            )),
            "/custom/pdf-python"
        );
        // 空白请求回退（环境或默认），仅断言非空，避免依赖测试环境的 QUANT_PDF_AUDIT_PYTHON
        let fallback =
            exchange_announcement_order_capacity_pdf_audit_python_path(Some("   ".into()));
        assert!(!fallback.is_empty());
    }

    #[test]
    fn smoke_date_range_validates_order_and_defaults_to_one_year_window() {
        let empty_req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: Vec::new(),
            categories: Vec::new(),
            market: None,
            start_date: None,
            end_date: None,
            limit: None,
            python: None,
        };
        // start > end 拒绝
        let inverted = ExchangeAnnouncementOrderCapacitySmokeReq {
            start_date: Some("20240105".into()),
            end_date: Some("20240101".into()),
            ..empty_req.clone()
        };
        assert!(exchange_announcement_order_capacity_date_range(&inverted).is_err());

        // 非法格式拒绝（parse_optional_date 要求 YYYYMMDD）
        let malformed = ExchangeAnnouncementOrderCapacitySmokeReq {
            start_date: Some("2024-01-05".into()),
            end_date: Some("20240110".into()),
            ..empty_req.clone()
        };
        assert!(exchange_announcement_order_capacity_date_range(&malformed).is_err());

        // 缺省：start=今天-365，end=今天，且 start <= end
        let (start, end) =
            exchange_announcement_order_capacity_date_range(&empty_req).expect("default range");
        assert_eq!(start.len(), 8);
        assert_eq!(end.len(), 8);
        assert!(start <= end);
    }

    // ── 私有纯函数：probe 汇总与元数据附加 ──

    #[test]
    fn summarize_pdf_detail_probes_counts_each_status_bucket() {
        let probes = vec![
            // ok + 稳定哈希 + timestamp 质量 + 证据 span
            json!({
                "status": "ok",
                "hash_stable": true,
                "text_hash": "abc",
                "source_published_at_quality": "timestamp",
                "evidence_spans": [{ "theme": "order_contract" }],
            }),
            // ok + date_only_next_session 质量（无哈希）
            json!({
                "status": "ok",
                "source_published_at_quality": "date_only_next_session",
            }),
            // 扫描件需 OCR
            json!({ "status": "scanned_pdf_ocr_required" }),
            // 链接元数据缺失（按 status 统计）
            json!({ "status": "incomplete_link_metadata" }),
            // 链接元数据缺失（按 metadata_complete=false 统计）
            json!({
                "status": "ok",
                "link_metadata": { "metadata_complete": false },
            }),
            // 运行时未配置
            json!({ "status": "runtime_not_configured" }),
        ];
        let summary = summarize_exchange_announcement_pdf_detail_probes(&probes);
        assert_eq!(summary.parsed_pdf_count, 3);
        assert_eq!(summary.stable_hash_count, 1);
        assert_eq!(summary.timestamp_count, 1);
        assert_eq!(summary.next_session_policy_count, 1);
        assert_eq!(summary.availability_count, 2);
        assert_eq!(summary.evidence_span_count, 1);
        assert_eq!(summary.incomplete_link_metadata_count, 2);
        assert_eq!(summary.scanned_pdf_count, 1);
        assert_eq!(summary.runtime_not_configured_count, 1);
    }

    #[test]
    fn summarize_ocr_blocked_row_probes_counts_each_status_bucket() {
        let probes = vec![
            // 完整 ok：OCR 文本 + 哈希 + 可用性 + 质量 + span
            json!({
                "status": "ok",
                "ocr_text_length": 120,
                "hash_stable": true,
                "ocr_text_hash": "abc",
                "source_published_at_quality": "timestamp",
                "available_at": "2024-01-03",
                "ocr_quality": { "status": "passed" },
                "evidence_spans": [{ "theme": "capacity" }],
            }),
            // ok 但无证据 span（缺字段按空处理）→ no_target_span
            json!({ "status": "ok" }),
            // ok + date_only_next_session 质量 + available_at
            json!({
                "status": "ok",
                "source_published_at_quality": "date_only_next_session",
                "available_at": "2024-01-03",
            }),
            // OCR 运行时缺失（两种状态都计入）
            json!({ "status": "ocr_runtime_not_configured" }),
            json!({ "status": "ocr_dependency_missing" }),
            // OCR 执行错误（error/timeout/ocr_error/pdf_fetch_error）
            json!({ "status": "ocr_error" }),
            json!({ "status": "pdf_fetch_error" }),
            // 原始 PDF 链接缺失
            json!({ "status": "incomplete_raw_pdf_link" }),
        ];
        let summary = summarize_exchange_announcement_ocr_blocked_row_probes(&probes);
        assert_eq!(summary.ocr_text_count, 1);
        assert_eq!(summary.stable_hash_count, 1);
        assert_eq!(summary.availability_count, 2);
        assert_eq!(summary.quality_pass_count, 1);
        assert_eq!(summary.evidence_span_count, 1);
        assert_eq!(summary.runtime_missing_count, 2);
        assert_eq!(summary.ocr_error_count, 2);
        assert_eq!(summary.no_target_span_count, 2);
        assert_eq!(summary.incomplete_raw_link_count, 1);
    }

    #[test]
    fn attach_link_metadata_enriches_payload_with_sample_counts() {
        let complete = complete_cninfo_link("121399");
        let payload = json!({
            "sample_rows": [
                { "公告链接": complete },
                { "公告链接": "http://broken/link-without-query" },
                { "其它字段": "无公告链接的行被跳过" },
            ]
        });
        let enriched = attach_exchange_announcement_link_metadata(payload);
        let metadata = enriched["link_metadata_sample"].as_array().unwrap();
        assert_eq!(metadata.len(), 2);
        assert!(metadata[0]["metadata_complete"].as_bool().unwrap());
        assert!(!metadata[1]["metadata_complete"].as_bool().unwrap());
        assert_eq!(enriched["complete_link_metadata_sample_rows"], 1);
        assert_eq!(enriched["incomplete_link_metadata_sample_rows"], 1);

        // 非 object payload 原样返回（无字段可插入）
        let array_payload = json!([1, 2]);
        assert_eq!(
            attach_exchange_announcement_link_metadata(array_payload),
            json!([1, 2])
        );
    }

    #[test]
    fn operator_evidence_forbidden_keys_cover_secret_and_leak_names() {
        let keys = cninfo_operator_evidence_forbidden_keys();
        assert_eq!(keys.len(), 13);
        for expected in [
            "api_token",
            "password",
            "cookie",
            "authorization",
            "secret",
            "token",
            "raw_payload",
            "oos_performance_label",
        ] {
            assert!(keys.contains(&expected), "缺少禁用键: {expected}");
        }
    }

    // ── 私有 async probe：不完整链接 / 缺失运行时早退（无网络无 spawn）──

    #[tokio::test]
    async fn detail_probe_short_circuits_on_incomplete_link_metadata() {
        // 无查询串且路径末段为空 → announcementId 无 path 兜底，四项元数据全缺
        let probe =
            run_exchange_announcement_detail_probe("/nonexistent-quant-test-python", "http://x/")
                .await;
        assert_eq!(probe["status"], "incomplete_link_metadata");
        assert_eq!(probe["permission"], "unknown_or_unavailable");
        let missing = probe["link_metadata"]["missing_fields"].as_array().unwrap();
        assert_eq!(missing.len(), 4, "四项元数据全缺");
    }

    #[tokio::test]
    async fn detail_probe_reports_runtime_not_configured_for_complete_link() {
        let probe = run_exchange_announcement_detail_probe(
            "/nonexistent-quant-test-python",
            &complete_cninfo_link("121399"),
        )
        .await;
        // 元数据完整但 Python 运行时缺失 → runtime_not_configured（不 spawn）
        assert_eq!(probe["status"], "runtime_not_configured");
        assert_eq!(probe["python"], "/nonexistent-quant-test-python");
        assert!(probe["link_metadata"]["metadata_complete"]
            .as_bool()
            .unwrap());
    }

    #[tokio::test]
    async fn pdf_detail_probe_reports_runtime_not_configured_for_complete_link() {
        let probe = run_exchange_announcement_pdf_detail_probe(
            "/nonexistent-quant-test-python",
            &complete_cninfo_link("121399"),
        )
        .await;
        assert_eq!(probe["status"], "runtime_not_configured");
        assert!(probe["link_metadata"]["metadata_complete"]
            .as_bool()
            .unwrap());

        // 不完整链接同样短路
        let incomplete = run_exchange_announcement_pdf_detail_probe(
            "/nonexistent-quant-test-python",
            "http://x/bare",
        )
        .await;
        assert_eq!(incomplete["status"], "incomplete_link_metadata");
    }

    #[tokio::test]
    async fn ocr_blocked_row_probe_short_circuits_without_link_or_runtime() {
        // 行内无任何 PDF 链接 → incomplete_raw_pdf_link
        let no_link = run_exchange_announcement_ocr_blocked_row_probe(
            "/nonexistent-quant-test-python",
            json!({ "symbol": "600000" }),
        )
        .await;
        assert_eq!(no_link["status"], "incomplete_raw_pdf_link");

        // 有链接但运行时缺失 → ocr_runtime_not_configured
        let runtime_missing = run_exchange_announcement_ocr_blocked_row_probe(
            "/nonexistent-quant-test-python",
            json!({ "pdf_final_url": "http://x/a.pdf" }),
        )
        .await;
        assert_eq!(runtime_missing["status"], "ocr_runtime_not_configured");
        assert_eq!(runtime_missing["pdf_url"], "http://x/a.pdf");
    }

    #[tokio::test]
    async fn pdf_parser_probe_reports_missing_runtime_without_spawning() {
        let checks = probe_exchange_announcement_order_capacity_pdf_parsers(
            "/nonexistent-quant-test-python",
        )
        .await;
        // 早退：pdftotext 二进制检查 + python_runtime 检查，共 2 项
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0]["tool"], "pdftotext");
        assert_eq!(checks[1]["tool"], "python_runtime");
        assert_eq!(checks[1]["available"], false);
        assert_eq!(checks[1]["python"], "/nonexistent-quant-test-python");
    }

    // ── build 函数：拒绝分支与 runtime_not_configured 全路径 ──

    #[tokio::test]
    async fn build_detail_audit_rejects_empty_links() {
        let req = ExchangeAnnouncementOrderCapacityDetailAuditReq {
            announcement_links: Vec::new(),
            limit: None,
            python: None,
        };
        let error = build_exchange_announcement_order_capacity_detail_audit(req)
            .await
            .expect_err("empty links");
        assert!(
            error.contains("requires at least one announcement link"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn build_pdf_detail_audit_rejects_empty_links() {
        let req = ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
            announcement_links: Vec::new(),
            limit: None,
            python: None,
        };
        let error = build_exchange_announcement_order_capacity_pdf_detail_audit(req)
            .await
            .expect_err("empty links");
        assert!(
            error.contains("requires at least one announcement link"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn build_detail_audit_walks_runtime_not_configured_path() {
        // 完整链接 + 不存在的 Python：probe 返回 runtime_not_configured，
        // 汇总与决策完整走通（无网络、无 spawn）
        let req = ExchangeAnnouncementOrderCapacityDetailAuditReq {
            announcement_links: vec![complete_cninfo_link("121399")],
            limit: Some(1),
            python: Some("/nonexistent-quant-test-python".into()),
        };
        let report = build_exchange_announcement_order_capacity_detail_audit(req)
            .await
            .expect("detail audit report");
        assert_eq!(report["summary"]["probe_count"], 1);
        assert_eq!(report["probes"][0]["status"], "runtime_not_configured");
        // fetched_text_count(0) < total_links(1) → 文本抓取不完整
        assert_eq!(
            report["decision"]["admission_decision"],
            "blocked_detail_text_fetch_incomplete"
        );
        assert_eq!(report["write_enabled"], false);
    }

    #[tokio::test]
    async fn build_pdf_detail_audit_walks_runtime_not_configured_path() {
        let req = ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
            announcement_links: vec![complete_cninfo_link("121399")],
            limit: Some(1),
            python: Some("/nonexistent-quant-test-python".into()),
        };
        let report = build_exchange_announcement_order_capacity_pdf_detail_audit(req)
            .await
            .expect("pdf detail audit report");
        assert_eq!(report["summary"]["probe_count"], 1);
        assert_eq!(report["summary"]["runtime_not_configured_count"], 1);
        assert_eq!(report["summary"]["parsed_pdf_count"], 0);
        // incomplete=0、scanned=0、parsed(0)<total(1) → PDF 抓取/解析不完整
        assert_eq!(
            report["decision"]["admission_decision"],
            "blocked_pdf_fetch_or_parse_incomplete"
        );
    }

    // ── handler inner：验证早退（不触库不写库）──

    #[tokio::test]
    async fn tiny_sync_rejects_background_requests_before_any_db_access() {
        let state = test_app_state().await;
        let req = ExchangeAnnouncementOrderCapacitySyncReq {
            symbols: None,
            categories: None,
            market: None,
            start_date: None,
            end_date: None,
            data_version_id: None,
            python: None,
            pdf_python: None,
            background: true,
        };
        let error = run_exchange_announcement_order_capacity_tiny_sync(&state, req)
            .await
            .expect_err("background=true");
        assert!(
            error.contains("requires background=false"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn bounded_sync_rejects_background_requests_before_any_db_access() {
        let state = test_app_state().await;
        let req = ExchangeAnnouncementOrderCapacityBoundedSyncReq {
            symbols: None,
            categories: None,
            market: None,
            start_date: None,
            end_date: None,
            batch: None,
            data_version_id: None,
            python: None,
            pdf_python: None,
            background: true,
            stop_on_audit_failure: None,
        };
        let error = run_exchange_announcement_order_capacity_bounded_sync(&state, req)
            .await
            .expect_err("background=true");
        assert!(
            error.contains("requires background=false"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn ocr_blocked_row_audit_rejects_inverted_date_range() {
        let state = test_app_state().await;
        let req = ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
            start_date: Some("20240105".into()),
            end_date: Some("20240101".into()),
            symbols: None,
            limit: None,
            python: None,
        };
        let error = build_exchange_announcement_order_capacity_ocr_blocked_row_audit(&state, req)
            .await
            .expect_err("inverted range");
        assert!(
            error.contains("cannot be after end_date"),
            "实际错误: {error}"
        );
    }

    // ── 连库只读审计：真实本机 PG，只 SELECT 不写入 ──

    #[tokio::test]
    async fn coverage_quality_audit_returns_read_only_structure() {
        let state = test_app_state().await;
        let req = ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
            start_date: Some("20240101".into()),
            end_date: Some("20240102".into()),
        };
        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(&state, req)
            .await
            .expect("coverage quality audit report");
        // 结构断言：不臆测本机库数据值，只验证报告骨架与回显
        assert_eq!(
            report["mode"],
            "read_only_db_backed_small_batch_coverage_pit_quality_audit"
        );
        assert_eq!(report["table"], "market_exchange_announcement_text_raw");
        assert_eq!(report["date_range"]["start_date"], "20240101");
        assert_eq!(report["date_range"]["end_date"], "20240102");
        assert!(report["table_exists"].is_boolean());
        for field in [
            "row_count",
            "pit_violation_rows",
            "target_event_rows",
            "scanned_pdf_ocr_required_rows",
            "completed_attempts",
        ] {
            assert!(
                report["summary"][field].is_i64(),
                "summary.{field} 应为整数，实际: {}",
                report["summary"][field]
            );
        }
        assert!(report["decision"].is_object());
        assert!(report["promotion_gate"]["factor_builder"].is_string());
        assert!(report["year_category_breakdown"].is_array());
        assert!(report["symbol_event_breakdown"].is_array());
    }

    #[tokio::test]
    async fn manual_precision_sample_audit_returns_read_only_structure() {
        let state = test_app_state().await;
        let req = ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq {
            start_date: Some("20240101".into()),
            end_date: Some("20240102".into()),
            limit: Some(3),
            include_negative_samples: Some(false),
        };
        let report =
            build_exchange_announcement_order_capacity_manual_precision_sample_audit(&state, req)
                .await
                .expect("manual precision sample audit report");
        // 表存在时输出样本骨架；表缺失时输出 blocked_raw_schema_not_applied——两种都合法
        if report["status"] == "blocked_raw_schema_not_applied" {
            assert_eq!(report["table_exists"], false);
        } else {
            assert!(report["review_items"].is_array());
            assert!(report["sample_policy"]["negative_samples"].is_boolean());
            // include_negative_samples=false → 负样本上限为 0
            assert_eq!(report["sample_policy"]["negative_sample_limit"], 0);
        }
        assert_eq!(report["date_range"]["start_date"], "20240101");
        assert_eq!(
            report["promotion_gate"]["p310_status"],
            "blocked_until_manual_precision_review_passes"
        );
    }

    #[tokio::test]
    async fn permission_smoke_reports_runtime_errors_without_spawning() {
        let state = test_app_state().await;
        // 显式 symbol + 默认类别（4 类）：1×4 个 probe，
        // Python 运行时不存在 → 全部 runtime_not_configured（秒回，不 spawn）
        // 该 Req 无 Default derive，手工列全字段（其余走 None/空默认语义）
        let req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: vec!["600000.SH".to_string()],
            categories: vec![],
            market: None,
            start_date: None,
            end_date: None,
            limit: None,
            python: Some("/nonexistent-quant-test-python".into()),
        };
        let report = build_exchange_announcement_order_capacity_permission_smoke(&state, req)
            .await
            .expect("permission smoke report");
        assert_eq!(report["summary"]["probe_count"], 4);
        assert_eq!(report["summary"]["runtime_error_count"], 4);
        assert_eq!(report["summary"]["nonempty_probe_count"], 0);
        assert_eq!(report["write_enabled"], false);
        assert_eq!(
            report["admission_decision"],
            "blocked_runtime_or_permission_errors_before_schema_apply"
        );
        assert_eq!(report["plan"]["symbols"][0], "600000");
    }
}

/// 第十批：连库写入（upsert raw rows）、交易日历读取、审计中段 SQL 分支补齐、纯函数缺口。
/// 造数窗口全部使用 2099/2100 年段（真实数据 2023-2025，零干扰）；
/// 每个测试前置+结尾双端精确清理（前缀 zzz_test_ea10_ / zzz10），前缀键全局唯一可并行。
#[cfg(test)]
mod tenth_batch {
    use super::*;
    use chrono::TimeZone;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
    }

    fn ts(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, s)
            .single()
            .expect("valid datetime")
    }

    /// 裸 PG 连接（calendar/upsert 直调无需 Tushare 客户端的测试用）。
    async fn test_db_pool() -> sqlx::PgPool {
        dotenv::dotenv().ok();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 完整 AppState（审计 build 函数需要；同 fourth_batch::test_app_state）。
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

    async fn cleanup_ea10_raw_rows(db: &sqlx::PgPool, announcement_id_prefix: &str) {
        sqlx::query(
            "DELETE FROM market_exchange_announcement_text_raw WHERE announcement_id LIKE $1",
        )
        .bind(format!("{announcement_id_prefix}%"))
        .execute(db)
        .await
        .expect("cleanup ea10 raw rows");
    }

    async fn cleanup_ea10_attempts(db: &sqlx::PgPool, symbol_prefix: &str) {
        sqlx::query("DELETE FROM data_sync_attempt WHERE symbol LIKE $1")
            .bind(format!("{symbol_prefix}%"))
            .execute(db)
            .await
            .expect("cleanup ea10 attempts");
    }

    async fn cleanup_ea10_calendar(db: &sqlx::PgPool) {
        sqlx::query("DELETE FROM market_trade_calendar WHERE exchange = 'zzz10_cx'")
            .execute(db)
            .await
            .expect("cleanup ea10 calendar");
    }

    /// 审计造数通用行：质量-时间戳语义由 quality 参数推导（满足 CHECK 约束）。
    #[allow(clippy::too_many_arguments)]
    async fn seed_ea10_raw_row(
        db: &sqlx::PgPool,
        announcement_id: &str,
        symbol: &str,
        category: &str,
        title: &str,
        announcement_time: NaiveDate,
        available_at: NaiveDate,
        quality: &str,
        pdf_parse_status: &str,
        event_type: Option<&str>,
        evidence_spans: Value,
        raw_payload_hash: &str,
        text_content: Option<&str>,
    ) {
        let source_published_at_ts: Option<DateTime<Utc>> = (quality == "timestamp").then(|| {
            announcement_time
                .and_hms_opt(1, 2, 3)
                .expect("valid time")
                .and_utc()
        });
        let source_published_date: Option<NaiveDate> =
            (quality == "date_only_next_session").then_some(announcement_time);
        sqlx::query(
            r#"
            INSERT INTO market_exchange_announcement_text_raw
            (vendor, vendor_endpoint, request_key, symbol, announcement_id,
             announcement_category, announcement_title, announcement_time,
             source_published_at, source_published_at_ts, source_published_date,
             source_published_at_quality, available_at, announcement_url,
             pdf_parse_status, event_type, evidence_spans, text_content,
             raw_payload, raw_payload_hash)
            VALUES ('zzz_test_ea10_vendor', 'zzz_test_ea10_audit_ep', 'zzz_test_ea10_seed', $1, $2,
                    $3, $4, $5, 'zzz-source-published-at', $6, $7, $8, $9,
                    'http://zzz.test/a.PDF', $10, $11, $12, $13, '{}'::jsonb, $14)
            "#,
        )
        .bind(symbol)
        .bind(announcement_id)
        .bind(category)
        .bind(title)
        .bind(announcement_time)
        .bind(source_published_at_ts)
        .bind(source_published_date)
        .bind(quality)
        .bind(available_at)
        .bind(pdf_parse_status)
        .bind(event_type)
        .bind(evidence_spans)
        .bind(text_content)
        .bind(raw_payload_hash)
        .execute(db)
        .await
        .expect("seed ea10 raw row");
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_ea10_attempt(
        db: &sqlx::PgPool,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        status: &str,
        row_count: i64,
        error_message: Option<&str>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO data_sync_attempt (source, symbol, start_date, end_date, status, row_count, error_message)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE)
        .bind(symbol)
        .bind(start)
        .bind(end)
        .bind(status)
        .bind(row_count)
        .bind(error_message)
        .execute(db)
        .await
        .expect("seed ea10 attempt");
    }

    /// 分布数组按名称取 rows 计数（quality / pdf_parse_status / event_type 通用）。
    fn dist_rows(entries: &Value, key: &str, name: &str) -> Option<i64> {
        entries.as_array().and_then(|array| {
            array
                .iter()
                .find(|entry| entry[key] == name)
                .and_then(|entry| entry["rows"].as_i64())
        })
    }

    fn coverage_audit_req(
        start: &str,
        end: &str,
    ) -> ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
        ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
            start_date: Some(start.to_string()),
            end_date: Some(end.to_string()),
        }
    }

    // ── 纯函数：cninfo_percent_decode_strict 缺口分支 ──

    #[test]
    fn strict_percent_decode_decodes_utf8_and_keeps_plus_sign() {
        // %XX 多字节序列解码为 UTF-8 中文；`+` 不转空格（与表单版 decode 的核心差异）
        assert_eq!(cninfo_percent_decode_strict("%E4%B8%AD%E6%96%87"), "中文");
        assert_eq!(cninfo_percent_decode_strict("a+b c"), "a+b c");
        // RFC3339 正 offset 场景（2026-09-21 修复回归锚点）
        assert_eq!(
            cninfo_percent_decode_strict("2024-01-02T09%3A00%3A00%2B08%3A00"),
            "2024-01-02T09:00:00+08:00"
        );
        // 混合：普通字符与转义共存
        assert_eq!(cninfo_percent_decode_strict("id%3D42%26x%3D1"), "id=42&x=1");
    }

    #[test]
    fn strict_percent_decode_keeps_invalid_and_truncated_percent_sequences() {
        // 非法十六进制：原样保留 '%'，后续字符继续逐字节处理
        assert_eq!(cninfo_percent_decode_strict("%GG"), "%GG");
        assert_eq!(cninfo_percent_decode_strict("%zz%41"), "%zzA");
        // 尾部截断：'%' 后不足两个字符（index + 2 < len 不成立）按字面保留
        assert_eq!(cninfo_percent_decode_strict("abc%"), "abc%");
        assert_eq!(cninfo_percent_decode_strict("a%4"), "a%4");
        // 末位完整三字符序列仍正常解码
        assert_eq!(cninfo_percent_decode_strict("%41"), "A");
        assert_eq!(cninfo_percent_decode_strict("x%4F"), "xO");
        // 空串
        assert_eq!(cninfo_percent_decode_strict(""), "");
    }

    #[test]
    fn strict_percent_decode_falls_back_to_original_on_invalid_utf8() {
        // %FF 单字节不是合法 UTF-8 序列 → from_utf8 失败回退原始字符串
        assert_eq!(cninfo_percent_decode_strict("%FF"), "%FF");
        // 合法前缀 + 非法尾字节：整体解码失败同样回退原文
        assert_eq!(cninfo_percent_decode_strict("ok%FF"), "ok%FF");
    }

    // ── 纯函数：probe 汇总缺口分支（只补 fourth_batch 未覆盖的形态）──

    #[test]
    fn pdf_detail_summary_counts_quality_without_ok_status() {
        let probes = vec![
            // 非 ok 状态但质量为 timestamp → timestamp/availability 仍计数（过滤不看 status）
            json!({ "status": "error", "source_published_at_quality": "timestamp" }),
            // ok + hash_stable 但缺 text_hash → 不计入 stable_hash_count
            json!({ "status": "ok", "hash_stable": true }),
            // ok + evidence_spans 非数组 → 不计入 evidence_span_count
            json!({ "status": "ok", "evidence_spans": { "theme": "order_contract" } }),
            // 非 ok + hash 三件套齐全 → stable 仍不计数
            json!({ "status": "scanned_pdf_ocr_required", "hash_stable": true, "text_hash": "h" }),
        ];
        let summary = summarize_exchange_announcement_pdf_detail_probes(&probes);
        assert_eq!(summary.parsed_pdf_count, 2);
        assert_eq!(summary.stable_hash_count, 0);
        assert_eq!(summary.timestamp_count, 1);
        assert_eq!(summary.next_session_policy_count, 0);
        assert_eq!(summary.availability_count, 1);
        assert_eq!(summary.evidence_span_count, 0);
        assert_eq!(summary.incomplete_link_metadata_count, 0);
        assert_eq!(summary.scanned_pdf_count, 1);
        assert_eq!(summary.runtime_not_configured_count, 0);
    }

    #[test]
    fn pdf_detail_summary_empty_probes_report_all_zeroes() {
        let summary = summarize_exchange_announcement_pdf_detail_probes(&[]);
        assert_eq!(summary.parsed_pdf_count, 0);
        assert_eq!(summary.stable_hash_count, 0);
        assert_eq!(summary.availability_count, 0);
        assert_eq!(summary.incomplete_link_metadata_count, 0);
        assert_eq!(summary.runtime_not_configured_count, 0);
    }

    #[test]
    fn ocr_blocked_summary_counts_error_timeout_and_partial_conditions() {
        let probes = vec![
            // error / timeout 状态计入 ocr_error_count（fourth_batch 只测过 ocr_error/pdf_fetch_error）
            json!({ "status": "error" }),
            json!({ "status": "timeout" }),
            // ok 但 ocr_text_length=0 → 不计 ocr_text_count，且计入 no_target_span
            json!({ "status": "ok", "ocr_text_length": 0 }),
            // ok + hash_stable 但缺 ocr_text_hash → 不计 stable_hash_count
            json!({ "status": "ok", "hash_stable": true }),
            // 质量达标但缺 available_at → 不计 availability_count
            json!({ "status": "scanned_pdf_ocr_required", "source_published_at_quality": "timestamp" }),
            // 非 ok 状态带 evidence_spans → evidence_span_count 仍计数（过滤不看 status）
            json!({ "status": "scanned_pdf_ocr_required", "evidence_spans": [{ "theme": "capacity" }] }),
        ];
        let summary = summarize_exchange_announcement_ocr_blocked_row_probes(&probes);
        assert_eq!(summary.ocr_text_count, 0);
        assert_eq!(summary.stable_hash_count, 0);
        assert_eq!(summary.availability_count, 0);
        assert_eq!(summary.quality_pass_count, 0);
        assert_eq!(summary.evidence_span_count, 1);
        assert_eq!(summary.runtime_missing_count, 0);
        assert_eq!(summary.ocr_error_count, 2);
        // no_target_span 判定=status=="ok" 且 evidence_spans 为空/缺失：probe3(ok,无spans)与
        // probe4(ok,无spans)均命中，故为 2（非 1，此前漏算 probe4）
        assert_eq!(summary.no_target_span_count, 2);
        assert_eq!(summary.incomplete_raw_link_count, 0);
    }

    #[test]
    fn ocr_blocked_summary_empty_probes_report_all_zeroes() {
        let summary = summarize_exchange_announcement_ocr_blocked_row_probes(&[]);
        assert_eq!(summary.ocr_text_count, 0);
        assert_eq!(summary.stable_hash_count, 0);
        assert_eq!(summary.availability_count, 0);
        assert_eq!(summary.ocr_error_count, 0);
        assert_eq!(summary.no_target_span_count, 0);
    }

    // ── 连库写入：upsert_exchange_announcement_order_capacity_raw_rows ──

    fn zzz_upsert_row(
        announcement_id: &str,
        symbol: &str,
        raw_payload_hash: &str,
    ) -> ExchangeAnnouncementOrderCapacityRawRow {
        ExchangeAnnouncementOrderCapacityRawRow {
            vendor: "zzz_test_ea10_vendor".to_string(),
            vendor_endpoint: "zzz_test_ea10_upsert_ep".to_string(),
            request_key: format!("zzz_test_ea10_rk_{announcement_id}"),
            symbol: symbol.to_string(),
            symbol_name: Some("zzz测试股份".to_string()),
            announcement_id: announcement_id.to_string(),
            org_id: "gssz0600001".to_string(),
            announcement_category: "日常经营".to_string(),
            announcement_title: format!("zzz测试公告-{announcement_id}"),
            announcement_time: date(2099, 3, 2),
            source_published_at: "2099-03-02 09:00:00".to_string(),
            source_published_at_ts: Some(ts(2099, 3, 2, 1, 2, 3)),
            source_published_date: None,
            source_published_at_quality: "timestamp".to_string(),
            available_at: date(2099, 3, 3),
            announcement_url: format!("http://zzz.test/{announcement_id}.PDF"),
            pdf_final_url: Some(format!("http://zzz.test/final/{announcement_id}.PDF")),
            text_content: Some(format!("zzz测试正文-{announcement_id}")),
            text_hash: Some(format!("zzz-hash-{announcement_id}")),
            timestamp_candidates: json!(["2099-03-02 09:00:00"]),
            pdf_metadata_keys: json!(["title", "creationdate"]),
            raw_payload: json!({ "zzz_key": announcement_id }),
            raw_payload_hash: raw_payload_hash.to_string(),
            parser_used: Some("pdftotext".to_string()),
            parser_version: Some("v1".to_string()),
            parser_errors: json!([]),
            pdf_parse_status: "ok".to_string(),
            event_type: Some("order_or_contract_signed".to_string()),
            evidence_spans: json!([{ "theme": "order_contract", "quote": "签署合同" }]),
        }
    }

    #[tokio::test]
    async fn upsert_raw_rows_returns_zero_for_empty_input() {
        let db = test_db_pool().await;
        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up1").await;
        // 空切片早退：不触碰数据库即返回 Ok(0)
        let saved =
            upsert_exchange_announcement_order_capacity_raw_rows(&db, &[], "zzz_test_ea10_dv0")
                .await
                .expect("empty upsert");
        assert_eq!(saved, 0);
        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up1").await;
    }

    #[tokio::test]
    async fn upsert_raw_rows_inserts_new_row_with_full_field_roundtrip() {
        let db = test_db_pool().await;
        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up2").await;

        let row = zzz_upsert_row("zzz_test_ea10_up2", "zzz10ups2", "zzz_test_ea10_up2_hash");
        let saved = upsert_exchange_announcement_order_capacity_raw_rows(
            &db,
            std::slice::from_ref(&row),
            "zzz_test_ea10_dv2",
        )
        .await
        .expect("insert upsert");
        assert_eq!(saved, 1, "单行 INSERT 应影响 1 行");

        let back_a: (
            String,
            Option<String>,
            String,
            Option<DateTime<Utc>>,
            String,
            NaiveDate,
            Option<String>,
            String,
            String,
            Option<String>,
        ) = sqlx::query_as(
            r#"
                SELECT request_key, symbol_name, announcement_title, source_published_at_ts,
                       source_published_at_quality, available_at, text_hash, text_hash_algorithm,
                       announcement_url, event_type
                FROM market_exchange_announcement_text_raw
                WHERE vendor = 'zzz_test_ea10_vendor' AND announcement_id = 'zzz_test_ea10_up2'
                "#,
        )
        .fetch_one(&db)
        .await
        .expect("read back inserted row (part a)");
        assert_eq!(back_a.0, row.request_key);
        assert_eq!(back_a.1.as_deref(), Some("zzz测试股份"));
        assert_eq!(back_a.2, row.announcement_title);
        assert_eq!(back_a.3, Some(ts(2099, 3, 2, 1, 2, 3)));
        assert_eq!(back_a.4, "timestamp");
        assert_eq!(back_a.5, date(2099, 3, 3));
        assert_eq!(back_a.6.as_deref(), Some("zzz-hash-zzz_test_ea10_up2"));
        assert_eq!(
            back_a.7, "sha256",
            "text_hash_algorithm 由实现固定写 sha256"
        );
        assert_eq!(back_a.8, row.announcement_url);
        assert_eq!(back_a.9.as_deref(), Some("order_or_contract_signed"));

        let back_b: (
            Option<String>,
            String,
            Option<String>,
            Value,
            Value,
            Value,
            Value,
            Value,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(
            r#"
                SELECT pdf_final_url, pdf_parse_status, parser_used, evidence_spans, raw_payload,
                       timestamp_candidates, pdf_metadata_keys, parser_errors, text_content,
                       data_version_id
                FROM market_exchange_announcement_text_raw
                WHERE vendor = 'zzz_test_ea10_vendor' AND announcement_id = 'zzz_test_ea10_up2'
                "#,
        )
        .fetch_one(&db)
        .await
        .expect("read back inserted row (part b)");
        assert_eq!(
            back_b.0.as_deref(),
            Some("http://zzz.test/final/zzz_test_ea10_up2.PDF")
        );
        assert_eq!(back_b.1, "ok");
        assert_eq!(back_b.2.as_deref(), Some("pdftotext"));
        assert_eq!(back_b.3, row.evidence_spans);
        assert_eq!(back_b.4, row.raw_payload);
        assert_eq!(back_b.5, row.timestamp_candidates);
        assert_eq!(back_b.6, row.pdf_metadata_keys);
        assert_eq!(back_b.7, row.parser_errors);
        assert_eq!(back_b.8.as_deref(), Some("zzz测试正文-zzz_test_ea10_up2"));
        assert_eq!(back_b.9.as_deref(), Some("zzz_test_ea10_dv2"));

        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up2").await;
    }

    #[tokio::test]
    async fn upsert_raw_rows_rewrites_same_key_and_stays_idempotent() {
        let db = test_db_pool().await;
        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up3").await;

        // 第一次：INSERT 分支
        let mut row = zzz_upsert_row("zzz_test_ea10_up3", "zzz10ups3", "zzz_test_ea10_up3_hash");
        let saved = upsert_exchange_announcement_order_capacity_raw_rows(
            &db,
            &[row.clone()],
            "zzz_test_ea10_dv3a",
        )
        .await
        .expect("first insert");
        assert_eq!(saved, 1);

        // 同 PK（vendor+endpoint+announcement_id+symbol+raw_payload_hash）再写 → UPDATE 分支
        row.request_key = "zzz_test_ea10_up3_rewritten".to_string();
        row.symbol_name = Some("zzz更新后名称".to_string());
        row.announcement_title = "zzz更新后标题".to_string();
        row.announcement_time = date(2099, 3, 5);
        row.source_published_at_ts = Some(ts(2099, 3, 5, 5, 6, 7));
        row.available_at = date(2099, 3, 6);
        row.text_content = Some("zzz更新后正文".to_string());
        row.text_hash = Some("zzz-hash-updated".to_string());
        row.parser_used = Some("pdfplumber".to_string());
        row.parser_version = Some("v2".to_string());
        row.event_type = Some("capacity_expansion_or_commissioning".to_string());
        row.evidence_spans = json!([{ "theme": "capacity" }]);
        row.timestamp_candidates = json!(["2099-03-05 05:06:07"]);
        row.pdf_metadata_keys = json!(["pages"]);
        row.raw_payload = json!({ "zzz_key": "updated" });
        let saved = upsert_exchange_announcement_order_capacity_raw_rows(
            &db,
            &[row.clone()],
            "zzz_test_ea10_dv3b",
        )
        .await
        .expect("conflict update");
        assert_eq!(saved, 1, "同键 UPDATE 应影响 1 行");

        let updated: (
            String,
            String,
            Option<DateTime<Utc>>,
            NaiveDate,
            Option<String>,
            Value,
            Option<String>,
        ) = sqlx::query_as(
            r#"
                SELECT request_key, announcement_title, source_published_at_ts, available_at,
                       text_hash, evidence_spans, data_version_id
                FROM market_exchange_announcement_text_raw
                WHERE vendor = 'zzz_test_ea10_vendor' AND announcement_id = 'zzz_test_ea10_up3'
                "#,
        )
        .fetch_one(&db)
        .await
        .expect("read back updated row");
        assert_eq!(updated.0, "zzz_test_ea10_up3_rewritten");
        assert_eq!(updated.1, "zzz更新后标题");
        assert_eq!(updated.2, Some(ts(2099, 3, 5, 5, 6, 7)));
        assert_eq!(updated.3, date(2099, 3, 6));
        assert_eq!(updated.4.as_deref(), Some("zzz-hash-updated"));
        assert_eq!(updated.5, json!([{ "theme": "capacity" }]));
        assert_eq!(updated.6.as_deref(), Some("zzz_test_ea10_dv3b"));

        // 第三次：完全相同的行再写 → 幂等，仍只有 1 行、字段不变
        let saved = upsert_exchange_announcement_order_capacity_raw_rows(
            &db,
            &[row.clone()],
            "zzz_test_ea10_dv3b",
        )
        .await
        .expect("idempotent rewrite");
        assert_eq!(saved, 1);
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM market_exchange_announcement_text_raw
            WHERE vendor = 'zzz_test_ea10_vendor' AND announcement_id = 'zzz_test_ea10_up3'
            "#,
        )
        .fetch_one(&db)
        .await
        .expect("count rows");
        assert_eq!(count, 1, "幂等重写不得产生重复行");

        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_up3").await;
    }

    #[tokio::test]
    async fn upsert_raw_rows_chunks_more_than_500_rows_in_single_call() {
        let db = test_db_pool().await;
        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_chunk").await;

        // 501 行 > chunk 上限 500 → 两个批次；每行 raw_payload_hash 唯一避免同语句内二次冲突
        let rows: Vec<ExchangeAnnouncementOrderCapacityRawRow> = (0..501)
            .map(|index| {
                zzz_upsert_row(
                    &format!("zzz_test_ea10_chunk_{index:03}"),
                    "zzz10chunksym",
                    &format!("zzz_test_ea10_chunk_hash_{index:03}"),
                )
            })
            .collect();
        let saved = upsert_exchange_announcement_order_capacity_raw_rows(
            &db,
            &rows,
            "zzz_test_ea10_dv_chunk",
        )
        .await
        .expect("chunked upsert");
        assert_eq!(saved, 501, "两批合计应写入 501 行");

        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM market_exchange_announcement_text_raw
            WHERE announcement_id LIKE 'zzz_test_ea10_chunk_%'
            "#,
        )
        .fetch_one(&db)
        .await
        .expect("count chunk rows");
        assert_eq!(count, 501);

        cleanup_ea10_raw_rows(&db, "zzz_test_ea10_chunk").await;
    }

    // ── 连库读：load_exchange_announcement_next_open_dates ──

    #[tokio::test]
    async fn next_open_dates_reads_open_calendar_with_14day_extension() {
        let db = test_db_pool().await;
        cleanup_ea10_calendar(&db).await;

        let seed = |trade_date: NaiveDate, is_open: bool| {
            sqlx::query(
                "INSERT INTO market_trade_calendar (exchange, trade_date, is_open) VALUES ('zzz10_cx', $1, $2)",
            )
            .bind(trade_date)
            .bind(is_open)
        };
        // 06-01 开但等于 start（严格 > 排除）；06-05/06-10/06-15 开（15 日超出 end 证明 +14 天扩展）；
        // 06-20 闭市排除；06-24 开（= end+14 边界含）；06-30 开但超出扩展窗口排除
        seed(date(2099, 6, 1), true).execute(&db).await.unwrap();
        seed(date(2099, 6, 5), true).execute(&db).await.unwrap();
        seed(date(2099, 6, 10), true).execute(&db).await.unwrap();
        seed(date(2099, 6, 15), true).execute(&db).await.unwrap();
        seed(date(2099, 6, 20), false).execute(&db).await.unwrap();
        seed(date(2099, 6, 24), true).execute(&db).await.unwrap();
        seed(date(2099, 6, 30), true).execute(&db).await.unwrap();

        let open_dates =
            load_exchange_announcement_next_open_dates(&db, date(2099, 6, 1), date(2099, 6, 10))
                .await
                .expect("load next open dates");
        assert_eq!(
            open_dates,
            vec![
                date(2099, 6, 5),
                date(2099, 6, 10),
                date(2099, 6, 15),
                date(2099, 6, 24)
            ],
            "开市日升序；闭市日剔除；仅含 (start, end+14] 窗口"
        );

        cleanup_ea10_calendar(&db).await;
    }

    #[tokio::test]
    async fn next_open_dates_returns_empty_for_window_without_open_days() {
        let db = test_db_pool().await;
        // 2099-08 无任何日历行（真实日历至 2026-12-31）→ 空序列分支
        let open_dates =
            load_exchange_announcement_next_open_dates(&db, date(2099, 8, 1), date(2099, 8, 5))
                .await
                .expect("load next open dates on empty window");
        assert!(open_dates.is_empty());
    }

    // ── 连库审计中段：coverage_quality_audit SQL 分支 ──

    #[tokio::test]
    async fn coverage_audit_aggregates_seeded_rows_and_attempts() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_cove").await;
        cleanup_ea10_attempts(&state.db, "zzz10coveatt").await;

        let spans = json!([{ "theme": "order_contract" }]);
        // e1：date_only_next_session 质量且 available_at > announcement_time（非 PIT 违规）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove1",
            "zzz10covea1",
            "日常经营",
            "关于对外投资的公告",
            date(2099, 10, 5),
            date(2099, 10, 6),
            "date_only_next_session",
            "ok",
            None,
            json!([]),
            "zzz10cove_h1",
            None,
        )
        .await;
        // e2：可入样 target（真实经营标题 + spans）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove2",
            "zzz10covea2",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2099, 10, 6),
            date(2099, 10, 7),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz10cove_h2",
            None,
        )
        .await;
        // e3：target 无 spans（missing evidence span）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove3",
            "zzz10covea3",
            "日常经营",
            "关于项目备案的公告",
            date(2099, 10, 7),
            date(2099, 10, 8),
            "timestamp",
            "ok",
            Some("capacity_expansion_or_commissioning"),
            json!([]),
            "zzz10cove_h3",
            None,
        )
        .await;
        // e4：股权激励类 target → taxonomy_blocked
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove4",
            "zzz10covea4",
            "股权激励",
            "股权激励计划公告",
            date(2099, 10, 8),
            date(2099, 10, 9),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz10cove_h4",
            None,
        )
        .await;
        // e5：风险标题（募集资金）非 target → 仅计 taxonomy_risk_category
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove5",
            "zzz10covea5",
            "日常经营",
            "关于募集资金存放与实际使用情况的公告",
            date(2099, 10, 9),
            date(2099, 10, 10),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10cove_h5",
            None,
        )
        .await;
        // e6：扫描件且标题不在 OCR 排除清单 → trainable blocking
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove6",
            "zzz10covea6",
            "日常经营",
            "项目进展公告",
            date(2099, 10, 10),
            date(2099, 10, 11),
            "timestamp",
            "scanned_pdf_ocr_required",
            None,
            json!([]),
            "zzz10cove_h6",
            None,
        )
        .await;
        // e7：扫描件 + 审计报告标题 → ocr_taxonomy_excluded（不阻塞）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove7",
            "zzz10covea7",
            "日常经营",
            "年度审计报告",
            date(2099, 10, 11),
            date(2099, 10, 12),
            "timestamp",
            "scanned_pdf_ocr_required",
            None,
            json!([]),
            "zzz10cove_h7",
            None,
        )
        .await;
        // e8a/e8b：同 (vendor,endpoint,announcement_id,symbol) 不同 hash → duplicate_announcement_id_rows
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove8",
            "zzz10covea8",
            "日常经营",
            "关于投资的公告",
            date(2099, 10, 12),
            date(2099, 10, 13),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10cove_h8a",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_cove8",
            "zzz10covea8",
            "日常经营",
            "关于投资的公告",
            date(2099, 10, 13),
            date(2099, 10, 14),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10cove_h8b",
            None,
        )
        .await;

        seed_ea10_attempt(
            &state.db,
            "zzz10coveatt1",
            date(2099, 10, 1),
            date(2099, 10, 5),
            "completed",
            5,
            None,
        )
        .await;
        seed_ea10_attempt(
            &state.db,
            "zzz10coveatt2",
            date(2099, 10, 1),
            date(2099, 10, 2),
            "completed",
            0,
            None,
        )
        .await;
        seed_ea10_attempt(
            &state.db,
            "zzz10coveatt3:重大事项",
            date(2099, 10, 1),
            date(2099, 10, 3),
            "failed",
            0,
            Some("'重大事项'"),
        )
        .await;
        seed_ea10_attempt(
            &state.db,
            "zzz10coveatt4",
            date(2099, 10, 4),
            date(2099, 10, 4),
            "failed",
            0,
            Some("boom"),
        )
        .await;

        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("20991001", "20991031"),
        )
        .await
        .expect("coverage audit report");

        assert_eq!(report["table_exists"], true);
        let summary = &report["summary"];
        assert_eq!(summary["row_count"], 9);
        assert_eq!(summary["distinct_symbol_count"], 8);
        assert_eq!(summary["distinct_category_count"], 2);
        assert_eq!(summary["distinct_available_at_count"], 9);
        assert_eq!(summary["pit_violation_rows"], 0);
        assert_eq!(summary["missing_available_at_rows"], 0);
        assert_eq!(summary["missing_source_published_at_quality_rows"], 0);
        assert_eq!(summary["duplicate_announcement_id_rows"], 1);
        assert_eq!(summary["duplicate_raw_payload_hash_groups"], 0);
        assert_eq!(summary["evidence_span_rows"], 2);
        assert_eq!(summary["target_event_rows"], 3);
        assert_eq!(summary["target_event_missing_evidence_span_rows"], 1);
        assert_eq!(summary["target_event_with_evidence_span_rows"], 2);
        assert_eq!(summary["scanned_pdf_ocr_required_rows"], 2);
        assert_eq!(summary["ocr_taxonomy_excluded_rows"], 1);
        assert_eq!(summary["trainable_scanned_pdf_blocking_rows"], 1);
        assert_eq!(summary["taxonomy_blocked_target_event_rows"], 1);
        assert_eq!(
            summary["taxonomy_risk_category_rows"], 3,
            "e4 股权激励 + e5 募集资金标题 + e7 审计报告标题（风险词表含「审计报告」）"
        );
        assert_eq!(summary["admissible_target_event_rows"], 2);
        // att1(completed,row=5) + att2(completed,row=0) 两条 completed → completed_attempts=2
        assert_eq!(summary["completed_attempts"], 2);
        assert_eq!(summary["completed_empty_attempts"], 1);
        assert_eq!(summary["attempt_row_count"], 5);
        assert_eq!(
            summary["failed_attempts"], 1,
            "仅非排除类失败计入 failed_attempts"
        );
        assert_eq!(summary["total_failed_attempts"], 2);
        assert_eq!(summary["excluded_unsupported_category_failed_attempts"], 1);

        // 分布：质量 8×timestamp + 1×date_only_next_session；解析状态 7×ok + 2×scanned
        assert_eq!(
            dist_rows(
                &report["source_published_at_quality_distribution"],
                "quality",
                "timestamp"
            ),
            Some(8)
        );
        assert_eq!(
            dist_rows(
                &report["source_published_at_quality_distribution"],
                "quality",
                "date_only_next_session"
            ),
            Some(1)
        );
        assert_eq!(
            dist_rows(
                &report["parser_status_distribution"],
                "pdf_parse_status",
                "ok"
            ),
            Some(7)
        );
        assert_eq!(
            dist_rows(
                &report["parser_status_distribution"],
                "pdf_parse_status",
                "scanned_pdf_ocr_required"
            ),
            Some(2)
        );
        // 事件分布：non_target 行带 target_event=false 标记
        assert_eq!(
            dist_rows(
                &report["event_type_distribution"],
                "event_type",
                "non_target_or_unclassified"
            ),
            Some(6)
        );
        let non_target = report["event_type_distribution"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["event_type"] == "non_target_or_unclassified")
            .expect("non target entry");
        assert_eq!(non_target["target_event"], false);
        // 年×类分布：2099 日常经营 8 行 / 股权激励 1 行（taxonomy_blocked=1）
        let year_rows = report["year_category_breakdown"].as_array().unwrap();
        let daily = year_rows
            .iter()
            .find(|entry| entry["year"] == 2099 && entry["category"] == "日常经营")
            .expect("2099 日常经营 entry");
        assert_eq!(daily["rows"], 8);
        assert_eq!(daily["target_event_rows"], 2);
        let equity = year_rows
            .iter()
            .find(|entry| entry["year"] == 2099 && entry["category"] == "股权激励")
            .expect("2099 股权激励 entry");
        assert_eq!(equity["rows"], 1);
        assert_eq!(equity["taxonomy_blocked_target_event_rows"], 1);
        // 符号分布：zzz10covea2 的 target 产出率为 1.0
        let symbol_rows = report["symbol_event_breakdown"].as_array().unwrap();
        let a2 = symbol_rows
            .iter()
            .find(|entry| entry["symbol"] == "zzz10covea2")
            .expect("a2 symbol entry");
        assert_eq!(a2["target_event_rows"], 1);
        assert!((a2["target_event_yield_ratio"].as_f64().unwrap() - 1.0).abs() < 1e-9);

        // 失败 attempt 优先阻断决策；attempt/ocr/taxonomy 三门均 blocked
        let decision = &report["decision"];
        assert_eq!(decision["status"], "blocked_failed_sync_attempts_present");
        assert_eq!(decision["attempt_failure_gate"]["status"], "blocked");
        assert_eq!(
            decision["attempt_failure_gate"]["blocking_failed_attempts"],
            1
        );
        assert_eq!(decision["attempt_failure_gate"]["total_failed_attempts"], 2);
        assert_eq!(
            decision["attempt_failure_gate"]["excluded_unsupported_category_failed_attempts"],
            1
        );
        assert_eq!(decision["ocr_quality_gate"]["status"], "blocked");
        assert_eq!(decision["taxonomy_precision_gate"]["status"], "blocked");
        assert_eq!(
            decision["taxonomy_precision_gate"]["admissible_target_event_rows"],
            2
        );
        assert_eq!(
            decision["bounded_sync"],
            "blocked_until_small_batch_audit_passes"
        );

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_cove").await;
        cleanup_ea10_attempts(&state.db, "zzz10coveatt").await;
    }

    #[tokio::test]
    async fn coverage_audit_blocks_on_raw_pit_duplicate_and_missing_quality() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_covf").await;

        // f1：date_only_next_session 且 available_at == announcement_time → PIT 违规
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf1",
            "zzz10covfa1",
            "日常经营",
            "关于对外投资的公告",
            date(2099, 11, 5),
            date(2099, 11, 5),
            "date_only_next_session",
            "ok",
            None,
            json!([]),
            "zzz10covf_h1",
            None,
        )
        .await;
        // f2：质量 missing → missing_source_published_at_quality_rows
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf2",
            "zzz10covfa2",
            "日常经营",
            "关于项目备案的公告",
            date(2099, 11, 6),
            date(2099, 11, 7),
            "missing",
            "ok",
            None,
            json!([]),
            "zzz10covf_h2",
            None,
        )
        .await;
        // f3a/f3b：同 announcement 组 → duplicate_announcement_id_rows
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf3",
            "zzz10covfa3",
            "日常经营",
            "关于投资的公告",
            date(2099, 11, 7),
            date(2099, 11, 8),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10covf_h3a",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf3",
            "zzz10covfa3",
            "日常经营",
            "关于投资的公告",
            date(2099, 11, 8),
            date(2099, 11, 9),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10covf_h3b",
            None,
        )
        .await;
        // f4a/f4b：同 raw_payload_hash 不同 announcement_id → duplicate_raw_payload_hash_groups
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf4a",
            "zzz10covfa4",
            "日常经营",
            "关于合作的公告",
            date(2099, 11, 9),
            date(2099, 11, 10),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10covf_same_hash",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covf4b",
            "zzz10covfa4",
            "日常经营",
            "关于合作的公告",
            date(2099, 11, 10),
            date(2099, 11, 11),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz10covf_same_hash",
            None,
        )
        .await;

        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("20991101", "20991130"),
        )
        .await
        .expect("coverage audit report");

        let summary = &report["summary"];
        assert_eq!(summary["row_count"], 6);
        assert_eq!(summary["pit_violation_rows"], 1);
        assert_eq!(summary["missing_source_published_at_quality_rows"], 1);
        assert_eq!(summary["duplicate_announcement_id_rows"], 1);
        assert_eq!(summary["duplicate_raw_payload_hash_groups"], 1);
        // 无失败 attempt → attempt 门 passed_or_not_observed；但原始质量失败优先阻断
        let decision = &report["decision"];
        assert_eq!(decision["status"], "blocked_raw_pit_or_quality_failed");
        assert_eq!(
            decision["next_step"],
            "repair_or_exclude_bad_raw_rows_before_expanding_sync"
        );
        assert_eq!(
            decision["attempt_failure_gate"]["status"],
            "passed_or_not_observed"
        );
        assert_eq!(
            decision["bounded_sync"],
            "blocked_until_small_batch_audit_passes"
        );

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_covf").await;
    }

    #[tokio::test]
    async fn coverage_audit_empty_window_requires_bounded_sync() {
        let state = test_app_state().await;
        // 2099-12 无行、无 attempt：表存在但零覆盖 → 要求先跑 tiny sync
        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("20991201", "20991231"),
        )
        .await
        .expect("coverage audit report");

        assert_eq!(report["table_exists"], true);
        assert_eq!(report["summary"]["row_count"], 0);
        assert_eq!(report["summary"]["completed_attempts"], 0);
        assert_eq!(
            report["year_category_breakdown"].as_array().unwrap().len(),
            0
        );
        assert_eq!(
            report["symbol_event_breakdown"].as_array().unwrap().len(),
            0
        );
        let decision = &report["decision"];
        assert_eq!(
            decision["status"],
            "raw_table_present_bounded_sync_required"
        );
        assert_eq!(
            decision["next_step"],
            "run_one_tiny_exchange_announcement_raw_sync_then_rerun_audit"
        );
        assert_eq!(
            decision["ocr_quality_gate"]["status"],
            "passed_or_not_observed"
        );
        assert_eq!(
            decision["taxonomy_precision_gate"]["status"],
            "passed_or_not_observed"
        );
        assert_eq!(
            decision["bounded_sync"],
            "blocked_until_small_batch_audit_passes"
        );
    }

    #[tokio::test]
    async fn coverage_audit_synced_empty_window_passes_accounting_only() {
        let state = test_app_state().await;
        cleanup_ea10_attempts(&state.db, "zzz10emptyatt").await;

        // 零行 + completed 空 attempt（row_count=0）→ 覆盖核算放行
        seed_ea10_attempt(
            &state.db,
            "zzz10emptyatt1",
            date(2100, 1, 1),
            date(2100, 1, 2),
            "completed",
            0,
            None,
        )
        .await;
        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("21000101", "21000131"),
        )
        .await
        .expect("coverage audit report");

        let summary = &report["summary"];
        assert_eq!(summary["row_count"], 0);
        assert_eq!(summary["completed_attempts"], 1);
        assert_eq!(summary["completed_empty_attempts"], 1);
        assert_eq!(summary["attempt_row_count"], 0);
        let decision = &report["decision"];
        assert_eq!(
            decision["status"],
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only"
        );
        assert_eq!(
            decision["next_step"],
            "continue_next_tiny_slice_or_batch_then_rerun_full_window_audit"
        );
        assert_eq!(
            decision["bounded_sync"],
            "continue_bounded_sync_for_coverage_accounting_only"
        );

        cleanup_ea10_attempts(&state.db, "zzz10emptyatt").await;
    }

    #[tokio::test]
    async fn coverage_audit_clean_target_rows_pass_small_batch_gate() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_covp").await;

        // 单行干净 target（avail>time、timestamp 质量、spans 齐全、无重复/扫描/风险类）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_covp1",
            "zzz10covpa1",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2100, 2, 5),
            date(2100, 2, 6),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            json!([{ "theme": "order_contract" }]),
            "zzz10covp_h1",
            Some("zzz干净样本文本"),
        )
        .await;
        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("21000201", "21000228"),
        )
        .await
        .expect("coverage audit report");

        let summary = &report["summary"];
        assert_eq!(summary["row_count"], 1);
        assert_eq!(summary["target_event_rows"], 1);
        assert_eq!(summary["target_event_with_evidence_span_rows"], 1);
        assert_eq!(summary["admissible_target_event_rows"], 1);
        assert_eq!(summary["pit_violation_rows"], 0);
        let decision = &report["decision"];
        assert_eq!(
            decision["status"],
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only"
        );
        assert_eq!(
            decision["next_step"],
            "expand_by_month_or_quarter_then_rerun_coverage_quality_audit"
        );
        assert_eq!(
            decision["bounded_sync"],
            "expand_bounded_sync_by_month_or_quarter_only"
        );

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_covp").await;
    }

    #[tokio::test]
    async fn coverage_audit_rejects_inverted_date_range() {
        let state = test_app_state().await;
        let error = build_exchange_announcement_order_capacity_coverage_quality_audit(
            &state,
            coverage_audit_req("20991231", "20991201"),
        )
        .await
        .expect_err("inverted range");
        assert!(
            error.contains("start_date must be <= end_date"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn coverage_audit_defaults_to_full_history_window() {
        let state = test_app_state().await;
        // 缺省窗口：start=20140101，end=今天（覆盖 249 行真实数据，仅做聚合回显断言）
        let req = ExchangeAnnouncementOrderCapacityCoverageQualityAuditReq {
            start_date: None,
            end_date: None,
        };
        let report = build_exchange_announcement_order_capacity_coverage_quality_audit(&state, req)
            .await
            .expect("default window audit");
        assert_eq!(report["date_range"]["start_date"], "20140101");
        let end_date = report["date_range"]["end_date"].as_str().unwrap();
        assert_eq!(end_date.len(), 8, "缺省 end_date 应为 YYYYMMDD");
        let parsed = NaiveDate::parse_from_str(end_date, "%Y%m%d").expect("parseable end_date");
        let today = Utc::now().date_naive();
        assert!(
            (parsed - today).num_days().abs() <= 1,
            "缺省 end_date 应为当天（跨天边界容忍 1 日）: {end_date}"
        );
    }

    // ── 连库审计中段：manual_precision_sample_audit SQL 分支 ──

    fn manual_audit_req(
        start: &str,
        end: &str,
        limit: Option<i64>,
        negative: Option<bool>,
    ) -> ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq {
        ExchangeAnnouncementOrderCapacityManualPrecisionSampleAuditReq {
            start_date: Some(start.to_string()),
            end_date: Some(end.to_string()),
            limit,
            include_negative_samples: negative,
        }
    }

    #[tokio::test]
    async fn manual_precision_blocked_shortfall_with_taxonomy_exclusion_matrix() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpa").await;

        let spans = json!([{ "theme": "order_contract" }]);
        // h1/h2：可入样 target（真实经营标题）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa1",
            "zzz10mpa1",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2099, 6, 2),
            date(2099, 6, 3),
            "date_only_next_session",
            "ok",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz_test_ea10_mp_h1",
            Some("zzz测试正文摘要"),
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa2",
            "zzz10mpa2",
            "日常经营",
            "投资建设产能项目公告",
            date(2099, 6, 3),
            date(2099, 6, 4),
            "timestamp",
            "ok",
            Some("capacity_expansion_or_commissioning"),
            spans.clone(),
            "zzz_test_ea10_mp_h2",
            None,
        )
        .await;
        // h3：风险标题（募集资金）→ 排除出可入样宇宙
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa3",
            "zzz10mpa3",
            "日常经营",
            "关于募集资金存放与实际使用情况的公告",
            date(2099, 6, 4),
            date(2099, 6, 5),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz_test_ea10_mp_h3",
            None,
        )
        .await;
        // h4：股权激励类 → 排除
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa4",
            "zzz10mpa4",
            "股权激励",
            "股权激励计划公告",
            date(2099, 6, 5),
            date(2099, 6, 6),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz_test_ea10_mp_h4",
            None,
        )
        .await;
        // h5：target 无 spans → 排除
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa5",
            "zzz10mpa5",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2099, 6, 6),
            date(2099, 6, 7),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            json!([]),
            "zzz_test_ea10_mp_h5",
            None,
        )
        .await;
        // h6：扫描件 → 排除
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa6",
            "zzz10mpa6",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2099, 6, 7),
            date(2099, 6, 8),
            "timestamp",
            "scanned_pdf_ocr_required",
            Some("order_or_contract_signed"),
            spans.clone(),
            "zzz_test_ea10_mp_h6",
            None,
        )
        .await;
        // h7：负样本（风险标题 → review 项带 taxonomy_risk_reason）；h8：负样本（中性标题）
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa7",
            "zzz10mpa7",
            "日常经营",
            "独立董事专项意见公告",
            date(2099, 6, 8),
            date(2099, 6, 9),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz_test_ea10_mp_h7",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpa8",
            "zzz10mpa8",
            "日常经营",
            "关于其他事项的公告",
            date(2099, 6, 9),
            date(2099, 6, 10),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz_test_ea10_mp_h8",
            None,
        )
        .await;

        let report = build_exchange_announcement_order_capacity_manual_precision_sample_audit(
            &state,
            manual_audit_req("20990601", "20990610", Some(3), Some(true)),
        )
        .await
        .expect("manual precision report");

        assert_eq!(
            report["status"],
            "blocked_insufficient_target_review_sample"
        );
        assert_eq!(
            report["admissible_target_event_rows"], 2,
            "仅 h1/h2 进入可入样宇宙"
        );
        assert_eq!(report["required_target_sample_size_min"], 3);
        assert_eq!(report["target_sample_rows"], 2);
        assert_eq!(report["negative_sample_rows"], 1);
        assert_eq!(report["target_sample_shortfall"], 1);
        assert_eq!(
            report["next_step"],
            "expand_pre_registered_coverage_until_minimum_target_review_sample_is_available"
        );
        assert_eq!(
            report["promotion_gate"]["p310_status"],
            "blocked_until_manual_precision_review_passes"
        );
        assert_eq!(report["sample_policy"]["negative_samples"], true);
        assert_eq!(
            report["sample_policy"]["negative_sample_limit"], 1,
            "limit=3 → (3/10).clamp(1,20)=1"
        );

        // 样本序：按 raw_payload_hash 升序 → h1、h2 target + h7 negative（h7 < h8）
        let items = report["review_items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["sample_kind"], "target");
        assert_eq!(items[0]["symbol"], "zzz10mpa1");
        assert_eq!(items[0]["event_type"], "order_or_contract_signed");
        assert_eq!(
            items[0]["taxonomy_risk_reason"],
            Value::Null,
            "可入样 target 的风险原因为空"
        );
        assert_eq!(items[0]["text_excerpt"], "zzz测试正文摘要");
        assert_eq!(items[0]["announcement_time"], "2099-06-02");
        assert!(items[0]["manual_review_fields"]["reviewer"].is_null());
        assert_eq!(items[1]["sample_kind"], "target");
        assert_eq!(items[1]["symbol"], "zzz10mpa2");
        assert_eq!(items[2]["sample_kind"], "negative");
        assert_eq!(items[2]["symbol"], "zzz10mpa7");
        assert_eq!(
            items[2]["taxonomy_risk_reason"], "admin_finance_governance_false_positive",
            "负样本带风险标题 → review 项标注 taxonomy 风险原因"
        );
        assert_eq!(items[2]["text_excerpt"], "");

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpa").await;
    }

    #[tokio::test]
    async fn manual_precision_ready_when_target_sample_meets_required_limit() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpb").await;

        let spans = json!([{ "theme": "capacity" }]);
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpb1",
            "zzz10mpb1",
            "日常经营",
            "合资建厂公告",
            date(2099, 7, 2),
            date(2099, 7, 3),
            "timestamp",
            "ok",
            Some("capacity_expansion_or_commissioning"),
            spans.clone(),
            "zzz_test_ea10_mp_b1",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpb2",
            "zzz10mpb2",
            "日常经营",
            "投资建设高效电池产能公告",
            date(2099, 7, 3),
            date(2099, 7, 4),
            "timestamp",
            "ok",
            Some("capacity_expansion_or_commissioning"),
            spans.clone(),
            "zzz_test_ea10_mp_b2",
            None,
        )
        .await;
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpb3",
            "zzz10mpb3",
            "日常经营",
            "关于其他事项的公告",
            date(2099, 7, 4),
            date(2099, 7, 5),
            "timestamp",
            "ok",
            None,
            json!([]),
            "zzz_test_ea10_mp_b3",
            None,
        )
        .await;

        // limit=2：可入样 2 行刚好达标 → ready；negative_limit=(2/10).clamp(1,20)=1
        let report = build_exchange_announcement_order_capacity_manual_precision_sample_audit(
            &state,
            manual_audit_req("20990701", "20990731", Some(2), Some(true)),
        )
        .await
        .expect("manual precision report");

        assert_eq!(report["status"], "manual_review_sample_ready");
        assert_eq!(report["target_sample_rows"], 2);
        assert_eq!(report["negative_sample_rows"], 1);
        assert_eq!(report["target_sample_shortfall"], 0);
        assert_eq!(
            report["next_step"],
            "record_human_labels_for_sample_then_compute_precision_before_correlation_audit"
        );
        assert_eq!(report["sample_policy"]["negative_sample_limit"], 1);
        let items = report["review_items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[2]["sample_kind"], "negative");

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpb").await;
    }

    #[tokio::test]
    async fn manual_precision_negative_universe_empty_and_limit_clamped_to_200() {
        let state = test_app_state().await;
        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpc").await;

        // 仅 1 行可入样 target，无负样本
        seed_ea10_raw_row(
            &state.db,
            "zzz_test_ea10_mpc1",
            "zzz10mpc1",
            "日常经营",
            "签订日常经营重大合同公告",
            date(2099, 8, 2),
            date(2099, 8, 3),
            "timestamp",
            "ok",
            Some("order_or_contract_signed"),
            json!([{ "theme": "order_contract" }]),
            "zzz_test_ea10_mp_c1",
            None,
        )
        .await;

        // limit=1：负样本查询执行但宇宙为空 → negative_sample_rows=0，样本仍 ready
        let report = build_exchange_announcement_order_capacity_manual_precision_sample_audit(
            &state,
            manual_audit_req("20990801", "20990831", Some(1), Some(true)),
        )
        .await
        .expect("manual precision report");
        assert_eq!(report["status"], "manual_review_sample_ready");
        assert_eq!(report["target_sample_rows"], 1);
        assert_eq!(report["negative_sample_rows"], 0, "负样本宇宙为空");
        assert_eq!(report["sample_policy"]["negative_sample_limit"], 1);
        assert_eq!(report["review_items"].as_array().unwrap().len(), 1);

        // limit=500 → clamp 到 200 → shortfall 199 阻断
        let clamped = build_exchange_announcement_order_capacity_manual_precision_sample_audit(
            &state,
            manual_audit_req("20990801", "20990831", Some(500), Some(true)),
        )
        .await
        .expect("manual precision report");
        assert_eq!(
            clamped["required_target_sample_size_min"], 200,
            "limit 钳制到上限 200"
        );
        assert_eq!(clamped["target_sample_rows"], 1);
        assert_eq!(clamped["target_sample_shortfall"], 199);
        assert_eq!(
            clamped["status"],
            "blocked_insufficient_target_review_sample"
        );

        cleanup_ea10_raw_rows(&state.db, "zzz_test_ea10_mpc").await;
    }

    #[tokio::test]
    async fn manual_precision_rejects_inverted_date_range() {
        let state = test_app_state().await;
        let error = build_exchange_announcement_order_capacity_manual_precision_sample_audit(
            &state,
            manual_audit_req("20990610", "20990601", None, None),
        )
        .await
        .expect_err("inverted range");
        assert!(
            error.contains("start_date cannot be after end_date"),
            "实际错误: {error}"
        );
    }
}
