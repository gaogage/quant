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
use std::{
    collections::BTreeSet,
    path::Path,
    sync::Arc,
    time::Duration as StdDuration,
};
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

#[derive(Debug, Clone, Deserialize)]


#[derive(Default)]
pub(crate) struct ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
    pub(crate) table_exists:bool,
    pub(crate) row_count:i64,
    pub(crate) distinct_symbol_count:i64,
    pub(crate) distinct_category_count:i64,
    pub(crate) pit_violation_rows:i64,
    pub(crate) missing_available_at_rows:i64,
    pub(crate) missing_source_published_at_quality_rows: i64,
    pub(crate) duplicate_announcement_id_rows:i64,
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



pub(crate) fn exchange_announcement_order_capacity_pdf_audit_python_path(requested: Option<String>) -> String {
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



pub(crate) fn exchange_announcement_order_capacity_csv_values(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}



pub(crate) fn exchange_announcement_order_capacity_categories(categories: &[String]) -> Vec<String> {
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
    pub(crate) fetched_text_count:usize,
    pub(crate) text_hash_count:usize,
    pub(crate) source_published_at_count: usize,
    pub(crate) incomplete_link_metadata_count:usize,
    pub(crate) pdf_parser_required_count:usize,
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
    pub(crate) symbols:Vec<String>,
    pub(crate) categories:Vec<String>,
    pub(crate) market:String,
    pub(crate) start:NaiveDate,
    pub(crate) end:NaiveDate,
    pub(crate) calendar_day_count:i64,
    pub(crate) query_count:usize,
    pub(crate) data_version_id:String,
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
    pub(crate) symbols:Vec<String>,
    pub(crate) categories:Vec<String>,
    pub(crate) market:String,
    pub(crate) start:NaiveDate,
    pub(crate) end:NaiveDate,
    pub(crate) calendar_day_count:i64,
    pub(crate) batch_mode:String,
    pub(crate) slices:Vec<ExchangeAnnouncementOrderCapacityTinySlice>,
    pub(crate) total_query_units: usize,
    pub(crate) data_version_id: String,
    pub(crate) python: String,
    pub(crate) pdf_python: String,
    pub(crate) stop_on_audit_failure: bool,
}

#[derive(Debug, Clone)]


pub(crate) struct ExchangeAnnouncementOrderCapacityRawRow {
    pub(crate) vendor:String,
    pub(crate) vendor_endpoint:String,
    pub(crate) request_key:String,
    pub(crate) symbol:String,
    pub(crate) symbol_name:Option<String>,
    pub(crate) announcement_id:String,
    pub(crate) org_id:String,
    pub(crate) announcement_category:String,
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



pub(crate) fn parse_exchange_announcement_date(value: &str) -> Option<NaiveDate> {
    let decoded = cninfo_percent_decode(value);
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
    let decoded = cninfo_percent_decode(value);
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

pub(crate) fn exchange_announcement_order_capacity_json_path_i64(value: &Value, path: &[&str]) -> i64 {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}



pub(crate) fn exchange_announcement_order_capacity_json_path_f64(value: &Value, path: &[&str]) -> Option<f64> {
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
            ..Default::default()
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
    ) in target_rows.into_iter().chain(negative_rows.into_iter())
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



pub(crate) fn exchange_announcement_order_capacity_is_akshare_empty_dataframe_key_error(error: &str) -> bool {
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
                .unwrap_or_else(|| json!(null)),
        );
        object.insert(
            "source_published_at".to_string(),
            row.get("source_published_at")
                .cloned()
                .unwrap_or_else(|| json!(null)),
        );
        object.insert(
            "available_at".to_string(),
            row.get("available_at")
                .cloned()
                .unwrap_or_else(|| json!(null)),
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



