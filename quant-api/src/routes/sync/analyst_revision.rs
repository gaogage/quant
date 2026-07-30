/// 数据同步路由
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::env;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hasher,
    path::Path,
    sync::Arc,
    time::Duration as StdDuration,
};
use tokio::{process::Command, time::timeout};

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub struct AkshareAnalystRevisionSmokeReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub dates: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct AkshareAnalystRevisionHistoryReplayAuditReq {
    #[serde(default)]
    pub dates: Vec<String>,
    #[serde(default)]
    pub start_year: Option<i32>,
    #[serde(default)]
    pub end_year: Option<i32>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub python: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct AkshareAnalystRevisionSyncPlanReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct AkshareAnalystRevisionReadinessAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct AkshareAnalystRevisionSyncReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub python: Option<String>,
    #[serde(default)]
    pub background: bool,
}

impl AkshareAnalystRevisionSyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: "akshare_analyst_revision".to_string(),
            source: AKSHARE_ANALYST_REVISION_TASK_SOURCE.to_string(),
            mode: Some("bounded_calendar_day_raw_sync".to_string()),
            symbols: Vec::new(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: self.start_date,
            end_date: self.end_date,
            data_version_id: self.data_version_id,
            background: self.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("p3.23d akshare analyst revision bounded raw sync".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]


pub struct AkshareAnalystRevisionCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct BroadAnalystRevisionAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]


pub(crate) struct BroadAnalystRevisionAuditDecision {
    pub(crate) passed:bool,
    pub(crate) status:&'static str,
    pub(crate) readiness:&'static str,
    pub(crate) admission_decision:&'static str,
    pub(crate) p310_status:&'static str,
    pub(crate) blocked_reason:&'static str,
}



fn akshare_analyst_revision_smoke_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(5)
        .clamp(1, AKSHARE_ANALYST_REVISION_MAX_ROWS)
}



fn akshare_analyst_revision_default_dates() -> Vec<String> {
    let latest_complete_date = chrono::Utc::now().date_naive() - Duration::days(1);
    vec![latest_complete_date.format("%Y%m%d").to_string()]
}



fn akshare_analyst_revision_smoke_dates(dates: &[String]) -> Result<Vec<String>, String> {
    let raw_dates = if dates.is_empty() {
        akshare_analyst_revision_default_dates()
    } else {
        dates
            .iter()
            .map(|date| date.trim().to_string())
            .filter(|date| !date.is_empty())
            .collect()
    };

    let mut seen = BTreeSet::new();
    let mut parsed = Vec::new();
    for date in raw_dates {
        parse_optional_date(Some(date.as_str()))?;
        if seen.insert(date.clone()) {
            parsed.push(date);
        }
        if parsed.len() >= AKSHARE_ANALYST_REVISION_MAX_DATES {
            break;
        }
    }
    Ok(parsed)
}



fn akshare_analyst_revision_history_dates(dates: &[String]) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let mut parsed = Vec::new();
    for date in dates
        .iter()
        .map(|date| date.trim().to_string())
        .filter(|date| !date.is_empty())
    {
        parse_optional_date(Some(date.as_str()))?;
        if seen.insert(date.clone()) {
            parsed.push(date);
        }
    }
    if parsed.len() > AKSHARE_ANALYST_REVISION_HISTORY_MAX_DATES {
        return Err(format!(
            "history replay audit accepts at most {} dates per request",
            AKSHARE_ANALYST_REVISION_HISTORY_MAX_DATES
        ));
    }
    Ok(parsed)
}



fn akshare_analyst_revision_symbol(value: &str) -> String {
    value
        .trim()
        .split('.')
        .next()
        .unwrap_or(value.trim())
        .to_string()
}



#[derive(Debug, Clone)]
pub(crate) struct AkshareAnalystRevisionSyncPlanBatch {
    pub(crate) label:String,
    pub(crate) start_date:NaiveDate,
    pub(crate) end_date:NaiveDate,
    pub(crate) calendar_day_count:i64,
}

#[derive(Debug, Clone)]


pub(crate) struct AkshareAnalystRevisionRawRow {
    pub(crate) vendor:String,
    pub(crate) vendor_source:String,
    pub(crate) vendor_endpoint:String,
    pub(crate) request_key:String,
    pub(crate) symbol:String,
    pub(crate) symbol_name:Option<String>,
    pub(crate) publication_date: NaiveDate,
    pub(crate) source_published_at: DateTime<Utc>,
    pub(crate) available_at: NaiveDate,
    pub(crate) institution_name: Option<String>,
    pub(crate) analyst_name: Option<String>,
    pub(crate) rating_current: Option<String>,
    pub(crate) rating_previous: Option<String>,
    pub(crate) rating_change: Option<String>,
    pub(crate) is_first_rating: Option<String>,
    pub(crate) target_price_min: Option<Decimal>,
    pub(crate) target_price_max: Option<Decimal>,
    pub(crate) raw_payload: Value,
    pub(crate) raw_payload_hash: String,
}



pub(crate) fn akshare_analyst_revision_batch_end(date: NaiveDate, batch_mode: &str) -> NaiveDate {
    match batch_mode {
        "year" => NaiveDate::from_ymd_opt(date.year(), 12, 31).unwrap(),
        "month" => last_day_of_month(date.year(), date.month()),
        _ => {
            let quarter_end_month = ((date.month() - 1) / 3 + 1) * 3;
            last_day_of_month(date.year(), quarter_end_month)
        }
    }
}



pub(crate) fn akshare_analyst_revision_batch_label(date: NaiveDate, batch_mode: &str) -> String {
    match batch_mode {
        "year" => format!("{}", date.year()),
        "month" => format!("{}{:02}", date.year(), date.month()),
        _ => format!("{}Q{}", date.year(), ((date.month() - 1) / 3) + 1),
    }
}



pub(crate) fn akshare_value_key_part(item: &serde_json::Map<String, Value>, key: &str) -> String {
    match item.get(key) {
        Some(Value::String(value)) => value.trim().to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(value) => value.to_string(),
    }
}



pub(crate) fn akshare_optional_string(item: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    let value = akshare_value_key_part(item, key);
    (!value.is_empty()).then_some(value)
}



pub(crate) fn akshare_optional_decimal(item: &serde_json::Map<String, Value>, key: &str) -> Option<Decimal> {
    item.get(key)
        .and_then(|value| {
            value.as_f64().or_else(|| {
                value
                    .as_str()
                    .and_then(|raw| raw.trim().parse::<f64>().ok())
            })
        })
        .and_then(Decimal::from_f64_retain)
}



pub(crate) fn parse_akshare_publication_date(value: &str) -> Option<NaiveDate> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .ok()
}



pub(crate) fn akshare_next_open_date(publication_date: NaiveDate, open_dates: &[NaiveDate]) -> NaiveDate {
    open_dates
        .iter()
        .copied()
        .find(|date| *date > publication_date)
        .unwrap_or_else(|| publication_date + Duration::days(1))
}



pub(crate) fn akshare_source_published_at(available_at: NaiveDate) -> DateTime<Utc> {
    available_at
        .and_hms_opt(0, 30, 0)
        .expect("valid conservative source publication timestamp")
        .and_utc()
}



fn akshare_analyst_revision_calendar_days(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        days.push(cursor);
        cursor += Duration::days(1);
    }
    days
}



async fn load_akshare_analyst_revision_available_open_dates(
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



async fn upsert_akshare_analyst_revision_raw_rows(
    db: &sqlx::PgPool,
    rows: &[AkshareAnalystRevisionRawRow],
    data_version_id: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut saved = 0usize;
    for chunk in rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_vendor_analyst_revision_raw \
             (vendor, vendor_source, vendor_endpoint, request_key, symbol, symbol_name, \
              publication_date, source_published_at, available_at, institution_name, \
              analyst_name, rating_current, rating_previous, rating_change, is_first_rating, \
              target_price_min, target_price_max, raw_payload, raw_payload_hash, data_version_id) ",
        );

        builder.push_values(chunk, |mut row_builder, row| {
            row_builder
                .push_bind(&row.vendor)
                .push_bind(&row.vendor_source)
                .push_bind(&row.vendor_endpoint)
                .push_bind(&row.request_key)
                .push_bind(&row.symbol)
                .push_bind(&row.symbol_name)
                .push_bind(row.publication_date)
                .push_bind(row.source_published_at)
                .push_bind(row.available_at)
                .push_bind(&row.institution_name)
                .push_bind(&row.analyst_name)
                .push_bind(&row.rating_current)
                .push_bind(&row.rating_previous)
                .push_bind(&row.rating_change)
                .push_bind(&row.is_first_rating)
                .push_bind(row.target_price_min)
                .push_bind(row.target_price_max)
                .push_bind(&row.raw_payload)
                .push_bind(&row.raw_payload_hash)
                .push_bind(data_version_id);
        });

        builder.push(
            " ON CONFLICT (vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash) \
              DO UPDATE SET \
                vendor_source = EXCLUDED.vendor_source, \
                symbol_name = EXCLUDED.symbol_name, \
                source_published_at = EXCLUDED.source_published_at, \
                available_at = EXCLUDED.available_at, \
                institution_name = EXCLUDED.institution_name, \
                analyst_name = EXCLUDED.analyst_name, \
                rating_current = EXCLUDED.rating_current, \
                rating_previous = EXCLUDED.rating_previous, \
                rating_change = EXCLUDED.rating_change, \
                is_first_rating = EXCLUDED.is_first_rating, \
                target_price_min = EXCLUDED.target_price_min, \
                target_price_max = EXCLUDED.target_price_max, \
                raw_payload = EXCLUDED.raw_payload, \
                data_version_id = EXCLUDED.data_version_id, \
                updated_at = now()",
        );

        let result = builder.build().execute(db).await?;
        saved += result.rows_affected() as usize;
    }
    Ok(saved)
}



fn broad_analyst_revision_breakdown_limit(limit: Option<usize>) -> i64 {
    limit
        .unwrap_or(BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT)
        .clamp(1, BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT) as i64
}



fn akshare_analyst_revision_probe_status(probes: &[Value]) -> &'static str {
    if probes.iter().any(|probe| {
        matches!(
            probe.get("status").and_then(|status| status.as_str()),
            Some("ok" | "ok_empty")
        )
    }) {
        "available"
    } else if probes.iter().any(|probe| {
        probe.get("status").and_then(|status| status.as_str()) == Some("runtime_not_configured")
    }) {
        "runtime_not_configured"
    } else {
        "error"
    }
}



fn akshare_analyst_revision_known_endpoint_gate(source: &str) -> Option<Value> {
    match source {
        "stock_profit_forecast_em" => Some(json!({
            "source": source,
            "query_scope": "current_snapshot",
            "status": "blocked_snapshot_not_pit_ready",
            "probes": [],
            "blocked_reason": "current snapshot has no historical snapshot date or source publication timestamp; cannot reconstruct 2014-2026 PIT analyst revision history",
            "admission_gate": "do_not_sync_without_prospective_snapshot_archive_or_vendor_history"
        })),
        "stock_institute_recommend"
        | "stock_institute_recommend_detail"
        | "stock_profit_forecast_ths" => Some(json!({
            "source": source,
            "query_scope": "parser_unstable_endpoint",
            "status": "blocked_parse_unreliable",
            "probes": [],
            "blocked_reason": "prior AkShare smoke observed parser/XML failures; repeatable source behavior must be proven before schema or sync",
            "admission_gate": "do_not_sync_until_permission_smoke_is_repeatable"
        })),
        _ => None,
    }
}



async fn run_akshare_analyst_revision_probe(
    python: &str,
    source: &str,
    request_key: &str,
    scope: &str,
    row_limit: usize,
) -> Value {
    if !Path::new(python).exists() {
        return json!({
            "source": source,
            "scope": scope,
            "request_key": request_key,
            "status": "runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "python": python,
            "error": "AKSHARE_PYTHON is not configured and /tmp/akshare-smoke/bin/python is missing"
        });
    }

    let script = r#"
import json
import sys

endpoint = sys.argv[1]
request_key = sys.argv[2]
row_limit = int(sys.argv[3])

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

try:
    import akshare as ak
    import pandas as pd
    if endpoint == "stock_rank_forecast_cninfo":
        df = ak.stock_rank_forecast_cninfo(date=request_key)
    elif endpoint == "stock_research_report_em":
        df = ak.stock_research_report_em(symbol=request_key)
    elif endpoint == "stock_profit_forecast_em":
        df = ak.stock_profit_forecast_em()
    else:
        raise ValueError(f"unsupported_endpoint:{endpoint}")

    records = []
    for row in df.head(row_limit).to_dict(orient="records"):
        records.append({str(key): scrub(value) for key, value in row.items()})

    def nonempty(value):
        try:
            if pd.isna(value):
                return False
        except Exception:
            pass
        return str(value).strip() != ""

    def normalize_date(value):
        if not nonempty(value):
            return None
        try:
            return pd.to_datetime(value).strftime("%Y%m%d")
        except Exception:
            return str(value).strip().replace("-", "")[:8] or None

    symbol_count = None
    publication_date_match_rows = None
    publication_date_mismatch_rows = None
    missing_publication_date_rows = None
    rating_change_nonnull_rows = None
    previous_rating_nonnull_rows = None
    missing_revision_semantics_rows = None

    if endpoint == "stock_rank_forecast_cninfo":
        symbol_count = int(df["证券代码"].nunique()) if "证券代码" in df.columns else 0
        if "发布日期" in df.columns:
            normalized_publication_dates = df["发布日期"].map(normalize_date)
            publication_date_match_rows = int((normalized_publication_dates == request_key).sum())
            missing_publication_date_rows = int(normalized_publication_dates.isna().sum())
            publication_date_mismatch_rows = int(((normalized_publication_dates.notna()) & (normalized_publication_dates != request_key)).sum())
        else:
            publication_date_match_rows = 0
            missing_publication_date_rows = int(len(df))
            publication_date_mismatch_rows = 0

        rating_change_present = df["评级变化"].map(nonempty) if "评级变化" in df.columns else pd.Series([False] * len(df))
        previous_rating_present = df["前一次投资评级"].map(nonempty) if "前一次投资评级" in df.columns else pd.Series([False] * len(df))
        rating_change_nonnull_rows = int(rating_change_present.sum())
        previous_rating_nonnull_rows = int(previous_rating_present.sum())
        missing_revision_semantics_rows = int((~(rating_change_present & previous_rating_present)).sum())

    print(json.dumps({
        "status": "ok" if len(df) > 0 else "ok_empty",
        "permission": "available",
        "akshare_version": getattr(ak, "__version__", None),
        "row_count": int(len(df)),
        "symbol_count": symbol_count,
        "publication_date_match_rows": publication_date_match_rows,
        "publication_date_mismatch_rows": publication_date_mismatch_rows,
        "missing_publication_date_rows": missing_publication_date_rows,
        "rating_change_nonnull_rows": rating_change_nonnull_rows,
        "previous_rating_nonnull_rows": previous_rating_nonnull_rows,
        "missing_revision_semantics_rows": missing_revision_semantics_rows,
        "fields": [str(field) for field in list(df.columns)],
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
        .arg(source)
        .arg(request_key)
        .arg(row_limit.to_string())
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "source": source,
                "scope": scope,
                "request_key": request_key,
                "status": "error",
                "permission": "unknown_or_unavailable",
                "python": python,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "source": source,
                "scope": scope,
                "request_key": request_key,
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
            "error": "failed_to_parse_akshare_probe_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });

    if let Some(object) = payload.as_object_mut() {
        object.insert("source".to_string(), json!(source));
        object.insert("scope".to_string(), json!(scope));
        object.insert("request_key".to_string(), json!(request_key));
        object.insert("python".to_string(), json!(python));
        object.insert("exit_status".to_string(), json!(output.status.code()));
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    payload
}



pub(crate) fn akshare_analyst_revision_is_empty_dataframe_length_mismatch(payload: &Value) -> bool {
    let status_is_error = payload
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status == "error")
        .unwrap_or(false);
    let error_type_is_value_error = payload
        .get("error_type")
        .and_then(Value::as_str)
        .map(|error_type| error_type == "ValueError")
        .unwrap_or(false);
    let Some(error) = payload.get("error").and_then(Value::as_str) else {
        return false;
    };

    status_is_error
        && error_type_is_value_error
        && error.contains("Length mismatch")
        && error.contains("Expected axis has 0 elements")
        && error.contains("new values have 11 elements")
}



async fn run_akshare_analyst_revision_full_date_fetch(python: &str, request_key: &str) -> Value {
    let source = "stock_rank_forecast_cninfo";
    if !Path::new(python).exists() {
        return json!({
            "source": source,
            "scope": "bounded_calendar_day_raw_sync",
            "request_key": request_key,
            "status": "runtime_not_configured",
            "permission": "unknown_or_unavailable",
            "python": python,
            "error": "AKSHARE_PYTHON is not configured and /tmp/akshare-smoke/bin/python is missing"
        });
    }

    let script = r#"
import json
import sys

request_key = sys.argv[1]

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

try:
    import akshare as ak
    df = ak.stock_rank_forecast_cninfo(date=request_key)
    if df is None:
        df = []
    records = []
    fields = []
    if hasattr(df, "columns"):
        fields = [str(field) for field in list(df.columns)]
        for row in df.to_dict(orient="records"):
            records.append({str(key): scrub(value) for key, value in row.items()})
        row_count = int(len(df))
    else:
        row_count = 0

    print(json.dumps({
        "status": "ok" if row_count > 0 else "ok_empty",
        "permission": "available",
        "akshare_version": getattr(ak, "__version__", None),
        "row_count": row_count,
        "fields": fields,
        "records": records,
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
        .arg(request_key)
        .env("HOME", home)
        .env("PYTHONUNBUFFERED", "1")
        .output();

    let output = match timeout(StdDuration::from_secs(timeout_seconds), child).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return json!({
                "source": source,
                "scope": "bounded_calendar_day_raw_sync",
                "request_key": request_key,
                "status": "error",
                "permission": "unknown_or_unavailable",
                "python": python,
                "error": error.to_string(),
            });
        }
        Err(_) => {
            return json!({
                "source": source,
                "scope": "bounded_calendar_day_raw_sync",
                "request_key": request_key,
                "status": "timeout",
                "permission": "unknown_or_unavailable",
                "python": python,
                "timeout_seconds": timeout_seconds,
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let payload: Value = serde_json::from_str(&stdout).unwrap_or_else(|_| {
        json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error": "failed_to_parse_akshare_full_fetch_stdout",
            "stdout": stdout,
            "stderr": stderr,
        })
    });
    let mut payload = normalize_akshare_analyst_revision_full_fetch_payload(payload);

    if let Some(object) = payload.as_object_mut() {
        object.insert("source".to_string(), json!(source));
        object.insert("scope".to_string(), json!("bounded_calendar_day_raw_sync"));
        object.insert("request_key".to_string(), json!(request_key));
        object.insert("python".to_string(), json!(python));
        object.insert("exit_status".to_string(), json!(output.status.code()));
        if !stderr.is_empty() {
            object.insert("stderr".to_string(), json!(stderr));
        }
    }
    payload
}



async fn run_akshare_analyst_revision_full_date_fetch_with_retry(
    python: &str,
    request_key: &str,
) -> (Value, usize) {
    let max_attempts = AKSHARE_ANALYST_REVISION_SYNC_MAX_RETRIES + 1;
    let mut latest = json!({
        "status": "error",
        "error": "akshare_fetch_not_started",
    });
    for attempt in 1..=max_attempts {
        latest = run_akshare_analyst_revision_full_date_fetch(python, request_key).await;
        let status = latest
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("error");
        if !akshare_analyst_revision_should_retry_fetch_status(status) || attempt == max_attempts {
            if let Some(object) = latest.as_object_mut() {
                object.insert("fetch_attempts".to_string(), json!(attempt));
                object.insert(
                    "max_retries".to_string(),
                    json!(AKSHARE_ANALYST_REVISION_SYNC_MAX_RETRIES),
                );
            }
            return (latest, attempt);
        }
        tracing::warn!(
            request_key = %request_key,
            attempt,
            status,
            "AkShare analyst revision full-date fetch retrying after transient failure"
        );
    }
    (latest, max_attempts)
}



pub async fn broad_analyst_revision_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<BroadAnalystRevisionAuditReq>,
) -> impl IntoResponse {
    match build_broad_analyst_revision_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/akshare/analyst-revision/schema-contract


pub async fn akshare_analyst_revision_schema_contract() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_akshare_analyst_revision_schema_contract()
    }))
}

/// POST /api/v1/quant/data/akshare/analyst-revision/permission-smoke


pub async fn akshare_analyst_revision_permission_smoke(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AkshareAnalystRevisionSmokeReq>,
) -> impl IntoResponse {
    match build_akshare_analyst_revision_permission_smoke(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/akshare/analyst-revision/available-at-audit


pub async fn akshare_analyst_revision_available_at_audit() -> impl IntoResponse {
    Json(json!({
        "code": 0,
        "data": phase7_akshare_analyst_revision_available_at_contract()
    }))
}

/// POST /api/v1/quant/data/akshare/analyst-revision/history-replay-audit


pub async fn akshare_analyst_revision_history_replay_audit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AkshareAnalystRevisionHistoryReplayAuditReq>,
) -> impl IntoResponse {
    match build_akshare_analyst_revision_history_replay_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/akshare/analyst-revision/sync-plan


pub async fn akshare_analyst_revision_sync_plan(
    Query(req): Query<AkshareAnalystRevisionSyncPlanReq>,
) -> impl IntoResponse {
    match build_akshare_analyst_revision_sync_plan(req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/akshare/analyst-revision/sync


pub async fn akshare_analyst_revision_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AkshareAnalystRevisionSyncReq>,
) -> impl IntoResponse {
    match run_akshare_analyst_revision_bounded_sync(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/akshare/analyst-revision/readiness-audit


pub async fn akshare_analyst_revision_readiness_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<AkshareAnalystRevisionReadinessAuditReq>,
) -> impl IntoResponse {
    match build_akshare_analyst_revision_readiness_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/akshare/analyst-revision/coverage-audit


pub async fn akshare_analyst_revision_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<AkshareAnalystRevisionCoverageAuditReq>,
) -> impl IntoResponse {
    match build_akshare_analyst_revision_coverage_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/phase7-optional-source-coverage-sync


async fn build_akshare_analyst_revision_permission_smoke(
    state: &AppState,
    req: AkshareAnalystRevisionSmokeReq,
) -> Result<Value, String> {
    let sources = akshare_analyst_revision_smoke_sources(&req.sources);
    let dates = akshare_analyst_revision_smoke_dates(&req.dates)?;
    let row_limit = akshare_analyst_revision_smoke_limit(req.limit);
    let python = akshare_analyst_revision_python_path(req.python);
    let symbols = resolve_phase7_permission_smoke_symbols(state, &req.symbols)
        .await?
        .into_iter()
        .map(|symbol| akshare_analyst_revision_symbol(&symbol))
        .take(AKSHARE_ANALYST_REVISION_MAX_SYMBOLS)
        .collect::<Vec<_>>();

    let mut source_results = Vec::new();
    for source in sources {
        if let Some(blocked) = akshare_analyst_revision_known_endpoint_gate(&source) {
            source_results.push(blocked);
            continue;
        }

        match source.as_str() {
            "stock_rank_forecast_cninfo" => {
                let mut probes = Vec::new();
                for date in &dates {
                    probes.push(
                        run_akshare_analyst_revision_probe(
                            &python,
                            &source,
                            date,
                            "history_publication_date",
                            row_limit,
                        )
                        .await,
                    );
                }
                source_results.push(json!({
                    "source": source,
                    "query_scope": "history_publication_date",
                    "status": akshare_analyst_revision_probe_status(&probes),
                    "probes": probes,
                    "symbol_filter_supported": false,
                    "history_date_smoke": "performed",
                    "source_semantics": "analyst rating and target-price forecast records by publication date",
                    "pit_available_at": "发布日期 is date-level available_at/source_published_at candidate; intraday use remains blocked until source publication timestamp is audited",
                    "required_fields_for_schema_audit": ["证券代码", "发布日期", "研究机构简称", "研究员名称", "投资评级", "评级变化", "前一次投资评级", "目标价格-上限", "目标价格-下限"],
                    "admission_gate": "permission_smoke_only_schema_available_at_history_coverage_audit_required_before_sync"
                }));
            }
            "stock_research_report_em" => {
                let mut probes = Vec::new();
                for symbol in &symbols {
                    probes.push(
                        run_akshare_analyst_revision_probe(
                            &python,
                            &source,
                            symbol,
                            "single_symbol_research_report_list",
                            row_limit,
                        )
                        .await,
                    );
                }
                source_results.push(json!({
                    "source": source,
                    "query_scope": "single_symbol_research_report_list",
                    "status": akshare_analyst_revision_probe_status(&probes),
                    "probes": probes,
                    "symbol_filter_supported": true,
                    "sample_symbols": symbols,
                    "source_semantics": "single-stock research report evidence layer",
                    "pit_available_at": "report date is candidate availability but symbol fanout, pagination stability and source publication lag must pass before broad-base use",
                    "required_fields_for_schema_audit": ["日期", "评级", "机构", "报告名称", "PDF链接"],
                    "admission_gate": "permission_smoke_only_low_fanout_evidence_no_factor_until_full_symbol_fanout_coverage_passes"
                }));
            }
            unsupported => {
                source_results.push(json!({
                    "source": unsupported,
                    "status": "unsupported_source",
                    "supported_sources": AKSHARE_ANALYST_REVISION_SMOKE_ALLOWED,
                }));
            }
        }
    }

    Ok(json!({
        "audit_version": "p3.23a-akshare-analyst-revision-permission-history-smoke-v1",
        "mode": "read_only_vendor_permission_history_date_smoke",
        "vendor": "akshare",
        "python": python,
        "row_limit_per_probe": row_limit,
        "date_keys": dates,
        "sample_symbols": symbols,
        "sources": source_results,
        "schema_contract_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/schema-contract",
        "available_at_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/available-at-audit",
        "history_replay_audit_endpoint": "POST /api/v1/quant/data/akshare/analyst-revision/history-replay-audit",
        "coverage_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/coverage-audit",
        "admission_gate": "permission_history_smoke_only_no_schema_no_sync_no_factor_p310_wfa_v19",
        "notes": [
            "This endpoint runs bounded read-only AkShare probes through a configured Python runtime. It never writes raw tables, factors, data versions, WFA tasks, or strategy configs.",
            "stock_rank_forecast_cninfo is the primary analyst revision candidate because it exposes publication date, rating change and previous rating fields.",
            "stock_research_report_em is treated as a low-fanout evidence layer until full symbol fanout coverage and pagination/history stability are audited.",
            "Snapshot or parser-unstable endpoints remain blocked even if a smoke call returns rows."
        ]
    }))
}



async fn build_akshare_analyst_revision_sync_plan(
    req: AkshareAnalystRevisionSyncPlanReq,
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
    let batch_mode = req
        .batch
        .unwrap_or_else(|| AKSHARE_ANALYST_REVISION_SYNC_PLAN_DEFAULT_BATCH.to_string())
        .trim()
        .to_ascii_lowercase();
    let batches = akshare_analyst_revision_sync_plan_batches(start, end, &batch_mode)?;
    Ok(akshare_analyst_revision_sync_plan_response(
        start,
        end,
        &batch_mode,
        batches,
    ))
}



async fn run_akshare_analyst_revision_bounded_sync(
    state: &AppState,
    req: AkshareAnalystRevisionSyncReq,
) -> Result<Value, String> {
    if req.background {
        return Err(
            "AkShare analyst revision P3.23D sync currently requires background=false; run <=100 calendar-day batches synchronously so readiness/coverage can be audited immediately."
                .to_string(),
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
    let calendar_day_count = validate_akshare_analyst_revision_sync_range(start, end)?;
    let table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('public.market_vendor_analyst_revision_raw')::text IS NOT NULL",
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to inspect AkShare analyst revision raw table: {error}"))?;
    if !table_exists {
        return Err(
            "market_vendor_analyst_revision_raw does not exist; apply sql/phase7_akshare_analyst_revision_source.sql before bounded sync"
                .to_string(),
        );
    }

    let data_version_id = req.data_version_id.clone().unwrap_or_else(|| {
        format!(
            "akshare-analyst-revision-{}-{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        )
    });
    let task_id = bounded_phase7_task_id(&[data_version_id.as_str(), "raw-sync"]);
    let sync_req = req.clone().into_sync_task_req();
    register_sync_task(state, &task_id, &sync_req, "running").await?;
    quant_data::repository::create_data_version(
        &state.db,
        &data_version_id,
        "AkShare analyst revision raw PIT sync",
        AKSHARE_ANALYST_REVISION_TASK_SOURCE,
        &["market_vendor_analyst_revision_raw"],
        start,
        end,
    )
    .await
    .map_err(|error| {
        format!("Failed to create AkShare analyst revision data_version {data_version_id}: {error}")
    })?;

    let days = akshare_analyst_revision_calendar_days(start, end);
    let total_days = days.len() as i32;
    quant_data::repository::heartbeat_sync_task(&state.db, &task_id, total_days, 0, 0, 0)
        .await
        .map_err(|error| format!("Failed to heartbeat AkShare analyst revision task: {error}"))?;
    let open_dates = load_akshare_analyst_revision_available_open_dates(&state.db, start, end)
        .await
        .map_err(|error| {
            format!("Failed to load market_trade_calendar open dates for AkShare analyst revision: {error}")
        })?;
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let source = AKSHARE_ANALYST_REVISION_ATTEMPT_SOURCE;

    let mut completed_dates = 0i32;
    let mut failed_dates = 0i32;
    let mut empty_dates = 0i32;
    let mut fetched_rows = 0i64;
    let mut mapped_rows = 0i64;
    let mut upserted_rows = 0i64;
    let mut malformed_rows = 0i64;
    let mut per_date = Vec::new();
    let mut failure_messages = Vec::new();

    for (index, day) in days.iter().enumerate() {
        let request_key = day.format("%Y%m%d").to_string();
        let attempt_symbol = format!("calendar:{request_key}");
        let (fetch, fetch_attempts) =
            run_akshare_analyst_revision_full_date_fetch_with_retry(&python, &request_key).await;
        let status = fetch
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("error");
        let row_count = fetch.get("row_count").and_then(Value::as_i64).unwrap_or(0);
        fetched_rows += row_count;

        if status == "ok_empty" || (status == "ok" && row_count == 0) {
            completed_dates += 1;
            empty_dates += 1;
            quant_data::repository::upsert_sync_attempt(
                &state.db,
                source,
                &attempt_symbol,
                *day,
                *day,
                &task_id,
                "completed",
                0,
                None,
            )
            .await
            .map_err(|error| format!("Failed to record AkShare empty sync attempt: {error}"))?;
            per_date.push(json!({
                "date": request_key,
                "status": "completed_empty",
                "fetched_rows": row_count,
                "mapped_rows": 0,
                "upserted_rows": 0,
                "fetch_attempts": fetch_attempts,
            }));
        } else if status == "ok" {
            let records = fetch
                .get("records")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    format!(
                        "AkShare full fetch for {request_key} returned ok without records array"
                    )
                })?;
            let mut rows = Vec::new();
            let mut date_errors = Vec::new();
            for record in records {
                match record.as_object() {
                    Some(object) => match akshare_analyst_revision_raw_row_from_record(
                        object,
                        &request_key,
                        &open_dates,
                    ) {
                        Ok(row) => rows.push(row),
                        Err(error) => date_errors.push(error),
                    },
                    None => date_errors.push("record_not_object".to_string()),
                }
            }

            if !date_errors.is_empty() {
                failed_dates += 1;
                malformed_rows += date_errors.len() as i64;
                let error_message = format!(
                    "akshare_analyst_revision_malformed_rows:{}:{}",
                    date_errors.len(),
                    date_errors
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join("|")
                );
                failure_messages.push(format!("{request_key}:{error_message}"));
                quant_data::repository::upsert_sync_attempt(
                    &state.db,
                    source,
                    &attempt_symbol,
                    *day,
                    *day,
                    &task_id,
                    "failed",
                    0,
                    Some(&error_message),
                )
                .await
                .map_err(|error| {
                    format!("Failed to record AkShare malformed sync attempt: {error}")
                })?;
                per_date.push(json!({
                    "date": request_key,
                    "status": "failed_malformed_rows",
                    "fetched_rows": row_count,
                    "mapped_rows": rows.len(),
                    "upserted_rows": 0,
                    "malformed_rows": date_errors.len(),
                    "fetch_attempts": fetch_attempts,
                    "errors": date_errors.into_iter().take(10).collect::<Vec<_>>(),
                }));
            } else {
                let upserted = upsert_akshare_analyst_revision_raw_rows(
                    &state.db,
                    &rows,
                    &data_version_id,
                )
                .await
                .map_err(|error| {
                    format!("Failed to upsert AkShare analyst revision raw rows for {request_key}: {error}")
                })?;
                completed_dates += 1;
                mapped_rows += rows.len() as i64;
                upserted_rows += upserted as i64;
                quant_data::repository::upsert_sync_attempt(
                    &state.db,
                    source,
                    &attempt_symbol,
                    *day,
                    *day,
                    &task_id,
                    "completed",
                    rows.len() as i64,
                    None,
                )
                .await
                .map_err(|error| {
                    format!("Failed to record AkShare completed sync attempt: {error}")
                })?;
                per_date.push(json!({
                    "date": request_key,
                    "status": "completed",
                    "fetched_rows": row_count,
                    "mapped_rows": rows.len(),
                    "upserted_rows": upserted,
                    "fetch_attempts": fetch_attempts,
                }));
            }
        } else {
            failed_dates += 1;
            let error_message = fetch
                .get("error")
                .and_then(Value::as_str)
                .or_else(|| fetch.get("error_type").and_then(Value::as_str))
                .unwrap_or(status)
                .to_string();
            failure_messages.push(format!("{request_key}:{error_message}"));
            quant_data::repository::upsert_sync_attempt(
                &state.db,
                source,
                &attempt_symbol,
                *day,
                *day,
                &task_id,
                "failed",
                0,
                Some(&error_message),
            )
            .await
            .map_err(|error| format!("Failed to record AkShare failed sync attempt: {error}"))?;
            per_date.push(json!({
                "date": request_key,
                "status": status,
                "fetched_rows": row_count,
                "mapped_rows": 0,
                "upserted_rows": 0,
                "fetch_attempts": fetch_attempts,
                "error": error_message,
            }));
        }

        let finished = completed_dates + failed_dates;
        let progress = ((finished as f64 / total_days as f64) * 100.0).round() as i32;
        quant_data::repository::heartbeat_sync_task(
            &state.db,
            &task_id,
            total_days,
            completed_dates,
            failed_dates,
            progress,
        )
        .await
        .map_err(|error| format!("Failed to heartbeat AkShare analyst revision task: {error}"))?;
        tracing::info!(
            task_id = %task_id,
            request_key = %request_key,
            completed_dates,
            failed_dates,
            index = index + 1,
            total_days,
            "AkShare analyst revision bounded raw sync progress"
        );
    }

    let final_status = if failed_dates == 0 {
        "completed"
    } else if completed_dates == 0 {
        "failed"
    } else {
        "partial"
    };
    let error_summary = failure_messages
        .iter()
        .take(20)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if failed_dates > 0 {
        quant_data::repository::update_sync_task_with_error(
            &state.db,
            &task_id,
            final_status,
            total_days,
            completed_dates,
            failed_dates,
            &error_summary,
        )
        .await
        .map_err(|error| format!("Failed to finalize AkShare analyst revision task: {error}"))?;
    } else {
        quant_data::repository::update_sync_task(
            &state.db,
            &task_id,
            final_status,
            total_days,
            completed_dates,
            failed_dates,
        )
        .await
        .map_err(|error| format!("Failed to finalize AkShare analyst revision task: {error}"))?;
    }

    Ok(json!({
        "audit_version": "p3.23d-akshare-analyst-revision-bounded-sync-v1",
        "source_id": "multi_vendor_analyst_revision",
        "vendor": "akshare",
        "vendor_endpoint": "stock_rank_forecast_cninfo",
        "mode": "bounded_calendar_day_raw_sync",
        "task_id": task_id,
        "data_version_id": data_version_id,
        "python": python,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
            "calendar_day_count": calendar_day_count,
        },
        "status": final_status,
        "summary": {
            "completed_dates": completed_dates,
            "failed_dates": failed_dates,
            "empty_dates": empty_dates,
            "fetched_rows": fetched_rows,
            "mapped_rows": mapped_rows,
            "upserted_rows": upserted_rows,
            "malformed_rows": malformed_rows,
            "max_retries_per_date": AKSHARE_ANALYST_REVISION_SYNC_MAX_RETRIES,
        },
        "per_date": per_date,
        "failure_messages": failure_messages,
        "next_gate": {
            "readiness_audit": "run GET /api/v1/quant/data/akshare/analyst-revision/readiness-audit for the same date range",
            "coverage_audit": "run GET /api/v1/quant/data/akshare/analyst-revision/coverage-audit after readiness passes",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "notes": [
            "This sync writes raw evidence rows only; it does not build factors, diagnostics, WFA sleeves, or v19 strategy candidates.",
            "Each request_key is a calendar publication date. Empty AkShare dates are recorded as completed attempts with row_count=0.",
            "Rows are accepted only when row publication_date equals request_key. Malformed dates fail the whole request date to avoid mixed-quality PIT evidence."
        ]
    }))
}



async fn build_akshare_analyst_revision_readiness_audit(
    state: &AppState,
    req: AkshareAnalystRevisionReadinessAuditReq,
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

    let table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('public.market_vendor_analyst_revision_raw')::text IS NOT NULL",
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to inspect AkShare analyst revision raw table: {error}"))?;

    let mut row_count = 0i64;
    let mut pit_violation_rows = 0i64;
    let mut missing_source_published_at_rows = 0i64;
    let mut missing_current_rating_rows = 0i64;
    let mut missing_revision_semantics_rows = 0i64;
    let mut duplicate_key_rows = 0i64;
    if table_exists {
        let summary = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            r#"
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(*) FILTER (
                       WHERE available_at < publication_date
                          OR publication_date IS NULL
                   )::bigint AS pit_violation_rows,
                   COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows,
                   COUNT(*) FILTER (WHERE rating_current IS NULL)::bigint AS missing_current_rating_rows,
                   COUNT(*) FILTER (
                       WHERE rating_previous IS NULL
                          OR rating_change IS NULL
                   )::bigint AS missing_revision_semantics_rows
            FROM market_vendor_analyst_revision_raw
            WHERE publication_date >= $1 AND publication_date <= $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize AkShare analyst revision readiness: {error}")
        })?;
        row_count = summary.0;
        pit_violation_rows = summary.1;
        missing_source_published_at_rows = summary.2;
        missing_current_rating_rows = summary.3;
        missing_revision_semantics_rows = summary.4;
        duplicate_key_rows = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(SUM(cnt - 1), 0)::bigint
            FROM (
                SELECT vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash,
                       COUNT(*)::bigint AS cnt
                FROM market_vendor_analyst_revision_raw
                WHERE publication_date >= $1 AND publication_date <= $2
                GROUP BY vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash
                HAVING COUNT(*) > 1
            ) d
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize AkShare analyst revision duplicate keys: {error}")
        })?;
    }

    let decision = decide_akshare_analyst_revision_readiness(
        table_exists,
        row_count,
        pit_violation_rows,
        missing_source_published_at_rows,
        missing_current_rating_rows,
        missing_revision_semantics_rows,
        duplicate_key_rows,
    );

    Ok(json!({
        "audit_version": "p3.23c-akshare-analyst-revision-readiness-v1",
        "source_id": "multi_vendor_analyst_revision",
        "vendor": "akshare",
        "vendor_endpoint": "stock_rank_forecast_cninfo",
        "mode": "read_only_schema_raw_pit_semantics_readiness",
        "table": "market_vendor_analyst_revision_raw",
        "ddl_path": "sql/phase7_akshare_analyst_revision_source.sql",
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "table_exists": table_exists,
        "summary": {
            "row_count": row_count,
            "pit_violation_rows": pit_violation_rows,
            "missing_source_published_at_rows": missing_source_published_at_rows,
            "missing_current_rating_rows": missing_current_rating_rows,
            "missing_revision_semantics_rows": missing_revision_semantics_rows,
            "duplicate_key_rows": duplicate_key_rows,
        },
        "decision": decision,
        "prohibited": [
            "factor_build_before_full_history_calendar_day_coverage_pit_quality_audit",
            "p310_before_coverage_readiness_and_correlation_audit",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



async fn resolve_akshare_analyst_revision_history_dates(
    state: &AppState,
    req: &AkshareAnalystRevisionHistoryReplayAuditReq,
) -> Result<Vec<String>, String> {
    if !req.dates.is_empty() {
        return akshare_analyst_revision_history_dates(&req.dates);
    }

    let current_year = Utc::now().date_naive().year();
    let start_year = req.start_year.unwrap_or(2014);
    let end_year = req.end_year.unwrap_or(current_year);
    if start_year > end_year {
        return Err("start_year must be <= end_year".to_string());
    }
    if end_year > current_year {
        return Err("end_year cannot be in the future".to_string());
    }

    let start_date = NaiveDate::from_ymd_opt(start_year, 1, 1)
        .ok_or_else(|| format!("invalid start_year: {start_year}"))?;
    let end_date = NaiveDate::from_ymd_opt(end_year, 12, 31)
        .ok_or_else(|| format!("invalid end_year: {end_year}"))?;
    let rows = sqlx::query_as::<_, (i32, NaiveDate)>(
        r#"
        WITH open_days AS (
            SELECT DISTINCT trade_date
            FROM market_trade_calendar
            WHERE is_open = true
              AND trade_date >= $1
              AND trade_date <= $2
        )
        SELECT EXTRACT(YEAR FROM trade_date)::int AS trade_year,
               MIN(trade_date) AS sample_date
        FROM open_days
        GROUP BY trade_year
        ORDER BY trade_year
        "#,
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(&state.db)
    .await
    .map_err(|error| {
        format!("Failed to resolve AkShare analyst revision history replay dates: {error}")
    })?;

    let expected_years: BTreeSet<i32> = (start_year..=end_year).collect();
    let observed_years: BTreeSet<i32> = rows.iter().map(|(year, _)| *year).collect();
    let missing_years: Vec<i32> = expected_years
        .difference(&observed_years)
        .copied()
        .collect();
    if !missing_years.is_empty() {
        return Err(format!(
            "market_trade_calendar has no open-day sample for years: {:?}",
            missing_years
        ));
    }
    if rows.len() > AKSHARE_ANALYST_REVISION_HISTORY_MAX_DATES {
        return Err(format!(
            "history replay audit resolved {} dates, above max {}",
            rows.len(),
            AKSHARE_ANALYST_REVISION_HISTORY_MAX_DATES
        ));
    }

    Ok(rows
        .into_iter()
        .map(|(_, date)| date.format("%Y%m%d").to_string())
        .collect())
}



async fn build_akshare_analyst_revision_history_replay_audit(
    state: &AppState,
    req: AkshareAnalystRevisionHistoryReplayAuditReq,
) -> Result<Value, String> {
    let dates = resolve_akshare_analyst_revision_history_dates(state, &req).await?;
    let row_limit = akshare_analyst_revision_smoke_limit(req.limit);
    let python = akshare_analyst_revision_python_path(req.python);
    let source = "stock_rank_forecast_cninfo";

    let mut probes = Vec::new();
    let mut per_date = Vec::new();
    let mut available_date_count = 0usize;
    let mut error_date_count = 0usize;
    let mut empty_date_count = 0usize;
    let mut row_count = 0i64;
    let mut symbol_count_sum = 0i64;
    let mut publication_date_mismatch_rows = 0i64;
    let mut missing_publication_date_rows = 0i64;
    let mut rating_change_nonnull_rows = 0i64;
    let mut previous_rating_nonnull_rows = 0i64;
    let mut missing_revision_semantics_rows = 0i64;

    for date in &dates {
        let probe = run_akshare_analyst_revision_probe(
            &python,
            source,
            date,
            "history_replay_available_at_audit",
            row_limit,
        )
        .await;
        let status = probe
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("error");
        let rows = probe
            .get("row_count")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        if status == "ok" && rows > 0 {
            available_date_count += 1;
        } else if status == "ok_empty" || (status == "ok" && rows == 0) {
            empty_date_count += 1;
        } else {
            error_date_count += 1;
        }

        let symbols = probe
            .get("symbol_count")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let mismatch_rows = probe
            .get("publication_date_mismatch_rows")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let missing_pub_rows = probe
            .get("missing_publication_date_rows")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let rating_rows = probe
            .get("rating_change_nonnull_rows")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let previous_rows = probe
            .get("previous_rating_nonnull_rows")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let missing_revision_rows = probe
            .get("missing_revision_semantics_rows")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);

        row_count += rows;
        symbol_count_sum += symbols;
        publication_date_mismatch_rows += mismatch_rows;
        missing_publication_date_rows += missing_pub_rows;
        rating_change_nonnull_rows += rating_rows;
        previous_rating_nonnull_rows += previous_rows;
        missing_revision_semantics_rows += missing_revision_rows;

        per_date.push(json!({
            "date": date,
            "status": status,
            "row_count": rows,
            "symbol_count": symbols,
            "publication_date_mismatch_rows": mismatch_rows,
            "missing_publication_date_rows": missing_pub_rows,
            "rating_change_nonnull_rows": rating_rows,
            "previous_rating_nonnull_rows": previous_rows,
            "missing_revision_semantics_rows": missing_revision_rows,
        }));
        probes.push(probe);
    }

    let decision = decide_akshare_analyst_revision_history_replay_audit(
        dates.len(),
        available_date_count,
        error_date_count,
        empty_date_count,
        row_count,
        publication_date_mismatch_rows,
        missing_publication_date_rows,
        missing_revision_semantics_rows,
    );
    let passed = decision["passed"].as_bool().unwrap_or(false);

    Ok(json!({
        "audit_version": "p3.23b-akshare-analyst-revision-history-replay-audit-v1",
        "source_id": "multi_vendor_analyst_revision",
        "vendor": "akshare",
        "vendor_endpoint": source,
        "mode": "read_only_history_date_replay_available_at_audit",
        "python": python,
        "sample_dates": dates,
        "sample_date_count": dates.len(),
        "row_limit_per_probe": row_limit,
        "summary": {
            "requested_date_count": dates.len(),
            "available_date_count": available_date_count,
            "error_date_count": error_date_count,
            "empty_date_count": empty_date_count,
            "row_count": row_count,
            "symbol_count_sum": symbol_count_sum,
            "publication_date_mismatch_rows": publication_date_mismatch_rows,
            "missing_publication_date_rows": missing_publication_date_rows,
            "rating_change_nonnull_rows": rating_change_nonnull_rows,
            "previous_rating_nonnull_rows": previous_rating_nonnull_rows,
            "missing_revision_semantics_rows": missing_revision_semantics_rows,
        },
        "per_date": per_date,
        "decision": decision,
        "admission_gate": "history_replay_audit_only_schema_review_next_no_sync_no_factor_no_p310_no_wfa_no_v19",
        "next_step": if passed {
            "manual_schema_review_and_raw_schema_contract_update_before_any_bounded_sync"
        } else {
            "fix_vendor_history_replay_or_available_at_semantics_before_schema_review"
        },
        "probes": probes,
        "notes": [
            "This endpoint is read-only and only calls AkShare for representative history dates.",
            "Passing this audit means the source can move to manual schema review only; bounded sync, factor builder, P3.10, WFA and v19 training remain blocked.",
            "Publication date must equal the requested history date. Without audited intraday publication timestamp, intraday trading must still use conservative next-session availability."
        ]
    }))
}



async fn build_akshare_analyst_revision_correlation_audit(
    db: &sqlx::PgPool,
    start: NaiveDate,
    end: NaiveDate,
    raw_table_exists: bool,
) -> Result<Value, String> {
    if !raw_table_exists {
        return Ok(json!({
            "status": "blocked_until_raw_table_exists",
            "decision": "blocked_until_correlation_sample_available",
            "sample_rows": 0,
        }));
    }

    let daily_bar_exists = table_exists(db, "market_stock_daily_bar").await?;
    let moneyflow_exists = table_exists(db, "market_stock_moneyflow").await?;
    if !daily_bar_exists || !moneyflow_exists {
        return Ok(json!({
            "status": "blocked_missing_reference_tables",
            "decision": "blocked_until_correlation_sample_available",
            "reference_tables": {
                "market_stock_daily_bar": daily_bar_exists,
                "market_stock_moneyflow": moneyflow_exists,
            },
            "sample_rows": 0,
        }));
    }

    let stats: (i64, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>) =
        sqlx::query_as(
            r#"
            WITH raw_events AS (
                SELECT CASE
                           WHEN symbol ~ '^[0-9]{6}\.' THEN symbol
                           WHEN LEFT(symbol, 1) IN ('6', '9') THEN symbol || '.SH'
                           WHEN LEFT(symbol, 1) IN ('0', '2', '3') THEN symbol || '.SZ'
                           WHEN LEFT(symbol, 1) IN ('4', '8') THEN symbol || '.BJ'
                           ELSE symbol
                       END AS normalized_symbol,
                       available_at AS trade_date,
                       rating_change
                FROM market_vendor_analyst_revision_raw
                WHERE publication_date >= $1
                  AND publication_date <= $2
            ),
            daily_features AS (
                SELECT normalized_symbol AS symbol,
                       trade_date,
                       COUNT(*)::double precision AS event_count,
                       SUM(CASE
                               WHEN rating_change = '调高' THEN 1
                               WHEN rating_change = '调低' THEN -1
                               ELSE 0
                           END)::double precision AS rating_change_net,
                       SUM(CASE WHEN rating_change = '调高' THEN 1 ELSE 0 END)::double precision AS upgrade_count,
                       SUM(CASE WHEN rating_change = '调低' THEN 1 ELSE 0 END)::double precision AS downgrade_count
                FROM raw_events
                GROUP BY normalized_symbol, trade_date
            ),
            joined AS (
                SELECT features.event_count,
                       features.rating_change_net,
                       features.upgrade_count,
                       features.downgrade_count,
                       (money.net_mf_amount::double precision / NULLIF(bar.amount::double precision, 0)) AS moneyflow_net_to_amount,
                       LN(NULLIF(bar.amount::double precision, 0)) AS ln_amount,
                       ABS((bar.close::double precision - bar.pre_close::double precision)
                           / NULLIF(bar.pre_close::double precision, 0)) AS abs_return
                FROM daily_features features
                JOIN market_stock_daily_bar bar
                  ON bar.symbol = features.symbol
                 AND bar.trade_date = features.trade_date
                LEFT JOIN market_stock_moneyflow money
                  ON money.symbol = features.symbol
                 AND money.trade_date = features.trade_date
            )
            SELECT COUNT(*)::bigint AS sample_rows,
                   CORR(rating_change_net, moneyflow_net_to_amount) AS corr_rating_net_moneyflow,
                   CORR(event_count, ln_amount) AS corr_event_count_ln_amount,
                   CORR(event_count, abs_return) AS corr_event_count_abs_return,
                   CORR(upgrade_count, moneyflow_net_to_amount) AS corr_upgrade_moneyflow,
                   CORR(downgrade_count, moneyflow_net_to_amount) AS corr_downgrade_moneyflow
            FROM joined
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .map_err(|error| {
            format!("Failed to build AkShare analyst revision correlation audit: {error}")
        })?;

    let correlations = [stats.1, stats.2, stats.3, stats.4, stats.5];
    let max_abs_correlation = correlations
        .into_iter()
        .flatten()
        .map(f64::abs)
        .reduce(f64::max);
    let decision = margin_detail_correlation_decision(max_abs_correlation);

    Ok(json!({
        "status": if decision == "passed_low_linear_correlation_screen" {
            "completed_low_linear_correlation_screen_passed"
        } else {
            "completed_correlation_screen_not_passed_or_needs_review"
        },
        "decision": decision,
        "sample_rows": stats.0,
        "max_abs_correlation": max_abs_correlation,
        "correlations": {
            "rating_change_net_vs_moneyflow_net_to_amount": stats.1,
            "event_count_vs_ln_amount_liquidity": stats.2,
            "event_count_vs_abs_return_price_volume": stats.3,
            "upgrade_count_vs_moneyflow_net_to_amount": stats.4,
            "downgrade_count_vs_moneyflow_net_to_amount": stats.5,
        },
        "feature_scope": "raw event-day linear screen only; P3.10 RankIC/group/decay/turnover-capacity is still mandatory",
        "pit_alignment": "raw events are joined on conservative available_at, not publication_date"
    }))
}



async fn build_akshare_analyst_revision_coverage_audit(
    state: &AppState,
    req: AkshareAnalystRevisionCoverageAuditReq,
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

    let raw_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.market_vendor_analyst_revision_raw')::text")
            .fetch_one(&state.db)
            .await
            .map_err(|error| {
                format!("Failed to inspect AkShare analyst revision raw table: {error}")
            })?;

    let table_exists = raw_table.is_some();
    let mut row_count = 0i64;
    let mut distinct_publication_date_count = 0i64;
    let mut distinct_raw_symbol_count = 0i64;
    let mut distinct_normalized_symbol_count = 0i64;
    let mut min_publication_date: Option<NaiveDate> = None;
    let mut max_publication_date: Option<NaiveDate> = None;
    let mut pit_violation_rows = 0i64;
    let mut missing_source_published_at_rows = 0i64;
    let mut missing_current_rating_rows = 0i64;
    let mut missing_revision_semantics_rows = 0i64;
    let mut duplicate_key_rows = 0i64;
    let mut duplicate_payload_hash_groups = 0i64;
    let mut duplicate_payload_hash_rows = 0i64;
    let mut completed_attempt_dates = 0i64;
    let mut empty_completed_attempt_dates = 0i64;
    let mut failed_attempt_dates = 0i64;
    let mut attempt_row_count = 0i64;
    let mut reference_symbols = 0i64;
    let mut year_breakdown = Vec::new();
    let mut market_breakdown = Vec::new();
    let mut missing_year_count = 0i64;

    if table_exists {
        let summary =
            sqlx::query_as::<_, (i64, i64, i64, i64, Option<NaiveDate>, Option<NaiveDate>)>(
                r#"
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(DISTINCT publication_date)::bigint AS distinct_publication_date_count,
                   COUNT(DISTINCT symbol)::bigint AS distinct_raw_symbol_count,
                   COUNT(DISTINCT CASE
                       WHEN symbol ~ '^[0-9]{6}\.' THEN symbol
                       WHEN LEFT(symbol, 1) IN ('6', '9') THEN symbol || '.SH'
                       WHEN LEFT(symbol, 1) IN ('0', '2', '3') THEN symbol || '.SZ'
                       WHEN LEFT(symbol, 1) IN ('4', '8') THEN symbol || '.BJ'
                       ELSE symbol
                   END)::bigint AS distinct_normalized_symbol_count,
                   MIN(publication_date) AS min_publication_date,
                   MAX(publication_date) AS max_publication_date
            FROM market_vendor_analyst_revision_raw
            WHERE publication_date >= $1 AND publication_date <= $2
            "#,
            )
            .bind(start)
            .bind(end)
            .fetch_one(&state.db)
            .await
            .map_err(|error| {
                format!("Failed to summarize AkShare analyst revision coverage: {error}")
            })?;
        row_count = summary.0;
        distinct_publication_date_count = summary.1;
        distinct_raw_symbol_count = summary.2;
        distinct_normalized_symbol_count = summary.3;
        min_publication_date = summary.4;
        max_publication_date = summary.5;

        let quality = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
            SELECT COUNT(*) FILTER (
                       WHERE available_at < publication_date
                          OR publication_date IS NULL
                          OR source_published_at::date < publication_date
                   )::bigint AS pit_violation_rows,
                   COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows,
                   COUNT(*) FILTER (WHERE rating_current IS NULL)::bigint AS missing_current_rating_rows,
                   COUNT(*) FILTER (
                       WHERE rating_previous IS NULL
                          OR rating_change IS NULL
                   )::bigint AS missing_revision_semantics_rows
            FROM market_vendor_analyst_revision_raw
            WHERE publication_date >= $1 AND publication_date <= $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize AkShare analyst revision PIT quality: {error}")
        })?;
        pit_violation_rows = quality.0;
        missing_source_published_at_rows = quality.1;
        missing_current_rating_rows = quality.2;
        missing_revision_semantics_rows = quality.3;

        duplicate_key_rows = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(SUM(cnt - 1), 0)::bigint
            FROM (
                SELECT vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash,
                       COUNT(*)::bigint AS cnt
                FROM market_vendor_analyst_revision_raw
                WHERE publication_date >= $1 AND publication_date <= $2
                GROUP BY vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash
                HAVING COUNT(*) > 1
            ) d
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize AkShare analyst revision duplicate keys: {error}")
        })?;

        let duplicate_hash = sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT COUNT(*)::bigint AS duplicate_payload_hash_groups,
                   COALESCE(SUM(cnt - 1), 0)::bigint AS duplicate_payload_hash_rows
            FROM (
                SELECT raw_payload_hash, COUNT(*)::bigint AS cnt
                FROM market_vendor_analyst_revision_raw
                WHERE publication_date >= $1 AND publication_date <= $2
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
            format!("Failed to summarize AkShare analyst revision duplicate hashes: {error}")
        })?;
        duplicate_payload_hash_groups = duplicate_hash.0;
        duplicate_payload_hash_rows = duplicate_hash.1;

        let attempts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
            SELECT COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempt_dates,
                   COUNT(*) FILTER (WHERE status = 'completed' AND row_count = 0)::bigint AS empty_completed_attempt_dates,
                   COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempt_dates,
                   COALESCE(SUM(row_count) FILTER (WHERE status = 'completed'), 0)::bigint AS attempt_row_count
            FROM data_sync_attempt
            WHERE source = $1
              AND start_date >= $2
              AND end_date <= $3
            "#,
        )
        .bind(AKSHARE_ANALYST_REVISION_ATTEMPT_SOURCE)
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to summarize AkShare analyst revision sync attempts: {error}")
        })?;
        completed_attempt_dates = attempts.0;
        empty_completed_attempt_dates = attempts.1;
        failed_attempt_dates = attempts.2;
        attempt_row_count = attempts.3;

        reference_symbols = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::bigint
            FROM market_stock
            WHERE symbol ~ '^[0-9]{6}\.(SH|SZ|BJ)$'
              AND list_date IS NOT NULL
              AND list_date <= $2
              AND (delist_date IS NULL OR delist_date >= $1)
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);

        let year_rows: Vec<(
            i32,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
        )> = sqlx::query_as(
            r#"
            SELECT EXTRACT(YEAR FROM publication_date)::int AS year,
                   COUNT(*)::bigint AS rows,
                   COUNT(DISTINCT publication_date)::bigint AS publication_dates,
                   COUNT(DISTINCT symbol)::bigint AS raw_symbols,
                   COUNT(DISTINCT CASE
                       WHEN symbol ~ '^[0-9]{6}\.' THEN symbol
                       WHEN LEFT(symbol, 1) IN ('6', '9') THEN symbol || '.SH'
                       WHEN LEFT(symbol, 1) IN ('0', '2', '3') THEN symbol || '.SZ'
                       WHEN LEFT(symbol, 1) IN ('4', '8') THEN symbol || '.BJ'
                       ELSE symbol
                   END)::bigint AS normalized_symbols,
                   COUNT(*) FILTER (WHERE rating_current IS NULL)::bigint AS missing_current_rating_rows,
                   COUNT(*) FILTER (WHERE rating_previous IS NULL OR rating_change IS NULL)::bigint AS missing_revision_semantics_rows,
                   MIN(publication_date) AS min_publication_date,
                   MAX(publication_date) AS max_publication_date
            FROM market_vendor_analyst_revision_raw
            WHERE publication_date >= $1 AND publication_date <= $2
            GROUP BY 1
            ORDER BY 1
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to build AkShare analyst revision year breakdown: {error}")
        })?;
        let mut rows_by_year: BTreeMap<
            i32,
            (
                i64,
                i64,
                i64,
                i64,
                i64,
                i64,
                Option<NaiveDate>,
                Option<NaiveDate>,
            ),
        > = BTreeMap::new();
        for (
            year,
            rows,
            publication_dates,
            raw_symbols,
            normalized_symbols,
            missing_current,
            missing_revision,
            min_date,
            max_date,
        ) in year_rows
        {
            rows_by_year.insert(
                year,
                (
                    rows,
                    publication_dates,
                    raw_symbols,
                    normalized_symbols,
                    missing_current,
                    missing_revision,
                    min_date,
                    max_date,
                ),
            );
        }

        let year_attempts: Vec<(i32, i64, i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT EXTRACT(YEAR FROM start_date)::int AS year,
                   COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempt_dates,
                   COUNT(*) FILTER (WHERE status = 'completed' AND row_count = 0)::bigint AS empty_completed_attempt_dates,
                   COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempt_dates,
                   COALESCE(SUM(row_count) FILTER (WHERE status = 'completed'), 0)::bigint AS attempt_row_count
            FROM data_sync_attempt
            WHERE source = $1
              AND start_date >= $2
              AND end_date <= $3
            GROUP BY 1
            ORDER BY 1
            "#,
        )
        .bind(AKSHARE_ANALYST_REVISION_ATTEMPT_SOURCE)
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to build AkShare analyst revision year attempt breakdown: {error}")
        })?;
        let mut attempts_by_year: BTreeMap<i32, (i64, i64, i64, i64)> = BTreeMap::new();
        for (year, completed, empty_completed, failed, attempt_rows) in year_attempts {
            attempts_by_year.insert(year, (completed, empty_completed, failed, attempt_rows));
        }

        let reference_by_year_rows: Vec<(i32, i64)> = sqlx::query_as(
            r#"
            WITH years AS (
                SELECT generate_series($1::int, $2::int) AS year
            )
            SELECT years.year,
                   COUNT(stock.symbol)::bigint AS reference_symbols
            FROM years
            LEFT JOIN market_stock stock
              ON stock.symbol ~ '^[0-9]{6}\.(SH|SZ|BJ)$'
             AND stock.list_date IS NOT NULL
             AND stock.list_date <= make_date(years.year, 12, 31)
             AND (stock.delist_date IS NULL OR stock.delist_date >= make_date(years.year, 1, 1))
            GROUP BY years.year
            ORDER BY years.year
            "#,
        )
        .bind(start.year())
        .bind(end.year())
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
        let reference_by_year: BTreeMap<i32, i64> = reference_by_year_rows.into_iter().collect();

        for year in start.year()..=end.year() {
            let year_start = NaiveDate::from_ymd_opt(year, 1, 1).expect("valid year start");
            let year_end = NaiveDate::from_ymd_opt(year, 12, 31).expect("valid year end");
            let effective_year_start = std::cmp::max(year_start, start);
            let effective_year_end = std::cmp::min(year_end, end);
            let calendar_day_count = (effective_year_end - effective_year_start).num_days() + 1;
            let (
                rows,
                publication_dates,
                raw_symbols,
                normalized_symbols,
                missing_current,
                missing_revision,
                min_date,
                max_date,
            ) = rows_by_year
                .remove(&year)
                .unwrap_or((0, 0, 0, 0, 0, 0, None, None));
            let (completed, empty_completed, failed, attempt_rows) =
                attempts_by_year.remove(&year).unwrap_or((0, 0, 0, 0));
            let audited_calendar_date_count = publication_dates + empty_completed;
            let year_coverage_ratio = if calendar_day_count > 0 {
                audited_calendar_date_count as f64 / calendar_day_count as f64
            } else {
                0.0
            };
            if year_coverage_ratio + f64::EPSILON < 1.0 {
                missing_year_count += 1;
            }
            let year_reference_symbols = reference_by_year.get(&year).copied().unwrap_or(0);
            year_breakdown.push(json!({
                "year": year,
                "rows": rows,
                "publication_dates": publication_dates,
                "calendar_day_count": calendar_day_count,
                "audited_calendar_date_count": audited_calendar_date_count,
                "coverage_ratio": year_coverage_ratio,
                "raw_symbols": raw_symbols,
                "normalized_symbols": normalized_symbols,
                "reference_symbols": year_reference_symbols,
                "symbol_coverage_ratio": phase7_ratio(normalized_symbols, year_reference_symbols).unwrap_or(0.0),
                "missing_current_rating_rows": missing_current,
                "missing_revision_semantics_rows": missing_revision,
                "completed_attempt_dates": completed,
                "empty_completed_attempt_dates": empty_completed,
                "failed_attempt_dates": failed,
                "attempt_row_count": attempt_rows,
                "min_publication_date": phase7_date_json(min_date),
                "max_publication_date": phase7_date_json(max_date),
            }));
        }

        let market_rows: Vec<(String, i64, i64, i64, i64, i64)> = sqlx::query_as(
            r#"
            WITH normalized AS (
                SELECT CASE
                           WHEN symbol ~ '^[0-9]{6}\.' THEN symbol
                           WHEN LEFT(symbol, 1) IN ('6', '9') THEN symbol || '.SH'
                           WHEN LEFT(symbol, 1) IN ('0', '2', '3') THEN symbol || '.SZ'
                           WHEN LEFT(symbol, 1) IN ('4', '8') THEN symbol || '.BJ'
                           ELSE symbol
                       END AS normalized_symbol,
                       publication_date,
                       rating_current,
                       rating_previous,
                       rating_change
                FROM market_vendor_analyst_revision_raw
                WHERE publication_date >= $1 AND publication_date <= $2
            )
            SELECT CASE
                       WHEN normalized_symbol LIKE '%.SH' THEN 'SH'
                       WHEN normalized_symbol LIKE '%.SZ' THEN 'SZ'
                       WHEN normalized_symbol LIKE '%.BJ' THEN 'BJ'
                       ELSE 'OTHER'
                   END AS market,
                   COUNT(*)::bigint AS rows,
                   COUNT(DISTINCT normalized_symbol)::bigint AS symbols,
                   COUNT(DISTINCT publication_date)::bigint AS publication_dates,
                   COUNT(*) FILTER (WHERE rating_current IS NULL)::bigint AS missing_current_rating_rows,
                   COUNT(*) FILTER (WHERE rating_previous IS NULL OR rating_change IS NULL)::bigint AS missing_revision_semantics_rows
            FROM normalized
            GROUP BY 1
            ORDER BY 1
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to build AkShare analyst revision market breakdown: {error}")
        })?;
        market_breakdown = market_rows
            .into_iter()
            .map(
                |(market, rows, symbols, publication_dates, missing_current, missing_revision)| {
                    json!({
                        "market": market,
                        "rows": rows,
                        "symbols": symbols,
                        "publication_dates": publication_dates,
                        "missing_current_rating_rows": missing_current,
                        "missing_revision_semantics_rows": missing_revision,
                    })
                },
            )
            .collect();
    }
    let requested_calendar_days = (end - start).num_days() + 1;
    let audited_calendar_date_count =
        distinct_publication_date_count + empty_completed_attempt_dates;
    let coverage_ratio = if requested_calendar_days > 0 {
        audited_calendar_date_count as f64 / requested_calendar_days as f64
    } else {
        0.0
    };
    let correlation_audit =
        build_akshare_analyst_revision_correlation_audit(&state.db, start, end, table_exists)
            .await?;
    let correlation_decision = correlation_audit
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_correlation_sample_available");
    let decision = decide_akshare_analyst_revision_coverage_audit(
        table_exists,
        row_count,
        coverage_ratio,
        failed_attempt_dates,
        missing_year_count,
        pit_violation_rows,
        missing_source_published_at_rows,
        missing_revision_semantics_rows,
        duplicate_key_rows,
        duplicate_payload_hash_rows,
        correlation_decision,
    );
    let status = decision["status"].clone();
    let admission_decision = decision["admission_decision"].clone();
    let p310_status = decision["p310_status"].clone();
    let bounded_wfa = decision["bounded_wfa"].clone();
    let v19_train_selection = decision["v19_train_selection"].clone();

    Ok(json!({
        "audit_version": "p3.23e-akshare-analyst-revision-full-history-coverage-quality-correlation-audit-v1",
        "source_id": "multi_vendor_analyst_revision",
        "mode": "read_only_year_market_symbol_pit_quality_duplicate_hash_correlation_gate",
        "table": "market_vendor_analyst_revision_raw",
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
            "calendar_day_count": requested_calendar_days,
        },
        "table_exists": table_exists,
        "raw_coverage": {
            "row_count": row_count,
            "distinct_publication_date_count": distinct_publication_date_count,
            "distinct_raw_symbol_count": distinct_raw_symbol_count,
            "distinct_normalized_symbol_count": distinct_normalized_symbol_count,
            "min_publication_date": phase7_date_json(min_publication_date),
            "max_publication_date": phase7_date_json(max_publication_date),
        },
        "raw_quality": {
            "pit_violation_rows": pit_violation_rows,
            "missing_source_published_at_rows": missing_source_published_at_rows,
            "missing_current_rating_rows": missing_current_rating_rows,
            "missing_revision_semantics_rows": missing_revision_semantics_rows,
            "current_rating_usage": if missing_current_rating_rows > 0 {
                "exclude_or_downweight_rows_before_current_rating_factor_use"
            } else {
                "fully_populated"
            },
            "revision_semantics_required_fields": ["rating_previous", "rating_change"]
        },
        "duplicate_hash_audit": {
            "duplicate_key_rows": duplicate_key_rows,
            "duplicate_payload_hash_groups": duplicate_payload_hash_groups,
            "duplicate_payload_hash_rows": duplicate_payload_hash_rows,
        },
        "sync_attempts": {
            "completed_attempt_dates": completed_attempt_dates,
            "empty_completed_attempt_dates": empty_completed_attempt_dates,
            "failed_attempt_dates": failed_attempt_dates,
            "attempt_row_count": attempt_row_count,
            "audited_calendar_date_count": audited_calendar_date_count,
            "coverage_ratio": coverage_ratio,
        },
        "symbol_breadth_vs_listed_stock_universe": {
            "reference_symbols": reference_symbols,
            "covered_normalized_symbols": distinct_normalized_symbol_count,
            "coverage_ratio": phase7_ratio(distinct_normalized_symbol_count, reference_symbols).unwrap_or(0.0),
            "note": "analyst revision is event-sparse; this is breadth evidence, not a daily panel completeness claim"
        },
        "year_breakdown": year_breakdown,
        "market_breakdown": market_breakdown,
        "correlation_audit": correlation_audit,
        "decision": decision,
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": p310_status,
        "bounded_wfa": bounded_wfa,
        "v19_train_selection": v19_train_selection,
        "required_before_p310": [
            "market_vendor_analyst_revision_raw schema reviewed and applied",
            "bounded full-history sync by history date or safe batches",
            "year/vendor_endpoint/symbol breadth coverage",
            "publication_date/source_published_at/available_at PIT audit",
            "revision semantics coverage for rating_change and previous_rating",
            "cross-vendor duplicate/raw-payload hash audit"
        ],
        "notes": [
            "Coverage audit is intentionally blocking until raw schema and bounded sync exist.",
            "Do not interpret a successful permission smoke as coverage readiness or alpha admission."
        ]
    }))
}



async fn build_broad_analyst_revision_audit(
    state: &AppState,
    req: BroadAnalystRevisionAuditReq,
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
    let breakdown_limit = broad_analyst_revision_breakdown_limit(req.limit);

    let reference_symbols: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM market_stock")
        .fetch_one(&state.db)
        .await
        .map_err(|error| {
            format!("Failed to load broad_analyst_revision reference symbols: {error}")
        })?;

    let source_rows = sqlx::query_as::<
        _,
        (String, i64, i64, Option<NaiveDate>, Option<NaiveDate>, i64),
    >(
        r#"
        SELECT 'forecast'::text AS source,
               COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(available_at) AS min_available_at,
               MAX(available_at) AS max_available_at,
               COUNT(*) FILTER (WHERE available_at <> ann_date)::bigint
                   AS available_at_rule_violations
        FROM market_stock_forecast
        WHERE available_at BETWEEN $1 AND $2
        UNION ALL
        SELECT 'express'::text AS source,
               COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(available_at) AS min_available_at,
               MAX(available_at) AS max_available_at,
               COUNT(*) FILTER (WHERE available_at <> ann_date)::bigint
                   AS available_at_rule_violations
        FROM market_stock_express
        WHERE available_at BETWEEN $1 AND $2
        UNION ALL
        SELECT 'disclosure_date'::text AS source,
               COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(available_at) AS min_available_at,
               MAX(available_at) AS max_available_at,
               COUNT(*) FILTER (
                   WHERE available_at <> GREATEST(
                       ann_date,
                       COALESCE(actual_date, ann_date),
                       COALESCE(modify_date, ann_date)
                   )
               )::bigint AS available_at_rule_violations
        FROM market_stock_disclosure_date
        WHERE available_at BETWEEN $1 AND $2
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to audit broad_analyst_revision source coverage: {error}"))?;

    let union_symbols: i64 = sqlx::query_scalar(
        r#"
        WITH source_symbols AS (
            SELECT symbol FROM market_stock_forecast WHERE available_at BETWEEN $1 AND $2
            UNION
            SELECT symbol FROM market_stock_express WHERE available_at BETWEEN $1 AND $2
            UNION
            SELECT symbol FROM market_stock_disclosure_date WHERE available_at BETWEEN $1 AND $2
        )
        SELECT COUNT(DISTINCT symbol)::bigint
        FROM source_symbols
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to audit broad_analyst_revision union coverage: {error}"))?;

    let revision_stats = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
        r#"
        WITH forecast_periods AS (
            SELECT symbol,
                   end_date,
                   COUNT(*)::bigint AS forecast_rows
            FROM market_stock_forecast
            WHERE available_at BETWEEN $1 AND $2
            GROUP BY symbol, end_date
        ),
        ordered AS (
            SELECT symbol,
                   end_date,
                   available_at,
                   forecast_type,
                   p_change_min,
                   p_change_max,
                   net_profit_min,
                   net_profit_max,
                   LAG(available_at) OVER forecast_window AS previous_available_at,
                   LAG(forecast_type) OVER forecast_window AS previous_forecast_type,
                   LAG(p_change_min) OVER forecast_window AS previous_p_change_min,
                   LAG(p_change_max) OVER forecast_window AS previous_p_change_max,
                   LAG(net_profit_min) OVER forecast_window AS previous_net_profit_min,
                   LAG(net_profit_max) OVER forecast_window AS previous_net_profit_max
            FROM market_stock_forecast
            WHERE available_at BETWEEN $1 AND $2
            WINDOW forecast_window AS (
                PARTITION BY symbol, end_date
                ORDER BY available_at, created_at
            )
        ),
        revision_events AS (
            SELECT *
            FROM ordered
            WHERE previous_available_at IS NOT NULL
              AND (
                  forecast_type IS DISTINCT FROM previous_forecast_type
                  OR p_change_min IS DISTINCT FROM previous_p_change_min
                  OR p_change_max IS DISTINCT FROM previous_p_change_max
                  OR net_profit_min IS DISTINCT FROM previous_net_profit_min
                  OR net_profit_max IS DISTINCT FROM previous_net_profit_max
              )
        ),
        period_stats AS (
            SELECT COUNT(*)::bigint AS symbol_periods,
                   COUNT(*) FILTER (WHERE forecast_rows >= 2)::bigint
                       AS multi_announcement_symbol_periods,
                   COUNT(DISTINCT symbol)::bigint AS forecast_symbols,
                   COUNT(DISTINCT CASE WHEN forecast_rows >= 2 THEN symbol END)::bigint
                       AS multi_announcement_symbols
            FROM forecast_periods
        ),
        event_stats AS (
            SELECT COUNT(*)::bigint AS revision_event_rows,
                   COUNT(DISTINCT symbol)::bigint AS revision_event_symbols
            FROM revision_events
        )
        SELECT period_stats.symbol_periods,
               period_stats.multi_announcement_symbol_periods,
               period_stats.forecast_symbols,
               period_stats.multi_announcement_symbols,
               event_stats.revision_event_rows,
               event_stats.revision_event_symbols
        FROM period_stats, event_stats
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        format!("Failed to audit broad_analyst_revision revision semantics: {error}")
    })?;

    let yearly_revision_rows = sqlx::query_as::<_, (i32, i64, i64, i64, i64)>(
        r#"
        WITH forecast_periods AS (
            SELECT symbol,
                   end_date,
                   COUNT(*)::bigint AS forecast_rows,
                   MAX(available_at) AS latest_available_at
            FROM market_stock_forecast
            WHERE available_at BETWEEN $1 AND $2
            GROUP BY symbol, end_date
        )
        SELECT EXTRACT(YEAR FROM latest_available_at)::int AS audit_year,
               COUNT(*)::bigint AS symbol_periods,
               COUNT(*) FILTER (WHERE forecast_rows >= 2)::bigint
                   AS multi_announcement_symbol_periods,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               COUNT(DISTINCT CASE WHEN forecast_rows >= 2 THEN symbol END)::bigint
                   AS multi_announcement_symbols
        FROM forecast_periods
        GROUP BY audit_year
        ORDER BY audit_year DESC
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(breakdown_limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to audit broad_analyst_revision yearly breakdown: {error}"))?;

    let disclosure_stats = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        r#"
        SELECT COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               COUNT(*) FILTER (WHERE modify_date IS NOT NULL)::bigint AS modify_rows,
               COUNT(*) FILTER (WHERE actual_date IS NOT NULL)::bigint AS actual_rows,
               COUNT(*) FILTER (
                   WHERE pre_date IS NOT NULL
                     AND actual_date IS NOT NULL
                     AND pre_date IS DISTINCT FROM actual_date
               )::bigint AS pre_actual_changed_rows
        FROM market_stock_disclosure_date
        WHERE available_at BETWEEN $1 AND $2
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        format!("Failed to audit broad_analyst_revision disclosure semantics: {error}")
    })?;

    let available_at_rule_violations: i64 = source_rows.iter().map(|row| row.5).sum();
    let union_symbol_coverage_ratio = safe_ratio(union_symbols, reference_symbols).unwrap_or(0.0);
    let forecast_symbol_coverage_ratio =
        safe_ratio(revision_stats.2, reference_symbols).unwrap_or(0.0);
    let revised_symbol_coverage_ratio =
        safe_ratio(revision_stats.5, reference_symbols).unwrap_or(0.0);
    let revised_symbol_period_ratio = safe_ratio(revision_stats.1, revision_stats.0).unwrap_or(0.0);

    let decision = decide_broad_analyst_revision_audit(
        available_at_rule_violations,
        union_symbol_coverage_ratio,
        forecast_symbol_coverage_ratio,
        revised_symbol_coverage_ratio,
        revised_symbol_period_ratio,
    );

    let mut blocking_reasons = Vec::new();
    if available_at_rule_violations > 0 {
        blocking_reasons
            .push("available_at policy violations must be repaired before any research use");
    }
    if union_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE {
        blocking_reasons.push("forecast/express/disclosure union coverage is not broad enough");
    }
    if forecast_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE {
        blocking_reasons.push("forecast source coverage is too narrow for broad-base revision");
    }
    if revised_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE {
        blocking_reasons.push("symbols with true forecast revisions are too sparse");
    }
    if revised_symbol_period_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO {
        blocking_reasons.push("symbol-periods with repeated forecast revisions are too sparse");
    }

    let source_coverage: Vec<Value> = source_rows
        .iter()
        .map(
            |(source, rows, symbols, min_available_at, max_available_at, violations)| {
                json!({
                    "source": source,
                    "rows": rows,
                    "symbols": symbols,
                    "reference_symbols": reference_symbols,
                    "symbol_coverage_ratio": safe_ratio(*symbols, reference_symbols),
                    "coverage_grade": phase7_coverage_grade(*symbols, reference_symbols),
                    "min_available_at": min_available_at,
                    "max_available_at": max_available_at,
                    "available_at_rule_violations": violations,
                })
            },
        )
        .collect();

    let yearly_revision_breakdown: Vec<Value> = yearly_revision_rows
        .iter()
        .map(
            |(
                audit_year,
                symbol_periods,
                multi_announcement_symbol_periods,
                symbols,
                multi_announcement_symbols,
            )| {
                json!({
                    "year": audit_year,
                    "symbol_periods": symbol_periods,
                    "multi_announcement_symbol_periods": multi_announcement_symbol_periods,
                    "multi_announcement_symbol_period_ratio": safe_ratio(*multi_announcement_symbol_periods, *symbol_periods),
                    "symbols": symbols,
                    "multi_announcement_symbols": multi_announcement_symbols,
                })
            },
        )
        .collect();

    Ok(json!({
        "audit_version": "p3.19g-broad-analyst-revision-full-history-audit-v1",
        "source_id": "broad_analyst_revision",
        "source_family": "broad_base_analyst_expectation_revision",
        "mode": "read_only_full_history_coverage_available_at_revision_semantics_audit",
        "passed": decision.passed,
        "status": decision.status,
        "readiness": decision.readiness,
        "admission_decision": decision.admission_decision,
        "p310_status": decision.p310_status,
        "blocked_reason": if decision.blocked_reason.is_empty() { Value::Null } else { json!(decision.blocked_reason) },
        "admission_gate": if decision.passed {
            "p310_diagnostics_allowed_next_factor_wfa_v19_blocked"
        } else if decision.status == "blocked_available_at_rule_violation" {
            "repair_data_before_any_factor_p310_wfa"
        } else {
            "stop_current_raw_bundle_search_replacement_source"
        },
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "thresholds": {
            "min_union_symbol_coverage_ratio": BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE,
            "min_forecast_symbol_coverage_ratio": BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE,
            "min_revised_symbol_coverage_ratio": BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE,
            "min_revised_symbol_period_ratio": BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO,
        },
        "coverage": {
            "reference_symbols": reference_symbols,
            "union_symbols": union_symbols,
            "union_symbol_coverage_ratio": union_symbol_coverage_ratio,
            "source_coverage": source_coverage,
        },
        "available_at_policy": {
            "forecast": "available_at must equal ann_date; available_at may be before end_date because forecasts can be published before fiscal period end",
            "express": "available_at must equal ann_date",
            "disclosure_date": "available_at must be max(ann_date, actual_date, modify_date); pre_date is not true availability",
            "downstream_rule": "any feature or diagnostic must filter source.available_at <= trade_date",
            "available_at_rule_violations": available_at_rule_violations,
        },
        "revision_semantics": {
            "forecast_symbol_periods": revision_stats.0,
            "multi_announcement_symbol_periods": revision_stats.1,
            "multi_announcement_symbol_period_ratio": revised_symbol_period_ratio,
            "forecast_symbols": revision_stats.2,
            "multi_announcement_symbols": revision_stats.3,
            "revision_event_rows": revision_stats.4,
            "revision_event_symbols": revision_stats.5,
            "forecast_symbol_coverage_ratio": forecast_symbol_coverage_ratio,
            "revision_event_symbol_coverage_ratio": revised_symbol_coverage_ratio,
            "verdict": if decision.status == "blocked_sparse_revision_semantics" {
                "too_sparse_would_degenerate_into_event_overlay"
            } else if decision.passed {
                "broad_enough_for_p310_diagnostics"
            } else {
                "blocked_before_semantics_admission"
            }
        },
        "disclosure_calendar_semantics": {
            "rows": disclosure_stats.0,
            "symbols": disclosure_stats.1,
            "modify_rows": disclosure_stats.2,
            "actual_rows": disclosure_stats.3,
            "pre_actual_changed_rows": disclosure_stats.4,
            "verdict": "disclosure_date can support availability and calendar context but is not itself a broad analyst expectation revision signal"
        },
        "yearly_revision_breakdown": yearly_revision_breakdown,
        "breakdown_limit": breakdown_limit,
        "blocking_reasons": blocking_reasons,
        "repairability": {
            "data_rule_violations_repairable": available_at_rule_violations > 0,
            "sparse_revision_semantics_repairable_by_more_sync": false,
            "recommended_action": if decision.status == "blocked_available_at_rule_violation" {
                "repair_available_at_mapping_then_rerun_audit"
            } else if decision.passed {
                "run_p310_rankic_group_decay_turnover_capacity_diagnostics"
            } else {
                "do_not_expand_same_tables_search_lower_correlation_broader_revision_or_order_price_chain_source"
            }
        },
        "notes": [
            "This endpoint is read-only and does not write factors, run P3.10 diagnostics, run WFA, or register v19 train selection.",
            "The audit distinguishes PIT data defects from source-economics defects. Sparse revision semantics are not repaired by sign flip, OOS reverse tuning, or same-table parameter expansion.",
            "A passed audit only allows P3.10 diagnostics; bounded WFA remains blocked until RankIC, group return, decay, turnover/capacity and regime diagnostics pass."
        ]
    }))
}



