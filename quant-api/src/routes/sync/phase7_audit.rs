/// 数据同步路由
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hasher,
    sync::Arc,
};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

use crate::phase7_alpha_admission::SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID;
use crate::AppState;

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub struct FuturesPriceChainSyncReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub exchanges: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
}

impl FuturesPriceChainSyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: "futures_price_chain".to_string(),
            source: "tushare:futures_price_chain".to_string(),
            mode: Some("bounded_raw_sync".to_string()),
            symbols: self.symbols,
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: self.exchanges,
            start_date: self.start_date,
            end_date: self.end_date,
            data_version_id: self.data_version_id,
            background: self.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("p3.19 futures price-chain bounded raw sync".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]


pub struct EquityPledgePressureSyncReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
}

impl EquityPledgePressureSyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: "equity_pledge_pressure".to_string(),
            source: "tushare:equity_pledge_pressure".to_string(),
            mode: Some("bounded_raw_sync".to_string()),
            symbols: self.symbols,
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
            reason: Some("p3.20 equity pledge pressure bounded raw sync".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]


pub struct ShareholderStructureSyncReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub source_filters: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
}

impl ShareholderStructureSyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: "shareholder_structure".to_string(),
            source: "tushare:shareholder_structure".to_string(),
            mode: Some("bounded_raw_sync".to_string()),
            symbols: self.symbols,
            source_filters: self.source_filters,
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: self.start_date,
            end_date: self.end_date,
            data_version_id: self.data_version_id,
            background: self.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("p3.21 shareholder structure bounded raw sync".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]


pub struct MarginDetailSyncReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
}

impl MarginDetailSyncReq {
    pub(crate) fn into_sync_task_req(self) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: "margin_detail".to_string(),
            source: "tushare:margin_detail".to_string(),
            mode: Some("bounded_raw_sync".to_string()),
            symbols: self.symbols,
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
            reason: Some("p3.22 margin detail bounded raw sync".to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]


pub struct ShareholderStructureSyncPlanReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct MarginDetailSyncPlanReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch: Option<String>,
}

#[derive(Debug, Clone)]


struct MarginDetailSyncPlanBatch {
    label: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    open_day_count: i64,
    observed_avg_rows_per_day: Option<f64>,
}

#[derive(Debug, Clone)]


pub(crate) struct ShareholderStructureSyncPlanBatch {
    pub(crate) year:i32,
    pub(crate) start_date:NaiveDate,
    pub(crate) end_date:NaiveDate,
    pub(crate) symbol_count:i64,
    pub(crate) quarter_count:i64,
}

#[derive(Debug, Clone, Deserialize)]


pub struct FuturesPriceChainMappingValidateReq {
    #[serde(default)]
    pub rows: Vec<FuturesPriceChainMappingCandidate>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct FuturesPriceChainCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct EquityPledgeCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct ShareholderStructureCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct MarginDetailCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct FuturesPriceChainMappingCandidate {
    pub product_symbol: String,
    pub exposure_type: String,
    pub exposure_code: String,
    pub direction: i16,
    pub weight: f64,
    pub valid_from: String,
    #[serde(default)]
    pub valid_to: Option<String>,
    pub available_at: String,
    pub source: String,
    pub mapping_version: String,
    #[serde(default)]
    pub evidence: Value,
}

#[derive(Debug, Clone, Serialize)]


pub(crate) struct FuturesPriceChainMappingCandidateValidation {
    pub(crate) product_symbol:String,
    pub(crate) exposure_type:String,
    pub(crate) exposure_code:String,
    pub(crate) passed:bool,
    pub(crate) errors:Vec<String>,
    pub(crate) warnings:Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct MainBusinessAvailableAtAuditReq {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct MainBusinessReadinessAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub business_type: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]


pub(crate) struct MainBusinessPeriodMapping {
    pub(crate) ts_code:String,
    pub(crate) end_date:NaiveDate,
    pub(crate) available_at:Option<NaiveDate>,
    pub(crate) source:Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]


pub(crate) struct MainBusinessAvailableAtJoinDecision {
    pub(crate) passed:bool,
    pub(crate) status:&'static str,
    pub(crate) readiness:&'static str,
    pub(crate) missing_mapping_count:usize,
    pub(crate) pit_violation_count:usize,
    pub(crate) source_counts:BTreeMap<String, usize>,
}



fn main_business_available_at_audit_period_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(MAIN_BUSINESS_AVAILABLE_AT_AUDIT_MAX_PERIODS)
        .clamp(1, MAIN_BUSINESS_AVAILABLE_AT_AUDIT_MAX_PERIODS)
}



fn main_business_readiness_breakdown_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(24)
        .clamp(1, MAIN_BUSINESS_READINESS_BREAKDOWN_MAX_PERIODS)
}



fn main_business_business_type(value: Option<&str>) -> &'static str {
    match value.map(str::trim).map(str::to_ascii_uppercase).as_deref() {
        Some("D") => "D",
        Some("I") => "I",
        _ => "P",
    }
}



fn main_business_quarter_end_dates_in_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    if start > end {
        return Vec::new();
    }

    let mut periods = Vec::new();
    for year in start.year()..=end.year() {
        for (month, day) in [(3, 31), (6, 30), (9, 30), (12, 31)] {
            if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
                if date >= start && date <= end {
                    periods.push(date);
                }
            }
        }
    }
    periods
}



pub(crate) fn main_business_attempt_metric(error_message: Option<&str>, prefix: &str) -> i64 {
    let Some(message) = error_message else {
        return 0;
    };
    message
        .split(',')
        .find_map(|part| {
            let part = part.trim();
            part.strip_prefix(prefix)?.parse::<i64>().ok()
        })
        .unwrap_or(0)
}



pub(crate) fn phase7_share_float_next_chunk_start(start: NaiveDate, granularity: &str) -> NaiveDate {
    let months = match granularity {
        "month" => 1,
        "quarter" => 3,
        _ => 12,
    };
    add_months_clamped(
        NaiveDate::from_ymd_opt(start.year(), start.month(), 1).expect("valid first day"),
        months,
    )
}



pub(crate) fn phase7_attempt_coverage_readiness(
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



pub(crate) fn phase7_optional_source_next_step(readiness: &str) -> &'static str {
    match readiness {
        "ready_for_feature_factory" => "build_pit_feature_factory",
        "partial_feature_candidate" => "run_feature_smoke_then_expand_coverage",
        "sample_only_do_not_train" => "expand_sync_before_training",
        "needs_sync" => "run_bounded_sync_smoke_then_coverage_audit",
        "schema_missing" => "apply_phase7_optional_financial_sources_schema",
        _ => "repair_reference_coverage_before_feature_factory",
    }
}



pub(crate) fn phase7_market_level_source_readiness(stats: &Phase7MarketLevelSourceAudit) -> &'static str {
    if stats.data_rows <= 0 || stats.latest_trade_date.is_none() {
        return "market_level_needs_sync";
    }
    if stats.open_day_lag.unwrap_or(i64::MAX) > 2 {
        return "market_level_stale_needs_sync";
    }
    "market_level_ready_for_regime_feature"
}



pub(crate) fn phase7_market_level_zero_row_sync_covers_gap(
    last_sync: Option<&Phase7MarketLevelSyncAudit>,
    sync_start: NaiveDate,
    today: NaiveDate,
) -> bool {
    let Some(last_sync) = last_sync else {
        return false;
    };
    last_sync.status == "completed"
        && last_sync.total_count == 0
        && last_sync.success_count == 0
        && last_sync
            .start_date
            .map(|start_date| start_date <= sync_start)
            .unwrap_or(false)
        && last_sync
            .end_date
            .map(|end_date| end_date >= today)
            .unwrap_or(false)
}



fn equity_pledge_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            "market_stock_pledge_stat",
            vec![
                "market_stock_pledge_stat_pkey",
                "market_stock_pledge_stat_available_at_check",
            ],
        ),
        (
            "market_stock_pledge_detail",
            vec![
                "market_stock_pledge_detail_pkey",
                "market_stock_pledge_detail_available_at_check",
            ],
        ),
    ]
}



fn margin_detail_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![(
        "market_stock_margin_detail",
        vec![
            "market_stock_margin_detail_pkey",
            "market_stock_margin_detail_available_at_check",
            "market_stock_margin_detail_core_nonnegative_check",
        ],
    )]
}



fn decide_shareholder_structure_readiness(
    schema_passed: bool,
    raw_rows: i64,
    pit_violation_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_readiness_audit",
        )
    } else if raw_rows <= 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_shareholder_structure_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "review_period_snapshot_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_readiness_audit_required_before_p310",
            "run_year_symbol_ann_date_breadth_duplicate_audit_before_factor_builder",
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
        "pit_violation_rows": pit_violation_rows,
        "next_step": next_step,
    })
}



fn shareholder_structure_quarter_count(start: NaiveDate, end: NaiveDate) -> i64 {
    if start > end {
        return 0;
    }
    let quarter_start_month = ((start.month0() / 3) * 3) + 1;
    let mut cursor =
        NaiveDate::from_ymd_opt(start.year(), quarter_start_month, 1).expect("valid quarter start");
    let mut count = 0_i64;
    while cursor <= end {
        count += 1;
        let (next_year, next_month) = if cursor.month() >= 10 {
            (cursor.year() + 1, 1)
        } else {
            (cursor.year(), cursor.month() + 3)
        };
        cursor = NaiveDate::from_ymd_opt(next_year, next_month, 1).expect("valid quarter start");
    }
    count
}



async fn build_shareholder_structure_sync_plan(
    db: &sqlx::PgPool,
    req: ShareholderStructureSyncPlanReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?
        .unwrap_or_else(|| Utc::now().date_naive());
    if start > end {
        return Err("shareholder_structure sync-plan start_date cannot be after end_date".into());
    }

    let mut batches = Vec::new();
    for year in start.year()..=end.year() {
        let year_start = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let year_end = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
        let batch_start = start.max(year_start);
        let batch_end = end.min(year_end);
        let symbol_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::bigint
            FROM market_stock
            WHERE symbol ~ '^[036][0-9]{5}\.(SH|SZ)$'
              AND list_date IS NOT NULL
              AND list_date <= $1
              AND (delist_date IS NULL OR delist_date >= $2)
            "#,
        )
        .bind(batch_end)
        .bind(batch_start)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to estimate shareholder symbols for {year}: {error}"))?;

        batches.push(ShareholderStructureSyncPlanBatch {
            year,
            start_date: batch_start,
            end_date: batch_end,
            symbol_count,
            quarter_count: shareholder_structure_quarter_count(batch_start, batch_end),
        });
    }

    Ok(shareholder_structure_sync_plan_response(
        start, end, batches,
    ))
}



pub(crate) async fn build_equity_pledge_pressure_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
    let mut table_results = Vec::new();
    let mut schema_passed = true;
    let mut stat_rows = 0_i64;
    let mut detail_rows = 0_i64;
    let mut pit_violation_rows = 0_i64;

    for (table, required_constraints) in equity_pledge_expected_schema() {
        let regclass_name = format!("public.{table}");
        let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1)::text IS NOT NULL")
            .bind(&regclass_name)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to inspect {table}: {error}"))?;

        let row_count = if table_exists {
            let sql = format!("SELECT COUNT(*)::bigint FROM {table}");
            sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count {table}: {error}"))?
        } else {
            0
        };

        let table_pit_violations = if table_exists {
            let sql = if table == "market_stock_pledge_stat" {
                "SELECT COUNT(*)::bigint FROM market_stock_pledge_stat WHERE available_at < end_date"
            } else {
                "SELECT COUNT(*)::bigint FROM market_stock_pledge_detail WHERE available_at < ann_date"
            };
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count PIT violations for {table}: {error}"))?
        } else {
            0
        };

        let constraints: Vec<String> = if table_exists {
            sqlx::query_scalar(
                r#"
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = to_regclass($1)
                ORDER BY conname
                "#,
            )
            .bind(&regclass_name)
            .fetch_all(db)
            .await
            .map_err(|error| format!("Failed to inspect constraints for {table}: {error}"))?
        } else {
            Vec::new()
        };

        let missing_constraints: Vec<&str> = required_constraints
            .iter()
            .copied()
            .filter(|constraint| !constraints.iter().any(|existing| existing == constraint))
            .collect();

        let passed = table_exists && missing_constraints.is_empty();
        schema_passed &= passed;
        if table == "market_stock_pledge_stat" {
            stat_rows = row_count;
        } else if table == "market_stock_pledge_detail" {
            detail_rows = row_count;
        }
        pit_violation_rows += table_pit_violations;

        table_results.push(json!({
            "table": table,
            "table_exists": table_exists,
            "row_count": row_count,
            "pit_violation_rows": table_pit_violations,
            "required_constraints": required_constraints,
            "missing_constraints": missing_constraints,
            "passed": passed,
        }));
    }

    let attempt_breakdown: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source IN (
            'equity_pledge_stat_symbol',
            'equity_pledge_stat_end_date',
            'equity_pledge_detail_ann_date'
        )
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let decision =
        decide_equity_pledge_readiness(schema_passed, stat_rows, detail_rows, pit_violation_rows);

    Ok(json!({
        "audit_version": "p3.20c-equity-pledge-pressure-readiness-v1",
        "source_id": "equity_pledge_pressure",
        "mode": "read_only_schema_raw_pit_rowcount_audit",
        "schema_passed": schema_passed,
        "tables": table_results,
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "decision": decision,
        "pit_policy": {
            "pledge_detail": "available_at equals ann_date and downstream filters must require available_at <= stock_trade_date",
            "pledge_stat": "available_at is conservatively set to end_date + 1 day until a source_published_at audit proves earlier availability",
            "intraday_rule": "same-day pledge updates are unavailable to intraday trading without source publication timestamps"
        },
        "prohibited": [
            "factor_backfill_before_full_history_coverage_pit_audit",
            "p310_before_year_symbol_ann_date_duplicate_and_sync_attempt_audit",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



async fn build_equity_pledge_pressure_coverage_audit(
    db: &sqlx::PgPool,
    req: EquityPledgeCoverageAuditReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?;
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?;
    if let (Some(start), Some(end)) = (start, end) {
        if start > end {
            return Err("equity_pledge coverage start_date cannot be after end_date".into());
        }
    }

    let readiness = build_equity_pledge_pressure_readiness_audit(db).await?;
    let schema_passed = readiness
        .get("schema_passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let stat_summary: (i64, i64, Option<NaiveDate>, Option<NaiveDate>, i64, i64) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(end_date) AS min_date,
               MAX(end_date) AS max_date,
               COUNT(*) FILTER (WHERE available_at < end_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (
                   WHERE pledge_ratio IS NOT NULL
                     AND (pledge_ratio < 0 OR pledge_ratio > 100)
               )::bigint AS pledge_ratio_out_of_range_count
        FROM market_stock_pledge_stat
        WHERE ($1::date IS NULL OR end_date >= $1)
          AND ($2::date IS NULL OR end_date <= $2)
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize pledge_stat coverage: {error}"))?;

    let detail_summary: (
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(ann_date) AS min_date,
               MAX(ann_date) AS max_date,
               COUNT(*) FILTER (WHERE available_at < ann_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (
                   WHERE release_date IS NOT NULL
                     AND pledge_start_date IS NOT NULL
                     AND release_date < pledge_start_date
               )::bigint AS release_before_start_count,
               COUNT(*) FILTER (
                   WHERE (p_total_ratio IS NOT NULL AND (p_total_ratio < 0 OR p_total_ratio > 100))
                      OR (h_total_ratio IS NOT NULL AND (h_total_ratio < 0 OR h_total_ratio > 100))
               )::bigint AS detail_ratio_out_of_range_count
        FROM market_stock_pledge_detail
        WHERE ($1::date IS NULL OR ann_date >= $1)
          AND ($2::date IS NULL OR ann_date <= $2)
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize pledge_detail coverage: {error}"))?;

    let year_breakdown: Vec<(
        String,
        i32,
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
    )> = sqlx::query_as(
        r#"
            SELECT source, year, rows, symbols, min_date, max_date, pit_violation_rows
            FROM (
                SELECT 'pledge_stat'::text AS source,
                       EXTRACT(YEAR FROM end_date)::int AS year,
                       COUNT(*)::bigint AS rows,
                       COUNT(DISTINCT symbol)::bigint AS symbols,
                       MIN(end_date) AS min_date,
                       MAX(end_date) AS max_date,
                       COUNT(*) FILTER (WHERE available_at < end_date)::bigint AS pit_violation_rows
                FROM market_stock_pledge_stat
                WHERE ($1::date IS NULL OR end_date >= $1)
                  AND ($2::date IS NULL OR end_date <= $2)
                GROUP BY 1, 2
                UNION ALL
                SELECT 'pledge_detail'::text AS source,
                       EXTRACT(YEAR FROM ann_date)::int AS year,
                       COUNT(*)::bigint AS rows,
                       COUNT(DISTINCT symbol)::bigint AS symbols,
                       MIN(ann_date) AS min_date,
                       MAX(ann_date) AS max_date,
                       COUNT(*) FILTER (WHERE available_at < ann_date)::bigint AS pit_violation_rows
                FROM market_stock_pledge_detail
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                GROUP BY 1, 2
            ) breakdown
            ORDER BY year, source
            "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to build pledge year breakdown: {error}"))?;

    let symbol_breadth: (i64, i64) = sqlx::query_as(
        r#"
        WITH reference AS (
            SELECT DISTINCT symbol
            FROM market_stock
            WHERE list_status = 'L'
              AND market IN ('主板', '创业板')
              AND COALESCE(name, '') NOT ILIKE '%ST%'
        ),
        raw_symbols AS (
            SELECT DISTINCT symbol
            FROM market_stock_pledge_stat
            WHERE ($1::date IS NULL OR end_date >= $1)
              AND ($2::date IS NULL OR end_date <= $2)
            UNION
            SELECT DISTINCT symbol
            FROM market_stock_pledge_detail
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
        )
        SELECT (SELECT COUNT(*)::bigint FROM reference) AS reference_symbols,
               COUNT(raw_symbols.symbol)::bigint AS covered_symbols
        FROM reference
        LEFT JOIN raw_symbols ON raw_symbols.symbol = reference.symbol
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to build pledge symbol breadth: {error}"))?;

    let duplicate_source_row_hash_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(row_count - 1), 0)::bigint
        FROM (
            SELECT source_row_hash, COUNT(*)::bigint AS row_count
            FROM market_stock_pledge_detail
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            GROUP BY source_row_hash
            HAVING COUNT(*) > 1
        ) duplicate_groups
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count pledge duplicate hashes: {error}"))?;

    let attempt_breakdown: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source IN (
            'equity_pledge_stat_symbol',
            'equity_pledge_stat_end_date',
            'equity_pledge_detail_ann_date'
        )
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let raw_rows = stat_summary.0 + detail_summary.0;
    let pit_violation_rows = stat_summary.4 + detail_summary.4;
    let symbol_coverage_ratio = phase7_ratio(symbol_breadth.1, symbol_breadth.0).unwrap_or(0.0);
    let decision = decide_equity_pledge_coverage_audit(
        schema_passed,
        raw_rows,
        symbol_coverage_ratio,
        pit_violation_rows,
        duplicate_source_row_hash_count,
    );

    Ok(json!({
        "audit_version": "p3.20d-equity-pledge-pressure-coverage-audit-v1",
        "source_id": "equity_pledge_pressure",
        "mode": "read_only_year_symbol_ann_date_coverage_duplicate_audit",
        "date_range": {
            "start_date": phase7_date_json(start),
            "end_date": phase7_date_json(end),
        },
        "schema_passed": schema_passed,
        "raw_summary": {
            "stat_rows": stat_summary.0,
            "stat_symbols": stat_summary.1,
            "stat_min_end_date": phase7_date_json(stat_summary.2),
            "stat_max_end_date": phase7_date_json(stat_summary.3),
            "detail_rows": detail_summary.0,
            "detail_symbols": detail_summary.1,
            "detail_min_ann_date": phase7_date_json(detail_summary.2),
            "detail_max_ann_date": phase7_date_json(detail_summary.3),
            "raw_rows": raw_rows,
            "pit_violation_rows": pit_violation_rows,
            "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
            "stat_pledge_ratio_out_of_range_count": stat_summary.5,
            "detail_release_before_start_count": detail_summary.5,
            "detail_ratio_out_of_range_count": detail_summary.6,
        },
        "symbol_breadth_vs_main_chinext_non_st": {
            "reference_symbols": symbol_breadth.0,
            "covered_symbols": symbol_breadth.1,
            "coverage_ratio": symbol_coverage_ratio,
        },
        "year_breakdown": year_breakdown
            .into_iter()
            .map(|(source, year, rows, symbols, min_date, max_date, pit_violation_rows)| json!({
                "source": source,
                "year": year,
                "rows": rows,
                "symbols": symbols,
                "min_date": phase7_date_json(min_date),
                "max_date": phase7_date_json(max_date),
                "pit_violation_rows": pit_violation_rows,
            }))
            .collect::<Vec<_>>(),
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, completed_attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "completed_attempts": completed_attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "decision": decision,
        "promotion_gate": {
            "factor_builder": "blocked_until_coverage_readiness_and_pit_pass",
            "p310_status": decision["p310_status"].clone(),
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    }))
}



async fn build_margin_detail_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
    let mut table_results = Vec::new();
    let mut schema_passed = true;
    let mut raw_rows = 0_i64;
    let mut pit_violation_rows = 0_i64;
    let mut missing_source_published_at_rows = 0_i64;
    let mut core_negative_rows = 0_i64;
    let mut negative_rzche_rows = 0_i64;
    let mut negative_rqchl_rows = 0_i64;

    for (table, required_constraints) in margin_detail_expected_schema() {
        let regclass_name = format!("public.{table}");
        let exists = table_exists(db, table).await?;

        let row_count = if exists {
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::bigint FROM {table}"))
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count {table}: {error}"))?
        } else {
            0
        };

        let (
            table_pit_violations,
            table_missing_published_at,
            table_core_negative_rows,
            table_negative_rzche_rows,
            table_negative_rqchl_rows,
        ): (i64, i64, i64, i64, i64) = if exists {
            sqlx::query_as(
                r#"
                SELECT COUNT(*) FILTER (WHERE available_at <= trade_date)::bigint AS pit_violation_rows,
                       COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows,
                       COUNT(*) FILTER (
                           WHERE (rzye IS NOT NULL AND rzye < 0)
                              OR (rqye IS NOT NULL AND rqye < 0)
                              OR (rzmre IS NOT NULL AND rzmre < 0)
                              OR (rqyl IS NOT NULL AND rqyl < 0)
                              OR (rqmcl IS NOT NULL AND rqmcl < 0)
                              OR (rzrqye IS NOT NULL AND rzrqye < 0)
                       )::bigint AS core_negative_rows,
                       COUNT(*) FILTER (WHERE rzche IS NOT NULL AND rzche < 0)::bigint AS negative_rzche_rows,
                       COUNT(*) FILTER (WHERE rqchl IS NOT NULL AND rqchl < 0)::bigint AS negative_rqchl_rows
                FROM market_stock_margin_detail
                "#,
            )
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to summarize margin_detail readiness: {error}"))?
        } else {
            (0, 0, 0, 0, 0)
        };

        let constraints: Vec<String> = if exists {
            sqlx::query_scalar(
                r#"
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = to_regclass($1)
                ORDER BY conname
                "#,
            )
            .bind(&regclass_name)
            .fetch_all(db)
            .await
            .map_err(|error| format!("Failed to inspect constraints for {table}: {error}"))?
        } else {
            Vec::new()
        };

        let missing_constraints: Vec<&str> = required_constraints
            .iter()
            .copied()
            .filter(|constraint| !constraints.iter().any(|existing| existing == constraint))
            .collect();

        let passed = exists && missing_constraints.is_empty();
        schema_passed &= passed;
        raw_rows += row_count;
        pit_violation_rows += table_pit_violations;
        missing_source_published_at_rows += table_missing_published_at;
        core_negative_rows += table_core_negative_rows;
        negative_rzche_rows += table_negative_rzche_rows;
        negative_rqchl_rows += table_negative_rqchl_rows;

        table_results.push(json!({
            "table": table,
            "table_exists": exists,
            "row_count": row_count,
            "pit_violation_rows": table_pit_violations,
            "missing_source_published_at_rows": table_missing_published_at,
            "core_negative_rows": table_core_negative_rows,
            "negative_adjustment_rows": {
                "rzche_negative_rows": table_negative_rzche_rows,
                "rqchl_negative_rows": table_negative_rqchl_rows,
            },
            "required_constraints": required_constraints,
            "missing_constraints": missing_constraints,
            "passed": passed,
        }));
    }

    let attempt_breakdown: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source = 'tushare:margin_detail'
           OR source = 'margin_detail'
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let decision = decide_margin_detail_readiness(
        schema_passed,
        raw_rows,
        pit_violation_rows,
        missing_source_published_at_rows,
        core_negative_rows,
    );

    Ok(json!({
        "audit_version": "p3.22d-margin-detail-readiness-v1",
        "source_id": "margin_detail_leverage_crowding",
        "mode": "read_only_schema_raw_pit_quality_rowcount_audit",
        "schema_passed": schema_passed,
        "tables": table_results,
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, completed_attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "completed_attempts": completed_attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "decision": decision,
        "pit_policy": {
            "raw_available_at": "conservative next open trading date after trade_date",
            "source_published_at": "conservative next-session 08:30 China time represented in UTC by sync layer",
            "intraday_rule": "intraday trading may only use margin_detail rows whose source_published_at is <= decision timestamp; same-day trade_date rows are never available intraday"
        },
        "quality_policy": {
            "negative_adjustments_are_allowed": ["rzche", "rqchl"],
            "core_nonnegative_fields": ["rzye", "rqye", "rzmre", "rqyl", "rqmcl", "rzrqye"],
            "negative_adjustment_rows": {
                "rzche_negative_rows": negative_rzche_rows,
                "rqchl_negative_rows": negative_rqchl_rows,
            }
        },
        "prohibited": [
            "factor_build_before_full_history_coverage_pit_quality_correlation_audit",
            "p310_before_year_market_symbol_negative_adjustment_and_correlation_audit",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



fn margin_detail_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
    batches: Vec<MarginDetailSyncPlanBatch>,
) -> Value {
    const DEFAULT_ESTIMATED_ROWS_PER_DAY: f64 = 4_500.0;

    let batch_values = batches
        .iter()
        .map(|batch| {
            let rows_per_day = batch
                .observed_avg_rows_per_day
                .unwrap_or(DEFAULT_ESTIMATED_ROWS_PER_DAY);
            let estimated_rows = (rows_per_day * batch.open_day_count as f64).round() as i64;
            json!({
                "batch": batch.label,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "open_day_count": batch.open_day_count,
                "estimated_rows": estimated_rows,
                "estimated_rows_basis": if batch.observed_avg_rows_per_day.is_some() {
                    "observed_market_stock_margin_detail_rows_per_open_day"
                } else {
                    "default_4500_rows_per_open_day_after_single_day_smoke"
                },
                "recommended_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("margin-detail-{}", batch.label),
                    "background": true
                }
            })
        })
        .collect::<Vec<_>>();

    let estimated_total_rows = batch_values
        .iter()
        .filter_map(|batch| batch.get("estimated_rows").and_then(Value::as_i64))
        .sum::<i64>();

    json!({
        "audit_version": "p3.22d-margin-detail-sync-plan-v1",
        "source_id": "margin_detail_leverage_crowding",
        "mode": "read_only_bounded_sync_plan",
        "date_range": {
            "start_date": start.format("%Y-%m-%d").to_string(),
            "end_date": end.format("%Y-%m-%d").to_string(),
        },
        "batch_mode": batch_mode,
        "batch_count": batch_values.len(),
        "estimated_total_rows": estimated_total_rows,
        "recommended_batch_granularity": batch_mode,
        "batches": batch_values,
        "required_follow_up_after_each_batch": [
            "GET /api/v1/quant/data/margin-detail/coverage-audit",
            "check decision.p310_status remains blocked until full-history coverage and correlation pass",
            "inspect negative_adjustment_rows for rzche and rqchl without deleting raw vendor adjustments"
        ],
        "prohibited": [
            "do_not_run_p310_before_coverage_pit_quality_correlation_audit_passes",
            "do_not_interpret_raw_sync_ready_as_trainable_ready"
        ]
    })
}



async fn build_margin_detail_sync_plan(
    db: &sqlx::PgPool,
    req: MarginDetailSyncPlanReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?
        .unwrap_or_else(|| Utc::now().date_naive());
    if start > end {
        return Err("margin_detail sync-plan start_date cannot be after end_date".into());
    }
    let batch_mode = req.batch.unwrap_or_else(|| "year".to_string());
    if !matches!(batch_mode.as_str(), "year" | "quarter") {
        return Err("margin_detail sync-plan batch must be 'year' or 'quarter'".into());
    }

    let open_dates: Vec<NaiveDate> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND trade_date >= $1
          AND trade_date <= $2
        ORDER BY trade_date
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load market open dates for margin_detail plan: {error}"))?;

    let observed_avg_rows_per_day = if table_exists(db, "market_stock_margin_detail").await? {
        sqlx::query_scalar::<_, Option<f64>>(
            r#"
            SELECT COUNT(*)::double precision / NULLIF(COUNT(DISTINCT trade_date), 0)::double precision
            FROM market_stock_margin_detail
            "#,
        )
        .fetch_one(db)
        .await
        .unwrap_or(None)
    } else {
        None
    };

    let mut grouped: BTreeMap<String, Vec<NaiveDate>> = BTreeMap::new();
    for date in open_dates {
        let label = if batch_mode == "quarter" {
            format!("{}q{}", date.year(), ((date.month0() / 3) + 1))
        } else {
            date.year().to_string()
        };
        grouped.entry(label).or_default().push(date);
    }

    let batches = grouped
        .into_iter()
        .filter_map(|(label, dates)| {
            let start_date = dates.first().copied()?;
            let end_date = dates.last().copied()?;
            Some(MarginDetailSyncPlanBatch {
                label,
                start_date,
                end_date,
                open_day_count: dates.len() as i64,
                observed_avg_rows_per_day,
            })
        })
        .collect::<Vec<_>>();

    Ok(margin_detail_sync_plan_response(
        start,
        end,
        &batch_mode,
        batches,
    ))
}



async fn build_margin_detail_correlation_audit(
    db: &sqlx::PgPool,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    margin_detail_exists: bool,
) -> Result<Value, String> {
    if !margin_detail_exists {
        return Ok(json!({
            "status": "blocked_until_margin_detail_table_exists",
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

    let stats: (i64, Option<f64>, Option<f64>, Option<f64>, Option<f64>) = sqlx::query_as(
        r#"
        WITH margin_features AS (
            SELECT symbol,
                   trade_date,
                   rzmre::double precision AS rzmre,
                   rzye::double precision AS rzye,
                   LAG(rzye::double precision) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_rzye
            FROM market_stock_margin_detail
            WHERE ($1::date IS NULL OR trade_date >= $1)
              AND ($2::date IS NULL OR trade_date <= $2)
        ),
        joined AS (
            SELECT (mf.rzmre / NULLIF(bar.amount::double precision, 0)) AS margin_buy_to_amount,
                   ((mf.rzye - mf.prev_rzye) / NULLIF(ABS(mf.prev_rzye), 0)) AS financing_balance_chg1,
                   (money.net_mf_amount::double precision / NULLIF(bar.amount::double precision, 0)) AS moneyflow_net_to_amount,
                   LN(NULLIF(bar.amount::double precision, 0)) AS ln_amount,
                   ABS((bar.close::double precision - bar.pre_close::double precision)
                       / NULLIF(bar.pre_close::double precision, 0)) AS abs_return
            FROM margin_features mf
            JOIN market_stock_daily_bar bar
              ON bar.symbol = mf.symbol
             AND bar.trade_date = mf.trade_date
            LEFT JOIN market_stock_moneyflow money
              ON money.symbol = mf.symbol
             AND money.trade_date = mf.trade_date
        )
        SELECT COUNT(*)::bigint AS sample_rows,
               CORR(margin_buy_to_amount, moneyflow_net_to_amount) AS corr_margin_buy_moneyflow,
               CORR(margin_buy_to_amount, ln_amount) AS corr_margin_buy_ln_amount,
               CORR(margin_buy_to_amount, abs_return) AS corr_margin_buy_abs_return,
               CORR(financing_balance_chg1, moneyflow_net_to_amount) AS corr_balance_chg_moneyflow
        FROM joined
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to build margin_detail correlation audit: {error}"))?;

    let correlations = [stats.1, stats.2, stats.3, stats.4];
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
            "margin_buy_to_amount_vs_moneyflow_net_to_amount": stats.1,
            "margin_buy_to_amount_vs_ln_amount_liquidity": stats.2,
            "margin_buy_to_amount_vs_abs_return_price_volume": stats.3,
            "financing_balance_chg1_vs_moneyflow_net_to_amount": stats.4,
        },
        "gate_note": "linear screen only; even if passed, P3.10 RankIC/group/decay/turnover-capacity remains mandatory"
    }))
}



async fn build_margin_detail_coverage_audit(
    db: &sqlx::PgPool,
    req: MarginDetailCoverageAuditReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?;
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?;
    if let (Some(start), Some(end)) = (start, end) {
        if start > end {
            return Err("margin_detail coverage start_date cannot be after end_date".into());
        }
    }

    let readiness = build_margin_detail_readiness_audit(db).await?;
    let schema_passed = readiness
        .get("schema_passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let margin_detail_exists = table_exists(db, "market_stock_margin_detail").await?;

    if !margin_detail_exists {
        let decision = decide_margin_detail_coverage_audit(
            schema_passed,
            0,
            0,
            0,
            0.0,
            0.0,
            0,
            0,
            0,
            0,
            "blocked_until_correlation_sample_available",
        );
        return Ok(json!({
            "audit_version": "p3.22d-margin-detail-coverage-audit-v1",
            "source_id": "margin_detail_leverage_crowding",
            "mode": "read_only_year_market_symbol_pit_quality_correlation_audit",
            "schema_passed": schema_passed,
            "readiness": readiness,
            "raw_summary": {
                "raw_rows": 0,
                "pit_violation_rows": 0,
                "missing_source_published_at_rows": 0,
                "core_negative_rows": 0
            },
            "correlation_audit": {
                "status": "blocked_until_margin_detail_table_exists",
                "decision": "blocked_until_correlation_sample_available"
            },
            "decision": decision,
            "promotion_gate": {
                "factor_builder": "blocked_until_coverage_pit_quality_and_correlation_pass",
                "p310_status": decision["p310_status"].clone(),
                "bounded_wfa": "blocked",
                "v19_train_selection": "blocked"
            }
        }));
    }

    let summary: (
        i64,
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               COUNT(DISTINCT trade_date)::bigint AS covered_trade_days,
               MIN(trade_date) AS min_trade_date,
               MAX(trade_date) AS max_trade_date,
               COUNT(*) FILTER (WHERE available_at <= trade_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows,
               COUNT(*) FILTER (
                   WHERE (rzye IS NOT NULL AND rzye < 0)
                      OR (rqye IS NOT NULL AND rqye < 0)
                      OR (rzmre IS NOT NULL AND rzmre < 0)
                      OR (rqyl IS NOT NULL AND rqyl < 0)
                      OR (rqmcl IS NOT NULL AND rqmcl < 0)
                      OR (rzrqye IS NOT NULL AND rzrqye < 0)
               )::bigint AS core_negative_rows,
               COUNT(*) FILTER (WHERE rzche IS NOT NULL AND rzche < 0)::bigint AS rzche_negative_rows,
               COUNT(*) FILTER (WHERE rqchl IS NOT NULL AND rqchl < 0)::bigint AS rqchl_negative_rows
        FROM market_stock_margin_detail
        WHERE ($1::date IS NULL OR trade_date >= $1)
          AND ($2::date IS NULL OR trade_date <= $2)
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize margin_detail coverage: {error}"))?;

    let effective_start = start.or(summary.3);
    let effective_end = end.or(summary.4);
    let open_day_count: i64 = if let (Some(start), Some(end)) = (effective_start, effective_end) {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(DISTINCT trade_date)::bigint
            FROM market_trade_calendar
            WHERE is_open = true
              AND trade_date >= $1
              AND trade_date <= $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to count open days for margin_detail coverage: {error}"))?
    } else {
        0
    };

    let reference_symbols: i64 = if let (Some(start), Some(end)) = (effective_start, effective_end)
    {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::bigint
            FROM market_stock
            WHERE symbol ~ '^[0-9]{6}\.(SH|SZ|BJ)$'
              AND list_date IS NOT NULL
              AND list_date <= $1
              AND (delist_date IS NULL OR delist_date >= $2)
            "#,
        )
        .bind(end)
        .bind(start)
        .fetch_one(db)
        .await
        .unwrap_or(0)
    } else {
        0
    };

    let year_rows: Vec<(
        i32,
        i64,
        i64,
        i64,
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
    )> = sqlx::query_as(
        r#"
        SELECT EXTRACT(YEAR FROM trade_date)::int AS year,
               COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               COUNT(DISTINCT trade_date)::bigint AS covered_trade_days,
               COUNT(*) FILTER (WHERE rzche IS NOT NULL AND rzche < 0)::bigint AS rzche_negative_rows,
               COUNT(*) FILTER (WHERE rqchl IS NOT NULL AND rqchl < 0)::bigint AS rqchl_negative_rows,
               MIN(trade_date) AS min_trade_date,
               MAX(trade_date) AS max_trade_date
        FROM market_stock_margin_detail
        WHERE ($1::date IS NULL OR trade_date >= $1)
          AND ($2::date IS NULL OR trade_date <= $2)
        GROUP BY 1
        ORDER BY 1
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to build margin_detail year breakdown: {error}"))?;

    let mut rows_by_year: BTreeMap<
        i32,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
        ),
    > = BTreeMap::new();
    for (year, rows, symbols, covered_days, rzche_negative, rqchl_negative, min_date, max_date) in
        year_rows
    {
        rows_by_year.insert(
            year,
            (
                rows,
                symbols,
                covered_days,
                rzche_negative,
                rqchl_negative,
                min_date,
                max_date,
            ),
        );
    }

    let mut missing_year_count = 0_i64;
    let mut year_breakdown = Vec::new();
    if let (Some(start), Some(end)) = (effective_start, effective_end) {
        for year in start.year()..=end.year() {
            let (rows, symbols, covered_days, rzche_negative, rqchl_negative, min_date, max_date) =
                rows_by_year
                    .remove(&year)
                    .unwrap_or((0, 0, 0, 0, 0, None, None));
            if rows == 0 {
                missing_year_count += 1;
            }
            year_breakdown.push(json!({
                "year": year,
                "rows": rows,
                "symbols": symbols,
                "covered_trade_days": covered_days,
                "rzche_negative_rows": rzche_negative,
                "rqchl_negative_rows": rqchl_negative,
                "min_trade_date": phase7_date_json(min_date),
                "max_trade_date": phase7_date_json(max_date),
            }));
        }
    }

    let market_breakdown: Vec<(String, i64, i64, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT CASE
                   WHEN symbol LIKE '%.SH' THEN 'SH'
                   WHEN symbol LIKE '%.SZ' THEN 'SZ'
                   WHEN symbol LIKE '%.BJ' THEN 'BJ'
                   ELSE 'OTHER'
               END AS market,
               COUNT(*)::bigint AS rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               COUNT(DISTINCT trade_date)::bigint AS covered_trade_days,
               COUNT(*) FILTER (WHERE rzche IS NOT NULL AND rzche < 0)::bigint AS rzche_negative_rows,
               COUNT(*) FILTER (WHERE rqchl IS NOT NULL AND rqchl < 0)::bigint AS rqchl_negative_rows
        FROM market_stock_margin_detail
        WHERE ($1::date IS NULL OR trade_date >= $1)
          AND ($2::date IS NULL OR trade_date <= $2)
        GROUP BY 1
        ORDER BY 1
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to build margin_detail market breakdown: {error}"))?;

    let attempt_breakdown: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source = 'tushare:margin_detail'
           OR source = 'margin_detail'
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let correlation_audit =
        build_margin_detail_correlation_audit(db, start, end, margin_detail_exists).await?;
    let correlation_decision = correlation_audit
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_correlation_sample_available");
    let covered_trade_day_ratio = phase7_ratio(summary.2, open_day_count).unwrap_or(0.0);
    let symbol_coverage_ratio = phase7_ratio(summary.1, reference_symbols).unwrap_or(0.0);
    let decision = decide_margin_detail_coverage_audit(
        schema_passed,
        summary.0,
        summary.2,
        open_day_count,
        covered_trade_day_ratio,
        symbol_coverage_ratio,
        summary.5,
        summary.6,
        summary.7,
        missing_year_count,
        correlation_decision,
    );

    Ok(json!({
        "audit_version": "p3.22d-margin-detail-coverage-audit-v1",
        "source_id": "margin_detail_leverage_crowding",
        "mode": "read_only_year_market_symbol_pit_quality_correlation_audit",
        "date_range": {
            "start_date": phase7_date_json(start),
            "end_date": phase7_date_json(end),
            "effective_start_date": phase7_date_json(effective_start),
            "effective_end_date": phase7_date_json(effective_end),
        },
        "schema_passed": schema_passed,
        "readiness": readiness,
        "raw_summary": {
            "raw_rows": summary.0,
            "symbols": summary.1,
            "covered_trade_days": summary.2,
            "open_trade_days": open_day_count,
            "covered_trade_day_ratio": covered_trade_day_ratio,
            "min_trade_date": phase7_date_json(summary.3),
            "max_trade_date": phase7_date_json(summary.4),
            "pit_violation_rows": summary.5,
            "missing_source_published_at_rows": summary.6,
            "core_negative_rows": summary.7,
            "negative_adjustment_rows": {
                "rzche_negative_rows": summary.8,
                "rqchl_negative_rows": summary.9,
            }
        },
        "symbol_breadth_vs_listed_stock_universe": {
            "reference_symbols": reference_symbols,
            "covered_symbols": summary.1,
            "coverage_ratio": symbol_coverage_ratio,
            "note": "margin_detail is naturally limited to marginable securities; low ratio blocks trainable alpha until explicitly accepted as a market-scope gate"
        },
        "year_breakdown": year_breakdown,
        "market_breakdown": market_breakdown
            .into_iter()
            .map(|(market, rows, symbols, covered_days, rzche_negative, rqchl_negative)| json!({
                "market": market,
                "rows": rows,
                "symbols": symbols,
                "covered_trade_days": covered_days,
                "rzche_negative_rows": rzche_negative,
                "rqchl_negative_rows": rqchl_negative,
            }))
            .collect::<Vec<_>>(),
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, completed_attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "completed_attempts": completed_attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "correlation_audit": correlation_audit,
        "decision": decision,
        "promotion_gate": {
            "factor_builder": "blocked_until_coverage_pit_quality_and_correlation_pass",
            "p310_status": decision["p310_status"].clone(),
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    }))
}



async fn build_shareholder_structure_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
    let mut table_results = Vec::new();
    let mut schema_passed = true;
    let mut raw_rows = 0_i64;
    let mut pit_violation_rows = 0_i64;

    for (table, required_constraints) in shareholder_structure_expected_schema() {
        let regclass_name = format!("public.{table}");
        let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1)::text IS NOT NULL")
            .bind(&regclass_name)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to inspect {table}: {error}"))?;

        let row_count = if table_exists {
            let sql = format!("SELECT COUNT(*)::bigint FROM {table}");
            sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count {table}: {error}"))?
        } else {
            0
        };

        let table_pit_violations = if table_exists {
            let sql = match table {
                "market_stock_holder_number" => {
                    "SELECT COUNT(*)::bigint FROM market_stock_holder_number WHERE available_at < ann_date OR available_at < end_date"
                }
                "market_stock_top10_holders" => {
                    "SELECT COUNT(*)::bigint FROM market_stock_top10_holders WHERE available_at < ann_date OR available_at < end_date"
                }
                "market_stock_top10_float_holders" => {
                    "SELECT COUNT(*)::bigint FROM market_stock_top10_float_holders WHERE available_at < ann_date OR available_at < end_date"
                }
                "market_stock_holder_trade" => {
                    "SELECT COUNT(*)::bigint FROM market_stock_holder_trade WHERE available_at < ann_date OR (begin_date IS NOT NULL AND close_date IS NOT NULL AND close_date < begin_date)"
                }
                _ => "SELECT 0::bigint",
            };
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count PIT violations for {table}: {error}"))?
        } else {
            0
        };

        let constraints: Vec<String> = if table_exists {
            sqlx::query_scalar(
                r#"
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = to_regclass($1)
                ORDER BY conname
                "#,
            )
            .bind(&regclass_name)
            .fetch_all(db)
            .await
            .map_err(|error| format!("Failed to inspect constraints for {table}: {error}"))?
        } else {
            Vec::new()
        };

        let missing_constraints: Vec<&str> = required_constraints
            .iter()
            .copied()
            .filter(|constraint| !constraints.iter().any(|existing| existing == constraint))
            .collect();

        let passed = table_exists && missing_constraints.is_empty();
        schema_passed &= passed;
        raw_rows += row_count;
        pit_violation_rows += table_pit_violations;

        table_results.push(json!({
            "table": table,
            "table_exists": table_exists,
            "row_count": row_count,
            "pit_violation_rows": table_pit_violations,
            "required_constraints": required_constraints,
            "missing_constraints": missing_constraints,
            "passed": passed,
        }));
    }

    let attempt_breakdown: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source IN (
            'shareholder_holder_number_ann_date',
            'shareholder_top10_holders_ann_date',
            'shareholder_top10_float_holders_ann_date',
            'shareholder_holder_trade_ann_date'
        )
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let decision =
        decide_shareholder_structure_readiness(schema_passed, raw_rows, pit_violation_rows);

    Ok(json!({
        "audit_version": "p3.21c-shareholder-structure-readiness-v1",
        "source_id": "shareholder_structure",
        "mode": "read_only_schema_raw_pit_rowcount_audit",
        "schema_passed": schema_passed,
        "tables": table_results,
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "decision": decision,
        "pit_policy": {
            "available_at": "available_at equals ann_date for all shareholder_structure raw rows",
            "period_snapshot_rule": "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated.",
            "intraday_rule": "same-day shareholder disclosures are unavailable to intraday trading without source publication timestamps"
        },
        "prohibited": [
            "factor_backfill_before_full_history_coverage_pit_audit",
            "p310_before_year_symbol_ann_date_duplicate_and_sync_attempt_audit",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



async fn build_shareholder_structure_coverage_audit(
    db: &sqlx::PgPool,
    req: ShareholderStructureCoverageAuditReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?;
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?;
    if let (Some(start), Some(end)) = (start, end) {
        if start > end {
            return Err(
                "shareholder_structure coverage start_date cannot be after end_date".into(),
            );
        }
    }

    let readiness = build_shareholder_structure_readiness_audit(db).await?;
    let schema_passed = readiness
        .get("schema_passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !schema_passed {
        let decision = decide_shareholder_structure_coverage_audit(false, 0, 0.0, 0, 0, 0, 0);
        return Ok(json!({
            "audit_version": "p3.21d-shareholder-structure-coverage-audit-v1",
            "source_id": "shareholder_structure",
            "mode": "read_only_year_source_symbol_ann_date_coverage_duplicate_audit",
            "schema_passed": false,
            "date_range": {
                "start_date": phase7_date_json(start),
                "end_date": phase7_date_json(end),
            },
            "decision": decision,
            "readiness_audit": readiness,
            "prohibited": [
                "raw_factor_backfill_before_schema_passes",
                "p310_before_coverage_and_pit_readiness",
                "bounded_wfa_or_v19_train_selection_before_p310_passes"
            ]
        }));
    }

    let raw_summary: (
        i64,
        i64,
        Option<NaiveDate>,
        Option<NaiveDate>,
        i64,
        i64,
        i64,
        i64,
    ) =
        sqlx::query_as(
            r#"
            WITH raw AS (
                SELECT 'holder_number'::text AS source, symbol, ann_date, end_date, available_at,
                       source_row_hash, NULL::numeric AS ratio_a, NULL::numeric AS ratio_b,
                       NULL::date AS begin_date, NULL::date AS close_date, holder_num
                FROM market_stock_holder_number
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'top10_holders'::text AS source, symbol, ann_date, end_date, available_at,
                       source_row_hash, hold_ratio AS ratio_a, hold_float_ratio AS ratio_b,
                       NULL::date AS begin_date, NULL::date AS close_date, NULL::bigint AS holder_num
                FROM market_stock_top10_holders
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'top10_float_holders'::text AS source, symbol, ann_date, end_date, available_at,
                       source_row_hash, hold_ratio AS ratio_a, hold_float_ratio AS ratio_b,
                       NULL::date AS begin_date, NULL::date AS close_date, NULL::bigint AS holder_num
                FROM market_stock_top10_float_holders
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'holder_trade'::text AS source, symbol, ann_date, NULL::date AS end_date,
                       available_at, source_row_hash, change_ratio AS ratio_a, after_ratio AS ratio_b,
                       begin_date, close_date, NULL::bigint AS holder_num
                FROM market_stock_holder_trade
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
            )
            SELECT COUNT(*)::bigint AS raw_rows,
                   COUNT(DISTINCT symbol)::bigint AS symbols,
                   MIN(ann_date) AS min_ann_date,
                   MAX(ann_date) AS max_ann_date,
                   COUNT(*) FILTER (
                       WHERE available_at < ann_date
                          OR (end_date IS NOT NULL AND available_at < end_date)
                   )::bigint AS pit_violation_rows,
                   COUNT(*) FILTER (
                       WHERE (ratio_a IS NOT NULL AND (ratio_a < -100 OR ratio_a > 100))
                          OR (ratio_b IS NOT NULL AND (ratio_b < -100 OR ratio_b > 100))
                   )::bigint AS ratio_out_of_range_count,
                   COUNT(*) FILTER (
                       WHERE begin_date IS NOT NULL
                         AND close_date IS NOT NULL
                         AND close_date < begin_date
                   )::bigint AS interval_violation_count,
                   COUNT(*) FILTER (
                       WHERE source = 'holder_number'
                         AND (holder_num IS NULL OR holder_num <= 0)
                   )::bigint AS holder_number_missing_value_count
            FROM raw
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to summarize shareholder structure coverage: {error}"))?;

    let year_breakdown: Vec<(String, i32, i64, i64, Option<NaiveDate>, Option<NaiveDate>, i64)> =
        sqlx::query_as(
            r#"
            WITH raw AS (
                SELECT 'holder_number'::text AS source, symbol, ann_date, end_date, available_at
                FROM market_stock_holder_number
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'top10_holders'::text AS source, symbol, ann_date, end_date, available_at
                FROM market_stock_top10_holders
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'top10_float_holders'::text AS source, symbol, ann_date, end_date, available_at
                FROM market_stock_top10_float_holders
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
                UNION ALL
                SELECT 'holder_trade'::text AS source, symbol, ann_date, NULL::date AS end_date, available_at
                FROM market_stock_holder_trade
                WHERE ($1::date IS NULL OR ann_date >= $1)
                  AND ($2::date IS NULL OR ann_date <= $2)
            )
            SELECT source,
                   EXTRACT(YEAR FROM ann_date)::int AS year,
                   COUNT(*)::bigint AS rows,
                   COUNT(DISTINCT symbol)::bigint AS symbols,
                   MIN(ann_date) AS min_ann_date,
                   MAX(ann_date) AS max_ann_date,
                   COUNT(*) FILTER (
                       WHERE available_at < ann_date
                          OR (end_date IS NOT NULL AND available_at < end_date)
                   )::bigint AS pit_violation_rows
            FROM raw
            GROUP BY source, year
            ORDER BY year, source
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to build shareholder year breakdown: {error}"))?;

    let symbol_breadth: (i64, i64) = sqlx::query_as(
        r#"
        WITH reference AS (
            SELECT DISTINCT symbol
            FROM market_stock
            WHERE list_status = 'L'
              AND market IN ('主板', '创业板')
              AND COALESCE(name, '') NOT ILIKE '%ST%'
        ),
        raw_symbols AS (
            SELECT DISTINCT symbol FROM market_stock_holder_number
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION
            SELECT DISTINCT symbol FROM market_stock_top10_holders
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION
            SELECT DISTINCT symbol FROM market_stock_top10_float_holders
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION
            SELECT DISTINCT symbol FROM market_stock_holder_trade
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
        )
        SELECT (SELECT COUNT(*)::bigint FROM reference) AS reference_symbols,
               COUNT(raw_symbols.symbol)::bigint AS covered_symbols
        FROM reference
        LEFT JOIN raw_symbols ON raw_symbols.symbol = reference.symbol
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to build shareholder symbol breadth: {error}"))?;

    let duplicate_source_row_hash_count: i64 = sqlx::query_scalar(
        r#"
        WITH raw AS (
            SELECT 'holder_number'::text AS source, source_row_hash
            FROM market_stock_holder_number
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION ALL
            SELECT 'top10_holders'::text AS source, source_row_hash
            FROM market_stock_top10_holders
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION ALL
            SELECT 'top10_float_holders'::text AS source, source_row_hash
            FROM market_stock_top10_float_holders
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
            UNION ALL
            SELECT 'holder_trade'::text AS source, source_row_hash
            FROM market_stock_holder_trade
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
        )
        SELECT COALESCE(SUM(row_count - 1), 0)::bigint
        FROM (
            SELECT source, source_row_hash, COUNT(*)::bigint AS row_count
            FROM raw
            GROUP BY source, source_row_hash
            HAVING COUNT(*) > 1
        ) duplicate_groups
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count shareholder duplicate hashes: {error}"))?;

    let admissible_summary: (i64, i64, Option<NaiveDate>, Option<NaiveDate>, i64, i64) =
        sqlx::query_as(
            r#"
        WITH admissible AS (
            SELECT 'holder_number'::text AS source, symbol, ann_date, available_at, source_row_hash
            FROM market_stock_holder_number
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND available_at >= end_date
              AND holder_num IS NOT NULL
              AND holder_num > 0
            UNION ALL
            SELECT 'holder_trade'::text AS source, symbol, ann_date, available_at, source_row_hash
            FROM market_stock_holder_trade
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND (change_ratio IS NULL OR (change_ratio >= -100 AND change_ratio <= 100))
              AND (after_ratio IS NULL OR (after_ratio >= 0 AND after_ratio <= 100))
              AND (begin_date IS NULL OR close_date IS NULL OR close_date >= begin_date)
        ),
        duplicate_groups AS (
            SELECT source, source_row_hash, COUNT(*)::bigint AS row_count
            FROM admissible
            GROUP BY source, source_row_hash
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*)::bigint AS admissible_rows,
               COUNT(DISTINCT symbol)::bigint AS symbols,
               MIN(ann_date) AS min_ann_date,
               MAX(ann_date) AS max_ann_date,
               COUNT(DISTINCT EXTRACT(YEAR FROM ann_date)::int)::bigint AS observed_year_count,
               COALESCE((SELECT SUM(row_count - 1)::bigint FROM duplicate_groups), 0)::bigint
                   AS duplicate_source_row_hash_count
        FROM admissible
        "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to summarize shareholder admissible rows: {error}"))?;

    let admissible_symbol_breadth: (i64, i64) = sqlx::query_as(
        r#"
        WITH reference AS (
            SELECT DISTINCT symbol
            FROM market_stock
            WHERE list_status = 'L'
              AND market IN ('主板', '创业板')
              AND COALESCE(name, '') NOT ILIKE '%ST%'
        ),
        admissible_symbols AS (
            SELECT DISTINCT symbol
            FROM market_stock_holder_number
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND available_at >= end_date
              AND holder_num IS NOT NULL
              AND holder_num > 0
            UNION
            SELECT DISTINCT symbol
            FROM market_stock_holder_trade
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND (change_ratio IS NULL OR (change_ratio >= -100 AND change_ratio <= 100))
              AND (after_ratio IS NULL OR (after_ratio >= 0 AND after_ratio <= 100))
              AND (begin_date IS NULL OR close_date IS NULL OR close_date >= begin_date)
        )
        SELECT (SELECT COUNT(*)::bigint FROM reference) AS reference_symbols,
               COUNT(admissible_symbols.symbol)::bigint AS covered_symbols
        FROM reference
        LEFT JOIN admissible_symbols ON admissible_symbols.symbol = reference.symbol
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to build shareholder admissible symbol breadth: {error}"))?;

    let attempt_breakdown: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT source,
               COUNT(*)::bigint AS attempts,
               COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed_attempts,
               COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_attempts
        FROM data_sync_attempt
        WHERE source IN (
            'shareholder_holder_number_ann_date',
            'shareholder_top10_holders_ann_date',
            'shareholder_top10_float_holders_ann_date',
            'shareholder_holder_trade_ann_date'
        )
        GROUP BY source
        ORDER BY source
        "#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let symbol_coverage_ratio = phase7_ratio(symbol_breadth.1, symbol_breadth.0).unwrap_or(0.0);
    let admissible_symbol_coverage_ratio =
        phase7_ratio(admissible_symbol_breadth.1, admissible_symbol_breadth.0).unwrap_or(0.0);
    let requested_years: Vec<i32> = match (start, end) {
        (Some(start), Some(end)) => (start.year()..=end.year()).collect(),
        _ => Vec::new(),
    };
    let observed_years: BTreeSet<i32> = year_breakdown.iter().map(|row| row.1).collect();
    let missing_years: Vec<i32> = requested_years
        .iter()
        .copied()
        .filter(|year| !observed_years.contains(year))
        .collect();
    let missing_year_count = missing_years.len();
    let admissible_observed_years: Vec<i32> = sqlx::query_scalar(
        r#"
        WITH admissible AS (
            SELECT ann_date
            FROM market_stock_holder_number
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND available_at >= end_date
              AND holder_num IS NOT NULL
              AND holder_num > 0
            UNION ALL
            SELECT ann_date
            FROM market_stock_holder_trade
            WHERE ($1::date IS NULL OR ann_date >= $1)
              AND ($2::date IS NULL OR ann_date <= $2)
              AND available_at >= ann_date
              AND (change_ratio IS NULL OR (change_ratio >= -100 AND change_ratio <= 100))
              AND (after_ratio IS NULL OR (after_ratio >= 0 AND after_ratio <= 100))
              AND (begin_date IS NULL OR close_date IS NULL OR close_date >= begin_date)
        )
        SELECT DISTINCT EXTRACT(YEAR FROM ann_date)::int AS year
        FROM admissible
        ORDER BY year
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to build shareholder admissible years: {error}"))?;
    let admissible_observed_year_set: BTreeSet<i32> =
        admissible_observed_years.iter().copied().collect();
    let admissible_missing_years: Vec<i32> = requested_years
        .iter()
        .copied()
        .filter(|year| !admissible_observed_year_set.contains(year))
        .collect();
    let admissible_missing_year_count = admissible_missing_years.len();
    let decision = decide_shareholder_structure_coverage_audit(
        schema_passed,
        raw_summary.0,
        symbol_coverage_ratio,
        raw_summary.4,
        duplicate_source_row_hash_count,
        raw_summary.5 + raw_summary.6 + raw_summary.7,
        missing_year_count as i64,
    );
    let strict_low_fanout_gate = decide_shareholder_structure_strict_low_fanout_gate(
        schema_passed,
        admissible_summary.0,
        admissible_symbol_coverage_ratio,
        admissible_summary.5,
        admissible_missing_year_count as i64,
    );

    Ok(json!({
        "audit_version": "p3.21d-shareholder-structure-coverage-audit-v1",
        "source_id": "shareholder_structure",
        "mode": "read_only_year_source_symbol_ann_date_coverage_duplicate_audit",
        "date_range": {
            "start_date": phase7_date_json(start),
            "end_date": phase7_date_json(end),
        },
        "schema_passed": schema_passed,
        "raw_summary": {
            "raw_rows": raw_summary.0,
            "symbols": raw_summary.1,
            "min_ann_date": phase7_date_json(raw_summary.2),
            "max_ann_date": phase7_date_json(raw_summary.3),
            "pit_violation_rows": raw_summary.4,
            "ratio_out_of_range_count": raw_summary.5,
            "interval_violation_count": raw_summary.6,
            "holder_number_missing_value_count": raw_summary.7,
            "data_quality_violation_rows": raw_summary.5 + raw_summary.6 + raw_summary.7,
            "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        },
        "strict_low_fanout_admissible_summary": {
            "gate_id": SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
            "source_scope": "holder_number_and_holder_trade_only",
            "row_filter": {
                "holder_number": "available_at >= ann_date AND available_at >= end_date AND holder_num > 0",
                "holder_trade": "available_at >= ann_date AND change_ratio BETWEEN -100 AND 100 when present AND after_ratio BETWEEN 0 AND 100 when present AND close_date >= begin_date when both present",
                "top10_sources": "excluded_until_separate_high_fanout_coverage_pit_audit"
            },
            "admissible_rows": admissible_summary.0,
            "excluded_rows": raw_summary.0 - admissible_summary.0,
            "symbols": admissible_summary.1,
            "min_ann_date": phase7_date_json(admissible_summary.2),
            "max_ann_date": phase7_date_json(admissible_summary.3),
            "observed_year_count": admissible_summary.4,
            "duplicate_source_row_hash_count": admissible_summary.5,
            "symbol_breadth_vs_main_chinext_non_st": {
                "reference_symbols": admissible_symbol_breadth.0,
                "covered_symbols": admissible_symbol_breadth.1,
                "coverage_ratio": admissible_symbol_coverage_ratio,
            },
            "requested_year_coverage": {
                "requested_years": requested_years.clone(),
                "observed_years": admissible_observed_years,
                "missing_years": admissible_missing_years,
                "missing_year_count": admissible_missing_year_count,
            },
            "excluded_by_policy": {
                "period_snapshot_pit_rows": raw_summary.4,
                "ratio_out_of_range_rows": raw_summary.5,
                "interval_violation_rows": raw_summary.6,
                "holder_number_missing_value_rows": raw_summary.7,
            },
            "decision": strict_low_fanout_gate,
        },
        "symbol_breadth_vs_main_chinext_non_st": {
            "reference_symbols": symbol_breadth.0,
            "covered_symbols": symbol_breadth.1,
            "coverage_ratio": symbol_coverage_ratio,
        },
        "requested_year_coverage": {
            "requested_years": requested_years,
            "observed_years": observed_years.into_iter().collect::<Vec<_>>(),
            "missing_years": missing_years,
            "missing_year_count": missing_year_count,
        },
        "year_breakdown": year_breakdown
            .into_iter()
            .map(|(source, year, rows, symbols, min_date, max_date, pit_violation_rows)| json!({
                "source": source,
                "year": year,
                "rows": rows,
                "symbols": symbols,
                "min_ann_date": phase7_date_json(min_date),
                "max_ann_date": phase7_date_json(max_date),
                "pit_violation_rows": pit_violation_rows,
            }))
            .collect::<Vec<_>>(),
        "sync_attempt_breakdown": attempt_breakdown
            .into_iter()
            .map(|(source, attempts, completed_attempts, failed_attempts)| json!({
                "source": source,
                "attempts": attempts,
                "completed_attempts": completed_attempts,
                "failed_attempts": failed_attempts,
            }))
            .collect::<Vec<_>>(),
        "decision": decision,
        "promotion_gate": {
            "factor_builder": "blocked_until_coverage_readiness_and_pit_pass",
            "p310_status": decision["p310_status"].clone(),
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    }))
}



pub(crate) fn futures_price_chain_normalize_raw_product_symbol(raw: &str) -> Option<String> {
    let mut product = raw.trim().to_ascii_uppercase();
    if product.is_empty() {
        return None;
    }
    if product == "PTA" {
        return Some("TA".to_string());
    }
    if product.len() > 4 && product.ends_with("ACTV") {
        product.truncate(product.len() - 4);
    } else if product.len() > 2 && product.ends_with('L') {
        product.pop();
    }
    (!product.is_empty()).then_some(product)
}


fn futures_price_chain_mapping_product_summary_sql() -> &'static str {
    r#"
    SELECT
        upper(trim(product_symbol)) AS product_symbol,
        COUNT(*)::bigint AS mapping_rows,
        string_agg(DISTINCT exposure_type, ',' ORDER BY exposure_type) AS exposure_types,
        string_agg(DISTINCT mapping_version, ',' ORDER BY mapping_version) AS mapping_versions,
        MIN(valid_from) AS min_valid_from,
        MAX(valid_to) AS max_valid_to,
        MIN(available_at) AS min_available_at,
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
    GROUP BY upper(trim(product_symbol))
    ORDER BY product_symbol
    "#
}



fn futures_price_chain_exclusion_product_summary_sql() -> &'static str {
    r#"
    SELECT
        upper(trim(product_symbol)) AS product_symbol,
        COUNT(*)::bigint AS exclusion_rows,
        string_agg(DISTINCT reason_code, ',' ORDER BY reason_code) AS reason_codes,
        string_agg(DISTINCT gate_version, ',' ORDER BY gate_version) AS gate_versions,
        MIN(valid_from) AS min_valid_from,
        MAX(valid_to) AS max_valid_to,
        MIN(available_at) AS min_available_at,
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
    GROUP BY upper(trim(product_symbol))
    ORDER BY product_symbol
    "#
}



pub(crate) fn parse_futures_price_chain_mapping_date(raw: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(raw, "%Y%m%d"))
        .map_err(|_| "date_must_be_yyyy_mm_dd_or_yyyymmdd".to_string())
}



fn parse_futures_price_chain_coverage_date(
    value: Option<&str>,
    field: &str,
) -> Result<Option<NaiveDate>, String> {
    value
        .map(|raw| {
            parse_futures_price_chain_mapping_date(raw)
                .map_err(|message| format!("{field}_{message}"))
        })
        .transpose()
}



fn futures_price_chain_product_set_from_audit(audit: &Value, key: &str) -> BTreeSet<String> {
    audit
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| row.get("product_symbol").and_then(Value::as_str))
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect()
}



fn futures_price_chain_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            "market_futures_daily",
            vec![
                "market_futures_daily_pkey",
                "market_futures_daily_pit_available_at_check",
            ],
        ),
        (
            "market_futures_warehouse_receipt",
            vec![
                "market_futures_warehouse_receipt_pkey",
                "market_futures_wsr_pit_available_at_check",
            ],
        ),
        (
            "market_futures_holding_rank",
            vec![
                "market_futures_holding_rank_pkey",
                "market_futures_holding_pit_available_at_check",
            ],
        ),
        (
            "market_futures_product_exposure_mapping_pit",
            vec![
                "market_futures_product_exposure_mapping_pit_pkey",
                "market_futures_product_exposure_direction_check",
                "market_futures_product_exposure_weight_check",
                "market_futures_product_exposure_type_check",
                "market_futures_product_exposure_interval_check",
            ],
        ),
        (
            "market_futures_product_exclusion_gate_pit",
            vec![
                "market_futures_product_exclusion_gate_pit_pkey",
                "market_futures_product_exclusion_scope_check",
                "market_futures_product_exclusion_reason_check",
                "market_futures_product_exclusion_interval_check",
            ],
        ),
        (
            "market_futures_product_signal_pit",
            vec![
                "market_futures_product_signal_pit_pkey",
                "market_futures_product_signal_available_at_check",
                "market_futures_product_signal_code_check",
            ],
        ),
    ]
}



pub(crate) async fn build_futures_price_chain_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
    let mut table_results = Vec::new();
    let mut schema_passed = true;
    let mut raw_rows = 0_i64;
    let mut mapping_rows = 0_i64;

    for (table, required_constraints) in futures_price_chain_expected_schema() {
        let regclass_name = format!("public.{table}");
        let table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1)::text IS NOT NULL")
            .bind(&regclass_name)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to inspect {table}: {error}"))?;

        let row_count = if table_exists {
            let sql = format!("SELECT COUNT(*)::bigint FROM {table}");
            sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(db)
                .await
                .map_err(|error| format!("Failed to count {table}: {error}"))?
        } else {
            0
        };

        let constraints: Vec<String> = if table_exists {
            sqlx::query_scalar(
                r#"
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = to_regclass($1)
                ORDER BY conname
                "#,
            )
            .bind(&regclass_name)
            .fetch_all(db)
            .await
            .map_err(|error| format!("Failed to inspect constraints for {table}: {error}"))?
        } else {
            Vec::new()
        };

        let missing_constraints: Vec<&str> = required_constraints
            .iter()
            .copied()
            .filter(|constraint| !constraints.iter().any(|existing| existing == constraint))
            .collect();

        let passed = table_exists && missing_constraints.is_empty();
        schema_passed &= passed;
        if table == "market_futures_product_exposure_mapping_pit" {
            mapping_rows = row_count;
        } else if table == "market_futures_daily"
            || table == "market_futures_warehouse_receipt"
            || table == "market_futures_holding_rank"
        {
            raw_rows += row_count;
        }

        table_results.push(json!({
            "table": table,
            "table_exists": table_exists,
            "row_count": row_count,
            "required_constraints": required_constraints,
            "missing_constraints": missing_constraints,
            "passed": passed,
        }));
    }

    let decision = decide_futures_price_chain_readiness(schema_passed, raw_rows, mapping_rows);

    Ok(json!({
        "audit_version": "p3.19k-futures-price-chain-readiness-v1",
        "source_id": "futures_price_chain",
        "mode": "read_only_schema_table_constraint_rowcount_audit",
        "schema_passed": schema_passed,
        "tables": table_results,
        "decision": decision,
        "pit_policy": {
            "required_raw_fields": ["trade_date", "available_at", "source_published_at"],
            "raw_table_check": "available_at >= trade_date",
            "downstream_filter": "source.available_at <= stock_trade_date",
            "intraday_rule": "use_previous_available_futures_trade_date_until_source_published_at_is_audited"
        },
        "prohibited": [
            "factor_backfill_before_mapping_available_at_audit",
            "p310_before_coverage_readiness",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



pub(crate) async fn build_futures_price_chain_mapping_audit(db: &sqlx::PgPool) -> Result<Value, String> {
    let readiness = build_futures_price_chain_readiness_audit(db).await?;
    let schema_passed = readiness
        .get("schema_passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if !schema_passed {
        let decision = decide_futures_price_chain_mapping_audit(false, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        return Ok(json!({
            "audit_version": "p3.19m-futures-price-chain-mapping-audit-v1",
            "source_id": "futures_price_chain",
            "mode": "read_only_product_mapping_pit_coverage_audit",
            "schema_passed": false,
            "decision": decision,
            "readiness_audit": readiness,
            "prohibited": [
                "mapping_backfill_before_schema_passes",
                "factor_backfill_before_mapping_available_at_audit",
                "p310_before_mapping_and_coverage_readiness",
                "bounded_wfa_or_v19_train_selection_before_p310_passes"
            ]
        }));
    }

    let raw_product_rows = sqlx::query_as::<
        _,
        (
            String,
            i64,
            String,
            String,
            NaiveDate,
            NaiveDate,
            NaiveDate,
            NaiveDate,
            i64,
            i64,
        ),
    >(futures_price_chain_raw_product_summary_sql())
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures raw products: {error}"))?;

    let endpoint_rows = sqlx::query_as::<
        _,
        (
            String,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
        ),
    >(futures_price_chain_raw_endpoint_breakdown_sql())
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures endpoint breakdown: {error}"))?;

    let mapping_summary = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
        futures_price_chain_mapping_summary_sql(),
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to audit futures mapping summary: {error}"))?;

    let mapping_product_rows = sqlx::query_as::<
        _,
        (
            String,
            i64,
            String,
            String,
            NaiveDate,
            Option<NaiveDate>,
            NaiveDate,
            i64,
            i64,
            i64,
        ),
    >(futures_price_chain_mapping_product_summary_sql())
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures mapping products: {error}"))?;

    let exclusion_summary =
        sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(futures_price_chain_exclusion_summary_sql())
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to audit futures exclusion summary: {error}"))?;

    let exclusion_product_rows = sqlx::query_as::<
        _,
        (
            String,
            i64,
            String,
            String,
            NaiveDate,
            Option<NaiveDate>,
            NaiveDate,
            i64,
            i64,
            i64,
        ),
    >(futures_price_chain_exclusion_product_summary_sql())
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures exclusion products: {error}"))?;

    let raw_products: BTreeSet<String> = raw_product_rows.iter().map(|row| row.0.clone()).collect();
    let mapping_products: BTreeSet<String> = mapping_product_rows
        .iter()
        .map(|row| row.0.clone())
        .collect();
    let exclusion_products: BTreeSet<String> = exclusion_product_rows
        .iter()
        .map(|row| row.0.clone())
        .collect();
    let covered_products = mapping_products
        .union(&exclusion_products)
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing_products = raw_products
        .difference(&covered_products)
        .cloned()
        .collect::<Vec<_>>();
    let mapped_raw_product_count = raw_products.intersection(&mapping_products).count() as i64;
    let excluded_raw_product_count = raw_products.intersection(&exclusion_products).count() as i64;
    let raw_product_count = raw_products.len() as i64;
    let raw_rows = raw_product_rows.iter().map(|row| row.1).sum::<i64>();
    let raw_pit_violation_rows = raw_product_rows.iter().map(|row| row.8).sum::<i64>();
    let raw_missing_source_published_at_rows =
        raw_product_rows.iter().map(|row| row.9).sum::<i64>();

    let raw_products_json = raw_product_rows
        .iter()
        .map(|row| {
            json!({
                "product_symbol": row.0,
                "raw_rows": row.1,
                "endpoints": row.2.split(',').collect::<Vec<_>>(),
                "exchanges": row.3.split(',').collect::<Vec<_>>(),
                "min_trade_date": row.4,
                "max_trade_date": row.5,
                "min_available_at": row.6,
                "max_available_at": row.7,
                "raw_pit_violation_rows": row.8,
                "missing_source_published_at_rows": row.9,
                "mapping_status": if mapping_products.contains(&row.0) {
                    "mapped"
                } else if exclusion_products.contains(&row.0) {
                    "excluded"
                } else {
                    "missing"
                },
            })
        })
        .collect::<Vec<_>>();

    let endpoint_breakdown = endpoint_rows
        .iter()
        .map(|row| {
            json!({
                "endpoint": row.0,
                "raw_rows": row.1,
                "product_count": row.2,
                "trade_date_count": row.3,
                "min_trade_date": row.4,
                "max_trade_date": row.5,
                "raw_pit_violation_rows": row.6,
                "missing_source_published_at_rows": row.7,
            })
        })
        .collect::<Vec<_>>();

    let mapping_products_json = mapping_product_rows
        .iter()
        .map(|row| {
            json!({
                "product_symbol": row.0,
                "mapping_rows": row.1,
                "exposure_types": row.2.split(',').collect::<Vec<_>>(),
                "mapping_versions": row.3.split(',').collect::<Vec<_>>(),
                "min_valid_from": row.4,
                "max_valid_to": row.5,
                "min_available_at": row.6,
                "invalid_interval_rows": row.7,
                "mapping_pit_violation_rows": row.8,
                "weak_evidence_rows": row.9,
                "raw_status": if raw_products.contains(&row.0) { "raw_product_present" } else { "mapping_without_current_raw_product" },
            })
        })
        .collect::<Vec<_>>();

    let exclusion_products_json = exclusion_product_rows
        .iter()
        .map(|row| {
            json!({
                "product_symbol": row.0,
                "exclusion_rows": row.1,
                "reason_codes": row.2.split(',').collect::<Vec<_>>(),
                "gate_versions": row.3.split(',').collect::<Vec<_>>(),
                "min_valid_from": row.4,
                "max_valid_to": row.5,
                "min_available_at": row.6,
                "invalid_interval_rows": row.7,
                "exclusion_pit_violation_rows": row.8,
                "weak_evidence_rows": row.9,
                "raw_status": if raw_products.contains(&row.0) { "raw_product_present" } else { "exclusion_without_current_raw_product" },
            })
        })
        .collect::<Vec<_>>();

    let decision = decide_futures_price_chain_mapping_audit(
        schema_passed,
        raw_product_count,
        mapped_raw_product_count,
        excluded_raw_product_count,
        missing_products.len() as i64,
        mapping_summary.3,
        mapping_summary.4,
        mapping_summary.2,
        exclusion_summary.2,
        exclusion_summary.3,
    );

    Ok(json!({
        "audit_version": "p3.19m-futures-price-chain-mapping-audit-v1",
        "source_id": "futures_price_chain",
        "mode": "read_only_product_mapping_pit_coverage_audit",
        "schema_passed": schema_passed,
        "raw_rows": raw_rows,
        "raw_product_count": raw_product_count,
        "raw_pit_violation_rows": raw_pit_violation_rows,
        "raw_missing_source_published_at_rows": raw_missing_source_published_at_rows,
        "mapping_rows": mapping_summary.0,
        "mapping_table_product_count": mapping_summary.1,
        "mapped_raw_product_count": mapped_raw_product_count,
        "exclusion_rows": exclusion_summary.0,
        "exclusion_table_product_count": exclusion_summary.1,
        "excluded_raw_product_count": excluded_raw_product_count,
        "missing_product_count": missing_products.len(),
        "missing_products": missing_products,
        "unsupported_exposure_rows": mapping_summary.2,
        "invalid_interval_rows": mapping_summary.3,
        "mapping_pit_violation_rows": mapping_summary.4,
        "weak_evidence_rows": mapping_summary.5,
        "invalid_exclusion_interval_rows": exclusion_summary.2,
        "exclusion_pit_violation_rows": exclusion_summary.3,
        "weak_exclusion_evidence_rows": exclusion_summary.4,
        "endpoint_breakdown": endpoint_breakdown,
        "raw_products": raw_products_json,
        "mapping_products": mapping_products_json,
        "exclusion_products": exclusion_products_json,
        "decision": decision,
        "readiness_audit": readiness,
        "pit_policy": {
            "mapping_filter": "mapping.available_at <= stock_trade_date AND mapping.valid_from <= stock_trade_date AND (mapping.valid_to IS NULL OR mapping.valid_to >= stock_trade_date)",
            "exclusion_filter": "exclusion gates remove non-industry products from this alpha source; they must not be transformed into synthetic industry mappings",
            "preferred_first_pass": "product_symbol_to_sw_industry_then_join_market_stock_industry_membership_pit",
            "direct_stock_mapping_requires": "strong_external_evidence_available_at_and_versioned_weight",
            "intraday_rule": "stock intraday rebalance must use previous available futures trade_date until source_published_at audit proves earlier availability"
        },
        "promotion_gate": {
            "p310_status": "blocked_until_mapping_and_coverage_readiness_pass",
            "wfa_status": "blocked",
            "v19_train_selection": "blocked"
        },
        "prohibited": [
            "static_hindsight_product_to_stock_mapping",
            "mapping_weights_derived_from_future_returns_or_future_factor_performance",
            "factor_backfill_before_mapping_available_at_audit",
            "p310_before_year_product_exchange_market_scope_coverage_readiness",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



async fn build_futures_price_chain_coverage_audit(
    db: &sqlx::PgPool,
    req: FuturesPriceChainCoverageAuditReq,
) -> Result<Value, String> {
    let start = parse_futures_price_chain_coverage_date(req.start_date.as_deref(), "start_date")?;
    let end = parse_futures_price_chain_coverage_date(req.end_date.as_deref(), "end_date")?;
    if let (Some(start), Some(end)) = (start, end) {
        if start > end {
            return Err("futures_price_chain coverage start_date cannot be after end_date".into());
        }
    }

    let mapping_audit = build_futures_price_chain_mapping_audit(db).await?;
    let schema_passed = mapping_audit
        .get("schema_passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !schema_passed {
        let decision = decide_futures_price_chain_coverage_audit(false, 0, 0, 0, 0, 0, 0);
        return Ok(json!({
            "audit_version": "p3.19p-futures-price-chain-coverage-audit-v1",
            "source_id": "futures_price_chain",
            "mode": "read_only_full_history_bounded_sync_coverage_audit",
            "schema_passed": false,
            "requested_range": {
                "start_date": start,
                "end_date": end,
            },
            "decision": decision,
            "mapping_audit": mapping_audit,
            "prohibited": [
                "raw_factor_backfill_before_schema_passes",
                "p310_before_coverage_mapping_and_pit_readiness",
                "bounded_wfa_or_v19_train_selection_before_p310_passes"
            ]
        }));
    }

    let coverage_rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            String,
            String,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
        ),
    >(futures_price_chain_coverage_breakdown_sql())
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures price-chain coverage: {error}"))?;

    let sync_attempt_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
            i64,
        ),
    >(futures_price_chain_sync_attempt_breakdown_sql())
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to audit futures price-chain sync attempts: {error}"))?;

    let mapped_products =
        futures_price_chain_product_set_from_audit(&mapping_audit, "mapping_products");
    let excluded_products =
        futures_price_chain_product_set_from_audit(&mapping_audit, "exclusion_products");
    let covered_products = mapped_products
        .union(&excluded_products)
        .cloned()
        .collect::<BTreeSet<_>>();
    let raw_products = coverage_rows
        .iter()
        .map(|row| row.2.clone())
        .collect::<BTreeSet<_>>();
    let missing_products = raw_products
        .difference(&covered_products)
        .cloned()
        .collect::<Vec<_>>();
    let covered_product_count = raw_products.intersection(&covered_products).count() as i64;
    let raw_rows = coverage_rows.iter().map(|row| row.4).sum::<i64>();
    let raw_pit_violation_rows = coverage_rows.iter().map(|row| row.8).sum::<i64>();
    let missing_source_published_at_rows = coverage_rows.iter().map(|row| row.9).sum::<i64>();
    let failed_sync_attempt_count = sync_attempt_rows
        .iter()
        .filter(|row| row.1 == "failed")
        .map(|row| row.2)
        .sum::<i64>();
    let non_open_sync_attempt_count = sync_attempt_rows.iter().map(|row| row.7).sum::<i64>();
    let non_open_sync_attempt_rows = sync_attempt_rows.iter().map(|row| row.8).sum::<i64>();

    let mut endpoint_year_summary: BTreeMap<
        (i32, String),
        (i64, i64, i64, BTreeSet<String>, BTreeSet<String>),
    > = BTreeMap::new();
    for row in &coverage_rows {
        let entry = endpoint_year_summary
            .entry((row.1, row.0.clone()))
            .or_insert_with(|| (0, 0, 0, BTreeSet::new(), BTreeSet::new()));
        entry.0 += row.4;
        entry.1 += row.8;
        entry.2 += row.9;
        entry.3.insert(row.2.clone());
        entry.4.insert(row.3.clone());
    }

    let endpoint_year_breakdown = endpoint_year_summary
        .into_iter()
        .map(|((trade_year, endpoint), summary)| {
            json!({
                "trade_year": trade_year,
                "endpoint": endpoint,
                "raw_rows": summary.0,
                "raw_pit_violation_rows": summary.1,
                "missing_source_published_at_rows": summary.2,
                "product_count": summary.3.len(),
                "exchange_count": summary.4.len(),
                "products": summary.3.into_iter().collect::<Vec<_>>(),
                "exchanges": summary.4.into_iter().collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let coverage_breakdown = coverage_rows
        .iter()
        .map(|row| {
            let mapping_status = if mapped_products.contains(&row.2) {
                "mapped"
            } else if excluded_products.contains(&row.2) {
                "excluded"
            } else {
                "missing"
            };
            json!({
                "endpoint": row.0,
                "trade_year": row.1,
                "product_symbol": row.2,
                "exchange": row.3,
                "raw_rows": row.4,
                "trade_date_count": row.5,
                "min_trade_date": row.6,
                "max_trade_date": row.7,
                "raw_pit_violation_rows": row.8,
                "missing_source_published_at_rows": row.9,
                "mapping_status": mapping_status,
            })
        })
        .collect::<Vec<_>>();

    let sync_attempt_breakdown = sync_attempt_rows
        .iter()
        .map(|row| {
            json!({
                "source": row.0,
                "status": row.1,
                "attempt_count": row.2,
                "row_count": row.3,
                "min_start_date": row.4,
                "max_end_date": row.5,
                "error_attempt_count": row.6,
                "non_open_attempt_count": row.7,
                "non_open_row_count": row.8,
            })
        })
        .collect::<Vec<_>>();

    let decision = decide_futures_price_chain_coverage_audit(
        schema_passed,
        raw_rows,
        raw_products.len() as i64,
        covered_product_count,
        missing_products.len() as i64,
        raw_pit_violation_rows,
        failed_sync_attempt_count,
    );

    Ok(json!({
        "audit_version": "p3.19p-futures-price-chain-coverage-audit-v1",
        "source_id": "futures_price_chain",
        "mode": "read_only_full_history_bounded_sync_coverage_audit",
        "schema_passed": schema_passed,
        "requested_range": {
            "start_date": start,
            "end_date": end,
        },
        "raw_rows": raw_rows,
        "raw_product_count": raw_products.len(),
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_products.len(),
        "missing_products": missing_products,
        "raw_pit_violation_rows": raw_pit_violation_rows,
        "missing_source_published_at_rows": missing_source_published_at_rows,
        "failed_sync_attempt_count": failed_sync_attempt_count,
        "non_open_sync_attempt_count": non_open_sync_attempt_count,
        "non_open_sync_attempt_rows": non_open_sync_attempt_rows,
        "endpoint_year_breakdown": endpoint_year_breakdown,
        "coverage_breakdown": coverage_breakdown,
        "sync_attempt_breakdown": sync_attempt_breakdown,
        "mapping_audit_decision": mapping_audit.get("decision").cloned().unwrap_or(Value::Null),
        "decision": decision,
        "pit_policy": {
            "raw_available_at_policy": "available_at = trade_date + 1 day until source publication timing is audited",
            "intraday_stock_decision_rule": "use previous available futures trade_date for intraday stock rebalance",
            "source_published_at_null_policy": "null publication timestamps are conservative raw rows and block same-day intraday use"
        },
        "promotion_gate": futures_price_chain_coverage_promotion_gate(&decision),
        "prohibited": [
            "factor_builder_before_all_raw_products_are_mapped_or_excluded",
            "p310_before_year_endpoint_product_exchange_coverage_audit_passes",
            "bounded_wfa_or_v19_train_selection_before_p310_passes"
        ]
    }))
}



async fn build_futures_price_chain_mapping_template(db: &sqlx::PgPool) -> Result<Value, String> {
    let mapping_audit = build_futures_price_chain_mapping_audit(db).await?;
    let raw_products = mapping_audit
        .get("raw_products")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let exclusion_products = mapping_audit
        .get("exclusion_products")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let excluded_product_symbols = exclusion_products
        .iter()
        .filter_map(|row| row.get("product_symbol").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();

    let industry_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            i64,
            NaiveDate,
            NaiveDate,
            NaiveDate,
        ),
    >(futures_price_chain_industry_targets_sql())
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load SW2021 L1 industry targets: {error}"))?;

    let sw2021_targets = industry_rows
        .iter()
        .map(|row| {
            json!({
                "exposure_code": row.0,
                "index_code": row.0,
                "index_name": row.1,
                "industry_code": row.2,
                "industry_name": row.3,
                "current_or_historical_symbols": row.4,
                "min_in_date": row.5,
                "max_in_date": row.6,
                "latest_available_at": row.7,
            })
        })
        .collect::<Vec<_>>();

    let candidate_rows = raw_products
        .iter()
        .filter(|product| {
            product
                .get("product_symbol")
                .and_then(Value::as_str)
                .map(|symbol| !excluded_product_symbols.contains(symbol))
                .unwrap_or(false)
        })
        .map(|product| {
            let product_symbol = product
                .get("product_symbol")
                .and_then(Value::as_str)
                .unwrap_or_default();
            json!({
                "product_symbol": product_symbol,
                "exposure_type": "sw_industry",
                "exposure_code": null,
                "direction": 1,
                "weight": 1.0,
                "valid_from": null,
                "valid_to": null,
                "available_at": null,
                "source": null,
                "mapping_version": "p319n-product-sw2021-l1-v1",
                "evidence": {
                    "source_url": null,
                    "source_document": null,
                    "review_note": null
                },
                "raw_product_summary": product,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "audit_version": "p3.19n-futures-price-chain-mapping-template-v1",
        "source_id": "futures_price_chain",
        "mode": "read_only_mapping_template_no_write",
        "write_enabled": false,
        "mapping_audit_decision": mapping_audit.get("decision").cloned().unwrap_or_else(|| json!({})),
        "raw_product_count": mapping_audit.get("raw_product_count").cloned().unwrap_or_else(|| json!(0)),
        "excluded_product_count": mapping_audit.get("excluded_raw_product_count").cloned().unwrap_or_else(|| json!(0)),
        "mapping_candidate_product_count": candidate_rows.len(),
        "exclusion_products": exclusion_products,
        "target_universe": {
            "classification_source": "SW2021",
            "industry_level": "L1",
            "exposure_type": "sw_industry",
            "exposure_code_field": "index_code",
            "target_count": sw2021_targets.len(),
            "targets": sw2021_targets,
        },
        "required_candidate_fields": [
            "product_symbol",
            "exposure_type",
            "exposure_code",
            "direction",
            "weight",
            "valid_from",
            "valid_to",
            "available_at",
            "source",
            "mapping_version",
            "evidence"
        ],
        "candidate_rows": candidate_rows,
        "guardrails": [
            "template generation is read-only and does not insert mapping rows",
            "first pass accepts only sw_industry exposure_code using SW2021 L1 index_code",
            "direct stock_symbol mapping requires a separate stronger evidence gate",
            "products listed in exclusion_products are deliberately not emitted as mapping candidates",
            "candidate rows must pass mapping-validate before any manual insert",
            "validated candidates still do not unlock factor backfill, P3.10, WFA, or v19 train selection"
        ],
    }))
}



async fn validate_futures_price_chain_mapping_candidates(
    db: &sqlx::PgPool,
    req: FuturesPriceChainMappingValidateReq,
) -> Result<Value, String> {
    let template = build_futures_price_chain_mapping_template(db).await?;
    let raw_products = template
        .get("candidate_rows")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("product_symbol").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let sw2021_targets = template
        .get("target_universe")
        .and_then(|target| target.get("targets"))
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("exposure_code").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();

    let validations = req
        .rows
        .iter()
        .map(|candidate| {
            validate_futures_price_chain_mapping_candidate(
                candidate,
                &raw_products,
                &sw2021_targets,
            )
        })
        .collect::<Vec<_>>();
    let covered_products = validations
        .iter()
        .filter(|validation| validation.passed)
        .map(|validation| validation.product_symbol.clone())
        .collect::<BTreeSet<_>>();
    let missing_products = raw_products
        .difference(&covered_products)
        .cloned()
        .collect::<Vec<_>>();
    let invalid_row_count = validations
        .iter()
        .filter(|validation| !validation.passed)
        .count() as i64;
    let decision = decide_futures_price_chain_mapping_candidate_validation(
        raw_products.len() as i64,
        covered_products.len() as i64,
        invalid_row_count,
        missing_products.len() as i64,
    );

    Ok(json!({
        "audit_version": "p3.19n-futures-price-chain-mapping-validate-v1",
        "source_id": "futures_price_chain",
        "mode": "read_only_candidate_validation_no_write",
        "write_enabled": false,
        "candidate_row_count": req.rows.len(),
        "valid_row_count": validations.len() as i64 - invalid_row_count,
        "invalid_row_count": invalid_row_count,
        "covered_product_count": covered_products.len(),
        "missing_product_count": missing_products.len(),
        "missing_products": missing_products,
        "row_results": validations,
        "decision": decision,
        "guardrails": [
            "this endpoint validates candidate mapping rows only and never writes market_futures_product_exposure_mapping_pit",
            "manual insert is allowed only after evidence review and must preserve mapping_version/source/evidence",
            "mapping insert still requires a follow-up mapping-audit and coverage/readiness audit before P3.10"
        ],
    }))
}



pub(crate) fn apply_p319_futures_price_chain_readiness(admission: &mut Value, readiness: &Value) {
    let Some(decision) = readiness.get("decision") else {
        return;
    };
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("coverage_readiness_audit_required_before_p310");
    let sync_status = decision
        .get("sync_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("run_futures_price_chain_readiness_audit");
    let schema_status = decision
        .get("schema_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let raw_rows = decision
        .get("raw_rows")
        .cloned()
        .unwrap_or_else(|| json!(0));
    let mapping_rows = decision
        .get("mapping_rows")
        .cloned()
        .unwrap_or_else(|| json!(0));

    let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(operations) = candidates.iter_mut().find(|candidate| {
        candidate
            .get("source_id")
            .and_then(Value::as_str)
            .map(|source| source == "futures_price_chain")
            .unwrap_or(false)
    }) else {
        return;
    };
    let Some(object) = operations.as_object_mut() else {
        return;
    };

    let data_gate_still_blocks = matches!(
        admission_decision,
        "apply_schema_before_mapping_audit"
            | "raw_sync_required_before_mapping_audit"
            | "mapping_required_before_feature_or_p310"
            | "mapping_integrity_failed_before_feature_or_p310"
            | "mapping_coverage_incomplete_before_feature_or_p310"
            | "all_products_excluded_no_trainable_price_chain_source"
            | "apply_schema_before_coverage_audit"
            | "raw_sync_required_before_coverage_audit"
            | "raw_pit_integrity_failed_before_feature_or_p310"
            | "sync_attempt_failures_require_retry_before_feature_or_p310"
    );
    if !data_gate_still_blocks {
        object.insert(
            "futures_price_chain_readiness".to_string(),
            readiness.clone(),
        );
        if let Some(evidence) = object
            .get_mut("source_discovery_evidence")
            .and_then(Value::as_array_mut)
            .and_then(|items| {
                items.iter_mut().find(|item| {
                    item.get("candidate")
                        .and_then(Value::as_str)
                        .map(|candidate| candidate == "tushare:futures_price_chain")
                        .unwrap_or(false)
                })
            })
            .and_then(Value::as_object_mut)
        {
            evidence.insert("readiness_decision".to_string(), decision.clone());
        }
        return;
    }

    object.insert("admission_decision".to_string(), json!(admission_decision));
    object.insert("sync_status".to_string(), json!(sync_status));
    object.insert(
        "schema_status".to_string(),
        json!(format!(
            "fina_mainbz_raw_source_schema_available_futures_price_chain_{schema_status}"
        )),
    );
    object.insert(
        "coverage_status".to_string(),
        json!("fina_mainbz_pit_green_failed_economics_futures_price_chain_raw_sync_mapping_and_coverage_required"),
    );
    object.insert(
        "p310_status".to_string(),
        decision
            .get("p310_status")
            .cloned()
            .unwrap_or_else(|| json!("blocked_until_coverage_readiness_passes")),
    );
    object.insert(
        "wfa_status".to_string(),
        decision
            .get("wfa_status")
            .cloned()
            .unwrap_or_else(|| json!("blocked_until_mapping_and_p310_pass")),
    );
    object.insert(
        "v19_train_selection".to_string(),
        decision
            .get("v19_train_selection")
            .cloned()
            .unwrap_or_else(|| json!("blocked")),
    );
    object.insert("next_step".to_string(), json!(next_step));
    object.insert(
        "blocked_reason".to_string(),
        json!("fina_mainbz_data_pit_coverage_green_but_economics_failed; futures_price_chain_raw_sync_started_but_mapping_publication_timing_full_history_coverage_audit_and_p310_are_not_done"),
    );
    object.insert(
        "futures_price_chain_readiness".to_string(),
        readiness.clone(),
    );

    if let Some(evidence) = object
        .get_mut("source_discovery_evidence")
        .and_then(Value::as_array_mut)
        .and_then(|items| {
            items.iter_mut().find(|item| {
                item.get("candidate")
                    .and_then(Value::as_str)
                    .map(|candidate| candidate == "tushare:futures_price_chain")
                    .unwrap_or(false)
            })
        })
        .and_then(Value::as_object_mut)
    {
        evidence.insert("status".to_string(), json!(sync_status));
        evidence.insert("decision".to_string(), json!(admission_decision));
        evidence.insert("raw_rows".to_string(), raw_rows);
        evidence.insert("mapping_rows".to_string(), mapping_rows);
        evidence.insert("readiness_decision".to_string(), decision.clone());
    }
}



pub(crate) fn phase7_block_trade_readiness(stats: &Phase7BlockTradeSourceAudit) -> &'static str {
    if stats.data_rows <= 0 || stats.latest_trade_date.is_none() {
        return "schema_and_client_ready_needs_bounded_sync";
    }
    if stats.pit_violation_rows > 0 {
        return "raw_source_pit_failed";
    }
    if stats.open_days_in_range >= 120
        && phase7_ratio(stats.covered_trade_days, stats.open_days_in_range).unwrap_or(0.0) >= 0.80
    {
        return "raw_source_ready_for_p310_diagnostics";
    }
    "bounded_sample_ready_needs_history_coverage"
}



pub(crate) fn equity_pledge_readiness_label(admission_decision: &str) -> &'static str {
    match admission_decision {
        "apply_schema_before_sync" => "schema_contract_ready_schema_not_applied",
        "bounded_sync_required_before_coverage_audit" => "schema_created_bounded_sync_required",
        "raw_pit_failed" => "raw_source_pit_failed",
        "coverage_readiness_audit_required_before_p310" => "raw_source_ready_for_coverage_audit",
        _ => "schema_contract_ready_review_required",
    }
}



pub(crate) fn phase7_industry_membership_readiness(
    stats: &Phase7IndustryMembershipSourceAudit,
) -> &'static str {
    if stats.data_rows <= 0 {
        return "industry_membership_schema_ready_needs_bounded_sync";
    }
    if stats.pit_violation_rows > 0
        || stats.invalid_interval_rows > 0
        || stats.duplicate_key_rows > 0
    {
        return "industry_membership_raw_source_pit_failed";
    }
    if phase7_ratio(
        stats.current_covered_stock_symbols,
        stats.current_active_stock_symbols,
    )
    .unwrap_or(0.0)
        < 0.95
    {
        return "industry_membership_current_coverage_undercovered";
    }
    "industry_membership_raw_source_ready_for_coverage_audit"
}



pub async fn phase7_feasibility_audit(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match build_phase7_feasibility_audit(&state).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/futures-price-chain/schema-contract


pub async fn futures_price_chain_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_futures_price_chain_schema_contract()}))
}

/// GET /api/v1/quant/data/equity-pledge-pressure/schema-contract


pub async fn equity_pledge_pressure_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_equity_pledge_schema_contract()}))
}

/// GET /api/v1/quant/data/margin-detail/schema-contract


pub async fn margin_detail_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_margin_detail_schema_contract()}))
}

/// GET /api/v1/quant/data/shareholder-structure/schema-contract


pub async fn shareholder_structure_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_shareholder_structure_schema_contract()}))
}

/// GET /api/v1/quant/data/exchange-announcement-order-capacity/schema-contract


pub async fn futures_price_chain_readiness_audit(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_futures_price_chain_readiness_audit(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/futures-price-chain/mapping-audit


pub async fn futures_price_chain_mapping_audit(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_futures_price_chain_mapping_audit(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/futures-price-chain/coverage-audit


pub async fn futures_price_chain_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<FuturesPriceChainCoverageAuditReq>,
) -> impl IntoResponse {
    match build_futures_price_chain_coverage_audit(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/futures-price-chain/mapping-template


pub async fn futures_price_chain_mapping_template(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_futures_price_chain_mapping_template(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/futures-price-chain/mapping-validate


pub async fn futures_price_chain_mapping_validate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FuturesPriceChainMappingValidateReq>,
) -> impl IntoResponse {
    match validate_futures_price_chain_mapping_candidates(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/futures-price-chain/sync


pub async fn futures_price_chain_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FuturesPriceChainSyncReq>,
) -> impl IntoResponse {
    let sync_req = req.into_sync_task_req();
    let task_id = sync_req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("futures-price-chain-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &sync_req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if sync_req.background {
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
                }
            },
        )
        .await;
        return Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "dataset": "futures_price_chain",
                "status": "running"
            }
        }));
    }

    match execute_sync_task(state.clone(), task_id.clone(), sync_req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => {
            let _ = quant_data::repository::fail_sync_task(&state.db, &task_id, &message).await;
            Json(json!({"code": 1, "message": message, "task_id": task_id}))
        }
    }
}

/// GET /api/v1/quant/data/shareholder-structure/readiness-audit


pub async fn shareholder_structure_readiness_audit(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_shareholder_structure_readiness_audit(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/margin-detail/readiness-audit


pub async fn margin_detail_readiness_audit(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_margin_detail_readiness_audit(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/margin-detail/coverage-audit


pub async fn margin_detail_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<MarginDetailCoverageAuditReq>,
) -> impl IntoResponse {
    match build_margin_detail_coverage_audit(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/margin-detail/sync-plan


pub async fn margin_detail_sync_plan(
    State(state): State<Arc<AppState>>,
    Query(req): Query<MarginDetailSyncPlanReq>,
) -> impl IntoResponse {
    match build_margin_detail_sync_plan(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/margin-detail/sync


pub async fn margin_detail_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MarginDetailSyncReq>,
) -> impl IntoResponse {
    let sync_req = req.into_sync_task_req();
    let task_id = sync_req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("margin-detail-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &sync_req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if sync_req.background {
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
                }
            },
        )
        .await;
        return Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "dataset": "margin_detail",
                "status": "running"
            }
        }));
    }

    match execute_sync_task(state.clone(), task_id.clone(), sync_req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => {
            let _ = quant_data::repository::fail_sync_task(&state.db, &task_id, &message).await;
            Json(json!({"code": 1, "message": message, "task_id": task_id}))
        }
    }
}

/// GET /api/v1/quant/data/shareholder-structure/coverage-audit


pub async fn shareholder_structure_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ShareholderStructureCoverageAuditReq>,
) -> impl IntoResponse {
    match build_shareholder_structure_coverage_audit(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/shareholder-structure/sync-plan


pub async fn shareholder_structure_sync_plan(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ShareholderStructureSyncPlanReq>,
) -> impl IntoResponse {
    match build_shareholder_structure_sync_plan(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/shareholder-structure/sync


pub async fn shareholder_structure_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShareholderStructureSyncReq>,
) -> impl IntoResponse {
    let sync_req = req.into_sync_task_req();
    let task_id = sync_req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("shareholder-structure-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &sync_req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if sync_req.background {
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
                }
            },
        )
        .await;
        return Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "dataset": "shareholder_structure",
                "status": "running"
            }
        }));
    }

    match execute_sync_task(state.clone(), task_id.clone(), sync_req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => {
            let _ = quant_data::repository::fail_sync_task(&state.db, &task_id, &message).await;
            Json(json!({"code": 1, "message": message, "task_id": task_id}))
        }
    }
}

/// GET /api/v1/quant/data/equity-pledge-pressure/readiness-audit


pub async fn equity_pledge_pressure_readiness_audit(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match build_equity_pledge_pressure_readiness_audit(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/equity-pledge-pressure/coverage-audit


pub async fn equity_pledge_pressure_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<EquityPledgeCoverageAuditReq>,
) -> impl IntoResponse {
    match build_equity_pledge_pressure_coverage_audit(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/equity-pledge-pressure/sync


pub async fn equity_pledge_pressure_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EquityPledgePressureSyncReq>,
) -> impl IntoResponse {
    let sync_req = req.into_sync_task_req();
    let task_id = sync_req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("equity-pledge-pressure-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &sync_req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if sync_req.background {
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
                }
            },
        )
        .await;
        return Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "dataset": "equity_pledge_pressure",
                "status": "running"
            }
        }));
    }

    match execute_sync_task(state.clone(), task_id.clone(), sync_req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => {
            let _ = quant_data::repository::fail_sync_task(&state.db, &task_id, &message).await;
            Json(json!({"code": 1, "message": message, "task_id": task_id}))
        }
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

/// POST /api/v1/quant/data/main-business/available-at-audit


pub async fn main_business_available_at_audit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MainBusinessAvailableAtAuditReq>,
) -> impl IntoResponse {
    match build_main_business_available_at_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/main-business/readiness-audit


pub async fn main_business_readiness_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<MainBusinessReadinessAuditReq>,
) -> impl IntoResponse {
    match build_main_business_readiness_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/broad-analyst-revision/audit


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

/// POST /api/v1/quant/data/phase7-share-float-coverage-batches


pub async fn phase7_share_float_coverage_batches(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ShareFloatCoverageReq>,
) -> impl IntoResponse {
    match build_phase7_share_float_coverage_batches(state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/phase7-share-float-readiness-audit


pub async fn phase7_share_float_readiness_audit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ShareFloatReadinessAuditReq>,
) -> impl IntoResponse {
    match build_phase7_share_float_readiness_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// GET /api/v1/quant/data/phase7-industry-membership-coverage-audit


pub async fn phase7_industry_membership_coverage_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<Phase7IndustryMembershipCoverageAuditReq>,
) -> impl IntoResponse {
    match build_phase7_industry_membership_coverage_audit(&state, req).await {
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
            "share_float is probed by unlock float_date range; ann_date is still persisted as PIT available_at.",
            "industry_membership probes index_classify and index_member only; it remains blocked from factor backfill until schema and available_at policy are audited.",
            "main_business probes fina_mainbz only; it has no native ann_date and remains blocked from schema/sync/factor backfill until available_at join audit passes.",
            "report_rc probes sell-side research earnings forecasts by report_date range only; production smoke on 2026-06-21 returned Tushare 40101 unknown data source, so it remains blocked from schema/sync/factor work until the callable API path is verified.",
            "futures_price_chain probes fut_daily/fut_wsr/fut_holding only; these remain blocked from schema/sync/factor work until permission, source publication timing, product-to-stock mapping, coverage and P3.10 diagnostics pass.",
            "equity_pledge_pressure probes pledge_stat and pledge_detail only; pledge_detail.ann_date is the native PIT candidate, while pledge_stat.end_date is a measurement date and must not be used alone as availability.",
            "shareholder_structure probes stk_holdernumber/top10_holders/top10_floatholders/stk_holdertrade only; ann_date is the native PIT candidate, and end_date must never be used as availability.",
            "margin_detail probes security-level financing and short-selling detail only; official publication timing must be handled as conservative next-session availability before any intraday use.",
            "Use this result to decide whether an optional source should proceed to Rust schema/repository/sync implementation or stay blocked."
        ],
    }))
}



async fn build_main_business_available_at_audit(
    state: &AppState,
    req: MainBusinessAvailableAtAuditReq,
) -> Result<Value, String> {
    parse_optional_date(req.start_date.as_deref())?;
    parse_optional_date(req.end_date.as_deref())?;

    let symbols = resolve_phase7_permission_smoke_symbols(state, &req.symbols).await?;
    let period_limit = main_business_available_at_audit_period_limit(req.limit);
    let today = chrono::Utc::now().date_naive();
    let default_start = (today - Duration::days(365 * 3))
        .format("%Y%m%d")
        .to_string();
    let default_end = today.format("%Y%m%d").to_string();
    let start_date = req.start_date.unwrap_or(default_start);
    let end_date = req.end_date.unwrap_or(default_end);
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let mut source_errors = Vec::new();
    let mut symbol_summaries = Vec::new();
    let mut observed_periods = BTreeSet::new();
    let mut malformed_row_count = 0usize;
    let mut sample_row_count = 0usize;

    for symbol in &symbols {
        match state
            .tushare
            .fina_mainbz(symbol, None, Some("P"), Some(&start_date), Some(&end_date))
            .await
        {
            Ok(rows) => {
                let maps = rows.data.map(|data| data.to_maps()).unwrap_or_default();
                let mut parsed_period_rows = 0usize;
                sample_row_count += maps.len();
                for row in &maps {
                    if let Some(period_key) =
                        main_business_period_key_from_row(&Value::Object(row.clone()))
                    {
                        observed_periods.insert(period_key);
                        parsed_period_rows += 1;
                    } else {
                        malformed_row_count += 1;
                    }
                }
                symbol_summaries.push(json!({
                    "symbol": symbol,
                    "status": "available",
                    "row_count": maps.len(),
                    "parsed_period_key_rows": parsed_period_rows,
                }));
            }
            Err(error) => {
                source_errors.push(json!({
                    "symbol": symbol,
                    "status": classify_tushare_permission_error(&error.to_string()),
                    "error": error.to_string(),
                }));
                symbol_summaries.push(json!({
                    "symbol": symbol,
                    "status": "error",
                }));
            }
        }
    }

    let observed_unique_periods = observed_periods.len();
    let audit_periods: Vec<(String, NaiveDate)> =
        observed_periods.into_iter().take(period_limit).collect();
    let mappings = load_main_business_available_at_mappings(&state.db, &audit_periods).await?;
    let mut decision = decide_main_business_available_at_join_audit(audit_periods.len(), &mappings);

    if !source_errors.is_empty() {
        decision.passed = false;
        decision.status = "blocked_source_probe_failed";
        decision.readiness = "blocked_available_at_join_audit_required";
    } else if malformed_row_count > 0 {
        decision.passed = false;
        decision.status = "blocked_malformed_main_business_rows";
        decision.readiness = "blocked_available_at_join_audit_required";
    }

    let missing_samples: Vec<Value> = mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_none())
        .take(20)
        .map(main_business_period_mapping_json)
        .collect();
    let pit_violation_samples: Vec<Value> = mappings
        .iter()
        .filter(|mapping| {
            mapping
                .available_at
                .map(|available_at| available_at < mapping.end_date)
                .unwrap_or(false)
        })
        .take(20)
        .map(main_business_period_mapping_json)
        .collect();
    let sample_mappings: Vec<Value> = mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_some())
        .take(20)
        .map(main_business_period_mapping_json)
        .collect();

    Ok(json!({
        "audit_version": "p3.19d-main-business-available-at-join-v1",
        "mode": "read_only_available_at_join_audit",
        "source": "tushare:fina_mainbz",
        "business_type": "P",
        "passed": decision.passed,
        "status": decision.status,
        "readiness": decision.readiness,
        "admission_gate": if decision.passed {
            "sample_join_passed_schema_sync_design_allowed_next"
        } else {
            "schema_sync_factor_p310_wfa_blocked"
        },
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "sample": {
            "symbols": symbols,
            "symbol_summaries": symbol_summaries,
            "row_count": sample_row_count,
            "malformed_row_count": malformed_row_count,
            "observed_unique_periods": observed_unique_periods,
            "audited_unique_periods": audit_periods.len(),
            "period_limit": period_limit,
            "truncated_by_period_limit": observed_unique_periods > audit_periods.len(),
        },
        "join_policy": {
            "join_key": ["ts_code", "end_date"],
            "primary_available_at": "MIN(market_financial_statement.ann_date) grouped by ts_code,end_date",
            "fallback_available_at": "MIN(market_stock_disclosure_date.available_at) grouped by symbol,end_date",
            "pit_violation_rule": "available_at must be >= end_date for segment values; available_at < end_date is blocked as impossible availability for report-period business composition",
            "prohibited": ["using fina_mainbz.end_date as available_at", "using market_stock_disclosure_date.pre_date as true available_at"]
        },
        "counts": {
            "missing_mapping_count": decision.missing_mapping_count,
            "pit_violation_count": decision.pit_violation_count,
            "mapped_period_count": mappings.iter().filter(|mapping| mapping.available_at.is_some()).count(),
            "source_errors_count": source_errors.len(),
        },
        "join_sources": decision.source_counts,
        "source_errors": source_errors,
        "missing_samples": missing_samples,
        "pit_violation_samples": pit_violation_samples,
        "sample_mappings": sample_mappings,
        "notes": [
            "This is a read-only sample audit. It does not create tables, sync main-business history, write factors, or launch P3.10/WFA.",
            "fina_mainbz has report-period segment values but no native ann_date/f_ann_date; schema and sync design stay blocked until this join audit passes.",
            "A passed sample only opens the next engineering step: schema/sync design with full-history coverage/readiness gates. It is not alpha admission."
        ]
    }))
}



async fn build_main_business_readiness_audit(
    state: &AppState,
    req: MainBusinessReadinessAuditReq,
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

    let business_type = main_business_business_type(req.business_type.as_deref());
    let periods = main_business_quarter_end_dates_in_range(start, end);
    let expected_period_count = periods.len();
    let period_keys: Vec<String> = periods
        .iter()
        .map(|period| format!("period:{}", period.format("%Y%m%d")))
        .collect();

    let summary = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
        ),
    >(main_business_readiness_summary_sql())
    .bind(start)
    .bind(end)
    .bind(business_type)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to audit main_business raw table readiness: {}",
            error
        )
    })?;

    let (
        row_count,
        symbol_count,
        distinct_periods,
        min_end_date,
        max_end_date,
        min_available_at,
        max_available_at,
        pit_violation_rows,
    ) = summary;

    let attempt_rows = if periods.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as::<
            _,
            (
                NaiveDate,
                String,
                Option<String>,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<chrono::DateTime<chrono::Utc>>,
            ),
        >(
            r#"
            WITH expected AS (
                SELECT *
                FROM UNNEST($1::date[], $2::text[]) AS input(period_end, period_key)
            )
            SELECT
                expected.period_end,
                expected.period_key,
                attempt.status,
                attempt.row_count,
                attempt.error_message,
                attempt.task_id,
                attempt.updated_at
            FROM expected
            LEFT JOIN data_sync_attempt attempt
              ON attempt.source = 'main_business'
             AND attempt.symbol = expected.period_key
             AND attempt.start_date = expected.period_end
             AND attempt.end_date = expected.period_end
            ORDER BY expected.period_end DESC
            "#,
        )
        .bind(&periods)
        .bind(&period_keys)
        .fetch_all(&state.db)
        .await
        .map_err(|error| {
            format!(
                "Failed to audit main_business period sync attempts: {}",
                error
            )
        })?
    };

    let completed_periods = attempt_rows
        .iter()
        .filter(|(_, _, status, _, _, _, _)| status.as_deref() == Some("completed"))
        .count();
    let failed_periods = attempt_rows
        .iter()
        .filter(|(_, _, status, _, _, _, _)| status.as_deref() == Some("failed"))
        .count();
    let missing_available_at_raw_rows: i64 = attempt_rows
        .iter()
        .map(|(_, _, _, _, error_message, _, _)| {
            main_business_missing_available_at_rows(error_message.as_deref())
        })
        .sum();
    let out_of_universe_raw_rows: i64 = attempt_rows
        .iter()
        .map(|(_, _, _, _, error_message, _, _)| {
            main_business_out_of_universe_rows(error_message.as_deref())
        })
        .sum();
    let periods_with_available_at_mapping_gaps = attempt_rows
        .iter()
        .filter(|(_, _, _, _, error_message, _, _)| {
            main_business_missing_available_at_rows(error_message.as_deref()) > 0
        })
        .count();
    let periods_with_out_of_universe_rows = attempt_rows
        .iter()
        .filter(|(_, _, _, _, error_message, _, _)| {
            main_business_out_of_universe_rows(error_message.as_deref()) > 0
        })
        .count();
    let missing_periods = expected_period_count.saturating_sub(completed_periods + failed_periods);
    let readiness = main_business_raw_source_readiness(
        row_count,
        expected_period_count,
        completed_periods,
        failed_periods,
        pit_violation_rows,
    );

    let breakdown_limit = main_business_readiness_breakdown_limit(req.limit);
    let period_breakdown: Vec<Value> = attempt_rows
        .iter()
        .take(breakdown_limit)
        .map(
            |(period_end, period_key, status, row_count, error_message, task_id, updated_at)| {
                json!({
                    "period_end": period_end.to_string(),
                    "period_key": period_key,
                    "status": status.as_deref().unwrap_or("missing"),
                    "row_count": row_count.unwrap_or(0),
                    "error_message": error_message,
                    "task_id": task_id,
                    "updated_at": fmt_rfc3339_local(*updated_at),
                })
            },
        )
        .collect();

    let blocking_reasons: Vec<&str> = [
        (
            row_count <= 0,
            "raw source table has no rows for requested scope",
        ),
        (
            pit_violation_rows > 0,
            "available_at before end_date violates PIT availability",
        ),
        (
            completed_periods < expected_period_count,
            "full-market period sync ledger is incomplete",
        ),
        (
            failed_periods > 0,
            "one or more full-market period sync attempts failed",
        ),
    ]
    .into_iter()
    .filter_map(|(blocked, reason)| blocked.then_some(reason))
    .collect();
    let data_quality_warnings: Vec<&str> = [(
        missing_available_at_raw_rows > 0,
        "some raw fina_mainbz rows could not be PIT-mapped and were skipped before persistence",
    )]
    .into_iter()
    .filter_map(|(warn, reason)| warn.then_some(reason))
    .collect();

    Ok(json!({
        "audit_version": "p3.19e-main-business-raw-readiness-v1",
        "dataset": "main_business",
        "source": "tushare:fina_mainbz_vip",
        "table": "market_stock_main_business",
        "business_type": business_type,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "readiness": readiness,
        "admission_gate": if readiness == "raw_source_ready_for_full_history_coverage_audit" {
            "raw_source_ready_for_coverage_readiness_audit_only"
        } else {
            "factor_p310_wfa_blocked"
        },
        "counts": {
            "row_count": row_count,
            "symbol_count": symbol_count,
            "distinct_periods": distinct_periods,
            "expected_period_count": expected_period_count,
            "completed_period_count": completed_periods,
            "failed_period_count": failed_periods,
            "missing_period_count": missing_periods,
            "pit_violation_rows": pit_violation_rows,
            "missing_available_at_raw_rows": missing_available_at_raw_rows,
            "periods_with_available_at_mapping_gaps": periods_with_available_at_mapping_gaps,
            "out_of_universe_raw_rows": out_of_universe_raw_rows,
            "periods_with_out_of_universe_rows": periods_with_out_of_universe_rows,
        },
        "range": {
            "min_end_date": min_end_date,
            "max_end_date": max_end_date,
            "min_available_at": min_available_at,
            "max_available_at": max_available_at,
        },
        "period_breakdown": period_breakdown,
        "breakdown_limit": breakdown_limit,
        "truncated_period_breakdown": attempt_rows.len() > period_breakdown.len(),
        "blocking_reasons": blocking_reasons,
        "data_quality_warnings": data_quality_warnings,
        "pit_contract": {
            "source_period": "fina_mainbz_vip.period/end_date",
            "available_at": "joined from market_financial_statement.ann_date first, then market_stock_disclosure_date.available_at fallback",
            "hard_rule": "available_at >= end_date and downstream features must filter available_at <= trade_date",
            "prohibited": ["using end_date as available_at", "treating symbol-limited smoke attempts as full-market completion"]
        },
        "notes": [
            "This endpoint audits raw source readiness only; it does not construct a factor, run P3.10 diagnostics, run WFA, or admit v19 train selection.",
            "Only data_sync_attempt.source='main_business' period rows count as full-market sync completion; symbol-limited smoke runs must use main_business_sample.",
            "After this passes, the next step is a separate full-history coverage/readiness audit and then P3.10A-D diagnostics before any bounded WFA."
        ]
    }))
}



fn main_business_period_key_from_row(row: &Value) -> Option<(String, NaiveDate)> {
    let ts_code = row.get("ts_code")?.as_str()?.trim().to_ascii_uppercase();
    if ts_code.is_empty() {
        return None;
    }
    let end_date = main_business_yyyymmdd_field(row, "end_date")?;
    Some((ts_code, end_date))
}



fn main_business_yyyymmdd_field(row: &Value, field: &str) -> Option<NaiveDate> {
    let raw = match row.get(field)? {
        Value::String(value) => value.trim().to_string(),
        Value::Number(value) => value.to_string(),
        _ => return None,
    };
    NaiveDate::parse_from_str(&raw, "%Y%m%d").ok()
}



async fn load_main_business_available_at_mappings(
    db: &sqlx::PgPool,
    keys: &[(String, NaiveDate)],
) -> Result<Vec<MainBusinessPeriodMapping>, String> {
    let mut mappings = BTreeMap::new();
    for (ts_code, end_date) in keys {
        mappings.insert(
            (ts_code.clone(), *end_date),
            MainBusinessPeriodMapping {
                ts_code: ts_code.clone(),
                end_date: *end_date,
                available_at: None,
                source: None,
            },
        );
    }
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let symbols: Vec<String> = keys.iter().map(|(ts_code, _)| ts_code.clone()).collect();
    let end_dates: Vec<NaiveDate> = keys.iter().map(|(_, end_date)| *end_date).collect();

    let financial_rows = sqlx::query_as::<_, (String, NaiveDate, Option<NaiveDate>, i64)>(
        r#"
        WITH keys AS (
            SELECT *
            FROM UNNEST($1::text[], $2::date[]) AS input(ts_code, end_date)
        )
        SELECT
            keys.ts_code,
            keys.end_date,
            MIN(fs.ann_date) AS available_at,
            COUNT(*)::bigint AS row_count
        FROM keys
        JOIN market_financial_statement fs
          ON fs.ts_code = keys.ts_code
         AND fs.end_date = keys.end_date
        GROUP BY keys.ts_code, keys.end_date
        "#,
    )
    .bind(&symbols)
    .bind(&end_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("financial_statement available_at join failed: {}", error))?;

    for (ts_code, end_date, available_at, _row_count) in financial_rows {
        if let Some(mapping) = mappings.get_mut(&(ts_code, end_date)) {
            if available_at.is_some() {
                mapping.available_at = available_at;
                mapping.source = Some("financial_statement".to_string());
            }
        }
    }

    let disclosure_rows = sqlx::query_as::<_, (String, NaiveDate, Option<NaiveDate>, i64)>(
        r#"
        WITH keys AS (
            SELECT *
            FROM UNNEST($1::text[], $2::date[]) AS input(ts_code, end_date)
        )
        SELECT
            keys.ts_code,
            keys.end_date,
            MIN(disclosure.available_at) AS available_at,
            COUNT(*)::bigint AS row_count
        FROM keys
        JOIN market_stock_disclosure_date disclosure
          ON disclosure.symbol = keys.ts_code
         AND disclosure.end_date = keys.end_date
        GROUP BY keys.ts_code, keys.end_date
        "#,
    )
    .bind(&symbols)
    .bind(&end_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("disclosure_date available_at join failed: {}", error))?;

    for (ts_code, end_date, available_at, _row_count) in disclosure_rows {
        if let Some(mapping) = mappings.get_mut(&(ts_code, end_date)) {
            if mapping.available_at.is_none() && available_at.is_some() {
                mapping.available_at = available_at;
                mapping.source = Some("disclosure_date".to_string());
            }
        }
    }

    Ok(mappings.into_values().collect())
}



fn main_business_period_mapping_json(mapping: &MainBusinessPeriodMapping) -> Value {
    json!({
        "ts_code": mapping.ts_code,
        "end_date": mapping.end_date.to_string(),
        "available_at": mapping.available_at.map(|date| date.to_string()),
        "source": mapping.source,
    })
}



pub(crate) async fn phase7_completed_attempts_by_source_for_window(
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



fn phase7_first_tushare_string_field(
    response: &quant_data::model::tushare_dto::TushareResponse<Vec<serde_json::Value>>,
    field: &str,
) -> Option<String> {
    response
        .data
        .as_ref()?
        .to_maps()
        .into_iter()
        .find_map(|row| {
            row.get(field)
                .and_then(|value| value.as_str())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
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
        "share_float" => {
            let result = state
                .tushare
                .share_float(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let probe = phase7_tushare_probe_json(source, None, "unlock_date_range", result);
            let probes = vec![probe];
            json!({
                "source": source,
                "query_scope": "unlock_date_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": false,
                "pit_available_at": "ann_date"
            })
        }
        "industry_membership" => {
            let classify_result = state
                .tushare
                .index_classify(None, Some("L1"), None, Some("SW2021"))
                .await;
            let index_code = classify_result
                .as_ref()
                .ok()
                .and_then(|response| phase7_first_tushare_string_field(response, "index_code"))
                .unwrap_or_else(|| "801010.SI".to_string());
            let classify_probe = phase7_tushare_probe_json(
                source,
                None,
                "sw2021_l1_index_classify",
                classify_result,
            );

            let member_result = state
                .tushare
                .index_member(Some(&index_code), None, Some("Y"), Some(row_limit), Some(0))
                .await;
            let member_probe = phase7_tushare_probe_json(
                source,
                None,
                "sw_index_member_by_index_code",
                member_result,
            );

            let history_member_result = state
                .tushare
                .index_member(Some(&index_code), None, None, Some(row_limit), Some(0))
                .await;
            let history_member_probe = phase7_tushare_probe_json(
                source,
                None,
                "sw_index_member_history_by_index_code",
                history_member_result,
            );
            let probes = vec![classify_probe, member_probe, history_member_probe];

            json!({
                "source": source,
                "query_scope": "classification_and_membership",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "classification_source": "SW2021",
                "sample_index_code": index_code,
                "symbol_filter_supported": true,
                "pit_required_fields": ["in_date", "out_date", "is_new"],
                "pit_available_at_policy": "schema audit required; if source has no publication date, available_at must be no earlier than membership effective in_date and must never revise prior samples before the change is observable",
                "admission_gate": "schema_and_available_at_audit_required_before_factor_backfill"
            })
        }
        "main_business" => {
            let mut probes = Vec::new();
            for symbol in symbols {
                let result = state
                    .tushare
                    .fina_mainbz(symbol, None, Some("P"), Some(start_date), Some(end_date))
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "symbol_report_period_range",
                    result,
                ));
            }
            json!({
                "source": source,
                "query_scope": "symbol_report_period_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": true,
                "business_type": "P",
                "pit_available_at": "not_native; must join financial announcement/disclosure date before schema or factor backfill",
                "admission_gate": "permission_smoke_only_available_at_audit_required_before_sync"
            })
        }
        "report_rc" => {
            let result = state
                .tushare
                .report_rc(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let probe = phase7_tushare_probe_json(source, None, "report_date_range", result);
            let probes = vec![probe];
            json!({
                "source": source,
                "query_scope": "report_date_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": true,
                "official_doc": "https://tushare.pro/wctapi/documents/292.md",
                "source_semantics": "sell_side_research_report_earnings_forecast_daily_since_2010",
                "pit_available_at": "report_date is native available_at candidate; create_time may audit Tushare update lag but must not move availability earlier",
                "required_fields_for_schema_audit": ["ts_code", "report_date", "quarter", "org_name", "author_name", "eps", "rating", "max_price", "min_price"],
                "admission_gate": "permission_smoke_only_schema_available_at_and_full_history_coverage_audit_required_before_sync"
            })
        }
        "futures_price_chain" => {
            let fut_daily_result = state
                .tushare
                .fut_daily(
                    None,
                    Some(start_date),
                    None,
                    None,
                    None,
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let fut_daily_probe =
                phase7_tushare_probe_json(source, None, "fut_daily_trade_date", fut_daily_result);

            let fut_wsr_result = state
                .tushare
                .fut_wsr(
                    Some(start_date),
                    None,
                    None,
                    None,
                    None,
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let fut_wsr_probe =
                phase7_tushare_probe_json(source, None, "fut_wsr_trade_date", fut_wsr_result);

            let fut_holding_result = state
                .tushare
                .fut_holding(
                    Some(start_date),
                    None,
                    None,
                    None,
                    None,
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let fut_holding_probe = phase7_tushare_probe_json(
                source,
                None,
                "fut_holding_trade_date",
                fut_holding_result,
            );

            let probes = vec![fut_daily_probe, fut_wsr_probe, fut_holding_probe];
            json!({
                "source": source,
                "query_scope": "futures_price_warehouse_holding_smoke",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": false,
                "official_docs": [
                    "https://tushare.pro/wctapi/documents/138.md",
                    "https://tushare.pro/wctapi/documents/139.md",
                    "https://tushare.pro/wctapi/documents/140.md"
                ],
                "source_semantics": "daily futures price settlement open_interest warehouse_receipt_and_position_ranking",
                "pit_available_at": "trade_date_after_futures_market_close_or_verified_source_published_at; intraday stock decisions must use previous available futures trade_date until publication timing is audited",
                "required_fields_for_schema_audit": ["trade_date", "ts_code_or_symbol", "close_or_settle", "vol", "oi", "vol_chg", "long_hld", "short_hld", "exchange"],
                "mapping_gate": "product-to-industry-stock exposure mapping must be versioned and PIT-stable before any factor backfill",
                "admission_gate": "permission_smoke_only_schema_mapping_available_at_and_full_history_coverage_audit_required_before_sync"
            })
        }
        "equity_pledge_pressure" => {
            let mut probes = Vec::new();
            for symbol in symbols {
                let result = state
                    .tushare
                    .pledge_stat(Some(symbol.as_str()), None, Some(row_limit), Some(0))
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "pledge_stat_by_symbol",
                    result,
                ));
            }

            let detail_result = state
                .tushare
                .pledge_detail(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            probes.push(phase7_tushare_probe_json(
                source,
                None,
                "pledge_detail_announcement_date_range",
                detail_result,
            ));

            json!({
                "source": source,
                "query_scope": "pledge_stat_symbol_and_detail_ann_date_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": true,
                "official_docs": [
                    "https://tushare.pro/wctapi/documents/110.md",
                    "https://tushare.pro/wctapi/documents/111.md"
                ],
                "source_semantics": "equity_pledge_snapshot_and_pledge_detail_events_for_shareholder_financing_pressure",
                "pit_available_at": "pledge_detail.ann_date is native available_at candidate; pledge_stat.end_date is only a measurement date and must be joined or derived conservatively before factor use",
                "required_fields_for_schema_audit": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledge_ratio"],
                "admission_gate": "permission_smoke_only_schema_available_at_and_full_history_coverage_audit_required_before_sync",
                "blocked_until": ["permission_available", "detail_ann_date_coverage_verified", "stat_snapshot_available_at_policy_verified", "bounded_sync_plan_reviewed"]
            })
        }
        "shareholder_structure" => {
            let mut probes = Vec::new();

            let holder_number_result = state
                .tushare
                .stk_holdernumber(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            probes.push(phase7_tushare_probe_json(
                source,
                None,
                "holder_number_announcement_date_range",
                holder_number_result,
            ));

            for symbol in symbols {
                let top10_result = state
                    .tushare
                    .top10_holders(
                        Some(symbol.as_str()),
                        None,
                        None,
                        None,
                        Some(row_limit),
                        Some(0),
                    )
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "top10_holders_by_symbol",
                    top10_result,
                ));

                let top10_float_result = state
                    .tushare
                    .top10_floatholders(
                        Some(symbol.as_str()),
                        None,
                        None,
                        None,
                        Some(row_limit),
                        Some(0),
                    )
                    .await;
                probes.push(phase7_tushare_probe_json(
                    source,
                    Some(symbol.as_str()),
                    "top10_floatholders_by_symbol",
                    top10_float_result,
                ));
            }

            let holder_trade_result = state
                .tushare
                .stk_holdertrade(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            probes.push(phase7_tushare_probe_json(
                source,
                None,
                "holder_trade_announcement_date_range",
                holder_trade_result,
            ));

            json!({
                "source": source,
                "query_scope": "holder_count_top10_and_holder_trade_smoke",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": true,
                "source_semantics": "shareholder_count_concentration_and_major_holder_trade_events",
                "pit_available_at": "ann_date is native available_at; end_date is only the measurement period and must not move availability earlier",
                "required_fields_for_schema_audit": [
                    "ts_code", "ann_date", "end_date", "holder_num",
                    "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type",
                    "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "begin_date", "close_date"
                ],
                "admission_gate": "permission_smoke_only_schema_available_at_full_history_coverage_and_p310_required_before_sync",
                "blocked_until": ["schema_contract_reviewed", "bounded_sync_plan_reviewed", "ann_date_coverage_verified", "holder_count_breadth_verified", "p310_diagnostics_passed"]
            })
        }
        "margin_detail" => {
            let result = state
                .tushare
                .margin_detail(
                    None,
                    None,
                    Some(start_date),
                    Some(end_date),
                    Some(row_limit),
                    Some(0),
                )
                .await;
            let probe =
                phase7_tushare_probe_json(source, None, "margin_detail_trade_date_range", result);
            let probes = vec![probe];
            json!({
                "source": source,
                "query_scope": "security_level_margin_detail_trade_date_range",
                "status": phase7_tushare_source_status(&probes),
                "probes": probes,
                "symbol_filter_supported": true,
                "official_doc": "https://tushare.pro/document/2?doc_id=59",
                "source_semantics": "security-level margin financing and short-selling detail by trade_date",
                "pit_available_at": "official source publishes prior-day records around next trading day 08:30; use conservative next-session available_at unless source_published_at is audited",
                "required_fields_for_schema_audit": [
                    "trade_date", "ts_code", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"
                ],
                "admission_gate": "permission_smoke_only_schema_available_at_correlation_and_full_history_coverage_audit_required_before_sync",
                "blocked_until": ["permission_available", "source_publication_timing_audited", "full_history_coverage_verified", "correlation_to_existing_moneyflow_liquidity_price_volume_checked", "p310_diagnostics_passed"]
            })
        }
        unsupported => json!({
            "source": unsupported,
            "status": "unsupported_source",
            "supported_sources": PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED,
        }),
    }
}



