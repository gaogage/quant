/// 股东结构（shareholder_structure）域：就绪/覆盖审计与同步计划
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::phase7_alpha_admission::SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID;
use crate::AppState;

use super::*;

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

pub struct ShareholderStructureSyncPlanReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]

pub(crate) struct ShareholderStructureSyncPlanBatch {
    pub(crate) year: i32,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) symbol_count: i64,
    pub(crate) quarter_count: i64,
}

#[derive(Debug, Clone, Deserialize)]

pub struct ShareholderStructureCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

pub(crate) fn decide_shareholder_structure_readiness(
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

pub(crate) fn shareholder_structure_quarter_count(start: NaiveDate, end: NaiveDate) -> i64 {
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

pub(crate) async fn build_shareholder_structure_sync_plan(
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

pub(crate) async fn build_shareholder_structure_readiness_audit(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) async fn build_shareholder_structure_coverage_audit(
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

// 测试辅助函数：被 #[cfg(test)] 的 futures_price_chain_product_symbol_from_daily_ts_code 调用，
// 非测试编译时无调用方，标记为允许死代码。

/// GET /api/v1/quant/data/shareholder-structure/schema-contract
pub async fn shareholder_structure_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_shareholder_structure_schema_contract()}))
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
