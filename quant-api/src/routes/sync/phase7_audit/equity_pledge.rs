/// 股权质押压力（equity_pledge_pressure）域：就绪/覆盖审计与同步
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

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

pub struct EquityPledgeCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

pub(crate) fn equity_pledge_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
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

pub(crate) async fn build_equity_pledge_pressure_readiness_audit(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) async fn build_equity_pledge_pressure_coverage_audit(
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

pub(crate) fn equity_pledge_readiness_label(admission_decision: &str) -> &'static str {
    match admission_decision {
        "apply_schema_before_sync" => "schema_contract_ready_schema_not_applied",
        "bounded_sync_required_before_coverage_audit" => "schema_created_bounded_sync_required",
        "raw_pit_failed" => "raw_source_pit_failed",
        "coverage_readiness_audit_required_before_p310" => "raw_source_ready_for_coverage_audit",
        _ => "schema_contract_ready_review_required",
    }
}

/// GET /api/v1/quant/data/equity-pledge-pressure/schema-contract
pub async fn equity_pledge_pressure_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_equity_pledge_schema_contract()}))
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
