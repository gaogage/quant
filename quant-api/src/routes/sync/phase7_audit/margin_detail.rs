/// 融资融券明细（margin_detail）域：就绪/相关性/覆盖审计与同步计划
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

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

pub struct MarginDetailSyncPlanReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch: Option<String>,
}

#[derive(Debug, Clone)]

pub(crate) struct MarginDetailSyncPlanBatch {
    pub(crate) label: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) open_day_count: i64,
    pub(crate) observed_avg_rows_per_day: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct MarginDetailCoverageAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

pub(crate) fn margin_detail_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![(
        "market_stock_margin_detail",
        vec![
            "market_stock_margin_detail_pkey",
            "market_stock_margin_detail_available_at_check",
            "market_stock_margin_detail_core_nonnegative_check",
        ],
    )]
}

pub(crate) async fn build_margin_detail_readiness_audit(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) fn margin_detail_sync_plan_response(
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

pub(crate) async fn build_margin_detail_sync_plan(
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

pub(crate) async fn build_margin_detail_correlation_audit(
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

pub(crate) async fn build_margin_detail_coverage_audit(
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

/// GET /api/v1/quant/data/margin-detail/schema-contract
pub async fn margin_detail_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_margin_detail_schema_contract()}))
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
