/// 期货价格链（futures_price_chain）域：映射/覆盖/就绪审计与同步
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

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
    pub(crate) product_symbol: String,
    pub(crate) exposure_type: String,
    pub(crate) exposure_code: String,
    pub(crate) passed: bool,
    pub(crate) errors: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

#[allow(dead_code)]
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

pub(crate) fn futures_price_chain_mapping_product_summary_sql() -> &'static str {
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

pub(crate) fn futures_price_chain_exclusion_product_summary_sql() -> &'static str {
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

pub(crate) fn parse_futures_price_chain_coverage_date(
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

pub(crate) fn futures_price_chain_product_set_from_audit(
    audit: &Value,
    key: &str,
) -> BTreeSet<String> {
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

pub(crate) fn futures_price_chain_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
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

pub(crate) async fn build_futures_price_chain_readiness_audit(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) async fn build_futures_price_chain_mapping_audit(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) async fn build_futures_price_chain_coverage_audit(
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

pub(crate) async fn build_futures_price_chain_mapping_template(
    db: &sqlx::PgPool,
) -> Result<Value, String> {
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

pub(crate) async fn validate_futures_price_chain_mapping_candidates(
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

/// GET /api/v1/quant/data/futures-price-chain/schema-contract
pub async fn futures_price_chain_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_futures_price_chain_schema_contract()}))
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
