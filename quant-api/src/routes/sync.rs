/// 数据同步路由
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tracing::info;
use uuid::Uuid;

use crate::phase7_alpha_admission::{
    industry_prosperity_alpha_admission_policy, industry_prosperity_alpha_admission_policy_static,
    INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD, INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
    SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
};
use crate::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct DataSyncTaskReq {
    pub dataset: String,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub source_filters: Vec<String>,
    #[serde(default)]
    pub index_codes: Vec<String>,
    #[serde(default)]
    pub exchanges: Vec<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub quality_check: bool,
    #[serde(default)]
    pub create_data_version: bool,
    #[serde(default)]
    pub retry_of_task_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CleanupStaleSyncTasksReq {
    #[serde(default)]
    pub dry_run: bool,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
}

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
    fn into_sync_task_req(self) -> DataSyncTaskReq {
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
    fn into_sync_task_req(self) -> DataSyncTaskReq {
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
    fn into_sync_task_req(self) -> DataSyncTaskReq {
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
struct ShareholderStructureSyncPlanBatch {
    year: i32,
    start_date: NaiveDate,
    end_date: NaiveDate,
    symbol_count: i64,
    quarter_count: i64,
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
struct FuturesPriceChainMappingCandidateValidation {
    product_symbol: String,
    exposure_type: String,
    exposure_code: String,
    passed: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
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
pub struct BroadAnalystRevisionAuditReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MainBusinessPeriodMapping {
    ts_code: String,
    end_date: NaiveDate,
    available_at: Option<NaiveDate>,
    source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MainBusinessAvailableAtJoinDecision {
    passed: bool,
    status: &'static str,
    readiness: &'static str,
    missing_mapping_count: usize,
    pit_violation_count: usize,
    source_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BroadAnalystRevisionAuditDecision {
    passed: bool,
    status: &'static str,
    readiness: &'static str,
    admission_decision: &'static str,
    p310_status: &'static str,
    blocked_reason: &'static str,
}

fn default_source() -> String {
    "tushare".into()
}

fn stale_cleanup_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(3600).clamp(60, 86_400)
}

fn stale_cleanup_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}

fn stale_sync_task_cleanup_terminal_status(status: &str) -> Option<&'static str> {
    match status {
        "running" => Some("failed"),
        "cancel_requested" => Some("cancelled"),
        _ => None,
    }
}

fn stale_sync_task_cleanup_action(status: &str) -> &'static str {
    match stale_sync_task_cleanup_terminal_status(status) {
        Some("cancelled") => "finalize_cancel_requested",
        Some("failed") => "mark_running_failed",
        _ => "ignore",
    }
}

fn generated_data_version_id() -> String {
    chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string()
}

fn bounded_phase7_task_id(parts: &[&str]) -> String {
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

fn parse_optional_date(value: Option<&str>) -> Result<Option<NaiveDate>, String> {
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
const PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED: &[&str] = &[
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
const PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS: usize = 3;
const PHASE7_PERMISSION_SMOKE_MAX_ROWS: usize = 5;
const MAIN_BUSINESS_AVAILABLE_AT_AUDIT_MAX_PERIODS: usize = 100;
const MAIN_BUSINESS_READINESS_BREAKDOWN_MAX_PERIODS: usize = 200;
const BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT: usize = 32;
const BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE: f64 = 0.50;
const BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE: f64 = 0.30;
const BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE: f64 = 0.30;
const BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO: f64 = 0.15;
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
pub struct Phase7OptionalSourceCoverageSyncReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub max_symbols: Option<usize>,
    #[serde(default)]
    pub offset_symbols: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OptionalSourceCoverageBatchReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub start_offset: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7CoverageExpansionRunnerReq {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
    #[serde(default)]
    pub stop_when_readiness_at_least_partial: Option<bool>,
    #[serde(default)]
    pub auto_continue: Option<bool>,
    #[serde(default)]
    pub max_rounds: Option<usize>,
    #[serde(default)]
    pub target_coverage_ratio: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7ShareFloatCoverageReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub chunk_granularity: Option<String>,
    #[serde(default)]
    pub max_chunks: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

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

fn phase7_permission_smoke_sources(requested: &[String]) -> Vec<String> {
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

fn phase7_permission_smoke_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(1)
        .clamp(1, PHASE7_PERMISSION_SMOKE_MAX_ROWS)
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

fn broad_analyst_revision_breakdown_limit(limit: Option<usize>) -> i64 {
    limit
        .unwrap_or(BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT)
        .clamp(1, BROAD_ANALYST_REVISION_BREAKDOWN_LIMIT) as i64
}

fn safe_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn decide_broad_analyst_revision_audit(
    available_at_rule_violations: i64,
    union_symbol_coverage_ratio: f64,
    forecast_symbol_coverage_ratio: f64,
    revised_symbol_coverage_ratio: f64,
    revised_symbol_period_ratio: f64,
) -> BroadAnalystRevisionAuditDecision {
    if available_at_rule_violations > 0 {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_available_at_rule_violation",
            readiness: "blocked_pit_available_at_repair_required",
            admission_decision: "blocked_broad_analyst_revision_available_at_rule_failed",
            p310_status: "not_started",
            blocked_reason:
                "one_or_more_event_source_rows_do_not_follow_the_registered_available_at_policy",
        }
    } else if union_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_undercovered_for_broad_base_revision",
            readiness: "blocked_full_history_coverage_not_broad_enough",
            admission_decision: "blocked_broad_analyst_revision_after_full_history_coverage_audit",
            p310_status: "not_started",
            blocked_reason: "forecast_express_disclosure_sources_do_not_cover_enough_symbols_for_broad_base_revision",
        }
    } else if forecast_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE
        || revised_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE
        || revised_symbol_period_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO
    {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_sparse_revision_semantics",
            readiness: "stopped_current_raw_bundle_revision_semantics_too_sparse",
            admission_decision: "stopped_broad_analyst_revision_current_raw_bundle_after_audit_sparse_revision_semantics",
            p310_status: "not_started",
            blocked_reason: "true_forecast_revision_events_are_too_sparse_and_would_degenerate_into_event_overlay",
        }
    } else {
        BroadAnalystRevisionAuditDecision {
            passed: true,
            status: "full_history_revision_semantics_audit_passed",
            readiness: "ready_for_p310_diagnostics_only",
            admission_decision:
                "coverage_available_at_revision_semantics_passed_p310_required_next",
            p310_status: "not_started",
            blocked_reason: "",
        }
    }
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

fn main_business_raw_source_readiness(
    row_count: i64,
    expected_periods: usize,
    completed_periods: usize,
    failed_periods: usize,
    pit_violation_rows: i64,
) -> &'static str {
    if row_count <= 0 {
        return "raw_source_missing";
    }
    if pit_violation_rows > 0 {
        return "raw_source_pit_failed";
    }
    if completed_periods < expected_periods {
        return "period_sync_incomplete";
    }
    if failed_periods > 0 {
        return "period_sync_failed";
    }
    "raw_source_ready_for_full_history_coverage_audit"
}

fn main_business_readiness_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT symbol)::bigint AS symbol_count,
        COUNT(DISTINCT end_date)::bigint AS distinct_periods,
        MIN(end_date) AS min_end_date,
        MAX(end_date) AS max_end_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < end_date)::bigint AS pit_violation_rows
    FROM market_stock_main_business
    WHERE end_date BETWEEN $1 AND $2
      AND business_type = $3
    "#
}

fn main_business_attempt_metric(error_message: Option<&str>, prefix: &str) -> i64 {
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

fn main_business_missing_available_at_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "missing_available_at_rows=")
}

fn main_business_out_of_universe_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "out_of_universe_rows=")
}

fn decide_main_business_available_at_join_audit(
    total_periods: usize,
    mappings: &[MainBusinessPeriodMapping],
) -> MainBusinessAvailableAtJoinDecision {
    let explicit_missing = mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_none())
        .count();
    let implicit_missing = total_periods.saturating_sub(mappings.len());
    let missing_mapping_count = explicit_missing + implicit_missing;
    let pit_violation_count = mappings
        .iter()
        .filter(|mapping| {
            mapping
                .available_at
                .map(|available_at| available_at < mapping.end_date)
                .unwrap_or(false)
        })
        .count();
    let mut source_counts = BTreeMap::new();
    for mapping in mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_some())
    {
        let source = mapping
            .source
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        *source_counts.entry(source).or_insert(0) += 1;
    }

    let (passed, status, readiness) = if total_periods == 0 {
        (
            false,
            "blocked_no_sample_periods",
            "blocked_available_at_join_audit_required",
        )
    } else if pit_violation_count > 0 {
        (
            false,
            "blocked_pit_available_at_violations",
            "blocked_available_at_join_audit_required",
        )
    } else if missing_mapping_count > 0 {
        (
            false,
            "blocked_available_at_join_gaps",
            "blocked_available_at_join_audit_required",
        )
    } else {
        (true, "passed", "available_at_join_ready_for_schema_design")
    };

    MainBusinessAvailableAtJoinDecision {
        passed,
        status,
        readiness,
        missing_mapping_count,
        pit_violation_count,
        source_counts,
    }
}

fn phase7_optional_source_sync_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(PHASE7_OPTIONAL_SOURCE_SYNC_DEFAULT_SYMBOLS)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS)
}

fn phase7_optional_source_sync_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

fn phase7_optional_source_batch_size(batch_size: Option<usize>) -> usize {
    phase7_optional_source_sync_limit(batch_size)
}

fn phase7_optional_source_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(1)
        .clamp(1, PHASE7_OPTIONAL_SOURCE_BATCH_MAX_COUNT)
}

fn phase7_optional_source_batch_offsets(
    start_offset: usize,
    batch_size: usize,
    batch_count: usize,
) -> Vec<usize> {
    (0..batch_count)
        .map(|index| start_offset + index * batch_size)
        .collect()
}

fn phase7_optional_source_batch_next_offset(offsets: &[usize], batch_size: usize) -> usize {
    offsets
        .last()
        .map(|offset| offset + batch_size)
        .unwrap_or_default()
}

fn phase7_optional_source_batch_recommended_resume_offset(
    plan_only: bool,
    offsets: &[usize],
    batch_size: usize,
) -> usize {
    if plan_only {
        phase7_optional_source_batch_next_offset(offsets, batch_size)
    } else {
        0
    }
}

fn phase7_optional_source_batch_child_background(plan_only: bool) -> bool {
    !plan_only
}

fn phase7_coverage_runner_profile(profile: Option<&str>) -> String {
    profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_PROFILE)
        .to_string()
}

fn phase7_coverage_runner_batch_size(batch_size: Option<usize>) -> usize {
    batch_size
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_SIZE)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_SIZE)
}

fn phase7_coverage_runner_batch_count(batch_count: Option<usize>) -> usize {
    batch_count
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_BATCH_COUNT)
        .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_BATCH_COUNT)
}

fn phase7_coverage_runner_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

fn phase7_coverage_runner_auto_continue(auto_continue: Option<bool>) -> bool {
    auto_continue.unwrap_or(false)
}

fn phase7_coverage_runner_max_rounds(max_rounds: Option<usize>, auto_continue: bool) -> usize {
    if auto_continue {
        max_rounds
            .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_MAX_ROUNDS)
            .clamp(1, PHASE7_COVERAGE_RUNNER_MAX_ROUNDS)
    } else {
        1
    }
}

fn phase7_coverage_runner_should_build_immediate_batches(
    plan_only: bool,
    auto_continue: bool,
) -> bool {
    plan_only || !auto_continue
}

fn phase7_coverage_autopilot_batch_offsets(batch_count: usize) -> Vec<usize> {
    vec![0; batch_count]
}

fn phase7_coverage_runner_target_ratio(target: Option<f64>) -> f64 {
    target
        .filter(|value| value.is_finite())
        .unwrap_or(PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO)
        .clamp(
            PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO,
            PHASE7_COVERAGE_RUNNER_DEFAULT_TARGET_COVERAGE_RATIO,
        )
}

fn phase7_share_float_chunk_granularity(granularity: Option<&str>) -> &'static str {
    match granularity
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("month") | Some("monthly") => "month",
        Some("quarter") | Some("quarterly") => "quarter",
        Some("year") | Some("yearly") => "year",
        _ => "year",
    }
}

fn phase7_share_float_coverage_plan_only(plan_only: Option<bool>) -> bool {
    plan_only.unwrap_or(true)
}

fn phase7_share_float_coverage_max_chunks(max_chunks: Option<usize>) -> usize {
    max_chunks
        .unwrap_or(PHASE7_SHARE_FLOAT_COVERAGE_DEFAULT_MAX_CHUNKS)
        .clamp(1, PHASE7_SHARE_FLOAT_COVERAGE_MAX_CHUNKS)
}

fn add_months_clamped(date: NaiveDate, months: u32) -> NaiveDate {
    let month0 = date.month0() + months;
    let year = date.year() + (month0 / 12) as i32;
    let month = (month0 % 12) + 1;
    NaiveDate::from_ymd_opt(year, month, 1).expect("valid first day")
}

fn phase7_share_float_next_chunk_start(start: NaiveDate, granularity: &str) -> NaiveDate {
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

fn phase7_share_float_date_chunks(
    start: NaiveDate,
    end: NaiveDate,
    granularity: &str,
    max_chunks: usize,
) -> Vec<(NaiveDate, NaiveDate)> {
    let mut chunks = Vec::new();
    let mut cursor = start;
    while cursor <= end && chunks.len() < max_chunks {
        let next_start = phase7_share_float_next_chunk_start(cursor, granularity);
        let chunk_end = (next_start - Duration::days(1)).min(end);
        chunks.push((cursor, chunk_end));
        cursor = next_start;
    }
    chunks
}

fn phase7_share_float_readiness_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT symbol)::bigint AS symbol_count,
        MIN(float_date) AS min_float_date,
        MAX(float_date) AS max_float_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at > float_date)::bigint AS late_or_invalid_count,
        COUNT(DISTINCT float_date)::bigint AS distinct_float_dates
    FROM market_stock_share_float
    WHERE float_date BETWEEN $1 AND $2
      AND available_at <= $2
    "#
}

fn phase7_share_float_expected_days(start: NaiveDate, end: NaiveDate) -> i64 {
    end.signed_duration_since(start)
        .num_days()
        .saturating_add(1)
}

fn phase7_share_float_covered_days_from_windows(
    mut windows: Vec<(NaiveDate, NaiveDate)>,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    windows.sort_by_key(|(window_start, window_end)| (*window_start, *window_end));
    let mut covered_days = 0i64;
    let mut current_start: Option<NaiveDate> = None;
    let mut current_end: Option<NaiveDate> = None;

    for (window_start, window_end) in windows {
        let clipped_start = window_start.max(start);
        let clipped_end = window_end.min(end);
        if clipped_start > clipped_end {
            continue;
        }

        match (current_start, current_end) {
            (Some(active_start), Some(active_end))
                if clipped_start <= active_end + Duration::days(1) =>
            {
                current_start = Some(active_start);
                current_end = Some(active_end.max(clipped_end));
            }
            (Some(active_start), Some(active_end)) => {
                covered_days += phase7_share_float_expected_days(active_start, active_end);
                current_start = Some(clipped_start);
                current_end = Some(clipped_end);
            }
            _ => {
                current_start = Some(clipped_start);
                current_end = Some(clipped_end);
            }
        }
    }

    if let (Some(active_start), Some(active_end)) = (current_start, current_end) {
        covered_days += phase7_share_float_expected_days(active_start, active_end);
    }

    covered_days
}

fn phase7_share_float_feature_readiness(
    row_count: i64,
    covered_days: i64,
    expected_days: i64,
    late_or_invalid_count: i64,
) -> &'static str {
    if expected_days > 0 && covered_days < expected_days {
        "needs_float_date_backfill"
    } else if row_count <= 0 {
        "full_sync_empty_review"
    } else if late_or_invalid_count > 0 {
        "ready_for_pit_feature_factory_with_late_exclusion"
    } else {
        "ready_for_pit_feature_factory"
    }
}

fn phase7_coverage_runner_sources(requested: &[String]) -> Result<Vec<String>, String> {
    let raw_sources: Vec<String> = if requested.is_empty() {
        PHASE7_COVERAGE_RUNNER_DEFAULT_SOURCES
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
    let mut sources = Vec::new();
    for source in raw_sources {
        if !PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES.contains(&source.as_str()) {
            return Err(format!(
                "unsupported coverage runner source: {}; supported sources are {}",
                source,
                PHASE7_COVERAGE_RUNNER_ALLOWED_SOURCES.join(", ")
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

fn phase7_coverage_runner_should_plan_source(readiness: &str) -> bool {
    !matches!(
        readiness,
        "partial_feature_candidate" | "ready_for_feature_factory"
    )
}

fn phase7_coverage_runner_should_plan_source_for_target(
    readiness: Option<&str>,
    coverage_ratio: Option<f64>,
    target_ratio: f64,
    stop_at_partial_gate: bool,
) -> bool {
    if stop_at_partial_gate {
        return readiness
            .map(phase7_coverage_runner_should_plan_source)
            .unwrap_or(true);
    }
    let ratio = coverage_ratio.unwrap_or(0.0);
    ratio < target_ratio
}

fn phase7_attempt_coverage_readiness(
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

fn phase7_coverage_runner_source_state_for_window(
    audit: &Value,
    window_attempts: Option<&BTreeMap<String, i64>>,
) -> (BTreeMap<String, String>, BTreeMap<String, f64>) {
    let mut readiness_by_source = BTreeMap::new();
    let mut coverage_by_source = BTreeMap::new();
    let reference_symbols = audit
        .get("listed_stock_count")
        .and_then(|count| count.as_i64())
        .unwrap_or_default();
    if let Some(sources) = audit
        .get("optional_data_sources")
        .and_then(|sources| sources.as_array())
    {
        for source in sources {
            if let Some(name) = source.get("source").and_then(|name| name.as_str()) {
                if let Some(attempted_symbols) = window_attempts
                    .and_then(|attempts| attempts.get(name))
                    .copied()
                {
                    readiness_by_source.insert(
                        name.to_string(),
                        phase7_attempt_coverage_readiness(
                            name,
                            attempted_symbols,
                            reference_symbols,
                        )
                        .to_string(),
                    );
                    coverage_by_source.insert(
                        name.to_string(),
                        if reference_symbols > 0 {
                            attempted_symbols as f64 / reference_symbols as f64
                        } else {
                            0.0
                        },
                    );
                    continue;
                }
                if let Some(readiness) = source
                    .get("feature_readiness")
                    .and_then(|readiness| readiness.as_str())
                {
                    readiness_by_source.insert(name.to_string(), readiness.to_string());
                }
                if let Some(ratio) = source
                    .get("symbol_coverage_ratio")
                    .and_then(|ratio| ratio.as_f64())
                {
                    coverage_by_source.insert(name.to_string(), ratio);
                }
            }
        }
    }

    if let Some(rows) = audit
        .get("financial_coverage")
        .and_then(|sources| sources.as_array())
    {
        let mut financial_ready = "ready_for_feature_factory".to_string();
        let mut financial_ratio = 1.0_f64;
        let mut has_financial_rows = false;
        for row in rows {
            if let (Some(name), Some(coverage_grade)) = (
                row.get("name").and_then(|name| name.as_str()),
                row.get("coverage_grade").and_then(|grade| grade.as_str()),
            ) {
                if matches!(
                    name,
                    "market_financial_statement" | "market_financial_indicator"
                ) {
                    has_financial_rows = true;
                    if let Some(ratio) = row
                        .get("symbol_coverage_ratio")
                        .and_then(|ratio| ratio.as_f64())
                    {
                        financial_ratio = financial_ratio.min(ratio);
                    }
                    let readiness = phase7_financial_source_readiness(coverage_grade).to_string();
                    if phase7_coverage_runner_should_plan_source(&readiness) {
                        financial_ready = readiness;
                    }
                }
            }
        }
        if has_financial_rows {
            if let Some(attempted_symbols) = window_attempts
                .and_then(|attempts| attempts.get("financial"))
                .copied()
            {
                readiness_by_source.insert(
                    "financial".to_string(),
                    phase7_attempt_coverage_readiness(
                        "financial",
                        attempted_symbols,
                        reference_symbols,
                    )
                    .to_string(),
                );
                coverage_by_source.insert(
                    "financial".to_string(),
                    if reference_symbols > 0 {
                        attempted_symbols as f64 / reference_symbols as f64
                    } else {
                        0.0
                    },
                );
            } else {
                readiness_by_source.insert("financial".to_string(), financial_ready);
                coverage_by_source.insert("financial".to_string(), financial_ratio);
            }
        }
    }

    (readiness_by_source, coverage_by_source)
}

fn phase7_financial_source_readiness(coverage_grade: &str) -> &'static str {
    match coverage_grade {
        "broad" => "ready_for_feature_factory",
        "partial" => "partial_feature_candidate",
        "unknown_reference" => "coverage_reference_missing",
        "missing" => "needs_sync",
        _ => "undercovered_do_not_train",
    }
}

fn phase7_optional_source_sync_sources(requested: &[String]) -> Result<Vec<String>, String> {
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
    let mut sources = Vec::new();
    for source in raw_sources {
        if !PHASE7_OPTIONAL_SOURCE_SYNC_ALLOWED.contains(&source.as_str()) {
            return Err(format!(
                "unsupported optional source: {}; supported sources are {}",
                source,
                PHASE7_OPTIONAL_SOURCE_SYNC_ALLOWED.join(", ")
            ));
        }
        if seen.insert(source.clone()) {
            sources.push(source);
        }
    }
    Ok(sources)
}

fn phase7_optional_source_table(source: &str) -> Option<&'static str> {
    match source {
        "cashflow" => Some("market_stock_cashflow"),
        "dividend" => Some("market_stock_dividend"),
        "repurchase" => Some("market_stock_repurchase"),
        "forecast" => Some("market_stock_forecast"),
        "express" => Some("market_stock_express"),
        "disclosure_date" => Some("market_stock_disclosure_date"),
        "share_float" => Some("market_stock_share_float"),
        _ => None,
    }
}

fn classify_tushare_permission_error(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if error.contains("权限")
        || error.contains("积分")
        || lower.contains("permission")
        || lower.contains("privilege")
        || lower.contains("2002")
    {
        "permission_denied"
    } else if lower.contains("token") || error.contains("TUSHARE_TOKEN") {
        "auth_error"
    } else {
        "error"
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

fn phase7_coverage_grade(symbols: i64, reference_symbols: i64) -> &'static str {
    if symbols <= 0 {
        "missing"
    } else if reference_symbols <= 0 {
        "unknown_reference"
    } else {
        let ratio = symbols as f64 / reference_symbols as f64;
        if ratio >= 0.80 {
            "broad"
        } else if ratio >= 0.30 {
            "partial"
        } else {
            "undercovered"
        }
    }
}

fn phase7_optional_source_readiness(
    table_exists: bool,
    rows: i64,
    symbols: i64,
    reference_symbols: i64,
) -> &'static str {
    if !table_exists {
        "schema_missing"
    } else if rows <= 0 {
        "needs_sync"
    } else {
        match phase7_coverage_grade(symbols, reference_symbols) {
            "broad" => "ready_for_feature_factory",
            "partial" => "partial_feature_candidate",
            "unknown_reference" => "coverage_reference_missing",
            _ => "sample_only_do_not_train",
        }
    }
}

fn phase7_optional_source_next_step(readiness: &str) -> &'static str {
    match readiness {
        "ready_for_feature_factory" => "build_pit_feature_factory",
        "partial_feature_candidate" => "run_feature_smoke_then_expand_coverage",
        "sample_only_do_not_train" => "expand_sync_before_training",
        "needs_sync" => "run_bounded_sync_smoke_then_coverage_audit",
        "schema_missing" => "apply_phase7_optional_financial_sources_schema",
        _ => "repair_reference_coverage_before_feature_factory",
    }
}

fn phase7_coverage_json(
    name: String,
    rows: i64,
    min_date: Option<NaiveDate>,
    max_date: Option<NaiveDate>,
    symbols: i64,
    reference_symbols: i64,
) -> Value {
    json!({
        "name": name,
        "rows": rows,
        "symbols": symbols,
        "reference_symbols": reference_symbols,
        "symbol_coverage_ratio": if reference_symbols > 0 {
            Some(symbols as f64 / reference_symbols as f64)
        } else {
            None
        },
        "min_date": min_date,
        "max_date": max_date,
        "coverage_grade": phase7_coverage_grade(symbols, reference_symbols),
    })
}

fn phase7_coverage_rows_to_json(
    rows: Vec<(String, i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    reference_symbols: i64,
) -> Vec<Value> {
    rows.into_iter()
        .map(|(name, rows, min_date, max_date, symbols)| {
            phase7_coverage_json(name, rows, min_date, max_date, symbols, reference_symbols)
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct Phase7OptionalSourceSpec {
    source: &'static str,
    table: &'static str,
    next_feature: &'static str,
}

fn phase7_optional_source_specs() -> &'static [Phase7OptionalSourceSpec] {
    &[
        Phase7OptionalSourceSpec {
            source: "cashflow",
            table: "market_stock_cashflow",
            next_feature: "cashflow_quality_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "dividend",
            table: "market_stock_dividend",
            next_feature: "dividend_stability_quality_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "repurchase",
            table: "market_stock_repurchase",
            next_feature: "repurchase_event_capital_return_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "forecast",
            table: "market_stock_forecast",
            next_feature: "event_post_announcement_return_curve_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "express",
            table: "market_stock_express",
            next_feature: "event_post_announcement_return_curve_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "disclosure_date",
            table: "market_stock_disclosure_date",
            next_feature: "event_disclosure_schedule_pit_features",
        },
        Phase7OptionalSourceSpec {
            source: "share_float",
            table: "market_stock_share_float",
            next_feature: "unlock_supply_pressure_pit_features",
        },
    ]
}

#[derive(Debug, Clone, Copy)]
struct Phase7MarketLevelSourceAudit {
    data_rows: i64,
    min_trade_date: Option<NaiveDate>,
    latest_trade_date: Option<NaiveDate>,
    open_day_lag: Option<i64>,
}

#[derive(Debug, Clone)]
struct Phase7MarketLevelSyncAudit {
    task_id: String,
    task_type: String,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    status: String,
    total_count: i32,
    success_count: i32,
    failed_count: i32,
    error_message: Option<String>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy)]
struct Phase7BlockTradeSourceAudit {
    data_rows: i64,
    symbols: i64,
    covered_trade_days: i64,
    open_days_in_range: i64,
    min_trade_date: Option<NaiveDate>,
    latest_trade_date: Option<NaiveDate>,
    min_available_at: Option<NaiveDate>,
    latest_available_at: Option<NaiveDate>,
    pit_violation_rows: i64,
}

#[derive(Debug, Clone, Copy)]
struct Phase7IndustryMembershipSourceAudit {
    data_rows: i64,
    symbols: i64,
    index_codes: i64,
    current_active_stock_symbols: i64,
    current_covered_stock_symbols: i64,
    min_in_date: Option<NaiveDate>,
    latest_in_date: Option<NaiveDate>,
    min_out_date: Option<NaiveDate>,
    latest_out_date: Option<NaiveDate>,
    min_available_at: Option<NaiveDate>,
    latest_available_at: Option<NaiveDate>,
    pit_violation_rows: i64,
    invalid_interval_rows: i64,
    duplicate_key_rows: i64,
}

fn phase7_market_level_sync_dataset(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("moneyflow_hsgt"),
        _ => None,
    }
}

fn phase7_market_level_source_readiness(stats: &Phase7MarketLevelSourceAudit) -> &'static str {
    if stats.data_rows <= 0 || stats.latest_trade_date.is_none() {
        return "market_level_needs_sync";
    }
    if stats.open_day_lag.unwrap_or(i64::MAX) > 2 {
        return "market_level_stale_needs_sync";
    }
    "market_level_ready_for_regime_feature"
}

fn phase7_market_level_zero_row_sync_covers_gap(
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

fn phase7_date_json(date: Option<NaiveDate>) -> Value {
    date.map(|date| json!(date.to_string()))
        .unwrap_or(Value::Null)
}

fn phase7_datetime_json(date: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    date.map(|date| json!(date.to_rfc3339()))
        .unwrap_or(Value::Null)
}

fn phase7_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    if denominator <= 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn phase7_p315_sync_start_date(
    stats: &Phase7MarketLevelSourceAudit,
    today: NaiveDate,
) -> NaiveDate {
    stats
        .latest_trade_date
        .map(|date| (date + Duration::days(1)).min(today))
        .unwrap_or(today)
}

fn phase7_new_alpha_candidate_sources() -> Vec<Value> {
    vec![
        json!({
            "source": "market_margin_regime",
            "current_tables": ["market_margin"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "融资融券汇总是交易所级时间序列，不能直接形成股票横截面排序 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "market_moneyflow_hsgt_regime",
            "current_tables": ["market_moneyflow_hsgt"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "北向资金汇总是市场级资金流，适合 regime/risk budget，不适合作为单股横截面 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "industry_prosperity_proxy",
            "current_tables": ["market_stock", "market_financial_indicator", "market_stock_daily_bar", "market_stock_moneyflow"],
            "candidate_raw_sources": ["tushare:index_classify", "tushare:index_member"],
            "admission_scope": "broad_base_proxy_candidate",
            "alpha_admission_gate": industry_prosperity_alpha_admission_policy_static(),
            "readiness": "industry_membership_permission_probe_required",
            "pit_boundary": "current market_stock.industry is static and cannot be treated as historical PIT industry classification",
            "why_not_trainable_now": "行业 PIT 原始源接入后仍需通过全历史 membership snapshot/coverage 审计和 P3.10 诊断，不能直接进入训练",
            "next_step": "run_industry_membership_permission_smoke_then_schema_available_at_audit"
        }),
        json!({
            "source": "block_trade_supply_demand",
            "current_tables": ["market_stock_block_trade"],
            "admission_scope": "stopped_same_family_after_p310",
            "readiness": "stopped_after_p310_economics_weak",
            "pit_boundary": "must persist announcement/trade publication date as available_at before any event-window feature",
            "why_not_trainable_now": "大宗交易供需源数据/PIT 可用，但 P3.10 显示覆盖偏窄、alpha economics 弱；不得继续同族扩参或直接 WFA",
            "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
        }),
        json!({
            "source": "equity_pledge_pressure",
            "current_tables": [],
            "candidate_raw_sources": ["tushare:pledge_stat", "tushare:pledge_detail"],
            "admission_scope": "new_p320_schema_contract_candidate",
            "readiness": "permission_smoke_passed_schema_contract_ready",
            "pit_boundary": "pledge_detail.ann_date is native available_at candidate; pledge_stat.end_date is a measurement date and cannot be used alone as availability",
            "why_not_trainable_now": "新候选源已完成生产 permission-smoke 和 schema contract，但尚未人工审查/应用 schema、全历史 bounded sync、coverage/readiness 或 P3.10",
            "next_step": "review_apply_equity_pledge_schema_then_bounded_sync_plan"
        }),
        json!({
            "source": "equity_incentive_execution_quality",
            "current_tables": [],
            "admission_scope": "raw_source_onboarding_required",
            "readiness": "schema_and_client_missing",
            "pit_boundary": "must persist disclosure/announcement date as available_at and execution periods as event attributes",
            "why_not_trainable_now": "当前没有股权激励原始表、Tushare client、同步账本或 PIT 可得日审计",
            "next_step": "add_permission_smoke_then_schema_and_bounded_sync"
        }),
    ]
}

fn phase7_futures_price_chain_schema_contract() -> Value {
    json!({
        "source_id": "futures_price_chain",
        "stage": "P3.19J",
        "mode": "read_only_schema_mapping_pit_contract",
        "source_status": "permission_smoke_available_schema_contract_defined",
        "admission_decision": "schema_mapping_available_at_audit_required_before_sync",
        "raw_sources": [
            {
                "api": "fut_daily",
                "doc": "https://tushare.pro/wctapi/documents/138.md",
                "semantics": "daily futures OHLC settlement volume and open-interest",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_wsr",
                "doc": "https://tushare.pro/wctapi/documents/140.md",
                "semantics": "warehouse receipt inventory and daily inventory change",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_holding",
                "doc": "https://tushare.pro/wctapi/documents/139.md",
                "semantics": "broker-level daily volume long and short holding ranking",
                "native_time_key": "trade_date"
            }
        ],
        "raw_tables": [
            {
                "table": "market_futures_daily",
                "natural_key": ["ts_code", "trade_date"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["close", "settle", "vol", "amount", "oi", "oi_chg"],
                "pit_rule": "available_at must be >= trade_date and downstream features must filter available_at <= stock_trade_date"
            },
            {
                "table": "market_futures_warehouse_receipt",
                "natural_key": ["trade_date", "symbol", "exchange", "warehouse"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["pre_vol", "vol", "vol_chg", "unit"],
                "pit_rule": "warehouse inventory changes are usable only after source publication"
            },
            {
                "table": "market_futures_holding_rank",
                "natural_key": ["trade_date", "symbol", "exchange", "broker"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg"],
                "pit_rule": "broker position changes are usable only after source publication"
            }
        ],
        "mapping_tables": [
            {
                "table": "market_futures_product_exposure_mapping_pit",
                "natural_key": ["product_symbol", "exposure_type", "exposure_code", "valid_from", "mapping_version"],
                "required_fields": ["product_symbol", "exposure_type", "exposure_code", "direction", "weight", "valid_from", "valid_to", "available_at", "source", "mapping_version"],
                "allowed_exposure_types": ["sw_industry", "stock_symbol"],
                "pit_rule": "mapping.available_at <= stock_trade_date; mapping rows must be versioned and must not be derived from future stock returns or future factor performance",
                "preferred_first_pass": "product-to-sw-industry mapping joined to market_stock_industry_membership_pit; direct stock_symbol mapping requires stronger evidence"
            },
            {
                "table": "market_futures_product_exclusion_gate_pit",
                "natural_key": ["product_symbol", "gate_scope", "valid_from", "gate_version"],
                "required_fields": ["product_symbol", "gate_scope", "reason_code", "valid_from", "valid_to", "available_at", "source", "gate_version", "evidence"],
                "allowed_reason_codes": ["financial_index_future", "interest_rate_future", "non_industry_derivative", "ambiguous_product_symbol", "insufficient_industry_evidence"],
                "pit_rule": "exclusion gates are admission controls: excluded products must not be forced into product-to-industry mappings or downstream factors",
                "preferred_first_pass": "pre-register non-industry derivatives such as equity index and treasury bond futures as excluded before product-to-SW-industry review"
            }
        ],
        "pit_policy": {
            "native_available_at_candidate": "trade_date_after_market_close",
            "source_published_at_required": true,
            "intraday_stock_decision_rule": "use_previous_available_futures_trade_date_until_source_published_at_is_audited",
            "prohibited": [
                "using same-day futures close or warehouse data in an intraday stock rebalance before publication",
                "using static hindsight product-to-stock mapping without available_at",
                "backfilling exposure weights from later performance or later industry reclassification"
            ]
        },
        "coverage_audit_required": {
            "full_history_range": "2014-01-01_to_latest_complete_trade_date",
            "required_breakdowns": ["year", "endpoint", "product_symbol", "exchange", "mapped_industry", "stock_market_scope"],
            "minimum_before_p310": "coverage/readiness green or explicitly gated market/date scope"
        },
        "promotion_gate": {
            "schema_status": "not_created",
            "sync_status": "not_started",
            "coverage_status": "not_started",
            "p310_status": "not_started",
            "wfa_status": "blocked_until_p310_passes",
            "v19_train_selection": "blocked"
        },
        "ddl_path": "sql/phase7_futures_price_chain_source.sql",
        "next_step": "create_schema_then_run_bounded_full_history_sync_and_coverage_readiness_audit"
    })
}

fn phase7_equity_pledge_schema_contract() -> Value {
    json!({
        "audit_version": "p3.20b-equity-pledge-pressure-schema-contract-v1",
        "source_id": "equity_pledge_pressure",
        "stage": "P3.20B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_equity_pledge_source.sql",
        "raw_sources": [
            {
                "api": "pledge_stat",
                "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                "semantics": "stock_equity_pledge_stat_snapshot",
                "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                "required_fields": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                "minimum_points": 2000
            },
            {
                "api": "pledge_detail",
                "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                "semantics": "stock_equity_pledge_detail_events",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_pledge_stat",
                "natural_key": ["symbol", "end_date"],
                "required_fields": ["symbol", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio", "available_at", "source_published_at", "raw_payload", "source", "data_version_id"],
                "pit_rule": "available_at must be >= end_date. Default sync may only use conservative end_date+1day unless a source_published_at audit proves earlier availability.",
                "training_gate": "stat snapshots cannot enter factor construction until joined to detail announcements or audited with a conservative availability lag."
            },
            {
                "table": "market_stock_pledge_detail",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "pledge_amount", "pledge_start_date", "pledge_end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date. Downstream features must use available_at <= stock_trade_date and must preserve multiple pledge rows on the same announcement date.",
                "nullable_source_fields": ["pledge_start_date", "pledge_end_date", "release_date", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio"],
                "duplicate_policy": "preserve source rows by source_row_hash; do not collapse same-day multiple pledges before audit. Nullable source fields must remain nullable and must not be promoted into the primary key."
            }
        ],
        "available_at_policy": {
            "pledge_detail": "ann_date is native available_at candidate and must be persisted as available_at.",
            "pledge_stat": "end_date is a measurement date, not disclosure availability; use only after conservative lag or detail-derived audit.",
            "intraday_trading": "without verified source publication timestamps, same-day pledge updates are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_symbol_ann_date_breakdown",
            "detail_available_at_null_or_future_leak_count",
            "detail_release_before_start_count",
            "detail_ratio_out_of_range_count",
            "stat_pledge_ratio_out_of_range_count",
            "stat_end_date_coverage_and_lag_policy",
            "symbol_breadth_vs_main_chinext_non_st",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}

fn phase7_shareholder_structure_schema_contract() -> Value {
    json!({
        "audit_version": "p3.21b-shareholder-structure-schema-contract-v1",
        "source_id": "shareholder_structure",
        "stage": "P3.21B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_shareholder_structure_source.sql",
        "raw_sources": [
            {
                "api": "stk_holdernumber",
                "semantics": "stock_shareholder_count_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_num"],
                "minimum_points": 2000
            },
            {
                "api": "top10_holders",
                "semantics": "top10_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "top10_floatholders",
                "semantics": "top10_float_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "stk_holdertrade",
                "semantics": "major_holder_or_insider_increase_decrease_event",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "begin_date", "close_date"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_holder_number",
                "natural_key": ["symbol", "ann_date", "end_date"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_num", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; ann_date before end_date is retained as a raw source anomaly and blocks admission until repaired, excluded, or gated."
            },
            {
                "table": "market_stock_top10_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; downstream features must filter available_at <= stock_trade_date and exclude or gate ann_date before end_date anomalies."
            },
            {
                "table": "market_stock_top10_float_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; do not infer float-holder concentration before disclosure, and treat ann_date before end_date as a blocking raw anomaly."
            },
            {
                "table": "market_stock_holder_trade",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share", "begin_date", "close_date", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; begin_date/close_date are event attributes and must not move availability earlier."
            }
        ],
        "available_at_policy": {
            "default": "available_at equals native ann_date for all four shareholder_structure raw tables; end_date is a measurement period only.",
            "period_snapshot_rule": "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated.",
            "raw_landing": "raw tables accept native source rows for auditability; admission gates, not insert constraints, decide whether rows can feed factor work.",
            "intraday_trading": "without verified source publication timestamps, same-day shareholder disclosures are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_source_symbol_ann_date_breakdown",
            "symbol_breadth_vs_main_chinext_non_st",
            "ann_date_before_end_date_count",
            "available_at_before_ann_date_count",
            "holder_ratio_out_of_range_count",
            "holder_trade_interval_violation_count",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
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

fn shareholder_structure_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            "market_stock_holder_number",
            vec![
                "market_stock_holder_number_pkey",
                "market_stock_holder_number_available_at_check",
            ],
        ),
        (
            "market_stock_top10_holders",
            vec![
                "market_stock_top10_holders_pkey",
                "market_stock_top10_holders_available_at_check",
            ],
        ),
        (
            "market_stock_top10_float_holders",
            vec![
                "market_stock_top10_float_holders_pkey",
                "market_stock_top10_float_holders_available_at_check",
            ],
        ),
        (
            "market_stock_holder_trade",
            vec![
                "market_stock_holder_trade_pkey",
                "market_stock_holder_trade_available_at_check",
                "market_stock_holder_trade_interval_check",
            ],
        ),
    ]
}

fn decide_equity_pledge_readiness(
    schema_passed: bool,
    stat_rows: i64,
    detail_rows: i64,
    pit_violation_rows: i64,
) -> Value {
    let raw_rows = stat_rows + detail_rows;
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_readiness_audit_required_before_p310",
            "run_year_symbol_ann_date_coverage_and_duplicate_audit_before_factor_builder",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_readiness_passes",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "stat_rows": stat_rows,
        "detail_rows": detail_rows,
        "raw_rows": raw_rows,
        "pit_violation_rows": pit_violation_rows,
        "next_step": next_step,
    })
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

fn decide_shareholder_structure_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
    data_quality_violation_rows: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_RAW_ROWS_FOR_P310: i64 = 250_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "review_period_snapshot_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if data_quality_violation_rows > 0 {
        (
            "raw_quality_failed",
            "raw_quality_failed",
            "review_ratio_and_interval_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_shareholder_structure_source_row_hash_duplicates_before_feature_builder",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "data_quality_violation_rows": data_quality_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
        "next_step": next_step,
    })
}

fn decide_shareholder_structure_strict_low_fanout_gate(
    schema_passed: bool,
    admissible_rows: i64,
    symbol_coverage_ratio: f64,
    duplicate_source_row_hash_count: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_ADMISSIBLE_ROWS_FOR_P310: i64 = 250_000;

    let (status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_strict_low_fanout_gate",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if admissible_rows <= 0 {
        (
            "admissible_rows_missing",
            "bounded_sync_required_before_strict_low_fanout_gate",
            "run_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_admissible_rows_failed",
            "duplicate_admissible_rows_failed",
            "inspect_strict_low_fanout_duplicate_source_hashes_before_p310",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || admissible_rows < MIN_ADMISSIBLE_ROWS_FOR_P310
    {
        (
            "undercovered_or_too_sparse",
            "bounded_sample_passed_needs_more_admissible_history",
            "increase_strict_low_fanout_admissible_coverage_before_p310",
        )
    } else {
        (
            "strict_low_fanout_ready_for_p310_diagnostics",
            "strict_low_fanout_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics_with_shareholder_structure_gate",
        )
    };

    json!({
        "gate_id": SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
        "source_scope": "low_fanout_holder_number_holder_trade",
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "strict_low_fanout_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_strict_low_fanout_gate_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "admissible_rows": admissible_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
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

fn shareholder_structure_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batches: Vec<ShareholderStructureSyncPlanBatch>,
) -> Value {
    const SAFE_FULL_RANGE_UNIT_LIMIT: i64 = 50_000;

    let batch_values = batches
        .iter()
        .map(|batch| {
            let global_ann_date_units = batch.quarter_count * 2;
            let symbol_quarter_units = batch.symbol_count * batch.quarter_count * 2;
            let estimated_units = global_ann_date_units + symbol_quarter_units;
            json!({
                "year": batch.year,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "symbol_count": batch.symbol_count,
                "quarter_count": batch.quarter_count,
                "global_ann_date_units": global_ann_date_units,
                "symbol_quarter_units": symbol_quarter_units,
                "estimated_units": estimated_units,
                "recommended_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("shareholder-structure-{}", batch.year),
                    "background": true
                },
                "recommended_low_fanout_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["holder_number", "holder_trade"],
                    "data_version_id": format!("shareholder-structure-{}-low-fanout", batch.year),
                    "background": true
                },
                "recommended_top10_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["top10_holders", "top10_float_holders"],
                    "data_version_id": format!("shareholder-structure-{}-top10", batch.year),
                    "background": true
                }
            })
        })
        .collect::<Vec<_>>();

    let estimated_total_units = batches
        .iter()
        .map(|batch| batch.quarter_count * 2 + batch.symbol_count * batch.quarter_count * 2)
        .sum::<i64>();
    let safe_to_run_full_range = estimated_total_units <= SAFE_FULL_RANGE_UNIT_LIMIT;

    json!({
        "audit_version": "p3.21c-shareholder-structure-sync-plan-v1",
        "source_id": "shareholder_structure",
        "mode": "read_only_bounded_sync_plan",
        "date_range": {
            "start_date": start.format("%Y-%m-%d").to_string(),
            "end_date": end.format("%Y-%m-%d").to_string(),
        },
        "batch_count": batch_values.len(),
        "estimated_total_units": estimated_total_units,
        "safe_full_range_unit_limit": SAFE_FULL_RANGE_UNIT_LIMIT,
        "safe_to_run_full_range": safe_to_run_full_range,
        "recommended_batch_granularity": if safe_to_run_full_range { "full_range" } else { "year" },
        "unit_model": {
            "global_ann_date_units": "quarter_count * 2 for holder_number and holder_trade",
            "symbol_quarter_units": "symbol_count * quarter_count * 2 for top10_holders and top10_floatholders",
            "warning": "plan is read-only and does not call Tushare"
        },
        "batches": batch_values,
        "prohibited": [
            "do_not_run_2014_2026_full_range_without_reviewing_estimated_units",
            "do_not_enter_factor_p310_wfa_until_coverage_readiness_and_anomaly_gates_pass"
        ]
    })
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

fn decide_equity_pledge_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.30;
    const MIN_RAW_ROWS_FOR_P310: i64 = 10_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_pledge_detail_source_row_hash_duplicates_before_feature_builder",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_equity_pledge_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "next_step": next_step,
    })
}

async fn build_equity_pledge_pressure_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
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

fn decide_futures_price_chain_readiness(
    schema_passed: bool,
    raw_rows: i64,
    mapping_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "schema_created_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_then_coverage_readiness_audit",
        )
    } else if mapping_rows == 0 {
        (
            "created",
            "raw_synced_mapping_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_before_factor_backfill",
        )
    } else {
        (
            "created",
            "raw_and_mapping_present",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
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
        "mapping_rows": mapping_rows,
        "next_step": next_step,
    })
}

#[cfg(test)]
fn futures_price_chain_normalize_raw_product_symbol(raw: &str) -> Option<String> {
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

#[cfg(test)]
fn futures_price_chain_product_symbol_from_daily_ts_code(ts_code: &str) -> Option<String> {
    let value = ts_code.trim();
    let product = value
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>();
    let month_code = value
        .chars()
        .skip(product.len())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if month_code.len() != 4 {
        return None;
    }
    if value.chars().nth(product.len() + month_code.len()) != Some('.') {
        return None;
    }
    futures_price_chain_normalize_raw_product_symbol(&product)
}

fn decide_futures_price_chain_mapping_audit(
    schema_passed: bool,
    raw_product_count: i64,
    mapped_product_count: i64,
    excluded_product_count: i64,
    missing_product_count: i64,
    invalid_interval_rows: i64,
    mapping_pit_violation_rows: i64,
    unsupported_exposure_rows: i64,
    invalid_exclusion_interval_rows: i64,
    exclusion_pit_violation_rows: i64,
) -> Value {
    let covered_product_count = mapped_product_count + excluded_product_count;
    let (mapping_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_mapping_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_mapping_audit",
        )
    } else if raw_product_count == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_mapping_audit",
            "run_bounded_futures_price_chain_sync_before_mapping_audit",
        )
    } else if covered_product_count == 0 {
        (
            "mapping_and_exclusion_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_or_exclusion_gate_before_factor_backfill",
        )
    } else if unsupported_exposure_rows > 0
        || invalid_interval_rows > 0
        || mapping_pit_violation_rows > 0
        || invalid_exclusion_interval_rows > 0
        || exclusion_pit_violation_rows > 0
    {
        (
            "mapping_integrity_failed",
            "mapping_integrity_failed_before_feature_or_p310",
            "repair_mapping_or_exclusion_type_interval_and_available_at_before_coverage_audit",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else if mapped_product_count == 0 {
        (
            "all_raw_products_excluded",
            "all_products_excluded_no_trainable_price_chain_source",
            "stop_futures_price_chain_factor_source_or_add_evidence_backed_industry_mappings",
        )
    } else {
        (
            "mapping_coverage_ready",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
        )
    };

    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "schema_status": if schema_passed { "created" } else { "missing_or_invalid" },
        "sync_status": if raw_product_count > 0 { "raw_synced" } else { "not_started" },
        "mapping_status": mapping_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_mapping_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_product_count": raw_product_count,
        "mapped_product_count": mapped_product_count,
        "excluded_product_count": excluded_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "invalid_interval_rows": invalid_interval_rows,
        "mapping_pit_violation_rows": mapping_pit_violation_rows,
        "unsupported_exposure_rows": unsupported_exposure_rows,
        "invalid_exclusion_interval_rows": invalid_exclusion_interval_rows,
        "exclusion_pit_violation_rows": exclusion_pit_violation_rows,
        "next_step": next_step,
    })
}

fn decide_futures_price_chain_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    raw_product_count: i64,
    covered_product_count: i64,
    missing_product_count: i64,
    raw_pit_violation_rows: i64,
    failed_sync_attempt_count: i64,
) -> Value {
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };
    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_coverage_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_coverage_audit",
        )
    } else if raw_rows == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_before_coverage_audit",
        )
    } else if raw_pit_violation_rows > 0 {
        (
            "raw_pit_integrity_failed",
            "raw_pit_integrity_failed_before_feature_or_p310",
            "repair_raw_available_at_before_mapping_or_p310",
        )
    } else if failed_sync_attempt_count > 0 {
        (
            "sync_attempt_failures_present",
            "sync_attempt_failures_require_retry_before_feature_or_p310",
            "retry_or_explain_failed_futures_price_chain_sync_attempts_before_p310",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "coverage_mapping_ready",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "raw_rows": raw_rows,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "raw_pit_violation_rows": raw_pit_violation_rows,
        "failed_sync_attempt_count": failed_sync_attempt_count,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_mapping_and_coverage_readiness_pass"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
}

fn futures_price_chain_coverage_promotion_gate(decision: &Value) -> Value {
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_mapping_and_coverage_readiness_pass");
    let p310_ready = p310_status == "ready_for_p310_diagnostics";

    json!({
        "factor_builder": if p310_ready {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_coverage_mapping_and_pit_pass"
        },
        "p310_status": p310_status,
        "wfa_status": "blocked",
        "v19_train_selection": "blocked"
    })
}

fn futures_price_chain_raw_product_summary_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        product_symbol,
        COUNT(*)::bigint AS raw_rows,
        string_agg(DISTINCT endpoint, ',' ORDER BY endpoint) AS endpoints,
        string_agg(DISTINCT exchange_key, ',' ORDER BY exchange_key) AS exchanges,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY product_symbol
    ORDER BY product_symbol
    "#
}

fn futures_price_chain_coverage_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    raw AS (
        SELECT
            endpoint,
            EXTRACT(YEAR FROM trade_date)::int AS trade_year,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        trade_year,
        product_symbol,
        exchange_key,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint, trade_year, product_symbol, exchange_key
    ORDER BY trade_year, endpoint, product_symbol, exchange_key
    "#
}

fn futures_price_chain_sync_attempt_breakdown_sql() -> &'static str {
    r#"
    WITH futures_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND exchange IN ('SHFE', 'DCE', 'CZCE', 'CFFEX', 'INE')
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    fallback_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    open_calendar AS (
        SELECT trade_date FROM futures_calendar
        UNION
        SELECT trade_date FROM fallback_calendar
        WHERE NOT EXISTS (SELECT 1 FROM futures_calendar)
    ),
    attempts AS (
        SELECT
            attempt.*,
            open_calendar.trade_date IS NOT NULL AS is_open_trade_date
        FROM data_sync_attempt attempt
        LEFT JOIN open_calendar
          ON attempt.start_date = open_calendar.trade_date
         AND attempt.end_date = open_calendar.trade_date
        WHERE source IN (
            'futures_price_chain_daily',
            'futures_price_chain_wsr',
            'futures_price_chain_holding'
        )
          AND ($1::date IS NULL OR end_date >= $1::date)
          AND ($2::date IS NULL OR start_date <= $2::date)
    )
    SELECT
        source,
        status,
        COUNT(*) FILTER (WHERE is_open_trade_date)::bigint AS attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE is_open_trade_date), 0)::bigint AS row_count,
        MIN(start_date) FILTER (WHERE is_open_trade_date) AS min_start_date,
        MAX(end_date) FILTER (WHERE is_open_trade_date) AS max_end_date,
        COUNT(*) FILTER (WHERE is_open_trade_date AND error_message IS NOT NULL)::bigint AS error_attempt_count,
        COUNT(*) FILTER (WHERE NOT is_open_trade_date)::bigint AS non_open_attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE NOT is_open_trade_date), 0)::bigint AS non_open_row_count
    FROM attempts
    GROUP BY source, status
    ORDER BY source, status
    "#
}

fn futures_price_chain_raw_endpoint_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT product_symbol)::bigint AS product_count,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint
    ORDER BY endpoint
    "#
}

fn futures_price_chain_mapping_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS mapping_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS mapping_table_product_count,
        COUNT(*) FILTER (
            WHERE exposure_type NOT IN ('sw_industry', 'stock_symbol')
        )::bigint AS unsupported_exposure_rows,
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
    "#
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

fn futures_price_chain_exclusion_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS exclusion_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS exclusion_table_product_count,
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

fn futures_price_chain_industry_targets_sql() -> &'static str {
    r#"
    SELECT
        index_code,
        index_name,
        industry_code,
        industry_name,
        COUNT(DISTINCT symbol)::bigint AS current_or_historical_symbols,
        MIN(in_date) AS min_in_date,
        MAX(in_date) AS max_in_date,
        MAX(available_at) AS latest_available_at
    FROM market_stock_industry_membership_pit
    WHERE classification_source = 'SW2021'
      AND industry_level = 'L1'
    GROUP BY index_code, index_name, industry_code, industry_name
    ORDER BY index_code
    "#
}

fn parse_futures_price_chain_mapping_date(raw: &str) -> Result<NaiveDate, String> {
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

fn validate_futures_price_chain_mapping_candidate(
    candidate: &FuturesPriceChainMappingCandidate,
    raw_products: &BTreeSet<String>,
    sw2021_l1_targets: &BTreeSet<String>,
) -> FuturesPriceChainMappingCandidateValidation {
    let product_symbol = candidate.product_symbol.trim().to_ascii_uppercase();
    let exposure_type = candidate.exposure_type.trim().to_string();
    let exposure_code = candidate.exposure_code.trim().to_ascii_uppercase();
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    if product_symbol.is_empty() {
        errors.push("product_symbol_required".to_string());
    } else if !raw_products.contains(&product_symbol) {
        errors.push("unknown_raw_product_symbol".to_string());
    }

    if exposure_type != "sw_industry" {
        if exposure_type == "stock_symbol" {
            errors.push("direct_stock_mapping_requires_separate_evidence_gate".to_string());
        } else {
            errors.push("unsupported_exposure_type".to_string());
        }
    } else if !sw2021_l1_targets.contains(&exposure_code) {
        errors.push("unknown_sw2021_l1_exposure_code".to_string());
    }

    if candidate.direction != -1 && candidate.direction != 1 {
        errors.push("direction_must_be_minus_one_or_one".to_string());
    }
    if !(candidate.weight > 0.0 && candidate.weight <= 1.0) {
        errors.push("weight_must_be_gt_0_and_lte_1".to_string());
    }

    let valid_from = parse_futures_price_chain_mapping_date(&candidate.valid_from);
    let valid_to = candidate
        .valid_to
        .as_deref()
        .map(parse_futures_price_chain_mapping_date)
        .transpose();
    let available_at = parse_futures_price_chain_mapping_date(&candidate.available_at);

    match (&valid_from, &valid_to) {
        (Ok(start), Ok(Some(end))) if end < start => {
            errors.push("valid_to_before_valid_from".to_string())
        }
        _ => {}
    }
    match (&valid_from, &available_at) {
        (Ok(start), Ok(available)) if available < start => {
            errors.push("available_at_before_valid_from".to_string())
        }
        _ => {}
    }
    if valid_from.is_err() {
        errors.push("valid_from_invalid".to_string());
    }
    if valid_to.is_err() {
        errors.push("valid_to_invalid".to_string());
    }
    if available_at.is_err() {
        errors.push("available_at_invalid".to_string());
    }

    if candidate.source.trim().is_empty() {
        errors.push("source_required".to_string());
    }
    if candidate.mapping_version.trim().is_empty() {
        errors.push("mapping_version_required".to_string());
    }
    match &candidate.evidence {
        Value::Object(object) if !object.is_empty() => {}
        _ => errors.push("evidence_required".to_string()),
    }
    if candidate.evidence.get("source_url").is_none()
        && candidate.evidence.get("source_document").is_none()
        && candidate.evidence.get("review_note").is_none()
    {
        warnings.push("evidence_should_include_source_url_or_review_note".to_string());
    }

    FuturesPriceChainMappingCandidateValidation {
        product_symbol,
        exposure_type,
        exposure_code,
        passed: errors.is_empty(),
        errors,
        warnings,
    }
}

fn decide_futures_price_chain_mapping_candidate_validation(
    raw_product_count: i64,
    covered_product_count: i64,
    invalid_row_count: i64,
    missing_product_count: i64,
) -> Value {
    let (admission_decision, next_step) = if raw_product_count == 0 {
        (
            "raw_sync_required_before_mapping_candidate_validation",
            "run_bounded_futures_price_chain_sync_before_mapping_template",
        )
    } else if invalid_row_count > 0 {
        (
            "mapping_candidate_validation_failed",
            "repair_candidate_rows_before_insert_or_review",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_candidate_coverage_incomplete_before_insert",
            "add_candidate_rows_for_all_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "mapping_candidate_ready_for_manual_review_before_insert",
            "manual_review_then_insert_versioned_mapping_rows",
        )
    };
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "admission_decision": admission_decision,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "invalid_row_count": invalid_row_count,
        "mapping_candidate_coverage_ratio": coverage_ratio,
        "write_enabled": false,
        "p310_status": "blocked_until_mapping_insert_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
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

async fn build_futures_price_chain_readiness_audit(db: &sqlx::PgPool) -> Result<Value, String> {
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

async fn build_futures_price_chain_mapping_audit(db: &sqlx::PgPool) -> Result<Value, String> {
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

fn phase7_p319_candidate_admission_sources(futures_price_chain_readiness: Option<&Value>) -> Value {
    let mut admission = json!({
        "stage": "P3.22",
        "objective": "discover lower-correlation broad-base PIT alpha sources before any factor build, ML training, WFA admission, or v19 train selection",
        "hard_gate": "permission_schema_available_at_first",
        "global_policy": {
            "pit_required": true,
            "no_oos_reverse_tuning": true,
            "no_same_family_parameter_expansion": true,
            "model_algorithm_policy": {
                "algorithm_is_secondary_to_source_economics": true,
                "allowed_after": "candidate source passes data/PIT coverage and P3.10A-D economics gates",
                "forbidden_use": "do not use a new ML algorithm, full-period sign flip, label mining, or OOS feedback to rescue a source that failed RankIC, group return, decay, turnover/capacity, or bounded train robustness",
                "allowed_use": "after source admission, compare linear, tree/boosting and calibrated ensemble models only inside rolling train windows with net-of-cost objectives and unchanged test-window evaluation"
            },
            "required_sequence": [
                "permission_smoke",
                "schema_and_available_at_audit",
                "bounded_history_sync",
                "coverage_readiness_audit",
                "p310_rankic_group_decay_turnover_capacity",
                "bounded_wfa_only_after_diagnostics_pass"
            ],
            "promotion_rule": "only candidates passing data/PIT, RankIC, group return, decay, turnover/capacity and regime exposure gates may enter bounded WFA"
        },
        "stopped_same_family_sources": [
            "industry_prosperity_proxy",
            "market_residual_risk",
            "liquidity_regime",
            "event_surprise",
            "event_post_return_overlay",
            "moneyflow_congestion",
            "repurchase",
            "supply_float",
            "unlock_pressure",
            "block_trade_supply_demand",
            "main_business_fina_mainbz",
            "broad_analyst_revision_current_raw_bundle",
            "futures_price_chain_current_version",
            "equity_pledge_pressure_current_low_ratio_atom",
            "shareholder_structure_current_low_fanout_sleeve"
        ],
        "candidates": [
            {
                "source_id": "p322_source_inventory",
                "source_family": "new_low_correlation_pit_broad_base_source_discovery",
                "economic_hypothesis": "蓝图达标需要新的信息增量，而不是继续压榨已证伪的公开低频同族源；优先寻找更接近经营兑现、订单、产能、价格链、真实预期修正或股权激励执行质量的 PIT broad-base 数据。",
                "candidate_raw_sources": [
                    "regulated_disclosure_or_exchange_feed_for_orders_capacity_price_chain",
                    "licensed_broad_base_analyst_revision_or_consensus_estimate_feed",
                    "regulatory_or_exchange_equity_incentive_employee_stock_plan_execution_feed"
                ],
                "schema_status": "source_discovery_required",
                "client_status": "not_started",
                "sync_status": "not_started",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "native announcement/report/publication timestamp required; conservative next-session availability is allowed only when source publication time cannot be audited",
                "admission_decision": "source_discovery_required_before_permission_smoke",
                "blocked_reason": "no_new_source_has_passed_permission_schema_available_at_audit_after_p321e",
                "next_step": "rank_candidate_sources_by_breadth_pit_availability_permission_and_economic_hypothesis_then_run_permission_smoke_for_top_source"
            },
            {
                "source_id": "futures_price_chain",
                "source_family": "real_operations_and_order_price_chain",
                "economic_hypothesis": "真实经营、订单、产能、价格链变化比价格成交同族特征更接近基本面边际变化，若可 PIT 化且覆盖 broad-base，可能提供低相关横截面信息。",
                "candidate_raw_sources": [
                    "tushare:fina_mainbz",
                    "tushare:fut_daily",
                    "tushare:fut_wsr",
                    "tushare:fut_holding",
                    "source_discovery_required_for_order_price_chain"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:fina_mainbz",
                        "status": "stopped_after_p310_economics_weak",
                        "official_semantics": "main_business_composition_by_product_region_or_industry",
                        "observed_fields": ["ts_code", "end_date", "bz_item", "bz_code", "bz_sales", "bz_profit", "bz_cost", "curr_type", "update_flag"],
                        "missing_pit_fields": ["ann_date", "f_ann_date", "disclosure_date"],
                        "available_at_join_candidates": [
                            "market_financial_statement.ann_date_by_ts_code_end_date",
                            "market_stock_disclosure_date.actual_date_by_ts_code_period"
                        ],
                        "audit_endpoint": "POST /api/v1/quant/data/main-business/available-at-audit",
                        "readiness_endpoint": "GET /api/v1/quant/data/main-business/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/main-business/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-f6374904-1d5d-4bfc-afaf-64f95ce24040",
                        "diagnostics_summary": {
                            "data_pit_coverage": "green",
                            "effective_start_date": "2014-08-29",
                            "raw_rows": 500223,
                            "pit_violation_rows": 0,
                            "period_universe_mismatch_rows": 0,
                            "rankic_verdict": "weak_or_negative_across_most_profiles_horizons",
                            "best_profile": "segment_concentration_inverse",
                            "best_profile_mean_rankic_range": "0.0022..0.0038",
                            "best_profile_spread_range": "-0.12%..0.17%",
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_fina_mainbz_main_business_source_after_p310_economics_failed"
                    },
                    {
                        "candidate": "tushare:futures_price_chain",
                        "status": "stopped_after_p310_component_economics_failed",
                        "smoke_source": "futures_price_chain",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "schema_contract_endpoint": "GET /api/v1/quant/data/futures-price-chain/schema-contract",
                        "readiness_endpoint": "GET /api/v1/quant/data/futures-price-chain/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                        "official_docs": [
                            "https://tushare.pro/wctapi/documents/138.md",
                            "https://tushare.pro/wctapi/documents/139.md",
                            "https://tushare.pro/wctapi/documents/140.md"
                        ],
                        "raw_endpoints": [
                            {
                                "api": "fut_daily",
                                "semantics": "daily futures OHLC/settlement/volume/open-interest",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_wsr",
                                "semantics": "warehouse receipt daily inventory changes",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_holding",
                                "semantics": "daily broker volume/long/short holding ranking",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            }
                        ],
                        "pit_policy": "trade_date may be used only after the futures market publication point; intraday stock decisions must use previous available futures trade_date unless a source_published_at audit proves earlier availability",
                        "mapping_gate": "must design product-to-industry/stock exposure mapping before factor backfill; no static hindsight mapping may revise prior samples",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "trade_date": "20181113",
                            "fut_daily_rows": 5,
                            "fut_wsr_rows": 5,
                            "fut_holding_rows": 5,
                            "status": "available"
                        },
                        "full_history_summary": {
                            "raw_rows": 28375979,
                            "mapped_products": 84,
                            "excluded_products": 10,
                            "missing_products": 0,
                            "factor_rows": 2770856,
                            "factor_start": "2014-04-03",
                            "factor_end": "2026-06-18",
                            "raw_mapping_exclusion_pit_violations": 0
                        },
                        "diagnostics_summary": {
                            "combo_report_id": "exp-e0e525c6-612d-4b12-b11e-91088dbc8150",
                            "component_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                            "coverage_pit_market_scope": "green",
                            "research_economic_admission": "blocked_by_p310_economics",
                            "component_passed_horizon_count": 0,
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_futures_price_chain_after_p310_component_economics_failed"
                    }
                ],
                "current_tables": ["market_stock_main_business", "market_futures_daily", "market_futures_warehouse_receipt", "market_futures_holding_rank", "market_futures_product_exposure_mapping_pit"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_mapping_exclusion_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "source_publication_or_disclosure_date_required_before_effective_period; futures trade_date is usable only after source publication/market close",
                "admission_decision": "stopped_after_p310_component_economics_failed",
                "blocked_reason": "data_pit_coverage_mapping_green_but_combo_and_component_p310_economics_failed; no_full_period_sign_flip_no_same_family_weight_rescue_no_oos_reverse_tuning",
                "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
            },
            {
                "source_id": "equity_pledge_pressure",
                "source_family": "shareholder_financing_pressure_and_governance_risk",
                "economic_hypothesis": "股权质押压力可能刻画控股股东融资约束、治理风险和潜在被动减持压力；若能用公告日 PIT 化并覆盖足够广的股票池，可能提供与价格成交、事件后收益、期货价格链较低相关的横截面信息。",
                "candidate_raw_sources": [
                    "tushare:pledge_stat",
                    "tushare:pledge_detail"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:pledge_stat",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                        "official_semantics": "stock_equity_pledge_stat_snapshot",
                        "observed_fields_from_doc": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "symbol_probe",
                            "sample_symbol_count": 3,
                            "status": "available"
                        },
                        "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                        "decision": "do_not_use_pledge_stat_alone_until_available_at_policy_is_joined_or_conservatively_derived"
                    },
                    {
                        "candidate": "tushare:pledge_detail",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                        "official_semantics": "stock_equity_pledge_detail_events",
                        "observed_fields_from_doc": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "announcement_date_range_probe",
                            "status": "available"
                        },
                        "native_available_at_candidate": "ann_date",
                        "decision": "schema_contract_ready_but_full_history_coverage_and_pit_audit_required_before_factor_design"
                    }
                ],
                "current_tables": ["market_stock_pledge_stat", "market_stock_pledge_detail"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "pledge_detail.ann_date is the native available_at candidate; pledge_stat.end_date is only a snapshot measurement date and must be joined to detail announcements or shifted conservatively before any feature use",
                "diagnostics_summary": {
                    "data_pit_coverage": "green_after_2015_coverage_cliff_check",
                    "latest_report_id": "exp-0dc8dfd6-c43b-4c01-a42b-99339fc5ef25",
                    "mean_rankic_20_45_60_120": [0.00353, 0.00505, 0.00580, 0.00895],
                    "high_minus_low_spread": [-0.00127, -0.00340, -0.00394, -0.00296],
                    "passed_horizon_count": 0,
                    "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                },
                "admission_decision": "stopped_after_p310_economics_failed",
                "blocked_reason": "coverage_and_pit_passed_but_low_pledge_ratio_atom_failed_rankic_group_spread_and_monotonicity; no_same_family_event_or_sign_flip_rescue",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "shareholder_structure",
                "source_family": "ownership_structure_and_governance_breadth",
                "economic_hypothesis": "股东户数和交易事件可刻画筹码扩散、低 fanout 持有人变化与治理压力，但当前 low-fanout 表达在 v19 control 上未能转化为可交易净收益。",
                "candidate_raw_sources": [
                    "tushare:stk_holdernumber",
                    "tushare:stk_holdertrade",
                    "tushare:top10_holders",
                    "tushare:top10_floatholders"
                ],
                "current_tables": [
                    "market_stock_holder_number",
                    "market_stock_holder_trade",
                    "market_stock_top10_holders",
                    "market_stock_top10_float_holders"
                ],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_low_fanout_raw_sync_completed",
                "coverage_status": "strict_low_fanout_coverage_pit_green_top10_high_fanout_not_admitted",
                "p310_status": "completed_passed_single_factor_diagnostics",
                "wfa_status": "completed_skipped_all_windows_by_train_robustness_and_cost_capacity_gate",
                "pit_required": true,
                "available_at_policy": "holder_number requires available_at >= end_date and holder_num > 0; holder_trade requires ann_date/native available_at and ratio/interval quality gates; top10 raw cannot bypass separate high-fanout coverage admission",
                "admission_decision": "stopped_after_bounded_wfa_train_robustness_failed",
                "diagnostics_summary": {
                    "p310_rankic_5_10_20d": [0.0187, 0.0285, 0.0361],
                    "p310_monotonicity": 0.75,
                    "wfa_experiment_id": "exp-39d6d820-0073-4a22-889f-e9eb67962785",
                    "wfa_windows": 13,
                    "skipped_windows": 13,
                    "stitched_oos": false,
                    "control_train_annual_return": -0.0677,
                    "sleeve_train_annual_return_5_10_15pct": [-0.0764, -0.0691, -0.0668],
                    "decision": "do_not_enter_v19_train_selection_or_expand_same_family_sleeve"
                },
                "blocked_reason": "raw_pit_and_p310_single_factor_passed_but_bounded_sleeve_wfa_failed_train_robustness_and_cost_capacity_gate",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "equity_incentive_execution_quality",
                "source_family": "equity_incentive_and_employee_stock_plan_execution",
                "economic_hypothesis": "股权激励、员工持股与执行进度可能代表治理层对未来经营兑现的约束和信号，但必须使用公告可得日与执行窗口，不能用事后完成状态回填。",
                "candidate_raw_sources": [
                    "tushare:stk_rewards_rejected_semantic_mismatch",
                    "tushare:stk_reward_rejected_invalid_endpoint",
                    "source_discovery_required"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:stk_rewards",
                        "status": "rejected_semantic_mismatch",
                        "official_semantics": "management_compensation_and_shareholding",
                        "observed_fields": ["ts_code", "ann_date", "end_date", "name", "title", "reward", "hold_vol"],
                        "missing_required_execution_fields": [
                            "plan_id",
                            "grant_date",
                            "grant_price_or_exercise_price",
                            "vesting_or_unlock_schedule",
                            "participant_scope",
                            "execution_progress",
                            "cancellation_or_adjustment_events"
                        ],
                        "decision": "do_not_build_schema_or_factor_from_stk_rewards_for_equity_incentive_execution_quality"
                    },
                    {
                        "candidate": "tushare:stk_reward",
                        "status": "rejected_invalid_endpoint",
                        "decision": "do_not_retry_without_official_endpoint_evidence"
                    }
                ],
                "current_tables": [],
                "schema_status": "blocked_until_valid_source_identified",
                "client_status": "do_not_add_stk_rewards_client_for_equity_incentive",
                "sync_status": "missing",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "announcement_or_disclosure_date_required_before_event_effective_date",
                "admission_decision": "blocked_no_valid_equity_incentive_source",
                "blocked_reason": "stk_rewards_is_management_compensation_shareholding_not_equity_incentive_execution_and_stk_reward_is_invalid",
                "next_step": "search_regulatory_disclosure_or_licensed_vendor_source_for_equity_incentive_employee_stock_plan_execution"
            },
            {
                "source_id": "broad_analyst_revision",
                "source_family": "broad_base_analyst_expectation_revision",
                "economic_hypothesis": "更 broad-base 的业绩预告/快报/披露日历修正可以刻画一致预期边际变化，但必须避免退化为稀疏公告后收益曲线 overlay。",
                "candidate_raw_sources": [
                    "tushare:forecast_stopped_sparse_revision_bundle",
                    "tushare:express_stopped_sparse_revision_bundle",
                    "tushare:disclosure_date_stopped_sparse_revision_bundle",
                    "tushare:report_rc"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:forecast/express/disclosure_date",
                        "status": "stopped_after_full_history_audit_sparse_revision_semantics",
                        "audit_endpoint": "GET /api/v1/quant/data/broad-analyst-revision/audit",
                        "available_at_policy": {
                            "forecast": "available_at equals ann_date; available_at can be before end_date because forecasts may be published before period end",
                            "express": "available_at equals ann_date",
                            "disclosure_date": "available_at equals max(ann_date, actual_date, modify_date); pre_date is not true availability"
                        },
                        "audit_summary": {
                            "union_symbols": 4074,
                            "union_symbol_coverage_ratio": 0.5650,
                            "forecast_symbols": 1682,
                            "forecast_symbol_coverage_ratio": 0.2333,
                            "forecast_symbol_periods": 27681,
                            "multi_announcement_symbol_periods": 1825,
                            "multi_announcement_symbol_period_ratio": 0.0659,
                            "revision_event_symbols": 832,
                            "revision_event_symbol_coverage_ratio": 0.1154,
                            "available_at_rule_violations": 0,
                            "decision": "do_not_enter_p310_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_current_forecast_express_disclosure_bundle_search_replacement_revision_source"
                    },
                    {
                        "candidate": "tushare:report_rc",
                        "status": "blocked_current_api_unknown_source_after_production_smoke",
                        "official_doc": "https://tushare.pro/wctapi/documents/292.md",
                        "official_semantics": "sell_side_research_report_earnings_forecast_daily_since_2010",
                        "observed_fields_from_doc": ["ts_code", "report_date", "report_title", "report_type", "classify", "org_name", "author_name", "quarter", "op_rt", "op_pr", "tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "rating", "max_price", "min_price", "imp_dg", "create_time"],
                        "native_available_at_candidate": "report_date",
                        "permission_note": "120 points can trial 10 requests/day; formal permission requires 8000 points according to official doc",
                        "smoke_source": "report_rc",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "request": {"sources": ["report_rc"], "start_date": "20260401", "end_date": "20260621", "limit": 5},
                            "status": "error",
                            "error_code": "40101",
                            "error": "未知的数据源"
                        },
                        "decision": "do_not_build_schema_sync_factor_or_p310_from_report_rc_until_the_callable_api_name_or_permission_path_is_verified"
                    }
                ],
                "current_tables": [
                    "market_stock_forecast",
                    "market_stock_express",
                    "market_stock_disclosure_date"
                ],
                "schema_status": "current_event_bundle_present_report_rc_blocked_current_api_unknown_source",
                "client_status": "report_rc_read_only_smoke_available_but_current_api_returns_unknown_source",
                "sync_status": "current_event_bundle_bounded_sync_available_report_rc_blocked_not_synced",
                "coverage_status": "current_event_bundle_full_history_audit_completed_report_rc_blocked_current_api_unknown_source",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "ann_date_or_latest_required_disclosure_date_as_available_at",
                "admission_decision": "blocked_report_rc_current_api_unknown_source_after_permission_smoke",
                "blocked_reason": "current_forecast_express_disclosure_bundle_stopped_and_report_rc_production_smoke_returned_tushare_40101_unknown_data_source",
                "guardrail": "must_be_broad_base_revision_not_sparse_event_post_return_overlay",
                "next_step": "search_licensed_or_alternative_broad_pit_expectation_revision_source; do_not_shift_back_to_stopped_futures_price_chain_proxy"
            }
        ]
    });
    if let Some(readiness) = futures_price_chain_readiness {
        apply_p319_futures_price_chain_readiness(&mut admission, readiness);
    }
    admission
}

fn apply_p319_futures_price_chain_readiness(admission: &mut Value, readiness: &Value) {
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

fn apply_p320_equity_pledge_readiness(admission: &mut Value, readiness: &Value) {
    let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let decision = readiness
        .get("decision")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let sync_status = decision
        .get("sync_status")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let schema_status = decision
        .get("schema_status")
        .and_then(Value::as_str)
        .unwrap_or("schema_contract_ready_review_required");
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_coverage_readiness_passes");

    for source in candidates.iter_mut() {
        let Some("equity_pledge_pressure") =
            source.get("source_id").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            let source_is_stopped_by_research = object
                .get("admission_decision")
                .and_then(Value::as_str)
                .map(|decision| decision.starts_with("stopped_"))
                .unwrap_or(false)
                || object
                    .get("p310_status")
                    .and_then(Value::as_str)
                    .map(|status| status == "completed_failed_economics")
                    .unwrap_or(false);

            object.insert("equity_pledge_readiness".to_string(), readiness.clone());
            if source_is_stopped_by_research {
                object.insert("latest_raw_readiness".to_string(), readiness.clone());
                continue;
            }

            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("schema_status".to_string(), json!(schema_status));
            object.insert("sync_status".to_string(), json!(sync_status));
            object.insert(
                "coverage_status".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "raw_pit_ready_coverage_audit_not_started",
                    "raw_pit_failed" => "blocked_by_raw_pit_violations",
                    "bounded_sync_required_before_coverage_audit" => "not_started",
                    "apply_schema_before_sync" => "blocked_until_schema_applied",
                    _ => "not_started",
                }),
            );
            object.insert("p310_status".to_string(), json!(p310_status));
            object.insert("admission_decision".to_string(), json!(admission_decision));
            object.insert(
                "blocked_reason".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "equity_pledge_raw_pit_passed_but_year_symbol_ann_date_coverage_duplicate_and_sync_attempt_audit_not_done",
                    "raw_pit_failed" =>
                        "equity_pledge_raw_rows_have_available_at_future_leak_or_schema_pit_violation",
                    "bounded_sync_required_before_coverage_audit" =>
                        "equity_pledge_schema_exists_but_no_raw_rows_synced",
                    "apply_schema_before_sync" =>
                        "equity_pledge_schema_contract_ready_but_database_schema_missing_or_invalid",
                    _ =>
                        "equity_pledge_candidate_waiting_for_schema_available_at_bounded_sync_and_p310",
                }),
            );
            object.insert("next_step".to_string(), json!(next_step));
        }
    }
}

fn phase7_new_alpha_candidate_sources_with_market_status(
    market_stats: &BTreeMap<String, Phase7MarketLevelSourceAudit>,
    market_sync_tasks: &BTreeMap<String, Phase7MarketLevelSyncAudit>,
    today: NaiveDate,
) -> Vec<Value> {
    let mut sources = phase7_new_alpha_candidate_sources();
    for source in sources.iter_mut() {
        let Some(source_name) = source.get("source").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some(stats) = market_stats.get(source_name) else {
            continue;
        };
        let readiness = phase7_market_level_source_readiness(stats);
        let sync_start = phase7_p315_sync_start_date(stats, today);
        let sync_dataset = phase7_market_level_sync_dataset(source_name);
        let last_sync = market_sync_tasks.get(source_name);
        let zero_row_sync_covers_gap =
            phase7_market_level_zero_row_sync_covers_gap(last_sync, sync_start, today);
        let effective_readiness =
            if readiness == "market_level_stale_needs_sync" && zero_row_sync_covers_gap {
                "market_level_upstream_zero_rows_unavailable"
            } else {
                readiness
            };
        let freshness_gate = if effective_readiness == "market_level_ready_for_regime_feature" {
            "passed"
        } else {
            "failed"
        };

        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(effective_readiness));
            object.insert(
                "market_data_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "open_day_lag": stats.open_day_lag,
                    "freshness_max_open_day_lag": 2,
                    "freshness_gate": freshness_gate,
                }),
            );
            if let Some(last_sync) = last_sync {
                object.insert(
                    "last_sync_task".to_string(),
                    json!({
                        "task_id": last_sync.task_id,
                        "task_type": last_sync.task_type,
                        "start_date": phase7_date_json(last_sync.start_date),
                        "end_date": phase7_date_json(last_sync.end_date),
                        "status": last_sync.status,
                        "total_count": last_sync.total_count,
                        "success_count": last_sync.success_count,
                        "failed_count": last_sync.failed_count,
                        "error_message": last_sync.error_message,
                        "completed_at": phase7_datetime_json(last_sync.completed_at),
                    }),
                );
            }
            if effective_readiness == "market_level_upstream_zero_rows_unavailable" {
                object.insert(
                    "sync_remediation".to_string(),
                    json!({
                        "status": "not_retriable_until_upstream_resolved",
                        "reason": "latest stale-gap sync completed successfully but returned zero rows; treat this source as live-unavailable until upstream endpoint, permission, fields, or replacement source is fixed",
                    }),
                );
            }
            if let Some(dataset) = sync_dataset {
                object.insert(
                    "sync_task_payload".to_string(),
                    json!({
                        "dataset": dataset,
                        "source": "tushare",
                        "start_date": sync_start.format("%Y%m%d").to_string(),
                        "end_date": today.format("%Y%m%d").to_string(),
                        "background": true,
                        "reason": "p315_market_level_regime_source_freshness"
                    }),
                );
            }
        }
    }
    sources
}

fn phase7_block_trade_readiness(stats: &Phase7BlockTradeSourceAudit) -> &'static str {
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

fn phase7_new_alpha_candidate_sources_with_block_trade_status(
    mut sources: Vec<Value>,
    stats: &Phase7BlockTradeSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("block_trade_supply_demand") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_block_trade_readiness(stats);
        if let Value::Object(object) = source {
            let final_readiness = if readiness == "raw_source_pit_failed" {
                readiness
            } else {
                "stopped_after_p310_economics_weak"
            };
            object.insert("readiness".to_string(), json!(final_readiness));
            object.insert("raw_source_readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "covered_trade_days": stats.covered_trade_days,
                    "open_days_in_range": stats.open_days_in_range,
                    "open_day_coverage_ratio": phase7_ratio(stats.covered_trade_days, stats.open_days_in_range),
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match final_readiness {
                    "stopped_after_p310_economics_weak" => {
                        "do_not_expand_same_family_shift_to_p320_new_source_admission"
                    }
                    "raw_source_pit_failed" => "repair_available_at_before_any_diagnostics",
                    _ => "do_not_expand_same_family_shift_to_p320_new_source_admission",
                }),
            );
        }
    }
    sources
}

fn equity_pledge_readiness_label(admission_decision: &str) -> &'static str {
    match admission_decision {
        "apply_schema_before_sync" => "schema_contract_ready_schema_not_applied",
        "bounded_sync_required_before_coverage_audit" => "schema_created_bounded_sync_required",
        "raw_pit_failed" => "raw_source_pit_failed",
        "coverage_readiness_audit_required_before_p310" => "raw_source_ready_for_coverage_audit",
        _ => "schema_contract_ready_review_required",
    }
}

fn phase7_new_alpha_candidate_sources_with_equity_pledge_status(
    mut sources: Vec<Value>,
    readiness: Option<&Value>,
) -> Vec<Value> {
    let Some(readiness) = readiness else {
        return sources;
    };
    let decision = readiness
        .get("decision")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let readiness_label = equity_pledge_readiness_label(admission_decision);
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");

    for source in sources.iter_mut() {
        let Some("equity_pledge_pressure") = source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness_label));
            object.insert("next_step".to_string(), json!(next_step));
            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("raw_source_status".to_string(), readiness.clone());
            object.insert(
                "why_not_trainable_now".to_string(),
                json!("股权质押源仍停在 schema/raw/coverage 准入层；只有全历史 coverage/readiness/PIT 与 P3.10A-D 通过后才允许进入 WFA/v19"),
            );
        }
    }
    sources
}

fn phase7_industry_membership_readiness(
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

fn phase7_new_alpha_candidate_sources_with_industry_membership_status(
    mut sources: Vec<Value>,
    stats: &Phase7IndustryMembershipSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("industry_prosperity_proxy") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_industry_membership_readiness(stats);
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "table": "market_stock_industry_membership_pit",
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "index_codes": stats.index_codes,
                    "current_active_stock_symbols": stats.current_active_stock_symbols,
                    "current_covered_stock_symbols": stats.current_covered_stock_symbols,
                    "current_missing_stock_symbols": stats.current_active_stock_symbols.saturating_sub(stats.current_covered_stock_symbols),
                    "current_stock_coverage_ratio": phase7_ratio(stats.current_covered_stock_symbols, stats.current_active_stock_symbols),
                    "min_in_date": phase7_date_json(stats.min_in_date),
                    "latest_in_date": phase7_date_json(stats.latest_in_date),
                    "min_out_date": phase7_date_json(stats.min_out_date),
                    "latest_out_date": phase7_date_json(stats.latest_out_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                    "invalid_interval_rows": stats.invalid_interval_rows,
                    "duplicate_key_rows": stats.duplicate_key_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match readiness {
                    "industry_membership_raw_source_ready_for_coverage_audit" => {
                        "run_full_history_coverage_and_pit_membership_snapshot_audit"
                    }
                    "industry_membership_raw_source_pit_failed" => {
                        "repair_industry_membership_intervals_before_factor_design"
                    }
                    "industry_membership_current_coverage_undercovered" => {
                        "run_full_l1_membership_sync_or_repair_missing_symbols"
                    }
                    _ => "run_bounded_industry_membership_sync",
                }),
            );
        }
    }
    sources
}

fn phase7_industry_membership_snapshot_readiness(
    expected_symbol_days: i64,
    covered_symbol_days: i64,
    _missing_symbol_days: i64,
    multi_membership_symbol_days: i64,
    pit_violation_rows: i64,
    invalid_interval_rows: i64,
    duplicate_key_rows: i64,
    missing_exit_available_at_rows: i64,
) -> &'static str {
    if expected_symbol_days <= 0 {
        return "snapshot_no_universe_symbol_days";
    }
    if pit_violation_rows > 0
        || invalid_interval_rows > 0
        || duplicate_key_rows > 0
        || missing_exit_available_at_rows > 0
    {
        return "snapshot_raw_source_pit_failed";
    }
    if multi_membership_symbol_days > 0 {
        return "snapshot_multi_membership_blocked";
    }
    if phase7_ratio(covered_symbol_days, expected_symbol_days).unwrap_or(0.0) < 0.995 {
        return "snapshot_coverage_gaps_need_review";
    }
    "snapshot_ready_for_p310_diagnostics"
}

fn phase7_industry_membership_audit_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(20).clamp(1, 100)
}

fn phase7_industry_membership_market_scope_eligible(
    coverage_ratio: Option<f64>,
    multi_membership_symbol_days: i64,
) -> bool {
    multi_membership_symbol_days == 0 && coverage_ratio.unwrap_or(0.0) >= 0.995
}

fn phase7_industry_membership_snapshot_summary_sql() -> &'static str {
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
    ),
    raw_stats AS (
        SELECT COUNT(*) FILTER (WHERE available_at < in_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND out_date < in_date)::bigint
                   AS invalid_interval_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND exit_available_at IS NULL)::bigint
                   AS missing_exit_available_at_rows
        FROM market_stock_industry_membership_pit
        WHERE classification_source IN ('SW2014', 'SW2021')
          AND industry_level = 'L1'
    ),
    duplicate_keys AS (
        SELECT COALESCE(SUM(row_count - 1), 0)::bigint AS duplicate_key_rows
        FROM (
            SELECT classification_source, index_code, symbol, in_date, COUNT(*)::bigint AS row_count
            FROM market_stock_industry_membership_pit
            WHERE classification_source IN ('SW2014', 'SW2021')
              AND industry_level = 'L1'
            GROUP BY classification_source, index_code, symbol, in_date
            HAVING COUNT(*) > 1
        ) duplicate_groups
    )
    SELECT COUNT(DISTINCT joined.trade_date)::bigint AS trade_days,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           MIN(joined.trade_date) AS min_trade_date,
           MAX(joined.trade_date) AS max_trade_date,
           raw_stats.pit_violation_rows,
           raw_stats.invalid_interval_rows,
           duplicate_keys.duplicate_key_rows,
           raw_stats.missing_exit_available_at_rows
    FROM joined, raw_stats, duplicate_keys
    GROUP BY raw_stats.pit_violation_rows,
             raw_stats.invalid_interval_rows,
             duplicate_keys.duplicate_key_rows,
             raw_stats.missing_exit_available_at_rows
    "#
}

fn phase7_industry_membership_year_breakdown_sql() -> &'static str {
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
    SELECT DATE_TRUNC('year', joined.trade_date)::date AS period_start,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY DATE_TRUNC('year', joined.trade_date)::date
    ORDER BY period_start
    "#
}

fn phase7_industry_membership_market_breakdown_sql() -> &'static str {
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
        SELECT days.trade_date,
               stock.symbol,
               COALESCE(stock.market, '') AS market
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
               universe.market,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    )
    SELECT joined.market,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY joined.market
    ORDER BY missing_symbol_days DESC, joined.market
    "#
}

fn phase7_optional_source_json(
    source: &str,
    table: &str,
    table_exists: bool,
    stats: Option<&(i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    attempted_symbols: i64,
    attempted_zero_row_symbols: i64,
    reference_symbols: i64,
    next_feature: &str,
) -> Value {
    let (rows, min_date, max_date, symbols) = stats.copied().unwrap_or((0, None, None, 0));
    let available_or_attempted_symbols = symbols.max(attempted_symbols);
    let readiness = phase7_optional_source_readiness(
        table_exists,
        rows,
        available_or_attempted_symbols,
        reference_symbols,
    );
    let mut value = phase7_coverage_json(
        source.to_string(),
        rows,
        min_date,
        max_date,
        available_or_attempted_symbols,
        reference_symbols,
    );
    if let Value::Object(ref mut object) = value {
        object.insert("source".to_string(), json!(source));
        object.insert("table".to_string(), json!(table));
        object.insert("table_exists".to_string(), json!(table_exists));
        object.insert("data_row_symbols".to_string(), json!(symbols));
        object.insert("attempted_symbols".to_string(), json!(attempted_symbols));
        object.insert(
            "attempted_zero_row_symbols".to_string(),
            json!(attempted_zero_row_symbols),
        );
        object.insert(
            "coverage_basis".to_string(),
            json!("data_rows_or_successful_attempts"),
        );
        object.insert("feature_readiness".to_string(), json!(readiness));
        object.insert("next_feature".to_string(), json!(next_feature));
        object.insert(
            "next_step".to_string(),
            json!(phase7_optional_source_next_step(readiness)),
        );
    }
    value
}

async fn register_sync_task(
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

fn require_range(req: &DataSyncTaskReq) -> Result<(&str, &str), String> {
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

fn optional_source_all_symbols_allowed(mode: Option<&str>) -> bool {
    mode == Some("full_market")
}

async fn execute_sync_task(
    state: Arc<AppState>,
    task_id: String,
    req: DataSyncTaskReq,
) -> Result<serde_json::Value, String> {
    match req.dataset.as_str() {
        "stock_basic" => {
            let count = quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &task_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "daily" | "stock_daily" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_bars(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily", "status": "completed", "count": count}),
            )
        }
        "daily_basic" | "stock_daily_basic" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_basic(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily_basic", "status": "completed", "count": count}),
            )
        }
        "moneyflow" | "stock_moneyflow" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_moneyflow(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "moneyflow", "status": "completed", "count": count}),
            )
        }
        "moneyflow_hsgt" | "hsgt_moneyflow" => {
            let (start, end) = require_range(&req)?;
            let count =
                quant_data::sync::sync_moneyflow_hsgt(&state.db, &state.tushare, start, end)
                    .await
                    .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "moneyflow_hsgt", "status": "completed", "count": count}),
            )
        }
        "margin" | "market_margin" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_margin(&state.db, &state.tushare, start, end)
                .await
                .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "margin", "status": "completed", "count": count}),
            )
        }
        "block_trade" | "market_stock_block_trade" => {
            let (start, end) = require_range(&req)?;
            let count =
                quant_data::sync::sync_block_trade(&state.db, &state.tushare, &task_id, start, end)
                    .await
                    .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "block_trade", "status": "completed", "count": count}),
            )
        }
        "industry_membership" | "market_stock_industry_membership_pit" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_industry_membership(
                &state.db,
                &state.tushare,
                &task_id,
                &req.index_codes,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "industry_membership", "status": "completed", "count": count}),
            )
        }
        "forecast" | "stock_forecast" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_forecast(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "forecast", "status": "completed", "count": count}),
            )
        }
        "express" | "stock_express" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_express(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "express", "status": "completed", "count": count}),
            )
        }
        "disclosure_date" | "stock_disclosure_date" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_disclosure_date(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "disclosure_date", "status": "completed", "count": count}),
            )
        }
        "cashflow" | "stock_cashflow" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for cashflow sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_cashflow(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "cashflow", "status": "completed", "count": count}),
            )
        }
        "dividend" | "stock_dividend" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for dividend sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_dividend(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "dividend", "status": "completed", "count": count}),
            )
        }
        "repurchase" | "stock_repurchase" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for repurchase sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_repurchase(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "repurchase", "status": "completed", "count": count}),
            )
        }
        "share_float" | "stock_share_float" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for share_float sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_share_float(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "share_float", "status": "completed", "count": count}),
            )
        }
        "main_business" | "stock_main_business" => {
            let (start, end) = require_range(&req)?;
            let business_type = req
                .mode
                .as_deref()
                .filter(|value| matches!(*value, "P" | "D" | "I"))
                .unwrap_or("P");
            let count = quant_data::sync::sync_main_business(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                start,
                end,
                business_type,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "main_business", "status": "completed", "count": count}),
            )
        }
        "futures_price_chain" | "futures_price_chain_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_futures_price_chain(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                &req.exchanges,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "futures_price_chain", "status": "completed", "count": count}),
            )
        }
        "equity_pledge_pressure" | "equity_pledge_pressure_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_equity_pledge_pressure(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "equity_pledge_pressure", "status": "completed", "count": count}),
            )
        }
        "shareholder_structure" | "shareholder_structure_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_shareholder_structure(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                &req.source_filters,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "shareholder_structure", "status": "completed", "count": count}),
            )
        }
        "adj_factor" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for adj_factor sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_adj_factor(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "index_daily" => {
            let codes = if req.index_codes.is_empty() {
                &req.symbols
            } else {
                &req.index_codes
            };
            if codes.is_empty() {
                return Err("index_codes must not be empty for index_daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_index_daily(
                &state.db,
                &state.tushare,
                codes,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "trade_cal" => {
            let exchanges = if req.exchanges.is_empty() {
                vec!["SSE".to_string(), "SZSE".to_string()]
            } else {
                req.exchanges.clone()
            };
            let mut total = 0usize;
            for exchange in &exchanges {
                let child_task_id = format!("{}-{}", task_id, exchange.to_lowercase());
                total += quant_data::sync::sync_trade_calendar_with_task(
                    &state.db,
                    &state.tushare,
                    exchange,
                    &child_task_id,
                )
                .await
                .map_err(|e| e.to_string())?;
            }
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                total as i32,
                total as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": total}),
            )
        }
        "financial" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for financial sync".into());
            }
            let (statements, indicators) = quant_data::sync::sync_financial_data_with_task(
                &state.db,
                &state.tushare,
                &req.symbols,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(json!({
                "task_id": task_id,
                "dataset": req.dataset,
                "status": "completed",
                "statements": statements,
                "indicators": indicators
            }))
        }
        other => Err(format!("unsupported dataset: {}", other)),
    }
}

/// POST /api/v1/quant/data/sync-tasks
///
/// 统一数据同步任务入口，专用 sync 接口保留为兼容层。
pub async fn create_sync_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DataSyncTaskReq>,
) -> impl IntoResponse {
    let task_id = req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("data-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if req.background {
        let state_for_task = state.clone();
        let task_id_for_task = task_id.clone();
        let req_for_task = req.clone();
        tokio::spawn(async move {
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
                tracing::error!(task_id = %task_id_for_task, error = %message, "统一同步任务失败");
            }
        });

        return Json(json!({"code": 0, "data": {
            "task_id": task_id,
            "status": "running",
            "dataset": req.dataset,
            "mode": req.mode,
            "quality_check": req.quality_check,
            "create_data_version": req.create_data_version,
            "reason": req.reason
        }}));
    }

    match execute_sync_task(state, task_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

/// POST /api/v1/quant/data/sync/stock-basic
#[derive(Debug, Deserialize)]
pub struct SyncStockBasicReq {
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_stock_basic(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncStockBasicReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, "开始同步 A 股基本信息");
    match quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &dv_id).await {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/daily
#[derive(Debug, Deserialize)]
pub struct SyncDailyReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步日线");
    match quant_data::sync::sync_daily_bars(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/adj-factor
#[derive(Debug, Deserialize)]
pub struct SyncAdjFactorReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

pub async fn sync_adj_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步复权因子");
    match quant_data::sync::sync_adj_factor(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/fund-adj — 同步 ETF/基金复权因子（Tushare fund_adj）
pub async fn sync_fund_adj(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步基金复权因子");
    match quant_data::sync::sync_fund_adj(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/adj-factor/background
///
/// 大批量后台同步复权因子，立即返回 task_id。
pub async fn sync_adj_factor_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncAdjFactorReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);

    let state = state.clone();
    let mut symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    // symbols 为空表示按区间修复全 A 股。必须按 PIT 上市/退市区间展开，
    // 不能只取当前仍上市股票，否则历史日线缺口会被退市/状态变更掩盖。
    if symbols.is_empty() {
        symbols = sqlx::query_as::<_, (String,)>(
            "SELECT symbol FROM market_stock
             WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
               AND list_date IS NOT NULL
               AND list_date <= $1::date
               AND (delist_date IS NULL OR delist_date >= $2::date)
             ORDER BY symbol",
        )
        .bind(&end)
        .bind(&start)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(s,)| s)
        .collect();
    }
    info!(data_version_id = %dv_id, symbols = symbols.len(), "后台同步复权因子");

    tokio::spawn(async move {
        match quant_data::sync::sync_adj_factor(
            &state.db,
            &state.tushare,
            &symbols,
            &start,
            &end,
            &task_id,
        )
        .await
        {
            Ok(count) => info!(task_id = %task_id, count = count, "后台同步复权因子完成"),
            Err(e) => {
                tracing::error!(task_id = %task_id, error = %e.to_string(), "后台同步复权因子失败")
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
}

/// POST /api/v1/quant/data/sync/index-daily
#[derive(Debug, Deserialize)]
pub struct SyncIndexDailyReq {
    pub index_codes: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub data_version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncHsgtRequest {
    start_date: String,
    end_date: String,
}

pub async fn sync_moneyflow_hsgt(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncHsgtRequest>,
) -> impl IntoResponse {
    let client =
        quant_data::tushare::client::TushareClient::from_env().expect("Tushare client init failed");
    match quant_data::sync::sync_moneyflow_hsgt(&state.db, &client, &req.start_date, &req.end_date)
        .await
    {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

#[derive(Debug, Deserialize)]
pub struct SyncMarginRequest {
    start_date: String,
    end_date: String,
}

pub async fn sync_margin(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncMarginRequest>,
) -> impl IntoResponse {
    let client =
        quant_data::tushare::client::TushareClient::from_env().expect("Tushare client init failed");
    match quant_data::sync::sync_margin(&state.db, &client, &req.start_date, &req.end_date).await {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

#[derive(Debug, Deserialize)]
pub struct SyncFundDailyReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    pub data_version_id: Option<String>,
}

pub async fn sync_fund_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFundDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, symbols = req.symbols.len(), "同步基金日线");
    match quant_data::sync::sync_fund_daily(
        &state.db,
        &state.tushare,
        &req.symbols,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(rows) => Json(json!({"code": 0, "data": {"rows_synced": rows}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

pub async fn sync_index_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncIndexDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);
    info!(data_version_id = %dv_id, indexes = req.index_codes.len(), "同步指数日线");
    match quant_data::sync::sync_index_daily(
        &state.db,
        &state.tushare,
        &req.index_codes,
        &req.start_date,
        &req.end_date,
        &dv_id,
    )
    .await
    {
        Ok(count) => Json(
            json!({"code": 0, "data": {"task_id": dv_id, "status": "completed", "count": count}}),
        ),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// POST /api/v1/quant/data/sync/trade-cal
pub async fn sync_trade_cal(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    info!("同步交易日历");
    let mut count = 0usize;
    for ex in &["SSE", "SZSE"] {
        match quant_data::sync::sync_trade_calendar(&state.db, &state.tushare, ex).await {
            Ok(c) => count += c,
            Err(e) => return Json(json!({"code": 1, "message": format!("{}: {}", ex, e)})),
        }
    }
    Json(json!({"code": 0, "data": {"status": "completed", "count": count}}))
}

/// POST /api/v1/quant/data/quality-check
#[derive(Debug, Deserialize)]
pub struct QualityCheckReq {
    pub symbols: Vec<String>,
    pub start_date: String,
    pub end_date: String,
}

pub async fn quality_check(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QualityCheckReq>,
) -> impl IntoResponse {
    info!(symbols = req.symbols.len(), "数据质量检查");
    match quant_data::sync::run_quality_check(
        &state.db,
        &req.symbols,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(result) => Json(json!({"code": 0, "data": result})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// GET /api/v1/quant/data/stats
pub async fn data_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stock_count = quant_data::repository::count_stocks(&state.db)
        .await
        .unwrap_or(0);
    let bar_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_bar_adj")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let adj_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_adjustment_factor")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let fin_stmt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_statement")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let fin_ind: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_financial_indicator")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    Json(json!({"code": 0, "data": {
        "stock_count": stock_count, "bar_count": bar_count, "adj_factor_count": adj_count,
        "fin_statement_count": fin_stmt, "fin_indicator_count": fin_ind
    }}))
}

/// GET /api/v1/quant/data/phase7-feasibility-audit
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

/// GET /api/v1/quant/data/shareholder-structure/schema-contract
pub async fn shareholder_structure_schema_contract() -> impl IntoResponse {
    Json(json!({"code": 0, "data": phase7_shareholder_structure_schema_contract()}))
}

/// GET /api/v1/quant/data/futures-price-chain/readiness-audit
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
        tokio::spawn(async move {
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
        });
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
        tokio::spawn(async move {
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
        });
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
        tokio::spawn(async move {
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
        });
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
pub async fn broad_analyst_revision_audit(
    State(state): State<Arc<AppState>>,
    Query(req): Query<BroadAnalystRevisionAuditReq>,
) -> impl IntoResponse {
    match build_broad_analyst_revision_audit(&state, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

/// POST /api/v1/quant/data/phase7-optional-source-coverage-sync
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
                    "updated_at": updated_at.map(|value| value.to_rfc3339()),
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

async fn build_phase7_optional_source_coverage_sync(
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
            tokio::spawn(async move {
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
            });
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

async fn build_phase7_optional_source_coverage_batches(
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

async fn build_phase7_share_float_coverage_batches(
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
            tokio::spawn(async move {
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
            });
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

async fn build_phase7_share_float_readiness_audit(
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

async fn build_phase7_industry_membership_coverage_audit(
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
            tokio::spawn(async move {
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
            });
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

async fn phase7_completed_attempts_by_source_for_window(
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

async fn build_phase7_coverage_expansion_runner(
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
        tokio::spawn(async move {
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
        });
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

async fn resolve_phase7_permission_smoke_symbols(
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
        unsupported => json!({
            "source": unsupported,
            "status": "unsupported_source",
            "supported_sources": PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED,
        }),
    }
}

async fn build_phase7_feasibility_audit(state: &AppState) -> Result<Value, String> {
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
            "For P3.22, rank genuinely new low-correlation PIT broad-base sources by breadth, native available_at quality, permission, and economic hypothesis before permission smoke.",
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
pub async fn sync_daily_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncDailyReq>,
) -> impl IntoResponse {
    let dv_id = req
        .data_version_id
        .unwrap_or_else(generated_data_version_id);

    let state = state.clone();
    let mut symbols = req.symbols.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let task_id = dv_id.clone();

    // symbols 为空表示按区间修复全 A 股。必须按 PIT 上市/退市区间展开，
    // 不能只取当前仍上市股票，否则历史日线缺口会被状态变更或 ETF/REIT 混入掩盖。
    if symbols.is_empty() {
        symbols = sqlx::query_as::<_, (String,)>(
            "SELECT symbol FROM market_stock
             WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
               AND list_date IS NOT NULL
               AND list_date <= $1::date
               AND (delist_date IS NULL OR delist_date >= $2::date)
             ORDER BY symbol",
        )
        .bind(&end)
        .bind(&start)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(s,)| s)
        .collect();
    }
    info!(data_version_id = %dv_id, symbols = symbols.len(), "后台同步日线");

    tokio::spawn(async move {
        match quant_data::sync::sync_daily_bars(
            &state.db,
            &state.tushare,
            &symbols,
            &start,
            &end,
            &task_id,
        )
        .await
        {
            Ok(count) => {
                info!(task_id = %task_id, count = count, "后台同步日线完成");
            }
            Err(e) => {
                tracing::error!(task_id = %task_id, error = %e.to_string(), "后台同步日线失败");
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": dv_id, "status": "running"}}))
}

/// GET /api/v1/quant/data/sync/tasks/:task_id
///
/// 查询数据同步任务状态（同步/后台均适用）。
pub async fn sync_task_status(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let row: Option<(
        String,
        String,
        Option<Vec<String>>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT task_type, status, symbols, start_date, end_date,
                    total_count, success_count, failed_count, progress,
                    last_heartbeat_at, heartbeat_timeout_seconds,
                    retry_of_task_id, error_message
             FROM data_sync_task WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    match row {
        Some((
            task_type,
            status,
            symbols,
            start_date,
            end_date,
            total,
            success,
            failed,
            progress,
            last_heartbeat_at,
            heartbeat_timeout_seconds,
            retry_of_task_id,
            error_message,
        )) => {
            let stale = match (last_heartbeat_at, heartbeat_timeout_seconds) {
                (Some(last), Some(timeout)) if status == "running" => {
                    chrono::Utc::now().signed_duration_since(last).num_seconds() > timeout as i64
                }
                _ => false,
            };
            Json(json!({"code": 0, "data": {
                "task_id": task_id,
                "task_type": task_type,
                "status": status,
                "symbols": symbols.unwrap_or_default(),
                "start_date": start_date.map(|date| date.to_string()),
                "end_date": end_date.map(|date| date.to_string()),
                "total": total,
                "success": success,
                "failed": failed,
                "progress": progress,
                "last_heartbeat_at": last_heartbeat_at.map(|ts| ts.to_rfc3339()),
                "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
                "stale": stale,
                "retry_of_task_id": retry_of_task_id,
                "error_message": error_message,
            }}))
        }
        None => Json(json!({"code": 1, "message": "task not found"})),
    }
}

/// POST /api/v1/quant/data/sync-tasks/cleanup-stale
///
/// 将 heartbeat 超时的 running 同步任务标记为 failed，将超时的 cancel_requested
/// 任务收敛为 cancelled。默认 dry-run=false；可用 dry_run=true 先查看候选任务，
/// 避免误伤仍在正常推进的后台任务。
pub async fn cleanup_stale_sync_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleSyncTasksReq>,
) -> impl IntoResponse {
    let default_timeout_seconds =
        stale_cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = stale_cleanup_limit(req.limit);

    let rows: Vec<(
        String,
        String,
        String,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT task_id, task_type, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, started_at, created_at
         FROM data_sync_task
         WHERE status IN ('running', 'cancel_requested')
           AND COALESCE(last_heartbeat_at, started_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                task_type,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                started_at,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.or(*started_at).or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "task_id": task_id,
                    "task_type": task_type,
                    "status": status,
                    "progress": progress,
                    "last_heartbeat_at": last_heartbeat_at.map(|ts| ts.to_rfc3339()),
                    "started_at": started_at.map(|ts| ts.to_rfc3339()),
                    "created_at": created_at.map(|ts| ts.to_rfc3339()),
                    "observed_at": observed_at.map(|ts| ts.to_rfc3339()),
                    "heartbeat_timeout_seconds": timeout_seconds,
                    "cleanup_action": stale_sync_task_cleanup_action(status),
                    "terminal_status": stale_sync_task_cleanup_terminal_status(status)
                })
            },
        )
        .collect::<Vec<_>>();

    if req.dry_run || rows.is_empty() {
        return Json(json!({"code": 0, "data": {
            "dry_run": true,
            "candidate_count": candidates.len(),
            "updated_count": 0,
            "candidates": candidates
        }}));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = CASE
                 WHEN status = 'cancel_requested' THEN 'cancelled'
                 ELSE 'failed'
             END,
             failed_count = CASE
                 WHEN status = 'running' THEN GREATEST(COALESCE(failed_count, 0), 1)
                 ELSE COALESCE(failed_count, 0)
             END,
             completed_at = now(),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 CASE
                     WHEN status = 'cancel_requested' THEN
                         'stale cancel_requested task finalized by cleanup-stale: no worker acknowledgement within configured timeout'
                     ELSE
                         'stale running task timed out by cleanup-stale: no heartbeat within configured timeout'
                 END
             )
         WHERE task_id = ANY($1) AND status IN ('running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) => Json(json!({"code": 0, "data": {
            "dry_run": false,
            "candidate_count": candidates.len(),
            "updated_count": result.rows_affected(),
            "candidates": candidates
        }})),
        Err(error) => Json(
            json!({"code": 1, "message": format!("cleanup stale sync tasks failed: {}", error)}),
        ),
    }
}

fn sync_task_cancel_transition(status: &str) -> Option<&'static str> {
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
pub async fn cancel_sync_task(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status
         FROM data_sync_task
         WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(status) = status else {
        return Json(json!({"code": 1, "message": "task not found"}));
    };

    let Some(next_status) = sync_task_cancel_transition(&status) else {
        return Json(json!({
            "code": 1,
            "message": format!("task cannot be cancelled from status {}", status),
            "data": {
                "task_id": task_id,
                "status": status,
            }
        }));
    };

    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = $2,
             error_message = COALESCE(error_message, 'cancel requested by user'),
             last_heartbeat_at = now(),
             completed_at = CASE WHEN $2 = 'cancelled' THEN now() ELSE completed_at END
         WHERE task_id = $1 AND status = $3",
    )
    .bind(&task_id)
    .bind(next_status)
    .bind(&status)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) if result.rows_affected() == 1 => Json(json!({
            "code": 0,
            "data": {
                "task_id": task_id,
                "previous_status": status,
                "status": next_status,
            }
        })),
        Ok(_) => Json(json!({
            "code": 1,
            "message": "task status changed before cancel request was applied",
            "data": {
                "task_id": task_id,
                "previous_status": status,
            }
        })),
        Err(error) => Json(json!({
            "code": 1,
            "message": format!("Failed to cancel sync task: {}", error)
        })),
    }
}

/// POST /api/v1/quant/data/sync/financial
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_health_date_accepts_html_and_compact_dates() {
        assert_eq!(
            parse_health_date("2026-05-31").expect("html date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert_eq!(
            parse_health_date("20260531").expect("compact date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert!(parse_health_date("2026/05/31").is_err());
    }

    #[test]
    fn coverage_level_requires_full_range_coverage() {
        assert_eq!(coverage_level(0, 0), "green");
        assert_eq!(coverage_level(10, 10), "green");
        assert_eq!(coverage_level(10, 9), "red");
    }

    #[test]
    fn data_readiness_gate_blocks_required_red_only() {
        let checks = vec![
            json!({"item": "A股日线", "level": "green", "required": true}),
            json!({"item": "涨跌停历史", "level": "red", "required": false}),
            json!({"item": "ML预测", "level": "red", "required": true}),
        ];

        let blocked = data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredRed);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("ML预测"));
    }

    #[test]
    fn data_readiness_strict_gate_blocks_required_yellow() {
        let checks = vec![
            json!({"item": "A股日线", "level": "yellow", "required": true}),
            json!({"item": "可观测性标记", "level": "yellow", "required": false}),
        ];

        let blocked =
            data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredYellow);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("A股日线"));
    }

    #[test]
    fn stale_cleanup_parameters_are_bounded() {
        assert_eq!(stale_cleanup_default_timeout_seconds(None), 3600);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(10)), 60);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(100_000)), 86_400);
        assert_eq!(stale_cleanup_limit(None), 100);
        assert_eq!(stale_cleanup_limit(Some(0)), 1);
        assert_eq!(stale_cleanup_limit(Some(10_000)), 1000);
    }

    #[test]
    fn stale_cleanup_terminal_statuses_cover_cancel_requested() {
        assert_eq!(
            stale_sync_task_cleanup_terminal_status("running"),
            Some("failed")
        );
        assert_eq!(
            stale_sync_task_cleanup_terminal_status("cancel_requested"),
            Some("cancelled")
        );
        assert_eq!(
            stale_sync_task_cleanup_action("running"),
            "mark_running_failed"
        );
        assert_eq!(
            stale_sync_task_cleanup_action("cancel_requested"),
            "finalize_cancel_requested"
        );
        assert_eq!(stale_sync_task_cleanup_terminal_status("completed"), None);
        assert_eq!(stale_sync_task_cleanup_action("completed"), "ignore");
    }

    #[test]
    fn event_sync_source_quality_requires_official_source_after_limit_api_earliest_date() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("tushare:limit_list_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:limit_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:limit_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_allows_pre_api_limit_derivation_only_before_earliest_date() {
        let pre_api_date = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:daily_limit"), pre_api_date),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:daily_limit"), pre_api_date),
            "green"
        );
    }

    #[test]
    fn event_sync_source_quality_flags_suspension_markers_as_unverified() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("tushare:suspend_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("derived:susp_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("suspension_daily", Some("derived:susp_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_accepts_daily_absence_suspension_derivation() {
        let date = NaiveDate::from_ymd_opt(2015, 7, 9).unwrap();

        assert_eq!(
            event_sync_source_quality(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            "green"
        );
    }

    #[test]
    fn parse_etf_symbols_defaults_when_strategy_field_is_empty() {
        assert_eq!(parse_etf_symbols(None), default_mvo_etfs());
        assert_eq!(
            parse_etf_symbols(Some(json!(["518880.SH", "", " 511010.SH "]))),
            vec!["518880.SH".to_string(), "511010.SH".to_string()]
        );
    }

    #[test]
    fn phase7_coverage_grade_classifies_symbol_breadth() {
        assert_eq!(phase7_coverage_grade(0, 5_000), "missing");
        assert_eq!(phase7_coverage_grade(400, 5_000), "undercovered");
        assert_eq!(phase7_coverage_grade(2_000, 5_000), "partial");
        assert_eq!(phase7_coverage_grade(4_200, 5_000), "broad");
    }

    #[test]
    fn phase7_optional_source_readiness_separates_smoke_from_trainable_coverage() {
        assert_eq!(
            phase7_optional_source_readiness(false, 0, 0, 5_000),
            "schema_missing"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 0, 0, 5_000),
            "needs_sync"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 19, 3, 5_000),
            "sample_only_do_not_train"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 5_000, 2_000, 5_000),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 50_000, 4_200, 5_000),
            "ready_for_feature_factory"
        );
    }

    #[test]
    fn phase7_optional_source_sync_limit_is_bounded_for_local_runs() {
        assert_eq!(phase7_optional_source_sync_limit(None), 20);
        assert_eq!(phase7_optional_source_sync_limit(Some(0)), 1);
        assert_eq!(phase7_optional_source_sync_limit(Some(30)), 30);
        assert_eq!(
            phase7_optional_source_sync_limit(Some(10_000)),
            PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS
        );
    }

    #[test]
    fn phase7_optional_source_sync_sources_are_known_deduped_and_defaulted() {
        assert_eq!(
            phase7_optional_source_sync_sources(&[]).expect("default sources"),
            vec!["cashflow", "dividend", "repurchase"]
        );
        assert_eq!(
            phase7_optional_source_sync_sources(&[
                " CashFlow ".to_string(),
                "dividend".to_string(),
                "cashflow".to_string(),
            ])
            .expect("deduped sources"),
            vec!["cashflow", "dividend"]
        );
        assert_eq!(
            phase7_optional_source_sync_sources(&[
                "forecast".to_string(),
                "express".to_string(),
                "disclosure_date".to_string(),
                "forecast".to_string(),
            ])
            .expect("event sources"),
            vec!["forecast", "express", "disclosure_date"]
        );
        assert!(phase7_optional_source_sync_sources(&["unknown".to_string()]).is_err());
    }

    #[test]
    fn phase7_optional_source_tables_include_event_sources() {
        assert_eq!(
            phase7_optional_source_table("forecast"),
            Some("market_stock_forecast")
        );
        assert_eq!(
            phase7_optional_source_table("express"),
            Some("market_stock_express")
        );
        assert_eq!(
            phase7_optional_source_table("disclosure_date"),
            Some("market_stock_disclosure_date")
        );
        assert_eq!(
            phase7_optional_source_table("share_float"),
            Some("market_stock_share_float")
        );
    }

    #[test]
    fn phase7_optional_source_specs_include_financial_and_event_expansion_sources() {
        let specs = phase7_optional_source_specs();
        let sources: Vec<&str> = specs.iter().map(|spec| spec.source).collect();

        assert_eq!(
            sources,
            vec![
                "cashflow",
                "dividend",
                "repurchase",
                "forecast",
                "express",
                "disclosure_date",
                "share_float",
            ]
        );
        assert!(specs
            .iter()
            .any(|spec| spec.table == "market_stock_forecast"
                && spec.next_feature == "event_post_announcement_return_curve_pit_features"));
        assert!(specs
            .iter()
            .any(|spec| spec.table == "market_stock_share_float"
                && spec.next_feature == "unlock_supply_pressure_pit_features"));
    }

    #[test]
    fn phase7_new_alpha_candidate_sources_mark_p315_boundaries() {
        let sources = phase7_new_alpha_candidate_sources();
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();

        assert_eq!(
            by_source["market_margin_regime"]["admission_scope"],
            "regime_or_risk_budget_only"
        );
        assert_eq!(
            by_source["market_moneyflow_hsgt_regime"]["admission_scope"],
            "regime_or_risk_budget_only"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["readiness"],
            "industry_membership_permission_probe_required"
        );
        assert_eq!(
            by_source["block_trade_supply_demand"]["readiness"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            by_source["equity_pledge_pressure"]["readiness"],
            "permission_smoke_passed_schema_contract_ready"
        );
        assert_eq!(
            by_source["equity_incentive_execution_quality"]["readiness"],
            "schema_and_client_missing"
        );
        assert_eq!(
            by_source["block_trade_supply_demand"]["next_step"],
            "do_not_expand_same_family_shift_to_p320_new_source_admission"
        );
        assert_eq!(
            by_source["equity_pledge_pressure"]["next_step"],
            "review_apply_equity_pledge_schema_then_bounded_sync_plan"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["next_step"],
            "run_industry_membership_permission_smoke_then_schema_available_at_audit"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["why_not_trainable_now"],
            "行业 PIT 原始源接入后仍需通过全历史 membership snapshot/coverage 审计和 P3.10 诊断，不能直接进入训练"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]["gate_id"],
            "phase7_industry_membership_market_scope_gate_v1"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]
                ["required_universe_profile"],
            "main_chinext_non_st"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]["excluded_markets"][0],
            "科创板"
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_tracks_new_sources_and_stop_families() {
        let admission = phase7_p319_candidate_admission_sources(None);
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let by_source: BTreeMap<&str, &Value> = candidates
            .iter()
            .map(|source| {
                (
                    source["source_id"]
                        .as_str()
                        .expect("candidate source has source_id"),
                    source,
                )
            })
            .collect();

        assert_eq!(admission["stage"], "P3.22");
        assert_eq!(
            admission["hard_gate"],
            "permission_schema_available_at_first"
        );
        assert_eq!(admission["global_policy"]["pit_required"], true);
        assert_eq!(admission["global_policy"]["no_oos_reverse_tuning"], true);
        assert_eq!(
            admission["global_policy"]["model_algorithm_policy"]
                ["algorithm_is_secondary_to_source_economics"],
            true
        );
        assert_eq!(
            admission["stopped_same_family_sources"][0],
            "industry_prosperity_proxy"
        );
        assert!(admission["stopped_same_family_sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| source == "shareholder_structure_current_low_fanout_sleeve"));

        let p322_inventory = by_source["p322_source_inventory"];
        assert_eq!(
            p322_inventory["admission_decision"],
            "source_discovery_required_before_permission_smoke"
        );
        assert_eq!(
            p322_inventory["next_step"],
            "rank_candidate_sources_by_breadth_pit_availability_permission_and_economic_hypothesis_then_run_permission_smoke_for_top_source"
        );

        let equity = by_source["equity_incentive_execution_quality"];
        assert_eq!(
            equity["admission_decision"],
            "blocked_no_valid_equity_incentive_source"
        );
        assert_eq!(equity["pit_required"], true);
        assert_eq!(
            equity["available_at_policy"],
            "announcement_or_disclosure_date_required_before_event_effective_date"
        );
        assert_eq!(
            equity["blocked_reason"],
            "stk_rewards_is_management_compensation_shareholding_not_equity_incentive_execution_and_stk_reward_is_invalid"
        );
        assert_eq!(
            equity["source_discovery_evidence"][0]["candidate"],
            "tushare:stk_rewards"
        );
        assert_eq!(
            equity["source_discovery_evidence"][0]["status"],
            "rejected_semantic_mismatch"
        );

        let operations = by_source["futures_price_chain"];
        assert_eq!(
            operations["admission_decision"],
            "stopped_after_p310_component_economics_failed"
        );
        assert_eq!(operations["p310_status"], "completed_failed_economics");
        assert_eq!(
            operations["candidate_raw_sources"][0],
            "tushare:fina_mainbz"
        );
        assert_eq!(operations["candidate_raw_sources"][1], "tushare:fut_daily");
        assert_eq!(
            operations["source_discovery_evidence"][0]["status"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            operations["source_discovery_evidence"][0]["audit_endpoint"],
            "POST /api/v1/quant/data/main-business/available-at-audit"
        );
        assert_eq!(
            operations["source_discovery_evidence"][0]["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["candidate"],
            "tushare:futures_price_chain"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["smoke_source"],
            "futures_price_chain"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["decision"],
            "stop_futures_price_chain_after_p310_component_economics_failed"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["production_smoke"]["status"],
            "available"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );

        let pledge = by_source["equity_pledge_pressure"];
        assert_eq!(
            pledge["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(pledge["pit_required"], true);
        assert_eq!(pledge["p310_status"], "completed_failed_economics");
        assert_eq!(
            pledge["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );
        assert_eq!(pledge["candidate_raw_sources"][0], "tushare:pledge_stat");
        assert_eq!(pledge["candidate_raw_sources"][1], "tushare:pledge_detail");
        assert_eq!(
            pledge["source_discovery_evidence"][0]["native_available_at_candidate"],
            "not_native_end_date_is_measurement_date"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][0]["status"],
            "production_permission_smoke_passed"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][1]["native_available_at_candidate"],
            "ann_date"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][1]["status"],
            "production_permission_smoke_passed"
        );

        let shareholder = by_source["shareholder_structure"];
        assert_eq!(
            shareholder["admission_decision"],
            "stopped_after_bounded_wfa_train_robustness_failed"
        );
        assert_eq!(
            shareholder["diagnostics_summary"]["wfa_experiment_id"],
            "exp-39d6d820-0073-4a22-889f-e9eb67962785"
        );
        assert_eq!(shareholder["diagnostics_summary"]["stitched_oos"], false);

        let analyst = by_source["broad_analyst_revision"];
        assert_eq!(
            analyst["admission_decision"],
            "blocked_report_rc_current_api_unknown_source_after_permission_smoke"
        );
        assert_eq!(
            analyst["coverage_status"],
            "current_event_bundle_full_history_audit_completed_report_rc_blocked_current_api_unknown_source"
        );
        assert_eq!(analyst["current_tables"][0], "market_stock_forecast");
        assert_eq!(
            analyst["guardrail"],
            "must_be_broad_base_revision_not_sparse_event_post_return_overlay"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][0]["audit_endpoint"],
            "GET /api/v1/quant/data/broad-analyst-revision/audit"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][0]["audit_summary"]["decision"],
            "do_not_enter_p310_wfa_or_v19_train_selection"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["candidate"],
            "tushare:report_rc"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["smoke_source"],
            "report_rc"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["decision"],
            "do_not_build_schema_sync_factor_or_p310_from_report_rc_until_the_callable_api_name_or_permission_path_is_verified"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["production_smoke"]["error_code"],
            "40101"
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_reflects_live_futures_readiness() {
        let readiness = json!({
            "decision": decide_futures_price_chain_readiness(true, 6349, 0),
            "tables": []
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&readiness));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(operations["sync_status"], "raw_synced_mapping_missing");
        assert_eq!(operations["wfa_status"], "blocked");
        assert_eq!(operations["v19_train_selection"], "blocked");
        assert_eq!(
            operations["source_discovery_evidence"][1]["status"],
            "raw_synced_mapping_missing"
        );
        assert_eq!(operations["source_discovery_evidence"][1]["raw_rows"], 6349);
        assert_eq!(
            operations["futures_price_chain_readiness"]["decision"]["mapping_rows"],
            0
        );
    }

    #[test]
    fn broad_analyst_revision_audit_blocks_available_at_rule_violations() {
        let decision = decide_broad_analyst_revision_audit(1, 0.90, 0.80, 0.70, 0.50);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_available_at_rule_violation");
        assert_eq!(
            decision.admission_decision,
            "blocked_broad_analyst_revision_available_at_rule_failed"
        );
        assert_eq!(decision.p310_status, "not_started");
    }

    #[test]
    fn broad_analyst_revision_audit_stops_sparse_revision_semantics() {
        let decision = decide_broad_analyst_revision_audit(0, 0.56, 0.31, 0.11, 0.06);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_sparse_revision_semantics");
        assert_eq!(
            decision.readiness,
            "stopped_current_raw_bundle_revision_semantics_too_sparse"
        );
        assert_eq!(
            decision.admission_decision,
            "stopped_broad_analyst_revision_current_raw_bundle_after_audit_sparse_revision_semantics"
        );
    }

    #[test]
    fn broad_analyst_revision_audit_treats_narrow_forecast_as_sparse_bundle() {
        let decision = decide_broad_analyst_revision_audit(0, 0.56, 0.23, 0.11, 0.06);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_sparse_revision_semantics");
        assert_eq!(
            decision.blocked_reason,
            "true_forecast_revision_events_are_too_sparse_and_would_degenerate_into_event_overlay"
        );
    }

    #[test]
    fn phase7_block_trade_status_reports_bounded_sample_coverage() {
        let sources = phase7_new_alpha_candidate_sources_with_block_trade_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7BlockTradeSourceAudit {
                data_rows: 2168,
                symbols: 1000,
                covered_trade_days: 14,
                open_days_in_range: 14,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 2).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 19).unwrap()),
                pit_violation_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let block_trade = by_source["block_trade_supply_demand"];

        assert_eq!(
            block_trade["readiness"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            block_trade["raw_source_readiness"],
            "bounded_sample_ready_needs_history_coverage"
        );
        assert_eq!(
            block_trade["next_step"],
            "do_not_expand_same_family_shift_to_p320_new_source_admission"
        );
        assert_eq!(block_trade["raw_source_status"]["data_rows"], 2168);
        assert_eq!(block_trade["raw_source_status"]["covered_trade_days"], 14);
        assert_eq!(block_trade["raw_source_status"]["open_days_in_range"], 14);
        assert_eq!(block_trade["raw_source_status"]["pit_violation_rows"], 0);
    }

    #[test]
    fn phase7_block_trade_status_blocks_pit_violations() {
        let sources = phase7_new_alpha_candidate_sources_with_block_trade_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7BlockTradeSourceAudit {
                data_rows: 10,
                symbols: 5,
                covered_trade_days: 3,
                open_days_in_range: 3,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 3).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()),
                pit_violation_rows: 2,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let block_trade = by_source["block_trade_supply_demand"];

        assert_eq!(block_trade["readiness"], "raw_source_pit_failed");
        assert_eq!(
            block_trade["next_step"],
            "repair_available_at_before_any_diagnostics"
        );
        assert_eq!(block_trade["raw_source_status"]["pit_violation_rows"], 2);
    }

    #[test]
    fn phase7_industry_membership_status_requires_bounded_sync_before_factor_design() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 0,
                symbols: 0,
                index_codes: 0,
                current_active_stock_symbols: 0,
                current_covered_stock_symbols: 0,
                min_in_date: None,
                latest_in_date: None,
                min_out_date: None,
                latest_out_date: None,
                min_available_at: None,
                latest_available_at: None,
                pit_violation_rows: 0,
                invalid_interval_rows: 0,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_schema_ready_needs_bounded_sync"
        );
        assert_eq!(
            industry["next_step"],
            "run_bounded_industry_membership_sync"
        );
    }

    #[test]
    fn phase7_industry_membership_status_blocks_interval_and_pit_violations() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 10,
                symbols: 8,
                index_codes: 1,
                current_active_stock_symbols: 10,
                current_covered_stock_symbols: 10,
                min_in_date: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_in_date: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                min_out_date: Some(NaiveDate::from_ymd_opt(2009, 6, 1).unwrap()),
                latest_out_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                pit_violation_rows: 0,
                invalid_interval_rows: 1,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_raw_source_pit_failed"
        );
        assert_eq!(
            industry["next_step"],
            "repair_industry_membership_intervals_before_factor_design"
        );
    }

    #[test]
    fn phase7_industry_membership_status_blocks_current_coverage_under_threshold() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 10,
                symbols: 8,
                index_codes: 1,
                current_active_stock_symbols: 100,
                current_covered_stock_symbols: 80,
                min_in_date: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_in_date: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                min_out_date: Some(NaiveDate::from_ymd_opt(2009, 6, 1).unwrap()),
                latest_out_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                pit_violation_rows: 0,
                invalid_interval_rows: 0,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_current_coverage_undercovered"
        );
        assert_eq!(
            industry["next_step"],
            "run_full_l1_membership_sync_or_repair_missing_symbols"
        );
        assert_eq!(
            industry["raw_source_status"]["current_active_stock_symbols"],
            100
        );
        assert_eq!(
            industry["raw_source_status"]["current_covered_stock_symbols"],
            80
        );
        assert_eq!(
            industry["raw_source_status"]["current_missing_stock_symbols"],
            20
        );
        assert_eq!(
            industry["raw_source_status"]["current_stock_coverage_ratio"],
            json!(0.8)
        );
    }

    #[test]
    fn phase7_industry_membership_snapshot_readiness_blocks_multi_membership_before_coverage() {
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 99, 1, 2, 0, 0, 0, 0),
            "snapshot_multi_membership_blocked"
        );
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 94, 6, 0, 0, 0, 0, 0),
            "snapshot_coverage_gaps_need_review"
        );
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 100, 0, 0, 0, 0, 0, 0),
            "snapshot_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn phase7_industry_membership_snapshot_sql_uses_pit_interval_contract() {
        let sql = phase7_industry_membership_snapshot_summary_sql();

        assert!(sql.contains("DATE '2021-12-13'"));
        assert!(sql.contains("THEN 'SW2014'"));
        assert!(sql.contains("ELSE 'SW2021'"));
        assert!(sql.contains("membership.available_at <= days.trade_date"));
        assert!(sql.contains("membership.exit_available_at > days.trade_date"));
        assert!(!sql.contains("market_stock.industry"));
    }

    #[test]
    fn phase7_industry_membership_breakdown_sql_uses_same_source_version_gate() {
        for sql in [
            phase7_industry_membership_year_breakdown_sql(),
            phase7_industry_membership_market_breakdown_sql(),
        ] {
            assert!(sql.contains("DATE '2021-12-13'"));
            assert!(sql.contains("THEN 'SW2014'"));
            assert!(sql.contains("ELSE 'SW2021'"));
            assert!(sql.contains("membership.available_at <= days.trade_date"));
            assert!(sql.contains("membership.exit_available_at > days.trade_date"));
            assert!(!sql.contains("market_stock.industry"));
        }
    }

    #[test]
    fn phase7_industry_membership_market_scope_gate_requires_high_coverage_and_no_multi_membership()
    {
        assert!(phase7_industry_membership_market_scope_eligible(
            Some(0.995),
            0
        ));
        assert!(!phase7_industry_membership_market_scope_eligible(
            Some(0.9949),
            0
        ));
        assert!(!phase7_industry_membership_market_scope_eligible(
            Some(0.999),
            1
        ));
    }

    #[test]
    fn p315_market_level_source_status_marks_stale_and_builds_sync_payload() {
        let mut stats = BTreeMap::new();
        stats.insert(
            "market_margin_regime".to_string(),
            Phase7MarketLevelSourceAudit {
                data_rows: 5_846,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2016, 1, 4).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 5, 29).unwrap()),
                open_day_lag: Some(14),
            },
        );

        let sources = phase7_new_alpha_candidate_sources_with_market_status(
            &stats,
            &BTreeMap::new(),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let margin = by_source["market_margin_regime"];

        assert_eq!(margin["admission_scope"], "regime_or_risk_budget_only");
        assert_eq!(margin["readiness"], "market_level_stale_needs_sync");
        assert_eq!(margin["market_data_status"]["data_rows"], 5_846);
        assert_eq!(margin["market_data_status"]["min_trade_date"], "2016-01-04");
        assert_eq!(
            margin["market_data_status"]["latest_trade_date"],
            "2026-05-29"
        );
        assert_eq!(margin["market_data_status"]["open_day_lag"], 14);
        assert_eq!(margin["sync_task_payload"]["dataset"], "margin");
        assert_eq!(margin["sync_task_payload"]["source"], "tushare");
        assert_eq!(margin["sync_task_payload"]["start_date"], "20260530");
        assert_eq!(margin["sync_task_payload"]["end_date"], "20260620");
        assert_eq!(margin["sync_task_payload"]["background"], true);
    }

    #[test]
    fn p315_market_level_status_reports_zero_row_upstream_unavailable_after_sync_attempt() {
        let mut stats = BTreeMap::new();
        stats.insert(
            "market_moneyflow_hsgt_regime".to_string(),
            Phase7MarketLevelSourceAudit {
                data_rows: 2_339,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2016, 1, 4).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 5, 29).unwrap()),
                open_day_lag: Some(14),
            },
        );
        let mut sync_tasks = BTreeMap::new();
        sync_tasks.insert(
            "market_moneyflow_hsgt_regime".to_string(),
            Phase7MarketLevelSyncAudit {
                task_id: "dv-p315-hsgt-20260620".to_string(),
                task_type: "moneyflow_hsgt".to_string(),
                start_date: Some(NaiveDate::from_ymd_opt(2026, 5, 30).unwrap()),
                end_date: Some(NaiveDate::from_ymd_opt(2026, 6, 20).unwrap()),
                status: "completed".to_string(),
                total_count: 0,
                success_count: 0,
                failed_count: 0,
                error_message: None,
                completed_at: None,
            },
        );

        let sources = phase7_new_alpha_candidate_sources_with_market_status(
            &stats,
            &sync_tasks,
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let hsgt = by_source["market_moneyflow_hsgt_regime"];

        assert_eq!(
            hsgt["readiness"],
            "market_level_upstream_zero_rows_unavailable"
        );
        assert_eq!(hsgt["market_data_status"]["freshness_gate"], "failed");
        assert_eq!(hsgt["last_sync_task"]["task_id"], "dv-p315-hsgt-20260620");
        assert_eq!(
            hsgt["sync_remediation"]["status"],
            "not_retriable_until_upstream_resolved"
        );
    }

    #[test]
    fn p315_market_level_sync_task_datasets_are_supported() {
        assert_eq!(
            phase7_market_level_sync_dataset("market_margin_regime"),
            Some("margin")
        );
        assert_eq!(
            phase7_market_level_sync_dataset("market_moneyflow_hsgt_regime"),
            Some("moneyflow_hsgt")
        );
        assert_eq!(
            phase7_market_level_sync_dataset("industry_prosperity_proxy"),
            None
        );
    }

    #[test]
    fn phase7_optional_source_sync_plan_only_defaults_to_safe_mode() {
        assert!(phase7_optional_source_sync_plan_only(None));
        assert!(phase7_optional_source_sync_plan_only(Some(true)));
        assert!(!phase7_optional_source_sync_plan_only(Some(false)));
    }

    #[test]
    fn phase7_optional_source_batch_size_and_count_are_bounded() {
        assert_eq!(phase7_optional_source_batch_size(None), 20);
        assert_eq!(phase7_optional_source_batch_size(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_size(Some(30)), 30);
        assert_eq!(phase7_optional_source_batch_size(Some(10_000)), 200);

        assert_eq!(phase7_optional_source_batch_count(None), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(3)), 3);
        assert_eq!(phase7_optional_source_batch_count(Some(10_000)), 10);
    }

    #[test]
    fn phase7_optional_source_batch_offsets_are_resumable() {
        let offsets = phase7_optional_source_batch_offsets(5, 20, 3);
        assert_eq!(offsets, vec![5, 25, 45]);
        assert_eq!(phase7_optional_source_batch_next_offset(&offsets, 20), 65);
    }

    #[test]
    fn phase7_optional_source_batch_resume_offset_resets_after_execution() {
        let offsets = phase7_optional_source_batch_offsets(0, 20, 3);
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(true, &offsets, 20),
            60
        );
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(false, &offsets, 20),
            0
        );
    }

    #[test]
    fn phase7_optional_source_batch_launches_background_only_when_executing() {
        assert!(!phase7_optional_source_batch_child_background(true));
        assert!(phase7_optional_source_batch_child_background(false));
    }

    #[test]
    fn phase7_coverage_runner_profile_is_bounded_and_plan_only_by_default() {
        assert_eq!(phase7_coverage_runner_profile(None), "local_mac_safe");
        assert_eq!(phase7_coverage_runner_batch_size(None), 50);
        assert_eq!(phase7_coverage_runner_batch_size(Some(10_000)), 100);
        assert_eq!(phase7_coverage_runner_batch_count(None), 4);
        assert_eq!(phase7_coverage_runner_batch_count(Some(10_000)), 10);
        assert_eq!(phase7_coverage_runner_plan_only(None), true);
        assert_eq!(
            phase7_coverage_runner_sources(&[]).unwrap(),
            vec!["cashflow", "dividend", "financial"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
    }

    #[test]
    fn phase7_bounded_task_id_keeps_database_varchar64_contract() {
        let short = bounded_phase7_task_id(&["dv-phase7", "cashflow", "b001"]);
        assert_eq!(short, "dv-phase7-cashflow-b001");
        assert!(short.len() <= 64);

        let long = bounded_phase7_task_id(&[
            "dv-phase7-ff-full-auto-2016-20260526a",
            "r001",
            "optional",
            "cashflow",
            "b001",
        ]);
        assert!(long.len() <= 64, "task id length was {}", long.len());
        assert!(long.starts_with("dv-phase7-ff-full-auto-2016-20260526a-r001"));
        assert_ne!(
            long,
            "dv-phase7-ff-full-auto-2016-20260526a-r001-optional-cashflow-b001"
        );
    }

    #[test]
    fn phase7_coverage_runner_sources_include_bounded_financial_sync() {
        assert_eq!(
            phase7_coverage_runner_sources(&[
                " financial ".to_string(),
                "cashflow".to_string(),
                "financial".to_string(),
            ])
            .expect("financial source accepted"),
            vec!["financial", "cashflow"]
        );
    }

    #[test]
    fn phase7_coverage_runner_sources_include_bounded_event_sync() {
        assert_eq!(
            phase7_coverage_runner_sources(&[
                " forecast ".to_string(),
                "express".to_string(),
                "disclosure_date".to_string(),
                "forecast".to_string(),
            ])
            .expect("event sources accepted"),
            vec!["forecast", "express", "disclosure_date"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
        assert!(phase7_coverage_runner_sources(&["share_float".to_string()]).is_err());
    }

    #[test]
    fn share_float_coverage_runner_uses_float_date_chunks_and_safe_defaults() {
        assert_eq!(phase7_share_float_chunk_granularity(None), "year");
        assert_eq!(
            phase7_share_float_chunk_granularity(Some(" month ")),
            "month"
        );
        assert_eq!(
            phase7_share_float_chunk_granularity(Some("quarter")),
            "quarter"
        );
        assert_eq!(phase7_share_float_chunk_granularity(Some("week")), "year");
        assert!(phase7_share_float_coverage_plan_only(None));
        assert_eq!(phase7_share_float_coverage_max_chunks(None), 16);
        assert_eq!(phase7_share_float_coverage_max_chunks(Some(10_000)), 64);

        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2015, 5, 31).unwrap();
        let chunks = phase7_share_float_date_chunks(start, end, "year", 16);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].0, NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
        assert_eq!(chunks[0].1, NaiveDate::from_ymd_opt(2014, 12, 31).unwrap());
        assert_eq!(chunks[1].0, NaiveDate::from_ymd_opt(2015, 1, 1).unwrap());
        assert_eq!(chunks[1].1, NaiveDate::from_ymd_opt(2015, 5, 31).unwrap());
    }

    #[test]
    fn share_float_readiness_sql_audits_float_date_and_pit_leaks() {
        let sql = phase7_share_float_readiness_sql();

        assert!(sql.contains("MIN(float_date)"));
        assert!(sql.contains("MAX(float_date)"));
        assert!(sql.contains("MIN(available_at)"));
        assert!(sql.contains("MAX(available_at)"));
        assert!(sql.contains("COUNT(*) FILTER (WHERE available_at > float_date)"));
        assert!(sql.contains("float_date BETWEEN $1 AND $2"));
        assert!(sql.contains("available_at <= $2"));
    }

    #[test]
    fn share_float_readiness_requires_requested_float_date_span() {
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 19).unwrap();
        let expected_days = phase7_share_float_expected_days(start, end);
        let smoke_covered_days = phase7_share_float_covered_days_from_windows(
            vec![(
                NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
            )],
            start,
            end,
        );
        let smoke_only =
            phase7_share_float_feature_readiness(10, smoke_covered_days, expected_days, 0);

        assert_eq!(smoke_only, "needs_float_date_backfill");

        let full_covered_days = phase7_share_float_covered_days_from_windows(
            vec![
                (start, NaiveDate::from_ymd_opt(2015, 12, 31).unwrap()),
                (NaiveDate::from_ymd_opt(2016, 1, 1).unwrap(), end),
            ],
            start,
            end,
        );
        let full_span =
            phase7_share_float_feature_readiness(50_000, full_covered_days, expected_days, 0);
        assert_eq!(full_span, "ready_for_pit_feature_factory");

        let full_span_with_late_rows =
            phase7_share_float_feature_readiness(50_000, full_covered_days, expected_days, 12);
        assert_eq!(
            full_span_with_late_rows,
            "ready_for_pit_feature_factory_with_late_exclusion"
        );
    }

    #[test]
    fn phase7_coverage_runner_auto_continue_targets_full_coverage_with_bounds() {
        assert!(!phase7_coverage_runner_auto_continue(None));
        assert!(phase7_coverage_runner_auto_continue(Some(true)));

        assert_eq!(phase7_coverage_runner_max_rounds(None, false), 1);
        assert_eq!(
            phase7_coverage_runner_max_rounds(None, true),
            PHASE7_COVERAGE_RUNNER_MAX_ROUNDS
        );
        assert_eq!(phase7_coverage_runner_max_rounds(Some(0), true), 1);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(12), true), 12);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(1_000), true), 20);

        assert_eq!(phase7_coverage_runner_target_ratio(None), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.10)), 0.30);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.75)), 0.75);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(2.0)), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(f64::NAN)), 1.0);
    }

    #[test]
    fn phase7_coverage_runner_autopilot_uses_round_budget_not_manual_batches() {
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, true
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, false
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            false, false
        ));
        assert!(!phase7_coverage_runner_should_build_immediate_batches(
            false, true
        ));
        assert_eq!(phase7_coverage_autopilot_batch_offsets(3), vec![0, 0, 0]);
    }

    #[test]
    fn phase7_optional_source_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_optional_source_uncovered_symbols_sql("cashflow").expect("cashflow sql");

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'cashflow'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_event_source_symbol_resolver_uses_attempt_ledger() {
        for source in ["forecast", "express", "disclosure_date"] {
            let sql = phase7_optional_source_uncovered_symbols_sql(source).expect("event sql");

            assert!(sql.contains("data_sync_attempt attempt"));
            assert!(sql.contains(&format!("attempt.source = '{}'", source)));
            assert!(sql.contains("attempt.status = 'completed'"));
            assert!(sql.contains("attempt.symbol = stock.symbol"));
            assert!(sql.contains("attempt.start_date <= $1"));
            assert!(sql.contains("attempt.end_date >= $2"));
            assert!(sql.contains("OFFSET $3 LIMIT $4"));
        }
    }

    #[test]
    fn phase7_financial_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_financial_uncovered_symbols_sql();

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'financial'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_coverage_runner_source_state_prefers_requested_window_attempts() {
        let audit = json!({
            "listed_stock_count": 100,
            "optional_data_sources": [
                {
                    "source": "cashflow",
                    "feature_readiness": "ready_for_feature_factory",
                    "symbol_coverage_ratio": 0.99
                }
            ],
            "financial_coverage": [
                {
                    "name": "market_financial_statement",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.98
                },
                {
                    "name": "market_financial_indicator",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.96
                }
            ]
        });
        let mut window_attempts = BTreeMap::new();
        window_attempts.insert("cashflow".to_string(), 25);
        window_attempts.insert("financial".to_string(), 40);

        let (_, coverage) =
            phase7_coverage_runner_source_state_for_window(&audit, Some(&window_attempts));

        assert_eq!(coverage.get("cashflow"), Some(&0.25));
        assert_eq!(coverage.get("financial"), Some(&0.40));
    }

    #[test]
    fn phase7_coverage_runner_partial_gate_does_not_block_full_coverage_target() {
        assert!(phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            0.30,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            true,
        ));
    }

    #[test]
    fn phase7_financial_source_readiness_maps_coverage_to_training_gate() {
        assert_eq!(phase7_financial_source_readiness("missing"), "needs_sync");
        assert_eq!(
            phase7_financial_source_readiness("thin"),
            "undercovered_do_not_train"
        );
        assert_eq!(
            phase7_financial_source_readiness("partial"),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_financial_source_readiness("broad"),
            "ready_for_feature_factory"
        );
    }

    #[test]
    fn phase7_coverage_runner_stops_when_source_is_feature_ready() {
        assert!(phase7_coverage_runner_should_plan_source(
            "sample_only_do_not_train"
        ));
        assert!(phase7_coverage_runner_should_plan_source(
            "coverage_reference_missing"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "partial_feature_candidate"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "ready_for_feature_factory"
        ));
    }

    #[test]
    fn phase7_permission_smoke_sources_default_and_deduped() {
        assert_eq!(
            phase7_permission_smoke_sources(&[]),
            vec!["cashflow", "dividend", "repurchase"]
        );

        let requested = vec![
            " CashFlow ".to_string(),
            "dividend".to_string(),
            "cashflow".to_string(),
            "repurchase".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["cashflow", "dividend", "repurchase"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_industry_membership_probe() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"industry_membership"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"industry_membership"));

        let requested = vec![
            " Industry_Membership ".to_string(),
            "industry_membership".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["industry_membership"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_main_business_probe_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"main_business"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"main_business"));

        let requested = vec![" Main_Business ".to_string(), "main_business".to_string()];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["main_business"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_report_rc_probe_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"report_rc"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"report_rc"));

        let requested = vec![" Report_RC ".to_string(), "report_rc".to_string()];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["report_rc"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_futures_price_chain_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"futures_price_chain"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"futures_price_chain"));

        let requested = vec![
            " Futures_Price_Chain ".to_string(),
            "futures_price_chain".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["futures_price_chain"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_equity_pledge_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"equity_pledge_pressure"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"equity_pledge_pressure"));

        let requested = vec![
            " Equity_Pledge_Pressure ".to_string(),
            "equity_pledge_pressure".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["equity_pledge_pressure"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_shareholder_structure_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"shareholder_structure"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"shareholder_structure"));

        let requested = vec![
            " Shareholder_Structure ".to_string(),
            "shareholder_structure".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["shareholder_structure"]
        );
    }

    #[test]
    fn futures_price_chain_schema_contract_blocks_training_until_mapping_and_pit_audit() {
        let contract = phase7_futures_price_chain_schema_contract();

        assert_eq!(contract["source_id"], "futures_price_chain");
        assert_eq!(
            contract["admission_decision"],
            "schema_mapping_available_at_audit_required_before_sync"
        );
        assert_eq!(
            contract["pit_policy"]["intraday_stock_decision_rule"],
            "use_previous_available_futures_trade_date_until_source_published_at_is_audited"
        );
        assert_eq!(contract["promotion_gate"]["p310_status"], "not_started");
        assert_eq!(contract["raw_tables"][0]["table"], "market_futures_daily");
        assert_eq!(
            contract["mapping_tables"][0]["table"],
            "market_futures_product_exposure_mapping_pit"
        );
        assert_eq!(
            contract["mapping_tables"][1]["table"],
            "market_futures_product_exclusion_gate_pit"
        );
    }

    #[test]
    fn equity_pledge_schema_contract_separates_stat_snapshot_from_detail_available_at() {
        let contract = phase7_equity_pledge_schema_contract();

        assert_eq!(contract["source_id"], "equity_pledge_pressure");
        assert_eq!(contract["stage"], "P3.20B");
        assert_eq!(contract["ddl_path"], "sql/phase7_equity_pledge_source.sql");
        assert_eq!(
            contract["raw_sources"][0]["native_available_at_candidate"],
            "not_native_end_date_is_measurement_date"
        );
        assert_eq!(
            contract["raw_sources"][1]["native_available_at_candidate"],
            "ann_date"
        );
        assert_eq!(contract["tables"][0]["table"], "market_stock_pledge_stat");
        assert_eq!(contract["tables"][1]["table"], "market_stock_pledge_detail");
        assert_eq!(
            contract["available_at_policy"]["pledge_detail"],
            "ann_date is native available_at candidate and must be persisted as available_at."
        );
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");

        let ddl = include_str!("../../../sql/phase7_equity_pledge_source.sql");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_pledge_stat"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_pledge_detail"));
        assert!(ddl.contains("available_at >= end_date"));
        assert!(ddl.contains("available_at >= ann_date"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, source_row_hash)"));
        assert_eq!(
            contract["tables"][1]["natural_key"],
            json!(["symbol", "ann_date", "source_row_hash"])
        );
        assert!(contract["tables"][1]["nullable_source_fields"]
            .as_array()
            .unwrap()
            .contains(&json!("pledge_start_date")));
    }

    #[test]
    fn shareholder_structure_schema_contract_requires_four_ann_date_tables() {
        let contract = phase7_shareholder_structure_schema_contract();

        assert_eq!(contract["source_id"], "shareholder_structure");
        assert_eq!(contract["stage"], "P3.21B");
        assert_eq!(
            contract["ddl_path"],
            "sql/phase7_shareholder_structure_source.sql"
        );
        assert_eq!(contract["raw_sources"][0]["api"], "stk_holdernumber");
        assert_eq!(contract["raw_sources"][1]["api"], "top10_holders");
        assert_eq!(contract["raw_sources"][2]["api"], "top10_floatholders");
        assert_eq!(contract["raw_sources"][3]["api"], "stk_holdertrade");
        assert_eq!(
            contract["available_at_policy"]["default"],
            "available_at equals native ann_date for all four shareholder_structure raw tables; end_date is a measurement period only."
        );
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");

        let ddl = include_str!("../../../sql/phase7_shareholder_structure_source.sql");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_holder_number"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_top10_holders"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_top10_float_holders"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_holder_trade"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, end_date)"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, end_date, source_row_hash)"));
        assert!(ddl.contains("available_at >= ann_date"));
    }

    #[test]
    fn shareholder_structure_raw_schema_lands_period_anomalies_for_audit() {
        let contract = phase7_shareholder_structure_schema_contract();
        assert_eq!(
            contract["available_at_policy"]["period_snapshot_rule"],
            "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated."
        );

        let ddl = include_str!("../../../sql/phase7_shareholder_structure_source.sql");
        assert!(!ddl.contains("CONSTRAINT market_stock_holder_number_period_pit_check"));
        assert!(!ddl.contains("CONSTRAINT market_stock_top10_holders_period_pit_check"));
        assert!(!ddl.contains("CONSTRAINT market_stock_top10_float_holders_period_pit_check"));
        assert!(
            ddl.contains("DROP CONSTRAINT IF EXISTS market_stock_holder_number_period_pit_check")
        );

        let expected_constraints = shareholder_structure_expected_schema();
        let holder_number_constraints = expected_constraints
            .iter()
            .find(|(table, _)| *table == "market_stock_holder_number")
            .map(|(_, constraints)| constraints)
            .expect("holder number schema");
        assert!(!holder_number_constraints
            .iter()
            .any(|constraint| constraint.contains("period_pit_check")));
    }

    #[test]
    fn shareholder_structure_sync_request_forces_bounded_raw_dataset() {
        let req = ShareholderStructureSyncReq {
            symbols: vec!["000001.SZ".to_string()],
            source_filters: Vec::new(),
            start_date: Some("20140101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("holder-smoke".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "shareholder_structure");
        assert_eq!(req.source, "tushare:shareholder_structure");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["000001.SZ".to_string()]);
        assert_eq!(
            req.reason.as_deref(),
            Some("p3.21 shareholder structure bounded raw sync")
        );
        assert!(req.background);
    }

    #[test]
    fn shareholder_structure_sync_request_preserves_source_filters_for_staged_history_sync() {
        let req = ShareholderStructureSyncReq {
            symbols: Vec::new(),
            source_filters: vec!["holder_number".to_string(), "holder_trade".to_string()],
            start_date: Some("20140101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("shareholder-low-fanout".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(
            req.source_filters,
            vec!["holder_number".to_string(), "holder_trade".to_string()]
        );
        assert_eq!(req.dataset, "shareholder_structure");
    }

    #[test]
    fn shareholder_structure_coverage_decision_requires_breadth_pit_and_duplicates() {
        assert_eq!(
            decide_shareholder_structure_coverage_audit(false, 0, 0.0, 0, 0, 0, 0)
                ["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 0, 0.0, 0, 0, 0, 0)
                ["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 1, 0, 0, 0)
                ["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 0, 1, 0, 0)
                ["admission_decision"],
            "duplicate_source_rows_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 0, 0, 1, 0)
                ["admission_decision"],
            "raw_quality_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 500_000, 0.80, 0, 0, 0, 1)
                ["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 100, 0.01, 0, 0, 0, 0)
                ["admission_decision"],
            "bounded_sample_passed_needs_full_history_sync"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 500_000, 0.80, 0, 0, 0, 0)
                ["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn shareholder_structure_strict_low_fanout_gate_allows_clean_exclusion_scope_only() {
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(false, 0, 0.0, 0, 0)
                ["admission_decision"],
            "apply_schema_before_strict_low_fanout_gate"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 1, 0)
                ["admission_decision"],
            "duplicate_admissible_rows_failed"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 0, 1)
                ["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 100, 0.80, 0, 0)
                ["admission_decision"],
            "bounded_sample_passed_needs_more_admissible_history"
        );
        let ready = decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 0, 0);
        assert_eq!(
            ready["gate_id"],
            "shareholder_structure_low_fanout_strict_pit_gate_v1"
        );
        assert_eq!(
            ready["admission_decision"],
            "strict_low_fanout_ready_for_p310_diagnostics"
        );
        assert_eq!(ready["p310_status"], "ready_for_p310_diagnostics_only");
        assert_eq!(ready["v19_train_selection"], "blocked");
    }

    #[test]
    fn shareholder_structure_sync_plan_estimates_year_batches_and_blocks_full_range_blast() {
        let batches = (2014..=2026)
            .map(|year| ShareholderStructureSyncPlanBatch {
                year,
                start_date: NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                end_date: NaiveDate::from_ymd_opt(year, 12, 31).unwrap(),
                symbol_count: 4_300,
                quarter_count: 4,
            })
            .collect::<Vec<_>>();

        let plan = shareholder_structure_sync_plan_response(
            NaiveDate::from_ymd_opt(2014, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 12, 31).unwrap(),
            batches,
        );

        assert_eq!(plan["mode"], "read_only_bounded_sync_plan");
        assert_eq!(plan["safe_to_run_full_range"], false);
        assert_eq!(plan["recommended_batch_granularity"], "year");
        assert_eq!(plan["batch_count"], 13);
        assert_eq!(plan["estimated_total_units"], 447304);
        assert_eq!(
            plan["batches"][0]["recommended_request"]["data_version_id"],
            "shareholder-structure-2014"
        );
        assert_eq!(
            plan["batches"][0]["recommended_low_fanout_request"]["source_filters"],
            json!(["holder_number", "holder_trade"])
        );
        assert_eq!(
            plan["batches"][0]["recommended_top10_request"]["source_filters"],
            json!(["top10_holders", "top10_float_holders"])
        );
        assert_eq!(plan["batches"][0]["estimated_units"], 34408);
        assert_eq!(plan["batches"][0]["symbol_quarter_units"], 34400);
        assert_eq!(plan["batches"][0]["global_ann_date_units"], 8);
    }

    #[test]
    fn futures_price_chain_readiness_decision_blocks_empty_schema_from_training() {
        let decision = decide_futures_price_chain_readiness(true, 0, 0);

        assert_eq!(decision["schema_status"], "created");
        assert_eq!(decision["sync_status"], "not_started");
        assert_eq!(
            decision["admission_decision"],
            "schema_created_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decision["p310_status"],
            "blocked_until_coverage_readiness_passes"
        );
    }

    #[test]
    fn futures_price_chain_readiness_blocks_raw_synced_without_mapping() {
        let decision = decide_futures_price_chain_readiness(true, 100, 0);

        assert_eq!(decision["schema_status"], "created");
        assert_eq!(decision["sync_status"], "raw_synced_mapping_missing");
        assert_eq!(
            decision["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_extracts_product_symbol_from_contract_codes() {
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("CU1811.SHF"),
            Some("CU".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("if1811.CFX"),
            Some("IF".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("  rb2405.SHFE "),
            Some("RB".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("A1901.DCE"),
            Some("A".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("AGL.SHF"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("PTA.ZCE"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("TL.CFX"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("TL1.CFX"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("SCTAS2011.INE"),
            Some("SCTAS".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("1811.SHF"),
            None
        );
    }

    #[test]
    fn futures_price_chain_mapping_audit_blocks_zero_mapping_rows() {
        let decision = decide_futures_price_chain_mapping_audit(true, 42, 0, 0, 42, 0, 0, 0, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(decision["raw_product_count"], 42);
        assert_eq!(decision["mapped_product_count"], 0);
        assert_eq!(decision["excluded_product_count"], 0);
        assert_eq!(decision["covered_product_count"], 0);
        assert_eq!(decision["missing_product_count"], 42);
        assert_eq!(decision["v19_train_selection"], "blocked");
        assert_eq!(decision["wfa_status"], "blocked");
    }

    #[test]
    fn futures_price_chain_mapping_audit_counts_exclusion_gate_as_covered_not_mapped() {
        let decision = decide_futures_price_chain_mapping_audit(true, 56, 0, 6, 50, 0, 0, 0, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(decision["mapped_product_count"], 0);
        assert_eq!(decision["excluded_product_count"], 6);
        assert_eq!(decision["covered_product_count"], 6);
        assert_eq!(decision["missing_product_count"], 50);
        assert_eq!(
            decision["next_step"],
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate"
        );
    }

    #[test]
    fn futures_price_chain_mapping_audit_sql_covers_raw_sources_and_pit_mapping() {
        let raw_sql = futures_price_chain_raw_product_summary_sql();
        assert!(raw_sql.contains("market_futures_daily"));
        assert!(raw_sql.contains("market_futures_warehouse_receipt"));
        assert!(raw_sql.contains("market_futures_holding_rank"));
        assert!(raw_sql.contains("substring(ts_code from '^([A-Za-z]+)[0-9]{4}"));
        assert!(raw_sql.contains("substring(symbol from '^[A-Za-z]+'"));
        assert!(raw_sql.contains("length(product_symbol_raw) > 2"));
        assert!(raw_sql.contains("right(product_symbol_raw, 1) = 'L'"));
        assert!(raw_sql.contains("available_at < trade_date"));

        let mapping_sql = futures_price_chain_mapping_summary_sql();
        assert!(mapping_sql.contains("market_futures_product_exposure_mapping_pit"));
        assert!(mapping_sql.contains("exposure_type NOT IN ('sw_industry', 'stock_symbol')"));
        assert!(mapping_sql.contains("valid_to IS NOT NULL AND valid_to < valid_from"));
        assert!(mapping_sql.contains("available_at < valid_from"));

        let exclusion_sql = futures_price_chain_exclusion_summary_sql();
        assert!(exclusion_sql.contains("market_futures_product_exclusion_gate_pit"));
        assert!(exclusion_sql.contains("gate_scope = 'futures_price_chain_factor'"));
        assert!(exclusion_sql.contains("valid_to IS NOT NULL AND valid_to < valid_from"));
    }

    #[test]
    fn futures_price_chain_raw_product_sql_normalizes_known_aliases_before_mapping_gate() {
        for sql in [
            futures_price_chain_raw_product_summary_sql(),
            futures_price_chain_coverage_breakdown_sql(),
            futures_price_chain_raw_endpoint_breakdown_sql(),
        ] {
            assert!(sql.contains("right(product_symbol_raw, 4) = 'ACTV'"));
            assert!(sql.contains("left(product_symbol_raw, length(product_symbol_raw) - 4)"));
            assert!(sql.contains("WHEN product_symbol_raw = 'PTA' THEN 'TA'"));
        }
    }

    #[test]
    fn futures_price_chain_exclusion_seed_blocks_non_industry_derivatives() {
        let sql = include_str!("../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('IM', 'futures_price_chain_factor', 'financial_index_future'"));
        assert!(sql.contains("中证1000股指期货"));
        assert!(sql.contains("('IO', 'futures_price_chain_factor', 'non_industry_derivative'"));
        assert!(sql.contains("沪深300股指期权"));
        assert!(sql.contains("('SCTAS', 'futures_price_chain_factor', 'non_industry_derivative'"));
        assert!(sql.contains("原油 TAS"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_contains_first_high_confidence_product_batch() {
        let sql = include_str!("../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("INSERT INTO market_futures_product_exposure_mapping_pit"));
        assert!(sql.contains("('CU', 'sw_industry', '801050.SI'"));
        assert!(sql.contains("('RB', 'sw_industry', '801040.SI'"));
        assert!(sql.contains("('SC', 'sw_industry', '801960.SI'"));
        assert!(sql.contains("('TA', 'sw_industry', '801030.SI'"));
        assert!(sql.contains("('FG', 'sw_industry', '801710.SI'"));
        assert!(sql.contains("p319q-product-sw2021-l1-direct-commodity-v1"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_contains_second_evidence_backed_product_batch() {
        let sql = include_str!("../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('A', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("('CF', 'sw_industry', '801130.SI', 1"));
        assert!(sql.contains("('I', 'sw_industry', '801040.SI', -1"));
        assert!(sql.contains("('PS', 'sw_industry', '801730.SI', 1"));
        assert!(sql.contains("('SP', 'sw_industry', '801140.SI', -1"));
        assert!(sql.contains("p319q-product-sw2021-l1-evidence-backed-v1"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_covers_officially_sourced_residual_products() {
        let sql = include_str!("../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('EC', 'sw_industry', '801170.SI', 1"));
        assert!(sql.contains("('OP', 'sw_industry', '801140.SI', 1"));
        assert!(sql.contains("('ME', 'sw_industry', '801030.SI', 1"));
        assert!(sql.contains("('TC', 'sw_industry', '801950.SI', 1"));
        assert!(sql.contains("('ER', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("('WS', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("p319q-product-sw2021-l1-official-residual-v1"));
    }

    #[test]
    fn futures_price_chain_coverage_audit_sql_breaks_down_year_endpoint_product_exchange() {
        let coverage_sql = futures_price_chain_coverage_breakdown_sql();

        assert!(coverage_sql.contains("market_futures_daily"));
        assert!(coverage_sql.contains("market_futures_warehouse_receipt"));
        assert!(coverage_sql.contains("market_futures_holding_rank"));
        assert!(coverage_sql.contains("EXTRACT(YEAR FROM trade_date)"));
        assert!(
            coverage_sql.contains("GROUP BY endpoint, trade_year, product_symbol, exchange_key")
        );
        assert!(coverage_sql.contains("available_at < trade_date"));

        let attempt_sql = futures_price_chain_sync_attempt_breakdown_sql();
        assert!(attempt_sql.contains("data_sync_attempt"));
        assert!(attempt_sql.contains("futures_price_chain_daily"));
        assert!(attempt_sql.contains("futures_price_chain_wsr"));
        assert!(attempt_sql.contains("futures_price_chain_holding"));
        assert!(attempt_sql.contains("error_message IS NOT NULL"));
        assert!(attempt_sql.contains("futures_calendar"));
        assert!(attempt_sql.contains("NOT EXISTS (SELECT 1 FROM futures_calendar)"));
        assert!(attempt_sql.contains("non_open_attempt_count"));
    }

    #[test]
    fn futures_price_chain_coverage_audit_blocks_until_mapping_is_complete() {
        let decision = decide_futures_price_chain_coverage_audit(true, 6_349, 56, 6, 50, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(
            decision["p310_status"],
            "blocked_until_mapping_and_coverage_readiness_pass"
        );
        assert_eq!(decision["wfa_status"], "blocked");
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_coverage_promotion_gate_opens_only_p310_when_ready() {
        let decision = decide_futures_price_chain_coverage_audit(true, 27_279_522, 94, 94, 0, 0, 0);
        let promotion_gate = futures_price_chain_coverage_promotion_gate(&decision);

        assert_eq!(promotion_gate["p310_status"], "ready_for_p310_diagnostics");
        assert_eq!(
            promotion_gate["factor_builder"],
            "ready_for_p310_diagnostics"
        );
        assert_eq!(promotion_gate["wfa_status"], "blocked");
        assert_eq!(promotion_gate["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_mapping_template_sql_uses_sw2021_l1_targets() {
        let sql = futures_price_chain_industry_targets_sql();

        assert!(sql.contains("market_stock_industry_membership_pit"));
        assert!(sql.contains("classification_source = 'SW2021'"));
        assert!(sql.contains("industry_level = 'L1'"));
        assert!(sql.contains("index_code"));
        assert!(sql.contains("industry_code"));
    }

    #[test]
    fn futures_price_chain_mapping_candidate_validation_rejects_invalid_target_and_empty_evidence()
    {
        let raw_products = BTreeSet::from(["CU".to_string()]);
        let sw2021_targets = BTreeSet::from(["801050.SI".to_string()]);
        let candidate = FuturesPriceChainMappingCandidate {
            product_symbol: "CU".to_string(),
            exposure_type: "sw_industry".to_string(),
            exposure_code: "801999.SI".to_string(),
            direction: 1,
            weight: 1.0,
            valid_from: "2014-01-01".to_string(),
            valid_to: None,
            available_at: "2014-01-01".to_string(),
            source: "manual-review".to_string(),
            mapping_version: "p319n-test".to_string(),
            evidence: json!({}),
        };

        let result = validate_futures_price_chain_mapping_candidate(
            &candidate,
            &raw_products,
            &sw2021_targets,
        );

        assert!(!result.passed);
        assert!(result
            .errors
            .contains(&"unknown_sw2021_l1_exposure_code".to_string()));
        assert!(result.errors.contains(&"evidence_required".to_string()));
    }

    #[test]
    fn futures_price_chain_mapping_candidate_decision_blocks_incomplete_coverage() {
        let decision = decide_futures_price_chain_mapping_candidate_validation(57, 56, 0, 1);

        assert_eq!(
            decision["admission_decision"],
            "mapping_candidate_coverage_incomplete_before_insert"
        );
        assert_eq!(decision["write_enabled"], false);
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn phase7_p319_candidate_admission_reflects_mapping_audit_missing_products() {
        let mapping_audit = json!({
            "decision": decide_futures_price_chain_mapping_audit(true, 12, 8, 0, 4, 0, 0, 0, 0, 0),
            "raw_product_count": 12,
            "mapped_product_count": 8,
            "excluded_product_count": 0,
            "missing_product_count": 4,
            "missing_products": ["AL", "CU", "IF", "RB"],
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&mapping_audit));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(operations["wfa_status"], "blocked");
        assert_eq!(operations["v19_train_selection"], "blocked");
        assert_eq!(
            operations["futures_price_chain_readiness"]["missing_product_count"],
            4
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_keeps_futures_stopped_after_data_gate_ready() {
        let coverage_ready = json!({
            "decision": decide_futures_price_chain_coverage_audit(true, 1000, 10, 10, 0, 0, 0),
            "raw_product_count": 10,
            "covered_product_count": 10,
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&coverage_ready));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "stopped_after_p310_component_economics_failed"
        );
        assert_eq!(operations["p310_status"], "completed_failed_economics");
        assert_eq!(
            operations["futures_price_chain_readiness"]["decision"]["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn futures_price_chain_sync_request_forces_bounded_raw_dataset() {
        let req = FuturesPriceChainSyncReq {
            symbols: vec!["CU".to_string()],
            exchanges: vec!["SHFE".to_string()],
            start_date: Some("20181113".to_string()),
            end_date: Some("20181113".to_string()),
            data_version_id: Some("task-1".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "futures_price_chain");
        assert_eq!(req.source, "tushare:futures_price_chain");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["CU".to_string()]);
        assert_eq!(req.exchanges, vec!["SHFE".to_string()]);
        assert!(req.background);
    }

    #[test]
    fn equity_pledge_sync_request_forces_bounded_raw_dataset() {
        let req = EquityPledgePressureSyncReq {
            symbols: vec!["000001.SZ".to_string()],
            start_date: Some("20240101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("pledge-smoke".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "equity_pledge_pressure");
        assert_eq!(req.source, "tushare:equity_pledge_pressure");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["000001.SZ".to_string()]);
        assert_eq!(
            req.reason.as_deref(),
            Some("p3.20 equity pledge pressure bounded raw sync")
        );
        assert!(req.background);
    }

    #[test]
    fn equity_pledge_readiness_blocks_until_schema_and_raw_pit_pass() {
        assert_eq!(
            decide_equity_pledge_readiness(false, 0, 0, 0)["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 0, 0, 0)["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 10, 5, 2)["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 10, 5, 0)["admission_decision"],
            "coverage_readiness_audit_required_before_p310"
        );
    }

    #[test]
    fn equity_pledge_status_reflects_readiness_audit_in_candidate_lists() {
        let readiness = json!({
            "decision": decide_equity_pledge_readiness(true, 10, 5, 0),
            "schema_passed": true,
            "tables": [],
        });

        let sources = phase7_new_alpha_candidate_sources_with_equity_pledge_status(
            phase7_new_alpha_candidate_sources(),
            Some(&readiness),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| (source["source"].as_str().unwrap(), source))
            .collect();

        assert_eq!(
            by_source["equity_pledge_pressure"]["readiness"],
            "raw_source_ready_for_coverage_audit"
        );

        let mut admission = phase7_p319_candidate_admission_sources(None);
        apply_p320_equity_pledge_readiness(&mut admission, &readiness);
        let pledge = admission["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| candidate["source_id"] == "equity_pledge_pressure")
            .unwrap();

        assert_eq!(
            pledge["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(pledge["sync_status"], "full_history_raw_sync_completed");
        assert_eq!(pledge["p310_status"], "completed_failed_economics");
        assert_eq!(
            pledge["latest_raw_readiness"]["decision"]["admission_decision"],
            "coverage_readiness_audit_required_before_p310"
        );
    }

    #[test]
    fn equity_pledge_coverage_decision_requires_breadth_and_pit_before_p310() {
        assert_eq!(
            decide_equity_pledge_coverage_audit(false, 0, 0.0, 0, 0)["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 0, 0.0, 0, 0)["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 100, 0.5, 1, 0)["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 100, 0.01, 0, 0)["admission_decision"],
            "bounded_sample_passed_needs_full_history_sync"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 50_000, 0.35, 0, 0)["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn main_business_available_at_join_decision_passes_only_complete_pit_mapping() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let ann = NaiveDate::from_ymd_opt(2023, 4, 25).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: Some(ann),
            source: Some("financial_statement".to_string()),
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(decision.passed);
        assert_eq!(decision.status, "passed");
        assert_eq!(
            decision.readiness,
            "available_at_join_ready_for_schema_design"
        );
        assert_eq!(decision.missing_mapping_count, 0);
        assert_eq!(decision.pit_violation_count, 0);
        assert_eq!(decision.source_counts.get("financial_statement"), Some(&1));
    }

    #[test]
    fn main_business_available_at_join_decision_blocks_missing_mapping() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: None,
            source: None,
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_available_at_join_gaps");
        assert_eq!(decision.missing_mapping_count, 1);
        assert_eq!(decision.pit_violation_count, 0);
    }

    #[test]
    fn main_business_available_at_join_decision_blocks_available_at_before_period_end() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let impossible_available_at = NaiveDate::from_ymd_opt(2023, 3, 1).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: Some(impossible_available_at),
            source: Some("financial_statement".to_string()),
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_pit_available_at_violations");
        assert_eq!(decision.missing_mapping_count, 0);
        assert_eq!(decision.pit_violation_count, 1);
    }

    #[test]
    fn main_business_readiness_requires_rows_periods_and_clean_pit() {
        assert_eq!(
            main_business_raw_source_readiness(0, 4, 4, 0, 0),
            "raw_source_missing"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 0, 2),
            "raw_source_pit_failed"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 3, 0, 0),
            "period_sync_incomplete"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 1, 0),
            "period_sync_failed"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 0, 0),
            "raw_source_ready_for_full_history_coverage_audit"
        );
    }

    #[test]
    fn main_business_readiness_sql_audits_type_and_pit_source_table() {
        let sql = main_business_readiness_summary_sql();

        assert!(sql.contains("FROM market_stock_main_business"));
        assert!(sql.contains("business_type = $3"));
        assert!(sql.contains("available_at < end_date"));
        assert!(sql.contains("COUNT(DISTINCT end_date)"));
    }

    #[test]
    fn main_business_readiness_parses_available_at_mapping_gaps() {
        assert_eq!(main_business_missing_available_at_rows(None), 0);
        assert_eq!(main_business_missing_available_at_rows(Some("")), 0);
        assert_eq!(
            main_business_missing_available_at_rows(Some(
                "missing_available_at_rows=5907, out_of_universe_rows=42, raw_rows=37766"
            )),
            5907
        );
        assert_eq!(
            main_business_out_of_universe_rows(Some(
                "missing_available_at_rows=5907, out_of_universe_rows=42, raw_rows=37766"
            )),
            42
        );
    }

    #[test]
    fn phase7_permission_smoke_limit_is_bounded() {
        assert_eq!(phase7_permission_smoke_limit(None), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(0)), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(3)), 3);
        assert_eq!(
            phase7_permission_smoke_limit(Some(100)),
            PHASE7_PERMISSION_SMOKE_MAX_ROWS
        );
    }

    #[test]
    fn tushare_permission_error_classifier_recognizes_permission_and_auth() {
        assert_eq!(
            classify_tushare_permission_error("API error (code=2002): 没有权限"),
            "permission_denied"
        );
        assert_eq!(
            classify_tushare_permission_error("Authentication error: TUSHARE_TOKEN 未设置"),
            "auth_error"
        );
        assert_eq!(classify_tushare_permission_error("timeout"), "error");
    }

    #[test]
    fn optional_source_full_market_sync_requires_explicit_mode() {
        assert!(!optional_source_all_symbols_allowed(None));
        assert!(!optional_source_all_symbols_allowed(Some("")));
        assert!(!optional_source_all_symbols_allowed(Some("full")));
        assert!(optional_source_all_symbols_allowed(Some("full_market")));
    }

    #[test]
    fn cancel_transition_matches_task_lifecycle() {
        assert_eq!(sync_task_cancel_transition("pending"), Some("cancelled"));
        assert_eq!(
            sync_task_cancel_transition("running"),
            Some("cancel_requested")
        );
        assert_eq!(
            sync_task_cancel_transition("cancel_requested"),
            Some("cancel_requested")
        );
        assert_eq!(sync_task_cancel_transition("completed"), None);
        assert_eq!(sync_task_cancel_transition("failed"), None);
        assert_eq!(sync_task_cancel_transition("cancelled"), None);
    }
}

/// POST /api/v1/quant/data/sync/fund-basic
///
/// 同步 ETF/LOF 基金基本信息（名称、类型、管理人）到 market_stock 表。
pub async fn sync_fund_basic(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match quant_data::sync::sync_fund_basic(&state.db, &state.tushare).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/namechange
///
/// 同步股票名称变更历史（ST 状态 PIT 合规数据）
pub async fn sync_namechange(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match quant_data::sync::sync_namechange(&state.db, &state.tushare).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension
///
/// 同步当日停牌股票数据
#[derive(Debug, serde::Deserialize)]
pub struct SyncSuspensionRequest {
    pub trade_date: String, // YYYYMMDD
}

pub async fn sync_suspension(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncSuspensionRequest>,
) -> impl IntoResponse {
    match quant_data::sync::sync_suspension(&state.db, &state.tushare, &req.trade_date).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/limit
#[derive(Debug, serde::Deserialize)]
pub struct SyncLimitListRequest {
    pub trade_date: String, // YYYYMMDD
}

pub async fn sync_limit_list(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncLimitListRequest>,
) -> impl IntoResponse {
    match quant_data::sync::sync_limit_list(&state.db, &state.tushare, &req.trade_date).await {
        Ok(count) => Json(json!({"code": 0, "data": {"count": count}})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension/backfill
///
/// 批量回填停牌历史数据（按交易日历逐日同步）
#[derive(Debug, serde::Deserialize)]
pub struct BackfillRequest {
    pub start_date: String, // YYYYMMDD
    pub end_date: String,   // YYYYMMDD
    #[serde(default)]
    pub force_tushare: bool,
}

pub async fn sync_suspension_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    if !req.force_tushare {
        return match quant_data::sync::backfill_suspension_completion_markers(
            &state.db,
            &req.start_date,
            &req.end_date,
        )
        .await
        {
            Ok(markers) => Json(json!({
                "code": 0,
                "data": {
                    "mode": "completion_marker_backfill",
                    "markers": markers,
                    "source": "derived:susp_existing"
                }
            })),
            Err(e) => Json(json!({"code": 1, "message": e})),
        };
    }

    match quant_data::sync::sync_suspension_range(
        &state.db,
        &state.tushare,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(total) => Json(json!({
            "code": 0,
            "data": {
                "mode": "tushare_range",
                "total_records": total,
                "source": "tushare:suspend_d"
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/suspension/derive-from-daily
///
/// 基于已同步 A 股日线缺失派生历史停牌事实；不补价格。
pub async fn derive_suspension_from_daily(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    match quant_data::sync::derive_suspension_from_daily_absence(
        &state.db,
        &req.start_date,
        &req.end_date,
    )
    .await
    {
        Ok(inserted) => Json(json!({
            "code": 0,
            "data": {
                "mode": "derived_from_daily_absence",
                "inserted_records": inserted,
                "source": "derived:daily_absence_suspension"
            }
        })),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

/// POST /api/v1/quant/data/sync/limit/backfill
pub async fn sync_limit_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> impl IntoResponse {
    let start = match parse_health_date(&req.start_date) {
        Ok(date) => date,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    let end = match parse_health_date(&req.end_date) {
        Ok(date) => date,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };
    if start > end {
        return Json(json!({"code": 1, "message": "start_date 不能晚于 end_date"}));
    }

    let earliest = proven_limit_list_earliest_date();
    let mut derived_records = 0u64;
    let mut tushare_records = 0usize;
    let mut marker_rows = 0u64;

    if start < earliest {
        let derive_end = end.min(earliest - Duration::days(1));
        if derive_end >= start {
            match quant_data::sync::derive_limit_list_from_daily_bars(
                &state.db,
                &yyyymmdd(start),
                &yyyymmdd(derive_end),
            )
            .await
            {
                Ok(n) => derived_records += n,
                Err(e) => return Json(json!({"code": 1, "message": e})),
            }
        }
    }

    if end >= earliest {
        let tushare_start = start.max(earliest);
        if req.force_tushare {
            match quant_data::sync::sync_limit_list_range(
                &state.db,
                &state.tushare,
                &yyyymmdd(tushare_start),
                &yyyymmdd(end),
            )
            .await
            {
                Ok(n) => tushare_records += n,
                Err(e) => return Json(json!({"code": 1, "message": e})),
            }
        }
        match quant_data::sync::backfill_limit_completion_markers(
            &state.db,
            &yyyymmdd(tushare_start),
            &yyyymmdd(end),
            if req.force_tushare {
                "tushare:limit_list_d_range"
            } else {
                "derived:limit_existing"
            },
        )
        .await
        {
            Ok(n) => marker_rows += n,
            Err(e) => return Json(json!({"code": 1, "message": e})),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "mode": if req.force_tushare { "derive_then_tushare_range" } else { "derive_then_marker_backfill" },
            "derived_records": derived_records,
            "tushare_records": tushare_records,
            "marker_rows": marker_rows,
            "derived_until": if start < earliest { Some(yyyymmdd(end.min(earliest - Duration::days(1)))) } else { None::<String> },
        }
    }))
}

/// POST /api/v1/quant/data/sync/historical
///
/// 补齐历史数据（2006-2015），参数：start_date、end_date
#[derive(Debug, serde::Deserialize)]
pub struct SyncHistoricalRequest {
    pub start_date: String, // YYYYMMDD
    pub end_date: String,   // YYYYMMDD
}

pub async fn sync_historical(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncHistoricalRequest>,
) -> impl IntoResponse {
    let db = &state.db;
    let client = &state.tushare;
    let empty_symbols: Vec<String> = vec![];

    let mut results = Vec::new();

    // 1. 日线行情
    info!(
        "[sync-hist] 同步日线行情 {} → {}",
        req.start_date, req.end_date
    );
    match quant_data::sync::sync_daily_bars(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "daily-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("daily_bar: {} 条", n)),
        Err(e) => results.push(format!("daily_bar 失败: {}", e)),
    }

    // 2. 复权因子
    info!("[sync-hist] 同步复权因子");
    match quant_data::sync::sync_adj_factor(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "adj-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("adj_factor: {} 条", n)),
        Err(e) => results.push(format!("adj_factor 失败: {}", e)),
    }

    // 3. 日线基础
    info!("[sync-hist] 同步日线基础指标");
    match quant_data::sync::sync_daily_basic(
        db,
        client,
        &empty_symbols,
        &req.start_date,
        &req.end_date,
        "basic-hist",
    )
    .await
    {
        Ok(n) => results.push(format!("daily_basic: {} 条", n)),
        Err(e) => results.push(format!("daily_basic 失败: {}", e)),
    }

    Json(json!({"code": 0, "data": {"results": results}}))
}

/// 组件4: 账号依赖加工数据健康检查请求。
/// 无 start_date/end_date = 轻量新鲜度检查；带时间段 = 逐年深度红黄绿扫描。
#[derive(Debug, serde::Deserialize)]
pub struct AccountDataHealthReq {
    pub user_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]
struct StrategyHealthConfig {
    combo_name: String,
    equity_curve_task_id: String,
    prediction_set_id: Option<String>,
    etf_symbols: Vec<String>,
    signal_source: String,
    prediction_blend_weight: f64,
}

const DEFAULT_MVO_ETFS: &[&str] = &[
    "518880.SH",
    "511010.SH",
    "513500.SH",
    "513100.SH",
    "159980.SZ",
    "159985.SZ",
    "501018.SH",
];

fn proven_limit_list_earliest_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2019, 11, 28).expect("valid limit_list_d earliest date")
}

fn parse_health_date(value: &str) -> Result<NaiveDate, String> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .map_err(|_| format!("日期格式无效: {}，需要 YYYY-MM-DD 或 YYYYMMDD", value))
}

fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn default_mvo_etfs() -> Vec<String> {
    DEFAULT_MVO_ETFS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect()
}

fn parse_etf_symbols(value: Option<Value>) -> Vec<String> {
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

fn coverage_level(expected: i64, actual: i64) -> &'static str {
    if expected <= 0 || actual >= expected {
        "green"
    } else {
        "red"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventSyncSourceQuality {
    Official,
    AcceptedDerived,
    UnverifiedDerived,
    Missing,
    Unknown,
}

fn event_sync_source_quality(
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

fn event_sync_source_level(
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

fn event_sync_source_label(
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

fn lag_level(lag_days: i64, yellow_after_days: i64, red_after_days: i64) -> &'static str {
    if lag_days > red_after_days {
        "red"
    } else if lag_days > yellow_after_days {
        "yellow"
    } else {
        "green"
    }
}

fn check_item(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataReadinessGate {
    BlockRequiredRed,
    BlockRequiredYellow,
}

fn data_readiness_required(check: &Value) -> bool {
    check
        .get("required")
        .and_then(|value| value.as_bool())
        .unwrap_or(true)
}

fn data_readiness_blocking_checks<'a>(
    checks: &'a [Value],
    gate: DataReadinessGate,
) -> Vec<&'a Value> {
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

fn data_readiness_failure_message(
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

async fn write_data_readiness_audit_event(
    db: &sqlx::PgPool,
    account_id: &str,
    operation: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, $2, 'paper_account', $3, 'system', $4, $5)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(format!("data_readiness.{}", status))
    .bind(account_id)
    .bind(format!("{} data readiness {}", operation, status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据门禁审计失败: {}", error))
}

async fn write_data_health_check_audit_event(
    db: &sqlx::PgPool,
    entity_id: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, 'data_readiness.checked', 'data_health_check', $2, 'system', $3, $4)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(entity_id)
    .bind(format!("account data health check {}", status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据健康检查审计失败: {}", error))
}

pub async fn check_paper_account_data_readiness(
    db: &sqlx::PgPool,
    account_id: &str,
    range: Option<(NaiveDate, NaiveDate)>,
    gate: DataReadinessGate,
    operation: &str,
) -> Result<Value, String> {
    let account: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id
         FROM paper_account WHERE paper_account_id=$1 AND status='active'",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询账号数据门禁: {}", e))?;
    let (account_id, account_name, strategy_id) =
        account.ok_or_else(|| format!("账号不存在或未激活: {}", account_id))?;
    let sid = strategy_id.unwrap_or_else(|| "v19".to_string());

    let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> = sqlx::query_as(
        "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                signal_source, prediction_blend_weight
         FROM strategy_config WHERE strategy_id=$1 AND status='active'",
    )
    .bind(&sid)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询策略数据门禁: {}", e))?;

    let checks = if let Some((
        combo_name,
        equity_curve_task_id,
        prediction_set_id,
        etf_symbols,
        signal_source,
        prediction_blend_weight,
    )) = cfg
    {
        let cfg = StrategyHealthConfig {
            combo_name,
            equity_curve_task_id,
            prediction_set_id,
            etf_symbols: parse_etf_symbols(etf_symbols),
            signal_source,
            prediction_blend_weight,
        };
        check_account_deps(db, &account_name, &sid, &cfg, range, true, true).await
    } else {
        vec![check_item(
            &account_name,
            &sid,
            "策略配置",
            "red",
            "strategy_config 未找到激活配置",
            None,
            None,
            Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
        )]
    };

    let red = checks
        .iter()
        .filter(|check| check["level"] == "red")
        .count();
    let yellow = checks
        .iter()
        .filter(|check| check["level"] == "yellow")
        .count();
    let blocked = data_readiness_blocking_checks(&checks, gate);
    let blocking_items = blocked.len();
    let failure_message = if blocking_items == 0 {
        None
    } else {
        Some(data_readiness_failure_message(
            operation,
            &account_name,
            &sid,
            &blocked,
        ))
    };
    let passed = blocking_items == 0;
    let report = json!({
        "operation": operation,
        "mode": if range.is_some() { "range_coverage" } else { "freshness" },
        "account_id": account_id,
        "account": account_name,
        "strategy": sid,
        "gate": match gate {
            DataReadinessGate::BlockRequiredRed => "block_required_red",
            DataReadinessGate::BlockRequiredYellow => "block_required_yellow",
        },
        "passed": passed,
        "red": red,
        "yellow": yellow,
        "blocking_items": blocking_items,
        "checks": checks,
    });

    let audit_status = if passed { "passed" } else { "failed" };
    if let Err(error) =
        write_data_readiness_audit_event(db, &account_id, operation, audit_status, &report).await
    {
        let base_message = failure_message
            .clone()
            .unwrap_or_else(|| format!("{} 数据门禁审计失败", operation));
        return Err(format!("{}; {}", base_message, error));
    }

    if passed {
        Ok(report)
    } else {
        Err(failure_message.unwrap_or_else(|| format!("{} 数据门禁失败", operation)))
    }
}

/// POST /api/v1/quant/data/account-data-health
/// 遍历激活账号(模拟+实盘) → 其策略依赖的加工数据(combo因子/PIT combo/权益曲线/滚动IC) → 红黄绿。
pub async fn account_data_health(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AccountDataHealthReq>,
) -> impl IntoResponse {
    let db = &state.db;
    let range = match (req.start_date.as_deref(), req.end_date.as_deref()) {
        (Some(start), Some(end)) => match (parse_health_date(start), parse_health_date(end)) {
            (Ok(start), Ok(end)) if start <= end => Some((start, end)),
            (Ok(_), Ok(_)) => {
                return Json(json!({"code": 1, "message": "start_date 不能晚于 end_date"}));
            }
            (Err(message), _) | (_, Err(message)) => {
                return Json(json!({"code": 1, "message": message}));
            }
        },
        _ => None,
    };
    let deep = range.is_some();
    let mut checks: Vec<serde_json::Value> = Vec::new();

    // 活跃账号(可选按 user 过滤)
    let accounts: Vec<(String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id, COALESCE(leverage_enabled,false)
         FROM paper_account WHERE status='active'
           AND ($1::text IS NULL OR user_id = $1)
         ORDER BY name",
    )
    .bind(req.user_id.as_deref())
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let mut cfg_cache: BTreeMap<String, Option<StrategyHealthConfig>> = BTreeMap::new();
    let mut deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut common_deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for (_acct_id, acct_name, strat_id, _lev) in &accounts {
        let sid = strat_id.as_deref().unwrap_or("v19");
        if !cfg_cache.contains_key(sid) {
            let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> =
                sqlx::query_as(
                    "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                        signal_source, prediction_blend_weight
                 FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                )
                .bind(sid)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
            cfg_cache.insert(
                sid.to_string(),
                cfg.map(
                    |(
                        combo_name,
                        equity_curve_task_id,
                        prediction_set_id,
                        etf_symbols,
                        signal_source,
                        prediction_blend_weight,
                    )| {
                        StrategyHealthConfig {
                            combo_name,
                            equity_curve_task_id,
                            prediction_set_id,
                            etf_symbols: parse_etf_symbols(etf_symbols),
                            signal_source,
                            prediction_blend_weight,
                        }
                    },
                ),
            );
        }

        let Some(Some(cfg)) = cfg_cache.get(sid) else {
            checks.push(check_item(
                acct_name,
                sid,
                "策略配置",
                "red",
                "strategy_config 未找到激活配置",
                None,
                None,
                Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
            ));
            continue;
        };

        let mut cached_checks = Vec::new();
        if let Some((start, end)) = range {
            let common_key = format!("{}|{}|{}", start, end, cfg.etf_symbols.join(","));
            if !common_deps_cache.contains_key(&common_key) {
                common_deps_cache.insert(
                    common_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, false)
                        .await,
                );
            }
            if let Some(common_checks) = common_deps_cache.get(&common_key) {
                cached_checks.extend(common_checks.iter().cloned());
            }

            let deps_key = format!(
                "{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or("")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, false, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        } else {
            let deps_key = format!(
                "{}|{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or(""),
                cfg.etf_symbols.join(",")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        }

        checks.extend(cached_checks.into_iter().map(|mut check| {
            check["account"] = json!(acct_name);
            check["strategy"] = json!(sid);
            if let Some(params) = check.get_mut("fix_params") {
                if params.get("strategy_id").and_then(|v| v.as_str()).is_some() {
                    params["strategy_id"] = json!(sid);
                }
            }
            check
        }));
    }

    let red = checks.iter().filter(|c| c["level"] == "red").count();
    let yellow = checks.iter().filter(|c| c["level"] == "yellow").count();
    let status = if red > 0 {
        "red"
    } else if yellow > 0 {
        "yellow"
    } else {
        "green"
    };
    let mut data = serde_json::json!({
        "mode": if deep {"range_coverage"} else {"freshness"},
        "accounts_checked": accounts.len(),
        "red": red,
        "yellow": yellow,
        "checks": checks
    });
    let audit_report = json!({
        "operation": "account_data_health",
        "status": status,
        "user_id": req.user_id,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "data": data.clone()
    });
    let audit_entity_id = audit_report
        .get("user_id")
        .and_then(|value| value.as_str())
        .unwrap_or("all_active_accounts");
    match write_data_health_check_audit_event(db, audit_entity_id, status, &audit_report).await {
        Ok(()) => data["audit_persisted"] = json!(true),
        Err(error) => {
            data["audit_persisted"] = json!(false);
            data["audit_error"] = json!(error);
        }
    }

    Json(serde_json::json!({
        "code": 0,
        "data": data
    }))
}

async fn latest_market_date(db: &sqlx::PgPool) -> NaiveDate {
    sqlx::query_scalar::<_, Option<NaiveDate>>(
        "SELECT GREATEST(
            (SELECT MAX(trade_date) FROM market_stock_daily_bar_adj),
            (SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH')
        )",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .flatten()
    .unwrap_or_else(|| chrono::Utc::now().date_naive())
}

fn strategy_needs_prediction(cfg: &StrategyHealthConfig) -> bool {
    matches!(
        cfg.signal_source.as_str(),
        "prediction" | "prediction_blend"
    ) && (cfg.signal_source == "prediction" || cfg.prediction_blend_weight > f64::EPSILON)
}

async fn resolve_live_prediction_set(
    db: &sqlx::PgPool,
    cfg: &StrategyHealthConfig,
    date: NaiveDate,
) -> Option<String> {
    if !strategy_needs_prediction(cfg) {
        return None;
    }
    if let Some(prediction_set_id) = cfg.prediction_set_id.as_ref() {
        return Some(prediction_set_id.clone());
    }
    sqlx::query_scalar(
        "SELECT ps.prediction_set_id FROM prediction_set ps
         WHERE ps.status = 'ready'
           AND ps.training_end_date IS NOT NULL
           AND ps.training_end_date < $1
           AND ps.start_date <= $1 AND ps.end_date >= $1
         ORDER BY ps.training_end_date DESC, ps.created_at DESC
         LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

async fn expected_open_day_count(db: &sqlx::PgPool, start: NaiveDate, end: NaiveDate) -> i64 {
    let calendar_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_trade_calendar
         WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true",
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0);
    if calendar_count > 0 {
        calendar_count
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .unwrap_or(0)
    }
}

async fn event_completion_latest_source(
    db: &sqlx::PgPool,
    table: &str,
    task_type: &str,
) -> (Option<NaiveDate>, Option<String>, i64) {
    let table_sql = format!(
        "SELECT MAX(trade_date), COUNT(*)::int8
         FROM {table}
         WHERE trade_date = (SELECT MAX(trade_date) FROM {table})"
    );
    let (table_date, table_rows): (Option<NaiveDate>, i64) = sqlx::query_as(&table_sql)
        .fetch_one(db)
        .await
        .unwrap_or((None, 0));

    let task_row: Option<(NaiveDate, Option<String>, i64)> = sqlx::query_as(
        "SELECT end_date, source, COALESCE(success_count, total_count, 0)::int8
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
         ORDER BY end_date DESC, completed_at DESC NULLS LAST
         LIMIT 1",
    )
    .bind(task_type)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    match (table_date, task_row) {
        (Some(td), Some((task_date, source, rows))) if task_date >= td => {
            (Some(task_date), source, rows)
        }
        (Some(td), _) => (Some(td), None, table_rows),
        (None, Some((task_date, source, rows))) => (Some(task_date), source, rows),
        (None, None) => (None, None, 0),
    }
}

async fn event_completed_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT end_date)::int8 FROM data_sync_task
         WHERE task_type = $1 AND status = 'completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0)
}

async fn event_verified_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    let rows: Vec<(NaiveDate, Option<String>)> = sqlx::query_as(
        "SELECT DISTINCT end_date, source
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    rows.into_iter()
        .filter(|(date, source)| {
            matches!(
                event_sync_source_quality(task_type, source.as_deref(), *date),
                EventSyncSourceQuality::Official | EventSyncSourceQuality::AcceptedDerived
            )
        })
        .map(|(date, _)| date)
        .collect::<std::collections::BTreeSet<_>>()
        .len() as i64
}

async fn rolling_pit_ic_quarter_coverage(
    db: &sqlx::PgPool,
    horizon: i16,
    start: NaiveDate,
    end: NaiveDate,
) -> (i64, i64, Option<NaiveDate>, Option<NaiveDate>) {
    sqlx::query_as(
        "WITH quarters AS (
           SELECT MIN(trade_date) AS as_of
           FROM (SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
                 WHERE trade_date >= $2 AND trade_date <= $3) d
           GROUP BY date_trunc('quarter', trade_date)
         ),
         covered AS (
           SELECT q.as_of
           FROM quarters q
           WHERE EXISTS (
             SELECT 1
             FROM factor_evaluation fe
             WHERE fe.horizon = $1
               AND fe.end_date <= q.as_of
               AND fe.mean_ic IS NOT NULL
               AND fe.ic_ir IS NOT NULL
               AND fe.factor_code !~ '^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)'
           )
         )
         SELECT COUNT(q.as_of)::int8,
                COUNT(c.as_of)::int8,
                MIN(q.as_of) FILTER (WHERE c.as_of IS NULL),
                MAX(q.as_of) FILTER (WHERE c.as_of IS NULL)
         FROM quarters q
         LEFT JOIN covered c USING (as_of)",
    )
    .bind(horizon)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or((0, 0, None, None))
}

/// 检查单个账号策略依赖的数据。range=None 查新鲜度；range=Some 查区间覆盖率。
async fn check_account_deps(
    db: &sqlx::PgPool,
    acct: &str,
    sid: &str,
    cfg: &StrategyHealthConfig,
    range: Option<(NaiveDate, NaiveDate)>,
    include_common: bool,
    include_strategy: bool,
) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let last_mkt = latest_market_date(db).await;
    let repair_start = yyyymmdd(last_mkt - Duration::days(30));
    let repair_end = yyyymmdd(last_mkt);

    if include_strategy {
        out.push(check_item(
            acct,
            sid,
            "策略配置",
            "green",
            format!(
                "combo={}, curve={}, signal_source={}, prediction_set={}, blend_weight={}, ETF{}只",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.signal_source,
                cfg.prediction_set_id.as_deref().unwrap_or("自动选择"),
                cfg.prediction_blend_weight,
                cfg.etf_symbols.len()
            ),
            None,
            None,
            None,
        ));
    }

    if let Some((start, end)) = range {
        let expected_days = expected_open_day_count(db, start, end).await;

        if include_common {
            let (a_expected_days, a_days, a_min_expected, a_min_symbols, a_weak_days): (
                i64,
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               LEFT JOIN market_stock_suspension susp
                 ON susp.symbol = s.symbol
                AND susp.trade_date = c.trade_date
                AND COALESCE(susp.suspend_type, 'S') = 'S'
               WHERE susp.symbol IS NULL
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_stock_daily_bar_adj
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_level = if a_days < a_expected_days || a_expected_days < expected_days {
                "red"
            } else if a_weak_days > 0 {
                "yellow"
            } else {
                "green"
            };
            let (a_fix_endpoint, a_fix_params) =
                if a_days < a_expected_days || a_expected_days < expected_days {
                    (
                        Some("/api/v1/quant/data/sync/daily/background"),
                        Some(json!({
                            "symbols": [],
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "data_version_id": format!("health-repair-daily-{}", yyyymmdd(end))
                        })),
                    )
                } else if a_weak_days > 0 {
                    (
                        Some("/api/v1/quant/data/sync/suspension/derive-from-daily"),
                        Some(json!({
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "force_tushare": false
                        })),
                    )
                } else {
                    (None, None)
                };
            out.push(check_item(
                acct,
                sid,
                "A股日线区间覆盖",
                a_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市/停牌口径单日最少{}/{}只，低于90%天数{}",
                    start, end, expected_days, a_days, a_min_symbols, a_min_expected, a_weak_days
                ),
                a_fix_endpoint,
                a_fix_params,
                None,
            ));

            let (
                a_adj_expected_days,
                a_adj_days,
                a_adj_min_expected,
                a_adj_min_symbols,
                a_adj_weak_days,
            ): (i64, i64, i64, i64, i64) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_adjustment_factor
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_adj_level =
                if a_adj_days < a_adj_expected_days || a_adj_expected_days < expected_days {
                    "red"
                } else if a_adj_weak_days > 0 {
                    "yellow"
                } else {
                    "green"
                };
            out.push(check_item(
                acct,
                sid,
                "A股复权因子区间覆盖",
                a_adj_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市口径单日最少{}/{}只，低于90%天数{}",
                    start,
                    end,
                    expected_days,
                    a_adj_days,
                    a_adj_min_symbols,
                    a_adj_min_expected,
                    a_adj_weak_days
                ),
                Some("/api/v1/quant/data/sync/adj-factor/background"),
                Some(json!({
                    "symbols": [],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let (etf_expected, etf_actual, etf_bad, etf_confirmed_absent): (
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
                 firsts AS (
                   SELECT symbol, MIN(trade_date) AS first_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                   GROUP BY symbol
                 ),
                 calendar AS (
                   SELECT DISTINCT trade_date
                   FROM market_trade_calendar
                   WHERE is_open = true
                     AND trade_date >= $2::date
                     AND trade_date <= $3::date
                 ),
                 expected AS (
                   SELECT s.symbol, c.trade_date
                   FROM symbols s
                   LEFT JOIN firsts f ON f.symbol = s.symbol
                   JOIN calendar c
                     ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
                 ),
                 actual AS (
                   SELECT DISTINCT symbol, trade_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                     AND trade_date >= $2 AND trade_date <= $3
                 ),
                 confirmed_absent AS (
                   SELECT e.symbol, e.trade_date
                   FROM expected e
                   JOIN data_sync_attempt attempt
                     ON attempt.source = 'fund_daily'
                    AND attempt.symbol = e.symbol
                    AND attempt.status = 'completed'
                    AND attempt.row_count = 0
                    AND attempt.start_date = e.trade_date
                    AND attempt.end_date = e.trade_date
                 )
                 SELECT COUNT(*)::int8,
                        COUNT(a.trade_date)::int8,
                        COUNT(DISTINCT e.symbol) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NULL
                        )::int8,
                        COUNT(*) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NOT NULL
                        )::int8
                 FROM expected e
                 LEFT JOIN actual a ON a.symbol = e.symbol AND a.trade_date = e.trade_date
                 LEFT JOIN confirmed_absent ca ON ca.symbol = e.symbol AND ca.trade_date = e.trade_date",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0));
            let etf_missing = (etf_expected - etf_actual).max(0);
            let etf_level = if etf_bad == 0 {
                "green"
            } else if etf_missing <= 5 {
                "yellow"
            } else {
                "red"
            };
            let (etf_fix_endpoint, etf_fix_params, etf_fix_reason) = if etf_level == "red" {
                (
                    Some("/api/v1/quant/data/sync/fund-daily"),
                    Some(json!({
                        "symbols": cfg.etf_symbols,
                        "start_date": yyyymmdd(start),
                        "end_date": yyyymmdd(end),
                        "data_version_id": format!("health-repair-etf-daily-{}", yyyymmdd(end))
                    })),
                    None,
                )
            } else if etf_level == "yellow" {
                (
                    None,
                    None,
                    Some(
                        "少量 ETF/QDII 日线缺口通常来自基金非交易日或上游空值；已尝试同步仍为空时不应静默补假价格"
                            .to_string(),
                    ),
                )
            } else {
                (None, None, None)
            };
            out.push(check_item(
                acct,
                sid,
                "ETF日线区间覆盖(MVO)",
                etf_level,
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口{}个 symbol-day/{}只ETF，其中{}个 symbol-day 已确认源端无行情",
                    cfg.etf_symbols.len(),
                    etf_expected,
                    etf_actual,
                    etf_missing,
                    etf_bad,
                    etf_confirmed_absent
                ),
                etf_fix_endpoint,
                etf_fix_params,
                etf_fix_reason,
            ));

            let (etf_adj_expected, etf_adj_actual, etf_adj_bad): (i64, i64, i64) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
             firsts AS (
               SELECT symbol, MIN(trade_date) AS first_date
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
               GROUP BY symbol
             ),
             calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true
                 AND trade_date >= $2::date
                 AND trade_date <= $3::date
             ),
             expected AS (
               SELECT s.symbol, COUNT(DISTINCT c.trade_date)::int8 AS expected_days
               FROM symbols s
               LEFT JOIN firsts f ON f.symbol = s.symbol
               LEFT JOIN calendar c
                 ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
               GROUP BY s.symbol
             ),
             actual AS (
               SELECT symbol, COUNT(DISTINCT trade_date)::int8 AS actual_days
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
                 AND trade_date >= $2 AND trade_date <= $3
               GROUP BY symbol
             )
             SELECT COALESCE(SUM(e.expected_days), 0)::int8,
                    COALESCE(SUM(COALESCE(a.actual_days, 0)), 0)::int8,
                    COUNT(*) FILTER (WHERE COALESCE(a.actual_days, 0) < e.expected_days)::int8
             FROM expected e
             LEFT JOIN actual a ON a.symbol = e.symbol",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0));
            out.push(check_item(
                acct,
                sid,
                "ETF复权因子区间覆盖(MVO)",
                if etf_adj_bad == 0 { "green" } else { "red" },
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口ETF数{}",
                    cfg.etf_symbols.len(),
                    etf_adj_expected,
                    etf_adj_actual,
                    etf_adj_bad
                ),
                Some("/api/v1/quant/data/sync/fund-adj"),
                Some(json!({
                    "symbols": cfg.etf_symbols,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-etf-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let stock_basic_days: i64 = sqlx::query_scalar(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             )
             SELECT COUNT(*)::int8 FROM expected WHERE expected_symbols > 0",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
            acct,
            sid,
            "股票基础信息区间覆盖",
            coverage_level(expected_days, stock_basic_days),
            format!(
                "market_stock 按 list_date/delist_date 可覆盖{}个交易日，期望{}天",
                stock_basic_days, expected_days
            ),
            Some("/api/v1/quant/data/sync/stock-basic"),
            Some(
                json!({"data_version_id": format!("health-repair-stock-basic-{}", yyyymmdd(end))}),
            ),
            None,
        ));

            let csi_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "CSI300区间覆盖",
                coverage_level(expected_days, csi_days),
                format!("期望{}个交易日，覆盖{}天", expected_days, csi_days),
                Some("/api/v1/quant/data/sync/index-daily"),
                Some(json!({
                    "index_codes": ["000300.SH"],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-csi300-{}", yyyymmdd(end))
                })),
                None,
            ));
        }

        if include_strategy {
            let (ic_quarters, ic_covered, ic_missing_min, ic_missing_max) =
                rolling_pit_ic_quarter_coverage(db, 20, start, end).await;
            let ic_level = coverage_level(ic_quarters, ic_covered);
            out.push(check_item(
                acct,
                sid,
                "滚动IC区间覆盖(PIT权重)",
                ic_level,
                format!(
                    "{}~{} 期望{}个季度as-of，PIT IC可用{}个；缺失as-of范围{}~{}",
                    start,
                    end,
                    ic_quarters,
                    ic_covered,
                    ic_missing_min
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string()),
                    ic_missing_max
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string())
                ),
                Some("/api/v1/quant/factors/evaluate-rolling-pit/background"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "version": "1.0.0",
                    "horizon": 20,
                    "train_lookback_days": 756
                })),
                None,
            ));

            let combo_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM multi_factor_value
             WHERE combo_name=$1 AND version='1.0.0'
               AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            let combo_future_leak: bool = sqlx::query_scalar(
                "SELECT EXISTS(
               SELECT 1 FROM multi_factor_value
               WHERE combo_name=$1 AND version='1.0.0'
                 AND trade_date >= $2 AND trade_date <= $3
                 AND available_at > trade_date
               LIMIT 1
             )",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(false);
            let combo_level = if combo_future_leak {
                "red"
            } else {
                coverage_level(expected_days, combo_days)
            };
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo区间覆盖({})", cfg.combo_name),
                combo_level,
                format!(
                    "期望{}个交易日，PIT覆盖{}天，future available_at={}",
                    expected_days, combo_days, combo_future_leak
                ),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({
                    "combo_name": cfg.combo_name,
                    "version": "1.0.0",
                    "horizon": 20,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            let curve_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM backtest_equity_curve
             WHERE task_id=$1 AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.equity_curve_task_id)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线区间覆盖",
                coverage_level(expected_days, curve_days),
                format!(
                    "期望{}个交易日，覆盖{}天，task={}",
                    expected_days, curve_days, cfg.equity_curve_task_id
                ),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({
                    "name": "权益曲线",
                    "strategy_id": sid,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            if strategy_needs_prediction(cfg) && cfg.prediction_set_id.is_none() {
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    "red",
                    format!(
                        "strategy signal_source={} 需要 ML，但未固定 prediction_set_id；历史回放不可复现",
                        cfg.signal_source
                    ),
                    None,
                    None,
                    Some(
                        "历史回放必须绑定 PIT prediction_set_id；不能用运行时最新预测集替代"
                            .to_string(),
                    ),
                ));
            } else if let Some(pred_set) = cfg
                .prediction_set_id
                .as_deref()
                .filter(|_| strategy_needs_prediction(cfg))
            {
                let ps_ready: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $3
                   AND training_end_date IS NOT NULL
                   AND training_end_date < $2
                 )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                let ml_days: i64 = sqlx::query_scalar(
                    "SELECT COUNT(DISTINCT trade_date)::int8 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND COALESCE(available_at, trade_date) <= trade_date",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(0);
                let ml_future_leak: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND available_at > trade_date
                 LIMIT 1
                )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    if !ps_ready || ml_future_leak {
                        "red"
                    } else {
                        coverage_level(expected_days, ml_days)
                    },
                    format!(
                        "期望{}个交易日，覆盖{}天，prediction_set_ready={}，future available_at={}，set={}",
                        expected_days, ml_days, ps_ready, ml_future_leak, pred_set
                    ),
                    None,
                    None,
                    Some(
                        "预测集由训练/预测流水线生成，不能用单点补数安全修复；需重建 PIT 预测集"
                            .to_string(),
                    ),
                ));
            }
        }

        if include_common {
            let suspension_completed =
                event_completed_day_count(db, "suspension_daily", start, end).await;
            let suspension_verified =
                event_verified_day_count(db, "suspension_daily", start, end).await;
            out.push(check_item(
                acct,
                sid,
                "停牌同步完成标记",
                coverage_level(expected_days, suspension_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；事件表空行和 derived 标记不能证明零停牌",
                    expected_days, suspension_verified, suspension_completed
                ),
                Some("/api/v1/quant/data/sync/suspension/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let limit_earliest = proven_limit_list_earliest_date();
            let limit_completed = event_completed_day_count(db, "limit_daily", start, end).await;
            let limit_verified = event_verified_day_count(db, "limit_daily", start, end).await;
            let limit_missing_source_note = if start < limit_earliest {
                format!(
                    "；{} 前 Tushare 不提供 limit_list_d，系统需用日线 close/pre_close 按交易规则派生",
                    limit_earliest
                )
            } else {
                String::new()
            };
            out.push(check_item(
                acct,
                sid,
                "涨跌停同步完成标记",
                coverage_level(expected_days, limit_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；2019-11-28后必须有 Tushare 来源，事件表空行不能证明零涨跌停{}",
                    expected_days, limit_verified, limit_completed, limit_missing_source_note
                ),
                Some("/api/v1/quant/data/sync/limit/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
            out.push(check_item(
                acct,
                sid,
                "数据同步调度任务",
                if scheduler_issues.is_empty() {
                    "green"
                } else {
                    "red"
                },
                if scheduler_issues.is_empty() {
                    "启用调度任务 CRON/依赖顺序检查通过".to_string()
                } else {
                    format!(
                        "调度任务问题{}项: {}",
                        scheduler_issues.len(),
                        scheduler_issues.join("；")
                    )
                },
                None,
                None,
                if scheduler_issues.is_empty() {
                    None
                } else {
                    Some(
                        "请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效"
                            .to_string(),
                    )
                },
            ));

            let st_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::int8 FROM market_stock_name_history
             WHERE start_date <= $1 AND (end_date IS NULL OR end_date >= $2)",
            )
            .bind(end)
            .bind(start)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "ST/名称历史",
                if st_count > 0 { "green" } else { "red" },
                format!("区间相交名称历史/ST记录{}条", st_count),
                Some("/api/v1/quant/data/sync/namechange"),
                Some(json!({})),
                None,
            ));
        }

        return out;
    }

    let active_stock_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::int8
         FROM market_stock s
         LEFT JOIN market_stock_suspension susp
           ON susp.symbol = s.symbol
          AND susp.trade_date = $1
          AND COALESCE(susp.suspend_type, 'S') = 'S'
         WHERE s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
           AND s.list_date IS NOT NULL
           AND s.list_date <= $1
           AND (s.delist_date IS NULL OR s.delist_date >= $1)
           AND susp.symbol IS NULL",
    )
    .bind(last_mkt)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    let (a_last, a_symbols): (Option<NaiveDate>, i64) = sqlx::query_as(
        "WITH latest AS (
           SELECT MAX(trade_date) AS trade_date FROM market_stock_daily_bar_adj
           WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         )
         SELECT latest.trade_date,
                COUNT(DISTINCT b.symbol)::int8
         FROM latest
         LEFT JOIN market_stock_daily_bar_adj b
           ON b.trade_date = latest.trade_date
          AND b.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         GROUP BY latest.trade_date",
    )
    .fetch_one(db)
    .await
    .unwrap_or((None, 0));
    let a_lag = a_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let a_partial = active_stock_count > 0 && a_symbols * 100 < active_stock_count * 80;
    out.push(check_item(
        acct,
        sid,
        "A股日线",
        if a_partial {
            "red"
        } else {
            lag_level(a_lag, 2, 7)
        },
        format!(
            "最新{}，落后{}天，最新日{}只/按上市退市停牌口径应有{}只",
            a_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            a_lag,
            a_symbols,
            active_stock_count
        ),
        Some("/api/v1/quant/data/sync/daily/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-daily-{}", repair_end)
        })),
        None,
    ));

    let (etf_present, etf_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_stock_daily_bar_adj
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_lag = etf_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF日线(MVO)",
        if etf_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_lag, 2, 7)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_lag
        ),
        Some("/api/v1/quant/data/sync/fund-daily"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-daily-{}", repair_end)
        })),
        None,
    ));

    let adj_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_adjustment_factor
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let adj_lag = adj_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "A股复权因子",
        lag_level(adj_lag, 30, 90),
        format!(
            "最新{}，落后{}天",
            adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            adj_lag
        ),
        Some("/api/v1/quant/data/sync/adj-factor/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-adj-{}", repair_end)
        })),
        None,
    ));

    let (etf_adj_present, etf_adj_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_adjustment_factor
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_adj_lag = etf_adj_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF复权因子(MVO)",
        if etf_adj_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_adj_lag, 30, 90)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_adj_lag
        ),
        Some("/api/v1/quant/data/sync/fund-adj"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-adj-{}", repair_end)
        })),
        None,
    ));

    let csi_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let csi_lag = csi_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "CSI300",
        lag_level(csi_lag, 2, 7),
        format!(
            "最新{}，落后{}天",
            csi_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            csi_lag
        ),
        Some("/api/v1/quant/data/sync/index-daily"),
        Some(json!({
            "index_codes": ["000300.SH"],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-csi300-{}", repair_end)
        })),
        None,
    ));

    let stock_basic_level = if active_stock_count >= 3000 {
        "green"
    } else {
        "red"
    };
    out.push(check_item(
        acct,
        sid,
        "股票基础信息",
        stock_basic_level,
        format!("当前上市A股{}只", active_stock_count),
        Some("/api/v1/quant/data/sync/stock-basic"),
        Some(json!({"data_version_id": format!("health-repair-stock-basic-{}", repair_end)})),
        None,
    ));

    let st_count: i64 = sqlx::query_scalar("SELECT COUNT(*)::int8 FROM market_stock_name_history")
        .fetch_one(db)
        .await
        .unwrap_or(0);
    out.push(check_item(
        acct,
        sid,
        "ST/名称历史",
        if st_count > 0 { "green" } else { "red" },
        format!("名称历史/ST记录{}条", st_count),
        Some("/api/v1/quant/data/sync/namechange"),
        Some(json!({})),
        None,
    ));

    let (suspension_last, suspension_source, suspension_rows) =
        event_completion_latest_source(db, "market_stock_suspension", "suspension_daily").await;
    let suspension_lag = suspension_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    let suspension_source_level = suspension_last
        .map(|d| event_sync_source_level("suspension_daily", suspension_source.as_deref(), d))
        .unwrap_or("red");
    let suspension_level = if suspension_source_level == "green" {
        lag_level(suspension_lag, 2, 7)
    } else {
        suspension_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "停牌",
        suspension_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}",
            suspension_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            suspension_lag,
            suspension_source.as_deref().unwrap_or("无"),
            suspension_last
                .map(|d| event_sync_source_label(
                    "suspension_daily",
                    suspension_source.as_deref(),
                    d
                ))
                .unwrap_or("missing"),
            suspension_rows
        ),
        Some("/api/v1/quant/data/sync/suspension"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let (limit_last, limit_source, limit_rows) =
        event_completion_latest_source(db, "market_stock_limit", "limit_daily").await;
    let limit_lag = limit_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let limit_source_level = limit_last
        .map(|d| event_sync_source_level("limit_daily", limit_source.as_deref(), d))
        .unwrap_or("red");
    let limit_level = if limit_source_level == "green" {
        lag_level(limit_lag, 2, 7)
    } else {
        limit_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "涨跌停",
        limit_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}；2019-11-28前历史需由日线按交易规则派生",
            limit_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            limit_lag,
            limit_source.as_deref().unwrap_or("无"),
            limit_last
                .map(|d| event_sync_source_label("limit_daily", limit_source.as_deref(), d))
                .unwrap_or("missing"),
            limit_rows
        ),
        Some("/api/v1/quant/data/sync/limit"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
    out.push(check_item(
        acct,
        sid,
        "数据同步调度任务",
        if scheduler_issues.is_empty() {
            "green"
        } else {
            "red"
        },
        if scheduler_issues.is_empty() {
            "启用调度任务 CRON/依赖顺序检查通过".to_string()
        } else {
            format!(
                "调度任务问题{}项: {}",
                scheduler_issues.len(),
                scheduler_issues.join("；")
            )
        },
        None,
        None,
        if scheduler_issues.is_empty() {
            None
        } else {
            Some("请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效".to_string())
        },
    ));

    // PIT combo 物化新鲜度
    let combo_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name=$1 AND version='1.0.0'
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(&cfg.combo_name)
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    match combo_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo物化({})", cfg.combo_name),
                lag_level(lag, 2, 7),
                format!("最新 {} (落后行情 {} 天)", d, lag),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            format!("PIT combo物化({})", cfg.combo_name),
            "red",
            "combo 无任何 PIT 合规物化数据",
            Some("/api/v1/quant/factors/materialize-pit-combo/background"),
            Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 权益曲线新鲜度
    let curve_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM backtest_equity_curve WHERE task_id=$1")
            .bind(&cfg.equity_curve_task_id)
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    match curve_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线",
                lag_level(lag, 4, 10),
                format!("最新 {} (落后 {} 天, task={})", d, lag, cfg.equity_curve_task_id),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            "A股权益曲线",
            "red",
            format!("曲线 {} 无数据", cfg.equity_curve_task_id),
            Some("/api/v1/admin/sync/repair"),
            Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 滚动 IC 新鲜度
    let ic_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(end_date) FROM factor_evaluation WHERE horizon=20")
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    if let Some(d) = ic_last {
        let lag = (last_mkt - d).num_days();
        out.push(check_item(
            acct,
            sid,
            "滚动IC窗口",
            if lag > 100 { "yellow" } else { "green" },
            format!("最新IC窗口 {} (距今 {} 天)", d, lag),
            Some("/api/v1/quant/factors/evaluate-all/background"),
            Some(json!({})),
            None,
        ));
    }

    // ML 预测集新鲜度(策略使用 prediction / prediction_blend 时为必需项)
    if strategy_needs_prediction(cfg) {
        let resolved_prediction_set = resolve_live_prediction_set(db, cfg, last_mkt).await;
        let Some(ps) = resolved_prediction_set.as_deref() else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!(
                    "strategy signal_source={} 需要 ML，但没有 PIT 合规预测集覆盖 {}",
                    cfg.signal_source, last_mkt
                ),
                None,
                None,
                Some("需要先运行 PIT 训练/预测流水线，不能降级成纯因子交易".to_string()),
            ));
            return out;
        };
        let ps_ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(
               SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $2
                 AND training_end_date IS NOT NULL
                 AND training_end_date < $2
             )",
        )
        .bind(ps)
        .bind(last_mkt)
        .fetch_one(db)
        .await
        .unwrap_or(false);
        if !ps_ready {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 未 ready 或未 PIT 覆盖 {}", ps, last_mkt),
                None,
                None,
                Some("需要重建或切换到覆盖当前交易日的 PIT 预测集".to_string()),
            ));
            return out;
        }
        let ml_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(trade_date) FROM model_prediction
             WHERE prediction_set_id=$1 AND COALESCE(available_at, trade_date) <= trade_date",
        )
        .bind(ps)
        .fetch_one(db)
        .await
        .ok()
        .flatten();
        if let Some(d) = ml_last {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                lag_level(lag, 2, 7),
                format!("最新 {} (落后 {} 天, set={})", d, lag, ps),
                None,
                None,
                Some(
                    "预测集由 PIT 训练/预测流水线生成；单点页面修复不能保证模型正确性".to_string(),
                ),
            ));
        } else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 无 PIT 合规预测行", ps),
                None,
                None,
                Some("需要重建 PIT 预测集".to_string()),
            ));
        }
    }
    out
}
