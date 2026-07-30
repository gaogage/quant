//! Auto-extracted from routes/optimization.rs (DDD Step 3b).
//! Do not edit by hand; logic is verbatim from the original module.
#![allow(unused_imports)]
#![allow(dead_code)]

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Duration, NaiveDate};
use quant_api::discovery::strategy_discovery::{
    build_layered_search_plan, CandidateMetrics, CandidateTargets, CandidateType,
    LayeredSearchConfig, LayeredSearchPlan, LocalResourcePlan,
};
// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::error;
use uuid::Uuid;

use crate::phase7_alpha_admission::{
    validate_analyst_revision_entrypoint_admission, validate_analyst_revision_trial_admission,
    validate_equity_pledge_entrypoint_admission, validate_equity_pledge_trial_admission,
    validate_futures_price_chain_entrypoint_admission,
    validate_industry_prosperity_entrypoint_admission,
    validate_industry_prosperity_trial_admission, validate_margin_detail_entrypoint_admission,
    validate_margin_detail_trial_admission, validate_shareholder_structure_entrypoint_admission,
    validate_shareholder_structure_trial_admission, FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID,
    INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE, MARGIN_DETAIL_COVERAGE_GATE_ID,
};
use crate::routes::backtest::{
    execute_factor_backtest_with_caches, execute_prediction_backtest,
    prewarm_factor_signal_cache_for_requests, CostModelReq, EffectiveCoverageReq,
    ExecutionRulesReq, FactorBacktestRunOutput, MarketRegimeBacktestReq, RunFactorBacktestReq,
    RunPredictionBacktestReq,
};
use crate::routes::ml::{
    build_prediction_set_readiness_report, create_walk_forward_nonlinear_quantile_ranker_inner,
    daily_count_distribution, prediction_readiness_passed, readiness_expected_open_day_count,
    train_nonlinear_quantile_ranker_inner, DailyCountDistribution, LinearFactorRef,
    ReadinessThresholds, TrainNonlinearQuantileRankerRequest,
    WalkForwardNonlinearQuantileRankerRequest,
};
use crate::AppState;
use quant_backtest::runner::{
    BacktestDataCache, BacktestDataCacheSnapshot, BacktestDataCacheStats,
};
use quant_backtest::signal_generator::{
    compare_return_risk_cache_economics, signal_cache_stats_delta, SignalDataCache,
    SignalDataCacheSnapshot, SignalDataCacheStats,
};


use super::*;

#[derive(Debug, Clone)]
pub(crate) struct SleeveAdmissionTrialDiagnosticRow {
    pub(crate) trial_id: String,
    pub(crate) trial_index: i32,
    pub(crate) status: String,
    pub(crate) backtest_task_id: Option<String>,
    pub(crate) score: Option<Decimal>,
    pub(crate) parameters: Value,
    pub(crate) backtest_parameters: Option<Value>,
    pub(crate) metrics: Option<Value>,
    pub(crate) constraint_violations: Option<Value>,
    pub(crate) portfolio_constraint_summary: Option<Value>,
    pub(crate) robustness_status: Option<String>,
    pub(crate) gate_results: Option<Value>,
}


pub(crate) struct SleeveAdmissionTrialDiagnostic {
    family: String,
    robustness_status: String,
    score: Option<Decimal>,
    annual_return: Option<f64>,
    sharpe: Option<f64>,
    calmar: Option<f64>,
    fill_ratio: Option<f64>,
    unfilled_gap: Option<f64>,
    excess_return: Option<f64>,
    stress_pass_ratio: Option<f64>,
    failed_gate_names: Vec<String>,
    json: Value,
}


#[derive(Default)]
struct SleeveAdmissionFamilyAccumulator {
    trial_count: usize,
    rejected_count: usize,
    approved_count: usize,
    annual_return_sum: f64,
    annual_return_count: usize,
    sharpe_sum: f64,
    sharpe_count: usize,
    calmar_sum: f64,
    calmar_count: usize,
    fill_ratio_sum: f64,
    fill_ratio_count: usize,
    unfilled_gap_sum: f64,
    unfilled_gap_count: usize,
    stress_pass_ratio_sum: f64,
    stress_pass_ratio_count: usize,
    failed_gates: BTreeMap<String, usize>,
}


#[derive(Default)]
struct SleeveAdmissionActionAccumulator {
    total_trial_count: usize,
    positive_trial_count: usize,
    positive_stress_evaluated_count: usize,
    positive_stress_passed_count: usize,
    positive_fill_below_90_count: usize,
    positive_unfilled_above_08_count: usize,
    positive_negative_excess_count: usize,
    weak_or_negative_alpha_count: usize,
    execution_capacity_families: BTreeSet<String>,
    weak_alpha_families: BTreeSet<String>,
    stress_failed_families: BTreeSet<String>,
    underbenchmark_families: BTreeSet<String>,
}


pub async fn report_feature_profile_readiness(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FeatureProfileReadinessRequest>,
) -> impl IntoResponse {
    match build_feature_profile_readiness_report_from_request(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn report_alpha_source_diagnostics(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AlphaSourceDiagnosticsRequest>,
) -> impl IntoResponse {
    match build_alpha_source_diagnostics_report_from_request(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn report_main_business_diagnostics(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MainBusinessDiagnosticsRequest>,
) -> impl IntoResponse {
    match build_main_business_diagnostics_report_from_request(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn get_sleeve_admission_diagnostics(
    State(state): State<Arc<AppState>>,
    Path(experiment_run_id): Path<String>,
) -> impl IntoResponse {
    match build_sleeve_admission_diagnostics(&state.db, &experiment_run_id).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn report_return_risk_cache_economics(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ReturnRiskCacheEconomicsReportRequest>,
) -> impl IntoResponse {
    match build_return_risk_cache_economics_report(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub(crate) async fn build_sleeve_admission_diagnostics(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
) -> Result<Value, String> {
    let row = sqlx::query_as::<_, (String, Option<Value>)>(
        "SELECT status, metrics
         FROM experiment_run
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .fetch_optional(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load experiment_run {}: {}",
            experiment_run_id, error
        )
    })?
    .ok_or_else(|| format!("experiment_run not found: {}", experiment_run_id))?;

    let metrics = row
        .1
        .ok_or_else(|| format!("experiment_run {} has no metrics", experiment_run_id))?;
    let task_ids = sleeve_admission_train_task_ids(&metrics);
    let mut rows_by_task = BTreeMap::new();
    for task_id in task_ids {
        let rows = load_sleeve_admission_trial_diagnostic_rows(db, &task_id).await?;
        rows_by_task.insert(task_id, rows);
    }

    Ok(sleeve_admission_diagnostic_matrix_json(
        experiment_run_id,
        &row.0,
        &metrics,
        &rows_by_task,
    ))
}


pub(crate) async fn load_sleeve_admission_trial_diagnostic_rows(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<Vec<SleeveAdmissionTrialDiagnosticRow>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            String,
            Option<String>,
            Option<Decimal>,
            Value,
            Option<Value>,
            Option<Value>,
            Option<Value>,
            Option<Value>,
            Option<String>,
            Option<Value>,
        ),
    >(
        "SELECT trial.trial_id,
                trial.trial_index,
                trial.status,
                trial.backtest_task_id,
                trial.score,
                trial.parameters,
                backtest.parameters,
                trial.metrics,
                trial.constraint_violations,
                violation_summary.summary,
                gate.status,
                gate.gate_results
         FROM optimization_trial trial
         LEFT JOIN backtest_task backtest ON backtest.task_id = trial.backtest_task_id
         LEFT JOIN LATERAL (
             SELECT jsonb_build_object(
                 'total_count', COALESCE(SUM(grouped.violation_count), 0),
                 'hard_count', COALESCE(SUM(grouped.violation_count) FILTER (WHERE grouped.severity = 'hard'), 0),
                 'by_constraint', COALESCE(
                     jsonb_agg(
                         jsonb_build_object(
                             'constraint_name', grouped.constraint_name,
                             'severity', grouped.severity,
                             'count', grouped.violation_count,
                             'max_limit_value', grouped.max_limit_value,
                             'max_actual_value', grouped.max_actual_value,
                             'avg_actual_value', grouped.avg_actual_value
                         )
                         ORDER BY grouped.violation_count DESC, grouped.constraint_name ASC
                     ),
                     '[]'::jsonb
                 )
             ) AS summary
             FROM (
                 SELECT constraint_name,
                        severity,
                        COUNT(*) AS violation_count,
                        MAX(limit_value) AS max_limit_value,
                        MAX(actual_value) AS max_actual_value,
                        AVG(actual_value) AS avg_actual_value
                 FROM portfolio_constraint_violation
                 WHERE task_id = trial.backtest_task_id
                 GROUP BY constraint_name, severity
             ) grouped
         ) violation_summary ON TRUE
         LEFT JOIN LATERAL (
             SELECT status, gate_results
             FROM robustness_gate_result
             WHERE optimization_task_id = trial.optimization_task_id
               AND trial_id = trial.trial_id
             ORDER BY created_at DESC
             LIMIT 1
         ) gate ON TRUE
         WHERE trial.optimization_task_id = $1
         ORDER BY trial.score DESC NULLS LAST, trial.trial_index ASC",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load sleeve admission trials for task {}: {}",
            task_id, error
        )
    })?;

    Ok(rows
        .into_iter()
        .map(
            |(
                trial_id,
                trial_index,
                status,
                backtest_task_id,
                score,
                parameters,
                backtest_parameters,
                metrics,
                constraint_violations,
                portfolio_constraint_summary,
                robustness_status,
                gate_results,
            )| SleeveAdmissionTrialDiagnosticRow {
                trial_id,
                trial_index,
                status,
                backtest_task_id,
                score,
                parameters,
                backtest_parameters,
                metrics,
                constraint_violations,
                portfolio_constraint_summary,
                robustness_status,
                gate_results,
            },
        )
        .collect())
}


pub(crate) fn sleeve_admission_train_task_ids(metrics: &Value) -> BTreeSet<String> {
    metrics
        .get("windows")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|window| {
            window
                .get("train_optimization_task_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}


pub(crate) fn sleeve_admission_diagnostic_matrix_json(
    experiment_run_id: &str,
    experiment_status: &str,
    metrics: &Value,
    rows_by_task: &BTreeMap<String, Vec<SleeveAdmissionTrialDiagnosticRow>>,
) -> Value {
    let windows = metrics
        .get("windows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut family_summary: BTreeMap<String, SleeveAdmissionFamilyAccumulator> = BTreeMap::new();
    let mut action_summary = SleeveAdmissionActionAccumulator::default();
    let mut window_reports = Vec::new();

    for window in windows {
        let task_id = window
            .get("train_optimization_task_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut rows = rows_by_task.get(&task_id).cloned().unwrap_or_default();
        rows.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.trial_index.cmp(&right.trial_index))
        });
        let diagnostics = rows
            .iter()
            .map(sleeve_admission_trial_diagnostic)
            .collect::<Vec<_>>();
        for diagnostic in &diagnostics {
            family_summary
                .entry(diagnostic.family.clone())
                .or_default()
                .record(diagnostic);
            action_summary.record(diagnostic);
        }

        let best_rejected_trial = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.robustness_status == "rejected")
            .map(sleeve_admission_best_trial_json)
            .unwrap_or(Value::Null);
        let best_trial = diagnostics
            .first()
            .map(sleeve_admission_best_trial_json)
            .unwrap_or(Value::Null);

        window_reports.push(json!({
            "window": window.get("window").cloned().unwrap_or(Value::Null),
            "status": window.get("status").cloned().unwrap_or(Value::Null),
            "skip_reason": window.get("skip_reason").cloned().unwrap_or(Value::Null),
            "train_optimization_task_id": task_id,
            "trial_count": diagnostics.len(),
            "family_count": diagnostics
                .iter()
                .map(|diagnostic| diagnostic.family.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            "best_trial": best_trial,
            "best_rejected_trial": best_rejected_trial,
            "families": diagnostics
                .iter()
                .map(|diagnostic| diagnostic.json.clone())
                .collect::<Vec<_>>(),
        }));
    }

    let family_summary = family_summary
        .into_iter()
        .map(|(family, accumulator)| accumulator.json(&family))
        .collect::<Vec<_>>();

    json!({
        "experiment_run_id": experiment_run_id,
        "experiment_status": experiment_status,
        "window_count": window_reports.len(),
        "windows": window_reports,
        "family_summary": family_summary,
        "action_summary": action_summary.json(),
        "diagnostic_scope": {
            "source": "experiment_run.metrics.windows -> optimization_trial -> latest robustness_gate_result",
            "point_in_time_contract": "diagnostic only; does not rerun selection, does not relax train/OOS gates",
        },
    })
}


pub(crate) fn sleeve_admission_trial_diagnostic(
    row: &SleeveAdmissionTrialDiagnosticRow,
) -> SleeveAdmissionTrialDiagnostic {
    let metrics = row.metrics.as_ref();
    let gate_results = row.gate_results.as_ref();
    let failed_gates = sleeve_failed_gate_reports(gate_results, row.constraint_violations.as_ref());
    let failed_gate_names = failed_gates
        .iter()
        .filter_map(|gate| gate.get("gate").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    let family = sleeve_admission_family(&row.parameters);
    let robustness_status = row
        .robustness_status
        .clone()
        .unwrap_or_else(|| sleeve_unevaluated_status(&row.status).to_string());
    let stress_pass_ratio = sleeve_stress_pass_ratio(gate_results);
    let portfolio_constraint_summary = row
        .portfolio_constraint_summary
        .clone()
        .unwrap_or_else(sleeve_empty_portfolio_constraint_summary);
    let portfolio_expression = sleeve_portfolio_expression_json(metrics);
    let capacity_headroom =
        sleeve_capacity_headroom_json(metrics, gate_results, &portfolio_constraint_summary);
    let execution_capacity = sleeve_execution_capacity_json(
        &row.parameters,
        row.backtest_parameters.as_ref(),
        &portfolio_constraint_summary,
        metrics,
        gate_results,
    );
    let diagnosis =
        sleeve_trial_capacity_diagnosis_json(metrics, gate_results, &portfolio_constraint_summary);

    let diagnostic_json = json!({
        "family": family,
        "trial_id": row.trial_id,
        "trial_index": row.trial_index,
        "trial_status": row.status,
        "backtest_task_id": row.backtest_task_id,
        "score": row.score,
        "robustness_status": robustness_status,
        "parameters": {
            "alpha_sleeve_family": row.parameters.get("alpha_sleeve_family").cloned().unwrap_or(Value::Null),
            "alpha_source_family": row.parameters.get("alpha_source_family").cloned().unwrap_or(Value::Null),
            "combo_name": row.parameters.get("combo_name").cloned().unwrap_or(Value::Null),
            "score_direction": row.parameters.get("score_direction").cloned().unwrap_or(Value::Null),
            "market_regime": row.parameters.get("market_regime").cloned().unwrap_or(Value::Null),
            "candidate_ranking": row.parameters.get("candidate_ranking").cloned().unwrap_or(Value::Null),
            "capacity_risk_budget": row.parameters.get("capacity_risk_budget").cloned().unwrap_or(Value::Null),
        },
        "portfolio_expression": portfolio_expression,
        "metrics": {
            "annual_return_pct": metric_value(metrics, "annual_return_pct"),
            "excess_return_pct": metric_value(metrics, "excess_return_pct"),
            "sharpe_ratio": metric_value(metrics, "sharpe_ratio"),
            "sortino_ratio": metric_value(metrics, "sortino_ratio"),
            "calmar_ratio": metric_value(metrics, "calmar_ratio"),
            "max_drawdown_pct": metric_value(metrics, "max_drawdown_pct"),
            "profit_factor": metric_value(metrics, "profit_factor"),
            "num_trades": metric_value(metrics, "num_trades"),
        },
        "execution_quality": {
            "fill_ratio": metric_value(metrics, "final_execution_fill_ratio"),
            "unfilled_target_gap_pct": metric_value(metrics, "final_unfilled_target_gap_pct"),
            "cash_weight_pct": metric_value(metrics, "final_cash_weight_pct"),
            "actual_gross_exposure_pct": metric_value(metrics, "final_actual_gross_exposure_pct"),
            "execution_schedule_expired_count": metric_value(metrics, "execution_schedule_expired_count"),
            "max_execution_target_gap_pct": metric_value(metrics, "max_execution_target_gap_pct"),
        },
        "capacity_headroom": capacity_headroom,
        "execution_capacity": execution_capacity,
        "stress": {
            "pass_ratio": stress_pass_ratio,
            "passed_count": sleeve_gate_field(gate_results, "train_cost_capacity_perturbation_pass_ratio", "passed_count"),
            "total_count": sleeve_gate_field(gate_results, "train_cost_capacity_perturbation_pass_ratio", "total_count"),
            "avg_perturbed_calmar": sleeve_gate_actual(gate_results, "train_avg_perturbed_calmar"),
            "min_perturbed_annual_return": sleeve_gate_actual(gate_results, "train_perturbed_annual_return"),
            "max_perturbed_drawdown_pct": sleeve_gate_field(gate_results, "train_cost_capacity_perturbation_pass_ratio", "max_perturbed_oos_drawdown_pct"),
        },
        "diagnosis": diagnosis,
        "failed_gates": failed_gates,
        "weak_regimes": sleeve_weak_regime_windows(gate_results, 3),
        "constraint_violations": row.constraint_violations.clone().unwrap_or_else(|| json!([])),
        "skip_reason": sleeve_trial_skip_reason(&robustness_status, &failed_gate_names),
    });

    SleeveAdmissionTrialDiagnostic {
        family,
        robustness_status,
        score: row.score,
        annual_return: metric_f64_value(metrics, "annual_return_pct"),
        sharpe: metric_f64_value(metrics, "sharpe_ratio"),
        calmar: metric_f64_value(metrics, "calmar_ratio"),
        fill_ratio: metric_f64_value(metrics, "final_execution_fill_ratio"),
        unfilled_gap: metric_f64_value(metrics, "final_unfilled_target_gap_pct"),
        excess_return: metric_f64_value(metrics, "excess_return_pct"),
        stress_pass_ratio,
        failed_gate_names,
        json: diagnostic_json,
    }
}


pub(crate) fn sleeve_admission_family(parameters: &Value) -> String {
    [
        "alpha_sleeve_family",
        "alpha_source_family",
        "multi_alpha_sleeve_profile",
        "combo_name",
    ]
    .iter()
    .find_map(|key| parameters.get(*key).and_then(Value::as_str))
    .unwrap_or("unknown")
    .to_string()
}


pub(crate) fn sleeve_unevaluated_status(trial_status: &str) -> &str {
    if trial_status == "completed" {
        "not_evaluated"
    } else {
        "trial_not_completed"
    }
}


pub(crate) fn sleeve_failed_gate_reports(
    gate_results: Option<&Value>,
    constraint_violations: Option<&Value>,
) -> Vec<Value> {
    let mut gates = gate_results
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|gate| gate.get("passed").and_then(Value::as_bool) == Some(false))
        .map(|gate| {
            json!({
                "gate": gate.get("gate").cloned().unwrap_or(Value::Null),
                "limit": gate.get("limit").cloned().unwrap_or(Value::Null),
                "actual": gate.get("actual").cloned().unwrap_or(Value::Null),
                "passed": false,
            })
        })
        .collect::<Vec<_>>();

    if gates.is_empty() {
        gates = constraint_violations
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|violation| {
                json!({
                    "gate": violation.get("constraint").cloned().unwrap_or(Value::Null),
                    "limit": violation.get("limit").cloned().unwrap_or(Value::Null),
                    "actual": violation.get("actual").cloned().unwrap_or(Value::Null),
                    "passed": false,
                })
            })
            .collect();
    }

    gates
}


pub(crate) fn sleeve_trial_skip_reason(robustness_status: &str, failed_gate_names: &[String]) -> Value {
    if robustness_status == "approved_candidate" {
        return Value::Null;
    }
    if failed_gate_names.is_empty() {
        return json!(format!("robustness_status={}", robustness_status));
    }
    json!(format!(
        "robustness_status={} failed_gates={}",
        robustness_status,
        failed_gate_names.join(",")
    ))
}


pub(crate) fn sleeve_gate<'a>(gate_results: Option<&'a Value>, gate_name: &str) -> Option<&'a Value> {
    gate_results
        .and_then(Value::as_array)?
        .iter()
        .find(|gate| gate.get("gate").and_then(Value::as_str) == Some(gate_name))
}


pub(crate) fn sleeve_gate_actual(gate_results: Option<&Value>, gate_name: &str) -> Value {
    sleeve_gate(gate_results, gate_name)
        .and_then(|gate| gate.get("actual"))
        .cloned()
        .unwrap_or(Value::Null)
}


pub(crate) fn sleeve_gate_field(gate_results: Option<&Value>, gate_name: &str, field_name: &str) -> Value {
    sleeve_gate(gate_results, gate_name)
        .and_then(|gate| gate.get(field_name))
        .cloned()
        .unwrap_or(Value::Null)
}


pub(crate) fn sleeve_stress_pass_ratio(gate_results: Option<&Value>) -> Option<f64> {
    let gate = sleeve_gate(gate_results, "train_cost_capacity_perturbation_pass_ratio")?;
    if let Some(actual) = gate.get("actual").and_then(value_as_f64) {
        return Some(actual);
    }
    let passed_count = gate.get("passed_count").and_then(value_as_f64)?;
    let total_count = gate.get("total_count").and_then(value_as_f64)?;
    if total_count > 0.0 {
        Some(passed_count / total_count)
    } else {
        None
    }
}


pub(crate) fn sleeve_weak_regime_windows(gate_results: Option<&Value>, limit: usize) -> Vec<Value> {
    let windows = sleeve_gate(gate_results, "walk_forward_min_window_count")
        .and_then(|gate| gate.get("details"))
        .and_then(|details| details.get("windows"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    rank_weak_walk_forward_windows(&windows, limit)
}


pub(crate) fn metric_value(metrics: Option<&Value>, key: &str) -> Value {
    metrics
        .and_then(|metrics| metrics.get(key))
        .cloned()
        .unwrap_or(Value::Null)
}


pub(crate) fn sleeve_empty_portfolio_constraint_summary() -> Value {
    json!({
        "total_count": 0,
        "hard_count": 0,
        "by_constraint": [],
    })
}


pub(crate) fn sleeve_value_at_path(value: Option<&Value>, path: &[&str]) -> Value {
    let mut current = match value {
        Some(value) => value,
        None => return Value::Null,
    };
    for key in path {
        current = match current.get(*key) {
            Some(next) => next,
            None => return Value::Null,
        };
    }
    current.clone()
}


pub(crate) fn sleeve_parameter_value(
    trial_parameters: &Value,
    backtest_parameters: Option<&Value>,
    key: &str,
) -> Value {
    backtest_parameters
        .and_then(|parameters| parameters.get(key))
        .or_else(|| trial_parameters.get(key))
        .cloned()
        .unwrap_or(Value::Null)
}


pub(crate) fn sleeve_nested_parameter_value(
    trial_parameters: &Value,
    backtest_parameters: Option<&Value>,
    path: &[&str],
) -> Value {
    let backtest_value = sleeve_value_at_path(backtest_parameters, path);
    if !backtest_value.is_null() {
        return backtest_value;
    }
    sleeve_value_at_path(Some(trial_parameters), path)
}


pub(crate) fn sleeve_positive_shortfall_json(limit: f64, actual: Option<f64>) -> Value {
    actual.map_or(Value::Null, |actual| {
        json!(positive_shortfall(limit, actual))
    })
}


pub(crate) fn sleeve_positive_excess_json(actual: Option<f64>, limit: f64) -> Value {
    actual.map_or(Value::Null, |actual| json!((actual - limit).max(0.0)))
}


pub(crate) fn sleeve_portfolio_constraint_count(summary: &Value, key: &str) -> usize {
    summary.get(key).and_then(Value::as_u64).unwrap_or_default() as usize
}


pub(crate) fn sleeve_portfolio_expression_json(metrics: Option<&Value>) -> Value {
    let target_gross = metric_f64_value(metrics, "final_target_gross_exposure_pct");
    let actual_gross = metric_f64_value(metrics, "final_actual_gross_exposure_pct");
    json!({
        "target_gross_exposure_pct": metric_value(metrics, "final_target_gross_exposure_pct"),
        "actual_gross_exposure_pct": metric_value(metrics, "final_actual_gross_exposure_pct"),
        "cash_weight_pct": metric_value(metrics, "final_cash_weight_pct"),
        "fill_ratio": metric_value(metrics, "final_execution_fill_ratio"),
        "unfilled_target_gap_pct": metric_value(metrics, "final_unfilled_target_gap_pct"),
        "actual_gross_shortfall_to_target": match (target_gross, actual_gross) {
            (Some(target), Some(actual)) => json!((target - actual).max(0.0)),
            _ => Value::Null,
        },
        "source": "optimization_trial.metrics",
    })
}


pub(crate) fn sleeve_capacity_headroom_json(
    metrics: Option<&Value>,
    gate_results: Option<&Value>,
    portfolio_constraint_summary: &Value,
) -> Value {
    let fill_ratio = metric_f64_value(metrics, "final_execution_fill_ratio");
    let unfilled_gap = metric_f64_value(metrics, "final_unfilled_target_gap_pct");
    let cash_weight = metric_f64_value(metrics, "final_cash_weight_pct");
    let actual_gross = metric_f64_value(metrics, "final_actual_gross_exposure_pct");
    let target_gross = metric_f64_value(metrics, "final_target_gross_exposure_pct");
    json!({
        "fill_shortfall_to_90": sleeve_positive_shortfall_json(0.90, fill_ratio),
        "unfilled_gap_excess_over_08": sleeve_positive_excess_json(unfilled_gap, 0.08),
        "cash_excess_over_25": sleeve_positive_excess_json(cash_weight, 0.25),
        "actual_gross_shortfall_to_target": match (target_gross, actual_gross) {
            (Some(target), Some(actual)) => json!((target - actual).max(0.0)),
            _ => Value::Null,
        },
        "train_gate_actuals": {
            "final_execution_fill_ratio": sleeve_gate_actual(gate_results, "train_final_execution_fill_ratio"),
            "final_unfilled_target_gap": sleeve_gate_actual(gate_results, "train_final_unfilled_target_gap"),
            "final_cash_weight": sleeve_gate_actual(gate_results, "train_final_cash_weight"),
            "final_actual_gross_exposure": sleeve_gate_actual(gate_results, "train_final_actual_gross_exposure"),
            "avg_perturbed_calmar": sleeve_gate_actual(gate_results, "train_avg_perturbed_calmar"),
            "perturbed_annual_return": sleeve_gate_actual(gate_results, "train_perturbed_annual_return"),
        },
        "portfolio_constraint_violation_count": sleeve_portfolio_constraint_count(portfolio_constraint_summary, "total_count"),
        "hard_portfolio_constraint_violation_count": sleeve_portfolio_constraint_count(portfolio_constraint_summary, "hard_count"),
    })
}


pub(crate) fn sleeve_execution_capacity_json(
    trial_parameters: &Value,
    backtest_parameters: Option<&Value>,
    portfolio_constraint_summary: &Value,
    metrics: Option<&Value>,
    gate_results: Option<&Value>,
) -> Value {
    let max_participation_rate = sleeve_nested_parameter_value(
        trial_parameters,
        backtest_parameters,
        &["execution_rules", "max_participation_rate"],
    );
    json!({
        "configured": {
            "capacity_risk_budget": sleeve_parameter_value(trial_parameters, backtest_parameters, "capacity_risk_budget"),
            "execution_impact_budget": sleeve_parameter_value(trial_parameters, backtest_parameters, "execution_impact_budget"),
            "execution_schedule_profile": sleeve_parameter_value(trial_parameters, backtest_parameters, "execution_schedule_profile"),
            "execution_carry_policy": sleeve_nested_parameter_value(
                trial_parameters,
                backtest_parameters,
                &["execution_rules", "execution_carry_policy"],
            ),
            "cash_utilization": sleeve_parameter_value(trial_parameters, backtest_parameters, "cash_utilization"),
            "candidate_ranking": sleeve_parameter_value(trial_parameters, backtest_parameters, "candidate_ranking"),
            "candidate_risk_filter": sleeve_parameter_value(trial_parameters, backtest_parameters, "candidate_risk_filter"),
            "top_n": sleeve_parameter_value(trial_parameters, backtest_parameters, "top_n"),
            "score_candidate_pool_size": sleeve_parameter_value(trial_parameters, backtest_parameters, "score_candidate_pool_size"),
            "max_gross_exposure": sleeve_parameter_value(trial_parameters, backtest_parameters, "max_gross_exposure"),
            "max_position_pct": sleeve_parameter_value(trial_parameters, backtest_parameters, "max_position_pct"),
            "max_participation_rate": max_participation_rate.clone(),
            "explicit_participation_cap_configured": !max_participation_rate.is_null(),
        },
        "realized": {
            "turnover": metric_value(metrics, "turnover"),
            "num_trades": metric_value(metrics, "num_trades"),
            "target_gross_exposure_pct": metric_value(metrics, "final_target_gross_exposure_pct"),
            "actual_gross_exposure_pct": metric_value(metrics, "final_actual_gross_exposure_pct"),
            "cash_weight_pct": metric_value(metrics, "final_cash_weight_pct"),
            "fill_ratio": metric_value(metrics, "final_execution_fill_ratio"),
            "unfilled_target_gap_pct": metric_value(metrics, "final_unfilled_target_gap_pct"),
        },
        "portfolio_constraints": portfolio_constraint_summary.clone(),
        "stress_gate": {
            "pass_ratio": sleeve_stress_pass_ratio(gate_results),
            "avg_perturbed_calmar": sleeve_gate_actual(gate_results, "train_avg_perturbed_calmar"),
            "min_perturbed_annual_return": sleeve_gate_actual(gate_results, "train_perturbed_annual_return"),
            "passed_count": sleeve_gate_field(gate_results, "train_cost_capacity_perturbation_pass_ratio", "passed_count"),
            "total_count": sleeve_gate_field(gate_results, "train_cost_capacity_perturbation_pass_ratio", "total_count"),
        },
    })
}


pub(crate) fn sleeve_trial_capacity_diagnosis_json(
    metrics: Option<&Value>,
    gate_results: Option<&Value>,
    portfolio_constraint_summary: &Value,
) -> Value {
    let annual_return = metric_f64_value(metrics, "annual_return_pct").unwrap_or(0.0);
    let excess_return = metric_f64_value(metrics, "excess_return_pct").unwrap_or(0.0);
    let sharpe = metric_f64_value(metrics, "sharpe_ratio").unwrap_or(0.0);
    let fill_ratio = metric_f64_value(metrics, "final_execution_fill_ratio");
    let unfilled_gap = metric_f64_value(metrics, "final_unfilled_target_gap_pct");
    let stress_pass_ratio = sleeve_stress_pass_ratio(gate_results);
    let avg_perturbed_calmar = sleeve_gate(gate_results, "train_avg_perturbed_calmar")
        .and_then(|gate| gate.get("actual"))
        .and_then(value_as_f64);
    let hard_constraint_count =
        sleeve_portfolio_constraint_count(portfolio_constraint_summary, "hard_count");

    let fill_below_gate = fill_ratio.is_some_and(|value| value < 0.90);
    let unfilled_above_gate = unfilled_gap.is_some_and(|value| value > 0.08);
    let stress_failed = stress_pass_ratio.is_some_and(|value| value < 0.80)
        || avg_perturbed_calmar.is_some_and(|value| value < 1.20);

    let mut findings = Vec::new();
    if annual_return > 0.0 {
        findings.push("positive_absolute_return");
    } else {
        findings.push("weak_or_negative_absolute_return");
    }
    if excess_return < 0.0 {
        findings.push("negative_excess_return");
    }
    if sharpe < 1.0 {
        findings.push("sub_professional_sharpe");
    }
    if fill_below_gate {
        findings.push("fill_below_90");
    }
    if unfilled_above_gate {
        findings.push("unfilled_gap_above_08");
    }
    if hard_constraint_count == 0 {
        findings.push("no_hard_portfolio_constraint_violations");
    } else {
        findings.push("hard_portfolio_constraint_violations_present");
    }
    if stress_failed {
        findings.push("cost_capacity_stress_failed");
    }

    let primary_failure_axis = if annual_return <= 0.0 || sharpe < 0.0 {
        "weak_or_negative_alpha"
    } else if hard_constraint_count > 0 {
        "hard_capacity_constraint"
    } else if fill_below_gate || unfilled_above_gate {
        "portfolio_expression_capacity_gap"
    } else if stress_failed && excess_return < 0.0 {
        "alpha_underbenchmark_and_stress_fragile"
    } else if stress_failed {
        "cost_capacity_stress_fragile"
    } else {
        "passed_observed_execution_capacity_diagnostics"
    };

    json!({
        "primary_failure_axis": primary_failure_axis,
        "findings": findings,
        "interpretation": match primary_failure_axis {
            "portfolio_expression_capacity_gap" => "Positive train-window alpha is not fully expressed after the configured execution/capacity envelope; inspect gross exposure, fill, and stress gates before any OOS promotion.",
            "alpha_underbenchmark_and_stress_fragile" => "Absolute train-window return is positive, but benchmark-relative economics and cost/capacity perturbations are too weak for professional promotion.",
            "cost_capacity_stress_fragile" => "Observed execution shape is acceptable, but cost/capacity perturbations do not preserve the candidate.",
            "hard_capacity_constraint" => "Backtest emitted hard portfolio capacity constraints; repair execution/liquidity rules before interpreting alpha economics.",
            "weak_or_negative_alpha" => "Train-window alpha economics are weak or negative; do not solve this with execution tuning.",
            _ => "No observed execution-capacity blocker in this diagnostic surface.",
        },
    })
}


pub(crate) fn sleeve_admission_best_trial_json(diagnostic: &SleeveAdmissionTrialDiagnostic) -> Value {
    json!({
        "trial_id": diagnostic.json.get("trial_id").cloned().unwrap_or(Value::Null),
        "family": diagnostic.family,
        "score": diagnostic.score,
        "robustness_status": diagnostic.robustness_status,
        "metrics": diagnostic.json.get("metrics").cloned().unwrap_or(Value::Null),
        "portfolio_expression": diagnostic.json.get("portfolio_expression").cloned().unwrap_or(Value::Null),
        "execution_quality": diagnostic.json.get("execution_quality").cloned().unwrap_or(Value::Null),
        "capacity_headroom": diagnostic.json.get("capacity_headroom").cloned().unwrap_or(Value::Null),
        "execution_capacity": diagnostic.json.get("execution_capacity").cloned().unwrap_or(Value::Null),
        "diagnosis": diagnostic.json.get("diagnosis").cloned().unwrap_or(Value::Null),
        "stress": diagnostic.json.get("stress").cloned().unwrap_or(Value::Null),
        "failed_gates": diagnostic.json.get("failed_gates").cloned().unwrap_or(Value::Null),
    })
}


pub(crate) fn avg_json(sum: f64, count: usize) -> Value {
    if count == 0 {
        Value::Null
    } else {
        json!(sum / count as f64)
    }
}


impl SleeveAdmissionFamilyAccumulator {
    fn record(&mut self, diagnostic: &SleeveAdmissionTrialDiagnostic) {
        self.trial_count += 1;
        match diagnostic.robustness_status.as_str() {
            "approved_candidate" => self.approved_count += 1,
            "rejected" => self.rejected_count += 1,
            _ => {}
        }
        if let Some(value) = diagnostic.annual_return {
            self.annual_return_sum += value;
            self.annual_return_count += 1;
        }
        if let Some(value) = diagnostic.sharpe {
            self.sharpe_sum += value;
            self.sharpe_count += 1;
        }
        if let Some(value) = diagnostic.calmar {
            self.calmar_sum += value;
            self.calmar_count += 1;
        }
        if let Some(value) = diagnostic.fill_ratio {
            self.fill_ratio_sum += value;
            self.fill_ratio_count += 1;
        }
        if let Some(value) = diagnostic.unfilled_gap {
            self.unfilled_gap_sum += value;
            self.unfilled_gap_count += 1;
        }
        if let Some(value) = diagnostic.stress_pass_ratio {
            self.stress_pass_ratio_sum += value;
            self.stress_pass_ratio_count += 1;
        }
        for gate in &diagnostic.failed_gate_names {
            *self.failed_gates.entry(gate.clone()).or_default() += 1;
        }
    }

    fn json(&self, family: &str) -> Value {
        let mut failures = self
            .failed_gates
            .iter()
            .map(|(gate, count)| (gate.clone(), *count))
            .collect::<Vec<_>>();
        failures.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

        json!({
            "family": family,
            "trial_count": self.trial_count,
            "rejected_count": self.rejected_count,
            "approved_count": self.approved_count,
            "avg_annual_return_pct": avg_json(self.annual_return_sum, self.annual_return_count),
            "avg_sharpe_ratio": avg_json(self.sharpe_sum, self.sharpe_count),
            "avg_calmar_ratio": avg_json(self.calmar_sum, self.calmar_count),
            "avg_fill_ratio": avg_json(self.fill_ratio_sum, self.fill_ratio_count),
            "avg_unfilled_target_gap_pct": avg_json(self.unfilled_gap_sum, self.unfilled_gap_count),
            "avg_stress_pass_ratio": avg_json(self.stress_pass_ratio_sum, self.stress_pass_ratio_count),
            "dominant_failure_modes": failures
                .into_iter()
                .take(5)
                .map(|(gate, count)| json!({"gate": gate, "count": count}))
                .collect::<Vec<_>>(),
        })
    }
}


impl SleeveAdmissionActionAccumulator {
    fn record(&mut self, diagnostic: &SleeveAdmissionTrialDiagnostic) {
        self.total_trial_count += 1;
        let annual_return = diagnostic.annual_return.unwrap_or(0.0);
        let sharpe = diagnostic.sharpe.unwrap_or(0.0);
        let positive = annual_return > 0.0;
        if positive {
            self.positive_trial_count += 1;
            if let Some(pass_ratio) = diagnostic.stress_pass_ratio {
                self.positive_stress_evaluated_count += 1;
                if pass_ratio >= 0.80 {
                    self.positive_stress_passed_count += 1;
                } else {
                    self.stress_failed_families
                        .insert(diagnostic.family.clone());
                    self.execution_capacity_families
                        .insert(diagnostic.family.clone());
                }
            }
            if diagnostic.fill_ratio.unwrap_or(1.0) < 0.90 {
                self.positive_fill_below_90_count += 1;
                self.execution_capacity_families
                    .insert(diagnostic.family.clone());
            }
            if diagnostic.unfilled_gap.unwrap_or(0.0) > 0.08 {
                self.positive_unfilled_above_08_count += 1;
                self.execution_capacity_families
                    .insert(diagnostic.family.clone());
            }
            if diagnostic.excess_return.unwrap_or(0.0) < 0.0 {
                self.positive_negative_excess_count += 1;
                self.underbenchmark_families
                    .insert(diagnostic.family.clone());
            }
        }

        if annual_return <= 0.0 || sharpe < 0.0 {
            self.weak_or_negative_alpha_count += 1;
            self.weak_alpha_families.insert(diagnostic.family.clone());
        }
    }

    fn json(&self) -> Value {
        let mut next_actions = Vec::new();
        if self.positive_trial_count > 0
            && (!self.execution_capacity_families.is_empty()
                || self.positive_stress_passed_count < self.positive_stress_evaluated_count)
        {
            next_actions.push(json!({
                "action": "repair_execution_capacity_for_positive_alpha",
                "priority": "P2",
                "families": self.execution_capacity_families.iter().cloned().collect::<Vec<_>>(),
                "evidence": {
                    "positive_trial_count": self.positive_trial_count,
                    "positive_stress_evaluated_count": self.positive_stress_evaluated_count,
                    "positive_stress_passed_count": self.positive_stress_passed_count,
                    "positive_fill_below_90_count": self.positive_fill_below_90_count,
                    "positive_unfilled_above_08_count": self.positive_unfilled_above_08_count,
                    "positive_negative_excess_count": self.positive_negative_excess_count,
                },
                "next_step": "Diagnose train-window fill, unfilled target gap, participation/capacity headroom, and perturbed Calmar before allowing any OOS promotion.",
            }));
        }
        if self.weak_or_negative_alpha_count > 0 {
            next_actions.push(json!({
                "action": "rebuild_alpha_sources_for_weak_or_negative_train_windows",
                "priority": "P2",
                "families": self.weak_alpha_families.iter().cloned().collect::<Vec<_>>(),
                "evidence": {
                    "weak_or_negative_alpha_count": self.weak_or_negative_alpha_count,
                    "total_trial_count": self.total_trial_count,
                },
                "next_step": "Introduce genuinely new PIT-proven low-correlation alpha sources; do not keep scaling the same rejected seed set.",
            }));
        }

        json!({
            "total_trial_count": self.total_trial_count,
            "positive_trial_count": self.positive_trial_count,
            "positive_stress_evaluated_count": self.positive_stress_evaluated_count,
            "positive_stress_passed_count": self.positive_stress_passed_count,
            "positive_fill_below_90_count": self.positive_fill_below_90_count,
            "positive_unfilled_above_08_count": self.positive_unfilled_above_08_count,
            "positive_negative_excess_count": self.positive_negative_excess_count,
            "weak_or_negative_alpha_count": self.weak_or_negative_alpha_count,
            "stress_failed_families": self.stress_failed_families.iter().cloned().collect::<Vec<_>>(),
            "underbenchmark_families": self.underbenchmark_families.iter().cloned().collect::<Vec<_>>(),
            "next_actions": next_actions,
            "non_goals": [
                "do_not_relax_train_or_oos_gates",
                "do_not_use_oos_strong_windows_for_reverse_tuning",
                "do_not_promote_rejected_trials"
            ],
        })
    }
}


pub(crate) fn alpha_source_diagnostics_combo_name(
    req: &AlphaSourceDiagnosticsRequest,
) -> Result<String, String> {
    let combo_name = req.combo_name.trim();
    if combo_name.is_empty() {
        return Err("alpha source diagnostics combo_name must not be empty".into());
    }
    Ok(combo_name.to_string())
}


pub(crate) fn alpha_source_diagnostics_version(req: &AlphaSourceDiagnosticsRequest) -> String {
    req.version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("1.0.0")
        .to_string()
}


pub(crate) fn alpha_source_diagnostics_thresholds(req: &AlphaSourceDiagnosticsRequest) -> ReadinessThresholds {
    ReadinessThresholds::from_options(
        req.min_day_coverage_ratio,
        req.min_daily_rows,
        req.min_p95_daily_row_ratio,
    )
}


pub(crate) fn alpha_source_diagnostics_persist_default(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}


pub(crate) fn alpha_source_diagnostics_gates_passed(gates: &[Value]) -> bool {
    gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false))
}


pub(crate) fn alpha_source_diagnostics_level(passed: bool) -> &'static str {
    if passed {
        "green"
    } else {
        "red"
    }
}


pub(crate) fn alpha_source_research_economic_admission(research_metrics: &Value) -> Value {
    if !research_metrics["included"].as_bool().unwrap_or(false) {
        return json!({
            "status": "not_evaluated",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked",
            "reason": "include_research_metrics=true is required before economic admission",
        });
    }

    let rank_by_horizon = research_metrics["rank_ic_by_horizon"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let group_by_horizon = research_metrics["group_return_by_horizon"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let turnover_by_horizon = research_metrics["turnover_capacity_by_horizon"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut passed_horizons = Vec::new();
    let mut failed_horizons = Vec::new();

    for rank in rank_by_horizon {
        let horizon = rank["horizon_days"].as_i64().unwrap_or_default();
        let mean_rank_ic = rank["mean_rank_ic"].as_f64().unwrap_or_default();
        let positive_day_ratio = rank["positive_day_ratio"].as_f64().unwrap_or_default();
        let group = group_by_horizon
            .iter()
            .find(|item| item["horizon_days"].as_i64() == Some(horizon))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let turnover = turnover_by_horizon
            .iter()
            .find(|item| item["horizon_days"].as_i64() == Some(horizon))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let high_minus_low_spread = group["high_minus_low_spread"].as_f64().unwrap_or_default();
        let monotonicity_score = group["monotonicity_score"].as_f64().unwrap_or_default();
        let avg_turnover = turnover["avg_high_score_bucket_turnover"]
            .as_f64()
            .unwrap_or_default();
        let horizon_passed = mean_rank_ic >= 0.01
            && positive_day_ratio >= 0.52
            && high_minus_low_spread > 0.0
            && monotonicity_score >= 0.55
            && avg_turnover <= 0.75;
        let item = json!({
            "horizon_days": horizon,
            "mean_rank_ic": mean_rank_ic,
            "positive_day_ratio": positive_day_ratio,
            "high_minus_low_spread": high_minus_low_spread,
            "monotonicity_score": monotonicity_score,
            "avg_high_score_bucket_turnover": avg_turnover,
            "passed": horizon_passed,
        });
        if horizon_passed {
            passed_horizons.push(item);
        } else {
            failed_horizons.push(item);
        }
    }

    let passed = passed_horizons.len() >= 2;
    json!({
        "status": if passed {
            "candidate_ready_for_bounded_wfa"
        } else {
            "blocked_by_p310_economics"
        },
        "passed": passed,
        "bounded_wfa": if passed { "eligible" } else { "blocked" },
        "v19_train_selection": "blocked_until_bounded_wfa_passes",
        "criteria": {
            "min_passing_horizons": 2,
            "mean_rank_ic": ">= 0.01",
            "positive_day_ratio": ">= 0.52",
            "high_minus_low_spread": "> 0",
            "monotonicity_score": ">= 0.55",
            "avg_high_score_bucket_turnover": "<= 0.75"
        },
        "passed_horizon_count": passed_horizons.len(),
        "passed_horizons": passed_horizons,
        "failed_horizons": failed_horizons,
    })
}


pub(crate) fn futures_price_chain_component_orientation_contract_json(
    include_research_metrics: bool,
) -> Value {
    if !include_research_metrics {
        return json!({
            "included": false,
            "reason": "set include_research_metrics=true to expose futures_price_chain component orientation diagnostics",
            "admission": {
                "status": "not_evaluated",
                "bounded_wfa": "blocked",
                "v19_train_selection": "blocked"
            }
        });
    }

    json!({
        "included": true,
        "research_only": true,
        "scope": {
            "component_source": "market_futures_product_signal_pit -> PIT product exposure mapping -> PIT stock industry membership",
            "point_in_time": "raw futures signals require available_at >= source trade_date and are joined only when every mapping/membership available_at <= stock trade_date",
            "label_policy": "forward stock returns are diagnostics labels only; component direction changes cannot be inferred from full-period test labels",
            "persistence": "does_not_write_multi_factor_value"
        },
        "components": [
            {
                "component": "price_momentum",
                "signal_code": "fpc_price_mom_20v60_std",
                "economic_link": "commodity main-contract price momentum is a producer revenue / input-cost pressure proxy after product->industry mapping direction is applied",
                "default_orientation": "higher mapped score is expected to be favorable for the mapped industry only under the pre-registered mapping direction",
                "failure_interpretation": "negative RankIC/spread may mean price momentum benefits downstream cost relief or marks crowded late-cycle input inflation",
                "sign_flip_policy": "blocked: no full-period sign flip; any orientation change requires train-only pre-registration and fresh bounded WFA"
            },
            {
                "component": "inventory_tightness",
                "signal_code": "fpc_inventory_tight_20v60_std",
                "economic_link": "falling warehouse receipts versus the medium-term baseline proxy supply tightness and near-term price support",
                "default_orientation": "higher mapped score is expected to favor producer/price beneficiaries after mapping direction is applied",
                "failure_interpretation": "negative RankIC/spread may mean tight inventory is already priced, demand destruction dominates, or downstream margin pressure matters more",
                "sign_flip_policy": "blocked: no full-period sign flip; any orientation change requires train-only pre-registration and fresh bounded WFA"
            },
            {
                "component": "net_position_trend",
                "signal_code": "fpc_net_position_20v60_std",
                "economic_link": "member long-short positioning trend proxies futures-market conviction and possible positioning crowding around the product",
                "default_orientation": "higher mapped score is expected to favor the mapped industry only if positioning trend is not crowded/late-cycle",
                "failure_interpretation": "negative RankIC/spread may mean positioning trend is a crowding/reversal signal rather than a fundamentals signal",
                "sign_flip_policy": "blocked: no full-period sign flip; any orientation change requires train-only pre-registration and fresh bounded WFA"
            }
        ],
        "admission": {
            "status": "blocked_until_component_economics_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked",
            "required_next_step": "run pre-registered train-only component RankIC/group-return/decay/turnover-capacity diagnostics; stop the source if no component has robust positive economics"
        }
    })
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceDailyBreadthStatus {
    pub(crate) passed: bool,
    pub(crate) status: &'static str,
    pub(crate) raw_weak_day_count: usize,
    pub(crate) structural_early_weak_day_count: usize,
    pub(crate) unexplained_weak_day_count: usize,
    pub(crate) weak_day_threshold: i64,
    pub(crate) weak_day_min_to_threshold_ratio: f64,
    pub(crate) weak_day_ratio: f64,
    pub(crate) weak_day_region_end_ratio: f64,
    pub(crate) first_weak_day: Option<NaiveDate>,
    pub(crate) last_weak_day: Option<NaiveDate>,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceMarketScopeBreadthStatus {
    pub(crate) passed: bool,
    pub(crate) status: &'static str,
    pub(crate) eligible_days: usize,
    pub(crate) joined_days: usize,
    pub(crate) missing_eligible_days: usize,
    pub(crate) min_ratio: f64,
    pub(crate) p10_ratio: f64,
    pub(crate) p50_ratio: f64,
    pub(crate) p95_ratio: f64,
    pub(crate) max_ratio: f64,
    pub(crate) weak_day_count: usize,
    pub(crate) weak_day_threshold_ratio: f64,
    pub(crate) first_weak_day: Option<NaiveDate>,
    pub(crate) last_weak_day: Option<NaiveDate>,
}


pub(crate) fn alpha_source_daily_breadth_status(
    daily_rows: &[(NaiveDate, i64)],
    distribution: &DailyCountDistribution,
) -> AlphaSourceDailyBreadthStatus {
    let raw_weak_day_count = distribution.weak_day_count;
    if daily_rows.is_empty() || raw_weak_day_count == 0 || distribution.weak_day_threshold <= 0 {
        return AlphaSourceDailyBreadthStatus {
            passed: true,
            status: "stable",
            raw_weak_day_count,
            structural_early_weak_day_count: 0,
            unexplained_weak_day_count: 0,
            weak_day_threshold: distribution.weak_day_threshold,
            weak_day_min_to_threshold_ratio: 1.0,
            weak_day_ratio: 0.0,
            weak_day_region_end_ratio: 0.0,
            first_weak_day: None,
            last_weak_day: None,
        };
    }

    let weak_rows = daily_rows
        .iter()
        .filter(|(_, count)| *count < distribution.weak_day_threshold)
        .copied()
        .collect::<Vec<_>>();
    let weak_day_ratio = raw_weak_day_count as f64 / daily_rows.len() as f64;
    let min_weak_rows = weak_rows.iter().map(|(_, count)| *count).min().unwrap_or(0);
    let weak_day_min_to_threshold_ratio =
        min_weak_rows.max(0) as f64 / distribution.weak_day_threshold as f64;
    let first_weak_day = weak_rows.first().map(|(trade_date, _)| *trade_date);
    let last_weak_day = weak_rows.last().map(|(trade_date, _)| *trade_date);
    let last_weak_index = daily_rows
        .iter()
        .rposition(|(_, count)| *count < distribution.weak_day_threshold)
        .unwrap_or(0);
    let weak_day_region_end_ratio = (last_weak_index + 1) as f64 / daily_rows.len() as f64;
    let structural_early_ramp = weak_day_ratio <= 0.10
        && weak_day_region_end_ratio <= 0.10
        && weak_day_min_to_threshold_ratio >= 0.80
        && distribution.p50_rows >= distribution.weak_day_threshold
        && distribution.max_rows > distribution.min_rows;
    let unexplained_weak_day_count = if structural_early_ramp {
        0
    } else {
        raw_weak_day_count
    };

    AlphaSourceDailyBreadthStatus {
        passed: unexplained_weak_day_count == 0,
        status: if structural_early_ramp {
            "structural_early_ramp"
        } else {
            "unexplained_cliff"
        },
        raw_weak_day_count,
        structural_early_weak_day_count: if structural_early_ramp {
            raw_weak_day_count
        } else {
            0
        },
        unexplained_weak_day_count,
        weak_day_threshold: distribution.weak_day_threshold,
        weak_day_min_to_threshold_ratio,
        weak_day_ratio,
        weak_day_region_end_ratio,
        first_weak_day,
        last_weak_day,
    }
}


pub(crate) fn alpha_source_market_scope_breadth_status(
    daily_rows: &[(NaiveDate, i64)],
    eligible_rows: &[(NaiveDate, i64)],
) -> AlphaSourceMarketScopeBreadthStatus {
    let eligible_by_day = eligible_rows
        .iter()
        .copied()
        .collect::<BTreeMap<NaiveDate, i64>>();
    let mut ratios = Vec::new();
    let mut dated_ratios = Vec::new();
    let mut missing_eligible_days = 0usize;
    for (trade_date, scored_count) in daily_rows {
        let Some(eligible_count) = eligible_by_day.get(trade_date).copied() else {
            missing_eligible_days += 1;
            continue;
        };
        if eligible_count <= 0 {
            missing_eligible_days += 1;
            continue;
        }
        let ratio = (*scored_count).max(0) as f64 / eligible_count as f64;
        if ratio.is_finite() {
            ratios.push(ratio);
            dated_ratios.push((*trade_date, ratio));
        }
    }
    ratios.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let p50_ratio = percentile(&ratios, 0.50);
    let weak_day_threshold_ratio = 0.10_f64.max(p50_ratio * 0.50);
    let weak_days = dated_ratios
        .iter()
        .copied()
        .filter(|(_, ratio)| *ratio < weak_day_threshold_ratio)
        .collect::<Vec<_>>();
    let passed = !ratios.is_empty() && missing_eligible_days == 0 && weak_days.is_empty();
    AlphaSourceMarketScopeBreadthStatus {
        passed,
        status: if passed {
            "stable_market_scope_ratio"
        } else if ratios.is_empty() || missing_eligible_days > 0 {
            "eligible_universe_missing"
        } else {
            "market_scope_ratio_cliff"
        },
        eligible_days: eligible_rows.len(),
        joined_days: dated_ratios.len(),
        missing_eligible_days,
        min_ratio: ratios.first().copied().unwrap_or(0.0),
        p10_ratio: percentile(&ratios, 0.10),
        p50_ratio,
        p95_ratio: percentile(&ratios, 0.95),
        max_ratio: ratios.last().copied().unwrap_or(0.0),
        weak_day_count: weak_days.len(),
        weak_day_threshold_ratio,
        first_weak_day: weak_days.first().map(|(trade_date, _)| *trade_date),
        last_weak_day: weak_days.last().map(|(trade_date, _)| *trade_date),
    }
}


pub(crate) fn alpha_source_market_scope_breadth_json(status: &AlphaSourceMarketScopeBreadthStatus) -> Value {
    json!({
        "status": status.status,
        "passed": status.passed,
        "eligible_days": status.eligible_days,
        "joined_days": status.joined_days,
        "missing_eligible_days": status.missing_eligible_days,
        "min_ratio": status.min_ratio,
        "p10_ratio": status.p10_ratio,
        "p50_ratio": status.p50_ratio,
        "p95_ratio": status.p95_ratio,
        "max_ratio": status.max_ratio,
        "weak_day_count": status.weak_day_count,
        "weak_day_threshold_ratio": status.weak_day_threshold_ratio,
        "first_weak_day": status.first_weak_day,
        "last_weak_day": status.last_weak_day,
        "policy": "market-scope gated alpha sources are checked for stable scored/eligible coverage ratio; minimum daily rows, PIT and economic diagnostics remain separate gates"
    })
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlphaSourceResearchDiagnosticsOptions {
    pub(crate) include_research_metrics: bool,
    pub(crate) include_exposure_regime_metrics: bool,
    pub(crate) return_horizons: Vec<i64>,
    pub(crate) bucket_count: i64,
    pub(crate) max_rank_ic_days: i64,
    pub(crate) max_exposure_regime_days: i64,
}


pub(crate) fn alpha_source_research_diagnostics_options(
    req: &AlphaSourceDiagnosticsRequest,
) -> AlphaSourceResearchDiagnosticsOptions {
    let mut horizons = req
        .return_horizons
        .clone()
        .unwrap_or_else(|| vec![20, 45, 60, 120])
        .into_iter()
        .filter(|horizon| *horizon > 0)
        .map(|horizon| horizon.clamp(1, 252))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if horizons.is_empty() {
        horizons = vec![20, 45, 60, 120];
    }

    AlphaSourceResearchDiagnosticsOptions {
        include_research_metrics: req.include_research_metrics.unwrap_or(false),
        include_exposure_regime_metrics: req.include_exposure_regime_metrics.unwrap_or(false),
        return_horizons: horizons,
        bucket_count: req.bucket_count.unwrap_or(10).clamp(3, 20),
        max_rank_ic_days: req.max_rank_ic_days.unwrap_or(260).clamp(1, 520),
        max_exposure_regime_days: req.max_exposure_regime_days.unwrap_or(260).clamp(1, 520),
    }
}


pub(crate) fn alpha_source_market_scope_breadth_enabled(
    req: &AlphaSourceDiagnosticsRequest,
    combo_name: &str,
) -> bool {
    combo_name
        .trim()
        .eq_ignore_ascii_case("futures_price_chain")
        && req.alpha_admission_gate_id.as_deref().map(str::trim)
            == Some(FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID)
        && req.universe_profile.as_deref().map(str::trim)
            == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
        || combo_name
            .trim()
            .eq_ignore_ascii_case("margin_detail_leverage_crowding")
            && req.alpha_admission_gate_id.as_deref().map(str::trim)
                == Some(MARGIN_DETAIL_COVERAGE_GATE_ID)
            && req.universe_profile.as_deref().map(str::trim)
                == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MainBusinessDiagnosticsUniverse {
    ListedNonSt,
    MainChinextNonSt,
}


impl MainBusinessDiagnosticsUniverse {
    fn as_str(self) -> &'static str {
        match self {
            Self::ListedNonSt => "listed_non_st",
            Self::MainChinextNonSt => "main_chinext_non_st",
        }
    }

    fn market_filter_sql(self) -> &'static str {
        match self {
            Self::ListedNonSt => "",
            Self::MainChinextNonSt => "AND ms.market IN ('主板', '创业板')",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::ListedNonSt => "all listed non-ST A-share symbols with daily bars",
            Self::MainChinextNonSt => {
                "main-board and ChiNext listed non-ST symbols only; STAR market excluded by explicit scope"
            }
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MainBusinessDiagnosticsProfile {
    SalesYoy,
    ProfitYoy,
    GrossMarginDeltaYoy,
    SegmentConcentrationInverse,
}


impl MainBusinessDiagnosticsProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::SalesYoy => "sales_yoy",
            Self::ProfitYoy => "profit_yoy",
            Self::GrossMarginDeltaYoy => "gross_margin_delta_yoy",
            Self::SegmentConcentrationInverse => "segment_concentration_inverse",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::SalesYoy => "YoY主营收入增长",
            Self::ProfitYoy => "YoY主营利润增长",
            Self::GrossMarginDeltaYoy => "主营毛利率同比变化",
            Self::SegmentConcentrationInverse => "业务分散度",
        }
    }

    fn score_expression_sql(self) -> &'static str {
        match self {
            Self::SalesYoy => {
                "CASE WHEN sales > 0 AND prev_sales > 0 THEN LN(sales / prev_sales) END"
            }
            Self::ProfitYoy => {
                "CASE WHEN profit > 0 AND prev_profit > 0 THEN LN(profit / prev_profit) END"
            }
            Self::GrossMarginDeltaYoy => {
                "CASE
                    WHEN sales > 0 AND prev_sales > 0 AND profit IS NOT NULL AND prev_profit IS NOT NULL
                    THEN (profit / sales) - (prev_profit / prev_sales)
                 END"
            }
            Self::SegmentConcentrationInverse => {
                "CASE WHEN sales_hhi IS NOT NULL THEN -sales_hhi END"
            }
        }
    }
}


pub(crate) fn main_business_diagnostics_business_type(req: &MainBusinessDiagnosticsRequest) -> String {
    req.business_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("P")
        .to_ascii_uppercase()
}


pub(crate) fn main_business_diagnostics_universe_profile(
    req: &MainBusinessDiagnosticsRequest,
) -> Result<MainBusinessDiagnosticsUniverse, String> {
    match req
        .universe_profile
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("listed_non_st")
    {
        "listed_non_st" | "listed-non-st" => Ok(MainBusinessDiagnosticsUniverse::ListedNonSt),
        "main_chinext_non_st" | "main-chinext-non-st" => {
            Ok(MainBusinessDiagnosticsUniverse::MainChinextNonSt)
        }
        other => Err(format!(
            "unsupported main_business universe_profile '{other}'; supported profiles are listed_non_st and main_chinext_non_st"
        )),
    }
}


pub(crate) fn parse_main_business_diagnostics_profile(
    value: &str,
) -> Result<MainBusinessDiagnosticsProfile, String> {
    match value.trim() {
        "sales_yoy" | "sales-yoy" => Ok(MainBusinessDiagnosticsProfile::SalesYoy),
        "profit_yoy" | "profit-yoy" => Ok(MainBusinessDiagnosticsProfile::ProfitYoy),
        "gross_margin_delta_yoy" | "gross-margin-delta-yoy" => {
            Ok(MainBusinessDiagnosticsProfile::GrossMarginDeltaYoy)
        }
        "segment_concentration_inverse" | "segment-concentration-inverse" => {
            Ok(MainBusinessDiagnosticsProfile::SegmentConcentrationInverse)
        }
        other => Err(format!(
            "main_business diagnostics profile '{other}' is not pre-registered; supported profiles are sales_yoy, profit_yoy, gross_margin_delta_yoy and segment_concentration_inverse"
        )),
    }
}


pub(crate) fn main_business_diagnostics_profiles(
    req: &MainBusinessDiagnosticsRequest,
) -> Result<Vec<MainBusinessDiagnosticsProfile>, String> {
    let requested = req.profiles.clone().unwrap_or_else(|| {
        vec![
            "sales_yoy".to_string(),
            "profit_yoy".to_string(),
            "gross_margin_delta_yoy".to_string(),
            "segment_concentration_inverse".to_string(),
        ]
    });
    let mut profiles = BTreeSet::new();
    for profile in requested {
        let trimmed = profile.trim();
        if trimmed.is_empty() {
            continue;
        }
        profiles.insert(parse_main_business_diagnostics_profile(trimmed)?);
    }
    if profiles.is_empty() {
        return Err(
            "main_business diagnostics requires at least one pre-registered profile".into(),
        );
    }
    Ok(profiles.into_iter().collect())
}


pub(crate) fn main_business_diagnostics_thresholds(
    req: &MainBusinessDiagnosticsRequest,
) -> ReadinessThresholds {
    ReadinessThresholds::from_options(
        req.min_day_coverage_ratio,
        req.min_daily_rows,
        req.min_p95_daily_row_ratio,
    )
}


pub(crate) fn bounded_main_business_f64(value: Option<f64>, default: f64, min: f64, max: f64) -> f64 {
    value
        .filter(|candidate| candidate.is_finite())
        .unwrap_or(default)
        .clamp(min, max)
}


pub(crate) fn main_business_daily_coverage_ratio_threshold(req: &MainBusinessDiagnosticsRequest) -> f64 {
    bounded_main_business_f64(req.min_daily_coverage_ratio, 0.90, 0.0, 1.0)
}


pub(crate) fn main_business_diagnostics_persist_default(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}


pub(crate) fn main_business_research_diagnostics_options(
    req: &MainBusinessDiagnosticsRequest,
) -> AlphaSourceResearchDiagnosticsOptions {
    let proxy = AlphaSourceDiagnosticsRequest {
        combo_name: "phase7_main_business_research_only_v1".to_string(),
        version: None,
        start_date: req.start_date.clone(),
        end_date: req.end_date.clone(),
        alpha_admission_gate_id: None,
        universe_profile: req.universe_profile.clone(),
        min_day_coverage_ratio: req.min_day_coverage_ratio,
        min_daily_rows: req.min_daily_rows,
        min_p95_daily_row_ratio: req.min_p95_daily_row_ratio,
        persist_report: req.persist_report,
        include_research_metrics: Some(true),
        return_horizons: req.return_horizons.clone(),
        bucket_count: req.bucket_count,
        max_rank_ic_days: req.max_rank_ic_days,
        include_exposure_regime_metrics: Some(req.include_exposure_regime_metrics.unwrap_or(true)),
        max_exposure_regime_days: req.max_exposure_regime_days,
    };
    alpha_source_research_diagnostics_options(&proxy)
}


pub(crate) fn parse_feature_profile_readiness_date(value: &str, field: &str) -> Result<NaiveDate, String> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d"))
        .map_err(|_| format!("{} must use YYYYMMDD or YYYY-MM-DD format", field))
}


pub(crate) fn profile_readiness_gate(
    gate: &str,
    passed: bool,
    actual: Value,
    expected: Value,
    detail: impl Into<String>,
) -> Value {
    json!({
        "gate": gate,
        "passed": passed,
        "actual": actual,
        "expected": expected,
        "detail": detail.into(),
    })
}


pub(crate) fn feature_profile_readiness_passed(report: &Value) -> bool {
    report
        .get("passed")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}


pub(crate) fn readiness_failure_summary(report: &Value) -> String {
    report
        .get("gates")
        .and_then(|value| value.as_array())
        .map(|gates| {
            gates
                .iter()
                .filter(|gate| !gate["passed"].as_bool().unwrap_or(false))
                .take(4)
                .map(|gate| {
                    format!(
                        "{} actual={} expected={}",
                        gate["gate"].as_str().unwrap_or("unknown_gate"),
                        gate["actual"],
                        gate["expected"]
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|summary| !summary.is_empty())
        .unwrap_or_else(|| "no failing gate detail".to_string())
}


pub(crate) async fn build_alpha_source_diagnostics_report_from_request(
    db: &sqlx::PgPool,
    req: &AlphaSourceDiagnosticsRequest,
) -> Result<Value, String> {
    let combo_name = alpha_source_diagnostics_combo_name(req)?;
    validate_alpha_source_diagnostics_admission(req, &combo_name)?;
    let version = alpha_source_diagnostics_version(req);
    let start = parse_feature_profile_readiness_date(&req.start_date, "start_date")?;
    let end = parse_feature_profile_readiness_date(&req.end_date, "end_date")?;
    if start > end {
        return Err("alpha source diagnostics start_date cannot be after end_date".into());
    }
    let thresholds = alpha_source_diagnostics_thresholds(req);
    let research_options = alpha_source_research_diagnostics_options(req);
    let use_market_scope_breadth = alpha_source_market_scope_breadth_enabled(req, &combo_name);
    let report = build_alpha_source_diagnostics_report(
        db,
        &combo_name,
        &version,
        start,
        end,
        thresholds,
        research_options,
        use_market_scope_breadth,
    )
    .await?;
    let experiment_run_id = if alpha_source_diagnostics_persist_default(req.persist_report) {
        Some(persist_alpha_source_diagnostics_report(db, &combo_name, &version, &report).await?)
    } else {
        None
    };
    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}


pub(crate) async fn build_alpha_source_diagnostics_report(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    start: NaiveDate,
    end: NaiveDate,
    thresholds: ReadinessThresholds,
    research_options: AlphaSourceResearchDiagnosticsOptions,
    use_market_scope_breadth: bool,
) -> Result<Value, String> {
    let summary =
        load_alpha_source_diagnostics_summary(db, combo_name, version, start, end).await?;
    let daily_rows =
        load_alpha_source_diagnostics_daily_rows(db, combo_name, version, start, end).await?;
    let expected_days = readiness_expected_open_day_count(db, start, end).await?;
    let covered_days = daily_rows.len() as i64;
    let daily_counts = daily_rows
        .iter()
        .map(|(_, count)| *count)
        .collect::<Vec<_>>();
    let distribution = daily_count_distribution(&daily_counts, thresholds);
    let breadth_status = alpha_source_daily_breadth_status(&daily_rows, &distribution);
    let market_scope_breadth_status = if use_market_scope_breadth {
        let eligible_rows = load_main_chinext_non_st_eligible_daily_rows(db, start, end).await?;
        Some(alpha_source_market_scope_breadth_status(
            &daily_rows,
            &eligible_rows,
        ))
    } else {
        None
    };
    let breadth_gate_passed = breadth_status.passed
        || market_scope_breadth_status
            .as_ref()
            .map(|status| status.passed)
            .unwrap_or(false);
    let day_coverage_ratio = if expected_days <= 0 {
        1.0
    } else {
        covered_days.max(0) as f64 / expected_days as f64
    };
    let gates = vec![
        profile_readiness_gate(
            "alpha_source_usable_rows",
            summary.usable_rows > 0,
            json!(summary.usable_rows),
            json!("> 0"),
            "alpha source must have PIT-usable normalized scores in the requested window",
        ),
        profile_readiness_gate(
            "alpha_source_day_coverage",
            day_coverage_ratio >= thresholds.min_day_coverage_ratio,
            json!(day_coverage_ratio),
            json!(thresholds.min_day_coverage_ratio),
            "alpha source must cover enough expected open days before WFA",
        ),
        profile_readiness_gate(
            "alpha_source_daily_median_symbols",
            distribution.p50_rows >= thresholds.min_daily_rows,
            json!(distribution.p50_rows),
            json!(thresholds.min_daily_rows),
            "alpha source median daily symbol count must be large enough for cross-sectional ranking",
        ),
        profile_readiness_gate(
            "alpha_source_daily_symbol_cliff",
            breadth_gate_passed,
            json!({
                "status": breadth_status.status,
                "raw_weak_day_count": breadth_status.raw_weak_day_count,
                "structural_early_weak_day_count": breadth_status.structural_early_weak_day_count,
                "unexplained_weak_day_count": breadth_status.unexplained_weak_day_count,
                "weak_day_threshold": breadth_status.weak_day_threshold,
                "p95_rows": distribution.p95_rows,
                "weak_day_ratio": breadth_status.weak_day_ratio,
                "weak_day_region_end_ratio": breadth_status.weak_day_region_end_ratio,
                "weak_day_min_to_threshold_ratio": breadth_status.weak_day_min_to_threshold_ratio,
                "first_weak_day": breadth_status.first_weak_day,
                "last_weak_day": breadth_status.last_weak_day,
                "market_scope_ratio": market_scope_breadth_status
                    .as_ref()
                    .map(alpha_source_market_scope_breadth_json),
            }),
            json!("unexplained_weak_day_count = 0 or stable market-scope scored/eligible ratio"),
            "alpha source daily breadth must not have unexplained symbol-count cliffs; gated market-scope sources may explain raw breadth changes with stable scored/eligible coverage ratio",
        ),
        profile_readiness_gate(
            "alpha_source_future_leak_rows",
            summary.future_leak_rows == 0,
            json!(summary.future_leak_rows),
            json!(0),
            "multi_factor_value.available_at must not be after trade_date",
        ),
        profile_readiness_gate(
            "alpha_source_null_available_at_rows",
            summary.null_available_at_rows == 0,
            json!(summary.null_available_at_rows),
            json!(0),
            "alpha source rows must bind a PIT available_at date before entering train selection",
        ),
        profile_readiness_gate(
            "alpha_source_null_score_rows",
            summary.null_score_rows == 0,
            json!(summary.null_score_rows),
            json!(0),
            "alpha source rows must have normalized_score for ranking",
        ),
    ];
    let passed = alpha_source_diagnostics_gates_passed(&gates);
    let research_metrics = build_alpha_source_research_diagnostics(
        db,
        combo_name,
        version,
        thresholds,
        &daily_rows,
        &research_options,
    )
    .await?;
    let research_economic_admission = alpha_source_research_economic_admission(&research_metrics);
    let research_next_stage = research_economic_admission["status"]
        .as_str()
        .unwrap_or("rank_ic_group_return_turnover_capacity_diagnostics")
        .to_string();
    let component_orientation_diagnostics =
        build_futures_price_chain_component_orientation_diagnostics(
            db,
            combo_name,
            version,
            thresholds,
            &daily_rows,
            &research_options,
        )
        .await?;

    Ok(json!({
        "diagnostics_type": "alpha_source",
        "combo_name": combo_name,
        "version": version,
        "requested_start_date": start,
        "requested_end_date": end,
        "passed": passed,
        "level": alpha_source_diagnostics_level(passed),
        "thresholds": {
            "min_day_coverage_ratio": thresholds.min_day_coverage_ratio,
            "min_daily_rows": thresholds.min_daily_rows,
            "min_p95_daily_row_ratio": thresholds.min_p95_daily_row_ratio,
        },
        "summary": {
            "expected_open_days": expected_days,
            "covered_days": covered_days,
            "missing_open_days": (expected_days - covered_days).max(0),
            "day_coverage_ratio": day_coverage_ratio,
            "usable_rows": summary.usable_rows,
            "usable_symbols": summary.usable_symbols,
            "null_score_rows": summary.null_score_rows,
            "null_available_at_rows": summary.null_available_at_rows,
            "future_leak_rows": summary.future_leak_rows,
            "first_trade_date": summary.first_trade_date,
            "last_trade_date": summary.last_trade_date,
            "daily_symbols": {
                "min": distribution.min_rows,
                "p50": distribution.p50_rows,
                "p95": distribution.p95_rows,
                "max": distribution.max_rows,
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
                "breadth_status": breadth_status.status,
                "raw_weak_day_count": breadth_status.raw_weak_day_count,
                "structural_early_weak_day_count": breadth_status.structural_early_weak_day_count,
                "unexplained_weak_day_count": breadth_status.unexplained_weak_day_count,
                "weak_day_ratio": breadth_status.weak_day_ratio,
                "weak_day_region_end_ratio": breadth_status.weak_day_region_end_ratio,
                "weak_day_min_to_threshold_ratio": breadth_status.weak_day_min_to_threshold_ratio,
                "first_weak_day": breadth_status.first_weak_day,
                "last_weak_day": breadth_status.last_weak_day,
                "market_scope_ratio": market_scope_breadth_status
                    .as_ref()
                    .map(alpha_source_market_scope_breadth_json),
            }
        },
        "gates": gates,
        "research_metrics": research_metrics,
        "research_economic_admission": research_economic_admission,
        "component_orientation_diagnostics": component_orientation_diagnostics,
        "repair": alpha_source_diagnostics_repair_hint(passed, &summary),
        "scope": {
            "point_in_time": "multi_factor_value.available_at <= trade_date",
            "research_stage": if research_options.include_research_metrics {
                "rank_ic_group_return_decay_turnover_capacity"
            } else {
                "coverage_and_pit_only"
            },
            "research_only_labels": if research_options.include_research_metrics {
                "forward returns are used only as post-hoc labels for diagnostics; they are not written to factor/model inputs"
            } else {
                "not requested"
            },
            "next_stage": if research_options.include_research_metrics {
                research_next_stage
            } else {
                "rank_ic_group_return_turnover_capacity_diagnostics".to_string()
            }
        }
    }))
}


#[derive(Debug, Clone)]
pub(crate) struct MainBusinessRawSummary {
    raw_rows: i64,
    raw_symbols: i64,
    report_periods: i64,
    pit_violation_rows: i64,
    out_of_market_stock_rows: i64,
    period_universe_mismatch_rows: i64,
    first_end_date: Option<NaiveDate>,
    last_end_date: Option<NaiveDate>,
    first_available_at: Option<NaiveDate>,
    last_available_at: Option<NaiveDate>,
}


#[derive(Debug, Clone)]
pub(crate) struct MainBusinessDailyCoverageRow {
    trade_date: NaiveDate,
    eligible_symbols: i64,
    covered_symbols: i64,
}


impl MainBusinessDailyCoverageRow {
    fn coverage_ratio(&self) -> f64 {
        if self.eligible_symbols <= 0 {
            0.0
        } else {
            self.covered_symbols.max(0) as f64 / self.eligible_symbols as f64
        }
    }
}


#[derive(Debug, Clone)]
pub(crate) struct MainBusinessScoreRow {
    trade_date: NaiveDate,
    symbol: String,
    score: f64,
    amount: Option<f64>,
    circ_mv: Option<f64>,
    total_mv: Option<f64>,
    industry: Option<String>,
}


pub(crate) async fn build_main_business_diagnostics_report_from_request(
    db: &sqlx::PgPool,
    req: &MainBusinessDiagnosticsRequest,
) -> Result<Value, String> {
    let business_type = main_business_diagnostics_business_type(req);
    let universe = main_business_diagnostics_universe_profile(req)?;
    let profiles = main_business_diagnostics_profiles(req)?;
    let start = parse_feature_profile_readiness_date(&req.start_date, "start_date")?;
    let end = parse_feature_profile_readiness_date(&req.end_date, "end_date")?;
    if start > end {
        return Err("main_business diagnostics start_date cannot be after end_date".into());
    }
    let thresholds = main_business_diagnostics_thresholds(req);
    let min_daily_coverage_ratio = main_business_daily_coverage_ratio_threshold(req);
    let research_options = main_business_research_diagnostics_options(req);
    let report = build_main_business_diagnostics_report(
        db,
        &business_type,
        universe,
        &profiles,
        start,
        end,
        thresholds,
        min_daily_coverage_ratio,
        research_options,
    )
    .await?;
    let experiment_run_id = if main_business_diagnostics_persist_default(req.persist_report) {
        Some(
            persist_main_business_diagnostics_report(
                db,
                &business_type,
                universe,
                &profiles,
                &report,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}


pub(crate) async fn build_main_business_diagnostics_report(
    db: &sqlx::PgPool,
    business_type: &str,
    universe: MainBusinessDiagnosticsUniverse,
    profiles: &[MainBusinessDiagnosticsProfile],
    start: NaiveDate,
    end: NaiveDate,
    thresholds: ReadinessThresholds,
    min_daily_coverage_ratio: f64,
    research_options: AlphaSourceResearchDiagnosticsOptions,
) -> Result<Value, String> {
    let raw_summary = load_main_business_raw_summary(db, business_type).await?;
    let daily_coverage =
        load_main_business_daily_coverage(db, business_type, universe, start, end).await?;
    let expected_days = readiness_expected_open_day_count(db, start, end).await?;
    let effective_start = daily_coverage
        .iter()
        .find(|row| {
            row.covered_symbols >= thresholds.min_daily_rows
                && row.coverage_ratio() >= min_daily_coverage_ratio
        })
        .map(|row| row.trade_date);
    let effective_coverage = daily_coverage
        .iter()
        .filter(|row| {
            effective_start
                .map(|date| row.trade_date >= date)
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    let effective_expected_days = match effective_start {
        Some(effective_start) => {
            readiness_expected_open_day_count(db, effective_start, end).await?
        }
        None => 0,
    };
    let effective_counts = effective_coverage
        .iter()
        .filter(|row| row.covered_symbols > 0)
        .map(|row| row.covered_symbols)
        .collect::<Vec<_>>();
    let effective_daily_rows = effective_coverage
        .iter()
        .filter(|row| row.covered_symbols > 0)
        .map(|row| (row.trade_date, row.covered_symbols))
        .collect::<Vec<_>>();
    let distribution = daily_count_distribution(&effective_counts, thresholds);
    let effective_covered_days = effective_daily_rows.len() as i64;
    let effective_day_coverage_ratio = if effective_expected_days <= 0 {
        0.0
    } else {
        effective_covered_days as f64 / effective_expected_days as f64
    };
    let daily_coverage_ratios = effective_coverage
        .iter()
        .map(MainBusinessDailyCoverageRow::coverage_ratio)
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    let sorted_daily_coverage_ratios = sorted_finite(daily_coverage_ratios.iter().copied());
    let median_daily_coverage_ratio = percentile(&sorted_daily_coverage_ratios, 0.50);
    let min_daily_coverage_ratio_actual =
        sorted_daily_coverage_ratios.first().copied().unwrap_or(0.0);
    let prefix_without_snapshot_days = daily_coverage
        .iter()
        .filter(|row| {
            effective_start
                .map(|date| row.trade_date < date)
                .unwrap_or(true)
        })
        .count() as i64;
    let gates = vec![
        profile_readiness_gate(
            "main_business_raw_rows",
            raw_summary.raw_rows > 0,
            json!(raw_summary.raw_rows),
            json!("> 0"),
            "market_stock_main_business must contain PIT-mapped raw rows before diagnostics",
        ),
        profile_readiness_gate(
            "main_business_raw_pit_contract",
            raw_summary.pit_violation_rows == 0,
            json!(raw_summary.pit_violation_rows),
            json!(0),
            "market_stock_main_business.available_at must be on or after end_date",
        ),
        profile_readiness_gate(
            "main_business_period_universe_clean",
            raw_summary.out_of_market_stock_rows == 0
                && raw_summary.period_universe_mismatch_rows == 0,
            json!({
                "out_of_market_stock_rows": raw_summary.out_of_market_stock_rows,
                "period_universe_mismatch_rows": raw_summary.period_universe_mismatch_rows,
            }),
            json!({"out_of_market_stock_rows": 0, "period_universe_mismatch_rows": 0}),
            "raw rows must not include non-market_stock symbols or pre-listing/post-delist report periods",
        ),
        profile_readiness_gate(
            "main_business_effective_snapshot_start",
            effective_start.is_some(),
            json!(effective_start),
            json!("first date with enough covered symbols and coverage ratio"),
            "diagnostics use the first stable PIT snapshot date instead of treating the pre-disclosure prefix as tradable coverage",
        ),
        profile_readiness_gate(
            "main_business_effective_day_coverage",
            effective_day_coverage_ratio >= thresholds.min_day_coverage_ratio,
            json!(effective_day_coverage_ratio),
            json!(thresholds.min_day_coverage_ratio),
            "after effective_start, PIT snapshots must cover enough expected open days",
        ),
        profile_readiness_gate(
            "main_business_daily_median_symbols",
            distribution.p50_rows >= thresholds.min_daily_rows,
            json!(distribution.p50_rows),
            json!(thresholds.min_daily_rows),
            "daily PIT snapshots must have enough cross-sectional breadth for RankIC/group-return diagnostics",
        ),
        profile_readiness_gate(
            "main_business_daily_median_coverage_ratio",
            median_daily_coverage_ratio >= min_daily_coverage_ratio,
            json!(median_daily_coverage_ratio),
            json!(min_daily_coverage_ratio),
            "covered symbols should be a stable share of the listed non-ST universe",
        ),
    ];
    let passed = alpha_source_diagnostics_gates_passed(&gates);
    let research_metrics = build_main_business_research_diagnostics(
        db,
        business_type,
        universe,
        profiles,
        thresholds,
        &effective_daily_rows,
        &research_options,
    )
    .await?;

    Ok(json!({
        "diagnostics_type": "main_business_source",
        "source": "tushare:fina_mainbz_vip",
        "business_type": business_type,
        "universe_profile": universe.as_str(),
        "requested_start_date": start,
        "requested_end_date": end,
        "effective_start_date": effective_start,
        "effective_end_date": if effective_start.is_some() { Some(end) } else { None },
        "passed": passed,
        "level": alpha_source_diagnostics_level(passed),
        "thresholds": {
            "min_day_coverage_ratio": thresholds.min_day_coverage_ratio,
            "min_daily_rows": thresholds.min_daily_rows,
            "min_p95_daily_row_ratio": thresholds.min_p95_daily_row_ratio,
            "min_daily_coverage_ratio": min_daily_coverage_ratio,
        },
        "summary": {
            "raw_rows": raw_summary.raw_rows,
            "raw_symbols": raw_summary.raw_symbols,
            "report_periods": raw_summary.report_periods,
            "pit_violation_rows": raw_summary.pit_violation_rows,
            "out_of_market_stock_rows": raw_summary.out_of_market_stock_rows,
            "period_universe_mismatch_rows": raw_summary.period_universe_mismatch_rows,
            "first_end_date": raw_summary.first_end_date,
            "last_end_date": raw_summary.last_end_date,
            "first_available_at": raw_summary.first_available_at,
            "last_available_at": raw_summary.last_available_at,
            "requested_open_days": expected_days,
            "prefix_without_stable_snapshot_days": prefix_without_snapshot_days,
            "effective_expected_open_days": effective_expected_days,
            "effective_covered_days": effective_covered_days,
            "effective_missing_open_days": (effective_expected_days - effective_covered_days).max(0),
            "effective_day_coverage_ratio": effective_day_coverage_ratio,
            "daily_symbols": {
                "min": distribution.min_rows,
                "p50": distribution.p50_rows,
                "p95": distribution.p95_rows,
                "max": distribution.max_rows,
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
            },
            "daily_universe_coverage_ratio": {
                "min": min_daily_coverage_ratio_actual,
                "p50": median_daily_coverage_ratio,
                "p95": percentile(&sorted_daily_coverage_ratios, 0.95),
            },
        },
        "profiles": profiles.iter().map(|profile| json!({
            "profile": profile.as_str(),
            "label": profile.label(),
        })).collect::<Vec<_>>(),
        "gates": gates,
        "research_metrics": research_metrics,
        "scope": {
            "point_in_time": "market_stock_main_business.available_at <= trade_date; report values are never joined by end_date alone",
            "raw_source_policy": "uses only period-universe-clean raw rows already persisted in market_stock_main_business",
            "research_stage": "coverage_readiness_rankic_group_decay_turnover_capacity_exposure_regime",
            "not_a_factor_backfill": true,
            "does_not_write_multi_factor_value": true,
            "forward_return_labels": "future returns are used only as diagnostic labels and are not persisted to factor/model inputs",
            "universe": universe.description(),
        },
        "repair": {
            "repairable": false,
            "reason": "negative economics or insufficient effective coverage must stop this source; do not repair by using pre-listing data, static backfill, sign flip, OOS reverse tuning, or v19 train-selection admission"
        }
    }))
}


pub(crate) async fn load_main_business_raw_summary(
    db: &sqlx::PgPool,
    business_type: &str,
) -> Result<MainBusinessRawSummary, String> {
    let row = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
        ),
    >(
        "SELECT
             COUNT(*)::int8 AS raw_rows,
             COUNT(DISTINCT mb.symbol)::int8 AS raw_symbols,
             COUNT(DISTINCT mb.end_date)::int8 AS report_periods,
             COUNT(*) FILTER (WHERE mb.available_at < mb.end_date)::int8 AS pit_violation_rows,
             COUNT(*) FILTER (WHERE ms.symbol IS NULL)::int8 AS out_of_market_stock_rows,
             COUNT(*) FILTER (
               WHERE ms.symbol IS NOT NULL
                 AND (ms.list_date > mb.end_date OR (ms.delist_date IS NOT NULL AND ms.delist_date < mb.end_date))
             )::int8 AS period_universe_mismatch_rows,
             MIN(mb.end_date) AS first_end_date,
             MAX(mb.end_date) AS last_end_date,
             MIN(mb.available_at) AS first_available_at,
             MAX(mb.available_at) AS last_available_at
         FROM market_stock_main_business mb
         LEFT JOIN market_stock ms ON ms.symbol = mb.symbol
         WHERE mb.business_type = $1",
    )
    .bind(business_type)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to load main_business raw summary: {error}"))?;
    Ok(MainBusinessRawSummary {
        raw_rows: row.0,
        raw_symbols: row.1,
        report_periods: row.2,
        pit_violation_rows: row.3,
        out_of_market_stock_rows: row.4,
        period_universe_mismatch_rows: row.5,
        first_end_date: row.6,
        last_end_date: row.7,
        first_available_at: row.8,
        last_available_at: row.9,
    })
}


pub(crate) fn main_business_daily_coverage_sql(universe: MainBusinessDiagnosticsUniverse) -> String {
    format!(
        "WITH reports AS (
             SELECT symbol, end_date, MIN(available_at) AS available_at
             FROM market_stock_main_business
             WHERE business_type = $1
             GROUP BY symbol, end_date
         ),
         intervals AS (
             SELECT symbol,
                    end_date,
                    available_at,
                    LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at, end_date) AS next_available_at
             FROM reports
         ),
         days AS (
             SELECT trade_date
             FROM market_trade_calendar
             WHERE exchange = 'SSE'
               AND is_open = true
               AND trade_date >= $2
               AND trade_date <= $3
         )
         SELECT d.trade_date,
                COUNT(DISTINCT db.symbol) FILTER (WHERE ms.symbol IS NOT NULL)::int8 AS eligible_symbols,
                COUNT(DISTINCT i.symbol) FILTER (WHERE ms.symbol IS NOT NULL)::int8 AS covered_symbols
         FROM days d
         LEFT JOIN market_stock_daily_bar db
           ON db.trade_date = d.trade_date
         LEFT JOIN market_stock ms
           ON ms.symbol = db.symbol
          AND ms.list_date <= d.trade_date
          AND (ms.delist_date IS NULL OR ms.delist_date >= d.trade_date)
          AND COALESCE(ms.is_st, false) = false
          {market_filter}
         LEFT JOIN intervals i
           ON i.symbol = db.symbol
          AND i.available_at <= d.trade_date
          AND (i.next_available_at IS NULL OR i.next_available_at > d.trade_date)
         GROUP BY d.trade_date
         ORDER BY d.trade_date",
        market_filter = universe.market_filter_sql()
    )
}


pub(crate) async fn load_main_business_daily_coverage(
    db: &sqlx::PgPool,
    business_type: &str,
    universe: MainBusinessDiagnosticsUniverse,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<MainBusinessDailyCoverageRow>, String> {
    let sql = main_business_daily_coverage_sql(universe);
    let rows = sqlx::query_as::<_, (NaiveDate, i64, i64)>(&sql)
        .bind(business_type)
        .bind(start)
        .bind(end)
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load main_business daily coverage: {error}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(trade_date, eligible_symbols, covered_symbols)| MainBusinessDailyCoverageRow {
                trade_date,
                eligible_symbols,
                covered_symbols,
            },
        )
        .collect())
}


pub(crate) async fn build_main_business_research_diagnostics(
    db: &sqlx::PgPool,
    business_type: &str,
    universe: MainBusinessDiagnosticsUniverse,
    profiles: &[MainBusinessDiagnosticsProfile],
    thresholds: ReadinessThresholds,
    daily_rows: &[(NaiveDate, i64)],
    options: &AlphaSourceResearchDiagnosticsOptions,
) -> Result<Value, String> {
    if daily_rows.is_empty() {
        return Ok(json!({
            "included": false,
            "reason": "no effective PIT main_business daily snapshots passed coverage/readiness gates",
        }));
    }
    let min_daily_sample_size = thresholds.min_daily_rows.max(100);
    let sampled_trade_dates =
        sample_alpha_source_research_days(daily_rows, options.max_rank_ic_days);
    let exposure_regime_trade_dates =
        sample_alpha_source_research_days(daily_rows, options.max_exposure_regime_days);
    let regime_split_trade_dates =
        sample_alpha_source_regime_days(&sampled_trade_dates, &exposure_regime_trade_dates);
    let market_regimes = if options.include_exposure_regime_metrics {
        load_alpha_source_market_regimes(db, &regime_split_trade_dates, 60).await?
    } else {
        BTreeMap::new()
    };
    let exposure_regime_trade_date_set = exposure_regime_trade_dates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let exposure_market_regimes = market_regimes
        .iter()
        .filter(|(trade_date, _)| exposure_regime_trade_date_set.contains(trade_date))
        .map(|(trade_date, regime)| (*trade_date, regime.clone()))
        .collect::<BTreeMap<_, _>>();
    let all_score_dates = sampled_trade_dates
        .iter()
        .chain(exposure_regime_trade_dates.iter())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut profile_reports = Vec::new();
    for profile in profiles {
        let score_rows =
            load_main_business_score_rows(db, business_type, universe, *profile, &all_score_dates)
                .await?;
        let mut rank_ic_by_horizon = Vec::new();
        let mut group_return_by_horizon = Vec::new();
        let mut turnover_capacity_by_horizon = Vec::new();
        let mut regime_split_by_horizon = Vec::new();
        let mut decay_curve = Vec::new();
        for horizon_days in &options.return_horizons {
            let labeled_rows = label_main_business_score_rows(
                db,
                &score_rows,
                &sampled_trade_dates,
                *horizon_days,
            )
            .await?;
            let rank_ic_rows = daily_rank_ic_from_labeled_rows(
                *horizon_days,
                &labeled_rows,
                min_daily_sample_size,
            );
            let rank_ic_summary = summarize_rank_ic_samples(*horizon_days, &rank_ic_rows);
            let bucket_rows = group_return_rows_from_labeled_rows(
                &labeled_rows,
                min_daily_sample_size,
                options.bucket_count,
            );
            let group_summary =
                summarize_group_return_buckets(*horizon_days, options.bucket_count, &bucket_rows);
            let turnover_capacity_summary = turnover_capacity_summary_from_labeled_rows(
                *horizon_days,
                &labeled_rows,
                min_daily_sample_size,
                options.bucket_count,
            );
            if options.include_exposure_regime_metrics {
                regime_split_by_horizon.push(json!({
                    "horizon_days": horizon_days,
                    "regimes": regime_split_summaries_from_labeled_rows(
                        *horizon_days,
                        &labeled_rows,
                        &market_regimes,
                        min_daily_sample_size,
                        options.bucket_count,
                    )
                    .iter()
                    .map(AlphaSourceRegimeSplitSummary::to_json)
                    .collect::<Vec<_>>()
                }));
            }
            decay_curve.push(json!({
                "horizon_days": horizon_days,
                "mean_rank_ic": rank_ic_summary.mean_rank_ic,
                "median_rank_ic": rank_ic_summary.median_rank_ic,
                "high_minus_low_spread": group_summary.high_minus_low_spread,
                "sampled_days": rank_ic_summary.sampled_days.min(turnover_capacity_summary.sampled_days),
            }));
            rank_ic_by_horizon.push(rank_ic_summary.to_json());
            group_return_by_horizon.push(group_summary.to_json());
            turnover_capacity_by_horizon.push(turnover_capacity_summary.to_json());
        }
        let exposure_regime_metrics = if options.include_exposure_regime_metrics {
            let exposure_rows = score_rows
                .iter()
                .filter(|row| exposure_regime_trade_date_set.contains(&row.trade_date))
                .map(|row| AlphaSourceExposureRow {
                    trade_date: row.trade_date,
                    score: row.score,
                    amount: row.amount,
                    circ_mv: row.circ_mv,
                    total_mv: row.total_mv,
                    industry: row.industry.clone(),
                })
                .collect::<Vec<_>>();
            let exposure_summary = high_bucket_exposure_summary_from_rows(
                &exposure_rows,
                options.bucket_count,
                min_daily_sample_size,
            );
            json!({
                "included": true,
                "research_only": true,
                "high_score_bucket_exposure": exposure_summary.to_json(),
                "market_regime_distribution": market_regime_distribution(&exposure_market_regimes),
                "market_regime_samples": exposure_market_regimes.values().map(AlphaSourceMarketRegime::to_json).collect::<Vec<_>>(),
                "regime_split_by_horizon": regime_split_by_horizon,
                "input_policy": {
                    "exposure": "same-day amount/circ_mv/total_mv are diagnostic descriptors; market_stock.industry is current/static and not promoted to PIT factor input",
                    "regime": "regime labels use only index bars with trade_date <= sample trade_date",
                    "forward_return_labels": "future returns are post-hoc diagnostics labels only"
                }
            })
        } else {
            json!({
                "included": false,
                "reason": "set include_exposure_regime_metrics=true to include P3.10D exposure/regime diagnostics"
            })
        };
        profile_reports.push(json!({
            "profile": profile.as_str(),
            "label": profile.label(),
            "score_policy": "pre-registered fixed transformation over the latest PIT main_business report; no OOS sign flip or weight tuning",
            "score_rows": score_rows.len(),
            "rank_ic_by_horizon": rank_ic_by_horizon,
            "group_return_by_horizon": group_return_by_horizon,
            "decay_curve": decay_curve,
            "turnover_capacity_by_horizon": turnover_capacity_by_horizon,
            "exposure_regime_metrics": exposure_regime_metrics,
        }));
    }
    Ok(json!({
        "included": true,
        "research_only": true,
        "label_policy": "Forward returns compound market_stock_daily_bar.pct_change over the next N SSE open days for diagnostics only; labels are not persisted to factor/model inputs.",
        "options": {
            "return_horizons": options.return_horizons,
            "bucket_count": options.bucket_count,
            "max_rank_ic_days": options.max_rank_ic_days,
            "include_exposure_regime_metrics": options.include_exposure_regime_metrics,
            "max_exposure_regime_days": options.max_exposure_regime_days,
            "sampled_trade_dates": sampled_trade_dates,
            "exposure_regime_sampled_trade_dates": exposure_regime_trade_dates,
            "min_daily_sample_size": min_daily_sample_size,
            "sampling": "evenly spaced eligible PIT main_business days within the effective range",
            "forward_calendar": "market_trade_calendar exchange=SSE, is_open=true",
        },
        "profiles": profile_reports,
    }))
}


pub(crate) fn main_business_score_rows_sql(
    profile: MainBusinessDiagnosticsProfile,
    universe: MainBusinessDiagnosticsUniverse,
) -> String {
    let score_expression = profile.score_expression_sql();
    format!(
        "WITH report_base AS (
             SELECT symbol,
                    end_date,
                    MIN(available_at) AS available_at,
                    SUM(bz_sales) AS sales,
                    SUM(bz_profit) AS profit,
                    CASE
                      WHEN SUM(GREATEST(COALESCE(bz_sales, 0), 0)) > 0
                      THEN SUM(POWER(GREATEST(COALESCE(bz_sales, 0), 0), 2))
                           / POWER(SUM(GREATEST(COALESCE(bz_sales, 0), 0)), 2)
                    END AS sales_hhi
             FROM market_stock_main_business
             WHERE business_type = $1
             GROUP BY symbol, end_date
         ),
         report_metrics AS (
             SELECT *,
                    CASE
                      WHEN EXTRACT(MONTH FROM end_date) = 6 THEN 'H1'
                      WHEN EXTRACT(MONTH FROM end_date) = 12 THEN 'FY'
                      ELSE TO_CHAR(end_date, 'MMDD')
                    END AS period_bucket,
                    LAG(sales) OVER (
                      PARTITION BY symbol,
                        CASE
                          WHEN EXTRACT(MONTH FROM end_date) = 6 THEN 'H1'
                          WHEN EXTRACT(MONTH FROM end_date) = 12 THEN 'FY'
                          ELSE TO_CHAR(end_date, 'MMDD')
                        END
                      ORDER BY end_date
                    ) AS prev_sales,
                    LAG(profit) OVER (
                      PARTITION BY symbol,
                        CASE
                          WHEN EXTRACT(MONTH FROM end_date) = 6 THEN 'H1'
                          WHEN EXTRACT(MONTH FROM end_date) = 12 THEN 'FY'
                          ELSE TO_CHAR(end_date, 'MMDD')
                        END
                      ORDER BY end_date
                    ) AS prev_profit
             FROM report_base
         ),
         intervals AS (
             SELECT *,
                    LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at, end_date) AS next_available_at
             FROM report_metrics
         ),
         scored AS (
             SELECT sample.trade_date,
                    db.symbol,
                    ({score_expression})::float8 AS score,
                    db.amount::float8 AS amount,
                    basic.circ_mv::float8 AS circ_mv,
                    basic.total_mv::float8 AS total_mv,
                    ms.industry
             FROM unnest($2::date[]) AS sample(trade_date)
             JOIN market_stock_daily_bar db
               ON db.trade_date = sample.trade_date
             JOIN market_stock ms
               ON ms.symbol = db.symbol
              AND ms.list_date <= sample.trade_date
              AND (ms.delist_date IS NULL OR ms.delist_date >= sample.trade_date)
              AND COALESCE(ms.is_st, false) = false
              {market_filter}
             JOIN intervals
               ON intervals.symbol = db.symbol
              AND intervals.available_at <= sample.trade_date
              AND (intervals.next_available_at IS NULL OR intervals.next_available_at > sample.trade_date)
             LEFT JOIN market_stock_daily_basic basic
               ON basic.trade_date = sample.trade_date
              AND basic.symbol = db.symbol
         )
         SELECT trade_date, symbol, score, amount, circ_mv, total_mv, industry
         FROM scored
         WHERE score IS NOT NULL
         ORDER BY trade_date, symbol",
        market_filter = universe.market_filter_sql()
    )
}


pub(crate) async fn load_main_business_score_rows(
    db: &sqlx::PgPool,
    business_type: &str,
    universe: MainBusinessDiagnosticsUniverse,
    profile: MainBusinessDiagnosticsProfile,
    sampled_trade_dates: &[NaiveDate],
) -> Result<Vec<MainBusinessScoreRow>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    let sql = main_business_score_rows_sql(profile, universe);
    let rows = sqlx::query_as::<
        _,
        (
            NaiveDate,
            String,
            f64,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<String>,
        ),
    >(&sql)
    .bind(business_type)
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load main_business {} score rows: {}",
            profile.as_str(),
            error
        )
    })?;
    Ok(rows
        .into_iter()
        .filter(|(_, _, score, _, _, _, _)| score.is_finite())
        .map(
            |(trade_date, symbol, score, amount, circ_mv, total_mv, industry)| {
                MainBusinessScoreRow {
                    trade_date,
                    symbol,
                    score,
                    amount,
                    circ_mv,
                    total_mv,
                    industry,
                }
            },
        )
        .collect())
}


pub(crate) async fn label_main_business_score_rows(
    db: &sqlx::PgPool,
    score_rows: &[MainBusinessScoreRow],
    sampled_trade_dates: &[NaiveDate],
    horizon_days: i64,
) -> Result<Vec<AlphaSourceLabeledRow>, String> {
    if score_rows.is_empty() || sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    let min_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .min()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let max_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .max()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let calendar_end = max_sample_date + Duration::days((horizon_days.max(1) * 5 + 30).min(1400));
    let open_days = sqlx::query_scalar::<_, NaiveDate>(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE exchange = 'SSE'
           AND is_open = true
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(min_sample_date)
    .bind(calendar_end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load main_business diagnostics calendar: {error}"))?;
    let open_day_index = open_days
        .iter()
        .enumerate()
        .map(|(idx, trade_date)| (*trade_date, idx))
        .collect::<BTreeMap<_, _>>();
    let mut future_dates_by_sample = BTreeMap::<NaiveDate, Vec<NaiveDate>>::new();
    let mut needed_bar_dates = BTreeSet::<NaiveDate>::new();
    for sample_date in sampled_trade_dates {
        let Some(start_idx) = open_day_index.get(sample_date).copied() else {
            continue;
        };
        let end_idx = start_idx + horizon_days.max(1) as usize;
        if end_idx >= open_days.len() {
            continue;
        }
        let future_dates = open_days[(start_idx + 1)..=end_idx].to_vec();
        for future_date in &future_dates {
            needed_bar_dates.insert(*future_date);
        }
        future_dates_by_sample.insert(*sample_date, future_dates);
    }
    if future_dates_by_sample.is_empty() {
        return Ok(Vec::new());
    }
    let needed_bar_dates = needed_bar_dates.into_iter().collect::<Vec<_>>();
    let bar_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>)>(
        "SELECT trade_date, symbol, pct_change::float8
         FROM market_stock_daily_bar
         WHERE trade_date = ANY($1::date[])",
    )
    .bind(&needed_bar_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load main_business diagnostics return rows: {error}"))?;
    let bar_by_key = bar_rows
        .into_iter()
        .map(|(trade_date, symbol, pct_change)| ((trade_date, symbol), pct_change))
        .collect::<HashMap<_, _>>();
    let sampled_trade_date_set = sampled_trade_dates.iter().copied().collect::<BTreeSet<_>>();
    let mut labeled_rows = Vec::new();
    for row in score_rows
        .iter()
        .filter(|row| sampled_trade_date_set.contains(&row.trade_date))
    {
        let Some(future_dates) = future_dates_by_sample.get(&row.trade_date) else {
            continue;
        };
        let mut compounded = 1.0;
        let mut complete = true;
        for future_date in future_dates {
            let Some(Some(pct_change)) = bar_by_key.get(&(*future_date, row.symbol.clone())) else {
                complete = false;
                break;
            };
            if !pct_change.is_finite() || *pct_change <= -0.999999 {
                complete = false;
                break;
            }
            compounded *= 1.0 + *pct_change;
        }
        if !complete {
            continue;
        }
        labeled_rows.push(AlphaSourceLabeledRow {
            trade_date: row.trade_date,
            symbol: row.symbol.clone(),
            score: row.score,
            forward_return: compounded - 1.0,
            amount: row.amount,
            circ_mv: row.circ_mv,
        });
    }
    Ok(labeled_rows)
}


pub(crate) fn validate_alpha_source_diagnostics_admission(
    req: &AlphaSourceDiagnosticsRequest,
    combo_name: &str,
) -> Result<(), String> {
    validate_industry_prosperity_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )?;
    validate_futures_price_chain_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )?;
    validate_equity_pledge_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )?;
    validate_shareholder_structure_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )?;
    validate_margin_detail_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )?;
    validate_analyst_revision_entrypoint_admission(
        combo_name,
        req.alpha_admission_gate_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        req.universe_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        "P3.10 diagnostics",
    )
}


#[derive(Debug)]
pub(crate) struct AlphaSourceDiagnosticsSummary {
    usable_rows: i64,
    usable_symbols: i64,
    null_score_rows: i64,
    null_available_at_rows: i64,
    future_leak_rows: i64,
    first_trade_date: Option<NaiveDate>,
    last_trade_date: Option<NaiveDate>,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceDailyRankIc {
    pub(crate) trade_date: NaiveDate,
    pub(crate) rank_ic: f64,
    pub(crate) sample_size: i64,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceRankIcSummary {
    pub(crate) horizon_days: i64,
    pub(crate) sampled_days: i64,
    pub(crate) first_sample_date: Option<NaiveDate>,
    pub(crate) last_sample_date: Option<NaiveDate>,
    pub(crate) mean_rank_ic: f64,
    pub(crate) median_rank_ic: f64,
    pub(crate) p05_rank_ic: f64,
    pub(crate) p95_rank_ic: f64,
    pub(crate) positive_day_ratio: f64,
    pub(crate) min_daily_sample_size: i64,
    pub(crate) max_daily_sample_size: i64,
}


impl AlphaSourceRankIcSummary {
    fn to_json(&self) -> Value {
        json!({
            "horizon_days": self.horizon_days,
            "sampled_days": self.sampled_days,
            "first_sample_date": self.first_sample_date,
            "last_sample_date": self.last_sample_date,
            "mean_rank_ic": self.mean_rank_ic,
            "median_rank_ic": self.median_rank_ic,
            "p05_rank_ic": self.p05_rank_ic,
            "p95_rank_ic": self.p95_rank_ic,
            "positive_day_ratio": self.positive_day_ratio,
            "min_daily_sample_size": self.min_daily_sample_size,
            "max_daily_sample_size": self.max_daily_sample_size,
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceBucketReturn {
    pub(crate) bucket: i64,
    pub(crate) avg_forward_return: f64,
    pub(crate) sample_count: i64,
}


impl AlphaSourceBucketReturn {
    fn to_json(&self) -> Value {
        json!({
            "bucket": self.bucket,
            "avg_forward_return": self.avg_forward_return,
            "sample_count": self.sample_count,
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceScoreRow {
    trade_date: NaiveDate,
    symbol: String,
    score: f64,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceLabeledRow {
    pub(crate) trade_date: NaiveDate,
    pub(crate) symbol: String,
    pub(crate) score: f64,
    pub(crate) forward_return: f64,
    pub(crate) amount: Option<f64>,
    pub(crate) circ_mv: Option<f64>,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceExposureRow {
    pub(crate) trade_date: NaiveDate,
    pub(crate) score: f64,
    pub(crate) amount: Option<f64>,
    pub(crate) circ_mv: Option<f64>,
    pub(crate) total_mv: Option<f64>,
    pub(crate) industry: Option<String>,
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceGroupReturnSummary {
    pub(crate) horizon_days: i64,
    pub(crate) bucket_count: i64,
    pub(crate) low_score_bucket_avg_return: f64,
    pub(crate) high_score_bucket_avg_return: f64,
    pub(crate) high_minus_low_spread: f64,
    pub(crate) monotonicity_score: f64,
    pub(crate) total_sample_count: i64,
    pub(crate) buckets: Vec<AlphaSourceBucketReturn>,
}


impl AlphaSourceGroupReturnSummary {
    fn to_json(&self) -> Value {
        json!({
            "horizon_days": self.horizon_days,
            "bucket_count": self.bucket_count,
            "low_score_bucket_avg_return": self.low_score_bucket_avg_return,
            "high_score_bucket_avg_return": self.high_score_bucket_avg_return,
            "high_minus_low_spread": self.high_minus_low_spread,
            "monotonicity_score": self.monotonicity_score,
            "total_sample_count": self.total_sample_count,
            "buckets": self.buckets.iter().map(AlphaSourceBucketReturn::to_json).collect::<Vec<_>>(),
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceIndustryExposure {
    pub(crate) industry: String,
    pub(crate) avg_weight: f64,
    pub(crate) max_daily_weight: f64,
    pub(crate) active_days: i64,
}


impl AlphaSourceIndustryExposure {
    fn to_json(&self) -> Value {
        json!({
            "industry": self.industry,
            "avg_weight": self.avg_weight,
            "max_daily_weight": self.max_daily_weight,
            "active_days": self.active_days,
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceHighBucketExposureSummary {
    pub(crate) sampled_days: i64,
    pub(crate) avg_high_score_bucket_symbols: f64,
    pub(crate) avg_high_bucket_industry_hhi: f64,
    pub(crate) max_single_industry_weight: f64,
    pub(crate) top_industries: Vec<AlphaSourceIndustryExposure>,
    pub(crate) median_high_vs_universe_amount_ratio: f64,
    pub(crate) median_high_vs_universe_circ_mv_ratio: f64,
    pub(crate) median_high_vs_universe_total_mv_ratio: f64,
    pub(crate) high_bucket_amount_missing_ratio: f64,
    pub(crate) high_bucket_circ_mv_missing_ratio: f64,
    pub(crate) high_bucket_industry_missing_ratio: f64,
}


impl AlphaSourceHighBucketExposureSummary {
    fn to_json(&self) -> Value {
        json!({
            "sampled_days": self.sampled_days,
            "avg_high_score_bucket_symbols": self.avg_high_score_bucket_symbols,
            "avg_high_bucket_industry_hhi": self.avg_high_bucket_industry_hhi,
            "max_single_industry_weight": self.max_single_industry_weight,
            "top_industries": self.top_industries.iter().map(AlphaSourceIndustryExposure::to_json).collect::<Vec<_>>(),
            "median_high_vs_universe_amount_ratio": self.median_high_vs_universe_amount_ratio,
            "median_high_vs_universe_circ_mv_ratio": self.median_high_vs_universe_circ_mv_ratio,
            "median_high_vs_universe_total_mv_ratio": self.median_high_vs_universe_total_mv_ratio,
            "high_bucket_amount_missing_ratio": self.high_bucket_amount_missing_ratio,
            "high_bucket_circ_mv_missing_ratio": self.high_bucket_circ_mv_missing_ratio,
            "high_bucket_industry_missing_ratio": self.high_bucket_industry_missing_ratio,
            "units": {
                "amount": "market_stock_daily_bar.amount, raw database unit",
                "circ_mv": "market_stock_daily_basic.circ_mv, raw database unit",
                "total_mv": "market_stock_daily_basic.total_mv, raw database unit"
            },
            "industry_scope": "market_stock.industry is a current/static classification snapshot; use for research diagnostics only, not as a strict historical PIT industry feature"
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceMarketRegime {
    pub(crate) trade_date: NaiveDate,
    pub(crate) regime: String,
    pub(crate) trailing_return: f64,
    pub(crate) annualized_volatility: f64,
    pub(crate) trailing_max_drawdown: f64,
}


impl AlphaSourceMarketRegime {
    fn to_json(&self) -> Value {
        json!({
            "trade_date": self.trade_date,
            "regime": self.regime,
            "trailing_return": self.trailing_return,
            "annualized_volatility": self.annualized_volatility,
            "trailing_max_drawdown": self.trailing_max_drawdown,
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceRegimeSplitSummary {
    pub(crate) horizon_days: i64,
    pub(crate) regime: String,
    pub(crate) sampled_days: i64,
    pub(crate) labeled_rows: i64,
    pub(crate) avg_forward_return: f64,
    pub(crate) rank_ic: AlphaSourceRankIcSummary,
    pub(crate) group_return: AlphaSourceGroupReturnSummary,
}


impl AlphaSourceRegimeSplitSummary {
    fn to_json(&self) -> Value {
        json!({
            "horizon_days": self.horizon_days,
            "regime": self.regime,
            "sampled_days": self.sampled_days,
            "labeled_rows": self.labeled_rows,
            "avg_forward_return": self.avg_forward_return,
            "rank_ic": self.rank_ic.to_json(),
            "group_return": self.group_return.to_json(),
        })
    }
}


#[derive(Debug, Clone)]
pub(crate) struct AlphaSourceTurnoverCapacitySummary {
    horizon_days: i64,
    sampled_days: i64,
    avg_high_score_bucket_symbols: f64,
    avg_high_score_bucket_turnover: f64,
    median_high_score_bucket_turnover: f64,
    avg_high_score_bucket_amount: f64,
    median_high_score_bucket_amount: f64,
    p10_high_score_bucket_amount: f64,
    avg_high_score_bucket_circ_mv: f64,
    median_high_score_bucket_circ_mv: f64,
    p10_high_score_bucket_circ_mv: f64,
}


impl AlphaSourceTurnoverCapacitySummary {
    fn to_json(&self) -> Value {
        json!({
            "horizon_days": self.horizon_days,
            "sampled_days": self.sampled_days,
            "avg_high_score_bucket_symbols": self.avg_high_score_bucket_symbols,
            "avg_high_score_bucket_turnover": self.avg_high_score_bucket_turnover,
            "median_high_score_bucket_turnover": self.median_high_score_bucket_turnover,
            "avg_high_score_bucket_amount": self.avg_high_score_bucket_amount,
            "median_high_score_bucket_amount": self.median_high_score_bucket_amount,
            "p10_high_score_bucket_amount": self.p10_high_score_bucket_amount,
            "avg_high_score_bucket_circ_mv": self.avg_high_score_bucket_circ_mv,
            "median_high_score_bucket_circ_mv": self.median_high_score_bucket_circ_mv,
            "p10_high_score_bucket_circ_mv": self.p10_high_score_bucket_circ_mv,
            "capacity_units": {
            "amount": "market_stock_daily_bar.amount, raw database unit",
                "circ_mv": "market_stock_daily_basic.circ_mv, raw database unit"
            }
        })
    }
}


pub(crate) fn summarize_rank_ic_samples(
    horizon_days: i64,
    rows: &[AlphaSourceDailyRankIc],
) -> AlphaSourceRankIcSummary {
    let filtered = rows
        .iter()
        .filter(|row| row.rank_ic.is_finite())
        .collect::<Vec<_>>();
    let mut values = filtered.iter().map(|row| row.rank_ic).collect::<Vec<_>>();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let sampled_days = values.len() as i64;
    let mean_rank_ic = if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    };
    let positive_day_ratio = if values.is_empty() {
        0.0
    } else {
        values.iter().filter(|value| **value > 0.0).count() as f64 / values.len() as f64
    };
    let min_daily_sample_size = filtered
        .iter()
        .map(|row| row.sample_size)
        .min()
        .unwrap_or(0);
    let max_daily_sample_size = filtered
        .iter()
        .map(|row| row.sample_size)
        .max()
        .unwrap_or(0);

    AlphaSourceRankIcSummary {
        horizon_days,
        sampled_days,
        first_sample_date: filtered.iter().map(|row| row.trade_date).min(),
        last_sample_date: filtered.iter().map(|row| row.trade_date).max(),
        mean_rank_ic,
        median_rank_ic: percentile(&values, 0.50),
        p05_rank_ic: percentile(&values, 0.05),
        p95_rank_ic: percentile(&values, 0.95),
        positive_day_ratio,
        min_daily_sample_size,
        max_daily_sample_size,
    }
}


pub(crate) fn summarize_group_return_buckets(
    horizon_days: i64,
    bucket_count: i64,
    buckets: &[AlphaSourceBucketReturn],
) -> AlphaSourceGroupReturnSummary {
    let mut sorted = buckets
        .iter()
        .filter(|bucket| bucket.avg_forward_return.is_finite())
        .cloned()
        .collect::<Vec<_>>();
    sorted.sort_by_key(|bucket| bucket.bucket);

    let low_score_bucket_avg_return = sorted
        .first()
        .map(|bucket| bucket.avg_forward_return)
        .unwrap_or(0.0);
    let high_score_bucket_avg_return = sorted
        .last()
        .map(|bucket| bucket.avg_forward_return)
        .unwrap_or(0.0);
    let adjacent_pairs = sorted.windows(2).collect::<Vec<_>>();
    let monotonicity_score = if adjacent_pairs.is_empty() {
        0.0
    } else {
        adjacent_pairs
            .iter()
            .filter(|pair| pair[1].avg_forward_return >= pair[0].avg_forward_return)
            .count() as f64
            / adjacent_pairs.len() as f64
    };

    AlphaSourceGroupReturnSummary {
        horizon_days,
        bucket_count,
        low_score_bucket_avg_return,
        high_score_bucket_avg_return,
        high_minus_low_spread: high_score_bucket_avg_return - low_score_bucket_avg_return,
        monotonicity_score,
        total_sample_count: sorted.iter().map(|bucket| bucket.sample_count).sum(),
        buckets: sorted,
    }
}


pub(crate) async fn load_alpha_source_diagnostics_summary(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<AlphaSourceDiagnosticsSummary, String> {
    let row = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
        ),
    >(
        "SELECT
             COUNT(*) FILTER (
               WHERE normalized_score IS NOT NULL
                 AND available_at IS NOT NULL
                 AND available_at <= trade_date
             )::int8 AS usable_rows,
             COUNT(DISTINCT symbol) FILTER (
               WHERE normalized_score IS NOT NULL
                 AND available_at IS NOT NULL
                 AND available_at <= trade_date
             )::int8 AS usable_symbols,
             COUNT(*) FILTER (WHERE normalized_score IS NULL)::int8 AS null_score_rows,
             COUNT(*) FILTER (WHERE available_at IS NULL)::int8 AS null_available_at_rows,
             COUNT(*) FILTER (WHERE available_at > trade_date)::int8 AS future_leak_rows,
             MIN(trade_date) FILTER (
               WHERE normalized_score IS NOT NULL
                 AND available_at IS NOT NULL
                 AND available_at <= trade_date
             ) AS first_trade_date,
             MAX(trade_date) FILTER (
               WHERE normalized_score IS NOT NULL
                 AND available_at IS NOT NULL
                 AND available_at <= trade_date
             ) AS last_trade_date
         FROM multi_factor_value
         WHERE combo_name = $1
           AND version = $2
           AND trade_date >= $3
           AND trade_date <= $4",
    )
    .bind(combo_name)
    .bind(version)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to load alpha source diagnostics summary: {error}"))?;

    Ok(AlphaSourceDiagnosticsSummary {
        usable_rows: row.0,
        usable_symbols: row.1,
        null_score_rows: row.2,
        null_available_at_rows: row.3,
        future_leak_rows: row.4,
        first_trade_date: row.5,
        last_trade_date: row.6,
    })
}


pub(crate) async fn load_alpha_source_diagnostics_daily_rows(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDate, i64)>, String> {
    sqlx::query_as::<_, (NaiveDate, i64)>(
        "SELECT trade_date,
                COUNT(*) FILTER (
                  WHERE normalized_score IS NOT NULL
                    AND available_at IS NOT NULL
                    AND available_at <= trade_date
                )::int8 AS symbol_count
         FROM multi_factor_value
         WHERE combo_name = $1
           AND version = $2
           AND trade_date >= $3
           AND trade_date <= $4
         GROUP BY trade_date
         HAVING COUNT(*) FILTER (
                  WHERE normalized_score IS NOT NULL
                    AND available_at IS NOT NULL
                    AND available_at <= trade_date
                ) > 0
         ORDER BY trade_date",
    )
    .bind(combo_name)
    .bind(version)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha source diagnostics daily rows: {error}"))
}


pub(crate) async fn load_main_chinext_non_st_eligible_daily_rows(
    db: &sqlx::PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDate, i64)>, String> {
    sqlx::query_as::<_, (NaiveDate, i64)>(
        "WITH stock_days AS (
             SELECT trade_date
             FROM market_trade_calendar
             WHERE exchange = 'SSE'
               AND is_open = true
               AND trade_date BETWEEN $1 AND $2
         )
         SELECT
             bar.trade_date,
             COUNT(*)::int8 AS eligible_symbols
         FROM stock_days td
         JOIN market_stock_daily_bar bar
           ON bar.trade_date = td.trade_date
          AND bar.trade_date BETWEEN $1 AND $2
         JOIN market_stock ms
           ON ms.symbol = bar.symbol
         JOIN market_stock_daily_basic basic
           ON basic.symbol = bar.symbol
          AND basic.trade_date = bar.trade_date
          AND basic.trade_date BETWEEN $1 AND $2
         WHERE bar.close IS NOT NULL
           AND bar.close > 0
           AND basic.circ_mv IS NOT NULL
           AND basic.circ_mv > 0
           AND ms.list_date IS NOT NULL
           AND ms.list_date <= bar.trade_date
           AND (
               ms.delist_date IS NULL
               OR ms.delist_date >= bar.trade_date
           )
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market IN ('主板', '创业板')
           AND ms.symbol NOT LIKE '688%SH'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
           AND NOT EXISTS (
               SELECT 1
               FROM market_stock_name_history st_name
               WHERE st_name.symbol = bar.symbol
                 AND st_name.is_st = true
                 AND st_name.start_date <= bar.trade_date
                 AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
           )
         GROUP BY bar.trade_date
         ORDER BY bar.trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load main_chinext_non_st eligible daily rows: {error}"))
}


pub(crate) async fn build_alpha_source_research_diagnostics(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    thresholds: ReadinessThresholds,
    daily_rows: &[(NaiveDate, i64)],
    options: &AlphaSourceResearchDiagnosticsOptions,
) -> Result<Value, String> {
    if !options.include_research_metrics {
        return Ok(json!({
            "included": false,
            "reason": "set include_research_metrics=true to run bounded research-only RankIC/group-return/turnover/capacity diagnostics",
            "options": {
                "return_horizons": options.return_horizons,
                "bucket_count": options.bucket_count,
                "max_rank_ic_days": options.max_rank_ic_days,
                "include_exposure_regime_metrics": options.include_exposure_regime_metrics,
                "max_exposure_regime_days": options.max_exposure_regime_days,
            }
        }));
    }

    let min_daily_sample_size = thresholds.min_daily_rows.max(100);
    let sampled_trade_dates =
        sample_alpha_source_research_days(daily_rows, options.max_rank_ic_days);
    let exposure_regime_trade_dates =
        sample_alpha_source_research_days(daily_rows, options.max_exposure_regime_days);
    let regime_split_trade_dates =
        sample_alpha_source_regime_days(&sampled_trade_dates, &exposure_regime_trade_dates);
    let mut rank_ic_by_horizon = Vec::new();
    let mut group_return_by_horizon = Vec::new();
    let mut turnover_capacity_by_horizon = Vec::new();
    let mut regime_split_by_horizon = Vec::new();
    let mut decay_curve = Vec::new();
    let market_regimes = if options.include_exposure_regime_metrics {
        load_alpha_source_market_regimes(db, &regime_split_trade_dates, 60).await?
    } else {
        BTreeMap::new()
    };
    let exposure_regime_trade_date_set = exposure_regime_trade_dates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let exposure_market_regimes = market_regimes
        .iter()
        .filter(|(trade_date, _)| exposure_regime_trade_date_set.contains(trade_date))
        .map(|(trade_date, regime)| (*trade_date, regime.clone()))
        .collect::<BTreeMap<_, _>>();

    for horizon_days in &options.return_horizons {
        let labeled_rows = load_alpha_source_labeled_rows(
            db,
            combo_name,
            version,
            &sampled_trade_dates,
            *horizon_days,
        )
        .await?;
        let rank_ic_rows =
            daily_rank_ic_from_labeled_rows(*horizon_days, &labeled_rows, min_daily_sample_size);
        let rank_ic_summary = summarize_rank_ic_samples(*horizon_days, &rank_ic_rows);

        let bucket_rows = group_return_rows_from_labeled_rows(
            &labeled_rows,
            min_daily_sample_size,
            options.bucket_count,
        );
        let group_summary =
            summarize_group_return_buckets(*horizon_days, options.bucket_count, &bucket_rows);

        let turnover_capacity_summary = turnover_capacity_summary_from_labeled_rows(
            *horizon_days,
            &labeled_rows,
            min_daily_sample_size,
            options.bucket_count,
        );

        if options.include_exposure_regime_metrics {
            regime_split_by_horizon.push(json!({
                "horizon_days": horizon_days,
                "regimes": regime_split_summaries_from_labeled_rows(
                    *horizon_days,
                    &labeled_rows,
                    &market_regimes,
                    min_daily_sample_size,
                    options.bucket_count,
                )
                .iter()
                .map(AlphaSourceRegimeSplitSummary::to_json)
                .collect::<Vec<_>>()
            }));
        }

        decay_curve.push(json!({
            "horizon_days": horizon_days,
            "mean_rank_ic": rank_ic_summary.mean_rank_ic,
            "median_rank_ic": rank_ic_summary.median_rank_ic,
            "high_minus_low_spread": group_summary.high_minus_low_spread,
            "sampled_days": rank_ic_summary.sampled_days.min(turnover_capacity_summary.sampled_days),
        }));
        rank_ic_by_horizon.push(rank_ic_summary.to_json());
        group_return_by_horizon.push(group_summary.to_json());
        turnover_capacity_by_horizon.push(turnover_capacity_summary.to_json());
    }

    let exposure_regime_metrics = if options.include_exposure_regime_metrics {
        let exposure_rows =
            load_alpha_source_exposure_rows(db, combo_name, version, &exposure_regime_trade_dates)
                .await?;
        let exposure_summary = high_bucket_exposure_summary_from_rows(
            &exposure_rows,
            options.bucket_count,
            min_daily_sample_size,
        );
        json!({
            "included": true,
            "research_only": true,
            "options": {
                "max_exposure_regime_days": options.max_exposure_regime_days,
                "sampled_trade_dates": exposure_regime_trade_dates,
                "regime_split_sampled_trade_dates": regime_split_trade_dates,
                "bucket_count": options.bucket_count,
                "min_daily_sample_size": min_daily_sample_size,
                "market_regime_benchmark": "000300.SH",
                "market_regime_trailing_window_days": 60,
            },
            "input_policy": {
                "exposure": "score, amount, circ_mv and total_mv are same-day cross-sectional diagnostics; market_stock.industry is current/static and is not promoted to PIT factor input",
                "regime": "regime labels use only market_index_daily_bar rows with trade_date <= sample trade_date",
                "forward_return_labels": "regime split performance uses future returns only as post-hoc research labels"
            },
            "high_score_bucket_exposure": exposure_summary.to_json(),
            "market_regime_distribution": market_regime_distribution(&exposure_market_regimes),
            "market_regime_samples": exposure_market_regimes.values().map(AlphaSourceMarketRegime::to_json).collect::<Vec<_>>(),
            "regime_split_by_horizon": regime_split_by_horizon,
        })
    } else {
        json!({
            "included": false,
            "reason": "set include_exposure_regime_metrics=true together with include_research_metrics=true to run exposure and market-regime diagnostics",
            "options": {
                "max_exposure_regime_days": options.max_exposure_regime_days,
            }
        })
    };

    Ok(json!({
        "included": true,
        "research_only": true,
        "label_policy": "Forward returns compound market_stock_daily_bar.pct_change over the next N SSE open days for diagnostics only; labels are not persisted to alpha/model inputs.",
        "options": {
            "return_horizons": options.return_horizons,
            "bucket_count": options.bucket_count,
            "max_rank_ic_days": options.max_rank_ic_days,
            "include_exposure_regime_metrics": options.include_exposure_regime_metrics,
            "max_exposure_regime_days": options.max_exposure_regime_days,
            "sampled_trade_dates": sampled_trade_dates,
            "min_daily_sample_size": min_daily_sample_size,
            "sampling": "evenly spaced eligible PIT alpha days within requested range",
            "forward_calendar": "market_trade_calendar exchange=SSE, is_open=true"
        },
        "rank_ic_by_horizon": rank_ic_by_horizon,
        "group_return_by_horizon": group_return_by_horizon,
        "decay_curve": decay_curve,
        "turnover_capacity_by_horizon": turnover_capacity_by_horizon,
        "exposure_regime_metrics": exposure_regime_metrics,
    }))
}


pub(crate) async fn build_futures_price_chain_component_orientation_diagnostics(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    thresholds: ReadinessThresholds,
    daily_rows: &[(NaiveDate, i64)],
    options: &AlphaSourceResearchDiagnosticsOptions,
) -> Result<Value, String> {
    if combo_name != "futures_price_chain" {
        return Ok(json!({
            "included": false,
            "reason": "component orientation diagnostics are currently defined only for futures_price_chain"
        }));
    }
    let mut report =
        futures_price_chain_component_orientation_contract_json(options.include_research_metrics);
    if !options.include_research_metrics || daily_rows.is_empty() {
        return Ok(report);
    }

    let min_daily_sample_size = thresholds.min_daily_rows.max(100);
    let sampled_trade_dates =
        sample_alpha_source_research_days(daily_rows, options.max_rank_ic_days);
    let mut component_reports = Vec::new();
    for component in report["components"].as_array().cloned().unwrap_or_default() {
        let signal_code = component["signal_code"]
            .as_str()
            .ok_or_else(|| "futures_price_chain component missing signal_code".to_string())?;
        let score_rows = load_futures_price_chain_component_score_rows(
            db,
            signal_code,
            version,
            &sampled_trade_dates,
        )
        .await?;
        let mut rank_ic_by_horizon = Vec::new();
        let mut group_return_by_horizon = Vec::new();
        let mut turnover_capacity_by_horizon = Vec::new();
        let mut decay_curve = Vec::new();
        for horizon_days in &options.return_horizons {
            let labeled_rows =
                label_alpha_source_score_rows(db, &score_rows, &sampled_trade_dates, *horizon_days)
                    .await?;
            let rank_ic_rows = daily_rank_ic_from_labeled_rows(
                *horizon_days,
                &labeled_rows,
                min_daily_sample_size,
            );
            let rank_ic_summary = summarize_rank_ic_samples(*horizon_days, &rank_ic_rows);
            let bucket_rows = group_return_rows_from_labeled_rows(
                &labeled_rows,
                min_daily_sample_size,
                options.bucket_count,
            );
            let group_summary =
                summarize_group_return_buckets(*horizon_days, options.bucket_count, &bucket_rows);
            let turnover_capacity_summary = turnover_capacity_summary_from_labeled_rows(
                *horizon_days,
                &labeled_rows,
                min_daily_sample_size,
                options.bucket_count,
            );
            decay_curve.push(json!({
                "horizon_days": horizon_days,
                "mean_rank_ic": rank_ic_summary.mean_rank_ic,
                "median_rank_ic": rank_ic_summary.median_rank_ic,
                "high_minus_low_spread": group_summary.high_minus_low_spread,
                "sampled_days": rank_ic_summary.sampled_days.min(turnover_capacity_summary.sampled_days),
            }));
            rank_ic_by_horizon.push(rank_ic_summary.to_json());
            group_return_by_horizon.push(group_summary.to_json());
            turnover_capacity_by_horizon.push(turnover_capacity_summary.to_json());
        }
        let metrics = json!({
            "included": true,
            "research_only": true,
            "label_policy": "Forward returns are diagnostics labels only; component scores are reconstructed PIT from market_futures_product_signal_pit and are not persisted.",
            "score_rows": score_rows.len(),
            "rank_ic_by_horizon": rank_ic_by_horizon,
            "group_return_by_horizon": group_return_by_horizon,
            "decay_curve": decay_curve,
            "turnover_capacity_by_horizon": turnover_capacity_by_horizon,
        });
        let admission = alpha_source_research_economic_admission(&metrics);
        let mut component_report = component;
        if let Some(object) = component_report.as_object_mut() {
            object.insert("score_rows".to_string(), json!(score_rows.len()));
            object.insert("research_metrics".to_string(), metrics);
            object.insert("research_economic_admission".to_string(), admission);
        }
        component_reports.push(component_report);
    }

    let passed_component_count = component_reports
        .iter()
        .filter(|component| {
            component["research_economic_admission"]["status"].as_str()
                == Some("candidate_ready_for_bounded_wfa")
        })
        .count();
    let admission = if passed_component_count > 0 {
        json!({
            "status": "component_candidate_ready_for_bounded_wfa_profile",
            "passed": true,
            "bounded_wfa": "eligible_for_pre_registered_component_sleeve_or_gate_design",
            "v19_train_selection": "blocked_until_bounded_wfa_passes",
            "passed_component_count": passed_component_count,
            "warning": "component pass does not authorize sign flip, weight tuning, or direct v19 admission"
        })
    } else {
        json!({
            "status": "blocked_until_component_economics_pass",
            "passed": false,
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked",
            "passed_component_count": passed_component_count,
            "required_next_step": "stop this source or pre-register a train-only economic-link test; do not rescue by full-period sign flip"
        })
    };

    if let Some(object) = report.as_object_mut() {
        object.insert("components".to_string(), json!(component_reports));
        object.insert("admission".to_string(), admission);
        object.insert(
            "options".to_string(),
            json!({
                "return_horizons": options.return_horizons,
                "bucket_count": options.bucket_count,
                "max_rank_ic_days": options.max_rank_ic_days,
                "sampled_trade_dates": sampled_trade_dates,
                "min_daily_sample_size": min_daily_sample_size,
            }),
        );
    }
    Ok(report)
}


pub(crate) fn sample_alpha_source_research_days(
    daily_rows: &[(NaiveDate, i64)],
    max_sample_days: i64,
) -> Vec<NaiveDate> {
    let max_sample_days = max_sample_days.max(1) as usize;
    if daily_rows.len() <= max_sample_days {
        return daily_rows
            .iter()
            .map(|(trade_date, _)| *trade_date)
            .collect();
    }
    let step = ((daily_rows.len() as f64) / (max_sample_days as f64)).ceil() as usize;
    daily_rows
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % step.max(1) == 0)
        .take(max_sample_days)
        .map(|(_, (trade_date, _))| *trade_date)
        .collect()
}


pub(crate) fn sample_alpha_source_regime_days(
    rank_ic_trade_dates: &[NaiveDate],
    exposure_trade_dates: &[NaiveDate],
) -> Vec<NaiveDate> {
    rank_ic_trade_dates
        .iter()
        .chain(exposure_trade_dates.iter())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}


pub(crate) fn rank_values(values: &[f64]) -> Vec<f64> {
    let mut indexed = values
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, f64)>>();
    indexed.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut ranks = vec![0.0; values.len()];
    let mut idx = 0usize;
    while idx < indexed.len() {
        let rank = (idx + 1) as f64;
        let mut end = idx + 1;
        while end < indexed.len() && indexed[end].1 == indexed[idx].1 {
            end += 1;
        }
        for tied in &indexed[idx..end] {
            ranks[tied.0] = rank;
        }
        idx = end;
    }
    ranks
}


pub(crate) fn pearson_corr(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 2 {
        return None;
    }
    let n = left.len() as f64;
    let left_mean = left.iter().sum::<f64>() / n;
    let right_mean = right.iter().sum::<f64>() / n;
    let mut numerator = 0.0;
    let mut left_var = 0.0;
    let mut right_var = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_delta = *left_value - left_mean;
        let right_delta = *right_value - right_mean;
        numerator += left_delta * right_delta;
        left_var += left_delta * left_delta;
        right_var += right_delta * right_delta;
    }
    let denominator = left_var.sqrt() * right_var.sqrt();
    if denominator <= 0.0 {
        None
    } else {
        Some(numerator / denominator)
    }
}


pub(crate) fn daily_rank_ic_from_labeled_rows(
    horizon_days: i64,
    rows: &[AlphaSourceLabeledRow],
    min_daily_sample_size: i64,
) -> Vec<AlphaSourceDailyRankIc> {
    let mut by_day: BTreeMap<NaiveDate, Vec<&AlphaSourceLabeledRow>> = BTreeMap::new();
    for row in rows {
        by_day.entry(row.trade_date).or_default().push(row);
    }
    by_day
        .into_iter()
        .filter_map(|(trade_date, day_rows)| {
            if day_rows.len() < min_daily_sample_size as usize {
                return None;
            }
            let scores = day_rows.iter().map(|row| row.score).collect::<Vec<_>>();
            let returns = day_rows
                .iter()
                .map(|row| row.forward_return)
                .collect::<Vec<_>>();
            let score_ranks = rank_values(&scores);
            let return_ranks = rank_values(&returns);
            pearson_corr(&score_ranks, &return_ranks).map(|rank_ic| AlphaSourceDailyRankIc {
                trade_date,
                rank_ic,
                sample_size: day_rows.len() as i64,
            })
        })
        .filter(|row| row.rank_ic.is_finite())
        .map(|row| {
            let _ = horizon_days;
            row
        })
        .collect()
}


pub(crate) fn group_return_rows_from_labeled_rows(
    rows: &[AlphaSourceLabeledRow],
    min_daily_sample_size: i64,
    bucket_count: i64,
) -> Vec<AlphaSourceBucketReturn> {
    let mut by_day: BTreeMap<NaiveDate, Vec<&AlphaSourceLabeledRow>> = BTreeMap::new();
    for row in rows {
        by_day.entry(row.trade_date).or_default().push(row);
    }
    let mut buckets: BTreeMap<i64, (f64, i64)> = BTreeMap::new();
    for (_, mut day_rows) in by_day {
        if day_rows.len() < min_daily_sample_size as usize {
            continue;
        }
        day_rows.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let n = day_rows.len();
        for (idx, row) in day_rows.into_iter().enumerate() {
            let bucket = ((idx * bucket_count as usize) / n) as i64 + 1;
            let entry = buckets.entry(bucket.min(bucket_count)).or_insert((0.0, 0));
            entry.0 += row.forward_return;
            entry.1 += 1;
        }
    }
    buckets
        .into_iter()
        .filter(|(_, (_, count))| *count > 0)
        .map(
            |(bucket, (sum_return, sample_count))| AlphaSourceBucketReturn {
                bucket,
                avg_forward_return: sum_return / sample_count as f64,
                sample_count,
            },
        )
        .collect()
}


pub(crate) fn avg_finite(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}


pub(crate) fn sorted_finite(values: impl Iterator<Item = f64>) -> Vec<f64> {
    let mut values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    values
}


pub(crate) fn turnover_capacity_summary_from_labeled_rows(
    horizon_days: i64,
    rows: &[AlphaSourceLabeledRow],
    min_daily_sample_size: i64,
    bucket_count: i64,
) -> AlphaSourceTurnoverCapacitySummary {
    let mut by_day: BTreeMap<NaiveDate, Vec<&AlphaSourceLabeledRow>> = BTreeMap::new();
    for row in rows {
        by_day.entry(row.trade_date).or_default().push(row);
    }
    let mut previous_symbols = BTreeSet::new();
    let mut daily_symbol_counts = Vec::new();
    let mut daily_turnovers = Vec::new();
    let mut daily_avg_amounts = Vec::new();
    let mut daily_median_amounts = Vec::new();
    let mut daily_p10_amounts = Vec::new();
    let mut daily_avg_circ_mv = Vec::new();
    let mut daily_median_circ_mv = Vec::new();
    let mut daily_p10_circ_mv = Vec::new();

    for (_, mut day_rows) in by_day {
        if day_rows.len() < min_daily_sample_size as usize {
            continue;
        }
        day_rows.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let n = day_rows.len();
        let high_bucket = day_rows
            .into_iter()
            .enumerate()
            .filter_map(|(idx, row)| {
                let bucket = ((idx * bucket_count as usize) / n) as i64 + 1;
                (bucket.min(bucket_count) == bucket_count).then_some(row)
            })
            .collect::<Vec<_>>();
        if high_bucket.is_empty() {
            continue;
        }
        let current_symbols = high_bucket
            .iter()
            .map(|row| row.symbol.clone())
            .collect::<BTreeSet<_>>();
        if !previous_symbols.is_empty() {
            let retained = current_symbols.intersection(&previous_symbols).count();
            daily_turnovers.push(1.0 - retained as f64 / current_symbols.len() as f64);
        }
        previous_symbols = current_symbols;

        let amounts = sorted_finite(high_bucket.iter().filter_map(|row| row.amount));
        let circ_mv = sorted_finite(high_bucket.iter().filter_map(|row| row.circ_mv));
        daily_symbol_counts.push(high_bucket.len() as f64);
        daily_avg_amounts.push(avg_finite(&amounts));
        daily_median_amounts.push(percentile(&amounts, 0.50));
        daily_p10_amounts.push(percentile(&amounts, 0.10));
        daily_avg_circ_mv.push(avg_finite(&circ_mv));
        daily_median_circ_mv.push(percentile(&circ_mv, 0.50));
        daily_p10_circ_mv.push(percentile(&circ_mv, 0.10));
    }

    let sorted_turnovers = sorted_finite(daily_turnovers.iter().copied());
    AlphaSourceTurnoverCapacitySummary {
        horizon_days,
        sampled_days: daily_symbol_counts.len() as i64,
        avg_high_score_bucket_symbols: avg_finite(&daily_symbol_counts),
        avg_high_score_bucket_turnover: avg_finite(&daily_turnovers),
        median_high_score_bucket_turnover: percentile(&sorted_turnovers, 0.50),
        avg_high_score_bucket_amount: avg_finite(&daily_avg_amounts),
        median_high_score_bucket_amount: percentile(
            &sorted_finite(daily_median_amounts.iter().copied()),
            0.50,
        ),
        p10_high_score_bucket_amount: percentile(
            &sorted_finite(daily_p10_amounts.iter().copied()),
            0.10,
        ),
        avg_high_score_bucket_circ_mv: avg_finite(&daily_avg_circ_mv),
        median_high_score_bucket_circ_mv: percentile(
            &sorted_finite(daily_median_circ_mv.iter().copied()),
            0.50,
        ),
        p10_high_score_bucket_circ_mv: percentile(
            &sorted_finite(daily_p10_circ_mv.iter().copied()),
            0.10,
        ),
    }
}


pub(crate) fn high_bucket_exposure_summary_from_rows(
    rows: &[AlphaSourceExposureRow],
    bucket_count: i64,
    min_daily_sample_size: i64,
) -> AlphaSourceHighBucketExposureSummary {
    let mut by_day: BTreeMap<NaiveDate, Vec<&AlphaSourceExposureRow>> = BTreeMap::new();
    for row in rows {
        by_day.entry(row.trade_date).or_default().push(row);
    }

    let mut daily_symbol_counts = Vec::new();
    let mut daily_hhi = Vec::new();
    let mut daily_max_industry_weight = Vec::new();
    let mut amount_ratios = Vec::new();
    let mut circ_mv_ratios = Vec::new();
    let mut total_mv_ratios = Vec::new();
    let mut high_rows_total = 0usize;
    let mut amount_missing = 0usize;
    let mut circ_mv_missing = 0usize;
    let mut industry_missing = 0usize;
    let mut industry_weight_sums = BTreeMap::<String, (f64, f64, i64)>::new();
    let mut sampled_days = 0i64;

    for (_, mut day_rows) in by_day {
        if day_rows.len() < min_daily_sample_size as usize {
            continue;
        }
        day_rows.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let n = day_rows.len();
        let high_bucket = day_rows
            .iter()
            .enumerate()
            .filter_map(|(idx, row)| {
                let bucket = ((idx * bucket_count as usize) / n) as i64 + 1;
                (bucket.min(bucket_count) == bucket_count).then_some(*row)
            })
            .collect::<Vec<_>>();
        if high_bucket.is_empty() {
            continue;
        }
        sampled_days += 1;
        daily_symbol_counts.push(high_bucket.len() as f64);
        high_rows_total += high_bucket.len();
        amount_missing += high_bucket
            .iter()
            .filter(|row| row.amount.is_none())
            .count();
        circ_mv_missing += high_bucket
            .iter()
            .filter(|row| row.circ_mv.is_none())
            .count();
        industry_missing += high_bucket
            .iter()
            .filter(|row| row.industry.as_deref().unwrap_or("").trim().is_empty())
            .count();

        let universe_amount = percentile(
            &sorted_finite(day_rows.iter().filter_map(|row| row.amount)),
            0.50,
        );
        let high_amount = percentile(
            &sorted_finite(high_bucket.iter().filter_map(|row| row.amount)),
            0.50,
        );
        if universe_amount > 0.0 && high_amount.is_finite() {
            amount_ratios.push(high_amount / universe_amount);
        }

        let universe_circ_mv = percentile(
            &sorted_finite(day_rows.iter().filter_map(|row| row.circ_mv)),
            0.50,
        );
        let high_circ_mv = percentile(
            &sorted_finite(high_bucket.iter().filter_map(|row| row.circ_mv)),
            0.50,
        );
        if universe_circ_mv > 0.0 && high_circ_mv.is_finite() {
            circ_mv_ratios.push(high_circ_mv / universe_circ_mv);
        }

        let universe_total_mv = percentile(
            &sorted_finite(day_rows.iter().filter_map(|row| row.total_mv)),
            0.50,
        );
        let high_total_mv = percentile(
            &sorted_finite(high_bucket.iter().filter_map(|row| row.total_mv)),
            0.50,
        );
        if universe_total_mv > 0.0 && high_total_mv.is_finite() {
            total_mv_ratios.push(high_total_mv / universe_total_mv);
        }

        let mut daily_industry_counts = BTreeMap::<String, i64>::new();
        for row in &high_bucket {
            let industry = row
                .industry
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            *daily_industry_counts.entry(industry).or_insert(0) += 1;
        }
        let denominator = high_bucket.len() as f64;
        let mut day_hhi = 0.0;
        let mut day_max_weight = 0.0;
        for (industry, count) in daily_industry_counts {
            let weight = count as f64 / denominator;
            day_hhi += weight * weight;
            if weight > day_max_weight {
                day_max_weight = weight;
            }
            let entry = industry_weight_sums
                .entry(industry)
                .or_insert((0.0, 0.0, 0));
            entry.0 += weight;
            entry.1 = entry.1.max(weight);
            entry.2 += 1;
        }
        daily_hhi.push(day_hhi);
        daily_max_industry_weight.push(day_max_weight);
    }

    let mut top_industries = industry_weight_sums
        .into_iter()
        .map(|(industry, (weight_sum, max_daily_weight, active_days))| {
            AlphaSourceIndustryExposure {
                industry,
                avg_weight: if sampled_days > 0 {
                    weight_sum / sampled_days as f64
                } else {
                    0.0
                },
                max_daily_weight,
                active_days,
            }
        })
        .collect::<Vec<_>>();
    top_industries.sort_by(|left, right| {
        right
            .avg_weight
            .partial_cmp(&left.avg_weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .max_daily_weight
                    .partial_cmp(&left.max_daily_weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.industry.cmp(&right.industry))
    });
    top_industries.truncate(10);

    AlphaSourceHighBucketExposureSummary {
        sampled_days,
        avg_high_score_bucket_symbols: avg_finite(&daily_symbol_counts),
        avg_high_bucket_industry_hhi: avg_finite(&daily_hhi),
        max_single_industry_weight: daily_max_industry_weight
            .into_iter()
            .fold(0.0_f64, f64::max),
        top_industries,
        median_high_vs_universe_amount_ratio: percentile(
            &sorted_finite(amount_ratios.into_iter()),
            0.50,
        ),
        median_high_vs_universe_circ_mv_ratio: percentile(
            &sorted_finite(circ_mv_ratios.into_iter()),
            0.50,
        ),
        median_high_vs_universe_total_mv_ratio: percentile(
            &sorted_finite(total_mv_ratios.into_iter()),
            0.50,
        ),
        high_bucket_amount_missing_ratio: if high_rows_total == 0 {
            0.0
        } else {
            amount_missing as f64 / high_rows_total as f64
        },
        high_bucket_circ_mv_missing_ratio: if high_rows_total == 0 {
            0.0
        } else {
            circ_mv_missing as f64 / high_rows_total as f64
        },
        high_bucket_industry_missing_ratio: if high_rows_total == 0 {
            0.0
        } else {
            industry_missing as f64 / high_rows_total as f64
        },
    }
}


pub(crate) fn classify_alpha_source_market_regime(
    trailing_return: f64,
    annualized_volatility: f64,
    trailing_max_drawdown: f64,
) -> &'static str {
    if annualized_volatility >= 0.30 {
        "high_volatility"
    } else if trailing_return <= -0.02 || trailing_max_drawdown >= 0.20 {
        "bear"
    } else if trailing_return >= 0.10 && trailing_max_drawdown <= 0.15 {
        "bull"
    } else if annualized_volatility <= 0.12 && trailing_return.abs() <= 0.05 {
        "sideways"
    } else {
        "mixed"
    }
}


pub(crate) fn market_regime_distribution(regimes: &BTreeMap<NaiveDate, AlphaSourceMarketRegime>) -> Value {
    let mut counts = BTreeMap::<String, i64>::new();
    for regime in regimes.values() {
        *counts.entry(regime.regime.clone()).or_insert(0) += 1;
    }
    let total = regimes.len() as f64;
    let regimes = counts
        .into_iter()
        .map(|(regime, count)| {
            json!({
                "regime": regime,
                "sampled_days": count,
                "sampled_day_ratio": if total > 0.0 { count as f64 / total } else { 0.0 },
            })
        })
        .collect::<Vec<_>>();
    json!({
        "sampled_days": total as i64,
        "regimes": regimes,
    })
}


pub(crate) fn regime_split_summaries_from_labeled_rows(
    horizon_days: i64,
    rows: &[AlphaSourceLabeledRow],
    regimes: &BTreeMap<NaiveDate, AlphaSourceMarketRegime>,
    min_daily_sample_size: i64,
    bucket_count: i64,
) -> Vec<AlphaSourceRegimeSplitSummary> {
    let mut by_regime = BTreeMap::<String, Vec<AlphaSourceLabeledRow>>::new();
    for row in rows {
        let regime = regimes
            .get(&row.trade_date)
            .map(|regime| regime.regime.clone())
            .unwrap_or_else(|| "unknown".to_string());
        by_regime.entry(regime).or_default().push(row.clone());
    }

    by_regime
        .into_iter()
        .map(|(regime, regime_rows)| {
            let rank_ic_rows =
                daily_rank_ic_from_labeled_rows(horizon_days, &regime_rows, min_daily_sample_size);
            let rank_ic = summarize_rank_ic_samples(horizon_days, &rank_ic_rows);
            let group_rows = group_return_rows_from_labeled_rows(
                &regime_rows,
                min_daily_sample_size,
                bucket_count,
            );
            let sampled_days = regime_rows
                .iter()
                .map(|row| row.trade_date)
                .collect::<BTreeSet<_>>()
                .len() as i64;
            let avg_forward_return = avg_finite(
                &regime_rows
                    .iter()
                    .map(|row| row.forward_return)
                    .filter(|value| value.is_finite())
                    .collect::<Vec<_>>(),
            );
            AlphaSourceRegimeSplitSummary {
                horizon_days,
                regime,
                sampled_days,
                labeled_rows: regime_rows.len() as i64,
                avg_forward_return,
                rank_ic,
                group_return: summarize_group_return_buckets(
                    horizon_days,
                    bucket_count,
                    &group_rows,
                ),
            }
        })
        .collect()
}


pub(crate) async fn load_alpha_source_labeled_rows(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    sampled_trade_dates: &[NaiveDate],
    horizon_days: i64,
) -> Result<Vec<AlphaSourceLabeledRow>, String> {
    let score_rows =
        load_alpha_source_score_rows(db, combo_name, version, sampled_trade_dates).await?;
    label_alpha_source_score_rows(db, &score_rows, sampled_trade_dates, horizon_days).await
}


pub(crate) async fn load_alpha_source_score_rows(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    sampled_trade_dates: &[NaiveDate],
) -> Result<Vec<AlphaSourceScoreRow>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, (NaiveDate, String, f64)>(
        "SELECT trade_date, symbol, normalized_score::float8
         FROM multi_factor_value
         WHERE combo_name = $1
           AND version = $2
           AND trade_date = ANY($3::date[])
           AND normalized_score IS NOT NULL
           AND available_at IS NOT NULL
           AND available_at <= trade_date",
    )
    .bind(combo_name)
    .bind(version)
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha diagnostics score rows: {error}"))
    .map(|rows| {
        rows.into_iter()
            .map(|(trade_date, symbol, score)| AlphaSourceScoreRow {
                trade_date,
                symbol,
                score,
            })
            .collect()
    })
}


pub(crate) fn futures_price_chain_component_score_rows_sql() -> &'static str {
    "WITH requested_days AS (
         SELECT unnest($3::date[])::date AS trade_date
     ),
     request_bounds AS (
         SELECT MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date
         FROM requested_days
     ),
     eligible_universe AS MATERIALIZED (
         SELECT
             bar.symbol,
             bar.trade_date
         FROM requested_days rd
         JOIN market_stock_daily_bar bar
           ON bar.trade_date = rd.trade_date
         JOIN market_stock ms
           ON ms.symbol = bar.symbol
         JOIN market_stock_daily_basic basic
           ON basic.symbol = bar.symbol
          AND basic.trade_date = bar.trade_date
         WHERE bar.close IS NOT NULL
           AND bar.close > 0
           AND basic.circ_mv IS NOT NULL
           AND basic.circ_mv > 0
           AND ms.list_date IS NOT NULL
           AND ms.list_date <= bar.trade_date
           AND (
               ms.delist_date IS NULL
               OR ms.delist_date >= bar.trade_date
           )
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market IN ('主板', '创业板')
           AND ms.symbol NOT LIKE '688%SH'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
           AND NOT EXISTS (
               SELECT 1
               FROM market_stock_name_history st_name
               WHERE st_name.symbol = bar.symbol
                 AND st_name.is_st = true
                 AND st_name.start_date <= bar.trade_date
                 AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
           )
     ),
     stock_membership AS MATERIALIZED (
         SELECT
             universe.symbol,
             universe.trade_date,
             membership.index_code,
             membership.available_at AS membership_available_at
         FROM eligible_universe universe
         JOIN market_stock_industry_membership_pit membership
           ON membership.symbol = universe.symbol
          AND membership.industry_level = 'L1'
          AND membership.classification_source = CASE
              WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
              ELSE 'SW2021'
          END
          AND membership.available_at <= universe.trade_date
          AND membership.in_date <= universe.trade_date
          AND (
              membership.exit_available_at IS NULL
              OR membership.exit_available_at > universe.trade_date
          )
     ),
     signal_intervals AS (
         SELECT
             signal.signal_code,
             signal.product_symbol,
             signal.trade_date AS source_trade_date,
             signal.available_at,
             LEAD(signal.available_at) OVER (
                 PARTITION BY signal.signal_code, signal.product_symbol
                 ORDER BY signal.available_at, signal.trade_date
             ) AS next_available_at,
             signal.raw_value
         FROM market_futures_product_signal_pit signal
         CROSS JOIN request_bounds bounds
         WHERE signal.source_version = $2
           AND signal.signal_code = $1
           AND signal.trade_date <= bounds.max_trade_date
           AND signal.available_at <= bounds.max_trade_date
           AND signal.available_at >= signal.trade_date
           AND signal.raw_value IS NOT NULL
           AND signal.product_symbol IS NOT NULL
           AND signal.product_symbol <> ''
     ),
     industry_signal AS MATERIALIZED (
         SELECT
             rd.trade_date,
             mapping.exposure_code AS index_code,
             GREATEST(MAX(signal.available_at), MAX(mapping.available_at)) AS available_at,
             SUM(signal.raw_value * mapping.direction::double precision * mapping.weight::double precision)
                 / NULLIF(SUM(ABS(mapping.weight::double precision)), 0.0) AS raw_value
         FROM requested_days rd
         JOIN signal_intervals signal
           ON rd.trade_date >= signal.available_at
          AND rd.trade_date < COALESCE(signal.next_available_at, rd.trade_date + INTERVAL '1 day')
         JOIN market_futures_product_exposure_mapping_pit mapping
           ON upper(mapping.product_symbol) = signal.product_symbol
          AND mapping.exposure_type = 'sw_industry'
          AND mapping.available_at <= rd.trade_date
          AND mapping.valid_from <= signal.source_trade_date
          AND (
              mapping.valid_to IS NULL
              OR mapping.valid_to >= signal.source_trade_date
          )
         GROUP BY rd.trade_date, mapping.exposure_code
     ),
     raw AS (
         SELECT
             stock_membership.symbol,
             stock_membership.trade_date,
             GREATEST(industry_signal.available_at, stock_membership.membership_available_at) AS available_at,
             industry_signal.raw_value
         FROM stock_membership
         JOIN industry_signal
           ON industry_signal.index_code = stock_membership.index_code
          AND industry_signal.trade_date = stock_membership.trade_date
         WHERE industry_signal.raw_value IS NOT NULL
           AND industry_signal.available_at <= stock_membership.trade_date
     ),
     ranked AS (
         SELECT
             trade_date,
             symbol,
             CASE
                 WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                 ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)
             END AS normalized_score
         FROM raw
         WHERE raw_value IS NOT NULL
           AND available_at <= trade_date
     )
     SELECT trade_date, symbol, normalized_score::float8
     FROM ranked
     WHERE normalized_score IS NOT NULL
     ORDER BY trade_date, symbol"
}


pub(crate) async fn load_futures_price_chain_component_score_rows(
    db: &sqlx::PgPool,
    signal_code: &str,
    source_version: &str,
    sampled_trade_dates: &[NaiveDate],
) -> Result<Vec<AlphaSourceScoreRow>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, (NaiveDate, String, f64)>(futures_price_chain_component_score_rows_sql())
        .bind(signal_code)
        .bind(source_version)
        .bind(sampled_trade_dates)
        .fetch_all(db)
        .await
        .map_err(|error| {
            format!("Failed to load futures_price_chain component score rows for {signal_code}: {error}")
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(trade_date, symbol, score)| AlphaSourceScoreRow {
                    trade_date,
                    symbol,
                    score,
                })
                .collect()
        })
}


pub(crate) async fn label_alpha_source_score_rows(
    db: &sqlx::PgPool,
    score_rows: &[AlphaSourceScoreRow],
    sampled_trade_dates: &[NaiveDate],
    horizon_days: i64,
) -> Result<Vec<AlphaSourceLabeledRow>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    let min_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .min()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let max_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .max()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let calendar_end = max_sample_date + Duration::days((horizon_days.max(1) * 5 + 30).min(1400));
    let open_days = sqlx::query_scalar::<_, NaiveDate>(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE exchange = 'SSE'
           AND is_open = true
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(min_sample_date)
    .bind(calendar_end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha diagnostics calendar: {error}"))?;

    let open_day_index = open_days
        .iter()
        .enumerate()
        .map(|(idx, trade_date)| (*trade_date, idx))
        .collect::<BTreeMap<_, _>>();
    let mut future_dates_by_sample = BTreeMap::<NaiveDate, Vec<NaiveDate>>::new();
    let mut needed_bar_dates = BTreeSet::<NaiveDate>::new();
    for sample_date in sampled_trade_dates {
        let Some(start_idx) = open_day_index.get(sample_date).copied() else {
            continue;
        };
        let end_idx = start_idx + horizon_days.max(1) as usize;
        if end_idx >= open_days.len() {
            continue;
        }
        let future_dates = open_days[(start_idx + 1)..=end_idx].to_vec();
        for future_date in &future_dates {
            needed_bar_dates.insert(*future_date);
        }
        needed_bar_dates.insert(*sample_date);
        future_dates_by_sample.insert(*sample_date, future_dates);
    }
    if future_dates_by_sample.is_empty() {
        return Ok(Vec::new());
    }
    let needed_bar_dates = needed_bar_dates.into_iter().collect::<Vec<_>>();

    let bar_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>, Option<f64>)>(
        "SELECT trade_date, symbol, pct_change::float8, amount::float8
         FROM market_stock_daily_bar
         WHERE trade_date = ANY($1::date[])",
    )
    .bind(&needed_bar_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha diagnostics return rows: {error}"))?;
    let bar_by_key = bar_rows
        .into_iter()
        .map(|(trade_date, symbol, pct_change, amount)| {
            ((trade_date, symbol), (pct_change, amount))
        })
        .collect::<HashMap<_, _>>();

    let circ_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>)>(
        "SELECT trade_date, symbol, circ_mv::float8
         FROM market_stock_daily_basic
         WHERE trade_date = ANY($1::date[])",
    )
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha diagnostics circ_mv rows: {error}"))?;
    let circ_by_key = circ_rows
        .into_iter()
        .map(|(trade_date, symbol, circ_mv)| ((trade_date, symbol), circ_mv))
        .collect::<HashMap<_, _>>();

    let mut labeled_rows = Vec::new();
    for score_row in score_rows {
        let trade_date = score_row.trade_date;
        let symbol = score_row.symbol.clone();
        let Some(future_dates) = future_dates_by_sample.get(&trade_date) else {
            continue;
        };
        let mut compounded = 1.0;
        let mut complete = true;
        for future_date in future_dates {
            let Some((Some(pct_change), _)) = bar_by_key.get(&(*future_date, symbol.clone()))
            else {
                complete = false;
                break;
            };
            if !pct_change.is_finite() || *pct_change <= -0.999999 {
                complete = false;
                break;
            }
            compounded *= 1.0 + *pct_change;
        }
        if !complete {
            continue;
        }
        let amount = bar_by_key
            .get(&(trade_date, symbol.clone()))
            .and_then(|(_, amount)| *amount);
        let circ_mv = circ_by_key
            .get(&(trade_date, symbol.clone()))
            .and_then(|value| *value);
        labeled_rows.push(AlphaSourceLabeledRow {
            trade_date,
            symbol,
            score: score_row.score,
            forward_return: compounded - 1.0,
            amount,
            circ_mv,
        });
    }
    Ok(labeled_rows)
}


pub(crate) async fn load_alpha_source_exposure_rows(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    sampled_trade_dates: &[NaiveDate],
) -> Result<Vec<AlphaSourceExposureRow>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(Vec::new());
    }
    let score_rows = sqlx::query_as::<_, (NaiveDate, String, f64)>(
        "SELECT trade_date, symbol, normalized_score::float8
         FROM multi_factor_value
         WHERE combo_name = $1
           AND version = $2
           AND trade_date = ANY($3::date[])
           AND normalized_score IS NOT NULL
           AND available_at IS NOT NULL
           AND available_at <= trade_date",
    )
    .bind(combo_name)
    .bind(version)
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha exposure score rows: {error}"))?;

    let bar_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>)>(
        "SELECT trade_date, symbol, amount::float8
         FROM market_stock_daily_bar
         WHERE trade_date = ANY($1::date[])",
    )
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha exposure amount rows: {error}"))?;
    let amount_by_key = bar_rows
        .into_iter()
        .map(|(trade_date, symbol, amount)| ((trade_date, symbol), amount))
        .collect::<HashMap<_, _>>();

    let basic_rows = sqlx::query_as::<_, (NaiveDate, String, Option<f64>, Option<f64>)>(
        "SELECT trade_date, symbol, circ_mv::float8, total_mv::float8
         FROM market_stock_daily_basic
         WHERE trade_date = ANY($1::date[])",
    )
    .bind(sampled_trade_dates)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha exposure daily-basic rows: {error}"))?;
    let basic_by_key = basic_rows
        .into_iter()
        .map(|(trade_date, symbol, circ_mv, total_mv)| ((trade_date, symbol), (circ_mv, total_mv)))
        .collect::<HashMap<_, _>>();

    let symbols = score_rows
        .iter()
        .map(|(_, symbol, _)| symbol.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let industry_rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, industry
         FROM market_stock
         WHERE symbol = ANY($1::text[])",
    )
    .bind(&symbols)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha exposure industry rows: {error}"))?;
    let industry_by_symbol = industry_rows.into_iter().collect::<HashMap<_, _>>();

    Ok(score_rows
        .into_iter()
        .map(|(trade_date, symbol, score)| {
            let amount = amount_by_key
                .get(&(trade_date, symbol.clone()))
                .and_then(|value| *value);
            let (circ_mv, total_mv) = basic_by_key
                .get(&(trade_date, symbol.clone()))
                .copied()
                .unwrap_or((None, None));
            let industry = industry_by_symbol
                .get(&symbol)
                .cloned()
                .flatten()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            AlphaSourceExposureRow {
                trade_date,
                score,
                amount,
                circ_mv,
                total_mv,
                industry,
            }
        })
        .collect())
}


pub(crate) async fn load_alpha_source_market_regimes(
    db: &sqlx::PgPool,
    sampled_trade_dates: &[NaiveDate],
    trailing_window_days: usize,
) -> Result<BTreeMap<NaiveDate, AlphaSourceMarketRegime>, String> {
    if sampled_trade_dates.is_empty() {
        return Ok(BTreeMap::new());
    }
    let min_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .min()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let max_sample_date = sampled_trade_dates
        .iter()
        .copied()
        .max()
        .ok_or_else(|| "sampled_trade_dates cannot be empty".to_string())?;
    let history_start =
        min_sample_date - Duration::days((trailing_window_days as i64 * 3).max(220));
    let index_rows = sqlx::query_as::<_, (NaiveDate, f64, Option<f64>)>(
        "SELECT trade_date, close::float8, pct_change::float8
         FROM market_index_daily_bar
         WHERE symbol = '000300.SH'
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(history_start)
    .bind(max_sample_date)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load alpha market-regime index rows: {error}"))?;

    let mut regimes = BTreeMap::new();
    for sample_date in sampled_trade_dates {
        let eligible = index_rows
            .iter()
            .filter(|(trade_date, close, _)| *trade_date <= *sample_date && close.is_finite())
            .collect::<Vec<_>>();
        if eligible.len() < trailing_window_days.min(40) {
            continue;
        }
        let window_len = trailing_window_days.min(eligible.len());
        let window = &eligible[(eligible.len() - window_len)..];
        let first_close = window.first().map(|(_, close, _)| *close).unwrap_or(0.0);
        let last_close = window.last().map(|(_, close, _)| *close).unwrap_or(0.0);
        if first_close <= 0.0 || last_close <= 0.0 {
            continue;
        }
        let trailing_return = last_close / first_close - 1.0;
        let returns = window
            .iter()
            .filter_map(|(_, _, pct_change)| *pct_change)
            .filter(|value| value.is_finite())
            .collect::<Vec<_>>();
        let annualized_volatility = annualized_volatility(&returns);
        let mut nav = window
            .iter()
            .map(|(_, close, _)| *close)
            .collect::<Vec<_>>();
        let trailing_max_drawdown = max_drawdown(&mut nav);
        regimes.insert(
            *sample_date,
            AlphaSourceMarketRegime {
                trade_date: *sample_date,
                regime: classify_alpha_source_market_regime(
                    trailing_return,
                    annualized_volatility,
                    trailing_max_drawdown,
                )
                .to_string(),
                trailing_return,
                annualized_volatility,
                trailing_max_drawdown,
            },
        );
    }
    Ok(regimes)
}


pub(crate) fn alpha_source_diagnostics_repair_hint(
    passed: bool,
    summary: &AlphaSourceDiagnosticsSummary,
) -> Value {
    if passed {
        return json!({
            "repairable": false,
            "reason": "coverage and PIT gates passed; continue to RankIC/group-return/turnover/capacity diagnostics before WFA"
        });
    }
    if summary.future_leak_rows > 0 {
        return json!({
            "repairable": true,
            "reason": "future leak rows exist; rebuild the alpha source with conservative available_at before any training"
        });
    }
    if summary.null_available_at_rows > 0 {
        return json!({
            "repairable": true,
            "reason": "available_at is missing; repair source PIT timestamp mapping before any training"
        });
    }
    if summary.usable_rows == 0 {
        return json!({
            "repairable": true,
            "reason": "no usable rows found; run or repair the source backfill for this combo/version/range"
        });
    }
    json!({
        "repairable": true,
        "reason": "coverage or daily breadth gate failed; repair missing days/symbol coverage before WFA"
    })
}


pub(crate) async fn build_feature_profile_readiness_report_from_request(
    db: &sqlx::PgPool,
    req: &FeatureProfileReadinessRequest,
) -> Result<Value, String> {
    let start = parse_feature_profile_readiness_date(&req.start_date, "start_date")?;
    let end = parse_feature_profile_readiness_date(&req.end_date, "end_date")?;
    if start > end {
        return Err("feature-profile readiness start_date cannot be after end_date".into());
    }
    let feature_profile = req.feature_profile.trim();
    if feature_profile.is_empty() {
        return Err("feature_profile must not be empty".into());
    }
    let factors = phase7_train_window_ml_factor_refs_for_profile(feature_profile);
    let thresholds = ReadinessThresholds::from_options(
        req.min_day_coverage_ratio,
        req.min_daily_rows,
        req.min_p95_daily_row_ratio,
    );
    let report = build_feature_profile_readiness_report(
        db,
        feature_profile,
        &factors,
        start,
        end,
        thresholds,
    )
    .await?;
    let experiment_run_id = if req.persist_report.unwrap_or(true) {
        Some(persist_feature_profile_readiness_report(db, feature_profile, &report).await?)
    } else {
        None
    };
    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}


pub(crate) async fn build_feature_profile_readiness_report(
    db: &sqlx::PgPool,
    feature_profile: &str,
    factors: &[LinearFactorRef],
    start: NaiveDate,
    end: NaiveDate,
    thresholds: ReadinessThresholds,
) -> Result<Value, String> {
    if factors.is_empty() {
        return Ok(json!({
            "readiness_type": "feature_profile",
            "feature_profile": feature_profile,
            "requested_start_date": start,
            "requested_end_date": end,
            "factor_count": 0,
            "passed": false,
            "level": "red",
            "gates": [profile_readiness_gate(
                "feature_profile_factor_count",
                false,
                json!(0),
                json!("> 0"),
                "feature profile must resolve to at least one factor",
            )],
            "repair": {
                "repairable": false,
                "reason": "unknown feature profile; register the profile/factor list before use"
            }
        }));
    }

    let factor_rows = load_feature_profile_factor_readiness_rows(db, factors, start, end).await?;
    let daily_rows = load_feature_profile_intersection_daily_rows(db, factors, start, end).await?;
    let expected_days = readiness_expected_open_day_count(db, start, end).await?;
    let actual_days = daily_rows.len() as i64;
    let daily_counts = daily_rows
        .iter()
        .map(|(_, count)| *count)
        .collect::<Vec<_>>();
    let distribution = daily_count_distribution(&daily_counts, thresholds);
    let day_coverage_ratio = if expected_days <= 0 {
        1.0
    } else {
        actual_days.max(0) as f64 / expected_days as f64
    };
    let factor_future_leak_rows = factor_rows
        .iter()
        .map(|row| row.future_leak_rows)
        .sum::<i64>();
    let missing_factor_count = factor_rows
        .iter()
        .filter(|row| row.usable_rows == 0)
        .count();
    let min_factor_day_coverage_ratio = factor_rows
        .iter()
        .map(|row| {
            if expected_days <= 0 {
                1.0
            } else {
                row.usable_days.max(0) as f64 / expected_days as f64
            }
        })
        .fold(1.0_f64, f64::min);
    let gates = vec![
        profile_readiness_gate(
            "feature_profile_factor_count",
            !factors.is_empty(),
            json!(factors.len()),
            json!("> 0"),
            "feature profile must resolve to at least one factor",
        ),
        profile_readiness_gate(
            "feature_factor_missing_count",
            missing_factor_count == 0,
            json!(missing_factor_count),
            json!(0),
            "every requested factor must have PIT-usable rows in the requested window",
        ),
        profile_readiness_gate(
            "feature_factor_day_coverage",
            min_factor_day_coverage_ratio >= thresholds.min_day_coverage_ratio,
            json!(min_factor_day_coverage_ratio),
            json!(thresholds.min_day_coverage_ratio),
            "each individual factor must cover nearly all expected open days",
        ),
        profile_readiness_gate(
            "feature_future_leak_rows",
            factor_future_leak_rows == 0,
            json!(factor_future_leak_rows),
            json!(0),
            "factor_value.available_at must not be after trade_date",
        ),
        profile_readiness_gate(
            "feature_profile_intersection_day_coverage",
            day_coverage_ratio >= thresholds.min_day_coverage_ratio,
            json!(day_coverage_ratio),
            json!(thresholds.min_day_coverage_ratio),
            "full-factor intersection must cover nearly all expected open days",
        ),
        profile_readiness_gate(
            "feature_profile_daily_median_symbols",
            distribution.p50_rows >= thresholds.min_daily_rows,
            json!(distribution.p50_rows),
            json!(thresholds.min_daily_rows),
            "full-factor intersection median symbol count must be large enough for ranking",
        ),
        profile_readiness_gate(
            "feature_profile_daily_symbol_cliff",
            distribution.weak_day_count == 0,
            json!({
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
                "p95_rows": distribution.p95_rows,
            }),
            json!("weak_day_count = 0"),
            "full-factor intersection must not collapse relative to its own p95 breadth",
        ),
    ];
    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));

    Ok(json!({
        "readiness_type": "feature_profile",
        "feature_profile": feature_profile,
        "requested_start_date": start,
        "requested_end_date": end,
        "factor_count": factors.len(),
        "passed": passed,
        "level": if passed { "green" } else { "red" },
        "thresholds": {
            "min_day_coverage_ratio": thresholds.min_day_coverage_ratio,
            "min_daily_rows": thresholds.min_daily_rows,
            "min_p95_daily_row_ratio": thresholds.min_p95_daily_row_ratio,
        },
        "summary": {
            "expected_open_days": expected_days,
            "intersection_days": actual_days,
            "missing_open_days": (expected_days - actual_days).max(0),
            "intersection_day_coverage_ratio": day_coverage_ratio,
            "daily_intersection_symbols": {
                "min": distribution.min_rows,
                "p50": distribution.p50_rows,
                "p95": distribution.p95_rows,
                "max": distribution.max_rows,
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
            },
            "factor_future_leak_rows": factor_future_leak_rows,
            "missing_factor_count": missing_factor_count,
            "min_factor_day_coverage_ratio": min_factor_day_coverage_ratio,
        },
        "factors": factor_rows.iter().map(FeatureProfileFactorReadinessRow::to_json).collect::<Vec<_>>(),
        "gates": gates,
        "repair": {
            "repairable": false,
            "reason": "profile-level gaps must be repaired by rebuilding the missing PIT factor sources/backfills; model_prediction row patching is not safe"
        }
    }))
}


#[derive(Debug)]
pub(crate) struct FeatureProfileFactorReadinessRow {
    factor_code: String,
    factor_version: String,
    usable_rows: i64,
    usable_days: i64,
    usable_symbols: i64,
    future_leak_rows: i64,
}


impl FeatureProfileFactorReadinessRow {
    fn to_json(&self) -> Value {
        json!({
            "factor_code": self.factor_code,
            "factor_version": self.factor_version,
            "usable_rows": self.usable_rows,
            "usable_days": self.usable_days,
            "usable_symbols": self.usable_symbols,
            "future_leak_rows": self.future_leak_rows,
        })
    }
}


pub(crate) async fn load_feature_profile_factor_readiness_rows(
    db: &sqlx::PgPool,
    factors: &[LinearFactorRef],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<FeatureProfileFactorReadinessRow>, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "WITH requested(factor_code, factor_version, factor_idx) AS (",
    );
    builder.push_values(
        factors.iter().enumerate(),
        |mut row, (factor_idx, factor)| {
            row.push_bind(&factor.factor_code)
                .push_bind(&factor.factor_version)
                .push_bind(factor_idx as i32);
        },
    );
    builder.push(
        ")
         SELECT requested.factor_code,
                requested.factor_version,
                COUNT(*) FILTER (
                  WHERE fv.symbol IS NOT NULL
                    AND fv.normalized_value IS NOT NULL
                    AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
                )::int8 AS usable_rows,
                COUNT(DISTINCT fv.trade_date) FILTER (
                  WHERE fv.symbol IS NOT NULL
                    AND fv.normalized_value IS NOT NULL
                    AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
                )::int8 AS usable_days,
                COUNT(DISTINCT fv.symbol) FILTER (
                  WHERE fv.symbol IS NOT NULL
                    AND fv.normalized_value IS NOT NULL
                    AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
                )::int8 AS usable_symbols,
                COUNT(*) FILTER (WHERE fv.available_at > fv.trade_date)::int8 AS future_leak_rows
         FROM requested
         LEFT JOIN factor_value fv
           ON fv.factor_code = requested.factor_code
          AND fv.factor_version = requested.factor_version
          AND fv.trade_date >= ",
    );
    builder.push_bind(start);
    builder.push(" AND fv.trade_date <= ");
    builder.push_bind(end);
    builder.push(
        "
         GROUP BY requested.factor_idx, requested.factor_code, requested.factor_version
         ORDER BY requested.factor_idx",
    );

    let rows = builder
        .build_query_as::<(String, String, i64, i64, i64, i64)>()
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load feature-profile factor readiness: {}", error))?;

    Ok(rows
        .into_iter()
        .map(
            |(
                factor_code,
                factor_version,
                usable_rows,
                usable_days,
                usable_symbols,
                future_leak_rows,
            )| FeatureProfileFactorReadinessRow {
                factor_code,
                factor_version,
                usable_rows,
                usable_days,
                usable_symbols,
                future_leak_rows,
            },
        )
        .collect())
}


pub(crate) async fn load_feature_profile_intersection_daily_rows(
    db: &sqlx::PgPool,
    factors: &[LinearFactorRef],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDate, i64)>, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "WITH requested(factor_code, factor_version, factor_idx) AS (",
    );
    builder.push_values(
        factors.iter().enumerate(),
        |mut row, (factor_idx, factor)| {
            row.push_bind(&factor.factor_code)
                .push_bind(&factor.factor_version)
                .push_bind(factor_idx as i32);
        },
    );
    builder.push(
        "),
         complete_rows AS (
           SELECT fv.symbol, fv.trade_date
           FROM factor_value fv
           JOIN requested
             ON requested.factor_code = fv.factor_code
            AND requested.factor_version = fv.factor_version
           WHERE fv.trade_date >= ",
    );
    builder.push_bind(start);
    builder.push(" AND fv.trade_date <= ");
    builder.push_bind(end);
    builder.push(
        "
             AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
           GROUP BY fv.symbol, fv.trade_date
           HAVING COUNT(*) = ",
    );
    builder.push_bind(factors.len() as i64);
    builder.push(
        "
              AND bool_and(fv.normalized_value IS NOT NULL)
         )
         SELECT trade_date, COUNT(*)::int8 AS symbol_count
         FROM complete_rows
         GROUP BY trade_date
         ORDER BY trade_date",
    );

    builder
        .build_query_as::<(NaiveDate, i64)>()
        .fetch_all(db)
        .await
        .map_err(|error| {
            format!(
                "Failed to load feature-profile intersection readiness: {}",
                error
            )
        })
}


pub(crate) async fn persist_alpha_source_diagnostics_report(
    db: &sqlx::PgPool,
    combo_name: &str,
    version: &str,
    report: &Value,
) -> Result<String, String> {
    let config = json!({
        "combo_name": combo_name,
        "version": version,
        "report_type": "alpha_source_diagnostics",
        "point_in_time_scope": "multi_factor_value only; no label/backtest/OOS metrics",
    });
    create_experiment_run(
        db,
        "alpha_source_diagnostics_report",
        "alpha_source",
        Some(&format!("{combo_name}@{version}")),
        &config,
        report,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to persist alpha source diagnostics report: {}", error))
}


pub(crate) async fn persist_main_business_diagnostics_report(
    db: &sqlx::PgPool,
    business_type: &str,
    universe: MainBusinessDiagnosticsUniverse,
    profiles: &[MainBusinessDiagnosticsProfile],
    report: &Value,
) -> Result<String, String> {
    let profile_names = profiles
        .iter()
        .map(|profile| profile.as_str())
        .collect::<Vec<_>>();
    let config = json!({
        "business_type": business_type,
        "universe_profile": universe.as_str(),
        "profiles": profile_names,
        "report_type": "main_business_source_diagnostics",
        "point_in_time_scope": "market_stock_main_business.available_at <= trade_date",
        "research_only": true,
        "does_not_write_multi_factor_value": true,
    });
    create_experiment_run(
        db,
        "main_business_source_diagnostics_report",
        "alpha_source",
        Some("main_business"),
        &config,
        report,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to persist main_business source diagnostics report: {}", error))
}


pub(crate) async fn persist_feature_profile_readiness_report(
    db: &sqlx::PgPool,
    feature_profile: &str,
    report: &Value,
) -> Result<String, String> {
    let config = json!({
        "feature_profile": feature_profile,
        "report_type": "feature_profile_readiness",
        "point_in_time_scope": "factor_value only; no backtest/OOS metrics",
    });
    create_experiment_run(
        db,
        "feature_profile_readiness_report",
        "feature_profile",
        Some(feature_profile),
        &config,
        report,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to persist feature-profile readiness report: {}", error))
}


