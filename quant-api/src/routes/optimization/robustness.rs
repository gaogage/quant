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
use quant_api::discovery::phase7::{
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

pub(crate) struct RobustnessEvaluation {
    status: String,
    gates: Value,
}


#[derive(Debug, Clone)]
pub(crate) struct RobustnessDailyPoint {
    pub(crate) trade_date: NaiveDate,
    pub(crate) portfolio_value: f64,
    pub(crate) benchmark_value: Option<f64>,
}


#[derive(Debug, Clone)]
pub(crate) struct RobustnessMetricSummary {
    pub(crate) total_return: f64,
    pub(crate) annual_return: f64,
    pub(crate) sharpe_ratio: f64,
    pub(crate) sortino_ratio: f64,
    pub(crate) calmar_ratio: f64,
    pub(crate) max_drawdown: f64,
    pub(crate) benchmark_return: Option<f64>,
    pub(crate) excess_return: Option<f64>,
}


#[derive(Debug, Clone)]
pub(crate) struct RobustnessTimeSeriesAnalysis {
    pub(crate) market_scenarios: Value,
    pub(crate) walk_forward: Value,
    pub(crate) bootstrap: Value,
}


pub(crate) struct OptimizationPerformanceGatePolicy {
    pub(crate) min_completed_trials: i64,
    pub(crate) max_failed_trials: i64,
    pub(crate) max_elapsed_ms: Option<i64>,
}


pub(crate) struct RobustnessOverlayPersistenceFields {
    pub(crate) gate_result_id: String,
    pub(crate) status: String,
    pub(crate) gate_results: Value,
}


pub(crate) fn resolve_robustness_gate_policy(gate_policy: Option<&Value>) -> Value {
    match gate_policy {
        Some(policy) => {
            let preset = policy
                .get("preset")
                .or_else(|| policy.get("candidate_tier"))
                .or_else(|| policy.get("tier"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match preset {
                "elite" | "professional_elite" => {
                    merge_gate_policy(default_professional_elite_robustness_policy(), policy)
                }
                "professional" | "professional_observation" | "observation" | "default" => {
                    merge_gate_policy(default_professional_robustness_policy(), policy)
                }
                _ => policy.clone(),
            }
        }
        None => default_professional_robustness_policy(),
    }
}


pub(crate) fn resolve_oos_train_selection_gate_policy(req: &Phase7OosWalkForwardDiscoveryRequest) -> Value {
    let base =
        default_oos_train_selection_gate_policy_for_search_profile(req.search_profile.as_deref());
    if let Some(policy) = req.train_selection_gate_policy.as_ref() {
        return resolve_oos_train_selection_policy(Some(policy), base);
    }
    if let Some(policy) = req.train_robustness_gate_policy.as_ref() {
        return resolve_robustness_gate_policy(Some(policy));
    }
    base
}


pub(crate) fn resolve_oos_train_selection_policy(gate_policy: Option<&Value>, base: Value) -> Value {
    match gate_policy {
        Some(policy) => {
            let preset = policy
                .get("preset")
                .or_else(|| policy.get("candidate_tier"))
                .or_else(|| policy.get("tier"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match preset {
                "professional"
                | "professional_observation"
                | "observation"
                | "default"
                | "elite"
                | "professional_elite" => resolve_robustness_gate_policy(Some(policy)),
                "oos_train_selection" | "train_selection" | "relaxed_train_selection" | "" => {
                    merge_gate_policy(base, policy)
                }
                _ => merge_gate_policy(base, policy),
            }
        }
        None => base,
    }
}


pub(crate) fn resolve_oos_final_promotion_gate_policy(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    validation_mode: &str,
) -> Value {
    let mut policy = default_oos_final_promotion_gate_policy(validation_mode);
    if let Some(overrides) = req.final_promotion_gate_policy.as_ref() {
        policy = merge_gate_policy(policy, overrides);
    }
    if let Some(value) = req.min_stitched_oos_calmar {
        insert_policy_number(&mut policy, "min_stitched_oos_calmar", value);
    }
    if let Some(value) = req.min_positive_oos_window_ratio {
        insert_policy_number(&mut policy, "min_positive_oos_window_ratio", value);
    }
    if let Some(value) = req.min_oos_window_count {
        insert_policy_integer(&mut policy, "min_oos_window_count", value as i64);
    }
    if let Some(value) = req.min_cost_capacity_perturbation_pass_ratio {
        insert_policy_number(
            &mut policy,
            "min_cost_capacity_perturbation_pass_ratio",
            value,
        );
    }
    if let Some(value) = req.min_perturbed_oos_calmar {
        insert_policy_number(&mut policy, "min_perturbed_oos_calmar", value);
    }
    if let Some(value) = req.max_perturbed_oos_drawdown_pct {
        insert_policy_number(&mut policy, "max_perturbed_oos_drawdown_pct", value);
    }
    policy
}


pub(crate) fn insert_policy_number(policy: &mut Value, key: &str, value: f64) {
    if let Some(object) = policy.as_object_mut() {
        object.insert(key.to_string(), json!(value));
    }
}


pub(crate) fn insert_policy_integer(policy: &mut Value, key: &str, value: i64) {
    if let Some(object) = policy.as_object_mut() {
        object.insert(key.to_string(), json!(value));
    }
}


pub(crate) fn merge_gate_policy(mut base: Value, overrides: &Value) -> Value {
    if let (Some(base_map), Some(override_map)) = (base.as_object_mut(), overrides.as_object()) {
        for (key, value) in override_map {
            base_map.insert(key.clone(), value.clone());
        }
    }
    base
}


pub(crate) fn metric_f64_value(metrics: Option<&Value>, key: &str) -> Option<f64> {
    metrics
        .and_then(|metrics| metrics.get(key))
        .and_then(value_as_f64)
}


pub(crate) fn robustness_result_is_approved(result: &Value) -> bool {
    result
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status == "approved_candidate")
        .unwrap_or(false)
}


pub async fn evaluate_optimization_robustness(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<EvaluateRobustnessRequest>,
) -> impl IntoResponse {
    match evaluate_and_persist_robustness(&state.db, &task_id, req.gate_policy.as_ref()).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn evaluate_optimization_trial_robustness(
    State(state): State<Arc<AppState>>,
    Path((task_id, trial_id)): Path<(String, String)>,
    Json(req): Json<EvaluateRobustnessRequest>,
) -> impl IntoResponse {
    match evaluate_and_persist_robustness_for_trial(
        &state.db,
        &task_id,
        &trial_id,
        req.gate_policy.as_ref(),
        "Trial robustness gate evaluated",
    )
    .await
    {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub(crate) fn normalize_performance_gate(
    gate: Option<&OptimizationPerformanceGateRequest>,
    trial_limit: i64,
) -> Result<OptimizationPerformanceGatePolicy, String> {
    let min_completed_trials = gate
        .and_then(|gate| gate.min_completed_trials)
        .unwrap_or(trial_limit);
    if min_completed_trials < 0 {
        return Err("performance_gate.min_completed_trials must be non-negative".into());
    }
    let max_failed_trials = gate.and_then(|gate| gate.max_failed_trials).unwrap_or(0);
    if max_failed_trials < 0 {
        return Err("performance_gate.max_failed_trials must be non-negative".into());
    }
    let max_elapsed_ms = gate.and_then(|gate| gate.max_elapsed_ms);
    if matches!(max_elapsed_ms, Some(value) if value < 0) {
        return Err("performance_gate.max_elapsed_ms must be non-negative".into());
    }

    Ok(OptimizationPerformanceGatePolicy {
        min_completed_trials,
        max_failed_trials,
        max_elapsed_ms,
    })
}


pub(crate) fn evaluate_optimization_performance_gates(
    executed: i64,
    completed: i64,
    failed: i64,
    elapsed_ms: i64,
    policy: &OptimizationPerformanceGatePolicy,
) -> Value {
    let mut gates = vec![
        json!({
            "gate": "min_completed_trials",
            "passed": completed >= policy.min_completed_trials,
            "limit": policy.min_completed_trials,
            "actual": completed,
        }),
        json!({
            "gate": "max_failed_trials",
            "passed": failed <= policy.max_failed_trials,
            "limit": policy.max_failed_trials,
            "actual": failed,
        }),
        json!({
            "gate": "executed_trials",
            "passed": executed >= policy.min_completed_trials,
            "limit": policy.min_completed_trials,
            "actual": executed,
        }),
    ];
    if let Some(limit) = policy.max_elapsed_ms {
        gates.push(json!({
            "gate": "max_elapsed_ms",
            "passed": elapsed_ms <= limit,
            "limit": limit,
            "actual": elapsed_ms,
        }));
    }
    Value::Array(gates)
}


pub(crate) fn performance_gate_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item.get("passed").and_then(Value::as_bool).unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "passed"
    } else {
        "review_required"
    }
}


pub(crate) async fn evaluate_and_persist_robustness(
    db: &sqlx::PgPool,
    task_id: &str,
    gate_policy: Option<&Value>,
) -> Result<Value, String> {
    let task = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT best_trial_id, status
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    let best_trial_id = task.0.as_deref().ok_or_else(|| {
        "optimization task has no best_trial_id; run completed trials first".to_string()
    })?;

    evaluate_and_persist_robustness_for_trial(
        db,
        task_id,
        best_trial_id,
        gate_policy,
        "Robustness gate evaluated",
    )
    .await
}


pub(crate) async fn evaluate_and_persist_robustness_for_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_id: &str,
    gate_policy: Option<&Value>,
    summary_prefix: &str,
) -> Result<Value, String> {
    let trial = sqlx::query_as::<_, (Decimal, Option<Value>, Option<Value>, Option<String>, i32)>(
        "SELECT score, metrics, constraint_violations, backtest_task_id, trial_index
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND trial_id = $2 AND status = 'completed'",
    )
    .bind(task_id)
    .bind(trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load best optimization trial: {}", error))?
    .ok_or_else(|| "best optimization trial is not completed".to_string())?;

    let runner_up = sqlx::query_as::<_, (Decimal,)>(
        "SELECT score
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND trial_id <> $2
           AND status = 'completed'
           AND score IS NOT NULL
           AND (score < $3 OR (score = $3 AND trial_index > $4))
         ORDER BY score DESC NULLS LAST, trial_index ASC
         LIMIT 1",
    )
    .bind(task_id)
    .bind(trial_id)
    .bind(trial.0)
    .bind(trial.4)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load runner-up optimization trial: {}", error))?
    .map(|row| row.0);

    let policy = resolve_robustness_gate_policy(gate_policy);
    let analysis = if let Some(backtest_task_id) = trial.3.as_deref() {
        load_robustness_timeseries_analysis(db, backtest_task_id, &policy).await?
    } else {
        None
    };
    let raw_metrics = trial.1.unwrap_or_else(|| json!({}));
    let (metrics, metric_sources, missing_elite_metrics) =
        enrich_trial_metrics(db, trial.3.as_deref(), raw_metrics).await?;
    let evaluation = evaluate_robustness_gates_with_analysis(
        trial.0,
        runner_up,
        &metrics,
        trial.2.as_ref().unwrap_or(&json!([])),
        Some(&policy),
        analysis.as_ref(),
    );
    let gate_result_id = format!("gate-{}", Uuid::new_v4());

    sqlx::query(
        "INSERT INTO robustness_gate_result
           (gate_result_id, optimization_task_id, trial_id, gate_policy, gate_results,
            status, summary)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&gate_result_id)
    .bind(task_id)
    .bind(trial_id)
    .bind(&policy)
    .bind(&evaluation.gates)
    .bind(&evaluation.status)
    .bind(format!("{} as {}", summary_prefix, evaluation.status))
    .execute(db)
    .await
    .map_err(|error| format!("Failed to persist robustness gate result: {}", error))?;

    Ok(json!({
        "gate_result_id": gate_result_id,
        "optimization_task_id": task_id,
        "trial_id": trial_id,
        "status": evaluation.status,
        "gate_results": evaluation.gates,
        "metrics": metrics,
        "metric_sources": metric_sources,
        "missing_elite_metrics": missing_elite_metrics,
        "failure_attribution": build_robustness_failure_attribution(&evaluation.gates),
    }))
}


pub(crate) async fn load_robustness_timeseries_analysis(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
    policy: &Value,
) -> Result<Option<RobustnessTimeSeriesAnalysis>, String> {
    let rows = sqlx::query_as::<_, (NaiveDate, f64, Option<f64>)>(
        "SELECT c.trade_date,
                c.portfolio_value::double precision,
                COALESCE(c.benchmark_value::double precision, i.close::double precision)
         FROM backtest_equity_curve c
         JOIN backtest_task t ON t.task_id = c.task_id
         LEFT JOIN market_index_daily_bar i
           ON i.symbol = t.benchmark_symbol
          AND i.trade_date = c.trade_date
         WHERE c.task_id = $1
         ORDER BY c.trade_date",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load backtest equity curve for robustness: {}",
            error
        )
    })?;

    if rows.len() < 3 {
        return Ok(None);
    }
    let points = rows
        .into_iter()
        .filter(|(_, portfolio_value, _)| portfolio_value.is_finite() && *portfolio_value > 0.0)
        .map(
            |(trade_date, portfolio_value, benchmark_value)| RobustnessDailyPoint {
                trade_date,
                portfolio_value,
                benchmark_value: benchmark_value.filter(|value| value.is_finite() && *value > 0.0),
            },
        )
        .collect::<Vec<_>>();
    if points.len() < 3 {
        return Ok(None);
    }

    let window_size = constraint_i64(Some(policy), "walk_forward_window_days")
        .unwrap_or(63)
        .max(2) as usize;
    let step_size = constraint_i64(Some(policy), "walk_forward_step_days")
        .unwrap_or(21)
        .max(1) as usize;
    let bootstrap_trials = constraint_i64(Some(policy), "bootstrap_trials")
        .unwrap_or(256)
        .clamp(1, 10_000) as usize;
    let bootstrap_seed = constraint_i64(Some(policy), "bootstrap_seed")
        .unwrap_or(42)
        .max(1) as u64;

    RobustnessTimeSeriesAnalysis::from_points(
        &points,
        window_size.min(points.len()),
        step_size,
        bootstrap_trials,
        bootstrap_seed,
    )
    .map(Some)
}


impl RobustnessDailyPoint {
    #[cfg(test)]
    fn new(trade_date: &str, portfolio_value: f64, benchmark_value: Option<f64>) -> Self {
        Self {
            trade_date: NaiveDate::parse_from_str(trade_date, "%Y-%m-%d").expect("valid date"),
            portfolio_value,
            benchmark_value,
        }
    }
}


impl RobustnessTimeSeriesAnalysis {
    fn from_points(
        points: &[RobustnessDailyPoint],
        window_size: usize,
        step_size: usize,
        bootstrap_trials: usize,
        bootstrap_seed: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            market_scenarios: build_market_scenario_analysis(points),
            walk_forward: build_walk_forward_analysis(points, window_size, step_size),
            bootstrap: build_bootstrap_analysis(points, bootstrap_trials, bootstrap_seed)?,
        })
    }
}


pub(crate) fn build_market_scenario_analysis(points: &[RobustnessDailyPoint]) -> Value {
    if points.len() < 2 {
        return json!({
            "scenario_count": 0,
            "scenarios": []
        });
    }
    let summary = summarize_points(points);
    let volatility =
        annualized_volatility(&daily_returns(points, |point| Some(point.portfolio_value)));
    let scenario = classify_market_scenario(&summary, volatility);
    json!({
        "scenario_count": 1,
        "scenarios": [{
            "scenario": scenario,
            "start_date": points.first().map(|point| point.trade_date),
            "end_date": points.last().map(|point| point.trade_date),
            "metrics": metric_summary_json(&summary),
            "annualized_volatility": volatility
        }]
    })
}


pub(crate) fn build_walk_forward_analysis(
    points: &[RobustnessDailyPoint],
    window_size: usize,
    step_size: usize,
) -> Value {
    if points.len() < 2 || window_size < 2 || step_size == 0 {
        return json!({
            "window_count": 0,
            "windows": [],
            "positive_excess_window_ratio": 0.0,
            "worst_window_drawdown": 0.0
        });
    }

    let mut windows = Vec::new();
    let mut annual_returns = Vec::new();
    let mut excess_returns = Vec::new();
    let mut sharpes = Vec::new();
    let mut sortinos = Vec::new();
    let mut calmars = Vec::new();
    let mut idx = 0;
    while idx + window_size <= points.len() {
        let slice = &points[idx..idx + window_size];
        let summary = summarize_points(slice);
        let volatility =
            annualized_volatility(&daily_returns(slice, |point| Some(point.portfolio_value)));
        annual_returns.push(summary.annual_return);
        if let Some(excess_return) = summary.excess_return {
            excess_returns.push(excess_return);
        }
        sharpes.push(summary.sharpe_ratio);
        sortinos.push(summary.sortino_ratio);
        calmars.push(summary.calmar_ratio);
        windows.push(json!({
            "window_index": windows.len() + 1,
            "start_date": slice.first().map(|point| point.trade_date),
            "end_date": slice.last().map(|point| point.trade_date),
            "scenario": classify_market_scenario(&summary, volatility),
            "metrics": metric_summary_json(&summary),
            "annualized_volatility": volatility
        }));
        idx += step_size;
    }

    let positive_excess_count = windows
        .iter()
        .filter(|window| {
            window["metrics"]["excess_return"]
                .as_f64()
                .map(|value| value > 0.0)
                .unwrap_or(false)
        })
        .count();
    let positive_annual_return_count = annual_returns
        .iter()
        .filter(|value| value.is_finite() && **value > 0.0)
        .count();
    let worst_window_drawdown = windows
        .iter()
        .filter_map(|window| window["metrics"]["max_drawdown"].as_f64())
        .fold(0.0_f64, f64::max);
    let scenario_count = windows
        .iter()
        .filter_map(|window| window["scenario"].as_str())
        .collect::<BTreeSet<_>>()
        .len();

    json!({
        "window_count": windows.len(),
        "window_size": window_size,
        "step_size": step_size,
        "positive_excess_window_ratio": if windows.is_empty() { 0.0 } else { positive_excess_count as f64 / windows.len() as f64 },
        "positive_annual_return_ratio": if windows.is_empty() { 0.0 } else { positive_annual_return_count as f64 / windows.len() as f64 },
        "worst_window_drawdown": worst_window_drawdown,
        "scenario_count": scenario_count,
        "annual_return": distribution_summary(&mut annual_returns),
        "excess_return": distribution_summary(&mut excess_returns),
        "sharpe_ratio": distribution_summary(&mut sharpes),
        "sortino_ratio": distribution_summary(&mut sortinos),
        "calmar_ratio": distribution_summary(&mut calmars),
        "windows": windows
    })
}


pub(crate) fn build_bootstrap_analysis(
    points: &[RobustnessDailyPoint],
    trials: usize,
    seed: u64,
) -> Result<Value, String> {
    if points.len() < 3 {
        return Err("bootstrap requires at least 3 equity points".into());
    }
    let returns = daily_returns(points, |point| Some(point.portfolio_value));
    if returns.is_empty() {
        return Err("bootstrap requires non-empty daily returns".into());
    }
    let trials = trials.clamp(1, 10_000);
    let mut rng = DeterministicRng::new(seed);
    let mut total_returns = Vec::with_capacity(trials);
    let mut sharpes = Vec::with_capacity(trials);
    let mut sortinos = Vec::with_capacity(trials);
    let mut calmars = Vec::with_capacity(trials);
    let mut drawdowns = Vec::with_capacity(trials);
    let mut positive = 0usize;

    for _ in 0..trials {
        let sample = (0..returns.len())
            .map(|_| returns[rng.gen_usize(returns.len())])
            .collect::<Vec<_>>();
        let summary = summarize_return_sample(&sample);
        if summary.total_return > 0.0 {
            positive += 1;
        }
        total_returns.push(summary.total_return);
        sharpes.push(summary.sharpe_ratio);
        sortinos.push(summary.sortino_ratio);
        calmars.push(summary.calmar_ratio);
        drawdowns.push(summary.max_drawdown);
    }

    Ok(json!({
        "trials": trials,
        "sample_size": returns.len(),
        "positive_return_probability": positive as f64 / trials as f64,
        "total_return": distribution_summary(&mut total_returns),
        "sharpe_ratio": distribution_summary(&mut sharpes),
        "sortino_ratio": distribution_summary(&mut sortinos),
        "calmar_ratio": distribution_summary(&mut calmars),
        "max_drawdown": distribution_summary(&mut drawdowns)
    }))
}


pub(crate) fn classify_market_scenario(
    summary: &RobustnessMetricSummary,
    annualized_volatility: f64,
) -> &'static str {
    let benchmark_return = summary.benchmark_return.unwrap_or(summary.total_return);
    if annualized_volatility >= 0.30 {
        "high_volatility"
    } else if benchmark_return <= -0.02 || summary.max_drawdown >= 0.20 {
        "bear"
    } else if benchmark_return >= 0.10 && summary.max_drawdown <= 0.15 {
        "bull"
    } else if annualized_volatility <= 0.12 && benchmark_return.abs() <= 0.05 {
        "sideways"
    } else {
        "mixed"
    }
}


pub(crate) fn summarize_points(points: &[RobustnessDailyPoint]) -> RobustnessMetricSummary {
    let portfolio_returns = daily_returns(points, |point| Some(point.portfolio_value));
    let benchmark_returns = daily_returns(points, |point| point.benchmark_value);
    let mut nav = points
        .iter()
        .map(|point| point.portfolio_value)
        .collect::<Vec<_>>();
    let total_return = ratio_return(
        points.first().map(|point| point.portfolio_value),
        points.last().map(|point| point.portfolio_value),
    );
    let benchmark_return = if benchmark_returns.is_empty() {
        None
    } else {
        ratio_return(
            points.first().and_then(|point| point.benchmark_value),
            points.last().and_then(|point| point.benchmark_value),
        )
        .into()
    };
    let sample = summarize_return_sample(&portfolio_returns);
    let max_drawdown = max_drawdown(&mut nav);
    RobustnessMetricSummary {
        total_return,
        annual_return: annualized_return(total_return, points.len().saturating_sub(1)),
        sharpe_ratio: sample.sharpe_ratio,
        sortino_ratio: sample.sortino_ratio,
        calmar_ratio: calmar_ratio(
            annualized_return(total_return, points.len().saturating_sub(1)),
            max_drawdown,
        ),
        max_drawdown,
        benchmark_return,
        excess_return: benchmark_return.map(|value| total_return - value),
    }
}


pub(crate) fn summarize_return_sample(returns: &[f64]) -> RobustnessMetricSummary {
    let total_return = returns.iter().fold(1.0, |acc, value| acc * (1.0 + value)) - 1.0;
    let volatility = annualized_volatility(returns);
    let annual_return = annualized_return(total_return, returns.len());
    let max_drawdown = drawdown_from_returns(returns);
    RobustnessMetricSummary {
        total_return,
        annual_return,
        sharpe_ratio: if volatility > 0.0 {
            annual_return / volatility
        } else {
            0.0
        },
        sortino_ratio: sortino_ratio(annual_return, returns),
        calmar_ratio: calmar_ratio(annual_return, max_drawdown),
        max_drawdown,
        benchmark_return: None,
        excess_return: None,
    }
}


pub(crate) fn metric_summary_json(summary: &RobustnessMetricSummary) -> Value {
    json!({
        "total_return": summary.total_return,
        "annual_return": summary.annual_return,
        "sharpe_ratio": summary.sharpe_ratio,
        "sortino_ratio": summary.sortino_ratio,
        "calmar_ratio": summary.calmar_ratio,
        "max_drawdown": summary.max_drawdown,
        "benchmark_return": summary.benchmark_return,
        "excess_return": summary.excess_return
    })
}


pub(crate) fn daily_returns<F>(points: &[RobustnessDailyPoint], value_fn: F) -> Vec<f64>
where
    F: Fn(&RobustnessDailyPoint) -> Option<f64>,
{
    points
        .windows(2)
        .filter_map(|window| {
            let prev = value_fn(&window[0])?;
            let next = value_fn(&window[1])?;
            if prev.is_finite() && next.is_finite() && prev > 0.0 {
                Some(next / prev - 1.0)
            } else {
                None
            }
        })
        .collect()
}


pub(crate) fn ratio_return(start: Option<f64>, end: Option<f64>) -> f64 {
    match (start, end) {
        (Some(start), Some(end)) if start.is_finite() && end.is_finite() && start > 0.0 => {
            end / start - 1.0
        }
        _ => 0.0,
    }
}


pub(crate) fn annualized_return(total_return: f64, periods: usize) -> f64 {
    if periods == 0 || total_return <= -1.0 {
        return 0.0;
    }
    (1.0 + total_return).powf(252.0 / periods as f64) - 1.0
}


pub(crate) fn annualized_volatility(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    variance.sqrt() * 252.0_f64.sqrt()
}


pub(crate) fn sortino_ratio(annual_return: f64, returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let downside_sum = returns
        .iter()
        .filter(|value| **value < 0.0)
        .map(|value| value * value)
        .sum::<f64>();
    if downside_sum <= 0.0 {
        return if annual_return > 0.0 { 999.0 } else { 0.0 };
    }
    let downside_deviation = (downside_sum / (returns.len() - 1) as f64).sqrt() * 252.0_f64.sqrt();
    if downside_deviation > 0.0 {
        annual_return / downside_deviation
    } else {
        0.0
    }
}


pub(crate) fn calmar_ratio(annual_return: f64, max_drawdown: f64) -> f64 {
    if max_drawdown > 0.0 {
        annual_return / max_drawdown
    } else if annual_return > 0.0 {
        999.0
    } else {
        0.0
    }
}


pub(crate) fn max_drawdown(nav: &mut [f64]) -> f64 {
    let mut peak = None::<f64>;
    let mut max_dd = 0.0;
    for value in nav
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        let current_peak = peak.map(|peak| peak.max(value)).unwrap_or(value);
        peak = Some(current_peak);
        if current_peak > 0.0 {
            let drawdown = (current_peak - value) / current_peak;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
    }
    max_dd
}


pub(crate) fn drawdown_from_returns(returns: &[f64]) -> f64 {
    let mut value = 1.0;
    let mut nav = Vec::with_capacity(returns.len() + 1);
    nav.push(value);
    for daily_return in returns {
        value *= 1.0 + daily_return;
        nav.push(value);
    }
    max_drawdown(&mut nav)
}


pub(crate) fn distribution_summary(values: &mut [f64]) -> Value {
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "p05": percentile(values, 0.05),
        "median": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "mean": if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
    })
}


pub(crate) fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx.min(values.len() - 1)]
}


#[cfg(test)]
pub(crate) fn evaluate_robustness_gates(
    best_score: Decimal,
    runner_up_score: Option<Decimal>,
    metrics: &Value,
    constraint_violations: &Value,
    gate_policy: Option<&Value>,
) -> RobustnessEvaluation {
    evaluate_robustness_gates_with_analysis(
        best_score,
        runner_up_score,
        metrics,
        constraint_violations,
        gate_policy,
        None,
    )
}


pub(crate) fn evaluate_robustness_gates_with_analysis(
    best_score: Decimal,
    runner_up_score: Option<Decimal>,
    metrics: &Value,
    constraint_violations: &Value,
    gate_policy: Option<&Value>,
    analysis: Option<&RobustnessTimeSeriesAnalysis>,
) -> RobustnessEvaluation {
    let min_trade_count = constraint_i64(gate_policy, "min_trade_count").unwrap_or(1);
    let min_annual_return = constraint_decimal(gate_policy, "min_annual_return");
    let min_excess_return = constraint_decimal(gate_policy, "min_excess_return");
    let min_sharpe = constraint_decimal(gate_policy, "min_sharpe");
    let min_sortino = constraint_decimal(gate_policy, "min_sortino");
    let min_calmar = constraint_decimal(gate_policy, "min_calmar");
    let min_profit_factor = constraint_decimal(gate_policy, "min_profit_factor");
    let min_win_rate = constraint_decimal(gate_policy, "min_win_rate");
    let max_drawdown_duration_days = constraint_i64(gate_policy, "max_drawdown_duration_days");
    let max_drawdown =
        constraint_decimal(gate_policy, "max_drawdown").unwrap_or(Decimal::new(20, 2));
    let min_score_gap = constraint_decimal(gate_policy, "min_score_gap").unwrap_or(Decimal::ZERO);
    let min_walk_forward_windows =
        constraint_i64(gate_policy, "min_walk_forward_windows").unwrap_or(0);
    let min_positive_excess_window_ratio =
        constraint_f64(gate_policy, "min_positive_excess_window_ratio").unwrap_or(0.0);
    let min_positive_annual_return_window_ratio =
        constraint_f64(gate_policy, "min_positive_annual_return_window_ratio").unwrap_or(0.0);
    let min_walk_forward_median_sharpe =
        constraint_f64(gate_policy, "min_walk_forward_median_sharpe").unwrap_or(0.0);
    let min_walk_forward_median_calmar =
        constraint_f64(gate_policy, "min_walk_forward_median_calmar");
    let min_bootstrap_positive_return_probability =
        constraint_f64(gate_policy, "min_bootstrap_positive_return_probability").unwrap_or(0.0);
    let min_bootstrap_sharpe_p05 =
        constraint_f64(gate_policy, "min_bootstrap_sharpe_p05").unwrap_or(f64::NEG_INFINITY);
    let min_bootstrap_calmar_p05 = constraint_f64(gate_policy, "min_bootstrap_calmar_p05");
    let max_bootstrap_drawdown_p95 = constraint_f64(gate_policy, "max_bootstrap_drawdown_p95");
    let min_market_scenarios = constraint_i64(gate_policy, "min_market_scenarios").unwrap_or(1);
    let enforce_trial_constraint_violations =
        constraint_bool(gate_policy, "enforce_trial_constraint_violations").unwrap_or(true);

    let num_trades = metrics
        .get("num_trades")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let annual_return =
        decimal_from_json(metrics.get("annual_return_pct")).unwrap_or(Decimal::ZERO);
    let excess_return =
        decimal_from_json(metrics.get("excess_return_pct")).unwrap_or(Decimal::ZERO);
    let sharpe = decimal_from_json(metrics.get("sharpe_ratio")).unwrap_or(Decimal::ZERO);
    let sortino = decimal_from_json(metrics.get("sortino_ratio")).unwrap_or(Decimal::ZERO);
    let calmar = decimal_from_json(metrics.get("calmar_ratio")).unwrap_or(Decimal::ZERO);
    let profit_factor = decimal_from_json(metrics.get("profit_factor")).unwrap_or(Decimal::ZERO);
    let win_rate = decimal_from_json(metrics.get("win_rate_pct")).unwrap_or(Decimal::ZERO);
    let drawdown = decimal_from_json(metrics.get("max_drawdown_pct")).unwrap_or(Decimal::ZERO);
    let drawdown_duration_days = metrics
        .get("max_drawdown_duration_days")
        .and_then(Value::as_i64);
    let hard_violations = constraint_violations
        .as_array()
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let score_gap = runner_up_score.map(|runner_up| best_score - runner_up);

    let mut gates = vec![
        json!({
            "gate": "no_hard_constraint_violations",
            "passed": !enforce_trial_constraint_violations || !hard_violations,
            "enforced": enforce_trial_constraint_violations,
            "actual": constraint_violations,
        }),
        json!({
            "gate": "min_trade_count",
            "passed": num_trades >= min_trade_count,
            "limit": min_trade_count,
            "actual": num_trades,
        }),
        json!({
            "gate": "max_drawdown",
            "passed": drawdown < max_drawdown,
            "limit": max_drawdown,
            "actual": drawdown,
        }),
        json!({
            "gate": "score_gap_vs_runner_up",
            "passed": score_gap.map(|gap| gap >= min_score_gap).unwrap_or(true),
            "limit": min_score_gap,
            "actual": score_gap,
        }),
    ];
    if let Some(coverage) = metrics.get("effective_coverage") {
        gates.push(json!({
            "gate": "effective_coverage_start",
            "passed": true,
            "actual": coverage.get("effective_start_date").cloned().unwrap_or(Value::Null),
            "details": coverage,
        }));
    }
    if let Some(limit) = min_annual_return {
        gates.push(json!({
            "gate": "min_annual_return",
            "passed": annual_return >= limit,
            "limit": limit,
            "actual": annual_return,
        }));
    }
    if let Some(limit) = min_excess_return {
        gates.push(json!({
            "gate": "min_excess_return",
            "passed": excess_return > limit,
            "limit": limit,
            "actual": excess_return,
        }));
    }
    if let Some(limit) = min_sharpe {
        gates.push(json!({
            "gate": "min_sharpe",
            "passed": sharpe > limit,
            "limit": limit,
            "actual": sharpe,
        }));
    }
    if let Some(limit) = min_sortino {
        gates.push(json!({
            "gate": "min_sortino",
            "passed": sortino >= limit,
            "limit": limit,
            "actual": sortino,
        }));
    }
    if let Some(limit) = min_calmar {
        gates.push(json!({
            "gate": "min_calmar",
            "passed": calmar > limit,
            "limit": limit,
            "actual": calmar,
        }));
    }
    if let Some(limit) = min_profit_factor {
        gates.push(json!({
            "gate": "min_profit_factor",
            "passed": profit_factor > limit,
            "limit": limit,
            "actual": profit_factor,
        }));
    }
    if let Some(limit) = min_win_rate {
        gates.push(json!({
            "gate": "min_win_rate",
            "passed": win_rate >= limit,
            "limit": limit,
            "actual": win_rate,
        }));
    }
    if let Some(limit) = max_drawdown_duration_days {
        gates.push(json!({
            "gate": "max_drawdown_duration_days",
            "passed": drawdown_duration_days.map(|actual| actual <= limit).unwrap_or(false),
            "limit": limit,
            "actual": drawdown_duration_days,
        }));
    }
    if let Some(analysis) = analysis {
        let window_count = analysis.walk_forward["window_count"].as_i64().unwrap_or(0);
        let positive_excess_window_ratio = analysis.walk_forward["positive_excess_window_ratio"]
            .as_f64()
            .unwrap_or(0.0);
        let positive_annual_return_ratio = analysis.walk_forward["positive_annual_return_ratio"]
            .as_f64()
            .unwrap_or(0.0);
        let walk_forward_median_sharpe = analysis.walk_forward["sharpe_ratio"]["median"]
            .as_f64()
            .unwrap_or(0.0);
        let walk_forward_median_calmar = analysis.walk_forward["calmar_ratio"]["median"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_positive_return_probability = analysis.bootstrap
            ["positive_return_probability"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_sharpe_p05 = analysis.bootstrap["sharpe_ratio"]["p05"]
            .as_f64()
            .unwrap_or(f64::NEG_INFINITY);
        let bootstrap_calmar_p05 = analysis.bootstrap["calmar_ratio"]["p05"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_drawdown_p95 = analysis.bootstrap["max_drawdown"]["p95"]
            .as_f64()
            .unwrap_or(0.0);
        let market_scenario_count = analysis.walk_forward["scenario_count"]
            .as_i64()
            .or_else(|| analysis.market_scenarios["scenario_count"].as_i64())
            .unwrap_or(0);
        gates.push(json!({
            "gate": "walk_forward_min_window_count",
            "passed": window_count >= min_walk_forward_windows,
            "limit": min_walk_forward_windows,
            "actual": window_count,
            "details": analysis.walk_forward,
        }));
        gates.push(json!({
            "gate": "walk_forward_positive_excess_ratio",
            "passed": positive_excess_window_ratio >= min_positive_excess_window_ratio,
            "limit": min_positive_excess_window_ratio,
            "actual": positive_excess_window_ratio,
        }));
        gates.push(json!({
            "gate": "walk_forward_positive_annual_return_ratio",
            "passed": positive_annual_return_ratio >= min_positive_annual_return_window_ratio,
            "limit": min_positive_annual_return_window_ratio,
            "actual": positive_annual_return_ratio,
            "details": {
                "annual_return": analysis.walk_forward["annual_return"].clone(),
                "window_count": window_count
            },
        }));
        gates.push(json!({
            "gate": "walk_forward_median_sharpe",
            "passed": walk_forward_median_sharpe >= min_walk_forward_median_sharpe,
            "limit": min_walk_forward_median_sharpe,
            "actual": walk_forward_median_sharpe,
            "details": analysis.walk_forward["sharpe_ratio"].clone(),
        }));
        if let Some(limit) = min_walk_forward_median_calmar {
            gates.push(json!({
                "gate": "walk_forward_median_calmar",
                "passed": walk_forward_median_calmar >= limit,
                "limit": limit,
                "actual": walk_forward_median_calmar,
                "details": analysis.walk_forward["calmar_ratio"].clone(),
            }));
        }
        gates.push(json!({
            "gate": "bootstrap_positive_return_probability",
            "passed": bootstrap_positive_return_probability >= min_bootstrap_positive_return_probability,
            "limit": min_bootstrap_positive_return_probability,
            "actual": bootstrap_positive_return_probability,
            "details": analysis.bootstrap,
        }));
        gates.push(json!({
            "gate": "bootstrap_sharpe_p05",
            "passed": bootstrap_sharpe_p05 >= min_bootstrap_sharpe_p05,
            "limit": min_bootstrap_sharpe_p05,
            "actual": bootstrap_sharpe_p05,
            "details": analysis.bootstrap["sharpe_ratio"].clone(),
        }));
        if let Some(limit) = min_bootstrap_calmar_p05 {
            gates.push(json!({
                "gate": "bootstrap_calmar_p05",
                "passed": bootstrap_calmar_p05 >= limit,
                "limit": limit,
                "actual": bootstrap_calmar_p05,
                "details": analysis.bootstrap["calmar_ratio"].clone(),
            }));
        }
        if let Some(limit) = max_bootstrap_drawdown_p95 {
            gates.push(json!({
                "gate": "bootstrap_drawdown_p95",
                "passed": bootstrap_drawdown_p95 <= limit,
                "limit": limit,
                "actual": bootstrap_drawdown_p95,
                "details": analysis.bootstrap["max_drawdown"].clone(),
            }));
        }
        gates.push(json!({
            "gate": "market_scenario_coverage",
            "passed": market_scenario_count >= min_market_scenarios,
            "limit": min_market_scenarios,
            "actual": market_scenario_count,
            "details": analysis.market_scenarios,
        }));
    }
    let any_failed = gates.iter().any(|gate| gate["passed"] == false);
    let has_review_only_failure = gates
        .iter()
        .any(|gate| gate["gate"] == "score_gap_vs_runner_up" && gate["passed"] == false)
        && gates
            .iter()
            .filter(|gate| gate["gate"] != "score_gap_vs_runner_up")
            .all(|gate| gate["passed"] == true);
    let status = if !any_failed {
        "approved_candidate"
    } else if has_review_only_failure {
        "review_required"
    } else {
        "rejected"
    };

    RobustnessEvaluation {
        status: status.to_string(),
        gates: Value::Array(gates),
    }
}


pub(crate) fn build_robustness_failure_attribution(gates: &Value) -> Value {
    let gate_items = gates.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let failed_gates = gate_items
        .iter()
        .filter(|gate| gate["passed"] == false)
        .map(compact_gate_failure)
        .collect::<Vec<_>>();
    let walk_forward_windows = extract_walk_forward_windows(gate_items);
    let worst_walk_forward_windows = rank_weak_walk_forward_windows(&walk_forward_windows, 5);
    let weak_market_scenarios = rank_weak_market_scenarios(&walk_forward_windows, 5);
    let bootstrap_tail = extract_bootstrap_tail(gate_items);
    let primary_failure_modes = build_primary_failure_modes(&failed_gates);

    json!({
        "status": if failed_gates.is_empty() { "no_failed_gates" } else { "has_failed_gates" },
        "failed_gates": failed_gates,
        "primary_failure_modes": primary_failure_modes,
        "worst_walk_forward_windows": worst_walk_forward_windows,
        "weak_market_scenarios": weak_market_scenarios,
        "bootstrap_tail": bootstrap_tail,
    })
}


pub(crate) fn compact_gate_failure(gate: &Value) -> Value {
    json!({
        "gate": gate["gate"].clone(),
        "limit": gate.get("limit").cloned().unwrap_or(Value::Null),
        "actual": gate.get("actual").cloned().unwrap_or(Value::Null),
    })
}


pub(crate) fn build_primary_failure_modes(failed_gates: &[Value]) -> Vec<Value> {
    let mut modes = BTreeSet::new();
    for gate in failed_gates {
        match gate["gate"].as_str().unwrap_or_default() {
            "min_annual_return" => {
                modes.insert("annual_return_shortfall");
            }
            "min_excess_return" => {
                modes.insert("excess_return_shortfall");
            }
            "min_sharpe" => {
                modes.insert("sharpe_shortfall");
            }
            "min_sortino" => {
                modes.insert("sortino_shortfall");
            }
            "min_calmar" | "walk_forward_median_calmar" | "bootstrap_calmar_p05" => {
                modes.insert("calmar_shortfall");
            }
            "min_profit_factor" => {
                modes.insert("profit_factor_shortfall");
            }
            "max_drawdown" => {
                modes.insert("drawdown_excess");
            }
            "max_drawdown_duration_days" => {
                modes.insert("drawdown_duration_excess");
            }
            "bootstrap_drawdown_p95" => {
                modes.insert("bootstrap_drawdown_tail_risk");
            }
            "walk_forward_positive_excess_ratio" | "walk_forward_min_window_count" => {
                modes.insert("walk_forward_instability");
            }
            "walk_forward_positive_annual_return_ratio" | "walk_forward_median_sharpe" => {
                modes.insert("overfit_window_concentration");
            }
            "bootstrap_positive_return_probability" => {
                modes.insert("bootstrap_tail_risk");
            }
            "bootstrap_sharpe_p05" => {
                modes.insert("bootstrap_sharpe_tail_risk");
            }
            "market_scenario_coverage" => {
                modes.insert("market_scenario_coverage_gap");
            }
            "score_gap_vs_runner_up" => {
                modes.insert("runner_up_gap_too_small");
            }
            "no_hard_constraint_violations" => {
                modes.insert("hard_constraint_violation");
            }
            "min_trade_count" => {
                modes.insert("insufficient_trade_sample");
            }
            _ => {
                modes.insert("other_gate_failure");
            }
        }
    }
    modes
        .into_iter()
        .map(|mode| Value::String(mode.to_string()))
        .collect()
}


pub(crate) fn extract_walk_forward_windows(gate_items: &[Value]) -> Vec<Value> {
    gate_items
        .iter()
        .find(|gate| gate["gate"] == "walk_forward_min_window_count")
        .and_then(|gate| gate["details"]["windows"].as_array())
        .cloned()
        .unwrap_or_default()
}


pub(crate) fn rank_weak_walk_forward_windows(windows: &[Value], limit: usize) -> Vec<Value> {
    let mut ranked = windows.to_vec();
    ranked.sort_by(|left, right| {
        weak_window_score(right)
            .partial_cmp(&weak_window_score(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(limit);
    ranked
        .into_iter()
        .map(|window| {
            json!({
                "window_index": window["window_index"].clone(),
                "start_date": window["start_date"].clone(),
                "end_date": window["end_date"].clone(),
                "scenario": window["scenario"].clone(),
                "metrics": window["metrics"].clone(),
                "weakness_score": weak_window_score(&window),
            })
        })
        .collect()
}


pub(crate) fn weak_window_score(window: &Value) -> f64 {
    let annual_return = metric_f64(window, "annual_return").unwrap_or(0.0);
    let excess_return = metric_f64(window, "excess_return").unwrap_or(0.0);
    let sharpe = metric_f64(window, "sharpe_ratio").unwrap_or(0.0);
    let sortino = metric_f64(window, "sortino_ratio").unwrap_or(0.0);
    let drawdown = metric_f64(window, "max_drawdown").unwrap_or(0.0);

    positive_shortfall(0.15, annual_return) * 2.0
        + positive_shortfall(0.0, excess_return) * 3.0
        + positive_shortfall(1.0, sharpe)
        + positive_shortfall(1.5, sortino) * 0.5
        + drawdown
}


pub(crate) fn rank_weak_market_scenarios(windows: &[Value], limit: usize) -> Vec<Value> {
    let mut by_scenario: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for window in windows {
        let scenario = window["scenario"].as_str().unwrap_or("unknown").to_string();
        by_scenario.entry(scenario).or_default().push(window);
    }
    let mut scenarios = by_scenario
        .into_iter()
        .map(|(scenario, windows)| scenario_weakness_summary(&scenario, &windows))
        .collect::<Vec<_>>();
    scenarios.sort_by(|left, right| {
        right["weakness_score"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&left["weakness_score"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scenarios.truncate(limit);
    scenarios
}


pub(crate) fn scenario_weakness_summary(scenario: &str, windows: &[&Value]) -> Value {
    let count = windows.len().max(1);
    let avg_annual_return = average_metric(windows, "annual_return");
    let avg_excess_return = average_metric(windows, "excess_return");
    let avg_sharpe = average_metric(windows, "sharpe_ratio");
    let avg_sortino = average_metric(windows, "sortino_ratio");
    let worst_drawdown = windows
        .iter()
        .filter_map(|window| metric_f64(window, "max_drawdown"))
        .fold(0.0_f64, f64::max);
    let weakness_score = windows
        .iter()
        .map(|window| weak_window_score(window))
        .sum::<f64>()
        / count as f64;
    let negative_excess_window_count = windows
        .iter()
        .filter(|window| metric_f64(window, "excess_return").unwrap_or(0.0) < 0.0)
        .count();

    json!({
        "scenario": scenario,
        "window_count": windows.len(),
        "negative_excess_window_count": negative_excess_window_count,
        "avg_annual_return": avg_annual_return,
        "avg_excess_return": avg_excess_return,
        "avg_sharpe_ratio": avg_sharpe,
        "avg_sortino_ratio": avg_sortino,
        "worst_drawdown": worst_drawdown,
        "weakness_score": weakness_score,
    })
}


pub(crate) fn average_metric(windows: &[&Value], metric: &str) -> f64 {
    let values = windows
        .iter()
        .filter_map(|window| metric_f64(window, metric))
        .collect::<Vec<_>>();
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}


pub(crate) fn extract_bootstrap_tail(gate_items: &[Value]) -> Value {
    gate_items
        .iter()
        .find(|gate| gate["gate"] == "bootstrap_positive_return_probability")
        .map(|gate| {
            json!({
                "positive_return_probability": gate["details"]["positive_return_probability"].clone(),
                "total_return": gate["details"]["total_return"].clone(),
                "sharpe_ratio": gate["details"]["sharpe_ratio"].clone(),
                "sortino_ratio": gate["details"]["sortino_ratio"].clone(),
                "calmar_ratio": gate["details"]["calmar_ratio"].clone(),
                "max_drawdown": gate["details"]["max_drawdown"].clone(),
            })
        })
        .unwrap_or_else(|| json!({}))
}


pub(crate) fn metric_f64(window: &Value, metric: &str) -> Option<f64> {
    value_as_f64(window.get("metrics")?.get(metric)?)
}


pub(crate) fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}


pub(crate) fn positive_shortfall(limit: f64, actual: f64) -> f64 {
    (limit - actual).max(0.0)
}


pub(crate) fn decimal_from_json(value: Option<&Value>) -> Option<Decimal> {
    match value {
        Some(Value::Number(number)) => number.as_f64().and_then(Decimal::from_f64_retain),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}


pub(crate) fn constraint_decimal(constraints: Option<&Value>, name: &str) -> Option<Decimal> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
        .and_then(Decimal::from_f64_retain)
}


pub(crate) fn constraint_i64(constraints: Option<&Value>, name: &str) -> Option<i64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_i64)
}


pub(crate) fn constraint_f64(constraints: Option<&Value>, name: &str) -> Option<f64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
}


pub(crate) fn constraint_bool(constraints: Option<&Value>, name: &str) -> Option<bool> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_bool)
}


pub(crate) fn constraint_str<'a>(constraints: Option<&'a Value>, name: &str) -> Option<&'a str> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_str)
}


