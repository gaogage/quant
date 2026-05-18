//! Optimization API routes — create optimization tasks and parameter trials

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use quant_common::phase7::{
    build_layered_search_plan, CandidateMetrics, CandidateTargets, CandidateType,
    LayeredSearchConfig, LayeredSearchPlan, LocalResourcePlan,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::routes::backtest::{
    execute_factor_backtest_with_caches, execute_prediction_backtest, MarketRegimeBacktestReq,
    RunFactorBacktestReq, RunPredictionBacktestReq,
};
use crate::AppState;
use quant_backtest::runner::BacktestDataCache;
use quant_backtest::signal_generator::SignalDataCache;

#[derive(Debug, Deserialize)]
pub struct CreateOptimizationRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub search_method: String,
    pub search_space: Value,
    pub objective: Value,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    #[serde(default = "default_random_seed")]
    pub random_seed: u64,
    #[serde(default = "default_max_trials")]
    pub max_trials: usize,
}

#[derive(Debug, Deserialize)]
pub struct Phase7LayeredOptimizationRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Value,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials: Option<usize>,
    pub search_profile: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7ProfessionalDiscoveryRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Option<Value>,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials: Option<usize>,
    pub search_profile: Option<String>,
    pub trial_batch_limit: Option<i64>,
    pub max_batches: Option<usize>,
    pub robustness_top_n: Option<usize>,
    pub robustness_gate_policy: Option<Value>,
    pub stop_after_professional_candidate: Option<bool>,
    pub stop_after_robust_approval: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TrialListQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct RunOptimizationRequest {
    pub trial_limit: Option<i64>,
    pub performance_gate: Option<OptimizationPerformanceGateRequest>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OptimizationPerformanceGateRequest {
    pub min_completed_trials: Option<i64>,
    pub max_failed_trials: Option<i64>,
    pub max_elapsed_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PromoteOptimizationRequest {
    pub trial_id: Option<String>,
    pub target_strategy_version: String,
    pub candidate_name: String,
    pub promotion_mode: Option<String>,
    pub gate_policy: Option<String>,
    pub freeze_after_approval: Option<bool>,
    pub reviewer: Option<String>,
    pub reason: String,
    pub notes: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRobustnessRequest {
    pub gate_policy: Option<Value>,
}

struct OptimizationTaskExecutionContext {
    strategy_version_id: String,
    data_version_id: String,
    backtest_template: Value,
    objective: Value,
    constraints: Option<Value>,
}

struct ScoredTrial {
    score: Decimal,
    metrics: Value,
    constraint_violations: Value,
}

struct RobustnessEvaluation {
    status: String,
    gates: Value,
}

#[derive(Debug, Clone)]
struct RobustnessDailyPoint {
    trade_date: NaiveDate,
    portfolio_value: f64,
    benchmark_value: Option<f64>,
}

#[derive(Debug, Clone)]
struct RobustnessMetricSummary {
    total_return: f64,
    annual_return: f64,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    max_drawdown: f64,
    benchmark_return: Option<f64>,
    excess_return: Option<f64>,
}

#[derive(Debug, Clone)]
struct RobustnessTimeSeriesAnalysis {
    market_scenarios: Value,
    walk_forward: Value,
    bootstrap: Value,
}

struct OptimizationPerformanceGatePolicy {
    min_completed_trials: i64,
    max_failed_trials: i64,
    max_elapsed_ms: Option<i64>,
}

struct NormalizedPromoteRequest {
    trial_id: String,
    target_strategy_version: String,
    candidate_name: String,
    promotion_mode: String,
    gate_policy: String,
    freeze_after_approval: bool,
    reviewer: Option<String>,
    reason: String,
    notes: Option<String>,
    status: String,
}

struct Phase7LayeredPlanBundle {
    resource_plan: LocalResourcePlan,
    plan: LayeredSearchPlan,
    search_space: Value,
}

#[derive(Debug, Clone)]
struct DiscoveryCandidate {
    trial_id: String,
    backtest_task_id: Option<String>,
    score: Option<Decimal>,
    candidate_type: CandidateType,
    professional_gap_score: Decimal,
    metrics: CandidateMetrics,
    parameters: Value,
}

enum OptimizationTrialBacktestRequest {
    Factor(RunFactorBacktestReq),
    Prediction(RunPredictionBacktestReq),
}

fn default_random_seed() -> u64 {
    42
}

fn default_max_trials() -> usize {
    20
}

fn normalize_max_trials(value: usize) -> usize {
    value.clamp(1, 500)
}

fn normalize_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 500)
}

fn default_professional_robustness_policy() -> Value {
    json!({
        "min_trade_count": 1,
        "min_annual_return": 0.15,
        "min_excess_return": 0.0,
        "min_sharpe": 1.0,
        "min_sortino": 1.5,
        "max_drawdown": 0.35,
        "min_score_gap": 0.0,
        "walk_forward_window_days": 756,
        "walk_forward_step_days": 63,
        "min_walk_forward_windows": 4,
        "min_positive_excess_window_ratio": 0.50,
        "min_bootstrap_positive_return_probability": 0.70,
        "min_market_scenarios": 2,
        "bootstrap_trials": 512,
        "bootstrap_seed": 42
    })
}

fn phase7_search_config(search_profile: Option<&str>) -> (String, LayeredSearchConfig) {
    match search_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("local_professional")
    {
        "professional_risk_breakthrough"
        | "risk_breakthrough"
        | "drawdown_sortino_breakthrough"
        | "phase7_risk_breakthrough" => (
            "professional_risk_breakthrough".to_string(),
            LayeredSearchConfig::professional_risk_breakthrough_default(),
        ),
        "professional_breakthrough" | "breakthrough" | "phase7_breakthrough" => (
            "professional_breakthrough".to_string(),
            LayeredSearchConfig::professional_breakthrough_default(),
        ),
        _ => (
            "local_professional".to_string(),
            LayeredSearchConfig::local_professional_default(),
        ),
    }
}

fn build_phase7_layered_plan_bundle(
    req: &Phase7LayeredOptimizationRequest,
    mut resource_plan: LocalResourcePlan,
) -> Phase7LayeredPlanBundle {
    if let Some(max_trials) = req.max_trials {
        resource_plan.max_trials = normalize_max_trials(max_trials);
    }

    let (search_profile, mut config) = phase7_search_config(req.search_profile.as_deref());
    if let Some(prediction_set_ids) = req.prediction_set_ids.as_ref() {
        config.prediction_set_ids = normalize_prediction_set_ids(prediction_set_ids);
    }
    let plan = build_layered_search_plan(&config, &resource_plan);
    let search_space = json!({
        "phase": "7-D",
        "search_method": "phase7_layered_grid",
        "search_profile": search_profile,
        "config": config,
        "resource_plan": resource_plan.clone(),
        "requested_trials": plan.requested_trials,
        "planned_trials": plan.planned_trials,
        "truncated": plan.truncated,
    });

    Phase7LayeredPlanBundle {
        resource_plan,
        plan,
        search_space,
    }
}

fn phase7_discovery_layered_request(
    req: &Phase7ProfessionalDiscoveryRequest,
) -> Phase7LayeredOptimizationRequest {
    Phase7LayeredOptimizationRequest {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        objective: req.objective.clone().unwrap_or_else(|| {
            json!({
                "type": "professional_candidate",
                "benchmark": "000300.SH",
                "maximize": true
            })
        }),
        constraints: req.constraints.clone().or_else(|| {
            Some(json!({
                "min_annual_return": 0.15,
                "min_excess_return": 0.0,
                "min_sharpe": 1.0,
                "min_sortino": 1.5,
                "max_drawdown": 0.35,
                "min_trade_count": 1
            }))
        }),
        walk_forward: req.walk_forward.clone(),
        backtest_template: req.backtest_template.clone(),
        prediction_set_ids: req.prediction_set_ids.clone(),
        max_trials: req.max_trials,
        search_profile: Some(
            req.search_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("professional_risk_breakthrough")
                .to_string(),
        ),
    }
}

fn normalize_prediction_set_ids(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

pub async fn create_optimization(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateOptimizationRequest>,
) -> impl IntoResponse {
    if req.search_method != "random_search" && req.search_method != "grid_search" {
        return Json(
            json!({"code": 1, "message": format!("unsupported search_method: {}", req.search_method)}),
        );
    }

    let max_trials = normalize_max_trials(req.max_trials);
    let trial_params =
        match generate_trial_parameters(&req.search_space, req.random_seed, max_trials) {
            Ok(params) => params,
            Err(message) => return Json(json!({"code": 1, "message": message})),
        };

    let task_id = format!("opt-{}", Uuid::new_v4());
    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to begin optimization transaction: {}", error)}),
            )
        }
    };

    let insert_task = sqlx::query(
        "INSERT INTO optimization_task
           (optimization_task_id, strategy_version_id, data_version_id, search_method,
            search_space, objective, constraints, walk_forward_config, backtest_template, status, progress)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', 0)",
    )
    .bind(&task_id)
    .bind(&req.strategy_version_id)
    .bind(&req.data_version_id)
    .bind(&req.search_method)
    .bind(&req.search_space)
    .bind(&req.objective)
    .bind(&req.constraints)
    .bind(&req.walk_forward)
    .bind(&req.backtest_template)
    .execute(&mut *tx)
    .await;

    if let Err(error) = insert_task {
        return Json(
            json!({"code": 1, "message": format!("Failed to create optimization task: {}", error)}),
        );
    }

    for (idx, params) in trial_params.iter().enumerate() {
        let trial_id = format!("trial-{}-{:04}", task_id, idx + 1);
        if let Err(error) = sqlx::query(
            "INSERT INTO optimization_trial
               (trial_id, optimization_task_id, trial_index, parameters, status, progress)
             VALUES ($1, $2, $3, $4, 'pending', 0)",
        )
        .bind(&trial_id)
        .bind(&task_id)
        .bind((idx + 1) as i32)
        .bind(params)
        .execute(&mut *tx)
        .await
        {
            return Json(
                json!({"code": 1, "message": format!("Failed to create optimization trial: {}", error)}),
            );
        }
    }

    if let Err(error) = tx.commit().await {
        return Json(
            json!({"code": 1, "message": format!("Failed to commit optimization task: {}", error)}),
        );
    }

    Json(json!({
        "code": 0,
        "data": {
            "optimization_task_id": task_id,
            "status": "pending",
            "max_trials": max_trials,
            "trial_count": trial_params.len(),
        }
    }))
}

pub async fn create_phase7_layered_optimization(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7LayeredOptimizationRequest>,
) -> impl IntoResponse {
    match insert_phase7_layered_optimization(&state.db, &req, LocalResourcePlan::local_mac()).await
    {
        Ok((task_id, bundle)) => Json(json!({
            "code": 0,
            "data": {
                "optimization_task_id": task_id,
                "status": "pending",
                "search_method": "phase7_layered_grid",
                "requested_trials": bundle.plan.requested_trials,
                "planned_trials": bundle.plan.planned_trials,
                "truncated": bundle.plan.truncated,
                "batch_size": bundle.plan.batch_size,
                "max_parallel_trials": bundle.plan.max_parallel_trials,
                "resource_plan": bundle.resource_plan,
                "search_profile": bundle.search_space["search_profile"],
            }
        })),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn insert_phase7_layered_optimization(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    let bundle = build_phase7_layered_plan_bundle(req, resource_plan);
    let task_id = format!("opt-phase7-{}", Uuid::new_v4());
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin phase7 optimization transaction: {}", error))?;

    let insert_task = sqlx::query(
        "INSERT INTO optimization_task
           (optimization_task_id, strategy_version_id, data_version_id, search_method,
            search_space, objective, constraints, walk_forward_config, backtest_template, status, progress)
         VALUES ($1, $2, $3, 'phase7_layered_grid', $4, $5, $6, $7, $8, 'pending', 0)",
    )
    .bind(&task_id)
    .bind(&req.strategy_version_id)
    .bind(&req.data_version_id)
    .bind(&bundle.search_space)
    .bind(&req.objective)
    .bind(&req.constraints)
    .bind(&req.walk_forward)
    .bind(&req.backtest_template)
    .execute(&mut *tx)
    .await;

    if let Err(error) = insert_task {
        return Err(format!(
            "Failed to create phase7 optimization task: {}",
            error
        ));
    }

    for trial in &bundle.plan.trials {
        let trial_id = format!("trial-{}-{:04}", task_id, trial.trial_index + 1);
        if let Err(error) = sqlx::query(
            "INSERT INTO optimization_trial
               (trial_id, optimization_task_id, trial_index, parameters, status, progress)
             VALUES ($1, $2, $3, $4, 'pending', 0)",
        )
        .bind(&trial_id)
        .bind(&task_id)
        .bind((trial.trial_index + 1) as i32)
        .bind(&trial.parameters)
        .execute(&mut *tx)
        .await
        {
            return Err(format!(
                "Failed to create phase7 optimization trial: {}",
                error
            ));
        }
    }

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit phase7 optimization task: {}", error))?;

    Ok((task_id, bundle))
}

pub async fn get_optimization(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            Value,
            Value,
            Option<Value>,
            Option<Value>,
            Option<Value>,
            String,
            Option<String>,
            i32,
            Option<chrono::DateTime<chrono::Utc>>,
        ),
    >(
        "SELECT optimization_task_id, strategy_version_id, data_version_id, search_method,
           search_space, objective, constraints, walk_forward_config, backtest_template, status,
           best_trial_id, progress, created_at
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await;

    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return Json(json!({"code": 1, "message": "optimization task not found"})),
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to get optimization task: {}", error)}),
            )
        }
    };

    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
           COUNT(*) FILTER (WHERE status = 'completed')::bigint,
           COUNT(*) FILTER (WHERE status = 'failed')::bigint
         FROM optimization_trial
         WHERE optimization_task_id = $1",
    )
    .bind(&task_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or((0, 0, 0));

    Json(json!({
        "code": 0,
        "data": {
            "optimization_task_id": row.0,
            "strategy_version_id": row.1,
            "data_version_id": row.2,
            "search_method": row.3,
            "search_space": row.4,
            "objective": row.5,
            "constraints": row.6,
            "walk_forward_config": row.7,
            "backtest_template": row.8,
            "status": row.9,
            "best_trial_id": row.10,
            "progress": row.11,
            "created_at": row.12.map(|ts| ts.to_rfc3339()),
            "trial_total": counts.0,
            "trial_completed": counts.1,
            "trial_failed": counts.2,
        }
    }))
}

pub async fn run_optimization_trials(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<RunOptimizationRequest>,
) -> impl IntoResponse {
    let trial_limit = normalize_limit(req.trial_limit);
    match execute_pending_trials(
        &state.db,
        &task_id,
        trial_limit,
        req.performance_gate.as_ref(),
    )
    .await
    {
        Ok(summary) => Json(json!({"code": 0, "data": summary})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn run_phase7_professional_discovery(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ProfessionalDiscoveryRequest>,
) -> impl IntoResponse {
    match execute_phase7_professional_discovery(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn promote_optimization_trial(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<PromoteOptimizationRequest>,
) -> impl IntoResponse {
    match promote_trial(&state.db, &task_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn execute_phase7_professional_discovery(
    db: &sqlx::PgPool,
    req: Phase7ProfessionalDiscoveryRequest,
) -> Result<Value, String> {
    let layered_req = phase7_discovery_layered_request(&req);
    let (task_id, bundle) =
        insert_phase7_layered_optimization(db, &layered_req, LocalResourcePlan::local_mac())
            .await?;
    let trial_batch_limit = req
        .trial_batch_limit
        .unwrap_or(bundle.plan.batch_size as i64)
        .clamp(1, 500);
    let max_batches = req.max_batches.unwrap_or_else(|| {
        let planned = bundle.plan.planned_trials.max(1);
        let batch = trial_batch_limit.max(1) as usize;
        planned.div_ceil(batch)
    });
    let max_batches = max_batches.clamp(1, 10_000);
    let robustness_top_n = req.robustness_top_n.unwrap_or(3).clamp(1, 20);
    let targets = CandidateTargets::default();
    let gate_policy = req
        .robustness_gate_policy
        .clone()
        .unwrap_or_else(default_professional_robustness_policy);
    let stop_after_professional = req.stop_after_professional_candidate.unwrap_or(true);
    let stop_after_robust_approval = req.stop_after_robust_approval.unwrap_or(true);

    let mut batch_summaries = Vec::new();
    let mut robustness_results = Vec::new();
    let mut evaluated_robustness_trials = BTreeSet::new();
    let mut approved_candidate_found = false;
    let mut stop_reason = "planned_trials_exhausted";
    for _ in 0..max_batches {
        let summary = execute_pending_trials(db, &task_id, trial_batch_limit, None).await?;
        let executed = summary["executed"].as_i64().unwrap_or(0);
        batch_summaries.push(summary);
        let candidates =
            load_discovery_candidates(db, &task_id, &targets, robustness_top_n).await?;
        if stop_after_professional
            && candidates
                .iter()
                .any(|candidate| candidate.candidate_type == CandidateType::Professional)
        {
            let professional_candidates = candidates
                .iter()
                .filter(|candidate| candidate.candidate_type == CandidateType::Professional)
                .cloned()
                .collect::<Vec<_>>();
            let batch_robustness_results = evaluate_discovery_candidates(
                db,
                &task_id,
                &professional_candidates,
                &gate_policy,
                &mut evaluated_robustness_trials,
            )
            .await?;
            approved_candidate_found = batch_robustness_results
                .iter()
                .any(robustness_result_is_approved);
            robustness_results.extend(batch_robustness_results);
            if stop_after_robust_approval {
                if approved_candidate_found {
                    stop_reason = "robust_professional_candidate_approved";
                    break;
                }
            } else {
                stop_reason = "professional_candidate_found_before_robust_approval";
                break;
            }
        }
        if executed == 0 {
            stop_reason = "no_pending_trials";
            break;
        }
    }

    let candidates = load_discovery_candidates(db, &task_id, &targets, robustness_top_n).await?;
    let final_robustness_results = evaluate_discovery_candidates(
        db,
        &task_id,
        &candidates,
        &gate_policy,
        &mut evaluated_robustness_trials,
    )
    .await?;
    approved_candidate_found = approved_candidate_found
        || final_robustness_results
            .iter()
            .any(robustness_result_is_approved);
    robustness_results.extend(final_robustness_results);

    Ok(json!({
        "optimization_task_id": task_id,
        "search_method": "phase7_professional_discovery",
        "search_profile": bundle.search_space["search_profile"],
        "requested_trials": bundle.plan.requested_trials,
        "planned_trials": bundle.plan.planned_trials,
        "trial_batch_limit": trial_batch_limit,
        "executed_batches": batch_summaries.len(),
        "batch_summaries": batch_summaries,
        "stop_reason": stop_reason,
        "stop_after_robust_approval": stop_after_robust_approval,
        "approved_candidate_found": approved_candidate_found,
        "robustness_top_n": robustness_top_n,
        "robustness_gate_policy": gate_policy,
        "robustness_candidates": candidates
            .iter()
            .map(discovery_candidate_json)
            .collect::<Vec<_>>(),
        "robustness_results": robustness_results,
    }))
}

async fn evaluate_discovery_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    candidates: &[DiscoveryCandidate],
    gate_policy: &Value,
    evaluated_trial_ids: &mut BTreeSet<String>,
) -> Result<Vec<Value>, String> {
    let mut results = Vec::new();
    for candidate in candidates {
        if !evaluated_trial_ids.insert(candidate.trial_id.clone()) {
            continue;
        }
        results.push(
            evaluate_and_persist_robustness_for_trial(
                db,
                task_id,
                &candidate.trial_id,
                Some(gate_policy),
                "Professional discovery robustness gate evaluated",
            )
            .await?,
        );
    }
    Ok(results)
}

fn robustness_result_is_approved(result: &Value) -> bool {
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

pub async fn list_optimization_trials(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Query(query): Query<TrialListQuery>,
) -> impl IntoResponse {
    let status = query
        .status
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let limit = normalize_limit(query.limit);

    let rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            Value,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            String,
            i32,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT trial_id, trial_index, parameters, score, metrics, constraint_violations,
           status, progress, backtest_task_id, error_message
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND ($2::text IS NULL OR status = $2)
         ORDER BY trial_index ASC
         LIMIT $3",
    )
    .bind(&task_id)
    .bind(status.as_deref())
    .bind(limit)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => Json(json!({
            "code": 0,
            "data": {
                "optimization_task_id": task_id,
                "trials": rows.into_iter().map(|row| json!({
                    "trial_id": row.0,
                    "trial_index": row.1,
                    "parameters": row.2,
                    "score": row.3,
                    "metrics": row.4,
                    "constraint_violations": row.5,
                    "status": row.6,
                    "progress": row.7,
                    "backtest_task_id": row.8,
                    "error_message": row.9,
                })).collect::<Vec<_>>()
            }
        })),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to list optimization trials: {}", error)}),
        ),
    }
}

fn generate_trial_parameters(
    search_space: &Value,
    seed: u64,
    max_trials: usize,
) -> Result<Vec<Value>, String> {
    let specs = search_space
        .as_object()
        .ok_or_else(|| "search_space must be a JSON object".to_string())?;
    let max_trials = normalize_max_trials(max_trials);
    let mut rng = DeterministicRng::new(seed);
    let mut trials = Vec::with_capacity(max_trials);

    for _ in 0..max_trials {
        let mut params = Map::new();
        for (name, spec) in specs {
            params.insert(name.clone(), sample_parameter(name, spec, &mut rng)?);
        }
        trials.push(Value::Object(params));
    }

    Ok(trials)
}

async fn execute_pending_trials(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    performance_gate: Option<&OptimizationPerformanceGateRequest>,
) -> Result<Value, String> {
    let started = Instant::now();
    let gate_policy = normalize_performance_gate(performance_gate, trial_limit)?;
    let task = load_execution_context(db, task_id).await?;
    let pending_trials = sqlx::query_as::<_, (String, i32, Value)>(
        "SELECT trial_id, trial_index, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'pending'
         ORDER BY trial_index ASC
         LIMIT $2",
    )
    .bind(task_id)
    .bind(trial_limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load pending optimization trials: {}", error))?;

    if pending_trials.is_empty() {
        refresh_task_progress(db, task_id).await?;
        let elapsed_ms = started.elapsed().as_millis() as i64;
        let best_trial_id = Value::Null;
        let gates = evaluate_optimization_performance_gates(0, 0, 0, elapsed_ms, &gate_policy);
        let gate_status = performance_gate_status(&gates);
        let experiment_run_id = persist_optimization_experiment_run(
            db,
            task_id,
            trial_limit,
            0,
            0,
            0,
            elapsed_ms,
            &best_trial_id,
            &gate_policy,
            &gates,
            gate_status,
            None,
            None,
        )
        .await?;
        return Ok(json!({
            "optimization_task_id": task_id,
            "executed": 0,
            "completed": 0,
            "failed": 0,
            "best_trial_id": best_trial_id,
            "elapsed_ms": elapsed_ms,
            "experiment_run_id": experiment_run_id,
            "performance_gate_status": gate_status,
            "performance_gates": gates,
        }));
    }

    sqlx::query(
        "UPDATE optimization_task
         SET status = 'running', last_heartbeat_at = now()
         WHERE optimization_task_id = $1 AND status IN ('pending', 'running', 'partial')",
    )
    .bind(task_id)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark optimization task running: {}", error))?;

    let mut completed = 0;
    let mut failed = 0;
    let mut signal_cache = SignalDataCache::default();
    let mut backtest_cache = BacktestDataCache::default();
    for (trial_id, _trial_index, params) in pending_trials.iter() {
        let backtest_task_id = format!("optbt-{}", Uuid::new_v4());
        if let Err(error) = mark_trial_running(db, trial_id).await {
            failed += 1;
            let _ = mark_trial_failed(db, trial_id, &error).await;
            continue;
        }

        if let Some(reused) = find_reusable_trial(db, task_id, &task, trial_id, params).await? {
            mark_trial_reused(db, trial_id, &reused).await?;
            completed += 1;
            continue;
        }

        let request = match build_optimization_trial_request(&task, params) {
            Ok(request) => request,
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
                continue;
            }
        };

        let execution_result = match request {
            OptimizationTrialBacktestRequest::Factor(request) => {
                execute_factor_backtest_with_caches(
                    db,
                    &backtest_task_id,
                    request,
                    Some(&mut signal_cache),
                    Some(&mut backtest_cache),
                )
                .await
            }
            OptimizationTrialBacktestRequest::Prediction(request) => {
                execute_prediction_backtest(db, &backtest_task_id, request).await
            }
        };

        match execution_result {
            Ok(output) => {
                let scored =
                    score_trial(&output.metrics, &task.objective, task.constraints.as_ref());
                mark_trial_completed(db, trial_id, &backtest_task_id, &scored).await?;
                completed += 1;
            }
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
            }
        }
    }

    let best_trial_id = refresh_task_progress(db, task_id).await?;
    let elapsed_ms = started.elapsed().as_millis() as i64;
    let signal_cache_stats = json!(signal_cache.stats());
    let backtest_cache_stats = json!(backtest_cache.stats());
    let gates = evaluate_optimization_performance_gates(
        pending_trials.len() as i64,
        completed,
        failed,
        elapsed_ms,
        &gate_policy,
    );
    let gate_status = performance_gate_status(&gates);
    let best_trial_value = json!(best_trial_id);
    let experiment_run_id = persist_optimization_experiment_run(
        db,
        task_id,
        trial_limit,
        pending_trials.len() as i64,
        completed,
        failed,
        elapsed_ms,
        &best_trial_value,
        &gate_policy,
        &gates,
        gate_status,
        Some(&signal_cache_stats),
        Some(&backtest_cache_stats),
    )
    .await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "executed": pending_trials.len(),
        "completed": completed,
        "failed": failed,
        "best_trial_id": best_trial_id,
        "elapsed_ms": elapsed_ms,
        "experiment_run_id": experiment_run_id,
        "performance_gate_status": gate_status,
        "performance_gates": gates,
        "signal_cache": signal_cache_stats,
        "backtest_cache": backtest_cache_stats,
    }))
}

fn normalize_performance_gate(
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

fn evaluate_optimization_performance_gates(
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

fn performance_gate_status(gates: &Value) -> &'static str {
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

async fn persist_optimization_experiment_run(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    executed: i64,
    completed: i64,
    failed: i64,
    elapsed_ms: i64,
    best_trial_id: &Value,
    policy: &OptimizationPerformanceGatePolicy,
    gates: &Value,
    gate_status: &str,
    signal_cache_stats: Option<&Value>,
    backtest_cache_stats: Option<&Value>,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let throughput = if elapsed_ms > 0 {
        Some(executed as f64 * 1000.0 / elapsed_ms as f64)
    } else {
        None
    };
    let config = json!({
        "optimization_task_id": task_id,
        "trial_limit": trial_limit,
        "gate_policy": {
            "min_completed_trials": policy.min_completed_trials,
            "max_failed_trials": policy.max_failed_trials,
            "max_elapsed_ms": policy.max_elapsed_ms,
        }
    });
    let metrics = json!({
        "executed": executed,
        "completed": completed,
        "failed": failed,
        "elapsed_ms": elapsed_ms,
        "throughput_trials_per_sec": throughput,
        "best_trial_id": best_trial_id,
        "gate_status": gate_status,
        "gates": gates,
        "signal_cache": signal_cache_stats,
        "backtest_cache": backtest_cache_stats,
    });
    let experiment_status = if gate_status == "passed" {
        "completed"
    } else if completed > 0 || failed > 0 || executed > 0 {
        "partial"
    } else {
        "failed"
    };

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'optimization_trial_batch_release_gate', 'optimization_task', $2,
                 $3, $4, $5, now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(task_id)
    .bind(&config)
    .bind(&metrics)
    .bind(experiment_status)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert optimization experiment_run: {}", error))?;

    Ok(experiment_run_id)
}

async fn promote_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    req: PromoteOptimizationRequest,
) -> Result<Value, String> {
    let best_trial_id = sqlx::query_as::<_, (Option<String>, String, String)>(
        "SELECT best_trial_id, strategy_version_id, data_version_id
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    let default_trial_id = best_trial_id.0.as_deref().ok_or_else(|| {
        "optimization task has no best_trial_id; run completed trials first".to_string()
    })?;
    let normalized = normalize_promote_request(default_trial_id, req)?;

    let trial = sqlx::query_as::<
        _,
        (
            String,
            Value,
            Option<Value>,
            Option<Decimal>,
            Option<Value>,
            String,
        ),
    >(
        "SELECT trial_id, parameters, metrics, score, constraint_violations, status
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND trial_id = $2",
    )
    .bind(task_id)
    .bind(&normalized.trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization trial: {}", error))?
    .ok_or_else(|| "optimization trial not found".to_string())?;

    if trial.5 != "completed" {
        return Err("only completed trials can be promoted".to_string());
    }
    let score = trial
        .3
        .ok_or_else(|| "completed trial has no score".to_string())?;

    let candidate_id = format!("candidate-{}", Uuid::new_v4());
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin promote transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO strategy_parameter_candidate
           (candidate_id, candidate_name, target_strategy_version, strategy_version_id,
            optimization_task_id, trial_id, parameters, metrics, objective_score,
            constraints, gate_policy, promotion_mode, status, reviewer, reason, notes)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
    )
    .bind(&candidate_id)
    .bind(&normalized.candidate_name)
    .bind(&normalized.target_strategy_version)
    .bind(&best_trial_id.1)
    .bind(task_id)
    .bind(&normalized.trial_id)
    .bind(&trial.1)
    .bind(&trial.2)
    .bind(score)
    .bind(&trial.4)
    .bind(&normalized.gate_policy)
    .bind(&normalized.promotion_mode)
    .bind(&normalized.status)
    .bind(&normalized.reviewer)
    .bind(&normalized.reason)
    .bind(&normalized.notes)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to create strategy parameter candidate: {}", error))?;

    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, 'strategy_parameter_candidate.promote', 'strategy_parameter_candidate',
                 $2, $3, $4, $5)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(&candidate_id)
    .bind(&normalized.reviewer)
    .bind(format!(
        "Promoted optimization trial {} to candidate {}",
        normalized.trial_id, candidate_id
    ))
    .bind(json!({
        "optimization_task_id": task_id,
        "trial_id": normalized.trial_id,
        "target_strategy_version": normalized.target_strategy_version,
        "gate_policy": normalized.gate_policy,
        "freeze_after_approval": normalized.freeze_after_approval,
    }))
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to write promote audit event: {}", error))?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit promote transaction: {}", error))?;

    Ok(json!({
        "candidate_id": candidate_id,
        "status": normalized.status,
        "source_trial_id": normalized.trial_id,
        "optimization_task_id": task_id,
        "target_strategy_version": normalized.target_strategy_version,
        "gate_policy": normalized.gate_policy,
        "requires_manual_review": true,
    }))
}

async fn evaluate_and_persist_robustness(
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

async fn evaluate_and_persist_robustness_for_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_id: &str,
    gate_policy: Option<&Value>,
    summary_prefix: &str,
) -> Result<Value, String> {
    let trial = sqlx::query_as::<_, (Decimal, Option<Value>, Option<Value>, Option<String>)>(
        "SELECT score, metrics, constraint_violations, backtest_task_id
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
         WHERE optimization_task_id = $1 AND trial_id <> $2 AND status = 'completed' AND score IS NOT NULL
         ORDER BY score DESC NULLS LAST, trial_index ASC
         LIMIT 1",
    )
    .bind(task_id)
    .bind(trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load runner-up optimization trial: {}", error))?
    .map(|row| row.0);

    let policy = gate_policy
        .cloned()
        .unwrap_or_else(|| default_professional_robustness_policy());
    let analysis = if let Some(backtest_task_id) = trial.3.as_deref() {
        load_robustness_timeseries_analysis(db, backtest_task_id, &policy).await?
    } else {
        None
    };
    let evaluation = evaluate_robustness_gates_with_analysis(
        trial.0,
        runner_up,
        trial.1.as_ref().unwrap_or(&json!({})),
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
    }))
}

async fn load_discovery_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    targets: &CandidateTargets,
    limit: usize,
) -> Result<Vec<DiscoveryCandidate>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            Option<String>,
            Value,
        ),
    >(
        "SELECT trial_id, score, metrics, constraint_violations, backtest_task_id, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed' AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load professional discovery candidates: {}",
            error
        )
    })?;

    let mut candidates = rows
        .into_iter()
        .filter_map(
            |(trial_id, score, metrics, _violations, backtest_task_id, parameters)| {
                let metrics_json = metrics?;
                let candidate_metrics = CandidateMetrics::from_optimization_metrics(&metrics_json);
                if candidate_metrics.annual_return < targets.min_annual_return
                    || candidate_metrics.excess_return <= targets.min_excess_return
                {
                    return None;
                }
                let candidate_type = targets.classify(&candidate_metrics);
                let professional_gap_score =
                    professional_candidate_gap_score(&candidate_metrics, targets);
                Some(DiscoveryCandidate {
                    trial_id,
                    backtest_task_id,
                    score,
                    candidate_type,
                    professional_gap_score,
                    metrics: candidate_metrics,
                    parameters,
                })
            },
        )
        .collect::<Vec<_>>();
    candidates.sort_by(discovery_candidate_order);
    candidates.truncate(limit);
    Ok(candidates)
}

fn discovery_candidate_order(
    left: &DiscoveryCandidate,
    right: &DiscoveryCandidate,
) -> std::cmp::Ordering {
    discovery_candidate_rank(left.candidate_type)
        .cmp(&discovery_candidate_rank(right.candidate_type))
        .then_with(|| {
            left.professional_gap_score
                .cmp(&right.professional_gap_score)
        })
        .then_with(|| left.metrics.max_drawdown.cmp(&right.metrics.max_drawdown))
        .then_with(|| right.metrics.sortino.cmp(&left.metrics.sortino))
        .then_with(|| right.metrics.sharpe.cmp(&left.metrics.sharpe))
        .then_with(|| right.metrics.annual_return.cmp(&left.metrics.annual_return))
}

fn professional_candidate_gap_score(
    metrics: &CandidateMetrics,
    targets: &CandidateTargets,
) -> Decimal {
    let annual_gap = positive_gap(targets.min_annual_return, metrics.annual_return);
    let excess_gap = positive_gap(targets.min_excess_return, metrics.excess_return);
    let sharpe_gap = positive_gap(targets.min_sharpe, metrics.sharpe);
    let sortino_gap = positive_gap(targets.min_sortino, metrics.sortino);
    let drawdown_gap = positive_gap(metrics.max_drawdown, targets.max_drawdown);

    annual_gap * Decimal::new(4, 0)
        + excess_gap * Decimal::new(2, 0)
        + sharpe_gap
        + sortino_gap
        + drawdown_gap * Decimal::new(5, 0)
}

fn positive_gap(limit: Decimal, actual: Decimal) -> Decimal {
    (limit - actual).max(Decimal::ZERO)
}

fn discovery_candidate_rank(candidate_type: CandidateType) -> u8 {
    match candidate_type {
        CandidateType::Professional => 0,
        CandidateType::ReviewRequired => 1,
        CandidateType::Defensive => 2,
        CandidateType::Research => 3,
    }
}

fn discovery_candidate_json(candidate: &DiscoveryCandidate) -> Value {
    json!({
        "trial_id": candidate.trial_id,
        "backtest_task_id": candidate.backtest_task_id,
        "score": candidate.score,
        "candidate_type": candidate.candidate_type,
        "professional_gap_score": candidate.professional_gap_score,
        "metrics": {
            "annual_return_pct": candidate.metrics.annual_return,
            "excess_return_pct": candidate.metrics.excess_return,
            "sharpe_ratio": candidate.metrics.sharpe,
            "sortino_ratio": candidate.metrics.sortino,
            "max_drawdown_pct": candidate.metrics.max_drawdown,
            "total_return_pct": candidate.metrics.total_return,
            "benchmark_return_pct": candidate.metrics.benchmark_return,
            "turnover": candidate.metrics.turnover,
            "num_trades": candidate.metrics.num_trades,
        },
        "parameters": candidate.parameters,
    })
}

async fn load_robustness_timeseries_analysis(
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

fn normalize_promote_request(
    default_trial_id: &str,
    req: PromoteOptimizationRequest,
) -> Result<NormalizedPromoteRequest, String> {
    let trial_id = req
        .trial_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_trial_id)
        .to_string();
    let target_strategy_version = req.target_strategy_version.trim().to_string();
    let candidate_name = req.candidate_name.trim().to_string();
    let reason = req.reason.trim().to_string();
    if target_strategy_version.is_empty() {
        return Err("target_strategy_version must not be empty".into());
    }
    if candidate_name.is_empty() {
        return Err("candidate_name must not be empty".into());
    }
    if reason.is_empty() {
        return Err("reason must not be empty".into());
    }
    let promotion_mode = req
        .promotion_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("create_candidate")
        .to_string();
    if promotion_mode != "create_candidate" {
        return Err(format!("unsupported promotion_mode: {}", promotion_mode));
    }

    Ok(NormalizedPromoteRequest {
        trial_id,
        target_strategy_version,
        candidate_name,
        promotion_mode,
        gate_policy: req
            .gate_policy
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("default_v1")
            .to_string(),
        freeze_after_approval: req.freeze_after_approval.unwrap_or(false),
        reviewer: req
            .reviewer
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        reason,
        notes: req
            .notes
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        status: "candidate".to_string(),
    })
}

async fn load_execution_context(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<OptimizationTaskExecutionContext, String> {
    let row = sqlx::query_as::<_, (String, String, Value, Value, Option<Value>)>(
        "SELECT strategy_version_id, data_version_id, objective, backtest_template, constraints
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    Ok(OptimizationTaskExecutionContext {
        strategy_version_id: row.0,
        data_version_id: row.1,
        objective: row.2,
        backtest_template: row.3,
        constraints: row.4,
    })
}

fn build_optimization_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<OptimizationTrialBacktestRequest, String> {
    match trial_signal_source(task, parameters)?.as_deref() {
        Some("prediction") | Some("model_prediction") | Some("ml_prediction") => {
            build_prediction_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Prediction)
        }
        Some("factor") | Some("factor_combo") | None => {
            build_factor_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Factor)
        }
        Some(other) => Err(format!("unsupported signal_source: {}", other)),
    }
}

fn trial_signal_source(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<Option<String>, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;

    match params
        .get("signal_source")
        .or_else(|| template.get("signal_source"))
    {
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value.to_string()))
            }
        }
        Some(Value::Null) | None => {
            let has_prediction = params
                .get("prediction_set_id")
                .or_else(|| template.get("prediction_set_id"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            let has_combo = params
                .get("combo_name")
                .or_else(|| template.get("combo_name"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if has_prediction && !has_combo {
                Ok(Some("model_prediction".to_string()))
            } else {
                Ok(None)
            }
        }
        Some(_) => Err("signal_source must be a string or null".to_string()),
    }
}

fn build_factor_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<RunFactorBacktestReq, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;

    let string_value = |name: &str, default: Option<&str>| -> Result<String, String> {
        if let Some(value) = params.get(name).or_else(|| template.get(name)) {
            match value {
                Value::String(value) => Ok(value.clone()),
                Value::Number(value) => Ok(value.to_string()),
                _ => Err(format!("{} must be a string or number", name)),
            }
        } else if let Some(default) = default {
            Ok(default.to_string())
        } else {
            Err(format!("backtest_template.{} is required", name))
        }
    };
    let optional_string = |name: &str| -> Result<Option<String>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(Value::Number(value)) => Ok(Some(value.to_string())),
            _ => Err(format!("{} must be a string or number", name)),
        }
    };
    let usize_value = |name: &str, default: usize| -> Result<usize, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_u64()
                .map(|value| value as usize)
                .ok_or_else(|| format!("{} must be a positive integer", name)),
            Some(Value::String(value)) => value
                .parse::<usize>()
                .map_err(|_| format!("{} must be a positive integer", name)),
            _ => Err(format!("{} must be a positive integer", name)),
        }
    };
    let f64_value = |name: &str, default: f64| -> Result<f64, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_f64()
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };
    let optional_f64_value = |name: &str| -> Result<Option<f64>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(value)) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };

    let benchmark = optional_string("benchmark")?.or_else(|| Some("000300.SH".into()));
    let market_regime = market_regime_request_from_value(
        params
            .get("market_regime")
            .or_else(|| template.get("market_regime")),
        benchmark.as_deref().unwrap_or("000300.SH"),
    )?;

    Ok(RunFactorBacktestReq {
        combo_name: string_value("combo_name", None)?,
        version: string_value("version", Some("1.0.0"))?,
        strategy_version_id: task.strategy_version_id.clone(),
        data_version_id: task.data_version_id.clone(),
        research_dataset_id: optional_string("research_dataset_id")?,
        feature_set_version_id: optional_string("feature_set_version_id")?,
        prediction_set_id: optional_string("prediction_set_id")?,
        prediction_blend_weight: optional_f64_value("prediction_blend_weight")?,
        prediction_min_percentile: optional_f64_value("prediction_min_percentile")?,
        portfolio_policy_id: optional_string("portfolio_policy_id")?,
        top_n: usize_value("top_n", 20)?,
        rebalance: string_value("rebalance", Some("monthly"))?,
        entry_delay: usize_value("entry_delay", 0)?,
        min_amount: f64_value("min_amount", 0.0)?,
        max_position_pct: f64_value("max_position_pct", 0.10)?,
        skip_top_pct: f64_value("skip_top_pct", 0.0)?,
        max_pairwise_correlation: optional_f64_value("max_pairwise_correlation")?,
        correlation_lookback_days: usize_value("correlation_lookback_days", 60)?,
        kelly_fraction: f64_value("kelly_fraction", 0.0)?,
        kelly_lookback_days: usize_value("kelly_lookback_days", 60)?,
        max_gross_exposure: f64_value("max_gross_exposure", 1.0)?,
        score_direction: string_value("score_direction", Some("descending"))?,
        portfolio_method: string_value("portfolio_method", Some("heuristic"))?,
        risk_budget_lookback_days: usize_value("risk_budget_lookback_days", 60)?,
        capacity_penalty_strength: f64_value("capacity_penalty_strength", 0.0)?,
        industry_max_weight_pct: optional_f64_value("industry_max_weight_pct")?,
        score_candidate_pool_size: match params
            .get("score_candidate_pool_size")
            .or_else(|| template.get("score_candidate_pool_size"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let size = usize_value("score_candidate_pool_size", 0)?;
                if size == 0 {
                    None
                } else {
                    Some(size)
                }
            }
        },
        universe_profile: optional_string("universe_profile")?,
        cost_model: None,
        execution_rules: None,
        benchmark,
        market_regime,
        portfolio_drawdown_reduce_start_pct: optional_f64_value(
            "portfolio_drawdown_reduce_start_pct",
        )?,
        portfolio_drawdown_reduce_full_pct: optional_f64_value(
            "portfolio_drawdown_reduce_full_pct",
        )?,
        portfolio_drawdown_min_exposure: optional_f64_value("portfolio_drawdown_min_exposure")?,
        portfolio_drawdown_peak_lookback_days: match params
            .get("portfolio_drawdown_peak_lookback_days")
            .or_else(|| template.get("portfolio_drawdown_peak_lookback_days"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let days = usize_value("portfolio_drawdown_peak_lookback_days", 0)?;
                if days == 0 {
                    None
                } else {
                    Some(days)
                }
            }
        },
        portfolio_drawdown_recovery_start_pct: optional_f64_value(
            "portfolio_drawdown_recovery_start_pct",
        )?,
        portfolio_drawdown_recovery_full_pct: optional_f64_value(
            "portfolio_drawdown_recovery_full_pct",
        )?,
        portfolio_drawdown_recovery_boost: optional_f64_value("portfolio_drawdown_recovery_boost")?,
        portfolio_volatility_target_pct: optional_f64_value("portfolio_volatility_target_pct")?,
        portfolio_volatility_lookback_days: match params
            .get("portfolio_volatility_lookback_days")
            .or_else(|| template.get("portfolio_volatility_lookback_days"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let days = usize_value("portfolio_volatility_lookback_days", 0)?;
                if days == 0 {
                    None
                } else {
                    Some(days)
                }
            }
        },
        portfolio_volatility_min_exposure: optional_f64_value("portfolio_volatility_min_exposure")?,
        portfolio_volatility_max_exposure: optional_f64_value("portfolio_volatility_max_exposure")?,
        start_date: string_value("start_date", None)?,
        end_date: string_value("end_date", None)?,
        initial_capital: f64_value("initial_capital", 1_000_000.0)?,
        mode: optional_string("mode")?.or_else(|| Some("standard".into())),
    })
}

fn build_prediction_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<RunPredictionBacktestReq, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;

    let string_value = |name: &str, default: Option<&str>| -> Result<String, String> {
        if let Some(value) = params.get(name).or_else(|| template.get(name)) {
            match value {
                Value::String(value) => Ok(value.clone()),
                Value::Number(value) => Ok(value.to_string()),
                _ => Err(format!("{} must be a string or number", name)),
            }
        } else if let Some(default) = default {
            Ok(default.to_string())
        } else {
            Err(format!("backtest_template.{} is required", name))
        }
    };
    let optional_string = |name: &str| -> Result<Option<String>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(Value::Number(value)) => Ok(Some(value.to_string())),
            _ => Err(format!("{} must be a string or number", name)),
        }
    };
    let usize_value = |name: &str, default: usize| -> Result<usize, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_u64()
                .map(|value| value as usize)
                .ok_or_else(|| format!("{} must be a positive integer", name)),
            Some(Value::String(value)) => value
                .parse::<usize>()
                .map_err(|_| format!("{} must be a positive integer", name)),
            _ => Err(format!("{} must be a positive integer", name)),
        }
    };
    let f64_value = |name: &str, default: f64| -> Result<f64, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_f64()
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };
    let optional_f64_value = |name: &str| -> Result<Option<f64>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(value)) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };

    Ok(RunPredictionBacktestReq {
        prediction_set_id: string_value("prediction_set_id", None)?,
        strategy_version_id: task.strategy_version_id.clone(),
        data_version_id: task.data_version_id.clone(),
        research_dataset_id: optional_string("research_dataset_id")?,
        feature_set_version_id: optional_string("feature_set_version_id")?,
        portfolio_policy_id: optional_string("portfolio_policy_id")?,
        top_n: usize_value("top_n", 20)?,
        rebalance: string_value("rebalance", Some("monthly"))?,
        entry_delay: usize_value("entry_delay", 0)?,
        min_amount: f64_value("min_amount", 0.0)?,
        max_position_pct: f64_value("max_position_pct", 0.10)?,
        skip_top_pct: f64_value("skip_top_pct", 0.0)?,
        max_pairwise_correlation: optional_f64_value("max_pairwise_correlation")?,
        correlation_lookback_days: usize_value("correlation_lookback_days", 60)?,
        kelly_fraction: f64_value("kelly_fraction", 0.0)?,
        kelly_lookback_days: usize_value("kelly_lookback_days", 60)?,
        max_gross_exposure: f64_value("max_gross_exposure", 1.0)?,
        score_direction: string_value("score_direction", Some("descending"))?,
        portfolio_method: string_value("portfolio_method", Some("heuristic"))?,
        risk_budget_lookback_days: usize_value("risk_budget_lookback_days", 60)?,
        capacity_penalty_strength: f64_value("capacity_penalty_strength", 0.0)?,
        industry_max_weight_pct: optional_f64_value("industry_max_weight_pct")?,
        cost_model: None,
        execution_rules: None,
        benchmark: optional_string("benchmark")?.or_else(|| Some("000300.SH".into())),
        start_date: string_value("start_date", None)?,
        end_date: string_value("end_date", None)?,
        initial_capital: f64_value("initial_capital", 1_000_000.0)?,
        mode: optional_string("mode")?.or_else(|| Some("standard".into())),
    })
}

fn market_regime_request_from_value(
    value: Option<&Value>,
    default_benchmark: &str,
) -> Result<Option<MarketRegimeBacktestReq>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(policy)) => match policy.as_str() {
            "off" | "none" | "disabled" => Ok(None),
            "professional_default"
            | "drawdown_control_v1"
            | "drawdown_control_v2"
            | "quality_risk_off_v1"
            | "quality_crash_guard_v1" => Ok(Some(MarketRegimeBacktestReq {
                enabled: Some(true),
                policy: Some(policy.to_string()),
                benchmark: Some(default_benchmark.to_string()),
                lookback_days: None,
                min_observations: None,
            })),
            other => Err(format!("unsupported market_regime policy: {}", other)),
        },
        Some(Value::Object(map)) => {
            let enabled = map
                .get("enabled")
                .and_then(Value::as_bool)
                .or_else(|| map.get("active").and_then(Value::as_bool));
            let benchmark = map
                .get("benchmark")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .or_else(|| Some(default_benchmark.to_string()));
            let policy = map
                .get("policy")
                .or_else(|| map.get("name"))
                .and_then(Value::as_str)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let lookback_days = optional_usize_from_object(map, "lookback_days")?;
            let min_observations = optional_usize_from_object(map, "min_observations")?;
            Ok(Some(MarketRegimeBacktestReq {
                enabled,
                policy,
                benchmark,
                lookback_days,
                min_observations,
            }))
        }
        Some(_) => Err("market_regime must be a string, object, or null".to_string()),
    }
}

fn optional_usize_from_object(
    map: &Map<String, Value>,
    key: &str,
) -> Result<Option<usize>, String> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(|value| Some(value as usize))
            .ok_or_else(|| format!("market_regime.{} must be a positive integer", key)),
        Some(Value::String(value)) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("market_regime.{} must be a positive integer", key)),
        Some(_) => Err(format!("market_regime.{} must be a positive integer", key)),
    }
}

struct ReusableTrial {
    backtest_task_id: Option<String>,
    score: Decimal,
    metrics: Value,
    constraint_violations: Value,
}

async fn find_reusable_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    task: &OptimizationTaskExecutionContext,
    current_trial_id: &str,
    parameters: &Value,
) -> Result<Option<ReusableTrial>, String> {
    let reuse_key = trial_reuse_key(
        &task.strategy_version_id,
        &task.data_version_id,
        &task.backtest_template,
        parameters,
    );
    let rows = sqlx::query_as::<_, (String, Value, Option<String>, Decimal, Value, Value)>(
        "SELECT trial_id, parameters, backtest_task_id, score, metrics, constraint_violations
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND trial_id <> $2
           AND status = 'completed'
           AND score IS NOT NULL
           AND metrics IS NOT NULL
         ORDER BY completed_at ASC NULLS LAST, trial_index ASC",
    )
    .bind(task_id)
    .bind(current_trial_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to find reusable optimization trial: {}", error))?;

    Ok(rows.into_iter().find_map(|row| {
        let other_key = trial_reuse_key(
            &task.strategy_version_id,
            &task.data_version_id,
            &task.backtest_template,
            &row.1,
        );
        if other_key == reuse_key {
            Some(ReusableTrial {
                backtest_task_id: row.2,
                score: row.3,
                metrics: row.4,
                constraint_violations: row.5,
            })
        } else {
            None
        }
    }))
}

async fn mark_trial_reused(
    db: &sqlx::PgPool,
    trial_id: &str,
    reused: &ReusableTrial,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'completed', progress = 100, backtest_task_id = $2,
             score = $3, metrics = $4, constraint_violations = $5,
             completed_at = now(), last_heartbeat_at = now(),
             error_message = 'reused completed trial with same strategy/data/template/parameters'
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(reused.backtest_task_id.as_deref())
    .bind(reused.score)
    .bind(&reused.metrics)
    .bind(&reused.constraint_violations)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial reused: {}", error))?;
    Ok(())
}

fn trial_reuse_key(
    strategy_version_id: &str,
    data_version_id: &str,
    backtest_template: &Value,
    parameters: &Value,
) -> String {
    format!(
        "{}|{}|{}|{}",
        strategy_version_id,
        data_version_id,
        canonical_json(backtest_template),
        canonical_json(parameters)
    )
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let body = entries
                .into_iter()
                .map(|(key, value)| format!("\"{}\":{}", key, canonical_json(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{}}}", body)
        }
        Value::Array(values) => {
            let body = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{}]", body)
        }
        _ => value.to_string(),
    }
}

fn score_trial(
    metrics: &quant_backtest::metrics::BacktestMetrics,
    _objective: &Value,
    constraints: Option<&Value>,
) -> ScoredTrial {
    let mut score = metrics.information_ratio + metrics.excess_return_pct * Decimal::new(2, 1);
    let mut violations = Vec::new();

    if let Some(limit) = constraint_decimal(constraints, "max_drawdown") {
        if metrics.max_drawdown_pct > limit {
            let excess = metrics.max_drawdown_pct - limit;
            score -= excess * Decimal::new(5, 1);
            violations.push(json!({
                "constraint": "max_drawdown",
                "limit": limit,
                "actual": metrics.max_drawdown_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_annual_return") {
        if metrics.annual_return_pct < limit {
            let gap = limit - metrics.annual_return_pct;
            score -= gap * Decimal::new(4, 1);
            violations.push(json!({
                "constraint": "min_annual_return",
                "limit": limit,
                "actual": metrics.annual_return_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_excess_return") {
        if metrics.excess_return_pct <= limit {
            let gap = limit - metrics.excess_return_pct;
            score -= gap.max(Decimal::ZERO) * Decimal::new(2, 1);
            violations.push(json!({
                "constraint": "min_excess_return",
                "limit": limit,
                "actual": metrics.excess_return_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_sharpe") {
        if metrics.sharpe_ratio <= limit {
            let gap = limit - metrics.sharpe_ratio;
            score -= gap.max(Decimal::ZERO);
            violations.push(json!({
                "constraint": "min_sharpe",
                "limit": limit,
                "actual": metrics.sharpe_ratio,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_sortino") {
        if metrics.sortino_ratio < limit {
            let gap = limit - metrics.sortino_ratio;
            score -= gap;
            violations.push(json!({
                "constraint": "min_sortino",
                "limit": limit,
                "actual": metrics.sortino_ratio,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "max_turnover") {
        if metrics.turnover > limit {
            let excess = metrics.turnover - limit;
            score -= excess * Decimal::new(2, 1);
            violations.push(json!({
                "constraint": "max_turnover",
                "limit": limit,
                "actual": metrics.turnover,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_i64(constraints, "min_trade_count") {
        if (metrics.num_trades as i64) < limit {
            score -=
                Decimal::new((limit - metrics.num_trades as i64).max(0), 0) * Decimal::new(1, 1);
            violations.push(json!({
                "constraint": "min_trade_count",
                "limit": limit,
                "actual": metrics.num_trades,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_information_ratio") {
        if metrics.information_ratio < limit {
            let gap = limit - metrics.information_ratio;
            score -= gap;
            violations.push(json!({
                "constraint": "min_information_ratio",
                "limit": limit,
                "actual": metrics.information_ratio,
                "severity": "hard"
            }));
        }
    }

    ScoredTrial {
        score,
        metrics: json!({
            "total_return_pct": metrics.total_return_pct,
            "annual_return_pct": metrics.annual_return_pct,
            "sharpe_ratio": metrics.sharpe_ratio,
            "sortino_ratio": metrics.sortino_ratio,
            "max_drawdown_pct": metrics.max_drawdown_pct,
            "benchmark_return_pct": metrics.benchmark_return_pct,
            "excess_return_pct": metrics.excess_return_pct,
            "information_ratio": metrics.information_ratio,
            "turnover": metrics.turnover,
            "num_trades": metrics.num_trades,
            "win_rate_pct": metrics.win_rate_pct,
        }),
        constraint_violations: Value::Array(violations),
    }
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

fn build_market_scenario_analysis(points: &[RobustnessDailyPoint]) -> Value {
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

fn build_walk_forward_analysis(
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
    let mut idx = 0;
    while idx + window_size <= points.len() {
        let slice = &points[idx..idx + window_size];
        let summary = summarize_points(slice);
        let volatility =
            annualized_volatility(&daily_returns(slice, |point| Some(point.portfolio_value)));
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
        "worst_window_drawdown": worst_window_drawdown,
        "scenario_count": scenario_count,
        "windows": windows
    })
}

fn build_bootstrap_analysis(
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
    }

    Ok(json!({
        "trials": trials,
        "sample_size": returns.len(),
        "positive_return_probability": positive as f64 / trials as f64,
        "total_return": distribution_summary(&mut total_returns),
        "sharpe_ratio": distribution_summary(&mut sharpes),
        "sortino_ratio": distribution_summary(&mut sortinos)
    }))
}

fn classify_market_scenario(
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

fn summarize_points(points: &[RobustnessDailyPoint]) -> RobustnessMetricSummary {
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
    RobustnessMetricSummary {
        total_return,
        annual_return: annualized_return(total_return, points.len().saturating_sub(1)),
        sharpe_ratio: sample.sharpe_ratio,
        sortino_ratio: sample.sortino_ratio,
        max_drawdown: max_drawdown(&mut nav),
        benchmark_return,
        excess_return: benchmark_return.map(|value| total_return - value),
    }
}

fn summarize_return_sample(returns: &[f64]) -> RobustnessMetricSummary {
    let total_return = returns.iter().fold(1.0, |acc, value| acc * (1.0 + value)) - 1.0;
    let volatility = annualized_volatility(returns);
    let annual_return = annualized_return(total_return, returns.len());
    RobustnessMetricSummary {
        total_return,
        annual_return,
        sharpe_ratio: if volatility > 0.0 {
            annual_return / volatility
        } else {
            0.0
        },
        sortino_ratio: sortino_ratio(annual_return, returns),
        max_drawdown: drawdown_from_returns(returns),
        benchmark_return: None,
        excess_return: None,
    }
}

fn metric_summary_json(summary: &RobustnessMetricSummary) -> Value {
    json!({
        "total_return": summary.total_return,
        "annual_return": summary.annual_return,
        "sharpe_ratio": summary.sharpe_ratio,
        "sortino_ratio": summary.sortino_ratio,
        "max_drawdown": summary.max_drawdown,
        "benchmark_return": summary.benchmark_return,
        "excess_return": summary.excess_return
    })
}

fn daily_returns<F>(points: &[RobustnessDailyPoint], value_fn: F) -> Vec<f64>
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

fn ratio_return(start: Option<f64>, end: Option<f64>) -> f64 {
    match (start, end) {
        (Some(start), Some(end)) if start.is_finite() && end.is_finite() && start > 0.0 => {
            end / start - 1.0
        }
        _ => 0.0,
    }
}

fn annualized_return(total_return: f64, periods: usize) -> f64 {
    if periods == 0 || total_return <= -1.0 {
        return 0.0;
    }
    (1.0 + total_return).powf(252.0 / periods as f64) - 1.0
}

fn annualized_volatility(returns: &[f64]) -> f64 {
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

fn sortino_ratio(annual_return: f64, returns: &[f64]) -> f64 {
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

fn max_drawdown(nav: &mut [f64]) -> f64 {
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

fn drawdown_from_returns(returns: &[f64]) -> f64 {
    let mut value = 1.0;
    let mut nav = Vec::with_capacity(returns.len() + 1);
    nav.push(value);
    for daily_return in returns {
        value *= 1.0 + daily_return;
        nav.push(value);
    }
    max_drawdown(&mut nav)
}

fn distribution_summary(values: &mut [f64]) -> Value {
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "p05": percentile(values, 0.05),
        "median": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "mean": if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
    })
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx.min(values.len() - 1)]
}

#[cfg(test)]
fn evaluate_robustness_gates(
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

fn evaluate_robustness_gates_with_analysis(
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
    let max_drawdown =
        constraint_decimal(gate_policy, "max_drawdown").unwrap_or(Decimal::new(20, 2));
    let min_score_gap = constraint_decimal(gate_policy, "min_score_gap").unwrap_or(Decimal::ZERO);
    let min_walk_forward_windows =
        constraint_i64(gate_policy, "min_walk_forward_windows").unwrap_or(0);
    let min_positive_excess_window_ratio =
        constraint_f64(gate_policy, "min_positive_excess_window_ratio").unwrap_or(0.0);
    let min_bootstrap_positive_return_probability =
        constraint_f64(gate_policy, "min_bootstrap_positive_return_probability").unwrap_or(0.0);
    let min_market_scenarios = constraint_i64(gate_policy, "min_market_scenarios").unwrap_or(1);

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
    let drawdown = decimal_from_json(metrics.get("max_drawdown_pct")).unwrap_or(Decimal::ZERO);
    let hard_violations = constraint_violations
        .as_array()
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let score_gap = runner_up_score.map(|runner_up| best_score - runner_up);

    let mut gates = vec![
        json!({
            "gate": "no_hard_constraint_violations",
            "passed": !hard_violations,
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
    if let Some(analysis) = analysis {
        let window_count = analysis.walk_forward["window_count"].as_i64().unwrap_or(0);
        let positive_excess_window_ratio = analysis.walk_forward["positive_excess_window_ratio"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_positive_return_probability = analysis.bootstrap
            ["positive_return_probability"]
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
            "gate": "bootstrap_positive_return_probability",
            "passed": bootstrap_positive_return_probability >= min_bootstrap_positive_return_probability,
            "limit": min_bootstrap_positive_return_probability,
            "actual": bootstrap_positive_return_probability,
            "details": analysis.bootstrap,
        }));
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

fn decimal_from_json(value: Option<&Value>) -> Option<Decimal> {
    match value {
        Some(Value::Number(number)) => number.as_f64().and_then(Decimal::from_f64_retain),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}

fn constraint_decimal(constraints: Option<&Value>, name: &str) -> Option<Decimal> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
        .and_then(Decimal::from_f64_retain)
}

fn constraint_i64(constraints: Option<&Value>, name: &str) -> Option<i64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_i64)
}

fn constraint_f64(constraints: Option<&Value>, name: &str) -> Option<f64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
}

async fn mark_trial_running(db: &sqlx::PgPool, trial_id: &str) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'running', progress = 10,
             started_at = now(), last_heartbeat_at = now(), error_message = NULL
         WHERE trial_id = $1 AND status = 'pending'",
    )
    .bind(trial_id)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial running: {}", error))?;
    Ok(())
}

async fn mark_trial_completed(
    db: &sqlx::PgPool,
    trial_id: &str,
    backtest_task_id: &str,
    scored: &ScoredTrial,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'completed', progress = 100, backtest_task_id = $2,
             score = $3, metrics = $4, constraint_violations = $5,
             completed_at = now(), last_heartbeat_at = now(), error_message = NULL
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(backtest_task_id)
    .bind(scored.score)
    .bind(&scored.metrics)
    .bind(&scored.constraint_violations)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial completed: {}", error))?;
    Ok(())
}

async fn mark_trial_failed(
    db: &sqlx::PgPool,
    trial_id: &str,
    error_message: &str,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'failed', progress = 100, completed_at = now(),
             last_heartbeat_at = now(), error_message = $2
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(error_message)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial failed: {}", error))?;
    Ok(())
}

async fn refresh_task_progress(db: &sqlx::PgPool, task_id: &str) -> Result<Option<String>, String> {
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
           COUNT(*) FILTER (WHERE status = 'completed')::bigint,
           COUNT(*) FILTER (WHERE status = 'failed')::bigint
         FROM optimization_trial
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count optimization trials: {}", error))?;

    let best_trial_id = sqlx::query_as::<_, (String,)>(
        "SELECT trial_id
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed'
         ORDER BY score DESC NULLS LAST, trial_index ASC
         LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to find best optimization trial: {}", error))?
    .map(|row| row.0);

    let finished = counts.1 + counts.2;
    let progress = if counts.0 == 0 {
        0
    } else {
        ((finished * 100) / counts.0).clamp(0, 100) as i32
    };
    let status = if counts.0 > 0 && finished == counts.0 {
        if counts.1 > 0 {
            "completed"
        } else {
            "failed"
        }
    } else if finished > 0 {
        "partial"
    } else {
        "pending"
    };

    sqlx::query(
        "UPDATE optimization_task
         SET status = $2, progress = $3, best_trial_id = $4, last_heartbeat_at = now()
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .bind(status)
    .bind(progress)
    .bind(best_trial_id.as_deref())
    .execute(db)
    .await
    .map_err(|error| format!("Failed to refresh optimization task progress: {}", error))?;

    Ok(best_trial_id)
}

fn sample_parameter(name: &str, spec: &Value, rng: &mut DeterministicRng) -> Result<Value, String> {
    let spec = spec
        .as_object()
        .ok_or_else(|| format!("search_space.{} must be an object", name))?;
    let kind = spec
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("search_space.{}.type is required", name))?;

    match kind {
        "int" => {
            let min = spec
                .get("min")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("search_space.{}.min is required", name))?;
            let max = spec
                .get("max")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("search_space.{}.max is required", name))?;
            if min > max {
                return Err(format!("search_space.{} min must be <= max", name));
            }
            Ok(json!(rng.gen_i64(min, max)))
        }
        "float" => {
            let min = spec
                .get("min")
                .and_then(Value::as_f64)
                .ok_or_else(|| format!("search_space.{}.min is required", name))?;
            let max = spec
                .get("max")
                .and_then(Value::as_f64)
                .ok_or_else(|| format!("search_space.{}.max is required", name))?;
            if min > max {
                return Err(format!("search_space.{} min must be <= max", name));
            }
            Ok(json!(min + (max - min) * rng.gen_f64()))
        }
        "choice" => {
            let values = spec
                .get("values")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("search_space.{}.values is required", name))?;
            if values.is_empty() {
                return Err(format!("search_space.{}.values must not be empty", name));
            }
            Ok(values[rng.gen_usize(values.len())].clone())
        }
        other => Err(format!("unsupported search_space.{}.type: {}", name, other)),
    }
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn gen_f64(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        value as f64 / ((1u64 << 53) as f64)
    }

    fn gen_i64(&mut self, min: i64, max: i64) -> i64 {
        let span = (max - min + 1) as u64;
        min + (self.next_u64() % span) as i64
    }

    fn gen_usize(&mut self, upper_exclusive: usize) -> usize {
        ((self.next_u64() >> 32) as usize) % upper_exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn random_search_trial_generation_is_seed_deterministic() {
        let search_space = json!({
            "top_n": {"type": "int", "min": 5, "max": 8},
            "rebalance": {"type": "choice", "values": ["5", "20"]},
            "max_position_pct": {"type": "float", "min": 0.05, "max": 0.10}
        });

        let first = generate_trial_parameters(&search_space, 42, 3).expect("first generation");
        let second = generate_trial_parameters(&search_space, 42, 3).expect("second generation");

        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|params| params.get("top_n").is_some()));
    }

    #[test]
    fn random_search_rejects_invalid_search_space_item() {
        let search_space = json!({
            "top_n": {"type": "int", "min": 8, "max": 5}
        });

        let err = generate_trial_parameters(&search_space, 42, 1).unwrap_err();

        assert!(err.contains("top_n"));
    }

    #[test]
    fn random_search_choice_sampling_does_not_collapse_power_of_two_choices() {
        let search_space = json!({
            "prediction_blend_weight": {"type": "choice", "values": [0.0, 0.02, 0.05, 0.1]},
            "prediction_min_percentile": {"type": "choice", "values": [null, 0.1, 0.2, 0.3]}
        });

        let trials = generate_trial_parameters(&search_space, 20260520, 16).expect("trial params");
        let unique_pairs = trials
            .iter()
            .map(|params| {
                format!(
                    "{}|{}",
                    params
                        .get("prediction_blend_weight")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into()),
                    params
                        .get("prediction_min_percentile")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into())
                )
            })
            .collect::<BTreeSet<_>>();

        assert!(
            unique_pairs.len() >= 8,
            "expected broad choice coverage, got {unique_pairs:?}"
        );
    }

    #[test]
    fn trial_backtest_request_applies_parameter_overrides() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase3d_alpha_smoke",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly",
                "max_position_pct": 0.10,
                "correlation_lookback_days": 60,
                "kelly_lookback_days": 60
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "rebalance": "5",
            "max_position_pct": 0.08,
            "max_pairwise_correlation": 0.65,
            "kelly_fraction": 0.25,
            "max_gross_exposure": 0.80,
            "score_direction": "ascending",
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": 80,
            "capacity_penalty_strength": 0.75,
            "industry_max_weight_pct": 0.35,
            "score_candidate_pool_size": 300,
            "universe_profile": "listed_non_st",
            "prediction_set_id": "pred-quality-growth-v1",
            "prediction_blend_weight": 0.35,
            "market_regime": "quality_crash_guard_v1",
            "portfolio_drawdown_reduce_start_pct": 0.10,
            "portfolio_drawdown_reduce_full_pct": 0.25,
            "portfolio_drawdown_min_exposure": 0.50,
            "portfolio_drawdown_peak_lookback_days": 252,
            "portfolio_drawdown_recovery_start_pct": 0.30,
            "portfolio_drawdown_recovery_full_pct": 0.70,
            "portfolio_drawdown_recovery_boost": 1.0,
            "portfolio_volatility_target_pct": 0.16,
            "portfolio_volatility_lookback_days": 60,
            "portfolio_volatility_min_exposure": 0.45,
            "portfolio_volatility_max_exposure": 1.0
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(req.strategy_version_id, "factor-combo-v1");
        assert_eq!(req.data_version_id, "perf-db-smoke-data-v1");
        assert_eq!(req.combo_name, "phase3d_alpha_smoke");
        assert_eq!(req.top_n, 8);
        assert_eq!(req.rebalance, "5");
        assert_eq!(req.max_position_pct, 0.08);
        assert_eq!(req.max_pairwise_correlation, Some(0.65));
        assert_eq!(req.correlation_lookback_days, 60);
        assert_eq!(req.kelly_fraction, 0.25);
        assert_eq!(req.kelly_lookback_days, 60);
        assert_eq!(req.max_gross_exposure, 0.80);
        assert_eq!(req.score_direction, "ascending");
        assert_eq!(req.portfolio_method, "risk_budget");
        assert_eq!(req.risk_budget_lookback_days, 80);
        assert_eq!(req.capacity_penalty_strength, 0.75);
        assert_eq!(req.industry_max_weight_pct, Some(0.35));
        assert_eq!(req.score_candidate_pool_size, Some(300));
        assert_eq!(req.universe_profile.as_deref(), Some("listed_non_st"));
        assert_eq!(
            req.prediction_set_id.as_deref(),
            Some("pred-quality-growth-v1")
        );
        assert_eq!(req.prediction_blend_weight, Some(0.35));
        assert_eq!(
            req.market_regime.as_ref().and_then(|policy| policy.enabled),
            Some(true)
        );
        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_crash_guard_v1")
        );
        assert_eq!(req.portfolio_drawdown_reduce_start_pct, Some(0.10));
        assert_eq!(req.portfolio_drawdown_reduce_full_pct, Some(0.25));
        assert_eq!(req.portfolio_drawdown_min_exposure, Some(0.50));
        assert_eq!(req.portfolio_drawdown_peak_lookback_days, Some(252));
        assert_eq!(req.portfolio_drawdown_recovery_start_pct, Some(0.30));
        assert_eq!(req.portfolio_drawdown_recovery_full_pct, Some(0.70));
        assert_eq!(req.portfolio_drawdown_recovery_boost, Some(1.0));
        assert_eq!(req.portfolio_volatility_target_pct, Some(0.16));
        assert_eq!(req.portfolio_volatility_lookback_days, Some(60));
        assert_eq!(req.portfolio_volatility_min_exposure, Some(0.45));
        assert_eq!(req.portfolio_volatility_max_exposure, Some(1.0));
    }

    #[test]
    fn optimization_trial_request_routes_model_prediction_source() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "prediction-strategy-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "signal_source": "model_prediction",
                "prediction_set_id": "pred-linear-v1",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "5",
                "max_position_pct": 0.08,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 10,
            "kelly_fraction": 0.25,
            "risk_budget_lookback_days": 120
        });

        let req = build_optimization_trial_request(&task, &params).expect("prediction request");

        match req {
            OptimizationTrialBacktestRequest::Prediction(req) => {
                assert_eq!(req.prediction_set_id, "pred-linear-v1");
                assert_eq!(req.strategy_version_id, "prediction-strategy-v1");
                assert_eq!(req.top_n, 10);
                assert_eq!(req.rebalance, "5");
                assert_eq!(req.max_position_pct, 0.08);
                assert_eq!(req.portfolio_method, "risk_budget");
                assert_eq!(req.kelly_fraction, 0.25);
                assert_eq!(req.risk_budget_lookback_days, 120);
            }
            OptimizationTrialBacktestRequest::Factor(_) => {
                panic!("expected model prediction request")
            }
        }
    }

    #[test]
    fn phase7_layered_request_builds_resource_limited_plan() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160405",
                "end_date": "20260511",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(7),
            search_profile: None,
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(bundle.plan.requested_trials, 3_197_988_864);
        assert_eq!(bundle.plan.planned_trials, 7);
        assert!(bundle.plan.truncated);
        assert_eq!(bundle.search_space["phase"], "7-D");
        assert_eq!(bundle.search_space["resource_plan"]["max_trials"], 7);
    }

    #[test]
    fn phase7_layered_request_can_include_prediction_candidates() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20250109",
                "end_date": "20250131",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![" pred-linear-v1 ".to_string(), "".to_string()]),
            max_trials: Some(40),
            search_profile: None,
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"][0],
            "pred-linear-v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["signal_source"] == "model_prediction"
                && trial.parameters["prediction_set_id"] == "pred-linear-v1"
        }));
    }

    #[test]
    fn professional_discovery_defaults_to_risk_breakthrough_search_profile() {
        let req = Phase7ProfessionalDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: None,
            trial_batch_limit: Some(4),
            max_batches: Some(2),
            robustness_top_n: Some(3),
            robustness_gate_policy: None,
            stop_after_professional_candidate: Some(true),
            stop_after_robust_approval: Some(true),
        };
        let layered_req = phase7_discovery_layered_request(&req);
        let bundle = build_phase7_layered_plan_bundle(
            &layered_req,
            quant_common::phase7::LocalResourcePlan::for_machine(10, 32),
        );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_breakthrough"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert_eq!(
            layered_req.constraints.as_ref().unwrap()["min_sortino"],
            1.5
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["score_direction"] == "ascending"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] != "off"
        }));
    }

    #[test]
    fn discovery_candidate_order_prefers_lower_professional_gap_over_raw_return() {
        let high_return_high_drawdown = DiscoveryCandidate {
            trial_id: "high-return".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(2, 1)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(75, 2),
            metrics: CandidateMetrics {
                annual_return: Decimal::new(19, 2),
                excess_return: Decimal::new(4, 2),
                sharpe: Decimal::new(60, 2),
                sortino: Decimal::new(11, 1),
                max_drawdown: Decimal::new(55, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let lower_return_better_risk = DiscoveryCandidate {
            trial_id: "better-risk".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(1, 1)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(25, 2),
            metrics: CandidateMetrics {
                annual_return: Decimal::new(151, 3),
                excess_return: Decimal::new(3, 2),
                sharpe: Decimal::new(85, 2),
                sortino: Decimal::new(13, 1),
                max_drawdown: Decimal::new(38, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut candidates = vec![high_return_high_drawdown, lower_return_better_risk];

        candidates.sort_by(discovery_candidate_order);

        assert_eq!(candidates[0].trial_id, "better-risk");
    }

    #[test]
    fn professional_discovery_only_approved_robustness_counts_as_found() {
        assert!(robustness_result_is_approved(&json!({
            "status": "approved_candidate"
        })));
        assert!(!robustness_result_is_approved(&json!({
            "status": "rejected"
        })));
        assert!(!robustness_result_is_approved(&json!({
            "status": "review_required"
        })));
    }

    #[test]
    fn risk_adjusted_score_records_constraint_violations() {
        let mut metrics = quant_backtest::metrics::BacktestMetrics::default();
        metrics.information_ratio = Decimal::new(8, 1);
        metrics.excess_return_pct = Decimal::new(12, 2);
        metrics.max_drawdown_pct = Decimal::new(25, 2);
        metrics.turnover = Decimal::new(12, 0);
        metrics.num_trades = 3;

        let scored = score_trial(
            &metrics,
            &json!({"type": "risk_adjusted", "maximize": true}),
            Some(&json!({
                "max_drawdown": 0.20,
                "max_turnover": 8.0,
                "min_trade_count": 10,
                "min_information_ratio": 1.0
            })),
        );

        assert!(scored.score < Decimal::new(8, 1));
        let violations = scored
            .constraint_violations
            .as_array()
            .expect("violations array");
        assert_eq!(violations.len(), 4);
        assert!(violations
            .iter()
            .any(|item| item["constraint"] == "min_trade_count"));
    }

    #[test]
    fn candidate_input_uses_best_trial_when_trial_id_is_blank() {
        let req = PromoteOptimizationRequest {
            trial_id: None,
            target_strategy_version: "factor-combo-v1-candidate".into(),
            candidate_name: "phase4c candidate".into(),
            promotion_mode: Some("create_candidate".into()),
            gate_policy: Some("default_v1".into()),
            freeze_after_approval: Some(false),
            reviewer: Some("gaocheng".into()),
            reason: "best completed trial".into(),
            notes: None,
        };

        let normalized = normalize_promote_request("trial-opt-demo-0001", req)
            .expect("normalized promote request");

        assert_eq!(normalized.trial_id, "trial-opt-demo-0001");
        assert_eq!(normalized.status, "candidate");
        assert_eq!(normalized.gate_policy, "default_v1");
    }

    #[test]
    fn trial_reuse_key_is_stable_for_same_template_and_parameters() {
        let template = json!({
            "combo_name": "phase3d_alpha_smoke",
            "version": "1.0.0",
            "start_date": "20250109",
            "end_date": "20250131"
        });
        let first = trial_reuse_key(
            "factor-combo-v1",
            "perf-db-smoke-data-v1",
            &template,
            &json!({"top_n": 5, "rebalance": "5"}),
        );
        let second = trial_reuse_key(
            "factor-combo-v1",
            "perf-db-smoke-data-v1",
            &template,
            &json!({"rebalance": "5", "top_n": 5}),
        );

        assert_eq!(first, second);
        assert!(first.contains("factor-combo-v1"));
        assert!(first.contains("phase3d_alpha_smoke"));
    }

    #[test]
    fn optimization_performance_gate_passes_completed_release_batch() {
        let policy = normalize_performance_gate(
            Some(&OptimizationPerformanceGateRequest {
                min_completed_trials: Some(200),
                max_failed_trials: Some(0),
                max_elapsed_ms: Some(60_000),
            }),
            200,
        )
        .expect("gate policy");

        let gates = evaluate_optimization_performance_gates(200, 200, 0, 42_000, &policy);

        assert_eq!(performance_gate_status(&gates), "passed");
        assert!(gates
            .as_array()
            .unwrap()
            .iter()
            .all(|gate| gate["passed"] == true));
    }

    #[test]
    fn optimization_performance_gate_requires_review_on_failures_or_shortfall() {
        let policy = normalize_performance_gate(None, 200).expect("default gate policy");

        let gates = evaluate_optimization_performance_gates(199, 198, 1, 42_000, &policy);

        assert_eq!(performance_gate_status(&gates), "review_required");
        assert_eq!(gates[0]["gate"], "min_completed_trials");
        assert_eq!(gates[0]["passed"], false);
        assert_eq!(gates[1]["gate"], "max_failed_trials");
        assert_eq!(gates[1]["passed"], false);
    }

    #[test]
    fn robustness_gate_approves_clean_dominant_trial() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({"num_trades": 12, "max_drawdown_pct": "0.08"}),
            &json!([]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20})),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .all(|gate| gate["passed"] == true));
    }

    #[test]
    fn robustness_gate_rejects_constraint_violations() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(4, 1),
            None,
            &json!({"num_trades": 2, "max_drawdown_pct": "0.25"}),
            &json!([{"constraint": "max_drawdown"}]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20})),
        );

        assert_eq!(evaluation.status, "rejected");
    }

    #[test]
    fn robustness_gate_requires_review_when_score_gap_is_small() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(99, 2)),
            &json!({"num_trades": 12, "max_drawdown_pct": "0.08"}),
            &json!([]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20, "min_score_gap": 0.05})),
        );

        assert_eq!(evaluation.status, "review_required");
    }

    #[test]
    fn professional_robustness_policy_encodes_core_targets() {
        let policy = default_professional_robustness_policy();

        assert_eq!(policy["min_annual_return"], 0.15);
        assert_eq!(policy["min_sharpe"], 1.0);
        assert_eq!(policy["min_sortino"], 1.5);
        assert_eq!(policy["max_drawdown"], 0.35);
        assert_eq!(policy["walk_forward_window_days"], 756);
        assert_eq!(policy["bootstrap_trials"], 512);
    }

    #[test]
    fn professional_robustness_gate_rejects_low_sortino_candidate() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({
                "num_trades": 120,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.04",
                "sharpe_ratio": "1.10",
                "sortino_ratio": "1.00",
                "max_drawdown_pct": "0.20"
            }),
            &json!([]),
            Some(&default_professional_robustness_policy()),
        );

        assert_eq!(evaluation.status, "rejected");
        assert!(evaluation
            .gates
            .as_array()
            .expect("gates")
            .iter()
            .any(|gate| gate["gate"] == "min_sortino" && gate["passed"] == false));
    }

    #[test]
    fn market_scenario_classifies_regime_from_benchmark_path() {
        let bull = RobustnessMetricSummary {
            total_return: 0.18,
            annual_return: 0.20,
            sharpe_ratio: 1.2,
            sortino_ratio: 1.8,
            max_drawdown: 0.04,
            benchmark_return: Some(0.18),
            excess_return: Some(0.02),
        };
        let bear = RobustnessMetricSummary {
            total_return: -0.02,
            annual_return: -0.02,
            sharpe_ratio: -0.3,
            sortino_ratio: -0.2,
            max_drawdown: 0.24,
            benchmark_return: Some(-0.18),
            excess_return: Some(0.16),
        };
        let high_vol = RobustnessMetricSummary {
            total_return: 0.01,
            annual_return: 0.01,
            sharpe_ratio: 0.1,
            sortino_ratio: 0.1,
            max_drawdown: 0.10,
            benchmark_return: Some(0.01),
            excess_return: Some(0.0),
        };

        assert_eq!(classify_market_scenario(&bull, 0.18), "bull");
        assert_eq!(classify_market_scenario(&bear, 0.18), "bear");
        assert_eq!(classify_market_scenario(&high_vol, 0.32), "high_volatility");
    }

    #[test]
    fn walk_forward_analysis_records_window_degradation_and_scenarios() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 103.0, Some(101.0)),
            RobustnessDailyPoint::new("2025-01-03", 106.0, Some(102.0)),
            RobustnessDailyPoint::new("2025-01-06", 101.0, Some(99.0)),
            RobustnessDailyPoint::new("2025-01-07", 98.0, Some(96.0)),
            RobustnessDailyPoint::new("2025-01-08", 104.0, Some(97.0)),
        ];
        let analysis = build_walk_forward_analysis(&points, 3, 2);

        assert_eq!(analysis["window_count"], 2);
        assert_eq!(analysis["windows"][0]["start_date"], "2025-01-01");
        assert_eq!(analysis["windows"][1]["scenario"], "bear");
        assert!(analysis["positive_excess_window_ratio"].as_f64().unwrap() > 0.0);
        assert!(analysis["worst_window_drawdown"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn bootstrap_analysis_reports_distribution_and_gate_probability() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 101.0, Some(100.4)),
            RobustnessDailyPoint::new("2025-01-03", 102.0, Some(100.8)),
            RobustnessDailyPoint::new("2025-01-06", 101.0, Some(100.2)),
            RobustnessDailyPoint::new("2025-01-07", 103.0, Some(100.5)),
        ];
        let analysis = build_bootstrap_analysis(&points, 64, 7).expect("bootstrap analysis");

        assert_eq!(analysis["trials"], 64);
        assert!(analysis["positive_return_probability"].as_f64().unwrap() > 0.50);
        assert!(analysis["total_return"]["p05"].is_number());
        assert!(analysis["sharpe_ratio"]["median"].is_number());
    }

    #[test]
    fn robustness_gate_includes_walk_forward_and_bootstrap_requirements() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 101.0, Some(100.2)),
            RobustnessDailyPoint::new("2025-01-03", 102.0, Some(100.4)),
            RobustnessDailyPoint::new("2025-01-06", 103.0, Some(100.6)),
            RobustnessDailyPoint::new("2025-01-07", 104.0, Some(100.8)),
            RobustnessDailyPoint::new("2025-01-08", 105.0, Some(101.0)),
        ];
        let analysis = RobustnessTimeSeriesAnalysis::from_points(&points, 3, 3, 32, 11)
            .expect("timeseries analysis");

        let evaluation = evaluate_robustness_gates_with_analysis(
            Decimal::new(10, 1),
            Some(Decimal::new(7, 1)),
            &json!({"num_trades": 20, "max_drawdown_pct": "0.05"}),
            &json!([]),
            Some(&json!({
                "min_trade_count": 5,
                "max_drawdown": 0.20,
                "min_walk_forward_windows": 2,
                "min_positive_excess_window_ratio": 0.5,
                "min_bootstrap_positive_return_probability": 0.50
            })),
            Some(&analysis),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "walk_forward_min_window_count"));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "bootstrap_positive_return_probability"));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "market_scenario_coverage"));
    }
}
