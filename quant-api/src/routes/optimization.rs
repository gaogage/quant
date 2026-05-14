//! Optimization API routes — create optimization tasks and parameter trials

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::routes::backtest::{execute_factor_backtest, RunFactorBacktestReq};
use crate::AppState;

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
pub struct TrialListQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct RunOptimizationRequest {
    pub trial_limit: Option<i64>,
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

pub async fn create_optimization(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateOptimizationRequest>,
) -> impl IntoResponse {
    if req.search_method != "random_search" && req.search_method != "grid_search" {
        return Json(json!({"code": 1, "message": format!("unsupported search_method: {}", req.search_method)}));
    }

    let max_trials = normalize_max_trials(req.max_trials);
    let trial_params = match generate_trial_parameters(&req.search_space, req.random_seed, max_trials) {
        Ok(params) => params,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };

    let task_id = format!("opt-{}", Uuid::new_v4());
    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(error) => return Json(json!({"code": 1, "message": format!("Failed to begin optimization transaction: {}", error)})),
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
        return Json(json!({"code": 1, "message": format!("Failed to create optimization task: {}", error)}));
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
            return Json(json!({"code": 1, "message": format!("Failed to create optimization trial: {}", error)}));
        }
    }

    if let Err(error) = tx.commit().await {
        return Json(json!({"code": 1, "message": format!("Failed to commit optimization task: {}", error)}));
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

pub async fn get_optimization(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<_, (String, String, String, String, Value, Value, Option<Value>, Option<Value>, Option<Value>, String, Option<String>, i32, Option<chrono::DateTime<chrono::Utc>>)>(
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
        Err(error) => return Json(json!({"code": 1, "message": format!("Failed to get optimization task: {}", error)})),
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
    match execute_pending_trials(&state.db, &task_id, trial_limit).await {
        Ok(summary) => Json(json!({"code": 0, "data": summary})),
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

    let rows = sqlx::query_as::<_, (String, i32, Value, Option<Decimal>, Option<Value>, Option<Value>, String, i32, Option<String>, Option<String>)>(
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
        Err(error) => Json(json!({"code": 1, "message": format!("Failed to list optimization trials: {}", error)})),
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
) -> Result<Value, String> {
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
        return Ok(json!({
            "optimization_task_id": task_id,
            "executed": 0,
            "completed": 0,
            "failed": 0,
            "best_trial_id": Value::Null,
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

        let request = match build_factor_trial_request(&task, params) {
            Ok(request) => request,
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
                continue;
            }
        };

        match execute_factor_backtest(db, &backtest_task_id, request).await {
            Ok(output) => {
                let scored = score_trial(&output.metrics, &task.objective, task.constraints.as_ref());
                mark_trial_completed(
                    db,
                    trial_id,
                    &backtest_task_id,
                    &scored,
                )
                .await?;
                completed += 1;
            }
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
            }
        }
    }

    let best_trial_id = refresh_task_progress(db, task_id).await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "executed": pending_trials.len(),
        "completed": completed,
        "failed": failed,
        "best_trial_id": best_trial_id,
    }))
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

    let default_trial_id = best_trial_id
        .0
        .as_deref()
        .ok_or_else(|| "optimization task has no best_trial_id; run completed trials first".to_string())?;
    let normalized = normalize_promote_request(default_trial_id, req)?;

    let trial = sqlx::query_as::<_, (String, Value, Option<Value>, Option<Decimal>, Option<Value>, String)>(
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

    let best_trial_id = task
        .0
        .as_deref()
        .ok_or_else(|| "optimization task has no best_trial_id; run completed trials first".to_string())?;

    let trial = sqlx::query_as::<_, (Decimal, Option<Value>, Option<Value>)>(
        "SELECT score, metrics, constraint_violations
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND trial_id = $2 AND status = 'completed'",
    )
    .bind(task_id)
    .bind(best_trial_id)
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
    .bind(best_trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load runner-up optimization trial: {}", error))?
    .map(|row| row.0);

    let policy = gate_policy.cloned().unwrap_or_else(|| json!({
        "min_trade_count": 1,
        "max_drawdown": 0.20,
        "min_score_gap": 0.0
    }));
    let evaluation = evaluate_robustness_gates(
        trial.0,
        runner_up,
        trial.1.as_ref().unwrap_or(&json!({})),
        trial.2.as_ref().unwrap_or(&json!([])),
        Some(&policy),
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
    .bind(best_trial_id)
    .bind(&policy)
    .bind(&evaluation.gates)
    .bind(&evaluation.status)
    .bind(format!("Robustness gate evaluated as {}", evaluation.status))
    .execute(db)
    .await
    .map_err(|error| format!("Failed to persist robustness gate result: {}", error))?;

    Ok(json!({
        "gate_result_id": gate_result_id,
        "optimization_task_id": task_id,
        "trial_id": best_trial_id,
        "status": evaluation.status,
        "gate_results": evaluation.gates,
    }))
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

    Ok(RunFactorBacktestReq {
        combo_name: string_value("combo_name", None)?,
        version: string_value("version", Some("1.0.0"))?,
        strategy_version_id: task.strategy_version_id.clone(),
        data_version_id: task.data_version_id.clone(),
        research_dataset_id: optional_string("research_dataset_id")?,
        feature_set_version_id: optional_string("feature_set_version_id")?,
        prediction_set_id: optional_string("prediction_set_id")?,
        portfolio_policy_id: optional_string("portfolio_policy_id")?,
        top_n: usize_value("top_n", 20)?,
        rebalance: string_value("rebalance", Some("monthly"))?,
        entry_delay: usize_value("entry_delay", 0)?,
        min_amount: f64_value("min_amount", 0.0)?,
        max_position_pct: f64_value("max_position_pct", 0.10)?,
        skip_top_pct: f64_value("skip_top_pct", 0.0)?,
        cost_model: None,
        execution_rules: None,
        benchmark: optional_string("benchmark")?.or_else(|| Some("000300.SH".into())),
        start_date: string_value("start_date", None)?,
        end_date: string_value("end_date", None)?,
        initial_capital: f64_value("initial_capital", 1_000_000.0)?,
        mode: optional_string("mode")?.or_else(|| Some("standard".into())),
    })
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
            score -= Decimal::new((limit - metrics.num_trades as i64).max(0), 0) * Decimal::new(1, 1);
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

fn evaluate_robustness_gates(
    best_score: Decimal,
    runner_up_score: Option<Decimal>,
    metrics: &Value,
    constraint_violations: &Value,
    gate_policy: Option<&Value>,
) -> RobustnessEvaluation {
    let min_trade_count = constraint_i64(gate_policy, "min_trade_count").unwrap_or(1);
    let max_drawdown = constraint_decimal(gate_policy, "max_drawdown").unwrap_or(Decimal::new(20, 2));
    let min_score_gap = constraint_decimal(gate_policy, "min_score_gap").unwrap_or(Decimal::ZERO);

    let num_trades = metrics
        .get("num_trades")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let drawdown = decimal_from_json(metrics.get("max_drawdown_pct")).unwrap_or(Decimal::ZERO);
    let hard_violations = constraint_violations
        .as_array()
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let score_gap = runner_up_score.map(|runner_up| best_score - runner_up);

    let gates = vec![
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
            "passed": drawdown <= max_drawdown,
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

async fn mark_trial_running(
    db: &sqlx::PgPool,
    trial_id: &str,
) -> Result<(), String> {
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

async fn refresh_task_progress(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<Option<String>, String> {
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
        if counts.1 > 0 { "completed" } else { "failed" }
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

fn sample_parameter(
    name: &str,
    spec: &Value,
    rng: &mut DeterministicRng,
) -> Result<Value, String> {
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
        (self.next_u64() as usize) % upper_exclusive
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
                "max_position_pct": 0.10
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "rebalance": "5",
            "max_position_pct": 0.08
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(req.strategy_version_id, "factor-combo-v1");
        assert_eq!(req.data_version_id, "perf-db-smoke-data-v1");
        assert_eq!(req.combo_name, "phase3d_alpha_smoke");
        assert_eq!(req.top_n, 8);
        assert_eq!(req.rebalance, "5");
        assert_eq!(req.max_position_pct, 0.08);
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

        let normalized = normalize_promote_request(
            "trial-opt-demo-0001",
            req,
        )
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
    fn robustness_gate_approves_clean_dominant_trial() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({"num_trades": 12, "max_drawdown_pct": "0.08"}),
            &json!([]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20})),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(evaluation.gates.as_array().unwrap().iter().all(|gate| gate["passed"] == true));
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
}
