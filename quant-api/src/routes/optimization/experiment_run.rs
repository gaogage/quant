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

pub(crate) struct NormalizedPromoteRequest {
    pub(crate) trial_id: String,
    pub(crate) target_strategy_version: String,
    pub(crate) candidate_name: String,
    pub(crate) promotion_mode: String,
    pub(crate) gate_policy: String,
    pub(crate) freeze_after_approval: bool,
    pub(crate) reviewer: Option<String>,
    pub(crate) reason: String,
    pub(crate) notes: Option<String>,
    pub(crate) status: String,
}


pub(crate) struct Phase7LayeredPlanBundle {
    pub(crate) resource_plan: LocalResourcePlan,
    pub(crate) plan: LayeredSearchPlan,
    pub(crate) search_space: Value,
}


#[derive(Debug, Clone)]
pub(crate) struct CompletedTrialSnapshot {
    pub(crate) trial_id: String,
    pub(crate) trial_index: i32,
    pub(crate) backtest_task_id: Option<String>,
    pub(crate) score: Option<Decimal>,
    pub(crate) metrics: Value,
    pub(crate) metric_sources: Value,
    pub(crate) missing_elite_metrics: Value,
    pub(crate) constraint_violations: Value,
    pub(crate) parameters: Value,
}


#[derive(Debug, Clone, Copy)]
struct EliteMetricProfile {
    annual_return: f64,
    excess_return: f64,
    sharpe: f64,
    sortino: f64,
    calmar: f64,
    profit_factor: f64,
    max_drawdown: f64,
    max_drawdown_duration_days: f64,
    num_trades: f64,
}


pub(crate) fn cleanup_dry_run_default(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}


pub(crate) fn cleanup_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(3600).clamp(60, 86_400)
}


pub(crate) fn cleanup_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}


pub(crate) fn optimization_cleanup_task_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" => Some("cancelled"),
        "running" | "cancel_requested" => Some("timeout"),
        _ => None,
    }
}


#[cfg(test)]
pub(crate) fn optimization_cleanup_trial_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" => Some("cancelled"),
        "running" | "cancel_requested" => Some("timeout"),
        _ => None,
    }
}


pub(crate) fn experiment_cleanup_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" | "running" => Some("failed"),
        _ => None,
    }
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
            "created_at": fmt_rfc3339_local(row.12),
            "trial_total": counts.0,
            "trial_completed": counts.1,
            "trial_failed": counts.2,
        }
    }))
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


pub(crate) fn missing_strategy_version_error_message(strategy_version_id: &str) -> String {
    format!(
        "strategy_version_id '{}' does not exist in strategy_version; use an existing canonical strategy_version_id or register the strategy version before running optimization",
        strategy_version_id
    )
}


pub(crate) fn missing_optimization_data_version_error_message(data_version_id: &str) -> String {
    format!(
        "data_version_id '{}' does not exist in data_version; run data readiness/sync first or use an existing canonical data_version_id",
        data_version_id
    )
}


pub(crate) async fn ensure_phase7_oos_request_references_exist(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Result<(), String> {
    let strategy_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM strategy_version WHERE strategy_version_id = $1)",
    )
    .bind(&req.strategy_version_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to check strategy_version: {}", error))?;
    if !strategy_exists {
        return Err(missing_strategy_version_error_message(
            &req.strategy_version_id,
        ));
    }

    let data_version_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM data_version WHERE data_version_id = $1)")
            .bind(&req.data_version_id)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to check data_version: {}", error))?;
    if !data_version_exists {
        return Err(missing_optimization_data_version_error_message(
            &req.data_version_id,
        ));
    }

    Ok(())
}


pub async fn cleanup_stale_optimization_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleBackgroundTasksReq>,
) -> impl IntoResponse {
    match cleanup_stale_optimization_tasks_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

/// POST /api/v1/quant/experiments/cleanup-stale
///
/// 清理 started_at/created_at 超时的 experiment_run 元数据。
/// 默认 dry_run=true；actual update 会把 pending/running 标记为 failed，
/// 并在 metrics 中写入 stale_cleanup 审计字段。

pub async fn cleanup_stale_experiment_runs(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleBackgroundTasksReq>,
) -> impl IntoResponse {
    match cleanup_stale_experiment_runs_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub(crate) async fn cleanup_stale_optimization_tasks_inner(
    db: &sqlx::PgPool,
    req: CleanupStaleBackgroundTasksReq,
) -> Result<Value, String> {
    let dry_run = cleanup_dry_run_default(req.dry_run);
    let default_timeout_seconds = cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = cleanup_limit(req.limit);
    let rows: Vec<(
        String,
        String,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT optimization_task_id, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, created_at
         FROM optimization_task
         WHERE status IN ('pending', 'running', 'cancel_requested')
           AND COALESCE(last_heartbeat_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("cleanup stale optimization tasks query failed: {error}"))?;

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.unwrap_or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "optimization_task_id": task_id,
                    "status": status,
                    "next_status": optimization_cleanup_task_status_transition(status),
                    "progress": progress,
                    "last_heartbeat_at": fmt_rfc3339_local(*last_heartbeat_at),
                    "created_at": fmt_rfc3339_local(Some(*created_at)),
                    "observed_at": fmt_rfc3339_local(Some(observed_at)),
                    "heartbeat_timeout_seconds": timeout_seconds,
                })
            },
        )
        .collect::<Vec<_>>();

    if dry_run || rows.is_empty() {
        return Ok(json!({
            "dry_run": dry_run,
            "candidate_count": candidates.len(),
            "updated_task_count": 0,
            "updated_trial_count": 0,
            "candidates": candidates,
        }));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();
    let trial_result = sqlx::query(
        "UPDATE optimization_trial
         SET status = CASE
                 WHEN status = 'pending' THEN 'cancelled'
                 ELSE 'timeout'
             END,
             progress = 100,
             completed_at = COALESCE(completed_at, now()),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 'stale optimization task cleaned up by cleanup-stale: parent task heartbeat timed out'
             )
         WHERE optimization_task_id = ANY($1)
           AND status IN ('pending', 'running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale optimization trials update failed: {error}"))?;

    let task_result = sqlx::query(
        "UPDATE optimization_task
         SET status = CASE
                 WHEN status = 'pending' THEN 'cancelled'
                 ELSE 'timeout'
             END,
             progress = 100,
             last_heartbeat_at = now()
         WHERE optimization_task_id = ANY($1)
           AND status IN ('pending', 'running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale optimization tasks update failed: {error}"))?;

    Ok(json!({
        "dry_run": false,
        "candidate_count": candidates.len(),
        "updated_task_count": task_result.rows_affected(),
        "updated_trial_count": trial_result.rows_affected(),
        "candidates": candidates,
    }))
}


pub(crate) async fn cleanup_stale_experiment_runs_inner(
    db: &sqlx::PgPool,
    req: CleanupStaleBackgroundTasksReq,
) -> Result<Value, String> {
    let dry_run = cleanup_dry_run_default(req.dry_run);
    let default_timeout_seconds = cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = cleanup_limit(req.limit);
    let rows: Vec<(
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                status, started_at, created_at
         FROM experiment_run
         WHERE status IN ('pending', 'running')
           AND COALESCE(started_at, created_at)
               < now() - ($1::text || ' seconds')::interval
         ORDER BY COALESCE(started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("cleanup stale experiment runs query failed: {error}"))?;

    let candidates = rows
        .iter()
        .map(
            |(
                experiment_run_id,
                experiment_type,
                related_entity_type,
                related_entity_id,
                status,
                started_at,
                created_at,
            )| {
                let observed_at = started_at.unwrap_or(*created_at);
                json!({
                    "experiment_run_id": experiment_run_id,
                    "experiment_type": experiment_type,
                    "related_entity_type": related_entity_type,
                    "related_entity_id": related_entity_id,
                    "status": status,
                    "next_status": experiment_cleanup_status_transition(status),
                    "started_at": fmt_rfc3339_local(*started_at),
                    "created_at": fmt_rfc3339_local(Some(*created_at)),
                    "observed_at": fmt_rfc3339_local(Some(observed_at)),
                    "timeout_seconds": default_timeout_seconds,
                })
            },
        )
        .collect::<Vec<_>>();

    if dry_run || rows.is_empty() {
        return Ok(json!({
            "dry_run": dry_run,
            "candidate_count": candidates.len(),
            "updated_experiment_count": 0,
            "candidates": candidates,
        }));
    }

    let experiment_run_ids = rows
        .iter()
        .map(|(experiment_run_id, ..)| experiment_run_id.clone())
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "UPDATE experiment_run
         SET status = 'failed',
             completed_at = now(),
             metrics = COALESCE(metrics, '{}'::jsonb) || jsonb_build_object(
                 'status', 'failed',
                 'stale_cleanup', jsonb_build_object(
                     'reason', 'stale experiment run cleaned up by cleanup-stale: no completion before timeout',
                     'timeout_seconds', $2,
                     'cleaned_at', now()
                 )
             )
         WHERE experiment_run_id = ANY($1)
           AND status IN ('pending', 'running')",
    )
    .bind(&experiment_run_ids)
    .bind(default_timeout_seconds)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale experiment runs update failed: {error}"))?;

    Ok(json!({
        "dry_run": false,
        "candidate_count": candidates.len(),
        "updated_experiment_count": result.rows_affected(),
        "candidates": candidates,
    }))
}


pub async fn get_experiment_run(
    State(state): State<Arc<AppState>>,
    Path(experiment_run_id): Path<String>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            Option<String>,
            Value,
            Option<Value>,
            String,
            Option<String>,
            Option<String>,
            String,
        ),
    >(
        "SELECT experiment_run_id,
                experiment_type,
                related_entity_type,
                related_entity_id,
                config,
                metrics,
                status,
                started_at::text,
                completed_at::text,
                created_at::text
         FROM experiment_run
         WHERE experiment_run_id = $1",
    )
    .bind(&experiment_run_id)
    .fetch_optional(&state.db)
    .await;

    match row {
        Ok(Some(row)) => Json(json!({
            "code": 0,
            "data": {
                "experiment_run_id": row.0,
                "experiment_type": row.1,
                "related_entity_type": row.2,
                "related_entity_id": row.3,
                "config": row.4,
                "metrics": row.5,
                "status": row.6,
                "started_at": row.7,
                "completed_at": row.8,
                "created_at": row.9,
            }
        })),
        Ok(None) => Json(json!({
            "code": 1,
            "message": format!("experiment_run not found: {}", experiment_run_id)
        })),
        Err(error) => Json(json!({
            "code": 1,
            "message": format!("Failed to get experiment_run: {}", error)
        })),
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


pub(crate) async fn execute_phase7_professional_discovery(
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
    let gate_policy = resolve_robustness_gate_policy(req.robustness_gate_policy.as_ref());
    let stop_after_professional = req.stop_after_professional_candidate.unwrap_or(true);
    let stop_after_robust_approval = req.stop_after_robust_approval.unwrap_or(true);

    let mut batch_summaries = Vec::new();
    let mut robustness_results = Vec::new();
    let mut evaluated_robustness_trials = BTreeSet::new();
    let mut signal_cache = SignalDataCache::default();
    let mut backtest_cache = BacktestDataCache::default();
    let mut approved_candidate_found = false;
    let mut stop_reason = "planned_trials_exhausted";
    for _ in 0..max_batches {
        let summary = execute_pending_trials_with_caches(
            db,
            &task_id,
            trial_batch_limit,
            None,
            &mut signal_cache,
            &mut backtest_cache,
        )
        .await?;
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


pub async fn generate_elite_validation_report(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<EliteValidationReportRequest>,
) -> impl IntoResponse {
    match build_and_persist_elite_validation_report(&state.db, &task_id, req).await {
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


pub(crate) async fn persist_optimization_experiment_run(
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

    create_experiment_run(
        db,
        "optimization_trial_batch_release_gate",
        "optimization_task",
        Some(task_id),
        &config,
        &metrics,
        experiment_status,
    )
    .await
    .map_err(|error| format!("Failed to insert optimization experiment_run: {}", error))
}


pub(crate) async fn promote_trial(
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


pub(crate) async fn build_and_persist_elite_validation_report(
    db: &sqlx::PgPool,
    task_id: &str,
    req: EliteValidationReportRequest,
) -> Result<Value, String> {
    let top_n = req.top_n.unwrap_or(3).clamp(1, 10);
    let requested_policy = req
        .gate_policy
        .unwrap_or_else(|| json!({"preset": "professional_elite"}));
    let gate_policy = resolve_robustness_gate_policy(Some(&requested_policy));
    let snapshots = load_completed_trial_snapshots(db, task_id).await?;
    if snapshots.is_empty() {
        return Err("optimization task has no completed trials with metrics".to_string());
    }

    let mut ranked = snapshots.clone();
    ranked.sort_by(elite_trial_report_order);
    let selected = ranked.into_iter().take(top_n).collect::<Vec<_>>();

    let mut rows = Vec::new();
    for (rank, trial) in selected.iter().enumerate() {
        let robustness = evaluate_and_persist_robustness_for_trial(
            db,
            task_id,
            &trial.trial_id,
            Some(&gate_policy),
            "Professional elite validation report gate evaluated",
        )
        .await?;
        rows.push(json!({
            "rank": rank + 1,
            "trial_id": trial.trial_id,
            "trial_index": trial.trial_index,
            "backtest_task_id": trial.backtest_task_id,
            "score": trial.score,
            "elite_gap_score": elite_gap_score(&trial.metrics),
            "metrics": trial.metrics,
            "metric_sources": trial.metric_sources,
            "missing_elite_metrics": trial.missing_elite_metrics,
            "constraint_violations": trial.constraint_violations,
            "parameters": trial.parameters,
            "elite_status": robustness["status"],
            "elite_robustness": robustness,
            "parameter_plateau": build_parameter_plateau_analysis(trial, &snapshots),
            "portfolio_correlation_contribution": build_portfolio_correlation_contribution_score(
                trial,
                &snapshots
            ),
        }));
    }

    let summary = summarize_elite_validation_rows(&rows);
    let metrics = json!({
        "top_n": top_n,
        "candidate_count": rows.len(),
        "summary": summary,
        "candidates": rows,
    });
    let config = json!({
        "optimization_task_id": task_id,
        "gate_policy": gate_policy,
        "ranking": "elite_gap_score_then_drawdown_then_sortino_sharpe_annual",
    });
    let experiment_run_id =
        persist_elite_validation_report_experiment(db, task_id, &config, &metrics).await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "experiment_run_id": experiment_run_id,
        "gate_policy": config["gate_policy"],
        "top_n": top_n,
        "summary": metrics["summary"],
        "candidates": metrics["candidates"],
    }))
}


pub(crate) async fn load_completed_trial_snapshots(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<Vec<CompletedTrialSnapshot>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            Value,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            Option<String>,
        ),
    >(
        "SELECT trial_id, trial_index, parameters, score, metrics, constraint_violations,
                backtest_task_id
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND status = 'completed'
           AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load completed optimization trials: {}", error))?;

    let mut snapshots = Vec::new();
    for (trial_id, trial_index, parameters, score, metrics, violations, backtest_task_id) in rows {
        let Some(metrics) = metrics else {
            continue;
        };
        let (metrics, metric_sources, missing_elite_metrics) =
            enrich_trial_metrics(db, backtest_task_id.as_deref(), metrics).await?;
        snapshots.push(CompletedTrialSnapshot {
            trial_id,
            trial_index,
            backtest_task_id,
            score,
            metrics,
            metric_sources,
            missing_elite_metrics,
            constraint_violations: violations.unwrap_or_else(|| json!([])),
            parameters,
        });
    }
    Ok(snapshots)
}


pub(crate) async fn enrich_trial_metrics(
    db: &sqlx::PgPool,
    backtest_task_id: Option<&str>,
    mut metrics: Value,
) -> Result<(Value, Value, Value), String> {
    let mut sources = existing_metric_sources(&metrics);
    if let Some(backtest_task_id) = backtest_task_id {
        enrich_trial_metrics_from_backtest_result(db, backtest_task_id, &mut metrics, &mut sources)
            .await?;
        if !metric_has_number(&metrics, "max_drawdown_duration_days") {
            if let Some(duration_days) =
                derive_max_drawdown_duration_days(db, backtest_task_id).await?
            {
                insert_metric_if_missing(
                    &mut metrics,
                    &mut sources,
                    "max_drawdown_duration_days",
                    Some(json!(duration_days)),
                    "backtest_equity_curve.portfolio_value",
                );
            }
        }
    }
    let missing = missing_elite_metrics(&metrics);
    Ok((metrics, Value::Object(sources), json!(missing)))
}


pub(crate) async fn enrich_trial_metrics_from_backtest_result(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
    metrics: &mut Value,
    sources: &mut Map<String, Value>,
) -> Result<(), String> {
    let row = sqlx::query_as::<
        _,
        (
            Option<Value>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<i32>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
        ),
    >(
        "SELECT metrics,
                total_return,
                annualized_return,
                benchmark_return,
                excess_return,
                annualized_excess_return,
                sharpe_ratio,
                sortino_ratio,
                information_ratio,
                max_drawdown,
                turnover,
                total_trades,
                win_rate,
                calmar_ratio,
                annualized_volatility
         FROM backtest_result
         WHERE task_id = $1",
    )
    .bind(backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_result metrics: {}", error))?;

    let Some(row) = row else {
        return Ok(());
    };

    if let Some(backtest_metrics) = row.0.as_ref().and_then(Value::as_object) {
        for key in ELITE_REPORT_METRIC_KEYS {
            insert_metric_if_missing(
                metrics,
                sources,
                key,
                backtest_metrics.get(*key).cloned(),
                "backtest_result.metrics",
            );
        }
    }

    insert_metric_if_missing(
        metrics,
        sources,
        "total_return",
        row.1.map(|value| json!(value)),
        "backtest_result.total_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "annual_return_pct",
        row.2.map(|value| json!(value)),
        "backtest_result.annualized_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "benchmark_return_pct",
        row.3.map(|value| json!(value)),
        "backtest_result.benchmark_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "excess_return_pct",
        row.4.or(row.5).map(|value| json!(value)),
        "backtest_result.excess_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "sharpe_ratio",
        row.6.map(|value| json!(value)),
        "backtest_result.sharpe_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "sortino_ratio",
        row.7.map(|value| json!(value)),
        "backtest_result.sortino_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "information_ratio",
        row.8.map(|value| json!(value)),
        "backtest_result.information_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "max_drawdown_pct",
        row.9.map(|value| json!(value)),
        "backtest_result.max_drawdown",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "turnover",
        row.10.map(|value| json!(value)),
        "backtest_result.turnover",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "num_trades",
        row.11.map(|value| json!(value)),
        "backtest_result.total_trades",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "win_rate_pct",
        row.12.map(|value| json!(value)),
        "backtest_result.win_rate",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "calmar_ratio",
        row.13.map(|value| json!(value)),
        "backtest_result.calmar_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "annualized_volatility",
        row.14.map(|value| json!(value)),
        "backtest_result.annualized_volatility",
    );

    Ok(())
}


pub(crate) async fn derive_max_drawdown_duration_days(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
) -> Result<Option<i64>, String> {
    let rows = sqlx::query_as::<_, (Option<Decimal>,)>(
        "SELECT portfolio_value
         FROM backtest_equity_curve
         WHERE task_id = $1
         ORDER BY trade_date ASC",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load backtest equity curve: {}", error))?;

    let mut peak: Option<Decimal> = None;
    let mut current_duration = 0i64;
    let mut max_duration = 0i64;
    let mut observed = false;
    for (portfolio_value,) in rows {
        let Some(portfolio_value) = portfolio_value else {
            continue;
        };
        if portfolio_value <= Decimal::ZERO {
            continue;
        }
        observed = true;
        if peak.map(|value| portfolio_value >= value).unwrap_or(true) {
            peak = Some(portfolio_value);
            current_duration = 0;
        } else {
            current_duration += 1;
            max_duration = max_duration.max(current_duration);
        }
    }

    Ok(observed.then_some(max_duration))
}


pub(crate) const ELITE_REPORT_METRIC_KEYS: &[&str] = &[
    "annual_return_pct",
    "excess_return_pct",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "profit_factor",
    "max_drawdown_pct",
    "max_drawdown_duration_days",
    "num_trades",
];


pub(crate) fn existing_metric_sources(metrics: &Value) -> Map<String, Value> {
    let mut sources = Map::new();
    for key in ELITE_REPORT_METRIC_KEYS {
        if metric_has_number(metrics, key) {
            sources.insert((*key).to_string(), json!("optimization_trial.metrics"));
        }
    }
    sources
}


pub(crate) fn insert_metric_if_missing(
    metrics: &mut Value,
    sources: &mut Map<String, Value>,
    key: &str,
    value: Option<Value>,
    source: &str,
) {
    if metric_has_number(metrics, key) {
        return;
    }
    let Some(value) = value else {
        return;
    };
    if value_as_f64(&value).map(|value| value.is_finite()) != Some(true) {
        return;
    }
    ensure_metrics_object(metrics).insert(key.to_string(), value);
    sources.insert(key.to_string(), json!(source));
}


pub(crate) fn ensure_metrics_object(metrics: &mut Value) -> &mut Map<String, Value> {
    if !metrics.is_object() {
        *metrics = json!({});
    }
    metrics.as_object_mut().expect("metrics object")
}


pub(crate) fn metric_has_number(metrics: &Value, key: &str) -> bool {
    metrics
        .get(key)
        .and_then(value_as_f64)
        .map(|value| value.is_finite())
        .unwrap_or(false)
}


pub(crate) fn missing_elite_metrics(metrics: &Value) -> Vec<&'static str> {
    ELITE_REPORT_METRIC_KEYS
        .iter()
        .copied()
        .filter(|key| !metric_has_number(metrics, key))
        .collect()
}


pub(crate) async fn persist_elite_validation_report_experiment(
    db: &sqlx::PgPool,
    task_id: &str,
    config: &Value,
    metrics: &Value,
) -> Result<String, String> {
    create_experiment_run(
        db,
        "professional_elite_validation_report",
        "optimization_task",
        Some(task_id),
        config,
        metrics,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to insert elite validation experiment_run: {}", error))
}


pub(crate) fn summarize_elite_validation_rows(rows: &[Value]) -> Value {
    let approved = rows
        .iter()
        .filter(|row| row["elite_status"] == "approved_candidate")
        .count();
    let review_required = rows
        .iter()
        .filter(|row| row["elite_status"] == "review_required")
        .count();
    let rejected = rows
        .iter()
        .filter(|row| row["elite_status"] == "rejected")
        .count();
    let best = rows.first().cloned().unwrap_or_else(|| json!({}));
    json!({
        "approved_count": approved,
        "review_required_count": review_required,
        "rejected_count": rejected,
        "missing_elite_metric_counts": summarize_missing_elite_metric_counts(rows),
        "best_trial_id": best["trial_id"],
        "best_elite_status": best["elite_status"],
        "best_elite_gap_score": best["elite_gap_score"],
        "best_metrics": best["metrics"],
        "best_missing_elite_metrics": best["missing_elite_metrics"],
    })
}


pub(crate) fn summarize_missing_elite_metric_counts(rows: &[Value]) -> Value {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows {
        let Some(missing) = row["missing_elite_metrics"].as_array() else {
            continue;
        };
        for item in missing {
            if let Some(key) = item.as_str() {
                *counts.entry(key.to_string()).or_default() += 1;
            }
        }
    }
    json!(counts)
}


pub(crate) fn elite_trial_report_order(
    left: &CompletedTrialSnapshot,
    right: &CompletedTrialSnapshot,
) -> std::cmp::Ordering {
    elite_gap_score(&left.metrics)
        .partial_cmp(&elite_gap_score(&right.metrics))
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            metric_number(&left.metrics, "max_drawdown_pct")
                .partial_cmp(&metric_number(&right.metrics, "max_drawdown_pct"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "sortino_ratio")
                .partial_cmp(&metric_number(&left.metrics, "sortino_ratio"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "sharpe_ratio")
                .partial_cmp(&metric_number(&left.metrics, "sharpe_ratio"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "annual_return_pct")
                .partial_cmp(&metric_number(&left.metrics, "annual_return_pct"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.trial_index.cmp(&right.trial_index))
}


pub(crate) fn elite_gap_score(metrics: &Value) -> f64 {
    let profile = EliteMetricProfile::from_metrics(metrics);
    positive_shortfall(0.15, profile.annual_return) * 4.0
        + positive_shortfall(0.0, profile.excess_return) * 2.0
        + positive_shortfall(1.5, profile.sharpe) * 1.5
        + positive_shortfall(1.8, profile.sortino)
        + positive_shortfall(2.0, profile.calmar) * 0.75
        + positive_shortfall(1.5, profile.profit_factor) * 0.75
        + positive_shortfall(profile.max_drawdown, 0.35) * 3.0
        + positive_shortfall(profile.max_drawdown_duration_days, 126.0) / 252.0
        + positive_shortfall(200.0, profile.num_trades) / 200.0
}


pub(crate) fn build_parameter_plateau_analysis(
    candidate: &CompletedTrialSnapshot,
    trials: &[CompletedTrialSnapshot],
) -> Value {
    let candidate_metrics = EliteMetricProfile::from_metrics(&candidate.metrics);
    let mut near_neighbor_count = 0usize;
    let mut stable_neighbor_count = 0usize;
    let mut axis_peers: BTreeMap<String, Vec<&CompletedTrialSnapshot>> = BTreeMap::new();

    for peer in trials {
        if peer.trial_id == candidate.trial_id {
            continue;
        }
        let diff_keys = differing_parameter_keys(&candidate.parameters, &peer.parameters);
        if diff_keys.is_empty() {
            continue;
        }
        if diff_keys.len() <= 3 {
            near_neighbor_count += 1;
            if is_plateau_stable_neighbor(candidate_metrics, &peer.metrics) {
                stable_neighbor_count += 1;
            }
        }
        if diff_keys.len() == 1 {
            axis_peers
                .entry(diff_keys[0].clone())
                .or_default()
                .push(peer);
        }
    }

    let axes = axis_peers
        .iter()
        .map(|(axis, peers)| parameter_axis_plateau_summary(axis, candidate, peers))
        .collect::<Vec<_>>();
    let plateau_score = ratio(stable_neighbor_count, near_neighbor_count);

    json!({
        "plateau_score": plateau_score,
        "near_neighbor_count": near_neighbor_count,
        "stable_neighbor_count": stable_neighbor_count,
        "stable_neighbor_ratio": plateau_score,
        "stability_rule": {
            "annual_return_drawdown": "peer annual_return >= candidate - 1pp and max_drawdown <= candidate + 3pp",
            "risk_adjusted": "peer Sharpe >= candidate - 0.10 and Sortino >= candidate - 0.15",
            "near_neighbor": "parameter diff count <= 3"
        },
        "single_axis": axes,
    })
}


pub(crate) fn parameter_axis_plateau_summary(
    axis: &str,
    candidate: &CompletedTrialSnapshot,
    peers: &[&CompletedTrialSnapshot],
) -> Value {
    let stable = peers
        .iter()
        .filter(|peer| {
            is_plateau_stable_neighbor(
                EliteMetricProfile::from_metrics(&candidate.metrics),
                &peer.metrics,
            )
        })
        .count();
    let mut values = peers
        .iter()
        .map(|peer| parameter_value(&peer.parameters, axis))
        .collect::<Vec<_>>();
    values.sort_by_key(canonical_json);
    values.dedup();

    let best_peer = peers
        .iter()
        .min_by(|left, right| elite_trial_report_order(left, right))
        .map(|peer| compact_trial_metrics(peer));
    json!({
        "parameter": axis,
        "candidate_value": parameter_value(&candidate.parameters, axis),
        "peer_values": values,
        "peer_count": peers.len(),
        "stable_peer_count": stable,
        "stable_ratio": ratio(stable, peers.len()),
        "best_peer": best_peer,
    })
}


pub(crate) fn is_plateau_stable_neighbor(candidate: EliteMetricProfile, peer_metrics: &Value) -> bool {
    let peer = EliteMetricProfile::from_metrics(peer_metrics);
    peer.annual_return >= candidate.annual_return - 0.01
        && peer.sharpe >= candidate.sharpe - 0.10
        && peer.sortino >= candidate.sortino - 0.15
        && peer.max_drawdown <= candidate.max_drawdown + 0.03
}


pub(crate) fn build_portfolio_correlation_contribution_score(
    candidate: &CompletedTrialSnapshot,
    trials: &[CompletedTrialSnapshot],
) -> Value {
    let explicit_score = correlation_control_score(&candidate.parameters);
    let controlled = trials
        .iter()
        .filter(|trial| correlation_control_score(&trial.parameters) > 0.0)
        .collect::<Vec<_>>();
    let uncontrolled = trials
        .iter()
        .filter(|trial| correlation_control_score(&trial.parameters) == 0.0)
        .collect::<Vec<_>>();
    let controlled_summary = summarize_trial_metric_group(&controlled);
    let uncontrolled_summary = summarize_trial_metric_group(&uncontrolled);
    let peer_impact = correlation_peer_impact(&controlled_summary, &uncontrolled_summary);
    let contribution_score = (explicit_score * 0.70
        + peer_impact["normalized_score"].as_f64().unwrap_or(0.0) * 0.30)
        .clamp(0.0, 1.0);

    json!({
        "contribution_score": contribution_score,
        "explicit_control_score": explicit_score,
        "active_controls": active_correlation_controls(&candidate.parameters),
        "controlled_peer_summary": controlled_summary,
        "uncontrolled_peer_summary": uncontrolled_summary,
        "peer_impact": peer_impact,
    })
}


pub(crate) fn active_correlation_controls(parameters: &Value) -> Vec<Value> {
    let mut controls = Vec::new();
    if let Some(limit) = parameter_f64(parameters, "max_pairwise_correlation") {
        controls.push(json!({"name": "max_pairwise_correlation", "value": limit}));
    }
    if let Some(lookback) = parameter_f64(parameters, "correlation_lookback_days") {
        controls.push(json!({"name": "correlation_lookback_days", "value": lookback}));
    }
    if let Some(filter) = parameter_str(parameters, "candidate_risk_filter") {
        if filter.contains("correlation") {
            controls.push(json!({"name": "candidate_risk_filter", "value": filter}));
        }
    }
    if let Some(method) = parameter_str(parameters, "portfolio_method") {
        if method == "risk_budget" || method == "min_variance" {
            controls.push(json!({"name": "portfolio_method", "value": method}));
        }
    }
    if let Some(control) = parameter_str(parameters, "risk_contribution_control") {
        if control != "off" {
            controls.push(json!({"name": "risk_contribution_control", "value": control}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "style_risk_budget") {
        if budget != "off" {
            controls.push(json!({"name": "style_risk_budget", "value": budget}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "capacity_risk_budget") {
        if budget != "off" {
            controls.push(json!({"name": "capacity_risk_budget", "value": budget}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "execution_impact_budget") {
        if budget != "off" {
            controls.push(json!({"name": "execution_impact_budget", "value": budget}));
        }
    }
    if let Some(profile) = parameter_str(parameters, "execution_schedule_profile") {
        if profile != "immediate" && profile != "off" {
            controls.push(json!({"name": "execution_schedule_profile", "value": profile}));
        }
    }
    if let Some(policy) = parameter_str(parameters, "execution_carry_policy") {
        if policy != "expire" && policy != "expire_v1" {
            controls.push(json!({"name": "execution_carry_policy", "value": policy}));
        }
    }
    if let Some(limit) = parameter_str(parameters, "execution_daily_target_move_limit_pct") {
        controls.push(json!({"name": "execution_daily_target_move_limit_pct", "value": limit}));
    }
    if let Some(days) = parameter_i64(parameters, "execution_max_carry_days") {
        controls.push(json!({"name": "execution_max_carry_days", "value": days}));
    }
    controls
}


pub(crate) fn correlation_control_score(parameters: &Value) -> f64 {
    let mut score: f64 = 0.0;
    if let Some(limit) = parameter_f64(parameters, "max_pairwise_correlation") {
        score += if limit <= 0.68 {
            0.35
        } else if limit <= 0.70 {
            0.30
        } else if limit <= 0.75 {
            0.20
        } else {
            0.10
        };
    }
    if parameter_str(parameters, "candidate_risk_filter")
        .map(|value| value.contains("correlation"))
        .unwrap_or(false)
    {
        score += 0.25;
    }
    if parameter_str(parameters, "portfolio_method")
        .map(|value| value == "risk_budget" || value == "min_variance")
        .unwrap_or(false)
    {
        score += 0.15;
    }
    if parameter_str(parameters, "risk_contribution_control")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.15;
    }
    if parameter_str(parameters, "style_risk_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "capacity_risk_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_impact_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_schedule_profile")
        .map(|value| value != "immediate" && value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_daily_target_move_limit_pct").is_some() {
        score += 0.05;
    }
    if parameter_i64(parameters, "execution_max_carry_days").is_some() {
        score += 0.05;
    }
    if let Some(lookback) = parameter_f64(parameters, "correlation_lookback_days") {
        score += if lookback >= 120.0 { 0.10 } else { 0.05 };
    }
    score.clamp(0.0, 1.0)
}


pub(crate) fn summarize_trial_metric_group(trials: &[&CompletedTrialSnapshot]) -> Value {
    if trials.is_empty() {
        return json!({
            "count": 0,
            "annual_return_avg": null,
            "sharpe_avg": null,
            "sortino_avg": null,
            "calmar_avg": null,
            "max_drawdown_avg": null,
        });
    }
    let count = trials.len() as f64;
    let profiles = trials
        .iter()
        .map(|trial| EliteMetricProfile::from_metrics(&trial.metrics))
        .collect::<Vec<_>>();
    json!({
        "count": trials.len(),
        "annual_return_avg": profiles.iter().map(|item| item.annual_return).sum::<f64>() / count,
        "sharpe_avg": profiles.iter().map(|item| item.sharpe).sum::<f64>() / count,
        "sortino_avg": profiles.iter().map(|item| item.sortino).sum::<f64>() / count,
        "calmar_avg": profiles.iter().map(|item| item.calmar).sum::<f64>() / count,
        "max_drawdown_avg": profiles.iter().map(|item| item.max_drawdown).sum::<f64>() / count,
    })
}


pub(crate) fn correlation_peer_impact(controlled: &Value, uncontrolled: &Value) -> Value {
    let controlled_count = controlled["count"].as_u64().unwrap_or(0);
    let uncontrolled_count = uncontrolled["count"].as_u64().unwrap_or(0);
    if controlled_count == 0 || uncontrolled_count == 0 {
        return json!({
            "normalized_score": 0.0,
            "reason": "insufficient_controlled_or_uncontrolled_peers",
        });
    }
    let sharpe_delta = controlled["sharpe_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["sharpe_avg"].as_f64().unwrap_or(0.0);
    let drawdown_delta = controlled["max_drawdown_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["max_drawdown_avg"].as_f64().unwrap_or(0.0);
    let annual_delta = controlled["annual_return_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["annual_return_avg"].as_f64().unwrap_or(0.0);
    let normalized_score = (0.50
        + sharpe_delta * 0.50
        + positive_shortfall(0.0, drawdown_delta) * 0.80
        + annual_delta.max(-0.05) * 0.20)
        .clamp(0.0, 1.0);
    json!({
        "normalized_score": normalized_score,
        "annual_return_delta": annual_delta,
        "sharpe_delta": sharpe_delta,
        "max_drawdown_delta": drawdown_delta,
    })
}


pub(crate) fn differing_parameter_keys(left: &Value, right: &Value) -> Vec<String> {
    let Some(left_map) = left.as_object() else {
        return Vec::new();
    };
    let Some(right_map) = right.as_object() else {
        return Vec::new();
    };
    let keys = left_map
        .keys()
        .chain(right_map.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter(|key| left_map.get(key) != right_map.get(key))
        .collect()
}


pub(crate) fn parameter_value(parameters: &Value, key: &str) -> Value {
    parameters.get(key).cloned().unwrap_or(Value::Null)
}


pub(crate) fn parameter_f64(parameters: &Value, key: &str) -> Option<f64> {
    parameters.get(key).and_then(value_as_f64)
}


pub(crate) fn parameter_i64(parameters: &Value, key: &str) -> Option<i64> {
    parameters.get(key).and_then(Value::as_i64)
}


pub(crate) fn parameter_str<'a>(parameters: &'a Value, key: &str) -> Option<&'a str> {
    parameters.get(key).and_then(Value::as_str)
}


pub(crate) fn compact_trial_metrics(trial: &CompletedTrialSnapshot) -> Value {
    json!({
        "trial_id": trial.trial_id,
        "trial_index": trial.trial_index,
        "elite_gap_score": elite_gap_score(&trial.metrics),
        "annual_return_pct": metric_number(&trial.metrics, "annual_return_pct"),
        "sharpe_ratio": metric_number(&trial.metrics, "sharpe_ratio"),
        "sortino_ratio": metric_number(&trial.metrics, "sortino_ratio"),
        "calmar_ratio": metric_number(&trial.metrics, "calmar_ratio"),
        "max_drawdown_pct": metric_number(&trial.metrics, "max_drawdown_pct"),
    })
}


pub(crate) fn metric_number(metrics: &Value, name: &str) -> f64 {
    metrics.get(name).and_then(value_as_f64).unwrap_or(0.0)
}


pub(crate) fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}


impl EliteMetricProfile {
    fn from_metrics(metrics: &Value) -> Self {
        Self {
            annual_return: metric_number(metrics, "annual_return_pct"),
            excess_return: metric_number(metrics, "excess_return_pct"),
            sharpe: metric_number(metrics, "sharpe_ratio"),
            sortino: metric_number(metrics, "sortino_ratio"),
            calmar: metric_number(metrics, "calmar_ratio"),
            profit_factor: metric_number(metrics, "profit_factor"),
            max_drawdown: metric_number(metrics, "max_drawdown_pct"),
            max_drawdown_duration_days: metric_number(metrics, "max_drawdown_duration_days"),
            num_trades: metric_number(metrics, "num_trades"),
        }
    }
}


pub(crate) fn normalize_promote_request(
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


pub(crate) async fn load_execution_context(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<OptimizationTaskExecutionContext, String> {
    let row = sqlx::query_as::<_, (String, String, Value, Option<Value>, Option<Value>)>(
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
        backtest_template: row.3.unwrap_or_else(|| json!({})),
        constraints: row.4,
    })
}


pub(crate) async fn mark_trial_running(db: &sqlx::PgPool, trial_id: &str) -> Result<(), String> {
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


pub(crate) async fn mark_trial_completed(
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


pub(crate) async fn mark_trial_failed(
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


pub(crate) async fn refresh_task_progress(db: &sqlx::PgPool, task_id: &str) -> Result<Option<String>, String> {
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

// ─── DDD Step 4b：experiment_run 通用写入 ─────────────────────────
//
// 消除 8 处重复的 `INSERT INTO experiment_run(...)` 样板。所有调用点的
// 表结构一致(experiment_run_id/experiment_type/related_entity_type/
// related_entity_id/config/metrics/status/started_at/completed_at)，
// 差异仅在于 related_entity_id 是否自引用、status 是否为 "running"(此时
// completed_at 留 NULL，其余状态写 now())。

/// 创建一条 `experiment_run` 记录，返回生成的 `experiment_run_id`。
///
/// - `related_entity_id`：为 `None` 时自引用（写入刚生成的 `experiment_run_id`），
///   对应原 wfa_engine.rs 两处 OOS walk-forward 用自身 ID 占位的场景。
/// - `status`：`"running"` 时 `completed_at` 留 NULL；其余状态（`completed`/
///   `partial`/`failed`）写入 `now()`。`started_at` 始终写 `now()`。
pub(crate) async fn create_experiment_run(
    db: &sqlx::PgPool,
    experiment_type: &str,
    related_entity_type: &str,
    related_entity_id: Option<&str>,
    config: &Value,
    metrics: &Value,
    status: &str,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let related_entity_id = related_entity_id.unwrap_or(&experiment_run_id);

    if status == "running" {
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
        )
        .bind(&experiment_run_id)
        .bind(experiment_type)
        .bind(related_entity_type)
        .bind(related_entity_id)
        .bind(config)
        .bind(metrics)
        .bind(status)
        .execute(db)
        .await
        .map_err(|error| format!("Failed to insert experiment_run ({experiment_type}): {error}"))?;
    } else {
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at, completed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())",
        )
        .bind(&experiment_run_id)
        .bind(experiment_type)
        .bind(related_entity_type)
        .bind(related_entity_id)
        .bind(config)
        .bind(metrics)
        .bind(status)
        .execute(db)
        .await
        .map_err(|error| format!("Failed to insert experiment_run ({experiment_type}): {error}"))?;
    }

    Ok(experiment_run_id)
}

#[cfg(test)]
mod experiment_run_tests {
    use super::create_experiment_run;
    use serde_json::json;
    use sqlx::PgPool;

    /// 验证 `create_experiment_run` 能向真实 DB 写入记录,且字段语义正确:
    /// - related_entity_id=None 时自引用 experiment_run_id
    /// - status="running" 时 completed_at 为 NULL
    /// - status="completed" 时 completed_at 非 NULL
    ///
    /// 需 DB,默认不跑(--ignored 触发)。运行:
    ///   cargo test -p quant-api --lib experiment_run_tests -- --ignored
    #[tokio::test]
    #[ignore]
    async fn create_experiment_run_persists_running_and_completed() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("DB 连接成功");

        // running 状态:related_entity_id 自引用,completed_at 应为 NULL
        let running_id = create_experiment_run(
            &db,
            "test_create_experiment_run",
            "test_entity",
            None,
            &json!({"scenario": "running"}),
            &json!({"step": 0}),
            "running",
        )
        .await
        .expect("写入 running 记录");

        let (status, related, completed): (String, String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, related_entity_id, completed_at FROM experiment_run \
                 WHERE experiment_run_id = $1",
            )
            .bind(&running_id)
            .fetch_one(&db)
            .await
            .expect("回读 running 记录");
        assert_eq!(status, "running");
        assert_eq!(related, running_id, "related_entity_id 应自引用");
        assert!(completed.is_none(), "running 状态 completed_at 必须为 NULL");

        // completed 状态:显式 related_entity_id,completed_at 应非 NULL
        let completed_id = create_experiment_run(
            &db,
            "test_create_experiment_run",
            "test_entity",
            Some("rel-123"),
            &json!({"scenario": "completed"}),
            &json!({"step": 1}),
            "completed",
        )
        .await
        .expect("写入 completed 记录");

        let (status, related, completed): (String, String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, related_entity_id, completed_at FROM experiment_run \
                 WHERE experiment_run_id = $1",
            )
            .bind(&completed_id)
            .fetch_one(&db)
            .await
            .expect("回读 completed 记录");
        assert_eq!(status, "completed");
        assert_eq!(related, "rel-123");
        assert!(completed.is_some(), "completed 状态 completed_at 必须非 NULL");

        // 清理测试数据(避免污染)
        let _ = sqlx::query(
            "DELETE FROM experiment_run WHERE experiment_type = 'test_create_experiment_run'",
        )
        .execute(&db)
        .await;
    }
}


