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
pub(crate) struct OosDiscoveryWindow {
    pub(crate) window_index: usize,
    pub(crate) validation_mode: String,
    pub(crate) train_start: NaiveDate,
    pub(crate) train_end: NaiveDate,
    pub(crate) test_start: NaiveDate,
    pub(crate) test_end: NaiveDate,
}


#[derive(Debug, Clone)]
pub(crate) struct OosDiscoveryPlan {
    pub(crate) validation_mode: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) train_window_days: i64,
    pub(crate) test_window_days: i64,
    pub(crate) step_days: i64,
    pub(crate) in_sample_ratio: f64,
    pub(crate) include_partial_last_window: bool,
    pub(crate) windows: Vec<OosDiscoveryWindow>,
}


pub(crate) struct OosWindowExecution {
    pub(crate) window: OosDiscoveryWindow,
    pub(crate) train_optimization_task_id: String,
    pub(crate) train_execution_policy: OosTrainExecutionPolicy,
    pub(crate) train_batches: Vec<Value>,
    pub(crate) selected_candidate: DiscoveryCandidate,
    pub(crate) train_robustness: Option<Value>,
    pub(crate) train_cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
    pub(crate) oos_backtest_task_id: String,
    pub(crate) oos_output: FactorBacktestRunOutput,
    pub(crate) oos_points: Vec<RobustnessDailyPoint>,
    pub(crate) cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
    /// When set, this window was skipped (no training candidate passed robustness gates).
    /// The OOS fields (oos_backtest_task_id, oos_output, oos_points, etc.) are empty defaults.
    pub(crate) skip_reason: Option<String>,
}


#[derive(Debug, Clone)]
pub(crate) struct TrainWindowMlPredictionSets {
    pub(crate) train_prediction_set_id: String,
    pub(crate) test_prediction_set_id: String,
    pub(crate) training_task_id: String,
}


pub(crate) struct OosCostCapacityPerturbationResult {
    name: String,
    perturbation: OosCostCapacityPerturbationRequest,
    backtest_task_id: String,
    output: FactorBacktestRunOutput,
    passed: bool,
}


pub(crate) struct OosTrainCandidateEvaluation {
    pub(crate) candidate: DiscoveryCandidate,
    pub(crate) robustness: Value,
    pub(crate) train_cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
    pub(crate) stress_summary: CostCapacityPerturbationSummary,
    pub(crate) stress_adjusted_score: Decimal,
    pub(crate) train_cost_gate_passed: bool,
}


#[derive(Debug, Clone)]
pub(crate) struct CostCapacityPerturbationSummary {
    pub(crate) passed_count: usize,
    pub(crate) total_count: usize,
    pub(crate) pass_ratio_ppm: i64,
    pub(crate) min_calmar: Decimal,
    pub(crate) avg_calmar: Decimal,
    pub(crate) max_drawdown: Decimal,
    pub(crate) min_annual_return: Decimal,
    pub(crate) avg_sharpe: Decimal,
    pub(crate) min_sortino: Decimal,
    pub(crate) min_num_trades: u64,
    pub(crate) max_final_cash_weight: Decimal,
    pub(crate) avg_final_cash_weight: Decimal,
    pub(crate) min_final_actual_gross_exposure: Decimal,
    pub(crate) max_final_unfilled_target_gap: Decimal,
    pub(crate) avg_final_unfilled_target_gap: Decimal,
    pub(crate) min_final_execution_fill_ratio: Decimal,
    pub(crate) avg_final_execution_fill_ratio: Decimal,
    pub(crate) max_execution_target_gap: Decimal,
    pub(crate) max_execution_schedule_expired_count: usize,
    pub(crate) total_execution_schedule_expired_count: usize,
}


#[derive(Debug, Clone)]
pub(crate) struct PredictionConfidenceStressFillQualityScoreBreakdown {
    total_score: Decimal,
    base_score: Decimal,
    stress_fill_objective_score: Decimal,
    prediction_confidence_score: Decimal,
    target_annual_return: Decimal,
    min_annual_return: Decimal,
    min_calmar: Decimal,
    min_sharpe: Decimal,
    min_sortino: Decimal,
    pass_ratio_bonus: Decimal,
    annual_quality_bonus: Decimal,
    calmar_quality_bonus: Decimal,
    sharpe_quality_bonus: Decimal,
    sortino_quality_bonus: Decimal,
    drawdown_tail_penalty: Decimal,
    min_annual_shortfall: Decimal,
    target_annual_shortfall: Decimal,
    calmar_shortfall: Decimal,
    sharpe_shortfall: Decimal,
    sortino_shortfall: Decimal,
}


#[derive(Debug, Clone, Copy)]
pub(crate) struct OosCostCapacityGateConfig {
    pub(crate) enabled: bool,
    pub(crate) min_pass_ratio: f64,
    pub(crate) min_perturbed_calmar: f64,
    pub(crate) max_perturbed_drawdown_pct: f64,
}


pub(crate) struct TrialExecutionOutcome {
    pub(crate) completed: i64,
    pub(crate) failed: i64,
    pub(crate) signal_cache_stats: SignalDataCacheStats,
    pub(crate) backtest_cache_stats: BacktestDataCacheStats,
    pub(crate) signal_cache_snapshot: Option<SignalDataCacheSnapshot>,
    pub(crate) backtest_cache_snapshot: Option<BacktestDataCacheSnapshot>,
}


#[cfg(test)]
pub(crate) fn build_phase7_layered_plan_bundle(
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
) -> Phase7LayeredPlanBundle {
    build_phase7_layered_plan_bundle_with_trial_cap(req, resource_plan, 500)
}


#[cfg(test)]
fn build_phase7_layered_plan_bundle_with_trial_cap(
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
) -> Phase7LayeredPlanBundle {
    build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
        req,
        resource_plan,
        max_trials_cap,
        None,
        None,
    )
}


pub(crate) fn build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
    req: &Phase7LayeredOptimizationRequest,
    mut resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
    internal_train_window_ml_prediction_set_id: Option<&str>,
    canonical_search_profile: Option<&str>,
) -> Phase7LayeredPlanBundle {
    if let Some(max_trials) = req.max_trials {
        resource_plan.max_trials = max_trials.clamp(1, max_trials_cap.max(1));
    }

    // R8 批次3b: 优先用调用方 async 解析的 DB 规范 profile 名，None 时 fallback 到 req 原值。
    let effective_profile = canonical_search_profile.or(req.search_profile.as_deref());
    let (search_profile, mut config) = phase7_search_config(effective_profile);
    if profile_accepts_prediction_set_override(&search_profile) {
        if let Some(prediction_set_ids) = req.prediction_set_ids.as_ref() {
            config.prediction_set_ids = normalize_prediction_set_ids(prediction_set_ids);
            apply_prediction_set_override_to_seed_trials(
                &mut config.seed_trials,
                &config.prediction_set_ids,
            );
        }
    }
    if let Some(prediction_set_id) = internal_train_window_ml_prediction_set_id {
        if search_profile == "professional_train_window_ml_stress_fill_discovery"
            || search_profile == "professional_ensemble_discovery"
            || search_profile == "professional_simple_nlqr_discovery"
            || search_profile == "professional_v19_train_window_ml_alpha_rebuild"
            || search_profile == "professional_v19_train_window_ml_simple_excess_rebuild"
            || search_profile == "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
            || search_profile == "professional_v19_train_window_ml_h120_low_impact_rebuild"
            || search_profile
                == "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            || search_profile == "professional_v19_train_window_ml_event_sentiment_rebuild"
        {
            apply_internal_train_window_ml_prediction_set_to_seed_trials(
                &mut config.seed_trials,
                prediction_set_id,
            );
        }
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


pub(crate) async fn insert_phase7_layered_optimization(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    insert_phase7_layered_optimization_with_trial_cap(db, req, resource_plan, 500).await
}


pub(crate) async fn insert_phase7_layered_optimization_with_trial_cap(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
        db,
        req,
        resource_plan,
        max_trials_cap,
        None,
    )
    .await
}


pub(crate) async fn insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
    internal_train_window_ml_prediction_set_id: Option<&str>,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    // R8 批次3b: 先解析 DB 规范 profile 名（async），传入同步 build 函数。
    let canonical_profile = resolve_search_profile_name(db, req.search_profile.as_deref()).await;
    let bundle =
        build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
            req,
            resource_plan,
            max_trials_cap,
            internal_train_window_ml_prediction_set_id,
            Some(&canonical_profile),
        );
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


pub async fn run_phase7_oos_walk_forward_discovery(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OosWalkForwardDiscoveryRequest>,
) -> impl IntoResponse {
    let result = if req.plan_only.unwrap_or(false) {
        match normalize_oos_execution_mode(req.execution_mode.as_deref()) {
            Ok(_) => execute_phase7_oos_walk_forward_discovery(&state.db, req, None).await,
            Err(message) => Err(message),
        }
    } else {
        match ensure_phase7_oos_request_references_exist(&state.db, &req).await {
            Ok(()) => match normalize_oos_execution_mode(req.execution_mode.as_deref()) {
                Ok("background") => {
                    start_phase7_oos_walk_forward_discovery_background(&state.db, req).await
                }
                Ok(_) => execute_phase7_oos_walk_forward_discovery(&state.db, req, None).await,
                Err(message) => Err(message),
            },
            Err(message) => Err(message),
        }
    };

    match result {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn plan_phase7_oos_profile_comparison(
    Json(req): Json<Phase7OosProfileComparisonPlanRequest>,
) -> impl IntoResponse {
    match build_phase7_oos_profile_comparison_plan(
        &req.base,
        req.profiles,
        req.return_risk_cache_comparison,
    ) {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub async fn launch_phase7_oos_profile_comparison_smoke(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OosProfileComparisonPlanRequest>,
) -> impl IntoResponse {
    match start_phase7_oos_profile_comparison_smoke(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

/// POST /api/v1/quant/optimizations/cleanup-stale
///
/// 清理 heartbeat/created_at 超时的 optimization_task 元数据。
/// 默认 dry_run=true；实际清理必须显式传 dry_run=false。

pub(crate) fn normalize_oos_execution_mode(mode: Option<&str>) -> Result<&'static str, String> {
    match mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("inline")
    {
        "inline" | "sync" | "synchronous" => Ok("inline"),
        "background" | "async" | "asynchronous" => Ok("background"),
        other => Err(format!("unsupported execution_mode: {}", other)),
    }
}


pub(crate) fn normalized_trial_concurrency(value: Option<usize>) -> usize {
    value
        .unwrap_or_else(|| LocalResourcePlan::local_mac().batch_size.min(4).max(1))
        .clamp(1, 16)
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OosTrainExecutionPolicy {
    pub(crate) requested_cache_mode: &'static str,
    pub(crate) cache_mode: &'static str,
    pub(crate) requested_trial_concurrency: usize,
    pub(crate) trial_concurrency: usize,
}


pub(crate) fn normalize_oos_train_cache_mode(mode: Option<&str>) -> Result<&'static str, String> {
    match mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("shared_window")
    {
        "shared_window" | "shared" | "window_shared" | "reuse" => Ok("shared_window"),
        "shared_run" | "run_shared" => Ok("shared_run"),
        "per_trial_isolated" | "isolated" | "parallel_isolated" => Ok("per_trial_isolated"),
        "auto" | "adaptive" | "resource_adaptive" => Ok("auto"),
        other => Err(format!("unsupported train_cache_mode: {}", other)),
    }
}


pub(crate) fn resolve_oos_train_execution_policy(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    resource_plan: &LocalResourcePlan,
) -> Result<OosTrainExecutionPolicy, String> {
    let requested_cache_mode = normalize_oos_train_cache_mode(req.train_cache_mode.as_deref())?;
    let requested_trial_concurrency = normalized_trial_concurrency(req.trial_concurrency);
    let (cache_mode, trial_concurrency) = match requested_cache_mode {
        "shared_window" => ("shared_window", 1),
        "shared_run" => ("shared_run", 1),
        "per_trial_isolated" => ("per_trial_isolated", requested_trial_concurrency),
        "auto" => {
            let planned_trials = req
                .max_trials_per_window
                .unwrap_or(resource_plan.max_trials);
            let enough_work = planned_trials
                >= resource_plan
                    .batch_size
                    .saturating_mul(4)
                    .max(requested_trial_concurrency.saturating_mul(2));
            let enough_resources =
                resource_plan.max_parallel_trials >= 2 && resource_plan.memory_budget_gb >= 8;
            if enough_work && enough_resources {
                (
                    "per_trial_isolated",
                    requested_trial_concurrency.min(resource_plan.max_parallel_trials),
                )
            } else {
                ("shared_window", 1)
            }
        }
        other => return Err(format!("unsupported train_cache_mode: {}", other)),
    };
    Ok(OosTrainExecutionPolicy {
        requested_cache_mode,
        cache_mode,
        requested_trial_concurrency,
        trial_concurrency,
    })
}


pub(crate) async fn start_phase7_oos_walk_forward_discovery_background(
    db: &sqlx::PgPool,
    req: Phase7OosWalkForwardDiscoveryRequest,
) -> Result<Value, String> {
    let plan = build_oos_discovery_plan(&req)?;
    let plan_json = oos_discovery_plan_json(&plan);
    let experiment_run_id =
        create_running_oos_walk_forward_experiment(db, &req, &plan_json).await?;
    let db = db.clone();
    let background_req = req.clone();
    let background_experiment_run_id = experiment_run_id.clone();
    tokio::spawn(async move {
        if let Err(message) = execute_phase7_oos_walk_forward_discovery(
            &db,
            background_req,
            Some(background_experiment_run_id.clone()),
        )
        .await
        {
            error!(
                experiment_run_id = %background_experiment_run_id,
                error = %message,
                "Phase 7 OOS walk-forward discovery failed"
            );
            let _ = mark_oos_walk_forward_experiment_failed(
                &db,
                &background_experiment_run_id,
                &message,
            )
            .await;
        }
    });

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "search_method": "phase7_oos_walk_forward_discovery",
        "execution_mode": "background",
        "status": "running",
        "plan": plan_json,
        "resource_plan": LocalResourcePlan::local_mac(),
        "poll_url": format!("/api/v1/quant/experiments/{}", experiment_run_id),
    }))
}


pub(crate) async fn start_phase7_oos_profile_comparison_smoke(
    db: &sqlx::PgPool,
    req: Phase7OosProfileComparisonPlanRequest,
) -> Result<Value, String> {
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(req.return_risk_cache_comparison);
    let launch_requests = build_phase7_oos_profile_comparison_launch_requests(
        &req.base,
        req.profiles,
        req.return_risk_cache_comparison,
    )?;
    let mut launches = Vec::new();
    for launch_req in launch_requests {
        let search_profile = launch_req
            .search_profile
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let return_risk_feature_cache_mode = request_return_risk_feature_cache_mode(&launch_req)
            .unwrap_or_else(|| "inherited".into());
        let request = oos_request_plan_json(&launch_req);
        let launched = start_phase7_oos_walk_forward_discovery_background(db, launch_req).await?;
        launches.push(json!({
            "search_profile": search_profile,
            "return_risk_feature_cache_mode": return_risk_feature_cache_mode,
            "experiment_run_id": launched["experiment_run_id"],
            "status": launched["status"],
            "poll_url": launched["poll_url"],
            "request": request,
            "plan": launched["plan"],
        }));
    }
    let profile_count = launches
        .iter()
        .filter_map(|launch| launch.get("search_profile").and_then(Value::as_str))
        .collect::<BTreeSet<_>>()
        .len();
    let cache_economics_report_plan =
        cache_economics_report_launch_plan(return_risk_cache_comparison, &launches);

    Ok(json!({
        "search_method": "phase7_oos_profile_comparison_smoke",
        "execution_mode": "background",
        "profile_count": profile_count,
        "launch_count": launches.len(),
        "return_risk_cache_comparison": cache_economics_report_plan,
        "launches": launches,
    }))
}


pub(crate) async fn execute_phase7_oos_walk_forward_discovery(
    db: &sqlx::PgPool,
    req: Phase7OosWalkForwardDiscoveryRequest,
    experiment_run_id: Option<String>,
) -> Result<Value, String> {
    let plan = build_oos_discovery_plan(&req)?;
    let plan_json = oos_discovery_plan_json(&plan);
    if req.plan_only.unwrap_or(false) {
        return Ok(json!({
            "search_method": "phase7_oos_walk_forward_discovery",
            "plan_only": true,
            "plan": plan_json,
            "resource_plan": LocalResourcePlan::local_mac(),
        }));
    }

    let mut window_results = Vec::new();
    let mut stitched_points = Vec::new();
    let mut oos_signal_cache = SignalDataCache::default();
    let mut oos_backtest_cache = BacktestDataCache::default();
    let train_execution_policy =
        resolve_oos_train_execution_policy(&req, &LocalResourcePlan::local_mac())?;
    let shared_run_train_cache = train_execution_policy.cache_mode == "shared_run";
    let mut shared_train_signal_cache = if shared_run_train_cache {
        Some(SignalDataCache::default())
    } else {
        None
    };
    let mut shared_train_backtest_cache = if shared_run_train_cache {
        Some(BacktestDataCache::default())
    } else {
        None
    };
    let train_gate_policy = resolve_oos_train_selection_gate_policy(&req);
    let final_promotion_gate_policy =
        resolve_oos_final_promotion_gate_policy(&req, &plan.validation_mode);
    let oos_top_n = req.oos_top_n.unwrap_or(1).clamp(1, 20);

    for window in &plan.windows {
        let mut local_train_signal_cache = SignalDataCache::default();
        let mut local_train_backtest_cache = BacktestDataCache::default();
        let (train_signal_cache, train_backtest_cache) = if shared_run_train_cache {
            (
                shared_train_signal_cache
                    .as_mut()
                    .expect("shared_run train signal cache"),
                shared_train_backtest_cache
                    .as_mut()
                    .expect("shared_run train backtest cache"),
            )
        } else {
            (
                &mut local_train_signal_cache,
                &mut local_train_backtest_cache,
            )
        };
        let execution = execute_oos_discovery_window(
            db,
            &req,
            experiment_run_id.as_deref(),
            &train_execution_policy,
            window,
            oos_top_n,
            &train_gate_policy,
            &final_promotion_gate_policy,
            train_signal_cache,
            train_backtest_cache,
            &mut oos_signal_cache,
            &mut oos_backtest_cache,
        )
        .await?;
        if execution.skip_reason.is_none() {
            append_stitched_oos_points(&mut stitched_points, &execution.oos_points);
        }
        window_results.push(oos_window_execution_json(&execution, &train_gate_policy));
        if let Some(experiment_run_id) = experiment_run_id.as_deref() {
            let stitched_summary = if stitched_points.len() >= 2 {
                metric_summary_json(&summarize_points(&stitched_points))
            } else {
                json!({})
            };
            update_oos_walk_forward_experiment_progress(
                db,
                experiment_run_id,
                &plan,
                &window_results,
                &stitched_summary,
                &oos_cache_report(&oos_signal_cache, &oos_backtest_cache),
            )
            .await?;
        }
    }

    let stitched_summary = if stitched_points.len() >= 2 {
        metric_summary_json(&summarize_points(&stitched_points))
    } else {
        json!({})
    };
    let gate_report = build_oos_gate_report(
        &req,
        &plan,
        &window_results,
        &stitched_summary,
        &final_promotion_gate_policy,
    );
    let cache_report = oos_cache_report(&oos_signal_cache, &oos_backtest_cache);
    let experiment_run_id = match experiment_run_id {
        Some(existing_experiment_run_id) => {
            complete_oos_walk_forward_experiment(
                db,
                &existing_experiment_run_id,
                &req,
                &plan_json,
                &window_results,
                &stitched_summary,
                &gate_report,
                &cache_report,
            )
            .await?;
            existing_experiment_run_id
        }
        None => {
            persist_oos_walk_forward_experiment(
                db,
                &req,
                &plan_json,
                &window_results,
                &stitched_summary,
                &gate_report,
                &cache_report,
            )
            .await?
        }
    };

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "search_method": "phase7_oos_walk_forward_discovery",
        "execution_mode": normalize_oos_execution_mode(req.execution_mode.as_deref()).unwrap_or("inline"),
        "plan": plan_json,
        "window_count": plan.windows.len(),
        "windows": window_results,
        "stitched_oos": {
            "point_count": stitched_points.len(),
            "metrics": stitched_summary,
            "gates": gate_report,
            "status": oos_gate_status(&gate_report),
        },
        "cache": cache_report
    }))
}


pub(crate) fn should_use_fixed_params_oos_mode(
    fixed_params_enabled: bool,
    has_train_window_ml_prediction_sets: bool,
    search_profile: Option<&str>,
) -> bool {
    if !fixed_params_enabled || has_train_window_ml_prediction_sets {
        return false;
    }
    let (_profile_name, search_config) = phase7_search_config(search_profile);
    search_config.seed_trials.len() == 1
}

/// R8 批次3b: should_use_fixed_params_oos_mode 的 DB-aware 版本。
/// 调用方（async 上下文）先用 resolve_search_profile_name 解析规范名，
/// 再传入此函数，避免同步函数内 await。
pub(crate) fn should_use_fixed_params_oos_mode_with_profile(
    fixed_params_enabled: bool,
    has_train_window_ml_prediction_sets: bool,
    canonical_profile: &str,
) -> bool {
    if !fixed_params_enabled || has_train_window_ml_prediction_sets {
        return false;
    }
    let (_profile_name, search_config) = phase7_search_config(Some(canonical_profile));
    search_config.seed_trials.len() == 1
}


pub(crate) async fn execute_oos_discovery_window(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    experiment_run_id: Option<&str>,
    train_execution_policy: &OosTrainExecutionPolicy,
    window: &OosDiscoveryWindow,
    oos_top_n: usize,
    train_gate_policy: &Value,
    final_promotion_gate_policy: &Value,
    train_signal_cache: &mut SignalDataCache,
    train_backtest_cache: &mut BacktestDataCache,
    oos_signal_cache: &mut SignalDataCache,
    oos_backtest_cache: &mut BacktestDataCache,
) -> Result<OosWindowExecution, String> {
    let train_template = backtest_template_for_window(
        req.backtest_template
            .clone()
            .unwrap_or_else(default_phase7_backtest_template),
        window.train_start,
        window.train_end,
        "summary_only",
    )?;
    let train_template_for_selection = train_template.clone();
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "window_started",
            Some(window.window_index),
            json!({
                "train_start": window.train_start.format("%Y-%m-%d").to_string(),
                "train_end": window.train_end.format("%Y-%m-%d").to_string(),
                "test_start": window.test_start.format("%Y-%m-%d").to_string(),
                "test_end": window.test_end.format("%Y-%m-%d").to_string(),
            }),
        ),
    )
    .await?;
    let train_window_ml_prediction_sets =
        prepare_train_window_ml_prediction_sets_for_oos_window(db, req, window, experiment_run_id)
            .await?;

    // Fixed-params WFA mode: skip grid search, use seed parameters directly for OOS.
    // Controlled by env var QUANT_WFA_FIXED_PARAMS=true.
    // Eliminates per-window training overfitting at the cost of not adapting
    // parameters to each window's specific regime.
    let fixed_params_enabled = std::env::var("QUANT_WFA_FIXED_PARAMS")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    // R8 批次3b: 先解析 DB 规范 profile 名（async），后续同步调用复用。
    let canonical_profile = resolve_search_profile_name(db, req.search_profile.as_deref()).await;

    if should_use_fixed_params_oos_mode_with_profile(
        fixed_params_enabled,
        train_window_ml_prediction_sets.is_some(),
        &canonical_profile,
    ) {
        // Resolve the search config to get the first seed trial's parameters
        let (_profile_name, search_config) = phase7_search_config(Some(&canonical_profile));
        let seed_params = search_config
            .seed_trials
            .first()
            .cloned()
            .expect("fixed params mode requires exactly one seed trial");

        let test_template = backtest_template_for_window(
            req.backtest_template
                .clone()
                .unwrap_or_else(default_phase7_backtest_template),
            window.test_start,
            window.test_end,
            "summary_only",
        )?;
        let oos_backtest_task_id = format!("oosbt-{}", Uuid::new_v4());
        let oos_parameters =
            train_window_ml_oos_parameters(&seed_params, train_window_ml_prediction_sets.as_ref())?;
        let oos_output = execute_oos_candidate_backtest(
            db,
            req,
            test_template.clone(),
            &oos_parameters,
            &oos_backtest_task_id,
            oos_signal_cache,
            oos_backtest_cache,
        )
        .await?;
        let oos_points = load_oos_equity_points(db, &oos_backtest_task_id).await?;

        return Ok(OosWindowExecution {
            window: window.clone(),
            train_optimization_task_id: format!("fixed-{}", Uuid::new_v4().simple()),
            train_execution_policy: train_execution_policy.clone(),
            train_batches: vec![json!({"mode": "fixed_params", "enabled": true})],
            selected_candidate: DiscoveryCandidate {
                trial_id: "fixed-params".to_string(),
                backtest_task_id: None,
                score: None,
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics::default(),
                parameters: seed_params,
            },
            train_robustness: None,
            train_cost_capacity_perturbations: Vec::new(),
            oos_backtest_task_id,
            oos_output,
            oos_points,
            cost_capacity_perturbations: Vec::new(),
            skip_reason: None,
        });
    }

    let train_req = Phase7ProfessionalDiscoveryRequest {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        objective: req.objective.clone(),
        constraints: req.constraints.clone(),
        walk_forward: req.walk_forward.clone(),
        backtest_template: Some(train_template),
        prediction_set_ids: req.prediction_set_ids.clone(),
        max_trials: req
            .max_trials_per_window
            .or_else(|| req.exhaustive_search.unwrap_or(false).then_some(100_000)),
        search_profile: req.search_profile.clone(),
        trial_batch_limit: req.trial_batch_limit,
        max_batches: None,
        robustness_top_n: Some(oos_top_n),
        robustness_gate_policy: Some(train_gate_policy.clone()),
        stop_after_professional_candidate: Some(false),
        stop_after_robust_approval: Some(false),
    };
    let layered_req = phase7_discovery_layered_request(&train_req);
    let (train_task_id, bundle) =
        insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
            db,
            &layered_req,
            LocalResourcePlan::local_mac(),
            100_000,
            train_window_ml_prediction_sets
                .as_ref()
                .map(|sets| sets.train_prediction_set_id.as_str()),
        )
        .await?;
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_optimization_task_created",
            Some(window.window_index),
            json!({
                "optimization_task_id": train_task_id,
                "planned_trials": bundle.plan.planned_trials,
                "requested_trials": bundle.plan.requested_trials,
            }),
        ),
    )
    .await?;
    let trial_batch_limit = req
        .trial_batch_limit
        .unwrap_or(bundle.plan.batch_size as i64)
        .clamp(1, 2_000);
    let max_batches = req.max_batches_per_window.unwrap_or_else(|| {
        let planned = bundle.plan.planned_trials.max(1);
        let batch = trial_batch_limit.max(1) as usize;
        planned.div_ceil(batch)
    });
    let max_batches = max_batches.clamp(1, 100_000);
    let mut train_batches = Vec::new();
    for batch_index in 0..max_batches {
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_batch_started",
                Some(window.window_index),
                json!({
                    "optimization_task_id": train_task_id,
                    "batch_index": batch_index + 1,
                    "max_batches": max_batches,
                    "trial_batch_limit": trial_batch_limit,
                    "train_cache_mode": train_execution_policy.cache_mode,
                }),
            ),
        )
        .await?;
        let batch = execute_pending_trials_with_caches_and_concurrency(
            db,
            &train_task_id,
            trial_batch_limit,
            None,
            &mut *train_signal_cache,
            &mut *train_backtest_cache,
            train_execution_policy.trial_concurrency,
        )
        .await?;
        let executed = batch["executed"].as_i64().unwrap_or(0);
        let annotated_batch = annotate_oos_train_batch_cache_mode(batch, train_execution_policy);
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_batch_completed",
                Some(window.window_index),
                json!({
                    "optimization_task_id": train_task_id,
                    "batch_index": batch_index + 1,
                    "executed": annotated_batch["executed"],
                    "completed": annotated_batch["completed"],
                    "failed": annotated_batch["failed"],
                    "performance_gate_status": annotated_batch["performance_gate_status"],
                }),
            ),
        )
        .await?;
        train_batches.push(annotated_batch);
        if executed == 0 {
            break;
        }
    }

    let training_candidates = load_oos_training_candidates(db, &train_task_id, oos_top_n).await?;
    let require_train_approval = req.require_train_robustness_approval.unwrap_or(true);
    let (selected_candidate, train_robustness, train_cost_capacity_perturbations) =
        match select_oos_training_candidate(
            db,
            req,
            &train_template_for_selection,
            &train_task_id,
            training_candidates,
            train_gate_policy,
            require_train_approval,
            window.window_index,
            &mut *train_signal_cache,
            &mut *train_backtest_cache,
        )
        .await
        {
            Ok(result) => result,
            Err(msg)
                if msg.contains("no training candidate passing robustness")
                    || msg.contains("no completed training candidates") =>
            {
                // Window skipped — no candidate passed robustness gates.
                // WFA methodology: individual window failures are expected and
                // should NOT halt the entire experiment. Continue to next window.
                return Ok(OosWindowExecution {
                    window: window.clone(),
                    train_optimization_task_id: train_task_id,
                    train_execution_policy: train_execution_policy.clone(),
                    train_batches,
                    selected_candidate: DiscoveryCandidate {
                        trial_id: String::new(),
                        backtest_task_id: None,
                        score: None,
                        candidate_type: CandidateType::ReviewRequired,
                        professional_gap_score: Decimal::ZERO,
                        metrics: CandidateMetrics::default(),
                        parameters: json!({}),
                    },
                    train_robustness: None,
                    train_cost_capacity_perturbations: Vec::new(),
                    oos_backtest_task_id: String::new(),
                    oos_output: FactorBacktestRunOutput {
                        signals_count: 0,
                        metrics: quant_backtest::metrics::BacktestMetrics::default(),
                        trades: 0,
                        equity_points: 0,
                        effective_coverage: None,
                        market_data_prewarm_report: None,
                        market_feature_prewarm_report: None,
                    },
                    oos_points: Vec::new(),
                    cost_capacity_perturbations: Vec::new(),
                    skip_reason: Some(msg),
                });
            }
            Err(msg) => return Err(msg),
        };

    let test_template = backtest_template_for_window(
        req.backtest_template
            .clone()
            .unwrap_or_else(default_phase7_backtest_template),
        window.test_start,
        window.test_end,
        "summary_only",
    )?;
    let oos_backtest_task_id = format!("oosbt-{}", Uuid::new_v4());
    let perturbation_template = test_template.clone();
    let oos_parameters = train_window_ml_oos_parameters(
        &selected_candidate.parameters,
        train_window_ml_prediction_sets.as_ref(),
    )?;
    let oos_output = execute_oos_candidate_backtest(
        db,
        req,
        test_template,
        &oos_parameters,
        &oos_backtest_task_id,
        oos_signal_cache,
        oos_backtest_cache,
    )
    .await?;
    let oos_points = load_oos_equity_points(db, &oos_backtest_task_id).await?;
    let cost_capacity_perturbations = execute_oos_cost_capacity_perturbations(
        db,
        req,
        perturbation_template,
        &oos_parameters,
        final_promotion_gate_policy,
        oos_signal_cache,
        oos_backtest_cache,
    )
    .await?;

    Ok(OosWindowExecution {
        window: window.clone(),
        train_optimization_task_id: train_task_id,
        train_execution_policy: train_execution_policy.clone(),
        train_batches,
        selected_candidate,
        train_robustness,
        train_cost_capacity_perturbations,
        oos_backtest_task_id,
        oos_output,
        oos_points,
        cost_capacity_perturbations,
        skip_reason: None,
    })
}

/// PIT routing for ensemble: selects which NLQR model to train based on
/// signals available at the window's test_start date (no future data).

pub(crate) async fn ensemble_model_params_for_window(
    db: &sqlx::PgPool,
    window: &OosDiscoveryWindow,
) -> Result<(String, usize, usize, usize), String> {
    // Load benchmark (CSI300) returns up to test_start
    let benchmark_start = window.test_start - Duration::days(365);
    let bench_rows = sqlx::query_as::<_, (NaiveDate, Option<f64>)>(
        "SELECT trade_date, pct_change::double precision
         FROM market_index_daily_bar
         WHERE symbol = '000300.SH'
           AND trade_date >= $1 AND trade_date < $2
         ORDER BY trade_date",
    )
    .bind(benchmark_start)
    .bind(window.test_start)
    .fetch_all(db)
    .await
    .map_err(|e| format!("benchmark query failed: {}", e))?;

    let bench_returns: Vec<f64> = bench_rows.iter().filter_map(|(_, r)| *r).collect();

    if bench_returns.len() < 42 {
        // Not enough data — default to EW3 (asymmetric bull)
        return Ok(("asymmetric_excess_return".to_string(), 60, 10, 100));
    }

    // Compute 42-day trailing return
    let tr_42d = bench_returns
        .iter()
        .rev()
        .take(42)
        .fold(1.0, |acc, &r| acc * (1.0 + r))
        - 1.0;

    // Compute previous year volatility
    let prev_year_rets: Vec<f64> = bench_returns.iter().rev().take(252).copied().collect();
    let prev_vol = if prev_year_rets.len() >= 100 {
        let mean = prev_year_rets.iter().sum::<f64>() / prev_year_rets.len() as f64;
        let var = prev_year_rets
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (prev_year_rets.len() - 1) as f64;
        var.sqrt() * (252.0_f64).sqrt()
    } else {
        0.15 // default moderate vol
    };

    // Load north_flow zscore at test_start
    let nf_row = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT normalized_value::double precision
         FROM factor_value
         WHERE factor_code = 'north_flow_std_20d'
           AND factor_version = '1.0.0'
           AND symbol = (SELECT symbol FROM market_stock_daily_bar WHERE trade_date >= $1 LIMIT 1)
           AND trade_date = (
               SELECT MAX(trade_date) FROM factor_value
               WHERE factor_code = 'north_flow_std_20d'
                 AND factor_version = '1.0.0'
                 AND trade_date < $1
           )",
    )
    .bind(window.test_start)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("north_flow query failed: {}", e))?
    .and_then(|(v,)| v)
    .unwrap_or(0.0);

    // ─── PIT Routing ───
    if prev_vol > 0.20 {
        // High volatility → Mean-Reversion model
        Ok(("future_return".to_string(), 5, 10, 100))
    } else if tr_42d < -0.02 {
        // Bear market → Quality defense
        Ok(("quality_adjusted_excess_return".to_string(), 60, 10, 100))
    } else if nf_row < -1.0 {
        // Systematic bear → Standard Bull (max_gross reduced at backtest level)
        Ok(("gradient_boosting_excess_return".to_string(), 60, 10, 100))
    } else if tr_42d > 0.05 && nf_row > 0.0 {
        // Strong bull + northbound inflow → Momentum (gradient boosting)
        Ok(("gradient_boosting_excess_return".to_string(), 60, 10, 100))
    } else {
        // Default: Asymmetric Bull (balanced)
        Ok(("asymmetric_excess_return".to_string(), 60, 10, 100))
    }
}


pub(crate) async fn prepare_train_window_ml_prediction_sets_for_oos_window(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    window: &OosDiscoveryWindow,
    experiment_run_id: Option<&str>,
) -> Result<Option<TrainWindowMlPredictionSets>, String> {
    if !is_train_window_ml_stress_fill_profile(req.search_profile.as_deref()) {
        return Ok(None);
    }

    let is_ensemble = is_ensemble_profile(req.search_profile.as_deref());
    let is_simple_nlqr = is_simple_nlqr_profile(req.search_profile.as_deref());
    let is_v19_alpha_rebuild =
        is_v19_train_window_ml_alpha_rebuild_profile(req.search_profile.as_deref());
    let short_id = Uuid::new_v4().simple().to_string();
    let short_id = &short_id[..12];
    let prefix = if is_ensemble {
        "p7en"
    } else if is_simple_nlqr {
        "p7sn"
    } else if is_v19_train_window_ml_event_sentiment_rebuild_profile(req.search_profile.as_deref())
    {
        "p7v19evt"
    } else if is_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(
        req.search_profile.as_deref(),
    ) {
        "p7v19rae"
    } else if is_v19_train_window_ml_h120_low_impact_rebuild_profile(req.search_profile.as_deref())
    {
        "p7v19h120"
    } else if is_v19_train_window_ml_simple_excess_rebuild_profile(req.search_profile.as_deref()) {
        "p7v19sx"
    } else if is_v19_alpha_rebuild {
        "p7v19ml"
    } else {
        "p7gb"
    };
    let train_prediction_set_id = format!("{}-w{}-{}-tr", prefix, window.window_index, short_id);
    let test_prediction_set_id = format!("{}-w{}-{}-te", prefix, window.window_index, short_id);
    let train_training_task_id =
        format!("train-{}-w{}-{}-tr", prefix, window.window_index, short_id);
    let test_training_task_id =
        format!("train-{}-w{}-{}-te", prefix, window.window_index, short_id);
    let training_task_id = format!("{}-w{}-{}", prefix, window.window_index, short_id);

    // Label/horizon/bucket selection
    let (label_objective, label_horizon_days, bucket_count, min_samples) = if is_ensemble {
        ensemble_model_params_for_window(db, window).await?
    } else if is_simple_nlqr {
        // Simplified NLQR: stable label, fewer buckets for better generalization
        let h = train_window_ml_label_horizon_days(window) as usize;
        ("future_excess_return".to_string(), h, 5usize, 50usize)
    } else if is_v19_alpha_rebuild {
        phase7_train_window_ml_label_config_for_search(req.search_profile.as_deref())
    } else {
        let h = train_window_ml_label_horizon_days(window) as usize;
        (
            "risk_adjusted_excess_return".to_string(),
            h,
            7usize,
            250usize,
        )
    };

    let train_lookback_days = train_window_ml_lookback_days(window, label_horizon_days as i64);
    let train_prediction_start =
        window.train_start + Duration::days(train_lookback_days + label_horizon_days as i64 - 1);
    if train_prediction_start > window.train_end {
        return Err(format!(
            "{} train window {} is too short for ML ranking",
            prefix, window.window_index
        ));
    }

    let feature_profile =
        phase7_train_window_ml_feature_profile_for_search(req.search_profile.as_deref());
    let factors = if is_simple_nlqr {
        phase7_train_window_ml_factor_refs_for_profile("phase7_simple_nlqr_core_15f_v1")
    } else {
        phase7_train_window_ml_factor_refs_for_profile(feature_profile)
    };
    if factors.is_empty() {
        return Err("train-window ML ranking factors must not be empty".into());
    }
    let readiness_thresholds = ReadinessThresholds::default();
    let train_feature_readiness = build_feature_profile_readiness_report(
        db,
        feature_profile,
        &factors,
        train_prediction_start,
        window.train_end,
        readiness_thresholds,
    )
    .await?;
    if !feature_profile_readiness_passed(&train_feature_readiness) {
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_window_ml_feature_profile_readiness_failed",
                Some(window.window_index),
                json!({
                    "phase": "train",
                    "feature_profile": feature_profile,
                    "readiness": train_feature_readiness,
                }),
            ),
        )
        .await?;
        return Err(format!(
            "{} window {} train feature-profile readiness failed: {}",
            prefix,
            window.window_index,
            readiness_failure_summary(&train_feature_readiness)
        ));
    }
    let test_feature_readiness = build_feature_profile_readiness_report(
        db,
        feature_profile,
        &factors,
        window.test_start,
        window.test_end,
        readiness_thresholds,
    )
    .await?;
    if !feature_profile_readiness_passed(&test_feature_readiness) {
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_window_ml_feature_profile_readiness_failed",
                Some(window.window_index),
                json!({
                    "phase": "test",
                    "feature_profile": feature_profile,
                    "readiness": test_feature_readiness,
                }),
            ),
        )
        .await?;
        return Err(format!(
            "{} window {} test feature-profile readiness failed: {}",
            prefix,
            window.window_index,
            readiness_failure_summary(&test_feature_readiness)
        ));
    }
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_window_ml_feature_profile_readiness_ready",
            Some(window.window_index),
            json!({
                "feature_profile": feature_profile,
                "train_readiness": train_feature_readiness,
                "test_readiness": test_feature_readiness,
            }),
        ),
    )
    .await?;

    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_window_ml_train_prediction_started",
            Some(window.window_index),
            json!({
                "prediction_set_id": train_prediction_set_id,
                "training_task_id": train_training_task_id,
                "prediction_start": train_prediction_start.format("%Y-%m-%d").to_string(),
                "prediction_end": window.train_end.format("%Y-%m-%d").to_string(),
                "feature_profile": feature_profile,
                "label_objective": label_objective,
                "label_horizon_days": label_horizon_days,
                "bucket_count": bucket_count,
                "min_samples_per_bucket": min_samples,
            }),
        ),
    )
    .await?;
    create_walk_forward_nonlinear_quantile_ranker_inner(
        db,
        WalkForwardNonlinearQuantileRankerRequest {
            model_code: format!("{}_nlq_ranker", prefix),
            model_version: format!("w{}-{}-train", window.window_index, short_id),
            model_version_id: Some(format!(
                "{}-nlq-w{}-{}-tr",
                prefix, window.window_index, short_id
            )),
            training_task_id: Some(train_training_task_id.clone()),
            prediction_set_id: Some(train_prediction_set_id.clone()),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: feature_profile.to_string(),
            training_dataset_id: format!("ds-p7gb-w{}-{}-tr", window.window_index, short_id),
            prediction_start_date: train_prediction_start.format("%Y%m%d").to_string(),
            prediction_end_date: window.train_end.format("%Y%m%d").to_string(),
            train_lookback_days: Some(train_lookback_days),
            prediction_step_days: Some(20),
            label_horizon_days: Some(label_horizon_days as i64),
            label_objective: Some(label_objective.clone()),
            min_training_samples: Some(250),
            max_windows: None,
            bucket_count: Some(bucket_count),
            min_samples_per_bucket: Some(min_samples),
            factors: factors.clone(),
        },
    )
    .await?;

    let train_prediction_readiness = build_prediction_set_readiness_report(
        db,
        &train_prediction_set_id,
        Some(train_prediction_start),
        Some(window.train_end),
        readiness_thresholds,
    )
    .await?;
    if !prediction_readiness_passed(&train_prediction_readiness) {
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_window_ml_prediction_set_readiness_failed",
                Some(window.window_index),
                json!({
                    "phase": "train",
                    "prediction_set_id": train_prediction_set_id,
                    "readiness": train_prediction_readiness,
                }),
            ),
        )
        .await?;
        return Err(format!(
            "{} window {} train prediction-set readiness failed: {}",
            prefix,
            window.window_index,
            readiness_failure_summary(&train_prediction_readiness)
        ));
    }
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_window_ml_train_prediction_ready",
            Some(window.window_index),
            json!({
                "prediction_set_id": train_prediction_set_id,
                "training_task_id": train_training_task_id,
                "readiness": train_prediction_readiness,
            }),
        ),
    )
    .await?;
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_window_ml_test_prediction_started",
            Some(window.window_index),
            json!({
                "prediction_set_id": test_prediction_set_id,
                "training_task_id": test_training_task_id,
                "prediction_start": window.test_start.format("%Y-%m-%d").to_string(),
                "prediction_end": window.test_end.format("%Y-%m-%d").to_string(),
                "feature_profile": feature_profile,
                "label_horizon_days": label_horizon_days,
                "bucket_count": bucket_count,
                "min_samples_per_bucket": min_samples,
            }),
        ),
    )
    .await?;
    train_nonlinear_quantile_ranker_inner(
        db,
        TrainNonlinearQuantileRankerRequest {
            model_code: format!("{}_nlq_ranker", prefix),
            model_version: format!("w{}-{}-test", window.window_index, short_id),
            model_version_id: Some(format!(
                "{}-nlq-w{}-{}-te",
                prefix, window.window_index, short_id
            )),
            training_task_id: Some(test_training_task_id.clone()),
            prediction_set_id: Some(test_prediction_set_id.clone()),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: feature_profile.to_string(),
            training_dataset_id: format!("ds-{}-w{}-{}-te", prefix, window.window_index, short_id),
            train_start_date: window.train_start.format("%Y%m%d").to_string(),
            train_end_date: window.train_end.format("%Y%m%d").to_string(),
            prediction_start_date: window.test_start.format("%Y%m%d").to_string(),
            prediction_end_date: window.test_end.format("%Y%m%d").to_string(),
            label_horizon_days: Some(label_horizon_days as i64),
            label_objective: Some(label_objective),
            bucket_count: Some(bucket_count),
            min_samples_per_bucket: Some(min_samples),
            factors,
        },
    )
    .await?;

    let test_prediction_readiness = build_prediction_set_readiness_report(
        db,
        &test_prediction_set_id,
        Some(window.test_start),
        Some(window.test_end),
        readiness_thresholds,
    )
    .await?;
    if !prediction_readiness_passed(&test_prediction_readiness) {
        update_oos_walk_forward_experiment_stage(
            db,
            experiment_run_id,
            oos_walk_forward_stage(
                "train_window_ml_prediction_set_readiness_failed",
                Some(window.window_index),
                json!({
                    "phase": "test",
                    "prediction_set_id": test_prediction_set_id,
                    "readiness": test_prediction_readiness,
                }),
            ),
        )
        .await?;
        return Err(format!(
            "{} window {} test prediction-set readiness failed: {}",
            prefix,
            window.window_index,
            readiness_failure_summary(&test_prediction_readiness)
        ));
    }
    update_oos_walk_forward_experiment_stage(
        db,
        experiment_run_id,
        oos_walk_forward_stage(
            "train_window_ml_test_prediction_ready",
            Some(window.window_index),
            json!({
                "prediction_set_id": test_prediction_set_id,
                "training_task_id": test_training_task_id,
                "readiness": test_prediction_readiness,
            }),
        ),
    )
    .await?;

    Ok(Some(TrainWindowMlPredictionSets {
        train_prediction_set_id,
        test_prediction_set_id,
        training_task_id,
    }))
}


pub(crate) async fn execute_oos_cost_capacity_perturbations(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    test_template: Value,
    parameters: &Value,
    final_promotion_gate_policy: &Value,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<Vec<OosCostCapacityPerturbationResult>, String> {
    let perturbations = resolved_oos_cost_capacity_perturbations(req);
    let mut results = Vec::with_capacity(perturbations.len());
    for (index, perturbation) in perturbations.into_iter().enumerate() {
        let name = oos_cost_capacity_perturbation_name(&perturbation, index);
        let perturbed_parameters =
            apply_cost_capacity_perturbation_to_parameters(parameters, &perturbation)?;
        let backtest_task_id = format!("oosstress-{}", Uuid::new_v4());
        let output = execute_oos_candidate_backtest(
            db,
            req,
            test_template.clone(),
            &perturbed_parameters,
            &backtest_task_id,
            &mut *signal_cache,
            &mut *backtest_cache,
        )
        .await?;
        let passed =
            oos_cost_capacity_perturbation_passed(req, final_promotion_gate_policy, &output);
        results.push(OosCostCapacityPerturbationResult {
            name,
            perturbation,
            backtest_task_id,
            output,
            passed,
        });
    }
    Ok(results)
}


pub(crate) async fn execute_train_cost_capacity_perturbations(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_template: &Value,
    parameters: &Value,
    train_gate_policy: &Value,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<Vec<OosCostCapacityPerturbationResult>, String> {
    let gate = train_cost_capacity_perturbation_gate_config(req, train_gate_policy);
    if !gate.enabled {
        return Ok(Vec::new());
    }

    let perturbations = resolved_train_cost_capacity_perturbations(req, train_gate_policy);
    let mut results = Vec::with_capacity(perturbations.len());
    for (index, perturbation) in perturbations.into_iter().enumerate() {
        let name = oos_cost_capacity_perturbation_name(&perturbation, index);
        let perturbed_parameters =
            apply_cost_capacity_perturbation_to_parameters(parameters, &perturbation)?;
        let backtest_task_id = format!("trainstress-{}", Uuid::new_v4());
        let output = execute_oos_candidate_backtest(
            db,
            req,
            train_template.clone(),
            &perturbed_parameters,
            &backtest_task_id,
            &mut *signal_cache,
            &mut *backtest_cache,
        )
        .await?;
        let passed = cost_capacity_perturbation_passed_with_thresholds(
            &output,
            gate.min_perturbed_calmar,
            gate.max_perturbed_drawdown_pct,
        );
        results.push(OosCostCapacityPerturbationResult {
            name,
            perturbation,
            backtest_task_id,
            output,
            passed,
        });
    }
    Ok(results)
}


pub(crate) async fn execute_oos_candidate_backtest(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    test_template: Value,
    parameters: &Value,
    backtest_task_id: &str,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<FactorBacktestRunOutput, String> {
    let task = OptimizationTaskExecutionContext {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        backtest_template: test_template,
        objective: req.objective.clone().unwrap_or_else(|| {
            json!({
                "type": "professional_candidate",
                "benchmark": "000300.SH",
                "maximize": true
            })
        }),
        constraints: req.constraints.clone(),
    };
    match build_optimization_trial_request(&task, parameters)? {
        OptimizationTrialBacktestRequest::Factor(request) => {
            let output = execute_factor_backtest_with_caches(
                db,
                backtest_task_id,
                request,
                Some(signal_cache),
                Some(backtest_cache),
            )
            .await?;
            if let Some(report) = output.market_data_prewarm_report.as_ref() {
                let _ = report;
            }
            Ok(output)
        }
        OptimizationTrialBacktestRequest::Prediction(request) => {
            execute_prediction_backtest(db, backtest_task_id, request).await
        }
    }
}


pub(crate) fn cost_capacity_perturbation_gate_enabled(req: &Phase7OosWalkForwardDiscoveryRequest) -> bool {
    req.enable_cost_capacity_perturbation_gate
        .unwrap_or_else(|| {
            req.cost_capacity_perturbations
                .as_ref()
                .map(|items| !items.is_empty())
                .unwrap_or(false)
        })
}


pub(crate) fn resolved_oos_cost_capacity_perturbations(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Vec<OosCostCapacityPerturbationRequest> {
    if !cost_capacity_perturbation_gate_enabled(req) {
        return Vec::new();
    }
    req.cost_capacity_perturbations
        .clone()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(default_oos_cost_capacity_perturbations)
}


pub(crate) fn resolved_train_cost_capacity_perturbations(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_gate_policy: &Value,
) -> Vec<OosCostCapacityPerturbationRequest> {
    let gate = train_cost_capacity_perturbation_gate_config(req, train_gate_policy);
    if !gate.enabled {
        return Vec::new();
    }
    req.cost_capacity_perturbations
        .clone()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(default_oos_cost_capacity_perturbations)
}


pub(crate) fn train_cost_capacity_perturbation_gate_config(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_gate_policy: &Value,
) -> OosCostCapacityGateConfig {
    let enabled = constraint_bool(
        Some(train_gate_policy),
        "enable_train_cost_capacity_perturbation_gate",
    )
    .or_else(|| {
        constraint_bool(
            Some(train_gate_policy),
            "enable_cost_capacity_perturbation_gate",
        )
    })
    .unwrap_or(false);
    let min_pass_ratio = constraint_f64(
        Some(train_gate_policy),
        "min_train_cost_capacity_perturbation_pass_ratio",
    )
    .or_else(|| {
        constraint_f64(
            Some(train_gate_policy),
            "min_cost_capacity_perturbation_pass_ratio",
        )
    })
    .or(req.min_cost_capacity_perturbation_pass_ratio)
    .unwrap_or(0.80)
    .clamp(0.0, 1.0);
    let min_perturbed_calmar =
        constraint_f64(Some(train_gate_policy), "min_train_perturbed_calmar")
            .or_else(|| constraint_f64(Some(train_gate_policy), "min_perturbed_oos_calmar"))
            .or(req.min_perturbed_oos_calmar)
            .unwrap_or(1.2);
    let max_perturbed_drawdown_pct =
        constraint_f64(Some(train_gate_policy), "max_train_perturbed_drawdown_pct")
            .or_else(|| constraint_f64(Some(train_gate_policy), "max_perturbed_oos_drawdown_pct"))
            .or(req.max_perturbed_oos_drawdown_pct)
            .unwrap_or(0.35);

    OosCostCapacityGateConfig {
        enabled,
        min_pass_ratio,
        min_perturbed_calmar,
        max_perturbed_drawdown_pct,
    }
}


pub(crate) fn train_cost_capacity_stress_aware_selection_enabled(
    train_gate_policy: &Value,
    gate: &OosCostCapacityGateConfig,
) -> bool {
    if !gate.enabled {
        return false;
    }
    constraint_bool(
        Some(train_gate_policy),
        "enable_train_cost_capacity_stress_aware_selection",
    )
    .or_else(|| constraint_bool(Some(train_gate_policy), "stress_aware_train_selection"))
    .unwrap_or(true)
}


pub(crate) fn best_effort_train_selection_for_diagnostics_enabled(train_gate_policy: &Value) -> bool {
    constraint_bool(
        Some(train_gate_policy),
        "allow_best_effort_train_selection_for_diagnostics",
    )
    .or_else(|| {
        constraint_bool(
            Some(train_gate_policy),
            "diagnostic_best_effort_train_selection",
        )
    })
    .unwrap_or(false)
}


pub(crate) fn oos_cost_capacity_perturbation_name(
    perturbation: &OosCostCapacityPerturbationRequest,
    index: usize,
) -> String {
    perturbation
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("cost_capacity_perturbation_{}", index + 1))
}


pub(crate) fn apply_cost_capacity_perturbation_to_parameters(
    parameters: &Value,
    perturbation: &OosCostCapacityPerturbationRequest,
) -> Result<Value, String> {
    let mut object = parameters
        .as_object()
        .cloned()
        .ok_or_else(|| "selected candidate parameters must be a JSON object".to_string())?;
    if let Some(value) = perturbation.capacity_penalty_strength {
        object.insert("capacity_penalty_strength".to_string(), json!(value));
    }
    upsert_nested_number(
        &mut object,
        "cost_model",
        "cost_multiplier",
        perturbation.cost_multiplier,
    )?;
    upsert_nested_number(
        &mut object,
        "cost_model",
        "slippage_bps",
        perturbation.slippage_bps,
    )?;
    upsert_nested_number(
        &mut object,
        "cost_model",
        "impact_cost_coefficient",
        perturbation.impact_cost_coefficient,
    )?;
    upsert_nested_number(
        &mut object,
        "execution_rules",
        "max_participation_rate",
        perturbation.max_participation_rate,
    )?;
    Ok(Value::Object(object))
}


pub(crate) fn upsert_nested_number(
    object: &mut Map<String, Value>,
    parent: &str,
    child: &str,
    value: Option<f64>,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    match object.entry(parent.to_string()) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(json!({ child: value }));
        }
        serde_json::map::Entry::Occupied(mut entry) => {
            let parent_object = entry
                .get_mut()
                .as_object_mut()
                .ok_or_else(|| format!("{} must be a JSON object", parent))?;
            parent_object.insert(child.to_string(), json!(value));
        }
    }
    Ok(())
}


pub(crate) fn oos_cost_capacity_perturbation_passed(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    final_promotion_gate_policy: &Value,
    output: &FactorBacktestRunOutput,
) -> bool {
    let min_calmar = req
        .min_perturbed_oos_calmar
        .or_else(|| {
            constraint_f64(
                Some(final_promotion_gate_policy),
                "min_perturbed_oos_calmar",
            )
        })
        .unwrap_or(1.2);
    let max_drawdown = req
        .max_perturbed_oos_drawdown_pct
        .or_else(|| {
            constraint_f64(
                Some(final_promotion_gate_policy),
                "max_perturbed_oos_drawdown_pct",
            )
        })
        .unwrap_or(0.35);
    cost_capacity_perturbation_passed_with_thresholds(output, min_calmar, max_drawdown)
}


pub(crate) fn cost_capacity_perturbation_passed_with_thresholds(
    output: &FactorBacktestRunOutput,
    min_calmar: f64,
    max_drawdown: f64,
) -> bool {
    let min_calmar = Decimal::from_f64_retain(min_calmar).unwrap_or(Decimal::ZERO);
    let max_drawdown = Decimal::from_f64_retain(max_drawdown).unwrap_or(Decimal::MAX);
    output.metrics.calmar_ratio >= min_calmar && output.metrics.max_drawdown_pct <= max_drawdown
}


pub(crate) fn build_cost_capacity_pass_ratio_gate(
    gate_name: &str,
    passed_count: usize,
    total_count: usize,
    gate: &OosCostCapacityGateConfig,
) -> Value {
    let pass_ratio = ratio(passed_count, total_count);
    json!({
        "gate": gate_name,
        "passed": total_count > 0 && pass_ratio >= gate.min_pass_ratio,
        "limit": gate.min_pass_ratio,
        "actual": pass_ratio,
        "passed_count": passed_count,
        "total_count": total_count,
        "min_perturbed_oos_calmar": gate.min_perturbed_calmar,
        "max_perturbed_oos_drawdown_pct": gate.max_perturbed_drawdown_pct,
    })
}


pub(crate) fn cost_capacity_perturbation_pass_counts(
    results: &[OosCostCapacityPerturbationResult],
) -> (usize, usize) {
    (
        results.iter().filter(|result| result.passed).count(),
        results.len(),
    )
}


pub(crate) fn cost_capacity_perturbation_summary(
    results: &[OosCostCapacityPerturbationResult],
) -> CostCapacityPerturbationSummary {
    let (passed_count, total_count) = cost_capacity_perturbation_pass_counts(results);
    let pass_ratio_ppm = if total_count == 0 {
        0
    } else {
        ((passed_count as i64) * 1_000_000) / total_count as i64
    };
    let mut min_calmar = Decimal::MAX;
    let mut total_calmar = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;
    let mut min_annual_return = Decimal::MAX;
    let mut total_sharpe = Decimal::ZERO;
    let mut min_sortino = Decimal::MAX;
    let mut min_num_trades = u64::MAX;
    let mut max_final_cash_weight = Decimal::ZERO;
    let mut total_final_cash_weight = Decimal::ZERO;
    let mut min_final_actual_gross_exposure = Decimal::MAX;
    let mut max_final_unfilled_target_gap = Decimal::ZERO;
    let mut total_final_unfilled_target_gap = Decimal::ZERO;
    let mut min_final_execution_fill_ratio = Decimal::MAX;
    let mut total_final_execution_fill_ratio = Decimal::ZERO;
    let mut max_execution_target_gap = Decimal::ZERO;
    let mut max_execution_schedule_expired_count = 0usize;
    let mut total_execution_schedule_expired_count = 0usize;

    for result in results {
        let metrics = &result.output.metrics;
        min_calmar = min_calmar.min(metrics.calmar_ratio);
        total_calmar += metrics.calmar_ratio;
        max_drawdown = max_drawdown.max(metrics.max_drawdown_pct);
        min_annual_return = min_annual_return.min(metrics.annual_return_pct);
        total_sharpe += metrics.sharpe_ratio;
        min_sortino = min_sortino.min(metrics.sortino_ratio);
        min_num_trades = min_num_trades.min(metrics.num_trades as u64);
        max_final_cash_weight = max_final_cash_weight.max(metrics.final_cash_weight_pct);
        total_final_cash_weight += metrics.final_cash_weight_pct;
        min_final_actual_gross_exposure =
            min_final_actual_gross_exposure.min(metrics.final_actual_gross_exposure_pct);
        max_final_unfilled_target_gap =
            max_final_unfilled_target_gap.max(metrics.final_unfilled_target_gap_pct);
        total_final_unfilled_target_gap += metrics.final_unfilled_target_gap_pct;
        min_final_execution_fill_ratio =
            min_final_execution_fill_ratio.min(metrics.final_execution_fill_ratio);
        total_final_execution_fill_ratio += metrics.final_execution_fill_ratio;
        max_execution_target_gap =
            max_execution_target_gap.max(metrics.max_execution_target_gap_pct);
        max_execution_schedule_expired_count =
            max_execution_schedule_expired_count.max(metrics.execution_schedule_expired_count);
        total_execution_schedule_expired_count += metrics.execution_schedule_expired_count;
    }

    if total_count == 0 {
        min_calmar = Decimal::ZERO;
        min_annual_return = Decimal::ZERO;
        min_sortino = Decimal::ZERO;
        min_num_trades = 0;
        min_final_actual_gross_exposure = Decimal::ZERO;
        min_final_execution_fill_ratio = Decimal::ONE;
    }
    let denominator = Decimal::from(total_count.max(1) as i64);
    CostCapacityPerturbationSummary {
        passed_count,
        total_count,
        pass_ratio_ppm,
        min_calmar,
        avg_calmar: total_calmar / denominator,
        max_drawdown,
        min_annual_return,
        avg_sharpe: total_sharpe / denominator,
        min_sortino,
        min_num_trades,
        max_final_cash_weight,
        avg_final_cash_weight: total_final_cash_weight / denominator,
        min_final_actual_gross_exposure,
        max_final_unfilled_target_gap,
        avg_final_unfilled_target_gap: total_final_unfilled_target_gap / denominator,
        min_final_execution_fill_ratio,
        avg_final_execution_fill_ratio: total_final_execution_fill_ratio / denominator,
        max_execution_target_gap,
        max_execution_schedule_expired_count,
        total_execution_schedule_expired_count,
    }
}


#[cfg(test)]
pub(crate) fn cost_capacity_perturbation_summary_from_counts(
    passed_count: usize,
    total_count: usize,
) -> CostCapacityPerturbationSummary {
    let pass_ratio_ppm = if total_count == 0 {
        0
    } else {
        ((passed_count as i64) * 1_000_000) / total_count as i64
    };
    CostCapacityPerturbationSummary {
        passed_count,
        total_count,
        pass_ratio_ppm,
        min_calmar: Decimal::ZERO,
        avg_calmar: Decimal::ZERO,
        max_drawdown: Decimal::ZERO,
        min_annual_return: Decimal::ZERO,
        avg_sharpe: Decimal::ZERO,
        min_sortino: Decimal::ZERO,
        min_num_trades: total_count as u64,
        max_final_cash_weight: Decimal::ZERO,
        avg_final_cash_weight: Decimal::ZERO,
        min_final_actual_gross_exposure: Decimal::ONE,
        max_final_unfilled_target_gap: Decimal::ZERO,
        avg_final_unfilled_target_gap: Decimal::ZERO,
        min_final_execution_fill_ratio: Decimal::ONE,
        avg_final_execution_fill_ratio: Decimal::ZERO,
        max_execution_target_gap: Decimal::ZERO,
        max_execution_schedule_expired_count: 0,
        total_execution_schedule_expired_count: 0,
    }
}


pub(crate) fn clamp_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal {
    value.max(lower).min(upper)
}


pub(crate) fn train_cost_capacity_stress_score_profile(train_gate_policy: &Value) -> &'static str {
    let profile = constraint_str(Some(train_gate_policy), "train_stress_score_profile")
        .or_else(|| constraint_str(Some(train_gate_policy), "stress_aware_selection_score"))
        .or_else(|| {
            constraint_str(
                Some(train_gate_policy),
                "train_cost_capacity_stress_score_profile",
            )
        })
        .unwrap_or("train_stress_adjusted_score_v1")
        .trim();
    match profile {
        "capacity_stress_calmar_score_v1"
        | "capacity_stress"
        | "capacity_aware"
        | "capacity_calmar" => "capacity_stress_calmar_score_v1",
        "cash_drag_fill_gap_score_v1"
        | "cash_drag_fill_gap"
        | "cash_drag_aware"
        | "fill_gap_aware" => "cash_drag_fill_gap_score_v1",
        "capacity_stress_return_score_v1"
        | "capacity_stress_return"
        | "capacity_return"
        | "stress_return" => "capacity_stress_return_score_v1",
        "stress_fill_objective_score_v1"
        | "stress_fill_objective"
        | "fill_objective"
        | "stress_fill" => "stress_fill_objective_score_v1",
        "prediction_confidence_stress_fill_objective_score_v1"
        | "prediction_confidence_stress_fill"
        | "confidence_stress_fill"
        | "ml_confidence_stress_fill" => "prediction_confidence_stress_fill_objective_score_v1",
        "prediction_confidence_stress_fill_quality_score_v1"
        | "prediction_confidence_stress_fill_quality"
        | "confidence_stress_fill_quality"
        | "ml_confidence_stress_fill_quality" => {
            "prediction_confidence_stress_fill_quality_score_v1"
        }
        _ => "train_stress_adjusted_score_v1",
    }
}


pub(crate) fn train_candidate_stress_adjusted_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
) -> Decimal {
    let pass_ratio_score = Decimal::from(summary.pass_ratio_ppm);
    let min_calmar_score = clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(100_000);
    let avg_calmar_score = clamp_decimal(summary.avg_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(25_000);
    let avg_sharpe_score = clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(20_000);
    let min_return_score = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(30_000);
    let drawdown_penalty = clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::from(1))
        * Decimal::from(50_000);
    let professional_gap_penalty = clamp_decimal(
        candidate.professional_gap_score,
        Decimal::ZERO,
        Decimal::from(10),
    ) * Decimal::from(20_000);
    let raw_score_bonus = clamp_decimal(
        candidate.score.unwrap_or(Decimal::ZERO),
        Decimal::from(-100),
        Decimal::from(100),
    ) * Decimal::from(1_000);
    let base_quality_score = candidate.metrics.sharpe * Decimal::from(10_000)
        + candidate.metrics.sortino * Decimal::from(5_000)
        + candidate.metrics.annual_return * Decimal::from(5_000)
        - candidate.metrics.max_drawdown * Decimal::from(20_000)
        - professional_gap_penalty
        + raw_score_bonus;

    pass_ratio_score + min_calmar_score + avg_calmar_score + avg_sharpe_score + min_return_score
        - drawdown_penalty
        + base_quality_score
}


pub(crate) fn train_candidate_capacity_stress_calmar_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let target_calmar =
        constraint_decimal(Some(train_gate_policy), "capacity_stress_target_calmar")
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar"))
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_perturbed_oos_calmar"))
            .unwrap_or_else(|| Decimal::new(12, 1));
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(5, 2));
    let pass_ratio_score = Decimal::from(summary.pass_ratio_ppm) * Decimal::from(2);
    let min_calmar_score = clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(220_000);
    let avg_calmar_score = clamp_decimal(summary.avg_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(60_000);
    let min_return_score = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(120_000);
    let avg_sharpe_score = clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(15_000);
    let drawdown_penalty = clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::from(1))
        * Decimal::from(120_000);
    let calmar_shortfall_penalty =
        (target_calmar - summary.min_calmar).max(Decimal::ZERO) * Decimal::from(300_000);
    let annual_return_shortfall_penalty = (target_annual_return - summary.min_annual_return)
        .max(Decimal::ZERO)
        * Decimal::from(220_000);
    let base_quality_score = candidate.metrics.sharpe * Decimal::from(2_000)
        + candidate.metrics.sortino * Decimal::from(1_000)
        + candidate.metrics.annual_return * Decimal::from(2_000)
        - candidate.metrics.max_drawdown * Decimal::from(5_000);

    pass_ratio_score
        + min_calmar_score
        + avg_calmar_score
        + min_return_score
        + avg_sharpe_score
        + base_quality_score
        - drawdown_penalty
        - calmar_shortfall_penalty
        - annual_return_shortfall_penalty
}


pub(crate) fn train_candidate_capacity_stress_return_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy);
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(15, 2));
    let min_annual_return =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
            .unwrap_or_else(|| Decimal::new(5, 2));
    let target_calmar =
        constraint_decimal(Some(train_gate_policy), "capacity_stress_target_calmar")
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar"))
            .unwrap_or_else(|| Decimal::new(2, 0));
    let min_annual_shortfall =
        positive_decimal_gap(min_annual_return, summary.min_annual_return) * Decimal::from(520_000);
    let target_annual_shortfall =
        positive_decimal_gap(target_annual_return, summary.min_annual_return)
            * Decimal::from(260_000);
    let target_calmar_shortfall =
        positive_decimal_gap(target_calmar, summary.min_calmar) * Decimal::from(180_000);
    let stress_return_bonus = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(220_000);
    let execution_quality_penalty =
        train_execution_quality_penalty(candidate, summary, train_gate_policy);

    base + stress_return_bonus
        - min_annual_shortfall
        - target_annual_shortfall
        - target_calmar_shortfall
        - execution_quality_penalty
}


pub(crate) fn train_execution_quality_penalty(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let mut penalty = Decimal::ZERO;
    if let Some(limit) = constraint_i64(Some(train_gate_policy), "min_train_trade_count") {
        let actual = candidate
            .metrics
            .num_trades
            .min(summary.min_num_trades)
            .min(i64::MAX as u64) as i64;
        penalty += Decimal::from((limit - actual).max(0)) * Decimal::from(12_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    ) {
        let actual = candidate
            .metrics
            .final_actual_gross_exposure
            .min(summary.min_final_actual_gross_exposure);
        penalty += positive_decimal_gap(limit, actual) * Decimal::from(700_000);
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
    {
        let actual = candidate
            .metrics
            .final_cash_weight
            .max(summary.max_final_cash_weight);
        penalty += positive_decimal_gap(actual, limit) * Decimal::from(420_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .final_unfilled_target_gap
            .max(summary.max_final_unfilled_target_gap);
        penalty += positive_decimal_gap(actual, limit) * Decimal::from(620_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    ) {
        let actual = candidate
            .metrics
            .final_execution_fill_ratio
            .min(summary.min_final_execution_fill_ratio);
        penalty += positive_decimal_gap(limit, actual) * Decimal::from(480_000);
    }
    penalty
}


pub(crate) fn train_candidate_stress_fill_objective_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_return_score(candidate, summary, train_gate_policy);
    let min_exposure_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    )
    .unwrap_or_else(|| Decimal::new(35, 2));
    let max_cash_limit =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
            .unwrap_or_else(|| Decimal::new(65, 2));
    let max_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    )
    .unwrap_or_else(|| Decimal::new(6, 2));
    let min_fill_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    )
    .unwrap_or_else(|| Decimal::new(95, 2));

    let actual_exposure = candidate
        .metrics
        .final_actual_gross_exposure
        .min(summary.min_final_actual_gross_exposure);
    let cash_weight = candidate
        .metrics
        .final_cash_weight
        .max(summary.max_final_cash_weight);
    let unfilled_gap = candidate
        .metrics
        .final_unfilled_target_gap
        .max(summary.max_final_unfilled_target_gap);
    let fill_ratio = candidate
        .metrics
        .final_execution_fill_ratio
        .min(summary.min_final_execution_fill_ratio);

    let tradable_exposure_bonus =
        clamp_decimal(actual_exposure, Decimal::ZERO, Decimal::ONE) * Decimal::from(260_000);
    let fill_ratio_bonus =
        clamp_decimal(fill_ratio, Decimal::ZERO, Decimal::ONE) * Decimal::from(180_000);
    let cash_efficiency_bonus =
        positive_decimal_gap(max_cash_limit, cash_weight) * Decimal::from(120_000);
    let exposure_shortfall_penalty =
        positive_decimal_gap(min_exposure_limit, actual_exposure) * Decimal::from(1_200_000);
    let cash_overrun_penalty =
        positive_decimal_gap(cash_weight, max_cash_limit) * Decimal::from(900_000);
    let gap_overrun_penalty =
        positive_decimal_gap(unfilled_gap, max_gap_limit) * Decimal::from(1_100_000);
    let fill_shortfall_penalty =
        positive_decimal_gap(min_fill_limit, fill_ratio) * Decimal::from(950_000);

    base + tradable_exposure_bonus + fill_ratio_bonus + cash_efficiency_bonus
        - exposure_shortfall_penalty
        - cash_overrun_penalty
        - gap_overrun_penalty
        - fill_shortfall_penalty
}


pub(crate) fn candidate_prediction_confidence_score(candidate: &DiscoveryCandidate) -> Decimal {
    let Some(parameters) = candidate.parameters.as_object() else {
        return Decimal::ZERO;
    };
    let has_train_window_internal_prediction = parameters
        .get("prediction_set_override_source")
        .and_then(Value::as_str)
        .map(|source| source == "train_window_ml_internal")
        .unwrap_or(false);
    let confidence_gate_bonus = parameters
        .get("prediction_confidence_gate_profile")
        .and_then(Value::as_str)
        .filter(|profile| *profile == "train_positive_raw_score_gate_v1")
        .map(|_| Decimal::from(180_000))
        .unwrap_or(Decimal::ZERO);
    let internal_prediction_bonus = if has_train_window_internal_prediction {
        Decimal::from(120_000)
    } else {
        Decimal::ZERO
    };
    let min_score = decimal_from_json(parameters.get("prediction_min_score"));
    let min_score_bonus = min_score
        .map(|score| {
            clamp_decimal(score, Decimal::new(-5, 2), Decimal::new(5, 2)) * Decimal::from(3_000_000)
        })
        .unwrap_or(Decimal::ZERO);
    let min_score_absence_penalty = if min_score.is_some() {
        Decimal::ZERO
    } else {
        Decimal::from(150_000)
    };
    let percentile_bonus = decimal_from_json(parameters.get("prediction_min_percentile"))
        .map(|percentile| {
            clamp_decimal(percentile, Decimal::ZERO, Decimal::ONE) * Decimal::from(160_000)
        })
        .unwrap_or(Decimal::ZERO);

    confidence_gate_bonus + internal_prediction_bonus + min_score_bonus + percentile_bonus
        - min_score_absence_penalty
}


pub(crate) fn train_candidate_prediction_confidence_stress_fill_objective_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy)
        + candidate_prediction_confidence_score(candidate)
}


pub(crate) fn prediction_confidence_stress_fill_quality_score_breakdown(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> PredictionConfidenceStressFillQualityScoreBreakdown {
    let stress_fill_objective_score =
        train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy);
    let prediction_confidence_score = candidate_prediction_confidence_score(candidate);
    let base_score = stress_fill_objective_score + prediction_confidence_score;
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(15, 2));
    let min_annual_return =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
            .unwrap_or_else(|| Decimal::new(5, 2));
    let min_calmar = constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar")
        .unwrap_or_else(|| Decimal::new(12, 1));
    let min_sharpe = constraint_decimal(Some(train_gate_policy), "min_train_avg_perturbed_sharpe")
        .unwrap_or_else(|| Decimal::new(5, 1));
    let min_sortino = constraint_decimal(Some(train_gate_policy), "min_train_perturbed_sortino")
        .unwrap_or_else(|| Decimal::ONE);

    let pass_ratio_bonus = Decimal::from(summary.pass_ratio_ppm) * Decimal::from(2);
    let annual_quality_bonus = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(360_000);
    let calmar_quality_bonus =
        clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(220_000);
    let sharpe_quality_bonus =
        clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(140_000);
    let sortino_quality_bonus =
        clamp_decimal(summary.min_sortino, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(120_000);
    let drawdown_tail_penalty =
        clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::ONE) * Decimal::from(260_000);
    let min_annual_shortfall =
        positive_decimal_gap(min_annual_return, summary.min_annual_return) * Decimal::from(760_000);
    let target_annual_shortfall =
        positive_decimal_gap(target_annual_return, summary.min_annual_return)
            * Decimal::from(380_000);
    let calmar_shortfall =
        positive_decimal_gap(min_calmar, summary.min_calmar) * Decimal::from(340_000);
    let sharpe_shortfall =
        positive_decimal_gap(min_sharpe, summary.avg_sharpe) * Decimal::from(260_000);
    let sortino_shortfall =
        positive_decimal_gap(min_sortino, summary.min_sortino) * Decimal::from(220_000);
    let total_score = base_score
        + pass_ratio_bonus
        + annual_quality_bonus
        + calmar_quality_bonus
        + sharpe_quality_bonus
        + sortino_quality_bonus
        - drawdown_tail_penalty
        - min_annual_shortfall
        - target_annual_shortfall
        - calmar_shortfall
        - sharpe_shortfall
        - sortino_shortfall;

    PredictionConfidenceStressFillQualityScoreBreakdown {
        total_score,
        base_score,
        stress_fill_objective_score,
        prediction_confidence_score,
        target_annual_return,
        min_annual_return,
        min_calmar,
        min_sharpe,
        min_sortino,
        pass_ratio_bonus,
        annual_quality_bonus,
        calmar_quality_bonus,
        sharpe_quality_bonus,
        sortino_quality_bonus,
        drawdown_tail_penalty,
        min_annual_shortfall,
        target_annual_shortfall,
        calmar_shortfall,
        sharpe_shortfall,
        sortino_shortfall,
    }
}


pub(crate) fn train_candidate_prediction_confidence_stress_fill_quality_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    prediction_confidence_stress_fill_quality_score_breakdown(candidate, summary, train_gate_policy)
        .total_score
}


pub(crate) fn train_candidate_cash_drag_fill_gap_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy);
    let max_cash_limit =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
            .unwrap_or_else(|| Decimal::new(20, 2));
    let max_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_execution_target_gap_pct",
    )
    .unwrap_or_else(|| Decimal::new(8, 2));
    let max_unfilled_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    )
    .unwrap_or(max_gap_limit);
    let min_fill_ratio_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    )
    .unwrap_or_else(|| Decimal::new(90, 2));
    let max_expired_count = constraint_i64(
        Some(train_gate_policy),
        "max_train_execution_schedule_expired_count",
    )
    .unwrap_or(0)
    .max(0) as usize;

    let max_cash_weight = candidate
        .metrics
        .final_cash_weight
        .max(summary.max_final_cash_weight);
    let max_target_gap = candidate
        .metrics
        .max_execution_target_gap
        .max(summary.max_execution_target_gap);
    let max_unfilled_target_gap = candidate
        .metrics
        .final_unfilled_target_gap
        .max(summary.max_final_unfilled_target_gap);
    let min_execution_fill_ratio = candidate
        .metrics
        .final_execution_fill_ratio
        .min(summary.min_final_execution_fill_ratio);
    let expired_count = (candidate.metrics.execution_schedule_expired_count as usize)
        .max(summary.max_execution_schedule_expired_count);
    let total_expired_count = (candidate.metrics.execution_schedule_expired_count as usize)
        + summary.total_execution_schedule_expired_count;
    let cash_shortfall_penalty =
        positive_decimal_gap(max_cash_weight, max_cash_limit) * Decimal::from(420_000);
    let avg_cash_penalty = positive_decimal_gap(summary.avg_final_cash_weight, max_cash_limit)
        * Decimal::from(180_000);
    let gap_shortfall_penalty =
        positive_decimal_gap(max_target_gap, max_gap_limit) * Decimal::from(520_000);
    let unfilled_gap_penalty =
        positive_decimal_gap(max_unfilled_target_gap, max_unfilled_gap_limit)
            * Decimal::from(620_000);
    let fill_ratio_shortfall_penalty =
        positive_decimal_gap(min_fill_ratio_limit, min_execution_fill_ratio)
            * Decimal::from(480_000);
    let expired_penalty = Decimal::from(expired_count.saturating_sub(max_expired_count) as i64)
        * Decimal::from(45_000)
        + Decimal::from(total_expired_count as i64) * Decimal::from(10_000);

    base - cash_shortfall_penalty
        - avg_cash_penalty
        - gap_shortfall_penalty
        - unfilled_gap_penalty
        - fill_ratio_shortfall_penalty
        - expired_penalty
}


pub(crate) fn train_candidate_stress_adjusted_score_for_policy(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    match train_cost_capacity_stress_score_profile(train_gate_policy) {
        "capacity_stress_calmar_score_v1" => {
            train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy)
        }
        "capacity_stress_return_score_v1" => {
            train_candidate_capacity_stress_return_score(candidate, summary, train_gate_policy)
        }
        "cash_drag_fill_gap_score_v1" => {
            train_candidate_cash_drag_fill_gap_score(candidate, summary, train_gate_policy)
        }
        "stress_fill_objective_score_v1" => {
            train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy)
        }
        "prediction_confidence_stress_fill_objective_score_v1" => {
            train_candidate_prediction_confidence_stress_fill_objective_score(
                candidate,
                summary,
                train_gate_policy,
            )
        }
        "prediction_confidence_stress_fill_quality_score_v1" => {
            train_candidate_prediction_confidence_stress_fill_quality_score(
                candidate,
                summary,
                train_gate_policy,
            )
        }
        _ => train_candidate_stress_adjusted_score(candidate, summary),
    }
}


pub(crate) fn train_candidate_stress_score_breakdown(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Value {
    let profile = train_cost_capacity_stress_score_profile(train_gate_policy);
    let total_score =
        train_candidate_stress_adjusted_score_for_policy(candidate, summary, train_gate_policy);
    if profile != "prediction_confidence_stress_fill_quality_score_v1" {
        return json!({
            "profile": profile,
            "total_score": total_score,
            "summary": cost_capacity_perturbation_summary_json(summary),
        });
    }

    let breakdown = prediction_confidence_stress_fill_quality_score_breakdown(
        candidate,
        summary,
        train_gate_policy,
    );

    json!({
        "profile": profile,
        "total_score": breakdown.total_score,
        "base_score": breakdown.base_score,
        "summary": cost_capacity_perturbation_summary_json(summary),
        "targets": {
            "target_annual_return_pct": breakdown.target_annual_return,
            "min_annual_return_pct": breakdown.min_annual_return,
            "min_calmar": breakdown.min_calmar,
            "min_avg_sharpe": breakdown.min_sharpe,
            "min_sortino": breakdown.min_sortino,
        },
        "components": {
            "stress_fill_objective_score": breakdown.stress_fill_objective_score,
            "prediction_confidence_score": breakdown.prediction_confidence_score,
            "pass_ratio_bonus": breakdown.pass_ratio_bonus,
            "annual_quality_bonus": breakdown.annual_quality_bonus,
            "calmar_quality_bonus": breakdown.calmar_quality_bonus,
            "sharpe_quality_bonus": breakdown.sharpe_quality_bonus,
            "sortino_quality_bonus": breakdown.sortino_quality_bonus,
        },
        "penalties": {
            "drawdown_tail_penalty": breakdown.drawdown_tail_penalty,
            "min_annual_shortfall": breakdown.min_annual_shortfall,
            "target_annual_shortfall": breakdown.target_annual_shortfall,
            "calmar_shortfall": breakdown.calmar_shortfall,
            "sharpe_shortfall": breakdown.sharpe_shortfall,
            "sortino_shortfall": breakdown.sortino_shortfall,
        },
    })
}


pub(crate) fn train_candidate_evaluation_order(
    left: &OosTrainCandidateEvaluation,
    right: &OosTrainCandidateEvaluation,
) -> std::cmp::Ordering {
    right
        .stress_summary
        .pass_ratio_ppm
        .cmp(&left.stress_summary.pass_ratio_ppm)
        .then_with(|| right.stress_adjusted_score.cmp(&left.stress_adjusted_score))
        .then_with(|| {
            right
                .stress_summary
                .min_calmar
                .cmp(&left.stress_summary.min_calmar)
        })
        .then_with(|| {
            right
                .stress_summary
                .avg_calmar
                .cmp(&left.stress_summary.avg_calmar)
        })
        .then_with(|| {
            left.stress_summary
                .max_drawdown
                .cmp(&right.stress_summary.max_drawdown)
        })
        .then_with(|| {
            right
                .stress_summary
                .min_annual_return
                .cmp(&left.stress_summary.min_annual_return)
        })
        .then_with(|| {
            right
                .stress_summary
                .avg_sharpe
                .cmp(&left.stress_summary.avg_sharpe)
        })
        .then_with(|| discovery_candidate_order(&left.candidate, &right.candidate))
}


pub(crate) fn attach_train_cost_capacity_gate_to_robustness(
    mut robustness: Value,
    passed_count: usize,
    total_count: usize,
    gate: &OosCostCapacityGateConfig,
) -> (Value, bool) {
    if !gate.enabled {
        return (robustness, true);
    }

    let gate_report = build_cost_capacity_pass_ratio_gate(
        "train_cost_capacity_perturbation_pass_ratio",
        passed_count,
        total_count,
        gate,
    );
    let passed = gate_report["passed"].as_bool().unwrap_or(false);
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(vec![gate_report.clone()]));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.push(gate_report.clone());
                }
            }
        }
        object.insert("train_cost_capacity_gate".to_string(), gate_report);
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}


pub(crate) fn attach_train_capacity_stress_return_gates_to_robustness(
    mut robustness: Value,
    _candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> (Value, bool) {
    let mut gates = Vec::new();
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
    {
        let actual = summary.min_annual_return;
        gates.push(json!({
            "gate": "train_perturbed_annual_return",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "min_train_avg_perturbed_calmar")
    {
        let actual = summary.avg_calmar;
        gates.push(json!({
            "gate": "train_avg_perturbed_calmar",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if gates.is_empty() {
        return (robustness, true);
    }

    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(gates.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.extend(gates.iter().cloned());
                }
            }
        }
        object.insert(
            "train_capacity_stress_return_gates".to_string(),
            Value::Array(gates),
        );
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}


pub(crate) fn attach_train_cash_drag_fill_gap_gates_to_robustness(
    mut robustness: Value,
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> (Value, bool) {
    let mut gates = Vec::new();
    if let Some(limit) = constraint_i64(Some(train_gate_policy), "min_train_trade_count") {
        let actual = candidate
            .metrics
            .num_trades
            .min(summary.min_num_trades)
            .min(i64::MAX as u64) as i64;
        gates.push(json!({
            "gate": "train_trade_count",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
    {
        let actual = candidate
            .metrics
            .final_cash_weight
            .max(summary.max_final_cash_weight);
        gates.push(json!({
            "gate": "train_final_cash_weight",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    ) {
        let actual = candidate
            .metrics
            .final_actual_gross_exposure
            .min(summary.min_final_actual_gross_exposure);
        gates.push(json!({
            "gate": "train_final_actual_gross_exposure",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_execution_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .max_execution_target_gap
            .max(summary.max_execution_target_gap);
        gates.push(json!({
            "gate": "train_execution_target_gap",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .final_unfilled_target_gap
            .max(summary.max_final_unfilled_target_gap);
        gates.push(json!({
            "gate": "train_final_unfilled_target_gap",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    ) {
        let actual = candidate
            .metrics
            .final_execution_fill_ratio
            .min(summary.min_final_execution_fill_ratio);
        gates.push(json!({
            "gate": "train_final_execution_fill_ratio",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_i64(
        Some(train_gate_policy),
        "max_train_execution_schedule_expired_count",
    ) {
        let limit = limit.max(0) as usize;
        let actual = (candidate.metrics.execution_schedule_expired_count as usize)
            .max(summary.max_execution_schedule_expired_count);
        gates.push(json!({
            "gate": "train_execution_schedule_expired_count",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if gates.is_empty() {
        return (robustness, true);
    }

    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(gates.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.extend(gates.iter().cloned());
                }
            }
        }
        object.insert(
            "train_cash_drag_fill_gap_gates".to_string(),
            Value::Array(gates),
        );
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}


pub(crate) fn train_cost_capacity_overlay_persistence_fields(
    robustness: &Value,
) -> Result<RobustnessOverlayPersistenceFields, String> {
    let gate_result_id = robustness["gate_result_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "train cost/capacity robustness overlay missing gate_result_id".to_string())?
        .to_string();
    let status = robustness["status"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "train cost/capacity robustness overlay missing status".to_string())?
        .to_string();
    let gate_results = robustness
        .get("gate_results")
        .cloned()
        .ok_or_else(|| "train cost/capacity robustness overlay missing gate_results".to_string())?;
    Ok(RobustnessOverlayPersistenceFields {
        gate_result_id,
        status,
        gate_results,
    })
}


pub(crate) async fn persist_train_cost_capacity_robustness_overlay(
    db: &sqlx::PgPool,
    robustness: &Value,
) -> Result<(), String> {
    let fields = train_cost_capacity_overlay_persistence_fields(robustness)?;
    sqlx::query(
        "UPDATE robustness_gate_result
         SET gate_results = $2,
             status = $3,
             summary = $4
         WHERE gate_result_id = $1",
    )
    .bind(&fields.gate_result_id)
    .bind(&fields.gate_results)
    .bind(&fields.status)
    .bind(format!(
        "OOS walk-forward train-window robustness gate evaluated as {}",
        fields.status
    ))
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to persist train cost/capacity robustness overlay: {}",
            error
        )
    })?;
    Ok(())
}


pub(crate) fn build_oos_discovery_plan(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Result<OosDiscoveryPlan, String> {
    let template = req
        .backtest_template
        .clone()
        .unwrap_or_else(default_phase7_backtest_template);
    let start_date = parse_template_date(&template, "start_date", "20160201")?;
    let end_date = parse_template_date(&template, "end_date", "20260515")?;
    if start_date >= end_date {
        return Err("backtest_template.start_date must be before end_date".into());
    }
    normalize_oos_train_cache_mode(req.train_cache_mode.as_deref())?;
    let validation_mode = req
        .validation_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("walk_forward")
        .to_string();
    let train_window_days = req.train_window_days.unwrap_or(365 * 3).max(30);
    let test_window_days = req.test_window_days.unwrap_or(365).max(5);
    let step_days = req.step_days.unwrap_or(test_window_days).max(1);
    let in_sample_ratio = req.in_sample_ratio.unwrap_or(0.80).clamp(0.50, 0.95);
    let include_partial_last_window = req.include_partial_last_window.unwrap_or(false);
    let windows = match validation_mode.as_str() {
        "holdout" | "holdout_80_20" | "oos_holdout" => {
            build_holdout_oos_windows(start_date, end_date, in_sample_ratio)?
        }
        "walk_forward" | "rolling_walk_forward" | "wfa" => build_rolling_oos_windows(
            start_date,
            end_date,
            train_window_days,
            test_window_days,
            step_days,
            include_partial_last_window,
        )?,
        other => return Err(format!("unsupported validation_mode: {}", other)),
    };
    if windows.is_empty() {
        return Err("OOS discovery plan has no valid train/test windows".into());
    }
    Ok(OosDiscoveryPlan {
        validation_mode,
        start_date,
        end_date,
        train_window_days,
        test_window_days,
        step_days,
        in_sample_ratio,
        include_partial_last_window,
        windows,
    })
}


pub(crate) fn default_phase7_oos_comparison_profiles() -> Vec<String> {
    vec![
        "phase7_ec".to_string(),
        "phase7_eu".to_string(),
        "phase7_ev".to_string(),
    ]
}


pub(crate) const RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT: &str =
    "/api/v1/quant/experiments/return-risk-cache-economics/report";


pub(crate) fn return_risk_cache_comparison_enabled(value: Option<bool>) -> bool {
    value.unwrap_or(false)
}


pub(crate) fn normalize_profile_comparison_profiles(
    profiles: Option<Vec<String>>,
) -> Result<Vec<String>, String> {
    let profiles = profiles.unwrap_or_else(default_phase7_oos_comparison_profiles);
    if profiles.is_empty() {
        return Err("profile comparison requires at least one search_profile".to_string());
    }
    profiles
        .into_iter()
        .map(|profile| {
            let profile = profile.trim().to_string();
            if profile.is_empty() {
                Err("profile comparison search_profile must not be empty".to_string())
            } else {
                Ok(profile)
            }
        })
        .collect()
}


pub(crate) fn profile_comparison_return_risk_cache_modes(enabled: bool) -> Vec<Option<&'static str>> {
    if enabled {
        vec![Some("raw_matrix"), Some("stats_matrix_experimental")]
    } else {
        vec![None]
    }
}


pub(crate) fn normalize_return_risk_feature_cache_mode(value: &str) -> Result<&'static str, String> {
    match value.trim() {
        "raw_matrix" | "raw-matrix" => Ok("raw_matrix"),
        "stats_matrix_experimental" | "stats-matrix-experimental" => {
            Ok("stats_matrix_experimental")
        }
        other => Err(format!(
            "return_risk_feature_cache_mode must be raw_matrix or stats_matrix_experimental, got {}",
            other
        )),
    }
}


pub(crate) fn set_request_return_risk_feature_cache_mode(
    req: &mut Phase7OosWalkForwardDiscoveryRequest,
    mode: &str,
) -> Result<(), String> {
    let mode = normalize_return_risk_feature_cache_mode(mode)?;
    let mut template = req
        .backtest_template
        .clone()
        .unwrap_or_else(default_phase7_backtest_template);
    let object = template
        .as_object_mut()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    object.insert("return_risk_feature_cache_mode".to_string(), json!(mode));
    req.backtest_template = Some(template);
    Ok(())
}


pub(crate) fn request_return_risk_feature_cache_mode(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Option<String> {
    req.backtest_template
        .as_ref()
        .and_then(|template| template.get("return_risk_feature_cache_mode"))
        .and_then(Value::as_str)
        .map(str::to_string)
}


pub(crate) fn profile_comparison_request(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
) -> Phase7OosWalkForwardDiscoveryRequest {
    let mut req = base.clone();
    req.search_profile = Some(search_profile.to_string());
    req.plan_only = Some(true);
    req.execution_mode = Some("inline".to_string());
    req.train_cache_mode = Some(
        req.train_cache_mode
            .filter(|mode| !mode.trim().is_empty())
            .unwrap_or_else(|| "auto".to_string()),
    );
    req
}


pub(crate) fn profile_comparison_request_for_cache_mode(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
    cache_mode: Option<&str>,
) -> Result<Phase7OosWalkForwardDiscoveryRequest, String> {
    let mut req = profile_comparison_request(base, search_profile);
    if let Some(cache_mode) = cache_mode {
        set_request_return_risk_feature_cache_mode(&mut req, cache_mode)?;
    }
    Ok(req)
}


pub(crate) fn oos_request_plan_json(req: &Phase7OosWalkForwardDiscoveryRequest) -> Value {
    json!({
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "search_profile": req.search_profile,
        "max_trials_per_window": req.max_trials_per_window,
        "trial_batch_limit": req.trial_batch_limit,
        "trial_concurrency": req.trial_concurrency,
        "train_cache_mode": req.train_cache_mode,
        "max_batches_per_window": req.max_batches_per_window,
        "train_window_days": req.train_window_days,
        "test_window_days": req.test_window_days,
        "step_days": req.step_days,
        "validation_mode": req.validation_mode,
        "include_partial_last_window": req.include_partial_last_window,
        "plan_only": req.plan_only,
        "execution_mode": req.execution_mode,
        "oos_top_n": req.oos_top_n,
        "enable_cost_capacity_perturbation_gate": req.enable_cost_capacity_perturbation_gate,
        "return_risk_feature_cache_mode": request_return_risk_feature_cache_mode(req),
    })
}


pub(crate) fn build_phase7_oos_profile_comparison_plan(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    profiles: Option<Vec<String>>,
    return_risk_cache_comparison: Option<bool>,
) -> Result<Value, String> {
    let profiles = normalize_profile_comparison_profiles(profiles)?;
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(return_risk_cache_comparison);
    let cache_modes = profile_comparison_return_risk_cache_modes(return_risk_cache_comparison);

    let mut profile_plans = Vec::new();
    for profile in &profiles {
        for cache_mode in &cache_modes {
            let req = profile_comparison_request_for_cache_mode(base, profile, *cache_mode)?;
            let plan = build_oos_discovery_plan(&req)?;
            let plan_json = oos_discovery_plan_json(&plan);
            let config = oos_walk_forward_experiment_config(&req, &plan_json);
            let return_risk_feature_cache_mode = request_return_risk_feature_cache_mode(&req);
            profile_plans.push(json!({
                "search_profile": profile,
                "return_risk_feature_cache_mode": return_risk_feature_cache_mode,
                "cache_pair_key": if return_risk_cache_comparison {
                    json!(format!("{}:return_risk_cache", profile))
                } else {
                    Value::Null
                },
                "request": oos_request_plan_json(&req),
                "plan": plan_json,
                "config": config,
            }));
        }
    }

    Ok(json!({
        "search_method": "phase7_oos_profile_comparison_plan",
        "profile_count": profiles.len(),
        "launch_count": profile_plans.len(),
        "return_risk_cache_comparison": return_risk_cache_comparison_plan(
            return_risk_cache_comparison,
            profiles.len()
        ),
        "profiles": profile_plans,
    }))
}


pub(crate) fn bounded_profile_comparison_smoke_request(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
) -> Result<Phase7OosWalkForwardDiscoveryRequest, String> {
    let mut req = profile_comparison_request(base, search_profile);
    req.plan_only = Some(false);
    req.execution_mode = Some("background".to_string());
    req.train_cache_mode = Some(
        req.train_cache_mode
            .filter(|mode| !mode.trim().is_empty())
            .unwrap_or_else(|| "auto".to_string()),
    );
    req.max_trials_per_window = Some(req.max_trials_per_window.unwrap_or(8).clamp(1, 8));
    req.trial_batch_limit = Some(req.trial_batch_limit.unwrap_or(8).clamp(1, 8));
    req.max_batches_per_window = Some(req.max_batches_per_window.unwrap_or(1).clamp(1, 1));
    req.oos_top_n = Some(req.oos_top_n.unwrap_or(3).clamp(1, 3));
    Ok(req)
}


pub(crate) fn build_phase7_oos_profile_comparison_launch_requests(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    profiles: Option<Vec<String>>,
    return_risk_cache_comparison: Option<bool>,
) -> Result<Vec<Phase7OosWalkForwardDiscoveryRequest>, String> {
    let profiles = normalize_profile_comparison_profiles(profiles)
        .map_err(|message| message.replace("requires", "launch requires"))?;
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(return_risk_cache_comparison);
    let cache_modes = profile_comparison_return_risk_cache_modes(return_risk_cache_comparison);

    let mut requests = Vec::new();
    for profile in &profiles {
        for cache_mode in &cache_modes {
            let mut req = bounded_profile_comparison_smoke_request(base, profile)?;
            if let Some(cache_mode) = cache_mode {
                set_request_return_risk_feature_cache_mode(&mut req, cache_mode)?;
            }
            requests.push(req);
        }
    }
    Ok(requests)
}


pub(crate) fn return_risk_cache_comparison_plan(enabled: bool, pair_count: usize) -> Value {
    json!({
        "enabled": enabled,
        "pair_count": if enabled { pair_count } else { 0 },
        "modes": if enabled {
            json!(["raw_matrix", "stats_matrix_experimental"])
        } else {
            json!([])
        },
        "report_endpoint": if enabled {
            json!(RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT)
        } else {
            Value::Null
        },
        "pairing_rule": if enabled {
            json!("same search_profile, windows, gates, search budget, and training data; only backtest_template.return_risk_feature_cache_mode differs")
        } else {
            Value::Null
        },
    })
}


pub(crate) fn cache_economics_report_launch_plan(enabled: bool, launches: &[Value]) -> Value {
    if !enabled {
        return return_risk_cache_comparison_plan(false, 0);
    }

    let mut pairs: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();
    for launch in launches {
        let Some(profile) = launch.get("search_profile").and_then(Value::as_str) else {
            continue;
        };
        let Some(mode) = launch
            .get("return_risk_feature_cache_mode")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(experiment_run_id) = launch.get("experiment_run_id").and_then(Value::as_str)
        else {
            continue;
        };
        let entry = pairs.entry(profile.to_string()).or_default();
        match mode {
            "raw_matrix" => entry.0 = Some(experiment_run_id.to_string()),
            "stats_matrix_experimental" => entry.1 = Some(experiment_run_id.to_string()),
            _ => {}
        }
    }

    let report_pairs = pairs
        .into_iter()
        .map(|(profile, (raw_id, stats_id))| {
            json!({
                "search_profile": profile,
                "raw_experiment_run_id": raw_id,
                "stats_experiment_run_id": stats_id,
                "ready_when": "both experiments are completed",
                "report_endpoint": RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT,
                "report_request": {
                    "raw_experiment_run_id": raw_id,
                    "stats_experiment_run_id": stats_id,
                }
            })
        })
        .collect::<Vec<_>>();

    json!({
        "enabled": true,
        "pair_count": report_pairs.len(),
        "modes": ["raw_matrix", "stats_matrix_experimental"],
        "report_endpoint": RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT,
        "ready_when": "call report endpoint only after each raw/stats pair reaches completed status",
        "pairs": report_pairs,
    })
}


pub(crate) fn build_holdout_oos_windows(
    start_date: NaiveDate,
    end_date: NaiveDate,
    in_sample_ratio: f64,
) -> Result<Vec<OosDiscoveryWindow>, String> {
    let total_days = (end_date - start_date).num_days();
    if total_days < 30 {
        return Err("holdout OOS requires at least 30 calendar days".into());
    }
    let train_days = ((total_days as f64) * in_sample_ratio).round() as i64;
    let train_end = start_date + Duration::days(train_days.max(1));
    let test_start = train_end + Duration::days(1);
    if test_start > end_date {
        return Err("holdout split leaves no OOS test period".into());
    }
    Ok(vec![OosDiscoveryWindow {
        window_index: 1,
        validation_mode: "holdout_80_20".to_string(),
        train_start: start_date,
        train_end,
        test_start,
        test_end: end_date,
    }])
}


pub(crate) fn build_rolling_oos_windows(
    start_date: NaiveDate,
    end_date: NaiveDate,
    train_window_days: i64,
    test_window_days: i64,
    step_days: i64,
    include_partial_last_window: bool,
) -> Result<Vec<OosDiscoveryWindow>, String> {
    if train_window_days <= 0 || test_window_days <= 0 || step_days <= 0 {
        return Err("walk-forward train/test/step days must be positive".into());
    }
    let mut windows = Vec::new();
    let mut train_start = start_date;
    loop {
        let train_end = train_start + Duration::days(train_window_days - 1);
        let test_start = train_end + Duration::days(1);
        let mut test_end = test_start + Duration::days(test_window_days - 1);
        if test_start > end_date {
            break;
        }
        if test_end > end_date {
            if include_partial_last_window {
                test_end = end_date;
            } else {
                break;
            }
        }
        windows.push(OosDiscoveryWindow {
            window_index: windows.len() + 1,
            validation_mode: "walk_forward".to_string(),
            train_start,
            train_end,
            test_start,
            test_end,
        });
        train_start += Duration::days(step_days);
    }
    Ok(windows)
}


pub(crate) fn parse_template_date(template: &Value, key: &str, default: &str) -> Result<NaiveDate, String> {
    let value = template
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .trim();
    parse_oos_yyyymmdd(value, key)
}


pub(crate) fn parse_oos_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d").map_err(|_| format!("{} must be YYYYMMDD", field))
}


pub(crate) fn format_oos_yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}


pub(crate) fn backtest_template_for_window(
    mut template: Value,
    start_date: NaiveDate,
    end_date: NaiveDate,
    persistence_mode: &str,
) -> Result<Value, String> {
    if start_date > end_date {
        return Err("OOS window start_date must be <= end_date".into());
    }
    let object = template
        .as_object_mut()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    object.insert(
        "start_date".to_string(),
        json!(format_oos_yyyymmdd(start_date)),
    );
    object.insert("end_date".to_string(), json!(format_oos_yyyymmdd(end_date)));
    object
        .entry("mode".to_string())
        .or_insert_with(|| json!("standard"));
    object.insert(
        "persistence_mode".to_string(),
        json!(persistence_mode.to_string()),
    );
    object
        .entry("effective_coverage".to_string())
        .or_insert_with(|| json!({"enabled": true, "mode": "adjust_start"}));
    Ok(template)
}


pub(crate) async fn load_oos_training_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    limit: usize,
) -> Result<Vec<DiscoveryCandidate>, String> {
    let targets = CandidateTargets::default();
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<Decimal>,
            Option<Value>,
            Option<String>,
            Value,
        ),
    >(
        "SELECT trial_id, score, metrics, backtest_task_id, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed' AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load OOS training candidates: {}", error))?;

    let mut candidates = rows
        .into_iter()
        .filter_map(|(trial_id, score, metrics, backtest_task_id, parameters)| {
            let metrics_json = metrics?;
            let candidate_metrics = CandidateMetrics::from_optimization_metrics(&metrics_json);
            let candidate_type = targets.classify(&candidate_metrics);
            let professional_gap_score =
                professional_candidate_gap_score(&candidate_metrics, &targets);
            Some(DiscoveryCandidate {
                trial_id,
                backtest_task_id,
                score,
                candidate_type,
                professional_gap_score,
                metrics: candidate_metrics,
                parameters,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(discovery_candidate_order);
    candidates.truncate(limit.max(1));
    Ok(candidates)
}


pub(crate) async fn select_oos_training_candidate(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_template: &Value,
    task_id: &str,
    candidates: Vec<DiscoveryCandidate>,
    gate_policy: &Value,
    require_train_approval: bool,
    window_index: usize,
    train_signal_cache: &mut SignalDataCache,
    train_backtest_cache: &mut BacktestDataCache,
) -> Result<
    (
        DiscoveryCandidate,
        Option<Value>,
        Vec<OosCostCapacityPerturbationResult>,
    ),
    String,
> {
    if candidates.is_empty() {
        return Err(format!(
            "window {} has no completed training candidates",
            window_index
        ));
    }

    let mut first_rejected: Option<(DiscoveryCandidate, Value)> = None;
    let train_cost_gate = train_cost_capacity_perturbation_gate_config(req, gate_policy);
    let stress_aware_selection =
        train_cost_capacity_stress_aware_selection_enabled(gate_policy, &train_cost_gate);
    let mut evaluated_candidates = Vec::new();
    for candidate in candidates {
        let robustness = evaluate_and_persist_robustness_for_trial(
            db,
            task_id,
            &candidate.trial_id,
            Some(gate_policy),
            "OOS walk-forward train-window robustness gate evaluated",
        )
        .await?;
        if !require_train_approval || robustness_result_is_approved(&robustness) {
            let train_cost_capacity_perturbations = execute_train_cost_capacity_perturbations(
                db,
                req,
                train_template,
                &candidate.parameters,
                gate_policy,
                train_signal_cache,
                train_backtest_cache,
            )
            .await?;
            let (passed_count, total_count) =
                cost_capacity_perturbation_pass_counts(&train_cost_capacity_perturbations);
            let stress_summary =
                cost_capacity_perturbation_summary(&train_cost_capacity_perturbations);
            let (robustness, train_cost_gate_passed) =
                attach_train_cost_capacity_gate_to_robustness(
                    robustness,
                    passed_count,
                    total_count,
                    &train_cost_gate,
                );
            let (robustness, fill_gap_gate_passed) =
                attach_train_cash_drag_fill_gap_gates_to_robustness(
                    robustness,
                    &candidate,
                    &stress_summary,
                    gate_policy,
                );
            let (robustness, stress_return_gate_passed) =
                attach_train_capacity_stress_return_gates_to_robustness(
                    robustness,
                    &candidate,
                    &stress_summary,
                    gate_policy,
                );
            let train_cost_gate_passed =
                train_cost_gate_passed && fill_gap_gate_passed && stress_return_gate_passed;
            if train_cost_gate.enabled {
                persist_train_cost_capacity_robustness_overlay(db, &robustness).await?;
            }
            if !stress_aware_selection && train_cost_gate_passed {
                return Ok((
                    candidate,
                    Some(robustness),
                    train_cost_capacity_perturbations,
                ));
            }
            let stress_adjusted_score = train_candidate_stress_adjusted_score_for_policy(
                &candidate,
                &stress_summary,
                gate_policy,
            );
            evaluated_candidates.push(OosTrainCandidateEvaluation {
                candidate: candidate.clone(),
                robustness: robustness.clone(),
                train_cost_capacity_perturbations,
                stress_summary,
                stress_adjusted_score,
                train_cost_gate_passed,
            });
            if first_rejected.is_none() {
                first_rejected = Some((candidate, robustness));
            }
            continue;
        }
        if first_rejected.is_none() {
            first_rejected = Some((candidate, robustness));
        }
    }

    if stress_aware_selection {
        evaluated_candidates.sort_by(train_candidate_evaluation_order);
        if let Some(index) = evaluated_candidates
            .iter()
            .position(|evaluation| evaluation.train_cost_gate_passed)
        {
            let evaluation = evaluated_candidates.remove(index);
            return Ok((
                evaluation.candidate,
                Some(evaluation.robustness),
                evaluation.train_cost_capacity_perturbations,
            ));
        }
        if best_effort_train_selection_for_diagnostics_enabled(gate_policy)
            && !evaluated_candidates.is_empty()
        {
            let evaluation = evaluated_candidates.remove(0);
            return Ok((
                evaluation.candidate,
                Some(evaluation.robustness),
                evaluation.train_cost_capacity_perturbations,
            ));
        }
        if let Some(evaluation) = evaluated_candidates.first() {
            return Err(format!(
                "window {} has no training candidate passing robustness; best stress-aware rejected trial {} status {} train cost/capacity pass ratio {}/{} stress_adjusted_score {}",
                window_index,
                evaluation.candidate.trial_id,
                evaluation
                    .robustness
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                evaluation.stress_summary.passed_count,
                evaluation.stress_summary.total_count,
                evaluation.stress_adjusted_score,
            ));
        }
    }

    if let Some((candidate, robustness)) = first_rejected {
        if best_effort_train_selection_for_diagnostics_enabled(gate_policy) {
            return Ok((candidate, Some(robustness), Vec::new()));
        }
        return Err(format!(
            "window {} has no training candidate passing robustness; best rejected trial {} status {}",
            window_index,
            candidate.trial_id,
            robustness
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ));
    }

    Err(format!(
        "window {} has no training candidate passing robustness",
        window_index
    ))
}


pub(crate) async fn load_oos_equity_points(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
) -> Result<Vec<RobustnessDailyPoint>, String> {
    sqlx::query_as::<_, (NaiveDate, f64, Option<f64>)>(
        "SELECT trade_date,
                portfolio_value::double precision,
                benchmark_value::double precision
         FROM backtest_equity_curve
         WHERE task_id = $1
         ORDER BY trade_date",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load OOS equity curve: {}", error))
    .map(|rows| {
        rows.into_iter()
            .map(
                |(trade_date, portfolio_value, benchmark_value)| RobustnessDailyPoint {
                    trade_date,
                    portfolio_value,
                    benchmark_value,
                },
            )
            .collect()
    })
}


pub(crate) fn append_stitched_oos_points(
    stitched: &mut Vec<RobustnessDailyPoint>,
    window_points: &[RobustnessDailyPoint],
) {
    if window_points.len() < 2 {
        return;
    }
    if stitched.is_empty() {
        stitched.push(RobustnessDailyPoint {
            trade_date: window_points[0].trade_date,
            portfolio_value: 1.0,
            benchmark_value: Some(1.0),
        });
    }
    let mut portfolio_nav = stitched
        .last()
        .map(|point| point.portfolio_value)
        .unwrap_or(1.0);
    let mut benchmark_nav = stitched
        .last()
        .and_then(|point| point.benchmark_value)
        .unwrap_or(1.0);
    let mut last_date = stitched.last().map(|point| point.trade_date);
    for pair in window_points.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        if last_date
            .map(|date| next.trade_date <= date)
            .unwrap_or(false)
        {
            continue;
        }
        if prev.portfolio_value > 0.0 && next.portfolio_value.is_finite() {
            portfolio_nav *= next.portfolio_value / prev.portfolio_value;
        }
        let benchmark_value = match (prev.benchmark_value, next.benchmark_value) {
            (Some(prev_benchmark), Some(next_benchmark))
                if prev_benchmark > 0.0 && next_benchmark.is_finite() =>
            {
                benchmark_nav *= next_benchmark / prev_benchmark;
                Some(benchmark_nav)
            }
            _ => None,
        };
        stitched.push(RobustnessDailyPoint {
            trade_date: next.trade_date,
            portfolio_value: portfolio_nav,
            benchmark_value,
        });
        last_date = Some(next.trade_date);
    }
}


pub(crate) fn build_oos_gate_report(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &OosDiscoveryPlan,
    window_results: &[Value],
    stitched_summary: &Value,
    final_promotion_gate_policy: &Value,
) -> Value {
    let min_calmar =
        constraint_f64(Some(final_promotion_gate_policy), "min_stitched_oos_calmar").unwrap_or(1.2);
    let min_positive_ratio = constraint_f64(
        Some(final_promotion_gate_policy),
        "min_positive_oos_window_ratio",
    )
    .unwrap_or(0.60);
    let min_window_count =
        constraint_i64(Some(final_promotion_gate_policy), "min_oos_window_count")
            .map(|value| value.max(0) as usize)
            .unwrap_or_else(|| {
                if plan.validation_mode == "holdout_80_20" {
                    1
                } else {
                    3
                }
            });
    let require_train_selection = final_promotion_gate_policy
        .get("require_train_selection_approval")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let require_no_overlap = final_promotion_gate_policy
        .get("require_no_train_test_overlap")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let positive_windows = window_results
        .iter()
        .filter(|window| {
            value_as_f64(&window["oos_metrics"]["annual_return_pct"])
                .map(|value| value > 0.0)
                .unwrap_or(false)
        })
        .count();
    let positive_ratio = ratio(positive_windows, window_results.len());
    let stitched_calmar = stitched_summary["calmar_ratio"].as_f64().unwrap_or(0.0);
    let stitched_annual = stitched_summary["annual_return"].as_f64().unwrap_or(0.0);
    let stitched_excess = stitched_summary["excess_return"]
        .as_f64()
        .or_else(|| stitched_summary["excess_return_pct"].as_f64())
        .unwrap_or(0.0);
    let min_stitched_annual = constraint_f64(
        Some(final_promotion_gate_policy),
        "min_stitched_annual_return",
    )
    .unwrap_or(0.15);
    let min_stitched_excess = constraint_f64(
        Some(final_promotion_gate_policy),
        "min_stitched_excess_return",
    )
    .unwrap_or(0.0);
    let no_overlap = plan
        .windows
        .iter()
        .all(|window| window.train_end < window.test_start);
    let train_robustness_passed = window_results.iter().all(|window| {
        window["train_robustness"]["status"]
            .as_str()
            .map(|status| status == "approved_candidate")
            .unwrap_or(false)
    });
    let mut gates = vec![
        {
            json!({
                "gate": "oos_window_count",
                "passed": window_results.len() >= min_window_count,
                "limit": min_window_count,
                "actual": window_results.len(),
            })
        },
        {
            json!({
                "gate": "no_train_test_overlap",
                "passed": !require_no_overlap || no_overlap,
                "actual": no_overlap,
                "required": require_no_overlap,
            })
        },
        {
            json!({
                "gate": "train_window_robustness_approved",
                "passed": !require_train_selection || train_robustness_passed,
                "actual": train_robustness_passed,
                "required": require_train_selection,
            })
        },
        {
            json!({
                "gate": "stitched_oos_calmar",
                "passed": stitched_calmar > min_calmar,
                "limit": min_calmar,
                "actual": stitched_calmar,
            })
        },
        {
            json!({
                "gate": "positive_oos_window_ratio",
                "passed": positive_ratio >= min_positive_ratio,
                "limit": min_positive_ratio,
                "actual": positive_ratio,
            })
        },
        {
            json!({
                "gate": "stitched_annual_return",
                "passed": stitched_annual >= min_stitched_annual,
                "limit": min_stitched_annual,
                "actual": stitched_annual,
            })
        },
        {
            json!({
                "gate": "stitched_excess_return",
                "passed": stitched_excess > min_stitched_excess,
                "limit": min_stitched_excess,
                "actual": stitched_excess,
            })
        },
    ];
    if cost_capacity_perturbation_gate_enabled(req) {
        let perturbation_results = window_results
            .iter()
            .filter_map(|window| window["cost_capacity_perturbations"].as_array())
            .flat_map(|items| items.iter())
            .collect::<Vec<_>>();
        let perturbation_count = perturbation_results.len();
        let passed_count = perturbation_results
            .iter()
            .filter(|result| result["passed"].as_bool().unwrap_or(false))
            .count();
        let pass_ratio = ratio(passed_count, perturbation_count);
        let min_pass_ratio = req
            .min_cost_capacity_perturbation_pass_ratio
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "min_cost_capacity_perturbation_pass_ratio",
                )
            })
            .unwrap_or(0.80)
            .clamp(0.0, 1.0);
        let min_perturbed_oos_calmar = req
            .min_perturbed_oos_calmar
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "min_perturbed_oos_calmar",
                )
            })
            .unwrap_or(1.2);
        let max_perturbed_oos_drawdown_pct = req
            .max_perturbed_oos_drawdown_pct
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "max_perturbed_oos_drawdown_pct",
                )
            })
            .unwrap_or(0.35);
        gates.push(json!({
            "gate": "cost_capacity_perturbation_pass_ratio",
            "passed": perturbation_count > 0 && pass_ratio >= min_pass_ratio,
            "limit": min_pass_ratio,
            "actual": pass_ratio,
            "passed_count": passed_count,
            "total_count": perturbation_count,
            "min_perturbed_oos_calmar": min_perturbed_oos_calmar,
            "max_perturbed_oos_drawdown_pct": max_perturbed_oos_drawdown_pct,
        }));
    }
    Value::Array(gates)
}


pub(crate) fn oos_gate_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item["passed"].as_bool().unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "approved_oos_candidate"
    } else {
        "rejected"
    }
}


pub(crate) fn oos_discovery_plan_json(plan: &OosDiscoveryPlan) -> Value {
    json!({
        "validation_mode": plan.validation_mode,
        "start_date": plan.start_date,
        "end_date": plan.end_date,
        "train_window_days": plan.train_window_days,
        "test_window_days": plan.test_window_days,
        "step_days": plan.step_days,
        "in_sample_ratio": plan.in_sample_ratio,
        "include_partial_last_window": plan.include_partial_last_window,
        "window_count": plan.windows.len(),
        "windows": plan.windows.iter().map(oos_window_json).collect::<Vec<_>>(),
        "point_in_time_contract": {
            "rule": "available_at <= signal_date/cutoff",
            "train_selection_scope": "train window only",
            "test_evaluation_scope": "selected train-window parameters only",
            "execution_timing": "next_open by default",
        },
    })
}


pub(crate) fn oos_window_json(window: &OosDiscoveryWindow) -> Value {
    json!({
        "window_index": window.window_index,
        "validation_mode": window.validation_mode,
        "train_start": window.train_start,
        "train_end": window.train_end,
        "test_start": window.test_start,
        "test_end": window.test_end,
    })
}


pub(crate) fn oos_window_execution_json(execution: &OosWindowExecution, train_gate_policy: &Value) -> Value {
    let train_cost_capacity_summary =
        cost_capacity_perturbation_summary(&execution.train_cost_capacity_perturbations);
    oos_window_execution_json_with_train_summary(
        execution,
        train_gate_policy,
        &train_cost_capacity_summary,
    )
}


pub(crate) fn oos_window_execution_json_with_train_summary(
    execution: &OosWindowExecution,
    train_gate_policy: &Value,
    train_cost_capacity_summary: &CostCapacityPerturbationSummary,
) -> Value {
    if let Some(reason) = &execution.skip_reason {
        return json!({
            "window": oos_window_json(&execution.window),
            "train_optimization_task_id": execution.train_optimization_task_id,
            "train_batches": execution.train_batches,
            "status": "skipped",
            "skip_reason": reason,
        });
    }
    let train_stress_score_profile = train_cost_capacity_stress_score_profile(train_gate_policy);
    let train_stress_adjusted_score = train_candidate_stress_adjusted_score_for_policy(
        &execution.selected_candidate,
        train_cost_capacity_summary,
        train_gate_policy,
    );
    let train_stress_score_breakdown = train_candidate_stress_score_breakdown(
        &execution.selected_candidate,
        train_cost_capacity_summary,
        train_gate_policy,
    );
    json!({
        "window": oos_window_json(&execution.window),
        "train_optimization_task_id": execution.train_optimization_task_id,
        "train_execution_policy": {
            "requested_cache_mode": execution.train_execution_policy.requested_cache_mode,
            "cache_mode": execution.train_execution_policy.cache_mode,
            "requested_trial_concurrency": execution.train_execution_policy.requested_trial_concurrency,
            "trial_concurrency": execution.train_execution_policy.trial_concurrency,
        },
        "train_batches": execution.train_batches,
        "selected_candidate": discovery_candidate_json(&execution.selected_candidate),
        "train_robustness": execution.train_robustness,
        "train_cost_capacity_perturbations": execution.train_cost_capacity_perturbations.iter().map(oos_cost_capacity_perturbation_result_json).collect::<Vec<_>>(),
        "train_cost_capacity_summary": cost_capacity_perturbation_summary_json(train_cost_capacity_summary),
        "train_stress_score_profile": train_stress_score_profile,
        "train_stress_adjusted_score": train_stress_adjusted_score,
        "train_stress_score_breakdown": train_stress_score_breakdown,
        "oos_backtest_task_id": execution.oos_backtest_task_id,
        "oos_market_data_prewarm_report": execution.oos_output.market_data_prewarm_report,
        "oos_market_feature_prewarm_report": execution.oos_output.market_feature_prewarm_report,
        "oos_metrics": {
            "annual_return_pct": execution.oos_output.metrics.annual_return_pct,
            "excess_return_pct": execution.oos_output.metrics.excess_return_pct,
            "sharpe_ratio": execution.oos_output.metrics.sharpe_ratio,
            "sortino_ratio": execution.oos_output.metrics.sortino_ratio,
            "calmar_ratio": execution.oos_output.metrics.calmar_ratio,
            "max_drawdown_pct": execution.oos_output.metrics.max_drawdown_pct,
            "max_drawdown_duration_days": execution.oos_output.metrics.max_drawdown_duration_days,
            "profit_factor": execution.oos_output.metrics.profit_factor,
            "num_trades": execution.oos_output.metrics.num_trades,
            "execution_schedule_expired_count": execution.oos_output.metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": execution.oos_output.metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": execution.oos_output.metrics.max_execution_target_gap_pct,
            "final_cash_weight_pct": execution.oos_output.metrics.final_cash_weight_pct,
            "final_target_gross_exposure_pct": execution.oos_output.metrics.final_target_gross_exposure_pct,
            "final_actual_gross_exposure_pct": execution.oos_output.metrics.final_actual_gross_exposure_pct,
            "final_unfilled_target_gap_pct": execution.oos_output.metrics.final_unfilled_target_gap_pct,
            "final_execution_fill_ratio": execution.oos_output.metrics.final_execution_fill_ratio,
        },
        "oos_point_count": execution.oos_points.len(),
        "cost_capacity_perturbations": execution.cost_capacity_perturbations.iter().map(oos_cost_capacity_perturbation_result_json).collect::<Vec<_>>(),
    })
}


pub(crate) fn annotate_oos_train_batch_cache_mode(
    mut batch: Value,
    train_execution_policy: &OosTrainExecutionPolicy,
) -> Value {
    let cache_scope = match train_execution_policy.cache_mode {
        "shared_run" => "run",
        "shared_window" => "window",
        "per_trial_isolated" => "trial",
        other => other,
    };
    if let Some(object) = batch.as_object_mut() {
        object.insert(
            "requested_cache_mode".to_string(),
            json!(train_execution_policy.requested_cache_mode),
        );
        object.insert(
            "cache_mode".to_string(),
            json!(train_execution_policy.cache_mode),
        );
        object.insert("cache_scope".to_string(), json!(cache_scope));
        object.insert(
            "requested_trial_concurrency".to_string(),
            json!(train_execution_policy.requested_trial_concurrency),
        );
        object.insert(
            "trial_concurrency".to_string(),
            json!(train_execution_policy.trial_concurrency),
        );
    }
    batch
}


pub(crate) fn cost_capacity_perturbation_summary_json(summary: &CostCapacityPerturbationSummary) -> Value {
    json!({
        "passed_count": summary.passed_count,
        "total_count": summary.total_count,
        "pass_ratio": ratio(summary.passed_count, summary.total_count),
        "min_calmar": summary.min_calmar,
        "avg_calmar": summary.avg_calmar,
        "max_drawdown_pct": summary.max_drawdown,
        "min_annual_return_pct": summary.min_annual_return,
        "avg_sharpe": summary.avg_sharpe,
        "min_sortino": summary.min_sortino,
        "max_final_cash_weight_pct": summary.max_final_cash_weight,
        "avg_final_cash_weight_pct": summary.avg_final_cash_weight,
        "max_final_unfilled_target_gap_pct": summary.max_final_unfilled_target_gap,
        "avg_final_unfilled_target_gap_pct": summary.avg_final_unfilled_target_gap,
        "min_final_execution_fill_ratio": summary.min_final_execution_fill_ratio,
        "avg_final_execution_fill_ratio": summary.avg_final_execution_fill_ratio,
        "max_execution_target_gap_pct": summary.max_execution_target_gap,
        "max_execution_schedule_expired_count": summary.max_execution_schedule_expired_count,
        "total_execution_schedule_expired_count": summary.total_execution_schedule_expired_count,
    })
}


pub(crate) fn oos_cost_capacity_perturbation_result_json(result: &OosCostCapacityPerturbationResult) -> Value {
    json!({
        "name": result.name,
        "perturbation": result.perturbation,
        "backtest_task_id": result.backtest_task_id,
        "passed": result.passed,
        "metrics": {
            "annual_return_pct": result.output.metrics.annual_return_pct,
            "excess_return_pct": result.output.metrics.excess_return_pct,
            "sharpe_ratio": result.output.metrics.sharpe_ratio,
            "sortino_ratio": result.output.metrics.sortino_ratio,
            "calmar_ratio": result.output.metrics.calmar_ratio,
            "max_drawdown_pct": result.output.metrics.max_drawdown_pct,
            "profit_factor": result.output.metrics.profit_factor,
            "num_trades": result.output.metrics.num_trades,
            "execution_schedule_expired_count": result.output.metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": result.output.metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": result.output.metrics.max_execution_target_gap_pct,
            "final_cash_weight_pct": result.output.metrics.final_cash_weight_pct,
            "final_target_gross_exposure_pct": result.output.metrics.final_target_gross_exposure_pct,
            "final_actual_gross_exposure_pct": result.output.metrics.final_actual_gross_exposure_pct,
            "final_unfilled_target_gap_pct": result.output.metrics.final_unfilled_target_gap_pct,
            "final_execution_fill_ratio": result.output.metrics.final_execution_fill_ratio,
        }
    })
}


pub(crate) async fn persist_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
    windows: &[Value],
    stitched_summary: &Value,
    gates: &Value,
    cache_report: &Value,
) -> Result<String, String> {
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = json!({
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
            "gates": gates,
            "status": oos_gate_status(gates),
        },
        "cache": cache_report,
    });
    create_experiment_run(
        db,
        "phase7_oos_walk_forward_discovery",
        "oos_discovery",
        None,
        &config,
        &metrics,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to insert OOS walk-forward experiment_run: {}", error))
}


pub(crate) async fn create_running_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
) -> Result<String, String> {
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = oos_walk_forward_progress_metrics(
        plan["window_count"].as_u64().unwrap_or(0) as usize,
        &[],
        &json!({}),
        &json!({}),
    );
    create_experiment_run(
        db,
        "phase7_oos_walk_forward_discovery",
        "oos_discovery",
        None,
        &config,
        &metrics,
        "running",
    )
    .await
    .map_err(|error| format!("Failed to insert running OOS walk-forward experiment_run: {}", error))
}


pub(crate) async fn update_oos_walk_forward_experiment_progress(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    plan: &OosDiscoveryPlan,
    windows: &[Value],
    stitched_summary: &Value,
    cache_report: &Value,
) -> Result<(), String> {
    let metrics = oos_walk_forward_progress_metrics(
        plan.windows.len(),
        windows,
        stitched_summary,
        cache_report,
    );
    sqlx::query(
        "UPDATE experiment_run
         SET metrics = $2,
             status = 'running'
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to update OOS walk-forward experiment progress: {}",
            error
        )
    })?;
    Ok(())
}


pub(crate) async fn complete_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
    windows: &[Value],
    stitched_summary: &Value,
    gates: &Value,
    cache_report: &Value,
) -> Result<(), String> {
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = json!({
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
            "gates": gates,
            "status": oos_gate_status(gates),
        },
        "cache": cache_report,
    });
    sqlx::query(
        "UPDATE experiment_run
         SET config = $2,
             metrics = $3,
             status = 'completed',
             completed_at = now()
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to complete OOS walk-forward experiment_run: {}",
            error
        )
    })?;
    Ok(())
}


pub(crate) async fn mark_oos_walk_forward_experiment_failed(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    message: &str,
) -> Result<(), String> {
    let metrics = json!({
        "status": "failed",
        "error_message": message,
    });
    sqlx::query(
        "UPDATE experiment_run
         SET metrics = coalesce(metrics, '{}'::jsonb) || $2::jsonb,
             status = 'failed',
             completed_at = now()
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to mark OOS walk-forward experiment_run failed: {}",
            error
        )
    })?;
    Ok(())
}


pub(crate) fn oos_walk_forward_experiment_config(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
) -> Value {
    let train_selection_gate_policy = resolve_oos_train_selection_gate_policy(req);
    let train_cost_gate =
        train_cost_capacity_perturbation_gate_config(req, &train_selection_gate_policy);
    let train_execution_policy =
        resolve_oos_train_execution_policy(req, &LocalResourcePlan::local_mac()).unwrap_or(
            OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
        );
    let train_stress_score_profile =
        train_cost_capacity_stress_score_profile(&train_selection_gate_policy);
    json!({
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "search_profile": req.search_profile,
        "max_trials_per_window": req.max_trials_per_window,
        "exhaustive_search": req.exhaustive_search.unwrap_or(false),
        "trial_batch_limit": req.trial_batch_limit,
        "requested_trial_concurrency": train_execution_policy.requested_trial_concurrency,
        "trial_concurrency": train_execution_policy.trial_concurrency,
        "requested_train_cache_mode": train_execution_policy.requested_cache_mode,
        "train_trial_concurrency": train_execution_policy.trial_concurrency,
        "train_cache_mode": train_execution_policy.cache_mode,
        "return_risk_feature_cache_mode": request_return_risk_feature_cache_mode(req),
        "max_batches_per_window": req.max_batches_per_window,
        "require_train_robustness_approval": req.require_train_robustness_approval.unwrap_or(true),
        "train_selection_gate_policy": train_selection_gate_policy,
        "train_cost_capacity_perturbation_gate": {
            "enabled": train_cost_gate.enabled,
            "stress_aware_selection_enabled": train_cost_capacity_stress_aware_selection_enabled(
                &train_selection_gate_policy,
                &train_cost_gate
            ),
            "stress_aware_selection_score": train_stress_score_profile,
            "perturbations": resolved_train_cost_capacity_perturbations(
                req,
                &train_selection_gate_policy
            ),
            "min_pass_ratio": train_cost_gate.min_pass_ratio,
            "min_perturbed_oos_calmar": train_cost_gate.min_perturbed_calmar,
            "max_perturbed_oos_drawdown_pct": train_cost_gate.max_perturbed_drawdown_pct,
        },
        "legacy_train_robustness_gate_policy": req.train_robustness_gate_policy,
        "final_promotion_gate_policy": resolve_oos_final_promotion_gate_policy(
            req,
            plan["validation_mode"].as_str().unwrap_or("walk_forward")
        ),
        "min_oos_window_count": req.min_oos_window_count,
        "min_stitched_oos_calmar": req.min_stitched_oos_calmar.unwrap_or(1.2),
        "min_positive_oos_window_ratio": req.min_positive_oos_window_ratio.unwrap_or(0.60),
        "oos_top_n": req.oos_top_n.unwrap_or(1),
        "cost_capacity_perturbation_gate": {
            "enabled": cost_capacity_perturbation_gate_enabled(req),
            "perturbations": resolved_oos_cost_capacity_perturbations(req),
            "min_pass_ratio": req.min_cost_capacity_perturbation_pass_ratio.unwrap_or(0.80),
            "min_perturbed_oos_calmar": req.min_perturbed_oos_calmar.unwrap_or(1.2),
            "max_perturbed_oos_drawdown_pct": req.max_perturbed_oos_drawdown_pct.unwrap_or(0.35),
        },
        "execution_mode": normalize_oos_execution_mode(req.execution_mode.as_deref()).unwrap_or("inline"),
        "resource_plan": LocalResourcePlan::local_mac(),
        "plan": plan,
    })
}


pub(crate) fn oos_walk_forward_progress_metrics(
    total_windows: usize,
    windows: &[Value],
    stitched_summary: &Value,
    cache_report: &Value,
) -> Value {
    let completed_windows = windows.len();
    let progress_pct = if total_windows == 0 {
        0
    } else {
        ((completed_windows * 100) / total_windows).min(100)
    };
    json!({
        "status": "running",
        "completed_windows": completed_windows,
        "total_windows": total_windows,
        "progress_pct": progress_pct,
        "latest_window": windows.last(),
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
        },
        "cache": cache_report,
    })
}


pub(crate) fn oos_walk_forward_stage(stage: &str, window_index: Option<usize>, details: Value) -> Value {
    json!({
        "stage": stage,
        "window_index": window_index,
        "details": details,
    })
}


pub(crate) fn append_oos_walk_forward_stage_metrics(mut metrics: Value, stage: Value) -> Value {
    if !metrics.is_object() {
        metrics = json!({});
    }
    metrics["current_stage"] = stage.clone();

    let mut stage_history = metrics
        .get("stage_history")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    stage_history.push(stage);
    if stage_history.len() > 50 {
        let keep_from = stage_history.len() - 50;
        stage_history = stage_history.split_off(keep_from);
    }
    metrics["stage_history"] = json!(stage_history);
    metrics
}


pub(crate) async fn update_oos_walk_forward_experiment_stage(
    db: &sqlx::PgPool,
    experiment_run_id: Option<&str>,
    stage: Value,
) -> Result<(), String> {
    let Some(experiment_run_id) = experiment_run_id else {
        return Ok(());
    };
    let metrics = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT metrics FROM experiment_run WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .fetch_optional(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load OOS walk-forward experiment metrics: {}",
            error
        )
    })?
    .flatten()
    .unwrap_or_else(|| json!({}));
    let metrics = append_oos_walk_forward_stage_metrics(metrics, stage);
    sqlx::query(
        "UPDATE experiment_run
         SET metrics = $2,
             status = 'running'
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to update OOS walk-forward experiment stage: {}",
            error
        )
    })?;
    Ok(())
}


pub(crate) fn oos_cache_report(signal_cache: &SignalDataCache, backtest_cache: &BacktestDataCache) -> Value {
    json!({
        "oos_signal_cache": signal_cache_stats_delta(
            SignalDataCacheStats::default(),
            signal_cache.stats()
        ),
        "oos_backtest_cache": backtest_cache_stats_delta(
            BacktestDataCacheStats::default(),
            backtest_cache.stats()
        ),
    })
}


pub(crate) async fn build_return_risk_cache_economics_report(
    db: &sqlx::PgPool,
    req: &ReturnRiskCacheEconomicsReportRequest,
) -> Result<Value, String> {
    let raw_run = load_experiment_run_metrics(db, &req.raw_experiment_run_id).await?;
    let stats_run = load_experiment_run_metrics(db, &req.stats_experiment_run_id).await?;
    ensure_completed_cache_economics_input(&req.raw_experiment_run_id, "raw", &raw_run.status)?;
    ensure_completed_cache_economics_input(
        &req.stats_experiment_run_id,
        "stats",
        &stats_run.status,
    )?;
    let raw_signal_cache = aggregate_signal_cache_stats_from_experiment_metrics(&raw_run.metrics);
    let stats_signal_cache =
        aggregate_signal_cache_stats_from_experiment_metrics(&stats_run.metrics);
    let report = return_risk_cache_economics_report_json(
        &req.raw_experiment_run_id,
        raw_signal_cache,
        &req.stats_experiment_run_id,
        stats_signal_cache,
    );
    let report_experiment_run_id = persist_return_risk_cache_economics_report(
        db,
        &req.raw_experiment_run_id,
        &req.stats_experiment_run_id,
        &report,
    )
    .await?;

    Ok(json!({
        "experiment_run_id": report_experiment_run_id,
        "report": report,
    }))
}


#[derive(Debug, Clone)]
pub(crate) struct ExperimentRunMetrics {
    metrics: Value,
    status: String,
}


pub(crate) async fn load_experiment_run_metrics(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
) -> Result<ExperimentRunMetrics, String> {
    let row = sqlx::query_as::<_, (Option<Value>, String)>(
        "SELECT metrics, status
         FROM experiment_run
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load experiment_run metrics: {}", error))?
    .ok_or_else(|| format!("experiment_run not found: {}", experiment_run_id))?;
    Ok(ExperimentRunMetrics {
        metrics: row.0.unwrap_or_else(|| json!({})),
        status: row.1,
    })
}


pub(crate) fn ensure_completed_cache_economics_input(
    experiment_run_id: &str,
    role: &str,
    status: &str,
) -> Result<(), String> {
    if status == "completed" {
        return Ok(());
    }
    Err(format!(
        "return-risk cache economics report requires completed {} experiment {}; status={}",
        role, experiment_run_id, status
    ))
}


pub(crate) async fn persist_return_risk_cache_economics_report(
    db: &sqlx::PgPool,
    raw_experiment_run_id: &str,
    stats_experiment_run_id: &str,
    report: &Value,
) -> Result<String, String> {
    let config = json!({
        "raw_experiment_run_id": raw_experiment_run_id,
        "stats_experiment_run_id": stats_experiment_run_id,
        "comparison": "return_risk_cache_economics",
    });
    create_experiment_run(
        db,
        "return_risk_cache_economics_report",
        "experiment_pair",
        Some(raw_experiment_run_id),
        &config,
        report,
        "completed",
    )
    .await
    .map_err(|error| format!("Failed to insert return/risk cache economics experiment_run: {}", error))
}


pub(crate) fn return_risk_cache_economics_report_json(
    raw_experiment_run_id: &str,
    raw_signal_cache: SignalDataCacheStats,
    stats_experiment_run_id: &str,
    stats_signal_cache: SignalDataCacheStats,
) -> Value {
    let economics = compare_return_risk_cache_economics(raw_signal_cache, stats_signal_cache);
    json!({
        "raw_experiment_run_id": raw_experiment_run_id,
        "stats_experiment_run_id": stats_experiment_run_id,
        "raw_signal_cache": raw_signal_cache,
        "stats_signal_cache": stats_signal_cache,
        "cache_economics": economics,
    })
}


pub(crate) fn aggregate_signal_cache_stats_from_experiment_metrics(metrics: &Value) -> SignalDataCacheStats {
    let mut total = SignalDataCacheStats::default();
    add_signal_cache_stats_from_value(&mut total, metrics.get("signal_cache"));
    let has_oos_signal_cache = add_signal_cache_stats_from_value(
        &mut total,
        metrics
            .get("cache")
            .and_then(|cache| cache.get("oos_signal_cache")),
    );

    if let Some(windows) = metrics.get("windows").and_then(Value::as_array) {
        for window in windows {
            if let Some(train_batches) = window.get("train_batches").and_then(Value::as_array) {
                for batch in train_batches {
                    let has_batch_signal_cache =
                        add_signal_cache_stats_from_value(&mut total, batch.get("signal_cache"));
                    if !has_batch_signal_cache {
                        add_signal_cache_stats_from_value(
                            &mut total,
                            batch.pointer("/signal_batch_prewarm/report/cache_delta"),
                        );
                    }
                }
            }
            if !has_oos_signal_cache {
                add_signal_cache_stats_from_value(
                    &mut total,
                    window.pointer("/oos_market_feature_prewarm_report/cache_delta"),
                );
            }
        }
    }

    total
}


pub(crate) fn add_signal_cache_stats_from_value(
    total: &mut SignalDataCacheStats,
    value: Option<&Value>,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    match serde_json::from_value::<SignalDataCacheStats>(value.clone()) {
        Ok(stats) => {
            add_signal_cache_stats(total, stats);
            true
        }
        Err(_) => false,
    }
}


