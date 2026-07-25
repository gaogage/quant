//! Optimization API routes — create optimization tasks and parameter trials
//!
//! DDD Step 3b: split into a module tree under `routes/optimization/`.

mod diagnostics;
mod experiment_run;
mod param_search;
mod profile_registry;
mod robustness;
mod wfa_engine;

pub use diagnostics::{
    get_sleeve_admission_diagnostics, report_alpha_source_diagnostics,
    report_feature_profile_readiness, report_main_business_diagnostics,
};
pub use experiment_run::{
    cleanup_stale_experiment_runs, cleanup_stale_optimization_tasks,
    create_optimization, generate_elite_validation_report, get_experiment_run,
    get_optimization, list_optimization_trials, promote_optimization_trial,
    run_phase7_professional_discovery,
};
pub use param_search::run_optimization_trials;
pub use robustness::{evaluate_optimization_robustness, evaluate_optimization_trial_robustness};
pub use wfa_engine::{
    create_phase7_layered_optimization, launch_phase7_oos_profile_comparison_smoke,
    plan_phase7_oos_profile_comparison, run_phase7_oos_walk_forward_discovery,
};


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


// Re-export submodule types so intra-crate references via `crate::routes::optimization::Foo`
// continue to resolve.
pub use diagnostics::*;
pub use experiment_run::*;
pub use param_search::*;
pub use profile_registry::*;
pub use robustness::*;
pub use wfa_engine::*;

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


#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OosWalkForwardDiscoveryRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Option<Value>,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials_per_window: Option<usize>,
    pub search_profile: Option<String>,
    pub trial_batch_limit: Option<i64>,
    pub trial_concurrency: Option<usize>,
    pub train_cache_mode: Option<String>,
    pub max_batches_per_window: Option<usize>,
    pub train_window_days: Option<i64>,
    pub test_window_days: Option<i64>,
    pub step_days: Option<i64>,
    pub validation_mode: Option<String>,
    pub in_sample_ratio: Option<f64>,
    pub include_partial_last_window: Option<bool>,
    pub plan_only: Option<bool>,
    pub execution_mode: Option<String>,
    pub exhaustive_search: Option<bool>,
    pub require_train_robustness_approval: Option<bool>,
    pub train_robustness_gate_policy: Option<Value>,
    pub train_selection_gate_policy: Option<Value>,
    pub final_promotion_gate_policy: Option<Value>,
    pub min_stitched_oos_calmar: Option<f64>,
    pub min_positive_oos_window_ratio: Option<f64>,
    pub min_oos_window_count: Option<usize>,
    pub oos_top_n: Option<usize>,
    pub enable_cost_capacity_perturbation_gate: Option<bool>,
    pub cost_capacity_perturbations: Option<Vec<OosCostCapacityPerturbationRequest>>,
    pub min_cost_capacity_perturbation_pass_ratio: Option<f64>,
    pub min_perturbed_oos_calmar: Option<f64>,
    pub max_perturbed_oos_drawdown_pct: Option<f64>,
}


#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OosProfileComparisonPlanRequest {
    pub base: Phase7OosWalkForwardDiscoveryRequest,
    pub profiles: Option<Vec<String>>,
    pub return_risk_cache_comparison: Option<bool>,
}


#[derive(Debug, Clone, Deserialize)]
pub struct CleanupStaleBackgroundTasksReq {
    pub dry_run: Option<bool>,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
}


#[derive(Debug, Deserialize)]
pub struct FeatureProfileReadinessRequest {
    pub feature_profile: String,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
}


#[derive(Debug, Deserialize)]
pub struct AlphaSourceDiagnosticsRequest {
    pub combo_name: String,
    pub version: Option<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub alpha_admission_gate_id: Option<String>,
    #[serde(default)]
    pub universe_profile: Option<String>,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
    #[serde(default)]
    pub include_research_metrics: Option<bool>,
    #[serde(default)]
    pub return_horizons: Option<Vec<i64>>,
    #[serde(default)]
    pub bucket_count: Option<i64>,
    #[serde(default)]
    pub max_rank_ic_days: Option<i64>,
    #[serde(default)]
    pub include_exposure_regime_metrics: Option<bool>,
    #[serde(default)]
    pub max_exposure_regime_days: Option<i64>,
}


#[derive(Debug, Deserialize)]
pub struct MainBusinessDiagnosticsRequest {
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub business_type: Option<String>,
    #[serde(default)]
    pub universe_profile: Option<String>,
    #[serde(default)]
    pub profiles: Option<Vec<String>>,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
    #[serde(default)]
    pub return_horizons: Option<Vec<i64>>,
    #[serde(default)]
    pub bucket_count: Option<i64>,
    #[serde(default)]
    pub max_rank_ic_days: Option<i64>,
    #[serde(default)]
    pub include_exposure_regime_metrics: Option<bool>,
    #[serde(default)]
    pub max_exposure_regime_days: Option<i64>,
}


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OosCostCapacityPerturbationRequest {
    pub name: Option<String>,
    pub cost_multiplier: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub impact_cost_coefficient: Option<f64>,
    pub max_participation_rate: Option<f64>,
    pub capacity_penalty_strength: Option<f64>,
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


#[derive(Debug, Deserialize)]
pub struct EliteValidationReportRequest {
    pub top_n: Option<usize>,
    pub gate_policy: Option<Value>,
}


#[derive(Debug, Deserialize)]
pub struct ReturnRiskCacheEconomicsReportRequest {
    pub raw_experiment_run_id: String,
    pub stats_experiment_run_id: String,
}


pub(crate) fn default_random_seed() -> u64 {
    42
}


pub(crate) fn default_max_trials() -> usize {
    20
}


pub(crate) fn normalize_max_trials(value: usize) -> usize {
    value.clamp(1, 500)
}


pub(crate) fn normalize_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 500)
}


pub(crate) fn phase7_search_config(search_profile: Option<&str>) -> (String, LayeredSearchConfig) {
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
        "professional_sharpe_stabilization"
        | "sharpe_stabilization"
        | "phase7_sharpe_stabilization"
        | "phase7_t" => (
            "professional_sharpe_stabilization".to_string(),
            LayeredSearchConfig::professional_sharpe_stabilization_default(),
        ),
        "professional_v19_current_baseline"
        | "v19_current_baseline"
        | "v19_current"
        | "phase7_v19_current_baseline"
        | "phase7_v19_current" => (
            "professional_v19_current_baseline".to_string(),
            LayeredSearchConfig::professional_v19_current_baseline_default(),
        ),
        "professional_regime_stabilization"
        | "regime_stabilization"
        | "phase7_regime_stabilization"
        | "phase7_u" => (
            "professional_regime_stabilization".to_string(),
            LayeredSearchConfig::professional_regime_stabilization_default(),
        ),
        "professional_bear_window_stabilization"
        | "bear_window_stabilization"
        | "phase7_bear_window"
        | "phase7_u2" => (
            "professional_bear_window_stabilization".to_string(),
            LayeredSearchConfig::professional_bear_window_stabilization_default(),
        ),
        "professional_style_risk_budget"
        | "style_risk_budget"
        | "phase7_style_risk_budget"
        | "phase7_v" => (
            "professional_style_risk_budget".to_string(),
            LayeredSearchConfig::professional_style_risk_budget_default(),
        ),
        "professional_second_alpha_source"
        | "second_alpha_source"
        | "phase7_second_alpha_source"
        | "phase7_w" => (
            "professional_second_alpha_source".to_string(),
            LayeredSearchConfig::professional_second_alpha_source_default(),
        ),
        "professional_residual_quality"
        | "residual_quality"
        | "industry_residual_quality"
        | "phase7_residual_quality"
        | "phase7_ag" => (
            "professional_residual_quality".to_string(),
            LayeredSearchConfig::professional_residual_quality_default(),
        ),
        "professional_residual_overlay_sharpe"
        | "residual_overlay_sharpe"
        | "quality_residual_overlay"
        | "phase7_ah" => (
            "professional_residual_overlay_sharpe".to_string(),
            LayeredSearchConfig::professional_residual_overlay_sharpe_default(),
        ),
        "professional_conditioned_second_alpha"
        | "conditioned_second_alpha"
        | "quality_conditioned_second_alpha"
        | "phase7_ak" => (
            "professional_conditioned_second_alpha".to_string(),
            LayeredSearchConfig::professional_conditioned_second_alpha_default(),
        ),
        "professional_valuation_guard_sharpe"
        | "valuation_guard_sharpe"
        | "phase7_valuation_guard"
        | "phase7_al" => (
            "professional_valuation_guard_sharpe".to_string(),
            LayeredSearchConfig::professional_valuation_guard_sharpe_default(),
        ),
        "professional_regime_conditioned_valuation_guard"
        | "regime_conditioned_valuation_guard"
        | "phase7_regime_conditioned_valuation_guard"
        | "phase7_am" => (
            "professional_regime_conditioned_valuation_guard".to_string(),
            LayeredSearchConfig::professional_regime_conditioned_valuation_guard_default(),
        ),
        "professional_regime_alpha_routing"
        | "regime_alpha_routing"
        | "phase7_regime_alpha_routing"
        | "phase7_an" => (
            "professional_regime_alpha_routing".to_string(),
            LayeredSearchConfig::professional_regime_alpha_routing_default(),
        ),
        "professional_regime_alpha_sleeve_search"
        | "regime_alpha_sleeve_search"
        | "phase7_regime_alpha_sleeve_search"
        | "phase7_ao" => (
            "professional_regime_alpha_sleeve_search".to_string(),
            LayeredSearchConfig::professional_regime_alpha_sleeve_search_default(),
        ),
        "professional_regime_alpha_overlay_search"
        | "regime_alpha_overlay_search"
        | "phase7_regime_alpha_overlay_search"
        | "phase7_ap" => (
            "professional_regime_alpha_overlay_search".to_string(),
            LayeredSearchConfig::professional_regime_alpha_overlay_search_default(),
        ),
        "professional_regime_alpha_sleeve_allocation"
        | "regime_alpha_sleeve_allocation"
        | "phase7_regime_alpha_sleeve_allocation"
        | "phase7_aq" => (
            "professional_regime_alpha_sleeve_allocation".to_string(),
            LayeredSearchConfig::professional_regime_alpha_sleeve_allocation_default(),
        ),
        "professional_low_risk_sleeve"
        | "low_risk_sleeve"
        | "phase7_low_risk_sleeve"
        | "phase7_ar" => (
            "professional_low_risk_sleeve".to_string(),
            LayeredSearchConfig::professional_low_risk_sleeve_default(),
        ),
        "professional_value_guard_sleeve_composition"
        | "value_guard_sleeve_composition"
        | "phase7_value_guard_sleeve"
        | "phase7_as" => (
            "professional_value_guard_sleeve_composition".to_string(),
            LayeredSearchConfig::professional_value_guard_sleeve_composition_default(),
        ),
        "professional_nearest_candidate_risk_model"
        | "nearest_candidate_risk_model"
        | "phase7_nearest_risk_model"
        | "phase7_at" => (
            "professional_nearest_candidate_risk_model".to_string(),
            LayeredSearchConfig::professional_nearest_candidate_risk_model_default(),
        ),
        "professional_event_regime_sleeve"
        | "event_regime_sleeve"
        | "phase7_event_regime_sleeve"
        | "phase7_av" => (
            "professional_event_regime_sleeve".to_string(),
            LayeredSearchConfig::professional_event_regime_sleeve_default(),
        ),
        "professional_event_window_sleeve_weight"
        | "event_window_sleeve_weight"
        | "phase7_event_window_sleeve_weight"
        | "phase7_aw" => (
            "professional_event_window_sleeve_weight".to_string(),
            LayeredSearchConfig::professional_event_window_sleeve_weight_default(),
        ),
        "professional_event_window_sleeve_upper_bound"
        | "event_window_sleeve_upper_bound"
        | "phase7_event_window_sleeve_upper_bound"
        | "phase7_ax" => (
            "professional_event_window_sleeve_upper_bound".to_string(),
            LayeredSearchConfig::professional_event_window_sleeve_upper_bound_default(),
        ),
        "professional_event_window_regime_placement"
        | "event_window_regime_placement"
        | "phase7_event_window_regime_placement"
        | "phase7_ay" => (
            "professional_event_window_regime_placement".to_string(),
            LayeredSearchConfig::professional_event_window_regime_placement_default(),
        ),
        "professional_event_window_decay"
        | "event_window_decay"
        | "phase7_event_window_decay"
        | "phase7_az" => (
            "professional_event_window_decay".to_string(),
            LayeredSearchConfig::professional_event_window_decay_default(),
        ),
        "professional_event_quality_segment"
        | "event_quality_segment"
        | "phase7_event_quality_segment"
        | "phase7_bb" => (
            "professional_event_quality_segment".to_string(),
            LayeredSearchConfig::professional_event_quality_segment_default(),
        ),
        "professional_event_surprise_nonlinear"
        | "event_surprise_nonlinear"
        | "phase7_event_surprise_nonlinear"
        | "phase7_ba" => (
            "professional_event_surprise_nonlinear".to_string(),
            LayeredSearchConfig::professional_event_surprise_nonlinear_default(),
        ),
        "professional_event_strength_segment"
        | "event_strength_segment"
        | "event_min_score_segment"
        | "phase7_event_strength_segment"
        | "phase7_bc" => (
            "professional_event_strength_segment".to_string(),
            LayeredSearchConfig::professional_event_strength_segment_default(),
        ),
        "professional_event_strength_boost"
        | "event_strength_boost"
        | "event_min_score_boost"
        | "phase7_event_strength_boost"
        | "phase7_bd" => (
            "professional_event_strength_boost".to_string(),
            LayeredSearchConfig::professional_event_strength_boost_default(),
        ),
        "professional_legacy_alpha_revalidation"
        | "legacy_alpha_revalidation"
        | "phase7_legacy_alpha_revalidation"
        | "phase7_be" => (
            "professional_legacy_alpha_revalidation".to_string(),
            LayeredSearchConfig::professional_legacy_alpha_revalidation_default(),
        ),
        "professional_current_anchor_risk_shape"
        | "current_anchor_risk_shape"
        | "event_window_anchor_risk_shape"
        | "phase7_current_anchor_risk_shape"
        | "phase7_bf" => (
            "professional_current_anchor_risk_shape".to_string(),
            LayeredSearchConfig::professional_current_anchor_risk_shape_default(),
        ),
        "professional_current_anchor_position_frontier"
        | "current_anchor_position_frontier"
        | "event_window_anchor_position_frontier"
        | "phase7_current_anchor_position_frontier"
        | "phase7_bg" => (
            "professional_current_anchor_position_frontier".to_string(),
            LayeredSearchConfig::professional_current_anchor_position_frontier_default(),
        ),
        "professional_current_anchor_weak_window_repair"
        | "current_anchor_weak_window_repair"
        | "event_window_anchor_weak_window_repair"
        | "phase7_current_anchor_weak_window_repair"
        | "phase7_bh" => (
            "professional_current_anchor_weak_window_repair".to_string(),
            LayeredSearchConfig::professional_current_anchor_weak_window_repair_default(),
        ),
        "professional_current_anchor_sharpe_return_bridge"
        | "current_anchor_sharpe_return_bridge"
        | "sharpe_return_bridge"
        | "phase7_current_anchor_sharpe_return_bridge"
        | "phase7_bi" => (
            "professional_current_anchor_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_current_anchor_sharpe_return_bridge_default(),
        ),
        "professional_high_sharpe_return_recovery"
        | "high_sharpe_return_recovery"
        | "sharpe_return_recovery"
        | "phase7_high_sharpe_return_recovery"
        | "phase7_bn" => (
            "professional_high_sharpe_return_recovery".to_string(),
            LayeredSearchConfig::professional_high_sharpe_return_recovery_default(),
        ),
        "professional_risk_memory_bridge"
        | "risk_memory_bridge"
        | "risk_budget_memory_bridge"
        | "phase7_risk_memory_bridge"
        | "phase7_bo" => (
            "professional_risk_memory_bridge".to_string(),
            LayeredSearchConfig::professional_risk_memory_bridge_default(),
        ),
        "professional_state_return_sharpe_router"
        | "state_return_sharpe_router"
        | "return_sharpe_router"
        | "phase7_state_return_sharpe_router"
        | "phase7_bp" => (
            "professional_state_return_sharpe_router".to_string(),
            LayeredSearchConfig::professional_state_return_sharpe_router_default(),
        ),
        "professional_state_return_sharpe_frontier"
        | "state_return_sharpe_frontier"
        | "return_sharpe_frontier"
        | "phase7_state_return_sharpe_frontier"
        | "phase7_bq" => (
            "professional_state_return_sharpe_frontier".to_string(),
            LayeredSearchConfig::professional_state_return_sharpe_frontier_default(),
        ),
        "professional_position_sharpe_return_bridge"
        | "position_sharpe_return_bridge"
        | "sharpe_return_position_bridge"
        | "phase7_position_sharpe_return_bridge"
        | "phase7_br" => (
            "professional_position_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_position_sharpe_return_bridge_default(),
        ),
        "professional_moderate_position_sharpe_return_bridge"
        | "moderate_position_sharpe_return_bridge"
        | "moderate_sharpe_return_position_bridge"
        | "phase7_moderate_position_sharpe_return_bridge"
        | "phase7_bs" => (
            "professional_moderate_position_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_moderate_position_sharpe_return_bridge_default(),
        ),
        "professional_correlation_frontier_sharpe_return"
        | "correlation_frontier_sharpe_return"
        | "corr_frontier_sharpe_return"
        | "phase7_correlation_frontier_sharpe_return"
        | "phase7_bt" => (
            "professional_correlation_frontier_sharpe_return".to_string(),
            LayeredSearchConfig::professional_correlation_frontier_sharpe_return_default(),
        ),
        "professional_correlation_threshold_sharpe_return"
        | "correlation_threshold_sharpe_return"
        | "corr_threshold_sharpe_return"
        | "phase7_correlation_threshold_sharpe_return"
        | "phase7_bu" => (
            "professional_correlation_threshold_sharpe_return".to_string(),
            LayeredSearchConfig::professional_correlation_threshold_sharpe_return_default(),
        ),
        "professional_soft_risk_frontier_sharpe_return"
        | "soft_risk_frontier_sharpe_return"
        | "phase7_soft_risk_frontier_sharpe_return"
        | "phase7_bv" => (
            "professional_soft_risk_frontier_sharpe_return".to_string(),
            LayeredSearchConfig::professional_soft_risk_frontier_sharpe_return_default(),
        ),
        "professional_regime_alpha_selector"
        | "regime_alpha_selector"
        | "phase7_regime_alpha_selector"
        | "phase7_bw" => (
            "professional_regime_alpha_selector".to_string(),
            LayeredSearchConfig::professional_regime_alpha_selector_default(),
        ),
        "professional_regime_alpha_overlay_frontier"
        | "regime_alpha_overlay_frontier"
        | "phase7_regime_alpha_overlay_frontier"
        | "phase7_bx" => (
            "professional_regime_alpha_overlay_frontier".to_string(),
            LayeredSearchConfig::professional_regime_alpha_overlay_frontier_default(),
        ),
        "professional_mixed_state_event_alpha"
        | "mixed_state_event_alpha"
        | "phase7_mixed_state_event_alpha"
        | "phase7_by" => (
            "professional_mixed_state_event_alpha".to_string(),
            LayeredSearchConfig::professional_mixed_state_event_alpha_default(),
        ),
        "professional_mixed_state_risk_memory"
        | "mixed_state_risk_memory"
        | "phase7_mixed_state_risk_memory"
        | "phase7_bz" => (
            "professional_mixed_state_risk_memory".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_default(),
        ),
        "professional_mixed_state_risk_memory_frontier"
        | "mixed_state_risk_memory_frontier"
        | "phase7_mixed_state_risk_memory_frontier"
        | "phase7_ca" => (
            "professional_mixed_state_risk_memory_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_frontier_default(),
        ),
        "professional_mixed_state_risk_memory_fine_frontier"
        | "mixed_state_risk_memory_fine_frontier"
        | "phase7_mixed_state_risk_memory_fine_frontier"
        | "phase7_cb" => (
            "professional_mixed_state_risk_memory_fine_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_fine_frontier_default(),
        ),
        "professional_mixed_state_exposure_frontier"
        | "mixed_state_exposure_frontier"
        | "phase7_mixed_state_exposure_frontier"
        | "phase7_cc" => (
            "professional_mixed_state_exposure_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_exposure_frontier_default(),
        ),
        "professional_mixed_state_orthogonal_alpha"
        | "mixed_state_orthogonal_alpha"
        | "phase7_mixed_state_orthogonal_alpha"
        | "phase7_cd" => (
            "professional_mixed_state_orthogonal_alpha".to_string(),
            LayeredSearchConfig::professional_mixed_state_orthogonal_alpha_default(),
        ),
        "professional_candidate_filter_alpha_bridge"
        | "candidate_filter_alpha_bridge"
        | "phase7_candidate_filter_alpha_bridge"
        | "phase7_ce" => (
            "professional_candidate_filter_alpha_bridge".to_string(),
            LayeredSearchConfig::professional_candidate_filter_alpha_bridge_default(),
        ),
        "professional_soft_candidate_filter_alpha_bridge"
        | "soft_candidate_filter_alpha_bridge"
        | "phase7_soft_candidate_filter_alpha_bridge"
        | "phase7_cf" => (
            "professional_soft_candidate_filter_alpha_bridge".to_string(),
            LayeredSearchConfig::professional_soft_candidate_filter_alpha_bridge_default(),
        ),
        "professional_sharpe_bridge_frontier"
        | "sharpe_bridge_frontier"
        | "phase7_sharpe_bridge_frontier"
        | "phase7_cg" => (
            "professional_sharpe_bridge_frontier".to_string(),
            LayeredSearchConfig::professional_sharpe_bridge_frontier_default(),
        ),
        "professional_annual_sharpe_floor_bridge"
        | "annual_sharpe_floor_bridge"
        | "phase7_annual_sharpe_floor_bridge"
        | "phase7_ch" => (
            "professional_annual_sharpe_floor_bridge".to_string(),
            LayeredSearchConfig::professional_annual_sharpe_floor_bridge_default(),
        ),
        "professional_risk_memory_relaxed_frontier"
        | "risk_memory_relaxed_frontier"
        | "phase7_risk_memory_relaxed_frontier"
        | "phase7_ci" => (
            "professional_risk_memory_relaxed_frontier".to_string(),
            LayeredSearchConfig::professional_risk_memory_relaxed_frontier_default(),
        ),
        "professional_sharpe_floor_auto_discovery"
        | "sharpe_floor_auto_discovery"
        | "phase7_sharpe_floor_auto_discovery"
        | "phase7_ck" => (
            "professional_sharpe_floor_auto_discovery".to_string(),
            LayeredSearchConfig::professional_sharpe_floor_auto_discovery_default(),
        ),
        "professional_nonlinear_alpha_auto_discovery"
        | "nonlinear_alpha_auto_discovery"
        | "phase7_nonlinear_alpha_auto_discovery"
        | "phase7_cl" => (
            "professional_nonlinear_alpha_auto_discovery".to_string(),
            LayeredSearchConfig::professional_nonlinear_alpha_auto_discovery_default(),
        ),
        "professional_nonlinear_sharpe_return_bridge"
        | "nonlinear_sharpe_return_bridge"
        | "phase7_nonlinear_sharpe_return_bridge"
        | "phase7_cm" => (
            "professional_nonlinear_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_nonlinear_sharpe_return_bridge_default(),
        ),
        "professional_prediction_confirmed_sharpe_bridge"
        | "prediction_confirmed_sharpe_bridge"
        | "phase7_prediction_confirmed_sharpe_bridge"
        | "phase7_cn" => (
            "professional_prediction_confirmed_sharpe_bridge".to_string(),
            LayeredSearchConfig::professional_prediction_confirmed_sharpe_bridge_default(),
        ),
        "professional_high_sharpe_boundary_return_bridge"
        | "high_sharpe_boundary_return_bridge"
        | "phase7_high_sharpe_boundary_return_bridge"
        | "phase7_co" => (
            "professional_high_sharpe_boundary_return_bridge".to_string(),
            LayeredSearchConfig::professional_high_sharpe_boundary_return_bridge_default(),
        ),
        "professional_high_sharpe_boundary_event_lift"
        | "high_sharpe_boundary_event_lift"
        | "phase7_high_sharpe_boundary_event_lift"
        | "phase7_cq" => (
            "professional_high_sharpe_boundary_event_lift".to_string(),
            LayeredSearchConfig::professional_high_sharpe_boundary_event_lift_default(),
        ),
        "professional_high_sharpe_micro_frontier"
        | "high_sharpe_micro_frontier"
        | "phase7_high_sharpe_micro_frontier"
        | "phase7_cr" => (
            "professional_high_sharpe_micro_frontier".to_string(),
            LayeredSearchConfig::professional_high_sharpe_micro_frontier_default(),
        ),
        "professional_v14_sharpe_return_lift"
        | "v14_sharpe_return_lift"
        | "phase7_v14_sharpe_return_lift"
        | "phase7_cs" => (
            "professional_v14_sharpe_return_lift".to_string(),
            LayeredSearchConfig::professional_v14_sharpe_return_lift_default(),
        ),
        "professional_v14_shape_lift"
        | "v14_shape_lift"
        | "phase7_v14_shape_lift"
        | "phase7_ct" => (
            "professional_v14_shape_lift".to_string(),
            LayeredSearchConfig::professional_v14_shape_lift_default(),
        ),
        "professional_v14_ultra_micro_lift"
        | "v14_ultra_micro_lift"
        | "phase7_v14_ultra_micro_lift"
        | "phase7_cu" => (
            "professional_v14_ultra_micro_lift".to_string(),
            LayeredSearchConfig::professional_v14_ultra_micro_lift_default(),
        ),
        "professional_v14_annual_floor_micro_lift"
        | "v14_annual_floor_micro_lift"
        | "phase7_v14_annual_floor_micro_lift"
        | "phase7_cz" => (
            "professional_v14_annual_floor_micro_lift".to_string(),
            LayeredSearchConfig::professional_v14_annual_floor_micro_lift_default(),
        ),
        "professional_v14_near_miss_annual_bridge"
        | "v14_near_miss_annual_bridge"
        | "phase7_v14_near_miss_annual_bridge"
        | "phase7_da" => (
            "professional_v14_near_miss_annual_bridge".to_string(),
            LayeredSearchConfig::professional_v14_near_miss_annual_bridge_default(),
        ),
        "professional_v14_corr70_annual_edge"
        | "v14_corr70_annual_edge"
        | "phase7_v14_corr70_annual_edge"
        | "phase7_db" => (
            "professional_v14_corr70_annual_edge".to_string(),
            LayeredSearchConfig::professional_v14_corr70_annual_edge_default(),
        ),
        "professional_execution_robust_candidate"
        | "execution_robust_candidate"
        | "phase7_execution_robust_candidate"
        | "phase7_dj" => (
            "professional_execution_robust_candidate".to_string(),
            LayeredSearchConfig::professional_execution_robust_candidate_default(),
        ),
        "professional_execution_low_turnover_alpha"
        | "execution_low_turnover_alpha"
        | "phase7_execution_low_turnover_alpha"
        | "phase7_dn" => (
            "professional_execution_low_turnover_alpha".to_string(),
            LayeredSearchConfig::professional_execution_low_turnover_alpha_default(),
        ),
        "professional_execution_capacity_budget"
        | "execution_capacity_budget"
        | "phase7_execution_capacity_budget"
        | "phase7_dq" => (
            "professional_execution_capacity_budget".to_string(),
            LayeredSearchConfig::professional_execution_capacity_budget_default(),
        ),
        "professional_execution_impact_budget"
        | "execution_impact_budget"
        | "phase7_execution_impact_budget"
        | "phase7_dr" => (
            "professional_execution_impact_budget".to_string(),
            LayeredSearchConfig::professional_execution_impact_budget_default(),
        ),
        "professional_execution_schedule_budget"
        | "execution_schedule_budget"
        | "phase7_execution_schedule_budget"
        | "phase7_ds" => (
            "professional_execution_schedule_budget".to_string(),
            LayeredSearchConfig::professional_execution_schedule_budget_default(),
        ),
        "professional_execution_patient_schedule_budget"
        | "execution_patient_schedule_budget"
        | "patient_execution_schedule_budget"
        | "phase7_execution_patient_schedule_budget"
        | "phase7_dt" => (
            "professional_execution_patient_schedule_budget".to_string(),
            LayeredSearchConfig::professional_execution_patient_schedule_budget_default(),
        ),
        "professional_execution_daily_cap_budget"
        | "execution_daily_cap_budget"
        | "execution_schedule_daily_cap_budget"
        | "phase7_execution_daily_cap_budget"
        | "phase7_du" => (
            "professional_execution_daily_cap_budget".to_string(),
            LayeredSearchConfig::professional_execution_daily_cap_budget_default(),
        ),
        "professional_execution_cash_drag_aware_budget"
        | "execution_cash_drag_aware_budget"
        | "cash_drag_aware_execution_budget"
        | "phase7_execution_cash_drag_aware_budget"
        | "phase7_dv" => (
            "professional_execution_cash_drag_aware_budget".to_string(),
            LayeredSearchConfig::professional_execution_cash_drag_aware_budget_default(),
        ),
        "professional_execution_feasible_fill_budget"
        | "execution_feasible_fill_budget"
        | "cash_utilization_execution_budget"
        | "phase7_execution_feasible_fill_budget"
        | "phase7_dx" => (
            "professional_execution_feasible_fill_budget".to_string(),
            LayeredSearchConfig::professional_execution_feasible_fill_budget_default(),
        ),
        "professional_execution_fill_ratio_budget"
        | "execution_fill_ratio_budget"
        | "execution_unfilled_gap_budget"
        | "phase7_execution_fill_ratio_budget"
        | "phase7_dy" => (
            "professional_execution_fill_ratio_budget".to_string(),
            LayeredSearchConfig::professional_execution_fill_ratio_budget_default(),
        ),
        "professional_execution_rolling_carry_budget"
        | "execution_rolling_carry_budget"
        | "execution_roll_forward_budget"
        | "phase7_execution_rolling_carry_budget"
        | "phase7_dz" => (
            "professional_execution_rolling_carry_budget".to_string(),
            LayeredSearchConfig::professional_execution_rolling_carry_budget_default(),
        ),
        "professional_execution_capacity_fill_frontier"
        | "execution_capacity_fill_frontier"
        | "capacity_fill_frontier"
        | "phase7_execution_capacity_fill_frontier"
        | "phase7_ea" => (
            "professional_execution_capacity_fill_frontier".to_string(),
            LayeredSearchConfig::professional_execution_capacity_fill_frontier_default(),
        ),
        "professional_execution_alpha_capacity_bridge"
        | "execution_alpha_capacity_bridge"
        | "alpha_capacity_bridge"
        | "phase7_execution_alpha_capacity_bridge"
        | "phase7_eb" => (
            "professional_execution_alpha_capacity_bridge".to_string(),
            LayeredSearchConfig::professional_execution_alpha_capacity_bridge_default(),
        ),
        "professional_simple_heuristic_discovery"
        | "simple_heuristic_discovery"
        | "phase7_s0" => (
            "professional_simple_heuristic_discovery".to_string(),
            LayeredSearchConfig::professional_simple_heuristic_discovery_default(),
        ),
        "professional_multi_factor_heuristic_discovery"
        | "multi_factor_heuristic_discovery"
        | "phase7_s2" => (
            "professional_multi_factor_heuristic_discovery".to_string(),
            LayeredSearchConfig::professional_multi_factor_heuristic_discovery_default(),
        ),
        "professional_blend_factor_heuristic_discovery"
        | "blend_factor_heuristic_discovery"
        | "phase7_s3" => (
            "professional_blend_factor_heuristic_discovery".to_string(),
            LayeredSearchConfig::professional_blend_factor_heuristic_discovery_default(),
        ),
        "professional_price_volume_heuristic_discovery"
        | "price_volume_heuristic_discovery"
        | "phase7_s4" => (
            "professional_price_volume_heuristic_discovery".to_string(),
            LayeredSearchConfig::professional_price_volume_heuristic_discovery_default(),
        ),
        "professional_risk_managed_price_volume_discovery"
        | "risk_managed_price_volume_discovery"
        | "phase7_s5" => (
            "professional_risk_managed_price_volume_discovery".to_string(),
            LayeredSearchConfig::professional_risk_managed_price_volume_discovery_default(),
        ),
        "professional_simple_nlqr_discovery"
        | "simple_nlqr_discovery"
        | "phase7_simple_nlqr"
        | "phase7_s1" => (
            "professional_simple_nlqr_discovery".to_string(),
            LayeredSearchConfig::professional_simple_nlqr_discovery_default(),
        ),
        "professional_execution_alpha_capacity_return_frontier"
        | "execution_alpha_capacity_return_frontier"
        | "alpha_capacity_return_frontier"
        | "phase7_execution_alpha_capacity_return_frontier"
        | "phase7_ec" => (
            "professional_execution_alpha_capacity_return_frontier".to_string(),
            LayeredSearchConfig::professional_execution_alpha_capacity_return_frontier_default(),
        ),
        "professional_execution_stress_fill_return_frontier"
        | "execution_stress_fill_return_frontier"
        | "stress_fill_return_frontier"
        | "phase7_execution_stress_fill_return_frontier"
        | "phase7_ed" => (
            "professional_execution_stress_fill_return_frontier".to_string(),
            LayeredSearchConfig::professional_execution_stress_fill_return_frontier_default(),
        ),
        "professional_execution_stress_risk_budget"
        | "execution_stress_risk_budget"
        | "stress_risk_budget"
        | "phase7_execution_stress_risk_budget"
        | "phase7_ee" => (
            "professional_execution_stress_risk_budget".to_string(),
            LayeredSearchConfig::professional_execution_stress_risk_budget_default(),
        ),
        "professional_execution_capacity_stress_return_gate"
        | "execution_capacity_stress_return_gate"
        | "capacity_stress_return_gate"
        | "phase7_execution_capacity_stress_return_gate"
        | "phase7_ef" => (
            "professional_execution_capacity_stress_return_gate".to_string(),
            LayeredSearchConfig::professional_execution_capacity_stress_return_gate_default(),
        ),
        "professional_execution_low_impact_alpha_stress_return"
        | "execution_low_impact_alpha_stress_return"
        | "low_impact_alpha_stress_return"
        | "phase7_execution_low_impact_alpha_stress_return"
        | "phase7_eg" => (
            "professional_execution_low_impact_alpha_stress_return".to_string(),
            LayeredSearchConfig::professional_execution_low_impact_alpha_stress_return_default(),
        ),
        "professional_execution_stress_target_scaling_return"
        | "execution_stress_target_scaling_return"
        | "stress_target_scaling_return"
        | "phase7_execution_stress_target_scaling_return"
        | "phase7_eh" => (
            "professional_execution_stress_target_scaling_return".to_string(),
            LayeredSearchConfig::professional_execution_stress_target_scaling_return_default(),
        ),
        "professional_execution_stress_floor_scaling_return"
        | "execution_stress_floor_scaling_return"
        | "stress_floor_scaling_return"
        | "phase7_execution_stress_floor_scaling_return"
        | "phase7_ei" => (
            "professional_execution_stress_floor_scaling_return".to_string(),
            LayeredSearchConfig::professional_execution_stress_floor_scaling_return_default(),
        ),
        "professional_execution_stress_floor_return_recovery"
        | "execution_stress_floor_return_recovery"
        | "stress_floor_return_recovery"
        | "phase7_execution_stress_floor_return_recovery"
        | "phase7_ej" => (
            "professional_execution_stress_floor_return_recovery".to_string(),
            LayeredSearchConfig::professional_execution_stress_floor_return_recovery_default(),
        ),
        "professional_execution_pressure_headroom_floor"
        | "execution_pressure_headroom_floor"
        | "pressure_headroom_floor"
        | "phase7_execution_pressure_headroom_floor"
        | "phase7_ek" => (
            "professional_execution_pressure_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_pressure_headroom_floor_default(),
        ),
        "professional_execution_alpha_headroom_floor"
        | "execution_alpha_headroom_floor"
        | "alpha_headroom_floor"
        | "phase7_execution_alpha_headroom_floor"
        | "phase7_el" => (
            "professional_execution_alpha_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_alpha_headroom_floor_default(),
        ),
        "professional_execution_blended_alpha_headroom_floor"
        | "execution_blended_alpha_headroom_floor"
        | "blended_alpha_headroom_floor"
        | "phase7_execution_blended_alpha_headroom_floor"
        | "phase7_em" => (
            "professional_execution_blended_alpha_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_blended_alpha_headroom_floor_default(),
        ),
        "professional_execution_event_anchor_stress_bridge"
        | "execution_event_anchor_stress_bridge"
        | "event_anchor_stress_bridge"
        | "phase7_execution_event_anchor_stress_bridge"
        | "phase7_en" => (
            "professional_execution_event_anchor_stress_bridge".to_string(),
            LayeredSearchConfig::professional_execution_event_anchor_stress_bridge_default(),
        ),
        "professional_execution_participation_aware_event_anchor"
        | "execution_participation_aware_event_anchor"
        | "participation_aware_event_anchor"
        | "phase7_execution_participation_aware_event_anchor"
        | "phase7_eo" => (
            "professional_execution_participation_aware_event_anchor".to_string(),
            LayeredSearchConfig::professional_execution_participation_aware_event_anchor_default(),
        ),
        "professional_execution_cl_anchor_fill_recovery"
        | "execution_cl_anchor_fill_recovery"
        | "cl_anchor_fill_recovery"
        | "phase7_execution_cl_anchor_fill_recovery"
        | "phase7_ep" => (
            "professional_execution_cl_anchor_fill_recovery".to_string(),
            LayeredSearchConfig::professional_execution_cl_anchor_fill_recovery_default(),
        ),
        "professional_execution_capacity_aware_candidate_ranking"
        | "execution_capacity_aware_candidate_ranking"
        | "capacity_aware_candidate_ranking"
        | "phase7_execution_capacity_aware_candidate_ranking"
        | "phase7_eq" => (
            "professional_execution_capacity_aware_candidate_ranking".to_string(),
            LayeredSearchConfig::professional_execution_capacity_aware_candidate_ranking_default(),
        ),
        "professional_execution_pit_capacity_ranking"
        | "execution_pit_capacity_ranking"
        | "pit_capacity_ranking"
        | "phase7_execution_pit_capacity_ranking"
        | "phase7_er" => (
            "professional_execution_pit_capacity_ranking".to_string(),
            LayeredSearchConfig::professional_execution_pit_capacity_ranking_default(),
        ),
        "professional_execution_pit_alpha_first_low_impact"
        | "execution_pit_alpha_first_low_impact"
        | "pit_alpha_first_low_impact"
        | "phase7_execution_pit_alpha_first_low_impact"
        | "phase7_es" => (
            "professional_execution_pit_alpha_first_low_impact".to_string(),
            LayeredSearchConfig::professional_execution_pit_alpha_first_low_impact_default(),
        ),
        "professional_execution_pit_excess_return_recovery"
        | "execution_pit_excess_return_recovery"
        | "pit_excess_return_recovery"
        | "phase7_execution_pit_excess_return_recovery"
        | "phase7_et" => (
            "professional_execution_pit_excess_return_recovery".to_string(),
            LayeredSearchConfig::professional_execution_pit_excess_return_recovery_default(),
        ),
        "professional_execution_bull_sleeve_cash_recovery"
        | "execution_bull_sleeve_cash_recovery"
        | "bull_sleeve_cash_recovery"
        | "phase7_execution_bull_sleeve_cash_recovery"
        | "phase7_eu" => (
            "professional_execution_bull_sleeve_cash_recovery".to_string(),
            LayeredSearchConfig::professional_execution_bull_sleeve_cash_recovery_default(),
        ),
        "professional_execution_return_first_fill_repair"
        | "execution_return_first_fill_repair"
        | "return_first_fill_repair"
        | "phase7_execution_return_first_fill_repair"
        | "phase7_ey" => (
            "professional_execution_return_first_fill_repair".to_string(),
            LayeredSearchConfig::professional_execution_return_first_fill_repair_default(),
        ),
        "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        | "execution_pit_nonlinear_alpha_regime_rebuild"
        | "pit_nonlinear_alpha_regime_rebuild"
        | "phase7_execution_pit_nonlinear_alpha_regime_rebuild"
        | "phase7_ez" => (
            "professional_execution_pit_nonlinear_alpha_regime_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_pit_nonlinear_alpha_regime_rebuild_default(
            ),
        ),
        "professional_execution_pit_quality_recovery_alpha"
        | "execution_pit_quality_recovery_alpha"
        | "pit_quality_recovery_alpha"
        | "phase7_execution_pit_quality_recovery_alpha"
        | "phase7_fa" => (
            "professional_execution_pit_quality_recovery_alpha".to_string(),
            LayeredSearchConfig::professional_execution_pit_quality_recovery_alpha_default(),
        ),
        "professional_execution_event_post_return_curve_alpha"
        | "execution_event_post_return_curve_alpha"
        | "event_post_return_curve_alpha"
        | "phase7_execution_event_post_return_curve_alpha"
        | "phase7_fb" => (
            "professional_execution_event_post_return_curve_alpha".to_string(),
            LayeredSearchConfig::professional_execution_event_post_return_curve_alpha_default(),
        ),
        "professional_execution_event_reaction_alpha"
        | "execution_event_reaction_alpha"
        | "event_reaction_alpha"
        | "phase7_execution_event_reaction_alpha"
        | "phase7_fc" => (
            "professional_execution_event_reaction_alpha".to_string(),
            LayeredSearchConfig::professional_execution_event_reaction_alpha_default(),
        ),
        "professional_execution_broad_financial_feature_discovery"
        | "execution_broad_financial_feature_discovery"
        | "broad_financial_feature_discovery"
        | "phase7_execution_broad_financial_feature_discovery"
        | "phase7_fg" => (
            "professional_execution_broad_financial_feature_discovery".to_string(),
            LayeredSearchConfig::professional_execution_broad_financial_feature_discovery_default(),
        ),
        "professional_execution_broad_financial_feature_stratified_discovery"
        | "execution_broad_financial_feature_stratified_discovery"
        | "broad_financial_feature_stratified_discovery"
        | "phase7_execution_broad_financial_feature_stratified_discovery"
        | "phase7_fh" => (
            "professional_execution_broad_financial_feature_stratified_discovery".to_string(),
            LayeredSearchConfig::professional_execution_broad_financial_feature_stratified_discovery_default(),
        ),
        "professional_execution_native_alpha_fusion_discovery"
        | "execution_native_alpha_fusion_discovery"
        | "native_alpha_fusion_discovery"
        | "phase7_execution_native_alpha_fusion_discovery"
        | "phase7_fj" => (
            "professional_execution_native_alpha_fusion_discovery".to_string(),
            LayeredSearchConfig::professional_execution_native_alpha_fusion_discovery_default(),
        ),
        "professional_trainable_alpha_admission_discovery"
        | "trainable_alpha_admission_discovery"
        | "phase7_trainable_alpha_admission"
        | "phase7_ft" => (
            "professional_trainable_alpha_admission_discovery".to_string(),
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default(),
        ),
        "professional_v19_multi_alpha_sleeve_admission"
        | "v19_multi_alpha_sleeve_admission"
        | "multi_alpha_sleeve_admission"
        | "phase7_v19_sleeves" => (
            "professional_v19_multi_alpha_sleeve_admission".to_string(),
            LayeredSearchConfig::professional_v19_multi_alpha_sleeve_admission_default(),
        ),
        "professional_v19_event_surprise_sleeve_gate_admission"
        | "v19_event_surprise_sleeve_gate"
        | "phase7_v19_event_surprise_sleeve_gate"
        | "phase7_p311_event_surprise_sleeve_gate" => (
            "professional_v19_event_surprise_sleeve_gate_admission".to_string(),
            LayeredSearchConfig::professional_v19_event_surprise_sleeve_gate_default(),
        ),
        "professional_v19_supply_float_sleeve_admission"
        | "v19_supply_float_sleeve"
        | "phase7_v19_supply_float_sleeve"
        | "phase7_p312_supply_float_sleeve" => (
            "professional_v19_supply_float_sleeve_admission".to_string(),
            LayeredSearchConfig::professional_v19_supply_float_sleeve_default(),
        ),
        "professional_v19_unlock_pressure_sleeve_admission"
        | "v19_unlock_pressure_sleeve"
        | "phase7_v19_unlock_pressure_sleeve"
        | "phase7_p314_unlock_pressure_sleeve" => (
            "professional_v19_unlock_pressure_sleeve_admission".to_string(),
            LayeredSearchConfig::professional_v19_unlock_pressure_sleeve_default(),
        ),
        "professional_v19_forecast_revision_sleeve_admission"
        | "v19_forecast_revision_sleeve"
        | "phase7_v19_forecast_revision_sleeve"
        | "phase7_p313_forecast_revision_sleeve" => (
            "professional_v19_forecast_revision_sleeve_admission".to_string(),
            LayeredSearchConfig::professional_v19_forecast_revision_sleeve_default(),
        ),
        "professional_v19_shareholder_structure_sleeve_admission"
        | "v19_shareholder_structure_sleeve"
        | "phase7_v19_shareholder_structure_sleeve"
        | "phase7_p321e_shareholder_structure_sleeve" => (
            "professional_v19_shareholder_structure_sleeve_admission".to_string(),
            LayeredSearchConfig::professional_v19_shareholder_structure_sleeve_default(),
        ),
        "professional_v19_event_post_return_overlay_admission"
        | "v19_event_post_return_overlay_admission"
        | "v19_event_post_return_overlay"
        | "phase7_v19_event_post_return_overlay" => (
            "professional_v19_event_post_return_overlay_admission".to_string(),
            LayeredSearchConfig::professional_v19_event_post_return_overlay_admission_default(),
        ),
        "professional_v19_execution_repair_admission"
        | "v19_execution_repair_admission"
        | "v19_execution_repair"
        | "phase7_v19_execution_repair"
        | "phase7_v19_exec_repair" => (
            "professional_v19_execution_repair_admission".to_string(),
            LayeredSearchConfig::professional_v19_execution_repair_admission_default(),
        ),
        "professional_v19_train_window_ml_alpha_rebuild"
        | "v19_train_window_ml_alpha_rebuild"
        | "v19_ml_alpha_rebuild"
        | "phase7_v19_train_window_ml_alpha_rebuild"
        | "phase7_v19_ml_alpha_rebuild" => (
            "professional_v19_train_window_ml_alpha_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_alpha_rebuild_default(),
        ),
        "professional_v19_train_window_ml_simple_excess_rebuild"
        | "v19_train_window_ml_simple_excess_rebuild"
        | "v19_ml_simple_excess_rebuild"
        | "phase7_v19_train_window_ml_simple_excess"
        | "phase7_v19_ml_simple_excess" => (
            "professional_v19_train_window_ml_simple_excess_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_simple_excess_rebuild_default(),
        ),
        "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
        | "v19_train_window_ml_simple_excess_low_impact_rebuild"
        | "v19_ml_simple_excess_low_impact_rebuild"
        | "phase7_v19_train_window_ml_simple_excess_low_impact"
        | "phase7_v19_ml_simple_excess_low_impact" => (
            "professional_v19_train_window_ml_simple_excess_low_impact_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_simple_excess_low_impact_rebuild_default(),
        ),
        "professional_v19_train_window_ml_h120_low_impact_rebuild"
        | "v19_train_window_ml_h120_low_impact_rebuild"
        | "v19_ml_h120_low_impact_rebuild"
        | "phase7_v19_train_window_ml_h120_low_impact"
        | "phase7_v19_ml_h120_low_impact" => (
            "professional_v19_train_window_ml_h120_low_impact_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_h120_low_impact_rebuild_default(),
        ),
        "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        | "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        | "v19_ml_rae_h120_residual_capacity_rebuild"
        | "phase7_v19_train_window_ml_rae_h120_residual_capacity"
        | "phase7_v19_ml_rae_h120_residual_capacity" => (
            "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_default(),
        ),
        "professional_v19_train_window_ml_event_sentiment_rebuild"
        | "v19_train_window_ml_event_sentiment_rebuild"
        | "v19_ml_event_sentiment_rebuild"
        | "phase7_v19_train_window_ml_event_sentiment"
        | "phase7_v19_ml_event_sentiment" => (
            "professional_v19_train_window_ml_event_sentiment_rebuild".to_string(),
            LayeredSearchConfig::professional_v19_train_window_ml_event_sentiment_rebuild_default(),
        ),
        "professional_prediction_capacity_dual_objective"
        | "prediction_capacity_dual_objective"
        | "phase7_prediction_capacity_dual_objective"
        | "phase7_fl" => (
            "professional_prediction_capacity_dual_objective".to_string(),
            LayeredSearchConfig::professional_prediction_capacity_dual_objective_default(),
        ),
        "professional_prediction_target_gross_signal_fidelity"
        | "prediction_target_gross_signal_fidelity"
        | "phase7_prediction_target_gross_signal_fidelity"
        | "phase7_fm" => (
            "professional_prediction_target_gross_signal_fidelity".to_string(),
            LayeredSearchConfig::professional_prediction_target_gross_signal_fidelity_default(),
        ),
        "professional_prediction_confidence_turnover_discovery"
        | "prediction_confidence_turnover_discovery"
        | "phase7_prediction_confidence_turnover_discovery"
        | "phase7_fn" => (
            "professional_prediction_confidence_turnover_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_confidence_turnover_discovery_default(),
        ),
        "professional_prediction_confidence_alpha_lift"
        | "prediction_confidence_alpha_lift"
        | "phase7_prediction_confidence_alpha_lift"
        | "phase7_fo" => (
            "professional_prediction_confidence_alpha_lift".to_string(),
            LayeredSearchConfig::professional_prediction_confidence_alpha_lift_default(),
        ),
        "professional_prediction_long_horizon_low_turnover"
        | "prediction_long_horizon_low_turnover"
        | "phase7_prediction_long_horizon_low_turnover"
        | "phase7_fp" => (
            "professional_prediction_long_horizon_low_turnover".to_string(),
            LayeredSearchConfig::professional_prediction_long_horizon_low_turnover_default(),
        ),
        "professional_prediction_long_horizon_regime_alpha"
        | "prediction_long_horizon_regime_alpha"
        | "phase7_prediction_long_horizon_regime_alpha"
        | "phase7_fr" => (
            "professional_prediction_long_horizon_regime_alpha".to_string(),
            LayeredSearchConfig::professional_prediction_long_horizon_regime_alpha_default(),
        ),
        "professional_prediction_h60_nonlinear_stress_discovery"
        | "prediction_h60_nonlinear_stress_discovery"
        | "h60_nonlinear_stress_discovery"
        | "phase7_prediction_h60_nonlinear_stress_discovery"
        | "phase7_fw" => (
            "professional_prediction_h60_nonlinear_stress_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_h60_nonlinear_stress_discovery_default(),
        ),
        "professional_prediction_h120_low_impact_stress_discovery"
        | "prediction_h120_low_impact_stress_discovery"
        | "h120_low_impact_stress_discovery"
        | "phase7_prediction_h120_low_impact_stress_discovery"
        | "phase7_fz" => (
            "professional_prediction_h120_low_impact_stress_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_h120_low_impact_stress_discovery_default(),
        ),
        "professional_train_window_nonlinear_ranking_discovery"
        | "train_window_nonlinear_ranking_discovery"
        | "phase7_train_window_nonlinear_ranking_discovery"
        | "phase7_fx" => (
            "professional_train_window_nonlinear_ranking_discovery".to_string(),
            LayeredSearchConfig::professional_train_window_nonlinear_ranking_discovery_default(),
        ),
        "professional_train_window_stress_fill_target_exposure"
        | "train_window_stress_fill_target_exposure"
        | "stress_fill_target_exposure"
        | "phase7_train_window_stress_fill_target_exposure"
        | "phase7_ga" => (
            "professional_train_window_stress_fill_target_exposure".to_string(),
            LayeredSearchConfig::professional_train_window_stress_fill_target_exposure_default(),
        ),
        "professional_train_window_ml_stress_fill_discovery"
        | "train_window_ml_stress_fill_discovery"
        | "ml_stress_fill_discovery"
        | "phase7_train_window_ml_stress_fill_discovery"
        | "phase7_gb" => (
            "professional_train_window_ml_stress_fill_discovery".to_string(),
            LayeredSearchConfig::professional_train_window_ml_stress_fill_discovery_default(),
        ),
        "professional_ensemble_discovery"
        | "phase7_ensemble_v1" => (
            "professional_ensemble_discovery".to_string(),
            LayeredSearchConfig::professional_ensemble_discovery_default(),
        ),
        "professional_current_event_nonlinear_alpha_discovery"
        | "current_event_nonlinear_alpha_discovery"
        | "phase7_current_event_nonlinear_alpha_discovery"
        | "phase7_fs" => (
            "professional_current_event_nonlinear_alpha_discovery".to_string(),
            LayeredSearchConfig::professional_current_event_nonlinear_alpha_discovery_default(),
        ),
        "professional_execution_oos_regime_alpha_rebuild"
        | "execution_oos_regime_alpha_rebuild"
        | "oos_regime_alpha_rebuild"
        | "phase7_execution_oos_regime_alpha_rebuild"
        | "phase7_ev" => (
            "professional_execution_oos_regime_alpha_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_regime_alpha_rebuild_default(),
        ),
        "professional_execution_oos_benchmark_excess_rebuild"
        | "execution_oos_benchmark_excess_rebuild"
        | "oos_benchmark_excess_rebuild"
        | "phase7_execution_oos_benchmark_excess_rebuild"
        | "phase7_ew" => (
            "professional_execution_oos_benchmark_excess_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_benchmark_excess_rebuild_default(),
        ),
        "professional_execution_oos_execution_adaptive_rebuild"
        | "execution_oos_execution_adaptive_rebuild"
        | "oos_execution_adaptive_rebuild"
        | "phase7_execution_oos_execution_adaptive_rebuild"
        | "phase7_ex" => (
            "professional_execution_oos_execution_adaptive_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_execution_adaptive_rebuild_default(),
        ),
        "professional_return_alpha_sharpe_bridge"
        | "return_alpha_sharpe_bridge"
        | "phase7_return_alpha_sharpe_bridge"
        | "phase7_cv" => (
            "professional_return_alpha_sharpe_bridge".to_string(),
            LayeredSearchConfig::professional_return_alpha_sharpe_bridge_default(),
        ),
        "professional_regime_frontier_bridge"
        | "regime_frontier_bridge"
        | "phase7_regime_frontier_bridge"
        | "phase7_cw" => (
            "professional_regime_frontier_bridge".to_string(),
            LayeredSearchConfig::professional_regime_frontier_bridge_default(),
        ),
        "professional_regime_frontier_decomposition"
        | "regime_frontier_decomposition"
        | "phase7_regime_frontier_decomposition"
        | "phase7_cx" => (
            "professional_regime_frontier_decomposition".to_string(),
            LayeredSearchConfig::professional_regime_frontier_decomposition_default(),
        ),
        "professional_high_sharpe_return_micro_bridge"
        | "high_sharpe_return_micro_bridge"
        | "phase7_high_sharpe_return_micro_bridge"
        | "phase7_cy" => (
            "professional_high_sharpe_return_micro_bridge".to_string(),
            LayeredSearchConfig::professional_high_sharpe_return_micro_bridge_default(),
        ),
        "professional_return_distribution_repair"
        | "return_distribution_repair"
        | "phase7_return_distribution_repair"
        | "phase7_cp" => (
            "professional_return_distribution_repair".to_string(),
            LayeredSearchConfig::professional_return_distribution_repair_default(),
        ),
        "professional_state_alpha_router"
        | "state_alpha_router"
        | "phase7_state_alpha_router"
        | "phase7_bj" => (
            "professional_state_alpha_router".to_string(),
            LayeredSearchConfig::professional_state_alpha_router_default(),
        ),
        "professional_state_position_risk_router"
        | "state_position_risk_router"
        | "phase7_state_position_risk_router"
        | "phase7_bk" => (
            "professional_state_position_risk_router".to_string(),
            LayeredSearchConfig::professional_state_position_risk_router_default(),
        ),
        "professional_event_position_risk_router"
        | "event_position_risk_router"
        | "phase7_event_position_risk_router"
        | "phase7_bl" => (
            "professional_event_position_risk_router".to_string(),
            LayeredSearchConfig::professional_event_position_risk_router_default(),
        ),
        "professional_all_regime_event_sleeve"
        | "all_regime_event_sleeve"
        | "phase7_all_regime_event_sleeve"
        | "phase7_bm" => (
            "professional_all_regime_event_sleeve".to_string(),
            LayeredSearchConfig::professional_all_regime_event_sleeve_default(),
        ),
        "professional_portfolio_sharpe_control"
        | "portfolio_sharpe_control"
        | "phase7_portfolio_sharpe_control"
        | "phase7_cj" => (
            "professional_portfolio_sharpe_control".to_string(),
            LayeredSearchConfig::professional_portfolio_sharpe_control_default(),
        ),
        "professional_volatility_sharpe" | "volatility_sharpe" | "phase7_ai" => (
            "professional_volatility_sharpe".to_string(),
            LayeredSearchConfig::professional_volatility_sharpe_default(),
        ),
        "professional_regime_position_sharpe"
        | "regime_position_sharpe"
        | "phase7_regime_position_sharpe"
        | "phase7_aj" => (
            "professional_regime_position_sharpe".to_string(),
            LayeredSearchConfig::professional_regime_position_sharpe_default(),
        ),
        "professional_anti_overfit_sharpe"
        | "anti_overfit_sharpe"
        | "phase7_anti_overfit_sharpe"
        | "phase7_ab" => (
            "professional_anti_overfit_sharpe".to_string(),
            LayeredSearchConfig::professional_anti_overfit_sharpe_default(),
        ),
        "professional_candidate_risk_filter"
        | "candidate_risk_filter"
        | "phase7_candidate_risk_filter"
        | "phase7_ac" => (
            "professional_candidate_risk_filter".to_string(),
            LayeredSearchConfig::professional_candidate_risk_filter_default(),
        ),
        "professional_risk_contribution"
        | "risk_contribution"
        | "phase7_risk_contribution"
        | "phase7_ad" => (
            "professional_risk_contribution".to_string(),
            LayeredSearchConfig::professional_risk_contribution_default(),
        ),
        "professional_event_conditioned_sharpe"
        | "event_conditioned_sharpe"
        | "phase7_event_conditioned"
        | "phase7_ae" => (
            "professional_event_conditioned_sharpe".to_string(),
            LayeredSearchConfig::professional_event_conditioned_sharpe_default(),
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


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn alpha_source_diagnostics_defaults_are_safe_and_bounded() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: " phase7_financial_quality_v1 ".to_string(),
            version: None,
            start_date: "20260101".to_string(),
            end_date: "20260131".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: None,
            min_day_coverage_ratio: Some(2.0),
            min_daily_rows: Some(0),
            min_p95_daily_row_ratio: Some(-1.0),
            persist_report: None,
            include_research_metrics: None,
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };

        assert_eq!(
            alpha_source_diagnostics_combo_name(&req).unwrap(),
            "phase7_financial_quality_v1"
        );
        assert_eq!(alpha_source_diagnostics_version(&req), "1.0.0");
        assert!(alpha_source_diagnostics_persist_default(None));
        assert!(!alpha_source_diagnostics_persist_default(Some(false)));

        let thresholds = alpha_source_diagnostics_thresholds(&req);
        assert_eq!(thresholds.min_day_coverage_ratio, 1.0);
        assert_eq!(thresholds.min_daily_rows, 1);
        assert_eq!(thresholds.min_p95_daily_row_ratio, 0.05);
    }

    #[test]
    fn alpha_source_diagnostics_blocks_industry_prosperity_without_admission_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_industry_prosperity_proxy_v1".to_string(),
            version: None,
            start_date: "20260101".to_string(),
            end_date: "20260131".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("phase7_industry_membership_market_scope_gate_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("科创板"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_industry_prosperity_with_market_scope_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_industry_prosperity_proxy_v1".to_string(),
            version: None,
            start_date: "20260101".to_string(),
            end_date: "20260131".to_string(),
            alpha_admission_gate_id: Some(
                "phase7_industry_membership_market_scope_gate_v1".to_string(),
            ),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("market-scope gated diagnostics request");
    }

    #[test]
    fn alpha_source_diagnostics_blocks_futures_price_chain_without_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "futures_price_chain".to_string(),
            version: Some("p319q-sw2021-l1-price-chain-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260618".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("futures_price_chain_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_futures_price_chain_with_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "futures_price_chain".to_string(),
            version: Some("p319q-sw2021-l1-price-chain-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260618".to_string(),
            alpha_admission_gate_id: Some("futures_price_chain_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("coverage-gated futures price-chain diagnostics request");
    }

    #[test]
    fn alpha_source_diagnostics_blocks_equity_pledge_without_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "equity_pledge_pressure".to_string(),
            version: Some("p320f-equity-pledge-pressure-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("equity_pledge_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_equity_pledge_with_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "equity_pledge_pressure".to_string(),
            version: Some("p320f-equity-pledge-pressure-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: Some("equity_pledge_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("coverage-gated equity pledge diagnostics request");
    }

    #[test]
    fn alpha_source_diagnostics_blocks_shareholder_structure_without_strict_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "shareholder_structure".to_string(),
            version: Some("p321c-low-fanout-strict-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("shareholder_structure_low_fanout_strict_pit_gate_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("holder_number available_at >= end_date"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_shareholder_structure_with_strict_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "shareholder_structure".to_string(),
            version: Some("p321c-low-fanout-strict-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: Some(
                "shareholder_structure_low_fanout_strict_pit_gate_v1".to_string(),
            ),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("strict-gated shareholder structure diagnostics request");
    }

    #[test]
    fn alpha_source_diagnostics_blocks_margin_detail_without_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "margin_detail_leverage_crowding".to_string(),
            version: Some("p322e-margin-leverage-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("margin_detail_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_margin_detail_with_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "margin_detail_leverage_crowding".to_string(),
            version: Some("p322e-margin-leverage-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260623".to_string(),
            alpha_admission_gate_id: Some("margin_detail_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("coverage-gated margin detail diagnostics request");
    }

    #[test]
    fn alpha_source_diagnostics_blocks_analyst_revision_without_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "multi_vendor_analyst_revision".to_string(),
            version: Some("p323f-akshare-cninfo-revision-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260624".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        let err = validate_alpha_source_diagnostics_admission(&req, &combo_name).unwrap_err();

        assert!(err.contains("P3.10 diagnostics"));
        assert!(err.contains("analyst_revision_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn alpha_source_diagnostics_allows_analyst_revision_with_coverage_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "multi_vendor_analyst_revision".to_string(),
            version: Some("p323f-akshare-cninfo-revision-v1".to_string()),
            start_date: "20140103".to_string(),
            end_date: "20260624".to_string(),
            alpha_admission_gate_id: Some("analyst_revision_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let combo_name = alpha_source_diagnostics_combo_name(&req).unwrap();

        validate_alpha_source_diagnostics_admission(&req, &combo_name)
            .expect("coverage-gated analyst revision diagnostics request");
    }

    #[test]
    fn futures_price_chain_component_orientation_contract_blocks_direct_wfa() {
        let report = futures_price_chain_component_orientation_contract_json(true);

        assert_eq!(report["included"], true);
        assert_eq!(report["research_only"], true);
        assert_eq!(
            report["admission"]["status"],
            "blocked_until_component_economics_pass"
        );
        assert_eq!(report["admission"]["bounded_wfa"], "blocked");
        assert_eq!(report["admission"]["v19_train_selection"], "blocked");
        assert_eq!(
            report["scope"]["component_source"],
            "market_futures_product_signal_pit -> PIT product exposure mapping -> PIT stock industry membership"
        );

        let components = report["components"].as_array().unwrap();
        assert_eq!(components.len(), 3);
        let component_codes = components
            .iter()
            .map(|component| component["signal_code"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            component_codes,
            vec![
                "fpc_price_mom_20v60_std",
                "fpc_inventory_tight_20v60_std",
                "fpc_net_position_20v60_std"
            ]
        );
        assert!(components
            .iter()
            .all(|component| component["sign_flip_policy"]
                .as_str()
                .unwrap()
                .contains("blocked")));
    }

    #[test]
    fn futures_price_chain_component_score_sql_is_pit_and_read_only() {
        let sql = futures_price_chain_component_score_rows_sql();

        assert!(sql.contains("FROM market_futures_product_signal_pit"));
        assert!(sql.contains("mapping.available_at <= rd.trade_date"));
        assert!(sql.contains("membership.available_at <= universe.trade_date"));
        assert!(sql.contains("industry_signal.available_at <= stock_membership.trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("market_stock_name_history"));
        assert!(!sql.contains("INSERT INTO"));
        assert!(!sql.contains("DELETE FROM"));
        assert!(!sql.contains("UPDATE "));
    }

    #[test]
    fn alpha_source_diagnostics_gates_require_all_checks_to_pass() {
        let passing_gates = vec![
            profile_readiness_gate("coverage", true, json!(1.0), json!(0.98), "ok"),
            profile_readiness_gate("pit", true, json!(0), json!(0), "ok"),
        ];
        assert!(alpha_source_diagnostics_gates_passed(&passing_gates));
        assert_eq!(alpha_source_diagnostics_level(true), "green");

        let failing_gates = vec![
            profile_readiness_gate("coverage", true, json!(1.0), json!(0.98), "ok"),
            profile_readiness_gate("pit", false, json!(3), json!(0), "future leak"),
        ];
        assert!(!alpha_source_diagnostics_gates_passed(&failing_gates));
        assert_eq!(alpha_source_diagnostics_level(false), "red");
    }

    #[test]
    fn alpha_source_daily_breadth_status_allows_structural_prefix_ramp() {
        let start = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();
        let mut daily_rows = Vec::new();
        for offset in 0..100 {
            let rows = if offset < 5 {
                1_080 + offset as i64
            } else {
                1_200
            };
            daily_rows.push((start + Duration::days(offset), rows));
        }
        let distribution = daily_count_distribution(
            &daily_rows
                .iter()
                .map(|(_, count)| *count)
                .collect::<Vec<_>>(),
            ReadinessThresholds {
                min_day_coverage_ratio: 0.98,
                min_daily_rows: 20,
                min_p95_daily_row_ratio: 0.95,
            },
        );

        let status = alpha_source_daily_breadth_status(&daily_rows, &distribution);

        assert_eq!(status.raw_weak_day_count, 5);
        assert_eq!(status.structural_early_weak_day_count, 5);
        assert_eq!(status.unexplained_weak_day_count, 0);
        assert_eq!(status.status, "structural_early_ramp");
        assert!(status.passed);
    }

    #[test]
    fn alpha_source_daily_breadth_status_allows_early_interleaved_ramp() {
        let start = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();
        let mut daily_rows = Vec::new();
        for offset in 0..100 {
            let rows = match offset {
                0 | 1 | 3 | 5 => 1_080 + offset as i64,
                2 | 4 | 6 | 7 => 1_160,
                _ => 1_200,
            };
            daily_rows.push((start + Duration::days(offset), rows));
        }
        let distribution = daily_count_distribution(
            &daily_rows
                .iter()
                .map(|(_, count)| *count)
                .collect::<Vec<_>>(),
            ReadinessThresholds {
                min_day_coverage_ratio: 0.98,
                min_daily_rows: 20,
                min_p95_daily_row_ratio: 0.95,
            },
        );

        let status = alpha_source_daily_breadth_status(&daily_rows, &distribution);

        assert_eq!(status.raw_weak_day_count, 4);
        assert_eq!(status.structural_early_weak_day_count, 4);
        assert_eq!(status.unexplained_weak_day_count, 0);
        assert_eq!(status.status, "structural_early_ramp");
        assert!(status.passed);
    }

    #[test]
    fn alpha_source_daily_breadth_status_rejects_late_symbol_cliffs() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let mut daily_rows = (0..100)
            .map(|offset| (start + Duration::days(offset), 1_200))
            .collect::<Vec<_>>();
        daily_rows[60].1 = 900;
        let distribution = daily_count_distribution(
            &daily_rows
                .iter()
                .map(|(_, count)| *count)
                .collect::<Vec<_>>(),
            ReadinessThresholds {
                min_day_coverage_ratio: 0.98,
                min_daily_rows: 20,
                min_p95_daily_row_ratio: 0.95,
            },
        );

        let status = alpha_source_daily_breadth_status(&daily_rows, &distribution);

        assert_eq!(status.raw_weak_day_count, 1);
        assert_eq!(status.structural_early_weak_day_count, 0);
        assert_eq!(status.unexplained_weak_day_count, 1);
        assert_eq!(status.status, "unexplained_cliff");
        assert!(!status.passed);
    }

    #[test]
    fn alpha_source_market_scope_breadth_status_allows_stable_restricted_ratio() {
        let start = NaiveDate::from_ymd_opt(2014, 4, 3).unwrap();
        let daily_rows = (0..120)
            .map(|offset| {
                let eligible = 2_000 + offset as i64 * 5;
                (
                    start + Duration::days(offset),
                    (eligible as f64 * 0.27) as i64,
                )
            })
            .collect::<Vec<_>>();
        let eligible_rows = (0..120)
            .map(|offset| (start + Duration::days(offset), 2_000 + offset as i64 * 5))
            .collect::<Vec<_>>();

        let status = alpha_source_market_scope_breadth_status(&daily_rows, &eligible_rows);

        assert!(status.passed);
        assert_eq!(status.status, "stable_market_scope_ratio");
        assert_eq!(status.missing_eligible_days, 0);
        assert_eq!(status.weak_day_count, 0);
        assert!(status.p50_ratio > 0.26);
    }

    #[test]
    fn alpha_source_market_scope_breadth_status_rejects_ratio_cliffs() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let mut daily_rows = (0..100)
            .map(|offset| (start + Duration::days(offset), 300))
            .collect::<Vec<_>>();
        daily_rows[70].1 = 20;
        let eligible_rows = (0..100)
            .map(|offset| (start + Duration::days(offset), 1_000))
            .collect::<Vec<_>>();

        let status = alpha_source_market_scope_breadth_status(&daily_rows, &eligible_rows);

        assert!(!status.passed);
        assert_eq!(status.status, "market_scope_ratio_cliff");
        assert_eq!(status.weak_day_count, 1);
        assert_eq!(status.first_weak_day, Some(start + Duration::days(70)));
    }

    #[test]
    fn alpha_source_market_scope_breadth_requires_registered_gate() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "futures_price_chain".to_string(),
            version: Some("p319q-sw2021-l1-price-chain-v1".to_string()),
            start_date: "20140403".to_string(),
            end_date: "20260618".to_string(),
            alpha_admission_gate_id: Some(FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID.to_string()),
            universe_profile: Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE.to_string()),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: None,
            include_research_metrics: None,
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        assert!(alpha_source_market_scope_breadth_enabled(
            &req,
            "futures_price_chain"
        ));

        let mut missing_gate_req = req;
        missing_gate_req.alpha_admission_gate_id = None;
        assert!(!alpha_source_market_scope_breadth_enabled(
            &missing_gate_req,
            "futures_price_chain"
        ));
    }

    #[test]
    fn alpha_source_research_economic_admission_blocks_negative_spreads() {
        let metrics = json!({
            "included": true,
            "rank_ic_by_horizon": [
                {"horizon_days": 20, "mean_rank_ic": -0.01, "positive_day_ratio": 0.45},
                {"horizon_days": 60, "mean_rank_ic": 0.005, "positive_day_ratio": 0.50}
            ],
            "group_return_by_horizon": [
                {"horizon_days": 20, "high_minus_low_spread": -0.01, "monotonicity_score": 0.44},
                {"horizon_days": 60, "high_minus_low_spread": -0.002, "monotonicity_score": 0.55}
            ],
            "turnover_capacity_by_horizon": [
                {"horizon_days": 20, "avg_high_score_bucket_turnover": 0.65},
                {"horizon_days": 60, "avg_high_score_bucket_turnover": 0.65}
            ]
        });

        let admission = alpha_source_research_economic_admission(&metrics);

        assert_eq!(admission["status"], "blocked_by_p310_economics");
        assert_eq!(admission["bounded_wfa"], "blocked");
        assert_eq!(admission["passed_horizon_count"], 0);
    }

    #[test]
    fn alpha_source_research_economic_admission_requires_two_good_horizons() {
        let metrics = json!({
            "included": true,
            "rank_ic_by_horizon": [
                {"horizon_days": 20, "mean_rank_ic": 0.012, "positive_day_ratio": 0.55},
                {"horizon_days": 60, "mean_rank_ic": 0.018, "positive_day_ratio": 0.57}
            ],
            "group_return_by_horizon": [
                {"horizon_days": 20, "high_minus_low_spread": 0.006, "monotonicity_score": 0.56},
                {"horizon_days": 60, "high_minus_low_spread": 0.011, "monotonicity_score": 0.62}
            ],
            "turnover_capacity_by_horizon": [
                {"horizon_days": 20, "avg_high_score_bucket_turnover": 0.40},
                {"horizon_days": 60, "avg_high_score_bucket_turnover": 0.50}
            ]
        });

        let admission = alpha_source_research_economic_admission(&metrics);

        assert_eq!(admission["status"], "candidate_ready_for_bounded_wfa");
        assert_eq!(admission["bounded_wfa"], "eligible");
        assert_eq!(admission["passed_horizon_count"], 2);
    }

    #[test]
    fn alpha_source_research_diagnostics_options_are_explicit_and_bounded() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_moneyflow_congestion_interaction_v1".to_string(),
            version: None,
            start_date: "20230101".to_string(),
            end_date: "20231231".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: None,
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: Some(vec![60, 0, 20, 500, 20]),
            bucket_count: Some(1),
            max_rank_ic_days: Some(10_000),
            include_exposure_regime_metrics: Some(true),
            max_exposure_regime_days: Some(10_000),
        };

        let options = alpha_source_research_diagnostics_options(&req);

        assert!(options.include_research_metrics);
        assert_eq!(options.return_horizons, vec![20, 60, 252]);
        assert_eq!(options.bucket_count, 3);
        assert_eq!(options.max_rank_ic_days, 520);

        let cheap_default = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_moneyflow_congestion_interaction_v1".to_string(),
            version: None,
            start_date: "20230101".to_string(),
            end_date: "20231231".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: None,
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: None,
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        assert!(
            !alpha_source_research_diagnostics_options(&cheap_default).include_research_metrics
        );
    }

    #[test]
    fn alpha_source_rank_ic_summary_handles_signed_samples() {
        let rows = vec![
            AlphaSourceDailyRankIc {
                trade_date: NaiveDate::from_ymd_opt(2023, 1, 3).unwrap(),
                rank_ic: 0.20,
                sample_size: 100,
            },
            AlphaSourceDailyRankIc {
                trade_date: NaiveDate::from_ymd_opt(2023, 1, 4).unwrap(),
                rank_ic: -0.10,
                sample_size: 100,
            },
            AlphaSourceDailyRankIc {
                trade_date: NaiveDate::from_ymd_opt(2023, 1, 5).unwrap(),
                rank_ic: 0.0,
                sample_size: 100,
            },
        ];

        let summary = summarize_rank_ic_samples(20, &rows);

        assert_eq!(summary.horizon_days, 20);
        assert_eq!(summary.sampled_days, 3);
        assert!((summary.mean_rank_ic - 0.033333333333).abs() < 1e-9);
        assert_eq!(summary.median_rank_ic, 0.0);
        assert!((summary.positive_day_ratio - (1.0 / 3.0)).abs() < 1e-9);
        assert_eq!(summary.min_daily_sample_size, 100);
    }

    #[test]
    fn alpha_source_research_day_sampling_is_evenly_spaced_and_bounded() {
        let rows = (0..10)
            .map(|offset| {
                (
                    NaiveDate::from_ymd_opt(2023, 1, 1).unwrap() + Duration::days(offset),
                    100,
                )
            })
            .collect::<Vec<_>>();

        let sampled = sample_alpha_source_research_days(&rows, 3);

        assert_eq!(
            sampled,
            vec![
                NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2023, 1, 5).unwrap(),
                NaiveDate::from_ymd_opt(2023, 1, 9).unwrap(),
            ]
        );
        assert_eq!(sample_alpha_source_research_days(&rows, 20).len(), 10);
        assert_eq!(sample_alpha_source_research_days(&rows, 0).len(), 1);
    }

    #[test]
    fn alpha_source_regime_sampling_uses_rank_ic_and_exposure_day_union() {
        let day1 = NaiveDate::from_ymd_opt(2023, 1, 3).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2023, 1, 4).unwrap();
        let day3 = NaiveDate::from_ymd_opt(2023, 1, 5).unwrap();
        let day4 = NaiveDate::from_ymd_opt(2023, 1, 6).unwrap();

        let regime_days = sample_alpha_source_regime_days(&[day1, day3, day4], &[day2, day3]);

        assert_eq!(regime_days, vec![day1, day2, day3, day4]);
    }

    #[test]
    fn alpha_source_group_return_summary_reports_spread_and_monotonicity() {
        let buckets = vec![
            AlphaSourceBucketReturn {
                bucket: 1,
                avg_forward_return: -0.02,
                sample_count: 50,
            },
            AlphaSourceBucketReturn {
                bucket: 2,
                avg_forward_return: -0.01,
                sample_count: 50,
            },
            AlphaSourceBucketReturn {
                bucket: 3,
                avg_forward_return: 0.03,
                sample_count: 50,
            },
        ];

        let summary = summarize_group_return_buckets(20, 3, &buckets);

        assert_eq!(summary.horizon_days, 20);
        assert_eq!(summary.bucket_count, 3);
        assert!((summary.high_minus_low_spread - 0.05).abs() < 1e-9);
        assert_eq!(summary.monotonicity_score, 1.0);
        assert_eq!(summary.total_sample_count, 150);
    }

    #[test]
    fn alpha_source_exposure_regime_options_are_explicit_and_bounded() {
        let req = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_financial_quality_change_v1".to_string(),
            version: None,
            start_date: "20230101".to_string(),
            end_date: "20231231".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: None,
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: Some(true),
            return_horizons: Some(vec![20]),
            bucket_count: Some(10),
            max_rank_ic_days: Some(40),
            include_exposure_regime_metrics: Some(true),
            max_exposure_regime_days: Some(10_000),
        };

        let options = alpha_source_research_diagnostics_options(&req);

        assert!(options.include_exposure_regime_metrics);
        assert_eq!(options.max_exposure_regime_days, 520);

        let cheap_default = AlphaSourceDiagnosticsRequest {
            combo_name: "phase7_financial_quality_change_v1".to_string(),
            version: None,
            start_date: "20230101".to_string(),
            end_date: "20231231".to_string(),
            alpha_admission_gate_id: None,
            universe_profile: None,
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            persist_report: Some(false),
            include_research_metrics: None,
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };
        let default_options = alpha_source_research_diagnostics_options(&cheap_default);
        assert!(!default_options.include_exposure_regime_metrics);
        assert_eq!(default_options.max_exposure_regime_days, 260);
    }

    #[test]
    fn alpha_source_regime_classifier_uses_only_trailing_market_state() {
        assert_eq!(
            classify_alpha_source_market_regime(0.20, 0.18, 0.08),
            "bull"
        );
        assert_eq!(
            classify_alpha_source_market_regime(-0.06, 0.18, 0.12),
            "bear"
        );
        assert_eq!(
            classify_alpha_source_market_regime(0.02, 0.34, 0.10),
            "high_volatility"
        );
        assert_eq!(
            classify_alpha_source_market_regime(0.03, 0.10, 0.04),
            "sideways"
        );
        assert_eq!(
            classify_alpha_source_market_regime(0.07, 0.18, 0.08),
            "mixed"
        );
    }

    #[test]
    fn alpha_source_high_bucket_exposure_reports_concentration_and_tilts() {
        let day = NaiveDate::from_ymd_opt(2023, 1, 3).unwrap();
        let rows = vec![
            AlphaSourceExposureRow {
                trade_date: day,
                score: -2.0,
                amount: Some(10.0),
                circ_mv: Some(100.0),
                total_mv: Some(120.0),
                industry: Some("Bank".to_string()),
            },
            AlphaSourceExposureRow {
                trade_date: day,
                score: 0.1,
                amount: Some(20.0),
                circ_mv: Some(200.0),
                total_mv: Some(240.0),
                industry: Some("Tech".to_string()),
            },
            AlphaSourceExposureRow {
                trade_date: day,
                score: 0.3,
                amount: Some(30.0),
                circ_mv: Some(300.0),
                total_mv: Some(360.0),
                industry: Some("Tech".to_string()),
            },
            AlphaSourceExposureRow {
                trade_date: day,
                score: 1.0,
                amount: Some(40.0),
                circ_mv: Some(400.0),
                total_mv: Some(480.0),
                industry: Some("Tech".to_string()),
            },
        ];

        let summary = high_bucket_exposure_summary_from_rows(&rows, 2, 1);

        assert_eq!(summary.sampled_days, 1);
        assert_eq!(summary.avg_high_score_bucket_symbols, 2.0);
        assert!((summary.avg_high_bucket_industry_hhi - 1.0).abs() < 1e-9);
        assert_eq!(summary.top_industries[0].industry, "Tech");
        assert!((summary.median_high_vs_universe_amount_ratio - (4.0 / 3.0)).abs() < 1e-9);
        assert!((summary.median_high_vs_universe_circ_mv_ratio - (4.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn alpha_source_regime_split_keeps_unknown_and_counts_samples() {
        let first_day = NaiveDate::from_ymd_opt(2023, 1, 3).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2023, 1, 4).unwrap();
        let rows = vec![
            AlphaSourceLabeledRow {
                trade_date: first_day,
                symbol: "000001.SZ".to_string(),
                score: 0.1,
                forward_return: 0.01,
                amount: None,
                circ_mv: None,
            },
            AlphaSourceLabeledRow {
                trade_date: first_day,
                symbol: "000002.SZ".to_string(),
                score: 0.2,
                forward_return: 0.02,
                amount: None,
                circ_mv: None,
            },
            AlphaSourceLabeledRow {
                trade_date: second_day,
                symbol: "000001.SZ".to_string(),
                score: 0.3,
                forward_return: -0.01,
                amount: None,
                circ_mv: None,
            },
            AlphaSourceLabeledRow {
                trade_date: second_day,
                symbol: "000002.SZ".to_string(),
                score: 0.4,
                forward_return: -0.02,
                amount: None,
                circ_mv: None,
            },
        ];
        let mut regimes = BTreeMap::new();
        regimes.insert(
            first_day,
            AlphaSourceMarketRegime {
                trade_date: first_day,
                regime: "bull".to_string(),
                trailing_return: 0.20,
                annualized_volatility: 0.18,
                trailing_max_drawdown: 0.08,
            },
        );

        let summaries = regime_split_summaries_from_labeled_rows(20, &rows, &regimes, 2, 2);

        assert_eq!(summaries.len(), 2);
        assert!(summaries
            .iter()
            .any(|summary| summary.regime == "bull" && summary.labeled_rows == 2));
        assert!(summaries
            .iter()
            .any(|summary| summary.regime == "unknown" && summary.labeled_rows == 2));
    }

    #[test]
    fn stale_optimization_cleanup_defaults_are_safe_and_bounded() {
        assert!(cleanup_dry_run_default(None));
        assert!(!cleanup_dry_run_default(Some(false)));
        assert_eq!(cleanup_default_timeout_seconds(None), 3600);
        assert_eq!(cleanup_default_timeout_seconds(Some(10)), 60);
        assert_eq!(cleanup_default_timeout_seconds(Some(100_000)), 86_400);
        assert_eq!(cleanup_limit(None), 100);
        assert_eq!(cleanup_limit(Some(0)), 1);
        assert_eq!(cleanup_limit(Some(10_000)), 1000);
    }

    #[test]
    fn stale_optimization_cleanup_transitions_running_to_timeout_and_pending_to_cancelled() {
        assert_eq!(
            optimization_cleanup_task_status_transition("pending"),
            Some("cancelled")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("running"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("cancel_requested"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("completed"),
            None
        );
        assert_eq!(optimization_cleanup_task_status_transition("failed"), None);
        assert_eq!(
            optimization_cleanup_task_status_transition("cancelled"),
            None
        );
    }

    #[test]
    fn stale_cleanup_trial_and_experiment_transitions_are_terminal_only() {
        assert_eq!(
            optimization_cleanup_trial_status_transition("pending"),
            Some("cancelled")
        );
        assert_eq!(
            optimization_cleanup_trial_status_transition("running"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_trial_status_transition("cancel_requested"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_trial_status_transition("completed"),
            None
        );

        assert_eq!(
            experiment_cleanup_status_transition("pending"),
            Some("failed")
        );
        assert_eq!(
            experiment_cleanup_status_transition("running"),
            Some("failed")
        );
        assert_eq!(experiment_cleanup_status_transition("completed"), None);
        assert_eq!(experiment_cleanup_status_transition("partial"), None);
        assert_eq!(experiment_cleanup_status_transition("failed"), None);
    }

    fn report_trial(
        trial_id: &str,
        trial_index: i32,
        parameters: Value,
        metrics: Value,
    ) -> CompletedTrialSnapshot {
        CompletedTrialSnapshot {
            trial_id: trial_id.to_string(),
            trial_index,
            backtest_task_id: Some(format!("bt-{trial_id}")),
            score: None,
            metrics,
            metric_sources: json!({}),
            missing_elite_metrics: json!([]),
            constraint_violations: json!([]),
            parameters,
        }
    }

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
    fn oos_walk_forward_stage_progress_sets_current_stage_and_appends_history() {
        let base = oos_walk_forward_progress_metrics(1, &[], &json!({}), &json!({}));
        let stage = oos_walk_forward_stage(
            "train_window_ml_train_prediction_started",
            Some(1),
            json!({
                "prediction_set_id": "p7v19sx-w1-test-tr",
                "training_task_id": "train-p7v19sx-w1-test-tr"
            }),
        );

        let updated = append_oos_walk_forward_stage_metrics(base, stage.clone());

        assert_eq!(updated["progress_pct"], json!(0));
        assert_eq!(updated["completed_windows"], json!(0));
        assert_eq!(updated["current_stage"], stage);
        assert_eq!(updated["stage_history"].as_array().unwrap().len(), 1);

        let next_stage = oos_walk_forward_stage(
            "train_window_ml_test_prediction_ready",
            Some(1),
            json!({"prediction_set_id": "p7v19sx-w1-test-te"}),
        );
        let updated = append_oos_walk_forward_stage_metrics(updated, next_stage.clone());

        assert_eq!(updated["current_stage"], next_stage);
        assert_eq!(updated["stage_history"].as_array().unwrap().len(), 2);
        assert_eq!(
            updated["stage_history"][0]["stage"],
            json!("train_window_ml_train_prediction_started")
        );
        assert_eq!(
            updated["stage_history"][1]["stage"],
            json!("train_window_ml_test_prediction_ready")
        );
    }

    #[test]
    fn oos_walk_forward_plan_uses_non_overlapping_train_and_test_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(16),
            search_profile: Some("phase7_db".to_string()),
            trial_batch_limit: Some(4),
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: Some(4),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(1),
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_oos_discovery_plan(&req).expect("oos plan");

        assert_eq!(plan.validation_mode, "walk_forward");
        assert_eq!(plan.windows.len(), 2);
        assert!(plan.windows.iter().all(|window| {
            window.train_start <= window.train_end && window.train_end < window.test_start
        }));
        assert_eq!(
            plan.windows[1].train_start - plan.windows[0].train_start,
            Duration::days(365)
        );
    }

    #[test]
    fn missing_strategy_version_error_is_actionable() {
        let message = missing_strategy_version_error_message("strategy-missing-smoke");

        assert!(message.contains("strategy-missing-smoke"));
        assert!(message.contains("does not exist in strategy_version"));
        assert!(message.contains("existing canonical strategy_version_id"));
    }

    #[test]
    fn oos_holdout_plan_uses_single_in_sample_out_of_sample_split() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20210101"
            })),
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: Some("holdout_80_20".to_string()),
            in_sample_ratio: Some(0.80),
            include_partial_last_window: None,
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_oos_discovery_plan(&req).expect("holdout plan");

        assert_eq!(plan.windows.len(), 1);
        assert_eq!(plan.windows[0].validation_mode, "holdout_80_20");
        assert!(plan.windows[0].train_end < plan.windows[0].test_start);
        assert_eq!(plan.windows[0].test_end, plan.end_date);
    }

    #[test]
    fn stitched_oos_curve_compounds_only_test_window_returns() {
        let first = vec![
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                portfolio_value: 100.0,
                benchmark_value: Some(100.0),
            },
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(),
                portfolio_value: 110.0,
                benchmark_value: Some(105.0),
            },
        ];
        let second = vec![
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                portfolio_value: 200.0,
                benchmark_value: Some(100.0),
            },
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
                portfolio_value: 220.0,
                benchmark_value: Some(110.0),
            },
        ];
        let mut stitched = Vec::new();

        append_stitched_oos_points(&mut stitched, &first);
        append_stitched_oos_points(&mut stitched, &second);

        assert_eq!(stitched.len(), 3);
        assert!((stitched.last().unwrap().portfolio_value - 1.21).abs() < 1e-12);
    }

    #[test]
    fn oos_execution_mode_accepts_background_aliases() {
        assert_eq!(
            normalize_oos_execution_mode(Some("background")).unwrap(),
            "background"
        );
        assert_eq!(
            normalize_oos_execution_mode(Some("async")).unwrap(),
            "background"
        );
        assert_eq!(normalize_oos_execution_mode(None).unwrap(), "inline");
        assert!(normalize_oos_execution_mode(Some("manual")).is_err());
    }

    #[test]
    fn oos_train_execution_defaults_to_shared_cache_even_when_concurrency_requested() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        assert_eq!(
            normalize_oos_train_cache_mode(None).unwrap(),
            "shared_window"
        );
        let policy =
            resolve_oos_train_execution_policy(&req, &LocalResourcePlan::for_machine(10, 32))
                .unwrap();
        assert_eq!(policy.trial_concurrency, 1);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["train_trial_concurrency"], json!(1));
        assert_eq!(config["train_cache_mode"], json!("shared_window"));
    }

    #[test]
    fn oos_train_execution_allows_explicit_isolated_parallel_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("per_trial_isolated".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        assert_eq!(
            normalize_oos_train_cache_mode(Some("isolated")).unwrap(),
            "per_trial_isolated"
        );
        let policy =
            resolve_oos_train_execution_policy(&req, &LocalResourcePlan::for_machine(10, 32))
                .unwrap();
        assert_eq!(policy.trial_concurrency, 4);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["train_trial_concurrency"], json!(4));
        assert_eq!(config["train_cache_mode"], json!("per_trial_isolated"));
    }

    #[test]
    fn oos_train_execution_auto_cache_prefers_shared_for_small_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("auto".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.cache_mode, "shared_window");
        assert_eq!(policy.trial_concurrency, 1);
        assert_eq!(policy.requested_cache_mode, "auto");
    }

    #[test]
    fn oos_train_execution_auto_cache_uses_isolated_parallel_for_large_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(96),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(8),
            train_cache_mode: Some("auto".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.cache_mode, "per_trial_isolated");
        assert_eq!(policy.trial_concurrency, 8);
        assert_eq!(policy.requested_cache_mode, "auto");
    }

    #[test]
    fn oos_train_execution_accepts_explicit_shared_run_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("shared_run".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        assert_eq!(
            normalize_oos_train_cache_mode(Some("run_shared")).unwrap(),
            "shared_run"
        );
        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.requested_cache_mode, "shared_run");
        assert_eq!(policy.cache_mode, "shared_run");
        assert_eq!(policy.trial_concurrency, 1);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["trial_concurrency"], json!(1));
        assert_eq!(config["train_trial_concurrency"], json!(1));
        assert_eq!(config["train_cache_mode"], json!("shared_run"));
    }

    #[test]
    fn oos_train_batch_summary_labels_shared_run_cache_scope() {
        let policy = OosTrainExecutionPolicy {
            requested_cache_mode: "shared_run",
            cache_mode: "shared_run",
            requested_trial_concurrency: 4,
            trial_concurrency: 1,
        };
        let batch = annotate_oos_train_batch_cache_mode(
            json!({
                "executed": 2,
                "trial_concurrency": 1,
                "cache_mode": "shared_window",
            }),
            &policy,
        );

        assert_eq!(batch["cache_mode"], json!("shared_run"));
        assert_eq!(batch["cache_scope"], json!("run"));
        assert_eq!(batch["requested_cache_mode"], json!("shared_run"));
    }

    #[test]
    fn phase7_oos_profile_comparison_plan_defaults_to_ec_eu_ev_with_auto_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: Some(2),
            trial_concurrency: Some(4),
            train_cache_mode: None,
            max_batches_per_window: Some(1),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: None,
            execution_mode: Some("background".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(3),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan =
            build_phase7_oos_profile_comparison_plan(&req, None, None).expect("comparison plan");

        let profiles = plan["profiles"].as_array().expect("profiles");
        assert_eq!(profiles.len(), 3);
        assert_eq!(profiles[0]["search_profile"], json!("phase7_ec"));
        assert_eq!(profiles[1]["search_profile"], json!("phase7_eu"));
        assert_eq!(profiles[2]["search_profile"], json!("phase7_ev"));
        assert!(profiles.iter().all(|profile| {
            profile["request"]["plan_only"] == json!(true)
                && profile["request"]["execution_mode"] == json!("inline")
                && profile["request"]["train_cache_mode"] == json!("auto")
                && profile["plan"]["window_count"] == json!(2)
                && profile["config"]["train_cache_mode"] == json!("shared_window")
        }));
    }

    #[test]
    fn phase7_oos_profile_comparison_launch_requests_are_bounded_background_smokes() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let requests = build_phase7_oos_profile_comparison_launch_requests(&req, None, None)
            .expect("launch reqs");

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].search_profile.as_deref(), Some("phase7_ec"));
        assert_eq!(requests[1].search_profile.as_deref(), Some("phase7_eu"));
        assert_eq!(requests[2].search_profile.as_deref(), Some("phase7_ev"));
        assert!(requests.iter().all(|request| {
            request.plan_only == Some(false)
                && request.execution_mode.as_deref() == Some("background")
                && request.train_cache_mode.as_deref() == Some("auto")
                && request.max_trials_per_window == Some(8)
                && request.trial_batch_limit == Some(8)
                && request.max_batches_per_window == Some(1)
                && request.oos_top_n == Some(3)
        }));
    }

    #[test]
    fn phase7_oos_profile_comparison_plan_can_build_raw_stats_cache_pairs() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_phase7_oos_profile_comparison_plan(
            &req,
            Some(vec!["phase7_er".to_string()]),
            Some(true),
        )
        .expect("paired comparison plan");

        let profiles = plan["profiles"].as_array().expect("profiles");
        assert_eq!(plan["profile_count"], json!(1));
        assert_eq!(plan["launch_count"], json!(2));
        assert_eq!(profiles.len(), 2);
        assert_eq!(
            plan["return_risk_cache_comparison"]["report_endpoint"],
            json!("/api/v1/quant/experiments/return-risk-cache-economics/report")
        );
        assert_eq!(profiles[0]["search_profile"], json!("phase7_er"));
        assert_eq!(
            profiles[0]["return_risk_feature_cache_mode"],
            json!("raw_matrix")
        );
        assert_eq!(
            profiles[0]["request"]["return_risk_feature_cache_mode"],
            json!("raw_matrix")
        );
        assert_eq!(
            profiles[1]["return_risk_feature_cache_mode"],
            json!("stats_matrix_experimental")
        );
        assert_eq!(
            profiles[1]["request"]["return_risk_feature_cache_mode"],
            json!("stats_matrix_experimental")
        );
    }

    #[test]
    fn phase7_oos_profile_comparison_launch_requests_can_build_raw_stats_cache_pairs() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let requests = build_phase7_oos_profile_comparison_launch_requests(
            &req,
            Some(vec!["phase7_er".to_string()]),
            Some(true),
        )
        .expect("paired launch reqs");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].search_profile.as_deref(), Some("phase7_er"));
        assert_eq!(requests[0].plan_only, Some(false));
        assert_eq!(requests[0].execution_mode.as_deref(), Some("background"));
        assert_eq!(
            requests[0]
                .backtest_template
                .as_ref()
                .and_then(|template| template.get("return_risk_feature_cache_mode")),
            Some(&json!("raw_matrix"))
        );
        assert_eq!(
            requests[1]
                .backtest_template
                .as_ref()
                .and_then(|template| template.get("return_risk_feature_cache_mode")),
            Some(&json!("stats_matrix_experimental"))
        );
    }

    #[test]
    fn oos_gate_report_rejects_weak_cost_capacity_perturbations() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(16),
            search_profile: Some("phase7_db".to_string()),
            trial_batch_limit: Some(4),
            trial_concurrency: Some(2),
            train_cache_mode: None,
            max_batches_per_window: Some(4),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: Some(1.2),
            min_positive_oos_window_ratio: Some(0.5),
            min_oos_window_count: Some(1),
            oos_top_n: Some(1),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: Some(vec![OosCostCapacityPerturbationRequest {
                name: Some("cost_up".to_string()),
                cost_multiplier: Some(1.5),
                slippage_bps: Some(0.0002),
                impact_cost_coefficient: None,
                max_participation_rate: None,
                capacity_penalty_strength: None,
            }]),
            min_cost_capacity_perturbation_pass_ratio: Some(0.75),
            min_perturbed_oos_calmar: Some(1.0),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };
        let plan = build_oos_discovery_plan(&req).expect("plan");
        let window_results = vec![
            json!({
                "oos_metrics": {"annual_return_pct": 0.10},
                "train_robustness": {"status": "approved_candidate"},
                "cost_capacity_perturbations": [
                    {"name": "cost_up", "passed": true}
                ]
            }),
            json!({
                "oos_metrics": {"annual_return_pct": 0.08},
                "train_robustness": {"status": "approved_candidate"},
                "cost_capacity_perturbations": [
                    {"name": "cost_up", "passed": false}
                ]
            }),
        ];

        let gates = build_oos_gate_report(
            &req,
            &plan,
            &window_results,
            &json!({"calmar_ratio": 1.4}),
            &resolve_oos_final_promotion_gate_policy(&req, &plan.validation_mode),
        );

        let perturbation_gate = gates
            .as_array()
            .unwrap()
            .iter()
            .find(|gate| gate["gate"] == "cost_capacity_perturbation_pass_ratio")
            .expect("cost/capacity gate");
        assert_eq!(perturbation_gate["passed"], json!(false));
        assert_eq!(perturbation_gate["actual"], json!(0.5));
    }

    #[test]
    fn train_selection_cost_capacity_gate_is_policy_scoped() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };
        let train_policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "min_train_cost_capacity_perturbation_pass_ratio": 0.67,
            "min_train_perturbed_calmar": 0.50,
            "max_train_perturbed_drawdown_pct": 0.45
        });

        let gate = train_cost_capacity_perturbation_gate_config(&req, &train_policy);

        assert!(gate.enabled);
        assert_eq!(gate.min_pass_ratio, 0.67);
        assert_eq!(gate.min_perturbed_calmar, 0.50);
        assert_eq!(gate.max_perturbed_drawdown_pct, 0.45);
    }

    #[test]
    fn train_cost_capacity_stress_aware_selection_defaults_on_when_gate_enabled() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.80,
            min_perturbed_calmar: 1.2,
            max_perturbed_drawdown_pct: 0.35,
        };

        assert!(train_cost_capacity_stress_aware_selection_enabled(
            &json!({}),
            &gate
        ));
        assert!(!train_cost_capacity_stress_aware_selection_enabled(
            &json!({"enable_train_cost_capacity_stress_aware_selection": false}),
            &gate
        ));
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_capacity_stress_calmar_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "capacity_stress_calmar_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "capacity_stress_calmar_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_cash_drag_fill_gap_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "cash_drag_fill_gap_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "cash_drag_fill_gap_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_capacity_stress_return_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "capacity_stress_return_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "capacity_stress_return_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_stress_fill_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "stress_fill_objective_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "stress_fill_objective_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_prediction_confidence_stress_fill_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "prediction_confidence_stress_fill_objective_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "prediction_confidence_stress_fill_objective_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_prediction_confidence_stress_fill_quality_objective(
    ) {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "prediction_confidence_stress_fill_quality_score_v1"
        );
    }

    #[test]
    fn cash_drag_fill_gap_score_prefers_filled_candidate_when_capacity_ties() {
        let candidate = |trial_id: &str,
                         final_cash_weight: Decimal,
                         target_gap: Decimal,
                         expired_count: u64|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(28, 2),
                    excess_return: Decimal::new(10, 2),
                    sharpe: Decimal::new(18, 1),
                    sortino: Decimal::new(28, 1),
                    max_drawdown: Decimal::new(12, 2),
                    final_cash_weight,
                    max_execution_target_gap: target_gap,
                    execution_schedule_expired_count: expired_count,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut cashized_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        cashized_summary.min_calmar = Decimal::new(150, 2);
        cashized_summary.avg_calmar = Decimal::new(180, 2);
        cashized_summary.avg_sharpe = Decimal::new(160, 2);
        cashized_summary.min_annual_return = Decimal::new(12, 2);
        cashized_summary.max_drawdown = Decimal::new(12, 2);
        cashized_summary.max_final_cash_weight = Decimal::new(55, 2);
        cashized_summary.avg_final_cash_weight = Decimal::new(42, 2);
        cashized_summary.max_execution_target_gap = Decimal::new(18, 2);
        cashized_summary.total_execution_schedule_expired_count = 5;
        let mut filled_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        filled_summary.min_calmar = cashized_summary.min_calmar;
        filled_summary.avg_calmar = cashized_summary.avg_calmar;
        filled_summary.avg_sharpe = cashized_summary.avg_sharpe;
        filled_summary.min_annual_return = cashized_summary.min_annual_return;
        filled_summary.max_drawdown = cashized_summary.max_drawdown;
        filled_summary.max_final_cash_weight = Decimal::new(8, 2);
        filled_summary.avg_final_cash_weight = Decimal::new(5, 2);
        filled_summary.max_execution_target_gap = Decimal::new(3, 2);
        filled_summary.total_execution_schedule_expired_count = 0;
        let policy = json!({
            "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "max_train_final_cash_weight_pct": 0.20,
            "max_train_execution_target_gap_pct": 0.08
        });

        let cashized_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("cashized", Decimal::new(60, 2), Decimal::new(20, 2), 4),
            &cashized_summary,
            &policy,
        );
        let filled_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("filled", Decimal::new(6, 2), Decimal::new(3, 2), 0),
            &filled_summary,
            &policy,
        );

        assert!(
            filled_score > cashized_score,
            "filled_score={filled_score}, cashized_score={cashized_score}"
        );
    }

    #[test]
    fn train_cash_drag_fill_gap_gate_rejects_cashized_train_candidate() {
        let candidate = DiscoveryCandidate {
            trial_id: "cashized".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(28, 2),
                sharpe: Decimal::new(18, 1),
                final_cash_weight: Decimal::new(35, 2),
                max_execution_target_gap: Decimal::new(18, 2),
                execution_schedule_expired_count: 2,
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.max_final_cash_weight = Decimal::new(60, 2);
        summary.max_execution_target_gap = Decimal::new(21, 2);
        summary.max_execution_schedule_expired_count = 4;
        let policy = json!({
            "max_train_final_cash_weight_pct": 0.20,
            "max_train_execution_target_gap_pct": 0.08,
            "max_train_execution_schedule_expired_count": 0
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight"
                && gate["passed"] == false
                && gate["actual"] == json!("0.60")));
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_execution_target_gap" && gate["passed"] == false));
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |gate| gate["gate"] == "train_execution_schedule_expired_count"
                    && gate["passed"] == false
            ));
    }

    #[test]
    fn train_fill_ratio_gate_allows_intentional_cash_when_target_is_filled() {
        let candidate = DiscoveryCandidate {
            trial_id: "intentional_cash".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(18, 2),
                sharpe: Decimal::new(12, 1),
                final_cash_weight: Decimal::new(65, 2),
                final_target_gross_exposure: Decimal::new(35, 2),
                final_actual_gross_exposure: Decimal::new(35, 2),
                final_unfilled_target_gap: Decimal::ZERO,
                final_execution_fill_ratio: Decimal::ONE,
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.max_final_cash_weight = Decimal::new(70, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(1, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(98, 2);
        let policy = json!({
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(passed);
        assert_eq!(robustness["status"], "approved_candidate");
        let gates = robustness["gate_results"].as_array().unwrap();
        assert!(gates.iter().any(
            |gate| gate["gate"] == "train_final_unfilled_target_gap" && gate["passed"] == true
        ));
        assert!(gates.iter().any(
            |gate| gate["gate"] == "train_final_execution_fill_ratio" && gate["passed"] == true
        ));
        assert!(!gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight"));
    }

    #[test]
    fn train_execution_quality_gate_rejects_zero_trade_low_exposure_candidate() {
        let candidate = DiscoveryCandidate {
            trial_id: "empty-exposure".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::ZERO,
                sharpe: Decimal::ZERO,
                num_trades: 0,
                final_cash_weight: Decimal::new(90, 2),
                final_actual_gross_exposure: Decimal::new(10, 2),
                final_unfilled_target_gap: Decimal::new(2, 2),
                final_execution_fill_ratio: Decimal::new(95, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_num_trades = 0;
        summary.max_final_cash_weight = Decimal::new(92, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(8, 2);
        let policy = json!({
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.25,
            "max_train_final_cash_weight_pct": 0.75,
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        let gates = robustness["gate_results"].as_array().unwrap();
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_trade_count" && gate["passed"] == false));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_actual_gross_exposure"
                && gate["passed"] == false
                && gate["actual"] == json!("0.08")));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight" && gate["passed"] == false));
    }

    #[test]
    fn capacity_stress_return_score_penalizes_low_trade_low_exposure_candidates() {
        let candidate = |trial_id: &str,
                         num_trades: u64,
                         final_cash_weight: Decimal,
                         final_actual_gross_exposure: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(20, 2),
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(18, 1),
                    max_drawdown: Decimal::new(20, 2),
                    num_trades,
                    final_cash_weight,
                    final_actual_gross_exposure,
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(95, 2),
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut low_exposure_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        low_exposure_summary.min_calmar = Decimal::new(150, 2);
        low_exposure_summary.avg_calmar = Decimal::new(170, 2);
        low_exposure_summary.avg_sharpe = Decimal::new(120, 2);
        low_exposure_summary.min_annual_return = Decimal::new(8, 2);
        low_exposure_summary.max_drawdown = Decimal::new(14, 2);
        low_exposure_summary.min_num_trades = 0;
        low_exposure_summary.max_final_cash_weight = Decimal::new(90, 2);
        low_exposure_summary.min_final_actual_gross_exposure = Decimal::new(10, 2);
        let mut tradable_summary = low_exposure_summary.clone();
        tradable_summary.min_num_trades = 260;
        tradable_summary.max_final_cash_weight = Decimal::new(28, 2);
        tradable_summary.min_final_actual_gross_exposure = Decimal::new(70, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_return_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.25,
            "max_train_final_cash_weight_pct": 0.75,
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let low_exposure_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("low-exposure", 0, Decimal::new(90, 2), Decimal::new(10, 2)),
            &low_exposure_summary,
            &policy,
        );
        let tradable_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("tradable", 260, Decimal::new(28, 2), Decimal::new(72, 2)),
            &tradable_summary,
            &policy,
        );

        assert!(
            tradable_score > low_exposure_score,
            "tradable_score={tradable_score}, low_exposure_score={low_exposure_score}"
        );
    }

    #[test]
    fn stress_fill_objective_score_prefers_filled_exposure_over_cashized_return() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         num_trades: u64,
                         final_cash_weight: Decimal,
                         final_actual_gross_exposure: Decimal,
                         final_unfilled_target_gap: Decimal,
                         final_execution_fill_ratio: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(18, 1),
                    max_drawdown: Decimal::new(20, 2),
                    num_trades,
                    final_cash_weight,
                    final_actual_gross_exposure,
                    final_unfilled_target_gap,
                    final_execution_fill_ratio,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut cashized_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        cashized_summary.min_calmar = Decimal::new(160, 2);
        cashized_summary.avg_calmar = Decimal::new(180, 2);
        cashized_summary.avg_sharpe = Decimal::new(130, 2);
        cashized_summary.min_annual_return = Decimal::new(18, 2);
        cashized_summary.max_drawdown = Decimal::new(18, 2);
        cashized_summary.min_num_trades = 80;
        cashized_summary.max_final_cash_weight = Decimal::new(82, 2);
        cashized_summary.min_final_actual_gross_exposure = Decimal::new(18, 2);
        cashized_summary.max_final_unfilled_target_gap = Decimal::new(18, 2);
        cashized_summary.min_final_execution_fill_ratio = Decimal::new(78, 2);
        let mut filled_summary = cashized_summary.clone();
        filled_summary.min_annual_return = Decimal::new(11, 2);
        filled_summary.min_num_trades = 240;
        filled_summary.max_final_cash_weight = Decimal::new(22, 2);
        filled_summary.min_final_actual_gross_exposure = Decimal::new(78, 2);
        filled_summary.max_final_unfilled_target_gap = Decimal::new(2, 2);
        filled_summary.min_final_execution_fill_ratio = Decimal::new(98, 2);
        let policy = json!({
            "train_stress_score_profile": "stress_fill_objective_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let cashized_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "cashized-high-return",
                Decimal::new(30, 2),
                80,
                Decimal::new(82, 2),
                Decimal::new(18, 2),
                Decimal::new(18, 2),
                Decimal::new(78, 2),
            ),
            &cashized_summary,
            &policy,
        );
        let filled_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "filled-lower-return",
                Decimal::new(18, 2),
                240,
                Decimal::new(22, 2),
                Decimal::new(78, 2),
                Decimal::new(2, 2),
                Decimal::new(98, 2),
            ),
            &filled_summary,
            &policy,
        );

        assert!(
            filled_score > cashized_score,
            "filled_score={filled_score}, cashized_score={cashized_score}"
        );
    }

    #[test]
    fn prediction_confidence_stress_fill_objective_prefers_positive_prediction_gate() {
        let candidate = |trial_id: &str, parameters: Value| -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(16, 2),
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(19, 1),
                    max_drawdown: Decimal::new(18, 2),
                    num_trades: 240,
                    final_cash_weight: Decimal::new(24, 2),
                    final_actual_gross_exposure: Decimal::new(76, 2),
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(98, 2),
                    ..CandidateMetrics::default()
                },
                parameters,
            }
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(150, 2);
        summary.avg_calmar = Decimal::new(180, 2);
        summary.avg_sharpe = Decimal::new(130, 2);
        summary.min_annual_return = Decimal::new(10, 2);
        summary.max_drawdown = Decimal::new(18, 2);
        summary.min_num_trades = 220;
        summary.max_final_cash_weight = Decimal::new(26, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(74, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(3, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_objective_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let unconfirmed_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("unconfirmed", json!({})),
            &summary,
            &policy,
        );
        let confirmed_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "positive-confirmed",
                json!({
                    "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                    "prediction_min_score": "0.01",
                    "prediction_min_percentile": "0.30",
                    "prediction_set_override_source": "train_window_ml_internal"
                }),
            ),
            &summary,
            &policy,
        );

        assert!(
            confirmed_score > unconfirmed_score,
            "confirmed_score={confirmed_score}, unconfirmed_score={unconfirmed_score}"
        );
    }

    #[test]
    fn prediction_confidence_stress_fill_quality_objective_prefers_left_tail_resilience() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         sortino: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(8, 2),
                    sharpe,
                    sortino,
                    max_drawdown,
                    num_trades: 260,
                    final_cash_weight: Decimal::new(24, 2),
                    final_actual_gross_exposure: Decimal::new(76, 2),
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(98, 2),
                    ..CandidateMetrics::default()
                },
                parameters: json!({
                    "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                    "prediction_min_score": "0.01",
                    "prediction_min_percentile": "0.35",
                    "prediction_set_override_source": "train_window_ml_internal"
                }),
            }
        };
        let mut high_return_fragile = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_return_fragile.min_calmar = Decimal::new(45, 2);
        high_return_fragile.avg_calmar = Decimal::new(80, 2);
        high_return_fragile.avg_sharpe = Decimal::new(40, 2);
        high_return_fragile.min_annual_return = Decimal::new(-3, 2);
        high_return_fragile.max_drawdown = Decimal::new(24, 2);
        high_return_fragile.min_num_trades = 240;
        high_return_fragile.max_final_cash_weight = Decimal::new(26, 2);
        high_return_fragile.min_final_actual_gross_exposure = Decimal::new(74, 2);
        high_return_fragile.max_final_unfilled_target_gap = Decimal::new(3, 2);
        high_return_fragile.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let mut lower_return_resilient = high_return_fragile.clone();
        lower_return_resilient.passed_count = 3;
        lower_return_resilient.pass_ratio_ppm = 1_000_000;
        lower_return_resilient.min_calmar = Decimal::new(140, 2);
        lower_return_resilient.avg_calmar = Decimal::new(165, 2);
        lower_return_resilient.avg_sharpe = Decimal::new(120, 2);
        lower_return_resilient.min_annual_return = Decimal::new(9, 2);
        lower_return_resilient.max_drawdown = Decimal::new(16, 2);
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_perturbed_calmar": 1.2,
            "min_train_trade_count": 100,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-return-fragile",
                Decimal::new(34, 2),
                Decimal::new(18, 1),
                Decimal::new(22, 1),
                Decimal::new(12, 2),
            ),
            &high_return_fragile,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-return-resilient",
                Decimal::new(18, 2),
                Decimal::new(12, 1),
                Decimal::new(17, 1),
                Decimal::new(18, 2),
            ),
            &lower_return_resilient,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn capacity_stress_calmar_score_prefers_capacity_resilience_when_pass_ratio_ties() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(10, 2),
                    sharpe,
                    sortino: Decimal::new(18, 1),
                    max_drawdown,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut high_return_fragile = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_return_fragile.min_calmar = Decimal::new(30, 2);
        high_return_fragile.avg_calmar = Decimal::new(75, 2);
        high_return_fragile.avg_sharpe = Decimal::new(90, 2);
        high_return_fragile.min_annual_return = Decimal::new(-5, 2);
        high_return_fragile.max_drawdown = Decimal::new(30, 2);
        let mut lower_return_resilient = cost_capacity_perturbation_summary_from_counts(2, 3);
        lower_return_resilient.min_calmar = Decimal::new(140, 2);
        lower_return_resilient.avg_calmar = Decimal::new(180, 2);
        lower_return_resilient.avg_sharpe = Decimal::new(160, 2);
        lower_return_resilient.min_annual_return = Decimal::new(7, 2);
        lower_return_resilient.max_drawdown = Decimal::new(18, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_calmar_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "capacity_stress_target_annual_return": 0.05
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-return-fragile",
                Decimal::new(36, 2),
                Decimal::new(24, 1),
                Decimal::new(10, 2),
            ),
            &high_return_fragile,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-return-resilient",
                Decimal::new(22, 2),
                Decimal::new(16, 1),
                Decimal::new(16, 2),
            ),
            &lower_return_resilient,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn capacity_stress_return_score_prefers_perturbed_return_resilience() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(10, 2),
                    sharpe,
                    sortino: Decimal::new(18, 1),
                    max_drawdown,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut high_base_low_stress = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_base_low_stress.min_calmar = Decimal::new(125, 2);
        high_base_low_stress.avg_calmar = Decimal::new(170, 2);
        high_base_low_stress.avg_sharpe = Decimal::new(160, 2);
        high_base_low_stress.min_annual_return = Decimal::new(2, 2);
        high_base_low_stress.max_drawdown = Decimal::new(16, 2);
        let mut lower_base_high_stress = cost_capacity_perturbation_summary_from_counts(2, 3);
        lower_base_high_stress.min_calmar = Decimal::new(120, 2);
        lower_base_high_stress.avg_calmar = Decimal::new(165, 2);
        lower_base_high_stress.avg_sharpe = Decimal::new(155, 2);
        lower_base_high_stress.min_annual_return = Decimal::new(12, 2);
        lower_base_high_stress.max_drawdown = Decimal::new(17, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_return_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "capacity_stress_target_annual_return": 0.15
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-base-low-stress",
                Decimal::new(34, 2),
                Decimal::new(24, 1),
                Decimal::new(10, 2),
            ),
            &high_base_low_stress,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-base-high-stress",
                Decimal::new(24, 2),
                Decimal::new(16, 1),
                Decimal::new(16, 2),
            ),
            &lower_base_high_stress,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn train_capacity_stress_return_gate_rejects_low_perturbed_annual_return() {
        let candidate = DiscoveryCandidate {
            trial_id: "stress-return-fragile".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(28, 2),
                sharpe: Decimal::new(18, 1),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(140, 2);
        summary.avg_calmar = Decimal::new(170, 2);
        summary.min_annual_return = Decimal::new(2, 2);
        let policy = json!({
            "min_train_perturbed_annual_return": 0.05,
            "min_train_avg_perturbed_calmar": 1.2
        });

        let (robustness, passed) = attach_train_capacity_stress_return_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_perturbed_annual_return"
                && gate["passed"] == false
                && gate["actual"] == json!("0.02")));
    }

    #[test]
    fn train_candidate_evaluation_order_prefers_stress_resilience() {
        let base_candidate = |trial_id: &str, annual_return: Decimal| DiscoveryCandidate {
            trial_id: trial_id.to_string(),
            backtest_task_id: None,
            score: None,
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(25, 2),
            metrics: CandidateMetrics {
                annual_return,
                excess_return: Decimal::new(10, 2),
                sharpe: Decimal::new(90, 2),
                sortino: Decimal::new(16, 1),
                max_drawdown: Decimal::new(25, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut fragile_summary = cost_capacity_perturbation_summary_from_counts(1, 3);
        fragile_summary.min_calmar = Decimal::new(36, 2);
        fragile_summary.max_drawdown = Decimal::new(9, 2);
        let mut resilient_summary = cost_capacity_perturbation_summary_from_counts(2, 3);
        resilient_summary.min_calmar = Decimal::new(125, 2);
        resilient_summary.max_drawdown = Decimal::new(12, 2);
        let fragile = OosTrainCandidateEvaluation {
            candidate: base_candidate("high-return-fragile", Decimal::new(32, 2)),
            robustness: json!({"status": "rejected"}),
            train_cost_capacity_perturbations: Vec::new(),
            stress_adjusted_score: train_candidate_stress_adjusted_score(
                &base_candidate("high-return-fragile", Decimal::new(32, 2)),
                &fragile_summary,
            ),
            stress_summary: fragile_summary,
            train_cost_gate_passed: false,
        };
        let resilient = OosTrainCandidateEvaluation {
            candidate: base_candidate("lower-return-resilient", Decimal::new(24, 2)),
            robustness: json!({"status": "approved_candidate"}),
            train_cost_capacity_perturbations: Vec::new(),
            stress_adjusted_score: train_candidate_stress_adjusted_score(
                &base_candidate("lower-return-resilient", Decimal::new(24, 2)),
                &resilient_summary,
            ),
            stress_summary: resilient_summary,
            train_cost_gate_passed: true,
        };
        let mut evaluations = vec![fragile, resilient];

        evaluations.sort_by(train_candidate_evaluation_order);

        assert_eq!(evaluations[0].candidate.trial_id, "lower-return-resilient");
        assert!(evaluations[0].stress_adjusted_score > evaluations[1].stress_adjusted_score);
    }

    #[test]
    fn train_cost_capacity_gate_report_rejects_weak_pass_ratio() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.75,
            min_perturbed_calmar: 1.0,
            max_perturbed_drawdown_pct: 0.35,
        };
        let report = build_cost_capacity_pass_ratio_gate(
            "train_cost_capacity_perturbation_pass_ratio",
            1,
            2,
            &gate,
        );

        assert_eq!(report["passed"], json!(false));
        assert_eq!(report["actual"], json!(0.5));
        assert_eq!(report["limit"], json!(0.75));
    }

    #[test]
    fn train_cost_capacity_perturbations_default_when_train_gate_enabled_without_final_gate() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_fo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let train_gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fo"));

        let perturbations = resolved_train_cost_capacity_perturbations(&req, &train_gate_policy);

        assert_eq!(perturbations.len(), 3);
        assert_eq!(perturbations[0].name.as_deref(), Some("cost_up_150pct"));
        assert_eq!(
            perturbations[1].name.as_deref(),
            Some("impact_cost_2pct_participation_10pct")
        );
        assert_eq!(
            perturbations[2].name.as_deref(),
            Some("capacity_tight_participation_5pct")
        );
        assert!(
            resolved_oos_cost_capacity_perturbations(&req).is_empty(),
            "final/OOS perturbation gate should remain opt-in for final promotion"
        );
    }

    #[test]
    fn oos_experiment_config_reports_train_perturbations_independent_of_final_gate() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(6),
            search_profile: Some("phase7_fo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["enabled"],
            true
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["perturbations"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(config["cost_capacity_perturbation_gate"]["enabled"], false);
        assert_eq!(
            config["cost_capacity_perturbation_gate"]["perturbations"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn train_cost_capacity_gate_rejects_candidate_robustness() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.75,
            min_perturbed_calmar: 1.0,
            max_perturbed_drawdown_pct: 0.35,
        };
        let (robustness, passed) = attach_train_cost_capacity_gate_to_robustness(
            json!({
                "status": "approved_candidate",
                "gate_results": []
            }),
            1,
            2,
            &gate,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], json!("rejected"));
        assert_eq!(
            robustness["gate_results"][0]["gate"],
            json!("train_cost_capacity_perturbation_pass_ratio")
        );
        assert_eq!(robustness["gate_results"][0]["passed"], json!(false));
    }

    #[test]
    fn train_cost_capacity_overlay_persistence_fields_use_updated_status_and_gates() {
        let fields = train_cost_capacity_overlay_persistence_fields(&json!({
            "gate_result_id": "gate-train-cost",
            "status": "rejected",
            "gate_results": [
                {
                    "gate": "train_cost_capacity_perturbation_pass_ratio",
                    "passed": false
                }
            ]
        }))
        .expect("persistence fields");

        assert_eq!(fields.gate_result_id, "gate-train-cost");
        assert_eq!(fields.status, "rejected");
        assert_eq!(
            fields.gate_results[0]["gate"],
            json!("train_cost_capacity_perturbation_pass_ratio")
        );
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
            "prediction_min_percentile": {"type": "choice", "values": [null, 0.1, 0.2, 0.3]},
            "prediction_min_score": {"type": "choice", "values": [null, -0.01, 0.0, 0.01]}
        });

        let trials = generate_trial_parameters(&search_space, 20260520, 16).expect("trial params");
        let unique_pairs = trials
            .iter()
            .map(|params| {
                format!(
                    "{}|{}|{}",
                    params
                        .get("prediction_blend_weight")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into()),
                    params
                        .get("prediction_min_percentile")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into()),
                    params
                        .get("prediction_min_score")
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
    fn trial_backtest_request_blocks_industry_prosperity_without_market_scope_gate() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_industry_prosperity_proxy_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "universe_profile": "listed_non_st"
        });

        let err = build_factor_trial_request(&task, &params).unwrap_err();

        assert!(err.contains("phase7_industry_membership_market_scope_gate_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("科创板"));
    }

    #[test]
    fn trial_backtest_request_allows_industry_prosperity_with_market_scope_gate_profile() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_industry_prosperity_proxy_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "universe_profile": "main_chinext_non_st",
            "alpha_admission_gate_id": "phase7_industry_membership_market_scope_gate_v1"
        });

        let req = build_factor_trial_request(&task, &params).expect("market-scope gated request");

        assert_eq!(req.combo_name, "phase7_industry_prosperity_proxy_v1");
        assert_eq!(req.universe_profile.as_deref(), Some("main_chinext_non_st"));
    }

    #[test]
    fn trial_backtest_request_blocks_equity_pledge_without_coverage_gate() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "equity_pledge_pressure",
                "version": "p320f-equity-pledge-pressure-v1",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "universe_profile": "listed_non_st"
        });

        let err = build_factor_trial_request(&task, &params).unwrap_err();

        assert!(err.contains("equity_pledge_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn trial_backtest_request_allows_equity_pledge_with_coverage_gate_profile() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "equity_pledge_pressure",
                "version": "p320f-equity-pledge-pressure-v1",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 8,
            "universe_profile": "main_chinext_non_st",
            "alpha_admission_gate_id": "equity_pledge_coverage_ready_v1"
        });

        let req = build_factor_trial_request(&task, &params).expect("coverage-gated request");

        assert_eq!(req.combo_name, "equity_pledge_pressure");
        assert_eq!(req.universe_profile.as_deref(), Some("main_chinext_non_st"));
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
        let mut params = json!({
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
            "capacity_risk_budget": "capacity_participation_strict_v1",
            "execution_impact_budget": "impact_turnover_20pct_v1",
            "style_risk_budget": "defensive_style_budget_v1",
            "candidate_risk_filter": "low_volatility_low_correlation_v1",
            "score_candidate_pool_size": 300,
            "universe_profile": "listed_non_st",
            "prediction_set_id": "pred-quality-growth-v1",
            "prediction_blend_weight": 0.35,
            "event_gate_combo_name": "phase7_event_window_earnings_v1",
            "event_gate_mode": "boost_positive",
            "event_gate_min_score": 0.0,
            "event_gate_boost_weight": 0.05,
            "event_gate_score_direction": "descending",
            "market_regime": "quality_crash_guard_v3",
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
            "portfolio_volatility_max_exposure": 1.0,
            "stop_loss_pct": 0.12,
            "take_profit_pct": null,
            "trailing_stop_pct": 0.18,
            "time_stop_days": "120",
            "reentry_cooldown_days": "10"
        });
        params["prediction_min_score"] = json!(0.0);
        params["execution_schedule_profile"] = json!("twap_5d_v1");
        params["execution_carry_policy"] = json!("roll_forward_v1");
        params["return_risk_feature_cache_mode"] = json!("stats_matrix_experimental");

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
        assert_eq!(
            req.capacity_risk_budget.as_deref(),
            Some("capacity_participation_strict_v1")
        );
        assert_eq!(
            req.execution_impact_budget.as_deref(),
            Some("impact_turnover_20pct_v1")
        );
        assert_eq!(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_schedule_profile.as_deref()),
            Some("twap_5d_v1")
        );
        assert_eq!(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_carry_policy.as_deref()),
            Some("roll_forward_v1")
        );
        assert_eq!(
            req.style_risk_budget.as_deref(),
            Some("defensive_style_budget_v1")
        );
        assert_eq!(
            req.candidate_risk_filter.as_deref(),
            Some("low_volatility_low_correlation_v1")
        );
        assert_eq!(req.score_candidate_pool_size, Some(300));
        assert_eq!(req.universe_profile.as_deref(), Some("listed_non_st"));
        assert_eq!(
            req.prediction_set_id.as_deref(),
            Some("pred-quality-growth-v1")
        );
        assert_eq!(req.prediction_blend_weight, Some(0.35));
        assert_eq!(req.prediction_min_score, Some(0.0));
        assert_eq!(
            req.event_gate_combo_name.as_deref(),
            Some("phase7_event_window_earnings_v1")
        );
        assert_eq!(req.event_gate_mode.as_deref(), Some("boost_positive"));
        assert_eq!(req.event_gate_min_score, Some(0.0));
        assert_eq!(req.event_gate_boost_weight, Some(0.05));
        assert_eq!(
            req.event_gate_score_direction.as_deref(),
            Some("descending")
        );
        assert_eq!(
            req.market_regime.as_ref().and_then(|policy| policy.enabled),
            Some(true)
        );
        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_crash_guard_v3")
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
        assert_eq!(req.stop_loss_pct, Some(0.12));
        assert_eq!(req.take_profit_pct, None);
        assert_eq!(req.trailing_stop_pct, Some(0.18));
        assert_eq!(req.time_stop_days, Some(120));
        assert_eq!(req.reentry_cooldown_days, Some(10));
        assert_eq!(req.persistence_mode.as_deref(), Some("summary_only"));
        assert_eq!(
            req.return_risk_feature_cache_mode.as_deref(),
            Some("stats_matrix_experimental")
        );
        let coverage = req
            .effective_coverage
            .as_ref()
            .expect("optimization requests should auto-enable effective coverage");
        assert_eq!(coverage.enabled, Some(true));
        assert_eq!(coverage.mode.as_deref(), Some("adjust_start"));
        assert_eq!(coverage.min_rows, Some(8));
        assert_eq!(coverage.include_rebalance_warmup, Some(true));
        assert_eq!(coverage.warmup_trading_days, Some(19));
    }

    #[test]
    fn trial_backtest_request_allows_effective_coverage_override() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20160201",
                "end_date": "20260515",
                "benchmark": "000300.SH",
                "top_n": 20,
                "effective_coverage": {
                    "enabled": true,
                    "mode": "guard_only",
                    "min_rows": 120
                }
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 30,
            "effective_coverage": {
                "enabled": true,
                "mode": "adjust_start",
                "min_rows": 180
            }
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        let coverage = req.effective_coverage.expect("effective coverage");
        assert_eq!(coverage.enabled, Some(true));
        assert_eq!(coverage.mode.as_deref(), Some("adjust_start"));
        assert_eq!(coverage.min_rows, Some(180));
        assert_eq!(coverage.include_rebalance_warmup, Some(true));
        assert_eq!(coverage.warmup_trading_days, Some(19));
    }

    #[test]
    fn trial_backtest_request_passes_cost_and_execution_rules() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "cost_model": {
                    "commission_rate": 0.0003,
                    "min_commission": 5.0,
                    "tax_rate": 0.001,
                    "slippage_bps": 0.0001,
                    "cost_multiplier": 1.0,
                    "impact_cost_coefficient": 0.01
                },
                "execution_rules": {
                    "execution_timing": "next_open",
                    "execution_price": "next_open",
                    "max_participation_rate": 0.10
                }
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "cost_model": {
                "cost_multiplier": 1.5,
                "slippage_bps": 0.0002
            },
            "execution_rules": {
                "max_participation_rate": 0.05
            }
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        let cost = req.cost_model.expect("cost model should be propagated");
        assert_eq!(cost.commission_rate, Some(0.0003));
        assert_eq!(cost.min_commission, Some(5.0));
        assert_eq!(cost.tax_rate, Some(0.001));
        assert_eq!(cost.slippage_bps, Some(0.0002));
        assert_eq!(cost.cost_multiplier, Some(1.5));
        assert_eq!(cost.impact_cost_coefficient, Some(0.01));
        let rules = req
            .execution_rules
            .expect("execution rules should be propagated");
        assert_eq!(rules.execution_timing.as_deref(), Some("next_open"));
        assert_eq!(rules.execution_price.as_deref(), Some("next_open"));
        assert_eq!(rules.max_participation_rate, Some(0.05));
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_selector_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "full-market-2016-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20160304",
                "end_date": "20260515",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60"
            }),
            objective: json!({"type": "professional_candidate"}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_state_alpha_selector_v2",
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": 160,
            "score_direction": "ascending",
            "max_position_pct": "0.15",
            "max_pairwise_correlation": "0.75",
            "candidate_risk_filter": "off",
            "risk_contribution_control": "soft_single_name_20pct_v1"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_state_alpha_selector_v2")
        );
        assert_eq!(
            req.risk_contribution_control.as_deref(),
            Some("soft_single_name_20pct_v1")
        );
    }

    #[test]
    fn trial_backtest_request_applies_rebalance_smoothing_overrides() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
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
            "rebalance_hysteresis_pct": 0.02,
            "partial_rebalance_ratio": 0.75
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(req.rebalance_hysteresis_pct, Some(0.02));
        assert_eq!(req.partial_rebalance_ratio, Some(0.75));
    }

    #[test]
    fn trial_backtest_request_accepts_bear_window_market_regime_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_bear_window_guard_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_bear_window_guard_v1")
        );
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_switch_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_regime_alpha_switch_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_regime_alpha_switch_v1")
        );
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_sleeve_search_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_switch_value_v1",
            "quality_regime_alpha_switch_recovery_v1",
            "quality_regime_alpha_switch_blend_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_overlay_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_overlay_value_05pct_v1",
            "quality_regime_alpha_overlay_value_10pct_v1",
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_state_sharpe_bridge_router_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_state_sharpe_bridge_router_v1",
            "quality_state_sharpe_bridge_router_v2",
            "quality_state_sharpe_bridge_router_v3",
            "quality_frontier_regime_bridge_router_v1",
            "quality_frontier_regime_bridge_router_v2",
            "quality_frontier_regime_bridge_router_v3",
            "quality_frontier_regime_bridge_router_v4",
            "quality_frontier_regime_bridge_router_v5",
            "quality_frontier_regime_bridge_router_v6",
            "quality_frontier_regime_bridge_router_v7",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_nonlinear_alpha_router_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_nonlinear_alpha_router_v1",
            "quality_nonlinear_alpha_router_v2",
            "quality_nonlinear_alpha_risk_memory_router_v1",
            "quality_nonlinear_alpha_risk_memory_router_v2",
            "quality_nonlinear_alpha_risk_memory_router_v3",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_portfolio_sleeve_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
            "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_bear_position_market_regime_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_bear_position_guard_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_bear_position_guard_v1")
        );
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
    fn optimization_trial_request_routes_prediction_blend_through_factor_backtest() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "dv-v19-audit-ready-20260615".into(),
            backtest_template: json!({
                "start_date": "20140102",
                "end_date": "20260615",
                "benchmark": "000300.SH",
                "combo_name": "full_pit_icir_37f",
                "version": "1.0.0"
            }),
            objective: json!({"type": "professional_candidate"}),
            constraints: None,
        };
        let params = json!({
            "signal_source": "prediction_blend",
            "prediction_set_id": "pred-fullperiod-nlqr-20140101-20260630",
            "prediction_blend_weight": "0.5",
            "top_n": 30,
            "rebalance": "10",
            "score_direction": "ascending"
        });

        let req = build_optimization_trial_request(&task, &params).expect("blend request");

        match req {
            OptimizationTrialBacktestRequest::Factor(req) => {
                assert_eq!(req.combo_name, "full_pit_icir_37f");
                assert_eq!(
                    req.prediction_set_id.as_deref(),
                    Some("pred-fullperiod-nlqr-20140101-20260630")
                );
                assert_eq!(req.prediction_blend_weight, Some(0.5));
                assert_eq!(req.top_n, 30);
                assert_eq!(req.rebalance, "10");
                assert_eq!(req.score_direction, "ascending");
            }
            OptimizationTrialBacktestRequest::Prediction(_) => {
                panic!("prediction_blend must use factor backtest with blend config")
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
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert!(bundle.plan.requested_trials > bundle.plan.planned_trials);
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
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

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
    fn professional_discovery_defaults_to_sharpe_stabilization_search_profile() {
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
            quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32),
        );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert_eq!(
            layered_req.constraints.as_ref().unwrap()["min_sortino"],
            1.5
        );
        let first_trial = bundle.plan.trials.first().unwrap();
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(first_trial.parameters["stop_loss_pct"], "0.075");
        assert_eq!(first_trial.parameters["reentry_cooldown_days"], 30);
        assert_eq!(first_trial.parameters["rebalance_hysteresis_pct"], "0");
        assert_eq!(first_trial.parameters["partial_rebalance_ratio"], "1");
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_t".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 20
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_current_baseline_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20140102",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["should-not-override-baseline".to_string()]),
            max_trials: Some(5),
            search_profile: Some("phase7_v19_current".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_current_baseline"
        );
        assert_eq!(bundle.plan.requested_trials, 1);
        assert_eq!(bundle.plan.planned_trials, 1);
        let trial = &bundle.plan.trials[0].parameters;
        assert_eq!(trial["signal_source"], "prediction_blend");
        assert_eq!(trial["combo_name"], "full_pit_icir_37f");
        assert_eq!(
            trial["prediction_set_id"],
            "pred-fullperiod-nlqr-20140101-20260630"
        );
        assert_eq!(trial["prediction_blend_weight"], "0.5");
        assert_eq!(trial["score_direction"], "ascending");
        assert_eq!(trial["top_n"], 30);
        assert_eq!(trial["rebalance"], "10");
    }

    #[test]
    fn phase7_layered_request_accepts_regime_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_u".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_crash_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_bear_window_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_u2".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_bear_window_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_window_guard_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_style_risk_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_v".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_style_risk_budget"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["style_risk_budget"] == "defensive_style_budget_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_second_alpha_source_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_w".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_second_alpha_source"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
                && trial.parameters["style_risk_budget"] == "liquidity_volatility_balanced_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_residual_quality_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ag".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_residual_quality"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_industry_residual_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_residual_overlay_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ah".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_residual_overlay_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[1].parameters["combo_name"],
            "phase7_quality_residual_confirm_5pct_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["market_regime"] == "quality_bear_window_guard_v2" }));
    }

    #[test]
    fn phase7_layered_request_accepts_conditioned_second_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ak".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_conditioned_second_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_require_top40"
                && trial.parameters["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial.parameters["event_gate_min_score"] == "0.60"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "moneyflow_require_top40"
                && trial.parameters["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| trial.parameters["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn phase7_layered_request_accepts_valuation_guard_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(9),
            search_profile: Some("phase7_al".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_valuation_guard_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 9);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial.parameters["event_gate_mode"] == "exclude_negative"
                && trial.parameters["event_gate_min_score"] == "0.40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_mode"] == "require_positive"
                || trial.parameters["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_conditioned_valuation_guard_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_am".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_conditioned_valuation_guard"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35_stress_only"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40_stress_only"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_routing_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_an".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_routing"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_sleeve_search_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ao".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_sleeve_search"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_value_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_recovery_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_blend_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_overlay_search_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ap".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_overlay_search"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_blend_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_sleeve_allocation_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_aq".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_sleeve_allocation"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_low_risk_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ar".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_low_risk_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_value_guard_sleeve_composition_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_as".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_value_guard_sleeve_composition"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nearest_candidate_risk_model_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_at".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nearest_candidate_risk_model"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial.parameters["portfolio_method"] == "min_variance"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_regime_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_av".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_regime_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_sleeve_weight_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_aw".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_sleeve_weight"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_sleeve_upper_bound_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ax".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_sleeve_upper_bound"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_regime_placement_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ay".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_regime_placement"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_decay_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_az".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_decay"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_surprise_nonlinear_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ba".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_surprise_nonlinear"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_combo_name"] != "phase7_event_window_earnings_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_pos_5pct_stress_only"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_quality_segment_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_bb".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_quality_segment"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 120
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_strength_segment_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_bc".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_strength_segment"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_mode"] == "require_positive"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_require_strong_p90"
                && trial.parameters["event_gate_min_score"] == "0.66"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_require_strong_p90"
                && trial.parameters["event_gate_min_score"] == "0.43"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_confirm_require_light_p50"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_earnings_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_strength_boost_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_bd".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_strength_boost"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_mode"] == "boost_positive"
                && trial.parameters["event_gate_boost_weight"] != "0"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_strong_p75_5pct"
                && trial.parameters["event_gate_min_score"] == "0.38"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_strong_p75_3pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["event_gate_min_score"] == "0.35"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_confirm_boost_light_p50_3pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_earnings_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_legacy_alpha_revalidation_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_be".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_legacy_alpha_revalidation"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_full_history_legacy_exact"
                && trial.parameters["combo_name"] == "full_icir_16f_v3"
                && trial.parameters["start_date"] == "20160201"
                && trial.parameters["end_date"] == "20260511"
                && trial.parameters["portfolio_method"] == "heuristic"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_full_history_risk_budget"
                && trial.parameters["combo_name"] == "full_icir_16f_v3"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_recent_window_diagnostic"
                && trial.parameters["start_date"] == "20230512"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_risk_shape_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bf".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_risk_shape"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_drawdown_control"] == "recover126_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_risk_control"] == "stop_loss_075_cooldown_45"
                && trial.parameters["reentry_cooldown_days"] == 45
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos12_corr65"
                && trial.parameters["max_position_pct"] == "0.12"
                && trial.parameters["max_pairwise_correlation"] == "0.65"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_position_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bg".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_position_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["risk_budget_shape_profile"] == "risk_budget_180" }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos14_corr70"
                && trial.parameters["max_position_pct"] == "0.14"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "vol16_recover08_maxpos14_corr70"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_weak_window_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bh".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_weak_window_repair"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "stress_event_window_boost_p75_3pct"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "stress_valuation_boost_p40_5pct"
                && trial.parameters["event_gate_min_score"] == "0.40"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bi".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos14_corr65_vol20"
                && trial.parameters["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos15_corr70_vol20"
                && trial.parameters["max_position_pct"] == "0.15"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bn".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_return_recovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert_eq!(
            bundle.plan.trials[0].parameters["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_19_58_100"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["event_gate_min_score"] == "0.35"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_memory_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_bo".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_memory_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_budget_shape_profile"] == "risk_budget_150"
                && trial.parameters["risk_budget_lookback_days"] == 150
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos145_corr65_rb150"
                && trial.parameters["max_position_pct"] == "0.145"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["risk_budget_lookback_days"] == 150
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_return_sharpe_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bp".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_return_sharpe_router"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v1"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_return_sharpe_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bq".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_return_sharpe_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["risk_budget_shape_profile"] == "bp_rb160_router_v3"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_position_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_br".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_position_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65_router_v4_rb160"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["position_shape_profile"] == "maxpos145_corr65_router_v3_rb160"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_moderate_position_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bs".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_moderate_position_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["position_shape_profile"]
                    == "maxpos16_corr70_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["position_shape_profile"]
                    == "maxpos17_corr725_router_v3_vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_correlation_frontier_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bt".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_correlation_frontier_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos16_corr705_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos165_corr72_router_v4_vol120_17_52_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_correlation_threshold_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bu".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_correlation_threshold_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos16_corr706_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos165_corr709_router_v4_vol120_17_52_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_soft_risk_frontier_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bv".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_soft_risk_frontier_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial.parameters["market_regime"]
                    == "quality_event_window_return_sharpe_router_v3"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_selector_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bw".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_selector"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_selector_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_overlay_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bx".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_overlay_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v3"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.005"
                && trial.parameters["partial_rebalance_ratio"] == "0.85"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_event_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_by".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_event_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_event_state_overlay_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_event_state_overlay_selector_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bz".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["risk_budget_lookback_days"] == 160
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_ca".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v5"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_fine_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cb".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory_fine_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v7"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v9"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_exposure_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cc".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_exposure_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v11"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_orthogonal_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cd".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_orthogonal_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_candidate_filter_alpha_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_ce".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_candidate_filter_alpha_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_soft_candidate_filter_alpha_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cf".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_soft_candidate_filter_alpha_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "soft_low_volatility_v1"
                && trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v3"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["candidate_risk_filter"] == "off" }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_bridge_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cg".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_bridge_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_annual_sharpe_floor_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ch".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_annual_sharpe_floor_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["score_direction"] == "ascending"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_memory_relaxed_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ci".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_memory_relaxed_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v18"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_floor_auto_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ck".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_floor_auto_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_portfolio_sharpe_control_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cj".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_portfolio_sharpe_control"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_sharpe_control"] == "roll_sharpe120_060_000_55"
                && trial.parameters["portfolio_sharpe_reduce_start"] == "0.60"
                && trial.parameters["portfolio_sharpe_lookback_days"] == 120
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_sharpe_control"]
                    == "bridge_roll_sharpe180_050_neg10_60"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nonlinear_alpha_auto_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cl".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nonlinear_alpha_auto_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_router_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_sharpe_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nonlinear_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_cm".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nonlinear_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["max_position_pct"] == "0.16"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confirmed_sharpe_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cn".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confirmed_sharpe_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["prediction_blend_weight"] == "0.02"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["prediction_min_percentile"] == "0.20"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["prediction_min_percentile"] == "0.30"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_boundary_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_co".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_boundary_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_boundary_event_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cq".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_boundary_event_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_micro_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_cr".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_micro_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"] == "soft_low_volatility_v1"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["candidate_risk_filter"]
                    == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_sharpe_return_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cs".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_sharpe_return_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom43"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_shape_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ct".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_shape_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 18
                && trial.parameters["rebalance"] == "55"
                && trial.parameters["skip_top_pct"] == "0.08"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 22
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_23_45_30_70"
                && trial.parameters["stop_loss_pct"] == "0.07"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_ultra_micro_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(15),
            search_profile: Some("phase7_cu".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_ultra_micro_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 15);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 20
                && trial.parameters["rebalance"] == "60"
                && trial.parameters["skip_top_pct"] == "0.10"
                && trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 21
                && trial.parameters["rebalance"] == "60"
                && trial.parameters["skip_top_pct"] == "0.10"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 20
                && trial.parameters["rebalance"] == "62"
                && trial.parameters["portfolio_volatility_control"] == "vol120_143_467_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_annual_floor_micro_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cz".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_annual_floor_micro_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v15"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial.parameters["risk_budget_lookback_days"] == 165
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial.parameters["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_near_miss_annual_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_da".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_near_miss_annual_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial.parameters["risk_budget_lookback_days"] == 165
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["max_position_pct"] == "0.145"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_corr70_annual_edge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(20),
            search_profile: Some("phase7_db".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_corr70_annual_edge"
        );
        assert_eq!(bundle.plan.planned_trials, 20);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["max_pairwise_correlation"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["max_position_pct"] == "0.15"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_robust_candidate_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_dj".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_robust_candidate"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 50
                && trial.parameters["rebalance"] == "120"
                && trial.parameters["max_position_pct"] == "0.12"
                && trial.parameters["capacity_penalty_strength"] == "1.25"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["rebalance_hysteresis_pct"] == "0.01"
                && trial.parameters["partial_rebalance_ratio"] == "0.75"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_low_turnover_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_dn".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_low_turnover_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"]
                    == "soft_low_volatility_low_correlation_v1"
                && trial.parameters["rebalance"] == "180"
                && trial.parameters["capacity_penalty_strength"] == "1.5"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_capacity_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_dq".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_impact_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(18),
            search_profile: Some("phase7_dr".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_impact_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_schedule_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ds".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_schedule_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_5d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"] == "twap_5d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_patient_execution_schedule_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dt".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_patient_schedule_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"]
                    == "twap_20d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_daily_cap_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_du".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_daily_cap_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_daily_target_move_limit_pct"] == "0.05"
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.05)
                && trial.parameters["execution_rules"]["execution_max_carry_days"] == 20
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_cash_drag_aware_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dv".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_cash_drag_aware_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["execution_daily_target_move_limit_pct"] == "0.075"
                && trial.parameters["execution_max_carry_days"] == 40
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.075)
        }));
    }

    #[test]
    fn phase7_dv_defaults_oos_train_score_to_cash_drag_fill_gap() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dv".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_cash_weight_pct"],
            json!(0.20)
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["enabled"],
            true
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["min_pass_ratio"],
            json!(0.80)
        );
    }

    #[test]
    fn phase7_dy_defaults_oos_train_gate_to_fill_ratio_not_final_cash() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dy".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert!(config["train_selection_gate_policy"]
            .get("max_train_final_cash_weight_pct")
            .is_none());
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_dz_defaults_oos_train_gate_to_rolling_carry_fill_ratio() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dz".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert!(config["train_selection_gate_policy"]
            .get("max_train_execution_target_gap_pct")
            .is_none());
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ea_defaults_oos_train_gate_to_capacity_fill_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ea".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_eb_defaults_oos_train_gate_to_alpha_capacity_bridge() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eb".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ec_defaults_oos_train_gate_to_alpha_capacity_return_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ec".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ed_defaults_oos_train_gate_to_stress_fill_return_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ed".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ee_defaults_oos_train_gate_to_stress_risk_budget() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ee".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
    }

    #[test]
    fn phase7_ef_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ef".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_perturbed_annual_return"],
            json!(0.05)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
    }

    #[test]
    fn phase7_eg_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eg".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_perturbed_annual_return"],
            json!(0.05)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_avg_perturbed_calmar"],
            json!(1.2)
        );
    }

    #[test]
    fn phase7_eh_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eh".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ei_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ei".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ej_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ej".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ek_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ek".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_el_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_el".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_em_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_em".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_en_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_en".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_eo_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ep_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ep".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_eq_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eq".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_er_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_er".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_execution_feasible_fill_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dx".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_feasible_fill_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_90_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_fill_ratio_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dy".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_fill_ratio_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_rolling_carry_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dz".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_rolling_carry_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["execution_rules"]["execution_carry_policy"]
                    == "roll_forward_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_capacity_fill_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ea".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_fill_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
                && trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_alpha_capacity_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eb".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_capacity_bridge"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["max_gross_exposure"] == "1"
                && trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_alpha_capacity_return_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ec".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_capacity_return_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["max_gross_exposure"] == "1"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["universe_profile"] == "listed_non_st"
                && trial.parameters["top_n"] == 60
                && trial.parameters["max_position_pct"] == "0.10"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_balanced_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_stress_fill_return_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ed".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_fill_return_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_stress_risk_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ee".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_risk_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_capacity_stress_return_gate_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ef".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_stress_return_gate"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["top_n"] == 100
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_low_impact_alpha_stress_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eg".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_low_impact_alpha_stress_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 100
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["rebalance"] == "180"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_target_scaling_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eh".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_target_scaling_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_target_scale_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.05"
                && trial.parameters["max_gross_exposure"] == "0.80"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_floor_scaling_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ei".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_floor_scaling_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_floor_35_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 100
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_floor_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ej".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_floor_return_recovery"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_pressure_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ek".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pressure_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 160
                && trial.parameters["max_position_pct"] == "0.05"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_alpha_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_el".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_blended_alpha_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_em".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_blended_alpha_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_anchor_stress_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_en".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_anchor_stress_bridge"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["rebalance"] == "160"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_participation_aware_event_anchor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eo".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_participation_aware_event_anchor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"]
                == "soft_liquidity_low_volatility_low_correlation_v1"
                && trial.parameters["execution_rules"]["max_participation_rate"] == json!(0.10)
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["rebalance"] == "180"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_cl_anchor_fill_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ep".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_cl_anchor_fill_recovery"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ep should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_mixed_orthogonal_risk_memory_router_v3"
        );
        assert_eq!(
            first_trial.parameters["execution_cl_anchor_fill_recovery_profile"],
            "cl_fill_recovery_top120_rebalance180"
        );
        assert_eq!(first_trial.parameters["top_n"], 120);
        assert_eq!(first_trial.parameters["max_position_pct"], "0.06");
        assert_eq!(first_trial.parameters["max_gross_exposure"], "0.90");
        assert_eq!(
            first_trial.parameters["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_capacity_aware_candidate_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eq".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_aware_candidate_ranking"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_eq should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
        assert_eq!(first_trial.parameters["top_n"], 100);
        assert_eq!(first_trial.parameters["max_position_pct"], "0.08");
        assert_eq!(
            first_trial.parameters["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_capacity_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_er".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_capacity_ranking"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_er"));
        assert_eq!(
            gate_policy["enable_train_cost_capacity_perturbation_gate"],
            true
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_er should seed the EQ capacity anchor first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_alpha_first_low_impact_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_es".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_alpha_first_low_impact"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_es"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_es should seed alpha-first PIT capacity recovery first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert_eq!(
            first_trial.parameters["pit_capacity_recovery_profile"],
            "pit_alpha_first_event_top80_rebalance160"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_excess_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_et".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_excess_return_recovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_et"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_et should seed PIT excess-return recovery first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "relative_strength_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["pit_excess_return_recovery_profile"],
            "pit_excess_event_top80_rebalance120"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_bull_sleeve_cash_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eu".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_bull_sleeve_cash_recovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_eu"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_eu should seed nearest-candidate recovery first");
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["event_gate_profile"],
            "valuation_exclude_bottom40"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(first_trial.parameters["candidate_risk_filter"], "off");
        assert_eq!(first_trial.parameters["risk_contribution_control"], "off");
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial.parameters["cash_utilization"] == "off"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_return_first_fill_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ey".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_return_first_fill_repair"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ey"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ey should seed return-first fill repair first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["cash_utilization"],
            "stress_fill_gross_98_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_10d_v1"
        );
        assert_eq!(
            first_trial.parameters["return_first_fill_repair_profile"],
            "return_first_fill_anchor1_top60_stress_fill"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_nonlinear_alpha_regime_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ez".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ez"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ez should seed PIT nonlinear alpha rebuild first");
        assert_eq!(
            first_trial.parameters["pit_nonlinear_alpha_regime_rebuild_profile"],
            "pit_nonlinear_event_overlay_softcap_top100_rebalance180"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_state_alpha_overlay_selector_v1"
        );
        assert_eq!(
            first_trial.parameters["cash_utilization"],
            "stress_fill_gross_98_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_20d_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_pit_quality_recovery_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fa".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_quality_recovery_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fa"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fa should seed PIT quality recovery first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_recovery_acceleration_v1"
        );
        assert_eq!(first_trial.parameters["score_direction"], "descending");
        assert_eq!(
            first_trial.parameters["pit_quality_recovery_alpha_profile"],
            "pit_quality_recovery_top100_rebalance160"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_post_return_curve_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fb".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_post_return_curve_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fb"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fb should seed PIT event post-return curve first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_event_post_return_curve_overlay_v1"
        );
        assert_eq!(
            first_trial.parameters["event_post_return_curve_alpha_profile"],
            "event_post_return_curve_top100_rebalance160"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_event_reaction_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fc".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_reaction_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fc"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fc should seed event reaction segment alpha first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_event_reaction_segments_overlay_v1"
        );
        assert_eq!(
            first_trial.parameters["event_reaction_alpha_profile"],
            "event_reaction_segments_top100_rebalance160"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_reaction_reversal_overlay_v1"
                && trial.parameters["event_reaction_alpha_profile"]
                    == "event_reaction_reversal_top100_rebalance160"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_broad_financial_feature_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fg".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_broad_financial_feature_discovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fg"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fg should seed broad FF financial combos first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_cashflow_dividend_confirm_v1"
        );
        assert_eq!(
            first_trial.parameters["broad_financial_feature_discovery_profile"],
            "broad_ff_dual_confirm_ascending_top100"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_dividend_confirm_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_dividend_quality_v1"
                || trial.parameters["combo_name"] == "phase7_cashflow_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_broad_financial_stratified_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fh".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_broad_financial_feature_stratified_discovery"
        );
        let first_four_combos = bundle
            .plan
            .trials
            .iter()
            .take(4)
            .map(|trial| trial.parameters["combo_name"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(first_four_combos.contains(&"phase7_financial_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_growth_recovery_v1"));
        assert!(first_four_combos.contains(&"phase7_industry_residual_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_quality_relative_strength_v1"));
        assert!(bundle.plan.trials.iter().take(8).all(|trial| {
            trial.parameters["broad_financial_feature_sampling"] == "stratified_seed_v1"
        }));
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fh"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_dividend_quality_v1"
                || trial.parameters["combo_name"] == "phase7_cashflow_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_native_alpha_fusion_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_fj".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_native_alpha_fusion_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7-wf-wide-qgvrel-v1-201602-202605"])
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fj"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_four_families = bundle
            .plan
            .trials
            .iter()
            .take(4)
            .map(|trial| {
                trial.parameters["alpha_source_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(first_four_families.contains(&"broad_financial"));
        assert!(first_four_families.contains(&"event_surprise"));
        assert!(first_four_families.contains(&"event_reaction"));
        assert!(first_four_families.contains(&"prediction_confirmation"));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "broad_financial_core"
                && trial.parameters["alpha_source_family"] == "broad_financial"
        }));
        let event_surprise_trial = bundle
            .plan
            .trials
            .iter()
            .find(|trial| trial.parameters["alpha_source_family"] == "event_surprise")
            .expect("phase7_fj should include an event surprise trial");
        assert_eq!(
            event_surprise_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            event_surprise_trial.parameters["event_gate_combo_name"],
            "phase7_event_surprise_v1"
        );
        assert_ne!(
            event_surprise_trial.parameters["combo_name"],
            "phase7_quality_event_surprise_confirm_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "event_reaction_segments"
                && trial.parameters["alpha_source_family"] == "event_reaction"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "prediction_confirmation"
                && trial.parameters["alpha_source_family"] == "prediction_confirmation"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
        }));
        let prediction_trial = bundle
            .plan
            .trials
            .iter()
            .find(|trial| trial.parameters["alpha_source_family"] == "prediction_confirmation")
            .expect("phase7_fj should include a prediction confirmation trial");
        assert_eq!(
            prediction_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            prediction_trial.parameters["capacity_risk_budget"],
            "capacity_stress_participation_alpha_headroom_floor_70_v1"
        );
        assert_eq!(prediction_trial.parameters["max_position_pct"], "0.08");
        assert_eq!(
            prediction_trial.parameters["universe_profile"],
            "listed_non_st"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_trainable_alpha_admission_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ft".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_trainable_alpha_admission_discovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ft"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_six_families = bundle
            .plan
            .trials
            .iter()
            .take(6)
            .map(|trial| {
                trial.parameters["alpha_source_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<BTreeSet<_>>();
        assert!(first_six_families.contains("broad_financial"));
        assert!(first_six_families.contains("residual_confirm"));
        assert!(first_six_families.contains("value_recovery"));
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            let combo_name = trial.parameters["combo_name"].as_str().unwrap_or_default();
            quant_api::discovery::phase7::is_phase7_base_trainable_alpha(combo_name)
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_surprise_v1"
                || trial.parameters["combo_name"] == "phase7_event_window_earnings_v1"
                || trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
                || trial.parameters["combo_name"]
                    == "phase7_quality_event_reaction_segments_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_multi_alpha_sleeve_admission_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20140102",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-sleeve-admission".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_sleeves".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_multi_alpha_sleeve_admission"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_v19_sleeves"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let sleeve_families = bundle
            .plan
            .trials
            .iter()
            .take(12)
            .map(|trial| {
                trial.parameters["alpha_sleeve_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<BTreeSet<_>>();
        assert!(sleeve_families.contains("quality_core"));
        assert!(sleeve_families.contains("valuation_guard"));
        assert!(sleeve_families.contains("growth_recovery"));
        assert!(sleeve_families.contains("relative_strength"));
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["multi_alpha_sleeve_profile"]
                    == "v19_p2_pit_sleeve_admission_v1"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_event_surprise_sleeve_gate_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260531",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-event-surprise".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_event_surprise_sleeve_gate".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_event_surprise_sleeve_gate_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_event_surprise_sleeve_gate",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["event_surprise_sleeve_gate_profile"]
                    == "v19_p311_fq_change_event_surprise_sleeve_gate_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_fq_change_event_surprise_sleeve_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["event_gate_profile"] == "event_surprise_boost_p75_3pct"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_moneyflow_congestion_interaction_v1")
                || serialized.contains("phase7_event_post_return_curve_20d_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_event_post_return_overlay_admission_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260531",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-event-overlay".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_event_post_return_overlay".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_event_post_return_overlay_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_event_post_return_overlay",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
                && trial.parameters["event_overlay_combo_name"]
                    == "phase7_event_post_return_curve_20d_v1"
                && trial.parameters["broad_base_combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["event_gate_combo_name"] == "phase7_event_post_return_curve_20d_v1"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_post_return_curve_20d_v1"
                || trial.parameters.to_string().contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_supply_float_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260531",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-supply-sleeve".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_supply_float_sleeve".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_supply_float_sleeve_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_supply_float_sleeve",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["supply_float_sleeve_profile"]
                    == "v19_p312_fq_change_supply_float_sleeve_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_fq_change_supply_float_sleeve_10pct_v1"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_moneyflow_congestion_interaction_v1")
                || serialized.contains("phase7_event_post_return_curve_20d_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_unlock_pressure_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260531",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-unlock-pressure-sleeve".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_unlock_pressure_sleeve".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_unlock_pressure_sleeve_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_unlock_pressure_sleeve",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["unlock_pressure_sleeve_profile"]
                    == "v19_p314_fq_change_unlock_pressure_sleeve_v1"
                && trial.parameters["unlock_pressure_sleeve_control"]
                    == "phase7_financial_quality_change_v1"
                && trial.parameters["sideways_regime_policy"] == "exclude"
                && trial.parameters["oos_policy"] == "evaluation_only"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_fq_change_unlock_pressure_sleeve_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_change_v1"
                && trial.parameters["event_gate_combo_name"] == "phase7_unlock_supply_pressure_v1"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "bull", "mixed", "high_volatility"])
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_moneyflow_congestion_interaction_v1")
                || serialized.contains("phase7_event_post_return_curve_20d_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_forecast_revision_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260531",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "must-not-override-forecast-revision-sleeve".to_string()
            ]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_forecast_revision_sleeve".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_forecast_revision_sleeve_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_forecast_revision_sleeve",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["forecast_revision_sleeve_profile"]
                    == "v19_p313_fq_change_forecast_revision_sleeve_v1"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"] == "phase7_fq_change_forecast_revision_sleeve_10pct_v1"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_moneyflow_congestion_interaction_v1")
                || serialized.contains("phase7_event_post_return_curve_20d_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_shareholder_structure_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20151022",
                "end_date": "20260623",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "must-not-override-shareholder-structure-sleeve".to_string()
            ]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_shareholder_structure_sleeve".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_shareholder_structure_sleeve_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_shareholder_structure_sleeve",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_shareholder_structure_sleeve_admission"
        ));
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["shareholder_structure_sleeve_profile"]
                    == "v19_p321e_fq_change_shareholder_structure_sleeve_v1"
                && trial.parameters["shareholder_structure_sleeve_control"]
                    == "phase7_financial_quality_change_v1"
                && trial.parameters["alpha_admission_gate_id"]
                    == "shareholder_structure_low_fanout_strict_pit_gate_v1"
                && trial.parameters["universe_profile"] == "main_chinext_non_st"
                && trial.parameters["market_regime"] == "off"
                && trial.parameters["oos_policy"] == "evaluation_only"
        }));
        assert!(bundle.plan.trials.iter().take(12).any(|trial| {
            trial.parameters["combo_name"]
                == "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
                && trial.parameters["shareholder_structure_sleeve_variant"]
                    == "shareholder_structure_sleeve_10pct"
        }));
        assert!(!bundle.plan.trials.iter().take(12).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_moneyflow_congestion_interaction_v1")
                || serialized.contains("phase7_event_post_return_curve_20d_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_execution_repair_admission_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20140102",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-v19-execution-repair".to_string()]),
            max_trials: Some(30),
            search_profile: Some("phase7_v19_execution_repair".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_execution_repair_admission"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_execution_repair",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_execution_repair_admission"
        ));
        assert!(bundle.plan.trials.iter().take(30).all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["v19_execution_repair_profile"]
                    == "v19_p2_pit_execution_repair_v1"
                && trial.parameters["multi_alpha_sleeve_profile"]
                    == "v19_p2_pit_sleeve_admission_v1"
        }));
        assert!(bundle.plan.trials.iter().take(30).any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
                && trial.parameters["score_candidate_pool_size"] == 900
        }));
        assert!(!bundle.plan.trials.iter().take(30).any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_alpha_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-v19-alpha-rebuild".to_string()]),
            max_trials: Some(12),
            search_profile: Some("phase7_v19_ml_alpha_rebuild".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19ml-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_alpha_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "v19 alpha rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_alpha_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_alpha_rebuild"
        )));
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some("phase7_v19_ml_alpha_rebuild")),
            "phase7_gb_quality_value_recovery_low_impact_v6"
        );
        let factors = phase7_train_window_ml_factor_refs_for_profile(
            phase7_train_window_ml_feature_profile_for_search(Some("phase7_v19_ml_alpha_rebuild")),
        );
        assert!(factors.len() >= 52);
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "margin_rz_std_20d"));

        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_alpha_rebuild",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            gate_policy["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(bundle.plan.trials.len(), 12);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_alpha_rebuild_v1"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"]
                    == "quality_adjusted_risk_adjusted_excess_return"
                && trial.parameters["prediction_set_id"] == "p7v19ml-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
                && trial.parameters["prediction_min_score"].is_string()
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("must-not-override-v19-alpha-rebuild")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_simple_excess_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-v19-simple-excess".to_string()]),
            max_trials: Some(9),
            search_profile: Some("phase7_v19_ml_simple_excess".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19sx-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_simple_excess_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "v19 simple excess rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_simple_excess_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_simple_excess"
        )));
        assert!(is_v19_train_window_ml_alpha_rebuild_profile(Some(
            "phase7_v19_ml_simple_excess"
        )));
        assert_eq!(
            phase7_train_window_ml_label_config_for_search(Some("phase7_v19_ml_simple_excess")),
            ("future_excess_return".to_string(), 45usize, 5usize, 50usize)
        );
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some("phase7_v19_ml_simple_excess")),
            "phase7_gb_quality_value_recovery_low_impact_v6"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_simple_excess",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 9);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_simple_excess_rebuild_v1"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 45
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["prediction_set_id"] == "p7v19sx-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("must-not-override-v19-simple-excess")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_simple_excess_low_impact_rebuild_profile()
    {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "must-not-override-v19-simple-excess-low-impact".to_string()
            ]),
            max_trials: Some(6),
            search_profile: Some("phase7_v19_ml_simple_excess_low_impact".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19sxli-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "low-impact simple excess rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_simple_excess_low_impact"
        )));
        assert!(is_v19_train_window_ml_alpha_rebuild_profile(Some(
            "phase7_v19_ml_simple_excess_low_impact"
        )));
        assert!(is_v19_train_window_ml_simple_excess_rebuild_profile(Some(
            "phase7_v19_ml_simple_excess_low_impact"
        )));
        assert_eq!(
            phase7_train_window_ml_label_config_for_search(Some(
                "phase7_v19_ml_simple_excess_low_impact",
            )),
            ("future_excess_return".to_string(), 45usize, 5usize, 50usize)
        );
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some(
                "phase7_v19_ml_simple_excess_low_impact",
            )),
            "phase7_gb_quality_value_recovery_low_impact_v6"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_simple_excess_low_impact",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            gate_policy["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(bundle.plan.trials.len(), 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_simple_excess_low_impact_rebuild_v1"
                && trial.parameters["alpha_sleeve_family"] == "growth_recovery"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 45
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["prediction_set_id"] == "p7v19sxli-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("must-not-override-v19-simple-excess-low-impact")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_h120_low_impact_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-v19-h120-low-impact".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_v19_ml_h120_low_impact".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19h120-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_h120_low_impact_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "H120 low-impact rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_h120_low_impact_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_h120_low_impact"
        )));
        assert!(is_v19_train_window_ml_alpha_rebuild_profile(Some(
            "phase7_v19_ml_h120_low_impact"
        )));
        assert!(!is_v19_train_window_ml_simple_excess_rebuild_profile(Some(
            "phase7_v19_ml_h120_low_impact"
        )));
        assert!(is_v19_train_window_ml_h120_low_impact_rebuild_profile(
            Some("phase7_v19_ml_h120_low_impact")
        ));
        assert_eq!(
            phase7_train_window_ml_label_config_for_search(Some("phase7_v19_ml_h120_low_impact",)),
            (
                "future_excess_return".to_string(),
                120usize,
                5usize,
                50usize
            )
        );
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some(
                "phase7_v19_ml_h120_low_impact",
            )),
            "phase7_gb_quality_value_recovery_low_impact_v6"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_h120_low_impact",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            gate_policy["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_h120_low_impact_rebuild_v1"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["prediction_set_id"] == "p7v19h120-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("must-not-override-v19-h120-low-impact")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(
    ) {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "must-not-override-v19-rae-h120-residual-capacity".to_string()
            ]),
            max_trials: Some(6),
            search_profile: Some("phase7_v19_ml_rae_h120_residual_capacity".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19rae-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "RAE H120 residual/capacity rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_rae_h120_residual_capacity"
        )));
        assert!(is_v19_train_window_ml_alpha_rebuild_profile(Some(
            "phase7_v19_ml_rae_h120_residual_capacity"
        )));
        assert!(
            is_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(Some(
                "phase7_v19_ml_rae_h120_residual_capacity"
            ))
        );
        assert_eq!(
            phase7_train_window_ml_label_config_for_search(Some(
                "phase7_v19_ml_rae_h120_residual_capacity",
            )),
            (
                "risk_adjusted_excess_return".to_string(),
                120usize,
                7usize,
                50usize
            )
        );
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some(
                "phase7_v19_ml_rae_h120_residual_capacity",
            )),
            "phase7_gb_quality_value_recovery_low_impact_v6"
        );
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_rae_h120_residual_capacity",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_3_train_window_ml_rae_h120_residual_capacity_rebuild_v1"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"]
                    == "risk_adjusted_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 7
                && trial.parameters["prediction_set_id"] == "p7v19rae-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("must-not-override-v19-rae-h120-residual-capacity")
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v19_train_window_ml_event_sentiment_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "dv-v19-audit-ready-20260615".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20170103",
                "end_date": "20260615",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-v19-event-sentiment".to_string()]),
            max_trials: Some(6),
            search_profile: Some("phase7_v19_ml_event_sentiment".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle =
            build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
                &req,
                resource_plan,
                500,
                Some("p7v19evt-train-window-001"),
            );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v19_train_window_ml_event_sentiment_rebuild"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "event/sentiment rebuild must not accept request-level prediction_set overrides"
        );
        assert!(!profile_accepts_prediction_set_override(
            "professional_v19_train_window_ml_event_sentiment_rebuild"
        ));
        assert!(is_train_window_ml_stress_fill_profile(Some(
            "phase7_v19_ml_event_sentiment"
        )));
        assert!(is_v19_train_window_ml_alpha_rebuild_profile(Some(
            "phase7_v19_ml_event_sentiment"
        )));
        assert!(is_v19_train_window_ml_event_sentiment_rebuild_profile(
            Some("phase7_v19_ml_event_sentiment")
        ));
        assert_eq!(
            phase7_train_window_ml_label_config_for_search(Some("phase7_v19_ml_event_sentiment",)),
            (
                "risk_adjusted_excess_return".to_string(),
                120usize,
                7usize,
                50usize
            )
        );
        assert_eq!(
            phase7_train_window_ml_feature_profile_for_search(Some(
                "phase7_v19_ml_event_sentiment",
            )),
            "phase7_p4_event_sentiment_high_coverage_v1"
        );
        let factors = phase7_train_window_ml_factor_refs_for_profile(
            phase7_train_window_ml_feature_profile_for_search(Some(
                "phase7_v19_ml_event_sentiment",
            )),
        );
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "event_forecast_surprise_bucket_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "event_disclosure_timing_bucket_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "event_express_roe_bucket_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "margin_rz_change_std_20d"));
        assert!(factors
            .iter()
            .all(|factor| factor.factor_version == "1.0.0"));
        assert!(!factors
            .iter()
            .any(|factor| factor.factor_code == "event_post_return_forecast_20d_indrel_std"));
        let gate_policy = default_oos_train_selection_gate_policy_for_search_profile(Some(
            "phase7_v19_ml_event_sentiment",
        ));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_4_train_window_ml_event_sentiment_rebuild_v1"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_p4_event_sentiment_high_coverage_v1"
                && trial.parameters["train_window_ml_label_objective"]
                    == "risk_adjusted_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 7
                && trial.parameters["prediction_set_id"] == "p7v19evt-train-window-001"
                && trial.parameters["prediction_set_override_source"] == "train_window_ml_internal"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("must-not-override-v19-event-sentiment")
        }));
    }

    #[test]
    fn fixed_params_oos_mode_does_not_short_circuit_multi_seed_sleeve_admission() {
        assert!(should_use_fixed_params_oos_mode(
            true,
            false,
            Some("phase7_v19_current")
        ));
        assert!(!should_use_fixed_params_oos_mode(
            true,
            false,
            Some("phase7_v19_sleeves")
        ));
        assert!(!should_use_fixed_params_oos_mode(
            true,
            true,
            Some("phase7_v19_current")
        ));
        assert!(!should_use_fixed_params_oos_mode(
            false,
            false,
            Some("phase7_v19_current")
        ));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_capacity_dual_objective_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fl".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_capacity_dual_objective"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7-wf-wide-qgvrel-v1-201602-202605"])
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fl"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_capacity_dual_objective_profile"]
                == "return_anchor_capacity_off"
                && trial.parameters["candidate_ranking"] == "off"
                && trial.parameters["capacity_risk_budget"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_capacity_dual_objective_profile"]
                == "capacity_aware_twap20_blended_headroom70"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_target_gross_signal_fidelity_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fm".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_target_gross_signal_fidelity"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fm"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_target_gross_signal_fidelity_profile"]
                == "gross65_signal_fidelity"
                && trial.parameters["max_gross_exposure"] == "0.65"
                && trial.parameters["candidate_ranking"] == "off"
                && trial.parameters["capacity_risk_budget"] == "off"
                && trial.parameters["cash_utilization"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confidence_turnover_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fn".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confidence_turnover_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FN should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fn"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_turnover_profile"]
                == "return_anchor_confidence_off"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
                && trial.parameters["execution_impact_budget"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_turnover_profile"]
                == "confidence30_turnover_smooth"
                && trial.parameters["prediction_min_percentile"] == "0.30"
                && trial.parameters["rebalance"] == "160"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.01"
                && trial.parameters["partial_rebalance_ratio"] == "0.75"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_20pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confidence_alpha_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fo".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confidence_alpha_lift"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FO should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fo"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_profile"]
                == "confidence40_quality_nonlinear_alpha_lift"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["prediction_min_percentile"] == "0.40"
                && trial.parameters["rebalance"] == "200"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.02"
                && trial.parameters["partial_rebalance_ratio"] == "0.65"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_family"] == "event_post_curve"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_family"] == "event_reaction"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_reaction_segments_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_long_horizon_low_turnover_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fp".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_long_horizon_low_turnover"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FP should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fp"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"]
                == "h60_confidence50_low_turnover_quality"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_min_percentile"] == "0.50"
                && trial.parameters["rebalance"] == "240"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.03"
                && trial.parameters["partial_rebalance_ratio"] == "0.60"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"]
                == "h60_confidence40_gross65_signal_fidelity"
                && trial.parameters["max_gross_exposure"] == "0.65"
        }));
    }

    #[test]
    fn phase7_layered_request_overrides_long_horizon_seed_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                " pred-phase7-fu-future-excess-h60-v1-202301-202501 ".to_string()
            ]),
            max_trials: Some(3),
            search_profile: Some("phase7_fp".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-phase7-fu-future-excess-h60-v1-202301-202501"])
        );
        assert_eq!(bundle.plan.trials.len(), 3);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-phase7-fu-future-excess-h60-v1-202301-202501"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_long_horizon_regime_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fr".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_long_horizon_regime_alpha"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FR should use H60 PIT prediction as an overlay in seeds"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fr"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(
            bundle.plan.trials.iter().all(|trial| {
                trial.parameters["prediction_long_horizon_regime_alpha_profile"].is_string()
                    && trial.parameters["prediction_set_id"]
                        == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                    && trial.parameters["prediction_label_horizon_days"] == 60
            }),
            "phase7_fr tiny smoke should not spill into generic non-H60 cartesian trials"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_profile"]
                == "h60_value_recovery_regime_alpha"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 60
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_family"] == "event_post_curve"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_overrides_regime_alpha_seed_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501".to_string(),
            ]),
            max_trials: Some(8),
            search_profile: Some("phase7_fr".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501"])
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_h60_nonlinear_stress_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fw".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_h60_nonlinear_stress_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FW should keep H60 prediction as seed overlay instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fw"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h60_nonlinear_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["signal_source"] == "factor_combo"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "relative_strength_alpha_liquidity_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_h120_low_impact_stress_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fz".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_h120_low_impact_stress_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FZ should keep H120 prediction as seed overlay instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fz"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h120_low_impact_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h120-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 120
                && trial.parameters["signal_source"] == "factor_combo"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "alpha_first_low_impact_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_fz_defaults_train_gate_to_execution_quality_requirements() {
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fz"));

        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(gate_policy["min_train_trade_count"], json!(50));
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.25)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.75));
        assert_eq!(
            gate_policy["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_layered_request_overrides_h60_nonlinear_stress_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "pred-p7fu-fex-h60-smk2-v1-20240902-20250324".to_string()
            ]),
            max_trials: Some(8),
            search_profile: Some("phase7_fw".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7fu-fex-h60-smk2-v1-20240902-20250324"])
        );
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h60_nonlinear_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7fu-fex-h60-smk2-v1-20240902-20250324"
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_nonlinear_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["should-not-be-used-by-fx".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_fx".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_nonlinear_ranking_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FX should ignore request prediction sets because it is a native train-window ranking profile"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fx"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_nonlinear_ranking_profile"].is_string()
                && trial.parameters["candidate_ranking"] == "nonlinear_regime_alpha_liquidity_v2"
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_stress_fill_target_exposure_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["should-not-be-used-by-ga".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_ga".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_stress_fill_target_exposure"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "GA should ignore request prediction sets because it is native train-window construction"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ga"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(gate_policy["min_train_trade_count"], json!(50));
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.25)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.75));
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_stress_fill_target_exposure_profile"].is_string()
                && trial.parameters["candidate_ranking"] == "nonlinear_regime_alpha_liquidity_v2"
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_alpha_headroom_floor_70_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_ml_stress_fill_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-train-window-ml".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_gb".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_ml_stress_fill_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "GB must not accept full-history or request-level prediction_set overrides"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_gb"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "prediction_confidence_stress_fill_quality_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.35)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.65));
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.95)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_ml_ranking_profile"].is_string()
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v5"
                && matches!(
                    trial.parameters["train_window_ml_label_objective"].as_str(),
                    Some("regime_conditional_excess_return" | "future_excess_return")
                )
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["stress_fill_objective_profile"].is_string()
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["portfolio_method"] == "stress_fill_aware_risk_budget"
                && trial.parameters["stress_fill_portfolio_construction"]
                    == "ml_score_capacity_correlation_risk_budget_target_exposure_v1"
                && trial.parameters["stress_fill_confidence_exposure"]
                    == "prediction_confidence_ascending_capacity_headroom_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["prediction_confidence_gate_profile"]
                    == "train_positive_raw_score_gate_v1"
                && trial.parameters["train_window_ml_prediction_min_score"].is_string()
                && trial.parameters.get("prediction_min_score").is_none()
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["train_window_ml_ranking_profile"]
                == "nlq_ranker_rae_h45_bucket7_fill95"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
        }));
    }

    #[test]
    fn internal_train_window_ml_prediction_override_only_applies_to_ml_seeds() {
        let mut seed_trials = vec![
            json!({
                "signal_source": "factor_combo",
                "train_window_ml_ranking_profile": "nlq_ranker_rae_h45_bucket7_fill95",
                "stress_fill_objective_profile": "gb_quality",
                "train_window_ml_prediction_min_score": "0.01"
            }),
            json!({
                "signal_source": "factor_combo",
                "prediction_set_id": "existing-static-prediction"
            }),
        ];

        apply_internal_train_window_ml_prediction_set_to_seed_trials(
            &mut seed_trials,
            "p7gb-train-window-001",
        );

        assert_eq!(seed_trials[0]["prediction_set_id"], "p7gb-train-window-001");
        assert_eq!(seed_trials[0]["prediction_blend_weight"], "0.20");
        assert_eq!(seed_trials[0]["prediction_min_percentile"], "0.30");
        assert_eq!(seed_trials[0]["prediction_min_score"], "0.01");
        assert_eq!(
            seed_trials[0]["prediction_set_override_source"],
            "train_window_ml_internal"
        );
        assert_eq!(
            seed_trials[1]["prediction_set_id"],
            "existing-static-prediction"
        );
        assert!(seed_trials[1]
            .get("prediction_set_override_source")
            .is_none());
    }

    #[test]
    fn train_window_ml_oos_parameters_swap_to_test_prediction_set() {
        let train_parameters = json!({
            "signal_source": "factor_combo",
            "combo_name": "phase7_financial_quality_v1",
            "prediction_set_id": "p7gb-train-window-001",
            "prediction_set_override_source": "train_window_ml_internal"
        });
        let sets = TrainWindowMlPredictionSets {
            train_prediction_set_id: "p7gb-train-window-001".to_string(),
            test_prediction_set_id: "p7gb-test-window-001".to_string(),
            training_task_id: "train-p7gb-window-001".to_string(),
        };

        let oos_parameters =
            train_window_ml_oos_parameters(&train_parameters, Some(&sets)).expect("oos params");

        assert_eq!(oos_parameters["prediction_set_id"], "p7gb-test-window-001");
        assert_eq!(
            oos_parameters["train_window_ml_train_prediction_set_id"],
            "p7gb-train-window-001"
        );
        assert_eq!(
            oos_parameters["train_window_ml_training_task_id"],
            "train-p7gb-window-001"
        );
        assert_eq!(
            oos_parameters["prediction_set_override_source"],
            "train_window_ml_internal_oos"
        );
    }

    #[test]
    fn train_window_ml_factor_refs_use_materialized_pit_atomic_features() {
        let factors = phase7_train_window_ml_factor_refs_for_profile(
            phase7_train_window_ml_feature_profile(),
        );

        assert_eq!(
            phase7_train_window_ml_feature_profile(),
            "phase7_gb_quality_value_recovery_low_impact_v5"
        );
        assert!(factors.len() >= 45);
        assert!(factors
            .iter()
            .all(|factor| factor.factor_version == "1.0.0"));
        assert!(factors
            .iter()
            .all(|factor| !factor.factor_code.starts_with("phase7_")));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roe_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_current_ratio_indrel_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_debt_to_assets_indrel_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_netprofit_margin_yoy_delta_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roa_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "val_pb_low_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "cf_ocf_to_profit_latest_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "amihud_20d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "val_pe_ttm_low_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roe_yoy_delta_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mkt_rel_mom_60d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "div_stability_4y_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "div_cash_sum_4y_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_net_amount_5d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_elg_net_amount_5d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_small_sell_pressure_20d_std"));
    }

    #[test]
    fn phase7_layered_request_accepts_current_event_nonlinear_alpha_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fs".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_event_nonlinear_alpha_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fs"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["current_event_nonlinear_alpha_profile"].is_string()
                && trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] != "phase7_quality_event_surprise_confirm_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["current_event_nonlinear_alpha_family"] == "event_surprise"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["current_event_nonlinear_alpha_family"] == "event_window"
                && matches!(
                    trial.parameters["event_gate_combo_name"].as_str(),
                    Some("phase7_event_window_earnings_v1" | "phase7_event_window_earnings_40d_v1")
                )
        }));
        for trial in &bundle.plan.trials {
            let serialized = trial.parameters.to_string();
            assert!(!serialized.contains("event_reaction"));
            assert!(!serialized.contains("post_return_curve"));
        }
    }

    #[test]
    fn phase7_layered_request_accepts_oos_regime_alpha_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ev".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_regime_alpha_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ev"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ev should seed OOS regime alpha rebuild first");
        assert_eq!(
            first_trial.parameters["oos_regime_alpha_rebuild_profile"],
            "oos_rebuild_quality_bull_top30_vol22"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(first_trial.parameters["candidate_risk_filter"], "off");
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
    }

    #[test]
    fn phase7_layered_request_accepts_oos_benchmark_excess_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ew".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_benchmark_excess_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ew"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ew should seed OOS benchmark-excess rebuild first");
        assert_eq!(
            first_trial.parameters["oos_benchmark_excess_rebuild_profile"],
            "oos_excess_event_nonlinear_top100_vol24"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_24_70_100"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_nonlinear_alpha_risk_memory_router_v3"
        );
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
    }

    #[test]
    fn phase7_layered_request_accepts_oos_execution_adaptive_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ex".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_execution_adaptive_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ex"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ex should seed OOS execution-adaptive rebuild first");
        assert_eq!(
            first_trial.parameters["oos_execution_adaptive_rebuild_profile"],
            "oos_execution_adaptive_event_top100_vol24"
        );
        assert_eq!(
            first_trial.parameters["portfolio_sharpe_control"],
            "gentle_roll_sharpe180_025_neg20_70"
        );
        assert_eq!(
            first_trial.parameters["capacity_risk_budget"],
            "capacity_stress_participation_headroom_floor_70_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_10d_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_carry_policy"],
            "roll_forward_v1"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_return_alpha_sharpe_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_cv".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_return_alpha_sharpe_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_frontier_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cw".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_frontier_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_frontier_decomposition_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cx".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_frontier_decomposition"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v4"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_141_462_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v5"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_return_micro_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cy".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_return_micro_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial.parameters["portfolio_volatility_control"] == "vol120_147_475_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn cache_stats_delta_reports_per_batch_increments_for_discovery_cache_reuse() {
        let before_signal = SignalDataCacheStats {
            combo_score_hits: 10,
            combo_score_misses: 4,
            prediction_score_hits: 2,
            prediction_score_misses: 1,
            return_history_hits: 100,
            return_history_covering_window_hits: 12,
            return_history_snapshot_hits: 7,
            average_amount_hits: 70,
            average_amount_symbol_hits: 28,
            average_amount_history_hits: 42,
            average_amount_history_covering_window_hits: 14,
            average_amount_history_snapshot_hits: 8,
            average_amount_symbol_misses: 11,
            average_amount_history_misses: 39,
            return_history_misses: 50,
            average_amount_misses: 50,
            ..Default::default()
        };
        let after_signal = SignalDataCacheStats {
            combo_score_hits: 13,
            combo_score_misses: 4,
            prediction_score_hits: 8,
            prediction_score_misses: 1,
            return_history_hits: 130,
            return_history_covering_window_hits: 20,
            return_history_snapshot_hits: 16,
            average_amount_hits: 95,
            average_amount_symbol_hits: 40,
            average_amount_history_hits: 55,
            average_amount_history_covering_window_hits: 20,
            average_amount_history_snapshot_hits: 18,
            average_amount_symbol_misses: 13,
            average_amount_history_misses: 42,
            return_history_misses: 55,
            average_amount_misses: 55,
            ..Default::default()
        };
        let signal_delta = signal_cache_stats_delta(before_signal, after_signal);

        assert_eq!(signal_delta.combo_score_hits, 3);
        assert_eq!(signal_delta.combo_score_misses, 0);
        assert_eq!(signal_delta.prediction_score_hits, 6);
        assert_eq!(signal_delta.prediction_score_misses, 0);
        assert_eq!(signal_delta.return_history_hits, 30);
        assert_eq!(signal_delta.return_history_covering_window_hits, 8);
        assert_eq!(signal_delta.return_history_snapshot_hits, 9);
        assert_eq!(signal_delta.average_amount_hits, 25);
        assert_eq!(signal_delta.average_amount_symbol_hits, 12);
        assert_eq!(signal_delta.average_amount_history_hits, 13);
        assert_eq!(signal_delta.average_amount_history_covering_window_hits, 6);
        assert_eq!(signal_delta.average_amount_history_snapshot_hits, 10);
        assert_eq!(signal_delta.return_history_misses, 5);
        assert_eq!(signal_delta.average_amount_misses, 5);
        assert_eq!(signal_delta.average_amount_symbol_misses, 2);
        assert_eq!(signal_delta.average_amount_history_misses, 3);

        let before_backtest = BacktestDataCacheStats {
            daily_bar_symbol_hits: 50,
            daily_bar_covering_window_hits: 10,
            daily_bar_snapshot_hits: 5,
            daily_bar_symbol_misses: 20,
            trading_profile_symbol_hits: 40,
            trading_profile_symbol_misses: 20,
            ..Default::default()
        };
        let after_backtest = BacktestDataCacheStats {
            daily_bar_symbol_hits: 80,
            daily_bar_covering_window_hits: 22,
            daily_bar_snapshot_hits: 9,
            daily_bar_symbol_misses: 22,
            trading_profile_symbol_hits: 70,
            trading_profile_symbol_misses: 22,
            ..Default::default()
        };
        let backtest_delta = backtest_cache_stats_delta(before_backtest, after_backtest);

        assert_eq!(backtest_delta.daily_bar_symbol_hits, 30);
        assert_eq!(backtest_delta.daily_bar_covering_window_hits, 12);
        assert_eq!(backtest_delta.daily_bar_snapshot_hits, 4);
        assert_eq!(backtest_delta.daily_bar_symbol_misses, 2);
        assert_eq!(backtest_delta.trading_profile_symbol_hits, 30);
        assert_eq!(backtest_delta.trading_profile_symbol_misses, 2);
    }

    #[test]
    fn aggregate_signal_cache_stats_collects_oos_cache_and_train_batches_without_double_counting() {
        let train_signal = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 2,
            persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
            ..Default::default()
        };
        let train_prewarm = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_return_values_loaded: 999_999,
            ..Default::default()
        };
        let oos_signal = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 1,
            persistent_return_risk_feature_matrix_return_values_loaded: 250_000,
            ..Default::default()
        };
        let oos_prewarm = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_return_values_loaded: 888_888,
            ..Default::default()
        };
        let metrics = json!({
            "cache": {
                "oos_signal_cache": oos_signal,
            },
            "windows": [{
                "train_batches": [{
                    "signal_cache": train_signal,
                    "signal_batch_prewarm": {
                        "report": {
                            "cache_delta": train_prewarm,
                        }
                    }
                }],
                "oos_market_feature_prewarm_report": {
                    "cache_delta": oos_prewarm,
                }
            }]
        });

        let stats = aggregate_signal_cache_stats_from_experiment_metrics(&metrics);

        assert_eq!(stats.persistent_return_risk_feature_matrix_hits, 3);
        assert_eq!(
            stats.persistent_return_risk_feature_matrix_return_values_loaded,
            750_482
        );
    }

    #[test]
    fn return_risk_cache_economics_report_json_prefers_stats_for_steady_state_pair() {
        let raw = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 2,
            persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
            ..Default::default()
        };
        let stats = SignalDataCacheStats {
            persistent_return_risk_stats_feature_matrix_hits: 4,
            persistent_return_risk_stats_feature_matrix_misses: 0,
            persistent_return_risk_stats_feature_matrix_writes: 0,
            persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 8_512,
            persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 191_580,
            ..Default::default()
        };

        let report = return_risk_cache_economics_report_json("exp-raw", raw, "exp-stats", stats);

        assert_eq!(report["raw_experiment_run_id"], "exp-raw");
        assert_eq!(report["stats_experiment_run_id"], "exp-stats");
        assert_eq!(
            report["cache_economics"]["recommendation"],
            "PreferStatsMatrix"
        );
        assert_eq!(
            report["cache_economics"]["reason"],
            "stats_payload_below_raw_return_values"
        );
    }

    #[test]
    fn return_risk_cache_economics_report_rejects_non_completed_inputs() {
        assert!(ensure_completed_cache_economics_input("exp-ok", "raw", "completed").is_ok());

        let error = ensure_completed_cache_economics_input("exp-failed", "stats", "failed")
            .expect_err("failed experiment should not be reportable");
        assert!(error.contains("requires completed stats experiment exp-failed"));
        assert!(error.contains("status=failed"));
    }

    #[test]
    fn sleeve_admission_diagnostic_matrix_explains_rejected_family_by_window() {
        let metrics = json!({
            "windows": [{
                "status": "skipped",
                "window": {
                    "window_index": 1,
                    "validation_mode": "walk_forward",
                    "train_start": "2014-01-02",
                    "train_end": "2016-12-31",
                    "test_start": "2017-01-01",
                    "test_end": "2017-12-31"
                },
                "train_optimization_task_id": "opt-train-1",
                "skip_reason": "window 1 has no training candidate passing robustness"
            }]
        });
        let mut rows_by_task = BTreeMap::new();
        rows_by_task.insert(
            "opt-train-1".to_string(),
            vec![SleeveAdmissionTrialDiagnosticRow {
                trial_id: "trial-valuation".to_string(),
                trial_index: 2,
                status: "completed".to_string(),
                backtest_task_id: Some("bt-valuation".to_string()),
                score: Some(Decimal::new(1209, 4)),
                parameters: json!({
                    "alpha_sleeve_family": "valuation_guard",
                    "alpha_source_family": "valuation_guard",
                    "combo_name": "phase7_valuation_v1",
                    "capacity_risk_budget": "capacity_stress_participation_alpha_headroom_floor_70_v1",
                    "execution_impact_budget": "impact_turnover_15pct_v1"
                }),
                backtest_parameters: Some(json!({
                    "top_n": 100,
                    "max_gross_exposure": 1.0,
                    "max_position_pct": 0.08,
                    "score_candidate_pool_size": 2200,
                    "cash_utilization": "stress_fill_gross_98_v1",
                    "candidate_ranking": "alpha_first_low_impact_v1",
                    "execution_schedule_profile": "twap_20d_v1",
                    "execution_rules": {
                        "execution_carry_policy": "roll_forward_v1",
                        "max_participation_rate": null
                    }
                })),
                metrics: Some(json!({
                    "annual_return_pct": "0.1209",
                    "excess_return_pct": "-0.0200",
                    "sharpe_ratio": "0.995",
                    "sortino_ratio": "1.355",
                    "calmar_ratio": "0.468",
                    "max_drawdown_pct": "0.258",
                    "final_execution_fill_ratio": "0.899",
                    "final_unfilled_target_gap_pct": "0.083",
                    "final_target_gross_exposure_pct": "0.780",
                    "final_actual_gross_exposure_pct": "0.697",
                    "final_cash_weight_pct": "0.303"
                })),
                constraint_violations: Some(json!([
                    {"constraint": "min_sharpe", "severity": "hard"}
                ])),
                portfolio_constraint_summary: Some(json!({
                    "total_count": 0,
                    "hard_count": 0,
                    "by_constraint": []
                })),
                robustness_status: Some("rejected".to_string()),
                gate_results: Some(json!([
                    {"gate": "min_sharpe", "passed": true, "actual": "0.995"},
                    {
                        "gate": "walk_forward_min_window_count",
                        "passed": true,
                        "details": {
                            "windows": [{
                                "window_index": 1,
                                "start_date": "2014-01-29",
                                "end_date": "2016-02-25",
                                "scenario": "bear",
                                "metrics": {
                                    "annual_return": 0.10,
                                    "excess_return": -0.04,
                                    "sharpe_ratio": 0.2,
                                    "sortino_ratio": 0.4,
                                    "max_drawdown": 0.22
                                }
                            }]
                        }
                    },
                    {
                        "gate": "train_cost_capacity_perturbation_pass_ratio",
                        "passed": false,
                        "actual": 0.0,
                        "passed_count": 0,
                        "total_count": 3
                    },
                    {
                        "gate": "train_avg_perturbed_calmar",
                        "passed": false,
                        "actual": "0.246"
                    },
                    {
                        "gate": "train_final_execution_fill_ratio",
                        "passed": false,
                        "actual": "0.808"
                    }
                ])),
            }],
        );

        let report = sleeve_admission_diagnostic_matrix_json(
            "exp-sleeve",
            "completed",
            &metrics,
            &rows_by_task,
        );

        assert_eq!(report["experiment_run_id"], "exp-sleeve");
        assert_eq!(report["window_count"], 1);
        assert_eq!(
            report["windows"][0]["best_rejected_trial"]["family"],
            "valuation_guard"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["family"],
            "valuation_guard"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["metrics"]["annual_return_pct"],
            "0.1209"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["execution_quality"]["fill_ratio"],
            "0.899"
        );
        let actual_gross_shortfall = report["windows"][0]["families"][0]["portfolio_expression"]
            ["actual_gross_shortfall_to_target"]
            .as_f64()
            .expect("numeric gross shortfall");
        assert!((actual_gross_shortfall - 0.083).abs() < 1e-9);
        assert_eq!(
            report["windows"][0]["families"][0]["capacity_headroom"]["fill_shortfall_to_90"],
            0.0010000000000000009
        );
        assert_eq!(
            report["windows"][0]["families"][0]["execution_capacity"]["configured"]
                ["explicit_participation_cap_configured"],
            false
        );
        assert_eq!(
            report["windows"][0]["families"][0]["diagnosis"]["primary_failure_axis"],
            "portfolio_expression_capacity_gap"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["stress"]["pass_ratio"],
            0.0
        );
        assert_eq!(
            report["windows"][0]["families"][0]["stress"]["avg_perturbed_calmar"],
            "0.246"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["failed_gates"][0]["gate"],
            "train_cost_capacity_perturbation_pass_ratio"
        );
        assert_eq!(
            report["windows"][0]["families"][0]["weak_regimes"][0]["scenario"],
            "bear"
        );
        assert_eq!(report["family_summary"][0]["family"], "valuation_guard");
        assert_eq!(report["family_summary"][0]["rejected_count"], 1);
    }

    #[test]
    fn sleeve_admission_diagnostic_matrix_recommends_compliant_next_actions() {
        let metrics = json!({
            "windows": [{
                "status": "skipped",
                "window": {
                    "window_index": 1,
                    "validation_mode": "walk_forward",
                    "train_start": "2014-01-02",
                    "train_end": "2016-12-31",
                    "test_start": "2017-01-01",
                    "test_end": "2017-12-31"
                },
                "train_optimization_task_id": "opt-train-actions",
                "skip_reason": "window 1 has no training candidate passing robustness"
            }]
        });
        let mut rows_by_task = BTreeMap::new();
        rows_by_task.insert(
            "opt-train-actions".to_string(),
            vec![
                SleeveAdmissionTrialDiagnosticRow {
                    trial_id: "trial-positive-unfilled".to_string(),
                    trial_index: 1,
                    status: "completed".to_string(),
                    backtest_task_id: Some("bt-positive-unfilled".to_string()),
                    score: Some(Decimal::new(11, 1)),
                    parameters: json!({
                        "alpha_sleeve_family": "valuation_guard",
                        "combo_name": "phase7_valuation_v1"
                    }),
                    backtest_parameters: Some(json!({
                        "capacity_risk_budget": "capacity_stress_participation_alpha_headroom_floor_70_v1",
                        "execution_impact_budget": "impact_turnover_15pct_v1",
                        "execution_schedule_profile": "twap_20d_v1",
                        "execution_rules": {
                            "max_participation_rate": null
                        }
                    })),
                    metrics: Some(json!({
                        "annual_return_pct": "0.12",
                        "excess_return_pct": "-0.03",
                        "sharpe_ratio": "0.95",
                        "calmar_ratio": "0.46",
                        "final_execution_fill_ratio": "0.82",
                        "final_unfilled_target_gap_pct": "0.12"
                    })),
                    constraint_violations: Some(json!([])),
                    portfolio_constraint_summary: Some(json!({
                        "total_count": 0,
                        "hard_count": 0,
                        "by_constraint": []
                    })),
                    robustness_status: Some("rejected".to_string()),
                    gate_results: Some(json!([
                        {
                            "gate": "train_cost_capacity_perturbation_pass_ratio",
                            "passed": false,
                            "actual": 0.0,
                            "passed_count": 0,
                            "total_count": 3
                        },
                        {
                            "gate": "train_final_unfilled_target_gap",
                            "passed": false,
                            "actual": "0.12",
                            "limit": "0.08"
                        }
                    ])),
                },
                SleeveAdmissionTrialDiagnosticRow {
                    trial_id: "trial-negative-alpha".to_string(),
                    trial_index: 2,
                    status: "completed".to_string(),
                    backtest_task_id: Some("bt-negative-alpha".to_string()),
                    score: Some(Decimal::new(-5, 1)),
                    parameters: json!({
                        "alpha_sleeve_family": "relative_strength",
                        "combo_name": "phase7_quality_relative_strength_v1"
                    }),
                    backtest_parameters: Some(json!({})),
                    metrics: Some(json!({
                        "annual_return_pct": "-0.04",
                        "excess_return_pct": "-0.10",
                        "sharpe_ratio": "-0.30",
                        "calmar_ratio": "-0.12",
                        "final_execution_fill_ratio": "0.93",
                        "final_unfilled_target_gap_pct": "0.04"
                    })),
                    constraint_violations: Some(json!([])),
                    portfolio_constraint_summary: Some(json!({
                        "total_count": 1,
                        "hard_count": 1,
                        "by_constraint": [{
                            "constraint_name": "participation_rate",
                            "severity": "hard",
                            "count": 1,
                            "max_limit_value": "0.10",
                            "max_actual_value": "0.22",
                            "avg_actual_value": "0.22"
                        }]
                    })),
                    robustness_status: Some("rejected".to_string()),
                    gate_results: Some(json!([
                        {
                            "gate": "min_annual_return",
                            "passed": false,
                            "actual": "-0.04",
                            "limit": "0"
                        },
                        {
                            "gate": "min_sharpe",
                            "passed": false,
                            "actual": "-0.30",
                            "limit": "0"
                        }
                    ])),
                },
            ],
        );

        let report = sleeve_admission_diagnostic_matrix_json(
            "exp-actions",
            "completed",
            &metrics,
            &rows_by_task,
        );
        let action_summary = &report["action_summary"];

        assert_eq!(action_summary["positive_trial_count"], 1);
        assert_eq!(action_summary["positive_stress_evaluated_count"], 1);
        assert_eq!(action_summary["positive_stress_passed_count"], 0);
        assert_eq!(action_summary["positive_fill_below_90_count"], 1);
        assert_eq!(action_summary["positive_unfilled_above_08_count"], 1);
        assert_eq!(action_summary["positive_negative_excess_count"], 1);
        assert_eq!(
            action_summary["next_actions"][0]["action"],
            "repair_execution_capacity_for_positive_alpha"
        );
        assert_eq!(
            action_summary["next_actions"][0]["families"][0],
            "valuation_guard"
        );
        assert_eq!(
            action_summary["next_actions"][1]["action"],
            "rebuild_alpha_sources_for_weak_or_negative_train_windows"
        );
        assert_eq!(
            action_summary["non_goals"][0],
            "do_not_relax_train_or_oos_gates"
        );
    }

    #[test]
    fn factor_batch_signal_prewarm_plan_extracts_only_valid_factor_requests() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "full-market-2016-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20200101",
                "end_date": "20230101",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "20",
                "max_position_pct": 0.08,
                "return_risk_feature_cache_mode": "stats_matrix_experimental"
            }),
            objective: json!({"type": "professional_candidate"}),
            constraints: None,
        };
        let pending_trials = vec![
            (
                "trial-factor-a".to_string(),
                1,
                json!({"top_n": 18, "market_regime": "quality_bear_window_guard_v2"}),
            ),
            (
                "trial-prediction".to_string(),
                2,
                json!({
                    "signal_source": "prediction",
                    "prediction_set_id": "pred-phase7-v1"
                }),
            ),
            (
                "trial-invalid".to_string(),
                3,
                json!({"top_n": {"bad": "shape"}}),
            ),
            (
                "trial-factor-b".to_string(),
                4,
                json!({"rebalance": "60", "score_candidate_pool_size": 600}),
            ),
        ];

        let plan = plan_factor_signal_batch_prewarm_requests(&task, &pending_trials);

        assert_eq!(plan.requested_trials, 4);
        assert_eq!(plan.factor_requests.len(), 2);
        assert_eq!(plan.prediction_trials, 1);
        assert_eq!(plan.invalid_trials, 1);
        assert_eq!(plan.factor_requests[0].top_n, 18);
        assert_eq!(
            plan.factor_requests[0]
                .market_regime
                .as_ref()
                .unwrap()
                .policy,
            Some("quality_bear_window_guard_v2".to_string())
        );
        assert_eq!(plan.factor_requests[1].rebalance, "60");
        assert_eq!(plan.factor_requests[1].score_candidate_pool_size, Some(600));
        assert_eq!(
            plan.factor_requests[0]
                .return_risk_feature_cache_mode
                .as_deref(),
            Some("stats_matrix_experimental")
        );
        assert_eq!(
            plan.factor_requests[1]
                .return_risk_feature_cache_mode
                .as_deref(),
            Some("stats_matrix_experimental")
        );
    }

    #[test]
    fn parallel_trial_cache_forks_start_from_shared_snapshot_without_parent_stats() {
        let parent_signal_cache = SignalDataCache::default();
        let parent_backtest_cache = BacktestDataCache::default();
        let signal_snapshot = parent_signal_cache.snapshot();
        let backtest_snapshot = parent_backtest_cache.snapshot();

        let signal_fork = SignalDataCache::from_snapshot(&signal_snapshot);
        let backtest_fork = BacktestDataCache::from_snapshot(&backtest_snapshot);

        assert_eq!(parent_signal_cache.stats().return_history_hits, 0);
        assert_eq!(parent_backtest_cache.stats().daily_bar_symbol_hits, 0);
        assert_eq!(signal_fork.stats().return_history_hits, 0);
        assert_eq!(backtest_fork.stats().daily_bar_symbol_hits, 0);
    }

    #[test]
    fn elite_validation_plateau_scores_stable_parameter_neighbors() {
        let candidate = report_trial(
            "candidate",
            1,
            json!({"top_n": 20, "rebalance": "60", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.1505,
                "excess_return_pct": 0.05,
                "sharpe_ratio": 1.02,
                "sortino_ratio": 1.733,
                "calmar_ratio": 0.715,
                "profit_factor": 1.20,
                "max_drawdown_pct": 0.2105,
                "max_drawdown_duration_days": 90,
                "num_trades": 260
            }),
        );
        let stable_neighbor = report_trial(
            "stable",
            2,
            json!({"top_n": 21, "rebalance": "60", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.147,
                "excess_return_pct": 0.04,
                "sharpe_ratio": 0.96,
                "sortino_ratio": 1.62,
                "calmar_ratio": 0.69,
                "profit_factor": 1.15,
                "max_drawdown_pct": 0.225,
                "max_drawdown_duration_days": 95,
                "num_trades": 255
            }),
        );
        let unstable_neighbor = report_trial(
            "unstable",
            3,
            json!({"top_n": 20, "rebalance": "55", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.120,
                "excess_return_pct": 0.01,
                "sharpe_ratio": 0.75,
                "sortino_ratio": 1.10,
                "calmar_ratio": 0.50,
                "profit_factor": 1.0,
                "max_drawdown_pct": 0.31,
                "max_drawdown_duration_days": 160,
                "num_trades": 240
            }),
        );
        let trials = vec![
            candidate.clone(),
            stable_neighbor.clone(),
            unstable_neighbor.clone(),
        ];

        let plateau = build_parameter_plateau_analysis(&candidate, &trials);

        assert_eq!(plateau["near_neighbor_count"], 2);
        assert_eq!(plateau["stable_neighbor_count"], 1);
        assert_eq!(plateau["single_axis"][0]["parameter"], "rebalance");
        assert_eq!(plateau["single_axis"][1]["parameter"], "top_n");
    }

    #[test]
    fn portfolio_correlation_contribution_scores_explicit_and_peer_controls() {
        let candidate = report_trial(
            "controlled",
            1,
            json!({
                "portfolio_method": "risk_budget",
                "max_pairwise_correlation": "0.70",
                "correlation_lookback_days": 120,
                "candidate_risk_filter": "low_volatility_low_correlation_v1",
                "risk_contribution_control": "soft_single_name_20pct_v1"
            }),
            json!({
                "annual_return_pct": 0.16,
                "excess_return_pct": 0.05,
                "sharpe_ratio": 1.05,
                "sortino_ratio": 1.70,
                "calmar_ratio": 0.75,
                "profit_factor": 1.30,
                "max_drawdown_pct": 0.22,
                "num_trades": 250
            }),
        );
        let uncontrolled = report_trial(
            "uncontrolled",
            2,
            json!({"portfolio_method": "heuristic"}),
            json!({
                "annual_return_pct": 0.17,
                "excess_return_pct": 0.06,
                "sharpe_ratio": 0.75,
                "sortino_ratio": 1.30,
                "calmar_ratio": 0.45,
                "profit_factor": 1.05,
                "max_drawdown_pct": 0.38,
                "num_trades": 260
            }),
        );
        let trials = vec![candidate.clone(), uncontrolled];

        let contribution = build_portfolio_correlation_contribution_score(&candidate, &trials);

        assert!(
            contribution["explicit_control_score"].as_f64().unwrap() > 0.85,
            "{contribution}"
        );
        assert!(
            contribution["contribution_score"].as_f64().unwrap() > 0.70,
            "{contribution}"
        );
        assert_eq!(contribution["active_controls"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn elite_metric_enrichment_marks_sources_and_missing_fields() {
        let mut metrics = json!({
            "annual_return_pct": 0.16,
            "excess_return_pct": 0.02,
            "sharpe_ratio": 1.1,
            "sortino_ratio": 1.7,
            "max_drawdown_pct": 0.22,
            "num_trades": 320
        });
        let mut sources = existing_metric_sources(&metrics);

        insert_metric_if_missing(
            &mut metrics,
            &mut sources,
            "calmar_ratio",
            Some(json!(0.72)),
            "backtest_result.calmar_ratio",
        );

        let missing = missing_elite_metrics(&metrics);
        assert_eq!(
            sources["annual_return_pct"],
            json!("optimization_trial.metrics")
        );
        assert_eq!(
            sources["calmar_ratio"],
            json!("backtest_result.calmar_ratio")
        );
        assert!(!missing.contains(&"calmar_ratio"));
        assert!(missing.contains(&"profit_factor"));
        assert!(missing.contains(&"max_drawdown_duration_days"));
    }

    #[test]
    fn phase7_layered_request_accepts_return_distribution_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cp".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_return_distribution_repair"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["event_gate_profile"] == "event_window_10d_boost_p75_3pct"
                && trial.parameters["event_gate_combo_name"]
                    == "phase7_event_window_earnings_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "residual_confirm_top40"
                && trial.parameters["event_gate_combo_name"]
                    == "phase7_quality_residual_confirm_10pct_v1"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_alpha_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bj".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_alpha_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_position_risk_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bk".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_position_risk_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_position_risk_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bl".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_position_risk_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_position_guard_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_position_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_all_regime_event_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bm".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_all_regime_event_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_15pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_volatility_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ai".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_volatility_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert_eq!(
            bundle.plan.trials[0].parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_position_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_aj".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_position_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v2"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_anti_overfit_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ab".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_anti_overfit_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_earnings_v1"
                || trial.parameters["combo_name"] == "phase7_event_surprise_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_candidate_risk_filter_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ac".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_candidate_risk_filter"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["candidate_risk_filter"],
            "off"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_contribution_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ad".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_contribution"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["risk_contribution_control"],
            "off"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_conditioned_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ae".to_string()),
        };
        let resource_plan = quant_api::discovery::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_conditioned_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_pos_5pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["event_gate_mode"] == "boost_positive"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
        }));
    }

    #[test]
    fn professional_discovery_defaults_backtest_template_to_full_history_window() {
        let req = Phase7ProfessionalDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: None,
            trial_batch_limit: Some(2),
            max_batches: Some(4),
            robustness_top_n: Some(3),
            robustness_gate_policy: None,
            stop_after_professional_candidate: Some(false),
            stop_after_robust_approval: Some(true),
        };

        let layered_req = phase7_discovery_layered_request(&req);

        let template = layered_req.backtest_template.expect("default template");
        assert_eq!(template["start_date"], "20160201");
        assert_eq!(template["end_date"], "20260515");
        assert_eq!(template["benchmark"], "000300.SH");
        assert_eq!(template["mode"], "standard");
        assert_eq!(template["effective_coverage"]["mode"], "adjust_start");
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
    fn professional_candidate_score_prefers_sharpe_gap_after_annual_floor() {
        let mut high_return_low_sharpe = quant_backtest::metrics::BacktestMetrics::default();
        high_return_low_sharpe.annual_return_pct = Decimal::new(19, 2);
        high_return_low_sharpe.excess_return_pct = Decimal::new(400, 2);
        high_return_low_sharpe.sharpe_ratio = Decimal::new(82, 2);
        high_return_low_sharpe.sortino_ratio = Decimal::new(20, 1);
        high_return_low_sharpe.max_drawdown_pct = Decimal::new(24, 2);
        high_return_low_sharpe.num_trades = 2000;

        let mut lower_return_better_sharpe = quant_backtest::metrics::BacktestMetrics::default();
        lower_return_better_sharpe.annual_return_pct = Decimal::new(151, 3);
        lower_return_better_sharpe.excess_return_pct = Decimal::new(220, 2);
        lower_return_better_sharpe.sharpe_ratio = Decimal::new(95, 2);
        lower_return_better_sharpe.sortino_ratio = Decimal::new(17, 1);
        lower_return_better_sharpe.max_drawdown_pct = Decimal::new(23, 2);
        lower_return_better_sharpe.num_trades = 1800;

        let objective = json!({"type": "professional_candidate"});
        let constraints = json!({
            "min_annual_return": 0.15,
            "min_excess_return": 0.0,
            "min_sharpe": 1.0,
            "min_sortino": 1.5,
            "max_drawdown": 0.35,
            "min_trade_count": 1
        });

        let high_return_score =
            score_trial(&high_return_low_sharpe, &objective, Some(&constraints)).score;
        let better_sharpe_score =
            score_trial(&lower_return_better_sharpe, &objective, Some(&constraints)).score;

        assert!(
            better_sharpe_score > high_return_score,
            "professional objective should rank smaller Sharpe gap above raw return once annual floor is met"
        );
    }

    #[test]
    fn scoring_and_robustness_preserve_effective_coverage_metadata() {
        let mut metrics = quant_backtest::metrics::BacktestMetrics::default();
        metrics.annual_return_pct = Decimal::new(18, 2);
        metrics.excess_return_pct = Decimal::new(10, 2);
        metrics.sharpe_ratio = Decimal::new(8, 1);
        metrics.sortino_ratio = Decimal::new(18, 1);
        metrics.max_drawdown_pct = Decimal::new(30, 2);
        metrics.num_trades = 128;
        metrics.execution_schedule_expired_count = 2;
        metrics.max_execution_target_gap_pct = Decimal::new(12, 2);
        metrics.final_cash_weight_pct = Decimal::new(18, 2);
        let output = FactorBacktestRunOutput {
            signals_count: 10,
            metrics,
            trades: 128,
            equity_points: 512,
            effective_coverage: Some(crate::routes::backtest::EffectiveCoverageRunSummary {
                requested_start_date: NaiveDate::from_ymd_opt(2016, 2, 1).unwrap(),
                effective_start_date: NaiveDate::from_ymd_opt(2016, 3, 4).unwrap(),
                adjusted: true,
                mode: "adjust_start".to_string(),
                min_rows: 20,
                observed_rows: 3470,
                coverage_start_date: NaiveDate::from_ymd_opt(2016, 2, 1).unwrap(),
                warmup_start_date: Some(NaiveDate::from_ymd_opt(2016, 3, 4).unwrap()),
                warmup_trading_days: Some(19),
                combo_name: "phase7_financial_quality_v1".to_string(),
                version: "1.0.0".to_string(),
                universe_profile: Some("listed_non_st".to_string()),
            }),
            market_data_prewarm_report: None,
            market_feature_prewarm_report: None,
        };

        let scored = score_trial_with_output(
            &output,
            &json!({"type": "professional_candidate"}),
            Some(&json!({"min_trade_count": 1})),
        );

        assert_eq!(
            scored.metrics["effective_coverage"]["effective_start_date"],
            "2016-03-04"
        );
        assert_eq!(scored.metrics["execution_schedule_expired_count"], 2);
        assert_eq!(scored.metrics["max_execution_target_gap_pct"], "0.12");
        assert_eq!(scored.metrics["final_cash_weight_pct"], "0.18");
        let evaluation = evaluate_robustness_gates_with_analysis(
            scored.score,
            None,
            &scored.metrics,
            &scored.constraint_violations,
            Some(&json!({"min_trade_count": 1, "max_drawdown": 0.35})),
            None,
        );
        assert!(evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| { gate["gate"] == "effective_coverage_start" && gate["passed"] == true }));
    }

    #[test]
    fn oos_window_execution_json_includes_market_data_prewarm_report() {
        let execution = OosWindowExecution {
            window: OosDiscoveryWindow {
                window_index: 1,
                validation_mode: "walk_forward".to_string(),
                train_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                train_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                test_start: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                test_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            },
            train_optimization_task_id: "train-task-1".to_string(),
            train_execution_policy: OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
            train_batches: Vec::new(),
            selected_candidate: DiscoveryCandidate {
                trial_id: "trial-1".to_string(),
                backtest_task_id: None,
                score: None,
                candidate_type: CandidateType::Professional,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics::default(),
                parameters: json!({}),
            },
            train_robustness: None,
            train_cost_capacity_perturbations: Vec::new(),
            oos_backtest_task_id: "oos-task-1".to_string(),
            oos_output: FactorBacktestRunOutput {
                signals_count: 0,
                metrics: quant_backtest::metrics::BacktestMetrics::default(),
                trades: 0,
                equity_points: 0,
                effective_coverage: None,
                market_data_prewarm_report: Some(
                    quant_backtest::runner::BacktestMarketDataPrewarmReport {
                        snapshot_key: quant_backtest::runner::BacktestMarketDataSnapshotKey::new(
                            "full-market-2016-v1",
                            "000300.SH",
                            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                            NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                            &["AAA".to_string(), "BBB".to_string()],
                        ),
                        start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                        end_date: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                        benchmark: "000300.SH".to_string(),
                        symbol_count: 2,
                        cache_delta: Default::default(),
                    },
                ),
                market_feature_prewarm_report: Some(
                    quant_backtest::signal_generator::MarketFeaturePrewarmReport {
                        snapshot_key:
                            quant_backtest::signal_generator::MarketFeatureSnapshotKey::new(
                                "full-market-2016-v1",
                                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                                NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                                NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                                NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                                60,
                                &["AAA".to_string(), "BBB".to_string()],
                            ),
                        feature_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                        feature_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                        symbol_count: 2,
                        lookback_days: 60,
                        return_risk_feature_cache_mode:
                            quant_backtest::signal_generator::ReturnRiskFeatureCacheMode::RawMatrix,
                        cache_delta: Default::default(),
                    },
                ),
            },
            oos_points: Vec::new(),
            cost_capacity_perturbations: Vec::new(),
            skip_reason: None,
        };

        let json = oos_window_execution_json(&execution, &json!({}));

        assert!(json.get("oos_market_data_prewarm_report").is_some());
        assert!(json.get("oos_market_feature_prewarm_report").is_some());
    }

    #[test]
    fn oos_window_execution_json_explains_gb_quality_train_stress_score() {
        let candidate = DiscoveryCandidate {
            trial_id: "trial-gb-quality".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(18, 2),
                excess_return: Decimal::new(8, 2),
                sharpe: Decimal::new(12, 1),
                sortino: Decimal::new(17, 1),
                max_drawdown: Decimal::new(18, 2),
                num_trades: 260,
                final_cash_weight: Decimal::new(24, 2),
                final_actual_gross_exposure: Decimal::new(76, 2),
                final_unfilled_target_gap: Decimal::new(2, 2),
                final_execution_fill_ratio: Decimal::new(98, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({
                "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                "prediction_min_score": "0.01",
                "prediction_min_percentile": "0.35",
                "prediction_set_override_source": "train_window_ml_internal"
            }),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(140, 2);
        summary.avg_calmar = Decimal::new(165, 2);
        summary.avg_sharpe = Decimal::new(120, 2);
        summary.min_sortino = Decimal::new(160, 2);
        summary.min_annual_return = Decimal::new(9, 2);
        summary.max_drawdown = Decimal::new(16, 2);
        summary.min_num_trades = 240;
        summary.max_final_cash_weight = Decimal::new(26, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(74, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(3, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let execution = OosWindowExecution {
            window: OosDiscoveryWindow {
                window_index: 1,
                validation_mode: "walk_forward".to_string(),
                train_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                train_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                test_start: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                test_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            },
            train_optimization_task_id: "train-task-1".to_string(),
            train_execution_policy: OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
            train_batches: Vec::new(),
            selected_candidate: candidate,
            train_robustness: None,
            train_cost_capacity_perturbations: Vec::new(),
            oos_backtest_task_id: "oos-task-1".to_string(),
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
            skip_reason: None,
        };
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_perturbed_calmar": 1.2,
            "min_train_avg_perturbed_sharpe": 0.5,
            "min_train_perturbed_sortino": 1.0,
            "min_train_trade_count": 100,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let json = oos_window_execution_json_with_train_summary(&execution, &policy, &summary);
        let breakdown = json["train_stress_score_breakdown"]
            .as_object()
            .expect("train stress score breakdown");

        assert_eq!(
            breakdown["profile"],
            "prediction_confidence_stress_fill_quality_score_v1"
        );
        assert_eq!(breakdown["summary"]["pass_ratio"], 1.0);
        assert_eq!(breakdown["summary"]["min_sortino"], "1.60");
        assert!(breakdown["components"]["prediction_confidence_score"].is_string());
        assert!(breakdown["components"]["pass_ratio_bonus"].is_string());
        assert!(breakdown["components"]["annual_quality_bonus"].is_string());
        assert!(breakdown["components"]["calmar_quality_bonus"].is_string());
        assert!(breakdown["components"]["sharpe_quality_bonus"].is_string());
        assert!(breakdown["components"]["sortino_quality_bonus"].is_string());
        assert!(breakdown["penalties"]["drawdown_tail_penalty"].is_string());
        assert!(breakdown["penalties"]["min_annual_shortfall"].is_string());
        assert!(breakdown["penalties"]["calmar_shortfall"].is_string());
        assert!(breakdown["penalties"]["sharpe_shortfall"].is_string());
        assert!(breakdown["penalties"]["sortino_shortfall"].is_string());
        assert_eq!(
            json["train_stress_adjusted_score"],
            breakdown["total_score"]
        );
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
        assert_eq!(policy["min_positive_annual_return_window_ratio"], 0.60);
        assert_eq!(policy["min_walk_forward_median_sharpe"], 0.30);
        assert_eq!(policy["min_bootstrap_sharpe_p05"], 0.0);
    }

    #[test]
    fn professional_elite_robustness_policy_encodes_metric_matrix() {
        let policy = default_professional_elite_robustness_policy();

        assert_eq!(policy["candidate_tier"], "professional_elite");
        assert_eq!(policy["min_annual_return"], 0.15);
        assert_eq!(policy["min_sharpe"], 1.5);
        assert_eq!(policy["min_sortino"], 1.8);
        assert_eq!(policy["min_calmar"], 2.0);
        assert_eq!(policy["min_profit_factor"], 1.5);
        assert_eq!(policy["min_trade_count"], 200);
        assert_eq!(policy["max_drawdown_duration_days"], 126);
        assert_eq!(policy["bootstrap_trials"], 1000);
        assert_eq!(
            resolve_robustness_gate_policy(Some(&json!({"preset": "professional_elite"})))
                ["min_calmar"],
            2.0
        );
    }

    #[test]
    fn oos_train_selection_policy_is_separate_from_final_professional_gate() {
        let train_policy = default_oos_train_selection_gate_policy();
        let professional_policy = default_professional_robustness_policy();

        assert_eq!(train_policy["candidate_tier"], "oos_train_selection");
        assert_eq!(train_policy["min_annual_return"], 0.0);
        assert!(train_policy.get("min_excess_return").is_none());
        assert_eq!(train_policy["min_sharpe"], 0.0);
        assert_eq!(train_policy["min_walk_forward_windows"], 1);
        assert!(train_policy.get("min_sortino").is_none());
        assert!(train_policy.get("min_calmar").is_none());
        assert!(
            train_policy["min_sharpe"].as_f64().unwrap()
                < professional_policy["min_sharpe"].as_f64().unwrap()
        );
        assert!(
            train_policy["min_walk_forward_windows"].as_i64().unwrap()
                < professional_policy["min_walk_forward_windows"]
                    .as_i64()
                    .unwrap()
        );
    }

    #[test]
    fn oos_final_promotion_policy_keeps_strict_stitched_oos_gates() {
        let policy = default_oos_final_promotion_gate_policy("walk_forward");

        assert_eq!(policy["candidate_tier"], "oos_final_promotion");
        assert_eq!(policy["min_stitched_oos_calmar"], 1.2);
        assert_eq!(policy["min_positive_oos_window_ratio"], 0.60);
        assert_eq!(policy["min_oos_window_count"], 3);
        assert_eq!(policy["require_train_selection_approval"], true);
        assert_eq!(policy["require_no_train_test_overlap"], true);
    }

    #[test]
    fn oos_train_selection_policy_preserves_legacy_professional_override() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: Some(json!({"preset": "professional"})),
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let policy = resolve_oos_train_selection_gate_policy(&req);

        assert_eq!(policy["candidate_tier"], "professional_observation");
        assert_eq!(policy["min_sharpe"], 1.0);
        assert_eq!(policy["min_walk_forward_windows"], 4);
    }

    #[test]
    fn oos_train_selection_policy_ignores_final_objective_constraint_violations_by_default() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(9, 1)),
            &json!({
                "num_trades": 40,
                "annual_return_pct": "0.02",
                "excess_return_pct": "0.01",
                "sharpe_ratio": "0.20",
                "max_drawdown_pct": "0.10"
            }),
            &json!([
                {
                    "constraint": "min_annual_return",
                    "severity": "hard",
                    "limit": "0.15",
                    "actual": "0.02"
                },
                {
                    "constraint": "min_sharpe",
                    "severity": "hard",
                    "limit": "1.0",
                    "actual": "0.20"
                }
            ]),
            Some(&default_oos_train_selection_gate_policy()),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(evaluation.gates.as_array().unwrap().iter().any(|gate| {
            gate["gate"] == "no_hard_constraint_violations"
                && gate["passed"] == true
                && gate["enforced"] == false
        }));
    }

    #[test]
    fn oos_train_selection_policy_does_not_require_positive_train_excess_by_default() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(9, 1)),
            &json!({
                "num_trades": 80,
                "annual_return_pct": "0.14",
                "excess_return_pct": "-0.15",
                "sharpe_ratio": "1.02",
                "sortino_ratio": "1.51",
                "calmar_ratio": "2.27",
                "max_drawdown_pct": "0.06"
            }),
            &json!([
                {
                    "constraint": "min_annual_return",
                    "severity": "hard",
                    "limit": "0.15",
                    "actual": "0.14"
                },
                {
                    "constraint": "min_excess_return",
                    "severity": "hard",
                    "limit": "0.0",
                    "actual": "-0.15"
                }
            ]),
            Some(&default_oos_train_selection_gate_policy()),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(!evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| { gate["gate"] == "min_excess_return" }));
    }

    #[test]
    fn oos_train_selection_best_effort_diagnostic_mode_is_opt_in() {
        let default_policy = default_oos_train_selection_gate_policy();
        let diagnostic_policy = merge_gate_policy(
            default_policy.clone(),
            &json!({"allow_best_effort_train_selection_for_diagnostics": true}),
        );

        assert!(!best_effort_train_selection_for_diagnostics_enabled(
            &default_policy
        ));
        assert!(best_effort_train_selection_for_diagnostics_enabled(
            &diagnostic_policy
        ));
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
                "calmar_ratio": "0.90",
                "profit_factor": "1.20",
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
    fn elite_robustness_gate_rejects_low_calmar_and_profit_factor() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({
                "num_trades": 250,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.04",
                "sharpe_ratio": "1.60",
                "sortino_ratio": "1.90",
                "calmar_ratio": "1.20",
                "profit_factor": "1.20",
                "max_drawdown_duration_days": 180,
                "max_drawdown_pct": "0.15"
            }),
            &json!([]),
            Some(&default_professional_elite_robustness_policy()),
        );

        assert_eq!(evaluation.status, "rejected");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "min_calmar" && gate["passed"] == false));
        assert!(gates
            .iter()
            .any(|gate| { gate["gate"] == "min_profit_factor" && gate["passed"] == false }));
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "max_drawdown_duration_days" && gate["passed"] == false
        }));
    }

    #[test]
    fn market_scenario_classifies_regime_from_benchmark_path() {
        let bull = RobustnessMetricSummary {
            total_return: 0.18,
            annual_return: 0.20,
            sharpe_ratio: 1.2,
            sortino_ratio: 1.8,
            calmar_ratio: 5.0,
            max_drawdown: 0.04,
            benchmark_return: Some(0.18),
            excess_return: Some(0.02),
        };
        let bear = RobustnessMetricSummary {
            total_return: -0.02,
            annual_return: -0.02,
            sharpe_ratio: -0.3,
            sortino_ratio: -0.2,
            calmar_ratio: -0.08,
            max_drawdown: 0.24,
            benchmark_return: Some(-0.18),
            excess_return: Some(0.16),
        };
        let high_vol = RobustnessMetricSummary {
            total_return: 0.01,
            annual_return: 0.01,
            sharpe_ratio: 0.1,
            sortino_ratio: 0.1,
            calmar_ratio: 0.1,
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
        assert!(analysis["calmar_ratio"]["median"].is_number());
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
        assert!(analysis["calmar_ratio"]["median"].is_number());
        assert!(analysis["max_drawdown"]["p95"].is_number());
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

    #[test]
    fn robustness_gate_rejects_overfit_candidate_with_weak_window_distribution() {
        let analysis = RobustnessTimeSeriesAnalysis {
            market_scenarios: json!({
                "scenario_count": 3,
                "scenarios": []
            }),
            walk_forward: json!({
                "window_count": 8,
                "positive_excess_window_ratio": 0.75,
                "positive_annual_return_ratio": 0.50,
                "sharpe_ratio": {
                    "p05": -0.20,
                    "median": 0.10,
                    "p95": 1.40,
                    "mean": 0.35
                },
                "windows": []
            }),
            bootstrap: json!({
                "positive_return_probability": 0.95,
                "sharpe_ratio": {
                    "p05": -0.05,
                    "median": 0.80,
                    "p95": 1.40,
                    "mean": 0.78
                }
            }),
        };

        let evaluation = evaluate_robustness_gates_with_analysis(
            Decimal::new(10, 1),
            Some(Decimal::new(7, 1)),
            &json!({
                "num_trades": 120,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.40",
                "sharpe_ratio": "1.10",
                "sortino_ratio": "1.80",
                "max_drawdown_pct": "0.25"
            }),
            &json!([]),
            Some(&default_professional_robustness_policy()),
            Some(&analysis),
        );

        assert_eq!(evaluation.status, "rejected");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "walk_forward_positive_annual_return_ratio" && gate["passed"] == false
        }));
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "walk_forward_median_sharpe" && gate["passed"] == false
        }));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "bootstrap_sharpe_p05" && gate["passed"] == false));
    }

    #[test]
    fn robustness_failure_attribution_ranks_failed_gates_and_weak_windows() {
        let gates = json!([
            {
                "gate": "min_sharpe",
                "passed": false,
                "limit": 1.0,
                "actual": 0.714
            },
            {
                "gate": "walk_forward_min_window_count",
                "passed": true,
                "limit": 2,
                "actual": 3,
                "details": {
                    "windows": [
                        {
                            "window_index": 1,
                            "start_date": "2016-03-04",
                            "end_date": "2019-03-04",
                            "scenario": "bear",
                            "metrics": {
                                "annual_return": 0.04,
                                "excess_return": -0.02,
                                "sharpe_ratio": 0.18,
                                "sortino_ratio": 0.40,
                                "max_drawdown": 0.34
                            }
                        },
                        {
                            "window_index": 2,
                            "start_date": "2019-06-03",
                            "end_date": "2022-06-03",
                            "scenario": "mixed",
                            "metrics": {
                                "annual_return": 0.13,
                                "excess_return": 0.03,
                                "sharpe_ratio": 0.52,
                                "sortino_ratio": 1.10,
                                "max_drawdown": 0.24
                            }
                        },
                        {
                            "window_index": 3,
                            "start_date": "2022-09-01",
                            "end_date": "2025-09-01",
                            "scenario": "bull",
                            "metrics": {
                                "annual_return": 0.22,
                                "excess_return": 0.08,
                                "sharpe_ratio": 0.92,
                                "sortino_ratio": 1.80,
                                "max_drawdown": 0.18
                            }
                        }
                    ]
                }
            },
            {
                "gate": "bootstrap_positive_return_probability",
                "passed": true,
                "limit": 0.70,
                "actual": 0.99,
                "details": {
                    "positive_return_probability": 0.99,
                    "total_return": {"p05": 0.18, "median": 3.0, "p95": 8.0},
                    "sharpe_ratio": {"p05": 0.20, "median": 0.70, "p95": 1.30},
                    "sortino_ratio": {"p05": 0.50, "median": 1.58, "p95": 2.40}
                }
            }
        ]);

        let attribution = build_robustness_failure_attribution(&gates);

        assert_eq!(attribution["failed_gates"][0]["gate"], "min_sharpe");
        assert_eq!(
            attribution["worst_walk_forward_windows"][0]["start_date"],
            "2016-03-04"
        );
        assert_eq!(attribution["weak_market_scenarios"][0]["scenario"], "bear");
        assert_eq!(attribution["bootstrap_tail"]["sharpe_ratio"]["p05"], 0.20);
        assert!(attribution["primary_failure_modes"]
            .as_array()
            .unwrap()
            .contains(&json!("sharpe_shortfall")));
    }

    #[test]
    fn main_business_diagnostics_defaults_are_research_only_and_bounded() {
        let req = MainBusinessDiagnosticsRequest {
            start_date: "20140101".to_string(),
            end_date: "20260531".to_string(),
            business_type: None,
            universe_profile: None,
            profiles: None,
            min_day_coverage_ratio: Some(2.0),
            min_daily_rows: Some(0),
            min_p95_daily_row_ratio: Some(-1.0),
            min_daily_coverage_ratio: Some(2.0),
            persist_report: None,
            return_horizons: None,
            bucket_count: None,
            max_rank_ic_days: None,
            include_exposure_regime_metrics: None,
            max_exposure_regime_days: None,
        };

        assert_eq!(main_business_diagnostics_business_type(&req), "P");
        assert_eq!(
            main_business_diagnostics_universe_profile(&req).unwrap(),
            MainBusinessDiagnosticsUniverse::ListedNonSt
        );
        assert!(main_business_diagnostics_persist_default(None));
        assert!(!main_business_diagnostics_persist_default(Some(false)));

        let thresholds = main_business_diagnostics_thresholds(&req);
        assert_eq!(thresholds.min_day_coverage_ratio, 1.0);
        assert_eq!(thresholds.min_daily_rows, 1);
        assert_eq!(thresholds.min_p95_daily_row_ratio, 0.05);

        let options = main_business_research_diagnostics_options(&req);
        assert_eq!(options.return_horizons, vec![20, 45, 60, 120]);
        assert_eq!(options.bucket_count, 10);
        assert_eq!(options.max_rank_ic_days, 260);
        assert!(options.include_research_metrics);
        assert!(options.include_exposure_regime_metrics);
    }

    #[test]
    fn main_business_diagnostics_profiles_are_pre_registered_not_free_form() {
        let req = MainBusinessDiagnosticsRequest {
            start_date: "20140101".to_string(),
            end_date: "20260531".to_string(),
            business_type: Some(" P ".to_string()),
            universe_profile: Some(" listed_non_st ".to_string()),
            profiles: Some(vec![
                "sales_yoy".to_string(),
                "profit_yoy".to_string(),
                "gross_margin_delta_yoy".to_string(),
                "segment_concentration_inverse".to_string(),
                "sales_yoy".to_string(),
            ]),
            min_day_coverage_ratio: None,
            min_daily_rows: None,
            min_p95_daily_row_ratio: None,
            min_daily_coverage_ratio: None,
            persist_report: Some(false),
            return_horizons: Some(vec![60, 20, 0, 500, 20]),
            bucket_count: Some(2),
            max_rank_ic_days: Some(10_000),
            include_exposure_regime_metrics: Some(false),
            max_exposure_regime_days: Some(10_000),
        };

        let profiles = main_business_diagnostics_profiles(&req).unwrap();
        assert_eq!(
            profiles,
            vec![
                MainBusinessDiagnosticsProfile::SalesYoy,
                MainBusinessDiagnosticsProfile::ProfitYoy,
                MainBusinessDiagnosticsProfile::GrossMarginDeltaYoy,
                MainBusinessDiagnosticsProfile::SegmentConcentrationInverse,
            ]
        );

        let options = main_business_research_diagnostics_options(&req);
        assert_eq!(options.return_horizons, vec![20, 60, 252]);
        assert_eq!(options.bucket_count, 3);
        assert_eq!(options.max_rank_ic_days, 520);
        assert!(!options.include_exposure_regime_metrics);

        let mut invalid = req;
        invalid.profiles = Some(vec!["free_form_factor".to_string()]);
        let err = main_business_diagnostics_profiles(&invalid).unwrap_err();
        assert!(err.contains("pre-registered"));
        assert!(err.contains("free_form_factor"));
    }

    /// PoC:验证 LayeredSearchConfig 的 序列化→反序列化 round-trip 无精度损失。
    /// 选 professional_risk_breakthrough(有继承链:local_professional → breakthrough →
    /// risk_breakthrough)作为最复杂样本。若此 profile round-trip 等价,则全部 162 个
    /// 构造器(同结构同类型)可安全外置为 JSON。
    ///
    /// Step 4g 外置方案的前提验证。运行:
    ///   cargo test -p quant-api --lib phase7_search_config_roundtrip
    #[test]
    fn phase7_search_config_roundtrip_preserves_complex_profile() {
        use quant_api::discovery::phase7::LayeredSearchConfig;

        let original = LayeredSearchConfig::professional_risk_breakthrough_default();

        // 序列化
        let json_str = serde_json::to_string(&original).expect("序列化成功");
        assert!(!json_str.is_empty(), "序列化结果非空");

        // 反序列化
        let restored: LayeredSearchConfig =
            serde_json::from_str(&json_str).expect("反序列化成功");

        // 逐字段比较(PartialEq 已 derive,直接 == 即可覆盖全部 40 个字段)
        assert_eq!(
            original, restored,
            "round-trip 后配置必须完全相等(含 Decimal 精度/Vec 顺序)"
        );
    }
}

