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

pub(crate) fn default_professional_robustness_policy() -> Value {
    json!({
        "candidate_tier": "professional_observation",
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
        "min_positive_annual_return_window_ratio": 0.60,
        "min_walk_forward_median_sharpe": 0.30,
        "min_bootstrap_positive_return_probability": 0.70,
        "min_bootstrap_sharpe_p05": 0.0,
        "min_market_scenarios": 2,
        "bootstrap_trials": 512,
        "bootstrap_seed": 42
    })
}


pub(crate) fn default_professional_elite_robustness_policy() -> Value {
    json!({
        "candidate_tier": "professional_elite",
        "min_trade_count": 200,
        "min_annual_return": 0.15,
        "min_excess_return": 0.0,
        "min_sharpe": 1.5,
        "min_sortino": 1.8,
        "min_calmar": 2.0,
        "min_profit_factor": 1.5,
        "max_drawdown_duration_days": 126,
        "max_drawdown": 0.35,
        "min_score_gap": 0.0,
        "walk_forward_window_days": 756,
        "walk_forward_step_days": 63,
        "min_walk_forward_windows": 4,
        "min_positive_excess_window_ratio": 0.50,
        "min_positive_annual_return_window_ratio": 0.60,
        "min_walk_forward_median_sharpe": 0.80,
        "min_walk_forward_median_calmar": 1.2,
        "min_bootstrap_positive_return_probability": 0.80,
        "min_bootstrap_sharpe_p05": 0.0,
        "min_bootstrap_calmar_p05": 0.0,
        "max_bootstrap_drawdown_p95": 0.35,
        "min_market_scenarios": 2,
        "bootstrap_trials": 1000,
        "bootstrap_seed": 42
    })
}


pub(crate) fn default_oos_train_selection_gate_policy() -> Value {
    json!({
        "candidate_tier": "oos_train_selection",
        "enforce_trial_constraint_violations": false,
        "min_trade_count": 20,
        "min_annual_return": 0.0,
        "min_sharpe": 0.0,
        "max_drawdown": 0.50,
        "min_score_gap": -999.0,
        "walk_forward_window_days": 504,
        "walk_forward_step_days": 126,
        "min_walk_forward_windows": 1,
        "min_positive_excess_window_ratio": 0.0,
        "min_positive_annual_return_window_ratio": 0.50,
        "min_walk_forward_median_sharpe": 0.0,
        "min_bootstrap_positive_return_probability": 0.50,
        "min_bootstrap_sharpe_p05": -1.0,
        "min_market_scenarios": 1,
        "bootstrap_trials": 256,
        "bootstrap_seed": 42
    })
}


pub(crate) fn default_oos_train_selection_gate_policy_for_search_profile(
    search_profile: Option<&str>,
) -> Value {
    let base = default_oos_train_selection_gate_policy();
    match search_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
    {
        "professional_execution_rolling_carry_budget"
        | "execution_rolling_carry_budget"
        | "execution_roll_forward_budget"
        | "phase7_execution_rolling_carry_budget"
        | "phase7_dz"
        | "professional_execution_capacity_fill_frontier"
        | "execution_capacity_fill_frontier"
        | "capacity_fill_frontier"
        | "phase7_execution_capacity_fill_frontier"
        | "phase7_ea"
        | "professional_execution_alpha_capacity_bridge"
        | "execution_alpha_capacity_bridge"
        | "alpha_capacity_bridge"
        | "phase7_execution_alpha_capacity_bridge"
        | "phase7_eb"
        | "professional_execution_alpha_capacity_return_frontier"
        | "execution_alpha_capacity_return_frontier"
        | "alpha_capacity_return_frontier"
        | "phase7_execution_alpha_capacity_return_frontier"
        | "phase7_ec"
        | "professional_execution_stress_fill_return_frontier"
        | "execution_stress_fill_return_frontier"
        | "stress_fill_return_frontier"
        | "phase7_execution_stress_fill_return_frontier"
        | "phase7_ed"
        | "professional_execution_stress_risk_budget"
        | "execution_stress_risk_budget"
        | "stress_risk_budget"
        | "phase7_execution_stress_risk_budget"
        | "phase7_ee" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_capacity_stress_return_gate"
        | "execution_capacity_stress_return_gate"
        | "capacity_stress_return_gate"
        | "phase7_execution_capacity_stress_return_gate"
        | "phase7_ef"
        | "professional_execution_low_impact_alpha_stress_return"
        | "execution_low_impact_alpha_stress_return"
        | "low_impact_alpha_stress_return"
        | "phase7_execution_low_impact_alpha_stress_return"
        | "phase7_eg"
        | "professional_execution_stress_target_scaling_return"
        | "execution_stress_target_scaling_return"
        | "stress_target_scaling_return"
        | "phase7_execution_stress_target_scaling_return"
        | "phase7_eh"
        | "professional_execution_stress_floor_scaling_return"
        | "execution_stress_floor_scaling_return"
        | "stress_floor_scaling_return"
        | "phase7_execution_stress_floor_scaling_return"
        | "phase7_ei"
        | "professional_execution_stress_floor_return_recovery"
        | "execution_stress_floor_return_recovery"
        | "stress_floor_return_recovery"
        | "phase7_execution_stress_floor_return_recovery"
        | "phase7_ej"
        | "professional_execution_pressure_headroom_floor"
        | "execution_pressure_headroom_floor"
        | "pressure_headroom_floor"
        | "phase7_execution_pressure_headroom_floor"
        | "phase7_ek"
        | "professional_execution_alpha_headroom_floor"
        | "execution_alpha_headroom_floor"
        | "alpha_headroom_floor"
        | "phase7_execution_alpha_headroom_floor"
        | "phase7_el"
        | "professional_execution_blended_alpha_headroom_floor"
        | "execution_blended_alpha_headroom_floor"
        | "blended_alpha_headroom_floor"
        | "phase7_execution_blended_alpha_headroom_floor"
        | "phase7_em"
        | "professional_execution_event_anchor_stress_bridge"
        | "execution_event_anchor_stress_bridge"
        | "event_anchor_stress_bridge"
        | "phase7_execution_event_anchor_stress_bridge"
        | "professional_execution_participation_aware_event_anchor"
        | "execution_participation_aware_event_anchor"
        | "participation_aware_event_anchor"
        | "phase7_execution_participation_aware_event_anchor"
        | "phase7_eo"
        | "professional_execution_cl_anchor_fill_recovery"
        | "execution_cl_anchor_fill_recovery"
        | "cl_anchor_fill_recovery"
        | "phase7_execution_cl_anchor_fill_recovery"
        | "phase7_ep"
        | "professional_execution_capacity_aware_candidate_ranking"
        | "execution_capacity_aware_candidate_ranking"
        | "capacity_aware_candidate_ranking"
        | "phase7_execution_capacity_aware_candidate_ranking"
        | "phase7_eq"
        | "professional_execution_pit_capacity_ranking"
        | "execution_pit_capacity_ranking"
        | "pit_capacity_ranking"
        | "phase7_execution_pit_capacity_ranking"
        | "phase7_er"
        | "professional_execution_pit_alpha_first_low_impact"
        | "execution_pit_alpha_first_low_impact"
        | "pit_alpha_first_low_impact"
        | "phase7_execution_pit_alpha_first_low_impact"
        | "phase7_es"
        | "professional_execution_pit_excess_return_recovery"
        | "execution_pit_excess_return_recovery"
        | "pit_excess_return_recovery"
        | "phase7_execution_pit_excess_return_recovery"
        | "phase7_et"
        | "professional_execution_bull_sleeve_cash_recovery"
        | "execution_bull_sleeve_cash_recovery"
        | "bull_sleeve_cash_recovery"
        | "phase7_execution_bull_sleeve_cash_recovery"
        | "phase7_eu"
        | "professional_execution_return_first_fill_repair"
        | "execution_return_first_fill_repair"
        | "return_first_fill_repair"
        | "phase7_execution_return_first_fill_repair"
        | "phase7_ey"
        | "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        | "execution_pit_nonlinear_alpha_regime_rebuild"
        | "pit_nonlinear_alpha_regime_rebuild"
        | "phase7_execution_pit_nonlinear_alpha_regime_rebuild"
        | "phase7_ez"
        | "professional_execution_pit_quality_recovery_alpha"
        | "execution_pit_quality_recovery_alpha"
        | "pit_quality_recovery_alpha"
        | "phase7_execution_pit_quality_recovery_alpha"
        | "phase7_fa"
        | "professional_execution_event_post_return_curve_alpha"
        | "execution_event_post_return_curve_alpha"
        | "event_post_return_curve_alpha"
        | "phase7_execution_event_post_return_curve_alpha"
        | "phase7_fb"
        | "professional_execution_event_reaction_alpha"
        | "execution_event_reaction_alpha"
        | "event_reaction_alpha"
        | "phase7_execution_event_reaction_alpha"
        | "phase7_fc"
        | "professional_execution_broad_financial_feature_discovery"
        | "execution_broad_financial_feature_discovery"
        | "broad_financial_feature_discovery"
        | "phase7_execution_broad_financial_feature_discovery"
        | "phase7_fg"
        | "professional_execution_broad_financial_feature_stratified_discovery"
        | "execution_broad_financial_feature_stratified_discovery"
        | "broad_financial_feature_stratified_discovery"
        | "phase7_execution_broad_financial_feature_stratified_discovery"
        | "phase7_fh"
        | "professional_execution_native_alpha_fusion_discovery"
        | "execution_native_alpha_fusion_discovery"
        | "native_alpha_fusion_discovery"
        | "phase7_execution_native_alpha_fusion_discovery"
        | "phase7_fj"
        | "professional_trainable_alpha_admission_discovery"
        | "trainable_alpha_admission_discovery"
        | "phase7_trainable_alpha_admission"
        | "phase7_ft"
        | "professional_v19_multi_alpha_sleeve_admission"
        | "v19_multi_alpha_sleeve_admission"
        | "multi_alpha_sleeve_admission"
        | "phase7_v19_sleeves"
        | "professional_v19_event_surprise_sleeve_gate_admission"
        | "v19_event_surprise_sleeve_gate"
        | "phase7_v19_event_surprise_sleeve_gate"
        | "phase7_p311_event_surprise_sleeve_gate"
        | "professional_v19_supply_float_sleeve_admission"
        | "v19_supply_float_sleeve"
        | "phase7_v19_supply_float_sleeve"
        | "phase7_p312_supply_float_sleeve"
        | "professional_v19_unlock_pressure_sleeve_admission"
        | "v19_unlock_pressure_sleeve"
        | "phase7_v19_unlock_pressure_sleeve"
        | "phase7_p314_unlock_pressure_sleeve"
        | "professional_v19_forecast_revision_sleeve_admission"
        | "v19_forecast_revision_sleeve"
        | "phase7_v19_forecast_revision_sleeve"
        | "phase7_p313_forecast_revision_sleeve"
        | "professional_v19_shareholder_structure_sleeve_admission"
        | "v19_shareholder_structure_sleeve"
        | "phase7_v19_shareholder_structure_sleeve"
        | "phase7_p321e_shareholder_structure_sleeve"
        | "professional_v19_event_post_return_overlay_admission"
        | "v19_event_post_return_overlay_admission"
        | "v19_event_post_return_overlay"
        | "phase7_v19_event_post_return_overlay"
        | "professional_v19_execution_repair_admission"
        | "v19_execution_repair_admission"
        | "v19_execution_repair"
        | "phase7_v19_execution_repair"
        | "phase7_v19_exec_repair"
        | "professional_v19_train_window_ml_alpha_rebuild"
        | "v19_train_window_ml_alpha_rebuild"
        | "v19_ml_alpha_rebuild"
        | "phase7_v19_train_window_ml_alpha_rebuild"
        | "phase7_v19_ml_alpha_rebuild"
        | "professional_v19_train_window_ml_simple_excess_rebuild"
        | "v19_train_window_ml_simple_excess_rebuild"
        | "v19_ml_simple_excess_rebuild"
        | "phase7_v19_train_window_ml_simple_excess"
        | "phase7_v19_ml_simple_excess"
        | "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
        | "v19_train_window_ml_simple_excess_low_impact_rebuild"
        | "v19_ml_simple_excess_low_impact_rebuild"
        | "phase7_v19_train_window_ml_simple_excess_low_impact"
        | "phase7_v19_ml_simple_excess_low_impact"
        | "professional_v19_train_window_ml_h120_low_impact_rebuild"
        | "v19_train_window_ml_h120_low_impact_rebuild"
        | "v19_ml_h120_low_impact_rebuild"
        | "phase7_v19_train_window_ml_h120_low_impact"
        | "phase7_v19_ml_h120_low_impact"
        | "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        | "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
        | "v19_ml_rae_h120_residual_capacity_rebuild"
        | "phase7_v19_train_window_ml_rae_h120_residual_capacity"
        | "phase7_v19_ml_rae_h120_residual_capacity"
        | "professional_v19_train_window_ml_event_sentiment_rebuild"
        | "v19_train_window_ml_event_sentiment_rebuild"
        | "v19_ml_event_sentiment_rebuild"
        | "phase7_v19_train_window_ml_event_sentiment"
        | "phase7_v19_ml_event_sentiment"
        | "professional_prediction_capacity_dual_objective"
        | "prediction_capacity_dual_objective"
        | "phase7_prediction_capacity_dual_objective"
        | "phase7_fl"
        | "professional_prediction_target_gross_signal_fidelity"
        | "prediction_target_gross_signal_fidelity"
        | "phase7_prediction_target_gross_signal_fidelity"
        | "phase7_fm"
        | "professional_prediction_confidence_turnover_discovery"
        | "prediction_confidence_turnover_discovery"
        | "phase7_prediction_confidence_turnover_discovery"
        | "phase7_fn"
        | "professional_prediction_confidence_alpha_lift"
        | "prediction_confidence_alpha_lift"
        | "phase7_prediction_confidence_alpha_lift"
        | "phase7_fo"
        | "professional_prediction_long_horizon_low_turnover"
        | "prediction_long_horizon_low_turnover"
        | "phase7_prediction_long_horizon_low_turnover"
        | "phase7_fp"
        | "professional_prediction_long_horizon_regime_alpha"
        | "prediction_long_horizon_regime_alpha"
        | "phase7_prediction_long_horizon_regime_alpha"
        | "phase7_fr"
        | "professional_prediction_h60_nonlinear_stress_discovery"
        | "prediction_h60_nonlinear_stress_discovery"
        | "h60_nonlinear_stress_discovery"
        | "phase7_prediction_h60_nonlinear_stress_discovery"
        | "phase7_fw"
        | "professional_prediction_h120_low_impact_stress_discovery"
        | "prediction_h120_low_impact_stress_discovery"
        | "h120_low_impact_stress_discovery"
        | "phase7_prediction_h120_low_impact_stress_discovery"
        | "phase7_fz"
        | "professional_train_window_nonlinear_ranking_discovery"
        | "train_window_nonlinear_ranking_discovery"
        | "phase7_train_window_nonlinear_ranking_discovery"
        | "phase7_fx"
        | "professional_train_window_stress_fill_target_exposure"
        | "train_window_stress_fill_target_exposure"
        | "stress_fill_target_exposure"
        | "phase7_train_window_stress_fill_target_exposure"
        | "phase7_ga" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "capacity_stress_return_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 50,
                "min_train_final_actual_gross_exposure_pct": 0.25,
                "max_train_final_cash_weight_pct": 0.75,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_train_window_ml_stress_fill_discovery"
        | "train_window_ml_stress_fill_discovery"
        | "ml_stress_fill_discovery"
        | "phase7_train_window_ml_stress_fill_discovery"
        | "phase7_gb" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 100,
                "min_train_final_actual_gross_exposure_pct": 0.35,
                "max_train_final_cash_weight_pct": 0.65,
                "max_train_final_unfilled_target_gap_pct": 0.06,
                "min_train_final_execution_fill_ratio": 0.95,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_ensemble_discovery"
        | "phase7_ensemble_v1"
        | "professional_current_event_nonlinear_alpha_discovery"
        | "current_event_nonlinear_alpha_discovery"
        | "phase7_current_event_nonlinear_alpha_discovery"
        | "phase7_fs"
        | "professional_execution_oos_regime_alpha_rebuild"
        | "execution_oos_regime_alpha_rebuild"
        | "oos_regime_alpha_rebuild"
        | "phase7_execution_oos_regime_alpha_rebuild"
        | "phase7_ev"
        | "professional_execution_oos_benchmark_excess_rebuild"
        | "execution_oos_benchmark_excess_rebuild"
        | "oos_benchmark_excess_rebuild"
        | "phase7_execution_oos_benchmark_excess_rebuild"
        | "phase7_ew"
        | "professional_execution_oos_execution_adaptive_rebuild"
        | "execution_oos_execution_adaptive_rebuild"
        | "oos_execution_adaptive_rebuild"
        | "phase7_execution_oos_execution_adaptive_rebuild"
        | "phase7_ex" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "capacity_stress_return_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 50,
                "min_train_final_actual_gross_exposure_pct": 0.25,
                "max_train_final_cash_weight_pct": 0.75,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_fill_ratio_budget"
        | "execution_fill_ratio_budget"
        | "execution_unfilled_gap_budget"
        | "phase7_execution_fill_ratio_budget"
        | "phase7_dy" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_execution_target_gap_pct": 0.08,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_cash_drag_aware_budget"
        | "execution_cash_drag_aware_budget"
        | "cash_drag_aware_execution_budget"
        | "phase7_execution_cash_drag_aware_budget"
        | "phase7_dv"
        | "professional_execution_feasible_fill_budget"
        | "execution_feasible_fill_budget"
        | "cash_utilization_execution_budget"
        | "phase7_execution_feasible_fill_budget"
        | "phase7_dx" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_cash_weight_pct": 0.20,
                "max_train_execution_target_gap_pct": 0.08,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        _ => base,
    }
}


pub(crate) fn default_oos_final_promotion_gate_policy(validation_mode: &str) -> Value {
    let min_oos_window_count = if validation_mode == "holdout_80_20" {
        1
    } else {
        3
    };
    json!({
        "candidate_tier": "oos_final_promotion",
        "min_stitched_oos_calmar": 1.2,
        "min_stitched_annual_return": 0.15,
        "min_stitched_excess_return": 0.0,
        "min_positive_oos_window_ratio": 0.60,
        "min_oos_window_count": min_oos_window_count,
        "require_train_selection_approval": true,
        "require_no_train_test_overlap": true,
        "min_cost_capacity_perturbation_pass_ratio": 0.80,
        "min_perturbed_oos_calmar": 1.2,
        "max_perturbed_oos_drawdown_pct": 0.35
    })
}


pub(crate) fn profile_accepts_prediction_set_override(search_profile: &str) -> bool {
    !matches!(
        search_profile,
        "professional_train_window_nonlinear_ranking_discovery"
            | "professional_train_window_stress_fill_target_exposure"
            | "professional_train_window_ml_stress_fill_discovery"
            | "professional_v19_multi_alpha_sleeve_admission"
            | "professional_v19_event_surprise_sleeve_gate_admission"
            | "professional_v19_supply_float_sleeve_admission"
            | "professional_v19_unlock_pressure_sleeve_admission"
            | "professional_v19_forecast_revision_sleeve_admission"
            | "professional_v19_shareholder_structure_sleeve_admission"
            | "professional_v19_execution_repair_admission"
            | "professional_v19_train_window_ml_alpha_rebuild"
            | "professional_v19_train_window_ml_simple_excess_rebuild"
            | "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "professional_v19_train_window_ml_h120_low_impact_rebuild"
            | "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "professional_v19_train_window_ml_event_sentiment_rebuild"
            | "professional_v19_current_baseline"
    )
}


pub(crate) fn is_train_window_ml_stress_fill_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_train_window_ml_stress_fill_discovery"
            | "train_window_ml_stress_fill_discovery"
            | "ml_stress_fill_discovery"
            | "phase7_train_window_ml_stress_fill_discovery"
            | "phase7_gb"
            | "professional_ensemble_discovery"
            | "phase7_ensemble_v1"
            | "professional_simple_nlqr_discovery"
            | "phase7_simple_nlqr"
            | "phase7_s1"
            | "professional_v19_train_window_ml_alpha_rebuild"
            | "v19_train_window_ml_alpha_rebuild"
            | "v19_ml_alpha_rebuild"
            | "phase7_v19_train_window_ml_alpha_rebuild"
            | "phase7_v19_ml_alpha_rebuild"
            | "professional_v19_train_window_ml_simple_excess_rebuild"
            | "v19_train_window_ml_simple_excess_rebuild"
            | "v19_ml_simple_excess_rebuild"
            | "phase7_v19_train_window_ml_simple_excess"
            | "phase7_v19_ml_simple_excess"
            | "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_ml_simple_excess_low_impact_rebuild"
            | "phase7_v19_train_window_ml_simple_excess_low_impact"
            | "phase7_v19_ml_simple_excess_low_impact"
            | "professional_v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_ml_h120_low_impact_rebuild"
            | "phase7_v19_train_window_ml_h120_low_impact"
            | "phase7_v19_ml_h120_low_impact"
            | "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_ml_rae_h120_residual_capacity_rebuild"
            | "phase7_v19_train_window_ml_rae_h120_residual_capacity"
            | "phase7_v19_ml_rae_h120_residual_capacity"
            | "professional_v19_train_window_ml_event_sentiment_rebuild"
            | "v19_train_window_ml_event_sentiment_rebuild"
            | "v19_ml_event_sentiment_rebuild"
            | "phase7_v19_train_window_ml_event_sentiment"
            | "phase7_v19_ml_event_sentiment"
    )
}


pub(crate) fn is_ensemble_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_ensemble_discovery" | "phase7_ensemble_v1"
    )
}


pub(crate) fn is_simple_nlqr_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_simple_nlqr_discovery" | "phase7_simple_nlqr" | "phase7_s1"
    )
}


pub(crate) fn is_v19_train_window_ml_alpha_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_v19_train_window_ml_alpha_rebuild"
            | "v19_train_window_ml_alpha_rebuild"
            | "v19_ml_alpha_rebuild"
            | "phase7_v19_train_window_ml_alpha_rebuild"
            | "phase7_v19_ml_alpha_rebuild"
            | "professional_v19_train_window_ml_simple_excess_rebuild"
            | "v19_train_window_ml_simple_excess_rebuild"
            | "v19_ml_simple_excess_rebuild"
            | "phase7_v19_train_window_ml_simple_excess"
            | "phase7_v19_ml_simple_excess"
            | "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_ml_simple_excess_low_impact_rebuild"
            | "phase7_v19_train_window_ml_simple_excess_low_impact"
            | "phase7_v19_ml_simple_excess_low_impact"
            | "professional_v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_ml_h120_low_impact_rebuild"
            | "phase7_v19_train_window_ml_h120_low_impact"
            | "phase7_v19_ml_h120_low_impact"
            | "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_ml_rae_h120_residual_capacity_rebuild"
            | "phase7_v19_train_window_ml_rae_h120_residual_capacity"
            | "phase7_v19_ml_rae_h120_residual_capacity"
            | "professional_v19_train_window_ml_event_sentiment_rebuild"
            | "v19_train_window_ml_event_sentiment_rebuild"
            | "v19_ml_event_sentiment_rebuild"
            | "phase7_v19_train_window_ml_event_sentiment"
            | "phase7_v19_ml_event_sentiment"
    )
}


pub(crate) fn is_v19_train_window_ml_event_sentiment_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_v19_train_window_ml_event_sentiment_rebuild"
            | "v19_train_window_ml_event_sentiment_rebuild"
            | "v19_ml_event_sentiment_rebuild"
            | "phase7_v19_train_window_ml_event_sentiment"
            | "phase7_v19_ml_event_sentiment"
    )
}


pub(crate) fn is_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(
    search_profile: Option<&str>,
) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
            | "v19_ml_rae_h120_residual_capacity_rebuild"
            | "phase7_v19_train_window_ml_rae_h120_residual_capacity"
            | "phase7_v19_ml_rae_h120_residual_capacity"
    )
}


pub(crate) fn is_v19_train_window_ml_h120_low_impact_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_train_window_ml_h120_low_impact_rebuild"
            | "v19_ml_h120_low_impact_rebuild"
            | "phase7_v19_train_window_ml_h120_low_impact"
            | "phase7_v19_ml_h120_low_impact"
    )
}


pub(crate) fn is_v19_train_window_ml_simple_excess_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_v19_train_window_ml_simple_excess_rebuild"
            | "v19_train_window_ml_simple_excess_rebuild"
            | "v19_ml_simple_excess_rebuild"
            | "phase7_v19_train_window_ml_simple_excess"
            | "phase7_v19_ml_simple_excess"
            | "professional_v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_train_window_ml_simple_excess_low_impact_rebuild"
            | "v19_ml_simple_excess_low_impact_rebuild"
            | "phase7_v19_train_window_ml_simple_excess_low_impact"
            | "phase7_v19_ml_simple_excess_low_impact"
    )
}


pub(crate) fn apply_internal_train_window_ml_prediction_set_to_seed_trials(
    seed_trials: &mut [Value],
    prediction_set_id: &str,
) {
    for seed in seed_trials {
        if seed.get("train_window_ml_ranking_profile").is_none() {
            continue;
        }
        let prediction_min_score = seed
            .get("train_window_ml_prediction_min_score")
            .and_then(Value::as_str)
            .unwrap_or("0.00")
            .to_string();
        seed["prediction_set_id"] = json!(prediction_set_id);
        seed["prediction_blend_weight"] = json!("0.20");
        seed["prediction_min_percentile"] = json!("0.30");
        seed["prediction_min_score"] = json!(prediction_min_score);
        seed["prediction_set_override"] = json!(true);
        seed["prediction_set_override_source"] = json!("train_window_ml_internal");
        seed["train_window_ml_prediction_set_scope"] = json!("generated_per_wfa_window_train_only");
    }
}


pub(crate) fn train_window_ml_oos_parameters(
    train_parameters: &Value,
    prediction_sets: Option<&TrainWindowMlPredictionSets>,
) -> Result<Value, String> {
    let Some(prediction_sets) = prediction_sets else {
        return Ok(train_parameters.clone());
    };
    let mut parameters = train_parameters.clone();
    let Some(object) = parameters.as_object_mut() else {
        return Err("train-window ML candidate parameters must be a JSON object".into());
    };
    object.insert(
        "prediction_set_id".to_string(),
        json!(prediction_sets.test_prediction_set_id),
    );
    object.insert("prediction_set_override".to_string(), json!(true));
    object.insert(
        "prediction_set_override_source".to_string(),
        json!("train_window_ml_internal_oos"),
    );
    object.insert(
        "train_window_ml_train_prediction_set_id".to_string(),
        json!(prediction_sets.train_prediction_set_id),
    );
    object.insert(
        "train_window_ml_training_task_id".to_string(),
        json!(prediction_sets.training_task_id),
    );
    Ok(parameters)
}


pub(crate) fn phase7_discovery_layered_request(
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
                "min_sharpe": 1.0,
                "min_sortino": 1.5,
                "max_drawdown": 0.35,
                "min_trade_count": 1
            }))
        }),
        walk_forward: req.walk_forward.clone(),
        backtest_template: Some(
            req.backtest_template
                .clone()
                .unwrap_or_else(default_phase7_backtest_template),
        ),
        prediction_set_ids: req.prediction_set_ids.clone(),
        max_trials: req.max_trials,
        search_profile: Some(
            req.search_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("professional_sharpe_stabilization")
                .to_string(),
        ),
    }
}


pub(crate) fn default_phase7_backtest_template() -> Value {
    json!({
        "mode": "standard",
        "persistence_mode": "summary_only",
        "benchmark": "000300.SH",
        "start_date": "20160201",
        "end_date": "20260515",
        "initial_capital": 1000000.0,
        "signal_timing": "close",
        "execution_timing": "next_open",
        "execution_price": "next_open",
        "effective_coverage": {
            "enabled": true,
            "mode": "adjust_start"
        }
    })
}


pub(crate) fn normalize_prediction_set_ids(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}


pub(crate) fn apply_prediction_set_override_to_seed_trials(
    seed_trials: &mut [Value],
    prediction_set_ids: &[String],
) {
    let Some(prediction_set_id) = prediction_set_ids.first() else {
        return;
    };
    for seed in seed_trials {
        if seed.get("prediction_set_id").is_some() {
            seed["prediction_set_id"] = json!(prediction_set_id);
            seed["prediction_set_override"] = json!(true);
            seed["prediction_set_override_source"] = json!("request");
        }
    }
}


pub(crate) fn default_effective_coverage_policy(top_n: usize) -> EffectiveCoverageReq {
    EffectiveCoverageReq {
        enabled: Some(true),
        mode: Some("adjust_start".to_string()),
        min_rows: Some(top_n.max(1)),
        include_rebalance_warmup: Some(true),
        warmup_trading_days: Some(19),
    }
}


pub(crate) fn effective_coverage_policy_from_value(
    value: Option<&Value>,
    top_n: usize,
) -> Result<Option<EffectiveCoverageReq>, String> {
    let Some(value) = value else {
        return Ok(Some(default_effective_coverage_policy(top_n)));
    };
    match value {
        Value::Null => Ok(None),
        Value::Bool(false) => Ok(Some(EffectiveCoverageReq {
            enabled: Some(false),
            mode: None,
            min_rows: None,
            include_rebalance_warmup: None,
            warmup_trading_days: None,
        })),
        Value::Bool(true) => Ok(Some(default_effective_coverage_policy(top_n))),
        Value::String(mode) => {
            let mode = mode.trim();
            if mode.is_empty() || mode == "adjust_start" {
                Ok(Some(default_effective_coverage_policy(top_n)))
            } else if mode == "guard_only" {
                Ok(Some(EffectiveCoverageReq {
                    enabled: Some(true),
                    mode: Some("guard_only".to_string()),
                    min_rows: Some(top_n.max(1)),
                    include_rebalance_warmup: Some(true),
                    warmup_trading_days: Some(19),
                }))
            } else if mode == "off" || mode == "disabled" {
                Ok(None)
            } else {
                Err(format!("unsupported effective_coverage policy: {}", mode))
            }
        }
        Value::Object(_) => {
            let mut policy: EffectiveCoverageReq = serde_json::from_value(value.clone())
                .map_err(|error| format!("effective_coverage must be an object: {}", error))?;
            if policy.enabled != Some(false) {
                if policy.enabled.is_none() {
                    policy.enabled = Some(true);
                }
                if policy.mode.is_none() {
                    policy.mode = Some("adjust_start".to_string());
                }
                if policy.min_rows.is_none() {
                    policy.min_rows = Some(top_n.max(1));
                }
                if policy.include_rebalance_warmup.is_none() {
                    policy.include_rebalance_warmup = Some(true);
                }
                if policy.include_rebalance_warmup == Some(true)
                    && policy.warmup_trading_days.is_none()
                {
                    policy.warmup_trading_days = Some(19);
                }
            }
            Ok(Some(policy))
        }
        _ => Err("effective_coverage must be null, boolean, string, or object".to_string()),
    }
}


pub(crate) fn train_window_ml_label_horizon_days(window: &OosDiscoveryWindow) -> i64 {
    let train_days = (window.train_end - window.train_start).num_days().max(1);
    45.min((train_days / 4).max(5)).max(5)
}


pub(crate) fn train_window_ml_lookback_days(window: &OosDiscoveryWindow, label_horizon_days: i64) -> i64 {
    let train_days = (window.train_end - window.train_start).num_days().max(1);
    let max_lookback = (train_days - label_horizon_days - 5).max(30);
    252.min(max_lookback).max(30)
}


pub(crate) fn phase7_train_window_ml_feature_profile() -> &'static str {
    "phase7_gb_quality_value_recovery_low_impact_v5"
}


pub(crate) fn phase7_simple_nlqr_feature_profile() -> &'static str {
    "phase7_simple_nlqr_core_15f_v1"
}


pub(crate) fn phase7_train_window_ml_label_config_for_search(
    search_profile: Option<&str>,
) -> (String, usize, usize, usize) {
    if is_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(search_profile) {
        ("risk_adjusted_excess_return".to_string(), 120, 7, 50)
    } else if is_v19_train_window_ml_event_sentiment_rebuild_profile(search_profile) {
        ("risk_adjusted_excess_return".to_string(), 120, 7, 50)
    } else if is_v19_train_window_ml_h120_low_impact_rebuild_profile(search_profile) {
        ("future_excess_return".to_string(), 120, 5, 50)
    } else if is_v19_train_window_ml_simple_excess_rebuild_profile(search_profile) {
        ("future_excess_return".to_string(), 45, 5, 50)
    } else if is_v19_train_window_ml_alpha_rebuild_profile(search_profile) {
        (
            "quality_adjusted_risk_adjusted_excess_return".to_string(),
            60,
            10,
            100,
        )
    } else {
        ("risk_adjusted_excess_return".to_string(), 45, 7, 250)
    }
}


pub(crate) fn phase7_train_window_ml_feature_profile_for_search(search_profile: Option<&str>) -> &'static str {
    if is_simple_nlqr_profile(search_profile) {
        phase7_simple_nlqr_feature_profile()
    } else if is_v19_train_window_ml_event_sentiment_rebuild_profile(search_profile) {
        "phase7_p4_event_sentiment_high_coverage_v1"
    } else if is_v19_train_window_ml_alpha_rebuild_profile(search_profile) {
        "phase7_gb_quality_value_recovery_low_impact_v6"
    } else {
        phase7_train_window_ml_feature_profile()
    }
}


pub(crate) fn phase7_train_window_ml_factor_refs_for_profile(profile: &str) -> Vec<LinearFactorRef> {
    let factor_codes: &[&str] = match profile {
        "phase7_gb_quality_value_recovery_low_impact_v2" => &[
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
        ],
        "phase7_gb_quality_value_recovery_low_impact_v3" => &[
            // Financial quality (v2 base + industry-relative)
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            // Financial improvement / earnings recovery
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            // Valuation
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            // Cashflow quality
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            // Dividend quality
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            // Multi-horizon momentum (5d/20d/60d)
            "mom_5d_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            // Multi-horizon reversal (5d/20d)
            "rev_5d_std",
            "rev_20d_std",
            // Liquidity & capacity
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            // Volatility & risk
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            // Money flow
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
            // Event alpha (sparse but high-signal)
            "event_express_roe_std",
            "event_disclosure_early_days_std",
            "event_forecast_surprise_bucket_std",
            "event_post_return_express_20d_indrel_std",
            "event_reaction_express_1_5d_indrel_std",
        ],
        "phase7_gb_quality_value_recovery_low_impact_v4" => &[
            // v3 base + north_flow sentiment
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            "mom_5d_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_5d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
            "event_express_roe_std",
            "event_disclosure_early_days_std",
            "event_forecast_surprise_bucket_std",
            "event_post_return_express_20d_indrel_std",
            "event_reaction_express_1_5d_indrel_std",
            // P1 新因子：北向资金情绪
            "north_flow_std_20d",
        ],
        "phase7_simple_nlqr_core_15f_v1" => &[
            // 15 core factors — simplified for WFA robustness
            // Quality (4): fundamental profitability
            "fin_roe_daily_std",
            "fin_roa_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_gross_margin_daily_std",
            // Value (3): cheapness
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_dividend_yield_ttm_std",
            // Cashflow (2): earnings quality
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            // Momentum (3): price trend
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            // Low risk (2): drawdown protection
            "vol_20d_std",
            "maxdd_60d_std",
            // Sentiment (1): northbound flow
            "north_flow_std_20d",
        ],
        "phase7_gb_quality_value_recovery_low_impact_v5" => &[
            // v4 minus sparse event alpha (51 factors, high intersection)
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            "mom_5d_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_5d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
            "north_flow_std_20d",
        ],
        "phase7_gb_quality_value_recovery_low_impact_v6" => &[
            // v5 + P2 margin sentiment factor (52 factors)
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            "mom_5d_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_5d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
            "north_flow_std_20d",
            "margin_rz_std_20d",
        ],
        "phase7_p4_event_sentiment_high_coverage_v1" => &[
            // High-coverage PIT event fundamentals + margin sentiment.
            // Excludes sparse post-event return curve factors to preserve train sample size.
            "event_disclosure_early_days_std",
            "event_disclosure_timing_bucket_std",
            "event_forecast_profit_floor_sign_std",
            "event_forecast_surprise_bucket_std",
            "event_forecast_change_mid_std",
            "event_forecast_profit_floor_std",
            "event_express_roe_bucket_std",
            "event_express_roe_std",
            "margin_rz_change_std_20d",
            "margin_rz_std_20d",
            "north_flow_std_20d",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_small_sell_pressure_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "vol_20d_std",
        ],
        _ => &[
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
        ],
    };

    factor_codes
        .iter()
        .map(|factor_code| LinearFactorRef {
            factor_code: (*factor_code).to_string(),
            factor_version: "1.0.0".to_string(),
        })
        .collect()
}


pub(crate) fn default_oos_cost_capacity_perturbations() -> Vec<OosCostCapacityPerturbationRequest> {
    vec![
        OosCostCapacityPerturbationRequest {
            name: Some("cost_up_150pct".to_string()),
            cost_multiplier: Some(1.5),
            slippage_bps: Some(0.0002),
            impact_cost_coefficient: None,
            max_participation_rate: None,
            capacity_penalty_strength: None,
        },
        OosCostCapacityPerturbationRequest {
            name: Some("impact_cost_2pct_participation_10pct".to_string()),
            cost_multiplier: Some(1.0),
            slippage_bps: None,
            impact_cost_coefficient: Some(0.02),
            max_participation_rate: Some(0.10),
            capacity_penalty_strength: None,
        },
        OosCostCapacityPerturbationRequest {
            name: Some("capacity_tight_participation_5pct".to_string()),
            cost_multiplier: None,
            slippage_bps: None,
            impact_cost_coefficient: None,
            max_participation_rate: Some(0.05),
            capacity_penalty_strength: Some(1.0),
        },
    ]
}


