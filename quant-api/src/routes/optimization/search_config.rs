//! phase7 搜索配置：按 search profile 解析 LayeredSearchConfig（配置单出口）。
use quant_api::discovery::strategy_discovery::LayeredSearchConfig;

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
/// 三层降级（同 factor_backfill_route 模式）：
/// 1. DB 命中（aliases jsonb 包含 input，且 enabled=true）→ 返回 profile_name（规范名）
/// 2. DB 空/异常/未命中 → 返回 input 原值（fallback 到 phase7_search_config 硬编码 match）
///
/// 配置化的是"profile 名规范化"——DB 只决定别名→规范名映射，
/// 具体走哪个 `LayeredSearchConfig::*_default()` 仍由 phase7_search_config 的硬编码 match 决定。
/// 新增 profile 的别名/优先级/启停可 DB 驱动；新增 default 方法仍需写代码。
pub(crate) async fn resolve_search_profile_name(
    db: &sqlx::PgPool,
    search_profile: Option<&str>,
) -> String {
    let input = search_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("local_professional");
    match sqlx::query_scalar::<_, String>(
        "SELECT profile_name FROM search_profile_config
         WHERE enabled = true AND $1 = ANY(aliases::text[])
         ORDER BY priority ASC LIMIT 1",
    )
    .bind(input)
    .fetch_optional(db)
    .await
    {
        Ok(Some(canonical)) => canonical,
        Ok(None) => input.to_string(),
        Err(_) => input.to_string(),
    }
}
