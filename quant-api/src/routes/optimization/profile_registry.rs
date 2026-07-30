//! Auto-extracted from routes/optimization.rs (DDD Step 3b).
//! Do not edit by hand; logic is verbatim from the original module.
#![allow(unused_imports)]
#![allow(dead_code)]

// ============================================================
// search_profile 单一真相源：category 标签驱动
// （别名列表只在 PROFILE_CATALOG 中定义一次，is_* 谓词、
// profile_accepts_prediction_set_override、gate policy 都从
// category 派生，消除别名在多处手动重复维护导致的遗漏 bug，如 phase7_en）
// ============================================================

/// Profile 的 is_* 维度类别。is_* 谓词和 accepts_override 从此派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchProfileCategory {
    /// train_window ML 压力填充发现型（is_train_window_ml_stress_fill_profile 的根类别）
    TrainWindowMlStressFill,
    /// Ensemble 发现型（is_ensemble_profile）
    Ensemble,
    /// Simple NLQR 发现型（is_simple_nlqr_profile）
    SimpleNlqr,
    /// V19 TrainWindow ML Alpha 重建型
    V19TrainWindowMlAlphaRebuild,
    /// V19 TrainWindow ML Simple Excess 重建型
    V19TrainWindowMlSimpleExcess,
    /// V19 TrainWindow ML Simple Excess Low Impact 重建型
    V19TrainWindowMlSimpleExcessLowImpact,
    /// V19 TrainWindow ML H120 Low Impact 重建型
    V19TrainWindowMlH120LowImpact,
    /// V19 TrainWindow ML RAE H120 残差容量重建型
    V19TrainWindowMlRaeH120Residual,
    /// V19 TrainWindow ML 事件情绪重建型
    V19TrainWindowMlEventSentiment,
    /// V19 Sleeve 准入型（accepts_override=false）
    V19SleeveAdmission,
    /// V19 执行修复型（accepts_override=false）
    V19ExecutionRepair,
    /// V19 Current Baseline（accepts_override=false）
    V19CurrentBaseline,
    /// TrainWindow 非线性排序发现型（accepts_override=false）
    TrainWindowNonlinearRanking,
    /// TrainWindow 压力填充目标敞口型（accepts_override=false）
    TrainWindowStressFillTargetExposure,
    /// 默认/非执行型（accepts_override=true，非上述任何类别）
    Default,
}

/// gate policy 维度类别（default_oos_train_selection_gate_policy_for_search_profile 派生）。
/// 与 SearchProfileCategory 正交：同一 profile 在 is_* 维度和 gate 维度可能不同。
/// Base 表示走默认 gate policy（无 override）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateCategory {
    /// 基础 cash_drag 填充（原 gate 组1，9 字段）
    CashDragBasic,
    /// capacity_stress_return 容量压力收益（原 gate 组2=组4，16 字段）
    CapacityStressReturn,
    /// prediction_confidence 压力填充质量（原 gate 组3，16 字段）
    PredictionConfidenceStressFill,
    /// cash_drag 填充比率（原 gate 组5，10 字段，含 max_train_execution_target_gap_pct）
    CashDragFillRatio,
    /// cash_drag 感知（原 gate 组6，9 字段，含 max_train_final_cash_weight_pct:0.20）
    CashDragAware,
    /// 默认 gate policy，无 override
    Base,
}

/// 单一条目：canonical 名 + is_* 类别 + gate 类别 + 别名列表。
/// 别名列表在此定义一次，所有分派点查 category 派生。
pub(crate) struct ProfileCatalogEntry {
    pub canonical: &'static str,
    pub category: SearchProfileCategory,
    pub gate_category: GateCategory,
    pub aliases: &'static [&'static str],
}

/// 单一真相源：每个 canonical profile 的别名列表只在此定义一次。
/// 仅含需要 category 派生的 profile；其余（Default 类）无需条目（fallback）。
pub(crate) const PROFILE_CATALOG: &[ProfileCatalogEntry] = &[
    ProfileCatalogEntry {
        canonical: "professional_current_event_nonlinear_alpha_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_current_event_nonlinear_alpha_discovery",
            "current_event_nonlinear_alpha_discovery",
            "phase7_current_event_nonlinear_alpha_discovery",
            "phase7_fs",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_ensemble_discovery",
        category: SearchProfileCategory::Ensemble,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_ensemble_discovery",
            "phase7_ensemble_v1",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_alpha_capacity_bridge",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_alpha_capacity_bridge",
            "execution_alpha_capacity_bridge",
            "alpha_capacity_bridge",
            "phase7_execution_alpha_capacity_bridge",
            "phase7_eb",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_alpha_capacity_return_frontier",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_alpha_capacity_return_frontier",
            "execution_alpha_capacity_return_frontier",
            "alpha_capacity_return_frontier",
            "phase7_execution_alpha_capacity_return_frontier",
            "phase7_ec",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_alpha_headroom_floor",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_alpha_headroom_floor",
            "execution_alpha_headroom_floor",
            "alpha_headroom_floor",
            "phase7_execution_alpha_headroom_floor",
            "phase7_el",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_blended_alpha_headroom_floor",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_blended_alpha_headroom_floor",
            "execution_blended_alpha_headroom_floor",
            "blended_alpha_headroom_floor",
            "phase7_execution_blended_alpha_headroom_floor",
            "phase7_em",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_broad_financial_feature_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_broad_financial_feature_discovery",
            "execution_broad_financial_feature_discovery",
            "broad_financial_feature_discovery",
            "phase7_execution_broad_financial_feature_discovery",
            "phase7_fg",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_broad_financial_feature_stratified_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_broad_financial_feature_stratified_discovery",
            "execution_broad_financial_feature_stratified_discovery",
            "broad_financial_feature_stratified_discovery",
            "phase7_execution_broad_financial_feature_stratified_discovery",
            "phase7_fh",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_bull_sleeve_cash_recovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_bull_sleeve_cash_recovery",
            "execution_bull_sleeve_cash_recovery",
            "bull_sleeve_cash_recovery",
            "phase7_execution_bull_sleeve_cash_recovery",
            "phase7_eu",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_capacity_aware_candidate_ranking",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_capacity_aware_candidate_ranking",
            "execution_capacity_aware_candidate_ranking",
            "capacity_aware_candidate_ranking",
            "phase7_execution_capacity_aware_candidate_ranking",
            "phase7_eq",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_capacity_fill_frontier",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_capacity_fill_frontier",
            "execution_capacity_fill_frontier",
            "capacity_fill_frontier",
            "phase7_execution_capacity_fill_frontier",
            "phase7_ea",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_capacity_stress_return_gate",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_capacity_stress_return_gate",
            "execution_capacity_stress_return_gate",
            "capacity_stress_return_gate",
            "phase7_execution_capacity_stress_return_gate",
            "phase7_ef",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_cash_drag_aware_budget",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragAware,
        aliases: &[
            "professional_execution_cash_drag_aware_budget",
            "execution_cash_drag_aware_budget",
            "cash_drag_aware_execution_budget",
            "phase7_execution_cash_drag_aware_budget",
            "phase7_dv",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_cl_anchor_fill_recovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_cl_anchor_fill_recovery",
            "execution_cl_anchor_fill_recovery",
            "cl_anchor_fill_recovery",
            "phase7_execution_cl_anchor_fill_recovery",
            "phase7_ep",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_event_anchor_stress_bridge",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_event_anchor_stress_bridge",
            "execution_event_anchor_stress_bridge",
            "event_anchor_stress_bridge",
            "phase7_execution_event_anchor_stress_bridge",
            "phase7_en",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_event_post_return_curve_alpha",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_event_post_return_curve_alpha",
            "execution_event_post_return_curve_alpha",
            "event_post_return_curve_alpha",
            "phase7_execution_event_post_return_curve_alpha",
            "phase7_fb",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_event_reaction_alpha",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_event_reaction_alpha",
            "execution_event_reaction_alpha",
            "event_reaction_alpha",
            "phase7_execution_event_reaction_alpha",
            "phase7_fc",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_feasible_fill_budget",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragAware,
        aliases: &[
            "professional_execution_feasible_fill_budget",
            "execution_feasible_fill_budget",
            "cash_utilization_execution_budget",
            "phase7_execution_feasible_fill_budget",
            "phase7_dx",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_fill_ratio_budget",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragFillRatio,
        aliases: &[
            "professional_execution_fill_ratio_budget",
            "execution_fill_ratio_budget",
            "execution_unfilled_gap_budget",
            "phase7_execution_fill_ratio_budget",
            "phase7_dy",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_low_impact_alpha_stress_return",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_low_impact_alpha_stress_return",
            "execution_low_impact_alpha_stress_return",
            "low_impact_alpha_stress_return",
            "phase7_execution_low_impact_alpha_stress_return",
            "phase7_eg",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_native_alpha_fusion_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_native_alpha_fusion_discovery",
            "execution_native_alpha_fusion_discovery",
            "native_alpha_fusion_discovery",
            "phase7_execution_native_alpha_fusion_discovery",
            "phase7_fj",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_oos_benchmark_excess_rebuild",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_oos_benchmark_excess_rebuild",
            "execution_oos_benchmark_excess_rebuild",
            "oos_benchmark_excess_rebuild",
            "phase7_execution_oos_benchmark_excess_rebuild",
            "phase7_ew",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_oos_execution_adaptive_rebuild",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_oos_execution_adaptive_rebuild",
            "execution_oos_execution_adaptive_rebuild",
            "oos_execution_adaptive_rebuild",
            "phase7_execution_oos_execution_adaptive_rebuild",
            "phase7_ex",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_oos_regime_alpha_rebuild",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_oos_regime_alpha_rebuild",
            "execution_oos_regime_alpha_rebuild",
            "oos_regime_alpha_rebuild",
            "phase7_execution_oos_regime_alpha_rebuild",
            "phase7_ev",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_participation_aware_event_anchor",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_participation_aware_event_anchor",
            "execution_participation_aware_event_anchor",
            "participation_aware_event_anchor",
            "phase7_execution_participation_aware_event_anchor",
            "phase7_eo",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pit_alpha_first_low_impact",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pit_alpha_first_low_impact",
            "execution_pit_alpha_first_low_impact",
            "pit_alpha_first_low_impact",
            "phase7_execution_pit_alpha_first_low_impact",
            "phase7_es",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pit_capacity_ranking",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pit_capacity_ranking",
            "execution_pit_capacity_ranking",
            "pit_capacity_ranking",
            "phase7_execution_pit_capacity_ranking",
            "phase7_er",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pit_excess_return_recovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pit_excess_return_recovery",
            "execution_pit_excess_return_recovery",
            "pit_excess_return_recovery",
            "phase7_execution_pit_excess_return_recovery",
            "phase7_et",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pit_nonlinear_alpha_regime_rebuild",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pit_nonlinear_alpha_regime_rebuild",
            "execution_pit_nonlinear_alpha_regime_rebuild",
            "pit_nonlinear_alpha_regime_rebuild",
            "phase7_execution_pit_nonlinear_alpha_regime_rebuild",
            "phase7_ez",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pit_quality_recovery_alpha",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pit_quality_recovery_alpha",
            "execution_pit_quality_recovery_alpha",
            "pit_quality_recovery_alpha",
            "phase7_execution_pit_quality_recovery_alpha",
            "phase7_fa",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_pressure_headroom_floor",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_pressure_headroom_floor",
            "execution_pressure_headroom_floor",
            "pressure_headroom_floor",
            "phase7_execution_pressure_headroom_floor",
            "phase7_ek",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_return_first_fill_repair",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_return_first_fill_repair",
            "execution_return_first_fill_repair",
            "return_first_fill_repair",
            "phase7_execution_return_first_fill_repair",
            "phase7_ey",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_rolling_carry_budget",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_rolling_carry_budget",
            "execution_rolling_carry_budget",
            "execution_roll_forward_budget",
            "phase7_execution_rolling_carry_budget",
            "phase7_dz",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_stress_fill_return_frontier",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_stress_fill_return_frontier",
            "execution_stress_fill_return_frontier",
            "stress_fill_return_frontier",
            "phase7_execution_stress_fill_return_frontier",
            "phase7_ed",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_stress_floor_return_recovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_stress_floor_return_recovery",
            "execution_stress_floor_return_recovery",
            "stress_floor_return_recovery",
            "phase7_execution_stress_floor_return_recovery",
            "phase7_ej",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_stress_floor_scaling_return",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_stress_floor_scaling_return",
            "execution_stress_floor_scaling_return",
            "stress_floor_scaling_return",
            "phase7_execution_stress_floor_scaling_return",
            "phase7_ei",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_stress_risk_budget",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CashDragBasic,
        aliases: &[
            "professional_execution_stress_risk_budget",
            "execution_stress_risk_budget",
            "stress_risk_budget",
            "phase7_execution_stress_risk_budget",
            "phase7_ee",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_execution_stress_target_scaling_return",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_execution_stress_target_scaling_return",
            "execution_stress_target_scaling_return",
            "stress_target_scaling_return",
            "phase7_execution_stress_target_scaling_return",
            "phase7_eh",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_capacity_dual_objective",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_capacity_dual_objective",
            "prediction_capacity_dual_objective",
            "phase7_prediction_capacity_dual_objective",
            "phase7_fl",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_confidence_alpha_lift",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_confidence_alpha_lift",
            "prediction_confidence_alpha_lift",
            "phase7_prediction_confidence_alpha_lift",
            "phase7_fo",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_confidence_turnover_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_confidence_turnover_discovery",
            "prediction_confidence_turnover_discovery",
            "phase7_prediction_confidence_turnover_discovery",
            "phase7_fn",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_h120_low_impact_stress_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_h120_low_impact_stress_discovery",
            "prediction_h120_low_impact_stress_discovery",
            "h120_low_impact_stress_discovery",
            "phase7_prediction_h120_low_impact_stress_discovery",
            "phase7_fz",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_h60_nonlinear_stress_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_h60_nonlinear_stress_discovery",
            "prediction_h60_nonlinear_stress_discovery",
            "h60_nonlinear_stress_discovery",
            "phase7_prediction_h60_nonlinear_stress_discovery",
            "phase7_fw",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_long_horizon_low_turnover",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_long_horizon_low_turnover",
            "prediction_long_horizon_low_turnover",
            "phase7_prediction_long_horizon_low_turnover",
            "phase7_fp",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_long_horizon_regime_alpha",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_long_horizon_regime_alpha",
            "prediction_long_horizon_regime_alpha",
            "phase7_prediction_long_horizon_regime_alpha",
            "phase7_fr",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_prediction_target_gross_signal_fidelity",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_prediction_target_gross_signal_fidelity",
            "prediction_target_gross_signal_fidelity",
            "phase7_prediction_target_gross_signal_fidelity",
            "phase7_fm",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_simple_nlqr_discovery",
        category: SearchProfileCategory::SimpleNlqr,
        gate_category: GateCategory::Base,
        aliases: &[
            "professional_simple_nlqr_discovery",
            "simple_nlqr_discovery",
            "phase7_simple_nlqr",
            "phase7_s1",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_train_window_ml_stress_fill_discovery",
        category: SearchProfileCategory::TrainWindowMlStressFill,
        gate_category: GateCategory::PredictionConfidenceStressFill,
        aliases: &[
            "professional_train_window_ml_stress_fill_discovery",
            "train_window_ml_stress_fill_discovery",
            "ml_stress_fill_discovery",
            "phase7_train_window_ml_stress_fill_discovery",
            "phase7_gb",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_train_window_nonlinear_ranking_discovery",
        category: SearchProfileCategory::TrainWindowNonlinearRanking,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_train_window_nonlinear_ranking_discovery",
            "train_window_nonlinear_ranking_discovery",
            "phase7_train_window_nonlinear_ranking_discovery",
            "phase7_fx",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_train_window_stress_fill_target_exposure",
        category: SearchProfileCategory::TrainWindowStressFillTargetExposure,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_train_window_stress_fill_target_exposure",
            "train_window_stress_fill_target_exposure",
            "stress_fill_target_exposure",
            "phase7_train_window_stress_fill_target_exposure",
            "phase7_ga",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_trainable_alpha_admission_discovery",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_trainable_alpha_admission_discovery",
            "trainable_alpha_admission_discovery",
            "phase7_trainable_alpha_admission",
            "phase7_ft",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_current_baseline",
        category: SearchProfileCategory::V19CurrentBaseline,
        gate_category: GateCategory::Base,
        aliases: &[
            "professional_v19_current_baseline",
            "v19_current_baseline",
            "v19_current",
            "phase7_v19_current_baseline",
            "phase7_v19_current",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_event_post_return_overlay_admission",
        category: SearchProfileCategory::Default,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_event_post_return_overlay_admission",
            "v19_event_post_return_overlay_admission",
            "v19_event_post_return_overlay",
            "phase7_v19_event_post_return_overlay",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_event_surprise_sleeve_gate_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_event_surprise_sleeve_gate_admission",
            "v19_event_surprise_sleeve_gate",
            "phase7_v19_event_surprise_sleeve_gate",
            "phase7_p311_event_surprise_sleeve_gate",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_execution_repair_admission",
        category: SearchProfileCategory::V19ExecutionRepair,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_execution_repair_admission",
            "v19_execution_repair_admission",
            "v19_execution_repair",
            "phase7_v19_execution_repair",
            "phase7_v19_exec_repair",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_forecast_revision_sleeve_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_forecast_revision_sleeve_admission",
            "v19_forecast_revision_sleeve",
            "phase7_v19_forecast_revision_sleeve",
            "phase7_p313_forecast_revision_sleeve",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_multi_alpha_sleeve_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_multi_alpha_sleeve_admission",
            "v19_multi_alpha_sleeve_admission",
            "multi_alpha_sleeve_admission",
            "phase7_v19_sleeves",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_shareholder_structure_sleeve_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_shareholder_structure_sleeve_admission",
            "v19_shareholder_structure_sleeve",
            "phase7_v19_shareholder_structure_sleeve",
            "phase7_p321e_shareholder_structure_sleeve",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_supply_float_sleeve_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_supply_float_sleeve_admission",
            "v19_supply_float_sleeve",
            "phase7_v19_supply_float_sleeve",
            "phase7_p312_supply_float_sleeve",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_alpha_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlAlphaRebuild,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_alpha_rebuild",
            "v19_train_window_ml_alpha_rebuild",
            "v19_ml_alpha_rebuild",
            "phase7_v19_train_window_ml_alpha_rebuild",
            "phase7_v19_ml_alpha_rebuild",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_event_sentiment_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlEventSentiment,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_event_sentiment_rebuild",
            "v19_train_window_ml_event_sentiment_rebuild",
            "v19_ml_event_sentiment_rebuild",
            "phase7_v19_train_window_ml_event_sentiment",
            "phase7_v19_ml_event_sentiment",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_h120_low_impact_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlH120LowImpact,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_h120_low_impact_rebuild",
            "v19_train_window_ml_h120_low_impact_rebuild",
            "v19_ml_h120_low_impact_rebuild",
            "phase7_v19_train_window_ml_h120_low_impact",
            "phase7_v19_ml_h120_low_impact",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlRaeH120Residual,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild",
            "v19_train_window_ml_rae_h120_residual_capacity_rebuild",
            "v19_ml_rae_h120_residual_capacity_rebuild",
            "phase7_v19_train_window_ml_rae_h120_residual_capacity",
            "phase7_v19_ml_rae_h120_residual_capacity",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_simple_excess_low_impact_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlSimpleExcessLowImpact,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_simple_excess_low_impact_rebuild",
            "v19_train_window_ml_simple_excess_low_impact_rebuild",
            "v19_ml_simple_excess_low_impact_rebuild",
            "phase7_v19_train_window_ml_simple_excess_low_impact",
            "phase7_v19_ml_simple_excess_low_impact",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_train_window_ml_simple_excess_rebuild",
        category: SearchProfileCategory::V19TrainWindowMlSimpleExcess,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_train_window_ml_simple_excess_rebuild",
            "v19_train_window_ml_simple_excess_rebuild",
            "v19_ml_simple_excess_rebuild",
            "phase7_v19_train_window_ml_simple_excess",
            "phase7_v19_ml_simple_excess",
        ],
    },
    ProfileCatalogEntry {
        canonical: "professional_v19_unlock_pressure_sleeve_admission",
        category: SearchProfileCategory::V19SleeveAdmission,
        gate_category: GateCategory::CapacityStressReturn,
        aliases: &[
            "professional_v19_unlock_pressure_sleeve_admission",
            "v19_unlock_pressure_sleeve",
            "phase7_v19_unlock_pressure_sleeve",
            "phase7_p314_unlock_pressure_sleeve",
        ],
    },
];

/// 输入任意 alias/canonical 名 → category。同步，查静态注册表。
/// 未命中返回 Default（accepts_override=true，非上述特殊类别）。
pub(crate) fn resolve_profile_category(input: Option<&str>) -> SearchProfileCategory {
    let name = input
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("local_professional");
    for entry in PROFILE_CATALOG {
        if entry.canonical == name || entry.aliases.contains(&name) {
            return entry.category;
        }
    }
    SearchProfileCategory::Default
}

/// 输入任意 alias/canonical 名 → gate 类别。同步，查静态注册表。
/// 未命中返回 Base（走默认 gate policy，无 override）。
pub(crate) fn resolve_gate_category(input: Option<&str>) -> GateCategory {
    let name = input
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("local_professional");
    for entry in PROFILE_CATALOG {
        if entry.canonical == name || entry.aliases.contains(&name) {
            return entry.gate_category;
        }
    }
    GateCategory::Base
}

use std::sync::LazyLock;

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


static CASH_DRAG_BASIC_GATE: LazyLock<Value> = LazyLock::new(|| json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }));

static CAPACITY_STRESS_RETURN_GATE: LazyLock<Value> = LazyLock::new(|| json!({
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
            }));

static PREDICTION_CONFIDENCE_STRESS_FILL_GATE: LazyLock<Value> = LazyLock::new(|| json!({
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
            }));

static CASH_DRAG_FILL_RATIO_GATE: LazyLock<Value> = LazyLock::new(|| json!({
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
            }));

static CASH_DRAG_AWARE_GATE: LazyLock<Value> = LazyLock::new(|| json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_cash_weight_pct": 0.20,
                "max_train_execution_target_gap_pct": 0.08,
                "max_train_execution_schedule_expired_count": 0
            }));

pub(crate) fn default_oos_train_selection_gate_policy_for_search_profile(
    search_profile: Option<&str>,
) -> Value {
    let base = default_oos_train_selection_gate_policy();
    // 查 gate_category 派生：别名列表仅在 PROFILE_CATALOG 中维护一次，
    // 消除原 6 组 301 别名的手动重复维护（如 phase7_en 遗漏 bug）。
    match resolve_gate_category(search_profile) {
        GateCategory::CashDragBasic => merge_gate_policy(base, &CASH_DRAG_BASIC_GATE),
        GateCategory::CapacityStressReturn => {
            merge_gate_policy(base, &CAPACITY_STRESS_RETURN_GATE)
        }
        GateCategory::PredictionConfidenceStressFill => {
            merge_gate_policy(base, &PREDICTION_CONFIDENCE_STRESS_FILL_GATE)
        }
        GateCategory::CashDragFillRatio => merge_gate_policy(base, &CASH_DRAG_FILL_RATIO_GATE),
        GateCategory::CashDragAware => merge_gate_policy(base, &CASH_DRAG_AWARE_GATE),
        GateCategory::Base => base,
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
    // 查 category 派生：非执行型/非门控型 profile 接受外部预测集覆盖。
    // 别名列表仅在 PROFILE_CATALOG 中维护一次，消除多处重复。
    let cat = resolve_profile_category(Some(search_profile));
    !matches!(
        cat,
        SearchProfileCategory::TrainWindowNonlinearRanking
            | SearchProfileCategory::TrainWindowStressFillTargetExposure
            | SearchProfileCategory::TrainWindowMlStressFill
            | SearchProfileCategory::V19SleeveAdmission
            | SearchProfileCategory::V19ExecutionRepair
            | SearchProfileCategory::V19TrainWindowMlAlphaRebuild
            | SearchProfileCategory::V19TrainWindowMlSimpleExcess
            | SearchProfileCategory::V19TrainWindowMlSimpleExcessLowImpact
            | SearchProfileCategory::V19TrainWindowMlH120LowImpact
            | SearchProfileCategory::V19TrainWindowMlRaeH120Residual
            | SearchProfileCategory::V19TrainWindowMlEventSentiment
            | SearchProfileCategory::V19CurrentBaseline
    )
}


pub(crate) fn is_train_window_ml_stress_fill_profile(search_profile: Option<&str>) -> bool {
    // 查 category 派生：匹配 stress_fill + ensemble + simple_nlqr + 6 个 v19 变体。
    // 等价于原 40 别名 matches! 列表，别名列表仅在 PROFILE_CATALOG 中维护一次。
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::TrainWindowMlStressFill
            | SearchProfileCategory::Ensemble
            | SearchProfileCategory::SimpleNlqr
            | SearchProfileCategory::V19TrainWindowMlAlphaRebuild
            | SearchProfileCategory::V19TrainWindowMlSimpleExcess
            | SearchProfileCategory::V19TrainWindowMlSimpleExcessLowImpact
            | SearchProfileCategory::V19TrainWindowMlH120LowImpact
            | SearchProfileCategory::V19TrainWindowMlRaeH120Residual
            | SearchProfileCategory::V19TrainWindowMlEventSentiment
    )
}


pub(crate) fn is_ensemble_profile(search_profile: Option<&str>) -> bool {
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::Ensemble
    )
}


pub(crate) fn is_simple_nlqr_profile(search_profile: Option<&str>) -> bool {
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::SimpleNlqr
    )
}


pub(crate) fn is_v19_train_window_ml_alpha_rebuild_profile(search_profile: Option<&str>) -> bool {
    // 历史命名陷阱：此谓词名为 alpha_rebuild，但实际匹配全部 6 个 v19 变体
    // （alpha + simple_excess + simple_excess_low_impact + h120 + rae_h120 + event_sentiment）。
    // 保持行为等价：查 category 匹配全部 6 个 V19 变体。
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::V19TrainWindowMlAlphaRebuild
            | SearchProfileCategory::V19TrainWindowMlSimpleExcess
            | SearchProfileCategory::V19TrainWindowMlSimpleExcessLowImpact
            | SearchProfileCategory::V19TrainWindowMlH120LowImpact
            | SearchProfileCategory::V19TrainWindowMlRaeH120Residual
            | SearchProfileCategory::V19TrainWindowMlEventSentiment
    )
}


pub(crate) fn is_v19_train_window_ml_event_sentiment_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::V19TrainWindowMlEventSentiment
    )
}


pub(crate) fn is_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile(
    search_profile: Option<&str>,
) -> bool {
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::V19TrainWindowMlRaeH120Residual
    )
}


pub(crate) fn is_v19_train_window_ml_h120_low_impact_rebuild_profile(search_profile: Option<&str>) -> bool {
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::V19TrainWindowMlH120LowImpact
    )
}


pub(crate) fn is_v19_train_window_ml_simple_excess_rebuild_profile(search_profile: Option<&str>) -> bool {
    // simple_excess + simple_excess_low_impact 两个变体（与原 10 别名等价）。
    matches!(
        resolve_profile_category(search_profile),
        SearchProfileCategory::V19TrainWindowMlSimpleExcess
            | SearchProfileCategory::V19TrainWindowMlSimpleExcessLowImpact
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


