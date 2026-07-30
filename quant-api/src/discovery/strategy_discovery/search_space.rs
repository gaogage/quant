//! BC4 策略发现 / search_space：搜索空间规格（LayeredSearchConfig + 种子 + 计划）。
//!
//! R8 批次3a 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。
//! 承载：6 个 professional_v14_*_seed_trials + LayeredSearchConfig（37 字段 + 162 default 方法）
//!      + LayeredSignalCandidate + LayeredSearchTrial/Plan + build_layered_search_plan
//!      + cost/execution profile helpers + LayeredTrialIndices。

use super::profiles::{
    LocalResourcePlan, ComboVersion, ScoreDirection,
    PortfolioDrawdownControlProfile, PortfolioVolatilityControlProfile,
    PortfolioSharpeControlProfile, PositionRiskControlProfile,
    CostCapacityStressProfile, EventGateProfile,
};
use super::alpha_admission::{phase7_alpha_blend_profiles, phase7_base_trainable_combo_versions};
use super::candidate_screening::decimal_string;
use super::seed_generators::*;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[allow(clippy::too_many_arguments)]

fn professional_v14_sharpe_return_lift_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_start,
        sharpe_full,
        sharpe_lookback,
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            180,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            180,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            400,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            170,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "soft_single_name_20pct_v1",
            650,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        with_event_combo_gate_seed_with_min_score(
                            boundary.clone(),
                            valuation_profile,
                            "phase7_valuation_v1",
                            "exclude_negative",
                            valuation_min_score,
                            "0",
                            ScoreDirection::Descending,
                        ),
                        "quality_mixed_state_risk_memory_router_v14",
                        &format!(
                            "v14_{valuation_profile}_{vol_profile}_{sharpe_profile}_return_lift"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb{risk_budget_lookback_days}_v14_{valuation_profile}_{vol_profile}"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            sharpe_start,
            sharpe_full,
            sharpe_lookback,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_shape_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base = with_sharpe_profile_seed(
        with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        anchor,
                        "valuation_exclude_bottom45",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.45",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_mixed_state_risk_memory_router_v14",
                    "v14_shape_lift_anchor",
                ),
                "vol120_142_465_100",
                "0.142",
                120,
                "0.465",
                "1",
            ),
            "bp_rb170_v14_shape_lift",
            170,
        ),
        "roll_sharpe180_050_neg10_65",
        "0.50",
        "-0.10",
        180,
        "0.65",
    );
    let base = with_candidate_risk_filter_seed(base, "off");

    let mut seeds = Vec::new();
    for (
        top_n,
        rebalance_days,
        skip_top_pct,
        risk_contribution_control,
        drawdown_profile,
        stop_loss_profile,
        smoothing_profile,
        score_candidate_pool_size,
    ) in [
        (
            20,
            60,
            "0.10",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            50,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            20,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
        (
            22,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            650,
        ),
        (
            22,
            50,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            650,
        ),
        (
            20,
            50,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            60,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_075_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
        (
            20,
            55,
            "0.10",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            22,
            60,
            "0.10",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            650,
        ),
        (
            20,
            55,
            "0.08",
            "soft_single_name_20pct_v1",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            55,
            "0.10",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_070_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
    ] {
        let mut seed = with_skip_top_seed(
            with_rebalance_days_seed(
                with_top_n_seed(base.clone(), top_n, &format!("top{top_n}_shape_lift")),
                rebalance_days,
                &format!("rebalance{rebalance_days}_shape_lift"),
            ),
            skip_top_pct,
            &format!("skip{}_shape_lift", skip_top_pct.replace('.', "")),
        );
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        seed = match drawdown_profile {
            "recover252_08_23_45_30_70" => with_drawdown_profile_seed(
                seed,
                "recover252_08_23_45_30_70",
                "0.08",
                "0.23",
                "0.45",
                252,
                "0.30",
                "0.70",
                "1",
            ),
            _ => seed,
        };
        seed = match stop_loss_profile {
            "stop_loss_070_cooldown_30" => {
                with_stop_loss_cooldown_seed(seed, "stop_loss_070_cooldown_30", "0.07", 30)
            }
            _ => seed,
        };
        seed = match smoothing_profile {
            "hysteresis_1pct_partial_75" => {
                with_rebalance_smoothing_seed(seed, "hysteresis_1pct_partial_75", "0.01", "0.75")
            }
            _ => seed,
        };
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_ultra_micro_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base = with_sharpe_profile_seed(
        with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        anchor,
                        "valuation_exclude_bottom45",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.45",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_mixed_state_risk_memory_router_v14",
                    "v14_ultra_micro_lift_anchor",
                ),
                "vol120_142_465_100",
                "0.142",
                120,
                "0.465",
                "1",
            ),
            "bp_rb170_v14_ultra_micro_lift",
            170,
        ),
        "roll_sharpe180_050_neg10_65",
        "0.50",
        "-0.10",
        180,
        "0.65",
    );
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(base, "off"), "off");

    let mut seeds = Vec::new();
    for (
        top_n,
        rebalance_days,
        skip_top_pct,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
    ) in [
        (20, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (19, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (21, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 58, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 62, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.09", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.11", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.10", "vol120_141_462_100", "0.141", "0.462", 170),
        (20, 60, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 60, "0.10", "vol120_144_47_100", "0.144", "0.47", 170),
        (19, 58, "0.09", "vol120_142_465_100", "0.142", "0.465", 170),
        (21, 62, "0.11", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 58, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 62, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 180),
    ] {
        let mut seed = with_skip_top_seed(
            with_rebalance_days_seed(
                with_top_n_seed(
                    with_risk_budget_lookback_seed(
                        with_volatility_profile_seed(
                            base.clone(),
                            vol_profile,
                            target_pct,
                            120,
                            min_exposure,
                            "1",
                        ),
                        &format!("bp_rb{risk_budget_lookback_days}_v14_ultra_micro_lift"),
                        risk_budget_lookback_days,
                    ),
                    top_n,
                    &format!("top{top_n}_v14_ultra_micro_lift"),
                ),
                rebalance_days,
                &format!("rebalance{rebalance_days}_v14_ultra_micro_lift"),
            ),
            skip_top_pct,
            &format!("skip{}_v14_ultra_micro_lift", skip_top_pct.replace('.', "")),
        );
        seed["score_candidate_pool_size"] = json!(500);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_annual_floor_micro_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        regime,
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
    ) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v18",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_0475_neg10_66",
            "0.475",
            "0.66",
            165,
            650,
        ),
    ] {
        let seed = with_market_regime_seed(
            with_skip_top_seed(
                with_rebalance_days_seed(
                    with_top_n_seed(
                        with_sharpe_profile_seed(
                            with_risk_budget_lookback_seed(
                                with_volatility_profile_seed(
                                    base.clone(),
                                    vol_profile,
                                    target_pct,
                                    120,
                                    min_exposure,
                                    "1",
                                ),
                                &format!("bp_rb{risk_budget_lookback_days}_{regime}_annual_floor"),
                                risk_budget_lookback_days,
                            ),
                            sharpe_profile,
                            sharpe_start,
                            "-0.10",
                            180,
                            sharpe_min_exposure,
                        ),
                        20,
                        "top20_v14_annual_floor",
                    ),
                    60,
                    "rebalance60_v14_annual_floor",
                ),
                "0.10",
                "skip010_v14_annual_floor",
            ),
            regime,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            regime,
            &format!("{regime}_{vol_profile}_{sharpe_profile}_annual_floor_micro_lift"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_near_miss_annual_bridge_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1408_4615_100",
            "0.1408",
            "0.4615",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1408_4615_100",
            "0.1408",
            "0.4615",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            160,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            650,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.145",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.145",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            160,
            650,
            "0.145",
            "0.70",
        ),
    ] {
        let seed = with_position_shape_seed(
            with_sharpe_profile_seed(
                with_risk_budget_lookback_seed(
                    with_volatility_profile_seed(
                        base.clone(),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("bp_rb{risk_budget_lookback_days}_v14_near_miss_annual_bridge"),
                    risk_budget_lookback_days,
                ),
                sharpe_profile,
                sharpe_start,
                "-0.10",
                180,
                sharpe_min_exposure,
            ),
            &format!(
                "maxpos{}_corr{}_v14_near_miss",
                max_position_pct.replace('.', ""),
                max_pairwise_correlation.replace('.', "")
            ),
            max_position_pct,
            max_pairwise_correlation,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            "quality_mixed_state_risk_memory_router_v14",
            &format!("{vol_profile}_{sharpe_profile}_v14_near_miss_annual_bridge"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_corr70_annual_edge_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1418_4645_100",
            "0.1418",
            "0.4645",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1418_4645_100",
            "0.1418",
            "0.4645",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1422_4655_100",
            "0.1422",
            "0.4655",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1422_4655_100",
            "0.1422",
            "0.4655",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            650,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            650,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            168,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            168,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.68",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.72",
        ),
    ] {
        let seed = with_position_shape_seed(
            with_sharpe_profile_seed(
                with_risk_budget_lookback_seed(
                    with_volatility_profile_seed(
                        base.clone(),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("bp_rb{risk_budget_lookback_days}_v14_corr70_annual_edge"),
                    risk_budget_lookback_days,
                ),
                sharpe_profile,
                sharpe_start,
                "-0.10",
                180,
                sharpe_min_exposure,
            ),
            &format!(
                "maxpos{}_corr{}_v14_corr70_edge",
                max_position_pct.replace('.', ""),
                max_pairwise_correlation.replace('.', "")
            ),
            max_position_pct,
            max_pairwise_correlation,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            "quality_mixed_state_risk_memory_router_v14",
            &format!("{vol_profile}_{sharpe_profile}_v14_corr70_annual_edge"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]

#[allow(dead_code)]

#[allow(clippy::too_many_arguments)]

#[allow(clippy::too_many_arguments)]


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchConfig {
    pub market_regime_policies: Vec<String>,
    pub combo_versions: Vec<ComboVersion>,
    pub prediction_set_ids: Vec<String>,
    pub top_n: Vec<usize>,
    pub rebalance_days: Vec<usize>,
    pub score_directions: Vec<ScoreDirection>,
    pub skip_top_pct: Vec<Decimal>,
    pub max_pairwise_correlation: Vec<Decimal>,
    pub kelly_fraction: Vec<Decimal>,
    pub max_position_pct: Vec<Decimal>,
    pub max_gross_exposure: Vec<Decimal>,
    pub portfolio_methods: Vec<String>,
    pub risk_budget_lookback_days: Vec<usize>,
    pub capacity_penalty_strength: Vec<Decimal>,
    pub capacity_risk_budget_profiles: Vec<String>,
    pub cash_utilization_profiles: Vec<String>,
    pub execution_impact_budget_profiles: Vec<String>,
    pub execution_schedule_profiles: Vec<String>,
    pub execution_carry_policy_profiles: Vec<String>,
    pub cost_capacity_stress_profiles: Vec<CostCapacityStressProfile>,
    pub industry_max_weight_pct: Vec<Option<Decimal>>,
    pub style_risk_budget_profiles: Vec<String>,
    pub candidate_risk_filter_profiles: Vec<String>,
    pub candidate_ranking_profiles: Vec<String>,
    pub risk_contribution_control_profiles: Vec<String>,
    pub stress_fill_confidence_exposure_profiles: Vec<String>,
    pub rebalance_hysteresis_pct: Vec<Decimal>,
    pub partial_rebalance_ratio: Vec<Decimal>,
    pub score_candidate_pool_sizes: Vec<usize>,
    pub universe_profiles: Vec<String>,
    pub portfolio_drawdown_controls: Vec<PortfolioDrawdownControlProfile>,
    pub portfolio_volatility_controls: Vec<PortfolioVolatilityControlProfile>,
    pub portfolio_sharpe_controls: Vec<PortfolioSharpeControlProfile>,
    pub position_risk_controls: Vec<PositionRiskControlProfile>,
    pub event_gate_profiles: Vec<EventGateProfile>,
    pub seed_trials: Vec<Value>,
    pub correlation_lookback_days: usize,
    pub kelly_lookback_days: usize,
    pub benchmark: String,
}

impl LayeredSearchConfig {
    pub fn local_professional_default() -> Self {
        let mut combo_versions = vec![
            ComboVersion::new("full_icir_16f_v3", "1.0.0"),
            ComboVersion::new("full_icir_16f_v2", "20260511"),
            ComboVersion::new("full_icir_16f", "1.0.0"),
            ComboVersion::new("full_eq_16f", "1.0.0"),
            ComboVersion::new("phase7_price_volume_expanded_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_valuation_v1", "1.0.0"),
            ComboVersion::new("phase7_moneyflow_v1", "1.0.0"),
            ComboVersion::new("phase7_event_earnings_v1", "1.0.0"),
            ComboVersion::new("phase7_event_surprise_v1", "1.0.0"),
            ComboVersion::new("phase7_event_window_earnings_v1", "1.0.0"),
        ];
        combo_versions.extend(
            phase7_alpha_blend_profiles()
                .into_iter()
                .map(|profile| profile.combo_version()),
        );

        Self {
            market_regime_policies: vec![
                "off".to_string(),
                "professional_default".to_string(),
                "drawdown_control_v1".to_string(),
                "drawdown_control_v2".to_string(),
                "quality_risk_off_v1".to_string(),
                "quality_crash_guard_v1".to_string(),
            ],
            combo_versions,
            prediction_set_ids: Vec::new(),
            top_n: vec![20, 30, 50, 80],
            rebalance_days: vec![5, 10, 20, 60],
            score_directions: vec![ScoreDirection::Descending, ScoreDirection::Ascending],
            skip_top_pct: vec![Decimal::ZERO, Decimal::new(5, 2), Decimal::new(10, 2)],
            max_pairwise_correlation: vec![
                Decimal::new(65, 2),
                Decimal::new(75, 2),
                Decimal::new(90, 2),
            ],
            kelly_fraction: vec![Decimal::ZERO, Decimal::new(25, 2), Decimal::new(50, 2)],
            max_position_pct: vec![Decimal::new(5, 2), Decimal::new(8, 2), Decimal::new(10, 2)],
            max_gross_exposure: vec![Decimal::new(80, 2), Decimal::ONE],
            portfolio_methods: vec!["heuristic".to_string(), "risk_budget".to_string()],
            risk_budget_lookback_days: vec![60, 120],
            capacity_penalty_strength: vec![Decimal::ZERO, Decimal::new(75, 2)],
            capacity_risk_budget_profiles: vec!["off".to_string()],
            cash_utilization_profiles: vec!["off".to_string()],
            execution_impact_budget_profiles: vec!["off".to_string()],
            execution_schedule_profiles: vec!["immediate".to_string()],
            execution_carry_policy_profiles: vec!["expire".to_string()],
            cost_capacity_stress_profiles: vec![CostCapacityStressProfile::off()],
            industry_max_weight_pct: vec![
                None,
                Some(Decimal::new(20, 2)),
                Some(Decimal::new(35, 2)),
            ],
            style_risk_budget_profiles: vec!["off".to_string()],
            candidate_risk_filter_profiles: vec!["off".to_string()],
            candidate_ranking_profiles: vec!["off".to_string()],
            risk_contribution_control_profiles: vec!["off".to_string()],
            stress_fill_confidence_exposure_profiles: vec!["off".to_string()],
            rebalance_hysteresis_pct: vec![Decimal::ZERO],
            partial_rebalance_ratio: vec![Decimal::ONE],
            score_candidate_pool_sizes: vec![0, 200, 500],
            universe_profiles: vec![
                "all".to_string(),
                "listed_non_st".to_string(),
                "main_board_non_st".to_string(),
            ],
            portfolio_drawdown_controls: vec![
                PortfolioDrawdownControlProfile::off(),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_10_25_50",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                ),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_12_30_60",
                    Decimal::new(12, 2),
                    Decimal::new(30, 2),
                    Decimal::new(60, 2),
                    Some(252),
                ),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling504_15_35_70",
                    Decimal::new(15, 2),
                    Decimal::new(35, 2),
                    Decimal::new(70, 2),
                    Some(504),
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_10_25_50_30_70",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_10_27_50_30_70",
                    Decimal::new(10, 2),
                    Decimal::new(27, 2),
                    Decimal::new(50, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_12_30_60_30_70",
                    Decimal::new(12, 2),
                    Decimal::new(30, 2),
                    Decimal::new(60, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
            ],
            portfolio_volatility_controls: vec![
                PortfolioVolatilityControlProfile::off(),
                PortfolioVolatilityControlProfile::target(
                    "vol252_16_45_100",
                    Decimal::new(16, 2),
                    252,
                    Decimal::new(45, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol120_18_50_100",
                    Decimal::new(18, 2),
                    120,
                    Decimal::new(50, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol60_20_60_100",
                    Decimal::new(20, 2),
                    60,
                    Decimal::new(60, 2),
                    Decimal::ONE,
                ),
            ],
            portfolio_sharpe_controls: vec![PortfolioSharpeControlProfile::off()],
            position_risk_controls: vec![PositionRiskControlProfile::off()],
            event_gate_profiles: vec![EventGateProfile::off()],
            seed_trials: Vec::new(),
            correlation_lookback_days: 60,
            kelly_lookback_days: 60,
            benchmark: "000300.SH".to_string(),
        }
    }

    pub fn professional_v19_current_baseline_default() -> Self {
        let mut config = Self::local_professional_default();
        config.market_regime_policies = vec!["off".to_string()];
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.top_n = vec![30];
        config.rebalance_days = vec![10];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.skip_top_pct = vec![Decimal::ZERO];
        config.max_pairwise_correlation = vec![Decimal::new(0, 0)];
        config.kelly_fraction = vec![Decimal::new(25, 2)];
        config.max_position_pct = vec![Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["heuristic".to_string()];
        config.risk_budget_lookback_days = vec![60];
        config.capacity_penalty_strength = vec![Decimal::ZERO];
        config.capacity_risk_budget_profiles = vec!["off".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.execution_impact_budget_profiles = vec!["off".to_string()];
        config.execution_schedule_profiles = vec!["immediate".to_string()];
        config.execution_carry_policy_profiles = vec!["expire".to_string()];
        config.cost_capacity_stress_profiles = vec![CostCapacityStressProfile::off()];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.stress_fill_confidence_exposure_profiles = vec!["off".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.score_candidate_pool_sizes = vec![200];
        config.universe_profiles = vec!["all".to_string()];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::off()];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::off()];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.correlation_lookback_days = 60;
        config.kelly_lookback_days = 60;
        config.benchmark = "000300.SH".to_string();
        config.seed_trials = vec![json!({
            "signal_source": "prediction_blend",
            "combo_name": "full_pit_icir_37f",
            "version": "1.0.0",
            "prediction_set_id": "pred-fullperiod-nlqr-20140101-20260630",
            "prediction_blend_weight": "0.5",
            "top_n": 30,
            "rebalance": "10",
            "entry_delay": 0,
            "score_direction": "ascending",
            "skip_top_pct": "0",
            "kelly_fraction": "0.25",
            "kelly_lookback_days": 60,
            "max_position_pct": "0.10",
            "max_gross_exposure": "1",
            "portfolio_method": "heuristic",
            "risk_budget_lookback_days": 60,
            "capacity_penalty_strength": "0",
            "score_candidate_pool_size": 200,
            "benchmark": "000300.SH"
        })];
        config
    }

    pub fn professional_breakthrough_default() -> Self {
        let mut config = Self::local_professional_default();
        config.market_regime_policies = vec![
            "off".to_string(),
            "quality_risk_off_v1".to_string(),
            "quality_crash_guard_v1".to_string(),
        ];
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0"),
        ];
        config.top_n = vec![20, 30, 50];
        config.rebalance_days = vec![40, 60, 80];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.skip_top_pct = vec![Decimal::new(10, 2), Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(90, 2)];
        config.kelly_fraction = vec![Decimal::ZERO, Decimal::new(25, 2), Decimal::new(50, 2)];
        config.max_position_pct = vec![
            Decimal::new(8, 2),
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![60, 120];
        config.capacity_penalty_strength = vec![Decimal::ZERO, Decimal::new(75, 2)];
        config.industry_max_weight_pct =
            vec![None, Some(Decimal::new(20, 2)), Some(Decimal::new(35, 2))];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 800, 1200];
        config.universe_profiles = vec![
            "all".to_string(),
            "listed_non_st".to_string(),
            "main_board_non_st".to_string(),
        ];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::off(),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_25_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(25, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_27_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(27, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::off(),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_50_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_20_60_100",
                Decimal::new(20, 2),
                60,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        config.seed_trials = professional_breakthrough_seed_trials();
        config
    }

    /// Minimal heuristic profile for WFA pipeline validation.
    /// No stress_fill, no TWAP, no roll_forward, no complex execution.
    /// Uses simple heuristic portfolio construction with monthly rebalance.
    /// Designed to isolate signal alpha from execution noise.
    pub fn professional_simple_heuristic_discovery_default() -> Self {
        let mut config = Self::local_professional_default();
        // Single factor combo — the one verified in Layer 0 backtest
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        // Simple grid: small top_n, monthly rebalance, moderate position sizing
        config.top_n = vec![20, 30];
        config.rebalance_days = vec![20, 30];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.skip_top_pct = vec![Decimal::ZERO, Decimal::new(10, 2)];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        // Heuristic only — NO stress_fill, NO risk_budget
        config.portfolio_methods = vec!["heuristic".to_string()];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(90, 2)];
        // Zero capacity penalty
        config.capacity_penalty_strength = vec![Decimal::ZERO];
        config.kelly_fraction = vec![Decimal::ZERO];
        // Keep local_professional_default execution profiles (immediate/expire/off)
        // — they are already the simplest valid settings. Do NOT override with empty vecs.
        // No market regime, no complex risk controls
        config.market_regime_policies = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        // Simple universe and candidate pool
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        // No drawdown/volatility/Sharpe controls
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::off()];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::off()];
        config.portfolio_sharpe_controls = vec![];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        // No prediction/event/alpha-sleeve complexity
        config.prediction_set_ids = vec![];
        config.event_gate_profiles = vec![];
        config.cost_capacity_stress_profiles = vec![CostCapacityStressProfile::off()];
        // Seed trials with exact parameters verified in Layer 0 manual backtests.
        // Each seed represents a distinct parameter combo for the grid search.
        config.seed_trials = vec![
            // Base config — matches Layer 0 W1 (AR +16.5%, Sharpe 0.86)
            json!({
                "combo_name": "phase7_financial_quality_v1",
                "top_n": 20,
                "rebalance": "30",
                "score_direction": "descending",
                "skip_top_pct": "0.00",
                "max_position_pct": "0.10",
                "max_gross_exposure": "0.95",
                "portfolio_method": "heuristic",
                "benchmark": "000300.SH",
                "universe_profile": "listed_non_st",
                "entry_delay": "1",
            }),
            // Variation: fewer stocks, tighter position limit
            json!({
                "combo_name": "phase7_financial_quality_v1",
                "top_n": 20,
                "rebalance": "20",
                "score_direction": "descending",
                "skip_top_pct": "0.00",
                "max_position_pct": "0.08",
                "max_gross_exposure": "0.95",
                "portfolio_method": "heuristic",
                "benchmark": "000300.SH",
                "universe_profile": "listed_non_st",
                "entry_delay": "1",
            }),
            // Variation: more stocks, ascending score direction
            json!({
                "combo_name": "phase7_financial_quality_v1",
                "top_n": 30,
                "rebalance": "30",
                "score_direction": "ascending",
                "skip_top_pct": "0.10",
                "max_position_pct": "0.10",
                "max_gross_exposure": "1",
                "portfolio_method": "heuristic",
                "benchmark": "000300.SH",
                "universe_profile": "listed_non_st",
                "entry_delay": "1",
            }),
            // Variation: tight risk controls
            json!({
                "combo_name": "phase7_financial_quality_v1",
                "top_n": 20,
                "rebalance": "30",
                "score_direction": "descending",
                "skip_top_pct": "0.10",
                "max_position_pct": "0.05",
                "max_gross_exposure": "0.95",
                "portfolio_method": "heuristic",
                "benchmark": "000300.SH",
                "universe_profile": "listed_non_st",
                "entry_delay": "1",
            }),
        ];
        config
    }

    /// Multi-factor heuristic profile for regime-aware WFA stock selection.
    /// Extends the simple profile with 5 factor combos spanning quality/growth/value/momentum/blend.
    /// Each WFA training window's grid search auto-selects the best combo for that regime.
    pub fn professional_multi_factor_heuristic_discovery_default() -> Self {
        let mut config = Self::professional_simple_heuristic_discovery_default();
        // 5 factor combos covering different market regimes:
        // - Quality: bear/defensive (financial quality, low debt)
        // - Growth/Recovery: bull/recovery (earnings growth, momentum)
        // - Quality+Value: sideways (cheap + profitable)
        // - Relative Strength: strong bull (pure momentum)
        // - Quality+RS: balanced (quality + momentum blend)
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
        ];
        // Slightly wider grid: more position sizing + correlation options
        config.top_n = vec![20, 30, 40];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(75, 2),
            Decimal::new(90, 2),
        ];
        // Seeds: one per combo for initial grid direction
        config.seed_trials = vec![
            json!({"combo_name": "phase7_financial_quality_v1", "top_n": 20, "rebalance": "30",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.10",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
            json!({"combo_name": "phase7_growth_recovery_v1", "top_n": 30, "rebalance": "20",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.10",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
            json!({"combo_name": "phase7_quality_value_recovery_confirm_v1", "top_n": 20, "rebalance": "30",
                   "score_direction": "descending", "skip_top_pct": "0.10", "max_position_pct": "0.07",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
            json!({"combo_name": "phase7_relative_strength_v1", "top_n": 40, "rebalance": "20",
                   "score_direction": "ascending", "skip_top_pct": "0.00", "max_position_pct": "0.10",
                   "max_gross_exposure": "1", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
        ];
        config
    }

    /// Price-volume momentum profile: phase7_price_volume_expanded_v1.
    /// Achieves +26.7% stitched AR in direct backtests — the strongest combo found.
    /// Momentum/reversal/volume patterns dominate quality factors in A-shares.
    pub fn professional_price_volume_heuristic_discovery_default() -> Self {
        let mut config = Self::professional_simple_heuristic_discovery_default();
        config.combo_versions = vec![ComboVersion::new(
            "phase7_price_volume_expanded_v1",
            "1.0.0",
        )];
        config.top_n = vec![20, 30];
        config.rebalance_days = vec![20, 30];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.seed_trials = vec![
            json!({"combo_name": "phase7_price_volume_expanded_v1", "top_n": 20, "rebalance": "30",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.10",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
            json!({"combo_name": "phase7_price_volume_expanded_v1", "top_n": 20, "rebalance": "20",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.07",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
        ];
        config
    }

    /// Blend-factor heuristic profile: single combo (recovery tilt) across all windows.
    /// blend_recovery_tilt_v1 achieves the best stitched AR (+8.9%) among all tested combos.
    /// Quality foundation provides bear-market defense; recovery tilt captures bull-market upside.
    pub fn professional_blend_factor_heuristic_discovery_default() -> Self {
        let mut config = Self::professional_simple_heuristic_discovery_default();
        config.combo_versions = vec![ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0")];
        config.top_n = vec![20, 30];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(75, 2),
            Decimal::new(90, 2),
        ];
        config.seed_trials = vec![
            json!({"combo_name": "phase7_blend_recovery_tilt_v1", "top_n": 20, "rebalance": "30",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.10",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
            json!({"combo_name": "phase7_blend_recovery_tilt_v1", "top_n": 30, "rebalance": "20",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.07",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1"}),
        ];
        config
    }

    /// Risk-managed price_volume profile for production-grade WFA.
    /// Three-layer risk control: stop-loss (per-position), drawdown (portfolio-level),
    /// and volatility targeting. Designed to compress MaxDD and improve Sharpe/Calmar
    /// while preserving the +26.7% stitched alpha of price_volume_expanded_v1.
    pub fn professional_risk_managed_price_volume_discovery_default() -> Self {
        let mut config = Self::professional_price_volume_heuristic_discovery_default();
        // Layer 1: Per-position stop-loss with cooldown
        // 7.5% stop prevents single-stock blowups; 20-day cooldown prevents whipsaw
        config.position_risk_controls = vec![
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_20",
                Decimal::new(75, 3),
                20,
            ),
            PositionRiskControlProfile::off(),
        ];
        // Layer 2: Portfolio drawdown control
        // When DD > 8%: reduce exposure to 50%. Recovery at 30% of peak DD: restore to 70%.
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "dd_recover_8_22_50_30_70",
                Decimal::new(8, 2),  // reduce_start_pct
                Decimal::new(22, 2), // reduce_full_pct
                Decimal::new(50, 2), // min_exposure
                Some(252),           // peak_lookback_days
                Decimal::new(30, 2), // recovery_start_pct
                Decimal::new(70, 2), // recovery_full_pct
                Decimal::ONE,        // recovery_boost
            ),
            PortfolioDrawdownControlProfile::preserve(
                "dd_preserve_10_25_50",
                Decimal::new(10, 2),
                Decimal::new(25, 2),
                Decimal::new(50, 2),
                Some(252),
            ),
            PortfolioDrawdownControlProfile::off(),
        ];
        // Layer 3: Portfolio volatility targeting
        // Target 18% annual vol with 120-day lookback; floor at 50% exposure
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_50_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_50_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::off(),
        ];
        // Concentrated portfolio: fewer stocks, higher conviction
        config.top_n = vec![15, 20];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![
            Decimal::new(60, 2),
            Decimal::new(70, 2),
            Decimal::new(80, 2),
        ];
        // Risk-managed seeds
        config.seed_trials = vec![
            json!({"combo_name": "phase7_price_volume_expanded_v1", "top_n": 20, "rebalance": "30",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.07",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1",
                   "position_risk_control": "stop_loss_075_cooldown_20",
                   "portfolio_drawdown_control": "dd_recover_8_22_50_30_70",
                   "portfolio_volatility_control": "vol120_18_50_100"}),
            json!({"combo_name": "phase7_price_volume_expanded_v1", "top_n": 15, "rebalance": "20",
                   "score_direction": "descending", "skip_top_pct": "0.00", "max_position_pct": "0.05",
                   "max_gross_exposure": "0.95", "portfolio_method": "heuristic",
                   "benchmark": "000300.SH", "universe_profile": "listed_non_st", "entry_delay": "1",
                   "position_risk_control": "stop_loss_075_cooldown_20",
                   "portfolio_drawdown_control": "dd_preserve_10_25_50",
                   "portfolio_volatility_control": "vol120_15_50_100"}),
        ];
        config
    }

    /// Simplified NLQR profile for WFA robustness validation.
    /// Uses 15 core factors (vs 51), 5 buckets (vs 10), excess_return label (no PIT routing),
    /// and heuristic portfolio construction (no stress_fill/TWAP/roll_forward).
    pub fn professional_simple_nlqr_discovery_default() -> Self {
        let mut config = Self::professional_simple_heuristic_discovery_default();
        // NLQR-specific: prediction_set driven, small rebalance
        config.top_n = vec![20];
        config.rebalance_days = vec![20];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2)];
        // Simple seed with ML ranking profile — triggers prediction_set_id injection
        config.seed_trials = vec![json!({
            "combo_name": "phase7_financial_quality_v1",
            "top_n": 20,
            "rebalance": "20",
            "score_direction": "descending",
            "skip_top_pct": "0.00",
            "max_position_pct": "0.07",
            "max_gross_exposure": "0.95",
            "portfolio_method": "heuristic",
            "benchmark": "000300.SH",
            "universe_profile": "listed_non_st",
            "entry_delay": "1",
            "train_window_ml_ranking_profile": "simple_nlqr_default",
        })];
        config
    }

    pub fn professional_risk_breakthrough_default() -> Self {
        let mut config = Self::professional_breakthrough_default();
        config.market_regime_policies = vec!["quality_crash_guard_v1".to_string()];
        config
            .market_regime_policies
            .push("quality_crash_guard_v2".to_string());
        config
            .market_regime_policies
            .push("quality_crash_guard_v3".to_string());
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_value_quality_growth_rel_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_value_tilt_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_defensive_rel_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0"),
        ];
        config.top_n = vec![20, 25, 30];
        config.rebalance_days = vec![50, 60, 70, 80];
        config.skip_top_pct = vec![
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(75, 2),
            Decimal::new(90, 2),
        ];
        config.kelly_fraction = vec![Decimal::ZERO, Decimal::new(25, 2)];
        config.max_position_pct =
            vec![Decimal::new(8, 2), Decimal::new(10, 2), Decimal::new(12, 2)];
        config.max_gross_exposure = vec![Decimal::new(80, 2), Decimal::new(90, 2), Decimal::ONE];
        config.risk_budget_lookback_days = vec![120, 180];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.industry_max_weight_pct = vec![
            Some(Decimal::new(20, 2)),
            Some(Decimal::new(25, 2)),
            Some(Decimal::new(30, 2)),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 800];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_25_60_25_75",
                Decimal::new(8, 2),
                Decimal::new(25, 2),
                Decimal::new(60, 2),
                Some(252),
                Decimal::new(25, 2),
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_30_55_25_75",
                Decimal::new(10, 2),
                Decimal::new(30, 2),
                Decimal::new(55, 2),
                Some(252),
                Decimal::new(25, 2),
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_27_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(27, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_26_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(26, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_26_50_30_70",
                Decimal::new(9, 2),
                Decimal::new(26, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_24_45_30_70",
                Decimal::new(9, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_24_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::off(),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_25_75_100",
                Decimal::new(25, 2),
                60,
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_30_85_100",
                Decimal::new(30, 2),
                120,
                Decimal::new(85, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_30_90_100",
                Decimal::new(30, 2),
                60,
                Decimal::new(90, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![
            PositionRiskControlProfile::off(),
            PositionRiskControlProfile::stop_loss("stop_loss_07", Decimal::new(7, 2)),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_5",
                Decimal::new(7, 2),
                5,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_10",
                Decimal::new(7, 2),
                10,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_20",
                Decimal::new(7, 2),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_30",
                Decimal::new(7, 2),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_065_cooldown_20",
                Decimal::new(65, 3),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_20",
                Decimal::new(75, 3),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_30",
                Decimal::new(75, 3),
                30,
            ),
            PositionRiskControlProfile::stop_loss("stop_loss_08", Decimal::new(8, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_085", Decimal::new(85, 3)),
            PositionRiskControlProfile::stop_loss("stop_loss_09", Decimal::new(9, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_095", Decimal::new(95, 3)),
            PositionRiskControlProfile::stop_loss("stop_loss_10", Decimal::new(10, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_12", Decimal::new(12, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_14", Decimal::new(14, 2)),
            PositionRiskControlProfile::trailing_stop("trailing_stop_18", Decimal::new(18, 2)),
        ];
        config.seed_trials = professional_risk_breakthrough_seed_trials();
        config
    }

    pub fn professional_sharpe_stabilization_default() -> Self {
        let mut config = Self::professional_risk_breakthrough_default();
        config.rebalance_hysteresis_pct = vec![
            Decimal::ZERO,
            Decimal::new(5, 3),
            Decimal::new(1, 2),
            Decimal::new(2, 2),
        ];
        config.partial_rebalance_ratio =
            vec![Decimal::ONE, Decimal::new(75, 2), Decimal::new(50, 2)];
        config.seed_trials = professional_sharpe_stabilization_seed_trials();
        config
    }

    pub fn professional_regime_stabilization_default() -> Self {
        let mut config = Self::professional_sharpe_stabilization_default();
        config.market_regime_policies = vec![
            "quality_crash_guard_v1".to_string(),
            "quality_crash_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(90, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.risk_budget_lookback_days = vec![120];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2)];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_10_24_50_30_70",
            Decimal::new(10, 2),
            Decimal::new(24, 2),
            Decimal::new(50, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_stabilization_seed_trials();
        config
    }

    pub fn professional_bear_window_stabilization_default() -> Self {
        let mut config = Self::professional_regime_stabilization_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.seed_trials = professional_bear_window_stabilization_seed_trials();
        config
    }

    pub fn professional_style_risk_budget_default() -> Self {
        let mut config = Self::professional_bear_window_stabilization_default();
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
            "defensive_style_budget_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_style_risk_budget_seed_trials();
        config
    }

    pub fn professional_second_alpha_source_default() -> Self {
        let mut config = Self::professional_style_risk_budget_default();
        config.combo_versions = phase7_base_trainable_combo_versions(&[
            "phase7_event_earnings_v1",
            "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_surprise_confirm_v1",
            "phase7_quality_value_recovery_confirm_v1",
            "phase7_quality_value_recovery_event_confirm_v1",
            "phase7_industry_residual_quality_v1",
            "phase7_financial_quality_v1",
            "phase7_quality_moneyflow_pos_5pct_v1",
            "phase7_quality_event_confirm_v1",
            "phase7_blend_quality_growth_v1",
            "phase7_blend_recovery_tilt_v1",
        ]);
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_require_positive",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_second_alpha_source_seed_trials();
        config
    }

    pub fn professional_residual_quality_default() -> Self {
        let mut config = Self::professional_style_risk_budget_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_residual_quality_seed_trials();
        config
    }

    pub fn professional_residual_overlay_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_residual_overlay_sharpe_seed_trials();
        config
    }

    pub fn professional_conditioned_second_alpha_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_require_top40",
                "phase7_valuation_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "residual_require_top40",
                "phase7_industry_residual_quality_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "moneyflow_require_top40",
                "phase7_moneyflow_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_conditioned_second_alpha_seed_trials();
        config
    }

    pub fn professional_valuation_guard_sharpe_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom25",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(25, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom30",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(30, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom50",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(50, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_valuation_guard_sharpe_seed_trials();
        config
    }

    pub fn professional_regime_conditioned_valuation_guard_default() -> Self {
        let mut config = Self::professional_valuation_guard_sharpe_default();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35_stress_only",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40_stress_only",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_regime_conditioned_valuation_guard_seed_trials();
        config
    }

    pub fn professional_regime_alpha_routing_default() -> Self {
        let mut config = Self::professional_regime_conditioned_valuation_guard_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_switch_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_regime_alpha_routing_seed_trials();
        config
    }

    pub fn professional_regime_alpha_sleeve_search_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_switch_value_v1".to_string(),
            "quality_regime_alpha_switch_recovery_v1".to_string(),
            "quality_regime_alpha_switch_blend_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_sleeve_search_seed_trials();
        config
    }

    pub fn professional_regime_alpha_overlay_search_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
            "quality_regime_alpha_overlay_value_10pct_v1".to_string(),
            "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_overlay_search_seed_trials();
        config
    }

    pub fn professional_regime_alpha_sleeve_allocation_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_sleeve_allocation_seed_trials();
        config
    }

    pub fn professional_low_risk_sleeve_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_low_risk_sleeve_seed_trials();
        config
    }

    pub fn professional_value_guard_sleeve_composition_default() -> Self {
        let mut config = Self::professional_valuation_guard_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.seed_trials = professional_value_guard_sleeve_composition_seed_trials();
        config
    }

    pub fn professional_nearest_candidate_risk_model_default() -> Self {
        let mut config = Self::professional_value_guard_sleeve_composition_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
        ];
        config.portfolio_methods = vec!["risk_budget".to_string(), "min_variance".to_string()];
        config.risk_budget_lookback_days = vec![120, 180];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_18_55_100",
            Decimal::new(18, 2),
            120,
            Decimal::new(55, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_nearest_candidate_risk_model_seed_trials();
        config
    }

    pub fn professional_event_regime_sleeve_default() -> Self {
        let mut config = Self::professional_nearest_candidate_risk_model_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1".to_string(),
        ];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![120];
        config.seed_trials = professional_event_regime_sleeve_seed_trials();
        config
    }

    pub fn professional_event_window_sleeve_weight_default() -> Self {
        let mut config = Self::professional_event_regime_sleeve_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_sleeve_weight_seed_trials();
        config
    }

    pub fn professional_event_window_sleeve_upper_bound_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_weight_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_sleeve_upper_bound_seed_trials();
        config
    }

    pub fn professional_event_window_regime_placement_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_upper_bound_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_regime_placement_seed_trials();
        config
    }

    pub fn professional_event_window_decay_default() -> Self {
        let mut config = Self::professional_event_window_regime_placement_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_decay_seed_trials();
        config
    }

    pub fn professional_event_quality_segment_default() -> Self {
        let mut config = Self::professional_event_window_decay_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_quality_segment_seed_trials();
        config
    }

    pub fn professional_event_surprise_nonlinear_default() -> Self {
        let mut config = Self::professional_event_window_decay_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_3pct_stress_only",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_5pct_stress_only",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_exclude_negative_stress_only",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_require_positive_stress_only",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_event_surprise_nonlinear_seed_trials();
        config
    }

    pub fn professional_event_strength_segment_default() -> Self {
        let mut config = Self::professional_event_surprise_nonlinear_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_require_strong_p75",
                "require_positive",
                Decimal::new(38, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_require_strong_p90",
                "require_positive",
                Decimal::new(66, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_strong_p75",
                "require_positive",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_strong_p90",
                "require_positive",
                Decimal::new(43, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_require_light_p50",
                "phase7_event_earnings_v1",
                "require_positive",
                Decimal::new(39, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_event_strength_segment_seed_trials();
        config
    }

    pub fn professional_event_strength_boost_default() -> Self {
        let mut config = Self::professional_event_strength_segment_default();
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_strong_p75_3pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_boost_strong_p75_5pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_strong_p75_3pct",
                "boost_positive",
                Decimal::new(35, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_strong_p75_5pct",
                "boost_positive",
                Decimal::new(35, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_boost_light_p50_3pct",
                "phase7_event_earnings_v1",
                "boost_positive",
                Decimal::new(39, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_boost_light_p50_5pct",
                "phase7_event_earnings_v1",
                "boost_positive",
                Decimal::new(39, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_event_strength_boost_seed_trials();
        config
    }

    pub fn professional_current_anchor_risk_shape_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_upper_bound_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.market_regime_policies =
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(12, 2), Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![120, 180];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2)];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover126_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(126),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol252_16_50_100",
                Decimal::new(16, 2),
                252,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_30",
                Decimal::new(75, 3),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_070_cooldown_30",
                Decimal::new(7, 2),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_45",
                Decimal::new(75, 3),
                45,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_current_anchor_risk_shape_seed_trials();
        config
    }

    pub fn professional_current_anchor_position_frontier_default() -> Self {
        let mut config = Self::professional_current_anchor_risk_shape_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(70, 2),
            Decimal::new(75, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(12, 2),
            Decimal::new(13, 2),
            Decimal::new(14, 2),
            Decimal::new(15, 2),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.seed_trials = professional_current_anchor_position_frontier_seed_trials();
        config
    }

    pub fn professional_current_anchor_weak_window_repair_default() -> Self {
        let mut config = Self::professional_current_anchor_position_frontier_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_budget_lookback_days = vec![180];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_16_50_100",
            Decimal::new(16, 2),
            120,
            Decimal::new(50, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "stress_event_window_boost_p75_3pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_window(
                "stress_event_window_boost_p75_5pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_window(
                "stress_event_window_exclude_negative_p40",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_boost_p40_3pct",
                "phase7_valuation_v1",
                "boost_positive",
                Decimal::new(40, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_boost_p40_5pct",
                "phase7_valuation_v1",
                "boost_positive",
                Decimal::new(40, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_exclude_p40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_current_anchor_weak_window_repair_seed_trials();
        config
    }

    pub fn professional_current_anchor_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_current_anchor_position_frontier_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(70, 2)];
        config.max_position_pct = vec![Decimal::new(14, 2), Decimal::new(15, 2)];
        config.top_n = vec![20, 25];
        config.risk_budget_lookback_days = vec![180];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_current_anchor_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_high_sharpe_return_recovery_default() -> Self {
        let mut config = Self::professional_current_anchor_sharpe_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(675, 3),
            Decimal::new(70, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.top_n = vec![20, 22, 25];
        config.risk_budget_lookback_days = vec![180];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_10_24_50_30_70",
            Decimal::new(10, 2),
            Decimal::new(24, 2),
            Decimal::new(50, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_19_58_100",
                Decimal::new(19, 2),
                120,
                Decimal::new(58, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_high_sharpe_return_recovery_seed_trials();
        config
    }

    pub fn professional_risk_memory_bridge_default() -> Self {
        let mut config = Self::professional_high_sharpe_return_recovery_default();
        config.risk_budget_lookback_days = vec![120, 140, 150, 160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(675, 3)];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.top_n = vec![20];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_18_55_100",
            Decimal::new(18, 2),
            120,
            Decimal::new(55, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_risk_memory_bridge_seed_trials();
        config
    }

    pub fn professional_state_return_sharpe_router_default() -> Self {
        let mut config = Self::professional_risk_memory_bridge_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_event_window_return_sharpe_router_v1".to_string(),
            "quality_event_window_return_sharpe_router_v2".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(14, 2), Decimal::new(15, 2)];
        config.top_n = vec![20, 22];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_state_return_sharpe_router_seed_trials();
        config
    }

    pub fn professional_state_return_sharpe_frontier_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_router_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v1".to_string(),
            "quality_event_window_return_sharpe_router_v2".to_string(),
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![150, 160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(15, 2),
            Decimal::new(16, 2),
        ];
        config.top_n = vec![20];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_state_return_sharpe_frontier_seed_trials();
        config
    }

    pub fn professional_position_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_frontier_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![150, 160, 170, 180];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(675, 3),
            Decimal::new(70, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_position_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_moderate_position_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_position_sharpe_return_bridge_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(70, 2),
            Decimal::new(725, 3),
            Decimal::new(75, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(16, 2),
            Decimal::new(17, 2),
            Decimal::new(18, 2),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.seed_trials = professional_moderate_position_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_correlation_frontier_sharpe_return_default() -> Self {
        let mut config = Self::professional_moderate_position_sharpe_return_bridge_default();
        config.market_regime_policies =
            vec!["quality_event_window_return_sharpe_router_v4".to_string()];
        config.max_pairwise_correlation = vec![
            Decimal::new(705, 3),
            Decimal::new(71, 2),
            Decimal::new(715, 3),
            Decimal::new(72, 2),
        ];
        config.max_position_pct = vec![Decimal::new(16, 2), Decimal::new(165, 3)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.seed_trials = professional_correlation_frontier_sharpe_return_seed_trials();
        config
    }

    pub fn professional_correlation_threshold_sharpe_return_default() -> Self {
        let mut config = Self::professional_correlation_frontier_sharpe_return_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(706, 3),
            Decimal::new(707, 3),
            Decimal::new(708, 3),
            Decimal::new(709, 3),
        ];
        config.seed_trials = professional_correlation_threshold_sharpe_return_seed_trials();
        config
    }

    pub fn professional_soft_risk_frontier_sharpe_return_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_frontier_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec![
            "off".to_string(),
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.seed_trials = professional_soft_risk_frontier_sharpe_return_seed_trials();
        config
    }

    pub fn professional_regime_alpha_selector_default() -> Self {
        let mut config = Self::professional_soft_risk_frontier_sharpe_return_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_state_alpha_selector_v1".to_string(),
            "quality_state_alpha_selector_v2".to_string(),
            "quality_state_alpha_selector_v3".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_regime_alpha_selector_seed_trials();
        config
    }

    pub fn professional_regime_alpha_overlay_frontier_default() -> Self {
        let mut config = Self::professional_regime_alpha_selector_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_selector_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_alpha_overlay_selector_v2".to_string(),
            "quality_state_alpha_overlay_selector_v3".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO, Decimal::new(5, 3)];
        config.partial_rebalance_ratio = vec![Decimal::ONE, Decimal::new(85, 2)];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_regime_alpha_overlay_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_event_alpha_default() -> Self {
        let mut config = Self::professional_regime_alpha_overlay_frontier_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v2".to_string(),
            "quality_mixed_event_state_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_overlay_selector_v2".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_mixed_state_event_alpha_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_default() -> Self {
        let mut config = Self::professional_mixed_state_event_alpha_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v2".to_string(),
            "quality_mixed_state_risk_memory_router_v3".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v5".to_string(),
            "quality_mixed_state_risk_memory_router_v6".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_fine_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v7".to_string(),
            "quality_mixed_state_risk_memory_router_v8".to_string(),
            "quality_mixed_state_risk_memory_router_v9".to_string(),
            "quality_mixed_state_risk_memory_router_v10".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_fine_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_exposure_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v11".to_string(),
            "quality_mixed_state_risk_memory_router_v12".to_string(),
            "quality_mixed_state_risk_memory_router_v13".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.seed_trials = professional_mixed_state_exposure_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_orthogonal_alpha_default() -> Self {
        let mut config = Self::professional_mixed_state_exposure_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v2".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_defensive_rel_v1", "1.0.0"),
        ];
        config.seed_trials = professional_mixed_state_orthogonal_alpha_seed_trials();
        config
    }

    pub fn professional_candidate_filter_alpha_bridge_default() -> Self {
        let mut config = Self::professional_mixed_state_orthogonal_alpha_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_alpha_overlay_selector_v2".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_candidate_filter_alpha_bridge_seed_trials();
        config
    }

    pub fn professional_soft_candidate_filter_alpha_bridge_default() -> Self {
        let mut config = Self::professional_candidate_filter_alpha_bridge_default();
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_soft_candidate_filter_alpha_bridge_seed_trials();
        config
    }

    pub fn professional_sharpe_bridge_frontier_default() -> Self {
        let mut config = Self::professional_soft_candidate_filter_alpha_bridge_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_sharpe_bridge_router_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_sharpe_bridge_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_sharpe_bridge_frontier_seed_trials();
        config
    }

    pub fn professional_annual_sharpe_floor_bridge_default() -> Self {
        let mut config = Self::professional_sharpe_bridge_frontier_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_annual_sharpe_floor_bridge_seed_trials();
        config
    }

    pub fn professional_risk_memory_relaxed_frontier_default() -> Self {
        let mut config = Self::professional_annual_sharpe_floor_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_state_risk_memory_router_v15".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v17".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_risk_memory_relaxed_frontier_seed_trials();
        config
    }

    pub fn professional_sharpe_floor_auto_discovery_default() -> Self {
        let mut config = Self::professional_annual_sharpe_floor_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_sharpe_floor_auto_discovery_seed_trials();
        config
    }

    pub fn professional_nonlinear_alpha_auto_discovery_default() -> Self {
        let mut config = Self::professional_sharpe_floor_auto_discovery_default();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_router_v1".to_string(),
            "quality_nonlinear_alpha_router_v2".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_060_000_60",
                Decimal::new(60, 2),
                Decimal::ZERO,
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_nonlinear_alpha_auto_discovery_seed_trials();
        config
    }

    pub fn professional_nonlinear_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_nonlinear_alpha_auto_discovery_default();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v2".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_nonlinear_alpha_router_v2".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_55",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_060_000_60",
                Decimal::new(60, 2),
                Decimal::ZERO,
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.seed_trials = professional_nonlinear_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_prediction_confirmed_sharpe_bridge_default() -> Self {
        let mut config = Self::professional_nonlinear_sharpe_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_prediction_confirmed_sharpe_bridge_seed_trials();
        config
    }

    pub fn professional_prediction_capacity_dual_objective_default() -> Self {
        let mut config = Self::professional_prediction_confirmed_sharpe_bridge_default();
        config.prediction_set_ids = vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()];
        config.top_n = vec![20, 40, 60];
        config.max_position_pct =
            vec![Decimal::new(15, 2), Decimal::new(10, 2), Decimal::new(8, 2)];
        config.candidate_ranking_profiles = vec![
            "off".to_string(),
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.capacity_risk_budget_profiles = vec![
            "off".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.partial_rebalance_ratio =
            vec![Decimal::ONE, Decimal::new(35, 2), Decimal::new(25, 2)];
        config.score_candidate_pool_sizes = vec![500, 1800, 2200];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_prediction_capacity_dual_objective_seed_trials();
        config
    }

    pub fn professional_prediction_target_gross_signal_fidelity_default() -> Self {
        let mut config = Self::professional_prediction_confirmed_sharpe_bridge_default();
        config.prediction_set_ids = vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()];
        config.top_n = vec![20];
        config.max_gross_exposure = vec![
            Decimal::new(35, 2),
            Decimal::new(50, 2),
            Decimal::new(65, 2),
            Decimal::new(80, 2),
            Decimal::ONE,
        ];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.capacity_risk_budget_profiles = vec!["off".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.execution_schedule_profiles = vec!["immediate".to_string()];
        config.execution_carry_policy_profiles = vec!["off".to_string()];
        config.execution_impact_budget_profiles = vec!["off".to_string()];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.seed_trials = professional_prediction_target_gross_signal_fidelity_seed_trials();
        config
    }

    pub fn professional_prediction_confidence_turnover_discovery_default() -> Self {
        let mut config = Self::professional_prediction_confirmed_sharpe_bridge_default();
        config.prediction_set_ids = Vec::new();
        config.rebalance_days = vec![120, 160, 200];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.capacity_risk_budget_profiles = vec!["off".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.execution_schedule_profiles = vec!["immediate".to_string()];
        config.execution_carry_policy_profiles =
            vec!["off".to_string(), "roll_forward_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "off".to_string(),
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.rebalance_hysteresis_pct = vec![
            Decimal::ZERO,
            Decimal::new(5, 3),
            Decimal::new(1, 2),
            Decimal::new(2, 2),
        ];
        config.partial_rebalance_ratio = vec![
            Decimal::ONE,
            Decimal::new(85, 2),
            Decimal::new(75, 2),
            Decimal::new(65, 2),
        ];
        config.seed_trials = professional_prediction_confidence_turnover_discovery_seed_trials();
        config
    }

    pub fn professional_prediction_confidence_alpha_lift_default() -> Self {
        let mut config = Self::professional_prediction_confidence_turnover_discovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_post_return_curve_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_reaction_segments_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_reaction_reversal_overlay_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
        ];
        config.rebalance_days = vec![160, 200];
        config.rebalance_hysteresis_pct = vec![Decimal::new(1, 2), Decimal::new(2, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(75, 2), Decimal::new(65, 2)];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.seed_trials = professional_prediction_confidence_alpha_lift_seed_trials();
        config
    }

    pub fn professional_prediction_long_horizon_low_turnover_default() -> Self {
        let mut config = Self::professional_prediction_confidence_turnover_discovery_default();
        config.prediction_set_ids = Vec::new();
        config.rebalance_days = vec![200, 240];
        config.rebalance_hysteresis_pct = vec![Decimal::new(2, 2), Decimal::new(3, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(65, 2), Decimal::new(60, 2)];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.capacity_risk_budget_profiles = vec!["off".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.seed_trials = professional_prediction_long_horizon_low_turnover_seed_trials();
        config
    }

    pub fn professional_prediction_long_horizon_regime_alpha_default() -> Self {
        let mut config = Self::professional_prediction_confidence_turnover_discovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_post_return_curve_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
        ];
        config.rebalance_days = vec![160, 180, 200];
        config.rebalance_hysteresis_pct = vec![Decimal::new(1, 2), Decimal::new(2, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(75, 2), Decimal::new(65, 2)];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["off".to_string()];
        config.capacity_risk_budget_profiles = vec!["off".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.seed_trials = professional_prediction_long_horizon_regime_alpha_seed_trials();
        config
    }

    pub fn professional_prediction_h60_nonlinear_stress_discovery_default() -> Self {
        let mut config = Self::professional_prediction_long_horizon_regime_alpha_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_post_return_curve_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![180, 220, 260];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(80, 2),
            Decimal::new(90, 2),
            Decimal::new(95, 2),
        ];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.candidate_ranking_profiles = vec![
            "relative_strength_alpha_liquidity_v1".to_string(),
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(2, 2), Decimal::new(3, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(65, 2), Decimal::new(60, 2)];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![2200, 2400];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_prediction_h60_nonlinear_stress_discovery_seed_trials();
        config
    }

    pub fn professional_prediction_h120_low_impact_stress_discovery_default() -> Self {
        let mut config = Self::professional_prediction_h60_nonlinear_stress_discovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![240, 300, 360];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(60, 2), Decimal::new(55, 2)];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.score_candidate_pool_sizes = vec![2200, 2400, 2600];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_prediction_h120_low_impact_stress_discovery_seed_trials();
        config
    }

    pub fn professional_train_window_stress_fill_target_exposure_default() -> Self {
        let mut config = Self::professional_train_window_nonlinear_ranking_discovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![100, 120, 140];
        config.rebalance_days = vec![240, 300, 360];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2), Decimal::new(250, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["nonlinear_regime_alpha_liquidity_v2".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_15pct_v1".to_string(),
            "soft_single_name_20pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(55, 2), Decimal::new(60, 2)];
        config.score_candidate_pool_sizes = vec![2400, 2600, 2800];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_train_window_stress_fill_target_exposure_seed_trials();
        config
    }

    pub fn professional_train_window_ml_stress_fill_discovery_default() -> Self {
        let mut config = Self::professional_train_window_stress_fill_target_exposure_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_event_surprise_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_post_return_curve_overlay_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "north_flow_regime_confirm_v1".to_string(),
        ];
        config.top_n = vec![100, 120, 140];
        config.rebalance_days = vec![240, 300];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.portfolio_methods = vec!["stress_fill_aware_risk_budget".to_string()];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2), Decimal::new(250, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["nonlinear_regime_alpha_liquidity_v2".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_15pct_v1".to_string(),
            "soft_single_name_20pct_v1".to_string(),
        ];
        config.stress_fill_confidence_exposure_profiles =
            vec!["prediction_confidence_ascending_capacity_headroom_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(55, 2), Decimal::new(60, 2)];
        config.score_candidate_pool_sizes = vec![2400, 2600, 2800];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_train_window_ml_stress_fill_discovery_seed_trials();
        config
    }

    /// Phase 7-EN: 4-Model Ensemble + PIT routing search profile.
    /// Each WFA window trains AsymBull/BearQ/BearDef/MR NLQR models,
    /// then uses PIT signals to select the best model for OOS testing.
    pub fn professional_ensemble_discovery_default() -> Self {
        let mut config = Self::professional_train_window_stress_fill_target_exposure_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Descending];
        config.top_n = vec![20];
        config.rebalance_days = vec![20];
        config.max_position_pct = vec![Decimal::new(4, 2), Decimal::new(7, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(70, 2), Decimal::new(95, 2)];
        config.portfolio_methods = vec!["stress_fill_aware_risk_budget".to_string()];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles =
            vec!["capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()];
        config.candidate_ranking_profiles = vec!["nonlinear_regime_alpha_liquidity_v2".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_15pct_v1".to_string()];
        config.stress_fill_confidence_exposure_profiles =
            vec!["prediction_confidence_ascending_capacity_headroom_v1".to_string()];
        config.market_regime_policies =
            vec!["quality_nonlinear_alpha_risk_memory_router_v3".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(60, 2)];
        config.score_candidate_pool_sizes = vec![800];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = vec![json!({
            "train_window_ml_ranking_profile": "ensemble_default",
            "label_objective": "asymmetric_excess_return",
            "top_n": 20,
            "rebalance": "20",
            "bucket_count": 10,
            "min_samples_per_bucket": 100,
            "score_direction": "descending",
            "combo_name": "phase7_financial_quality_v1",
            "portfolio_method": "stress_fill_aware_risk_budget",
            "candidate_ranking": "nonlinear_regime_alpha_liquidity_v2",
            "max_position_pct": "0.07",
            "max_gross_exposure": "0.95",
            "capacity_penalty_strength": "2",
            "capacity_risk_budget": "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "cash_utilization": "stress_fill_gross_98_v1",
            "candidate_risk_filter": "soft_liquidity_low_volatility_low_correlation_v1",
            "risk_contribution_control": "soft_single_name_15pct_v1",
            "stress_fill_confidence_exposure": "prediction_confidence_ascending_capacity_headroom_v1",
            "execution_impact_budget": "impact_turnover_15pct_v1",
            "execution_schedule_profile": "twap_20d_v1",
            "execution_carry_policy": "roll_forward_v1",
            "rebalance_hysteresis_pct": "0.03",
            "partial_rebalance_ratio": "0.6",
            "score_candidate_pool_size": 800,
            "universe_profile": "listed_non_st",
            "market_regime": "quality_nonlinear_alpha_risk_memory_router_v3",
            "prediction_blend_weight": "0.20",
            "prediction_min_percentile": "0.30",
            "prediction_min_score": "0.00"
        })];
        config
    }

    pub fn professional_current_event_nonlinear_alpha_discovery_default() -> Self {
        let mut config = Self::professional_nonlinear_sharpe_return_bridge_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_surprise(
                "event_surprise_boost_p75_3pct",
                "boost_positive",
                Decimal::new(35, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_p90_5pct",
                "boost_positive",
                Decimal::new(43, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_20d_boost_p75_3pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_40d_boost_p75_3pct",
                "phase7_event_window_earnings_40d_v1",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_40d_exclude_negative",
                "phase7_event_window_earnings_40d_v1",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_current_event_nonlinear_alpha_discovery_seed_trials();
        config
    }

    pub fn professional_high_sharpe_boundary_return_bridge_default() -> Self {
        let mut config = Self::professional_prediction_confirmed_sharpe_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_155_49_100",
                Decimal::new(155, 3),
                120,
                Decimal::new(49, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_high_sharpe_boundary_return_bridge_seed_trials();
        config
    }

    pub fn professional_high_sharpe_boundary_event_lift_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_high_sharpe_boundary_event_lift_seed_trials();
        config
    }

    pub fn professional_high_sharpe_micro_frontier_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_155_49_100",
                Decimal::new(155, 3),
                120,
                Decimal::new(49, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.score_candidate_pool_sizes = vec![400, 500, 650, 800];
        config.seed_trials = professional_high_sharpe_micro_frontier_seed_trials();
        config
    }

    pub fn professional_v14_sharpe_return_lift_default() -> Self {
        let mut config = Self::professional_high_sharpe_micro_frontier_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom43",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(43, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_148_475_100",
                Decimal::new(148, 3),
                120,
                Decimal::new(475, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_045_neg10_63",
                Decimal::new(45, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(63, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![400, 500, 650];
        config.seed_trials = professional_v14_sharpe_return_lift_seed_trials();
        config
    }

    pub fn professional_v14_shape_lift_default() -> Self {
        let mut config = Self::professional_v14_sharpe_return_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![18, 20, 22];
        config.rebalance_days = vec![50, 55, 60];
        config.skip_top_pct = vec![Decimal::new(8, 2), Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_142_465_100",
            Decimal::new(142, 3),
            120,
            Decimal::new(465, 3),
            Decimal::ONE,
        )];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::reduce(
            "roll_sharpe180_050_neg10_65",
            Decimal::new(50, 2),
            Decimal::new(-10, 2),
            180,
            Decimal::new(65, 2),
        )];
        config.risk_budget_lookback_days = vec![170];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_shape_lift_seed_trials();
        config
    }

    pub fn professional_v14_ultra_micro_lift_default() -> Self {
        let mut config = Self::professional_v14_shape_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![19, 20, 21];
        config.rebalance_days = vec![58, 60, 62];
        config.skip_top_pct = vec![Decimal::new(9, 2), Decimal::new(10, 2), Decimal::new(11, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_144_47_100",
                Decimal::new(144, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::reduce(
            "roll_sharpe180_050_neg10_65",
            Decimal::new(50, 2),
            Decimal::new(-10, 2),
            180,
            Decimal::new(65, 2),
        )];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.seed_trials = professional_v14_ultra_micro_lift_seed_trials();
        config
    }

    pub fn professional_v14_annual_floor_micro_lift_default() -> Self {
        let mut config = Self::professional_v14_ultra_micro_lift_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_state_risk_memory_router_v15".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v17".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1405_461_100",
                Decimal::new(1405, 4),
                120,
                Decimal::new(461, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_0475_neg10_66",
                Decimal::new(475, 3),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![165, 170];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_annual_floor_micro_lift_seed_trials();
        config
    }

    pub fn professional_v14_near_miss_annual_bridge_default() -> Self {
        let mut config = Self::professional_v14_annual_floor_micro_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1405_461_100",
                Decimal::new(1405, 4),
                120,
                Decimal::new(461, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1408_4615_100",
                Decimal::new(1408, 4),
                120,
                Decimal::new(4615, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1412_463_100",
                Decimal::new(1412, 4),
                120,
                Decimal::new(463, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1415_464_100",
                Decimal::new(1415, 4),
                120,
                Decimal::new(464, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_67",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(67, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 165, 170];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(145, 3), Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_near_miss_annual_bridge_seed_trials();
        config
    }

    pub fn professional_v14_corr70_annual_edge_default() -> Self {
        let mut config = Self::professional_v14_near_miss_annual_bridge_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1415_464_100",
                Decimal::new(1415, 4),
                120,
                Decimal::new(464, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1418_4645_100",
                Decimal::new(1418, 4),
                120,
                Decimal::new(4645, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1422_4655_100",
                Decimal::new(1422, 4),
                120,
                Decimal::new(4655, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1425_466_100",
                Decimal::new(1425, 4),
                120,
                Decimal::new(466, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_67",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(67, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![165, 168];
        config.max_pairwise_correlation = vec![
            Decimal::new(68, 2),
            Decimal::new(70, 2),
            Decimal::new(72, 2),
        ];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.cost_capacity_stress_profiles = vec![
            CostCapacityStressProfile::off(),
            CostCapacityStressProfile::cost_up(
                "cost_up_125pct_slip_2bps",
                Decimal::new(125, 2),
                Decimal::new(2, 4),
            ),
            CostCapacityStressProfile::impact_limited(
                "impact_2pct_participation_10pct",
                Decimal::new(2, 2),
                Decimal::new(10, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_5pct",
                Decimal::new(5, 2),
            ),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_corr70_annual_edge_seed_trials();
        config
    }

    pub fn professional_execution_robust_candidate_default() -> Self {
        let mut config = Self::professional_v14_corr70_annual_edge_default();
        config.top_n = vec![20, 30, 50];
        config.rebalance_days = vec![60, 80, 120];
        config.risk_budget_lookback_days = vec![165, 168, 180];
        config.max_pairwise_correlation = vec![
            Decimal::new(68, 2),
            Decimal::new(70, 2),
            Decimal::new(72, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.cost_capacity_stress_profiles = vec![
            CostCapacityStressProfile::off(),
            CostCapacityStressProfile::cost_up(
                "cost_up_125pct_slip_2bps",
                Decimal::new(125, 2),
                Decimal::new(2, 4),
            ),
            CostCapacityStressProfile::impact_limited(
                "impact_2pct_participation_10pct",
                Decimal::new(2, 2),
                Decimal::new(10, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_5pct",
                Decimal::new(5, 2),
            ),
        ];
        config.rebalance_hysteresis_pct =
            vec![Decimal::ZERO, Decimal::new(1, 2), Decimal::new(2, 2)];
        config.partial_rebalance_ratio =
            vec![Decimal::ONE, Decimal::new(75, 2), Decimal::new(50, 2)];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_execution_robust_candidate_seed_trials();
        config
    }

    pub fn professional_execution_low_turnover_alpha_default() -> Self {
        let mut config = Self::professional_execution_robust_candidate_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![30, 50, 80];
        config.rebalance_days = vec![120, 160, 180];
        config.risk_budget_lookback_days = vec![180, 220];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(68, 2),
            Decimal::new(70, 2),
        ];
        config.max_position_pct =
            vec![Decimal::new(8, 2), Decimal::new(10, 2), Decimal::new(12, 2)];
        config.capacity_penalty_strength =
            vec![Decimal::ONE, Decimal::new(125, 2), Decimal::new(150, 2)];
        config.cost_capacity_stress_profiles = vec![
            CostCapacityStressProfile::off(),
            CostCapacityStressProfile::cost_up(
                "cost_up_125pct_slip_2bps",
                Decimal::new(125, 2),
                Decimal::new(2, 4),
            ),
            CostCapacityStressProfile::impact_limited(
                "impact_2pct_participation_10pct",
                Decimal::new(2, 2),
                Decimal::new(10, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_5pct",
                Decimal::new(5, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_3pct",
                Decimal::new(3, 2),
            ),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::new(2, 2), Decimal::new(3, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(50, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_low_turnover_alpha_seed_trials();
        config
    }

    pub fn professional_execution_capacity_budget_default() -> Self {
        let mut config = Self::professional_execution_low_turnover_alpha_default();
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_balanced_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.cost_capacity_stress_profiles = vec![
            CostCapacityStressProfile::off(),
            CostCapacityStressProfile::cost_up(
                "cost_up_125pct_slip_2bps",
                Decimal::new(125, 2),
                Decimal::new(2, 4),
            ),
            CostCapacityStressProfile::impact_limited(
                "impact_2pct_participation_10pct",
                Decimal::new(2, 2),
                Decimal::new(10, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_5pct",
                Decimal::new(5, 2),
            ),
            CostCapacityStressProfile::participation_limited(
                "participation_3pct",
                Decimal::new(3, 2),
            ),
        ];
        config.seed_trials = professional_execution_capacity_budget_seed_trials();
        config
    }

    pub fn professional_execution_impact_budget_default() -> Self {
        let mut config = Self::professional_execution_capacity_budget_default();
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_30pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.rebalance_hysteresis_pct =
            vec![Decimal::new(2, 2), Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![
            Decimal::new(50, 2),
            Decimal::new(35, 2),
            Decimal::new(25, 2),
        ];
        config.seed_trials = professional_execution_impact_budget_seed_trials();
        config
    }

    pub fn professional_execution_schedule_budget_default() -> Self {
        let mut config = Self::professional_execution_impact_budget_default();
        config.execution_schedule_profiles = vec![
            "twap_3d_v1".to_string(),
            "twap_5d_v1".to_string(),
            "twap_10d_v1".to_string(),
        ];
        config.seed_trials = professional_execution_schedule_budget_seed_trials();
        config
    }

    pub fn professional_execution_patient_schedule_budget_default() -> Self {
        let mut config = Self::professional_execution_schedule_budget_default();
        config.execution_schedule_profiles = vec![
            "twap_10d_v1".to_string(),
            "twap_15d_v1".to_string(),
            "twap_20d_v1".to_string(),
        ];
        config.seed_trials = professional_execution_patient_schedule_budget_seed_trials();
        config
    }

    pub fn professional_execution_daily_cap_budget_default() -> Self {
        let mut config = Self::professional_execution_patient_schedule_budget_default();
        config.seed_trials = professional_execution_daily_cap_budget_seed_trials();
        config
    }

    pub fn professional_execution_cash_drag_aware_budget_default() -> Self {
        let mut config = Self::professional_execution_daily_cap_budget_default();
        config.seed_trials = professional_execution_cash_drag_aware_budget_seed_trials();
        config
    }

    pub fn professional_execution_feasible_fill_budget_default() -> Self {
        let mut config = Self::professional_execution_cash_drag_aware_budget_default();
        config.cash_utilization_profiles = vec![
            "fillable_gross_90_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.seed_trials = professional_execution_feasible_fill_budget_seed_trials();
        config
    }

    pub fn professional_execution_fill_ratio_budget_default() -> Self {
        Self::professional_execution_feasible_fill_budget_default()
    }

    pub fn professional_execution_rolling_carry_budget_default() -> Self {
        let mut config = Self::professional_execution_fill_ratio_budget_default();
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.seed_trials = professional_execution_rolling_carry_budget_seed_trials();
        config
    }

    pub fn professional_execution_capacity_fill_frontier_default() -> Self {
        let mut config = Self::professional_execution_rolling_carry_budget_default();
        config.top_n = vec![80, 120, 160];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(80, 2), Decimal::new(90, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(150, 2),
            Decimal::new(200, 2),
            Decimal::new(250, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["fillable_gross_95_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![900, 1200];
        config.seed_trials = professional_execution_capacity_fill_frontier_seed_trials();
        config
    }

    pub fn professional_execution_alpha_capacity_bridge_default() -> Self {
        let mut config = Self::professional_return_alpha_sharpe_bridge_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.top_n = vec![80, 120];
        config.rebalance_days = vec![60, 120];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_balanced_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["fillable_gross_95_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![900, 1200];
        config.seed_trials = professional_execution_alpha_capacity_bridge_seed_trials();
        config
    }

    pub fn professional_execution_alpha_capacity_return_frontier_default() -> Self {
        let mut config = Self::professional_execution_alpha_capacity_bridge_default();
        config.top_n = vec![60, 80, 100];
        config.max_position_pct = vec![Decimal::new(7, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_balanced_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["fillable_gross_95_v1".to_string()];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_30pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![650, 900, 1200];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_alpha_capacity_return_frontier_seed_trials();
        config
    }

    pub fn professional_execution_bull_sleeve_cash_recovery_default() -> Self {
        let mut config = Self::professional_execution_alpha_capacity_return_frontier_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_state_alpha_selector_v1".to_string(),
            "quality_state_alpha_selector_v2".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_frontier_regime_bridge_router_v2".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160];
        config.max_position_pct = vec![Decimal::new(7, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(125, 2), Decimal::new(150, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_balanced_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1800, 2200];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_bull_sleeve_cash_recovery_seed_trials();
        config
    }

    pub fn professional_execution_return_first_fill_repair_default() -> Self {
        let mut config = Self::professional_execution_bull_sleeve_cash_recovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.top_n = vec![40, 60, 80, 100];
        config.rebalance_days = vec![60, 90, 120];
        config.max_position_pct =
            vec![Decimal::new(8, 2), Decimal::new(10, 2), Decimal::new(12, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.capacity_risk_budget_profiles = vec![
            "off".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles =
            vec!["off".to_string(), "impact_turnover_20pct_v1".to_string()];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(2, 2), Decimal::new(3, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(50, 2), Decimal::new(75, 2)];
        config.score_candidate_pool_sizes = vec![900, 1500, 2200];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_return_first_fill_repair_seed_trials();
        config
    }

    pub fn professional_execution_oos_regime_alpha_rebuild_default() -> Self {
        let mut config = Self::professional_execution_bull_sleeve_cash_recovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_state_alpha_selector_v1".to_string(),
            "quality_state_alpha_selector_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
        ];
        config.top_n = vec![30, 40, 60, 80, 100];
        config.rebalance_days = vec![60, 90, 120];
        config.max_position_pct = vec![
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.capacity_risk_budget_profiles = vec![
            "off".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["off".to_string(), "soft_low_volatility_v1".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.execution_impact_budget_profiles = vec!["off".to_string()];
        config.execution_schedule_profiles = vec!["immediate".to_string()];
        config.execution_carry_policy_profiles = vec!["expire".to_string()];
        config.score_candidate_pool_sizes = vec![500, 1000, 1800];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_execution_oos_regime_alpha_rebuild_seed_trials();
        config
    }

    pub fn professional_execution_oos_benchmark_excess_rebuild_default() -> Self {
        let mut config = Self::professional_execution_oos_regime_alpha_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Descending, ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_alpha_selector_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.top_n = vec![40, 60, 80, 100];
        config.rebalance_days = vec![60, 90, 120];
        config.max_position_pct = vec![
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.capacity_risk_budget_profiles = vec![
            "off".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["off".to_string(), "soft_low_volatility_v1".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.cash_utilization_profiles = vec!["off".to_string()];
        config.execution_impact_budget_profiles = vec!["off".to_string()];
        config.execution_schedule_profiles = vec!["immediate".to_string()];
        config.score_candidate_pool_sizes = vec![1000, 1800, 2200];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_26_75_100",
                Decimal::new(26, 2),
                120,
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_execution_oos_benchmark_excess_rebuild_seed_trials();
        config
    }

    pub fn professional_execution_oos_execution_adaptive_rebuild_default() -> Self {
        let mut config = Self::professional_execution_oos_benchmark_excess_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Descending, ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_alpha_selector_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.top_n = vec![80, 100];
        config.rebalance_days = vec![90, 120];
        config.max_position_pct = vec![Decimal::new(12, 2), Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2), Decimal::ONE];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["off".to_string(), "soft_low_volatility_v1".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200];
        config.universe_profiles = vec!["listed_non_st".to_string(), "all".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_26_75_100",
                Decimal::new(26, 2),
                120,
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "gentle_roll_sharpe180_025_neg20_70",
                Decimal::new(25, 2),
                Decimal::new(-20, 2),
                180,
                Decimal::new(70, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "gentle_roll_sharpe120_030_neg20_70",
                Decimal::new(30, 2),
                Decimal::new(-20, 2),
                120,
                Decimal::new(70, 2),
            ),
        ];
        config.seed_trials = professional_execution_oos_execution_adaptive_rebuild_seed_trials();
        config
    }

    pub fn professional_execution_stress_fill_return_frontier_default() -> Self {
        let mut config = Self::professional_execution_alpha_capacity_return_frontier_default();
        config.top_n = vec![80, 120, 160];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.score_candidate_pool_sizes = vec![900, 1200, 1500];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_stress_fill_return_frontier_seed_trials();
        config
    }

    pub fn professional_execution_stress_risk_budget_default() -> Self {
        let mut config = Self::professional_execution_alpha_capacity_return_frontier_default();
        config.top_n = vec![60, 80, 100];
        config.max_position_pct = vec![Decimal::new(7, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.score_candidate_pool_sizes = vec![900, 1200];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_stress_risk_budget_seed_trials();
        config
    }

    pub fn professional_execution_capacity_stress_return_gate_default() -> Self {
        let mut config = Self::professional_execution_stress_risk_budget_default();
        config.top_n = vec![80, 100, 120];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2), Decimal::new(250, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.seed_trials = professional_execution_capacity_stress_return_gate_seed_trials();
        config
    }

    pub fn professional_execution_low_impact_alpha_stress_return_default() -> Self {
        let mut config = Self::professional_execution_capacity_stress_return_gate_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(68, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles =
            vec!["twap_20d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec!["soft_single_name_15pct_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_low_impact_alpha_stress_return_seed_trials();
        config
    }

    pub fn professional_execution_stress_target_scaling_return_default() -> Self {
        let mut config = Self::professional_execution_low_impact_alpha_stress_return_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
        ];
        config.top_n = vec![100, 120, 160];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(60, 2),
            Decimal::new(70, 2),
            Decimal::new(80, 2),
            Decimal::new(90, 2),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(250, 2),
            Decimal::new(300, 2),
            Decimal::new(350, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_target_scale_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_stress_target_scaling_return_seed_trials();
        config
    }

    pub fn professional_execution_stress_floor_scaling_return_default() -> Self {
        let mut config = Self::professional_execution_stress_target_scaling_return_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
        ];
        config.top_n = vec![100, 120, 160];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(70, 2),
            Decimal::new(80, 2),
            Decimal::new(90, 2),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_floor_35_v1".to_string(),
            "capacity_stress_participation_floor_50_v1".to_string(),
            "capacity_stress_participation_target_scale_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_stress_floor_scaling_return_seed_trials();
        config
    }

    pub fn professional_execution_stress_floor_return_recovery_default() -> Self {
        let mut config = Self::professional_execution_stress_floor_scaling_return_default();
        config.top_n = vec![100, 120, 160];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(80, 2),
            Decimal::new(90, 2),
            Decimal::new(100, 2),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_floor_60_v1".to_string(),
            "capacity_stress_participation_floor_70_v1".to_string(),
            "capacity_stress_participation_soft_floor_60_v1".to_string(),
            "capacity_stress_participation_floor_50_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_stress_floor_return_recovery_seed_trials();
        config
    }

    pub fn professional_execution_pressure_headroom_floor_default() -> Self {
        let mut config = Self::professional_execution_stress_floor_return_recovery_default();
        config.top_n = vec![120, 160, 200];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(80, 2),
            Decimal::new(90, 2),
            Decimal::new(100, 2),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(250, 2),
            Decimal::new(300, 2),
            Decimal::new(350, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_floor_70_v1".to_string(),
            "capacity_stress_participation_soft_floor_60_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1500, 1800];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_pressure_headroom_floor_seed_trials();
        config
    }

    pub fn professional_execution_alpha_headroom_floor_default() -> Self {
        let mut config = Self::professional_execution_pressure_headroom_floor_default();
        config.top_n = vec![100, 120, 160];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![
            Decimal::new(80, 2),
            Decimal::new(90, 2),
            Decimal::new(100, 2),
        ];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1500, 1800];
        config.universe_profiles =
            vec!["listed_non_st".to_string(), "main_board_non_st".to_string()];
        config.seed_trials = professional_execution_alpha_headroom_floor_seed_trials();
        config
    }

    pub fn professional_execution_blended_alpha_headroom_floor_default() -> Self {
        let mut config = Self::professional_execution_alpha_headroom_floor_default();
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.seed_trials = professional_execution_blended_alpha_headroom_floor_seed_trials();
        config
    }

    pub fn professional_execution_event_anchor_stress_bridge_default() -> Self {
        let mut config = Self::professional_execution_alpha_headroom_floor_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![80, 120, 160];
        config.rebalance_days = vec![160, 180];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_15pct_v1".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2)];
        config.score_candidate_pool_sizes = vec![1500, 1800];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_execution_event_anchor_stress_bridge_seed_trials();
        config
    }

    pub fn professional_execution_participation_aware_event_anchor_default() -> Self {
        let mut config = Self::professional_execution_event_anchor_stress_bridge_default();
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200];
        config.seed_trials = professional_execution_participation_aware_event_anchor_seed_trials();
        config
    }

    pub fn professional_execution_cl_anchor_fill_recovery_default() -> Self {
        let mut config = Self::professional_execution_participation_aware_event_anchor_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.top_n = vec![100, 120, 160];
        config.rebalance_days = vec![160, 180];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(7, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![
            Decimal::new(200, 2),
            Decimal::new(250, 2),
            Decimal::new(300, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::new(3, 2), Decimal::new(4, 2)];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![2200, 2400, 2600];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_execution_cl_anchor_fill_recovery_seed_trials();
        config
    }

    pub fn professional_execution_capacity_aware_candidate_ranking_default() -> Self {
        let mut config = Self::professional_execution_cl_anchor_fill_recovery_default();
        config.top_n = vec![80, 100, 120];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.candidate_ranking_profiles = vec!["capacity_aware_alpha_liquidity_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200];
        config.seed_trials = professional_execution_capacity_aware_candidate_ranking_seed_trials();
        config
    }

    pub fn professional_execution_pit_capacity_ranking_default() -> Self {
        Self::professional_execution_capacity_aware_candidate_ranking_default()
    }

    pub fn professional_execution_pit_alpha_first_low_impact_default() -> Self {
        let mut config = Self::professional_execution_low_impact_alpha_stress_return_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![160, 180, 220];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![
            Decimal::new(150, 2),
            Decimal::new(200, 2),
            Decimal::new(250, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["alpha_first_low_impact_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_execution_pit_alpha_first_low_impact_seed_trials();
        config
    }

    pub fn professional_execution_pit_excess_return_recovery_default() -> Self {
        let mut config = Self::professional_execution_pit_alpha_first_low_impact_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![60, 80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.max_position_pct = vec![Decimal::new(7, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_blended_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
        ];
        config.candidate_ranking_profiles =
            vec!["relative_strength_alpha_liquidity_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "off".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_15pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200, 2400, 2600];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_execution_pit_excess_return_recovery_seed_trials();
        config
    }

    pub fn professional_execution_pit_nonlinear_alpha_regime_rebuild_default() -> Self {
        let mut config = Self::professional_execution_pit_excess_return_recovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![80, 100, 120, 160];
        config.rebalance_days = vec![120, 160, 180, 220];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
            Decimal::new(250, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
            "off".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200, 2400, 2600];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials =
            professional_execution_pit_nonlinear_alpha_regime_rebuild_seed_trials();
        config
    }

    pub fn professional_train_window_nonlinear_ranking_discovery_default() -> Self {
        let mut config = Self::professional_execution_pit_nonlinear_alpha_regime_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
        ];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![180, 220, 260];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec!["nonlinear_regime_alpha_liquidity_v2".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec![
            "stress_fill_gross_98_v1".to_string(),
            "fillable_gross_95_v1".to_string(),
        ];
        config.score_candidate_pool_sizes = vec![2200, 2400];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_train_window_nonlinear_ranking_discovery_seed_trials();
        config
    }

    pub fn professional_execution_pit_quality_recovery_alpha_default() -> Self {
        let mut config = Self::professional_execution_pit_nonlinear_alpha_regime_rebuild_default();
        config.combo_versions = vec![ComboVersion::new(
            "phase7_quality_recovery_acceleration_v1",
            "1.0.0",
        )];
        config.score_directions = vec![ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200, 2400];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_execution_pit_quality_recovery_alpha_seed_trials();
        config
    }

    pub fn professional_execution_event_post_return_curve_alpha_default() -> Self {
        let mut config = Self::professional_execution_pit_quality_recovery_alpha_default();
        config.combo_versions = vec![ComboVersion::new(
            "phase7_quality_event_post_return_curve_overlay_v1",
            "1.0.0",
        )];
        config.score_directions = vec![ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
        ];
        config.seed_trials = professional_execution_event_post_return_curve_alpha_seed_trials();
        config
    }

    pub fn professional_execution_event_reaction_alpha_default() -> Self {
        let mut config = Self::professional_execution_event_post_return_curve_alpha_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_quality_event_reaction_segments_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_reaction_reversal_overlay_v1", "1.0.0"),
        ];
        config.seed_trials = professional_execution_event_reaction_alpha_seed_trials();
        config
    }

    pub fn professional_execution_broad_financial_feature_discovery_default() -> Self {
        let mut config = Self::professional_execution_pit_nonlinear_alpha_regime_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_dividend_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.max_position_pct = vec![Decimal::new(6, 2), Decimal::new(8, 2), Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![
            Decimal::new(125, 2),
            Decimal::new(150, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200, 2400];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_execution_broad_financial_feature_discovery_seed_trials();
        config
    }

    pub fn professional_execution_broad_financial_feature_stratified_discovery_default() -> Self {
        let mut config = Self::professional_execution_broad_financial_feature_discovery_default();
        config.seed_trials =
            professional_execution_broad_financial_feature_stratified_discovery_seed_trials();
        config
    }

    pub fn professional_execution_native_alpha_fusion_discovery_default() -> Self {
        let mut config =
            Self::professional_execution_broad_financial_feature_stratified_discovery_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_event_surprise_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_post_return_curve_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_reaction_segments_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_reaction_reversal_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
        ];
        config.prediction_set_ids = vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()];
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.candidate_ranking_profiles = vec![
            "alpha_first_low_impact_v1".to_string(),
            "capacity_aware_alpha_liquidity_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "soft_liquidity_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
            "soft_low_volatility_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_60_v1".to_string(),
            "capacity_stress_participation_soft_cap_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_15pct_v1".to_string(),
            "impact_turnover_20pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.score_candidate_pool_sizes = vec![1800, 2200, 2400];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.seed_trials = professional_execution_native_alpha_fusion_discovery_seed_trials();
        config
    }

    pub fn professional_trainable_alpha_admission_discovery_default() -> Self {
        let mut config =
            Self::professional_execution_broad_financial_feature_stratified_discovery_default();
        config.combo_versions = phase7_base_trainable_combo_versions(&[
            "phase7_financial_quality_v1",
            "phase7_financial_quality_change_v1",
            "phase7_earnings_recovery_persistence_v1",
            "phase7_industry_residual_quality_v1",
            "phase7_growth_recovery_v1",
            "phase7_quality_relative_strength_v1",
            "phase7_valuation_v1",
            "phase7_moneyflow_v1",
            "phase7_moneyflow_congestion_interaction_v1",
            "phase7_quality_moneyflow_pos_5pct_v1",
            "phase7_quality_cashflow_confirm_v1",
            "phase7_quality_dividend_confirm_v1",
            "phase7_quality_cashflow_dividend_confirm_v1",
            "phase7_quality_value_recovery_confirm_v1",
            "phase7_quality_residual_confirm_5pct_v1",
            "phase7_quality_residual_confirm_10pct_v1",
            "phase7_blend_quality_growth_v1",
            "phase7_blend_recovery_tilt_v1",
            "phase7_event_surprise_v1",
            "phase7_quality_event_post_return_curve_overlay_v1",
            "phase7_quality_event_reaction_segments_overlay_v1",
        ]);
        config.prediction_set_ids = Vec::new();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_trainable_alpha_admission_discovery_seed_trials();
        config
    }

    pub fn professional_v19_multi_alpha_sleeve_admission_default() -> Self {
        let mut config = Self::professional_trainable_alpha_admission_discovery_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.capacity_risk_budget_profiles =
            vec!["capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_v19_multi_alpha_sleeve_admission_seed_trials();
        config
    }

    pub fn professional_v19_event_surprise_sleeve_gate_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_event_surprise_sleeve_gate_seed_trials();
        config
    }

    pub fn professional_v19_supply_float_sleeve_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_supply_float_sleeve_seed_trials();
        config
    }

    pub fn professional_v19_unlock_pressure_sleeve_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_unlock_pressure_sleeve_seed_trials();
        config
    }

    pub fn professional_v19_forecast_revision_sleeve_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_forecast_revision_sleeve_seed_trials();
        config
    }

    pub fn professional_v19_shareholder_structure_sleeve_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = Vec::new();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.universe_profiles = vec!["main_chinext_non_st".to_string()];
        config.seed_trials = professional_v19_shareholder_structure_sleeve_seed_trials();
        config
    }

    pub fn professional_v19_event_post_return_overlay_admission_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.combo_versions = vec![ComboVersion::new(
            "phase7_quality_event_post_return_curve_overlay_v1",
            "1.0.0",
        )];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Descending];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![120, 160, 180];
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "event_post_return_boost_pos_5pct",
                "phase7_event_post_return_curve_20d_v1",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_post_return_exclude_negative",
                "phase7_event_post_return_curve_20d_v1",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.capacity_risk_budget_profiles =
            vec!["capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()];
        config.cash_utilization_profiles = vec!["stress_fill_gross_98_v1".to_string()];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_v19_event_post_return_overlay_admission_seed_trials();
        config
    }

    pub fn professional_v19_execution_repair_admission_default() -> Self {
        let mut config = Self::professional_v19_multi_alpha_sleeve_admission_default();
        config.top_n = vec![80, 100, 120];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_gross_exposure = vec![Decimal::new(95, 2), Decimal::ONE];
        config.capacity_penalty_strength = vec![Decimal::ONE, Decimal::new(125, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_balanced_v1".to_string(),
            "capacity_participation_strict_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec![
            "impact_turnover_20pct_v1".to_string(),
            "impact_turnover_30pct_v1".to_string(),
        ];
        config.execution_schedule_profiles =
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.candidate_risk_filter_profiles =
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![900, 1200, 1500];
        config.seed_trials = professional_v19_execution_repair_admission_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_alpha_rebuild_default() -> Self {
        let mut config = Self::professional_v19_execution_repair_admission_default();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_train_window_ml_alpha_rebuild_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_simple_excess_rebuild_default() -> Self {
        let mut config = Self::professional_v19_execution_repair_admission_default();
        config.prediction_set_ids = Vec::new();
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_v19_train_window_ml_simple_excess_rebuild_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_simple_excess_low_impact_rebuild_default() -> Self {
        let mut config = Self::professional_v19_train_window_ml_simple_excess_rebuild_default();
        config.combo_versions = vec![ComboVersion::new("phase7_growth_recovery_v1", "1.0.0")];
        config.top_n = vec![80, 100];
        config.rebalance_days = vec![120, 160, 180];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(150, 2), Decimal::new(200, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
            "capacity_participation_balanced_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles =
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![1200, 1500, 1800];
        config.seed_trials =
            professional_v19_train_window_ml_simple_excess_low_impact_rebuild_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_h120_low_impact_rebuild_default() -> Self {
        let mut config = Self::professional_v19_train_window_ml_simple_excess_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_valuation_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![240, 300, 360];
        config.max_position_pct = vec![Decimal::new(5, 2), Decimal::new(6, 2)];
        config.max_gross_exposure = vec![Decimal::new(90, 2), Decimal::new(95, 2)];
        config.capacity_penalty_strength = vec![
            Decimal::new(150, 2),
            Decimal::new(175, 2),
            Decimal::new(200, 2),
        ];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2), Decimal::new(35, 2)];
        config.score_candidate_pool_sizes = vec![2200, 2400, 2600];
        config.seed_trials = professional_v19_train_window_ml_h120_low_impact_rebuild_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_default() -> Self {
        let mut config = Self::professional_v19_train_window_ml_simple_excess_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_cashflow_dividend_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
        ];
        config.top_n = vec![80, 100, 120];
        config.rebalance_days = vec![240, 300, 360];
        config.max_position_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2)];
        config.max_gross_exposure = vec![Decimal::new(85, 2), Decimal::new(90, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2), Decimal::new(225, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2)];
        config.score_candidate_pool_sizes = vec![2600, 2800, 3000];
        config.seed_trials =
            professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_seed_trials();
        config
    }

    pub fn professional_v19_train_window_ml_event_sentiment_rebuild_default() -> Self {
        let mut config = Self::professional_v19_train_window_ml_simple_excess_rebuild_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
        ];
        config.top_n = vec![100, 120];
        config.rebalance_days = vec![240, 300];
        config.max_position_pct = vec![Decimal::new(4, 2), Decimal::new(5, 2)];
        config.max_gross_exposure = vec![Decimal::new(85, 2), Decimal::new(90, 2)];
        config.capacity_penalty_strength = vec![Decimal::new(200, 2), Decimal::new(225, 2)];
        config.capacity_risk_budget_profiles = vec![
            "capacity_participation_strict_v1".to_string(),
            "capacity_stress_participation_alpha_headroom_floor_70_v1".to_string(),
        ];
        config.cash_utilization_profiles = vec![
            "fillable_gross_95_v1".to_string(),
            "stress_fill_gross_98_v1".to_string(),
        ];
        config.execution_impact_budget_profiles = vec!["impact_turnover_15pct_v1".to_string()];
        config.execution_schedule_profiles = vec!["twap_20d_v1".to_string()];
        config.execution_carry_policy_profiles = vec!["roll_forward_v1".to_string()];
        config.partial_rebalance_ratio = vec![Decimal::new(25, 2)];
        config.score_candidate_pool_sizes = vec![2800, 3000];
        config.seed_trials = professional_v19_train_window_ml_event_sentiment_rebuild_seed_trials();
        config
    }

    pub fn professional_return_alpha_sharpe_bridge_default() -> Self {
        let mut config = Self::professional_v14_ultra_micro_lift_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_return_alpha_sharpe_bridge_seed_trials();
        config
    }

    pub fn professional_regime_frontier_bridge_default() -> Self {
        let mut config = Self::professional_return_alpha_sharpe_bridge_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v1".to_string(),
            "quality_frontier_regime_bridge_router_v2".to_string(),
            "quality_frontier_regime_bridge_router_v3".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_regime_frontier_bridge_seed_trials();
        config
    }

    pub fn professional_regime_frontier_decomposition_default() -> Self {
        let mut config = Self::professional_regime_frontier_bridge_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v4".to_string(),
            "quality_frontier_regime_bridge_router_v5".to_string(),
            "quality_frontier_regime_bridge_router_v6".to_string(),
            "quality_frontier_regime_bridge_router_v7".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_regime_frontier_decomposition_seed_trials();
        config
    }

    pub fn professional_high_sharpe_return_micro_bridge_default() -> Self {
        let mut config = Self::professional_regime_frontier_decomposition_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v6".to_string(),
            "quality_frontier_regime_bridge_router_v7".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_147_475_100",
                Decimal::new(147, 3),
                120,
                Decimal::new(475, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_70",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(70, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_72",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(72, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_high_sharpe_return_micro_bridge_seed_trials();
        config
    }

    pub fn professional_return_distribution_repair_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_10d_boost_p75_3pct",
                "phase7_event_window_earnings_10d_v1",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_20d_boost_p75_3pct",
                "phase7_event_window_earnings_v1",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_40d_exclude_negative",
                "phase7_event_window_earnings_40d_v1",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "residual_confirm_top40",
                "phase7_quality_residual_confirm_10pct_v1",
                "require_positive",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_return_distribution_repair_seed_trials();
        config
    }

    pub fn professional_state_alpha_router_default() -> Self {
        let mut config = Self::professional_current_anchor_weak_window_repair_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
            "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_state_alpha_router_seed_trials();
        config
    }

    pub fn professional_state_position_risk_router_default() -> Self {
        let mut config = Self::professional_state_alpha_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_bear_position_guard_v3".to_string(),
            "quality_bear_position_guard_v1".to_string(),
            "quality_bear_position_guard_v2".to_string(),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_state_position_risk_router_seed_trials();
        config
    }

    pub fn professional_event_position_risk_router_default() -> Self {
        let mut config = Self::professional_state_position_risk_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_event_window_position_guard_v3".to_string(),
            "quality_event_window_position_guard_v1".to_string(),
            "quality_event_window_position_guard_v2".to_string(),
        ];
        config.seed_trials = professional_event_position_risk_router_seed_trials();
        config
    }

    pub fn professional_all_regime_event_sleeve_default() -> Self {
        let mut config = Self::professional_event_position_risk_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_all_regime_event_sleeve_seed_trials();
        config
    }

    pub fn professional_portfolio_sharpe_control_default() -> Self {
        let mut config = Self::professional_all_regime_event_sleeve_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(14, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(70, 2)];
        config.seed_trials = professional_portfolio_sharpe_control_seed_trials();
        config
    }

    pub fn professional_volatility_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol252_16_45_100",
                Decimal::new(16, 2),
                252,
                Decimal::new(45, 2),
                Decimal::ONE,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_volatility_sharpe_seed_trials();
        config
    }

    pub fn professional_regime_position_sharpe_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_bear_position_guard_v1".to_string(),
            "quality_bear_position_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
            "quality_risk_off_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_position_sharpe_seed_trials();
        config
    }

    pub fn professional_anti_overfit_sharpe_default() -> Self {
        let mut config = Self::professional_bear_window_stabilization_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
        ];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.risk_budget_lookback_days = vec![120];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2), Decimal::ONE];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_24_45_30_70",
                Decimal::new(9, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_24_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_anti_overfit_sharpe_seed_trials();
        config
    }

    pub fn professional_candidate_risk_filter_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.seed_trials = professional_candidate_risk_filter_seed_trials();
        config
    }

    pub fn professional_risk_contribution_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.risk_contribution_control_profiles = vec![
            "off".to_string(),
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_risk_contribution_seed_trials();
        config
    }

    pub fn professional_event_conditioned_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_3pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_positive",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_boost_pos_3pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.seed_trials = professional_event_conditioned_sharpe_seed_trials();
        config
    }

    pub fn search_space_size(&self) -> usize {
        [
            self.market_regime_policies.len(),
            self.signal_candidate_count(),
            self.top_n.len(),
            self.rebalance_days.len(),
            self.score_directions.len(),
            self.skip_top_pct.len(),
            self.max_pairwise_correlation.len(),
            self.kelly_fraction.len(),
            self.max_position_pct.len(),
            self.max_gross_exposure.len(),
            self.portfolio_methods.len(),
            self.risk_budget_lookback_days.len(),
            self.capacity_penalty_strength.len(),
            self.capacity_risk_budget_profiles.len(),
            self.cash_utilization_profiles.len(),
            self.execution_impact_budget_profiles.len(),
            self.execution_schedule_profiles.len(),
            self.execution_carry_policy_profiles.len(),
            self.cost_capacity_stress_profiles.len(),
            self.industry_max_weight_pct.len(),
            self.style_risk_budget_profiles.len(),
            self.candidate_risk_filter_profiles.len(),
            self.candidate_ranking_profiles.len(),
            self.risk_contribution_control_profiles.len(),
            self.stress_fill_confidence_exposure_profiles.len(),
            self.rebalance_hysteresis_pct.len(),
            self.partial_rebalance_ratio.len(),
            self.score_candidate_pool_sizes.len(),
            self.universe_profiles.len(),
            self.portfolio_drawdown_controls.len(),
            self.portfolio_volatility_controls.len(),
            self.portfolio_sharpe_controls.len(),
            self.position_risk_controls.len(),
            self.event_gate_profiles.len(),
        ]
        .into_iter()
        .fold(1usize, |total, size| total.saturating_mul(size))
    }

    fn signal_candidate_count(&self) -> usize {
        self.combo_versions
            .len()
            .saturating_add(self.prediction_set_ids.len())
    }
}

#[derive(Debug, Clone)]
enum LayeredSignalCandidate<'a> {
    FactorCombo(&'a ComboVersion),
    ModelPrediction(&'a str),
}

impl LayeredSearchConfig {
    fn signal_candidate(&self, index: usize) -> Option<LayeredSignalCandidate<'_>> {
        if index < self.combo_versions.len() {
            self.combo_versions
                .get(index)
                .map(LayeredSignalCandidate::FactorCombo)
        } else {
            self.prediction_set_ids
                .get(index.saturating_sub(self.combo_versions.len()))
                .map(|value| LayeredSignalCandidate::ModelPrediction(value.as_str()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchTrial {
    pub trial_id: String,
    pub trial_index: usize,
    pub batch_index: usize,
    pub parameters: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchPlan {
    pub requested_trials: usize,
    pub planned_trials: usize,
    pub truncated: bool,
    pub batch_size: usize,
    pub max_parallel_trials: usize,
    pub trials: Vec<LayeredSearchTrial>,
}

pub fn build_layered_search_plan(
    config: &LayeredSearchConfig,
    resource_plan: &LocalResourcePlan,
) -> LayeredSearchPlan {
    let cartesian_trials = config.search_space_size();
    let requested_trials = cartesian_trials.saturating_add(config.seed_trials.len());
    let max_trials = resource_plan.max_trials;
    let batch_size = resource_plan.batch_size.max(1);
    let mut trials = Vec::with_capacity(requested_trials.min(max_trials));

    for seed in config.seed_trials.iter().take(max_trials) {
        let trial_index = trials.len();
        trials.push(LayeredSearchTrial {
            trial_id: format!("phase7d-{:06}", trial_index + 1),
            trial_index,
            batch_index: trial_index / batch_size + 1,
            parameters: seed.clone(),
        });
    }

    let remaining_trials = max_trials.saturating_sub(trials.len());
    for source_index in selected_cartesian_indices(cartesian_trials, remaining_trials) {
        let Some(indices) = LayeredTrialIndices::from_flat_index(config, source_index) else {
            continue;
        };

        let market_regime_policy = &config.market_regime_policies[indices.market_regime_policy];
        let signal_candidate = config.signal_candidate(indices.signal_candidate);
        let top_n = config.top_n[indices.top_n];
        let rebalance = config.rebalance_days[indices.rebalance_days];
        let direction = config.score_directions[indices.score_direction];
        let skip_top_pct = config.skip_top_pct[indices.skip_top_pct];
        let max_pairwise_correlation =
            config.max_pairwise_correlation[indices.max_pairwise_correlation];
        let kelly_fraction = config.kelly_fraction[indices.kelly_fraction];
        let max_position_pct = config.max_position_pct[indices.max_position_pct];
        let max_gross_exposure = config.max_gross_exposure[indices.max_gross_exposure];
        let portfolio_method = &config.portfolio_methods[indices.portfolio_method];
        let risk_budget_lookback_days =
            config.risk_budget_lookback_days[indices.risk_budget_lookback_days];
        let capacity_penalty_strength =
            config.capacity_penalty_strength[indices.capacity_penalty_strength];
        let capacity_risk_budget_profile =
            &config.capacity_risk_budget_profiles[indices.capacity_risk_budget_profile];
        let cash_utilization_profile =
            &config.cash_utilization_profiles[indices.cash_utilization_profile];
        let execution_impact_budget_profile =
            &config.execution_impact_budget_profiles[indices.execution_impact_budget_profile];
        let execution_schedule_profile =
            &config.execution_schedule_profiles[indices.execution_schedule_profile];
        let execution_carry_policy_profile =
            &config.execution_carry_policy_profiles[indices.execution_carry_policy_profile];
        let cost_capacity_stress_profile =
            &config.cost_capacity_stress_profiles[indices.cost_capacity_stress_profile];
        let industry_max_weight_pct =
            config.industry_max_weight_pct[indices.industry_max_weight_pct];
        let style_risk_budget_profile =
            &config.style_risk_budget_profiles[indices.style_risk_budget_profile];
        let candidate_risk_filter_profile =
            &config.candidate_risk_filter_profiles[indices.candidate_risk_filter_profile];
        let candidate_ranking_profile =
            &config.candidate_ranking_profiles[indices.candidate_ranking_profile];
        let risk_contribution_control_profile =
            &config.risk_contribution_control_profiles[indices.risk_contribution_control_profile];
        let stress_fill_confidence_exposure_profile = &config
            .stress_fill_confidence_exposure_profiles
            [indices.stress_fill_confidence_exposure_profile];
        let rebalance_hysteresis_pct =
            config.rebalance_hysteresis_pct[indices.rebalance_hysteresis_pct];
        let partial_rebalance_ratio =
            config.partial_rebalance_ratio[indices.partial_rebalance_ratio];
        let score_candidate_pool_size =
            config.score_candidate_pool_sizes[indices.score_candidate_pool_size];
        let universe_profile = &config.universe_profiles[indices.universe_profile];
        let portfolio_drawdown_control =
            &config.portfolio_drawdown_controls[indices.portfolio_drawdown_control];
        let portfolio_volatility_control =
            &config.portfolio_volatility_controls[indices.portfolio_volatility_control];
        let portfolio_sharpe_control =
            &config.portfolio_sharpe_controls[indices.portfolio_sharpe_control];
        let position_risk_control = &config.position_risk_controls[indices.position_risk_control];
        let event_gate_profile = &config.event_gate_profiles[indices.event_gate_profile];

        let trial_index = trials.len();
        let mut parameters = serde_json::json!({
            "market_regime": market_regime_policy,
            "top_n": top_n,
            "rebalance": rebalance.to_string(),
            "score_direction": direction.as_str(),
            "skip_top_pct": decimal_string(skip_top_pct),
            "max_pairwise_correlation": decimal_string(max_pairwise_correlation),
            "correlation_lookback_days": config.correlation_lookback_days,
            "kelly_fraction": decimal_string(kelly_fraction),
            "kelly_lookback_days": config.kelly_lookback_days,
            "max_position_pct": decimal_string(max_position_pct),
            "max_gross_exposure": decimal_string(max_gross_exposure),
            "portfolio_method": portfolio_method,
            "risk_budget_lookback_days": risk_budget_lookback_days,
            "capacity_penalty_strength": decimal_string(capacity_penalty_strength),
            "capacity_risk_budget": capacity_risk_budget_profile,
            "cash_utilization": cash_utilization_profile,
            "execution_impact_budget": execution_impact_budget_profile,
            "execution_schedule_profile": execution_schedule_profile,
            "cost_capacity_stress_profile": cost_capacity_stress_profile.profile_name,
            "industry_max_weight_pct": industry_max_weight_pct.map(decimal_string),
            "style_risk_budget": style_risk_budget_profile,
            "candidate_risk_filter": candidate_risk_filter_profile,
            "candidate_ranking": candidate_ranking_profile,
            "risk_contribution_control": risk_contribution_control_profile,
            "stress_fill_confidence_exposure": stress_fill_confidence_exposure_profile,
            "rebalance_hysteresis_pct": decimal_string(rebalance_hysteresis_pct),
            "partial_rebalance_ratio": decimal_string(partial_rebalance_ratio),
            "score_candidate_pool_size": score_candidate_pool_size,
            "universe_profile": universe_profile,
            "portfolio_drawdown_control": portfolio_drawdown_control.profile_name,
            "portfolio_volatility_control": portfolio_volatility_control.profile_name,
            "portfolio_sharpe_control": portfolio_sharpe_control.profile_name,
            "position_risk_control": position_risk_control.profile_name,
            "event_gate_profile": event_gate_profile.profile_name,
            "benchmark": config.benchmark,
        });
        apply_cost_capacity_stress_profile(&mut parameters, cost_capacity_stress_profile);
        apply_execution_schedule_profile(&mut parameters, execution_schedule_profile);
        apply_execution_carry_policy_profile(&mut parameters, execution_carry_policy_profile);
        if let (Some(start), Some(full), Some(min_exposure)) = (
            portfolio_drawdown_control.reduce_start_pct,
            portfolio_drawdown_control.reduce_full_pct,
            portfolio_drawdown_control.min_exposure,
        ) {
            parameters["portfolio_drawdown_reduce_start_pct"] =
                serde_json::json!(decimal_string(start));
            parameters["portfolio_drawdown_reduce_full_pct"] =
                serde_json::json!(decimal_string(full));
            parameters["portfolio_drawdown_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(lookback_days) = portfolio_drawdown_control.peak_lookback_days {
            parameters["portfolio_drawdown_peak_lookback_days"] = serde_json::json!(lookback_days);
        }
        if let (Some(start), Some(full)) = (
            portfolio_drawdown_control.recovery_start_pct,
            portfolio_drawdown_control.recovery_full_pct,
        ) {
            parameters["portfolio_drawdown_recovery_start_pct"] =
                serde_json::json!(decimal_string(start));
            parameters["portfolio_drawdown_recovery_full_pct"] =
                serde_json::json!(decimal_string(full));
        }
        if let Some(boost) = portfolio_drawdown_control.recovery_boost {
            parameters["portfolio_drawdown_recovery_boost"] =
                serde_json::json!(decimal_string(boost));
        }
        if let Some(target_pct) = portfolio_volatility_control.target_pct {
            parameters["portfolio_volatility_target_pct"] =
                serde_json::json!(decimal_string(target_pct));
        }
        if let Some(lookback_days) = portfolio_volatility_control.lookback_days {
            parameters["portfolio_volatility_lookback_days"] = serde_json::json!(lookback_days);
        }
        if let Some(min_exposure) = portfolio_volatility_control.min_exposure {
            parameters["portfolio_volatility_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(max_exposure) = portfolio_volatility_control.max_exposure {
            parameters["portfolio_volatility_max_exposure"] =
                serde_json::json!(decimal_string(max_exposure));
        }
        if let (Some(start), Some(full), Some(min_exposure)) = (
            portfolio_sharpe_control.reduce_start,
            portfolio_sharpe_control.reduce_full,
            portfolio_sharpe_control.min_exposure,
        ) {
            parameters["portfolio_sharpe_reduce_start"] = serde_json::json!(decimal_string(start));
            parameters["portfolio_sharpe_reduce_full"] = serde_json::json!(decimal_string(full));
            parameters["portfolio_sharpe_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(lookback_days) = portfolio_sharpe_control.lookback_days {
            parameters["portfolio_sharpe_lookback_days"] = serde_json::json!(lookback_days);
        }
        if let Some(stop_loss_pct) = position_risk_control.stop_loss_pct {
            parameters["stop_loss_pct"] = serde_json::json!(decimal_string(stop_loss_pct));
        }
        if let Some(take_profit_pct) = position_risk_control.take_profit_pct {
            parameters["take_profit_pct"] = serde_json::json!(decimal_string(take_profit_pct));
        }
        if let Some(trailing_stop_pct) = position_risk_control.trailing_stop_pct {
            parameters["trailing_stop_pct"] = serde_json::json!(decimal_string(trailing_stop_pct));
        }
        if let Some(time_stop_days) = position_risk_control.time_stop_days {
            parameters["time_stop_days"] = serde_json::json!(time_stop_days);
        }
        if let Some(reentry_cooldown_days) = position_risk_control.reentry_cooldown_days {
            parameters["reentry_cooldown_days"] = serde_json::json!(reentry_cooldown_days);
        }
        if let (Some(combo_name), Some(mode)) = (
            event_gate_profile.combo_name.as_ref(),
            event_gate_profile.mode.as_ref(),
        ) {
            parameters["event_gate_combo_name"] = serde_json::json!(combo_name);
            parameters["event_gate_version"] = serde_json::json!(event_gate_profile
                .version
                .clone()
                .unwrap_or_else(|| "1.0.0".to_string()));
            parameters["event_gate_mode"] = serde_json::json!(mode);
            if let Some(min_score) = event_gate_profile.min_score {
                parameters["event_gate_min_score"] = serde_json::json!(decimal_string(min_score));
            }
            if let Some(boost_weight) = event_gate_profile.boost_weight {
                parameters["event_gate_boost_weight"] =
                    serde_json::json!(decimal_string(boost_weight));
            }
            if let Some(score_direction) = event_gate_profile.score_direction {
                parameters["event_gate_score_direction"] =
                    serde_json::json!(score_direction.as_str());
            }
            if !event_gate_profile.active_regimes.is_empty() {
                parameters["event_gate_active_regimes"] =
                    serde_json::json!(event_gate_profile.active_regimes.clone());
            }
        }
        match signal_candidate {
            Some(LayeredSignalCandidate::FactorCombo(combo)) => {
                parameters["signal_source"] = serde_json::json!("factor_combo");
                parameters["combo_name"] = serde_json::json!(combo.combo_name.clone());
                parameters["version"] = serde_json::json!(combo.version.clone());
            }
            Some(LayeredSignalCandidate::ModelPrediction(prediction_set_id)) => {
                parameters["signal_source"] = serde_json::json!("model_prediction");
                parameters["prediction_set_id"] = serde_json::json!(prediction_set_id);
            }
            None => continue,
        }

        trials.push(LayeredSearchTrial {
            trial_id: format!("phase7d-{:06}", trial_index + 1),
            trial_index,
            batch_index: trial_index / batch_size + 1,
            parameters,
        });
    }

    LayeredSearchPlan {
        requested_trials,
        planned_trials: trials.len(),
        truncated: requested_trials > trials.len(),
        batch_size,
        max_parallel_trials: resource_plan.max_parallel_trials,
        trials,
    }
}

fn apply_cost_capacity_stress_profile(parameters: &mut Value, profile: &CostCapacityStressProfile) {
    let mut cost_model = serde_json::Map::new();
    insert_decimal_if_some(&mut cost_model, "cost_multiplier", profile.cost_multiplier);
    insert_decimal_if_some(&mut cost_model, "slippage_bps", profile.slippage_bps);
    insert_decimal_if_some(
        &mut cost_model,
        "impact_cost_coefficient",
        profile.impact_cost_coefficient,
    );
    if !cost_model.is_empty() {
        parameters["cost_model"] = Value::Object(cost_model);
    }

    if let Some(max_participation_rate) = profile.max_participation_rate {
        insert_execution_rule_value(
            parameters,
            "max_participation_rate",
            json!(decimal_f64(max_participation_rate)),
        );
    }
}

fn apply_execution_schedule_profile(parameters: &mut Value, execution_schedule_profile: &str) {
    if execution_schedule_profile.trim().is_empty() || execution_schedule_profile == "immediate" {
        return;
    }
    insert_execution_rule_value(
        parameters,
        "execution_schedule_profile",
        json!(execution_schedule_profile),
    );
}

fn apply_execution_carry_policy_profile(parameters: &mut Value, execution_carry_policy: &str) {
    let policy = execution_carry_policy.trim();
    if policy.is_empty() || policy == "expire" || policy == "off" || policy == "default" {
        return;
    }
    parameters["execution_carry_policy"] = json!(policy);
    insert_execution_rule_value(parameters, "execution_carry_policy", json!(policy));
}

pub(crate) fn insert_execution_rule_value(parameters: &mut Value, key: &str, value: Value) {
    let Some(parameters) = parameters.as_object_mut() else {
        return;
    };
    let execution_rules = parameters
        .entry("execution_rules".to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !execution_rules.is_object() {
        *execution_rules = Value::Object(serde_json::Map::new());
    }
    if let Value::Object(execution_rules) = execution_rules {
        execution_rules.insert(key.to_string(), value);
    }
}

fn insert_decimal_if_some(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<Decimal>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), json!(decimal_f64(value)));
    }
}

pub(crate) fn decimal_f64(value: Decimal) -> f64 {
    value
        .to_f64()
        .expect("phase7 decimal search parameter should fit f64")
}

#[derive(Debug, Clone, Copy)]
struct LayeredTrialIndices {
    market_regime_policy: usize,
    signal_candidate: usize,
    top_n: usize,
    rebalance_days: usize,
    score_direction: usize,
    skip_top_pct: usize,
    max_pairwise_correlation: usize,
    kelly_fraction: usize,
    max_position_pct: usize,
    max_gross_exposure: usize,
    portfolio_method: usize,
    risk_budget_lookback_days: usize,
    capacity_penalty_strength: usize,
    capacity_risk_budget_profile: usize,
    cash_utilization_profile: usize,
    execution_impact_budget_profile: usize,
    execution_schedule_profile: usize,
    execution_carry_policy_profile: usize,
    cost_capacity_stress_profile: usize,
    industry_max_weight_pct: usize,
    style_risk_budget_profile: usize,
    candidate_risk_filter_profile: usize,
    candidate_ranking_profile: usize,
    risk_contribution_control_profile: usize,
    stress_fill_confidence_exposure_profile: usize,
    rebalance_hysteresis_pct: usize,
    partial_rebalance_ratio: usize,
    score_candidate_pool_size: usize,
    universe_profile: usize,
    portfolio_drawdown_control: usize,
    portfolio_volatility_control: usize,
    portfolio_sharpe_control: usize,
    position_risk_control: usize,
    event_gate_profile: usize,
}

impl LayeredTrialIndices {
    fn from_flat_index(config: &LayeredSearchConfig, mut index: usize) -> Option<Self> {
        let market_regime_policy =
            take_axis_index(&mut index, config.market_regime_policies.len())?;
        let capacity_penalty_strength =
            take_axis_index(&mut index, config.capacity_penalty_strength.len())?;
        let capacity_risk_budget_profile =
            take_axis_index(&mut index, config.capacity_risk_budget_profiles.len())?;
        let cash_utilization_profile =
            take_axis_index(&mut index, config.cash_utilization_profiles.len())?;
        let execution_impact_budget_profile =
            take_axis_index(&mut index, config.execution_impact_budget_profiles.len())?;
        let execution_schedule_profile =
            take_axis_index(&mut index, config.execution_schedule_profiles.len())?;
        let execution_carry_policy_profile =
            take_axis_index(&mut index, config.execution_carry_policy_profiles.len())?;
        let cost_capacity_stress_profile =
            take_axis_index(&mut index, config.cost_capacity_stress_profiles.len())?;
        let industry_max_weight_pct =
            take_axis_index(&mut index, config.industry_max_weight_pct.len())?;
        let style_risk_budget_profile =
            take_axis_index(&mut index, config.style_risk_budget_profiles.len())?;
        let candidate_risk_filter_profile =
            take_axis_index(&mut index, config.candidate_risk_filter_profiles.len())?;
        let candidate_ranking_profile =
            take_axis_index(&mut index, config.candidate_ranking_profiles.len())?;
        let risk_contribution_control_profile =
            take_axis_index(&mut index, config.risk_contribution_control_profiles.len())?;
        let stress_fill_confidence_exposure_profile = take_axis_index(
            &mut index,
            config.stress_fill_confidence_exposure_profiles.len(),
        )?;
        let rebalance_hysteresis_pct =
            take_axis_index(&mut index, config.rebalance_hysteresis_pct.len())?;
        let partial_rebalance_ratio =
            take_axis_index(&mut index, config.partial_rebalance_ratio.len())?;
        let score_candidate_pool_size =
            take_axis_index(&mut index, config.score_candidate_pool_sizes.len())?;
        let universe_profile = take_axis_index(&mut index, config.universe_profiles.len())?;
        let portfolio_drawdown_control =
            take_axis_index(&mut index, config.portfolio_drawdown_controls.len())?;
        let portfolio_volatility_control =
            take_axis_index(&mut index, config.portfolio_volatility_controls.len())?;
        let portfolio_sharpe_control =
            take_axis_index(&mut index, config.portfolio_sharpe_controls.len())?;
        let position_risk_control =
            take_axis_index(&mut index, config.position_risk_controls.len())?;
        let event_gate_profile = take_axis_index(&mut index, config.event_gate_profiles.len())?;
        let risk_budget_lookback_days =
            take_axis_index(&mut index, config.risk_budget_lookback_days.len())?;
        let portfolio_method = take_axis_index(&mut index, config.portfolio_methods.len())?;
        let max_gross_exposure = take_axis_index(&mut index, config.max_gross_exposure.len())?;
        let max_position_pct = take_axis_index(&mut index, config.max_position_pct.len())?;
        let kelly_fraction = take_axis_index(&mut index, config.kelly_fraction.len())?;
        let max_pairwise_correlation =
            take_axis_index(&mut index, config.max_pairwise_correlation.len())?;
        let skip_top_pct = take_axis_index(&mut index, config.skip_top_pct.len())?;
        let score_direction = take_axis_index(&mut index, config.score_directions.len())?;
        let rebalance_days = take_axis_index(&mut index, config.rebalance_days.len())?;
        let top_n = take_axis_index(&mut index, config.top_n.len())?;
        let signal_candidate = take_axis_index(&mut index, config.signal_candidate_count())?;

        Some(Self {
            market_regime_policy,
            signal_candidate,
            top_n,
            rebalance_days,
            score_direction,
            skip_top_pct,
            max_pairwise_correlation,
            kelly_fraction,
            max_position_pct,
            max_gross_exposure,
            portfolio_method,
            risk_budget_lookback_days,
            capacity_penalty_strength,
            capacity_risk_budget_profile,
            cash_utilization_profile,
            execution_impact_budget_profile,
            execution_schedule_profile,
            execution_carry_policy_profile,
            cost_capacity_stress_profile,
            industry_max_weight_pct,
            style_risk_budget_profile,
            candidate_risk_filter_profile,
            candidate_ranking_profile,
            risk_contribution_control_profile,
            stress_fill_confidence_exposure_profile,
            rebalance_hysteresis_pct,
            partial_rebalance_ratio,
            score_candidate_pool_size,
            universe_profile,
            portfolio_drawdown_control,
            portfolio_volatility_control,
            portfolio_sharpe_control,
            position_risk_control,
            event_gate_profile,
        })
    }
}

fn take_axis_index(index: &mut usize, axis_len: usize) -> Option<usize> {
    if axis_len == 0 {
        return None;
    }
    let axis_index = *index % axis_len;
    *index /= axis_len;
    Some(axis_index)
}

fn selected_cartesian_indices(total: usize, max_trials: usize) -> Vec<usize> {
    if total == 0 || max_trials == 0 {
        return Vec::new();
    }
    if max_trials >= total {
        return (0..total).collect();
    }
    if max_trials == 1 {
        return vec![0];
    }

    let last = total - 1;
    (0..max_trials)
        .map(|idx| (idx * last + (max_trials - 1) / 2) / (max_trials - 1))
        .collect()
}

