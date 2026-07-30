//! BC4 策略发现 / strategy_discovery 领域内核：phase7 搜索规划与 alpha 准入。
//!
//! 由 quant-common 共享内核迁入（DDD 重构 Step 1），仅 quant-api 引用。
//! R8 批次1 重命名 phase7 -> strategy_discovery，过渡法拆出 3 个叶子子模块：
//! - profiles：搜索空间配置类型层（11 类型 + ScoreDirection）
//! - alpha_admission：alpha 源准入分级（4 函数）
//! - candidate_screening：候选筛选门禁
//!
//! mod.rs 主体保留 LayeredSearchConfig / seeds / tests 原位不动（后续批次拆分）。

mod profiles;
mod alpha_admission;
mod candidate_screening;
mod seed_generators;

pub use profiles::{
    LocalResourcePlan, ComboVersion, Phase7AlphaSourceRole, Phase7AlphaSourceAdmission,
    AlphaBlendSource, AlphaBlendProfile, PortfolioDrawdownControlProfile,
    PortfolioVolatilityControlProfile, PortfolioSharpeControlProfile,
    PositionRiskControlProfile, CostCapacityStressProfile, EventGateProfile, ScoreDirection,
};
pub use alpha_admission::{
    phase7_alpha_source_admission, is_phase7_base_trainable_alpha,
    phase7_alpha_blend_profiles,
};
pub use candidate_screening::{
    CandidateTargets, CandidateType, CandidateMetrics, CandidateScreeningRow,
    screen_optimization_results,
};

// 主体剩余代码原位保留：复用子模块的 helper。
use alpha_admission::phase7_base_trainable_combo_versions;
use candidate_screening::decimal_string;
use seed_generators::*;

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


#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use serde_json::json;

    #[test]
    fn local_mac_resource_plan_reserves_headroom() {
        let plan = LocalResourcePlan::for_machine(10, 32);

        assert_eq!(plan.profile, "local_mac");
        assert_eq!(plan.cpu_count, 10);
        assert_eq!(plan.memory_gb, 32);
        assert_eq!(plan.max_parallel_trials, 8);
        assert_eq!(plan.memory_budget_gb, 20);
        assert_eq!(plan.max_trials, 64);
    }

    #[test]
    fn candidate_screening_marks_professional_candidate() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(18, 2),
            excess_return: Decimal::new(5, 2),
            sharpe: Decimal::new(12, 1),
            sortino: Decimal::new(16, 1),
            max_drawdown: Decimal::new(20, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::Professional
        );
    }

    #[test]
    fn candidate_screening_requires_sortino_for_professional_candidate() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(18, 2),
            excess_return: Decimal::new(5, 2),
            sharpe: Decimal::new(12, 1),
            sortino: Decimal::new(10, 1),
            max_drawdown: Decimal::new(20, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::ReviewRequired
        );
    }

    #[test]
    fn candidate_metrics_parse_execution_fill_diagnostics() {
        let metrics = CandidateMetrics::from_optimization_metrics(&json!({
            "annual_return_pct": 0.18,
            "final_cash_weight_pct": 0.65,
            "final_target_gross_exposure_pct": 0.35,
            "final_actual_gross_exposure_pct": 0.34,
            "final_unfilled_target_gap_pct": 0.01,
            "final_execution_fill_ratio": 0.9714
        }));

        assert_eq!(metrics.final_cash_weight, Decimal::new(65, 2));
        assert_eq!(metrics.final_target_gross_exposure, Decimal::new(35, 2));
        assert_eq!(metrics.final_actual_gross_exposure, Decimal::new(34, 2));
        assert_eq!(metrics.final_unfilled_target_gap, Decimal::new(1, 2));
        assert_eq!(metrics.final_execution_fill_ratio, Decimal::new(9714, 4));
    }

    #[test]
    fn professional_execution_fill_ratio_profile_preserves_feasible_fill_axis() {
        let config = LayeredSearchConfig::professional_execution_fill_ratio_budget_default();

        assert_eq!(
            config.cash_utilization_profiles,
            vec![
                "fillable_gross_90_v1".to_string(),
                "fillable_gross_95_v1".to_string()
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["cash_utilization"] == "fillable_gross_90_v1"
                && trial["execution_schedule_profile"] == "twap_15d_v1"
        }));
    }

    #[test]
    fn professional_execution_rolling_carry_profile_adds_roll_forward_policy() {
        let config = LayeredSearchConfig::professional_execution_rolling_carry_budget_default();

        assert!(config.seed_trials.iter().any(|trial| {
            trial["execution_carry_policy"] == "roll_forward_v1"
                && trial["execution_rules"]["execution_carry_policy"] == "roll_forward_v1"
                && trial["cash_utilization"] == "fillable_gross_95_v1"
        }));
    }

    #[test]
    fn professional_execution_capacity_fill_frontier_profile_expands_fill_search_space() {
        let config = LayeredSearchConfig::professional_execution_capacity_fill_frontier_default();

        assert_eq!(config.top_n, vec![80, 120, 160]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)]
        );
        assert_eq!(
            config.max_gross_exposure,
            vec![Decimal::new(80, 2), Decimal::new(90, 2), Decimal::ONE]
        );
        assert_eq!(
            config.cash_utilization_profiles,
            vec!["fillable_gross_95_v1".to_string()]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["execution_carry_policy"] == "roll_forward_v1"
                && trial["execution_rules"]["execution_carry_policy"] == "roll_forward_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["cash_utilization"] == "fillable_gross_95_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
        }));
    }

    #[test]
    fn professional_execution_alpha_capacity_bridge_profile_combines_alpha_and_fill_constraints() {
        let config = LayeredSearchConfig::professional_execution_alpha_capacity_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
                ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ]
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_state_alpha_overlay_selector_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert_eq!(config.top_n, vec![80, 120]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(6, 2), Decimal::new(8, 2)]
        );
        assert_eq!(
            config.max_gross_exposure,
            vec![Decimal::new(90, 2), Decimal::ONE]
        );
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1".to_string()]
        );
        assert!(config.seed_trials.len() >= 24);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["capacity_fill_frontier_profile"] == "capacity_fill_top80_maxpos008_gross1"
                && trial["top_n"] == 80
                && trial["max_position_pct"] == "0.08"
                && trial["max_gross_exposure"] == "1"
                && trial["cash_utilization"] == "fillable_gross_95_v1"
                && trial["execution_carry_policy"] == "roll_forward_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["capacity_fill_frontier_profile"]
                    == "capacity_fill_top120_maxpos006_gross090"
                && trial["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
    }

    #[test]
    fn professional_execution_alpha_capacity_return_frontier_profile_expands_near_miss_axes() {
        let config =
            LayeredSearchConfig::professional_execution_alpha_capacity_return_frontier_default();

        assert_eq!(config.top_n, vec![60, 80, 100]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(7, 2), Decimal::new(8, 2), Decimal::new(10, 2)]
        );
        assert_eq!(
            config.max_gross_exposure,
            vec![Decimal::new(95, 2), Decimal::ONE]
        );
        assert_eq!(
            config.capacity_penalty_strength,
            vec![
                Decimal::new(125, 2),
                Decimal::new(150, 2),
                Decimal::new(200, 2)
            ]
        );
        assert_eq!(
            config.universe_profiles,
            vec!["all".to_string(), "listed_non_st".to_string()]
        );
        assert!(config
            .execution_impact_budget_profiles
            .contains(&"impact_turnover_30pct_v1".to_string()));
        assert!(config
            .execution_schedule_profiles
            .contains(&"twap_10d_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["capacity_fill_frontier_profile"] == "capacity_fill_top80_maxpos008_gross1"
                && trial["top_n"] == 80
                && trial["max_position_pct"] == "0.08"
                && trial["max_gross_exposure"] == "1"
                && trial["capacity_risk_budget"] == "capacity_participation_balanced_v1"
                && trial["execution_schedule_profile"] == "twap_10d_v1"
                && trial["execution_impact_budget"] == "impact_turnover_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["universe_profile"] == "listed_non_st"
                && trial["top_n"] == 60
                && trial["max_position_pct"] == "0.10"
                && trial["max_gross_exposure"] == "1"
        }));
    }

    #[test]
    fn professional_execution_stress_fill_return_frontier_profile_adds_stress_fill_axis() {
        let config =
            LayeredSearchConfig::professional_execution_stress_fill_return_frontier_default();

        assert_eq!(config.top_n, vec![80, 120, 160]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)]
        );
        assert_eq!(
            config.cash_utilization_profiles,
            vec![
                "stress_fill_gross_98_v1".to_string(),
                "fillable_gross_95_v1".to_string()
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "1"
                && trial["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn professional_execution_stress_risk_budget_profile_adds_soft_participation_axis() {
        let config = LayeredSearchConfig::professional_execution_stress_risk_budget_default();

        assert_eq!(config.top_n, vec![60, 80, 100]);
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_soft_cap_v1".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_soft_cap_v1"
                && trial["top_n"] == 80
                && trial["max_position_pct"] == "0.08"
                && trial["max_gross_exposure"] == "1"
                && trial["execution_schedule_profile"] == "twap_10d_v1"
        }));
    }

    #[test]
    fn professional_execution_capacity_stress_return_gate_profile_targets_stress_return_frontier() {
        let config =
            LayeredSearchConfig::professional_execution_capacity_stress_return_gate_default();

        assert_eq!(config.top_n, vec![80, 100, 120]);
        assert_eq!(
            config.capacity_penalty_strength,
            vec![Decimal::new(200, 2), Decimal::new(250, 2)]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec![
                "impact_turnover_15pct_v1".to_string(),
                "impact_turnover_20pct_v1".to_string()
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_soft_cap_v1"
                && trial["top_n"] == 100
                && trial["max_position_pct"] == "0.06"
                && trial["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial["execution_schedule_profile"] == "twap_15d_v1"
        }));
    }

    #[test]
    fn professional_execution_low_impact_alpha_stress_return_profile_targets_long_hold_sources() {
        let config =
            LayeredSearchConfig::professional_execution_low_impact_alpha_stress_return_default();

        assert_eq!(config.top_n, vec![80, 100, 120]);
        assert_eq!(config.rebalance_days, vec![160, 180, 220]);
        assert_eq!(
            config.combo_versions,
            vec![
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
                ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0")
            ]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_20d_v1".to_string(), "twap_15d_v1".to_string()]
        );
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1".to_string()]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_soft_cap_v1"
                && trial["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 100
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.95"
                && trial["rebalance"] == "180"
                && trial["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn professional_execution_stress_target_scaling_return_profile_adds_target_scale_axis() {
        let config =
            LayeredSearchConfig::professional_execution_stress_target_scaling_return_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert_eq!(
            config.max_gross_exposure,
            vec![
                Decimal::new(60, 2),
                Decimal::new(70, 2),
                Decimal::new(80, 2),
                Decimal::new(90, 2)
            ]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_target_scale_v1".to_string()));
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_20d_v1".to_string()]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_target_scale_v1"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.05"
                && trial["max_gross_exposure"] == "0.80"
                && trial["rebalance"] == "180"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_target_scale_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 100
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_stress_floor_scaling_return_profile_adds_gross_floor_axis() {
        let config =
            LayeredSearchConfig::professional_execution_stress_floor_scaling_return_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert_eq!(
            config.max_gross_exposure,
            vec![
                Decimal::new(70, 2),
                Decimal::new(80, 2),
                Decimal::new(90, 2)
            ]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_floor_35_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_floor_50_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_floor_35_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 100
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_stress_floor_return_recovery_profile_extends_floor_axis() {
        let config =
            LayeredSearchConfig::professional_execution_stress_floor_return_recovery_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert_eq!(
            config.max_gross_exposure,
            vec![
                Decimal::new(80, 2),
                Decimal::new(90, 2),
                Decimal::new(100, 2)
            ]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_floor_60_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_floor_70_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_soft_floor_60_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"] == "capacity_stress_participation_floor_70_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_pressure_headroom_floor_profile_targets_perturbed_fill_recovery() {
        let config = LayeredSearchConfig::professional_execution_pressure_headroom_floor_default();

        assert_eq!(config.top_n, vec![120, 160, 200]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(4, 2), Decimal::new(5, 2), Decimal::new(6, 2)]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_headroom_floor_70_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_headroom_floor_60_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"]
                    == "capacity_stress_participation_headroom_floor_70_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 160
                && trial["max_position_pct"] == "0.05"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_alpha_headroom_floor_profile_balances_return_and_fill() {
        let config = LayeredSearchConfig::professional_execution_alpha_headroom_floor_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(5, 2), Decimal::new(6, 2), Decimal::new(8, 2)]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_60_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_blended_alpha_headroom_floor_profile_keeps_dual_axis() {
        let config =
            LayeredSearchConfig::professional_execution_blended_alpha_headroom_floor_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert!(config.capacity_risk_budget_profiles.contains(
            &"capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string()
        ));
        assert!(config.capacity_risk_budget_profiles.contains(
            &"capacity_stress_participation_blended_alpha_headroom_floor_60_v1".to_string()
        ));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 120
                && trial["max_position_pct"] == "0.06"
                && trial["max_gross_exposure"] == "0.90"
                && trial["rebalance"] == "180"
        }));
    }

    #[test]
    fn professional_execution_event_anchor_stress_bridge_profile_keeps_strong_alpha_anchors() {
        let config =
            LayeredSearchConfig::professional_execution_event_anchor_stress_bridge_default();

        assert_eq!(config.top_n, vec![80, 120, 160]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_mixed_orthogonal_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["top_n"] == 80
                && trial["rebalance"] == "160"
                && trial["universe_profile"] == "listed_non_st"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                || trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
    }

    #[test]
    fn professional_execution_participation_aware_event_anchor_profile_prefers_tradable_events() {
        let config =
            LayeredSearchConfig::professional_execution_participation_aware_event_anchor_default();

        assert_eq!(config.top_n, vec![80, 120, 160]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["candidate_risk_filter"]
                    == "soft_liquidity_low_volatility_low_correlation_v1"
                && trial["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial["execution_schedule_profile"] == "twap_20d_v1"
                && trial["execution_rules"]["max_participation_rate"] == json!(0.10)
                && trial["top_n"] == 120
                && trial["rebalance"] == "180"
                && trial["universe_profile"] == "listed_non_st"
        }));
        assert!(
            config.seed_trials.iter().take(3).any(|trial| {
                trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                    || trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
            }),
            "tiny OOS smoke should cover at least one CL/CM-style high-Sharpe anchor"
        );
    }

    #[test]
    fn professional_execution_cl_anchor_fill_recovery_profile_starts_from_high_sharpe_anchor() {
        let config = LayeredSearchConfig::professional_execution_cl_anchor_fill_recovery_default();

        assert_eq!(config.top_n, vec![100, 120, 160]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["soft_liquidity_low_volatility_low_correlation_v1".to_string()]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_mixed_orthogonal_risk_memory_router_v3".to_string()));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ep should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_seed["market_regime"],
            "quality_mixed_orthogonal_risk_memory_router_v3"
        );
        assert_eq!(
            first_seed["candidate_risk_filter"],
            "soft_liquidity_low_volatility_low_correlation_v1"
        );
        assert_eq!(first_seed["top_n"], 120);
        assert_eq!(first_seed["max_position_pct"], "0.06");
        assert_eq!(first_seed["max_gross_exposure"], "0.90");
        assert_eq!(
            first_seed["capacity_risk_budget"],
            "capacity_stress_participation_alpha_headroom_floor_70_v1"
        );
        assert_eq!(
            first_seed["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
        assert_eq!(
            first_seed["execution_cl_anchor_fill_recovery_profile"],
            "cl_fill_recovery_top120_rebalance180"
        );
    }

    #[test]
    fn professional_execution_capacity_aware_candidate_ranking_profile_adds_ranking_axis() {
        let config =
            LayeredSearchConfig::professional_execution_capacity_aware_candidate_ranking_default();

        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );
        assert_eq!(config.top_n, vec![80, 100, 120]);
        assert_eq!(config.score_candidate_pool_sizes, vec![1800, 2200]);

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_eq should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_seed["candidate_risk_filter"],
            "soft_liquidity_low_volatility_low_correlation_v1"
        );
        assert_eq!(
            first_seed["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
        assert_eq!(first_seed["top_n"], 100);
        assert_eq!(first_seed["max_position_pct"], "0.08");
        assert_eq!(first_seed["max_gross_exposure"], "0.90");
        assert_eq!(
            first_seed["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
    }

    #[test]
    fn professional_execution_pit_capacity_ranking_profile_reuses_strict_capacity_search_space() {
        let config = LayeredSearchConfig::professional_execution_pit_capacity_ranking_default();

        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );
        assert_eq!(config.top_n, vec![80, 100, 120]);
        assert_eq!(config.score_candidate_pool_sizes, vec![1800, 2200]);

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_er should seed the EQ high-Sharpe capacity anchor first");
        assert_eq!(
            first_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_seed["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
        assert_eq!(first_seed["top_n"], 100);
        assert_eq!(first_seed["max_position_pct"], "0.08");
    }

    #[test]
    fn professional_execution_pit_alpha_first_low_impact_profile_restarts_from_alpha_sources() {
        let config =
            LayeredSearchConfig::professional_execution_pit_alpha_first_low_impact_default();

        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["alpha_first_low_impact_v1".to_string()]
        );
        assert_eq!(config.top_n, vec![80, 100, 120]);
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()]
        );

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_es should restart from an alpha-first low-impact seed");
        assert_eq!(first_seed["candidate_ranking"], "alpha_first_low_impact_v1");
        assert_eq!(
            first_seed["pit_capacity_recovery_profile"],
            "pit_alpha_first_event_top80_rebalance160"
        );
        assert_eq!(
            first_seed["market_regime"],
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        );
        assert_eq!(first_seed["top_n"], 80);
        assert_eq!(first_seed["rebalance"], "160");
    }

    #[test]
    fn professional_execution_pit_excess_return_recovery_profile_adds_relative_strength_axis() {
        let config =
            LayeredSearchConfig::professional_execution_pit_excess_return_recovery_default();

        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["relative_strength_alpha_liquidity_v1".to_string()]
        );
        assert!(config
            .candidate_risk_filter_profiles
            .contains(&"soft_low_volatility_v1".to_string()));
        assert!(config.max_gross_exposure.contains(&Decimal::ONE));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_et should seed the PIT excess-return recovery profile first");
        assert_eq!(
            first_seed["candidate_ranking"],
            "relative_strength_alpha_liquidity_v1"
        );
        assert_eq!(
            first_seed["pit_excess_return_recovery_profile"],
            "pit_excess_event_top80_rebalance120"
        );
        assert_eq!(first_seed["top_n"], 80);
        assert_eq!(first_seed["rebalance"], "120");
        assert_eq!(first_seed["max_gross_exposure"], "1");
    }

    #[test]
    fn professional_execution_pit_nonlinear_alpha_regime_rebuild_profile_restarts_alpha_search() {
        let config =
            LayeredSearchConfig::professional_execution_pit_nonlinear_alpha_regime_rebuild_default(
            );

        assert_eq!(
            config.candidate_ranking_profiles,
            vec![
                "alpha_first_low_impact_v1".to_string(),
                "capacity_aware_alpha_liquidity_v1".to_string(),
            ]
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert_eq!(config.top_n, vec![80, 100, 120, 160]);
        assert_eq!(config.rebalance_days, vec![120, 160, 180, 220]);

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ez should seed PIT nonlinear alpha rebuild first");
        assert_eq!(
            first_seed["combo_name"],
            "phase7_quality_event_window_overlay_v1"
        );
        assert_eq!(first_seed["candidate_ranking"], "alpha_first_low_impact_v1");
        assert_eq!(
            first_seed["candidate_risk_filter"],
            "soft_liquidity_low_volatility_low_correlation_v1"
        );
        assert_eq!(
            first_seed["market_regime"],
            "quality_state_alpha_overlay_selector_v1"
        );
        assert_eq!(
            first_seed["pit_nonlinear_alpha_regime_rebuild_profile"],
            "pit_nonlinear_event_overlay_softcap_top100_rebalance180"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_pit_quality_recovery_alpha_profile_uses_native_pit_alpha() {
        let config =
            LayeredSearchConfig::professional_execution_pit_quality_recovery_alpha_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new(
                "phase7_quality_recovery_acceleration_v1",
                "1.0.0"
            )]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Descending]);
        assert_eq!(
            config.candidate_ranking_profiles,
            vec![
                "alpha_first_low_impact_v1".to_string(),
                "capacity_aware_alpha_liquidity_v1".to_string(),
            ]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_fa should seed native PIT quality recovery first");
        assert_eq!(
            first_seed["combo_name"],
            "phase7_quality_recovery_acceleration_v1"
        );
        assert_eq!(first_seed["score_direction"], "descending");
        assert_eq!(first_seed["candidate_ranking"], "alpha_first_low_impact_v1");
        assert_eq!(
            first_seed["pit_quality_recovery_alpha_profile"],
            "pit_quality_recovery_top100_rebalance160"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_recovery_acceleration_v1"
                && trial["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_event_post_return_curve_profile_uses_pit_event_curve_alpha() {
        let config =
            LayeredSearchConfig::professional_execution_event_post_return_curve_alpha_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new(
                "phase7_quality_event_post_return_curve_overlay_v1",
                "1.0.0"
            )]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Descending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_event_window_return_sharpe_router_v4".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_fb should seed PIT event post-return curve first");
        assert_eq!(
            first_seed["combo_name"],
            "phase7_quality_event_post_return_curve_overlay_v1"
        );
        assert_eq!(
            first_seed["event_post_return_curve_alpha_profile"],
            "event_post_return_curve_top100_rebalance160"
        );
        assert_eq!(first_seed["candidate_ranking"], "alpha_first_low_impact_v1");
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_event_reaction_profile_compares_segments_and_reversal_alpha() {
        let config = LayeredSearchConfig::professional_execution_event_reaction_alpha_default();

        assert_eq!(
            config.combo_versions,
            vec![
                ComboVersion::new("phase7_quality_event_reaction_segments_overlay_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_event_reaction_reversal_overlay_v1", "1.0.0"),
            ]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Descending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_event_window_return_sharpe_router_v4".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_reaction_segments_overlay_v1"
                && trial["event_reaction_alpha_profile"]
                    == "event_reaction_segments_top100_rebalance160"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_reaction_reversal_overlay_v1"
                && trial["event_reaction_alpha_profile"]
                    == "event_reaction_reversal_top100_rebalance160"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_broad_financial_feature_discovery_profile_uses_trainable_ff_combos() {
        let config =
            LayeredSearchConfig::professional_execution_broad_financial_feature_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<Vec<_>>();

        assert!(combo_names.contains(&"phase7_financial_quality_v1"));
        assert!(combo_names.contains(&"phase7_industry_residual_quality_v1"));
        assert!(combo_names.contains(&"phase7_growth_recovery_v1"));
        assert!(combo_names.contains(&"phase7_quality_relative_strength_v1"));
        assert!(combo_names.contains(&"phase7_quality_cashflow_confirm_v1"));
        assert!(combo_names.contains(&"phase7_quality_dividend_confirm_v1"));
        assert!(combo_names.contains(&"phase7_quality_cashflow_dividend_confirm_v1"));
        assert!(!combo_names.contains(&"phase7_dividend_quality_v1"));
        assert!(!combo_names.contains(&"phase7_cashflow_quality_v1"));
        assert_eq!(
            config.candidate_ranking_profiles,
            vec![
                "alpha_first_low_impact_v1".to_string(),
                "capacity_aware_alpha_liquidity_v1".to_string(),
            ]
        );
        assert_eq!(
            config.score_directions,
            vec![ScoreDirection::Ascending, ScoreDirection::Descending]
        );

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_fg should seed broad FF financial combos first");
        assert_eq!(
            first_seed["combo_name"],
            "phase7_quality_cashflow_dividend_confirm_v1"
        );
        assert_eq!(
            first_seed["broad_financial_feature_discovery_profile"],
            "broad_ff_dual_confirm_ascending_top100"
        );
        assert_eq!(first_seed["candidate_ranking"], "alpha_first_low_impact_v1");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_dividend_confirm_v1"
                && trial["score_direction"] == "descending"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("phase7_dividend_quality_v1"));
            assert!(!serialized.contains("phase7_cashflow_quality_v1"));
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_broad_financial_feature_stratified_discovery_profile_diversifies_small_budgets(
    ) {
        let config = LayeredSearchConfig::professional_execution_broad_financial_feature_stratified_discovery_default();
        let first_four_combos = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| trial["combo_name"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        let first_four_directions = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| trial["score_direction"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();

        assert!(first_four_combos.contains(&"phase7_financial_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_growth_recovery_v1"));
        assert!(first_four_combos.contains(&"phase7_industry_residual_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_quality_relative_strength_v1"));
        assert!(first_four_directions.contains(&"ascending"));
        assert!(first_four_directions.contains(&"descending"));
        for seed in config.seed_trials.iter().take(8) {
            assert_eq!(
                seed["broad_financial_feature_sampling"],
                "stratified_seed_v1"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("phase7_dividend_quality_v1"));
            assert!(!serialized.contains("phase7_cashflow_quality_v1"));
        }
    }

    #[test]
    fn professional_execution_native_alpha_fusion_discovery_profile_combines_broad_event_prediction_sources(
    ) {
        let config =
            LayeredSearchConfig::professional_execution_native_alpha_fusion_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<Vec<_>>();

        assert!(combo_names.contains(&"phase7_financial_quality_v1"));
        assert!(combo_names.contains(&"phase7_industry_residual_quality_v1"));
        assert!(combo_names.contains(&"phase7_growth_recovery_v1"));
        assert!(combo_names.contains(&"phase7_quality_relative_strength_v1"));
        assert!(combo_names.contains(&"phase7_event_surprise_v1"));
        assert!(
            !combo_names.contains(&"phase7_quality_event_surprise_confirm_v1"),
            "sparse event surprise confirmation should be used as a gate/overlay, not as an FJ base combo"
        );
        assert!(combo_names.contains(&"phase7_quality_event_reaction_segments_overlay_v1"));
        assert_eq!(
            config.prediction_set_ids,
            vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()]
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_state_alpha_overlay_selector_v1".to_string()));
        assert!(config
            .candidate_ranking_profiles
            .contains(&"capacity_aware_alpha_liquidity_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));

        let first_four_families = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| trial["alpha_source_family"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(
            first_four_families.contains(&"broad_financial"),
            "tiny FJ smoke should cover broad financial"
        );
        assert!(
            first_four_families.contains(&"event_surprise"),
            "tiny FJ smoke should cover event surprise"
        );
        assert!(
            first_four_families.contains(&"event_reaction"),
            "tiny FJ smoke should cover event reaction"
        );
        assert!(
            first_four_families.contains(&"prediction_confirmation"),
            "tiny FJ smoke should cover prediction confirmation"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["native_alpha_fusion_profile"] == "broad_financial_core"
                && trial["alpha_source_family"] == "broad_financial"
                && trial["broad_financial_feature_sampling"] == "stratified_seed_v1"
        }));
        let event_surprise_seed = config
            .seed_trials
            .iter()
            .find(|trial| trial["alpha_source_family"] == "event_surprise")
            .expect("phase7_fj should include an event surprise seed");
        assert_eq!(
            event_surprise_seed["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            event_surprise_seed["event_gate_combo_name"],
            "phase7_event_surprise_v1"
        );
        assert_ne!(
            event_surprise_seed["combo_name"],
            "phase7_quality_event_surprise_confirm_v1"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["native_alpha_fusion_profile"] == "event_reaction_segments"
                && trial["alpha_source_family"] == "event_reaction"
                && trial["combo_name"] == "phase7_quality_event_reaction_segments_overlay_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["native_alpha_fusion_profile"] == "prediction_confirmation"
                && trial["alpha_source_family"] == "prediction_confirmation"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
        }));
        let prediction_seed = config
            .seed_trials
            .iter()
            .find(|trial| trial["alpha_source_family"] == "prediction_confirmation")
            .expect("phase7_fj should include a prediction confirmation seed");
        assert_eq!(
            prediction_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            prediction_seed["capacity_risk_budget"],
            "capacity_stress_participation_alpha_headroom_floor_70_v1"
        );
        assert_eq!(prediction_seed["max_position_pct"], "0.08");
        assert_eq!(prediction_seed["universe_profile"], "listed_non_st");

        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("phase7_dividend_quality_v1"));
            assert!(!serialized.contains("phase7_cashflow_quality_v1"));
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_trainable_alpha_admission_discovery_profile_keeps_base_sources_trainable() {
        let config =
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_industry_residual_quality_v1"));
        assert!(combo_names.contains("phase7_growth_recovery_v1"));
        assert!(combo_names.contains("phase7_quality_relative_strength_v1"));
        assert!(combo_names.contains("phase7_quality_cashflow_dividend_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        for combo_name in &combo_names {
            assert!(
                is_phase7_base_trainable_alpha(combo_name),
                "{combo_name} must be admitted as a trainable Phase 7 base alpha"
            );
        }
        for forbidden_base in [
            "phase7_event_earnings_v1",
            "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_surprise_confirm_v1",
            "phase7_quality_event_post_return_curve_overlay_v1",
            "phase7_quality_event_reaction_segments_overlay_v1",
            "phase7_quality_event_reaction_reversal_overlay_v1",
        ] {
            assert!(
                !combo_names.contains(forbidden_base),
                "{forbidden_base} must not enter the trainable base combo universe"
            );
        }

        let first_six_families = config
            .seed_trials
            .iter()
            .take(6)
            .map(|trial| trial["alpha_source_family"].as_str().unwrap_or_default())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(first_six_families.contains("broad_financial"));
        assert!(first_six_families.contains("residual_confirm"));
        assert!(first_six_families.contains("value_recovery"));

        for seed in &config.seed_trials {
            let combo_name = seed["combo_name"].as_str().unwrap_or_default();
            assert!(
                is_phase7_base_trainable_alpha(combo_name),
                "{combo_name} seed base must be trainable"
            );
            assert_ne!(
                seed["alpha_source_family"], "event_reaction",
                "stale event reaction overlays must not be part of trainable admission seeds"
            );
            if let Some(event_gate_combo_name) = seed["event_gate_combo_name"].as_str() {
                assert_eq!(
                    phase7_alpha_source_admission(event_gate_combo_name).role,
                    Phase7AlphaSourceRole::EventGateOnly
                );
            }
        }
    }

    #[test]
    fn phase7_financial_quality_change_is_p37_base_trainable_alpha_source() {
        let admission = phase7_alpha_source_admission("phase7_financial_quality_change_v1");

        assert_eq!(
            admission.role,
            Phase7AlphaSourceRole::BaseTrainable,
            "financial quality acceleration must be admitted as a PIT base alpha"
        );
        assert_eq!(admission.combo_name, "phase7_financial_quality_change_v1");
        assert!(admission.reason.contains("PIT"));
    }

    #[test]
    fn professional_trainable_alpha_admission_discovery_includes_p37_quality_change_seed() {
        let config =
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(combo_names.contains("phase7_financial_quality_change_v1"));

        let seed = config
            .seed_trials
            .iter()
            .find(|seed| seed["combo_name"] == "phase7_financial_quality_change_v1")
            .expect("P3.7 financial quality change seed");

        assert_eq!(
            seed["trainable_alpha_admission_profile"],
            "financial_quality_change_acceleration"
        );
        assert_eq!(seed["alpha_source_family"], "financial_quality_change");
        assert_eq!(seed["signal_source"], "factor_combo");
        assert_eq!(seed["version"], "1.0.0");
        assert!(
            !seed.to_string().contains("pred-"),
            "P3.7 first atom must stay a native PIT factor combo, not a prediction overlay"
        );
    }

    #[test]
    fn phase7_earnings_recovery_persistence_is_p37_base_trainable_alpha_source() {
        let admission = phase7_alpha_source_admission("phase7_earnings_recovery_persistence_v1");

        assert_eq!(
            admission.role,
            Phase7AlphaSourceRole::BaseTrainable,
            "earnings recovery persistence must be admitted as a PIT base alpha"
        );
        assert_eq!(
            admission.combo_name,
            "phase7_earnings_recovery_persistence_v1"
        );
        assert!(admission.reason.contains("PIT"));
    }

    #[test]
    fn professional_trainable_alpha_admission_discovery_includes_p37_earnings_recovery_seed() {
        let config =
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(combo_names.contains("phase7_earnings_recovery_persistence_v1"));

        let seed = config
            .seed_trials
            .iter()
            .find(|seed| seed["combo_name"] == "phase7_earnings_recovery_persistence_v1")
            .expect("P3.7 earnings recovery persistence seed");

        assert_eq!(
            seed["trainable_alpha_admission_profile"],
            "earnings_recovery_persistence"
        );
        assert_eq!(seed["alpha_source_family"], "earnings_recovery_persistence");
        assert_eq!(seed["signal_source"], "factor_combo");
        assert_eq!(seed["version"], "1.0.0");
        assert!(
            !seed.to_string().contains("pred-"),
            "P3.7 second atom must stay a native PIT factor combo, not a prediction overlay"
        );
    }

    #[test]
    fn phase7_moneyflow_congestion_is_p38_base_trainable_alpha_source() {
        let admission = phase7_alpha_source_admission("phase7_moneyflow_congestion_interaction_v1");

        assert_eq!(
            admission.role,
            Phase7AlphaSourceRole::BaseTrainable,
            "moneyflow congestion interaction must be admitted as a PIT base alpha"
        );
        assert_eq!(
            admission.combo_name,
            "phase7_moneyflow_congestion_interaction_v1"
        );
        assert!(admission.reason.contains("PIT"));
    }

    #[test]
    fn professional_trainable_alpha_admission_discovery_includes_p38_moneyflow_congestion_seed() {
        let config =
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default();
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert!(combo_names.contains("phase7_moneyflow_congestion_interaction_v1"));

        let seed = config
            .seed_trials
            .iter()
            .find(|seed| seed["combo_name"] == "phase7_moneyflow_congestion_interaction_v1")
            .expect("P3.8 moneyflow congestion seed");

        assert_eq!(
            seed["trainable_alpha_admission_profile"],
            "moneyflow_congestion"
        );
        assert_eq!(seed["alpha_source_family"], "moneyflow_congestion");
        assert_eq!(seed["signal_source"], "factor_combo");
        assert_eq!(seed["version"], "1.0.0");
        assert!(
            !seed.to_string().contains("pred-"),
            "P3.8 atom must stay a native PIT factor combo, not a prediction overlay"
        );
    }

    #[test]
    fn professional_v19_multi_alpha_sleeve_admission_profile_is_pit_base_trainable_only() {
        let config = LayeredSearchConfig::professional_v19_multi_alpha_sleeve_admission_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 12;

        let plan = build_layered_search_plan(&config, &resource_plan);
        let sleeve_families = plan
            .trials
            .iter()
            .map(|trial| {
                trial.parameters["alpha_sleeve_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<std::collections::BTreeSet<_>>();

        for required_family in [
            "quality_core",
            "valuation_guard",
            "growth_recovery",
            "relative_strength",
            "moneyflow_quality",
            "cashflow_dividend_quality",
            "value_recovery",
            "residual_quality",
        ] {
            assert!(
                sleeve_families.contains(required_family),
                "{required_family} sleeve must be represented in first-stage v19 sleeve admission"
            );
        }
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["multi_alpha_sleeve_profile"] == "v19_p2_pit_sleeve_admission_v1"
        }));
        assert!(plan.trials.iter().all(|trial| {
            let combo_name = trial.parameters["combo_name"].as_str().unwrap_or_default();
            is_phase7_base_trainable_alpha(combo_name)
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("phase7_quality_event_post_return_curve_overlay_v1")
                || serialized.contains("phase7_quality_event_reaction_segments_overlay_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn professional_v19_event_post_return_overlay_profile_keeps_event_source_as_overlay_or_gate() {
        let config =
            LayeredSearchConfig::professional_v19_event_post_return_overlay_admission_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 12;

        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert!(config.combo_versions.iter().all(|combo| {
            combo.combo_name == "phase7_quality_event_post_return_curve_overlay_v1"
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_post_return_boost_pos_5pct"
                && profile.combo_name.as_deref() == Some("phase7_event_post_return_curve_20d_v1")
        }));

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
                && trial.parameters["event_overlay_profile"]
                    == "v19_p39_broad_base_event_post_return_overlay_v1"
                && trial.parameters["broad_base_combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["event_overlay_combo_name"]
                    == "phase7_event_post_return_curve_20d_v1"
                && trial.parameters["alpha_source_family"] == "event_post_return_overlay"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_post_return_curve_20d_v1"
                || trial.parameters.to_string().contains("pred-")
        }));
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_event_post_return_curve_overlay_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert!(
            !is_phase7_base_trainable_alpha("phase7_event_post_return_curve_20d_v1"),
            "raw post-return event curve must never become a v19 train-selection base"
        );
    }

    #[test]
    fn professional_v19_execution_repair_admission_profile_reuses_v19_alpha_with_bounded_execution_axes(
    ) {
        let config = LayeredSearchConfig::professional_v19_execution_repair_admission_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 30;

        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.cash_utilization_profiles,
            vec![
                "fillable_gross_95_v1".to_string(),
                "stress_fill_gross_98_v1".to_string()
            ]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![900, 1200, 1500]);

        let plan = build_layered_search_plan(&config, &resource_plan);
        let sleeve_families = plan
            .trials
            .iter()
            .map(|trial| {
                trial.parameters["alpha_sleeve_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<std::collections::BTreeSet<_>>();

        for required_family in [
            "quality_core",
            "valuation_guard",
            "growth_recovery",
            "relative_strength",
            "moneyflow_quality",
            "cashflow_dividend_quality",
            "value_recovery",
            "residual_quality",
            "blend_quality_growth",
            "recovery_tilt",
        ] {
            assert!(
                sleeve_families.contains(required_family),
                "{required_family} sleeve must remain in the v19 execution repair admission surface"
            );
        }
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_execution_repair_profile"] == "v19_p2_pit_execution_repair_v1"
                && trial.parameters["multi_alpha_sleeve_profile"]
                    == "v19_p2_pit_sleeve_admission_v1"
        }));
        assert!(plan.trials.iter().all(|trial| {
            let combo_name = trial.parameters["combo_name"].as_str().unwrap_or_default();
            is_phase7_base_trainable_alpha(combo_name)
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
                && trial.parameters["score_candidate_pool_size"] == 900
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_balanced_v1"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("phase7_quality_event_post_return_curve_overlay_v1")
                || serialized.contains("phase7_quality_event_reaction_segments_overlay_v1")
                || serialized.contains("pred-")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_alpha_rebuild_profile_is_train_scoped_and_pit_auditable() {
        let config = LayeredSearchConfig::professional_v19_train_window_ml_alpha_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 24;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "v19 alpha rebuild must generate per-window ML predictions, not reuse full-period sets"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.cash_utilization_profiles,
            vec![
                "fillable_gross_95_v1".to_string(),
                "stress_fill_gross_98_v1".to_string()
            ]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_10d_v1".to_string(), "twap_15d_v1".to_string()]
        );

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 24);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_alpha_rebuild_v1"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"]
                    == "quality_adjusted_risk_adjusted_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 60
                && trial.parameters["train_window_ml_bucket_count"] == 10
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["portfolio_method"] == "stress_fill_aware_risk_budget"
                && trial.parameters["stress_fill_portfolio_construction"]
                    == "ml_score_capacity_correlation_risk_budget_target_exposure_v1"
                && trial.parameters["prediction_confidence_gate_profile"]
                    == "train_positive_raw_score_gate_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "quality_core"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "relative_strength"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_simple_excess_rebuild_profile_is_train_scoped_and_pit_auditable(
    ) {
        let config =
            LayeredSearchConfig::professional_v19_train_window_ml_simple_excess_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 18;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "v19 simple excess rebuild must generate per-window ML predictions, not reuse full-period sets"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 18);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_simple_excess_rebuild_v1"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 45
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["alpha_source_family"]
                    == "v19_train_window_ml_simple_excess_rebuild"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "quality_core"
                && trial.parameters["v19_execution_repair_variant"]
                    == "v19_exec_fill95_twap10_balanced_top80_pool900"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "relative_strength"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_simple_excess_low_impact_rebuild_profile_focuses_train_scoped_growth_recovery(
    ) {
        let config = LayeredSearchConfig::professional_v19_train_window_ml_simple_excess_low_impact_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 6;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "low-impact simple excess rebuild must generate per-window ML predictions"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_growth_recovery_v1", "1.0.0")]
        );
        assert_eq!(config.top_n, vec![80, 100]);
        assert_eq!(config.rebalance_days, vec![120, 160, 180]);
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::new(25, 2), Decimal::new(35, 2)]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()]
        );

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 6);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_simple_excess_low_impact_rebuild_v1"
                && trial.parameters["alpha_sleeve_family"] == "growth_recovery"
                && trial.parameters["combo_name"] == "phase7_growth_recovery_v1"
                && trial.parameters["score_direction"] == "descending"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 45
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["train_window_ml_min_samples_per_bucket"] == 50
                && trial.parameters["alpha_source_family"]
                    == "v19_train_window_ml_simple_excess_low_impact_rebuild"
                && trial.parameters["candidate_ranking"] == "alpha_first_low_impact_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 80
                && trial.parameters["rebalance"] == "120"
                && trial.parameters["partial_rebalance_ratio"] == "0.35"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 100
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_h120_low_impact_rebuild_profile_is_train_scoped_and_long_horizon(
    ) {
        let config =
            LayeredSearchConfig::professional_v19_train_window_ml_h120_low_impact_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 8;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "H120 low-impact rebuild must generate per-window ML predictions"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_20d_v1".to_string()]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::new(25, 2), Decimal::new(35, 2)]
        );

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 8);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_train_window_ml_h120_low_impact_rebuild_v1"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"] == "future_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 5
                && trial.parameters["train_window_ml_min_samples_per_bucket"] == 50
                && trial.parameters["alpha_source_family"]
                    == "v19_train_window_ml_h120_low_impact_rebuild"
                && trial.parameters["portfolio_method"] == "stress_fill_aware_risk_budget"
                && trial.parameters["stress_fill_portfolio_construction"]
                    == "ml_score_capacity_correlation_risk_budget_target_exposure_v1"
                && trial.parameters["candidate_ranking"] == "alpha_first_low_impact_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "quality_core"
                && trial.parameters["rebalance"] == "240"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "residual_quality"
                && trial.parameters["rebalance"] == "360"
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_profile_is_train_scoped()
    {
        let config =
            LayeredSearchConfig::professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 6;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "RAE H120 residual/capacity rebuild must generate per-window ML predictions"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_20d_v1".to_string()]
        );

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 6);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_3_train_window_ml_rae_h120_residual_capacity_rebuild_v1"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v6"
                && trial.parameters["train_window_ml_label_objective"]
                    == "risk_adjusted_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 7
                && trial.parameters["train_window_ml_min_samples_per_bucket"] == 50
                && trial.parameters["alpha_source_family"]
                    == "v19_train_window_ml_rae_h120_residual_capacity_rebuild"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["portfolio_method"] == "stress_fill_aware_risk_budget"
                && trial.parameters["stress_fill_portfolio_construction"]
                    == "ml_score_capacity_correlation_risk_budget_target_exposure_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "residual_quality"
                && trial.parameters["rebalance"] == "360"
                && trial.parameters["max_position_pct"] == "0.04"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "moneyflow_quality"
                && trial.parameters["score_candidate_pool_size"] == 2800
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("phase7_event_surprise_v1")
                || serialized.contains("phase7_event_window_earnings_v1")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_v19_train_window_ml_event_sentiment_rebuild_profile_is_train_scoped() {
        let config =
            LayeredSearchConfig::professional_v19_train_window_ml_event_sentiment_rebuild_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 6;

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "event/sentiment rebuild must generate per-window ML predictions"
        );
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_20d_v1".to_string()]
        );

        let plan = build_layered_search_plan(&config, &resource_plan);
        assert_eq!(plan.trials.len(), 6);
        assert!(plan.trials.iter().all(|trial| {
            trial.parameters["v19_alpha_rebuild_profile"]
                == "v19_p3_4_train_window_ml_event_sentiment_rebuild_v1"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_p4_event_sentiment_high_coverage_v1"
                && trial.parameters["train_window_ml_label_objective"]
                    == "risk_adjusted_excess_return"
                && trial.parameters["train_window_ml_label_horizon_days"] == 120
                && trial.parameters["train_window_ml_bucket_count"] == 7
                && trial.parameters["train_window_ml_min_samples_per_bucket"] == 50
                && trial.parameters["alpha_source_family"]
                    == "v19_train_window_ml_event_sentiment_rebuild"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "event_sentiment_quality"
                && trial.parameters["rebalance"] == "240"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["alpha_sleeve_family"] == "event_moneyflow_capacity"
                && trial.parameters["score_candidate_pool_size"] == 3000
        }));
        assert!(!plan.trials.iter().any(|trial| {
            let serialized = trial.parameters.to_string();
            serialized.contains("prediction_blend")
                || serialized.contains("pred-")
                || serialized.contains("2017")
                || serialized.contains("2020")
        }));
    }

    #[test]
    fn professional_prediction_capacity_dual_objective_profile_bridges_return_and_tradability() {
        let config = LayeredSearchConfig::professional_prediction_capacity_dual_objective_default();

        assert_eq!(
            config.prediction_set_ids,
            vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()]
        );
        assert_eq!(
            config.top_n,
            vec![20, 40, 60],
            "FL should expand candidate count gradually from the positive prediction bridge"
        );
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(15, 2), Decimal::new(10, 2), Decimal::new(8, 2)]
        );
        assert_eq!(
            config.candidate_ranking_profiles,
            vec![
                "off".to_string(),
                "alpha_first_low_impact_v1".to_string(),
                "capacity_aware_alpha_liquidity_v1".to_string(),
            ]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config.capacity_risk_budget_profiles.contains(
            &"capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string()
        ));
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["twap_15d_v1".to_string(), "twap_20d_v1".to_string()]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::ONE, Decimal::new(35, 2), Decimal::new(25, 2)]
        );

        let first_four_profiles = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| {
                trial["prediction_capacity_dual_objective_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(first_four_profiles.contains(&"return_anchor_capacity_off"));
        assert!(first_four_profiles.contains(&"alpha_first_twap15_headroom70"));
        assert!(first_four_profiles.contains(&"capacity_aware_twap15_headroom70"));
        assert!(first_four_profiles.contains(&"capacity_aware_twap20_blended_headroom70"));

        let return_anchor = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_capacity_dual_objective_profile"] == "return_anchor_capacity_off"
            })
            .expect("FL should retain the original positive prediction bridge shape");
        let original_bridge = professional_prediction_confirmed_sharpe_bridge_seed_trials()
            .into_iter()
            .next()
            .expect("prediction bridge should have a return anchor");
        assert_eq!(return_anchor["candidate_ranking"], "off");
        assert_eq!(return_anchor["capacity_risk_budget"], "off");
        assert_eq!(return_anchor["max_position_pct"], "0.15");
        assert_eq!(return_anchor["partial_rebalance_ratio"], "1");
        assert_eq!(
            return_anchor.get("execution_schedule_profile"),
            original_bridge.get("execution_schedule_profile"),
            "return anchor must preserve the original prediction bridge timing fields"
        );
        assert_eq!(
            return_anchor.get("capacity_penalty_strength"),
            original_bridge.get("capacity_penalty_strength"),
            "return anchor must not rewrite the original bridge capacity penalty"
        );
        assert_eq!(
            return_anchor.get("cash_utilization"),
            original_bridge.get("cash_utilization"),
            "return anchor must not add fill-repair cash utilization"
        );

        let capacity_seed = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_capacity_dual_objective_profile"]
                    == "capacity_aware_twap20_blended_headroom70"
            })
            .expect("FL should include a stricter capacity-aware prediction neighbor");
        assert_eq!(
            capacity_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            capacity_seed["capacity_risk_budget"],
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
        );
        assert_eq!(capacity_seed["execution_schedule_profile"], "twap_20d_v1");
        assert_eq!(capacity_seed["partial_rebalance_ratio"], "0.25");
        assert_eq!(capacity_seed["max_position_pct"], "0.08");
        assert_eq!(capacity_seed["universe_profile"], "listed_non_st");

        for seed in &config.seed_trials {
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-v1-201602-202605"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_target_gross_signal_fidelity_profile_scales_exposure_only() {
        let config =
            LayeredSearchConfig::professional_prediction_target_gross_signal_fidelity_default();

        assert_eq!(
            config.prediction_set_ids,
            vec!["pred-p7-wf-wide-qgvrel-v1-201602-202605".to_string()]
        );
        assert_eq!(
            config.max_gross_exposure,
            vec![
                Decimal::new(35, 2),
                Decimal::new(50, 2),
                Decimal::new(65, 2),
                Decimal::new(80, 2),
                Decimal::ONE,
            ]
        );
        assert_eq!(config.candidate_ranking_profiles, vec!["off".to_string()]);
        assert_eq!(
            config.capacity_risk_budget_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.cash_utilization_profiles, vec!["off".to_string()]);
        assert_eq!(
            config.execution_schedule_profiles,
            vec!["immediate".to_string()]
        );

        let first_four_profiles = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| {
                trial["prediction_target_gross_signal_fidelity_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(first_four_profiles.contains(&"gross100_return_anchor"));
        assert!(first_four_profiles.contains(&"gross65_signal_fidelity"));
        assert!(first_four_profiles.contains(&"gross50_signal_fidelity"));
        assert!(first_four_profiles.contains(&"gross35_signal_fidelity"));

        let original_bridge = professional_prediction_confirmed_sharpe_bridge_seed_trials()
            .into_iter()
            .next()
            .expect("prediction bridge should have a return anchor");
        let gross65 = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_target_gross_signal_fidelity_profile"]
                    == "gross65_signal_fidelity"
            })
            .expect("FM should include gross65 signal-fidelity seed");
        assert_eq!(gross65["max_gross_exposure"], "0.65");
        assert_eq!(gross65["candidate_ranking"], "off");
        assert_eq!(gross65["capacity_risk_budget"], "off");
        assert_eq!(gross65["cash_utilization"], "off");
        assert_eq!(
            gross65.get("execution_schedule_profile"),
            original_bridge.get("execution_schedule_profile"),
            "FM should not change prediction bridge execution timing"
        );
        assert_eq!(
            gross65["prediction_set_id"],
            original_bridge["prediction_set_id"]
        );
        assert_eq!(
            gross65["prediction_blend_weight"],
            original_bridge["prediction_blend_weight"]
        );

        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_confidence_turnover_discovery_profile_gates_prediction_and_turnover()
    {
        let config =
            LayeredSearchConfig::professional_prediction_confidence_turnover_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FN keeps prediction as a PIT confirmation overlay on factor_combo seeds, not as a standalone model_prediction source"
        );
        assert_eq!(
            config.rebalance_days,
            vec![120, 160, 200],
            "FN should explore longer holding horizons instead of the high-turnover bridge default"
        );
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![
                Decimal::ZERO,
                Decimal::new(5, 3),
                Decimal::new(1, 2),
                Decimal::new(2, 2),
            ]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![
                Decimal::ONE,
                Decimal::new(85, 2),
                Decimal::new(75, 2),
                Decimal::new(65, 2),
            ]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec![
                "off".to_string(),
                "impact_turnover_20pct_v1".to_string(),
                "impact_turnover_15pct_v1".to_string(),
            ]
        );

        let first_four_profiles = config
            .seed_trials
            .iter()
            .take(4)
            .map(|trial| {
                trial["prediction_confidence_turnover_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(first_four_profiles.contains(&"return_anchor_confidence_off"));
        assert!(first_four_profiles.contains(&"confidence20_turnover_smooth"));
        assert!(first_four_profiles.contains(&"confidence30_turnover_smooth"));
        assert!(first_four_profiles.contains(&"confidence40_low_turnover"));

        let return_anchor = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_confidence_turnover_profile"] == "return_anchor_confidence_off"
            })
            .expect("FN should retain the FM/bridge return anchor before confidence gating");
        let original_bridge = professional_prediction_confirmed_sharpe_bridge_seed_trials()
            .into_iter()
            .next()
            .expect("prediction bridge should have a return anchor");
        assert_eq!(
            return_anchor.get("prediction_min_percentile"),
            original_bridge.get("prediction_min_percentile")
        );
        assert_eq!(
            return_anchor["prediction_blend_weight"],
            original_bridge["prediction_blend_weight"]
        );
        assert_eq!(return_anchor["rebalance_hysteresis_pct"], "0");
        assert_eq!(return_anchor["partial_rebalance_ratio"], "1");
        assert_eq!(return_anchor["execution_impact_budget"], "off");
        assert!(
            return_anchor.get("execution_carry_policy").is_none(),
            "off/default carry policy must not be serialized into trial parameters"
        );
        assert!(
            return_anchor
                .get("execution_rules")
                .and_then(Value::as_object)
                .and_then(|rules| rules.get("execution_carry_policy"))
                .is_none(),
            "off/default carry policy must not be serialized into execution_rules"
        );

        let confidence30 = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_confidence_turnover_profile"] == "confidence30_turnover_smooth"
            })
            .expect("FN should include a prediction confidence gate plus turnover smoothing seed");
        assert_eq!(
            confidence30["prediction_set_id"],
            "pred-p7-wf-wide-qgvrel-v1-201602-202605"
        );
        assert_eq!(confidence30["prediction_min_percentile"], "0.30");
        assert_eq!(confidence30["prediction_blend_weight"], "0.05");
        assert_eq!(confidence30["rebalance"], "160");
        assert_eq!(confidence30["rebalance_hysteresis_pct"], "0.01");
        assert_eq!(confidence30["partial_rebalance_ratio"], "0.75");
        assert_eq!(
            confidence30["execution_impact_budget"],
            "impact_turnover_20pct_v1"
        );
        assert_eq!(confidence30["execution_carry_policy"], "roll_forward_v1");

        let confidence40 = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_confidence_turnover_profile"] == "confidence40_low_turnover"
            })
            .expect("FN should include a stricter low-turnover confidence seed");
        assert_eq!(confidence40["prediction_min_percentile"], "0.40");
        assert_eq!(confidence40["rebalance"], "200");
        assert_eq!(confidence40["partial_rebalance_ratio"], "0.65");
        assert_eq!(
            confidence40["execution_impact_budget"],
            "impact_turnover_15pct_v1"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-v1-201602-202605"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_confidence_alpha_lift_profile_preserves_low_gap_structure_and_adds_alpha_lift(
    ) {
        let config = LayeredSearchConfig::professional_prediction_confidence_alpha_lift_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FO keeps prediction as a PIT confirmation overlay on factor_combo seeds, not as a standalone model_prediction source"
        );
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<Vec<_>>();
        assert!(combo_names.contains(&"phase7_financial_quality_v1"));
        assert!(combo_names.contains(&"phase7_quality_value_recovery_confirm_v1"));
        assert!(combo_names.contains(&"phase7_quality_event_post_return_curve_overlay_v1"));
        assert!(combo_names.contains(&"phase7_quality_event_reaction_segments_overlay_v1"));
        assert!(combo_names.contains(&"phase7_quality_event_reaction_reversal_overlay_v1"));
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_event_window_return_sharpe_router_v4".to_string()));

        let profile_names = config
            .seed_trials
            .iter()
            .map(|trial| {
                trial["prediction_confidence_alpha_lift_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(profile_names.contains(&"confidence40_quality_nonlinear_alpha_lift"));
        assert!(profile_names.contains(&"confidence40_value_recovery_alpha_lift"));
        assert!(profile_names.contains(&"confidence40_event_post_curve_lift"));
        assert!(profile_names.contains(&"confidence40_event_reaction_segments_lift"));
        assert!(profile_names.contains(&"confidence40_event_reaction_reversal_lift"));

        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["prediction_confidence_alpha_lift_family"] == "nonlinear_regime"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_post_return_curve_overlay_v1"
                && trial["prediction_confidence_alpha_lift_family"] == "event_post_curve"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_reaction_segments_overlay_v1"
                && trial["prediction_confidence_alpha_lift_family"] == "event_reaction"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_reaction_reversal_overlay_v1"
                && trial["prediction_confidence_alpha_lift_family"] == "event_reaction"
        }));

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-v1-201602-202605"
            );
            assert!(
                matches!(
                    seed["prediction_min_percentile"].as_str(),
                    Some("0.35" | "0.40")
                ),
                "FO should preserve a confidence gate around FN's low-gap anchor"
            );
            assert!(
                matches!(seed["rebalance"].as_str(), Some("160" | "200")),
                "FO should keep longer holding periods to avoid rebuilding high-turnover trials"
            );
            assert!(
                matches!(
                    seed["rebalance_hysteresis_pct"].as_str(),
                    Some("0.01" | "0.02")
                ),
                "FO should keep hysteresis from FN"
            );
            assert!(
                matches!(
                    seed["partial_rebalance_ratio"].as_str(),
                    Some("0.65" | "0.75")
                ),
                "FO should keep partial rebalance smoothing from FN"
            );
            assert!(
                matches!(
                    seed["execution_impact_budget"].as_str(),
                    Some("impact_turnover_15pct_v1" | "impact_turnover_20pct_v1")
                ),
                "FO should keep impact-aware execution budgets"
            );
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_long_horizon_low_turnover_profile_uses_h60_overlay() {
        let config =
            LayeredSearchConfig::professional_prediction_long_horizon_low_turnover_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FP should use long-horizon PIT prediction as an overlay in factor_combo seeds, not standalone model_prediction trials"
        );
        assert_eq!(
            config.rebalance_days,
            vec![200, 240],
            "FP should stay on low-turnover holding periods instead of rebuilding high-turnover prediction trials"
        );
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![Decimal::new(2, 2), Decimal::new(3, 2)]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::new(65, 2), Decimal::new(60, 2)]
        );
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["impact_turnover_15pct_v1".to_string()]
        );
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1".to_string()]
        );

        let profiles = config
            .seed_trials
            .iter()
            .map(|trial| {
                trial["prediction_long_horizon_low_turnover_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"h60_confidence40_low_turnover_quality"));
        assert!(profiles.contains(&"h60_confidence50_low_turnover_quality"));
        assert!(profiles.contains(&"h60_confidence40_gross65_signal_fidelity"));

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
            );
            assert!(
                matches!(
                    seed["prediction_min_percentile"].as_str(),
                    Some("0.40" | "0.50")
                ),
                "FP should require a high-confidence long-horizon prediction gate"
            );
            assert!(
                matches!(seed["rebalance"].as_str(), Some("200" | "240")),
                "FP should preserve low-turnover timing"
            );
            assert!(
                matches!(
                    seed["rebalance_hysteresis_pct"].as_str(),
                    Some("0.02" | "0.03")
                ),
                "FP should keep a material hysteresis band"
            );
            assert!(
                matches!(
                    seed["partial_rebalance_ratio"].as_str(),
                    Some("0.60" | "0.65")
                ),
                "FP should keep partial rebalance smoothing"
            );
            assert_eq!(seed["execution_impact_budget"], "impact_turnover_15pct_v1");
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }

        let gross65 = config
            .seed_trials
            .iter()
            .find(|trial| {
                trial["prediction_long_horizon_low_turnover_profile"]
                    == "h60_confidence40_gross65_signal_fidelity"
            })
            .expect("FP should include a lower gross exposure signal-fidelity neighbor");
        assert_eq!(gross65["max_gross_exposure"], "0.65");
    }

    #[test]
    fn professional_prediction_long_horizon_regime_alpha_profile_keeps_h60_overlay_and_adds_return_sources(
    ) {
        let config =
            LayeredSearchConfig::professional_prediction_long_horizon_regime_alpha_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FR should use H60 prediction as a PIT overlay, not standalone model_prediction trials"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_event_window_return_sharpe_router_v4".to_string()));

        let profiles = config
            .seed_trials
            .iter()
            .map(|trial| {
                trial["prediction_long_horizon_regime_alpha_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"h60_value_recovery_regime_alpha"));
        assert!(profiles.contains(&"h60_event_post_curve_regime_alpha"));
        assert!(profiles.contains(&"h60_relative_strength_regime_alpha"));
        assert!(
            config.seed_trials.len() >= 8,
            "FR should provide enough H60 overlay seeds for tiny 8-trial strict smoke without falling back to generic cartesian trials"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
            );
            assert_eq!(seed["prediction_label_horizon_days"], 60);
            assert!(
                matches!(seed["rebalance"].as_str(), Some("160" | "180" | "200")),
                "FR should stay in long-horizon, low-turnover timing"
            );
            assert!(
                matches!(
                    seed["prediction_min_percentile"].as_str(),
                    Some("0.35" | "0.40" | "0.45")
                ),
                "FR should keep H60 confidence gating"
            );
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            assert!(
                seed["prediction_long_horizon_regime_alpha_family"].is_string(),
                "FR should tag alpha source family for attribution"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_h60_nonlinear_stress_discovery_profile_combines_ranking_and_fill() {
        let config =
            LayeredSearchConfig::professional_prediction_h60_nonlinear_stress_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FW should use H60 prediction as a PIT overlay in factor_combo seeds, not standalone model_prediction trials"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_mixed_orthogonal_risk_memory_router_v3".to_string()));
        assert_eq!(config.rebalance_days, vec![180, 220, 260]);
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1"]
        );
        assert!(config
            .candidate_ranking_profiles
            .contains(&"relative_strength_alpha_liquidity_v1".to_string()));
        assert!(config
            .candidate_ranking_profiles
            .contains(&"alpha_first_low_impact_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(
            config.seed_trials.len() >= 8,
            "FW needs enough seed trials for an 8-trial strict smoke without generic cartesian spillover"
        );

        let profiles = config
            .seed_trials
            .iter()
            .map(|trial| {
                trial["prediction_h60_nonlinear_stress_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"h60_fex_relative_strength_rank_fill70"));
        assert!(profiles.contains(&"h60_fex_alpha_first_value_recovery_fill60"));
        assert!(profiles.contains(&"h60_raex_nonlinear_quality_rank_fill70"));

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
            );
            assert_eq!(seed["prediction_label_horizon_days"], 60);
            assert!(
                matches!(seed["rebalance"].as_str(), Some("180" | "220" | "260")),
                "FW should stay close to H60 holding periods"
            );
            assert!(
                matches!(
                    seed["candidate_ranking"].as_str(),
                    Some(
                        "relative_strength_alpha_liquidity_v1"
                            | "alpha_first_low_impact_v1"
                            | "capacity_aware_alpha_liquidity_v1"
                    )
                ),
                "FW should test train-window ranking rather than raw score order only"
            );
            assert!(
                matches!(
                    seed["capacity_risk_budget"].as_str(),
                    Some(
                        "capacity_stress_participation_alpha_headroom_floor_60_v1"
                            | "capacity_stress_participation_alpha_headroom_floor_70_v1"
                            | "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                    )
                ),
                "FW should keep stress-fill-aware capacity controls in every seed"
            );
            assert!(
                matches!(
                    seed["cash_utilization"].as_str(),
                    Some("stress_fill_gross_98_v1" | "fillable_gross_95_v1")
                ),
                "FW should keep final fill observable during train-window robustness"
            );
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            assert!(
                seed["prediction_h60_label_family"].is_string(),
                "FW should tag whether the intended H60 overlay is excess or risk-adjusted excess"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_h120_low_impact_stress_discovery_profile_extends_horizon_without_relaxing_gates(
    ) {
        let config =
            LayeredSearchConfig::professional_prediction_h120_low_impact_stress_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FZ should use H120 prediction as a PIT overlay in factor_combo seeds, not standalone model_prediction trials"
        );
        assert_eq!(config.rebalance_days, vec![240, 300, 360]);
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1"]
        );
        assert!(config
            .candidate_ranking_profiles
            .contains(&"alpha_first_low_impact_v1".to_string()));
        assert!(config
            .candidate_ranking_profiles
            .contains(&"capacity_aware_alpha_liquidity_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(
            config.seed_trials.len() >= 8,
            "FZ needs enough seed trials for an 8-trial strict smoke without generic cartesian spillover"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-h120-v1-201602-202605"
            );
            assert_eq!(seed["prediction_label_horizon_days"], 120);
            assert!(
                matches!(seed["rebalance"].as_str(), Some("240" | "300" | "360")),
                "FZ should lengthen holding/rebalance cadence with H120 labels"
            );
            assert!(
                matches!(
                    seed["candidate_ranking"].as_str(),
                    Some("alpha_first_low_impact_v1" | "capacity_aware_alpha_liquidity_v1")
                ),
                "FZ should prioritize low-impact/capacity-aware ranking"
            );
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            assert!(
                matches!(
                    seed["cash_utilization"].as_str(),
                    Some("stress_fill_gross_98_v1" | "fillable_gross_95_v1")
                ),
                "FZ must keep fill observability in train-window robustness"
            );
            assert!(
                seed["prediction_h120_low_impact_stress_profile"].is_string(),
                "FZ should tag H120 low-impact profile for attribution"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_train_window_nonlinear_ranking_discovery_profile_is_prediction_free_and_stress_aware(
    ) {
        let config =
            LayeredSearchConfig::professional_train_window_nonlinear_ranking_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FX should not depend on standalone prediction_set trials; it is a train-window ranking rebuild profile"
        );
        assert!(config
            .candidate_ranking_profiles
            .contains(&"nonlinear_regime_alpha_liquidity_v2".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert_eq!(
            config.execution_carry_policy_profiles,
            vec!["roll_forward_v1"]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(
            config.seed_trials.len() >= 8,
            "FX needs enough native train-window seeds for an 8-trial strict smoke"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["candidate_ranking"],
                "nonlinear_regime_alpha_liquidity_v2"
            );
            assert!(seed["train_window_nonlinear_ranking_profile"].is_string());
            assert!(
                seed.get("prediction_set_id").is_none(),
                "FX seeds must not smuggle H60 prediction overlays into train-window ranking trials"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_train_window_stress_fill_target_exposure_profile_is_prediction_free_and_keeps_exposure_floor(
    ) {
        let config =
            LayeredSearchConfig::professional_train_window_stress_fill_target_exposure_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "GA should be native train-window construction, not a prediction overlay profile"
        );
        assert_eq!(config.rebalance_days, vec![240, 300, 360]);
        assert_eq!(
            config.cash_utilization_profiles,
            vec!["stress_fill_gross_98_v1"]
        );
        assert!(config
            .candidate_ranking_profiles
            .contains(&"nonlinear_regime_alpha_liquidity_v2".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_alpha_headroom_floor_70_v1".to_string()));
        assert!(config.capacity_risk_budget_profiles.contains(
            &"capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string()
        ));
        assert!(
            config.seed_trials.len() >= 8,
            "GA needs enough native stress-fill target-exposure seeds for an 8-trial strict smoke"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(
                seed["candidate_ranking"],
                "nonlinear_regime_alpha_liquidity_v2"
            );
            assert_eq!(seed["cash_utilization"], "stress_fill_gross_98_v1");
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            assert_eq!(seed["execution_impact_budget"], "impact_turnover_15pct_v1");
            assert!(seed["train_window_stress_fill_target_exposure_profile"].is_string());
            assert!(
                matches!(
                    seed["capacity_risk_budget"].as_str(),
                    Some(
                        "capacity_stress_participation_alpha_headroom_floor_60_v1"
                            | "capacity_stress_participation_alpha_headroom_floor_70_v1"
                            | "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                    )
                ),
                "GA should force stress-fill-aware target exposure construction"
            );
            assert!(
                matches!(seed["rebalance"].as_str(), Some("240" | "300")),
                "GA seeds should use longer-hold train-window construction"
            );
            assert!(
                seed.get("prediction_set_id").is_none(),
                "GA seeds must not smuggle prediction overlays into native train-window construction"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_train_window_ml_stress_fill_discovery_profile_is_train_scoped_and_stress_fill_aware(
    ) {
        let config =
            LayeredSearchConfig::professional_train_window_ml_stress_fill_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "GB must build train-window ML artifacts per WFA window, not reuse standalone prediction sets"
        );
        assert_eq!(
            config.cash_utilization_profiles,
            vec!["stress_fill_gross_98_v1"]
        );
        assert!(config.capacity_risk_budget_profiles.contains(
            &"capacity_stress_participation_blended_alpha_headroom_floor_70_v1".to_string()
        ));
        assert!(
            config.seed_trials.len() >= 8,
            "GB needs enough train-window ML stress-fill seeds for an 8-trial strict smoke"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["cash_utilization"], "stress_fill_gross_98_v1");
            assert_eq!(seed["execution_carry_policy"], "roll_forward_v1");
            assert!(
                matches!(
                    seed["train_window_ml_label_objective"].as_str(),
                    Some("regime_conditional_excess_return" | "future_excess_return")
                ),
                "GB train-window ML seeds must use parser-supported label objectives"
            );
            assert_eq!(
                seed["train_window_ml_pit_policy"],
                "train-window rolling fit; no OOS labels"
            );
            assert!(seed["train_window_ml_ranking_profile"].is_string());
            assert_eq!(
                seed["train_window_ml_feature_profile"],
                "phase7_gb_quality_value_recovery_low_impact_v5"
            );
            assert!(seed["stress_fill_objective_profile"].is_string());
            assert_eq!(
                seed["stress_fill_confidence_exposure"],
                "prediction_confidence_ascending_capacity_headroom_v1",
                "GB stress-fill construction should turn train-window ML score confidence into target exposure shaping with low-score-is-better alignment"
            );
            assert_eq!(seed["alpha_source_family"], "train_window_ml_stress_fill");
            assert_eq!(
                seed["prediction_confidence_gate_profile"],
                "train_positive_raw_score_gate_v1"
            );
            assert!(
                matches!(
                    seed["train_window_ml_prediction_min_score"].as_str(),
                    Some("0.00" | "0.01")
                ),
                "GB should filter weak/negative train-window ML predictions before stress-fill construction"
            );
            assert!(
                seed.get("prediction_set_id").is_none(),
                "GB seeds must not smuggle fixed prediction sets into train-window ML ranking"
            );
            assert!(
                seed.get("prediction_min_score").is_none(),
                "GB should only materialize prediction_min_score after per-window train prediction set exists"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_current_event_nonlinear_alpha_discovery_profile_uses_only_current_event_sources(
    ) {
        let config =
            LayeredSearchConfig::professional_current_event_nonlinear_alpha_discovery_default();

        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "FS should be a native event/nonlinear alpha profile, not another prediction overlay"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_mixed_orthogonal_risk_memory_router_v3".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_event_window_return_sharpe_router_v4".to_string()));

        let profiles = config
            .seed_trials
            .iter()
            .map(|trial| {
                trial["current_event_nonlinear_alpha_profile"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"cm_event_surprise_boost_p75"));
        assert!(profiles.contains(&"cm_event_surprise_boost_p90"));
        assert!(profiles.contains(&"cl_event_window_40d_exclude_negative"));
        assert!(profiles.contains(&"cl_residual_confirm_router"));
        assert!(
            config.seed_trials.len() >= 8,
            "FS should provide enough current-event seeds for an 8-trial smoke without generic cartesian spillover"
        );

        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert!(
                seed.get("prediction_set_id").is_none(),
                "FS should not inherit H60 or short-horizon prediction overlays"
            );
            assert_ne!(
                seed["combo_name"], "phase7_quality_event_surprise_confirm_v1",
                "FS should not use the low-coverage event surprise confirmation combo as a base"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("event_reaction"));
            assert!(!serialized.contains("post_return_curve"));
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(
                seed["current_event_nonlinear_alpha_family"].is_string(),
                "FS should tag alpha source family for attribution"
            );
        }
    }

    #[test]
    fn professional_execution_bull_sleeve_cash_recovery_profile_extends_ec_anchor() {
        let config =
            LayeredSearchConfig::professional_execution_bull_sleeve_cash_recovery_default();

        assert_eq!(
            config.combo_versions,
            vec![
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
                ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
                ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0")
            ]
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_state_alpha_selector_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_nonlinear_alpha_risk_memory_router_v3".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_eu should seed the nearest-candidate recovery anchor first");
        assert_eq!(first_seed["combo_name"], "phase7_financial_quality_v1");
        assert_eq!(first_seed["score_direction"], "ascending");
        assert_eq!(first_seed["market_regime"], "quality_bear_window_guard_v2");
        assert_eq!(
            first_seed["event_gate_profile"],
            "valuation_exclude_bottom40"
        );
        assert_eq!(
            first_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(first_seed["candidate_risk_filter"], "off");
        assert_eq!(first_seed["risk_contribution_control"], "off");
        assert_eq!(first_seed["cash_utilization"], "off");
        assert_eq!(first_seed["execution_impact_budget"], "off");
        assert_eq!(
            first_seed["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial["cash_utilization"] == "off"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_execution_return_first_fill_repair_profile_bridges_return_and_fill() {
        let config = LayeredSearchConfig::professional_execution_return_first_fill_repair_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"off".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_participation_balanced_v1".to_string()));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ey should seed a return-first fill-repair profile first");
        assert_eq!(first_seed["combo_name"], "phase7_financial_quality_v1");
        assert_eq!(first_seed["market_regime"], "quality_bear_window_guard_v2");
        assert_eq!(
            first_seed["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(first_seed["candidate_risk_filter"], "off");
        assert_eq!(first_seed["risk_contribution_control"], "off");
        assert_eq!(first_seed["cash_utilization"], "stress_fill_gross_98_v1");
        assert_eq!(first_seed["execution_schedule_profile"], "twap_10d_v1");
        assert_eq!(first_seed["top_n"], 60);
        assert_eq!(first_seed["max_position_pct"], "0.10");
        assert_eq!(
            first_seed["return_first_fill_repair_profile"],
            "return_first_fill_anchor1_top60_stress_fill"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial["top_n"] == 80
                && trial["capacity_risk_budget"] == "capacity_participation_balanced_v1"
        }));
    }

    #[test]
    fn professional_execution_oos_regime_alpha_rebuild_profile_targets_train_return_gap() {
        let config = LayeredSearchConfig::professional_execution_oos_regime_alpha_rebuild_default();

        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );
        assert_eq!(config.cash_utilization_profiles, vec!["off".to_string()]);
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["off".to_string()]
        );
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_22_65_100"));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ev should seed the train-window return-gap repair first");
        assert_eq!(
            first_seed["oos_regime_alpha_rebuild_profile"],
            "oos_rebuild_quality_bull_top30_vol22"
        );
        assert_eq!(
            first_seed["market_regime"],
            "quality_state_alpha_selector_v1"
        );
        assert_eq!(
            first_seed["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(first_seed["portfolio_volatility_min_exposure"], "0.65");
        assert_eq!(first_seed["candidate_risk_filter"], "off");
        assert_eq!(first_seed["risk_contribution_control"], "off");
        assert_eq!(first_seed["cash_utilization"], "off");
        assert_eq!(first_seed["execution_impact_budget"], "off");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
        }));
    }

    #[test]
    fn professional_execution_oos_benchmark_excess_rebuild_profile_targets_excess_gap() {
        let config =
            LayeredSearchConfig::professional_execution_oos_benchmark_excess_rebuild_default();

        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_26_75_100"));
        assert_eq!(
            config.candidate_ranking_profiles,
            vec!["capacity_aware_alpha_liquidity_v1".to_string()]
        );
        assert_eq!(config.cash_utilization_profiles, vec!["off".to_string()]);
        assert_eq!(
            config.execution_impact_budget_profiles,
            vec!["off".to_string()]
        );

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ew should seed the benchmark-excess repair first");
        assert_eq!(
            first_seed["oos_benchmark_excess_rebuild_profile"],
            "oos_excess_event_nonlinear_top100_vol24"
        );
        assert_eq!(
            first_seed["combo_name"],
            "phase7_quality_event_window_overlay_v1"
        );
        assert_eq!(
            first_seed["market_regime"],
            "quality_nonlinear_alpha_risk_memory_router_v3"
        );
        assert_eq!(
            first_seed["portfolio_volatility_control"],
            "vol120_24_70_100"
        );
        assert_eq!(first_seed["top_n"], 100);
        assert_eq!(first_seed["rebalance"], "120");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_26_75_100"
                && trial["candidate_risk_filter"] == "off"
                && trial["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn professional_execution_oos_execution_adaptive_rebuild_profile_targets_fill_and_sharpe_gap() {
        let config =
            LayeredSearchConfig::professional_execution_oos_execution_adaptive_rebuild_default();

        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(|profile| profile.profile_name == "off"));
        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(|profile| profile.profile_name == "gentle_roll_sharpe180_025_neg20_70"));
        assert!(config
            .execution_schedule_profiles
            .contains(&"twap_10d_v1".to_string()));
        assert!(config
            .execution_carry_policy_profiles
            .contains(&"roll_forward_v1".to_string()));
        assert!(config
            .cash_utilization_profiles
            .contains(&"stress_fill_gross_98_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_stress_participation_headroom_floor_70_v1".to_string()));

        let first_seed = config
            .seed_trials
            .first()
            .expect("phase7_ex should seed the EV near-pass candidate with execution recovery");
        assert_eq!(
            first_seed["oos_execution_adaptive_rebuild_profile"],
            "oos_execution_adaptive_event_top100_vol24"
        );
        assert_eq!(
            first_seed["oos_benchmark_excess_rebuild_profile"],
            "oos_excess_event_nonlinear_top100_vol24"
        );
        assert_eq!(
            first_seed["portfolio_sharpe_control"],
            "gentle_roll_sharpe180_025_neg20_70"
        );
        assert_eq!(
            first_seed["capacity_risk_budget"],
            "capacity_stress_participation_headroom_floor_70_v1"
        );
        assert_eq!(first_seed["execution_schedule_profile"], "twap_10d_v1");
        assert_eq!(first_seed["execution_carry_policy"], "roll_forward_v1");
        assert!(config
            .seed_trials
            .iter()
            .filter(|trial| trial["portfolio_sharpe_control"] == "off")
            .all(|trial| trial.get("portfolio_sharpe_reduce_full").is_none()
                && trial.get("portfolio_sharpe_reduce_start").is_none()));
    }

    #[test]
    fn candidate_screening_keeps_low_return_result_as_defensive() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(209, 4),
            excess_return: Decimal::new(-2937, 4),
            sharpe: Decimal::new(18, 2),
            max_drawdown: Decimal::new(23, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::Defensive
        );
    }

    #[test]
    fn candidate_screening_sorts_professional_before_defensive() {
        let results = vec![
            json!({
                "candidate_id": "weak",
                "best_trial": {
                    "metrics": {
                        "annual_return_pct": "0.02",
                        "excess_return_pct": "-0.10",
                        "sharpe_ratio": "0.10",
                        "sortino_ratio": "0.12",
                        "max_drawdown_pct": "0.20",
                        "num_trades": 30
                    }
                },
                "robustness": {"data": {"status": "approved_candidate"}}
            }),
            json!({
                "candidate_id": "strong",
                "best_trial": {
                    "metrics": {
                        "annual_return_pct": "0.16",
                        "excess_return_pct": "0.03",
                        "sharpe_ratio": "1.10",
                        "sortino_ratio": "1.60",
                        "max_drawdown_pct": "0.22",
                        "num_trades": 80
                    }
                },
                "robustness": {"data": {"status": "approved_candidate"}}
            }),
        ];

        let rows = screen_optimization_results(&results, &CandidateTargets::default());

        assert_eq!(rows[0].candidate_id, "strong");
        assert_eq!(rows[0].candidate_type, CandidateType::Professional);
        assert_eq!(rows[1].candidate_type, CandidateType::Defensive);
    }

    #[test]
    fn layered_search_plan_respects_local_resource_budget() {
        let config = LayeredSearchConfig {
            market_regime_policies: vec!["off".to_string(), "professional_default".to_string()],
            combo_versions: vec![
                ComboVersion::new("full_icir_16f_v3", "1.0.0"),
                ComboVersion::new("phase7_price_volume_expanded_v1", "1.0.0"),
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ],
            prediction_set_ids: Vec::new(),
            top_n: vec![20, 30],
            rebalance_days: vec![10, 20],
            score_directions: vec![ScoreDirection::Descending, ScoreDirection::Ascending],
            skip_top_pct: vec![Decimal::ZERO],
            max_pairwise_correlation: vec![Decimal::new(75, 2)],
            kelly_fraction: vec![Decimal::new(25, 2)],
            max_position_pct: vec![Decimal::new(8, 2)],
            max_gross_exposure: vec![Decimal::ONE],
            portfolio_methods: vec!["heuristic".to_string(), "risk_budget".to_string()],
            risk_budget_lookback_days: vec![60],
            capacity_penalty_strength: vec![Decimal::ZERO],
            capacity_risk_budget_profiles: vec!["off".to_string()],
            cash_utilization_profiles: vec!["off".to_string()],
            execution_impact_budget_profiles: vec!["off".to_string()],
            execution_schedule_profiles: vec!["immediate".to_string()],
            execution_carry_policy_profiles: vec!["expire".to_string()],
            cost_capacity_stress_profiles: vec![CostCapacityStressProfile::off()],
            industry_max_weight_pct: vec![None],
            style_risk_budget_profiles: vec!["off".to_string()],
            candidate_risk_filter_profiles: vec!["off".to_string()],
            candidate_ranking_profiles: vec!["off".to_string()],
            risk_contribution_control_profiles: vec!["off".to_string()],
            stress_fill_confidence_exposure_profiles: vec!["off".to_string()],
            rebalance_hysteresis_pct: vec![Decimal::ZERO],
            partial_rebalance_ratio: vec![Decimal::ONE],
            score_candidate_pool_sizes: vec![0, 200],
            universe_profiles: vec!["all".to_string(), "listed_non_st".to_string()],
            portfolio_drawdown_controls: vec![
                PortfolioDrawdownControlProfile::off(),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_10_25_50",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                ),
            ],
            portfolio_volatility_controls: vec![
                PortfolioVolatilityControlProfile::off(),
                PortfolioVolatilityControlProfile::target(
                    "vol120_18_50_100",
                    Decimal::new(18, 2),
                    120,
                    Decimal::new(50, 2),
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
        };
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 5;

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(plan.requested_trials, 1536);
        assert_eq!(plan.trials.len(), 5);
        assert!(plan.truncated);
        assert_eq!(plan.trials[0].trial_id, "phase7d-000001");
        assert_eq!(plan.trials[0].parameters["combo_name"], "full_icir_16f_v3");
        assert_eq!(plan.trials[0].parameters["market_regime"], "off");
        assert_eq!(plan.trials[0].parameters["universe_profile"], "all");
        assert!(plan.trials[0].parameters["industry_max_weight_pct"].is_null());
        assert_eq!(
            plan.trials[0].parameters["portfolio_drawdown_control"],
            "off"
        );
        assert_eq!(
            plan.trials[0].parameters["portfolio_volatility_control"],
            "off"
        );
        let market_regimes = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["market_regime"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(market_regimes.contains("professional_default"));
        assert_eq!(plan.trials[0].parameters["rebalance"], "10");
        assert_eq!(plan.trials[0].parameters["score_direction"], "descending");
    }

    #[test]
    fn default_layered_search_config_includes_professional_axes() {
        let config = LayeredSearchConfig::local_professional_default();

        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_price_volume_expanded_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_financial_quality_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_relative_strength_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_relative_strength_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_growth_recovery_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_valuation_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_moneyflow_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_value_quality_growth_rel_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_value_recovery_confirm_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_value_tilt_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_quality_growth_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_defensive_rel_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_recovery_tilt_v1"));
        assert!(config.top_n.contains(&30));
        assert!(config.rebalance_days.contains(&20));
        assert!(config
            .score_directions
            .contains(&ScoreDirection::Descending));
        assert!(config.market_regime_policies.contains(&"off".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"professional_default".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"drawdown_control_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"drawdown_control_v2".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_risk_off_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v1".to_string()));
        assert!(config.kelly_fraction.contains(&Decimal::new(25, 2)));
        assert!(config
            .max_pairwise_correlation
            .contains(&Decimal::new(75, 2)));
        assert!(config
            .portfolio_methods
            .contains(&"risk_budget".to_string()));
        assert!(config.risk_budget_lookback_days.contains(&120));
        assert!(config
            .capacity_penalty_strength
            .contains(&Decimal::new(75, 2)));
        assert_eq!(
            config.cost_capacity_stress_profiles,
            vec![CostCapacityStressProfile::off()]
        );
        assert!(config.score_candidate_pool_sizes.contains(&0));
        assert!(config.score_candidate_pool_sizes.contains(&500));
        assert!(config.universe_profiles.contains(&"all".to_string()));
        assert!(config
            .universe_profiles
            .contains(&"listed_non_st".to_string()));
        assert!(config
            .universe_profiles
            .contains(&"main_board_non_st".to_string()));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling252_10_25_50"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling252_12_30_60"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling504_15_35_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_25_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_27_50_30_70"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol252_16_45_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_18_50_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol60_20_60_100"));
    }

    #[test]
    fn layered_search_plan_emits_cost_capacity_stress_parameters() {
        let mut config = LayeredSearchConfig::local_professional_default();
        config.cost_capacity_stress_profiles = vec![CostCapacityStressProfile::impact_limited(
            "impact_2pct_participation_10pct",
            Decimal::new(2, 2),
            Decimal::new(10, 2),
        )];
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 1;

        let plan = build_layered_search_plan(&config, &resource_plan);
        let params = &plan.trials[0].parameters;

        assert_eq!(
            params["cost_capacity_stress_profile"],
            "impact_2pct_participation_10pct"
        );
        assert_eq!(params["cost_model"]["impact_cost_coefficient"], 0.02);
        assert_eq!(params["execution_rules"]["max_participation_rate"], 0.10);
    }

    #[test]
    fn professional_breakthrough_config_focuses_on_high_return_neighborhood() {
        let config = LayeredSearchConfig::professional_breakthrough_default();

        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_financial_quality_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_moneyflow_pos_5pct_v1"));
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config.top_n.contains(&20));
        assert!(config.rebalance_days.contains(&60));
        assert!(config.max_position_pct.contains(&Decimal::new(15, 2)));
        assert!(config
            .portfolio_methods
            .contains(&"risk_budget".to_string()));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_25_50_30_70"));
        assert!(config.seed_trials.len() >= 3);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["risk_budget_lookback_days"] == 120
                && trial["capacity_penalty_strength"] == "0.75"
        }));
    }

    #[test]
    fn professional_risk_breakthrough_config_focuses_on_drawdown_sortino_neighborhood() {
        let config = LayeredSearchConfig::professional_risk_breakthrough_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(combo_names.contains("phase7_blend_value_tilt_v1"));
        assert!(combo_names.contains("phase7_blend_quality_growth_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert!(combo_names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(combo_names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v1".to_string()));
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config.top_n.contains(&20));
        assert!(config.top_n.contains(&25));
        assert!(config.rebalance_days.contains(&60));
        assert!(config.max_position_pct.contains(&Decimal::new(10, 2)));
        assert!(config.max_gross_exposure.contains(&Decimal::new(90, 2)));
        assert!(config.risk_budget_lookback_days.contains(&180));
        assert!(config.capacity_penalty_strength.contains(&Decimal::ONE));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_08_25_60_25_75"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_30_55_25_75"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_26_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_24_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_09_26_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_09_24_45_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_08_24_45_30_70"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_22_65_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_24_70_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_30_85_100"));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["portfolio_drawdown_control"] == "recover252_08_25_60_25_75"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["max_position_pct"] == "0.10"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_30_85_100"
                && trial["max_gross_exposure"] == "1"
                && trial["portfolio_volatility_min_exposure"] == "0.85"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "vol60_30_90_100"
                && trial["max_gross_exposure"] == "1"
                && trial["portfolio_volatility_min_exposure"] == "0.90"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["max_gross_exposure"] == "1"
                && trial["industry_max_weight_pct"].is_null()
                && trial.get("portfolio_volatility_target_pct").is_none()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_26_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.14"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v3"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.02"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.05"
                && trial["prediction_min_percentile"] == "0.20"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["trailing_stop_pct"] == "0.18"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.12"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_25_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.14"
        }));
        assert!(config
            .position_risk_controls
            .iter()
            .any(|profile| profile.profile_name == "stop_loss_12"
                && profile.stop_loss_pct == Some(Decimal::new(12, 2))));
        for (profile_name, cooldown_days) in [
            ("stop_loss_07_cooldown_5", 5),
            ("stop_loss_07_cooldown_10", 10),
            ("stop_loss_07_cooldown_20", 20),
            ("stop_loss_07_cooldown_30", 30),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name
                    && profile.stop_loss_pct == Some(Decimal::new(7, 2))
                    && profile.reentry_cooldown_days == Some(cooldown_days)
            }));
        }
        for (profile_name, stop_loss_pct, cooldown_days) in [
            ("stop_loss_065_cooldown_20", Decimal::new(65, 3), 20),
            ("stop_loss_075_cooldown_20", Decimal::new(75, 3), 20),
            ("stop_loss_075_cooldown_30", Decimal::new(75, 3), 30),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name
                    && profile.stop_loss_pct == Some(stop_loss_pct)
                    && profile.reentry_cooldown_days == Some(cooldown_days)
            }));
        }
        for (profile_name, stop_loss_pct) in [
            ("stop_loss_07", Decimal::new(7, 2)),
            ("stop_loss_08", Decimal::new(8, 2)),
            ("stop_loss_085", Decimal::new(85, 3)),
            ("stop_loss_09", Decimal::new(9, 2)),
            ("stop_loss_095", Decimal::new(95, 3)),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name && profile.stop_loss_pct == Some(stop_loss_pct)
            }));
        }
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["max_position_pct"] == "0.13"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_drawdown_control"] == "recover252_08_24_45_30_70"
                && trial["max_gross_exposure"] == "0.90"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_moneyflow_pos_5pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_blend_defensive_rel_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_blend_recovery_tilt_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 10
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"] == "0.20"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"] == "0.25"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.065"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_gross_exposure"] == "0.90"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["max_pairwise_correlation"] == "0.65"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_moneyflow_pos_5pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.09"
        }));
    }

    #[test]
    fn professional_sharpe_stabilization_profile_starts_from_u2_exact_anchor() {
        let risk_config = LayeredSearchConfig::professional_risk_breakthrough_default();
        let config = LayeredSearchConfig::professional_sharpe_stabilization_default();

        assert!(config.seed_trials.len() < risk_config.seed_trials.len());
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(config.seed_trials[0]["stop_loss_pct"], "0.075");
        assert_eq!(config.seed_trials[0]["reentry_cooldown_days"], 30);
        assert_eq!(config.seed_trials[0]["rebalance_hysteresis_pct"], "0");
        assert_eq!(config.seed_trials[0]["partial_rebalance_ratio"], "1");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(!config.seed_trials.iter().any(|trial| {
            trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.07"
        }));
    }

    #[test]
    fn professional_sharpe_stabilization_profile_adds_turnover_smoothing_axes() {
        let config = LayeredSearchConfig::professional_sharpe_stabilization_default();

        assert!(config.rebalance_hysteresis_pct.contains(&Decimal::ZERO));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(1, 2)));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(2, 2)));
        assert!(config.partial_rebalance_ratio.contains(&Decimal::ONE));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(75, 2)));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(50, 2)));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["rebalance_hysteresis_pct"] == "0.01"
                && trial["partial_rebalance_ratio"] == "0.75"
        }));
    }

    #[test]
    fn professional_regime_stabilization_profile_focuses_on_state_triggered_risk_controls() {
        let config = LayeredSearchConfig::professional_regime_stabilization_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_crash_guard_v1".to_string(),
                "quality_crash_guard_v2".to_string(),
                "quality_crash_guard_v3".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().all(|trial| {
            matches!(
                trial["market_regime"].as_str(),
                Some("quality_crash_guard_v1")
                    | Some("quality_crash_guard_v2")
                    | Some("quality_crash_guard_v3")
            )
        }));
        assert_eq!(config.max_gross_exposure, vec![Decimal::ONE]);
        assert!(config
            .position_risk_controls
            .iter()
            .any(|profile| profile.profile_name == "stop_loss_075_cooldown_30"));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v3"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["max_pairwise_correlation"] == "0.65"));
    }

    #[test]
    fn professional_bear_window_stabilization_profile_targets_attribution_failure_window() {
        let config = LayeredSearchConfig::professional_bear_window_stabilization_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.max_gross_exposure, vec![Decimal::ONE]);
        assert_eq!(
            config.portfolio_volatility_controls.len(),
            2,
            "U2 keeps only Phase 7-T/U working volatility shapes"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
        }));
    }

    #[test]
    fn professional_style_risk_budget_profile_extends_u2_with_style_axes() {
        let config = LayeredSearchConfig::professional_style_risk_budget_default();

        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
                "defensive_style_budget_v1".to_string(),
            ]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["style_risk_budget"] == "defensive_style_budget_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(4, 16)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["style_risk_budget"] == "liquidity_volatility_balanced_v1"
        }));
    }

    #[test]
    fn professional_residual_quality_profile_targets_industry_residual_quality_source() {
        let config = LayeredSearchConfig::professional_residual_quality_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_industry_residual_quality_v1"));
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["score_direction"] == "ascending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial["score_direction"] == "descending"
        }));
    }

    #[test]
    fn phase7_residual_confirmation_blends_keep_quality_as_core_alpha() {
        let profiles = phase7_alpha_blend_profiles();
        let light = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_residual_confirm_5pct_v1")
            .expect("5pct residual confirmation profile");
        let medium = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_residual_confirm_10pct_v1")
            .expect("10pct residual confirmation profile");

        let light_sources = light
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<Vec<_>>();
        assert_eq!(
            light_sources,
            vec![
                ("phase7_financial_quality_v1", Decimal::new(95, 2)),
                ("phase7_industry_residual_quality_v1", Decimal::new(5, 2)),
            ]
        );

        let medium_sources = medium
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<Vec<_>>();
        assert_eq!(
            medium_sources,
            vec![
                ("phase7_financial_quality_v1", Decimal::new(90, 2)),
                ("phase7_industry_residual_quality_v1", Decimal::new(10, 2)),
            ]
        );
    }

    #[test]
    fn professional_residual_overlay_sharpe_profile_preserves_u2_anchor() {
        let config = LayeredSearchConfig::professional_residual_overlay_sharpe_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(!combo_names.contains("phase7_industry_residual_quality_v1"));
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_residual_confirm_5pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
                && trial["style_risk_budget"] == "liquidity_volatility_balanced_v1"
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["combo_name"] == "phase7_industry_residual_quality_v1"));
    }

    #[test]
    fn professional_conditioned_second_alpha_profile_uses_gates_not_linear_overlay() {
        let config = LayeredSearchConfig::professional_conditioned_second_alpha_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom40"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(40, 2))
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "residual_require_top40"
                && profile.combo_name.as_deref() == Some("phase7_industry_residual_quality_v1")
                && profile.mode.as_deref() == Some("require_positive")
                && profile.min_score == Some(Decimal::new(60, 2))
        }));
        assert_eq!(
            config.seed_trials[0]["event_gate_profile"],
            Value::Null,
            "first seed must keep the U2 anchor as an unchanged control"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "valuation_require_top40"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["event_gate_mode"] == "require_positive"
                && trial["event_gate_min_score"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "moneyflow_require_top40"
                && trial["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
    }

    #[test]
    fn professional_valuation_guard_sharpe_profile_tunes_exclusion_thresholds_only() {
        let config = LayeredSearchConfig::professional_valuation_guard_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().all(|profile| {
            profile.mode.as_deref() != Some("require_positive")
                && profile.combo_name.as_deref() != Some("phase7_moneyflow_v1")
                && profile.combo_name.as_deref() != Some("phase7_industry_residual_quality_v1")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom35"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(35, 2))
        }));
        assert_eq!(
            config.seed_trials[0]["event_gate_profile"],
            Value::Null,
            "first seed must keep the U2 anchor as an unchanged control"
        );
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_min_score"] == "0.40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom50"
                && trial["event_gate_mode"] == "exclude_negative"
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["event_gate_mode"] == "require_positive"));
    }

    #[test]
    fn professional_regime_conditioned_valuation_guard_profile_scopes_guard_to_stress_regimes() {
        let config = LayeredSearchConfig::professional_regime_conditioned_valuation_guard_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom35_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(35, 2))
                && profile.active_regimes.as_slice() == ["bear", "high_volatility"]
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35_stress_only"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40_stress_only"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_min_score"] == "0.40"
        }));
    }

    #[test]
    fn professional_regime_alpha_routing_profile_seeds_regime_specific_alpha_trials() {
        let config = LayeredSearchConfig::professional_regime_alpha_routing_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_regime_alpha_sleeve_search_profile_seeds_multiple_stress_alpha_sources() {
        let config = LayeredSearchConfig::professional_regime_alpha_sleeve_search_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_value_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_recovery_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_blend_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_value_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_recovery_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_blend_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
    }

    #[test]
    fn professional_regime_alpha_overlay_search_profile_seeds_small_stress_overlays() {
        let config = LayeredSearchConfig::professional_regime_alpha_overlay_search_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_value_05pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_blend_10pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_value_10pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_regime_alpha_sleeve_allocation_profile_seeds_portfolio_sleeves() {
        let config = LayeredSearchConfig::professional_regime_alpha_sleeve_allocation_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_blend_10pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_low_risk_sleeve_profile_seeds_low_risk_portfolio_sleeves() {
        let config = LayeredSearchConfig::professional_low_risk_sleeve_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_value_guard_sleeve_composition_profile_combines_fallback_components() {
        let config = LayeredSearchConfig::professional_value_guard_sleeve_composition_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config
            .seed_trials
            .iter()
            .all(|trial| trial["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn professional_nearest_candidate_risk_model_profile_searches_risk_model_neighbors() {
        let config = LayeredSearchConfig::professional_nearest_candidate_risk_model_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()
            ]
        );
        assert_eq!(
            config.portfolio_methods,
            vec!["risk_budget".to_string(), "min_variance".to_string()]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![120, 180]);
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["portfolio_method"] == "min_variance"
                && trial["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn professional_event_regime_sleeve_profile_searches_event_sleeve_neighbors() {
        let config = LayeredSearchConfig::professional_event_regime_sleeve_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string()));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_sleeve_weight_profile_searches_weight_neighbors() {
        let config = LayeredSearchConfig::professional_event_window_sleeve_weight_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_sleeve_upper_bound_profile_searches_upper_bound_weight() {
        let config = LayeredSearchConfig::professional_event_window_sleeve_upper_bound_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_regime_placement_profile_searches_regime_routes() {
        let config = LayeredSearchConfig::professional_event_window_regime_placement_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1".to_string()
        ));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
    }

    #[test]
    fn professional_event_window_decay_profile_searches_short_and_long_decay_variants() {
        let config = LayeredSearchConfig::professional_event_window_decay_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1".to_string()
        ));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
    }

    #[test]
    fn professional_event_surprise_nonlinear_profile_keeps_search_narrow_and_stress_conditioned() {
        let config = LayeredSearchConfig::professional_event_surprise_nonlinear_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            combo_names,
            std::collections::BTreeSet::from([
                "phase7_financial_quality_v1",
                "phase7_quality_event_surprise_confirm_v1"
            ])
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(config.event_gate_profiles.len(), 5);
        assert!(config.event_gate_profiles.iter().all(|profile| {
            profile.combo_name.as_deref() != Some("phase7_event_window_earnings_v1")
                && (profile.profile_name == "off"
                    || profile.active_regimes.as_slice() == ["bear", "high_volatility"])
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_boost_pos_5pct_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
                && profile.mode.as_deref() == Some("boost_positive")
                && profile.boost_weight == Some(Decimal::new(5, 2))
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_exclude_negative_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
        }));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_combo_name"] != "phase7_event_window_earnings_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_require_positive_stress_only"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
    }

    #[test]
    fn professional_event_quality_segment_profile_keeps_ax_anchor_and_compares_event_quality() {
        let config = LayeredSearchConfig::professional_event_quality_segment_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(config.seed_trials.len(), 4);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1"
        }));
    }

    #[test]
    fn professional_event_strength_segment_profile_uses_min_score_without_widening_grid() {
        let config = LayeredSearchConfig::professional_event_strength_segment_default();

        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config
                .event_gate_profiles
                .iter()
                .map(|profile| profile.profile_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "off",
                "event_window_require_strong_p75",
                "event_window_require_strong_p90",
                "event_surprise_require_strong_p75",
                "event_surprise_require_strong_p90",
                "event_confirm_require_light_p50",
            ]
        );
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_mode"] == "require_positive"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_require_strong_p75"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_require_strong_p90"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.66"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_require_strong_p75"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_min_score"] == "0.35"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_confirm_require_light_p50"
                && trial["event_gate_combo_name"] == "phase7_event_earnings_v1"
                && trial["event_gate_min_score"] == "0.39"
        }));
    }

    #[test]
    fn professional_event_strength_boost_profile_segments_without_hard_filtering() {
        let config = LayeredSearchConfig::professional_event_strength_boost_default();

        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_boost_weight"] != "0"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_boost_strong_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_boost_strong_p75_5pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.05"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_boost_strong_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_min_score"] == "0.35"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_confirm_boost_light_p50_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_earnings_v1"
                && trial["event_gate_min_score"] == "0.39"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
    }

    #[test]
    fn professional_current_anchor_risk_shape_profile_keeps_ax_anchor_and_sweeps_risk_shape() {
        let config = LayeredSearchConfig::professional_current_anchor_risk_shape_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120, 180]);
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["stop_loss_pct"].is_string()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_20_60_100"
                && trial["portfolio_volatility_target_pct"] == "0.20"
                && trial["portfolio_volatility_min_exposure"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_volatility_target_pct"] == "0.16"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_drawdown_control"] == "recover126_08_22_45_30_70"
                && trial["portfolio_drawdown_peak_lookback_days"] == 126
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_risk_control"] == "stop_loss_075_cooldown_45"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 45
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos12_corr65"
                && trial["max_position_pct"] == "0.12"
                && trial["max_pairwise_correlation"] == "0.65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_180"
                && trial["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn professional_current_anchor_position_frontier_profile_interpolates_position_risk() {
        let config = LayeredSearchConfig::professional_current_anchor_position_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(12, 2),
                Decimal::new(13, 2),
                Decimal::new(14, 2),
                Decimal::new(15, 2),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(70, 2),
                Decimal::new(75, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["stop_loss_pct"] == "0.075"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_180"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.75"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr70"
                && trial["max_position_pct"] == "0.14"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "vol16_recover08_maxpos14_corr70"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
    }

    #[test]
    fn professional_current_anchor_weak_window_repair_profile_keeps_bg_anchor_and_uses_stress_only_repairs(
    ) {
        let config = LayeredSearchConfig::professional_current_anchor_weak_window_repair_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_event_window_boost_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.03"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_valuation_boost_p40_5pct"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.40"
                && trial["event_gate_boost_weight"] == "0.05"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_valuation_exclude_p40"
                && trial["event_gate_mode"] == "exclude_negative"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
    }

    #[test]
    fn professional_current_anchor_sharpe_return_bridge_profile_starts_from_high_sharpe_boundary() {
        let config =
            LayeredSearchConfig::professional_current_anchor_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![Decimal::new(65, 2), Decimal::new(70, 2)]
        );
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(14, 2), Decimal::new(15, 2)]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert_eq!(
            config.seed_trials[0]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert_eq!(config.seed_trials[0]["max_position_pct"], "0.14");
        assert_eq!(config.seed_trials[0]["max_pairwise_correlation"], "0.65");
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr65_vol20"
                && trial["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr65_vol22"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos15_corr70_vol20"
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial["position_shape_profile"] == "maxpos14_corr65_vol20_event125"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_return_recovery_profile_bridges_boundary_without_wide_grid() {
        let config = LayeredSearchConfig::professional_high_sharpe_return_recovery_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(675, 3),
                Decimal::new(70, 2)
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert_eq!(
            config.seed_trials[0]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert_eq!(
            config.seed_trials[1]["portfolio_drawdown_control"],
            "recover252_08_22_45_30_70"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_19_58_100"
                && trial["position_shape_profile"] == "maxpos14_corr65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial["event_sleeve_profile"] == "event_window_125pct"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["event_gate_min_score"] == "0.35"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos145_corr65"
                && trial["max_position_pct"] == "0.145"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_risk_memory_bridge_profile_searches_middle_lookbacks() {
        let config = LayeredSearchConfig::professional_risk_memory_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.risk_budget_lookback_days,
            vec![120, 140, 150, 160, 170, 180]
        );
        assert_eq!(
            config.portfolio_volatility_controls[0].profile_name,
            "vol120_18_55_100"
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_08_22_45_30_70"
        );
        assert_eq!(
            config.seed_trials[1]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_140"
                && trial["risk_budget_lookback_days"] == 140
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos145_corr65_rb150"
                && trial["risk_budget_lookback_days"] == 150
                && trial["max_position_pct"] == "0.145"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "valuation45_rb150"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_return_sharpe_router_profile_bridges_return_and_boundary() {
        let config = LayeredSearchConfig::professional_state_return_sharpe_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_event_window_return_sharpe_router_v1".to_string(),
                "quality_event_window_return_sharpe_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v1"
                && trial["position_shape_profile"] == "maxpos14_corr65"
                && trial["max_position_pct"] == "0.14"
                && trial["max_pairwise_correlation"] == "0.65"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v2"
                && trial["risk_budget_lookback_days"] == 160
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["risk_budget_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_return_sharpe_frontier_profile_sweeps_bp_neighborhood() {
        let config = LayeredSearchConfig::professional_state_return_sharpe_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![150, 160, 170, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v1".to_string(),
                "quality_event_window_return_sharpe_router_v2".to_string(),
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["risk_budget_shape_profile"] == "bp_rb160_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_position_sharpe_return_bridge_profile_blends_bq_and_boundary_shapes() {
        let config = LayeredSearchConfig::professional_position_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![150, 160, 170, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(675, 3),
                Decimal::new(70, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(14, 2),
                Decimal::new(145, 3),
                Decimal::new(15, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["position_shape_profile"] == "maxpos14_corr65_router_v4_rb160"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["position_shape_profile"] == "maxpos145_corr65_router_v3_rb160"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["risk_budget_shape_profile"] == "bp_rb170_router_v4"
                && trial["max_pairwise_correlation"] == "0.65"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_moderate_position_sharpe_return_bridge_profile_keeps_return_room() {
        let config =
            LayeredSearchConfig::professional_moderate_position_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(70, 2),
                Decimal::new(725, 3),
                Decimal::new(75, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(16, 2),
                Decimal::new(17, 2),
                Decimal::new(18, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["position_shape_profile"] == "maxpos16_corr70_router_v4_vol120_16_50_100"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["position_shape_profile"] == "maxpos17_corr725_router_v3_vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_correlation_frontier_sharpe_return_profile_sweeps_narrow_corr_band() {
        let config = LayeredSearchConfig::professional_correlation_frontier_sharpe_return_default();

        assert_eq!(
            config.market_regime_policies,
            vec!["quality_event_window_return_sharpe_router_v4".to_string()]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(705, 3),
                Decimal::new(71, 2),
                Decimal::new(715, 3),
                Decimal::new(72, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(16, 2), Decimal::new(165, 3)]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos16_corr705_router_v4_vol120_16_50_100"
                && trial["max_pairwise_correlation"] == "0.705"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos165_corr72_router_v4_vol120_17_52_100"
                && trial["max_position_pct"] == "0.165"
                && trial["max_pairwise_correlation"] == "0.72"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_correlation_threshold_sharpe_return_profile_sweeps_jump_boundary() {
        let config =
            LayeredSearchConfig::professional_correlation_threshold_sharpe_return_default();

        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(706, 3),
                Decimal::new(707, 3),
                Decimal::new(708, 3),
                Decimal::new(709, 3),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos16_corr706_router_v4_vol120_16_50_100"
                && trial["max_pairwise_correlation"] == "0.706"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos165_corr709_router_v4_vol120_17_52_100"
                && trial["max_pairwise_correlation"] == "0.709"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_soft_risk_frontier_sharpe_return_profile_adds_soft_risk_controls() {
        let config = LayeredSearchConfig::professional_soft_risk_frontier_sharpe_return_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "off".to_string(),
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_contribution_control"] == "soft_single_name_15pct_v1"
                && trial["candidate_risk_filter"] == "low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_regime_alpha_selector_profile_changes_alpha_mix_not_dates() {
        let config = LayeredSearchConfig::professional_regime_alpha_selector_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v4".to_string(),
                "quality_state_alpha_selector_v1".to_string(),
                "quality_state_alpha_selector_v2".to_string(),
                "quality_state_alpha_selector_v3".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_event_window_return_sharpe_router_v4"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_regime_alpha_overlay_frontier_profile_adds_selector_overlay_and_smoothing() {
        let config = LayeredSearchConfig::professional_regime_alpha_overlay_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_selector_v3".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_alpha_overlay_selector_v2".to_string(),
                "quality_state_alpha_overlay_selector_v3".to_string(),
            ]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![Decimal::ZERO, Decimal::new(5, 3)]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::ONE, Decimal::new(85, 2)]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["rebalance_hysteresis_pct"] == "0"
                && trial["partial_rebalance_ratio"] == "1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["rebalance_hysteresis_pct"] == "0.005"
                && trial["partial_rebalance_ratio"] == "0.85"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_event_alpha_profile_routes_mixed_state_without_date_fitting() {
        let config = LayeredSearchConfig::professional_mixed_state_event_alpha_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v2".to_string(),
                "quality_mixed_event_state_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_overlay_selector_v2".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.rebalance_hysteresis_pct, vec![Decimal::ZERO]);
        assert_eq!(config.partial_rebalance_ratio, vec![Decimal::ONE]);
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["rebalance_hysteresis_pct"] == "0"
                && trial["partial_rebalance_ratio"] == "1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_overlay_selector_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_profile_tightens_mixed_state_only() {
        let config = LayeredSearchConfig::professional_mixed_state_risk_memory_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v2".to_string(),
                "quality_mixed_state_risk_memory_router_v3".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 160
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_frontier_profile_bridges_return_and_sharpe() {
        let config = LayeredSearchConfig::professional_mixed_state_risk_memory_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v5".to_string(),
                "quality_mixed_state_risk_memory_router_v6".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v5"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_fine_frontier_profile_scans_near_boundary() {
        let config =
            LayeredSearchConfig::professional_mixed_state_risk_memory_fine_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v7".to_string(),
                "quality_mixed_state_risk_memory_router_v8".to_string(),
                "quality_mixed_state_risk_memory_router_v9".to_string(),
                "quality_mixed_state_risk_memory_router_v10".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v9"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_exposure_frontier_profile_isolates_exposure_axis() {
        let config = LayeredSearchConfig::professional_mixed_state_exposure_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v11".to_string(),
                "quality_mixed_state_risk_memory_router_v12".to_string(),
                "quality_mixed_state_risk_memory_router_v13".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v11"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_orthogonal_alpha_profile_changes_mixed_alpha_source() {
        let config = LayeredSearchConfig::professional_mixed_state_orthogonal_alpha_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
                "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
                "quality_mixed_orthogonal_alpha_selector_v3".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v2".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_candidate_filter_alpha_bridge_profile_crosses_filter_and_alpha_axes() {
        let config = LayeredSearchConfig::professional_candidate_filter_alpha_bridge_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial["candidate_risk_filter"] == "low_volatility_v1"
                && trial["risk_contribution_control"] == "soft_single_name_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["candidate_risk_filter"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_soft_candidate_filter_alpha_bridge_profile_relaxes_hard_filter() {
        let config = LayeredSearchConfig::professional_soft_candidate_filter_alpha_bridge_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "soft_low_volatility_v1".to_string(),
                "soft_low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v3"
                && trial["candidate_risk_filter"] == "soft_low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_sharpe_bridge_frontier_profile_centers_bx_anchor_without_filtering() {
        let config = LayeredSearchConfig::professional_sharpe_bridge_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_sharpe_bridge_router_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_sharpe_bridge_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_annual_sharpe_floor_bridge_profile_bridges_risk_memory_without_date_fitting() {
        let config = LayeredSearchConfig::professional_annual_sharpe_floor_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_risk_memory_relaxed_frontier_profile_tests_internal_mixed_risk_boundary() {
        let config = LayeredSearchConfig::professional_risk_memory_relaxed_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_state_risk_memory_router_v15".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v17".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v18"
                && trial["event_gate_profile"] == "valuation_exclude_bottom35"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_sharpe_floor_auto_discovery_profile_bridges_sharpe_boundary_and_annual_floor() {
        let config = LayeredSearchConfig::professional_sharpe_floor_auto_discovery_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_confirmed_sharpe_bridge_profile_adds_weak_ml_confirmation_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_prediction_confirmed_sharpe_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
            ]
        );
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "CN uses prediction as a weak blend/filter over factor_combo seeds, not standalone model_prediction"
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.02"
                && trial.get("prediction_min_percentile").is_none()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["prediction_blend_weight"] == "0.05"
                && trial["prediction_min_percentile"] == "0.20"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["prediction_blend_weight"] == "0.08"
                && trial["prediction_min_percentile"] == "0.30"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-v1-201602-202605"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_boundary_return_bridge_profile_targets_real_boundary_without_prediction(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_boundary_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(
            config.portfolio_volatility_controls,
            vec![
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
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["risk_budget_lookback_days"] == 180
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.75"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert!(seed.get("prediction_set_id").is_none());
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(
                seed["risk_contribution_control"],
                "soft_single_name_20pct_v1"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_boundary_event_lift_profile_adds_small_sleeves_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_boundary_event_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            let regime = trial["market_regime"].as_str().unwrap_or_default();
            trial["combo_name"] == "phase7_financial_quality_v1"
                && matches!(
                    regime,
                    "quality_mixed_state_risk_memory_router_v14"
                        | "quality_all_regime_event_window_sleeve_05pct_v1"
                        | "quality_all_regime_event_window_sleeve_10pct_v1"
                        | "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                )
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_micro_frontier_profile_searches_near_cl_cm_v14_without_overfit_stacking(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_micro_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.score_candidate_pool_sizes, vec![400, 500, 650, 800]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "soft_low_volatility_v1".to_string(),
                "soft_low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["candidate_risk_filter"] == "soft_low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert!(seed.get("prediction_set_id").is_none());
            let regime = seed["market_regime"].as_str().unwrap_or_default();
            assert!(
                regime == "quality_mixed_orthogonal_risk_memory_router_v3"
                    || regime == "quality_nonlinear_alpha_risk_memory_router_v3"
                    || regime == "quality_mixed_state_risk_memory_router_v14"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_sharpe_return_lift_profile_keeps_sharpe_anchor_and_only_lifts_return_gently(
    ) {
        let config = LayeredSearchConfig::professional_v14_sharpe_return_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![400, 500, 650]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["risk_contribution_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom43"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_148_475_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_045_neg10_63"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_shape_lift_profile_varies_position_shape_without_alpha_or_date_overfit() {
        let config = LayeredSearchConfig::professional_v14_shape_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![18, 20, 22]);
        assert_eq!(config.rebalance_days, vec![50, 55, 60]);
        assert_eq!(
            config.skip_top_pct,
            vec![Decimal::new(8, 2), Decimal::new(10, 2)]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 18
                && trial["rebalance"] == "55"
                && trial["skip_top_pct"] == "0.08"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 22
                && trial["rebalance"] == "50"
                && trial["portfolio_drawdown_control"] == "recover252_08_23_45_30_70"
                && trial["stop_loss_pct"] == "0.07"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20
                && trial["rebalance"] == "55"
                && trial["rebalance_smoothing_profile"] == "hysteresis_1pct_partial_75"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_ultra_micro_lift_profile_only_perturbs_the_high_sharpe_anchor() {
        let config = LayeredSearchConfig::professional_v14_ultra_micro_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![19, 20, 21]);
        assert_eq!(config.rebalance_days, vec![58, 60, 62]);
        assert_eq!(
            config.skip_top_pct,
            vec![Decimal::new(9, 2), Decimal::new(10, 2), Decimal::new(11, 2)]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500]);
        assert_eq!(config.seed_trials.len(), 15);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20
                && trial["rebalance"] == "60"
                && trial["skip_top_pct"] == "0.10"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 19
                && trial["rebalance"] == "60"
                && trial["skip_top_pct"] == "0.10"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20 && trial["rebalance"] == "58" && trial["skip_top_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20 && trial["rebalance"] == "60" && trial["skip_top_pct"] == "0.09"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(
                seed["portfolio_sharpe_control"],
                "roll_sharpe180_050_neg10_65"
            );
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert_eq!(seed["score_candidate_pool_size"], 500);
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_return_alpha_sharpe_bridge_profile_combines_return_alpha_with_sharpe_controls()
    {
        let config = LayeredSearchConfig::professional_return_alpha_sharpe_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500, 650]);
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_budget_lookback_days"] == 170
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_regime_frontier_bridge_profile_connects_high_sharpe_and_return_frontiers() {
        let config = LayeredSearchConfig::professional_regime_frontier_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v1".to_string(),
                "quality_frontier_regime_bridge_router_v2".to_string(),
                "quality_frontier_regime_bridge_router_v3".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500, 650]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["score_candidate_pool_size"] == 500
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_regime_frontier_decomposition_profile_splits_risk_axes_without_overfit() {
        let config = LayeredSearchConfig::professional_regime_frontier_decomposition_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v4".to_string(),
                "quality_frontier_regime_bridge_router_v5".to_string(),
                "quality_frontier_regime_bridge_router_v6".to_string(),
                "quality_frontier_regime_bridge_router_v7".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v5"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_contribution_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["score_candidate_pool_size"] == 500
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["score_candidate_pool_size"] == 650
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_high_sharpe_return_micro_bridge_profile_relaxes_only_exposure_and_vol() {
        let config = LayeredSearchConfig::professional_high_sharpe_return_micro_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v6".to_string(),
                "quality_frontier_regime_bridge_router_v7".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial["portfolio_volatility_control"] == "vol120_147_475_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_70"
                && trial["portfolio_sharpe_min_exposure"] == "0.70"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_72"
                && trial["score_candidate_pool_size"] == 650
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_annual_floor_micro_lift_profile_bridges_only_the_high_sharpe_edge() {
        let config = LayeredSearchConfig::professional_v14_annual_floor_micro_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_state_risk_memory_router_v15".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v17".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![165, 170]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v15"
                && trial["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_0475_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["score_candidate_pool_size"] == 650
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_near_miss_annual_bridge_profile_only_lifts_the_v14_near_miss() {
        let config = LayeredSearchConfig::professional_v14_near_miss_annual_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 165, 170]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["score_candidate_pool_size"] == 500
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.67"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["max_position_pct"] == "0.145"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["top_n"], 20);
            assert_eq!(seed["rebalance"], "60");
            assert_eq!(seed["skip_top_pct"], "0.10");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_corr70_annual_edge_profile_extends_only_the_corr70_near_miss() {
        let config = LayeredSearchConfig::professional_v14_corr70_annual_edge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![165, 168]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(68, 2),
                Decimal::new(70, 2),
                Decimal::new(72, 2)
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "cost_up_125pct_slip_2bps"));
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "impact_2pct_participation_10pct"));
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "participation_5pct"));
        assert_eq!(config.seed_trials.len(), 20);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.68"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["risk_budget_lookback_days"] == 168
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["max_pairwise_correlation"] == "0.68"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["top_n"], 20);
            assert_eq!(seed["rebalance"], "60");
            assert_eq!(seed["skip_top_pct"], "0.10");
            assert_eq!(seed["max_position_pct"], "0.15");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_execution_robust_candidate_profile_targets_capacity_fragility() {
        let config = LayeredSearchConfig::professional_execution_robust_candidate_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.top_n, vec![20, 30, 50]);
        assert_eq!(config.rebalance_days, vec![60, 80, 120]);
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(10, 2),
                Decimal::new(12, 2),
                Decimal::new(15, 2)
            ]
        );
        assert!(config.capacity_penalty_strength.contains(&Decimal::ONE));
        assert!(config
            .capacity_penalty_strength
            .contains(&Decimal::new(125, 2)));
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "impact_2pct_participation_10pct"));
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "participation_5pct"));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(1, 2)));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(2, 2)));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(75, 2)));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(50, 2)));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["max_position_pct"] == "0.10"
                && trial["capacity_penalty_strength"] == "1"
                && trial["rebalance_hysteresis_pct"] == "0.01"
                && trial["partial_rebalance_ratio"] == "0.75"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 50
                && trial["rebalance"] == "120"
                && trial["capacity_penalty_strength"] == "1.25"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_execution_low_turnover_alpha_profile_targets_capacity_friendly_orthogonal_sources(
    ) {
        let config = LayeredSearchConfig::professional_execution_low_turnover_alpha_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.top_n, vec![30, 50, 80]);
        assert_eq!(config.rebalance_days, vec![120, 160, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string()
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "soft_low_volatility_v1".to_string(),
                "soft_low_volatility_low_correlation_v1".to_string()
            ]
        );
        assert!(config
            .capacity_penalty_strength
            .contains(&Decimal::new(150, 2)));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(3, 2)));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(35, 2)));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
                && trial["rebalance"] == "180"
                && trial["capacity_penalty_strength"] == "1.5"
                && trial["partial_rebalance_ratio"] == "0.35"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_execution_capacity_budget_profile_adds_capacity_budget_axis() {
        let config = LayeredSearchConfig::professional_execution_capacity_budget_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_participation_balanced_v1".to_string()));
        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_participation_strict_v1".to_string()));
        assert!(config
            .cost_capacity_stress_profiles
            .iter()
            .any(|profile| profile.profile_name == "participation_5pct"));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["capacity_risk_budget"] == "capacity_participation_strict_v1"
                && trial["capacity_penalty_strength"] == "1.5"
        }));

        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
    }

    #[test]
    fn professional_execution_impact_budget_profile_adds_turnover_budget_axis() {
        let config = LayeredSearchConfig::professional_execution_impact_budget_default();

        assert!(config
            .capacity_risk_budget_profiles
            .contains(&"capacity_participation_strict_v1".to_string()));
        assert!(config
            .execution_impact_budget_profiles
            .contains(&"impact_turnover_20pct_v1".to_string()));
        assert!(config
            .execution_impact_budget_profiles
            .contains(&"impact_turnover_15pct_v1".to_string()));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(25, 2)));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["capacity_risk_budget"] == "capacity_participation_strict_v1"
                && trial["execution_impact_budget"] == "impact_turnover_20pct_v1"
        }));

        let resource_plan = LocalResourcePlan {
            max_trials: 32,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["execution_impact_budget"] == "impact_turnover_20pct_v1"
        }));
    }

    #[test]
    fn professional_execution_schedule_budget_profile_adds_twap_axis() {
        let config = LayeredSearchConfig::professional_execution_schedule_budget_default();

        assert!(config
            .execution_schedule_profiles
            .contains(&"twap_5d_v1".to_string()));
        assert!(config
            .execution_impact_budget_profiles
            .contains(&"impact_turnover_20pct_v1".to_string()));

        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_5d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"] == "twap_5d_v1"
        }));
    }

    #[test]
    fn professional_execution_patient_schedule_profile_adds_long_twap_axis() {
        let config = LayeredSearchConfig::professional_execution_patient_schedule_budget_default();

        assert_eq!(
            config.execution_schedule_profiles,
            vec![
                "twap_10d_v1".to_string(),
                "twap_15d_v1".to_string(),
                "twap_20d_v1".to_string()
            ]
        );

        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"]
                    == "twap_20d_v1"
        }));
    }

    #[test]
    fn professional_execution_daily_cap_profile_adds_target_move_and_carry_limits() {
        let config = LayeredSearchConfig::professional_execution_daily_cap_budget_default();
        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["execution_daily_target_move_limit_pct"] == "0.05"
                && trial.parameters["execution_max_carry_days"] == 20
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.05)
                && trial.parameters["execution_rules"]["execution_max_carry_days"] == 20
        }));
    }

    #[test]
    fn professional_execution_cash_drag_aware_profile_adds_fill_friendly_controls() {
        let config = LayeredSearchConfig::professional_execution_cash_drag_aware_budget_default();
        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["execution_daily_target_move_limit_pct"] == "0.075"
                && trial.parameters["execution_max_carry_days"] == 40
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.075)
                && trial.parameters["execution_rules"]["execution_max_carry_days"] == 40
        }));
    }

    #[test]
    fn professional_execution_feasible_fill_profile_adds_cash_utilization_axis() {
        let config = LayeredSearchConfig::professional_execution_feasible_fill_budget_default();
        let resource_plan = LocalResourcePlan {
            max_trials: 24,
            batch_size: 8,
            ..LocalResourcePlan::for_machine(10, 32)
        };
        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(
            config.cash_utilization_profiles,
            vec![
                "fillable_gross_90_v1".to_string(),
                "fillable_gross_95_v1".to_string()
            ]
        );
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_90_v1"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
        }));
    }

    #[test]
    fn professional_return_distribution_repair_profile_combines_event_decay_and_orthogonal_alpha_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_return_distribution_repair_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().all(|trial| {
            trial.get("prediction_set_id").is_none()
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["event_gate_profile"] == "event_window_10d_boost_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_10d_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.38"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "event_window_40d_exclude_negative"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_40d_v1"
                && trial["event_gate_mode"] == "exclude_negative"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "residual_confirm_top40"
                && trial["event_gate_combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
                && trial["event_gate_min_score"] == "0.40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["portfolio_sharpe_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_alpha_router_profile_keeps_current_anchor_and_routes_second_alpha() {
        let config = LayeredSearchConfig::professional_state_alpha_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
                "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_blend_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_state_position_risk_router_profile_keeps_anchor_and_sweeps_position_regime() {
        let config = LayeredSearchConfig::professional_state_position_risk_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_bear_position_guard_v3".to_string(),
                "quality_bear_position_guard_v1".to_string(),
                "quality_bear_position_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_event_position_risk_router_profile_keeps_event_sleeve_and_position_guard() {
        let config = LayeredSearchConfig::professional_event_position_risk_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_event_window_position_guard_v3".to_string(),
                "quality_event_window_position_guard_v1".to_string(),
                "quality_event_window_position_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_position_guard_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_all_regime_event_sleeve_profile_mixes_event_flow_across_states() {
        let config = LayeredSearchConfig::professional_all_regime_event_sleeve_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_15pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_portfolio_sharpe_control_profile_adds_self_risk_axis_without_date_fitting() {
        let config = LayeredSearchConfig::professional_portfolio_sharpe_control_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(
                |profile| profile.profile_name == "roll_sharpe120_060_000_55"
                    && profile.reduce_start == Some(Decimal::new(60, 2))
                    && profile.reduce_full == Some(Decimal::ZERO)
                    && profile.min_exposure == Some(Decimal::new(55, 2))
            ));
        let sharpe_seed = config
            .seed_trials
            .iter()
            .find(|trial| trial["portfolio_sharpe_control"] == "roll_sharpe120_060_000_55")
            .expect("rolling Sharpe seed");
        assert_eq!(sharpe_seed["portfolio_sharpe_reduce_start"], "0.60");
        assert_eq!(sharpe_seed["portfolio_sharpe_reduce_full"], "0.00");
        assert_eq!(sharpe_seed["portfolio_sharpe_lookback_days"], 120);
        assert_eq!(sharpe_seed["portfolio_sharpe_min_exposure"], "0.55");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_sharpe_control"] == "bridge_roll_sharpe180_050_neg10_60"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_nonlinear_alpha_auto_discovery_profile_combines_new_alpha_and_rolling_sharpe() {
        let config = LayeredSearchConfig::professional_nonlinear_alpha_auto_discovery_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_nonlinear_alpha_router_v1".to_string(),
                "quality_nonlinear_alpha_router_v2".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(
                |profile| profile.profile_name == "roll_sharpe180_050_neg10_65"
                    && profile.reduce_start == Some(Decimal::new(50, 2))
                    && profile.reduce_full == Some(Decimal::new(-10, 2))
                    && profile.min_exposure == Some(Decimal::new(65, 2))
            ));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_router_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_sharpe_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_nonlinear_sharpe_return_bridge_profile_targets_cl_boundary() {
        let config = LayeredSearchConfig::professional_nonlinear_sharpe_return_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_nonlinear_alpha_risk_memory_router_v2".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_nonlinear_alpha_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["max_position_pct"] == "0.16"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_volatility_sharpe_profile_searches_tighter_u2_vol_targets() {
        let config = LayeredSearchConfig::professional_volatility_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_20_60_100"
                && trial["portfolio_volatility_target_pct"] == "0.20"
                && trial["portfolio_volatility_min_exposure"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_volatility_target_pct"] == "0.18"
                && trial["portfolio_volatility_min_exposure"] == "0.55"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol252_16_45_100"
                && trial["portfolio_volatility_target_pct"] == "0.16"
                && trial["portfolio_volatility_min_exposure"] == "0.45"
        }));
    }

    #[test]
    fn professional_regime_position_sharpe_profile_keeps_u2_alpha_and_sweeps_regime_overlay() {
        let config = LayeredSearchConfig::professional_regime_position_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_bear_position_guard_v1".to_string(),
                "quality_bear_position_guard_v2".to_string(),
                "quality_crash_guard_v3".to_string(),
                "quality_risk_off_v1".to_string(),
            ]
        );
        assert_eq!(
            config
                .portfolio_volatility_controls
                .iter()
                .map(|profile| profile.profile_name.as_str())
                .collect::<Vec<_>>(),
            vec!["vol120_18_55_100", "vol120_22_65_100"]
        );
        assert!(config.seed_trials.len() <= 8);
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_risk_off_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config
            .seed_trials
            .iter()
            .all(|trial| trial["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn phase7_w_alpha_blend_adds_non_momentum_quality_value_recovery_source() {
        let profiles = phase7_alpha_blend_profiles();
        let profile = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_value_recovery_confirm_v1")
            .expect("Phase 7-W quality/value/recovery profile");

        let source_weights = profile
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(profile.profile_name, "quality_value_recovery_confirm_5pct");
        assert_eq!(
            source_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(60, 2))
        );
        assert_eq!(
            source_weights.get("phase7_valuation_v1"),
            Some(&Decimal::new(20, 2))
        );
        assert_eq!(
            source_weights.get("phase7_growth_recovery_v1"),
            Some(&Decimal::new(15, 2))
        );
        assert_eq!(
            source_weights.get("phase7_moneyflow_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert!(
            !source_weights.contains_key("phase7_relative_strength_v1"),
            "Phase 7-W should add a non-momentum second alpha source"
        );
    }

    #[test]
    fn phase7_event_alpha_blends_are_light_confirmation_overlays() {
        let profiles = phase7_alpha_blend_profiles();
        let quality_event = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_confirm_v1")
            .expect("quality/event profile");
        let quality_event_surprise = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_surprise_confirm_v1")
            .expect("quality/event-surprise profile");
        let quality_event_window = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_window_overlay_v1")
            .expect("quality/event-window overlay profile");
        let quality_value_recovery_event = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_value_recovery_event_confirm_v1")
            .expect("quality/value/recovery/event profile");

        let quality_event_weights = quality_event
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_confirm_weights = quality_value_recovery_event
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_window_weights = quality_event_window
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_surprise_weights = quality_event_surprise
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(
            quality_event_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            quality_event_weights.get("phase7_event_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_confirm_weights.get("phase7_event_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_window_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            event_window_weights.get("phase7_event_window_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_surprise_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            event_surprise_weights.get("phase7_event_surprise_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert!(
            !event_confirm_weights.contains_key("phase7_relative_strength_v1"),
            "event confirmation profile should not reintroduce a momentum dependency"
        );
    }

    #[test]
    fn phase7_fq_change_event_surprise_sleeves_are_bounded_optional_blends() {
        let profiles = phase7_alpha_blend_profiles();
        let expected = [
            (
                "fq_change_event_surprise_sleeve_05pct",
                "phase7_fq_change_event_surprise_sleeve_05pct_v1",
                Decimal::new(95, 2),
                Decimal::new(5, 2),
            ),
            (
                "fq_change_event_surprise_sleeve_10pct",
                "phase7_fq_change_event_surprise_sleeve_10pct_v1",
                Decimal::new(90, 2),
                Decimal::new(10, 2),
            ),
            (
                "fq_change_event_surprise_sleeve_15pct_boundary",
                "phase7_fq_change_event_surprise_sleeve_15pct_v1",
                Decimal::new(85, 2),
                Decimal::new(15, 2),
            ),
        ];

        for (profile_name, combo_name, fq_weight, event_weight) in expected {
            let profile = profiles
                .iter()
                .find(|profile| profile.combo_name == combo_name)
                .expect("P3.11 FQ-change/event-surprise sleeve blend profile");
            let weights = profile
                .sources
                .iter()
                .map(|source| (source.combo_name.as_str(), source.weight))
                .collect::<std::collections::BTreeMap<_, _>>();

            assert_eq!(profile.profile_name, profile_name);
            assert_eq!(
                phase7_alpha_source_admission(combo_name).role,
                Phase7AlphaSourceRole::OptionalOverlayOnly
            );
            assert_eq!(
                weights.get("phase7_financial_quality_change_v1"),
                Some(&fq_weight)
            );
            assert_eq!(weights.get("phase7_event_surprise_v1"), Some(&event_weight));
        }
    }

    #[test]
    fn phase7_fq_change_supply_float_sleeves_are_bounded_optional_blends() {
        let profiles = phase7_alpha_blend_profiles();
        let expected = [
            (
                "fq_change_supply_float_sleeve_05pct",
                "phase7_fq_change_supply_float_sleeve_05pct_v1",
                Decimal::new(95, 2),
                Decimal::new(5, 2),
            ),
            (
                "fq_change_supply_float_sleeve_10pct",
                "phase7_fq_change_supply_float_sleeve_10pct_v1",
                Decimal::new(90, 2),
                Decimal::new(10, 2),
            ),
            (
                "fq_change_supply_float_sleeve_15pct_boundary",
                "phase7_fq_change_supply_float_sleeve_15pct_v1",
                Decimal::new(85, 2),
                Decimal::new(15, 2),
            ),
        ];

        for (profile_name, combo_name, fq_weight, supply_weight) in expected {
            let profile = profiles
                .iter()
                .find(|profile| profile.combo_name == combo_name)
                .expect("P3.12 FQ-change/supply-float sleeve blend profile");
            let weights = profile
                .sources
                .iter()
                .map(|source| (source.combo_name.as_str(), source.weight))
                .collect::<std::collections::BTreeMap<_, _>>();

            assert_eq!(profile.profile_name, profile_name);
            assert_eq!(
                phase7_alpha_source_admission(combo_name).role,
                Phase7AlphaSourceRole::OptionalOverlayOnly
            );
            assert_eq!(
                weights.get("phase7_financial_quality_change_v1"),
                Some(&fq_weight)
            );
            assert_eq!(
                weights.get("phase7_supply_float_shock_v1"),
                Some(&supply_weight)
            );
        }
        assert_eq!(
            phase7_alpha_source_admission("phase7_supply_float_shock_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
    }

    #[test]
    fn phase7_fq_change_forecast_revision_sleeves_are_bounded_optional_blends() {
        let profiles = phase7_alpha_blend_profiles();
        let expected = [
            (
                "fq_change_forecast_revision_sleeve_05pct",
                "phase7_fq_change_forecast_revision_sleeve_05pct_v1",
                Decimal::new(95, 2),
                Decimal::new(5, 2),
            ),
            (
                "fq_change_forecast_revision_sleeve_10pct",
                "phase7_fq_change_forecast_revision_sleeve_10pct_v1",
                Decimal::new(90, 2),
                Decimal::new(10, 2),
            ),
            (
                "fq_change_forecast_revision_sleeve_15pct_boundary",
                "phase7_fq_change_forecast_revision_sleeve_15pct_v1",
                Decimal::new(85, 2),
                Decimal::new(15, 2),
            ),
        ];

        for (profile_name, combo_name, fq_weight, revision_weight) in expected {
            let profile = profiles
                .iter()
                .find(|profile| profile.combo_name == combo_name)
                .expect("P3.13 FQ-change/forecast-revision sleeve blend profile");
            let weights = profile
                .sources
                .iter()
                .map(|source| (source.combo_name.as_str(), source.weight))
                .collect::<std::collections::BTreeMap<_, _>>();

            assert_eq!(profile.profile_name, profile_name);
            assert_eq!(
                phase7_alpha_source_admission(combo_name).role,
                Phase7AlphaSourceRole::OptionalOverlayOnly
            );
            assert_eq!(
                weights.get("phase7_financial_quality_change_v1"),
                Some(&fq_weight)
            );
            assert_eq!(
                weights.get("phase7_forecast_revision_surprise_v1"),
                Some(&revision_weight)
            );
        }
        assert_eq!(
            phase7_alpha_source_admission("phase7_forecast_revision_surprise_v1").role,
            Phase7AlphaSourceRole::EventGateOnly
        );
    }

    #[test]
    fn phase7_fq_change_shareholder_structure_sleeves_are_bounded_optional_blends() {
        let profiles = phase7_alpha_blend_profiles();
        let expected = [
            (
                "fq_change_shareholder_structure_sleeve_05pct",
                "phase7_fq_change_shareholder_structure_sleeve_05pct_v1",
                Decimal::new(95, 2),
                Decimal::new(5, 2),
            ),
            (
                "fq_change_shareholder_structure_sleeve_10pct",
                "phase7_fq_change_shareholder_structure_sleeve_10pct_v1",
                Decimal::new(90, 2),
                Decimal::new(10, 2),
            ),
            (
                "fq_change_shareholder_structure_sleeve_15pct_boundary",
                "phase7_fq_change_shareholder_structure_sleeve_15pct_v1",
                Decimal::new(85, 2),
                Decimal::new(15, 2),
            ),
        ];

        for (profile_name, combo_name, fq_weight, shareholder_weight) in expected {
            let profile = profiles
                .iter()
                .find(|profile| profile.combo_name == combo_name)
                .expect("P3.21E FQ-change/shareholder-structure sleeve blend profile");
            let sources = profile
                .sources
                .iter()
                .map(|source| (source.combo_name.as_str(), source))
                .collect::<std::collections::BTreeMap<_, _>>();

            assert_eq!(profile.profile_name, profile_name);
            assert_eq!(
                phase7_alpha_source_admission(combo_name).role,
                Phase7AlphaSourceRole::OptionalOverlayOnly
            );
            assert_eq!(
                sources
                    .get("phase7_financial_quality_change_v1")
                    .map(|source| source.weight),
                Some(fq_weight)
            );
            let shareholder_source = sources
                .get("shareholder_structure")
                .expect("shareholder_structure source");
            assert_eq!(shareholder_source.weight, shareholder_weight);
            assert_eq!(
                shareholder_source.version,
                "p321d-shareholder-low-fanout-v1"
            );
        }
        assert_eq!(
            phase7_alpha_source_admission("shareholder_structure").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
    }

    #[test]
    fn professional_v19_event_surprise_sleeve_gate_profile_is_seed_only_and_bounded() {
        let config = LayeredSearchConfig::professional_v19_event_surprise_sleeve_gate_default();

        assert!(
            config.combo_versions.is_empty(),
            "bounded profile must not reopen cartesian combo search"
        );
        assert!(config.prediction_set_ids.is_empty());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);

        let serialized = serde_json::to_string(&config.seed_trials).unwrap();
        assert!(serialized.contains("v19_p311_fq_change_event_surprise_sleeve_gate_v1"));
        assert!(serialized.contains("phase7_financial_quality_change_v1"));
        assert!(serialized.contains("phase7_fq_change_event_surprise_sleeve_05pct_v1"));
        assert!(serialized.contains("phase7_fq_change_event_surprise_sleeve_10pct_v1"));
        assert!(serialized.contains("event_surprise_boost_p75_3pct"));
        assert!(!serialized.contains("phase7_moneyflow_congestion_interaction_v1"));
        assert!(!serialized.contains("phase7_event_post_return_curve_20d_v1"));
        assert!(!serialized.contains("pred-"));

        assert!(config.seed_trials.iter().all(|trial| {
            trial["signal_source"] == "factor_combo"
                && trial["event_surprise_sleeve_gate_profile"]
                    == "v19_p311_fq_change_event_surprise_sleeve_gate_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_change_v1"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.35"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
    }

    #[test]
    fn professional_v19_supply_float_sleeve_profile_is_seed_only_and_bounded() {
        let config = LayeredSearchConfig::professional_v19_supply_float_sleeve_default();

        assert!(
            config.combo_versions.is_empty(),
            "bounded profile must not reopen cartesian combo search"
        );
        assert!(config.prediction_set_ids.is_empty());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);

        let serialized = serde_json::to_string(&config.seed_trials).unwrap();
        assert!(serialized.contains("v19_p312_fq_change_supply_float_sleeve_v1"));
        assert!(serialized.contains("phase7_financial_quality_change_v1"));
        assert!(serialized.contains("phase7_fq_change_supply_float_sleeve_05pct_v1"));
        assert!(serialized.contains("phase7_fq_change_supply_float_sleeve_10pct_v1"));
        assert!(serialized.contains("phase7_fq_change_supply_float_sleeve_15pct_v1"));
        assert!(!serialized.contains("phase7_moneyflow_congestion_interaction_v1"));
        assert!(!serialized.contains("phase7_event_post_return_curve_20d_v1"));
        assert!(!serialized.contains("pred-"));

        assert!(config.seed_trials.iter().all(|trial| {
            trial["signal_source"] == "factor_combo"
                && trial["supply_float_sleeve_profile"]
                    == "v19_p312_fq_change_supply_float_sleeve_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_fq_change_supply_float_sleeve_10pct_v1"
                && trial["supply_float_sleeve_variant"] == "supply_float_sleeve_10pct"
        }));
    }

    #[test]
    fn phase7_fq_change_unlock_pressure_sleeves_are_bounded_optional_blends() {
        let profiles = phase7_alpha_blend_profiles();
        let expected = [
            (
                "fq_change_unlock_pressure_sleeve_05pct",
                "phase7_fq_change_unlock_pressure_sleeve_05pct_v1",
                Decimal::new(95, 2),
                Decimal::new(5, 2),
            ),
            (
                "fq_change_unlock_pressure_sleeve_10pct",
                "phase7_fq_change_unlock_pressure_sleeve_10pct_v1",
                Decimal::new(90, 2),
                Decimal::new(10, 2),
            ),
            (
                "fq_change_unlock_pressure_sleeve_15pct_boundary",
                "phase7_fq_change_unlock_pressure_sleeve_15pct_v1",
                Decimal::new(85, 2),
                Decimal::new(15, 2),
            ),
        ];

        for (profile_name, combo_name, fq_weight, unlock_weight) in expected {
            let profile = profiles
                .iter()
                .find(|profile| profile.combo_name == combo_name)
                .expect("P3.14 FQ-change/unlock-pressure sleeve blend profile");
            let weights = profile
                .sources
                .iter()
                .map(|source| (source.combo_name.as_str(), source.weight))
                .collect::<std::collections::BTreeMap<_, _>>();

            assert_eq!(profile.profile_name, profile_name);
            assert_eq!(
                phase7_alpha_source_admission(combo_name).role,
                Phase7AlphaSourceRole::OptionalOverlayOnly
            );
            assert_eq!(
                weights.get("phase7_financial_quality_change_v1"),
                Some(&fq_weight)
            );
            assert_eq!(
                weights.get("phase7_unlock_supply_pressure_v1"),
                Some(&unlock_weight)
            );
        }
        assert_eq!(
            phase7_alpha_source_admission("phase7_unlock_supply_pressure_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
    }

    #[test]
    fn professional_v19_unlock_pressure_sleeve_profile_is_seed_only_and_sideways_bounded() {
        let config = LayeredSearchConfig::professional_v19_unlock_pressure_sleeve_default();

        assert!(
            config.combo_versions.is_empty(),
            "bounded profile must not reopen cartesian combo search"
        );
        assert!(config.prediction_set_ids.is_empty());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);

        let serialized = serde_json::to_string(&config.seed_trials).unwrap();
        assert!(serialized.contains("v19_p314_fq_change_unlock_pressure_sleeve_v1"));
        assert!(serialized.contains("phase7_financial_quality_change_v1"));
        assert!(serialized.contains("phase7_fq_change_unlock_pressure_sleeve_05pct_v1"));
        assert!(serialized.contains("phase7_fq_change_unlock_pressure_sleeve_10pct_v1"));
        assert!(serialized.contains("phase7_fq_change_unlock_pressure_sleeve_15pct_v1"));
        assert!(serialized.contains("unlock_pressure_exclude_sideways_gate"));
        assert!(serialized.contains("\"sideways_regime_policy\":\"exclude\""));
        assert!(!serialized.contains("phase7_moneyflow_congestion_interaction_v1"));
        assert!(!serialized.contains("phase7_event_post_return_curve_20d_v1"));
        assert!(!serialized.contains("pred-"));

        assert!(config.seed_trials.iter().all(|trial| {
            trial["signal_source"] == "factor_combo"
                && trial["unlock_pressure_sleeve_profile"]
                    == "v19_p314_fq_change_unlock_pressure_sleeve_v1"
                && trial["unlock_pressure_sleeve_control"] == "phase7_financial_quality_change_v1"
                && trial["sideways_regime_policy"] == "exclude"
                && trial["oos_policy"] == "evaluation_only"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_fq_change_unlock_pressure_sleeve_10pct_v1"
                && trial["unlock_pressure_sleeve_variant"] == "unlock_pressure_sleeve_10pct"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_change_v1"
                && trial["event_gate_combo_name"] == "phase7_unlock_supply_pressure_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_active_regimes"]
                    == json!(["bear", "bull", "mixed", "high_volatility"])
        }));
    }

    #[test]
    fn professional_v19_forecast_revision_sleeve_profile_is_seed_only_and_bounded() {
        let config = LayeredSearchConfig::professional_v19_forecast_revision_sleeve_default();

        assert!(
            config.combo_versions.is_empty(),
            "bounded profile must not reopen cartesian combo search"
        );
        assert!(config.prediction_set_ids.is_empty());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);

        let serialized = serde_json::to_string(&config.seed_trials).unwrap();
        assert!(serialized.contains("v19_p313_fq_change_forecast_revision_sleeve_v1"));
        assert!(serialized.contains("phase7_financial_quality_change_v1"));
        assert!(serialized.contains("phase7_fq_change_forecast_revision_sleeve_05pct_v1"));
        assert!(serialized.contains("phase7_fq_change_forecast_revision_sleeve_10pct_v1"));
        assert!(serialized.contains("phase7_fq_change_forecast_revision_sleeve_15pct_v1"));
        assert!(!serialized.contains("phase7_moneyflow_congestion_interaction_v1"));
        assert!(!serialized.contains("phase7_event_post_return_curve_20d_v1"));
        assert!(!serialized.contains("pred-"));

        assert!(config.seed_trials.iter().all(|trial| {
            trial["signal_source"] == "factor_combo"
                && trial["forecast_revision_sleeve_profile"]
                    == "v19_p313_fq_change_forecast_revision_sleeve_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_fq_change_forecast_revision_sleeve_10pct_v1"
                && trial["forecast_revision_sleeve_variant"] == "forecast_revision_sleeve_10pct"
        }));
    }

    #[test]
    fn professional_v19_shareholder_structure_sleeve_profile_is_seed_only_and_gated() {
        let config = LayeredSearchConfig::professional_v19_shareholder_structure_sleeve_default();

        assert!(
            config.combo_versions.is_empty(),
            "bounded profile must not reopen cartesian combo search"
        );
        assert!(config.prediction_set_ids.is_empty());
        assert_eq!(config.event_gate_profiles, vec![EventGateProfile::off()]);
        assert_eq!(
            config.universe_profiles,
            vec!["main_chinext_non_st".to_string()]
        );

        let serialized = serde_json::to_string(&config.seed_trials).unwrap();
        assert!(serialized.contains("v19_p321e_fq_change_shareholder_structure_sleeve_v1"));
        assert!(serialized.contains("phase7_financial_quality_change_v1"));
        assert!(serialized.contains("phase7_fq_change_shareholder_structure_sleeve_05pct_v1"));
        assert!(serialized.contains("phase7_fq_change_shareholder_structure_sleeve_10pct_v1"));
        assert!(serialized.contains("phase7_fq_change_shareholder_structure_sleeve_15pct_v1"));
        assert!(serialized.contains("shareholder_structure_low_fanout_strict_pit_gate_v1"));
        assert!(serialized.contains("p321d-shareholder-low-fanout-v1"));
        assert!(serialized.contains("\"universe_profile\":\"main_chinext_non_st\""));
        assert!(serialized.contains("\"market_regime\":\"off\""));
        assert!(!serialized.contains("quality_mixed_state_risk_memory_router_v14"));
        assert!(!serialized.contains("phase7_moneyflow_congestion_interaction_v1"));
        assert!(!serialized.contains("phase7_event_post_return_curve_20d_v1"));
        assert!(!serialized.contains("pred-"));

        assert!(config.seed_trials.iter().all(|trial| {
            trial["signal_source"] == "factor_combo"
                && trial["shareholder_structure_sleeve_profile"]
                    == "v19_p321e_fq_change_shareholder_structure_sleeve_v1"
                && trial["shareholder_structure_sleeve_control"]
                    == "phase7_financial_quality_change_v1"
                && trial["alpha_admission_gate_id"]
                    == "shareholder_structure_low_fanout_strict_pit_gate_v1"
                && trial["universe_profile"] == "main_chinext_non_st"
                && trial["market_regime"] == "off"
                && trial["oos_policy"] == "evaluation_only"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
                && trial["shareholder_structure_sleeve_variant"]
                    == "shareholder_structure_sleeve_10pct"
        }));
    }

    #[test]
    fn professional_event_conditioned_sharpe_profile_targets_event_surprise_gates() {
        let config = LayeredSearchConfig::professional_event_conditioned_sharpe_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(!combo_names.contains("phase7_event_earnings_v1"));
        assert!(!combo_names.contains("phase7_event_surprise_v1"));

        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_boost_pos_5pct"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_3pct"
                && profile.combo_name.as_deref() == Some("phase7_event_window_earnings_v1")
        }));

        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_boost_pos_5pct"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_exclude_negative"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
        }));
    }

    #[test]
    fn professional_second_alpha_source_profile_targets_phase7_w_search() {
        let config = LayeredSearchConfig::professional_second_alpha_source_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!combo_names.contains("phase7_event_earnings_v1"));
        assert!(!combo_names.contains("phase7_event_surprise_v1"));
        assert!(!combo_names.contains("phase7_event_window_earnings_v1"));
        assert!(!combo_names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(!combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(!combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(!combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(combo_names.contains("phase7_blend_quality_growth_v1"));
        assert!(combo_names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(config
            .style_risk_budget_profiles
            .contains(&"liquidity_volatility_balanced_v1".to_string()));
        assert!(config.score_directions.contains(&ScoreDirection::Ascending));
        assert!(config
            .score_directions
            .contains(&ScoreDirection::Descending));
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(config.seed_trials[0]["score_direction"], "descending");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["style_risk_budget"] == "liquidity_volatility_balanced_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        for event_like_combo in [
            "phase7_event_earnings_v1",
            "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_surprise_confirm_v1",
            "phase7_quality_event_confirm_v1",
            "phase7_quality_value_recovery_event_confirm_v1",
        ] {
            assert!(
                !config
                    .seed_trials
                    .iter()
                    .any(|trial| trial["combo_name"] == event_like_combo),
                "{event_like_combo} must not be used as a base alpha in Phase 7-W trainable seeds"
            );
        }
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 24,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(4, 16)
            },
        );

        for event_like_combo in [
            "phase7_event_earnings_v1",
            "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_surprise_confirm_v1",
            "phase7_quality_event_confirm_v1",
            "phase7_quality_value_recovery_event_confirm_v1",
        ] {
            assert!(
                !plan
                    .trials
                    .iter()
                    .any(|trial| trial.parameters["combo_name"] == event_like_combo),
                "{event_like_combo} must not be planned as a base alpha"
            );
        }
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
        }));
    }

    #[test]
    fn professional_second_alpha_source_search_can_generate_event_gate_trials() {
        let config = LayeredSearchConfig::professional_second_alpha_source_default();
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_5pct"
                && profile.combo_name.as_deref() == Some("phase7_event_window_earnings_v1")
                && profile.mode.as_deref() == Some("boost_positive")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_exclude_negative"
                && profile.mode.as_deref() == Some("exclude_negative")
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 128,
                batch_size: 4,
                max_parallel_trials: 4,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial.parameters["event_gate_mode"] == "boost_positive"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_exclude_negative"
                && trial.parameters["event_gate_mode"] == "exclude_negative"
        }));
    }

    #[test]
    fn professional_anti_overfit_sharpe_profile_generalizes_u2_without_date_fitting() {
        let config = LayeredSearchConfig::professional_anti_overfit_sharpe_default();

        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(config.seed_trials[0]["rebalance_hysteresis_pct"], "0");
        assert_eq!(config.seed_trials[0]["partial_rebalance_ratio"], "1");

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(!combo_names.contains("phase7_event_earnings_v1"));
        assert!(!combo_names.contains("phase7_event_surprise_v1"));

        assert!(config
            .market_regime_policies
            .contains(&"quality_bear_window_guard_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_bear_window_guard_v2".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v3".to_string()));
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![Decimal::ZERO],
            "anti-overfit profile should not prioritize smoothing that already diluted returns"
        );
        assert_eq!(config.partial_rebalance_ratio, vec![Decimal::ONE]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );

        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_5pct"
                && profile.mode.as_deref() == Some("boost_positive")
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial["event_gate_mode"] == "boost_positive"
        }));

        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_candidate_risk_filter_profile_adds_low_vol_low_corr_axis() {
        let config = LayeredSearchConfig::professional_candidate_risk_filter_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["candidate_risk_filter"], "off",
            "Phase 7-AC should keep the exact anchor first for fair comparison"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["rebalance_hysteresis_pct"] == "0"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
        }));
    }

    #[test]
    fn professional_risk_contribution_profile_adds_single_name_risk_axis() {
        let config = LayeredSearchConfig::professional_risk_contribution_default();

        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "off".to_string(),
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["risk_contribution_control"], "off",
            "Phase 7-AD should keep the exact anchor first for fair comparison"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
    }

    #[test]
    fn alpha_blend_profiles_are_valid_weight_search_candidates() {
        let profiles = phase7_alpha_blend_profiles();
        let names = profiles
            .iter()
            .map(|profile| profile.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(profiles.len(), 34);
        assert!(names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(names.contains("phase7_blend_value_tilt_v1"));
        assert!(names.contains("phase7_blend_quality_growth_v1"));
        assert!(names.contains("phase7_blend_defensive_rel_v1"));
        assert!(names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(names.contains("phase7_quality_cashflow_confirm_v1"));
        assert!(names.contains("phase7_quality_dividend_confirm_v1"));
        assert!(names.contains("phase7_quality_cashflow_dividend_confirm_v1"));
        assert!(names.contains("phase7_quality_event_confirm_v1"));
        assert!(names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(names.contains("phase7_fq_change_event_surprise_sleeve_05pct_v1"));
        assert!(names.contains("phase7_fq_change_event_surprise_sleeve_10pct_v1"));
        assert!(names.contains("phase7_fq_change_event_surprise_sleeve_15pct_v1"));
        assert!(names.contains("phase7_fq_change_supply_float_sleeve_05pct_v1"));
        assert!(names.contains("phase7_fq_change_supply_float_sleeve_10pct_v1"));
        assert!(names.contains("phase7_fq_change_supply_float_sleeve_15pct_v1"));
        assert!(names.contains("phase7_fq_change_unlock_pressure_sleeve_05pct_v1"));
        assert!(names.contains("phase7_fq_change_unlock_pressure_sleeve_10pct_v1"));
        assert!(names.contains("phase7_fq_change_unlock_pressure_sleeve_15pct_v1"));
        assert!(names.contains("phase7_fq_change_forecast_revision_sleeve_05pct_v1"));
        assert!(names.contains("phase7_fq_change_forecast_revision_sleeve_10pct_v1"));
        assert!(names.contains("phase7_fq_change_forecast_revision_sleeve_15pct_v1"));
        assert!(names.contains("phase7_fq_change_shareholder_structure_sleeve_05pct_v1"));
        assert!(names.contains("phase7_fq_change_shareholder_structure_sleeve_10pct_v1"));
        assert!(names.contains("phase7_fq_change_shareholder_structure_sleeve_15pct_v1"));
        assert!(names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        for profile in profiles {
            let weight_sum = profile
                .sources
                .iter()
                .map(|source| source.weight)
                .sum::<Decimal>();
            assert_eq!(weight_sum, Decimal::ONE);
            assert!(profile.sources.len() >= 2);
        }
    }

    #[test]
    fn phase7_alpha_source_admission_separates_trainable_base_from_sparse_event_sources() {
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_cashflow_dividend_confirm_v1").role,
            Phase7AlphaSourceRole::BaseTrainable
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_value_recovery_confirm_v1").role,
            Phase7AlphaSourceRole::BaseTrainable
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_supply_float_shock_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_event_surprise_v1").role,
            Phase7AlphaSourceRole::EventGateOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_forecast_revision_surprise_v1").role,
            Phase7AlphaSourceRole::EventGateOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_repurchase_supply_shock_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_unlock_supply_pressure_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("shareholder_structure").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_event_window_overlay_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_event_post_return_curve_overlay_v1").role,
            Phase7AlphaSourceRole::OptionalOverlayOnly
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_quality_event_surprise_confirm_v1").role,
            Phase7AlphaSourceRole::Excluded
        );
        assert_eq!(
            phase7_alpha_source_admission("phase7_event_reaction_segments_20d_v1").role,
            Phase7AlphaSourceRole::Excluded
        );
        assert!(
            !is_phase7_base_trainable_alpha("phase7_event_window_earnings_v1"),
            "sparse current event sources must stay as gates until rebuilt into broad optional overlays"
        );
    }

    #[test]
    fn layered_search_can_plan_model_prediction_trials() {
        let mut config = LayeredSearchConfig::local_professional_default();
        config.market_regime_policies = vec!["off".to_string()];
        config.combo_versions = Vec::new();
        config.prediction_set_ids = vec!["pred-linear-v1".to_string()];
        config.top_n = vec![20];
        config.rebalance_days = vec![20];
        config.score_directions = vec![ScoreDirection::Descending];
        config.skip_top_pct = vec![Decimal::ZERO];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["heuristic".to_string()];
        config.risk_budget_lookback_days = vec![60];
        config.capacity_penalty_strength = vec![Decimal::ZERO];
        config.industry_max_weight_pct = vec![None];
        config.score_candidate_pool_sizes = vec![0];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::off()];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::off()];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 1;

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(plan.requested_trials, 1);
        assert_eq!(
            plan.trials[0].parameters["signal_source"],
            "model_prediction"
        );
        assert_eq!(
            plan.trials[0].parameters["prediction_set_id"],
            "pred-linear-v1"
        );
        assert!(plan.trials[0].parameters.get("combo_name").is_none());
    }

    #[test]
    fn v19_current_baseline_profile_is_single_seed_only() {
        let config = LayeredSearchConfig::professional_v19_current_baseline_default();
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 8;

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(plan.requested_trials, 1);
        assert_eq!(plan.planned_trials, 1);
        let trial = &plan.trials[0].parameters;
        assert_eq!(trial["signal_source"], "prediction_blend");
        assert_eq!(trial["combo_name"], "full_pit_icir_37f");
        assert_eq!(trial["version"], "1.0.0");
        assert_eq!(
            trial["prediction_set_id"],
            "pred-fullperiod-nlqr-20140101-20260630"
        );
        assert_eq!(trial["prediction_blend_weight"], "0.5");
        assert_eq!(trial["score_direction"], "ascending");
        assert_eq!(trial["top_n"], 30);
        assert_eq!(trial["rebalance"], "10");
        assert_eq!(trial["portfolio_method"], "heuristic");
        assert_eq!(trial["score_candidate_pool_size"], 200);
    }

    #[test]
    fn truncated_layered_search_samples_across_major_axes() {
        let config = LayeredSearchConfig::local_professional_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 8;

        let plan = build_layered_search_plan(&config, &resource_plan);
        let combo_names = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["combo_name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let directions = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["score_direction"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let top_ns = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["top_n"].as_u64())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(plan.requested_trials, 5_267_275_776);
        assert_eq!(plan.planned_trials, 8);
        assert!(plan
            .trials
            .iter()
            .any(|trial| trial.parameters["universe_profile"] == "main_board_non_st"));
        assert!(combo_names.len() > 1);
        assert!(directions.contains("ascending"));
        assert!(directions.contains("descending"));
        assert!(top_ns.len() > 1);
        assert!(plan
            .trials
            .iter()
            .any(|trial| trial.parameters["portfolio_method"] == "risk_budget"));
    }
}
