//! BC4 策略发现 / seed_generators / regime：市场状态/风格守门 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_regime_stabilization_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_sharpe_stabilization_seed_trials() {
        let is_quality_mainline = seed["combo_name"] == "phase7_financial_quality_v1"
            && seed["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
            && seed["stop_loss_pct"] == "0.075"
            && seed["reentry_cooldown_days"] == 30;
        let volatility_control = seed["portfolio_volatility_control"]
            .as_str()
            .unwrap_or("off");
        let keeps_phase7_t_volatility_shape =
            volatility_control == "vol120_24_70_100" || volatility_control == "vol120_22_65_100";

        if !is_quality_mainline || !keeps_phase7_t_volatility_shape {
            continue;
        }

        for market_regime in [
            "quality_crash_guard_v1",
            "quality_crash_guard_v2",
            "quality_crash_guard_v3",
        ] {
            let mut regime_seed = seed.clone();
            regime_seed["market_regime"] = json!(market_regime);
            seeds.push(regime_seed);
        }
    }
    seeds
}

pub(crate) fn professional_bear_window_stabilization_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_regime_stabilization_seed_trials() {
        for market_regime in [
            "quality_bear_window_guard_v1",
            "quality_bear_window_guard_v2",
        ] {
            let mut bear_seed = seed.clone();
            bear_seed["market_regime"] = json!(market_regime);
            if !seeds.contains(&bear_seed) {
                seeds.push(bear_seed);
            }
        }
    }
    seeds
}

pub(crate) fn professional_style_risk_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_bear_window_stabilization_seed_trials() {
        for style_risk_budget in [
            "liquidity_volatility_balanced_v1",
            "defensive_style_budget_v1",
        ] {
            let mut style_seed = seed.clone();
            style_seed["style_risk_budget"] = json!(style_risk_budget);
            if !seeds.contains(&style_seed) {
                seeds.push(style_seed);
            }
        }
    }
    seeds
}

pub(crate) fn professional_regime_conditioned_valuation_guard_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let stress_regimes = ["bear", "high_volatility"];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            vol22_anchor.clone(),
            "valuation_exclude_bottom35",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            "0",
            ScoreDirection::Descending,
        )],
    );
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_active_in(
            vol22_anchor,
            "valuation_exclude_bottom35_stress_only",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            ScoreDirection::Descending,
            &stress_regimes,
        )],
    );

    append_unique_seeds(&mut seeds, vec![vol18_anchor.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            vol18_anchor.clone(),
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            "0",
            ScoreDirection::Descending,
        )],
    );
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_active_in(
            vol18_anchor,
            "valuation_exclude_bottom40_stress_only",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            ScoreDirection::Descending,
            &stress_regimes,
        )],
    );

    seeds
}

pub(crate) fn professional_regime_alpha_routing_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let routed_vol22 =
        with_market_regime_seed(vol22_anchor.clone(), "quality_regime_alpha_switch_v1");
    let routed_vol18 =
        with_market_regime_seed(vol18_anchor.clone(), "quality_regime_alpha_switch_v1");
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone(), routed_vol22.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            routed_vol22,
            "valuation_exclude_bottom35",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            "0",
            ScoreDirection::Descending,
        )],
    );

    append_unique_seeds(&mut seeds, vec![vol18_anchor.clone(), routed_vol18.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            routed_vol18,
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            "0",
            ScoreDirection::Descending,
        )],
    );

    seeds
}

pub(crate) fn professional_regime_alpha_sleeve_search_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let stress_policies = [
        "quality_regime_alpha_switch_value_v1",
        "quality_regime_alpha_switch_recovery_v1",
        "quality_regime_alpha_switch_blend_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in stress_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

pub(crate) fn professional_regime_alpha_overlay_search_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let overlay_policies = [
        "quality_regime_alpha_overlay_value_05pct_v1",
        "quality_regime_alpha_overlay_value_10pct_v1",
        "quality_regime_alpha_overlay_blend_10pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in overlay_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

pub(crate) fn professional_regime_alpha_sleeve_allocation_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

pub(crate) fn professional_low_risk_sleeve_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

pub(crate) fn professional_value_guard_sleeve_composition_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let mut vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    vol18_anchor["event_gate_profile"] = json!("off");

    let value35_vol22 = with_event_combo_gate_seed_with_min_score(
        vol22_anchor,
        "valuation_exclude_bottom35",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.35",
        "0",
        ScoreDirection::Descending,
    );
    let value40_vol18 = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
    ];
    let guarded_anchors = [value35_vol22, value40_vol18];
    let mut seeds = Vec::new();

    for seed in &guarded_anchors {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for policy in sleeve_policies {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), policy)],
            );
        }
    }

    seeds
}

pub(crate) fn professional_nearest_candidate_risk_model_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let nearest_candidate = with_market_regime_seed(
        value40_anchor.clone(),
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
    );
    let anchors = [value40_anchor, nearest_candidate];
    let mut seeds = Vec::new();

    for seed in &anchors {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(seed.clone(), "risk_budget", 120)],
        );
        for lookback_days in [120, 180] {
            append_unique_seeds(
                &mut seeds,
                vec![with_portfolio_method_seed(
                    seed.clone(),
                    "min_variance",
                    lookback_days,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_state_return_sharpe_router_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let Some(high_sharpe_boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return vec![return_anchor];
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        high_sharpe_boundary.clone(),
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let boundary_vol18 = with_volatility_profile_seed(
        high_sharpe_boundary.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let mut seeds = Vec::new();

    for (seed, regime, profile_name) in [
        (
            return_anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "return_anchor_event_window_15pct",
        ),
        (
            return_anchor.clone(),
            "quality_event_window_return_sharpe_router_v1",
            "return_anchor_router_v1",
        ),
        (
            return_anchor.clone(),
            "quality_event_window_return_sharpe_router_v2",
            "return_anchor_router_v2",
        ),
        (
            boundary_vol18.clone(),
            "quality_event_window_return_sharpe_router_v1",
            "boundary_vol18_router_v1",
        ),
        (
            boundary_vol18.clone(),
            "quality_event_window_return_sharpe_router_v2",
            "boundary_vol18_router_v2",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_sleeve_seed(seed, regime, profile_name)],
        );
    }

    for lookback_days in [160, 180] {
        for (regime, suffix) in [
            ("quality_event_window_return_sharpe_router_v1", "router_v1"),
            ("quality_event_window_return_sharpe_router_v2", "router_v2"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        regime,
                        &format!("valuation45_{suffix}"),
                    ),
                    &format!("valuation45_rb{lookback_days}_{suffix}"),
                    lookback_days,
                )],
            );
        }
    }

    append_unique_seeds(
        &mut seeds,
        vec![with_top_n_seed(
            with_event_sleeve_seed(
                boundary_vol18,
                "quality_event_window_return_sharpe_router_v2",
                "boundary_top22_router_v2",
            ),
            22,
            "top22_boundary_router_v2",
        )],
    );

    seeds.into_iter().take(10).collect()
}

pub(crate) fn professional_state_return_sharpe_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation40 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        valuation40.clone(),
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, suffix, source_seed) in [
        (
            "quality_event_window_return_sharpe_router_v1",
            "router_v1",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v2",
            "router_v2",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "router_v4",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "valuation45_router_v3",
            valuation45.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "valuation45_router_v4",
            valuation45,
        ),
    ] {
        for (vol_profile, target_pct, min_exposure) in [
            ("vol120_16_50_100", "0.16", "0.50"),
            ("vol120_15_48_100", "0.15", "0.48"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            source_seed.clone(),
                            regime,
                            &format!("{suffix}_{vol_profile}"),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("bp_rb160_{suffix}"),
                    160,
                )],
            );
        }
    }

    for (lookback_days, regime, suffix) in [
        (
            150,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
        (
            170,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
        (
            180,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_event_sleeve_seed(valuation40.clone(), regime, &format!("bp_{suffix}")),
                &format!("bp_rb{lookback_days}_{suffix}"),
                lookback_days,
            )],
        );
    }

    seeds.into_iter().take(12).collect()
}

pub(crate) fn professional_regime_alpha_selector_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_event_window_return_sharpe_router_v4",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_selector_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}"),
            160,
        );
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_regime_alpha_overlay_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure, smoothing_profile, hysteresis, partial) in [
        (
            "quality_state_alpha_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
    ] {
        let mut seed = with_rebalance_smoothing_seed(
            with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        regime,
                        &format!("{regime}_{vol_profile}_{smoothing_profile}"),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb160_{regime}_{vol_profile}_{smoothing_profile}"),
                160,
            ),
            smoothing_profile,
            hysteresis,
            partial,
        );
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_regime_frontier_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "off",
            650,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            with_event_combo_gate_seed_with_min_score(
                                return_anchor.clone(),
                                valuation_profile,
                                "phase7_valuation_v1",
                                "exclude_negative",
                                valuation_min_score,
                                "0",
                                ScoreDirection::Descending,
                            ),
                            regime,
                            &format!(
                                "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_frontier_bridge"
                            ),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("maxpos15_corr75_{regime}_{vol_profile}"),
                    "0.15",
                    "0.75",
                ),
                &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            "0.50",
            "-0.10",
            180,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_regime_frontier_decomposition_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "off",
            650,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            with_event_combo_gate_seed_with_min_score(
                                return_anchor.clone(),
                                valuation_profile,
                                "phase7_valuation_v1",
                                "exclude_negative",
                                valuation_min_score,
                                "0",
                                ScoreDirection::Descending,
                            ),
                            regime,
                            &format!(
                                "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_frontier_decomp"
                            ),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("maxpos15_corr75_{regime}_{vol_profile}"),
                    "0.15",
                    "0.75",
                ),
                &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            "0.50",
            "-0.10",
            180,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_volatility_sharpe_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let tighter_profiles = [
        ("vol120_20_60_100", "0.20", 120, "0.60", "1"),
        ("vol120_18_55_100", "0.18", 120, "0.55", "1"),
        ("vol252_16_45_100", "0.16", 252, "0.45", "1"),
    ];
    let mut seeds = Vec::new();

    for seed in &anchor_seeds {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for profile in tighter_profiles {
            append_unique_seeds(
                &mut seeds,
                vec![with_volatility_profile_seed(
                    seed.clone(),
                    profile.0,
                    profile.1,
                    profile.2,
                    profile.3,
                    profile.4,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_regime_position_sharpe_seed_trials() -> Vec<Value> {
    let mut anchors = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchors.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let mut seeds = Vec::new();

    for market_regime in [
        "quality_bear_window_guard_v2",
        "quality_bear_position_guard_v1",
        "quality_bear_position_guard_v2",
        "quality_crash_guard_v3",
        "quality_risk_off_v1",
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol18_anchor.clone(), market_regime)],
        );
    }

    for market_regime in [
        "quality_bear_position_guard_v1",
        "quality_bear_position_guard_v2",
        "quality_bear_window_guard_v2",
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), market_regime)],
        );
    }

    seeds
}

pub(crate) fn professional_state_alpha_router_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(anchor.clone(), "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let seed_specs = [
        (
            anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_overlay_value_05pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
        ),
        (
            anchor,
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        ),
        (
            vol18_anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        ),
        (
            vol18_anchor.clone(),
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ),
        (
            vol18_anchor,
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        ),
    ];
    let mut seeds = Vec::new();

    for (seed, policy) in seed_specs {
        append_unique_seeds(&mut seeds, vec![with_market_regime_seed(seed, policy)]);
    }

    seeds
}

pub(crate) fn professional_state_position_risk_router_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(anchor.clone(), "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let mut seeds = Vec::new();

    for seed in [anchor.clone(), vol18_anchor.clone()] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(
                seed.clone(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            )],
        );
        for market_regime in [
            "quality_bear_position_guard_v3",
            "quality_bear_position_guard_v1",
            "quality_bear_position_guard_v2",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }
    }

    seeds
}

pub(crate) fn professional_all_regime_event_sleeve_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(anchor.clone(), "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let mut seeds = Vec::new();

    for seed in [anchor.clone(), vol18_anchor.clone()] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(
                seed.clone(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            )],
        );
        for market_regime in [
            "quality_all_regime_event_window_sleeve_05pct_v1",
            "quality_all_regime_event_window_sleeve_10pct_v1",
            "quality_all_regime_event_window_sleeve_15pct_v1",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }
    }

    seeds
}

pub(crate) fn professional_portfolio_sharpe_control_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let Some(high_sharpe_boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![anchor.clone(), high_sharpe_boundary]);

    for (profile_name, reduce_start, reduce_full, lookback_days, min_exposure) in [
        ("roll_sharpe120_060_000_55", "0.60", "0.00", 120, "0.55"),
        ("roll_sharpe120_050_neg10_60", "0.50", "-0.10", 120, "0.60"),
        ("roll_sharpe180_060_000_55", "0.60", "0.00", 180, "0.55"),
        ("roll_sharpe180_050_neg10_60", "0.50", "-0.10", 180, "0.60"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_sharpe_profile_seed(
                anchor.clone(),
                profile_name,
                reduce_start,
                reduce_full,
                lookback_days,
                min_exposure,
            )],
        );
    }

    let bridge = with_market_regime_seed(anchor, "quality_state_sharpe_bridge_router_v2");
    append_unique_seeds(
        &mut seeds,
        vec![
            with_sharpe_profile_seed(
                bridge.clone(),
                "bridge_roll_sharpe120_050_neg10_60",
                "0.50",
                "-0.10",
                120,
                "0.60",
            ),
            with_sharpe_profile_seed(
                bridge,
                "bridge_roll_sharpe180_050_neg10_60",
                "0.50",
                "-0.10",
                180,
                "0.60",
            ),
        ],
    );

    seeds
}
