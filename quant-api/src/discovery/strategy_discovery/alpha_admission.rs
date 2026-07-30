//! BC4 策略发现 / alpha 源准入分级：判定 combo 的角色与可训练性。
//!
//! 由 phase7.rs 拆出（DDD 重构 R8 批次1），纯 move，零行为变更。

use rust_decimal::Decimal;

use super::profiles::{AlphaBlendProfile, AlphaBlendSource, ComboVersion, Phase7AlphaSourceAdmission, Phase7AlphaSourceRole};

pub fn phase7_alpha_source_admission(combo_name: &str) -> Phase7AlphaSourceAdmission {
    let (role, reason) = match combo_name {
        "phase7_financial_quality_v1"
        | "phase7_financial_quality_change_v1"
        | "phase7_earnings_recovery_persistence_v1"
        | "phase7_industry_residual_quality_v1"
        | "phase7_growth_recovery_v1"
        | "phase7_quality_relative_strength_v1"
        | "phase7_valuation_v1"
        | "phase7_moneyflow_v1"
        | "phase7_moneyflow_congestion_interaction_v1"
        | "phase7_quality_moneyflow_pos_5pct_v1"
        | "phase7_quality_cashflow_confirm_v1"
        | "phase7_quality_dividend_confirm_v1"
        | "phase7_quality_cashflow_dividend_confirm_v1"
        | "phase7_quality_value_recovery_confirm_v1"
        | "phase7_quality_residual_confirm_5pct_v1"
        | "phase7_quality_residual_confirm_10pct_v1"
        | "phase7_blend_quality_growth_v1"
        | "phase7_blend_recovery_tilt_v1" => (
            Phase7AlphaSourceRole::BaseTrainable,
            "broad PIT source or broad quality-core optional overlay",
        ),
        "phase7_event_earnings_v1"
        | "phase7_event_surprise_v1"
        | "phase7_forecast_revision_surprise_v1"
        | "phase7_event_window_earnings_v1"
        | "phase7_event_window_earnings_10d_v1"
        | "phase7_event_window_earnings_40d_v1"
        | "phase7_event_post_return_curve_20d_v1" => (
            Phase7AlphaSourceRole::EventGateOnly,
            "current event source is sparse and must gate/boost a broad base instead of defining the base universe",
        ),
        "phase7_quality_event_window_overlay_v1"
        | "phase7_quality_event_post_return_curve_overlay_v1"
        | "phase7_fq_change_event_surprise_sleeve_05pct_v1"
        | "phase7_fq_change_event_surprise_sleeve_10pct_v1"
        | "phase7_fq_change_event_surprise_sleeve_15pct_v1"
        | "phase7_supply_float_shock_v1"
        | "phase7_fq_change_supply_float_sleeve_05pct_v1"
        | "phase7_fq_change_supply_float_sleeve_10pct_v1"
        | "phase7_fq_change_supply_float_sleeve_15pct_v1"
        | "phase7_unlock_supply_pressure_v1"
        | "phase7_fq_change_unlock_pressure_sleeve_05pct_v1"
        | "phase7_fq_change_unlock_pressure_sleeve_10pct_v1"
        | "phase7_fq_change_unlock_pressure_sleeve_15pct_v1"
        | "phase7_fq_change_forecast_revision_sleeve_05pct_v1"
        | "phase7_fq_change_forecast_revision_sleeve_10pct_v1"
        | "phase7_fq_change_forecast_revision_sleeve_15pct_v1"
        | "shareholder_structure"
        | "phase7_fq_change_shareholder_structure_sleeve_05pct_v1"
        | "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
        | "phase7_fq_change_shareholder_structure_sleeve_15pct_v1"
        | "phase7_repurchase_supply_shock_v1" => (
            Phase7AlphaSourceRole::OptionalOverlayOnly,
            "optional overlay must preserve and audit a broad quality base before entering full-market training",
        ),
        "phase7_quality_event_confirm_v1"
        | "phase7_quality_event_surprise_confirm_v1"
        | "phase7_quality_value_recovery_event_confirm_v1" => (
            Phase7AlphaSourceRole::Excluded,
            "event-confirm blend is sparse under the current full-intersection backfill and must be rebuilt as optional overlay before training",
        ),
        "phase7_event_reaction_segments_20d_v1"
        | "phase7_event_reaction_reversal_20d_v1" => (
            Phase7AlphaSourceRole::Excluded,
            "event reaction source is stale/sample-limited and must be rebuilt before full-market training",
        ),
        _ => (
            Phase7AlphaSourceRole::Excluded,
            "source has not been admitted to Phase 7 professional discovery",
        ),
    };

    Phase7AlphaSourceAdmission {
        combo_name: match combo_name {
            "phase7_financial_quality_v1" => "phase7_financial_quality_v1",
            "phase7_financial_quality_change_v1" => "phase7_financial_quality_change_v1",
            "phase7_earnings_recovery_persistence_v1" => "phase7_earnings_recovery_persistence_v1",
            "phase7_supply_float_shock_v1" => "phase7_supply_float_shock_v1",
            "phase7_industry_residual_quality_v1" => "phase7_industry_residual_quality_v1",
            "phase7_growth_recovery_v1" => "phase7_growth_recovery_v1",
            "phase7_quality_relative_strength_v1" => "phase7_quality_relative_strength_v1",
            "phase7_valuation_v1" => "phase7_valuation_v1",
            "phase7_moneyflow_v1" => "phase7_moneyflow_v1",
            "phase7_moneyflow_congestion_interaction_v1" => {
                "phase7_moneyflow_congestion_interaction_v1"
            }
            "phase7_quality_moneyflow_pos_5pct_v1" => "phase7_quality_moneyflow_pos_5pct_v1",
            "phase7_quality_cashflow_confirm_v1" => "phase7_quality_cashflow_confirm_v1",
            "phase7_quality_dividend_confirm_v1" => "phase7_quality_dividend_confirm_v1",
            "phase7_quality_cashflow_dividend_confirm_v1" => {
                "phase7_quality_cashflow_dividend_confirm_v1"
            }
            "phase7_quality_value_recovery_confirm_v1" => {
                "phase7_quality_value_recovery_confirm_v1"
            }
            "phase7_quality_residual_confirm_5pct_v1" => "phase7_quality_residual_confirm_5pct_v1",
            "phase7_quality_residual_confirm_10pct_v1" => {
                "phase7_quality_residual_confirm_10pct_v1"
            }
            "phase7_quality_value_recovery_event_confirm_v1" => {
                "phase7_quality_value_recovery_event_confirm_v1"
            }
            "phase7_blend_quality_growth_v1" => "phase7_blend_quality_growth_v1",
            "phase7_blend_recovery_tilt_v1" => "phase7_blend_recovery_tilt_v1",
            "phase7_event_earnings_v1" => "phase7_event_earnings_v1",
            "phase7_event_surprise_v1" => "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1" => "phase7_event_window_earnings_v1",
            "phase7_event_window_earnings_10d_v1" => "phase7_event_window_earnings_10d_v1",
            "phase7_event_window_earnings_40d_v1" => "phase7_event_window_earnings_40d_v1",
            "phase7_quality_event_confirm_v1" => "phase7_quality_event_confirm_v1",
            "phase7_quality_event_surprise_confirm_v1" => {
                "phase7_quality_event_surprise_confirm_v1"
            }
            "phase7_forecast_revision_surprise_v1" => "phase7_forecast_revision_surprise_v1",
            "phase7_quality_event_window_overlay_v1" => "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_post_return_curve_overlay_v1" => {
                "phase7_quality_event_post_return_curve_overlay_v1"
            }
            "phase7_fq_change_event_surprise_sleeve_05pct_v1" => {
                "phase7_fq_change_event_surprise_sleeve_05pct_v1"
            }
            "phase7_fq_change_event_surprise_sleeve_10pct_v1" => {
                "phase7_fq_change_event_surprise_sleeve_10pct_v1"
            }
            "phase7_fq_change_event_surprise_sleeve_15pct_v1" => {
                "phase7_fq_change_event_surprise_sleeve_15pct_v1"
            }
            "phase7_fq_change_supply_float_sleeve_05pct_v1" => {
                "phase7_fq_change_supply_float_sleeve_05pct_v1"
            }
            "phase7_fq_change_supply_float_sleeve_10pct_v1" => {
                "phase7_fq_change_supply_float_sleeve_10pct_v1"
            }
            "phase7_fq_change_supply_float_sleeve_15pct_v1" => {
                "phase7_fq_change_supply_float_sleeve_15pct_v1"
            }
            "phase7_unlock_supply_pressure_v1" => "phase7_unlock_supply_pressure_v1",
            "phase7_fq_change_unlock_pressure_sleeve_05pct_v1" => {
                "phase7_fq_change_unlock_pressure_sleeve_05pct_v1"
            }
            "phase7_fq_change_unlock_pressure_sleeve_10pct_v1" => {
                "phase7_fq_change_unlock_pressure_sleeve_10pct_v1"
            }
            "phase7_fq_change_unlock_pressure_sleeve_15pct_v1" => {
                "phase7_fq_change_unlock_pressure_sleeve_15pct_v1"
            }
            "phase7_fq_change_forecast_revision_sleeve_05pct_v1" => {
                "phase7_fq_change_forecast_revision_sleeve_05pct_v1"
            }
            "phase7_fq_change_forecast_revision_sleeve_10pct_v1" => {
                "phase7_fq_change_forecast_revision_sleeve_10pct_v1"
            }
            "phase7_fq_change_forecast_revision_sleeve_15pct_v1" => {
                "phase7_fq_change_forecast_revision_sleeve_15pct_v1"
            }
            "shareholder_structure" => "shareholder_structure",
            "phase7_fq_change_shareholder_structure_sleeve_05pct_v1" => {
                "phase7_fq_change_shareholder_structure_sleeve_05pct_v1"
            }
            "phase7_fq_change_shareholder_structure_sleeve_10pct_v1" => {
                "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
            }
            "phase7_fq_change_shareholder_structure_sleeve_15pct_v1" => {
                "phase7_fq_change_shareholder_structure_sleeve_15pct_v1"
            }
            "phase7_event_post_return_curve_20d_v1" => "phase7_event_post_return_curve_20d_v1",
            "phase7_event_reaction_segments_20d_v1" => "phase7_event_reaction_segments_20d_v1",
            "phase7_event_reaction_reversal_20d_v1" => "phase7_event_reaction_reversal_20d_v1",
            _ => "unknown",
        },
        role,
        reason,
    }
}

pub fn is_phase7_base_trainable_alpha(combo_name: &str) -> bool {
    phase7_alpha_source_admission(combo_name).role == Phase7AlphaSourceRole::BaseTrainable
}

pub(super) fn phase7_base_trainable_combo_versions(candidates: &[&str]) -> Vec<ComboVersion> {
    candidates
        .iter()
        .copied()
        .filter(|combo_name| is_phase7_base_trainable_alpha(combo_name))
        .map(|combo_name| ComboVersion::new(combo_name, "1.0.0"))
        .collect()
}

pub fn phase7_alpha_blend_profiles() -> Vec<AlphaBlendProfile> {
    let source = |combo: &str, weight: Decimal| AlphaBlendSource::new(combo, "1.0.0", weight);
    let source_version =
        |combo: &str, version: &str, weight: Decimal| AlphaBlendSource::new(combo, version, weight);
    vec![
        AlphaBlendProfile {
            profile_name: "balanced".to_string(),
            combo_name: "phase7_value_quality_growth_rel_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Balanced valuation, quality, growth/recovery, and relative-strength blend"
                    .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(30, 2)),
                source("phase7_financial_quality_v1", Decimal::new(30, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(30, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "value_tilt".to_string(),
            combo_name: "phase7_blend_value_tilt_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Valuation-heavy blend for cheap-quality recovery candidates".to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(45, 2)),
                source("phase7_financial_quality_v1", Decimal::new(25, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(20, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_growth".to_string(),
            combo_name: "phase7_blend_quality_growth_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Quality and growth/recovery-led blend with valuation as guardrail"
                .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(15, 2)),
                source("phase7_financial_quality_v1", Decimal::new(40, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(35, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "defensive_relative".to_string(),
            combo_name: "phase7_blend_defensive_rel_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Defensive quality blend with a larger relative-strength stabilizer"
                .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(25, 2)),
                source("phase7_financial_quality_v1", Decimal::new(40, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_relative_strength_v1", Decimal::new(20, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "recovery_tilt".to_string(),
            combo_name: "phase7_blend_recovery_tilt_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Growth/recovery-heavy blend for earnings-repair regimes".to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_financial_quality_v1", Decimal::new(25, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(45, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_moneyflow_confirm_5pct".to_string(),
            combo_name: "phase7_quality_moneyflow_pos_5pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light moneyflow confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_moneyflow_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_cashflow_confirm_5pct".to_string(),
            combo_name: "phase7_quality_cashflow_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light cashflow-quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_cashflow_quality_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_dividend_confirm_5pct".to_string(),
            combo_name: "phase7_quality_dividend_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light dividend-quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_dividend_quality_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_cashflow_dividend_confirm_10pct".to_string(),
            combo_name: "phase7_quality_cashflow_dividend_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial quality with cashflow and dividend quality confirmation overlays"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(90, 2)),
                source("phase7_cashflow_quality_v1", Decimal::new(5, 2)),
                source("phase7_dividend_quality_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_confirm_5pct".to_string(),
            combo_name: "phase7_quality_event_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light earnings-event confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_earnings_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_surprise_confirm_5pct".to_string(),
            combo_name: "phase7_quality_event_surprise_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light segmented event-surprise overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_surprise_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_event_surprise_sleeve_05pct".to_string(),
            combo_name: "phase7_fq_change_event_surprise_sleeve_05pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 5% event-surprise sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(95, 2)),
                source("phase7_event_surprise_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_event_surprise_sleeve_10pct".to_string(),
            combo_name: "phase7_fq_change_event_surprise_sleeve_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 10% event-surprise sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(90, 2)),
                source("phase7_event_surprise_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_event_surprise_sleeve_15pct_boundary".to_string(),
            combo_name: "phase7_fq_change_event_surprise_sleeve_15pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a 15% event-surprise boundary sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(85, 2)),
                source("phase7_event_surprise_v1", Decimal::new(15, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_supply_float_sleeve_05pct".to_string(),
            combo_name: "phase7_fq_change_supply_float_sleeve_05pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 5% supply-float shock sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(95, 2)),
                source("phase7_supply_float_shock_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_supply_float_sleeve_10pct".to_string(),
            combo_name: "phase7_fq_change_supply_float_sleeve_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 10% supply-float shock sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(90, 2)),
                source("phase7_supply_float_shock_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_supply_float_sleeve_15pct_boundary".to_string(),
            combo_name: "phase7_fq_change_supply_float_sleeve_15pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a 15% supply-float shock boundary sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(85, 2)),
                source("phase7_supply_float_shock_v1", Decimal::new(15, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_unlock_pressure_sleeve_05pct".to_string(),
            combo_name: "phase7_fq_change_unlock_pressure_sleeve_05pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 5% unlock-pressure sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(95, 2)),
                source("phase7_unlock_supply_pressure_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_unlock_pressure_sleeve_10pct".to_string(),
            combo_name: "phase7_fq_change_unlock_pressure_sleeve_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 10% unlock-pressure sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(90, 2)),
                source("phase7_unlock_supply_pressure_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_unlock_pressure_sleeve_15pct_boundary".to_string(),
            combo_name: "phase7_fq_change_unlock_pressure_sleeve_15pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a 15% unlock-pressure boundary sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(85, 2)),
                source("phase7_unlock_supply_pressure_v1", Decimal::new(15, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_forecast_revision_sleeve_05pct".to_string(),
            combo_name: "phase7_fq_change_forecast_revision_sleeve_05pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 5% forecast-revision sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(95, 2)),
                source("phase7_forecast_revision_surprise_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_forecast_revision_sleeve_10pct".to_string(),
            combo_name: "phase7_fq_change_forecast_revision_sleeve_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 10% forecast-revision sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(90, 2)),
                source("phase7_forecast_revision_surprise_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_forecast_revision_sleeve_15pct_boundary".to_string(),
            combo_name: "phase7_fq_change_forecast_revision_sleeve_15pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a 15% forecast-revision boundary sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(85, 2)),
                source("phase7_forecast_revision_surprise_v1", Decimal::new(15, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_shareholder_structure_sleeve_05pct".to_string(),
            combo_name: "phase7_fq_change_shareholder_structure_sleeve_05pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 5% strict-PIT shareholder-structure sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(95, 2)),
                source_version(
                    "shareholder_structure",
                    "p321d-shareholder-low-fanout-v1",
                    Decimal::new(5, 2),
                ),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_shareholder_structure_sleeve_10pct".to_string(),
            combo_name: "phase7_fq_change_shareholder_structure_sleeve_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a bounded 10% strict-PIT shareholder-structure sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(90, 2)),
                source_version(
                    "shareholder_structure",
                    "p321d-shareholder-low-fanout-v1",
                    Decimal::new(10, 2),
                ),
            ],
        },
        AlphaBlendProfile {
            profile_name: "fq_change_shareholder_structure_sleeve_15pct_boundary".to_string(),
            combo_name: "phase7_fq_change_shareholder_structure_sleeve_15pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial-quality-change acceleration with a 15% strict-PIT shareholder-structure boundary sleeve"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_change_v1", Decimal::new(85, 2)),
                source_version(
                    "shareholder_structure",
                    "p321d-shareholder-low-fanout-v1",
                    Decimal::new(15, 2),
                ),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_window_overlay_5pct".to_string(),
            combo_name: "phase7_quality_event_window_overlay_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with optional sparse post-event window overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_window_earnings_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_post_return_curve_overlay_5pct".to_string(),
            combo_name: "phase7_quality_event_post_return_curve_overlay_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with optional sparse PIT event post-return curve overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_post_return_curve_20d_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_reaction_segments_overlay_5pct".to_string(),
            combo_name: "phase7_quality_event_reaction_segments_overlay_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with optional segmented post-event reaction overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_reaction_segments_20d_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_reaction_reversal_overlay_5pct".to_string(),
            combo_name: "phase7_quality_event_reaction_reversal_overlay_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with optional negative post-event reaction reversal overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_reaction_reversal_20d_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_residual_confirm_5pct".to_string(),
            combo_name: "phase7_quality_residual_confirm_5pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with a small industry-residual quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_industry_residual_quality_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_residual_confirm_10pct".to_string(),
            combo_name: "phase7_quality_residual_confirm_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with a moderate industry-residual quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(90, 2)),
                source("phase7_industry_residual_quality_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_value_recovery_confirm_5pct".to_string(),
            combo_name: "phase7_quality_value_recovery_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial quality core with valuation, earnings-recovery, and moneyflow confirmation"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(60, 2)),
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_moneyflow_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_value_recovery_event_confirm_5pct".to_string(),
            combo_name: "phase7_quality_value_recovery_event_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial quality core with valuation, earnings-recovery, and event confirmation"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(60, 2)),
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_event_earnings_v1", Decimal::new(5, 2)),
            ],
        },
    ]
}
