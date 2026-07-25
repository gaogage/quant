//! Capacity / risk budget / regime policy profiles and presets.
use super::*;

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            combo_name: "icir_weighted_3f".into(),
            version: "1.0.0".into(),
            top_n: 20,
            rebalance_freq_days: 20,    // monthly default
            entry_delay_days: 0,        // no delay by default
            min_daily_amount_cny: None, // no filter by default
            max_position_pct: Decimal::new(10, 2),
            portfolio_notional_cny: None,
            max_participation_rate: None,
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::Off,
            cash_utilization_profile: CashUtilizationProfile::Off,
            execution_impact_budget_profile: ExecutionImpactBudgetProfile::Off,
            skip_top_pct: 0.0,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            score_direction: ScoreDirection::Descending,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
            industry_max_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
            candidate_ranking_profile: CandidateRankingProfile::Off,
            risk_contribution_control_profile: RiskContributionControlProfile::Off,
            stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile::Off,
            rebalance_hysteresis_pct: 0.0,
            partial_rebalance_ratio: 1.0,
            score_candidate_pool_size: None,
            universe_profile: TradableUniverseProfile::All,
            prediction_blend: None,
            event_gate: None,
            score_overlay: None,
            portfolio_sleeve: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    Descending,
    Ascending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioConstructionMethod {
    Heuristic,
    RiskBudget,
    StressFillAwareRiskBudget,
    MinVariance,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityRiskBudgetProfile {
    #[default]
    Off,
    ParticipationBalancedV1,
    ParticipationStrictV1,
    StressParticipationSoftCapV1,
    StressParticipationTargetScaleV1,
    StressParticipationFloor35V1,
    StressParticipationFloor50V1,
    StressParticipationFloor60V1,
    StressParticipationFloor70V1,
    StressParticipationSoftFloor60V1,
    StressParticipationHeadroomFloor60V1,
    StressParticipationHeadroomFloor70V1,
    StressParticipationAlphaHeadroomFloor60V1,
    StressParticipationAlphaHeadroomFloor70V1,
    StressParticipationAlphaHeadroomFloor85V1,
    StressParticipationBlendedAlphaHeadroomFloor60V1,
    StressParticipationBlendedAlphaHeadroomFloor70V1,
    StressParticipationBlendedAlphaHeadroomFloor85V1,
}

impl CapacityRiskBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "participation_balanced_v1"
            | "participation-balanced-v1"
            | "capacity_participation_balanced_v1"
            | "capacity-participation-balanced-v1" => Ok(Self::ParticipationBalancedV1),
            "participation_strict_v1"
            | "participation-strict-v1"
            | "capacity_participation_strict_v1"
            | "capacity-participation-strict-v1" => Ok(Self::ParticipationStrictV1),
            "stress_participation_soft_cap_v1"
            | "stress-participation-soft-cap-v1"
            | "capacity_stress_participation_soft_cap_v1"
            | "capacity-stress-participation-soft-cap-v1" => Ok(Self::StressParticipationSoftCapV1),
            "stress_participation_target_scale_v1"
            | "stress-participation-target-scale-v1"
            | "capacity_stress_participation_target_scale_v1"
            | "capacity-stress-participation-target-scale-v1" => {
                Ok(Self::StressParticipationTargetScaleV1)
            }
            "stress_participation_floor_35_v1"
            | "stress-participation-floor-35-v1"
            | "capacity_stress_participation_floor_35_v1"
            | "capacity-stress-participation-floor-35-v1" => Ok(Self::StressParticipationFloor35V1),
            "stress_participation_floor_50_v1"
            | "stress-participation-floor-50-v1"
            | "capacity_stress_participation_floor_50_v1"
            | "capacity-stress-participation-floor-50-v1" => Ok(Self::StressParticipationFloor50V1),
            "stress_participation_floor_60_v1"
            | "stress-participation-floor-60-v1"
            | "capacity_stress_participation_floor_60_v1"
            | "capacity-stress-participation-floor-60-v1" => Ok(Self::StressParticipationFloor60V1),
            "stress_participation_floor_70_v1"
            | "stress-participation-floor-70-v1"
            | "capacity_stress_participation_floor_70_v1"
            | "capacity-stress-participation-floor-70-v1" => Ok(Self::StressParticipationFloor70V1),
            "stress_participation_soft_floor_60_v1"
            | "stress-participation-soft-floor-60-v1"
            | "capacity_stress_participation_soft_floor_60_v1"
            | "capacity-stress-participation-soft-floor-60-v1" => {
                Ok(Self::StressParticipationSoftFloor60V1)
            }
            "stress_participation_headroom_floor_60_v1"
            | "stress-participation-headroom-floor-60-v1"
            | "capacity_stress_participation_headroom_floor_60_v1"
            | "capacity-stress-participation-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationHeadroomFloor60V1)
            }
            "stress_participation_headroom_floor_70_v1"
            | "stress-participation-headroom-floor-70-v1"
            | "capacity_stress_participation_headroom_floor_70_v1"
            | "capacity-stress-participation-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationHeadroomFloor70V1)
            }
            "stress_participation_alpha_headroom_floor_60_v1"
            | "stress-participation-alpha-headroom-floor-60-v1"
            | "capacity_stress_participation_alpha_headroom_floor_60_v1"
            | "capacity-stress-participation-alpha-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationAlphaHeadroomFloor60V1)
            }
            "stress_participation_alpha_headroom_floor_70_v1"
            | "stress-participation-alpha-headroom-floor-70-v1"
            | "capacity_stress_participation_alpha_headroom_floor_70_v1"
            | "capacity-stress-participation-alpha-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationAlphaHeadroomFloor70V1)
            }
            "stress_participation_alpha_headroom_floor_85_v1"
            | "stress-participation-alpha-headroom-floor-85-v1"
            | "capacity_stress_participation_alpha_headroom_floor_85_v1"
            | "capacity-stress-participation-alpha-headroom-floor-85-v1" => {
                Ok(Self::StressParticipationAlphaHeadroomFloor85V1)
            }
            "stress_participation_blended_alpha_headroom_floor_60_v1"
            | "stress-participation-blended-alpha-headroom-floor-60-v1"
            | "capacity_stress_participation_blended_alpha_headroom_floor_60_v1"
            | "capacity-stress-participation-blended-alpha-headroom-floor-60-v1" => {
                Ok(Self::StressParticipationBlendedAlphaHeadroomFloor60V1)
            }
            "stress_participation_blended_alpha_headroom_floor_70_v1"
            | "stress-participation-blended-alpha-headroom-floor-70-v1"
            | "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
            | "capacity-stress-participation-blended-alpha-headroom-floor-70-v1" => {
                Ok(Self::StressParticipationBlendedAlphaHeadroomFloor70V1)
            }
            "stress_participation_blended_alpha_headroom_floor_85_v1"
            | "stress-participation-blended-alpha-headroom-floor-85-v1"
            | "capacity_stress_participation_blended_alpha_headroom_floor_85_v1"
            | "capacity-stress-participation-blended-alpha-headroom-floor-85-v1" => {
                Ok(Self::StressParticipationBlendedAlphaHeadroomFloor85V1)
            }
            other => Err(format!("unsupported capacity_risk_budget: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<CapacityRiskBudgetParams> {
        match self {
            Self::Off => None,
            Self::ParticipationBalancedV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.30,
                low_capacity_max_weight_pct: 0.30,
                refill_gross_exposure: true,
                participation_cap_multiplier: 1.0,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::ParticipationStrictV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.40,
                low_capacity_max_weight_pct: 0.20,
                refill_gross_exposure: true,
                participation_cap_multiplier: 1.0,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationSoftCapV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.15,
                refill_gross_exposure: true,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationTargetScaleV1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: None,
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor35V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.35),
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor50V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.50),
                floor_refill_cap_multiplier: 1.0,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationSoftFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.15,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.65,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::ExistingWeight,
            }),
            Self::StressParticipationHeadroomFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::Headroom,
            }),
            Self::StressParticipationHeadroomFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::Headroom,
            }),
            Self::StressParticipationAlphaHeadroomFloor60V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.60),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::AlphaHeadroom,
            }),
            Self::StressParticipationAlphaHeadroomFloor70V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.12,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.70),
                floor_refill_cap_multiplier: 1.50,
                floor_refill_mode: CapacityFloorRefillMode::AlphaHeadroom,
            }),
            Self::StressParticipationBlendedAlphaHeadroomFloor60V1 => {
                Some(CapacityRiskBudgetParams {
                    low_capacity_quantile: 0.35,
                    low_capacity_max_weight_pct: 0.12,
                    refill_gross_exposure: false,
                    participation_cap_multiplier: 0.50,
                    min_target_gross_exposure_pct: Some(0.60),
                    floor_refill_cap_multiplier: 1.25,
                    floor_refill_mode: CapacityFloorRefillMode::BlendedAlphaHeadroom,
                })
            }
            Self::StressParticipationBlendedAlphaHeadroomFloor70V1 => {
                Some(CapacityRiskBudgetParams {
                    low_capacity_quantile: 0.35,
                    low_capacity_max_weight_pct: 0.12,
                    refill_gross_exposure: false,
                    participation_cap_multiplier: 0.50,
                    min_target_gross_exposure_pct: Some(0.70),
                    floor_refill_cap_multiplier: 1.50,
                    floor_refill_mode: CapacityFloorRefillMode::BlendedAlphaHeadroom,
                })
            }
            Self::StressParticipationAlphaHeadroomFloor85V1 => Some(CapacityRiskBudgetParams {
                low_capacity_quantile: 0.35,
                low_capacity_max_weight_pct: 0.14,
                refill_gross_exposure: false,
                participation_cap_multiplier: 0.50,
                min_target_gross_exposure_pct: Some(0.85),
                floor_refill_cap_multiplier: 1.25,
                floor_refill_mode: CapacityFloorRefillMode::AlphaHeadroom,
            }),
            Self::StressParticipationBlendedAlphaHeadroomFloor85V1 => {
                Some(CapacityRiskBudgetParams {
                    low_capacity_quantile: 0.35,
                    low_capacity_max_weight_pct: 0.14,
                    refill_gross_exposure: false,
                    participation_cap_multiplier: 0.50,
                    min_target_gross_exposure_pct: Some(0.85),
                    floor_refill_cap_multiplier: 1.25,
                    floor_refill_mode: CapacityFloorRefillMode::BlendedAlphaHeadroom,
                })
            }
        }
    }

    pub(crate) fn uses_capacity(self) -> bool {
        self.params().is_some()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CapacityRiskBudgetParams {
    pub(crate) low_capacity_quantile: f64,
    pub(crate) low_capacity_max_weight_pct: f64,
    pub(crate) refill_gross_exposure: bool,
    pub(crate) participation_cap_multiplier: f64,
    pub(crate) min_target_gross_exposure_pct: Option<f64>,
    pub(crate) floor_refill_cap_multiplier: f64,
    pub(crate) floor_refill_mode: CapacityFloorRefillMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapacityFloorRefillMode {
    ExistingWeight,
    Headroom,
    AlphaHeadroom,
    BlendedAlphaHeadroom,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CashUtilizationProfile {
    #[default]
    Off,
    FillableGross90V1,
    FillableGross95V1,
    StressFillGross98V1,
}

impl CashUtilizationProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "fillable_gross_90_v1"
            | "fillable-gross-90-v1"
            | "cash_utilization_90pct_v1"
            | "cash-utilization-90pct-v1"
            | "cash_utilization_fillable_gross_90_v1"
            | "cash-utilization-fillable-gross-90-v1" => Ok(Self::FillableGross90V1),
            "fillable_gross_95_v1"
            | "fillable-gross-95-v1"
            | "cash_utilization_95pct_v1"
            | "cash-utilization-95pct-v1"
            | "cash_utilization_fillable_gross_95_v1"
            | "cash-utilization-fillable-gross-95-v1" => Ok(Self::FillableGross95V1),
            "stress_fill_gross_98_v1"
            | "stress-fill-gross-98-v1"
            | "fillable_gross_98_v1"
            | "fillable-gross-98-v1"
            | "cash_utilization_stress_fill_gross_98_v1"
            | "cash-utilization-stress-fill-gross-98-v1" => Ok(Self::StressFillGross98V1),
            other => Err(format!("unsupported cash_utilization: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<CashUtilizationParams> {
        match self {
            Self::Off => None,
            Self::FillableGross90V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.90,
                max_holdings: 50,
            }),
            Self::FillableGross95V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.95,
                max_holdings: 60,
            }),
            Self::StressFillGross98V1 => Some(CashUtilizationParams {
                min_gross_exposure_pct: 0.98,
                max_holdings: 120,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CashUtilizationParams {
    pub(crate) min_gross_exposure_pct: f64,
    pub(crate) max_holdings: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionImpactBudgetProfile {
    #[default]
    Off,
    Turnover30PctV1,
    Turnover20PctV1,
    Turnover15PctV1,
}

impl ExecutionImpactBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "turnover_30pct_v1"
            | "turnover-30pct-v1"
            | "impact_turnover_30pct_v1"
            | "impact-turnover-30pct-v1"
            | "execution_impact_turnover_30pct_v1"
            | "execution-impact-turnover-30pct-v1" => Ok(Self::Turnover30PctV1),
            "turnover_20pct_v1"
            | "turnover-20pct-v1"
            | "impact_turnover_20pct_v1"
            | "impact-turnover-20pct-v1"
            | "execution_impact_turnover_20pct_v1"
            | "execution-impact-turnover-20pct-v1" => Ok(Self::Turnover20PctV1),
            "turnover_15pct_v1"
            | "turnover-15pct-v1"
            | "impact_turnover_15pct_v1"
            | "impact-turnover-15pct-v1"
            | "execution_impact_turnover_15pct_v1"
            | "execution-impact-turnover-15pct-v1" => Ok(Self::Turnover15PctV1),
            other => Err(format!("unsupported execution_impact_budget: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<ExecutionImpactBudgetParams> {
        match self {
            Self::Off => None,
            Self::Turnover30PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.30,
                max_new_name_weight_pct: 0.06,
            }),
            Self::Turnover20PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.20,
                max_new_name_weight_pct: 0.04,
            }),
            Self::Turnover15PctV1 => Some(ExecutionImpactBudgetParams {
                max_rebalance_turnover_pct: 0.15,
                max_new_name_weight_pct: 0.03,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionImpactBudgetParams {
    pub(crate) max_rebalance_turnover_pct: f64,
    pub(crate) max_new_name_weight_pct: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StyleRiskBudgetProfile {
    #[default]
    Off,
    LiquidityVolatilityBalancedV1,
    DefensiveStyleBudgetV1,
}

impl StyleRiskBudgetProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "liquidity_volatility_balanced_v1" | "liquidity-volatility-balanced-v1" => {
                Ok(Self::LiquidityVolatilityBalancedV1)
            }
            "defensive_style_budget_v1" | "defensive-style-budget-v1" => {
                Ok(Self::DefensiveStyleBudgetV1)
            }
            other => Err(format!("unsupported style_risk_budget: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<StyleRiskBudgetParams> {
        match self {
            Self::Off => None,
            Self::LiquidityVolatilityBalancedV1 => Some(StyleRiskBudgetParams {
                high_volatility_quantile: 0.70,
                high_volatility_max_weight_pct: 0.40,
                low_liquidity_quantile: 0.30,
                low_liquidity_max_weight_pct: 0.35,
            }),
            Self::DefensiveStyleBudgetV1 => Some(StyleRiskBudgetParams {
                high_volatility_quantile: 0.60,
                high_volatility_max_weight_pct: 0.30,
                low_liquidity_quantile: 0.35,
                low_liquidity_max_weight_pct: 0.30,
            }),
        }
    }

    pub(crate) fn uses_liquidity(self) -> bool {
        self.params()
            .map(|params| params.low_liquidity_max_weight_pct < 1.0)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StyleRiskBudgetParams {
    pub(crate) high_volatility_quantile: f64,
    pub(crate) high_volatility_max_weight_pct: f64,
    pub(crate) low_liquidity_quantile: f64,
    pub(crate) low_liquidity_max_weight_pct: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRiskFilterProfile {
    #[default]
    Off,
    LowVolatilityV1,
    LowVolatilityLowCorrelationV1,
    SoftLowVolatilityV1,
    SoftLowVolatilityLowCorrelationV1,
    SoftLiquidityLowVolatilityLowCorrelationV1,
}

impl CandidateRiskFilterProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "low_volatility_v1" | "low-volatility-v1" => Ok(Self::LowVolatilityV1),
            "low_volatility_low_correlation_v1" | "low-volatility-low-correlation-v1" => {
                Ok(Self::LowVolatilityLowCorrelationV1)
            }
            "soft_low_volatility_v1" | "soft-low-volatility-v1" => Ok(Self::SoftLowVolatilityV1),
            "soft_low_volatility_low_correlation_v1" | "soft-low-volatility-low-correlation-v1" => {
                Ok(Self::SoftLowVolatilityLowCorrelationV1)
            }
            "soft_liquidity_low_volatility_low_correlation_v1"
            | "soft-liquidity-low-volatility-low-correlation-v1" => {
                Ok(Self::SoftLiquidityLowVolatilityLowCorrelationV1)
            }
            other => Err(format!("unsupported candidate_risk_filter: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<CandidateRiskFilterParams> {
        match self {
            Self::Off => None,
            Self::LowVolatilityV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.70,
                max_average_abs_correlation: None,
                correlation_reference_limit: 0,
                min_liquidity_quantile: None,
            }),
            Self::LowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.70,
                max_average_abs_correlation: Some(0.55),
                correlation_reference_limit: 120,
                min_liquidity_quantile: None,
            }),
            Self::SoftLowVolatilityV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: None,
                correlation_reference_limit: 0,
                min_liquidity_quantile: None,
            }),
            Self::SoftLowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: Some(0.70),
                correlation_reference_limit: 120,
                min_liquidity_quantile: None,
            }),
            Self::SoftLiquidityLowVolatilityLowCorrelationV1 => Some(CandidateRiskFilterParams {
                max_volatility_quantile: 0.85,
                max_average_abs_correlation: Some(0.70),
                correlation_reference_limit: 120,
                min_liquidity_quantile: Some(0.50),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CandidateRiskFilterParams {
    pub(crate) max_volatility_quantile: f64,
    pub(crate) max_average_abs_correlation: Option<f64>,
    pub(crate) correlation_reference_limit: usize,
    pub(crate) min_liquidity_quantile: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRankingProfile {
    #[default]
    Off,
    CapacityAwareAlphaLiquidityV1,
    AlphaFirstLowImpactV1,
    RelativeStrengthAlphaLiquidityV1,
    NonlinearRegimeAlphaLiquidityV1,
    NonlinearRegimeAlphaLiquidityV2,
}

impl CandidateRankingProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "capacity_aware_alpha_liquidity_v1"
            | "capacity-aware-alpha-liquidity-v1"
            | "capacity_aware_candidate_ranking_v1"
            | "capacity-aware-candidate-ranking-v1" => Ok(Self::CapacityAwareAlphaLiquidityV1),
            "alpha_first_low_impact_v1"
            | "alpha-first-low-impact-v1"
            | "alpha_first_liquidity_v1"
            | "alpha-first-liquidity-v1" => Ok(Self::AlphaFirstLowImpactV1),
            "relative_strength_alpha_liquidity_v1"
            | "relative-strength-alpha-liquidity-v1"
            | "return_aware_alpha_liquidity_v1"
            | "return-aware-alpha-liquidity-v1"
            | "pit_relative_strength_alpha_liquidity_v1"
            | "pit-relative-strength-alpha-liquidity-v1" => {
                Ok(Self::RelativeStrengthAlphaLiquidityV1)
            }
            "nonlinear_regime_alpha_liquidity_v1"
            | "nonlinear-regime-alpha-liquidity-v1"
            | "train_window_nonlinear_alpha_liquidity_v1"
            | "train-window-nonlinear-alpha-liquidity-v1"
            | "pit_nonlinear_regime_alpha_liquidity_v1"
            | "pit-nonlinear-regime-alpha-liquidity-v1" => {
                Ok(Self::NonlinearRegimeAlphaLiquidityV1)
            }
            "nonlinear_regime_alpha_liquidity_v2"
            | "nonlinear-regime-alpha-liquidity-v2"
            | "pit_nonlinear_regime_alpha_liquidity_v2"
            | "pit-nonlinear-regime-alpha-liquidity-v2" => {
                Ok(Self::NonlinearRegimeAlphaLiquidityV2)
            }
            other => Err(format!("unsupported candidate_ranking: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<CandidateRankingParams> {
        match self {
            Self::Off => None,
            Self::CapacityAwareAlphaLiquidityV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.30,
                liquidity_rank_weight: 0.70,
                relative_strength_rank_weight: 0.0,
                volatility_rank_weight: 0.0,
                use_regime_aware_weights: false,
            }),
            Self::AlphaFirstLowImpactV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.75,
                liquidity_rank_weight: 0.25,
                relative_strength_rank_weight: 0.0,
                volatility_rank_weight: 0.0,
                use_regime_aware_weights: false,
            }),
            Self::RelativeStrengthAlphaLiquidityV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.45,
                liquidity_rank_weight: 0.25,
                relative_strength_rank_weight: 0.30,
                volatility_rank_weight: 0.0,
                use_regime_aware_weights: false,
            }),
            Self::NonlinearRegimeAlphaLiquidityV1 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.58,
                liquidity_rank_weight: 0.22,
                relative_strength_rank_weight: 0.20,
                volatility_rank_weight: 0.0,
                use_regime_aware_weights: false,
            }),
            Self::NonlinearRegimeAlphaLiquidityV2 => Some(CandidateRankingParams {
                alpha_rank_weight: 0.38,
                liquidity_rank_weight: 0.22,
                relative_strength_rank_weight: 0.15,
                volatility_rank_weight: 0.25,
                use_regime_aware_weights: true,
            }),
        }
    }

    pub(crate) fn uses_capacity(self) -> bool {
        self.params().is_some()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CandidateRankingParams {
    pub(crate) alpha_rank_weight: f64,
    pub(crate) liquidity_rank_weight: f64,
    pub(crate) relative_strength_rank_weight: f64,
    pub(crate) volatility_rank_weight: f64,
    pub(crate) use_regime_aware_weights: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskContributionControlProfile {
    #[default]
    Off,
    SoftSingleName20PctV1,
    SoftSingleName15PctV1,
}

impl RiskContributionControlProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "soft_single_name_20pct_v1" | "soft-single-name-20pct-v1" => {
                Ok(Self::SoftSingleName20PctV1)
            }
            "soft_single_name_15pct_v1" | "soft-single-name-15pct-v1" => {
                Ok(Self::SoftSingleName15PctV1)
            }
            other => Err(format!("unsupported risk_contribution_control: {}", other)),
        }
    }

    pub(crate) fn params(self) -> Option<RiskContributionControlParams> {
        match self {
            Self::Off => None,
            Self::SoftSingleName20PctV1 => Some(RiskContributionControlParams {
                max_single_name_contribution_pct: 0.20,
                iterations: 6,
            }),
            Self::SoftSingleName15PctV1 => Some(RiskContributionControlParams {
                max_single_name_contribution_pct: 0.15,
                iterations: 6,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RiskContributionControlParams {
    pub(crate) max_single_name_contribution_pct: f64,
    pub(crate) iterations: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StressFillConfidenceExposureProfile {
    #[default]
    Off,
    PredictionConfidenceV1,
    PredictionConfidenceAscendingV1,
    PredictionConfidenceCapacityHeadroomV1,
    PredictionConfidenceAscendingCapacityHeadroomV1,
}

impl StressFillConfidenceExposureProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "prediction_confidence_v1"
            | "prediction-confidence-v1"
            | "ml_prediction_confidence_v1"
            | "ml-prediction-confidence-v1"
            | "stress_fill_prediction_confidence_v1"
            | "stress-fill-prediction-confidence-v1" => Ok(Self::PredictionConfidenceV1),
            "prediction_confidence_ascending_v1"
            | "prediction-confidence-ascending-v1"
            | "ml_prediction_confidence_ascending_v1"
            | "ml-prediction-confidence-ascending-v1"
            | "stress_fill_prediction_confidence_ascending_v1"
            | "stress-fill-prediction-confidence-ascending-v1" => {
                Ok(Self::PredictionConfidenceAscendingV1)
            }
            "prediction_confidence_capacity_headroom_v1"
            | "prediction-confidence-capacity-headroom-v1"
            | "ml_prediction_confidence_capacity_headroom_v1"
            | "ml-prediction-confidence-capacity-headroom-v1"
            | "stress_fill_prediction_confidence_capacity_headroom_v1"
            | "stress-fill-prediction-confidence-capacity-headroom-v1" => {
                Ok(Self::PredictionConfidenceCapacityHeadroomV1)
            }
            "prediction_confidence_ascending_capacity_headroom_v1"
            | "prediction-confidence-ascending-capacity-headroom-v1"
            | "ml_prediction_confidence_ascending_capacity_headroom_v1"
            | "ml-prediction-confidence-ascending-capacity-headroom-v1"
            | "stress_fill_prediction_confidence_ascending_capacity_headroom_v1"
            | "stress-fill-prediction-confidence-ascending-capacity-headroom-v1" => {
                Ok(Self::PredictionConfidenceAscendingCapacityHeadroomV1)
            }
            other => Err(format!(
                "unsupported stress_fill_confidence_exposure: {}",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradableUniverseProfile {
    All,
    ListedNonSt,
    MainBoardNonSt,
    MainChinextNonSt,
}

impl TradableUniverseProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "all" | "full" => Ok(Self::All),
            "listed_non_st" | "listed-non-st" => Ok(Self::ListedNonSt),
            "main_board_non_st" | "main-board-non-st" => Ok(Self::MainBoardNonSt),
            "main_chinext_non_st" | "main-chinext-non-st" => Ok(Self::MainChinextNonSt),
            other => Err(format!("unsupported universe_profile: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Bull,
    Bear,
    HighVolatility,
    Sideways,
    Mixed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegimeSignalRule {
    pub combo_name: Option<String>,
    pub version: Option<String>,
    pub top_n: Option<usize>,
    pub rebalance_freq_days: Option<usize>,
    pub max_gross_exposure: Option<f64>,
    pub score_direction: Option<ScoreDirection>,
    pub skip_top_pct: Option<f64>,
    pub max_pairwise_correlation: Option<f64>,
    pub max_position_pct: Option<Decimal>,
    pub score_overlay: Option<FactorScoreOverlayConfig>,
    pub portfolio_sleeve: Option<FactorPortfolioSleeveConfig>,
}

impl RegimeSignalRule {
    fn apply_to(&self, base: &SignalConfig) -> SignalConfig {
        let mut config = base.clone();
        if let Some(combo_name) = self.combo_name.as_ref() {
            config.combo_name = combo_name.clone();
        }
        if let Some(version) = self.version.as_ref() {
            config.version = version.clone();
        }
        if let Some(top_n) = self.top_n {
            config.top_n = top_n.max(1);
        }
        if let Some(rebalance_freq_days) = self.rebalance_freq_days {
            config.rebalance_freq_days = rebalance_freq_days.max(1);
        }
        if let Some(max_gross_exposure) = self.max_gross_exposure {
            config.max_gross_exposure = config
                .max_gross_exposure
                .clamp(0.0, 1.0)
                .min(max_gross_exposure.clamp(0.0, 1.0));
        }
        if let Some(score_direction) = self.score_direction {
            config.score_direction = score_direction;
        }
        if let Some(skip_top_pct) = self.skip_top_pct {
            config.skip_top_pct = skip_top_pct.clamp(0.0, 0.95);
        }
        if let Some(max_pairwise_correlation) = self.max_pairwise_correlation {
            let rule_correlation = max_pairwise_correlation.clamp(0.0, 1.0);
            config.max_pairwise_correlation = Some(
                config
                    .max_pairwise_correlation
                    .map(|base| base.clamp(0.0, 1.0).min(rule_correlation))
                    .unwrap_or(rule_correlation),
            );
        }
        if let Some(max_position_pct) = self.max_position_pct {
            config.max_position_pct = config
                .max_position_pct
                .clamp(Decimal::ZERO, Decimal::ONE)
                .min(max_position_pct.clamp(Decimal::ZERO, Decimal::ONE));
        }
        if let Some(score_overlay) = self.score_overlay.as_ref() {
            config.score_overlay = Some(score_overlay.clone());
        }
        if let Some(portfolio_sleeve) = self.portfolio_sleeve.as_ref() {
            config.portfolio_sleeve = Some(portfolio_sleeve.clone());
        }
        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegimePolicy {
    pub benchmark: String,
    pub lookback_days: usize,
    pub min_observations: usize,
    pub high_volatility_threshold: f64,
    pub bear_return_threshold: f64,
    pub bear_drawdown_threshold: f64,
    pub bull_return_threshold: f64,
    pub bull_max_drawdown: f64,
    pub sideways_volatility_threshold: f64,
    pub sideways_abs_return_threshold: f64,
    pub rules: HashMap<MarketRegime, RegimeSignalRule>,
}

type StateAlphaSleeveSpec = (&'static str, f64, ScoreDirection);

struct StateAlphaSelectorSpec {
    bull_sleeve: StateAlphaSleeveSpec,
    bear_sleeve: StateAlphaSleeveSpec,
    high_volatility_sleeve: StateAlphaSleeveSpec,
    sideways_sleeve: StateAlphaSleeveSpec,
    mixed_sleeve: StateAlphaSleeveSpec,
    bear_exposure: f64,
    high_volatility_exposure: f64,
    bear_max_position_pct: Decimal,
    high_volatility_max_position_pct: Decimal,
}

#[allow(clippy::too_many_arguments)]
fn apply_state_alpha_rule(
    policy: &mut MarketRegimePolicy,
    regime: MarketRegime,
    top_n: Option<usize>,
    rebalance_freq_days: Option<usize>,
    max_gross_exposure: Option<f64>,
    max_pairwise_correlation: Option<f64>,
    max_position_pct: Option<Decimal>,
    sleeve: StateAlphaSleeveSpec,
) {
    let Some(rule) = policy.rules.get_mut(&regime) else {
        return;
    };
    if let Some(top_n) = top_n {
        rule.top_n = Some(top_n);
    }
    if let Some(rebalance_freq_days) = rebalance_freq_days {
        rule.rebalance_freq_days = Some(rebalance_freq_days);
    }
    if let Some(max_gross_exposure) = max_gross_exposure {
        rule.max_gross_exposure = Some(max_gross_exposure);
    }
    if let Some(max_pairwise_correlation) = max_pairwise_correlation {
        rule.max_pairwise_correlation = Some(max_pairwise_correlation);
    }
    if let Some(max_position_pct) = max_position_pct {
        rule.max_position_pct = Some(max_position_pct);
    }
    rule.portfolio_sleeve = Some(FactorPortfolioSleeveConfig {
        combo_name: sleeve.0.to_string(),
        version: "1.0.0".to_string(),
        weight: sleeve.1.clamp(0.0, 1.0),
        score_direction: sleeve.2,
    });
}

impl MarketRegimePolicy {
    pub fn professional_default(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.50),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.05),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.35),
                score_direction: Some(ScoreDirection::Ascending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 5,
            high_volatility_threshold: 0.30,
            bear_return_threshold: -0.02,
            bear_drawdown_threshold: 0.20,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.15,
            sideways_volatility_threshold: 0.12,
            sideways_abs_return_threshold: 0.05,
            rules,
        }
    }

    pub fn drawdown_control_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(20),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.35),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.10),
                max_position_pct: Some(Decimal::new(5, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(20),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.25),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(5, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.24,
            bear_return_threshold: -0.015,
            bear_drawdown_threshold: 0.12,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    pub fn drawdown_control_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(15),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.25),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.15),
                max_position_pct: Some(Decimal::new(4, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(15),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.18),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.10),
                max_position_pct: Some(Decimal::new(4, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(40),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.55),
                max_position_pct: Some(Decimal::new(6, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(40),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.55),
                max_position_pct: Some(Decimal::new(6, 2)),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.20,
            bear_return_threshold: -0.01,
            bear_drawdown_threshold: 0.08,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.10,
            sideways_volatility_threshold: 0.09,
            sideways_abs_return_threshold: 0.035,
            rules,
        }
    }

    /// Risk-off overlay for quality/value style alpha where the score direction
    /// itself is the edge. Unlike the generic policies, this never flips
    /// `score_direction`; it only scales exposure and position caps in weak
    /// markets.
    pub fn quality_risk_off_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.80),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.65),
                max_position_pct: Some(Decimal::new(12, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.24,
            bear_return_threshold: -0.015,
            bear_drawdown_threshold: 0.12,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Tail-risk guard for quality/value style alpha. It keeps the stock
    /// selection shape intact and only reduces exposure in severe market
    /// stress, so high-return quality candidates are not diluted in normal
    /// sideways or mild risk-off regimes.
    pub fn quality_crash_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.85),
                max_position_pct: Some(Decimal::new(12, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.75),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.50,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.25,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Stronger tail-risk guard for Phase 7-R bridge candidates. It keeps the
    /// quality alpha shape intact, but cuts exposure harder once benchmark
    /// drawdown or volatility confirms a deeper stress regime.
    pub fn quality_crash_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.75),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.60),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.45,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.22,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Mid-strength tail guard for bridge candidates that lost too much return
    /// under v2. It preserves v1's late trigger thresholds while using slightly
    /// stronger exposure caps once the tail regime is already confirmed.
    pub fn quality_crash_guard_v3(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.80),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.68),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 63,
            min_observations: 10,
            high_volatility_threshold: 0.50,
            bear_return_threshold: -0.08,
            bear_drawdown_threshold: 0.25,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Earlier bear-window guard for Phase 7-X attribution findings. It targets
    /// long weak windows by triggering earlier than crash guards, while keeping
    /// the quality alpha direction, holding count, and rebalance cadence intact.
    pub fn quality_bear_window_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.78),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.66),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.32,
            bear_return_threshold: -0.04,
            bear_drawdown_threshold: 0.16,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Regime-conditioned alpha router for Phase 7-AN. Normal regimes keep the
    /// high-return financial-quality anchor, while weak/high-volatility regimes
    /// switch to industry-residual quality as a defensive second alpha source.
    pub fn quality_regime_alpha_switch_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_industry_residual_quality_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_value_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_valuation_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_recovery_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_switch_blend_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_switch(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_value_05pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_valuation_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_value_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_valuation_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_overlay_blend_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_regime_alpha_overlay(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_value_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_valuation_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_value_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_valuation_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_blend_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_quality_value_recovery_confirm_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.075,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.125,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_all_regime_event_window_sleeve_05pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.05)
    }

    pub fn quality_all_regime_event_window_sleeve_10pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.10)
    }

    pub fn quality_all_regime_event_window_sleeve_15pct_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_sleeve_all_regimes(benchmark, 0.15)
    }

    fn quality_event_window_sleeve_all_regimes(
        benchmark: impl Into<String>,
        sleeve_weight: f64,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            sleeve_weight,
            ScoreDirection::Descending,
            [
                MarketRegime::Bull,
                MarketRegime::Bear,
                MarketRegime::HighVolatility,
                MarketRegime::Sideways,
                MarketRegime::Mixed,
            ],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
            [MarketRegime::Bear],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            "phase7_event_window_earnings_v1",
            0.15,
            ScoreDirection::Descending,
            [MarketRegime::HighVolatility],
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_10d_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_window_earnings_40d_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.05,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.10,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_surprise_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_event_earnings_v1",
            0.15,
            ScoreDirection::Descending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_price_volume_expanded_v1",
            0.10,
            ScoreDirection::Ascending,
        )
    }

    pub fn quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1(
        benchmark: impl Into<String>,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve(
            benchmark,
            "phase7_price_volume_expanded_v1",
            0.15,
            ScoreDirection::Ascending,
        )
    }

    fn quality_regime_alpha_switch(
        benchmark: impl Into<String>,
        stress_combo_name: &str,
        stress_score_direction: ScoreDirection,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v1(benchmark);
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.combo_name = Some(stress_combo_name.to_string());
                rule.version = Some("1.0.0".to_string());
                rule.score_direction = Some(stress_score_direction);
            }
        }
        policy
    }

    /// Regime-conditioned alpha overlay for Phase 7-AP. This preserves the
    /// quality anchor and only adds a small secondary score in bear or
    /// high-volatility regimes, avoiding the return dilution seen in hard
    /// alpha-source replacement.
    fn quality_regime_alpha_overlay(
        benchmark: impl Into<String>,
        overlay_combo_name: &str,
        overlay_weight: f64,
        overlay_score_direction: ScoreDirection,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        let overlay = FactorScoreOverlayConfig {
            combo_name: overlay_combo_name.to_string(),
            version: "1.0.0".to_string(),
            weight: overlay_weight.clamp(0.0, 1.0),
            score_direction: overlay_score_direction,
        };
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.score_overlay = Some(overlay.clone());
            }
        }
        policy
    }

    /// Regime-conditioned portfolio sleeve for Phase 7-AQ. It builds a small
    /// independent stress sleeve after portfolio construction and blends target
    /// weights, instead of perturbing the main quality ranking.
    fn quality_regime_alpha_portfolio_sleeve(
        benchmark: impl Into<String>,
        sleeve_combo_name: &str,
        sleeve_weight: f64,
        sleeve_score_direction: ScoreDirection,
    ) -> Self {
        Self::quality_regime_alpha_portfolio_sleeve_for_regimes(
            benchmark,
            sleeve_combo_name,
            sleeve_weight,
            sleeve_score_direction,
            [MarketRegime::Bear, MarketRegime::HighVolatility],
        )
    }

    fn quality_regime_alpha_portfolio_sleeve_for_regimes(
        benchmark: impl Into<String>,
        sleeve_combo_name: &str,
        sleeve_weight: f64,
        sleeve_score_direction: ScoreDirection,
        regimes: impl IntoIterator<Item = MarketRegime>,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        let sleeve = FactorPortfolioSleeveConfig {
            combo_name: sleeve_combo_name.to_string(),
            version: "1.0.0".to_string(),
            weight: sleeve_weight.clamp(0.0, 1.0),
            score_direction: sleeve_score_direction,
        };
        for regime in regimes {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.portfolio_sleeve = Some(sleeve.clone());
            }
        }
        policy
    }

    /// Stronger early bear-window guard. This is still quality-shape preserving,
    /// but cuts tail regimes harder when U2 needs more Sharpe stabilization.
    pub fn quality_bear_window_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                max_gross_exposure: Some(0.72),
                max_position_pct: Some(Decimal::new(10, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                max_gross_exposure: Some(0.58),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Position-aware bear-window guard for the U2 quality anchor. It keeps the
    /// quality score direction intact, but diversifies and slows the book when
    /// the benchmark enters a weak or high-volatility regime.
    pub fn quality_bear_position_guard_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.74),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.58),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(75, 3)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Stronger position-aware guard for local Sharpe searches. Use as a
    /// stress-neighborhood candidate, not as a broad default.
    pub fn quality_bear_position_guard_v2(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.68),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(8, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(30),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.52),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(65, 3)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    /// Mild position-aware guard for the current Phase 7 anchor. It uses the
    /// same generic bear/high-volatility triggers as v1/v2, but cuts less
    /// aggressively so the 15%+ return target has a better chance to survive.
    pub fn quality_bear_position_guard_v3(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(80),
                max_gross_exposure: Some(0.82),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(11, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(40),
                max_gross_exposure: Some(0.66),
                skip_top_pct: Some(0.05),
                max_position_pct: Some(Decimal::new(9, 2)),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                max_gross_exposure: Some(1.0),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 126,
            min_observations: 20,
            high_volatility_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
            bull_return_threshold: 0.10,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }

    pub fn quality_event_window_position_guard_v1(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v1(benchmark))
    }

    pub fn quality_event_window_position_guard_v2(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v2(benchmark))
    }

    pub fn quality_event_window_position_guard_v3(benchmark: impl Into<String>) -> Self {
        Self::with_event_window_position_sleeve(Self::quality_bear_position_guard_v3(benchmark))
    }

    fn with_event_window_position_sleeve(mut policy: Self) -> Self {
        let sleeve = FactorPortfolioSleeveConfig {
            combo_name: "phase7_event_window_earnings_v1".to_string(),
            version: "1.0.0".to_string(),
            weight: 0.15,
            score_direction: ScoreDirection::Descending,
        };
        for regime in [MarketRegime::Bear, MarketRegime::HighVolatility] {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.portfolio_sleeve = Some(sleeve.clone());
            }
        }
        policy
    }

    pub fn quality_event_window_return_sharpe_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.72,
            0.58,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.78,
            0.64,
            Decimal::new(11, 2),
            Decimal::new(9, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.75,
            0.62,
            Decimal::new(10, 2),
            Decimal::new(9, 2),
        )
    }

    pub fn quality_event_window_return_sharpe_router_v4(benchmark: impl Into<String>) -> Self {
        Self::quality_event_window_return_sharpe_router(
            benchmark,
            0.68,
            0.54,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
        )
    }

    pub fn quality_state_alpha_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                sideways_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                mixed_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                bear_exposure: 0.68,
                high_volatility_exposure: 0.54,
                bear_max_position_pct: Decimal::new(9, 2),
                high_volatility_max_position_pct: Decimal::new(7, 2),
            },
        )
    }

    pub fn quality_state_alpha_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.125,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.125,
                    ScoreDirection::Descending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.75,
                high_volatility_exposure: 0.62,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(9, 2),
            },
        )
    }

    pub fn quality_state_alpha_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    /// B1 horizon 自适应:Bull 用 h1 combo(full_pit_icir_37f,牛市强 Sharpe 1.2),
    /// Bear/HighVol/Sideways/Mixed 用 h20 combo(full_pit_icir_37f_h20,震荡市强 MaxDD 19%)。
    /// 基于 Task19 研究:horizon=1 在牛市 IC 强,horizon=20 在震荡市 IC 强。
    /// 切换阈值由 quality_bear_window_guard_v2 基底的 regime 检测决定(trailing-12m),非全周期调参。
    pub fn quality_state_alpha_h1h20_selector(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "full_pit_icir_37f",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "full_pit_icir_37f_h20",
                    0.15,
                    ScoreDirection::Ascending,
                ),
                high_volatility_sleeve: (
                    "full_pit_icir_37f_h20",
                    0.125,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: (
                    "full_pit_icir_37f_h20",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                mixed_sleeve: (
                    "full_pit_icir_37f_h20",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.10,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                mixed_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                bear_exposure: 0.68,
                high_volatility_exposure: 0.54,
                bear_max_position_pct: Decimal::new(9, 2),
                high_volatility_max_position_pct: Decimal::new(7, 2),
            },
        )
    }

    pub fn quality_mixed_event_state_overlay_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
            ],
        )
    }

    pub fn quality_mixed_event_state_overlay_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_mixed_event_state_selector_v2(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_residual_confirm_10pct_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    bear_exposure: 0.72,
                    high_volatility_exposure: 0.58,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [(
                MarketRegime::Mixed,
                "phase7_valuation_v1",
                0.03,
                ScoreDirection::Descending,
            )],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    mixed_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.10,
                        ScoreDirection::Descending,
                    ),
                    bear_exposure: 0.72,
                    high_volatility_exposure: 0.58,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [(
                MarketRegime::Mixed,
                "phase7_quality_residual_confirm_10pct_v1",
                0.03,
                ScoreDirection::Ascending,
            )],
        )
    }

    pub fn quality_mixed_orthogonal_alpha_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector(
            benchmark,
            StateAlphaSelectorSpec {
                bull_sleeve: (
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                bear_sleeve: (
                    "phase7_event_window_earnings_v1",
                    0.15,
                    ScoreDirection::Descending,
                ),
                high_volatility_sleeve: (
                    "phase7_price_volume_expanded_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                mixed_sleeve: (
                    "phase7_blend_defensive_rel_v1",
                    0.10,
                    ScoreDirection::Ascending,
                ),
                bear_exposure: 0.72,
                high_volatility_exposure: 0.58,
                bear_max_position_pct: Decimal::new(10, 2),
                high_volatility_max_position_pct: Decimal::new(8, 2),
            },
        )
    }

    pub fn quality_nonlinear_alpha_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.15,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_residual_confirm_10pct_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    bear_exposure: 0.74,
                    high_volatility_exposure: 0.60,
                    bear_max_position_pct: Decimal::new(10, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.04,
                    ScoreDirection::Descending,
                ),
            ],
        )
        .with_nonlinear_mixed_risk(0.98, Decimal::new(14, 2), 0.72)
    }

    pub fn quality_nonlinear_alpha_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector(
                benchmark,
                StateAlphaSelectorSpec {
                    bull_sleeve: (
                        "phase7_quality_value_recovery_confirm_v1",
                        0.05,
                        ScoreDirection::Descending,
                    ),
                    bear_sleeve: (
                        "phase7_event_window_earnings_v1",
                        0.125,
                        ScoreDirection::Descending,
                    ),
                    high_volatility_sleeve: (
                        "phase7_price_volume_expanded_v1",
                        0.10,
                        ScoreDirection::Ascending,
                    ),
                    sideways_sleeve: ("phase7_valuation_v1", 0.10, ScoreDirection::Descending),
                    mixed_sleeve: (
                        "phase7_quality_value_recovery_event_confirm_v1",
                        0.075,
                        ScoreDirection::Descending,
                    ),
                    bear_exposure: 0.78,
                    high_volatility_exposure: 0.62,
                    bear_max_position_pct: Decimal::new(11, 2),
                    high_volatility_max_position_pct: Decimal::new(8, 2),
                },
            ),
            [
                (
                    MarketRegime::Mixed,
                    "phase7_quality_residual_confirm_10pct_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_event_window_earnings_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
            ],
        )
        .with_nonlinear_mixed_risk(1.0, Decimal::new(15, 2), 0.75)
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v1(benchmark).with_nonlinear_mixed_risk(
            0.94,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v1(benchmark).with_nonlinear_mixed_risk(
            0.97,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_nonlinear_alpha_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_nonlinear_alpha_router_v2(benchmark).with_nonlinear_mixed_risk(
            0.98,
            Decimal::new(14, 2),
            0.75,
        )
    }

    fn with_nonlinear_mixed_risk(
        mut self,
        mixed_exposure: f64,
        mixed_max_position_pct: Decimal,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = self.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        self
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v2(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_orthogonal_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_orthogonal_alpha_selector_v3(benchmark),
            0.92,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.85,
            Decimal::new(12, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.75,
            Decimal::new(10, 2),
            0.65,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_overlay_selector_v1(benchmark),
            0.82,
            Decimal::new(11, 2),
            0.65,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v4(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v5(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.95,
            Decimal::new(14, 2),
            0.75,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v6(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_overlay_selector_v1(benchmark),
            0.90,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v7(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.92,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v8(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.93,
            Decimal::new(13, 2),
            0.73,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v9(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.94,
            Decimal::new(14, 2),
            0.74,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v10(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.95,
            Decimal::new(13, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v11(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.94,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v12(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.96,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v13(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v14(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(13, 2),
            0.70,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v15(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v16(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            1.00,
            Decimal::new(15, 2),
            0.75,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v17(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(14, 2),
            0.72,
        )
    }

    pub fn quality_mixed_state_risk_memory_router_v18(benchmark: impl Into<String>) -> Self {
        Self::with_mixed_state_risk_memory(
            Self::quality_mixed_event_state_selector_v1(benchmark),
            0.98,
            Decimal::new(15, 2),
            0.75,
        )
    }

    fn with_mixed_state_risk_memory(
        mut policy: Self,
        mixed_exposure: f64,
        mixed_max_position_pct: Decimal,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        policy
    }

    pub fn quality_state_alpha_overlay_selector_v1(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v3(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.05,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_state_alpha_overlay_selector_v2(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v2(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_quality_value_recovery_confirm_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::Mixed,
                    "phase7_valuation_v1",
                    0.05,
                    ScoreDirection::Descending,
                ),
            ],
        )
    }

    pub fn quality_state_alpha_overlay_selector_v3(benchmark: impl Into<String>) -> Self {
        Self::quality_state_alpha_selector_with_overlay(
            Self::quality_state_alpha_selector_v3(benchmark),
            [
                (
                    MarketRegime::Bear,
                    "phase7_valuation_v1",
                    0.03,
                    ScoreDirection::Descending,
                ),
                (
                    MarketRegime::HighVolatility,
                    "phase7_price_volume_expanded_v1",
                    0.03,
                    ScoreDirection::Ascending,
                ),
            ],
        )
    }

    pub fn quality_state_sharpe_bridge_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.70,
            0.56,
            0.96,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_state_sharpe_bridge_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            0.98,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_state_sharpe_bridge_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.66,
            0.52,
            0.92,
            Decimal::new(8, 2),
            Decimal::new(7, 2),
            Decimal::new(12, 2),
            0.62,
            0.62,
            0.68,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v1(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v2(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v3(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.70,
            0.56,
            0.96,
            Decimal::new(9, 2),
            Decimal::new(7, 2),
            Decimal::new(13, 2),
            0.62,
            0.62,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v4(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v5(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.74,
            0.60,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v6(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(14, 2),
            0.65,
            0.65,
            0.70,
        )
    }

    pub fn quality_frontier_regime_bridge_router_v7(benchmark: impl Into<String>) -> Self {
        Self::with_state_sharpe_bridge_risk(
            Self::quality_state_alpha_overlay_selector_v1(benchmark),
            0.76,
            0.62,
            1.00,
            Decimal::new(10, 2),
            Decimal::new(8, 2),
            Decimal::new(13, 2),
            0.65,
            0.65,
            0.72,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_state_sharpe_bridge_risk(
        mut policy: Self,
        bear_exposure: f64,
        high_volatility_exposure: f64,
        mixed_exposure: f64,
        bear_max_position_pct: Decimal,
        high_volatility_max_position_pct: Decimal,
        mixed_max_position_pct: Decimal,
        bear_max_pairwise_correlation: f64,
        high_volatility_max_pairwise_correlation: f64,
        mixed_max_pairwise_correlation: f64,
    ) -> Self {
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Bear) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(bear_exposure);
            rule.max_position_pct = Some(bear_max_position_pct);
            rule.max_pairwise_correlation = Some(bear_max_pairwise_correlation);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::HighVolatility) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(high_volatility_exposure);
            rule.max_position_pct = Some(high_volatility_max_position_pct);
            rule.max_pairwise_correlation = Some(high_volatility_max_pairwise_correlation);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Mixed) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(mixed_exposure);
            rule.max_position_pct = Some(mixed_max_position_pct);
            rule.max_pairwise_correlation = Some(mixed_max_pairwise_correlation);
        }
        policy
    }

    fn quality_state_alpha_selector_with_overlay(
        mut policy: Self,
        overlays: impl IntoIterator<Item = (MarketRegime, &'static str, f64, ScoreDirection)>,
    ) -> Self {
        for (regime, combo_name, weight, score_direction) in overlays {
            if let Some(rule) = policy.rules.get_mut(&regime) {
                rule.score_overlay = Some(FactorScoreOverlayConfig {
                    combo_name: combo_name.to_string(),
                    version: "1.0.0".to_string(),
                    weight: weight.clamp(0.0, 1.0),
                    score_direction,
                });
            }
        }
        policy
    }

    fn quality_state_alpha_selector(
        benchmark: impl Into<String>,
        spec: StateAlphaSelectorSpec,
    ) -> Self {
        let mut policy = Self::quality_bear_window_guard_v2(benchmark);
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Bull,
            None,
            None,
            None,
            None,
            None,
            spec.bull_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Bear,
            Some(20),
            Some(60),
            Some(spec.bear_exposure),
            Some(0.65),
            Some(spec.bear_max_position_pct),
            spec.bear_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::HighVolatility,
            Some(20),
            Some(60),
            Some(spec.high_volatility_exposure),
            Some(0.65),
            Some(spec.high_volatility_max_position_pct),
            spec.high_volatility_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Sideways,
            None,
            None,
            Some(1.0),
            None,
            None,
            spec.sideways_sleeve,
        );
        apply_state_alpha_rule(
            &mut policy,
            MarketRegime::Mixed,
            None,
            None,
            Some(1.0),
            None,
            None,
            spec.mixed_sleeve,
        );
        policy
    }

    fn quality_event_window_return_sharpe_router(
        benchmark: impl Into<String>,
        bear_exposure: f64,
        high_vol_exposure: f64,
        bear_max_position_pct: Decimal,
        high_vol_max_position_pct: Decimal,
    ) -> Self {
        let mut policy =
            Self::quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(benchmark);
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::Bear) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(bear_exposure);
            rule.max_pairwise_correlation = Some(0.65);
            rule.max_position_pct = Some(bear_max_position_pct);
        }
        if let Some(rule) = policy.rules.get_mut(&MarketRegime::HighVolatility) {
            rule.top_n = Some(20);
            rule.rebalance_freq_days = Some(60);
            rule.max_gross_exposure = Some(high_vol_exposure);
            rule.max_pairwise_correlation = Some(0.65);
            rule.max_position_pct = Some(high_vol_max_position_pct);
        }
        policy
    }

    pub fn apply(&self, base: &SignalConfig, regime: MarketRegime) -> SignalConfig {
        self.rules
            .get(&regime)
            .or_else(|| self.rules.get(&MarketRegime::Mixed))
            .map(|rule| rule.apply_to(base))
            .unwrap_or_else(|| base.clone())
    }

    /// North-flow-aware regime policy: north_flow confirms bull → higher exposure;
    /// north_flow confirms bear → defensive sleeve with tighter exposure.
    /// Uses 42-day lookback for faster regime detection (vs 63-day default).
    pub fn north_flow_regime_confirm_v1(benchmark: impl Into<String>) -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            MarketRegime::Bull,
            RegimeSignalRule {
                top_n: Some(80),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(1.0),
                score_direction: Some(ScoreDirection::Descending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Bear,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.45),
                score_direction: Some(ScoreDirection::Ascending),
                skip_top_pct: Some(0.05),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::HighVolatility,
            RegimeSignalRule {
                top_n: Some(25),
                rebalance_freq_days: Some(60),
                max_gross_exposure: Some(0.35),
                score_direction: Some(ScoreDirection::Ascending),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Sideways,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.85),
                ..Default::default()
            },
        );
        rules.insert(
            MarketRegime::Mixed,
            RegimeSignalRule {
                top_n: Some(50),
                rebalance_freq_days: Some(20),
                max_gross_exposure: Some(0.85),
                ..Default::default()
            },
        );

        Self {
            benchmark: benchmark.into(),
            lookback_days: 42,
            min_observations: 10,
            high_volatility_threshold: 0.26,
            bear_return_threshold: -0.02,
            bear_drawdown_threshold: 0.15,
            bull_return_threshold: 0.08,
            bull_max_drawdown: 0.12,
            sideways_volatility_threshold: 0.10,
            sideways_abs_return_threshold: 0.04,
            rules,
        }
    }
}
