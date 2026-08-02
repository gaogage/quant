//! Portfolio construction config and helpers.
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct PortfolioConstructionConfig {
    pub(crate) top_n: usize,
    pub(crate) max_position_pct: Decimal,
    pub(crate) portfolio_notional_cny: Option<f64>,
    pub(crate) max_participation_rate: Option<f64>,
    pub(crate) capacity_risk_budget_profile: CapacityRiskBudgetProfile,
    pub(crate) cash_utilization_profile: CashUtilizationProfile,
    pub(crate) max_pairwise_correlation: Option<f64>,
    pub(crate) correlation_lookback_days: usize,
    pub(crate) kelly_fraction: f64,
    pub(crate) kelly_lookback_days: usize,
    pub(crate) max_gross_exposure: f64,
    pub(crate) portfolio_method: PortfolioConstructionMethod,
    pub(crate) risk_budget_lookback_days: usize,
    pub(crate) capacity_penalty_strength: f64,
    pub(crate) max_industry_weight_pct: Option<f64>,
    pub(crate) style_risk_budget_profile: StyleRiskBudgetProfile,
    pub(crate) candidate_risk_filter_profile: CandidateRiskFilterProfile,
    pub(crate) candidate_ranking_profile: CandidateRankingProfile,
    pub(crate) risk_contribution_control_profile: RiskContributionControlProfile,
    pub(crate) stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile,
}

impl Default for PortfolioConstructionConfig {
    fn default() -> Self {
        Self {
            top_n: 20,
            max_position_pct: Decimal::new(10, 2),
            portfolio_notional_cny: None,
            max_participation_rate: None,
            capacity_risk_budget_profile: CapacityRiskBudgetProfile::Off,
            cash_utilization_profile: CashUtilizationProfile::Off,
            max_pairwise_correlation: None,
            correlation_lookback_days: 60,
            kelly_fraction: 0.0,
            kelly_lookback_days: 60,
            max_gross_exposure: 1.0,
            portfolio_method: PortfolioConstructionMethod::Heuristic,
            risk_budget_lookback_days: 60,
            capacity_penalty_strength: 0.0,
            max_industry_weight_pct: None,
            style_risk_budget_profile: StyleRiskBudgetProfile::Off,
            candidate_risk_filter_profile: CandidateRiskFilterProfile::Off,
            candidate_ranking_profile: CandidateRankingProfile::Off,
            risk_contribution_control_profile: RiskContributionControlProfile::Off,
            stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile::Off,
        }
    }
}

impl From<&SignalConfig> for PortfolioConstructionConfig {
    fn from(config: &SignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
            portfolio_notional_cny: config.portfolio_notional_cny,
            max_participation_rate: config.max_participation_rate,
            capacity_risk_budget_profile: config.capacity_risk_budget_profile,
            cash_utilization_profile: config.cash_utilization_profile,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
            max_industry_weight_pct: config.industry_max_weight_pct,
            style_risk_budget_profile: config.style_risk_budget_profile,
            candidate_risk_filter_profile: config.candidate_risk_filter_profile,
            candidate_ranking_profile: config.candidate_ranking_profile,
            risk_contribution_control_profile: config.risk_contribution_control_profile,
            stress_fill_confidence_exposure_profile: config.stress_fill_confidence_exposure_profile,
        }
    }
}

impl From<&PredictionSignalConfig> for PortfolioConstructionConfig {
    fn from(config: &PredictionSignalConfig) -> Self {
        Self {
            top_n: capped_portfolio_top_n(config.top_n, config.portfolio_method),
            max_position_pct: config.max_position_pct,
            portfolio_notional_cny: config.portfolio_notional_cny,
            max_participation_rate: config.max_participation_rate,
            capacity_risk_budget_profile: config.capacity_risk_budget_profile,
            cash_utilization_profile: config.cash_utilization_profile,
            max_pairwise_correlation: config.max_pairwise_correlation,
            correlation_lookback_days: config.correlation_lookback_days,
            kelly_fraction: config.kelly_fraction,
            kelly_lookback_days: config.kelly_lookback_days,
            max_gross_exposure: config.max_gross_exposure,
            portfolio_method: config.portfolio_method,
            risk_budget_lookback_days: config.risk_budget_lookback_days,
            capacity_penalty_strength: config.capacity_penalty_strength,
            max_industry_weight_pct: config.industry_max_weight_pct,
            style_risk_budget_profile: config.style_risk_budget_profile,
            candidate_risk_filter_profile: config.candidate_risk_filter_profile,
            candidate_ranking_profile: config.candidate_ranking_profile,
            risk_contribution_control_profile: config.risk_contribution_control_profile,
            stress_fill_confidence_exposure_profile: config.stress_fill_confidence_exposure_profile,
        }
    }
}

impl PortfolioConstructionConfig {
    pub(crate) fn uses_capacity_inputs(&self) -> bool {
        matches!(
            self.portfolio_method,
            PortfolioConstructionMethod::RiskBudget
                | PortfolioConstructionMethod::StressFillAwareRiskBudget
        ) || self.style_risk_budget_profile.uses_liquidity()
            || self.candidate_ranking_profile.uses_capacity()
            || self.capacity_risk_budget_profile.uses_capacity()
            || (self.max_participation_rate.is_some() && self.portfolio_notional_cny.is_some())
    }
}

fn capped_portfolio_top_n(top_n: usize, method: PortfolioConstructionMethod) -> usize {
    match method {
        PortfolioConstructionMethod::RiskBudget
        | PortfolioConstructionMethod::StressFillAwareRiskBudget
        | PortfolioConstructionMethod::MinVariance
        | PortfolioConstructionMethod::RiskParity
        | PortfolioConstructionMethod::MaxDiversification => top_n.min(50),
        PortfolioConstructionMethod::Heuristic => top_n,
    }
}
