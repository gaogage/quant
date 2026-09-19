//! phase7 因子回填 specs：各因子族的 Phase7BackfillFactorSpec 声明（纯数据，无外部依赖）。
use super::*;

pub(crate) fn phase7_price_volume_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "rev_5d_std",
            name: "Phase 7 5d reversal rank",
            period: 5,
            kind: Phase7BackfillFactorKind::Reversal,
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "rev_20d_std",
            name: "Phase 7 20d reversal rank",
            period: 20,
            kind: Phase7BackfillFactorKind::Reversal,
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "downvol_20d_std",
            name: "Phase 7 20d downside volatility rank",
            period: 20,
            kind: Phase7BackfillFactorKind::DownsideVolatility,
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "amihud_20d_std",
            name: "Phase 7 20d Amihud illiquidity rank",
            period: 20,
            kind: Phase7BackfillFactorKind::AmihudIlliquidity,
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "amt_intensity_20d_std",
            name: "Phase 7 20d amount intensity rank",
            period: 20,
            kind: Phase7BackfillFactorKind::AmountIntensity,
            weight: 0.2,
        },
    ]
}

pub(crate) fn p42b_large_cap_momentum_reversal_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![Phase7BackfillFactorSpec {
        factor_code: "large_cap_mom_rev_daily_std",
        name: "P4.2b large-cap momentum-reversal interaction rank",
        period: 0,
        kind: Phase7BackfillFactorKind::LargeCapMomentumReversal {
            reversal_period: 5,
            momentum_period: 60,
            large_cap_threshold_yi: 500,
        },
        weight: 1.0,
    }]
}

/// P4.2b 防御板块列表(申万行业名,据 P4.1c 发现的 ascending 失效/近零 IC 板块)。
const P42B_DEFENSIVE_INDUSTRIES: &[&str] =
    &["保险", "银行", "白酒", "黄金", "机场", "电信运营", "啤酒"];

pub(crate) fn p42b_defensive_low_vol_quality_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![Phase7BackfillFactorSpec {
        factor_code: "defensive_lowvol_quality_daily_std",
        name: "P4.2b defensive low-volatility quality interaction rank",
        period: 20,
        kind: Phase7BackfillFactorKind::DefensiveLowVolQuality {
            volatility_period: 20,
            industries: P42B_DEFENSIVE_INDUSTRIES,
        },
        weight: 1.0,
    }]
}

pub(crate) fn phase7_financial_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_daily_std",
            name: "Phase 7 daily PIT ROE quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "roe",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roa_daily_std",
            name: "Phase 7 daily PIT ROA quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "roa",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_daily_std",
            name: "Phase 7 daily PIT gross margin rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "gross_margin",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_daily_std",
            name: "Phase 7 daily PIT net profit margin rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "netprofit_margin",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_current_ratio_daily_std",
            name: "Phase 7 daily PIT current ratio rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "current_ratio",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_debt_to_assets_daily_std",
            name: "Phase 7 daily PIT low leverage rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "debt_to_assets",
                higher_is_better: false,
            },
            weight: 1.0 / 6.0,
        },
    ]
}

pub(crate) fn phase7_financial_quality_change_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_yoy_accel_std",
            name: "Phase 7 PIT ROE YoY acceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "roe",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roa_yoy_accel_std",
            name: "Phase 7 PIT ROA YoY acceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "roa",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_yoy_accel_std",
            name: "Phase 7 PIT gross margin YoY acceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "gross_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_yoy_accel_std",
            name: "Phase 7 PIT net margin YoY acceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "netprofit_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_current_ratio_yoy_accel_std",
            name: "Phase 7 PIT current ratio YoY acceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "current_ratio",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_debt_to_assets_yoy_decel_std",
            name: "Phase 7 PIT debt-to-assets YoY deceleration rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualAcceleration {
                source_column: "debt_to_assets",
                mode: FinancialAnnualChangeMode::Decrease,
            },
            weight: 1.0 / 6.0,
        },
    ]
}

pub(crate) fn phase7_earnings_recovery_persistence_backfill_specs() -> Vec<Phase7BackfillFactorSpec>
{
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_eps_yoy_recovery_persist_std",
            name: "Phase 7 PIT EPS YoY recovery persistence rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualPersistence {
                source_column: "eps",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_yoy_recovery_persist_std",
            name: "Phase 7 PIT ROE YoY recovery persistence rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualPersistence {
                source_column: "roe",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roa_yoy_recovery_persist_std",
            name: "Phase 7 PIT ROA YoY recovery persistence rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualPersistence {
                source_column: "roa",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_yoy_recovery_persist_std",
            name: "Phase 7 PIT gross margin YoY recovery persistence rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualPersistence {
                source_column: "gross_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_yoy_recovery_persist_std",
            name: "Phase 7 PIT net margin YoY recovery persistence rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualPersistence {
                source_column: "netprofit_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 0.2,
        },
    ]
}

pub(crate) fn phase7_industry_residual_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual ROE quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "roe",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roa_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual ROA quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "roa",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual gross margin rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "gross_margin",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual net profit margin rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "netprofit_margin",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_current_ratio_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual current ratio rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "current_ratio",
                higher_is_better: true,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_debt_to_assets_indrel_daily_std",
            name: "Phase 7 daily PIT industry-residual low leverage rank",
            period: 0,
            kind: Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
                source_column: "debt_to_assets",
                higher_is_better: false,
            },
            weight: 1.0 / 6.0,
        },
    ]
}

pub(crate) fn phase7_relative_strength_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "mkt_rel_mom_20d_std",
            name: "Phase 7 20d market-relative momentum rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MarketRelativeMomentum,
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mkt_rel_mom_60d_std",
            name: "Phase 7 60d market-relative momentum rank",
            period: 60,
            kind: Phase7BackfillFactorKind::MarketRelativeMomentum,
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_rel_mom_20d_std",
            name: "Phase 7 20d industry-relative momentum rank",
            period: 20,
            kind: Phase7BackfillFactorKind::IndustryRelativeMomentum,
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_rel_mom_60d_std",
            name: "Phase 7 60d industry-relative momentum rank",
            period: 60,
            kind: Phase7BackfillFactorKind::IndustryRelativeMomentum,
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_quality_relative_strength_combo_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_daily_std",
            name: "Phase 7 daily PIT ROE quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "roe",
                higher_is_better: true,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roa_daily_std",
            name: "Phase 7 daily PIT ROA quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "roa",
                higher_is_better: true,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_daily_std",
            name: "Phase 7 daily PIT gross margin quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "gross_margin",
                higher_is_better: true,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_daily_std",
            name: "Phase 7 daily PIT net margin quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "netprofit_margin",
                higher_is_better: true,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_current_ratio_daily_std",
            name: "Phase 7 daily PIT current ratio quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "current_ratio",
                higher_is_better: true,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_debt_to_assets_daily_std",
            name: "Phase 7 daily PIT low leverage quality rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialLatest {
                source_column: "debt_to_assets",
                higher_is_better: false,
            },
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mkt_rel_mom_20d_std",
            name: "Phase 7 20d market-relative momentum rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MarketRelativeMomentum,
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mkt_rel_mom_60d_std",
            name: "Phase 7 60d market-relative momentum rank",
            period: 60,
            kind: Phase7BackfillFactorKind::MarketRelativeMomentum,
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_rel_mom_20d_std",
            name: "Phase 7 20d industry-relative momentum rank",
            period: 20,
            kind: Phase7BackfillFactorKind::IndustryRelativeMomentum,
            weight: 0.10,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_rel_mom_60d_std",
            name: "Phase 7 60d industry-relative momentum rank",
            period: 60,
            kind: Phase7BackfillFactorKind::IndustryRelativeMomentum,
            weight: 0.10,
        },
    ]
}

pub(crate) fn phase7_growth_recovery_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fin_eps_yoy_recovery_std",
            name: "Phase 7 PIT EPS YoY recovery rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "eps",
                mode: FinancialAnnualChangeMode::PercentChange,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_roe_yoy_delta_std",
            name: "Phase 7 PIT ROE YoY improvement rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "roe",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_gross_margin_yoy_delta_std",
            name: "Phase 7 PIT gross margin YoY improvement rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "gross_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_netprofit_margin_yoy_delta_std",
            name: "Phase 7 PIT net margin YoY improvement rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "netprofit_margin",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_debt_to_assets_yoy_improve_std",
            name: "Phase 7 PIT leverage reduction YoY rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "debt_to_assets",
                mode: FinancialAnnualChangeMode::Decrease,
            },
            weight: 1.0 / 6.0,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fin_current_ratio_yoy_delta_std",
            name: "Phase 7 PIT current ratio YoY improvement rank",
            period: 0,
            kind: Phase7BackfillFactorKind::FinancialAnnualChange {
                source_column: "current_ratio",
                mode: FinancialAnnualChangeMode::Difference,
            },
            weight: 1.0 / 6.0,
        },
    ]
}

pub(crate) fn phase7_valuation_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "val_pe_ttm_low_std",
            name: "Phase 7 low PE TTM valuation rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DailyBasicLatest {
                source_column: "pe_ttm",
                higher_is_better: false,
                positive_only: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "val_pb_low_std",
            name: "Phase 7 low PB valuation rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DailyBasicLatest {
                source_column: "pb",
                higher_is_better: false,
                positive_only: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "val_ps_ttm_low_std",
            name: "Phase 7 low PS TTM valuation rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DailyBasicLatest {
                source_column: "ps_ttm",
                higher_is_better: false,
                positive_only: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "val_dividend_yield_ttm_std",
            name: "Phase 7 dividend yield TTM rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DailyBasicLatest {
                source_column: "dv_ttm",
                higher_is_better: true,
                positive_only: false,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_moneyflow_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "mf_net_amount_5d_std",
            name: "Phase 7 5d net moneyflow intensity rank",
            period: 5,
            kind: Phase7BackfillFactorKind::MoneyflowRolling {
                amount_expression: "COALESCE(mf.net_mf_amount::double precision, 0.0)",
                higher_is_better: true,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_net_amount_20d_std",
            name: "Phase 7 20d net moneyflow intensity rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowRolling {
                amount_expression: "COALESCE(mf.net_mf_amount::double precision, 0.0)",
                higher_is_better: true,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_elg_net_amount_5d_std",
            name: "Phase 7 5d extra-large order net inflow rank",
            period: 5,
            kind: Phase7BackfillFactorKind::MoneyflowRolling {
                amount_expression: "COALESCE(mf.buy_elg_amount::double precision, 0.0) - COALESCE(mf.sell_elg_amount::double precision, 0.0)",
                higher_is_better: true,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_lg_elg_net_amount_20d_std",
            name: "Phase 7 20d large and extra-large order net inflow rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowRolling {
                amount_expression: "COALESCE(mf.buy_lg_amount::double precision, 0.0) - COALESCE(mf.sell_lg_amount::double precision, 0.0) + COALESCE(mf.buy_elg_amount::double precision, 0.0) - COALESCE(mf.sell_elg_amount::double precision, 0.0)",
                higher_is_better: true,
            },
            weight: 0.2,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_small_sell_pressure_20d_std",
            name: "Phase 7 20d low small-order sell pressure rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowRolling {
                amount_expression: "COALESCE(mf.sell_sm_amount::double precision, 0.0) - COALESCE(mf.buy_sm_amount::double precision, 0.0)",
                higher_is_better: false,
            },
            weight: 0.2,
        },
    ]
}

pub(crate) fn phase7_moneyflow_congestion_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "mf_net_inflow_low_crowding_20d_std",
            name: "Phase 7 20d net inflow adjusted by low crowding rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowCongestionInteraction {
                flow_expression: "COALESCE(mf.net_mf_amount::double precision, 0.0)",
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_elg_inflow_low_crowding_10d_std",
            name: "Phase 7 10d extra-large inflow adjusted by low crowding rank",
            period: 10,
            kind: Phase7BackfillFactorKind::MoneyflowCongestionInteraction {
                flow_expression: "COALESCE(mf.buy_elg_amount::double precision, 0.0) - COALESCE(mf.sell_elg_amount::double precision, 0.0)",
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_lg_elg_inflow_low_crowding_20d_std",
            name: "Phase 7 20d large and extra-large inflow adjusted by low crowding rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowCongestionInteraction {
                flow_expression: "COALESCE(mf.buy_lg_amount::double precision, 0.0) - COALESCE(mf.sell_lg_amount::double precision, 0.0) + COALESCE(mf.buy_elg_amount::double precision, 0.0) - COALESCE(mf.sell_elg_amount::double precision, 0.0)",
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mf_small_order_relief_low_crowding_20d_std",
            name: "Phase 7 20d small-order sell relief adjusted by low crowding rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MoneyflowCongestionInteraction {
                flow_expression: "COALESCE(mf.buy_sm_amount::double precision, 0.0) - COALESCE(mf.sell_sm_amount::double precision, 0.0)",
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_supply_float_shock_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "float_share_growth_20d_inverse_std",
            name: "Phase 7 20d inverse circulating-share supply growth rank",
            period: 20,
            kind: Phase7BackfillFactorKind::SupplyFloatShock {
                share_expression: "basic.float_share::double precision",
                horizon_days: 20,
                mode: SupplyFloatShockMode::GrowthInverse,
            },
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "total_share_growth_60d_inverse_std",
            name: "Phase 7 60d inverse total-share supply growth rank",
            period: 60,
            kind: Phase7BackfillFactorKind::SupplyFloatShock {
                share_expression: "basic.total_share::double precision",
                horizon_days: 60,
                mode: SupplyFloatShockMode::GrowthInverse,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "free_share_churn_120d_inverse_std",
            name: "Phase 7 120d inverse free-float share churn rank",
            period: 120,
            kind: Phase7BackfillFactorKind::SupplyFloatShock {
                share_expression: "basic.free_share::double precision",
                horizon_days: 120,
                mode: SupplyFloatShockMode::ChurnInverse,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_cashflow_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "cf_ocf_to_profit_latest_std",
            name: "Phase 7 PIT operating cashflow to profit rank",
            period: 0,
            kind: Phase7BackfillFactorKind::CashflowLatest {
                value_expression: "n_cashflow_act::double precision / NULLIF(ABS(net_profit::double precision), 0.0)",
                required_filter: "cf.n_cashflow_act IS NOT NULL AND cf.net_profit IS NOT NULL",
                higher_is_better: true,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "cf_ocf_profit_gap_latest_std",
            name: "Phase 7 PIT operating cashflow minus profit rank",
            period: 0,
            kind: Phase7BackfillFactorKind::CashflowLatest {
                value_expression: "(n_cashflow_act::double precision - net_profit::double precision) / NULLIF(ABS(net_profit::double precision), 0.0)",
                required_filter: "cf.n_cashflow_act IS NOT NULL AND cf.net_profit IS NOT NULL",
                higher_is_better: true,
            },
            weight: 0.30,
        },
        Phase7BackfillFactorSpec {
            factor_code: "cf_cash_buffer_latest_std",
            name: "Phase 7 PIT cash buffer to profit rank",
            period: 0,
            kind: Phase7BackfillFactorKind::CashflowLatest {
                value_expression: "c_cash_equ_end_period::double precision / NULLIF(ABS(net_profit::double precision), 0.0)",
                required_filter: "cf.c_cash_equ_end_period IS NOT NULL AND cf.net_profit IS NOT NULL",
                higher_is_better: true,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: "cf_ocf_positive_latest_std",
            name: "Phase 7 PIT positive operating cashflow rank",
            period: 0,
            kind: Phase7BackfillFactorKind::CashflowLatest {
                value_expression: "CASE WHEN n_cashflow_act::double precision > 0.0 THEN 1.0 ELSE 0.0 END",
                required_filter: "cf.n_cashflow_act IS NOT NULL",
                higher_is_better: true,
            },
            weight: 0.15,
        },
    ]
}

pub(crate) fn phase7_dividend_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "div_paid_years_4y_std",
            name: "Phase 7 PIT 4y dividend paid-years rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DividendRollingQuality {
                value_expression: "dividend_years::double precision",
                higher_is_better: true,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "div_cash_sum_4y_std",
            name: "Phase 7 PIT 4y cash-dividend sum rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DividendRollingQuality {
                value_expression: "cash_div_sum",
                higher_is_better: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "div_stability_4y_std",
            name: "Phase 7 PIT 4y dividend stability rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DividendRollingQuality {
                value_expression:
                    "CASE WHEN cash_div_avg > 0.0 THEN cash_div_stdev / cash_div_avg ELSE NULL END",
                higher_is_better: false,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "div_recent_positive_std",
            name: "Phase 7 PIT recent positive dividend rank",
            period: 0,
            kind: Phase7BackfillFactorKind::DividendRollingQuality {
                value_expression: "recent_positive_dividend::double precision",
                higher_is_better: true,
            },
            weight: 0.15,
        },
    ]
}

pub(crate) fn phase7_event_alpha_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "event_forecast_change_mid_std",
            name: "Phase 7 PIT forecast profit-change midpoint rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_forecast",
                value_expression: "(COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0",
                higher_is_better: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_forecast_profit_floor_std",
            name: "Phase 7 PIT forecast net-profit floor rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_forecast",
                value_expression: "event.net_profit_min::double precision",
                higher_is_better: true,
            },
            weight: 0.15,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_express_yoy_dedu_np_std",
            name: "Phase 7 PIT express deducted net-profit YoY rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_express",
                value_expression: "event.yoy_dedu_np::double precision",
                higher_is_better: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_express_roe_std",
            name: "Phase 7 PIT express diluted ROE rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_express",
                value_expression: "event.diluted_roe::double precision",
                higher_is_better: true,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_disclosure_early_days_std",
            name: "Phase 7 PIT early disclosure timing rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_disclosure_date",
                value_expression: "CASE WHEN event.pre_date IS NOT NULL AND event.actual_date IS NOT NULL THEN (event.pre_date - event.actual_date)::double precision ELSE NULL END",
                higher_is_better: true,
            },
            weight: 0.15,
        },
    ]
}

pub(crate) fn phase7_event_window_alpha_backfill_specs_for_plan(
    plan: &Phase7EventWindowAlphaBackfillPlan,
) -> Vec<Phase7BackfillFactorSpec> {
    if is_event_reaction_segments_combo(&plan.combo_name) {
        return phase7_event_reaction_segment_backfill_specs_for_days(event_window_days_for_combo(
            &plan.combo_name,
        ));
    }
    if is_event_reaction_reversal_combo(&plan.combo_name) {
        return phase7_event_reaction_reversal_backfill_specs_for_days(
            event_window_days_for_combo(&plan.combo_name),
        );
    }
    if is_event_post_return_curve_combo(&plan.combo_name) {
        return phase7_event_post_return_curve_backfill_specs_for_days(
            event_window_days_for_combo(&plan.combo_name),
        );
    }
    phase7_event_window_alpha_backfill_specs_for_days(event_window_days_for_combo(&plan.combo_name))
}

fn is_event_post_return_curve_combo(combo_name: &str) -> bool {
    matches!(combo_name, "phase7_event_post_return_curve_20d_v1")
}

fn is_event_reaction_segments_combo(combo_name: &str) -> bool {
    matches!(combo_name, "phase7_event_reaction_segments_20d_v1")
}

fn is_event_reaction_reversal_combo(combo_name: &str) -> bool {
    matches!(combo_name, "phase7_event_reaction_reversal_20d_v1")
}

fn event_window_days_for_combo(combo_name: &str) -> i32 {
    match combo_name {
        "phase7_event_window_earnings_10d_v1" => 10,
        "phase7_event_window_earnings_40d_v1" => 40,
        "phase7_event_post_return_curve_20d_v1" => 20,
        "phase7_event_reaction_segments_20d_v1" => 20,
        "phase7_event_reaction_reversal_20d_v1" => 20,
        _ => 20,
    }
}

pub(crate) fn event_window_bundle_name(combo_name: &str) -> &'static str {
    match combo_name {
        "phase7_event_window_earnings_10d_v1" => "phase7_event_window_earnings_10d_v1",
        "phase7_event_window_earnings_40d_v1" => "phase7_event_window_earnings_40d_v1",
        "phase7_event_post_return_curve_20d_v1" => "phase7_event_post_return_curve_20d_v1",
        "phase7_event_reaction_segments_20d_v1" => "phase7_event_reaction_segments_20d_v1",
        "phase7_event_reaction_reversal_20d_v1" => "phase7_event_reaction_reversal_20d_v1",
        _ => "phase7_event_window_earnings_v1",
    }
}

pub(crate) fn event_window_phase(combo_name: &str) -> &'static str {
    match combo_name {
        "phase7_event_window_earnings_10d_v1" => "7-AZ10",
        "phase7_event_window_earnings_40d_v1" => "7-AZ40",
        "phase7_event_post_return_curve_20d_v1" => "7-FB",
        "phase7_event_reaction_segments_20d_v1" => "7-FC",
        "phase7_event_reaction_reversal_20d_v1" => "7-FC",
        _ => "7-Y2",
    }
}

pub(crate) fn event_window_combo_method(combo_name: &str) -> &'static str {
    if is_event_post_return_curve_combo(combo_name)
        || is_event_reaction_segments_combo(combo_name)
        || is_event_reaction_reversal_combo(combo_name)
    {
        "weighted_event_post_return_curve"
    } else {
        "weighted_event_window_earnings"
    }
}

pub(crate) fn event_window_dependencies(combo_name: &str) -> &'static [&'static str] {
    if is_event_post_return_curve_combo(combo_name)
        || is_event_reaction_segments_combo(combo_name)
        || is_event_reaction_reversal_combo(combo_name)
    {
        &[
            "market_stock_forecast",
            "market_stock_express",
            "market_stock_daily_bar",
            "market_stock",
            "market_trade_calendar",
        ]
    } else {
        &[
            "market_stock_forecast",
            "market_stock_express",
            "market_stock_disclosure_date",
            "market_trade_calendar",
        ]
    }
}

pub(crate) fn phase7_event_window_alpha_backfill_specs_for_days(
    window_days: i32,
) -> Vec<Phase7BackfillFactorSpec> {
    let window_days = window_days.max(1);
    let suffix = match window_days {
        10 => "10d",
        40 => "40d",
        _ => "20d",
    };
    let forecast_change_code =
        Box::leak(format!("event_window_forecast_change_{}_decay_std", suffix).into_boxed_str());
    let forecast_profit_code = Box::leak(
        format!("event_window_forecast_profit_floor_{}_decay_std", suffix).into_boxed_str(),
    );
    let express_roe_code =
        Box::leak(format!("event_window_express_roe_{}_decay_std", suffix).into_boxed_str());
    let disclosure_code = Box::leak(
        format!("event_window_disclosure_early_days_{}_decay_std", suffix).into_boxed_str(),
    );

    vec![
        Phase7BackfillFactorSpec {
            factor_code: forecast_change_code,
            name: "Phase 7 20d decayed forecast profit-change midpoint rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_forecast",
                value_expression: "(COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0",
                higher_is_better: true,
                window_days,
                decay_days: window_days,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: forecast_profit_code,
            name: "Phase 7 20d decayed forecast net-profit floor rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_forecast",
                value_expression: "event.net_profit_min::double precision",
                higher_is_better: true,
                window_days,
                decay_days: window_days,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: express_roe_code,
            name: "Phase 7 20d decayed express diluted ROE rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_express",
                value_expression: "event.diluted_roe::double precision",
                higher_is_better: true,
                window_days,
                decay_days: window_days,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: disclosure_code,
            name: "Phase 7 20d decayed early disclosure timing rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_disclosure_date",
                value_expression: "CASE WHEN event.pre_date IS NOT NULL AND event.actual_date IS NOT NULL THEN (event.pre_date - event.actual_date)::double precision ELSE NULL END",
                higher_is_better: true,
                window_days,
                decay_days: window_days,
            },
            weight: 0.20,
        },
    ]
}

pub(crate) fn phase7_event_post_return_curve_backfill_specs_for_days(
    window_days: i32,
) -> Vec<Phase7BackfillFactorSpec> {
    let window_days = window_days.max(1);
    let suffix = match window_days {
        10 => "10d",
        40 => "40d",
        _ => "20d",
    };
    let forecast_code =
        Box::leak(format!("event_post_return_forecast_{}_indrel_std", suffix).into_boxed_str());
    let express_code =
        Box::leak(format!("event_post_return_express_{}_indrel_std", suffix).into_boxed_str());

    vec![
        Phase7BackfillFactorSpec {
            factor_code: forecast_code,
            name: "Phase 7 PIT forecast post-event industry-relative return curve rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventPostReturnCurve {
                source_table: "market_stock_forecast",
                event_filter_expression: "event.p_change_min IS NOT NULL OR event.p_change_max IS NOT NULL OR event.net_profit_min IS NOT NULL OR event.net_profit_max IS NOT NULL",
                higher_is_better: true,
                window_days,
                industry_relative: true,
                min_event_age_days: 0,
                max_event_age_days: window_days,
            },
            weight: 0.5,
        },
        Phase7BackfillFactorSpec {
            factor_code: express_code,
            name: "Phase 7 PIT express post-event industry-relative return curve rank",
            period: window_days,
            kind: Phase7BackfillFactorKind::EventPostReturnCurve {
                source_table: "market_stock_express",
                event_filter_expression: "event.diluted_roe IS NOT NULL OR event.yoy_sales IS NOT NULL OR event.n_income IS NOT NULL",
                higher_is_better: true,
                window_days,
                industry_relative: true,
                min_event_age_days: 0,
                max_event_age_days: window_days,
            },
            weight: 0.5,
        },
    ]
}

pub(crate) fn phase7_event_reaction_segment_backfill_specs_for_days(
    window_days: i32,
) -> Vec<Phase7BackfillFactorSpec> {
    phase7_event_reaction_backfill_specs_for_days(window_days, false)
}

pub(crate) fn phase7_event_reaction_reversal_backfill_specs_for_days(
    window_days: i32,
) -> Vec<Phase7BackfillFactorSpec> {
    phase7_event_reaction_backfill_specs_for_days(window_days, true)
}

pub(crate) fn phase7_event_reaction_backfill_specs_for_days(
    window_days: i32,
    reversal: bool,
) -> Vec<Phase7BackfillFactorSpec> {
    let window_days = window_days.max(1);
    let late_start = 6.min(window_days);
    let segments = [
        (1, 5.min(window_days), "1_5d"),
        (late_start, window_days, "6_20d"),
    ];
    let prefix = if reversal {
        "event_reaction_reversal"
    } else {
        "event_reaction"
    };
    let higher_is_better = !reversal;
    let name_direction = if reversal {
        "negative post-event reaction reversal"
    } else {
        "positive post-event reaction continuation"
    };

    let mut specs = Vec::with_capacity(segments.len() * 2);
    for (min_event_age_days, max_event_age_days, suffix) in segments {
        specs.push(Phase7BackfillFactorSpec {
            factor_code: Box::leak(
                format!("{prefix}_forecast_{suffix}_indrel_std").into_boxed_str(),
            ),
            name: Box::leak(
                format!("Phase 7 PIT forecast {name_direction} industry-relative {suffix} rank")
                    .into_boxed_str(),
            ),
            period: window_days,
            kind: Phase7BackfillFactorKind::EventPostReturnCurve {
                source_table: "market_stock_forecast",
                event_filter_expression: "event.p_change_min IS NOT NULL OR event.p_change_max IS NOT NULL OR event.net_profit_min IS NOT NULL OR event.net_profit_max IS NOT NULL",
                higher_is_better,
                window_days,
                industry_relative: true,
                min_event_age_days,
                max_event_age_days,
            },
            weight: 0.25,
        });
    }
    for (min_event_age_days, max_event_age_days, suffix) in segments {
        specs.push(Phase7BackfillFactorSpec {
            factor_code: Box::leak(format!("{prefix}_express_{suffix}_indrel_std").into_boxed_str()),
            name: Box::leak(
                format!("Phase 7 PIT express {name_direction} industry-relative {suffix} rank")
                    .into_boxed_str(),
            ),
            period: window_days,
            kind: Phase7BackfillFactorKind::EventPostReturnCurve {
                source_table: "market_stock_express",
                event_filter_expression: "event.diluted_roe IS NOT NULL OR event.yoy_sales IS NOT NULL OR event.n_income IS NOT NULL",
                higher_is_better,
                window_days,
                industry_relative: true,
                min_event_age_days,
                max_event_age_days,
            },
            weight: 0.25,
        });
    }
    specs
}

pub(crate) fn phase7_event_surprise_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "event_forecast_surprise_bucket_std",
            name: "Phase 7 PIT forecast surprise bucket rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_forecast",
                value_expression: "CASE
                    WHEN ((COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0) >= 100.0
                         AND COALESCE(event.net_profit_min::double precision, 0.0) > 0.0 THEN 3.0
                    WHEN ((COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0) >= 50.0
                         AND COALESCE(event.net_profit_min::double precision, 0.0) > 0.0 THEN 2.0
                    WHEN ((COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0) >= 20.0 THEN 1.0
                    WHEN ((COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0) >= 0.0 THEN 0.0
                    WHEN ((COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0) >= -20.0 THEN -1.0
                    ELSE -2.0
                END",
                higher_is_better: true,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_forecast_profit_floor_sign_std",
            name: "Phase 7 PIT forecast profit-floor sign rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_forecast",
                value_expression: "CASE
                    WHEN COALESCE(event.net_profit_min::double precision, 0.0) > 0.0 THEN 1.0
                    WHEN COALESCE(event.net_profit_min::double precision, 0.0) < 0.0 THEN -1.0
                    ELSE 0.0
                END",
                higher_is_better: true,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_express_roe_bucket_std",
            name: "Phase 7 PIT express diluted ROE bucket rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_express",
                value_expression: "CASE
                    WHEN COALESCE(event.diluted_roe::double precision, 0.0) >= 20.0 THEN 3.0
                    WHEN COALESCE(event.diluted_roe::double precision, 0.0) >= 10.0 THEN 2.0
                    WHEN COALESCE(event.diluted_roe::double precision, 0.0) >= 5.0 THEN 1.0
                    WHEN COALESCE(event.diluted_roe::double precision, 0.0) >= 0.0 THEN 0.0
                    WHEN COALESCE(event.diluted_roe::double precision, 0.0) >= -5.0 THEN -1.0
                    ELSE -2.0
                END",
                higher_is_better: true,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_disclosure_timing_bucket_std",
            name: "Phase 7 PIT disclosure timing bucket rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_disclosure_date",
                value_expression: "CASE
                    WHEN event.pre_date IS NOT NULL
                         AND event.actual_date IS NOT NULL
                         AND (event.pre_date - event.actual_date) >= 7 THEN 2.0
                    WHEN event.pre_date IS NOT NULL
                         AND event.actual_date IS NOT NULL
                         AND (event.pre_date - event.actual_date) >= 1 THEN 1.0
                    WHEN event.pre_date IS NOT NULL
                         AND event.actual_date IS NOT NULL
                         AND (event.pre_date - event.actual_date) = 0 THEN 0.0
                    WHEN event.pre_date IS NOT NULL
                         AND event.actual_date IS NOT NULL
                         AND (event.pre_date - event.actual_date) >= -7 THEN -1.0
                    WHEN event.pre_date IS NOT NULL
                         AND event.actual_date IS NOT NULL THEN -2.0
                    ELSE NULL
                END",
                higher_is_better: true,
            },
            weight: 0.20,
        },
    ]
}

pub(crate) fn phase7_forecast_revision_surprise_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "forecast_pchange_revision_delta_120d_std",
            name: "Phase 7 PIT forecast profit-change revision delta rank",
            period: 120,
            kind: Phase7BackfillFactorKind::ForecastRevision {
                value_expression: "(
                    (COALESCE(latest.p_change_min, latest.p_change_max)::double precision
                     + COALESCE(latest.p_change_max, latest.p_change_min)::double precision) / 2.0
                ) - (
                    (COALESCE(previous.p_change_min, previous.p_change_max)::double precision
                     + COALESCE(previous.p_change_max, previous.p_change_min)::double precision) / 2.0
                )",
                higher_is_better: true,
                max_event_age_days: 120,
            },
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "forecast_profit_mid_revision_pct_120d_std",
            name: "Phase 7 PIT forecast profit midpoint revision pct rank",
            period: 120,
            kind: Phase7BackfillFactorKind::ForecastRevision {
                value_expression: "GREATEST(-5.0, LEAST(5.0, (
                    (
                        (COALESCE(latest.net_profit_min, latest.net_profit_max)::double precision
                         + COALESCE(latest.net_profit_max, latest.net_profit_min)::double precision) / 2.0
                    ) - (
                        (COALESCE(previous.net_profit_min, previous.net_profit_max)::double precision
                         + COALESCE(previous.net_profit_max, previous.net_profit_min)::double precision) / 2.0
                    )
                ) / NULLIF(GREATEST(ABS((
                    COALESCE(previous.net_profit_min, previous.net_profit_max)::double precision
                    + COALESCE(previous.net_profit_max, previous.net_profit_min)::double precision
                ) / 2.0), 1000.0), 0.0)))",
                higher_is_better: true,
                max_event_age_days: 120,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "forecast_type_upgrade_120d_std",
            name: "Phase 7 PIT forecast type upgrade revision rank",
            period: 120,
            kind: Phase7BackfillFactorKind::ForecastRevision {
                value_expression: "(
                    CASE latest.forecast_type
                        WHEN '扭亏' THEN 4.0
                        WHEN '预增' THEN 3.0
                        WHEN '略增' THEN 2.0
                        WHEN '续盈' THEN 1.0
                        WHEN '略减' THEN -1.0
                        WHEN '预减' THEN -2.0
                        WHEN '首亏' THEN -3.0
                        WHEN '续亏' THEN -4.0
                        ELSE 0.0
                    END
                ) - (
                    CASE previous.forecast_type
                        WHEN '扭亏' THEN 4.0
                        WHEN '预增' THEN 3.0
                        WHEN '略增' THEN 2.0
                        WHEN '续盈' THEN 1.0
                        WHEN '略减' THEN -1.0
                        WHEN '预减' THEN -2.0
                        WHEN '首亏' THEN -3.0
                        WHEN '续亏' THEN -4.0
                        ELSE 0.0
                    END
                )",
                higher_is_better: true,
                max_event_age_days: 120,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_repurchase_supply_shock_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "repurchase_amount_log_latest_std",
            name: "Phase 7 PIT repurchase amount log rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_repurchase",
                value_expression: "LN(1.0 + GREATEST(COALESCE(event.amount::double precision, 0.0), 0.0))",
                higher_is_better: true,
            },
            weight: 0.45,
        },
        Phase7BackfillFactorSpec {
            factor_code: "repurchase_volume_log_latest_std",
            name: "Phase 7 PIT repurchase volume log rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_repurchase",
                value_expression: "LN(1.0 + GREATEST(COALESCE(event.vol::double precision, 0.0), 0.0))",
                higher_is_better: true,
            },
            weight: 0.30,
        },
        Phase7BackfillFactorSpec {
            factor_code: "repurchase_price_band_mid_latest_std",
            name: "Phase 7 PIT repurchase price band midpoint rank",
            period: 0,
            kind: Phase7BackfillFactorKind::EventLatest {
                source_table: "market_stock_repurchase",
                value_expression: "(COALESCE(event.high_limit, event.low_limit)::double precision + COALESCE(event.low_limit, event.high_limit)::double precision) / 2.0",
                higher_is_better: true,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_block_trade_supply_demand_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "block_trade_inst_buy_20d_decay_std",
            name: "Phase 7 PIT block-trade institutional buy intensity",
            period: 20,
            kind: Phase7BackfillFactorKind::BlockTradeWindow {
                value_expression: "CASE WHEN event.buyer LIKE '%机构专用%' THEN LN(1.0 + GREATEST(COALESCE(event.amount::double precision, 0.0), 0.0)) ELSE 0.0 END",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "block_trade_inst_sell_inverse_20d_decay_std",
            name: "Phase 7 PIT block-trade institutional sell pressure inverse",
            period: 20,
            kind: Phase7BackfillFactorKind::BlockTradeWindow {
                value_expression: "CASE WHEN event.seller LIKE '%机构专用%' THEN -LN(1.0 + GREATEST(COALESCE(event.amount::double precision, 0.0), 0.0)) ELSE 0.0 END",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "block_trade_premium_20d_decay_std",
            name: "Phase 7 PIT block-trade premium/discount pressure",
            period: 20,
            kind: Phase7BackfillFactorKind::BlockTradeWindow {
                value_expression: "LN(1.0 + GREATEST(COALESCE(event.amount::double precision, 0.0), 0.0)) * ((event.price::double precision / NULLIF(bar.close::double precision, 0.0)) - 1.0)",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_unlock_supply_pressure_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "unlock_pressure_30d_neg_ratio_std",
            name: "Phase 7 PIT 30d unlock pressure inverse rank",
            period: 30,
            kind: Phase7BackfillFactorKind::UnlockPressure { horizon_days: 30 },
            weight: 0.50,
        },
        Phase7BackfillFactorSpec {
            factor_code: "unlock_pressure_90d_neg_ratio_std",
            name: "Phase 7 PIT 90d unlock pressure inverse rank",
            period: 90,
            kind: Phase7BackfillFactorKind::UnlockPressure { horizon_days: 90 },
            weight: 0.30,
        },
        Phase7BackfillFactorSpec {
            factor_code: "unlock_pressure_180d_neg_ratio_std",
            name: "Phase 7 PIT 180d unlock pressure inverse rank",
            period: 180,
            kind: Phase7BackfillFactorKind::UnlockPressure { horizon_days: 180 },
            weight: 0.20,
        },
    ]
}

pub(crate) fn phase7_liquidity_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "liq_impact_improve_20v120_std",
            name: "Phase 7 PIT liquidity impact improvement 20d versus 120d",
            period: 20,
            kind: Phase7BackfillFactorKind::LiquidityQuality {
                signal: LiquidityQualitySignal::ImpactImprovement,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "liq_amount_trend_20v120_std",
            name: "Phase 7 PIT traded amount trend 20d versus 120d",
            period: 20,
            kind: Phase7BackfillFactorKind::LiquidityQuality {
                signal: LiquidityQualitySignal::AmountTrend,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "liq_amount_stability_60d_std",
            name: "Phase 7 PIT traded amount stability 60d",
            period: 60,
            kind: Phase7BackfillFactorKind::LiquidityQuality {
                signal: LiquidityQualitySignal::AmountStability,
                short_window: 60,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "liq_turnover_stability_60d_std",
            name: "Phase 7 PIT turnover proxy stability 60d",
            period: 60,
            kind: Phase7BackfillFactorKind::LiquidityQuality {
                signal: LiquidityQualitySignal::TurnoverStability,
                short_window: 60,
                long_window: 120,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_market_residual_risk_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "mkt_low_beta_120d_std",
            name: "Phase 7 PIT low market beta 120d rank",
            period: 120,
            kind: Phase7BackfillFactorKind::MarketResidualRisk {
                signal: MarketResidualRiskSignal::LowBeta,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "mkt_downside_beta_120d_std",
            name: "Phase 7 PIT low downside market beta 120d rank",
            period: 120,
            kind: Phase7BackfillFactorKind::MarketResidualRisk {
                signal: MarketResidualRiskSignal::LowDownsideBeta,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "resid_low_vol_120d_std",
            name: "Phase 7 PIT low market-residual volatility 120d rank",
            period: 120,
            kind: Phase7BackfillFactorKind::MarketResidualRisk {
                signal: MarketResidualRiskSignal::LowResidualVolatility,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "resid_reversal_20d_std",
            name: "Phase 7 PIT market-residual 20d reversal rank",
            period: 20,
            kind: Phase7BackfillFactorKind::MarketResidualRisk {
                signal: MarketResidualRiskSignal::ResidualReversal,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_industry_prosperity_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "ind_pros_ret_mom_20v120_std",
            name: "Phase 7 PIT industry prosperity return momentum 20d versus 120d",
            period: 20,
            kind: Phase7BackfillFactorKind::IndustryProsperity {
                signal: IndustryProsperitySignal::ReturnMomentum,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.34,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_pros_breadth_60d_std",
            name: "Phase 7 PIT industry prosperity positive breadth 60d",
            period: 60,
            kind: Phase7BackfillFactorKind::IndustryProsperity {
                signal: IndustryProsperitySignal::PositiveBreadth,
                short_window: 60,
                long_window: 120,
            },
            weight: 0.33,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ind_pros_amount_trend_20v120_std",
            name: "Phase 7 PIT industry prosperity traded amount trend 20d versus 120d",
            period: 20,
            kind: Phase7BackfillFactorKind::IndustryProsperity {
                signal: IndustryProsperitySignal::AmountTrend,
                short_window: 20,
                long_window: 120,
            },
            weight: 0.33,
        },
    ]
}

pub(crate) fn phase7_futures_price_chain_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "fpc_price_mom_20v60_std",
            name: "P3.19 PIT futures price-chain product momentum 20d versus 60d",
            period: 20,
            kind: Phase7BackfillFactorKind::FuturesPriceChain {
                signal: FuturesPriceChainSignal::PriceMomentum,
            },
            weight: 0.50,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fpc_inventory_tight_20v60_std",
            name: "P3.19 PIT futures warehouse inventory tightness 20d versus 60d",
            period: 20,
            kind: Phase7BackfillFactorKind::FuturesPriceChain {
                signal: FuturesPriceChainSignal::InventoryTightness,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "fpc_net_position_20v60_std",
            name: "P3.19 PIT futures holding net-position trend 20d versus 60d",
            period: 20,
            kind: Phase7BackfillFactorKind::FuturesPriceChain {
                signal: FuturesPriceChainSignal::NetPositionTrend,
            },
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_equity_pledge_pressure_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![Phase7BackfillFactorSpec {
        factor_code: "eq_pledge_low_ratio_std",
        name: "P3.20 PIT equity pledge low pressure ratio",
        period: 0,
        kind: Phase7BackfillFactorKind::EquityPledgePressure,
        weight: 1.0,
    }]
}

pub(crate) fn phase7_shareholder_structure_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "sh_holder_count_decline_1y_std",
            name: "P3.21 PIT shareholder holder-count decline versus prior-year snapshot",
            period: 252,
            kind: Phase7BackfillFactorKind::ShareholderStructure,
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "sh_holder_count_decline_prev_std",
            name: "P3.21 PIT shareholder holder-count decline versus previous snapshot",
            period: 63,
            kind: Phase7BackfillFactorKind::ShareholderStructure,
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "sh_holder_trade_net_increase_120d_std",
            name: "P3.21 PIT shareholder net increase event intensity over 120 calendar days",
            period: 120,
            kind: Phase7BackfillFactorKind::ShareholderStructure,
            weight: 0.25,
        },
    ]
}

pub(crate) fn phase7_margin_detail_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "md_financing_buy_intensity_20d_std",
            name: "P3.22 PIT margin-detail financing buy intensity over traded amount 20d",
            period: 20,
            kind: Phase7BackfillFactorKind::MarginDetailLeverageCrowding,
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "md_financing_balance_chg_20d_std",
            name: "P3.22 PIT margin-detail financing balance change versus 20 sessions",
            period: 20,
            kind: Phase7BackfillFactorKind::MarginDetailLeverageCrowding,
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "md_short_sell_pressure_relief_20d_std",
            name: "P3.22 PIT margin-detail inverse short-selling pressure over 20 sessions",
            period: 20,
            kind: Phase7BackfillFactorKind::MarginDetailLeverageCrowding,
            weight: 0.20,
        },
    ]
}

pub(crate) fn phase7_analyst_revision_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "ar_rating_change_net_20d_std",
            name: "P3.23 PIT AkShare analyst revision net upgrade/downgrade 20d",
            period: 20,
            kind: Phase7BackfillFactorKind::AnalystRevision {
                value_expression: "rating_change_score",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.40,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ar_upgrade_event_20d_std",
            name: "P3.23 PIT AkShare analyst revision upgrade event intensity 20d",
            period: 20,
            kind: Phase7BackfillFactorKind::AnalystRevision {
                value_expression: "CASE WHEN rating_change_score > 0.0 THEN 1.0 ELSE NULL END",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ar_downgrade_pressure_20d_std",
            name: "P3.23 PIT AkShare analyst revision inverse downgrade pressure 20d",
            period: 20,
            kind: Phase7BackfillFactorKind::AnalystRevision {
                value_expression: "CASE WHEN rating_change_score < 0.0 THEN -1.0 ELSE NULL END",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: "ar_bullish_first_rating_60d_std",
            name: "P3.23 PIT AkShare bullish first-rating attention 60d",
            period: 60,
            kind: Phase7BackfillFactorKind::AnalystRevision {
                value_expression: "bullish_first_rating_score",
                higher_is_better: true,
                window_days: 60,
                decay_days: 60,
            },
            weight: 0.15,
        },
    ]
}
