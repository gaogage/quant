//! Phase 7 factor backfill routes — 30+ factor family backfill implementations.

use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, NaiveDate};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use quant_api::discovery::phase7::phase7_alpha_blend_profiles;

use crate::phase7_alpha_admission::{
    validate_analyst_revision_entrypoint_admission, validate_equity_pledge_entrypoint_admission,
    validate_futures_price_chain_entrypoint_admission,
    validate_industry_prosperity_entrypoint_admission,
    validate_industry_prosperity_factor_builder_admission,
    validate_margin_detail_entrypoint_admission,
    validate_shareholder_structure_entrypoint_admission, ANALYST_REVISION_SOURCE,
    EQUITY_PLEDGE_PRESSURE_SOURCE, FUTURES_PRICE_CHAIN_SOURCE, INDUSTRY_PROSPERITY_SOURCE,
    MARGIN_DETAIL_SOURCE, SHAREHOLDER_STRUCTURE_SOURCE,
};
use crate::AppState;
use super::{
    background_factor_task_id, parse_phase7_backfill_date, trim_or_default, trim_required,
    usize_to_i32, FactorDefinitionInput, upsert_factor_definition_input,
};

#[derive(Debug, Deserialize)]
pub struct Phase7PriceVolumeBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7FinancialQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7FinancialQualityChangeBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7EarningsRecoveryPersistenceBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7IndustryResidualQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7RelativeStrengthBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7QualityRelativeStrengthBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7GrowthRecoveryBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7ValuationBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7MoneyflowBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7MoneyflowCongestionBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7SupplyFloatShockBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7CashflowQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7DividendQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7EventAlphaBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7EventWindowAlphaBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7EventSurpriseBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7ForecastRevisionSurpriseBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7RepurchaseSupplyShockBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7BlockTradeSupplyDemandBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7UnlockSupplyPressureBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7LiquidityQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7MarketResidualRiskBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7IndustryProsperityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7FuturesPriceChainBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7EquityPledgePressureBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7ShareholderStructureBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7MarginDetailBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7AnalystRevisionBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub alpha_admission_gate_id: Option<String>,
    pub universe_profile: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7AlphaBlendSourceRequest {
    pub combo_name: String,
    pub version: Option<String>,
    pub weight: f64,
}

#[derive(Debug, Deserialize)]
pub struct Phase7AlphaBlendBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub allow_signed_weights: bool,
    pub sources: Vec<Phase7AlphaBlendSourceRequest>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7AlphaBlendProfilesBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub profile_names: Option<Vec<String>>,
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Phase7AlphaBlendSourcePlan {
    pub(crate) combo_name: String,
    pub(crate) version: String,
    pub(crate) weight: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct SetBasedFactorBackfillPlan {
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) version: String,
    pub(crate) combo_name: String,
    pub(crate) statement_timeout_ms: u64,
    pub(crate) task_type: &'static str,
    pub(crate) source: &'static str,
    pub(crate) heartbeat_timeout_seconds: i32,
    pub(crate) bundle_name: &'static str,
    pub(crate) category: &'static str,
    pub(crate) phase: &'static str,
    pub(crate) dependencies: &'static [&'static str],
    pub(crate) combo_method: &'static str,
    pub(crate) experiment_type: &'static str,
    pub(crate) source_combos: Vec<Phase7AlphaBlendSourcePlan>,
}

pub(crate) type Phase7PriceVolumeBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7FinancialQualityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7FinancialQualityChangeBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7EarningsRecoveryPersistenceBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7IndustryResidualQualityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7RelativeStrengthBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7QualityRelativeStrengthBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7GrowthRecoveryBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7ValuationBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7MoneyflowBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7MoneyflowCongestionBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7SupplyFloatShockBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7CashflowQualityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7DividendQualityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7EventAlphaBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7EventWindowAlphaBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7EventSurpriseBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7ForecastRevisionSurpriseBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7RepurchaseSupplyShockBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7BlockTradeSupplyDemandBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7UnlockSupplyPressureBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7LiquidityQualityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7MarketResidualRiskBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7IndustryProsperityBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7FuturesPriceChainBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7EquityPledgePressureBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7ShareholderStructureBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7MarginDetailBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7AnalystRevisionBackfillPlan = SetBasedFactorBackfillPlan;
pub(crate) type Phase7AlphaBlendBackfillPlan = SetBasedFactorBackfillPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinancialAnnualChangeMode {
    PercentChange,
    Difference,
    Decrease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SupplyFloatShockMode {
    GrowthInverse,
    ChurnInverse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiquidityQualitySignal {
    ImpactImprovement,
    AmountTrend,
    AmountStability,
    TurnoverStability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarketResidualRiskSignal {
    LowBeta,
    LowDownsideBeta,
    LowResidualVolatility,
    ResidualReversal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndustryProsperitySignal {
    ReturnMomentum,
    PositiveBreadth,
    AmountTrend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuturesPriceChainSignal {
    PriceMomentum,
    InventoryTightness,
    NetPositionTrend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase7BackfillFactorKind {
    Reversal,
    DownsideVolatility,
    AmihudIlliquidity,
    AmountIntensity,
    MarketRelativeMomentum,
    IndustryRelativeMomentum,
    FinancialLatest {
        source_column: &'static str,
        higher_is_better: bool,
    },
    IndustryRelativeFinancialLatest {
        source_column: &'static str,
        higher_is_better: bool,
    },
    DailyBasicLatest {
        source_column: &'static str,
        higher_is_better: bool,
        positive_only: bool,
    },
    MoneyflowRolling {
        amount_expression: &'static str,
        higher_is_better: bool,
    },
    MoneyflowCongestionInteraction {
        flow_expression: &'static str,
    },
    SupplyFloatShock {
        share_expression: &'static str,
        horizon_days: i32,
        mode: SupplyFloatShockMode,
    },
    CashflowLatest {
        value_expression: &'static str,
        required_filter: &'static str,
        higher_is_better: bool,
    },
    DividendRollingQuality {
        value_expression: &'static str,
        higher_is_better: bool,
    },
    EventLatest {
        source_table: &'static str,
        value_expression: &'static str,
        higher_is_better: bool,
    },
    BlockTradeWindow {
        value_expression: &'static str,
        higher_is_better: bool,
        window_days: i32,
        decay_days: i32,
    },
    UnlockPressure {
        horizon_days: i32,
    },
    LiquidityQuality {
        signal: LiquidityQualitySignal,
        short_window: i32,
        long_window: i32,
    },
    MarketResidualRisk {
        signal: MarketResidualRiskSignal,
        short_window: i32,
        long_window: i32,
    },
    IndustryProsperity {
        signal: IndustryProsperitySignal,
        short_window: i32,
        long_window: i32,
    },
    FuturesPriceChain {
        signal: FuturesPriceChainSignal,
    },
    EquityPledgePressure,
    ShareholderStructure,
    MarginDetailLeverageCrowding,
    AnalystRevision {
        value_expression: &'static str,
        higher_is_better: bool,
        window_days: i32,
        decay_days: i32,
    },
    ForecastRevision {
        value_expression: &'static str,
        higher_is_better: bool,
        max_event_age_days: i32,
    },
    EventWindow {
        source_table: &'static str,
        value_expression: &'static str,
        higher_is_better: bool,
        window_days: i32,
        decay_days: i32,
    },
    EventPostReturnCurve {
        source_table: &'static str,
        event_filter_expression: &'static str,
        higher_is_better: bool,
        window_days: i32,
        industry_relative: bool,
        min_event_age_days: i32,
        max_event_age_days: i32,
    },
    FinancialAnnualChange {
        source_column: &'static str,
        mode: FinancialAnnualChangeMode,
    },
    FinancialAnnualAcceleration {
        source_column: &'static str,
        mode: FinancialAnnualChangeMode,
    },
    FinancialAnnualPersistence {
        source_column: &'static str,
        mode: FinancialAnnualChangeMode,
    },
    /// P4.2b 大盘动量反转交互特征:大盘股池内 reversal × momentum。
    /// 补偿生产 combo ascending 在大盘段的失效(P4.1c 发现大盘 IC 为正)。
    LargeCapMomentumReversal {
        reversal_period: i32,
        momentum_period: i32,
        /// 大盘市值阈值(亿元),total_mv > 此值才入选
        large_cap_threshold_yi: i32,
    },
    /// P4.2b 防御板块低波质量交互特征:防御行业池内 low_volatility × fin_roe。
    /// 补偿生产 combo ascending 在防御板块的失效(P4.1c 发现保险正 IC/银行白酒近零)。
    DefensiveLowVolQuality {
        volatility_period: i32,
        /// 防御行业列表(申万行业名)
        industries: &'static [&'static str],
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetBasedFactorSpec {
    pub(crate) factor_code: &'static str,
    pub(crate) name: &'static str,
    pub(crate) period: i32,
    pub(crate) kind: Phase7BackfillFactorKind,
    pub(crate) weight: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct SetBasedFactorBackfillReport {
    pub(crate) factor_rows: usize,
    pub(crate) combo_rows: usize,
    pub(crate) factor_rows_by_code: Vec<(String, usize)>,
}

#[derive(Debug, Clone)]
pub(crate) enum SetBasedFactorBackfillCompletion {
    Completed {
        report: SetBasedFactorBackfillReport,
        elapsed_ms: u64,
    },
    Cancelled {
        report: SetBasedFactorBackfillReport,
        elapsed_ms: u64,
    },
}

pub(crate) type Phase7BackfillFactorSpec = SetBasedFactorSpec;
pub(crate) type Phase7BackfillCompletion = SetBasedFactorBackfillCompletion;

pub(crate) type SetBasedFactorSqlBuilder = fn(&SetBasedFactorSpec) -> String;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetBasedFactorBackfillJob<'a> {
    pub(crate) plan: &'a SetBasedFactorBackfillPlan,
    pub(crate) specs: &'a [SetBasedFactorSpec],
    pub(crate) factor_sql: SetBasedFactorSqlBuilder,
}

impl<'a> SetBasedFactorBackfillJob<'a> {
    pub(crate) fn new(
        plan: &'a SetBasedFactorBackfillPlan,
        specs: &'a [SetBasedFactorSpec],
        factor_sql: SetBasedFactorSqlBuilder,
    ) -> Self {
        Self {
            plan,
            specs,
            factor_sql,
        }
    }

    pub(crate) fn total_steps(&self) -> usize {
        self.specs.len().saturating_add(1)
    }
}

impl SetBasedFactorBackfillCompletion {
    pub(crate) fn completed_with(report: SetBasedFactorBackfillReport, elapsed_ms: u64) -> Self {
        Self::Completed { report, elapsed_ms }
    }

    pub(crate) fn cancelled_with(report: SetBasedFactorBackfillReport, elapsed_ms: u64) -> Self {
        Self::Cancelled { report, elapsed_ms }
    }

    pub(crate) fn task_status(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    pub(crate) fn experiment_status(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Cancelled { .. } => "partial",
        }
    }

    pub(crate) fn report(&self) -> &SetBasedFactorBackfillReport {
        match self {
            Self::Completed { report, .. } | Self::Cancelled { report, .. } => report,
        }
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        match self {
            Self::Completed { elapsed_ms, .. } | Self::Cancelled { elapsed_ms, .. } => *elapsed_ms,
        }
    }
}

impl SetBasedFactorBackfillReport {
    fn total_rows(&self) -> usize {
        self.factor_rows.saturating_add(self.combo_rows)
    }
}

impl Phase7PriceVolumeBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7PriceVolumeBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_price_volume_expanded_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7PriceVolumeBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_price_volume_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_price_volume_expanded_v1",
            category: "price_volume",
            phase: "7-J",
            dependencies: &["market_stock_daily_bar"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7FinancialQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7FinancialQualityBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_financial_quality_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7FinancialQualityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_financial_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_financial_quality_v1",
            category: "fundamental",
            phase: "7-J",
            dependencies: &["market_financial_indicator", "market_trade_calendar"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7FinancialQualityChangeBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7FinancialQualityChangeBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_financial_quality_change_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7FinancialQualityChangeBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_financial_quality_change_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_financial_quality_change_v1",
            category: "fundamental_change",
            phase: "7-P3.7",
            dependencies: &["market_financial_indicator", "market_trade_calendar"],
            combo_method: "equal_weight_fq_change",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EarningsRecoveryPersistenceBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7EarningsRecoveryPersistenceBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_earnings_recovery_persistence_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7EarningsRecoveryPersistenceBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_earnings_recovery_persistence_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_earnings_recovery_persistence_v1",
            category: "earnings_recovery_persistence",
            phase: "7-P3.7",
            dependencies: &["market_financial_indicator", "market_trade_calendar"],
            combo_method: "equal_weight_earn_persist",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7IndustryResidualQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7IndustryResidualQualityBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_industry_residual_quality_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7IndustryResidualQualityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_industry_residual_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_industry_residual_quality_v1",
            category: "fundamental_residual",
            phase: "7-AG/7-J",
            dependencies: &[
                "market_financial_indicator",
                "market_trade_calendar",
                "market_stock",
            ],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7RelativeStrengthBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7RelativeStrengthBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_relative_strength_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7RelativeStrengthBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_relative_strength_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_relative_strength_v1",
            category: "relative_strength",
            phase: "7-B/7-J",
            dependencies: &["market_stock_daily_bar", "market_stock"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7QualityRelativeStrengthBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7QualityRelativeStrengthBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_quality_relative_strength_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7QualityRelativeStrengthBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_quality_relative_strength_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_quality_relative_strength_v1",
            category: "composite_alpha",
            phase: "7-B/7-J",
            dependencies: &[
                "factor_value",
                "market_financial_indicator",
                "market_trade_calendar",
                "market_stock_daily_bar",
                "market_stock",
            ],
            combo_method: "quality_60_relative_strength_40",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7GrowthRecoveryBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7GrowthRecoveryBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_growth_recovery_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7GrowthRecoveryBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_growth_recovery_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_growth_recovery_v1",
            category: "fundamental_growth",
            phase: "7-B/7-J",
            dependencies: &["market_financial_indicator", "market_trade_calendar"],
            combo_method: "equal_weight_growth_recovery",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7ValuationBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7ValuationBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(self.combo_name, "phase7_valuation_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7ValuationBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_valuation_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_valuation_v1",
            category: "valuation",
            phase: "7-B/7-J",
            dependencies: &["market_stock_daily_basic"],
            combo_method: "equal_weight_valuation",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7MoneyflowBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7MoneyflowBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(self.combo_name, "phase7_moneyflow_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7MoneyflowBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_moneyflow_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_moneyflow_v1",
            category: "moneyflow_alpha",
            phase: "7-B/7-J",
            dependencies: &["market_stock_moneyflow", "market_stock_daily_bar"],
            combo_method: "equal_weight_moneyflow",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7MoneyflowCongestionBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7MoneyflowCongestionBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_moneyflow_congestion_interaction_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7MoneyflowCongestionBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_moneyflow_congestion_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_moneyflow_congestion_interaction_v1",
            category: "moneyflow_congestion_alpha",
            phase: "7-P3.8",
            dependencies: &[
                "market_stock_moneyflow",
                "market_stock_daily_bar_adj",
                "market_stock_daily_basic",
            ],
            combo_method: "equal_weight_mf_congest",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7SupplyFloatShockBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7SupplyFloatShockBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_supply_float_shock_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7SupplyFloatShockBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_supply_float_shock_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_supply_float_shock_v1",
            category: "supply_float_shock_alpha",
            phase: "7-P3.11",
            dependencies: &["market_stock_daily_basic"],
            combo_method: "equal_weight_supply_float",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7CashflowQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7CashflowQualityBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_cashflow_quality_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7CashflowQualityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_cashflow_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_cashflow_quality_v1",
            category: "cashflow_quality",
            phase: "7-FF/7-J",
            dependencies: &["market_stock_cashflow", "market_trade_calendar"],
            combo_method: "equal_weight_cashflow_quality",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7DividendQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7DividendQualityBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_dividend_quality_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7DividendQualityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_dividend_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_dividend_quality_v1",
            category: "dividend_quality",
            phase: "7-FF/7-J",
            dependencies: &["market_stock_dividend", "market_trade_calendar"],
            combo_method: "equal_weight_dividend_quality",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EventAlphaBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7EventAlphaBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_event_earnings_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7EventAlphaBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_event_alpha_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_event_earnings_v1",
            category: "event_alpha",
            phase: "7-Y",
            dependencies: &[
                "market_stock_forecast",
                "market_stock_express",
                "market_stock_disclosure_date",
                "market_trade_calendar",
            ],
            combo_method: "weighted_event_earnings",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EventWindowAlphaBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7EventWindowAlphaBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_event_window_earnings_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        let combo_name = combo_name;
        let bundle_name = event_window_bundle_name(&combo_name);
        let phase = event_window_phase(&combo_name);
        let dependencies = event_window_dependencies(&combo_name);
        let combo_method = event_window_combo_method(&combo_name);

        Ok(Phase7EventWindowAlphaBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_event_window_alpha_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name,
            category: "event_alpha",
            phase,
            dependencies,
            combo_method,
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EventSurpriseBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7EventSurpriseBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_event_surprise_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7EventSurpriseBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_event_surprise_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_event_surprise_v1",
            category: "event_alpha",
            phase: "7-Y4",
            dependencies: &[
                "market_stock_forecast",
                "market_stock_express",
                "market_stock_disclosure_date",
                "market_trade_calendar",
            ],
            combo_method: "weighted_event_surprise",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7ForecastRevisionSurpriseBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7ForecastRevisionSurpriseBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_forecast_revision_surprise_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7ForecastRevisionSurpriseBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_forecast_revision_surprise_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_forecast_revision_surprise_v1",
            category: "event_revision_alpha",
            phase: "7-P3.11",
            dependencies: &["market_stock_forecast", "market_trade_calendar"],
            combo_method: "weighted_forecast_revision",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7RepurchaseSupplyShockBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7RepurchaseSupplyShockBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_repurchase_supply_shock_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7RepurchaseSupplyShockBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_repurchase_supply_shock_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_repurchase_supply_shock_v1",
            category: "supply_demand_shock",
            phase: "7-P3.12",
            dependencies: &["market_stock_repurchase", "market_trade_calendar"],
            combo_method: "weighted_repurchase_supply_shock",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7BlockTradeSupplyDemandBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7BlockTradeSupplyDemandBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_block_trade_supply_demand_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7BlockTradeSupplyDemandBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_block_trade_supply_demand_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_block_trade_supply_demand_v1",
            category: "supply_demand_shock",
            phase: "7-P3.15",
            dependencies: &[
                "market_stock_block_trade",
                "market_stock_daily_bar_adj",
                "market_trade_calendar",
            ],
            combo_method: "weighted_block_trade_sd",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7UnlockSupplyPressureBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7UnlockSupplyPressureBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_unlock_supply_pressure_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7UnlockSupplyPressureBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_unlock_supply_pressure_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_unlock_supply_pressure_v1",
            category: "supply_demand_shock",
            phase: "7-P3.11",
            dependencies: &[
                "market_stock_share_float",
                "market_stock_daily_bar_adj",
                "market_trade_calendar",
            ],
            combo_method: "weighted_unlock_supply_pressure",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7LiquidityQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7LiquidityQualityBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name =
            trim_or_default(self.combo_name, "phase7_liquidity_quality_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7LiquidityQualityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_liquidity_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_liquidity_quality_v1",
            category: "liquidity_quality_alpha",
            phase: "7-P3.16",
            dependencies: &["market_stock_daily_bar_adj", "market_stock_daily_basic"],
            combo_method: "weighted_liquidity_quality",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7MarketResidualRiskBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7MarketResidualRiskBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_market_residual_risk_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7MarketResidualRiskBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_market_residual_risk_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_market_residual_risk_v1",
            category: "market_residual_risk_alpha",
            phase: "7-P3.17",
            dependencies: &["market_stock_daily_bar_adj", "market_index_daily_bar"],
            combo_method: "weighted_market_residual",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7IndustryProsperityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7IndustryProsperityBackfillPlan, String> {
        validate_industry_prosperity_entrypoint_admission(
            INDUSTRY_PROSPERITY_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2017, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_industry_prosperity_proxy_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7IndustryProsperityBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_industry_prosperity_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_industry_prosperity_proxy_v1",
            category: "industry_prosperity_alpha",
            phase: "7-P3.18G",
            dependencies: &[
                "market_stock_industry_membership_pit",
                "market_stock_daily_bar_adj",
                "market_stock_daily_basic",
                "market_stock",
                "market_trade_calendar",
            ],
            combo_method: "weighted_ind_prosperity",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7FuturesPriceChainBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7FuturesPriceChainBackfillPlan, String> {
        validate_futures_price_chain_entrypoint_admission(
            FUTURES_PRICE_CHAIN_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "p319q-sw2021-l1-price-chain-v1", "version")?;
        let combo_name = trim_or_default(self.combo_name, "futures_price_chain", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7FuturesPriceChainBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_futures_price_chain_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "futures_price_chain",
            category: "futures_price_chain_alpha",
            phase: "7-P3.19Q",
            dependencies: &[
                "market_futures_daily",
                "market_futures_warehouse_receipt",
                "market_futures_holding_rank",
                "market_futures_product_exposure_mapping_pit",
                "market_futures_product_exclusion_gate_pit",
                "market_stock_industry_membership_pit",
                "market_stock_name_history",
                "market_stock_daily_bar",
                "market_stock_daily_basic",
                "market_stock",
                "market_trade_calendar",
            ],
            combo_method: "weighted_futures_price_chain",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EquityPledgePressureBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7EquityPledgePressureBackfillPlan, String> {
        validate_equity_pledge_entrypoint_admission(
            EQUITY_PLEDGE_PRESSURE_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "p320f-equity-pledge-pressure-v1", "version")?;
        let combo_name = trim_or_default(self.combo_name, "equity_pledge_pressure", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7EquityPledgePressureBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_equity_pledge_pressure_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "equity_pledge_pressure",
            category: "equity_pledge_pressure_alpha",
            phase: "7-P3.20F",
            dependencies: &[
                "market_stock_pledge_stat",
                "market_stock_pledge_detail",
                "market_stock_daily_bar",
                "market_stock_daily_basic",
                "market_stock",
                "market_stock_name_history",
                "market_trade_calendar",
            ],
            combo_method: "weighted_equity_pledge_pressure",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7ShareholderStructureBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7ShareholderStructureBackfillPlan, String> {
        validate_shareholder_structure_entrypoint_admission(
            SHAREHOLDER_STRUCTURE_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "p321d-shareholder-low-fanout-v1", "version")?;
        let combo_name = trim_or_default(self.combo_name, "shareholder_structure", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7ShareholderStructureBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_shareholder_structure_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "shareholder_structure",
            category: "shareholder_structure_alpha",
            phase: "7-P3.21D",
            dependencies: &[
                "market_stock_holder_number",
                "market_stock_holder_trade",
                "market_stock_daily_bar",
                "market_stock_daily_basic",
                "market_stock",
                "market_stock_name_history",
                "market_trade_calendar",
            ],
            combo_method: "weighted_shareholder_structure",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7MarginDetailBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7MarginDetailBackfillPlan, String> {
        validate_margin_detail_entrypoint_admission(
            MARGIN_DETAIL_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "p322e-margin-leverage-v1", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "margin_detail_leverage_crowding",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7MarginDetailBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_margin_detail_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "margin_detail",
            category: "margin_detail_leverage_crowding_alpha",
            phase: "7-P3.22E",
            dependencies: &[
                "market_stock_margin_detail",
                "market_stock_daily_bar",
                "market_stock_daily_basic",
                "market_stock",
                "market_stock_name_history",
                "market_trade_calendar",
            ],
            combo_method: "weighted_margin_detail",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7AnalystRevisionBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7AnalystRevisionBackfillPlan, String> {
        validate_analyst_revision_entrypoint_admission(
            ANALYST_REVISION_SOURCE,
            self.alpha_admission_gate_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.universe_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            "factor builder",
        )?;

        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 3).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "p323f-akshare-cninfo-revision-v1", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "multi_vendor_analyst_revision",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7AnalystRevisionBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_analyst_revision_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "multi_vendor_analyst_revision",
            category: "analyst_revision_alpha",
            phase: "7-P3.23F",
            dependencies: &[
                "market_vendor_analyst_revision_raw",
                "market_stock_daily_bar",
                "market_stock_daily_basic",
                "market_stock",
                "market_stock_name_history",
                "market_trade_calendar",
            ],
            combo_method: "weighted_analyst_revision",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7AlphaBlendBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7AlphaBlendBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        if self.sources.len() < 2 {
            return Err("sources must include at least 2 combo sources".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "phase7_value_quality_growth_rel_v1",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }
        validate_industry_prosperity_factor_builder_admission(&combo_name)?;

        let allow_signed_weights = self.allow_signed_weights;
        let mut source_combos = Vec::with_capacity(self.sources.len());
        for source in self.sources {
            if !source.weight.is_finite() {
                return Err("source weights must be finite values".to_string());
            }
            if allow_signed_weights {
                if source.weight.abs() <= f64::EPSILON {
                    return Err("signed source weights must be non-zero finite values".to_string());
                }
            } else if source.weight <= 0.0 {
                return Err("source weights must be finite positive values".to_string());
            }
            let combo_name = trim_required(source.combo_name, "source.combo_name")?;
            if combo_name.len() > 128 {
                return Err("source.combo_name must be <= 128 chars".to_string());
            }
            validate_industry_prosperity_factor_builder_admission(&combo_name)?;
            let version = trim_or_default(source.version, "1.0.0", "source.version")?;
            if version.len() > 32 {
                return Err("source.version must be <= 32 chars".to_string());
            }
            source_combos.push(Phase7AlphaBlendSourcePlan {
                combo_name,
                version,
                weight: source.weight,
            });
        }

        if allow_signed_weights {
            let weight_abs_sum = source_combos
                .iter()
                .map(|source| source.weight.abs())
                .sum::<f64>();
            if (weight_abs_sum - 1.0).abs() > 1e-9 {
                return Err("signed source weights must have absolute sum 1.0".to_string());
            }
        } else {
            let weight_sum = source_combos
                .iter()
                .map(|source| source.weight)
                .sum::<f64>();
            if (weight_sum - 1.0).abs() > 1e-9 {
                return Err("source weights must sum to 1.0".to_string());
            }
        }

        Ok(Phase7AlphaBlendBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_alpha_blend_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_alpha_blend_v1",
            category: "composite_alpha",
            phase: "7-B/7-J",
            dependencies: &["multi_factor_value"],
            combo_method: if allow_signed_weights {
                "weighted_combo_blend_signed"
            } else {
                "weighted_combo_blend"
            },
            experiment_type: "phase7_factor_backfill_profile",
            source_combos,
        })
    }
}

impl Phase7AlphaBlendProfilesBackfillRequest {
    pub(crate) fn into_plans(self) -> Result<Vec<Phase7AlphaBlendBackfillPlan>, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version_override = match self.version {
            Some(value) => Some(trim_required(value, "version")?),
            None => None,
        };
        if matches!(version_override.as_ref(), Some(version) if version.len() > 32) {
            return Err("version must be <= 32 chars".to_string());
        }
        let selected_names = self.profile_names.map(|names| {
            names
                .into_iter()
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
                .collect::<std::collections::BTreeSet<_>>()
        });

        let mut plans = Vec::new();
        for profile in phase7_alpha_blend_profiles() {
            if let Some(selected_names) = selected_names.as_ref() {
                if !selected_names.contains(&profile.profile_name)
                    && !selected_names.contains(&profile.combo_name)
                {
                    continue;
                }
            }
            let version = version_override
                .clone()
                .unwrap_or_else(|| profile.version.clone());
            let mut sources = Vec::with_capacity(profile.sources.len());
            for source in profile.sources {
                let weight = source.weight.to_f64().ok_or_else(|| {
                    format!(
                        "profile {} source {} weight is not representable as f64",
                        profile.profile_name, source.combo_name
                    )
                })?;
                sources.push(Phase7AlphaBlendSourcePlan {
                    combo_name: source.combo_name,
                    version: source.version,
                    weight,
                });
            }
            let combo_method = if matches!(
                profile.combo_name.as_str(),
                "phase7_quality_cashflow_confirm_v1"
                    | "phase7_quality_dividend_confirm_v1"
                    | "phase7_quality_cashflow_dividend_confirm_v1"
                    | "phase7_quality_event_window_overlay_v1"
                    | "phase7_quality_event_post_return_curve_overlay_v1"
                    | "phase7_fq_change_event_surprise_sleeve_05pct_v1"
                    | "phase7_fq_change_event_surprise_sleeve_10pct_v1"
                    | "phase7_fq_change_event_surprise_sleeve_15pct_v1"
                    | "phase7_fq_change_supply_float_sleeve_05pct_v1"
                    | "phase7_fq_change_supply_float_sleeve_10pct_v1"
                    | "phase7_fq_change_supply_float_sleeve_15pct_v1"
                    | "phase7_fq_change_unlock_pressure_sleeve_05pct_v1"
                    | "phase7_fq_change_unlock_pressure_sleeve_10pct_v1"
                    | "phase7_fq_change_unlock_pressure_sleeve_15pct_v1"
                    | "phase7_fq_change_forecast_revision_sleeve_05pct_v1"
                    | "phase7_fq_change_forecast_revision_sleeve_10pct_v1"
                    | "phase7_fq_change_forecast_revision_sleeve_15pct_v1"
                    | "phase7_fq_change_shareholder_structure_sleeve_05pct_v1"
                    | "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
                    | "phase7_fq_change_shareholder_structure_sleeve_15pct_v1"
                    | "phase7_quality_event_reaction_segments_overlay_v1"
                    | "phase7_quality_event_reaction_reversal_overlay_v1"
            ) {
                "weighted_combo_optional_overlay"
            } else {
                "weighted_combo_blend_profile"
            };
            plans.push(Phase7AlphaBlendBackfillPlan {
                start_date,
                end_date,
                version,
                combo_name: profile.combo_name,
                statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
                task_type: "phase7_alpha_blend_profiles_backfill",
                source: "factor",
                heartbeat_timeout_seconds: 3600,
                bundle_name: "phase7_alpha_blend_profiles_v1",
                category: "composite_alpha",
                phase: "7-B/7-J",
                dependencies: &["multi_factor_value"],
                combo_method,
                experiment_type: "phase7_factor_backfill_profile",
                source_combos: sources,
            });
        }

        if plans.is_empty() {
            return Err("no alpha blend profiles selected".to_string());
        }

        Ok(plans)
    }
}


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

fn p42b_large_cap_momentum_reversal_specs() -> Vec<Phase7BackfillFactorSpec> {
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
const P42B_DEFENSIVE_INDUSTRIES: &[&str] = &[
    "保险",
    "银行",
    "白酒",
    "黄金",
    "机场",
    "电信运营",
    "啤酒",
];

fn p42b_defensive_low_vol_quality_specs() -> Vec<Phase7BackfillFactorSpec> {
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

pub(crate) fn phase7_earnings_recovery_persistence_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn event_window_bundle_name(combo_name: &str) -> &'static str {
    match combo_name {
        "phase7_event_window_earnings_10d_v1" => "phase7_event_window_earnings_10d_v1",
        "phase7_event_window_earnings_40d_v1" => "phase7_event_window_earnings_40d_v1",
        "phase7_event_post_return_curve_20d_v1" => "phase7_event_post_return_curve_20d_v1",
        "phase7_event_reaction_segments_20d_v1" => "phase7_event_reaction_segments_20d_v1",
        "phase7_event_reaction_reversal_20d_v1" => "phase7_event_reaction_reversal_20d_v1",
        _ => "phase7_event_window_earnings_v1",
    }
}

fn event_window_phase(combo_name: &str) -> &'static str {
    match combo_name {
        "phase7_event_window_earnings_10d_v1" => "7-AZ10",
        "phase7_event_window_earnings_40d_v1" => "7-AZ40",
        "phase7_event_post_return_curve_20d_v1" => "7-FB",
        "phase7_event_reaction_segments_20d_v1" => "7-FC",
        "phase7_event_reaction_reversal_20d_v1" => "7-FC",
        _ => "7-Y2",
    }
}

fn event_window_combo_method(combo_name: &str) -> &'static str {
    if is_event_post_return_curve_combo(combo_name)
        || is_event_reaction_segments_combo(combo_name)
        || is_event_reaction_reversal_combo(combo_name)
    {
        "weighted_event_post_return_curve"
    } else {
        "weighted_event_window_earnings"
    }
}

fn event_window_dependencies(combo_name: &str) -> &'static [&'static str] {
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

pub async fn backfill_phase7_price_volume_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7PriceVolumeBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_price_volume_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_price_volume_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 price-volume backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 price-volume backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 price-volume backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/p42b-large-cap-momentum-reversal-backfill/background
///
/// Set-based daily backfill for the P4.2b large-cap momentum-reversal
/// interaction factor (`large_cap_mom_rev_daily_std`): within the large-cap
/// pool (total_mv > threshold), CUME_DIST(reversal) x CUME_DIST(momentum)
/// interaction, cross-sectional percent_rank normalization.
pub async fn backfill_p42b_large_cap_momentum_reversal_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<P42bLargeCapMomentumReversalBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create p42b large-cap momentum-reversal backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_p42b_large_cap_momentum_reversal_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = p42b_large_cap_momentum_reversal_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist P4.2b large-cap momentum-reversal backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "P4.2b large-cap momentum-reversal backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "P4.2b large-cap momentum-reversal backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

#[derive(Debug, serde::Deserialize)]

pub struct P42bLargeCapMomentumReversalBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

impl P42bLargeCapMomentumReversalBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<SetBasedFactorBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "p42b_large_cap_mom_rev_daily_std",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(SetBasedFactorBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "p42b_large_cap_momentum_reversal_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "p42b_large_cap_mom_rev_daily_std",
            category: "price_volume",
            phase: "P4.2b",
            dependencies: &["market_stock_daily_bar_adj", "market_stock_daily_basic"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

/// POST /api/v1/quant/factors/p42b-defensive-low-vol-quality-backfill/background
///
/// Set-based daily backfill for the P4.2b defensive low-volatility quality
/// interaction factor (`defensive_lowvol_quality_daily_std`): within the
/// defensive industry pool, CUME_DIST(-volatility) x CUME_DIST(fin_roe)
/// interaction, cross-sectional percent_rank normalization.

pub async fn backfill_p42b_defensive_low_vol_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<P42bDefensiveLowVolQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create p42b defensive low-vol quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_p42b_defensive_low_vol_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = p42b_defensive_low_vol_quality_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist P4.2b defensive low-vol quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "P4.2b defensive low-vol quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "P4.2b defensive low-vol quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

#[derive(Debug, serde::Deserialize)]

pub struct P42bDefensiveLowVolQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

impl P42bDefensiveLowVolQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<SetBasedFactorBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "defensive_lowvol_quality_daily_std",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(SetBasedFactorBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "p42b_defensive_low_vol_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "defensive_lowvol_quality_daily_std",
            category: "price_volume",
            phase: "P4.2b",
            dependencies: &["market_stock_daily_bar_adj", "market_stock", "factor_value"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

/// POST /api/v1/quant/factors/phase7-financial-quality-backfill/background
///
/// Set-based daily PIT carry-forward backfill for the Phase 7 financial
/// quality alpha bundle and its equal-weight combo score.

pub async fn backfill_phase7_financial_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FinancialQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 financial quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_financial_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_financial_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 financial quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 financial quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 financial quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-financial-quality-change-backfill/background
///
/// Set-based daily PIT backfill for financial quality YoY acceleration sources.
pub async fn backfill_phase7_financial_quality_change_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FinancialQualityChangeBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 financial quality change backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_financial_quality_change_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_financial_quality_change_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 financial quality change backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 financial quality change backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 financial quality change backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-earnings-recovery-persistence-backfill/background
///
/// Set-based daily PIT backfill for multi-period earnings recovery persistence sources.
pub async fn backfill_phase7_earnings_recovery_persistence_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EarningsRecoveryPersistenceBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 earnings recovery persistence backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_earnings_recovery_persistence_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_earnings_recovery_persistence_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 earnings recovery persistence backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 earnings recovery persistence backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 earnings recovery persistence backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-industry-residual-quality-backfill/background
///
/// Set-based daily PIT financial quality backfill that removes same-industry
/// mean exposure before ranking, so discovery can test quality alpha beyond
/// broad industry tilts.
pub async fn backfill_phase7_industry_residual_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7IndustryResidualQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 industry-residual quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_industry_residual_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_industry_residual_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 industry-residual quality profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 industry-residual quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 industry-residual quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-relative-strength-backfill/background
///
/// Set-based market/industry-relative momentum backfill for expanding Phase 7
/// alpha sources beyond absolute price-volume and financial quality signals.
pub async fn backfill_phase7_relative_strength_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7RelativeStrengthBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 relative strength backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_relative_strength_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_relative_strength_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 relative strength backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 relative strength backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 relative strength backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-quality-relative-strength-backfill/background
///
/// Combo-only backfill for the Phase 7 composite alpha candidate that blends
/// daily PIT financial quality with market/industry relative strength.
pub async fn backfill_phase7_quality_relative_strength_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7QualityRelativeStrengthBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 quality-relative-strength backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_quality_relative_strength_combo_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_quality_relative_strength_combo_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 quality-relative-strength profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 quality-relative-strength combo backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 quality-relative-strength backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-growth-recovery-backfill/background
///
/// Set-based daily PIT backfill for non-momentum fundamental growth and
/// earnings-recovery alpha sources.
pub async fn backfill_phase7_growth_recovery_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7GrowthRecoveryBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 growth recovery backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_growth_recovery_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_growth_recovery_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 growth-recovery backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 growth-recovery backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 growth-recovery backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-valuation-backfill/background
///
/// Set-based daily valuation alpha backfill from `market_stock_daily_basic`.
pub async fn backfill_phase7_valuation_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ValuationBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 valuation backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_valuation_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_valuation_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 valuation backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 valuation backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 valuation backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-moneyflow-backfill/background
///
/// Set-based rolling moneyflow alpha backfill from `market_stock_moneyflow`.
pub async fn backfill_phase7_moneyflow_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MoneyflowBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 moneyflow backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_moneyflow_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_moneyflow_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 moneyflow backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 moneyflow backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 moneyflow backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-moneyflow-congestion-backfill/background
///
/// Set-based rolling moneyflow alpha backfill adjusted by capacity crowding.
pub async fn backfill_phase7_moneyflow_congestion_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MoneyflowCongestionBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 moneyflow congestion backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_moneyflow_congestion_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_moneyflow_congestion_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 moneyflow congestion backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 moneyflow congestion backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 moneyflow congestion backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-cashflow-quality-backfill/background
///
/// Set-based PIT cashflow quality alpha backfill from `market_stock_cashflow`.
pub async fn backfill_phase7_cashflow_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7CashflowQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 cashflow quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_cashflow_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_cashflow_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 cashflow quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 cashflow quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 cashflow quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-dividend-quality-backfill/background
///
/// Set-based PIT dividend quality alpha backfill from `market_stock_dividend`.
pub async fn backfill_phase7_dividend_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7DividendQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 dividend quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_dividend_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_dividend_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 dividend quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 dividend quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 dividend quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-alpha-backfill/background
///
/// Set-based PIT event alpha backfill from forecast, express, and disclosure
/// date event tables.
pub async fn backfill_phase7_event_alpha_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventAlphaBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event alpha backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_alpha_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_alpha_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event alpha backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event alpha backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event alpha backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-surprise-backfill/background
///
/// Set-based PIT event surprise backfill. This keeps the ordinary-permission
/// forecast, express, and disclosure data path, but turns available fields into
/// bucketed/nonlinear event surprise factors.
pub async fn backfill_phase7_event_surprise_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventSurpriseBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event surprise backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_surprise_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_surprise_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event surprise backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event surprise backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event surprise backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-forecast-revision-surprise-backfill/background
///
/// Set-based PIT forecast revision surprise backfill. This source uses only
/// same-symbol/same-period forecast revisions that were both visible by the
/// signal date.
pub async fn backfill_phase7_forecast_revision_surprise_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ForecastRevisionSurpriseBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 forecast revision surprise backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_forecast_revision_surprise_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_forecast_revision_surprise_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 forecast revision surprise backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 forecast revision surprise backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 forecast revision surprise backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-repurchase-supply-shock-backfill/background
///
/// Set-based PIT supply-demand shock alpha backfill from repurchase announcements.
pub async fn backfill_phase7_repurchase_supply_shock_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7RepurchaseSupplyShockBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 repurchase supply shock backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_repurchase_supply_shock_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_repurchase_supply_shock_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 repurchase supply shock backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 repurchase supply shock backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 repurchase supply shock backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-block-trade-supply-demand-backfill/background
///
/// Set-based PIT supply-demand alpha backfill from block-trade disclosures.
pub async fn backfill_phase7_block_trade_supply_demand_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7BlockTradeSupplyDemandBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 block-trade supply-demand backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_block_trade_supply_demand_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_block_trade_supply_demand_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 block-trade supply-demand backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 block-trade supply-demand backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 block-trade supply-demand backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-unlock-supply-pressure-backfill/background
///
/// Set-based PIT unlock pressure alpha backfill from share_float announcements.
pub async fn backfill_phase7_unlock_supply_pressure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7UnlockSupplyPressureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 unlock supply pressure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_unlock_supply_pressure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_unlock_supply_pressure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 unlock supply pressure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 unlock supply pressure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 unlock supply pressure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-supply-float-shock-backfill/background
///
/// Set-based PIT broad supply proxy from daily float/total market value and
/// unadjusted close. This approximates share-base changes without using static
/// industry membership or future corporate-action knowledge.
pub async fn backfill_phase7_supply_float_shock_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7SupplyFloatShockBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 supply float shock backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_supply_float_shock_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_supply_float_shock_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 supply float shock backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 supply float shock backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 supply float shock backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-liquidity-quality-backfill/background
///
/// Set-based PIT broad-base liquidity-quality alpha from daily price, traded
/// amount, and same-day float market value snapshots.
pub async fn backfill_phase7_liquidity_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7LiquidityQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 liquidity quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_liquidity_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_liquidity_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 liquidity quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 liquidity quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 liquidity quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-market-residual-risk-backfill/background
///
/// Set-based PIT broad-base market beta and residual-risk alpha from stock
/// daily returns and same-day CSI 300 index returns.
pub async fn backfill_phase7_market_residual_risk_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MarketResidualRiskBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 market residual risk backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_market_residual_risk_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_market_residual_risk_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 market residual risk backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 market residual risk backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 market residual risk backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-industry-prosperity-backfill/background
///
/// Dedicated PIT market-scope industry prosperity proxy. It only runs with the
/// alpha admission gate and uses PIT industry membership to restrict evaluation
/// to the main-board + ChiNext scope approved by coverage audit.
pub async fn backfill_phase7_industry_prosperity_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7IndustryProsperityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 industry prosperity backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_industry_prosperity_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_industry_prosperity_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 industry prosperity backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 industry prosperity backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 industry prosperity backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-futures-price-chain-backfill/background
///
/// Dedicated PIT futures price-chain factor builder. It requires the coverage
/// admission gate and only writes research-source factor/multi-factor rows for
/// P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_futures_price_chain_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FuturesPriceChainBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 futures price-chain backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_futures_price_chain_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_futures_price_chain_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 futures price-chain backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 futures price-chain backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 futures price-chain backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-equity-pledge-pressure-backfill/background
///
/// Dedicated PIT equity pledge pressure factor builder. It requires the raw
/// coverage admission gate and only writes a research-source combo for P3.10
/// diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_equity_pledge_pressure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EquityPledgePressureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 equity pledge pressure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_equity_pledge_pressure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_equity_pledge_pressure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 equity pledge pressure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 equity pledge pressure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 equity pledge pressure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-window-alpha-backfill/background
///
/// Set-based PIT event-window alpha backfill from forecast, express, and
/// disclosure-date event tables. Unlike latest-event carry-forward, this keeps
/// each event signal alive only for a decayed post-event window.
pub async fn backfill_phase7_event_window_alpha_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventWindowAlphaBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event-window alpha backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_window_alpha_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_window_alpha_backfill_specs_for_plan(&task_plan);
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event-window alpha backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event-window alpha backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event-window alpha backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-shareholder-structure-backfill/background
///
/// Dedicated PIT shareholder-structure low-fanout factor builder. It requires
/// the strict low-fanout admission gate and writes only a research-source combo
/// for P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_shareholder_structure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ShareholderStructureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 shareholder structure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_shareholder_structure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_shareholder_structure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 shareholder structure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 shareholder structure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 shareholder structure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-margin-detail-backfill/background
///
/// Dedicated PIT margin-detail leverage-crowding factor builder. It requires
/// the margin-detail coverage gate and writes only a research-source combo for
/// P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_margin_detail_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MarginDetailBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 margin detail backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_margin_detail_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_margin_detail_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 margin detail backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 margin detail backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 margin detail backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-analyst-revision-backfill/background
///
/// Dedicated PIT AkShare/multi-vendor analyst-revision factor builder. It
/// requires the full-history coverage/PIT/correlation admission gate and writes
/// only a research-source combo for P3.10 diagnostics; WFA and v19 train
/// selection remain separate gates.
pub async fn backfill_phase7_analyst_revision_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AnalystRevisionBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 analyst revision backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_analyst_revision_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_analyst_revision_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 analyst revision backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 analyst revision backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 analyst revision backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-alpha-blend-backfill/background
///
/// Combo-only backfill that blends existing multi-factor alpha scores by
/// explicit source weights.
pub async fn backfill_phase7_alpha_blend_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AlphaBlendBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 alpha blend backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_alpha_blend_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &[],
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 alpha blend profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 alpha blend backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 alpha blend backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
            "sources": plan.source_combos,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-alpha-blend-profiles-backfill/background
///
/// Batch backfill the canonical Phase 7 alpha blend weight profiles so the
/// optimizer can search across valuation/quality/growth/recovery/relative
/// strength weight mixes as normal factor combos.
pub async fn backfill_phase7_alpha_blend_profiles_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AlphaBlendProfilesBackfillRequest>,
) -> impl IntoResponse {
    let plans = match req.into_plans() {
        Ok(plans) => plans,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();
    let first_plan = plans.first().expect("plans checked non-empty");
    let last_plan = plans.last().expect("plans checked non-empty");
    let total_steps = alpha_blend_profile_backfill_total_steps(&plans);

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', $6, 0, 0, 0, now(), $7, now())",
    )
    .bind(&task_id)
    .bind(first_plan.task_type)
    .bind(first_plan.source)
    .bind(first_plan.start_date)
    .bind(first_plan.end_date)
    .bind(usize_to_i32(total_steps))
    .bind(first_plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 alpha blend profiles backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plans = plans.clone();

    tokio::spawn(async move {
        let result = run_phase7_alpha_blend_profiles_backfill(&state.db, &tid, &task_plans).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 alpha blend profiles backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 alpha blend profiles backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": first_plan.task_type,
            "version": first_plan.version,
            "start_date": first_plan.start_date,
            "end_date": last_plan.end_date,
            "profiles": plans.iter().map(|plan| json!({
                "combo_name": plan.combo_name,
                "version": plan.version,
                "sources": plan.source_combos,
            })).collect::<Vec<_>>(),
        }
    }))
}


async fn run_phase7_price_volume_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7PriceVolumeBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_price_volume_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_p42b_large_cap_momentum_reversal_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let specs = p42b_large_cap_momentum_reversal_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_p42b_defensive_low_vol_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let specs = p42b_defensive_low_vol_quality_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_financial_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7FinancialQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_financial_quality_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_financial_quality_change_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7FinancialQualityChangeBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_financial_quality_change_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_earnings_recovery_persistence_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EarningsRecoveryPersistenceBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_earnings_recovery_persistence_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_industry_residual_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7IndustryResidualQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_industry_residual_quality_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_relative_strength_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7RelativeStrengthBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_relative_strength_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_quality_relative_strength_combo_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7QualityRelativeStrengthBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let started_at = Instant::now();
    let specs = phase7_quality_relative_strength_combo_specs();

    if factor_backfill_cancel_requested(db, task_id).await? {
        let report = SetBasedFactorBackfillReport {
            factor_rows: 0,
            combo_rows: 0,
            factor_rows_by_code: Vec::new(),
        };
        return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
            report,
            elapsed_millis(started_at),
        ));
    }

    let combo_rows = execute_set_based_combo_backfill(db, &specs, plan).await?;
    update_factor_backfill_progress(db, task_id, 1, 1, combo_rows).await?;

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: Vec::new(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_phase7_growth_recovery_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7GrowthRecoveryBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_growth_recovery_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_valuation_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7ValuationBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_valuation_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_moneyflow_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7MoneyflowBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_moneyflow_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_moneyflow_congestion_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7MoneyflowCongestionBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_moneyflow_congestion_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_cashflow_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7CashflowQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_cashflow_quality_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_dividend_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7DividendQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_dividend_quality_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_event_alpha_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EventAlphaBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_event_alpha_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_event_surprise_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EventSurpriseBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_event_surprise_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_forecast_revision_surprise_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7ForecastRevisionSurpriseBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_forecast_revision_surprise_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_repurchase_supply_shock_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7RepurchaseSupplyShockBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_repurchase_supply_shock_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_block_trade_supply_demand_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7BlockTradeSupplyDemandBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_block_trade_supply_demand_backfill_specs();
    run_segmented_set_based_factor_backfill(db, task_id, plan, &specs, phase7_factor_backfill_sql)
        .await
}

async fn run_phase7_unlock_supply_pressure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7UnlockSupplyPressureBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_unlock_supply_pressure_backfill_specs();
    run_segmented_set_based_factor_backfill(db, task_id, plan, &specs, phase7_factor_backfill_sql)
        .await
}

async fn run_phase7_supply_float_shock_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7SupplyFloatShockBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_supply_float_shock_backfill_specs();
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_liquidity_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7LiquidityQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_liquidity_quality_backfill_specs();
    run_segmented_set_based_factor_backfill(db, task_id, plan, &specs, phase7_factor_backfill_sql)
        .await
}

async fn run_phase7_market_residual_risk_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7MarketResidualRiskBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_market_residual_risk_backfill_specs();
    run_segmented_set_based_factor_backfill(db, task_id, plan, &specs, phase7_factor_backfill_sql)
        .await
}

async fn run_phase7_industry_prosperity_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7IndustryProsperityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_industry_prosperity_backfill_specs();
    run_segmented_industry_prosperity_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_futures_price_chain_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7FuturesPriceChainBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    ensure_futures_price_chain_factor_readiness(db).await?;
    let specs = phase7_futures_price_chain_backfill_specs();
    run_segmented_futures_price_chain_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_equity_pledge_pressure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EquityPledgePressureBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_equity_pledge_pressure_backfill_specs();
    run_segmented_equity_pledge_pressure_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_shareholder_structure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7ShareholderStructureBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_shareholder_structure_backfill_specs();
    run_segmented_shareholder_structure_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_margin_detail_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7MarginDetailBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_margin_detail_backfill_specs();
    run_segmented_margin_detail_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_analyst_revision_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7AnalystRevisionBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_analyst_revision_backfill_specs();
    run_segmented_analyst_revision_backfill(db, task_id, plan, &specs).await
}

async fn run_phase7_event_window_alpha_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EventWindowAlphaBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_event_window_alpha_backfill_specs_for_plan(plan);
    let job = SetBasedFactorBackfillJob::new(plan, &specs, phase7_factor_backfill_sql);
    run_set_based_factor_backfill(db, task_id, job).await
}

async fn run_phase7_alpha_blend_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7AlphaBlendBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let started_at = Instant::now();

    if factor_backfill_cancel_requested(db, task_id).await? {
        let report = SetBasedFactorBackfillReport {
            factor_rows: 0,
            combo_rows: 0,
            factor_rows_by_code: Vec::new(),
        };
        return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
            report,
            elapsed_millis(started_at),
        ));
    }

    let combo_rows = execute_phase7_alpha_blend_backfill(db, plan).await?;
    update_factor_backfill_progress(db, task_id, 1, 1, combo_rows).await?;

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: Vec::new(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_phase7_alpha_blend_profiles_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plans: &[Phase7AlphaBlendBackfillPlan],
) -> Result<Phase7BackfillCompletion, String> {
    let started_at = Instant::now();
    let total_steps = alpha_blend_profile_backfill_total_steps(plans).max(1);
    let mut completed_steps = 0usize;
    let mut combo_rows = 0usize;

    for plan in plans {
        for (segment_start, segment_end) in
            quarterly_backfill_segments(plan.start_date, plan.end_date)
        {
            if factor_backfill_cancel_requested(db, task_id).await? {
                let report = SetBasedFactorBackfillReport {
                    factor_rows: 0,
                    combo_rows,
                    factor_rows_by_code: Vec::new(),
                };
                return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                    report,
                    elapsed_millis(started_at),
                ));
            }

            let segment_plan = segmented_backfill_plan(plan, segment_start, segment_end);
            let rows = execute_phase7_alpha_blend_backfill(db, &segment_plan).await?;
            combo_rows = combo_rows.saturating_add(rows);
            completed_steps = completed_steps.saturating_add(1);
            update_factor_backfill_progress(db, task_id, completed_steps, total_steps, combo_rows)
                .await?;
        }
    }

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: Vec::new(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}


pub(crate) fn alpha_blend_profile_backfill_total_steps(plans: &[Phase7AlphaBlendBackfillPlan]) -> usize {
    plans
        .iter()
        .map(|plan| quarterly_backfill_segments(plan.start_date, plan.end_date).len())
        .sum()
}


async fn run_set_based_factor_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    job: SetBasedFactorBackfillJob<'_>,
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let total_steps = job.total_steps();
    let mut factor_rows = 0usize;
    let mut factor_rows_by_code = Vec::with_capacity(job.specs.len());

    for (index, spec) in job.specs.iter().enumerate() {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows,
                combo_rows: 0,
                factor_rows_by_code,
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }
        upsert_set_based_factor_definition(db, spec, job.plan).await?;
        let rows = execute_set_based_factor_backfill(db, spec, job.plan, job.factor_sql).await?;
        factor_rows = factor_rows.saturating_add(rows);
        factor_rows_by_code.push((spec.factor_code.to_string(), rows));
        update_factor_backfill_progress(db, task_id, index + 1, total_steps, factor_rows).await?;
    }

    if factor_backfill_cancel_requested(db, task_id).await? {
        let report = SetBasedFactorBackfillReport {
            factor_rows,
            combo_rows: 0,
            factor_rows_by_code,
        };
        return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
            report,
            elapsed_millis(started_at),
        ));
    }

    let combo_rows = execute_set_based_combo_backfill(db, job.specs, job.plan).await?;
    update_factor_backfill_progress(
        db,
        task_id,
        total_steps,
        total_steps,
        factor_rows.saturating_add(combo_rows),
    )
    .await?;

    let report = SetBasedFactorBackfillReport {
        factor_rows,
        combo_rows,
        factor_rows_by_code,
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}


pub(crate) fn yearly_backfill_segments(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    if start > end {
        return Vec::new();
    }

    let mut segments = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let year_end = NaiveDate::from_ymd_opt(cursor.year(), 12, 31).expect("valid year end");
        let segment_end = year_end.min(end);
        segments.push((cursor, segment_end));
        if segment_end == end {
            break;
        }
        cursor = NaiveDate::from_ymd_opt(cursor.year() + 1, 1, 1).expect("valid next year start");
    }
    segments
}

fn quarter_end_date(date: NaiveDate) -> NaiveDate {
    let quarter_end_month = ((date.month() - 1) / 3 + 1) * 3;
    let next_month_start = if quarter_end_month == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).expect("valid next year start")
    } else {
        NaiveDate::from_ymd_opt(date.year(), quarter_end_month + 1, 1)
            .expect("valid next quarter month start")
    };
    next_month_start
        .pred_opt()
        .expect("quarter end must have a previous day")
}

fn next_quarter_start(date: NaiveDate) -> NaiveDate {
    let quarter_end_month = ((date.month() - 1) / 3 + 1) * 3;
    if quarter_end_month == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).expect("valid next year start")
    } else {
        NaiveDate::from_ymd_opt(date.year(), quarter_end_month + 1, 1)
            .expect("valid next quarter start")
    }
}

pub(crate) fn quarterly_backfill_segments(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    if start > end {
        return Vec::new();
    }

    let mut segments = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let segment_end = quarter_end_date(cursor).min(end);
        segments.push((cursor, segment_end));
        if segment_end == end {
            break;
        }
        cursor = next_quarter_start(cursor);
    }
    segments
}

pub(crate) fn margin_detail_backfill_segments(
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<(NaiveDate, NaiveDate)> {
    quarterly_backfill_segments(start, end)
}

pub(crate) fn monthly_backfill_segments(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    if start > end {
        return Vec::new();
    }

    let mut segments = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let next_month_start = if cursor.month() == 12 {
            NaiveDate::from_ymd_opt(cursor.year() + 1, 1, 1).expect("valid next year start")
        } else {
            NaiveDate::from_ymd_opt(cursor.year(), cursor.month() + 1, 1)
                .expect("valid next month start")
        };
        let segment_end = next_month_start
            .pred_opt()
            .expect("month start must have previous day")
            .min(end);
        segments.push((cursor, segment_end));
        if segment_end == end {
            break;
        }
        cursor = next_month_start;
    }
    segments
}

fn segmented_backfill_plan(
    plan: &SetBasedFactorBackfillPlan,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> SetBasedFactorBackfillPlan {
    let mut segment_plan = plan.clone();
    segment_plan.start_date = start_date;
    segment_plan.end_date = end_date;
    segment_plan
}


async fn run_segmented_set_based_factor_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
    factor_sql: SetBasedFactorSqlBuilder,
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = quarterly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = specs
        .len()
        .saturating_mul(segments.len())
        .saturating_add(segments.len());
    let mut completed_steps = 0usize;
    let mut factor_rows = 0usize;
    let mut combo_rows = 0usize;
    let mut factor_rows_by_code = Vec::with_capacity(specs.len());

    for spec in specs {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows,
                combo_rows,
                factor_rows_by_code,
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }
        upsert_set_based_factor_definition(db, spec, plan).await?;

        let mut spec_rows = 0usize;
        for (segment_start, segment_end) in &segments {
            if factor_backfill_cancel_requested(db, task_id).await? {
                factor_rows_by_code.push((spec.factor_code.to_string(), spec_rows));
                let report = SetBasedFactorBackfillReport {
                    factor_rows,
                    combo_rows,
                    factor_rows_by_code,
                };
                return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                    report,
                    elapsed_millis(started_at),
                ));
            }

            let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
            let rows =
                execute_set_based_factor_backfill(db, spec, &segment_plan, factor_sql).await?;
            spec_rows = spec_rows.saturating_add(rows);
            factor_rows = factor_rows.saturating_add(rows);
            completed_steps = completed_steps.saturating_add(1);
            update_factor_backfill_progress(db, task_id, completed_steps, total_steps, factor_rows)
                .await?;
        }
        factor_rows_by_code.push((spec.factor_code.to_string(), spec_rows));
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows,
                combo_rows,
                factor_rows_by_code,
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_set_based_combo_backfill(db, specs, &segment_plan).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(
            db,
            task_id,
            completed_steps,
            total_steps,
            factor_rows.saturating_add(combo_rows),
        )
        .await?;
    }

    let report = SetBasedFactorBackfillReport {
        factor_rows,
        combo_rows,
        factor_rows_by_code,
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_industry_prosperity_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = quarterly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len().saturating_mul(2);
    let mut completed_steps = 0usize;
    let mut factor_rows = 0usize;
    let mut combo_rows = 0usize;
    let mut factor_row_totals: HashMap<String, usize> = HashMap::new();

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
        factor_row_totals.insert(spec.factor_code.to_string(), 0);
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report =
                industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_industry_prosperity_factor_backfill(db, &segment_plan).await?;
        for (factor_code, row_count) in rows {
            *factor_row_totals.entry(factor_code).or_insert(0) += row_count;
            factor_rows = factor_rows.saturating_add(row_count);
        }
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(db, task_id, completed_steps, total_steps, factor_rows)
            .await?;
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report =
                industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_set_based_combo_backfill(db, specs, &segment_plan).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(
            db,
            task_id,
            completed_steps,
            total_steps,
            factor_rows.saturating_add(combo_rows),
        )
        .await?;
    }

    let report = industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_analyst_revision_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = quarterly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len().saturating_mul(2);
    let mut completed_steps = 0usize;
    let mut factor_rows = 0usize;
    let mut combo_rows = 0usize;
    let mut factor_row_totals: HashMap<String, usize> = HashMap::new();

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
        factor_row_totals.insert(spec.factor_code.to_string(), 0);
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report =
                industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_analyst_revision_factor_backfill(db, &segment_plan).await?;
        for (factor_code, row_count) in rows {
            *factor_row_totals.entry(factor_code).or_insert(0) += row_count;
            factor_rows = factor_rows.saturating_add(row_count);
        }
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(db, task_id, completed_steps, total_steps, factor_rows)
            .await?;
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report =
                industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_set_based_combo_backfill(db, specs, &segment_plan).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(
            db,
            task_id,
            completed_steps,
            total_steps,
            factor_rows.saturating_add(combo_rows),
        )
        .await?;
    }

    let report = industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_futures_price_chain_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = quarterly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len();
    let mut completed_steps = 0usize;
    let mut factor_rows = 0usize;
    let mut combo_rows = 0usize;
    let mut factor_row_totals: HashMap<String, usize> = HashMap::new();

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
        factor_row_totals.insert(spec.factor_code.to_string(), 0);
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report =
                industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_futures_price_chain_factor_backfill(db, &segment_plan, specs).await?;
        for (factor_code, row_count) in rows.product_rows_by_code {
            *factor_row_totals.entry(factor_code).or_insert(0) += row_count;
            factor_rows = factor_rows.saturating_add(row_count);
        }
        combo_rows = combo_rows.saturating_add(rows.combo_rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(
            db,
            task_id,
            completed_steps,
            total_steps,
            factor_rows.saturating_add(combo_rows),
        )
        .await?;
    }

    let report = industry_prosperity_report(factor_rows, combo_rows, &factor_row_totals, specs);
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_equity_pledge_pressure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = monthly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len();
    let mut completed_steps = 0usize;
    let mut combo_rows = 0usize;

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows: 0,
                combo_rows,
                factor_rows_by_code: specs
                    .iter()
                    .map(|spec| (spec.factor_code.to_string(), 0))
                    .collect(),
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_equity_pledge_pressure_backfill(db, &segment_plan, specs).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(db, task_id, completed_steps, total_steps, combo_rows)
            .await?;
    }

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: specs
            .iter()
            .map(|spec| (spec.factor_code.to_string(), 0))
            .collect(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_shareholder_structure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = yearly_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len();
    let mut completed_steps = 0usize;
    let mut combo_rows = 0usize;

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows: 0,
                combo_rows,
                factor_rows_by_code: specs
                    .iter()
                    .map(|spec| (spec.factor_code.to_string(), 0))
                    .collect(),
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_shareholder_structure_backfill(db, &segment_plan, specs).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(db, task_id, completed_steps, total_steps, combo_rows)
            .await?;
    }

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: specs
            .iter()
            .map(|spec| (spec.factor_code.to_string(), 0))
            .collect(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}

async fn run_segmented_margin_detail_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<SetBasedFactorBackfillCompletion, String> {
    let started_at = Instant::now();
    let segments = margin_detail_backfill_segments(plan.start_date, plan.end_date);
    let total_steps = segments.len();
    let mut completed_steps = 0usize;
    let mut combo_rows = 0usize;

    for spec in specs {
        upsert_set_based_factor_definition(db, spec, plan).await?;
    }

    for (segment_start, segment_end) in &segments {
        if factor_backfill_cancel_requested(db, task_id).await? {
            let report = SetBasedFactorBackfillReport {
                factor_rows: 0,
                combo_rows,
                factor_rows_by_code: specs
                    .iter()
                    .map(|spec| (spec.factor_code.to_string(), 0))
                    .collect(),
            };
            return Ok(SetBasedFactorBackfillCompletion::cancelled_with(
                report,
                elapsed_millis(started_at),
            ));
        }

        let segment_plan = segmented_backfill_plan(plan, *segment_start, *segment_end);
        let rows = execute_margin_detail_backfill(db, &segment_plan, specs).await?;
        combo_rows = combo_rows.saturating_add(rows);
        completed_steps = completed_steps.saturating_add(1);
        update_factor_backfill_progress(db, task_id, completed_steps, total_steps, combo_rows)
            .await?;
    }

    let report = SetBasedFactorBackfillReport {
        factor_rows: 0,
        combo_rows,
        factor_rows_by_code: specs
            .iter()
            .map(|spec| (spec.factor_code.to_string(), 0))
            .collect(),
    };
    Ok(SetBasedFactorBackfillCompletion::completed_with(
        report,
        elapsed_millis(started_at),
    ))
}


pub(crate) struct FuturesPriceChainSegmentBackfillRows {
    product_rows_by_code: Vec<(String, usize)>,
    combo_rows: usize,
}

fn industry_prosperity_report(
    factor_rows: usize,
    combo_rows: usize,
    factor_row_totals: &HashMap<String, usize>,
    specs: &[SetBasedFactorSpec],
) -> SetBasedFactorBackfillReport {
    let factor_rows_by_code = specs
        .iter()
        .map(|spec| {
            (
                spec.factor_code.to_string(),
                *factor_row_totals.get(spec.factor_code).unwrap_or(&0),
            )
        })
        .collect();
    SetBasedFactorBackfillReport {
        factor_rows,
        combo_rows,
        factor_rows_by_code,
    }
}

async fn execute_industry_prosperity_factor_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
) -> Result<Vec<(String, usize)>, String> {
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start industry prosperity backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    let rows: Vec<(String, i64)> = sqlx::query_as(&phase7_industry_prosperity_multi_backfill_sql())
        .bind("ind_pros_ret_mom_20v120_std")
        .bind("ind_pros_breadth_60d_std")
        .bind("ind_pros_amount_trend_20v120_std")
        .bind(&plan.version)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill industry prosperity factors for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit industry prosperity backfill transaction: {}",
            error
        )
    })?;

    Ok(rows
        .into_iter()
        .map(|(factor_code, row_count)| (factor_code, row_count.max(0) as usize))
        .collect())
}

async fn execute_analyst_revision_factor_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
) -> Result<Vec<(String, usize)>, String> {
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start analyst revision backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    let rows: Vec<(String, i64)> = sqlx::query_as(&phase7_analyst_revision_multi_backfill_sql())
        .bind("ar_rating_change_net_20d_std")
        .bind("ar_upgrade_event_20d_std")
        .bind("ar_downgrade_pressure_20d_std")
        .bind("ar_bullish_first_rating_60d_std")
        .bind(&plan.version)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill analyst revision factors for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit analyst revision backfill transaction: {}",
            error
        )
    })?;

    Ok(rows
        .into_iter()
        .map(|(factor_code, row_count)| (factor_code, row_count.max(0) as usize))
        .collect())
}

async fn execute_futures_price_chain_factor_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<FuturesPriceChainSegmentBackfillRows, String> {
    let weights_json = factor_backfill_combo_weights_json(specs)?;
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start futures price-chain backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    let product_rows: Vec<(String, i64)> =
        sqlx::query_as(phase7_futures_price_chain_product_signal_backfill_sql())
            .bind("fpc_price_mom_20v60_std")
            .bind("fpc_inventory_tight_20v60_std")
            .bind("fpc_net_position_20v60_std")
            .bind(&plan.version)
            .bind(plan.start_date)
            .bind(plan.end_date)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                format!(
                    "Failed to backfill futures price-chain product signals for {}..{}: {}",
                    plan.start_date, plan.end_date, error
                )
            })?;
    tracing::info!(
        start_date = %plan.start_date,
        end_date = %plan.end_date,
        product_signal_rows = ?product_rows,
        "Futures price-chain product signals backfilled"
    );

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert futures price-chain combo weights: {}",
            error
        )
    })?;

    let combo_result = sqlx::query(phase7_futures_price_chain_combo_backfill_sql())
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&weights_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .bind(phase7_combo_required_factor_count(specs, plan))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill futures price-chain combo for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;
    let combo_rows = combo_result.rows_affected() as usize;
    tracing::info!(
        start_date = %plan.start_date,
        end_date = %plan.end_date,
        combo_rows,
        "Futures price-chain combo backfilled"
    );

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit futures price-chain backfill transaction: {}",
            error
        )
    })?;

    Ok(FuturesPriceChainSegmentBackfillRows {
        product_rows_by_code: product_rows
            .into_iter()
            .map(|(factor_code, row_count)| (factor_code, row_count.max(0) as usize))
            .collect(),
        combo_rows,
    })
}

async fn execute_equity_pledge_pressure_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<usize, String> {
    let weights_json = factor_backfill_combo_weights_json(specs)?;
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start equity pledge pressure backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert equity pledge pressure combo weights: {}",
            error
        )
    })?;

    let combo_result = sqlx::query(phase7_equity_pledge_pressure_backfill_sql())
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&weights_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill equity pledge pressure combo for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;
    let combo_rows = combo_result.rows_affected() as usize;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit equity pledge pressure backfill transaction: {}",
            error
        )
    })?;

    Ok(combo_rows)
}

async fn execute_shareholder_structure_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<usize, String> {
    let weights_json = factor_backfill_combo_weights_json(specs)?;
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start shareholder structure backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        format!(
            "Failed to upsert shareholder structure combo weights: {}",
            error
        )
    })?;

    let combo_result = sqlx::query(phase7_shareholder_structure_backfill_sql())
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&weights_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill shareholder structure combo for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;
    let combo_rows = combo_result.rows_affected() as usize;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit shareholder structure backfill transaction: {}",
            error
        )
    })?;

    Ok(combo_rows)
}

async fn execute_margin_detail_backfill(
    db: &sqlx::PgPool,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
) -> Result<usize, String> {
    let weights_json = factor_backfill_combo_weights_json(specs)?;
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start margin detail backfill transaction: {}",
            error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert margin detail combo weights: {}", error))?;

    let combo_result = sqlx::query(phase7_margin_detail_backfill_sql())
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&weights_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill margin detail combo for {}..{}: {}",
                plan.start_date, plan.end_date, error
            )
        })?;
    let combo_rows = combo_result.rows_affected() as usize;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit margin detail backfill transaction: {}",
            error
        )
    })?;

    Ok(combo_rows)
}


async fn ensure_futures_price_chain_factor_readiness(db: &sqlx::PgPool) -> Result<(), String> {
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        r#"
        WITH raw_symbol AS (
            SELECT
                upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
                available_at,
                trade_date
            FROM market_futures_daily
            WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
            UNION ALL
            SELECT
                upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
                available_at,
                trade_date
            FROM market_futures_warehouse_receipt
            WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
            UNION ALL
            SELECT
                upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
                available_at,
                trade_date
            FROM market_futures_holding_rank
            WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        ),
        raw AS (
            SELECT
                CASE
                    WHEN product_symbol_raw = 'PTA' THEN 'TA'
                    WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                    THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                    WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                    THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                    ELSE product_symbol_raw
                END AS product_symbol,
                available_at,
                trade_date
            FROM raw_symbol
        ),
        raw_products AS (
            SELECT DISTINCT product_symbol
            FROM raw
            WHERE product_symbol IS NOT NULL AND product_symbol <> ''
        ),
        covered_products AS (
            SELECT DISTINCT upper(product_symbol) AS product_symbol
            FROM market_futures_product_exposure_mapping_pit
            WHERE exposure_type = 'sw_industry'
            UNION
            SELECT DISTINCT upper(product_symbol) AS product_symbol
            FROM market_futures_product_exclusion_gate_pit
            WHERE gate_scope = 'futures_price_chain_factor'
        ),
        missing_products AS (
            SELECT raw_products.product_symbol
            FROM raw_products
            LEFT JOIN covered_products
              ON covered_products.product_symbol = raw_products.product_symbol
            WHERE covered_products.product_symbol IS NULL
        ),
        mapping_quality AS (
            SELECT COUNT(*)::int8 AS bad_rows
            FROM market_futures_product_exposure_mapping_pit
            WHERE exposure_type <> 'sw_industry'
               OR direction NOT IN (-1, 1)
               OR weight <= 0
               OR weight > 1
               OR available_at < valid_from
               OR (valid_to IS NOT NULL AND valid_to < valid_from)
               OR NULLIF(trim(source), '') IS NULL
               OR evidence IS NULL
               OR evidence = '{}'::jsonb
        ),
        exclusion_quality AS (
            SELECT COUNT(*)::int8 AS bad_rows
            FROM market_futures_product_exclusion_gate_pit
            WHERE gate_scope <> 'futures_price_chain_factor'
               OR available_at < valid_from
               OR (valid_to IS NOT NULL AND valid_to < valid_from)
               OR NULLIF(trim(source), '') IS NULL
               OR evidence IS NULL
               OR evidence = '{}'::jsonb
        )
        SELECT
            (SELECT COUNT(*)::int8 FROM raw) AS raw_rows,
            (SELECT COUNT(*)::int8 FROM raw_products) AS raw_product_count,
            (SELECT COUNT(*)::int8 FROM covered_products) AS covered_product_count,
            (SELECT COUNT(*)::int8 FROM missing_products) AS missing_product_count,
            (SELECT COUNT(*)::int8 FROM raw WHERE available_at < trade_date) AS raw_pit_violation_rows,
            (SELECT bad_rows FROM mapping_quality) AS mapping_bad_rows,
            (SELECT bad_rows FROM exclusion_quality) AS exclusion_bad_rows
        "#,
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to verify futures price-chain readiness: {error}"))?;

    let (
        raw_rows,
        raw_product_count,
        covered_product_count,
        missing_product_count,
        raw_pit_violation_rows,
        mapping_bad_rows,
        exclusion_bad_rows,
    ) = row;

    if raw_rows <= 0
        || raw_product_count <= 0
        || missing_product_count != 0
        || raw_pit_violation_rows != 0
        || mapping_bad_rows != 0
        || exclusion_bad_rows != 0
    {
        return Err(format!(
            "futures_price_chain coverage gate is not ready: raw_rows={}, raw_product_count={}, covered_product_count={}, missing_product_count={}, raw_pit_violation_rows={}, mapping_bad_rows={}, exclusion_bad_rows={}",
            raw_rows,
            raw_product_count,
            covered_product_count,
            missing_product_count,
            raw_pit_violation_rows,
            mapping_bad_rows,
            exclusion_bad_rows
        ));
    }

    Ok(())
}


async fn upsert_set_based_factor_definition(
    db: &sqlx::PgPool,
    spec: &SetBasedFactorSpec,
    plan: &SetBasedFactorBackfillPlan,
) -> Result<(), String> {
    let definition = FactorDefinitionInput {
        factor_code: spec.factor_code.to_string(),
        version: plan.version.clone(),
        name: spec.name.to_string(),
        category: plan.category.to_string(),
        frequency: "daily".to_string(),
        dependencies: json!(plan.dependencies),
        parameters: json!({
            "period": spec.period,
            "standardize": "daily_percent_rank",
            "execution": "set_based_sql",
            "phase": plan.phase,
            "bundle": plan.bundle_name
        }),
        status: "active".to_string(),
    };

    upsert_factor_definition_input(db, &definition)
        .await
        .map(|_| ())
        .map_err(|error| {
            format!(
                "Failed to upsert set-based factor definition {}@{}: {}",
                spec.factor_code, plan.version, error
            )
        })
}


async fn execute_set_based_factor_backfill(
    db: &sqlx::PgPool,
    spec: &SetBasedFactorSpec,
    plan: &SetBasedFactorBackfillPlan,
    factor_sql: SetBasedFactorSqlBuilder,
) -> Result<usize, String> {
    let mut tx = db.begin().await.map_err(|error| {
        format!(
            "Failed to start transaction for {} backfill: {}",
            spec.factor_code, error
        )
    })?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    let sql = factor_sql(spec);
    let result = sqlx::query(&sql)
        .bind(spec.factor_code)
        .bind(&plan.version)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to backfill {}: {}", spec.factor_code, error))?;

    tx.commit().await.map_err(|error| {
        format!(
            "Failed to commit {} backfill transaction: {}",
            spec.factor_code, error
        )
    })?;

    Ok(result.rows_affected() as usize)
}

async fn execute_set_based_combo_backfill(
    db: &sqlx::PgPool,
    specs: &[SetBasedFactorSpec],
    plan: &SetBasedFactorBackfillPlan,
) -> Result<usize, String> {
    let weights_json = factor_backfill_combo_weights_json(specs)?;
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to start combo backfill transaction: {}", error))?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert phase7 combo weights: {}", error))?;

    set_local_combo_backfill_planner(&mut tx).await?;

    let result = sqlx::query(&phase7_combo_backfill_sql())
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&weights_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .bind(phase7_combo_required_factor_count(specs, plan))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill phase7 combo {}@{}: {}",
                plan.combo_name, plan.version, error
            )
        })?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit combo backfill transaction: {}", error))?;

    Ok(result.rows_affected() as usize)
}


pub(crate) fn phase7_combo_required_factor_count(
    specs: &[SetBasedFactorSpec],
    plan: &SetBasedFactorBackfillPlan,
) -> i64 {
    match plan.combo_method {
        "weighted_event_earnings"
        | "weighted_event_window_earnings"
        | "weighted_event_surprise"
        | "weighted_event_post_return_curve"
        | "weighted_forecast_revision"
        | "weighted_repurchase_supply_shock"
        | "weighted_block_trade_sd"
        | "weighted_unlock_supply_pressure"
        | "weighted_analyst_revision" => 1,
        "weighted_futures_price_chain" => 1,
        _ => specs.len() as i64,
    }
}


async fn execute_phase7_alpha_blend_backfill(
    db: &sqlx::PgPool,
    plan: &Phase7AlphaBlendBackfillPlan,
) -> Result<usize, String> {
    let source_records_json = alpha_blend_source_records_json(&plan.source_combos)?;
    let weights_json = alpha_blend_weights_json(&plan.source_combos)?;
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to start alpha blend transaction: {}", error))?;
    set_local_statement_timeout(&mut tx, plan.statement_timeout_ms).await?;

    sqlx::query(
        "INSERT INTO multi_factor_weight (combo_name, version, weights, method, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (combo_name, version) DO UPDATE SET
           weights = EXCLUDED.weights,
           method = EXCLUDED.method,
           status = EXCLUDED.status,
           created_at = NOW()",
    )
    .bind(&plan.combo_name)
    .bind(&plan.version)
    .bind(&weights_json)
    .bind(plan.combo_method)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert alpha blend weights: {}", error))?;

    let result = sqlx::query(phase7_alpha_blend_backfill_sql(plan.combo_method))
        .bind(&plan.combo_name)
        .bind(&plan.version)
        .bind(&source_records_json)
        .bind(plan.start_date)
        .bind(plan.end_date)
        .bind(phase7_alpha_blend_required_source_count(plan))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            format!(
                "Failed to backfill alpha blend combo {}@{}: {}",
                plan.combo_name, plan.version, error
            )
        })?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit alpha blend transaction: {}", error))?;

    Ok(result.rows_affected() as usize)
}


pub(crate) fn phase7_alpha_blend_required_source_count(plan: &Phase7AlphaBlendBackfillPlan) -> i64 {
    match plan.combo_method {
        "weighted_combo_optional_overlay" => 1,
        _ => plan.source_combos.len() as i64,
    }
}


async fn set_local_statement_timeout(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    statement_timeout_ms: u64,
) -> Result<(), String> {
    let statement = format!("SET LOCAL statement_timeout = {}", statement_timeout_ms);
    sqlx::query(&statement)
        .execute(&mut **tx)
        .await
        .map_err(|error| format!("Failed to set statement_timeout: {}", error))?;
    Ok(())
}

async fn set_local_combo_backfill_planner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), String> {
    // Combo weights are tiny; nested-loop index scans by factor_code avoid
    // scanning every factor in the trade-date chunk.
    for statement in [
        "SET LOCAL enable_bitmapscan = off",
        "SET LOCAL enable_hashjoin = off",
        "SET LOCAL enable_mergejoin = off",
    ] {
        sqlx::query(statement)
            .execute(&mut **tx)
            .await
            .map_err(|error| format!("Failed to set combo planner hint: {}", error))?;
    }
    Ok(())
}

async fn update_factor_backfill_progress(
    db: &sqlx::PgPool,
    task_id: &str,
    completed_steps: usize,
    total_steps: usize,
    success_rows: usize,
) -> Result<(), String> {
    let progress = if total_steps == 0 {
        0
    } else {
        ((completed_steps.min(total_steps) * 100) / total_steps) as i32
    };
    sqlx::query(
        "UPDATE data_sync_task
         SET progress=$2,
             success_count=$3,
             last_heartbeat_at=now()
         WHERE task_id=$1",
    )
    .bind(task_id)
    .bind(progress)
    .bind(usize_to_i32(success_rows))
    .execute(db)
    .await
    .map_err(|error| format!("Failed to update factor backfill progress: {}", error))?;
    Ok(())
}

async fn factor_backfill_cancel_requested(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<bool, String> {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status
         FROM data_sync_task
         WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to check factor backfill cancel status: {}", error))?;

    Ok(matches!(
        status.as_deref(),
        Some("cancel_requested") | Some("cancelled")
    ))
}

fn elapsed_millis(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn factor_backfill_profile_metrics(
    report: &SetBasedFactorBackfillReport,
    elapsed_ms: u64,
) -> serde_json::Value {
    let total_rows = report.total_rows();
    let rows_per_sec = if elapsed_ms > 0 {
        Some(total_rows as f64 * 1000.0 / elapsed_ms as f64)
    } else {
        None
    };
    let factor_rows_by_code = report
        .factor_rows_by_code
        .iter()
        .map(|(code, rows)| (code.clone(), *rows))
        .collect::<HashMap<_, _>>();

    json!({
        "factor_rows": report.factor_rows,
        "combo_rows": report.combo_rows,
        "total_rows": total_rows,
        "elapsed_ms": elapsed_ms,
        "rows_per_sec": rows_per_sec,
        "factor_rows_by_code": factor_rows_by_code,
    })
}

async fn persist_factor_backfill_experiment_run(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &SetBasedFactorBackfillPlan,
    specs: &[SetBasedFactorSpec],
    completion: &SetBasedFactorBackfillCompletion,
) -> Result<(), String> {
    let experiment_run_id = format!("exp-{}", uuid::Uuid::new_v4());
    let config = json!({
        "task_id": task_id,
        "bundle_name": plan.bundle_name,
        "combo_name": plan.combo_name,
        "combo_method": plan.combo_method,
        "version": plan.version,
        "start_date": plan.start_date,
        "end_date": plan.end_date,
        "statement_timeout_ms": plan.statement_timeout_ms,
        "category": plan.category,
        "phase": plan.phase,
        "dependencies": plan.dependencies,
        "source_combos": plan.source_combos.iter().map(|source| json!({
            "combo_name": source.combo_name,
            "version": source.version,
            "weight": source.weight,
        })).collect::<Vec<_>>(),
        "factors": specs.iter().map(|spec| json!({
            "factor_code": spec.factor_code,
            "period": spec.period,
            "weight": spec.weight,
        })).collect::<Vec<_>>(),
    });
    let metrics = factor_backfill_profile_metrics(completion.report(), completion.elapsed_ms());

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, $2, 'data_sync_task', $3,
                 $4, $5, $6, now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(plan.experiment_type)
    .bind(task_id)
    .bind(&config)
    .bind(&metrics)
    .bind(completion.experiment_status())
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert phase7 backfill experiment_run: {}", error))?;

    Ok(())
}


pub(crate) fn factor_backfill_combo_weights_json(
    specs: &[SetBasedFactorSpec],
) -> Result<serde_json::Value, String> {
    let weights = specs
        .iter()
        .map(|spec| (spec.factor_code.to_string(), spec.weight))
        .collect::<HashMap<_, _>>();
    serde_json::to_value(weights).map_err(|error| format!("Failed to encode weights: {}", error))
}

pub(crate) fn alpha_blend_source_records_json(
    sources: &[Phase7AlphaBlendSourcePlan],
) -> Result<serde_json::Value, String> {
    serde_json::to_value(sources).map_err(|error| format!("Failed to encode sources: {}", error))
}

pub(crate) fn alpha_blend_weights_json(
    sources: &[Phase7AlphaBlendSourcePlan],
) -> Result<serde_json::Value, String> {
    let weights = sources
        .iter()
        .map(|source| {
            (
                format!("{}@{}", source.combo_name, source.version),
                source.weight,
            )
        })
        .collect::<HashMap<_, _>>();
    serde_json::to_value(weights).map_err(|error| format!("Failed to encode weights: {}", error))
}


pub(crate) fn phase7_factor_backfill_sql(spec: &Phase7BackfillFactorSpec) -> String {
    match spec.kind {
        Phase7BackfillFactorKind::Reversal => phase7_reversal_backfill_sql(spec.period),
        Phase7BackfillFactorKind::DownsideVolatility => {
            phase7_downside_volatility_backfill_sql(spec.period)
        }
        Phase7BackfillFactorKind::AmihudIlliquidity => phase7_amihud_backfill_sql(spec.period),
        Phase7BackfillFactorKind::AmountIntensity => {
            phase7_amount_intensity_backfill_sql(spec.period)
        }
        Phase7BackfillFactorKind::MarketRelativeMomentum => {
            phase7_relative_momentum_backfill_sql(spec.period, false)
        }
        Phase7BackfillFactorKind::IndustryRelativeMomentum => {
            phase7_relative_momentum_backfill_sql(spec.period, true)
        }
        Phase7BackfillFactorKind::FinancialLatest {
            source_column,
            higher_is_better,
        } => phase7_financial_latest_backfill_sql(source_column, higher_is_better),
        Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
            source_column,
            higher_is_better,
        } => {
            phase7_industry_relative_financial_latest_backfill_sql(source_column, higher_is_better)
        }
        Phase7BackfillFactorKind::DailyBasicLatest {
            source_column,
            higher_is_better,
            positive_only,
        } => phase7_daily_basic_latest_backfill_sql(source_column, higher_is_better, positive_only),
        Phase7BackfillFactorKind::MoneyflowRolling {
            amount_expression,
            higher_is_better,
        } => phase7_moneyflow_backfill_sql(spec.period, amount_expression, higher_is_better),
        Phase7BackfillFactorKind::MoneyflowCongestionInteraction { flow_expression } => {
            phase7_moneyflow_congestion_backfill_sql(spec.period, flow_expression)
        }
        Phase7BackfillFactorKind::SupplyFloatShock {
            share_expression,
            horizon_days,
            mode,
        } => phase7_supply_float_shock_backfill_sql(share_expression, horizon_days, mode),
        Phase7BackfillFactorKind::CashflowLatest {
            value_expression,
            required_filter,
            higher_is_better,
        } => {
            phase7_cashflow_latest_backfill_sql(value_expression, required_filter, higher_is_better)
        }
        Phase7BackfillFactorKind::DividendRollingQuality {
            value_expression,
            higher_is_better,
        } => phase7_dividend_rolling_quality_backfill_sql(value_expression, higher_is_better),
        Phase7BackfillFactorKind::EventLatest {
            source_table,
            value_expression,
            higher_is_better,
        } => phase7_event_latest_backfill_sql(source_table, value_expression, higher_is_better),
        Phase7BackfillFactorKind::BlockTradeWindow {
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_block_trade_window_backfill_sql(
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::UnlockPressure { horizon_days } => {
            phase7_unlock_pressure_backfill_sql(horizon_days)
        }
        Phase7BackfillFactorKind::LiquidityQuality {
            signal,
            short_window,
            long_window,
        } => phase7_liquidity_quality_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::MarketResidualRisk {
            signal,
            short_window,
            long_window,
        } => phase7_market_residual_risk_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::IndustryProsperity {
            signal,
            short_window,
            long_window,
        } => phase7_industry_prosperity_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::FuturesPriceChain { signal } => {
            phase7_futures_price_chain_backfill_sql(signal)
        }
        Phase7BackfillFactorKind::EquityPledgePressure => {
            panic!("equity_pledge_pressure must use the dedicated PIT combo builder")
        }
        Phase7BackfillFactorKind::ShareholderStructure => {
            panic!(
                "shareholder_structure must use the dedicated strict PIT low-fanout combo builder"
            )
        }
        Phase7BackfillFactorKind::MarginDetailLeverageCrowding => {
            panic!("margin_detail must use the dedicated next-session PIT combo builder")
        }
        Phase7BackfillFactorKind::AnalystRevision {
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_analyst_revision_backfill_sql(
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::ForecastRevision {
            value_expression,
            higher_is_better,
            max_event_age_days,
        } => phase7_forecast_revision_backfill_sql(
            value_expression,
            higher_is_better,
            max_event_age_days,
        ),
        Phase7BackfillFactorKind::EventWindow {
            source_table,
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_event_window_backfill_sql(
            source_table,
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::EventPostReturnCurve {
            source_table,
            event_filter_expression,
            higher_is_better,
            window_days,
            industry_relative,
            min_event_age_days,
            max_event_age_days,
        } => phase7_event_post_return_curve_backfill_sql(
            source_table,
            event_filter_expression,
            higher_is_better,
            window_days,
            industry_relative,
            min_event_age_days,
            max_event_age_days,
        ),
        Phase7BackfillFactorKind::FinancialAnnualChange {
            source_column,
            mode,
        } => phase7_financial_annual_change_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::FinancialAnnualAcceleration {
            source_column,
            mode,
        } => phase7_financial_annual_acceleration_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::FinancialAnnualPersistence {
            source_column,
            mode,
        } => phase7_financial_annual_persistence_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::LargeCapMomentumReversal {
            reversal_period,
            momentum_period,
            large_cap_threshold_yi,
        } => phase7_large_cap_momentum_reversal_backfill_sql(
            reversal_period,
            momentum_period,
            large_cap_threshold_yi,
        ),
        Phase7BackfillFactorKind::DefensiveLowVolQuality {
            volatility_period,
            industries,
        } => phase7_defensive_low_vol_quality_backfill_sql(volatility_period, industries),
    }
}

pub(crate) fn phase7_reversal_backfill_sql(period: i32) -> String {
    format!(
        "WITH priced AS (
            SELECT
                symbol,
                trade_date,
                close::double precision AS close,
                LAG(close::double precision, {period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj
            WHERE close IS NOT NULL
              AND trade_date <= $4
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                (prev_close - close) / NULLIF(prev_close, 0.0) AS raw_value
            FROM priced
            WHERE trade_date BETWEEN $3 AND $4
              AND prev_close IS NOT NULL
              AND prev_close <> 0.0
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

/// P4.2b 大盘动量反转交互特征 backfill SQL。
/// 大盘股池(total_mv > 阈值,PIT)内,reversal × momentum 交互:
/// raw_value = CUME_DIST(reversal) × CUME_DIST(momentum),捕获"既超跌又强势"的非线性协同。
/// reversal = (prev_rev_close - close)/prev_rev_close(超跌为正),
/// momentum = (close - prev_mom_close)/prev_mom_close(强势为正)。
/// 两个 CUME_DIST ∈ [0,1],乘积 ∈ [0,1],高=同时超跌+强势。
/// PIT:量价当日已知;市值取 trade_date<=当日 最近值;available_at = trade_date。
pub(crate) fn phase7_large_cap_momentum_reversal_backfill_sql(
    reversal_period: i32,
    momentum_period: i32,
    large_cap_threshold_yi: i32,
) -> String {
    // total_mv 单位为万元,阈值亿元 → 万元 = ×1e4
    let threshold_wan: i64 = (large_cap_threshold_yi as i64) * 10_000;
    format!(
        "WITH priced AS (
            SELECT
                symbol,
                trade_date,
                close::double precision AS close,
                LAG(close::double precision, {reversal_period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_rev_close,
                LAG(close::double precision, {momentum_period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_mom_close
            FROM market_stock_daily_bar_adj
            WHERE close IS NOT NULL AND trade_date <= $4
              AND trade_date >= ($3::date - INTERVAL '120 days')
        ),
        -- 大盘股池:用 start_date($3) 当日市值固定分桶(PIT,与 P4.1c 口径一致),
        -- 避免逐日 LATERAL 市值查询的性能开销。大盘股池相对稳定,固定分桶是可接受的简化。
        large_cap_symbols AS (
            SELECT DISTINCT ON (symbol) symbol
            FROM market_stock_daily_basic
            WHERE trade_date <= $3 AND total_mv > {threshold_wan}
            ORDER BY symbol, trade_date DESC
        ),
        large_cap AS (
            SELECT p.symbol, p.trade_date, p.close, p.prev_rev_close, p.prev_mom_close
            FROM priced p
            JOIN large_cap_symbols l ON p.symbol = l.symbol
            WHERE p.trade_date BETWEEN $3 AND $4
              AND p.prev_rev_close IS NOT NULL AND p.prev_mom_close IS NOT NULL
              AND p.prev_rev_close <> 0.0 AND p.prev_mom_close <> 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                (prev_rev_close - close) / NULLIF(prev_rev_close, 0.0) AS reversal,
                (close - prev_mom_close) / NULLIF(prev_mom_close, 0.0) AS momentum
            FROM large_cap
            WHERE prev_rev_close <> 0.0 AND prev_mom_close <> 0.0
        ),
        interaction AS (
            SELECT
                symbol,
                trade_date,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY reversal) AS rev_dist,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY momentum) AS mom_dist
            FROM raw
            WHERE reversal IS NOT NULL AND momentum IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                rev_dist * mom_dist AS raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY rev_dist * mom_dist) AS normalized_value
            FROM interaction
            WHERE rev_dist IS NOT NULL AND mom_dist IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        WHERE raw_value IS NOT NULL
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

/// P4.2b 防御板块低波质量交互特征 backfill SQL。
/// 防御行业池(银行/保险/白酒/黄金/机场/电信运营/啤酒)内,low_volatility × fin_roe 交互:
/// raw_value = CUME_DIST(-volatility) × CUME_DIST(fin_roe),捕获"既低波又高质量"的非线性协同。
/// 低波(防御性)+ 高质量(高 ROE)是防御板块的核心选股逻辑,补偿 ascending 在此失效。
/// PIT:量价当日已知;fin_roe 用 available_at <= trade_date;available_at = trade_date。
pub(crate) fn phase7_defensive_low_vol_quality_backfill_sql(
    volatility_period: i32,
    industries: &[&str],
) -> String {
    let preceding = volatility_period - 1;
    // 行业列表展开为 SQL IN 列表:'银行','白酒',...
    let industry_list: String = industries
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH ret AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 THEN (close - prev_close) / prev_close
                    ELSE NULL
                END AS ret
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
                  AND trade_date >= ($3::date - INTERVAL '120 days')
            ) bars
        ),
        vol AS (
            SELECT
                symbol,
                trade_date,
                SQRT(
                    AVG(POWER(ret, 2)) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ) * 252.0
                ) AS volatility
            FROM ret
            WHERE ret IS NOT NULL
        ),
        defensive AS (
            SELECT v.symbol, v.trade_date, v.volatility, fv.normalized_value AS fin_roe
            FROM vol v
            JOIN market_stock ms ON v.symbol = ms.symbol
            JOIN LATERAL (
                SELECT normalized_value FROM factor_value
                WHERE factor_code = 'fin_roe_daily_std'
                  AND factor_version = '1.0.0'
                  AND symbol = v.symbol
                  AND trade_date = v.trade_date
                  AND normalized_value IS NOT NULL
                  AND available_at <= v.trade_date
                ORDER BY available_at DESC LIMIT 1
            ) fv ON true
            WHERE v.trade_date BETWEEN $3 AND $4
              AND v.volatility IS NOT NULL AND v.volatility > 0.0
              AND ms.industry IN ({industry_list})
        ),
        interaction AS (
            SELECT
                symbol,
                trade_date,
                volatility,
                fin_roe,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY -volatility) AS lowvol_dist,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY fin_roe) AS quality_dist
            FROM defensive
            WHERE fin_roe IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                lowvol_dist * quality_dist AS raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY lowvol_dist * quality_dist) AS normalized_value
            FROM interaction
            WHERE lowvol_dist IS NOT NULL AND quality_dist IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        WHERE raw_value IS NOT NULL
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_downside_volatility_backfill_sql(period: i32) -> String {
    let preceding = period - 1;
    format!(
        "WITH returns AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 THEN (close - prev_close) / prev_close
                    ELSE NULL
                END AS ret
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
            ) bars
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                SQRT(
                    AVG(POWER(LEAST(ret, 0.0), 2)) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ) * 252.0
                ) AS raw_value,
                COUNT(ret) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM returns
            WHERE ret IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_amihud_backfill_sql(period: i32) -> String {
    let preceding = period - 1;
    format!(
        "WITH observations AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 AND amount > 0.0
                        THEN ABS((close - prev_close) / prev_close) / amount * 1000000000.0
                    ELSE NULL
                END AS illiquidity
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    amount::double precision AS amount,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND amount IS NOT NULL
                  AND trade_date <= $4
            ) bars
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS raw_value,
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM observations
            WHERE illiquidity IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_amount_intensity_backfill_sql(period: i32) -> String {
    format!(
        "WITH raw AS (
            SELECT
                symbol,
                trade_date,
                amount::double precision
                    / NULLIF(
                        AVG(amount::double precision) OVER (
                            PARTITION BY symbol ORDER BY trade_date
                            ROWS BETWEEN {period} PRECEDING AND 1 PRECEDING
                        ),
                        0.0
                    ) AS raw_value,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {period} PRECEDING AND 1 PRECEDING
                ) AS obs_count
            FROM market_stock_daily_bar_adj
            WHERE amount IS NOT NULL
              AND trade_date <= $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_relative_momentum_backfill_sql(period: i32, industry_relative: bool) -> String {
    let baseline_select = if industry_relative {
        "trade_date, industry, AVG(stock_return) AS baseline_return"
    } else {
        "trade_date, AVG(stock_return) AS baseline_return"
    };
    let baseline_group_by = if industry_relative {
        "trade_date, industry"
    } else {
        "trade_date"
    };
    let baseline_join = if industry_relative {
        "bl.trade_date = sr.trade_date AND bl.industry = sr.industry"
    } else {
        "bl.trade_date = sr.trade_date"
    };

    format!(
        "WITH stock_returns AS (
            SELECT
                bars.symbol,
                bars.trade_date,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                CASE
                    WHEN bars.prev_close > 0.0 THEN (bars.close - bars.prev_close) / bars.prev_close
                    ELSE NULL
                END AS stock_return
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision, {period}) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
            ) bars
            JOIN market_stock ms ON ms.symbol = bars.symbol
            WHERE ms.list_status = 'L'
        ),
        baseline AS (
            SELECT {baseline_select}
            FROM stock_returns
            WHERE stock_return IS NOT NULL
            GROUP BY {baseline_group_by}
        ),
        raw AS (
            SELECT
                sr.symbol,
                sr.trade_date,
                sr.stock_return - bl.baseline_return AS raw_value
            FROM stock_returns sr
            JOIN baseline bl ON {baseline_join}
            WHERE sr.trade_date BETWEEN $3 AND $4
              AND sr.stock_return IS NOT NULL
              AND bl.baseline_return IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                fi.ann_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, ann_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_relative_financial_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                fi.ann_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN market_stock ms ON ms.symbol = symbols.symbol
            JOIN LATERAL (
                SELECT
                    ann_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value - AVG(raw_value) OVER (
                    PARTITION BY trade_date, industry
                ) AS raw_value
            FROM latest
            WHERE raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM residualized
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, ann_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_change_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let raw_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(latest.raw_value - prev.raw_value) / NULLIF(ABS(prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference => "latest.raw_value - prev.raw_value",
        FinancialAnnualChangeMode::Decrease => "prev.raw_value - latest.raw_value",
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                fi.ann_date,
                fi.end_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        matched AS (
            SELECT
                latest.symbol,
                latest.trade_date,
                latest.ann_date,
                latest.end_date,
                latest.raw_value AS current_raw_value,
                prev.ann_date AS prev_ann_date,
                prev.raw_value AS previous_raw_value,
                {raw_expression} AS raw_value
            FROM latest
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prev
                WHERE prev.ts_code = latest.symbol
                  AND prev.end_date = (latest.end_date - INTERVAL '1 year')::date
                  AND prev.ann_date <= latest.trade_date
                  AND prev.{source_column} IS NOT NULL
                ORDER BY prev.ann_date DESC
                LIMIT 1
            ) prev ON true
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                GREATEST(ann_date, prev_ann_date) AS available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM matched
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_acceleration_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let latest_yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(latest.raw_value - latest_prev.raw_value) / NULLIF(ABS(latest_prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference | FinancialAnnualChangeMode::Decrease => {
            "latest.raw_value - latest_prev.raw_value"
        }
    };
    let prior_yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(prior_latest.raw_value - prior_prev.raw_value) / NULLIF(ABS(prior_prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference | FinancialAnnualChangeMode::Decrease => {
            "prior_latest.raw_value - prior_prev.raw_value"
        }
    };
    let raw_expression = match mode {
        FinancialAnnualChangeMode::Decrease => "prior_yoy.raw_value - latest_yoy.raw_value",
        FinancialAnnualChangeMode::PercentChange | FinancialAnnualChangeMode::Difference => {
            "latest_yoy.raw_value - prior_yoy.raw_value"
        }
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        source_reports AS (
            SELECT
                latest.ts_code AS symbol,
                latest.ann_date,
                latest.end_date,
                latest.{source_column}::double precision AS raw_value
            FROM market_financial_indicator latest
            WHERE latest.{source_column} IS NOT NULL
              AND latest.ann_date <= $4
        ),
        yoy_points AS (
            SELECT
                latest.symbol,
                latest.ann_date AS latest_ann_date,
                latest.end_date,
                GREATEST(latest.ann_date, latest_prev.ann_date, prior_latest.ann_date, prior_prev.ann_date) AS available_at,
                {raw_expression} AS raw_value
            FROM source_reports latest
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator latest_prev
                WHERE latest_prev.ts_code = latest.symbol
                  AND latest_prev.end_date = (latest.end_date - INTERVAL '1 year')::date
                  AND latest_prev.{source_column} IS NOT NULL
                ORDER BY latest_prev.ann_date DESC
                LIMIT 1
            ) latest_prev ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prior_latest
                WHERE prior_latest.ts_code = latest.symbol
                  AND prior_latest.ann_date < latest.ann_date
                  AND prior_latest.end_date < latest.end_date
                  AND prior_latest.{source_column} IS NOT NULL
                ORDER BY prior_latest.ann_date DESC, prior_latest.end_date DESC
                LIMIT 1
            ) prior_latest ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prior_prev
                WHERE prior_prev.ts_code = latest.symbol
                  AND prior_prev.end_date = (prior_latest.end_date - INTERVAL '1 year')::date
                  AND prior_prev.{source_column} IS NOT NULL
                ORDER BY prior_prev.ann_date DESC
                LIMIT 1
            ) prior_prev ON true
            CROSS JOIN LATERAL (
                SELECT {latest_yoy_expression} AS raw_value
            ) latest_yoy
            CROSS JOIN LATERAL (
                SELECT {prior_yoy_expression} AS raw_value
            ) prior_yoy
            WHERE latest_yoy.raw_value IS NOT NULL
              AND prior_yoy.raw_value IS NOT NULL
        ),
        deduped_points AS (
            SELECT DISTINCT ON (symbol, available_at)
                symbol,
                latest_ann_date,
                end_date,
                available_at,
                raw_value
            FROM yoy_points
            WHERE available_at <= $4
              AND raw_value IS NOT NULL
            ORDER BY symbol, available_at, latest_ann_date DESC, end_date DESC
        ),
        yoy_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM deduped_points
        ),
        raw AS (
            SELECT
                yi.symbol,
                td.trade_date,
                yi.available_at,
                yi.raw_value
            FROM yoy_intervals yi
            JOIN trade_days td
              ON td.trade_date >= yi.available_at
             AND (yi.next_available_at IS NULL OR td.trade_date < yi.next_available_at)
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_persistence_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(current_report.raw_value - previous_report.raw_value) / NULLIF(ABS(previous_report.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference => "current_report.raw_value - previous_report.raw_value",
        FinancialAnnualChangeMode::Decrease => "previous_report.raw_value - current_report.raw_value",
    };
    let raw_expression = "yoy_value + 0.5 * prior_yoy_value + 0.25 * second_yoy_value";

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        source_reports AS (
            SELECT
                latest.ts_code AS symbol,
                latest.ann_date,
                latest.end_date,
                latest.{source_column}::double precision AS raw_value
            FROM market_financial_indicator latest
            WHERE latest.{source_column} IS NOT NULL
              AND latest.ann_date <= $4
        ),
        annual_yoy_points AS (
            SELECT
                current_report.symbol,
                current_report.ann_date AS current_ann_date,
                current_report.end_date,
                GREATEST(current_report.ann_date, previous_report.ann_date) AS yoy_available_at,
                {yoy_expression} AS raw_value
            FROM source_reports current_report
            JOIN source_reports previous_report
              ON previous_report.symbol = current_report.symbol
             AND previous_report.end_date = (current_report.end_date - INTERVAL '1 year')::date
            WHERE {yoy_expression} IS NOT NULL
        ),
        sequenced_yoy AS (
            SELECT
                symbol,
                current_ann_date,
                end_date,
                yoy_available_at,
                raw_value AS yoy_value,
                LAG(yoy_available_at, 1) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS prior_yoy_available_at,
                LAG(raw_value, 1) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS prior_yoy_value,
                LAG(yoy_available_at, 2) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS second_yoy_available_at,
                LAG(raw_value, 2) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS second_yoy_value
            FROM annual_yoy_points
        ),
        persistence_points AS (
            SELECT
                symbol,
                current_ann_date AS latest_ann_date,
                end_date,
                GREATEST(yoy_available_at, prior_yoy_available_at, second_yoy_available_at) AS available_at,
                {raw_expression} AS raw_value
            FROM sequenced_yoy
            WHERE prior_yoy_value IS NOT NULL
              AND second_yoy_value IS NOT NULL
              AND prior_yoy_available_at IS NOT NULL
              AND second_yoy_available_at IS NOT NULL
        ),
        deduped_points AS (
            SELECT DISTINCT ON (symbol, available_at)
                symbol,
                latest_ann_date,
                end_date,
                available_at,
                raw_value
            FROM persistence_points
            WHERE available_at <= $4
              AND raw_value IS NOT NULL
            ORDER BY symbol, available_at, latest_ann_date DESC, end_date DESC
        ),
        persistence_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM deduped_points
        ),
        raw AS (
            SELECT
                yi.symbol,
                td.trade_date,
                yi.available_at,
                yi.raw_value
            FROM persistence_intervals yi
            JOIN trade_days td
              ON td.trade_date >= yi.available_at
             AND (yi.next_available_at IS NULL OR td.trade_date < yi.next_available_at)
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_daily_basic_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
    positive_only: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let positive_filter = if positive_only {
        "AND raw_value > 0.0"
    } else {
        ""
    };

    format!(
        "WITH raw AS (
            SELECT
                symbol,
                trade_date,
                {source_column}::double precision AS raw_value
            FROM market_stock_daily_basic
            WHERE trade_date BETWEEN $3 AND $4
              AND {source_column} IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
              {positive_filter}
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_moneyflow_backfill_sql(
    period: i32,
    amount_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let preceding = period - 1;
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH observations AS (
            SELECT
                mf.symbol,
                mf.trade_date,
                {amount_expression} AS flow_amount,
                bar.amount::double precision AS traded_amount
            FROM market_stock_moneyflow mf
            JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = mf.symbol
             AND bar.trade_date = mf.trade_date
            WHERE mf.trade_date <= $4
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                SUM(flow_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(traded_amount) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS raw_value,
                COUNT(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM observations
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_moneyflow_congestion_backfill_sql(period: i32, flow_expression: &'static str) -> String {
    let preceding = period - 1;

    format!(
        "WITH observations AS (
            SELECT
                mf.symbol,
                mf.trade_date,
                {flow_expression} AS flow_amount,
                bar.amount::double precision AS traded_amount,
                basic.circ_mv::double precision AS float_market_value,
                AVG(bar.amount::double precision) OVER (
                    PARTITION BY mf.symbol ORDER BY mf.trade_date
                    ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                ) AS prior_traded_amount_avg_60
            FROM market_stock_moneyflow mf
            JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = mf.symbol
             AND bar.trade_date = mf.trade_date
            JOIN market_stock_daily_basic basic
              ON basic.symbol = mf.symbol
             AND basic.trade_date = mf.trade_date
            WHERE mf.trade_date <= $4
              AND mf.trade_date >= ($3::date - INTERVAL '180 days')
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
        ),
        daily_crowding AS (
            SELECT
                symbol,
                trade_date,
                flow_amount,
                traded_amount,
                float_market_value,
                traded_amount / NULLIF(prior_traded_amount_avg_60, 0.0) AS amount_crowding
            FROM observations
            WHERE prior_traded_amount_avg_60 IS NOT NULL
              AND prior_traded_amount_avg_60 > 0.0
              AND flow_amount IS NOT NULL
        ),
        rolling AS (
            SELECT
                symbol,
                trade_date,
                SUM(flow_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(traded_amount) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS flow_intensity,
                AVG(amount_crowding) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS amount_crowding,
                SUM(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(float_market_value) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS capacity_pressure,
                COUNT(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM daily_crowding
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                flow_intensity,
                GREATEST(COALESCE(amount_crowding, 0.0) - 1.0, 0.0)
                    + GREATEST(COALESCE(capacity_pressure, 0.0), 0.0) AS crowding_penalty
            FROM rolling
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND flow_intensity IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                flow_intensity / (1.0 + crowding_penalty) AS raw_value,
                percent_rank() OVER (
                    PARTITION BY trade_date
                    ORDER BY flow_intensity / (1.0 + crowding_penalty)
                ) AS normalized_value
            FROM raw
            WHERE crowding_penalty IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_supply_float_shock_backfill_sql(
    share_expression: &'static str,
    horizon_days: i32,
    mode: SupplyFloatShockMode,
) -> String {
    let horizon_days = horizon_days.max(1);
    let delta_preceding = horizon_days - 1;
    let warmup_days = (horizon_days * 3).max(90);
    let raw_projection = match mode {
        SupplyFloatShockMode::GrowthInverse => {
            format!("-LN(share_value / NULLIF(prev_share_{horizon_days}, 0.0)) AS raw_value")
        }
        SupplyFloatShockMode::ChurnInverse => {
            format!(
                "-STDDEV_POP(one_day_share_delta) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {delta_preceding} PRECEDING AND CURRENT ROW
                ) AS raw_value"
            )
        }
    };
    let raw_filter = match mode {
        SupplyFloatShockMode::GrowthInverse => {
            format!("obs_count_{horizon_days} = {}", horizon_days + 1)
        }
        SupplyFloatShockMode::ChurnInverse => {
            format!("delta_count_{horizon_days} = {horizon_days}")
        }
    };

    format!(
        "WITH supply AS (
            SELECT
                basic.symbol,
                basic.trade_date,
                {share_expression} AS share_value
            FROM market_stock_daily_basic basic
            WHERE basic.trade_date <= $4
              AND basic.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
        ),
        observations AS (
            SELECT
                symbol,
                trade_date,
                share_value,
                LAG(share_value, {horizon_days}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_share_{horizon_days},
                CASE
                    WHEN LAG(share_value) OVER (PARTITION BY symbol ORDER BY trade_date) > 0.0
                         AND share_value > 0.0
                    THEN LN(share_value / NULLIF(
                        LAG(share_value) OVER (PARTITION BY symbol ORDER BY trade_date),
                        0.0
                    ))
                    ELSE NULL
                END AS one_day_share_delta,
                COUNT(share_value) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {horizon_days} PRECEDING AND CURRENT ROW
                ) AS obs_count_{horizon_days}
            FROM supply
            WHERE share_value IS NOT NULL
              AND share_value > 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                obs_count_{horizon_days},
                {raw_projection},
                COUNT(one_day_share_delta) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {delta_preceding} PRECEDING AND CURRENT ROW
                ) AS delta_count_{horizon_days}
            FROM observations
        ),
        filtered AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                raw_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
              AND raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM filtered
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_liquidity_quality_backfill_sql(
    signal: LiquidityQualitySignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    let warmup_days = ((long_window * 3) / 2).max(180);
    let raw_value_expression = match signal {
        LiquidityQualitySignal::ImpactImprovement => {
            "LN((long_illiq + 1e-12) / (short_illiq + 1e-12)) AS raw_value".to_string()
        }
        LiquidityQualitySignal::AmountTrend => {
            "LN((short_amount + 1.0) / (long_amount + 1.0)) AS raw_value".to_string()
        }
        LiquidityQualitySignal::AmountStability => {
            format!("-COALESCE(std_log_amount_{short_window}, 0.0) AS raw_value")
        }
        LiquidityQualitySignal::TurnoverStability => {
            format!(
                "-ABS((turnover_proxy - avg_turnover_{short_window}) / NULLIF(std_turnover_{short_window}, 0.0))
                 - COALESCE(std_turnover_{short_window}, 0.0) AS raw_value"
            )
        }
    };
    let raw_filter = match signal {
        LiquidityQualitySignal::ImpactImprovement => {
            format!(
                "illiq_obs_count_{short_window} = {short_window}
              AND illiq_obs_count_{long_window} = {long_window}
              AND short_illiq IS NOT NULL
              AND long_illiq IS NOT NULL"
            )
        }
        LiquidityQualitySignal::AmountTrend => {
            format!(
                "amount_obs_count_{short_window} = {short_window}
              AND amount_obs_count_{long_window} = {long_window}
              AND short_amount IS NOT NULL
              AND long_amount IS NOT NULL"
            )
        }
        LiquidityQualitySignal::AmountStability => {
            format!(
                "amount_obs_count_{short_window} = {short_window}
              AND std_log_amount_{short_window} IS NOT NULL"
            )
        }
        LiquidityQualitySignal::TurnoverStability => {
            format!(
                "turnover_obs_count_{short_window} = {short_window}
              AND avg_turnover_{short_window} IS NOT NULL
              AND std_turnover_{short_window} IS NOT NULL"
            )
        }
    };

    format!(
        "WITH joined AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount,
                basic.circ_mv::double precision AS circ_mv,
                LAG(close::double precision) OVER (
                    PARTITION BY bar.symbol ORDER BY bar.trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.amount IS NOT NULL
              AND basic.circ_mv IS NOT NULL
        ),
        observations AS (
            SELECT
                symbol,
                trade_date,
                amount,
                CASE
                    WHEN prev_close > 0.0 AND close > 0.0 AND amount > 0.0
                    THEN ABS((close - prev_close) / prev_close) / amount * 1000000000.0
                    ELSE NULL
                END AS illiquidity,
                CASE WHEN amount > 0.0 THEN LN(1.0 + amount) ELSE NULL END AS log_amount,
                CASE
                    WHEN amount > 0.0 AND circ_mv > 0.0 THEN amount / NULLIF(circ_mv, 0.0)
                    ELSE NULL
                END AS turnover_proxy
            FROM joined
        ),
        rolling AS (
            SELECT
                symbol,
                trade_date,
                amount,
                turnover_proxy,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS short_illiq,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS long_illiq,
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS illiq_obs_count_{short_window},
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS illiq_obs_count_{long_window},
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS short_amount,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS long_amount,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_{short_window},
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_{long_window},
                STDDEV_SAMP(log_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS std_log_amount_{short_window},
                AVG(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS avg_turnover_{short_window},
                STDDEV_SAMP(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS std_turnover_{short_window},
                COUNT(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS turnover_obs_count_{short_window}
            FROM observations
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                {raw_value_expression}
            FROM rolling
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_market_residual_risk_backfill_sql(
    signal: MarketResidualRiskSignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    let warmup_days = (long_window * 4).max(480);
    let min_downside_observations = (long_window / 4).max(20);
    let raw_value_expression = match signal {
        MarketResidualRiskSignal::LowBeta => format!("-beta_{long_window} AS raw_value"),
        MarketResidualRiskSignal::LowDownsideBeta => {
            format!("-downside_beta_{long_window} AS raw_value")
        }
        MarketResidualRiskSignal::LowResidualVolatility => {
            format!("-residual_vol_{long_window} AS raw_value")
        }
        MarketResidualRiskSignal::ResidualReversal => format!(
            "-residual_mean_{short_window} / NULLIF(residual_vol_{long_window}, 0.0) AS raw_value"
        ),
    };
    let raw_filter = match signal {
        MarketResidualRiskSignal::LowBeta => {
            format!("obs_count_{long_window} = {long_window} AND beta_{long_window} IS NOT NULL")
        }
        MarketResidualRiskSignal::LowDownsideBeta => format!(
            "downside_obs_count_{long_window} >= {min_downside_observations}
             AND downside_beta_{long_window} IS NOT NULL"
        ),
        MarketResidualRiskSignal::LowResidualVolatility => format!(
            "residual_obs_count_{long_window} = {long_window}
             AND residual_vol_{long_window} IS NOT NULL"
        ),
        MarketResidualRiskSignal::ResidualReversal => format!(
            "residual_obs_count_{short_window} = {short_window}
             AND residual_vol_{long_window} IS NOT NULL
             AND residual_mean_{short_window} IS NOT NULL"
        ),
    };

    format!(
        "WITH market AS (
            SELECT
                idx.trade_date,
                CASE
                    WHEN idx.close > 0 AND idx.pre_close > 0
                    THEN (idx.close::double precision / idx.pre_close::double precision) - 1.0
                    ELSE NULL
                END AS market_return
            FROM market_index_daily_bar idx
            WHERE idx.symbol = '000300.SH'
              AND idx.trade_date <= $4
              AND idx.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND idx.close IS NOT NULL
              AND idx.pre_close IS NOT NULL
        ),
        stock AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                LAG(bar.close::double precision) OVER (
                    PARTITION BY bar.symbol ORDER BY bar.trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj bar
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
        ),
        joined AS (
            SELECT
                stock.symbol,
                stock.trade_date,
                CASE
                    WHEN stock.prev_close > 0.0 AND stock.close > 0.0
                    THEN stock.close / stock.prev_close - 1.0
                    ELSE NULL
                END AS stock_return,
                market.market_return
            FROM stock
            JOIN market
              ON market.trade_date = stock.trade_date
            WHERE market.market_return IS NOT NULL
        ),
        rolling_beta AS (
            SELECT
                symbol,
                trade_date,
                stock_return,
                market_return,
                REGR_SLOPE(stock_return, market_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS beta_{long_window},
                REGR_COUNT(stock_return, market_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count_{long_window},
                REGR_SLOPE(
                    CASE WHEN market_return < 0.0 THEN stock_return ELSE NULL END,
                    CASE WHEN market_return < 0.0 THEN market_return ELSE NULL END
                ) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS downside_beta_{long_window},
                REGR_COUNT(
                    CASE WHEN market_return < 0.0 THEN stock_return ELSE NULL END,
                    CASE WHEN market_return < 0.0 THEN market_return ELSE NULL END
                ) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS downside_obs_count_{long_window}
            FROM joined
            WHERE stock_return IS NOT NULL
              AND market_return IS NOT NULL
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                stock_return,
                market_return,
                beta_{long_window},
                downside_beta_{long_window},
                obs_count_{long_window},
                downside_obs_count_{long_window},
                stock_return - beta_{long_window} * market_return AS residual_return
            FROM rolling_beta
            WHERE obs_count_{long_window} = {long_window}
              AND beta_{long_window} IS NOT NULL
        ),
        rolling_residual AS (
            SELECT
                symbol,
                trade_date,
                beta_{long_window},
                downside_beta_{long_window},
                obs_count_{long_window},
                downside_obs_count_{long_window},
                residual_return,
                STDDEV_SAMP(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_vol_{long_window},
                COUNT(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_obs_count_{long_window},
                AVG(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_mean_{short_window},
                COUNT(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_obs_count_{short_window}
            FROM residualized
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                {raw_value_expression}
            FROM rolling_residual
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_cashflow_latest_backfill_sql(
    value_expression: &'static str,
    required_filter: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        event_points AS (
            SELECT DISTINCT ON (cf.symbol, cf.available_at)
                cf.symbol,
                cf.available_at,
                {value_expression} AS raw_value
            FROM market_stock_cashflow cf
            WHERE cf.symbol IS NOT NULL
              AND cf.available_at IS NOT NULL
              AND {required_filter}
            ORDER BY cf.symbol, cf.available_at, cf.end_date DESC, cf.ann_date DESC
        ),
        version_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM event_points
        ),
        latest AS (
            SELECT
                vi.symbol,
                td.trade_date,
                vi.available_at,
                vi.raw_value
            FROM version_intervals vi
            JOIN trade_days td
              ON td.trade_date >= vi.available_at
             AND (vi.next_available_at IS NULL OR td.trade_date < vi.next_available_at)
            WHERE vi.raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM latest
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_dividend_rolling_quality_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT symbol
            FROM market_stock_dividend
            WHERE COALESCE(cash_div_tax, cash_div) IS NOT NULL
        ),
        history AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                MAX(div.ann_date) AS available_at,
                COUNT(DISTINCT div.end_date) FILTER (WHERE div.cash_div_value > 0.0) AS dividend_years,
                SUM(div.cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_sum,
                AVG(div.cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_avg,
                STDDEV_POP(cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_stdev,
                MAX(
                    CASE
                        WHEN div.end_date >= (td.trade_date - INTERVAL '18 months')::date
                         AND div.cash_div_value > 0.0
                        THEN 1
                        ELSE 0
                    END
                ) AS recent_positive_dividend
            FROM symbols
            JOIN trade_days td ON true
            LEFT JOIN LATERAL (
                SELECT
                    div.ann_date,
                    div.end_date,
                    COALESCE(div.cash_div_tax, div.cash_div)::double precision AS cash_div_value
                FROM market_stock_dividend div
                WHERE div.symbol = symbols.symbol
                  AND div.ann_date <= td.trade_date
                  AND div.end_date >= (td.trade_date - INTERVAL '4 years')::date
                  AND COALESCE(div.cash_div_tax, div.cash_div) IS NOT NULL
            ) div ON true
            GROUP BY symbols.symbol, td.trade_date
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                {value_expression} AS raw_value
            FROM history
            WHERE available_at IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_latest_backfill_sql(
    source_table: &'static str,
    value_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        ranked_events AS (
            SELECT
                event.symbol,
                event.available_at,
                {value_expression} AS raw_value,
                ROW_NUMBER() OVER (
                    PARTITION BY event.symbol, event.available_at
                    ORDER BY event.end_date DESC NULLS LAST, event.created_at DESC NULLS LAST
                ) AS event_rank
            FROM {source_table} event
            WHERE event.symbol IS NOT NULL
              AND event.available_at IS NOT NULL
              AND event.available_at <= $4
        ),
        deduped_events AS (
            SELECT
                symbol,
                available_at,
                raw_value
            FROM ranked_events
            WHERE event_rank = 1
        ),
        event_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at) AS next_available_at,
                raw_value
            FROM deduped_events
        ),
        latest AS (
            SELECT
                event_intervals.symbol,
                td.trade_date,
                event_intervals.available_at,
                event_intervals.raw_value
            FROM event_intervals
            JOIN trade_days td
              ON td.trade_date >= event_intervals.available_at
             AND td.trade_date < COALESCE(event_intervals.next_available_at, $4 + 1)
             AND td.trade_date BETWEEN $3 AND $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_block_trade_window_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.clamp(1, 120);
    let decay_days = decay_days.clamp(1, window_days);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.ts_code AS symbol,
                event.trade_date AS event_trade_date,
                event.available_at,
                event.source_row_no,
                event.created_at,
                event.price,
                event.vol,
                event.amount,
                event.buyer,
                event.seller,
                {value_expression} AS event_raw_value
            FROM market_stock_block_trade event
            LEFT JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = event.ts_code
             AND bar.trade_date = event.trade_date
            WHERE event.ts_code IS NOT NULL
              AND event.available_at IS NOT NULL
              AND event.available_at > event.trade_date
              AND event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                events.event_raw_value,
                GREATEST(
                    0.0,
                    1.0 - ((td.trade_date - events.available_at)::double precision / {decay_days}.0)
                ) AS decay_weight
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at
             AND td.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
              AND events.event_raw_value <> 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                MAX(available_at) AS available_at,
                SUM(event_raw_value * decay_weight) AS raw_value
            FROM expanded
            WHERE decay_weight > 0.0
            GROUP BY symbol, trade_date
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_unlock_pressure_backfill_sql(horizon_days: i32) -> String {
    let horizon_days = horizon_days.clamp(1, 365);
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        universe AS (
            SELECT DISTINCT
                universe.symbol,
                universe.trade_date
            FROM market_stock_daily_bar_adj universe
            JOIN trade_days td
              ON td.trade_date = universe.trade_date
            WHERE universe.symbol IS NOT NULL
        ),
        raw_pressure AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                COALESCE(SUM(COALESCE(event.float_ratio::double precision, 0.0)), 0.0) AS raw_unlock_ratio,
                COALESCE(MAX(event.available_at), universe.trade_date) AS available_at
            FROM universe
            LEFT JOIN market_stock_share_float event
              ON event.symbol = universe.symbol
             AND event.available_at <= universe.trade_date
             AND event.float_date >= universe.trade_date
             AND event.float_date <= universe.trade_date + INTERVAL '{horizon_days} days'
            GROUP BY universe.symbol, universe.trade_date
        ),
        scored AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                -raw_unlock_ratio AS raw_value
            FROM raw_pressure
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)
                END AS normalized_value
            FROM scored
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_prosperity_backfill_sql(
    signal: IndustryProsperitySignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    let warmup_days = (long_window * 3).max(240);
    let min_members = 20;
    let raw_value_expression = match signal {
        IndustryProsperitySignal::ReturnMomentum => {
            "industry_ret_short - industry_ret_long AS raw_value"
        }
        IndustryProsperitySignal::PositiveBreadth => "positive_breadth_short AS raw_value",
        IndustryProsperitySignal::AmountTrend => {
            "LN((industry_amount_short + 1.0) / (industry_amount_long + 1.0)) AS raw_value"
        }
    };
    let raw_filter = match signal {
        IndustryProsperitySignal::ReturnMomentum => {
            format!(
                "ret_short_member_count >= {min_members}
              AND ret_long_member_count >= {min_members}
              AND industry_ret_short IS NOT NULL
              AND industry_ret_long IS NOT NULL"
            )
        }
        IndustryProsperitySignal::PositiveBreadth => {
            format!(
                "ret_short_member_count >= {min_members}
              AND positive_breadth_short IS NOT NULL"
            )
        }
        IndustryProsperitySignal::AmountTrend => {
            format!(
                "amount_member_count >= {min_members}
              AND industry_amount_short IS NOT NULL
              AND industry_amount_long IS NOT NULL"
            )
        }
    };

    format!(
        "WITH eligible_universe AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.close > 0
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_status = 'L'
              AND COALESCE(ms.is_st, false) = false
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        universe AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                universe.close,
                universe.amount,
                membership.index_code,
                membership.industry_name
            FROM eligible_universe universe
            JOIN market_stock_industry_membership_pit membership
              ON membership.symbol = universe.symbol
             AND membership.industry_level = 'L1'
             AND membership.classification_source = CASE
                 WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
             AND membership.available_at <= universe.trade_date
             AND membership.in_date <= universe.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > universe.trade_date
             )
        ),
        stock_roll AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                close,
                amount,
                LAG(close, {short_window}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_short,
                LAG(close, {long_window}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_long,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_short,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_long,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_short,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_long
            FROM universe
        ),
        stock_features AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                CASE
                    WHEN prev_close_short > 0.0 AND close > 0.0
                    THEN close / prev_close_short - 1.0
                    ELSE NULL
                END AS ret_short,
                CASE
                    WHEN prev_close_long > 0.0 AND close > 0.0
                    THEN close / prev_close_long - 1.0
                    ELSE NULL
                END AS ret_long,
                CASE
                    WHEN amount_obs_count_short = {short_window} THEN amount_short
                    ELSE NULL
                END AS amount_short,
                CASE
                    WHEN amount_obs_count_long = {long_window} THEN amount_long
                    ELSE NULL
                END AS amount_long
            FROM stock_roll
        ),
        industry_daily AS (
            SELECT
                index_code,
                industry_name,
                trade_date,
                AVG(ret_short) FILTER (WHERE ret_short IS NOT NULL) AS industry_ret_short,
                AVG(ret_long) FILTER (WHERE ret_long IS NOT NULL) AS industry_ret_long,
                AVG(
                    CASE
                        WHEN ret_short IS NULL THEN NULL
                        WHEN ret_short > 0.0 THEN 1.0
                        ELSE 0.0
                    END
                ) AS positive_breadth_short,
                AVG(amount_short) FILTER (WHERE amount_short IS NOT NULL) AS industry_amount_short,
                AVG(amount_long) FILTER (WHERE amount_long IS NOT NULL) AS industry_amount_long,
                COUNT(*) FILTER (WHERE ret_short IS NOT NULL) AS ret_short_member_count,
                COUNT(*) FILTER (WHERE ret_long IS NOT NULL) AS ret_long_member_count,
                COUNT(*) FILTER (
                    WHERE amount_short IS NOT NULL AND amount_long IS NOT NULL
                ) AS amount_member_count
            FROM stock_features
            GROUP BY index_code, industry_name, trade_date
        ),
        raw AS (
            SELECT
                stock_features.symbol,
                stock_features.trade_date,
                stock_features.trade_date AS available_at,
                {raw_value_expression}
            FROM stock_features
            JOIN industry_daily
              ON industry_daily.index_code = stock_features.index_code
             AND industry_daily.trade_date = stock_features.trade_date
            WHERE stock_features.trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_prosperity_multi_backfill_sql() -> String {
    let warmup_days = 360;
    let min_members = 20;
    format!(
        "WITH eligible_universe AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $6
              AND bar.trade_date >= ($5::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.close > 0
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_status = 'L'
              AND COALESCE(ms.is_st, false) = false
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        universe AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                universe.close,
                universe.amount,
                membership.index_code,
                membership.industry_name
            FROM eligible_universe universe
            JOIN market_stock_industry_membership_pit membership
              ON membership.symbol = universe.symbol
             AND membership.industry_level = 'L1'
             AND membership.classification_source = CASE
                 WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
             AND membership.available_at <= universe.trade_date
             AND membership.in_date <= universe.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > universe.trade_date
             )
        ),
        stock_roll AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                close,
                amount,
                LAG(close, 20) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_20,
                LAG(close, 60) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_60,
                LAG(close, 120) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_120,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_20,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS amount_120,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_20,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_120
            FROM universe
        ),
        stock_features AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                CASE
                    WHEN prev_close_20 > 0.0 AND close > 0.0
                    THEN close / prev_close_20 - 1.0
                    ELSE NULL
                END AS ret_20,
                CASE
                    WHEN prev_close_60 > 0.0 AND close > 0.0
                    THEN close / prev_close_60 - 1.0
                    ELSE NULL
                END AS ret_60,
                CASE
                    WHEN prev_close_120 > 0.0 AND close > 0.0
                    THEN close / prev_close_120 - 1.0
                    ELSE NULL
                END AS ret_120,
                CASE
                    WHEN amount_obs_count_20 = 20 THEN amount_20
                    ELSE NULL
                END AS amount_20,
                CASE
                    WHEN amount_obs_count_120 = 120 THEN amount_120
                    ELSE NULL
                END AS amount_120
            FROM stock_roll
        ),
        industry_daily AS (
            SELECT
                index_code,
                industry_name,
                trade_date,
                AVG(ret_20) FILTER (WHERE ret_20 IS NOT NULL) AS industry_ret_20,
                AVG(ret_120) FILTER (WHERE ret_120 IS NOT NULL) AS industry_ret_120,
                AVG(
                    CASE
                        WHEN ret_60 IS NULL THEN NULL
                        WHEN ret_60 > 0.0 THEN 1.0
                        ELSE 0.0
                    END
                ) AS positive_breadth_60,
                AVG(amount_20) FILTER (WHERE amount_20 IS NOT NULL) AS industry_amount_20,
                AVG(amount_120) FILTER (WHERE amount_120 IS NOT NULL) AS industry_amount_120,
                COUNT(*) FILTER (WHERE ret_20 IS NOT NULL) AS ret_20_member_count,
                COUNT(*) FILTER (WHERE ret_60 IS NOT NULL) AS ret_60_member_count,
                COUNT(*) FILTER (WHERE ret_120 IS NOT NULL) AS ret_120_member_count,
                COUNT(*) FILTER (
                    WHERE amount_20 IS NOT NULL AND amount_120 IS NOT NULL
                ) AS amount_member_count
            FROM stock_features
            GROUP BY index_code, industry_name, trade_date
        ),
        raw AS (
            SELECT
                stock_features.symbol,
                stock_features.trade_date,
                stock_features.trade_date AS available_at,
                signals.factor_code,
                signals.raw_value
            FROM stock_features
            JOIN industry_daily
              ON industry_daily.index_code = stock_features.index_code
             AND industry_daily.trade_date = stock_features.trade_date
            CROSS JOIN LATERAL (
                VALUES
                    (
                        $1::varchar,
                        CASE
                            WHEN ret_20_member_count >= {min_members}
                             AND ret_120_member_count >= {min_members}
                             AND industry_ret_20 IS NOT NULL
                             AND industry_ret_120 IS NOT NULL
                            THEN industry_ret_20 - industry_ret_120
                            ELSE NULL
                        END
                    ),
                    (
                        $2::varchar,
                        CASE
                            WHEN ret_60_member_count >= {min_members}
                             AND positive_breadth_60 IS NOT NULL
                            THEN positive_breadth_60
                            ELSE NULL
                        END
                    ),
                    (
                        $3::varchar,
                        CASE
                            WHEN amount_member_count >= {min_members}
                             AND industry_amount_20 IS NOT NULL
                             AND industry_amount_120 IS NOT NULL
                            THEN LN((industry_amount_20 + 1.0) / (industry_amount_120 + 1.0))
                            ELSE NULL
                        END
                    )
            ) AS signals(factor_code, raw_value)
            WHERE stock_features.trade_date BETWEEN $5 AND $6
        ),
        ranked AS (
            SELECT
                factor_code,
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                ) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        ),
        inserted AS (
            INSERT INTO factor_value
                (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
            SELECT factor_code, $4, symbol, trade_date, raw_value, normalized_value, available_at
            FROM ranked
            ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value,
                normalized_value = EXCLUDED.normalized_value,
                available_at = EXCLUDED.available_at,
                created_at = NOW()
            RETURNING factor_code
        )
        SELECT factor_code, COUNT(*)::int8 AS row_count
        FROM inserted
        GROUP BY factor_code
        ORDER BY factor_code"
    )
}

pub(crate) fn phase7_futures_price_chain_backfill_sql(_signal: FuturesPriceChainSignal) -> String {
    panic!("futures_price_chain factors must use the shared multi-signal builder to avoid repeated raw-table scans")
}

pub(crate) fn phase7_futures_price_chain_product_signal_backfill_sql() -> &'static str {
    "WITH daily_raw_symbol AS (
        SELECT
            ts_code,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\\.')) AS product_symbol_raw,
            trade_date,
            available_at,
            close::double precision AS close,
            amount::double precision AS amount,
            oi::double precision AS oi
        FROM market_futures_daily
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND close IS NOT NULL
          AND close > 0
          AND substring(ts_code from '^([A-Za-z]+)[0-9]{4}\\.') IS NOT NULL
    ),
    daily_contracts AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            available_at,
            close,
            amount,
            oi,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CASE
                        WHEN product_symbol_raw = 'PTA' THEN 'TA'
                        WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                        THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                        WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                        THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                        ELSE product_symbol_raw
                    END,
                    trade_date
                ORDER BY COALESCE(amount, 0.0) DESC, COALESCE(oi, 0.0) DESC, ts_code
            ) AS contract_rank
        FROM daily_raw_symbol
    ),
    main_contract AS (
        SELECT product_symbol, trade_date, available_at, close
        FROM daily_contracts
        WHERE contract_rank = 1
          AND product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    price_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            close,
            LAG(close, 20) OVER (PARTITION BY product_symbol ORDER BY trade_date) AS close_20,
            LAG(close, 60) OVER (PARTITION BY product_symbol ORDER BY trade_date) AS close_60
        FROM main_contract
    ),
    price_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN close_20 > 0.0 AND close_60 > 0.0
                THEN (close / close_20 - 1.0) - (close / close_60 - 1.0)
                ELSE NULL
            END AS raw_value
        FROM price_roll
    ),
    wsr_raw_symbol AS (
        SELECT
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            vol::double precision AS vol
        FROM market_futures_warehouse_receipt
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND vol IS NOT NULL
          AND vol >= 0
          AND substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    wsr_daily AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(vol) AS inventory_vol
        FROM wsr_raw_symbol
        GROUP BY 1, trade_date
    ),
    wsr_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            AVG(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS inventory_20,
            AVG(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS inventory_60,
            COUNT(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS inventory_obs_20,
            COUNT(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS inventory_obs_60
        FROM wsr_daily
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    inventory_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN inventory_obs_20 = 20
                 AND inventory_obs_60 = 60
                 AND inventory_20 IS NOT NULL
                 AND inventory_60 IS NOT NULL
                THEN -LN((inventory_20 + 1.0) / (inventory_60 + 1.0))
                ELSE NULL
            END AS raw_value
        FROM wsr_roll
    ),
    holding_raw_symbol AS (
        SELECT
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            long_hld::double precision AS long_hld,
            short_hld::double precision AS short_hld
        FROM market_futures_holding_rank
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND (long_hld IS NOT NULL OR short_hld IS NOT NULL)
    ),
    holding_daily AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(COALESCE(long_hld, 0.0)) AS long_hld,
            SUM(COALESCE(short_hld, 0.0)) AS short_hld
        FROM holding_raw_symbol
        GROUP BY 1, trade_date
    ),
    holding_features AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN long_hld + short_hld > 0.0
                THEN (long_hld - short_hld) / NULLIF(long_hld + short_hld, 0.0)
                ELSE NULL
            END AS net_position_ratio
        FROM holding_daily
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    holding_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            AVG(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS net_ratio_20,
            AVG(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS net_ratio_60,
            COUNT(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS net_obs_20,
            COUNT(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS net_obs_60
        FROM holding_features
    ),
    holding_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN net_obs_20 = 20
                 AND net_obs_60 = 60
                 AND net_ratio_20 IS NOT NULL
                 AND net_ratio_60 IS NOT NULL
                THEN net_ratio_20 - net_ratio_60
                ELSE NULL
            END AS raw_value
        FROM holding_roll
    ),
    product_signal AS (
        SELECT $1::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM price_signal
        WHERE raw_value IS NOT NULL
        UNION ALL
        SELECT $2::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM inventory_signal
        WHERE raw_value IS NOT NULL
        UNION ALL
        SELECT $3::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM holding_signal
        WHERE raw_value IS NOT NULL
    ),
    inserted AS (
        INSERT INTO market_futures_product_signal_pit
            (signal_code, source_version, product_symbol, trade_date, available_at, raw_value)
        SELECT factor_code, $4, product_symbol, trade_date, available_at, raw_value
        FROM product_signal
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
          AND trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at IS NOT NULL
          AND available_at >= trade_date
          AND available_at <= ($6::date + INTERVAL '7 days')
        ON CONFLICT (signal_code, source_version, product_symbol, trade_date) DO UPDATE SET
            available_at = EXCLUDED.available_at,
            raw_value = EXCLUDED.raw_value,
            updated_at = NOW()
        RETURNING signal_code
    )
    SELECT signal_code, COUNT(*)::int8 AS row_count
    FROM inserted
    GROUP BY signal_code
    ORDER BY signal_code"
}

pub(crate) fn phase7_futures_price_chain_combo_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    stock_membership AS MATERIALIZED (
        SELECT
            universe.symbol,
            universe.trade_date,
            membership.index_code,
            membership.available_at AS membership_available_at
        FROM eligible_universe universe
        JOIN market_stock_industry_membership_pit membership
          ON membership.symbol = universe.symbol
         AND membership.industry_level = 'L1'
         AND membership.classification_source = CASE
             WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
             ELSE 'SW2021'
         END
         AND membership.available_at <= universe.trade_date
         AND membership.in_date <= universe.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > universe.trade_date
         )
    ),
    signal_intervals AS (
        SELECT
            signal_code AS factor_code,
            product_symbol,
            trade_date,
            available_at,
            LEAD(available_at) OVER (
                PARTITION BY signal_code, product_symbol ORDER BY available_at, trade_date
            ) AS next_available_at,
            raw_value
        FROM market_futures_product_signal_pit
        WHERE source_version = $2
          AND signal_code IN (SELECT factor_code FROM weights)
          AND trade_date >= ($4::date - INTERVAL '180 days')
          AND trade_date <= $5
          AND available_at <= $5
          AND raw_value IS NOT NULL
          AND product_symbol IS NOT NULL
          AND product_symbol <> ''
          AND available_at >= trade_date
    ),
    signal_window AS (
        SELECT *
        FROM signal_intervals
        WHERE available_at IS NOT NULL
          AND available_at <= $5
    ),
    industry_signal AS MATERIALIZED (
        SELECT
            signal_window.factor_code,
            td.trade_date,
            mapping.exposure_code AS index_code,
            GREATEST(MAX(signal_window.available_at), MAX(mapping.available_at)) AS available_at,
            SUM(signal_window.raw_value * mapping.direction::double precision * mapping.weight::double precision)
                / NULLIF(SUM(ABS(mapping.weight::double precision)), 0.0) AS raw_value
        FROM stock_days td
        JOIN signal_window
          ON td.trade_date >= signal_window.available_at
         AND td.trade_date < COALESCE(signal_window.next_available_at, ($5::date + INTERVAL '1 day'))
        JOIN market_futures_product_exposure_mapping_pit mapping
          ON upper(mapping.product_symbol) = signal_window.product_symbol
         AND mapping.exposure_type = 'sw_industry'
         AND mapping.available_at <= td.trade_date
         AND mapping.valid_from <= signal_window.trade_date
         AND (
             mapping.valid_to IS NULL
             OR mapping.valid_to >= signal_window.trade_date
         )
        WHERE td.trade_date BETWEEN $4 AND $5
        GROUP BY signal_window.factor_code, td.trade_date, mapping.exposure_code
    ),
    raw AS (
        SELECT
            stock_membership.symbol,
            stock_membership.trade_date,
            GREATEST(industry_signal.available_at, stock_membership.membership_available_at) AS available_at,
            industry_signal.factor_code,
            industry_signal.raw_value
        FROM stock_membership
        JOIN industry_signal
          ON industry_signal.index_code = stock_membership.index_code
         AND industry_signal.trade_date = stock_membership.trade_date
        WHERE industry_signal.raw_value IS NOT NULL
          AND industry_signal.available_at <= stock_membership.trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_value
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.normalized_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
        HAVING COUNT(DISTINCT ranked.factor_code) >= $6
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_equity_pledge_pressure_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key = 'eq_pledge_low_ratio_std'
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    latest_stat AS MATERIALIZED (
        SELECT
            universe.symbol,
            universe.trade_date,
            stat.available_at,
            COALESCE(
                stat.pledge_ratio::double precision,
                (
                    (COALESCE(stat.unrest_pledge, 0)::double precision
                     + COALESCE(stat.rest_pledge, 0)::double precision)
                    / NULLIF(stat.total_share::double precision, 0.0)
                ) * 100.0
            ) AS pledge_ratio
        FROM eligible_universe universe
        JOIN LATERAL (
            SELECT
                stat.end_date,
                stat.available_at,
                stat.pledge_ratio,
                stat.unrest_pledge,
                stat.rest_pledge,
                stat.total_share
            FROM market_stock_pledge_stat stat
            WHERE stat.symbol = universe.symbol
              AND stat.available_at <= universe.trade_date
              AND stat.end_date <= universe.trade_date
            ORDER BY stat.available_at DESC, stat.end_date DESC
            LIMIT 1
        ) stat ON true
    ),
    raw AS (
        SELECT
            symbol,
            trade_date,
            available_at,
            'eq_pledge_low_ratio_std'::varchar AS factor_code,
            -pledge_ratio AS raw_value
        FROM latest_stat
        WHERE pledge_ratio BETWEEN 0.0 AND 100.0
          AND available_at <= trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_score
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.raw_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            SUM(ranked.normalized_score::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS normalized_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_shareholder_structure_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key IN (
            'sh_holder_count_decline_1y_std',
            'sh_holder_count_decline_prev_std',
            'sh_holder_trade_net_increase_120d_std'
        )
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    holder_number_candidates AS MATERIALIZED (
        SELECT DISTINCT ON (hn.symbol)
            hn.symbol,
            hn.available_at,
            hn.end_date,
            hn.holder_num::double precision AS holder_num,
            hn.source_row_hash
        FROM market_stock_holder_number hn
        WHERE hn.available_at < $4
          AND hn.available_at <= $5
          AND hn.available_at >= hn.ann_date
          AND hn.available_at >= hn.end_date
          AND hn.holder_num IS NOT NULL
          AND hn.holder_num > 0
        ORDER BY hn.symbol, hn.available_at DESC, hn.end_date DESC, hn.source_row_hash DESC
    ),
    holder_number_segment_events AS MATERIALIZED (
        SELECT
            hn.symbol,
            hn.available_at,
            hn.end_date,
            hn.holder_num::double precision AS holder_num,
            hn.source_row_hash
        FROM market_stock_holder_number hn
        WHERE hn.available_at >= $4
          AND hn.available_at <= $5
          AND hn.available_at >= hn.ann_date
          AND hn.available_at >= hn.end_date
          AND hn.holder_num IS NOT NULL
          AND hn.holder_num > 0
    ),
    holder_number_base AS MATERIALIZED (
        SELECT symbol, available_at, end_date, holder_num, source_row_hash
        FROM holder_number_candidates
        UNION ALL
        SELECT symbol, available_at, end_date, holder_num, source_row_hash
        FROM holder_number_segment_events
    ),
    holder_number_events AS MATERIALIZED (
        SELECT
            base.symbol,
            base.available_at,
            base.end_date,
            LEAD(base.available_at) OVER (
                PARTITION BY base.symbol
                ORDER BY base.available_at, base.end_date, base.source_row_hash
            ) AS next_available_at,
            base.holder_num,
            prev.holder_num AS prev_holder_num,
            prior_yoy.holder_num AS prior_yoy_holder_num
        FROM holder_number_base base
        LEFT JOIN LATERAL (
            SELECT hn.holder_num::double precision AS holder_num
            FROM market_stock_holder_number hn
            WHERE hn.symbol = base.symbol
              AND hn.available_at <= base.available_at
              AND hn.available_at >= hn.ann_date
              AND hn.available_at >= hn.end_date
              AND hn.holder_num IS NOT NULL
              AND hn.holder_num > 0
              AND (
                  hn.end_date < base.end_date
                  OR (
                      hn.end_date = base.end_date
                      AND hn.available_at < base.available_at
                  )
              )
            ORDER BY hn.end_date DESC, hn.available_at DESC, hn.source_row_hash DESC
            LIMIT 1
        ) prev ON true
        LEFT JOIN LATERAL (
            SELECT hn.holder_num::double precision AS holder_num
            FROM market_stock_holder_number hn
            WHERE hn.symbol = base.symbol
              AND hn.end_date <= base.end_date - INTERVAL '300 days'
              AND hn.available_at <= base.available_at
              AND hn.available_at >= hn.ann_date
              AND hn.available_at >= hn.end_date
              AND hn.holder_num IS NOT NULL
              AND hn.holder_num > 0
            ORDER BY hn.end_date DESC, hn.available_at DESC, hn.source_row_hash DESC
            LIMIT 1
        ) prior_yoy ON true
    ),
    holder_number_raw AS (
        SELECT
            universe.symbol,
            universe.trade_date,
            event.available_at,
            factor.factor_code,
            factor.raw_value
        FROM eligible_universe universe
        JOIN holder_number_events event
          ON event.symbol = universe.symbol
         AND event.available_at <= universe.trade_date
         AND universe.trade_date < COALESCE(event.next_available_at, DATE '9999-12-31')
        CROSS JOIN LATERAL (
            VALUES
                (
                    'sh_holder_count_decline_prev_std'::varchar,
                    CASE
                        WHEN event.prev_holder_num > 0
                        THEN -1.0 * ((event.holder_num / NULLIF(event.prev_holder_num, 0.0)) - 1.0)
                        ELSE NULL
                    END
                ),
                (
                    'sh_holder_count_decline_1y_std'::varchar,
                    CASE
                        WHEN event.prior_yoy_holder_num > 0
                        THEN -1.0 * ((event.holder_num / NULLIF(event.prior_yoy_holder_num, 0.0)) - 1.0)
                        ELSE NULL
                    END
                )
        ) AS factor(factor_code, raw_value)
        WHERE event.available_at <= universe.trade_date
    ),
    holder_trade_raw AS (
        SELECT
            universe.symbol,
            universe.trade_date,
            MAX(trade.available_at) AS available_at,
            'sh_holder_trade_net_increase_120d_std'::varchar AS factor_code,
            SUM(
                CASE
                    WHEN UPPER(COALESCE(trade.in_de, '')) = 'IN'
                        THEN ABS(COALESCE(trade.change_ratio::double precision, 0.0))
                    WHEN UPPER(COALESCE(trade.in_de, '')) = 'DE'
                        THEN -ABS(COALESCE(trade.change_ratio::double precision, 0.0))
                    ELSE COALESCE(trade.change_ratio::double precision, 0.0)
                END
            ) AS raw_value
        FROM eligible_universe universe
        JOIN market_stock_holder_trade trade
          ON trade.symbol = universe.symbol
         AND trade.available_at <= universe.trade_date
         AND trade.available_at > universe.trade_date - INTERVAL '120 days'
        WHERE trade.available_at >= trade.ann_date
          AND (trade.change_ratio IS NULL OR (trade.change_ratio >= -100 AND trade.change_ratio <= 100))
          AND (trade.after_ratio IS NULL OR (trade.after_ratio >= 0 AND trade.after_ratio <= 100))
          AND (trade.begin_date IS NULL OR trade.close_date IS NULL OR trade.close_date >= trade.begin_date)
        GROUP BY universe.symbol, universe.trade_date
    ),
    raw AS (
        SELECT symbol, trade_date, available_at, factor_code, raw_value
        FROM holder_number_raw
        UNION ALL
        SELECT symbol, trade_date, available_at, factor_code, raw_value
        FROM holder_trade_raw
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_score
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.raw_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            SUM(ranked.normalized_score::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS normalized_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_margin_detail_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key IN (
            'md_financing_buy_intensity_20d_std',
            'md_financing_balance_chg_20d_std',
            'md_short_sell_pressure_relief_20d_std'
        )
    ),
    weight_params AS (
        SELECT
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_financing_buy_intensity_20d_std'
                ),
                0.0
            ) AS financing_buy_weight,
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_financing_balance_chg_20d_std'
                ),
                0.0
            ) AS financing_balance_weight,
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_short_sell_pressure_relief_20d_std'
                ),
                0.0
            ) AS short_sell_relief_weight
        FROM weights
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    margin_observations AS MATERIALIZED (
        SELECT
            md.symbol,
            md.trade_date,
            md.available_at,
            md.rzmre::double precision AS rzmre,
            md.rzye::double precision AS rzye,
            md.rqmcl::double precision AS rqmcl,
            bar.volume::double precision AS volume,
            bar.amount::double precision AS amount,
            basic.circ_mv::double precision AS circ_mv
        FROM market_stock_margin_detail md
        JOIN market_stock_daily_bar bar
          ON bar.symbol = md.symbol
         AND bar.trade_date = md.trade_date
        JOIN market_stock_daily_basic basic
          ON basic.symbol = md.symbol
         AND basic.trade_date = md.trade_date
        WHERE md.trade_date >= ($4::date - INTERVAL '90 days')
          AND md.trade_date <= $5
          AND md.available_at <= $5
          AND md.available_at > md.trade_date
          AND md.available_at IS NOT NULL
          AND md.rzye IS NOT NULL
          AND md.rzmre IS NOT NULL
          AND bar.amount IS NOT NULL
          AND bar.amount > 0
          AND bar.volume IS NOT NULL
          AND bar.volume > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
    ),
    rolling AS MATERIALIZED (
        SELECT
            symbol,
            trade_date,
            available_at,
            SUM(md.rzmre::double precision) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                SUM(md.amount::double precision) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ),
                0.0
            ) AS financing_buy_intensity_20d,
            (
                rzye::double precision
                - LAG(md.rzye::double precision, 20) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                )
            ) / NULLIF(circ_mv::double precision, 0.0) AS financing_balance_chg_20d,
            -SUM(COALESCE(md.rqmcl::double precision, 0.0)) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                SUM(volume::double precision) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ),
                0.0
            ) AS short_sell_pressure_relief_20d,
            COUNT(md.rzmre) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS obs_count
        FROM margin_observations md
    ),
    latest_signals AS (
        SELECT DISTINCT ON (universe.symbol, universe.trade_date)
            universe.symbol,
            universe.trade_date,
            rolling.available_at,
            rolling.financing_buy_intensity_20d,
            rolling.financing_balance_chg_20d,
            rolling.short_sell_pressure_relief_20d,
            GREATEST(
                0,
                (CASE WHEN rolling.financing_buy_intensity_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.financing_balance_chg_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.short_sell_pressure_relief_20d IS NOT NULL THEN 1 ELSE 0 END)
            ) AS valid_signal_count
        FROM eligible_universe universe
        JOIN rolling
          ON rolling.symbol = universe.symbol
         AND rolling.available_at = universe.trade_date
        WHERE rolling.obs_count >= 20
          AND rolling.available_at <= universe.trade_date
          AND GREATEST(
                0,
                (CASE WHEN rolling.financing_buy_intensity_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.financing_balance_chg_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.short_sell_pressure_relief_20d IS NOT NULL THEN 1 ELSE 0 END)
          ) >= 3
        ORDER BY universe.symbol, universe.trade_date, rolling.available_at DESC
    ),
    ranked_signals AS (
        SELECT
            symbol,
            trade_date,
            available_at,
            financing_buy_intensity_20d,
            financing_balance_chg_20d,
            short_sell_pressure_relief_20d,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY financing_buy_intensity_20d
                )
            END AS financing_buy_intensity_rank,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY financing_balance_chg_20d
                )
            END AS financing_balance_chg_rank,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY short_sell_pressure_relief_20d
                )
            END AS short_sell_pressure_relief_rank
        FROM latest_signals
        WHERE valid_signal_count >= 3
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked_signals.symbol,
            ranked_signals.trade_date,
            (
                ranked_signals.financing_buy_intensity_20d
                    * weight_params.financing_buy_weight
                + ranked_signals.financing_balance_chg_20d
                    * weight_params.financing_balance_weight
                + ranked_signals.short_sell_pressure_relief_20d
                    * weight_params.short_sell_relief_weight
            ) / NULLIF(
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight,
                0.0
            ) AS raw_score,
            (
                ranked_signals.financing_buy_intensity_rank
                    * weight_params.financing_buy_weight
                + ranked_signals.financing_balance_chg_rank
                    * weight_params.financing_balance_weight
                + ranked_signals.short_sell_pressure_relief_rank
                    * weight_params.short_sell_relief_weight
            ) / NULLIF(
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight,
                0.0
            ) AS normalized_score,
            ranked_signals.available_at AS available_at
        FROM ranked_signals
        CROSS JOIN weight_params
        WHERE (
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight
            ) > 0.0
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_analyst_revision_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let decay_days = decay_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        eligible_universe AS MATERIALIZED (
            SELECT
                bar.symbol,
                bar.trade_date
            FROM trade_days td
            JOIN market_stock_daily_bar bar
              ON bar.trade_date = td.trade_date
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.close IS NOT NULL
              AND bar.close > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_date IS NOT NULL
              AND ms.list_date <= bar.trade_date
              AND (
                  ms.delist_date IS NULL
                  OR ms.delist_date >= bar.trade_date
              )
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
              AND NOT EXISTS (
                  SELECT 1
                  FROM market_stock_name_history st_name
                  WHERE st_name.symbol = bar.symbol
                    AND st_name.is_st = true
                    AND st_name.start_date <= bar.trade_date
                    AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
              )
        ),
        raw_events AS MATERIALIZED (
            SELECT
                ms.symbol,
                raw.available_at,
                raw.publication_date,
                CASE
                    WHEN raw.rating_change = '调高' THEN 1.0
                    WHEN raw.rating_change = '调低' THEN -1.0
                    ELSE NULL
                END AS rating_change_score,
                CASE
                    WHEN raw.is_first_rating = '是首次评级'
                     AND COALESCE(raw.rating_current, '') ~* '(买入|增持|推荐|强烈推荐|谨慎买入|谨慎增持|审慎推荐|BUY|OVERWEIGHT)'
                    THEN 1.0
                    WHEN raw.is_first_rating = '是首次评级' THEN 0.0
                    ELSE NULL
                END AS bullish_first_rating_score
            FROM market_vendor_analyst_revision_raw raw
            JOIN market_stock ms
              ON LEFT(ms.symbol, 6) = raw.symbol
            WHERE raw.vendor = 'akshare'
              AND raw.vendor_endpoint = 'stock_rank_forecast_cninfo'
              AND raw.available_at IS NOT NULL
              AND raw.available_at <= $4
              AND raw.available_at >= ($3::date - INTERVAL '{window_days} days')
              AND raw.available_at >= raw.publication_date
              AND raw.source_published_at IS NOT NULL
              AND raw.rating_previous IS NOT NULL
              AND raw.rating_change IS NOT NULL
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        events AS (
            SELECT
                symbol,
                available_at,
                {value_expression} AS event_raw_value
            FROM raw_events
        ),
        expanded AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                events.available_at,
                events.event_raw_value,
                GREATEST(
                    0.0,
                    1.0 - ((universe.trade_date - events.available_at)::double precision / {decay_days}.0)
                ) AS decay_weight
            FROM eligible_universe universe
            JOIN events
              ON events.symbol = universe.symbol
             AND universe.trade_date >= events.available_at
             AND universe.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
              AND events.available_at <= universe.trade_date
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                MAX(available_at) AS available_at,
                SUM(event_raw_value * decay_weight) AS raw_value
            FROM expanded
            WHERE decay_weight > 0.0
            GROUP BY symbol, trade_date
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (
                        PARTITION BY trade_date ORDER BY {rank_order}
                    )
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
              AND available_at <= trade_date
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_analyst_revision_multi_backfill_sql() -> String {
    "WITH trade_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $6 AND $7
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM trade_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    raw_events AS MATERIALIZED (
        SELECT
            ms.symbol,
            raw.available_at,
            raw.publication_date,
            CASE
                WHEN raw.rating_change = '调高' THEN 1.0
                WHEN raw.rating_change = '调低' THEN -1.0
                ELSE NULL
            END AS rating_change_score,
            CASE
                WHEN raw.is_first_rating = '是首次评级'
                 AND COALESCE(raw.rating_current, '') ~* '(买入|增持|推荐|强烈推荐|谨慎买入|谨慎增持|审慎推荐|BUY|OVERWEIGHT)'
                THEN 1.0
                WHEN raw.is_first_rating = '是首次评级' THEN 0.0
                ELSE NULL
            END AS bullish_first_rating_score
        FROM market_vendor_analyst_revision_raw raw
        JOIN market_stock ms
          ON LEFT(ms.symbol, 6) = raw.symbol
        WHERE raw.vendor = 'akshare'
          AND raw.vendor_endpoint = 'stock_rank_forecast_cninfo'
          AND raw.available_at IS NOT NULL
          AND raw.available_at <= $7
          AND raw.available_at >= ($6::date - INTERVAL '60 days')
          AND raw.available_at >= raw.publication_date
          AND raw.source_published_at IS NOT NULL
          AND raw.rating_previous IS NOT NULL
          AND raw.rating_change IS NOT NULL
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
    ),
    events AS MATERIALIZED (
        SELECT $1::varchar AS factor_code,
               symbol,
               available_at,
               rating_change_score AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score IS NOT NULL
        UNION ALL
        SELECT $2::varchar AS factor_code,
               symbol,
               available_at,
               1.0 AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score > 0.0
        UNION ALL
        SELECT $3::varchar AS factor_code,
               symbol,
               available_at,
               -1.0 AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score < 0.0
        UNION ALL
        SELECT $4::varchar AS factor_code,
               symbol,
               available_at,
               bullish_first_rating_score AS event_raw_value,
               60::int AS window_days,
               60::int AS decay_days
        FROM raw_events
        WHERE bullish_first_rating_score IS NOT NULL
    ),
    expanded AS (
        SELECT
            events.factor_code,
            universe.symbol,
            universe.trade_date,
            events.available_at,
            events.event_raw_value,
            GREATEST(
                0.0,
                1.0 - ((universe.trade_date - events.available_at)::double precision / events.decay_days::double precision)
            ) AS decay_weight
        FROM eligible_universe universe
        JOIN events
          ON events.symbol = universe.symbol
         AND universe.trade_date >= events.available_at
         AND universe.trade_date <= events.available_at + events.window_days * INTERVAL '1 day'
        WHERE events.event_raw_value IS NOT NULL
          AND events.available_at <= universe.trade_date
    ),
    raw AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(event_raw_value * decay_weight) AS raw_value
        FROM expanded
        WHERE decay_weight > 0.0
        GROUP BY factor_code, symbol, trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_value
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    deleted AS (
        DELETE FROM factor_value
        WHERE factor_code IN ($1, $2, $3, $4)
          AND factor_version = $5
          AND trade_date BETWEEN $6 AND $7
        RETURNING 1
    ),
    upserted AS (
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT factor_code, $5, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()
        RETURNING factor_code
    )
    SELECT factor_code, COUNT(*)::int8 AS row_count
    FROM upserted
    GROUP BY factor_code
    ORDER BY factor_code"
        .to_string()
}

pub(crate) fn phase7_forecast_revision_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    max_event_age_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let max_event_age_days = max_event_age_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        revision_events AS (
            SELECT
                latest.symbol,
                GREATEST(latest.available_at, previous.available_at) AS available_at,
                latest.end_date,
                latest.created_at,
                {value_expression} AS raw_value
            FROM market_stock_forecast latest
            JOIN LATERAL (
                SELECT
                    previous.available_at,
                    previous.ann_date,
                    previous.end_date,
                    previous.forecast_type,
                    previous.p_change_min,
                    previous.p_change_max,
                    previous.net_profit_min,
                    previous.net_profit_max,
                    previous.created_at
                FROM market_stock_forecast previous
                WHERE previous.symbol = latest.symbol
                  AND previous.end_date = latest.end_date
                  AND previous.available_at < latest.available_at
                  AND previous.available_at IS NOT NULL
                  AND (
                      previous.p_change_min IS NOT NULL
                      OR previous.p_change_max IS NOT NULL
                      OR previous.net_profit_min IS NOT NULL
                      OR previous.net_profit_max IS NOT NULL
                      OR previous.forecast_type IS NOT NULL
                  )
                ORDER BY previous.available_at DESC, previous.created_at DESC NULLS LAST
                LIMIT 1
            ) previous ON true
            WHERE latest.symbol IS NOT NULL
              AND latest.available_at IS NOT NULL
              AND latest.available_at <= $4
              AND (
                  latest.p_change_min IS NOT NULL
                  OR latest.p_change_max IS NOT NULL
                  OR latest.net_profit_min IS NOT NULL
                  OR latest.net_profit_max IS NOT NULL
                  OR latest.forecast_type IS NOT NULL
              )
        ),
        ranked_events AS (
            SELECT
                symbol,
                available_at,
                raw_value,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, available_at
                    ORDER BY end_date DESC NULLS LAST, created_at DESC NULLS LAST
                ) AS event_rank
            FROM revision_events
            WHERE raw_value IS NOT NULL
        ),
        deduped_events AS (
            SELECT symbol, available_at, raw_value
            FROM ranked_events
            WHERE event_rank = 1
        ),
        event_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at) AS next_available_at,
                raw_value
            FROM deduped_events
        ),
        latest AS (
            SELECT
                event_intervals.symbol,
                td.trade_date,
                event_intervals.available_at,
                event_intervals.raw_value
            FROM event_intervals
            JOIN trade_days td
              ON td.trade_date >= event_intervals.available_at
             AND td.trade_date <= event_intervals.available_at + INTERVAL '{max_event_age_days} days'
             AND td.trade_date < COALESCE(event_intervals.next_available_at, $4 + 1)
             AND td.trade_date BETWEEN $3 AND $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_window_backfill_sql(
    source_table: &'static str,
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let decay_days = decay_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.symbol,
                event.available_at,
                event.end_date,
                event.created_at,
                {value_expression} AS event_raw_value
            FROM {source_table} event
            WHERE event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                events.end_date,
                events.created_at,
                events.event_raw_value,
                GREATEST(0.0, 1.0 - ((td.trade_date - events.available_at)::double precision / {decay_days}.0)) AS decay_weight,
                ROW_NUMBER() OVER (
                    PARTITION BY events.symbol, td.trade_date
                    ORDER BY events.available_at DESC, events.end_date DESC, events.created_at DESC
                ) AS event_rank
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at
             AND td.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                event_raw_value * decay_weight AS raw_value
            FROM expanded
            WHERE event_rank = 1
              AND decay_weight > 0.0
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_post_return_curve_backfill_sql(
    source_table: &'static str,
    event_filter_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    industry_relative: bool,
    min_event_age_days: i32,
    max_event_age_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let min_event_age_days = min_event_age_days.clamp(0, window_days);
    let max_event_age_days = max_event_age_days.clamp(min_event_age_days, window_days);
    let raw_value_expression = if industry_relative {
        "raw_event_return - AVG(raw_event_return) OVER (PARTITION BY trade_date, industry)"
    } else {
        "raw_event_return"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.symbol,
                event.available_at,
                event.end_date,
                event.created_at
            FROM {source_table} event
            WHERE event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
              AND ({event_filter_expression})
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                (
                    current_bar.close::double precision
                    / NULLIF(anchor_bar.close::double precision, 0.0)
                    - 1.0
                )
                * GREATEST(
                    0.0,
                    1.0 - ((td.trade_date - events.available_at)::double precision / {window_days}.0)
                ) AS raw_event_return,
                ROW_NUMBER() OVER (
                    PARTITION BY events.symbol, td.trade_date
                    ORDER BY events.available_at DESC, events.end_date DESC, events.created_at DESC
                ) AS event_rank
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at + INTERVAL '{min_event_age_days} days'
             AND td.trade_date <= events.available_at + INTERVAL '{max_event_age_days} days'
            JOIN market_stock ms
              ON ms.symbol = events.symbol
            JOIN LATERAL (
                SELECT close, trade_date
                FROM market_stock_daily_bar_adj anchor_bar
                WHERE anchor_bar.symbol = events.symbol
                  AND anchor_bar.trade_date <= events.available_at
                  AND anchor_bar.close IS NOT NULL
                  AND anchor_bar.close > 0
                ORDER BY anchor_bar.trade_date DESC
                LIMIT 1
            ) anchor_bar ON true
            JOIN market_stock_daily_bar_adj current_bar
              ON current_bar.symbol = events.symbol
             AND current_bar.trade_date = td.trade_date
             AND current_bar.close IS NOT NULL
             AND current_bar.close > 0
        ),
        latest_event AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                industry,
                raw_event_return
            FROM expanded
            WHERE event_rank = 1
              AND raw_event_return IS NOT NULL
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                {raw_value_expression} AS raw_value
            FROM latest_event
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM residualized
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_combo_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
    ),
    scores AS (
        SELECT
            fv.symbol,
            fv.trade_date,
            SUM(fv.normalized_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            MAX(COALESCE(fv.available_at, fv.trade_date)) AS available_at
        FROM weights
        JOIN factor_value fv
          ON fv.factor_code = weights.factor_code
         AND fv.factor_version = $2
         AND fv.trade_date BETWEEN $4 AND $5
         AND fv.normalized_value IS NOT NULL
        GROUP BY fv.symbol, fv.trade_date
        HAVING COUNT(DISTINCT fv.factor_code) >= $6
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}


pub(crate) fn phase7_alpha_blend_backfill_sql(combo_method: &str) -> &'static str {
    match combo_method {
        "weighted_combo_optional_overlay" => phase7_optional_overlay_blend_backfill_sql(),
        _ => phase7_strict_alpha_blend_backfill_sql(),
    }
}

pub(crate) fn phase7_strict_alpha_blend_backfill_sql() -> &'static str {
    "WITH sources AS (
        SELECT combo_name, version, weight
        FROM jsonb_to_recordset($3::jsonb)
             AS sources(combo_name text, version text, weight double precision)
    ),
    scores AS (
        SELECT
            mfv.symbol,
            mfv.trade_date,
            SUM(mfv.normalized_score::double precision * sources.weight) AS raw_score,
            MAX(COALESCE(mfv.available_at, mfv.trade_date)) AS available_at
        FROM multi_factor_value mfv
        JOIN sources
          ON sources.combo_name = mfv.combo_name
         AND sources.version = mfv.version
        WHERE mfv.trade_date BETWEEN $4 AND $5
          AND mfv.normalized_score IS NOT NULL
        GROUP BY mfv.symbol, mfv.trade_date
        HAVING COUNT(DISTINCT (sources.combo_name, sources.version)) = $6
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_optional_overlay_blend_backfill_sql() -> &'static str {
    "WITH sources AS (
        SELECT
            source_record.item ->> 'combo_name' AS combo_name,
            source_record.item ->> 'version' AS version,
            (source_record.item ->> 'weight')::double precision AS weight,
            source_record.ordinality
        FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY AS source_record(item, ordinality)
    ),
    required_sources AS (
        SELECT combo_name, version, weight
        FROM sources
        WHERE ordinality = 1
    ),
    optional_sources AS (
        SELECT combo_name, version, weight
        FROM sources
        WHERE ordinality > 1
    ),
    required_scores AS (
        SELECT
            mfv.symbol,
            mfv.trade_date,
            SUM(mfv.normalized_score::double precision * required_sources.weight) AS required_score,
            MAX(COALESCE(mfv.available_at, mfv.trade_date)) AS required_available_at
        FROM multi_factor_value mfv
        JOIN required_sources
          ON required_sources.combo_name = mfv.combo_name
         AND required_sources.version = mfv.version
        WHERE mfv.trade_date BETWEEN $4 AND $5
          AND mfv.normalized_score IS NOT NULL
        GROUP BY mfv.symbol, mfv.trade_date
        HAVING COUNT(DISTINCT (required_sources.combo_name, required_sources.version)) = $6
    ),
    scores AS (
        SELECT
            required_scores.symbol,
            required_scores.trade_date,
            required_scores.required_score
                + COALESCE(SUM(optional_mfv.normalized_score::double precision * optional_sources.weight), 0.0)
                AS raw_score,
            GREATEST(
                required_scores.required_available_at,
                COALESCE(MAX(COALESCE(optional_mfv.available_at, optional_mfv.trade_date)), required_scores.required_available_at)
            ) AS available_at
        FROM required_scores
        LEFT JOIN optional_sources ON true
        LEFT JOIN multi_factor_value optional_mfv
          ON optional_mfv.combo_name = optional_sources.combo_name
         AND optional_mfv.version = optional_sources.version
         AND optional_mfv.symbol = required_scores.symbol
         AND optional_mfv.trade_date = required_scores.trade_date
         AND optional_mfv.normalized_score IS NOT NULL
        GROUP BY required_scores.symbol, required_scores.trade_date, required_scores.required_score, required_scores.required_available_at
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}


