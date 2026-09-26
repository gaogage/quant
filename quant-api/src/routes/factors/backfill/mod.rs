//! Phase 7 factor backfill routes — 30+ factor family backfill implementations.

mod handlers;
mod specs;
mod sql;
pub use handlers::*;
pub(crate) use specs::*;
pub(crate) use sql::*;

use chrono::{Datelike, NaiveDate};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

use quant_api::discovery::strategy_discovery::phase7_alpha_blend_profiles;

use super::{
    background_factor_task_id, parse_phase7_backfill_date, trim_or_default, trim_required,
    upsert_factor_definition_input, usize_to_i32, FactorDefinitionInput,
};
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
pub struct Phase7LimitPressureBackfillRequest {
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
pub(crate) type Phase7LimitPressureBackfillPlan = SetBasedFactorBackfillPlan;
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
    /// 涨跌停净压力（P3 第二轮 2026-09-26）：窗口内 D 次数 - U 次数（inverse 方向，
    /// 涨停密度=投机过热→弱）。PIT 用事件日严格早于截面日（T-1 保守）。
    /// decay_days <= 0 = 无衰减纯计数（验证口径），> 0 = 线性衰减（预留变体）。
    LimitPressureWindow {
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

impl Phase7LimitPressureBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<Phase7LimitPressureBackfillPlan, String> {
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
            trim_or_default(self.combo_name, "phase7_limit_pressure_v1", "combo_name")?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(Phase7LimitPressureBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_limit_pressure_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_limit_pressure_v1",
            category: "limit_behavior_alpha",
            phase: "7-P3.23",
            dependencies: &[
                "market_stock_limit",
                "market_trade_calendar",
            ],
            combo_method: "weighted_combo_blend",
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

async fn run_phase7_limit_pressure_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7LimitPressureBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_limit_pressure_backfill_specs();
    run_segmented_set_based_factor_backfill(db, task_id, plan, &specs, phase7_factor_backfill_sql)
        .await
}

#[cfg(test)]
mod limit_pressure_tests {
    use super::*;

    /// SQL PIT 纪律断言：T-1 严格口径（截面日 > 事件日）、limit_type 方向过滤、
    /// 无衰减纯计数（验证口径）、NULL limit_type 不进事件集。
    #[test]
    fn phase7_limit_pressure_sql_is_strict_t1_no_decay() {
        let specs = phase7_limit_pressure_backfill_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].factor_code, "limit_net_pressure_40d_inverse_std");

        let sql = phase7_factor_backfill_sql(&specs[0]);
        // 事件源与方向过滤（NULL limit_type 不进事件集）
        assert!(sql.contains("FROM market_stock_limit event"));
        assert!(sql.contains("event.limit_type IN ('U', 'D')"));
        assert!(sql.contains("WHEN event.limit_type = 'D' THEN 1.0"));
        assert!(sql.contains("WHEN event.limit_type = 'U' THEN -1.0"));
        // PIT 严格 T-1：截面日必须严格晚于事件日（当日涨跌停收盘后才可得）
        assert!(sql.contains("td.trade_date > events.event_trade_date"));
        // 无衰减纯计数（pre-register 验证口径：decay_days=0 → 权重恒 1.0）
        assert!(sql.contains("1.0 AS decay_weight"));
        assert!(!sql.contains("GREATEST(\n                    0.0"));
        // 聚合语义与 PIT available_at 口径
        assert!(sql.contains("SUM(event_raw_value * decay_weight) AS raw_value"));
        assert!(sql.contains("MAX(event_trade_date) AS available_at"));
        assert!(sql.contains("percent_rank() OVER"));
    }

    /// 请求计划默认值断言（对齐 block_trade 模板契约）。
    #[test]
    fn phase7_limit_pressure_request_builds_plan_defaults() {
        let plan = Phase7LimitPressureBackfillRequest {
            start_date: None,
            end_date: None,
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid limit-pressure plan");

        assert_eq!(plan.combo_name, "phase7_limit_pressure_v1");
        assert_eq!(plan.task_type, "phase7_limit_pressure_backfill");
        assert_eq!(plan.phase, "7-P3.23");
        assert_eq!(
            plan.dependencies,
            &["market_stock_limit", "market_trade_calendar"]
        );
        assert!(plan.combo_method.len() <= 32);
    }
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

pub(crate) fn alpha_blend_profile_backfill_total_steps(
    plans: &[Phase7AlphaBlendBackfillPlan],
) -> usize {
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

pub(crate) fn yearly_backfill_segments(
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<(NaiveDate, NaiveDate)> {
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

pub(crate) fn quarterly_backfill_segments(
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<(NaiveDate, NaiveDate)> {
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

pub(crate) fn monthly_backfill_segments(
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<(NaiveDate, NaiveDate)> {
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
        // 0 行告警(2026-09-18 事故防复发): warmup 不足或源表缺口会让 raw_filter
        // 全过滤, 任务显示 completed 但目标因子零写入(静默留洞)。区间内应有数据
        // 时该 warn 是唯一暴露口。
        if spec_rows == 0 {
            tracing::warn!(
                task_id = %task_id,
                factor_code = %spec.factor_code,
                start = %plan.start_date,
                end = %plan.end_date,
                "set-based 回填 0 行写入: 区间含交易日时提示 warmup 窗口不足或上游源表缺口"
            );
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
    set_local_tuples_decompressed_limit(&mut tx).await?;

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
    set_local_tuples_decompressed_limit(&mut tx).await?;

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

    let result = sqlx::query(phase7_combo_backfill_sql())
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

/// 回填大事务写入已压缩 chunk 时, TimescaleDB 限制单事务解压 tuple 数(默认 10 万),
/// 跨季度区间(约 68 万行)触发 "tuple decompression limit exceeded by operation"。
/// 事务内放宽该限制(仅本事务生效, 不改全局)。
async fn set_local_tuples_decompressed_limit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), String> {
    sqlx::query("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 1000000")
        .execute(&mut **tx)
        .await
        .map_err(|error| format!("Failed to set tuples_decompressed limit: {}", error))?;
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
    let progress = (completed_steps.min(total_steps) * 100)
        .checked_div(total_steps)
        .unwrap_or(0) as i32;
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

#[cfg(test)]
mod tests;
