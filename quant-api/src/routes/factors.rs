//! Factor API routes — compute, standardize, and evaluate factors

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use quant_common::phase7::phase7_alpha_blend_profiles;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use quant_factor::factors::price_volume::*;
use quant_factor::neutralize::NeutralizeConfig;
use quant_factor::*;

use crate::AppState;

// ─── Request/Response types ───────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ComputeFactorRequest {
    pub factor: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Standardization method: "zscore", "rank", "winsorized_3"
    #[serde(default)]
    pub standardize: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FactorInfo {
    pub name: String,
    pub category: String,
    pub version: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateFactorRequest {
    pub factor: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default = "default_n_quantiles")]
    pub n_quantiles: usize,
}

fn default_n_quantiles() -> usize {
    5
}

#[derive(Debug, Deserialize)]
pub struct BatchSyncRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub standardize: Option<String>,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
}

fn default_version() -> String {
    "1.0.0".to_string()
}
fn default_chunk_size() -> usize {
    100
}

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
struct Phase7AlphaBlendSourcePlan {
    combo_name: String,
    version: String,
    weight: f64,
}

#[derive(Debug, Clone)]
struct SetBasedFactorBackfillPlan {
    start_date: NaiveDate,
    end_date: NaiveDate,
    version: String,
    combo_name: String,
    statement_timeout_ms: u64,
    task_type: &'static str,
    source: &'static str,
    heartbeat_timeout_seconds: i32,
    bundle_name: &'static str,
    category: &'static str,
    phase: &'static str,
    dependencies: &'static [&'static str],
    combo_method: &'static str,
    experiment_type: &'static str,
    source_combos: Vec<Phase7AlphaBlendSourcePlan>,
}

type Phase7PriceVolumeBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7FinancialQualityBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7IndustryResidualQualityBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7RelativeStrengthBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7QualityRelativeStrengthBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7GrowthRecoveryBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7ValuationBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7MoneyflowBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7EventAlphaBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7EventWindowAlphaBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7EventSurpriseBackfillPlan = SetBasedFactorBackfillPlan;
type Phase7AlphaBlendBackfillPlan = SetBasedFactorBackfillPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinancialAnnualChangeMode {
    PercentChange,
    Difference,
    Decrease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase7BackfillFactorKind {
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
    EventLatest {
        source_table: &'static str,
        value_expression: &'static str,
        higher_is_better: bool,
    },
    EventWindow {
        source_table: &'static str,
        value_expression: &'static str,
        higher_is_better: bool,
        window_days: i32,
        decay_days: i32,
    },
    FinancialAnnualChange {
        source_column: &'static str,
        mode: FinancialAnnualChangeMode,
    },
}

#[derive(Debug, Clone, Copy)]
struct SetBasedFactorSpec {
    factor_code: &'static str,
    name: &'static str,
    period: i32,
    kind: Phase7BackfillFactorKind,
    weight: f64,
}

#[derive(Debug, Clone)]
struct SetBasedFactorBackfillReport {
    factor_rows: usize,
    combo_rows: usize,
    factor_rows_by_code: Vec<(String, usize)>,
}

#[derive(Debug, Clone)]
enum SetBasedFactorBackfillCompletion {
    Completed {
        report: SetBasedFactorBackfillReport,
        elapsed_ms: u64,
    },
    Cancelled {
        report: SetBasedFactorBackfillReport,
        elapsed_ms: u64,
    },
}

type Phase7BackfillFactorSpec = SetBasedFactorSpec;
type Phase7BackfillCompletion = SetBasedFactorBackfillCompletion;

type SetBasedFactorSqlBuilder = fn(&SetBasedFactorSpec) -> String;

#[derive(Debug, Clone, Copy)]
struct SetBasedFactorBackfillJob<'a> {
    plan: &'a SetBasedFactorBackfillPlan,
    specs: &'a [SetBasedFactorSpec],
    factor_sql: SetBasedFactorSqlBuilder,
}

impl<'a> SetBasedFactorBackfillJob<'a> {
    fn new(
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

    fn total_steps(&self) -> usize {
        self.specs.len().saturating_add(1)
    }
}

impl SetBasedFactorBackfillCompletion {
    fn completed_with(report: SetBasedFactorBackfillReport, elapsed_ms: u64) -> Self {
        Self::Completed { report, elapsed_ms }
    }

    fn cancelled_with(report: SetBasedFactorBackfillReport, elapsed_ms: u64) -> Self {
        Self::Cancelled { report, elapsed_ms }
    }

    fn task_status(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    fn experiment_status(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Cancelled { .. } => "partial",
        }
    }

    fn report(&self) -> &SetBasedFactorBackfillReport {
        match self {
            Self::Completed { report, .. } | Self::Cancelled { report, .. } => report,
        }
    }

    fn elapsed_ms(&self) -> u64 {
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
    fn into_plan(self) -> Result<Phase7PriceVolumeBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7FinancialQualityBackfillPlan, String> {
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

impl Phase7IndustryResidualQualityBackfillRequest {
    fn into_plan(self) -> Result<Phase7IndustryResidualQualityBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7RelativeStrengthBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7QualityRelativeStrengthBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7GrowthRecoveryBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7ValuationBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7MoneyflowBackfillPlan, String> {
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

impl Phase7EventAlphaBackfillRequest {
    fn into_plan(self) -> Result<Phase7EventAlphaBackfillPlan, String> {
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
    fn into_plan(self) -> Result<Phase7EventWindowAlphaBackfillPlan, String> {
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

        Ok(Phase7EventWindowAlphaBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "phase7_event_window_alpha_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "phase7_event_window_earnings_v1",
            category: "event_alpha",
            phase: "7-Y2",
            dependencies: &[
                "market_stock_forecast",
                "market_stock_express",
                "market_stock_disclosure_date",
                "market_trade_calendar",
            ],
            combo_method: "weighted_event_window_earnings",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

impl Phase7EventSurpriseBackfillRequest {
    fn into_plan(self) -> Result<Phase7EventSurpriseBackfillPlan, String> {
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

impl Phase7AlphaBlendBackfillRequest {
    fn into_plan(self) -> Result<Phase7AlphaBlendBackfillPlan, String> {
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
    fn into_plans(self) -> Result<Vec<Phase7AlphaBlendBackfillPlan>, String> {
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
            let combo_method = if profile.combo_name == "phase7_quality_event_window_overlay_v1" {
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

fn trim_or_default(value: Option<String>, default: &str, field: &str) -> Result<String, String> {
    match value {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Err(format!("{} must not be blank", field))
            } else {
                Ok(trimmed.to_string())
            }
        }
        None => Ok(default.to_string()),
    }
}

fn parse_phase7_backfill_date(
    value: Option<String>,
    default: NaiveDate,
    field: &str,
) -> Result<NaiveDate, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{} must not be blank", field));
    }

    for format in ["%Y-%m-%d", "%Y%m%d"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            return Ok(date);
        }
    }

    Err(format!("{} must use YYYY-MM-DD or YYYYMMDD", field))
}

fn phase7_price_volume_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_financial_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_industry_residual_quality_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_relative_strength_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_quality_relative_strength_combo_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_growth_recovery_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_valuation_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_moneyflow_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_event_alpha_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

fn phase7_event_window_alpha_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
    vec![
        Phase7BackfillFactorSpec {
            factor_code: "event_window_forecast_change_20d_decay_std",
            name: "Phase 7 20d decayed forecast profit-change midpoint rank",
            period: 20,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_forecast",
                value_expression: "(COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.35,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_window_forecast_profit_floor_20d_decay_std",
            name: "Phase 7 20d decayed forecast net-profit floor rank",
            period: 20,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_forecast",
                value_expression: "event.net_profit_min::double precision",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.20,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_window_express_roe_20d_decay_std",
            name: "Phase 7 20d decayed express diluted ROE rank",
            period: 20,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_express",
                value_expression: "event.diluted_roe::double precision",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.25,
        },
        Phase7BackfillFactorSpec {
            factor_code: "event_window_disclosure_early_days_20d_decay_std",
            name: "Phase 7 20d decayed early disclosure timing rank",
            period: 20,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_disclosure_date",
                value_expression: "CASE WHEN event.pre_date IS NOT NULL AND event.actual_date IS NOT NULL THEN (event.pre_date - event.actual_date)::double precision ELSE NULL END",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 0.20,
        },
    ]
}

fn phase7_event_surprise_backfill_specs() -> Vec<Phase7BackfillFactorSpec> {
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

#[derive(Debug, Deserialize)]
pub struct RegisterFactorDefinitionRequest {
    pub factor_code: String,
    pub version: String,
    pub name: String,
    pub category: String,
    pub frequency: Option<String>,
    pub dependencies: Option<serde_json::Value>,
    pub parameters: Option<serde_json::Value>,
    pub status: Option<String>,
}

#[derive(Debug)]
struct FactorDefinitionInput {
    factor_code: String,
    version: String,
    name: String,
    category: String,
    frequency: String,
    dependencies: serde_json::Value,
    parameters: serde_json::Value,
    status: String,
}

#[derive(Debug, Deserialize)]
pub struct ListFactorDefinitionsQuery {
    pub factor_code: Option<String>,
    pub version: Option<String>,
    pub category: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct FactorDefinitionRecord {
    factor_id: i64,
    factor_code: String,
    version: String,
    name: String,
    category: String,
    frequency: String,
    dependencies: serde_json::Value,
    parameters: serde_json::Value,
    status: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

type FactorDefinitionRow = (
    i64,
    String,
    String,
    String,
    String,
    String,
    serde_json::Value,
    Option<serde_json::Value>,
    String,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

impl RegisterFactorDefinitionRequest {
    fn into_definition(self) -> Result<FactorDefinitionInput, String> {
        let dependencies = self.dependencies.unwrap_or_else(|| json!([]));
        if !dependencies.is_array() {
            return Err("dependencies must be a JSON array".to_string());
        }

        Ok(FactorDefinitionInput {
            factor_code: trim_required(self.factor_code, "factor_code")?,
            version: trim_required(self.version, "version")?,
            name: trim_required(self.name, "name")?,
            category: trim_required(self.category, "category")?,
            frequency: trim_optional(self.frequency, "daily"),
            dependencies,
            parameters: self.parameters.unwrap_or_else(|| json!({})),
            status: trim_optional(self.status, "active"),
        })
    }
}

fn trim_required(value: String, field: &str) -> Result<String, String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(format!("{} is required", field))
    } else {
        Ok(value)
    }
}

fn trim_optional(value: Option<String>, default_value: &str) -> String {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .unwrap_or_else(|| default_value.to_string())
}

fn normalize_filter(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

fn normalize_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(100).clamp(1, 500)
}

fn factor_definition_from_row(row: FactorDefinitionRow) -> FactorDefinitionRecord {
    FactorDefinitionRecord {
        factor_id: row.0,
        factor_code: row.1,
        version: row.2,
        name: row.3,
        category: row.4,
        frequency: row.5,
        dependencies: row.6,
        parameters: row.7.unwrap_or_else(|| json!({})),
        status: row.8,
        created_at: row.9,
        updated_at: row.10,
    }
}

async fn upsert_factor_definition_input(
    db: &sqlx::PgPool,
    input: &FactorDefinitionInput,
) -> Result<FactorDefinitionRecord, sqlx::Error> {
    let row = sqlx::query_as::<_, FactorDefinitionRow>(
        "INSERT INTO factor_definition
           (factor_code, version, name, category, frequency, dependencies, parameters, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (factor_code, version) DO UPDATE SET
           name = EXCLUDED.name,
           category = EXCLUDED.category,
           frequency = EXCLUDED.frequency,
           dependencies = EXCLUDED.dependencies,
           parameters = EXCLUDED.parameters,
           status = EXCLUDED.status,
           updated_at = NOW()
         RETURNING factor_id, factor_code, version, name, category, frequency,
           dependencies, parameters, status, created_at, updated_at",
    )
    .bind(&input.factor_code)
    .bind(&input.version)
    .bind(&input.name)
    .bind(&input.category)
    .bind(&input.frequency)
    .bind(&input.dependencies)
    .bind(&input.parameters)
    .bind(&input.status)
    .fetch_one(db)
    .await?;

    Ok(factor_definition_from_row(row))
}

async fn upsert_factor_definition(
    db: &sqlx::PgPool,
    output: &FactorOutput,
    version: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO factor_definition
           (factor_code, version, name, category, frequency, dependencies, parameters, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
         ON CONFLICT (factor_code, version) DO UPDATE SET
           name = EXCLUDED.name,
           category = EXCLUDED.category,
           frequency = EXCLUDED.frequency,
           dependencies = EXCLUDED.dependencies,
           parameters = EXCLUDED.parameters,
           status = EXCLUDED.status,
           updated_at = NOW()",
    )
    .bind(&output.name)
    .bind(version)
    .bind(&output.metadata.factor_name)
    .bind(output.metadata.category.to_string())
    .bind("daily")
    .bind(json!(["market_stock_daily_bar"]))
    .bind(&output.metadata.params)
    .execute(db)
    .await?;
    Ok(())
}

// ─── Handlers ─────────────────────────────────────────────────────

pub async fn list_factors() -> impl IntoResponse {
    let factors = vec![
        FactorInfo {
            name: "mom_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "mom_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "mom_60d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 60}),
        },
        FactorInfo {
            name: "vol_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "annualized": true}),
        },
        FactorInfo {
            name: "vol_60d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 60, "annualized": true}),
        },
        FactorInfo {
            name: "downvol_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "annualized": true}),
        },
        FactorInfo {
            name: "rev_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "rev_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "turn_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "turn_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "amihud_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "scale": 1_000_000_000.0}),
        },
        FactorInfo {
            name: "amt_intensity_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
    ];

    Json(json!({"code": 0, "data": {"factors": factors}}))
}

pub async fn list_factor_definitions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListFactorDefinitionsQuery>,
) -> impl IntoResponse {
    let factor_code = normalize_filter(query.factor_code);
    let version = normalize_filter(query.version);
    let category = normalize_filter(query.category);
    let status = normalize_filter(query.status);
    let limit = normalize_limit(query.limit);

    let rows = sqlx::query_as::<_, FactorDefinitionRow>(
        "SELECT factor_id, factor_code, version, name, category, frequency,
           dependencies, parameters, status, created_at, updated_at
         FROM factor_definition
         WHERE ($1::text IS NULL OR factor_code = $1)
           AND ($2::text IS NULL OR version = $2)
           AND ($3::text IS NULL OR category = $3)
           AND ($4::text IS NULL OR status = $4)
         ORDER BY updated_at DESC, factor_code ASC, version ASC
         LIMIT $5",
    )
    .bind(factor_code.as_deref())
    .bind(version.as_deref())
    .bind(category.as_deref())
    .bind(status.as_deref())
    .bind(limit)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => Json(json!({
            "code": 0,
            "data": {
                "definitions": rows.into_iter().map(factor_definition_from_row).collect::<Vec<_>>()
            }
        })),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to list factor definitions: {}", error)}),
        ),
    }
}

pub async fn get_factor_definition(
    State(state): State<Arc<AppState>>,
    Path((factor_code, version)): Path<(String, String)>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<_, FactorDefinitionRow>(
        "SELECT factor_id, factor_code, version, name, category, frequency,
           dependencies, parameters, status, created_at, updated_at
         FROM factor_definition
         WHERE factor_code = $1 AND version = $2",
    )
    .bind(&factor_code)
    .bind(&version)
    .fetch_optional(&state.db)
    .await;

    match row {
        Ok(Some(row)) => Json(json!({"code": 0, "data": factor_definition_from_row(row)})),
        Ok(None) => Json(
            json!({"code": 1, "message": format!("factor definition not found: {}@{}", factor_code, version)}),
        ),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to get factor definition: {}", error)}),
        ),
    }
}

pub async fn register_factor_definition(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterFactorDefinitionRequest>,
) -> impl IntoResponse {
    let input = match req.into_definition() {
        Ok(input) => input,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };

    match upsert_factor_definition_input(&state.db, &input).await {
        Ok(definition) => Json(json!({"code": 0, "data": definition})),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        ),
    }
}

pub async fn compute_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ComputeFactorRequest>,
) -> impl IntoResponse {
    // Parse factor parameters
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Determine symbols (as Vec<String> for load_bars)
    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = req.symbols.as_ref().unwrap_or(&default_symbols);

    // Determine date range
    let end_date = req.end_date.as_deref().unwrap_or("20250509");
    let start_date = req.start_date.as_deref().unwrap_or("20240101");

    // Load daily bars from database
    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 1).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}));
        }
    };

    let input = FactorInput {
        bars,
        trade_dates: vec![],
    };

    // Compute factor
    let mut output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Apply standardization if requested
    if let Some(ref std_method) = req.standardize {
        let method = match std_method.as_str() {
            "zscore" => StandardizeMethod::ZScore,
            "rank" => StandardizeMethod::Rank,
            s if s.starts_with("winsorized_") => {
                let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                StandardizeMethod::Winsorized(sigma)
            }
            _ => StandardizeMethod::ZScore,
        };
        output = standardize(&output, method);
    }

    // Convert to JSON-friendly format
    let values: Vec<serde_json::Value> = output
        .values
        .iter()
        .map(|fv| {
            json!({
                "symbol": fv.symbol,
                "date": fv.date.format("%Y%m%d").to_string(),
                "value": fv.value,
            })
        })
        .collect();

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": output.name,
            "metadata": output.metadata,
            "value_count": values.len(),
            "values": values,
        }
    }))
}

pub async fn evaluate_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateFactorRequest>,
) -> impl IntoResponse {
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = req.end_date.as_deref().unwrap_or("20250509");
    let start_date = req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 2).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}));
        }
    };

    let input = FactorInput {
        bars: bars.clone(),
        trade_dates: vec![],
    };

    let output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Build forward returns: 1-day forward return per (symbol, date)
    let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
    for (sym, sym_bars) in &bars {
        for i in 0..sym_bars.len() - 1 {
            let close_t: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
            let close_t1: f64 = sym_bars[i + 1].close.try_into().unwrap_or(f64::NAN);
            if close_t > 0.0 {
                forward_returns.insert(
                    (sym.clone(), sym_bars[i].trade_date),
                    (close_t1 - close_t) / close_t,
                );
            }
        }
    }

    let evaluation = evaluate(&output, &forward_returns, req.n_quantiles);

    Json(json!({
        "code": 0,
        "data": evaluation,
    }))
}

// ─── Persist computed factors ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SyncFactorRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub standardize: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncFinancialFactorRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug)]
struct FinancialIndicatorRow {
    symbol: String,
    ann_date: NaiveDate,
    end_date: NaiveDate,
    value: Decimal,
}

impl FinancialIndicatorRow {
    fn to_factor_value(&self) -> FinancialFactorValue {
        let value: f64 = self.value.try_into().unwrap_or(f64::NAN);
        FinancialFactorValue {
            symbol: self.symbol.clone(),
            date: self.ann_date,
            available_at: self.ann_date,
            report_end_date: self.end_date,
            value,
        }
    }
}

#[derive(Debug)]
struct FinancialFactorValue {
    symbol: String,
    date: NaiveDate,
    available_at: NaiveDate,
    report_end_date: NaiveDate,
    value: f64,
}

struct FinancialFactorSpec {
    code: &'static str,
    source_column: &'static str,
    name: &'static str,
    category: &'static str,
}

fn parse_financial_factor(name: &str) -> Option<FinancialFactorSpec> {
    match name {
        "roe" | "roe_ttm" => Some(FinancialFactorSpec {
            code: "fin_roe",
            source_column: "roe",
            name: "roe",
            category: "fundamental",
        }),
        "roa" | "roa_ttm" => Some(FinancialFactorSpec {
            code: "fin_roa",
            source_column: "roa",
            name: "roa",
            category: "fundamental",
        }),
        "eps" => Some(FinancialFactorSpec {
            code: "fin_eps",
            source_column: "eps",
            name: "eps",
            category: "fundamental",
        }),
        "gross_margin" => Some(FinancialFactorSpec {
            code: "fin_gross_margin",
            source_column: "gross_margin",
            name: "gross_margin",
            category: "fundamental",
        }),
        "netprofit_margin" => Some(FinancialFactorSpec {
            code: "fin_netprofit_margin",
            source_column: "netprofit_margin",
            name: "netprofit_margin",
            category: "fundamental",
        }),
        "debt_to_assets" => Some(FinancialFactorSpec {
            code: "fin_debt_to_assets",
            source_column: "debt_to_assets",
            name: "debt_to_assets",
            category: "fundamental",
        }),
        "current_ratio" => Some(FinancialFactorSpec {
            code: "fin_current_ratio",
            source_column: "current_ratio",
            name: "current_ratio",
            category: "fundamental",
        }),
        "quick_ratio" => Some(FinancialFactorSpec {
            code: "fin_quick_ratio",
            source_column: "quick_ratio",
            name: "quick_ratio",
            category: "fundamental",
        }),
        _ => None,
    }
}

pub async fn sync_factor_values(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFactorRequest>,
) -> impl IntoResponse {
    let compute_req = ComputeFactorRequest {
        factor: req.factor.clone(),
        symbols: req.symbols,
        start_date: req.start_date,
        end_date: req.end_date,
        standardize: req.standardize,
    };

    // Reuse compute_factor logic by calling the inner function directly
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}))
        }
    };

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = compute_req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = compute_req.end_date.as_deref().unwrap_or("20250509");
    let start_date = compute_req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 1).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}))
        }
    };

    let input = FactorInput {
        bars,
        trade_dates: vec![],
    };
    let mut output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    let std_method = if let Some(ref m) = compute_req.standardize {
        match m.as_str() {
            "zscore" => Some("zscore".to_string()),
            "rank" => Some("rank".to_string()),
            s if s.starts_with("winsorized_") => Some(s.to_string()),
            _ => None,
        }
    } else {
        None
    };

    if let Some(ref m) = std_method {
        let method = match m.as_str() {
            "zscore" => StandardizeMethod::ZScore,
            "rank" => StandardizeMethod::Rank,
            s if s.starts_with("winsorized_") => {
                let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                StandardizeMethod::Winsorized(sigma)
            }
            _ => StandardizeMethod::ZScore,
        };
        output = standardize(&output, method);
    }

    if let Err(error) = upsert_factor_definition(&state.db, &output, &req.version).await {
        return Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        );
    }

    // Persist to DB
    let mut inserted = 0u64;
    for fv in &output.values {
        let available_at = fv.available_at.unwrap_or(fv.date);
        let result = sqlx::query(
            "INSERT INTO factor_value
               (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value,
               normalized_value = EXCLUDED.normalized_value,
               available_at = EXCLUDED.available_at,
               created_at = NOW()"
        )
        .bind(&output.name)
        .bind(&req.version)
        .bind(&fv.symbol)
        .bind(fv.date)
        .bind(fv.value)
        .bind(if std_method.is_some() { Some(fv.value) } else { None::<f64> })
        .bind(available_at)
        .execute(&state.db)
        .await;

        match result {
            Ok(_) => inserted += 1,
            Err(e) => tracing::warn!(
                "Failed to insert factor value for {} {}: {}",
                fv.symbol,
                fv.date,
                e
            ),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": output.name,
            "version": req.version,
            "total_values": output.values.len(),
            "inserted": inserted,
            "standardized": std_method.is_some(),
        }
    }))
}

pub async fn sync_financial_factor_values(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFinancialFactorRequest>,
) -> impl IntoResponse {
    let spec = match parse_financial_factor(&req.factor) {
        Some(spec) => spec,
        None => {
            return Json(
                json!({"code": 1, "message": format!("Unknown financial factor: {}", req.factor)}),
            )
        }
    };

    let start_d = req.start_date.as_deref().unwrap_or("20100101");
    let end_d = req.end_date.as_deref().unwrap_or("20261231");
    let symbols = req.symbols.unwrap_or_default();
    let symbol_filter = if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    };

    let rows = match sqlx::query_as::<_, (String, NaiveDate, NaiveDate, Decimal)>(&format!(
        "SELECT ts_code, ann_date, end_date, {column}
             FROM market_financial_indicator
             WHERE {column} IS NOT NULL
               AND ann_date >= $1::date
               AND ann_date <= $2::date
               AND ($3::text[] IS NULL OR ts_code = ANY($3))
             ORDER BY ann_date, ts_code",
        column = spec.source_column
    ))
    .bind(start_d)
    .bind(end_d)
    .bind(symbol_filter.as_deref())
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load financial factor source: {}", error)}),
            );
        }
    };

    let factor_values = rows
        .into_iter()
        .map(
            |(symbol, ann_date, end_date, value)| FinancialIndicatorRow {
                symbol,
                ann_date,
                end_date,
                value,
            },
        )
        .map(|row| row.to_factor_value())
        .filter(|fv| fv.value.is_finite())
        .collect::<Vec<_>>();

    let definition = FactorDefinitionInput {
        factor_code: spec.code.to_string(),
        version: req.version.clone(),
        name: spec.name.to_string(),
        category: spec.category.to_string(),
        frequency: "quarterly_report".to_string(),
        dependencies: json!(["market_financial_indicator"]),
        parameters: json!({
            "source_column": spec.source_column,
            "pit_date": "ann_date",
            "report_period_column": "end_date",
        }),
        status: "active".to_string(),
    };

    if let Err(error) = upsert_factor_definition_input(&state.db, &definition).await {
        return Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        );
    }

    let mut inserted = 0u64;
    let mut min_trade_date: Option<NaiveDate> = None;
    let mut max_trade_date: Option<NaiveDate> = None;
    let mut max_report_end_date: Option<NaiveDate> = None;

    for fv in &factor_values {
        let result = sqlx::query(
            "INSERT INTO factor_value
               (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
             VALUES ($1, $2, $3, $4, $5, NULL, $6)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value,
               normalized_value = EXCLUDED.normalized_value,
               available_at = EXCLUDED.available_at,
               created_at = NOW()",
        )
        .bind(spec.code)
        .bind(&req.version)
        .bind(&fv.symbol)
        .bind(fv.date)
        .bind(fv.value)
        .bind(fv.available_at)
        .execute(&state.db)
        .await;

        match result {
            Ok(_) => {
                inserted += 1;
                min_trade_date = Some(min_trade_date.map_or(fv.date, |date| date.min(fv.date)));
                max_trade_date = Some(max_trade_date.map_or(fv.date, |date| date.max(fv.date)));
                max_report_end_date = Some(
                    max_report_end_date
                        .map_or(fv.report_end_date, |date| date.max(fv.report_end_date)),
                );
            }
            Err(error) => tracing::warn!(
                factor = spec.code,
                symbol = %fv.symbol,
                date = %fv.date,
                "Failed to insert financial factor value: {}",
                error
            ),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": spec.code,
            "version": req.version,
            "source_column": spec.source_column,
            "total_values": factor_values.len(),
            "inserted": inserted,
            "pit_date": "ann_date",
            "report_period_column": "end_date",
            "min_trade_date": min_trade_date.map(|date| date.to_string()),
            "max_trade_date": max_trade_date.map(|date| date.to_string()),
            "max_report_end_date": max_report_end_date.map(|date| date.to_string()),
        }
    }))
}

// ─── Batch sync all symbols (inline, no callback overhead) ───────

pub async fn batch_sync_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchSyncRequest>,
) -> impl IntoResponse {
    let start_d = req.start_date.as_deref().unwrap_or("20240101");
    let end_d = req.end_date.as_deref().unwrap_or("20250509");
    let is_std = req.standardize.is_some();
    let chunk_size = req.chunk_size;

    // Load symbol list
    let all_syms: Vec<String> = match sqlx::query_scalar(
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => return Json(json!({"code":1,"message":format!("{}",e)})),
    };

    let (ftype, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => return Json(json!({"code":1,"message":format!("unknown factor: {}", req.factor)})),
    };
    let mut total_vals = 0usize;
    let mut inserted = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for chunk in all_syms.chunks(chunk_size) {
        let syms: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();

        // Load bars for this chunk
        let mut bars_map: HashMap<String, Vec<DailyBar>> = HashMap::new();
        for &sym in &syms {
            let rows = sqlx::query_as::<_, (String, NaiveDate, Decimal, Decimal, Decimal, Decimal, Option<Decimal>, Option<Decimal>, Decimal, Decimal)>(
                "SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
                 FROM market_stock_daily_bar
                 WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
                 ORDER BY trade_date ASC"
            ).bind(sym).bind(start_d).bind(end_d).fetch_all(&state.db).await;

            match rows {
                Ok(r) if r.len() > period + 1 => {
                    let bars: Vec<DailyBar> = r
                        .into_iter()
                        .map(|(s, d, o, h, l, c, pc, cp, v, a)| DailyBar {
                            symbol: s,
                            trade_date: d,
                            open: o,
                            high: h,
                            low: l,
                            close: c,
                            pre_close: pc,
                            change_pct: cp,
                            volume: v,
                            amount: a,
                        })
                        .collect();
                    bars_map.insert(sym.to_string(), bars);
                }
                Ok(_) => {}
                Err(e) => {
                    errors.push(format!("{}/{}: {}", sym, "load", e));
                }
            }
        }

        if bars_map.is_empty() {
            continue;
        }

        let input = FactorInput {
            bars: bars_map,
            trade_dates: vec![],
        };

        // Compute
        let mut output = match compute_price_volume_factor(ftype, period, &input) {
            Some(output) => output,
            None => break,
        };

        // Standardize
        if let Some(ref m) = req.standardize {
            let method = match m.as_str() {
                "zscore" => StandardizeMethod::ZScore,
                "rank" => StandardizeMethod::Rank,
                s if s.starts_with("winsorized_") => {
                    let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                    StandardizeMethod::Winsorized(sigma)
                }
                _ => StandardizeMethod::ZScore,
            };
            tracing::info!(factor=%req.factor, method=%m, sigma=?method, "foreground standardize");
            output = standardize(&output, method);
        }

        if let Err(e) = upsert_factor_definition(&state.db, &output, &req.version).await {
            errors.push(format!("factor_definition/{}: {}", output.name, e));
            continue;
        }

        total_vals += output.values.len();

        match upsert_factor_values(&state.db, &output, &req.version, is_std).await {
            Ok(saved) => inserted += saved,
            Err(error) => errors.push(error),
        }
    }

    let factor_output_name = format!("{}_{}d{}", ftype, period, if is_std { "_std" } else { "" });

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": factor_output_name,
            "version": req.version,
            "symbols_total": all_syms.len(),
            "total_values": total_vals,
            "inserted": inserted,
            "errors": errors.iter().take(5).collect::<Vec<_>>(),
            "error_count": errors.len(),
            "standardized": is_std,
        }
    }))
}

/// POST /api/v1/quant/factors/batch-sync/background
///
/// 后台批量计算单个因子，立即返回 task_id。
/// 通过 GET /api/v1/quant/data/sync/tasks/:task_id 查询进度。
pub async fn batch_sync_factors_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchSyncRequest>,
) -> impl IntoResponse {
    let task_id = background_factor_task_id();
    let factor_name = req.factor.clone();
    let version = req.version.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let std_method = req.standardize.clone();
    let chunk_size = req.chunk_size;

    info!(task_id = %task_id, factor = %factor_name, "后台计算因子");

    // Create sync task record for status tracking
    let _ = sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, $2, 'factor', 'running')"
    )
    .bind(&task_id)
    .bind(&format!("factor:{}", factor_name))
    .execute(&state.db)
    .await;

    let state = state.clone();
    let tid = task_id.clone();
    let fname = factor_name.clone();

    tokio::spawn(async move {
        let result: Result<serde_json::Value, String> = async {
            let all_syms: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol"
            ).fetch_all(&state.db).await.map_err(|e| e.to_string())?;

            let (ftype, period) = parse_factor(&fname)
                .ok_or_else(|| format!("unknown factor: {}", fname))?;

            let start_d = start.as_deref().unwrap_or("20160101");
            let end_d = end.as_deref().unwrap_or("20260511");
            let is_std = std_method.is_some();
            let mut total_vals = 0usize;
            let mut inserted = 0usize;
            let mut errors: Vec<String> = Vec::new();

            for chunk in all_syms.chunks(chunk_size) {
                let syms: Vec<String> = chunk.iter().map(|s| s.clone()).collect();
                let sym_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();

                // Batch-load all bars for this chunk in ONE query
                let all_rows: Vec<(String, NaiveDate, Decimal, Decimal, Decimal, Decimal, Option<Decimal>, Option<Decimal>, Decimal, Decimal)> =
                    sqlx::query_as("SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
                        FROM market_stock_daily_bar WHERE symbol = ANY($1) AND trade_date >= $2::date AND trade_date <= $3::date ORDER BY symbol, trade_date ASC")
                    .bind(&sym_refs).bind(start_d).bind(end_d)
                    .fetch_all(&state.db).await
                    .map_err(|e| format!("batch query failed ({} symbols, {} - {}): {}", sym_refs.len(), start_d, end_d, e))?;

                // Group by symbol
                let mut bars_map: HashMap<String, Vec<DailyBar>> = HashMap::new();
                for (sym, d, o, h, l, c, pc, cp, v, a) in all_rows {
                    if let (Some(pc_val), Some(cp_val)) = (pc, cp) {
                        bars_map.entry(sym.clone()).or_default().push(DailyBar {
                            symbol: sym, trade_date: d, open: o, high: h, low: l,
                            close: c, pre_close: Some(pc_val), change_pct: Some(cp_val),
                            volume: v, amount: a,
                        });
                    } else {
                        bars_map.entry(sym.clone()).or_default().push(DailyBar {
                            symbol: sym, trade_date: d, open: o, high: h, low: l,
                            close: c, pre_close: pc, change_pct: cp,
                            volume: v, amount: a,
                        });
                    }
                }
                // Filter symbols with insufficient data
                bars_map.retain(|_, bars| bars.len() > period + 1);

                if bars_map.is_empty() { continue; }

                let input = FactorInput { bars: bars_map, trade_dates: vec![] };
                let mut output = match compute_price_volume_factor(ftype, period, &input) {
                    Some(output) => output,
                    None => { errors.push(format!("Unknown factor: {}", ftype)); break; }
                };

                if let Some(ref m) = std_method {
                    let method = match m.as_str() {
                        "zscore" => StandardizeMethod::ZScore,
                        "rank" => StandardizeMethod::Rank,
                        s if s.starts_with("winsorized_") => {
                            let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                            StandardizeMethod::Winsorized(sigma)
                        }
                        _ => StandardizeMethod::ZScore,
                    };
                    output = standardize(&output, method);
                }

                if let Err(e) = upsert_factor_definition(&state.db, &output, &version).await {
                    errors.push(format!("factor_definition/{}: {}", output.name, e));
                    continue;
                }

                total_vals += output.values.len();

                match upsert_factor_values(&state.db, &output, &version, is_std).await {
                    Ok(saved) => inserted += saved,
                    Err(error) => errors.push(error),
                }
            }

            let factor_output_name = format!("{}_{}d{}", ftype, period, if is_std { "_std" } else { "" });
            Ok(serde_json::json!({
                "factor_name": factor_output_name, "version": version,
                "total_values": total_vals, "inserted": inserted,
                "error_count": errors.len()
            }))
        }.await;

        match result {
            Ok(data) => {
                info!(task_id = %tid, "后台计算因子完成");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='completed', total_count=$1, success_count=$2, failed_count=$3, progress=100, completed_at=now() WHERE task_id=$4"
                )
                .bind(data["total_values"].as_i64().unwrap_or(0) as i32)
                .bind(data["inserted"].as_i64().unwrap_or(0) as i32)
                .bind(data["error_count"].as_i64().unwrap_or(0) as i32)
                .bind(&tid)
                .execute(&state.db).await;
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "后台计算因子失败");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='failed', error_message=$2, progress=0, completed_at=now() WHERE task_id=$1"
                )
                .bind(&tid)
                .bind(&e)
                .execute(&state.db).await;
            }
        }
    });

    Json(
        json!({"code": 0, "data": {"task_id": task_id, "status": "running", "factor": factor_name}}),
    )
}

/// POST /api/v1/quant/factors/phase7-price-volume-backfill/background
///
/// Set-based backfill for the Phase 7 price-volume alpha bundle and its
/// equal-weight combo score. This is the Rust API path for full historical
/// backfills, replacing ad-hoc SQL/Python glue with an observable background task.
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
                let specs = phase7_event_window_alpha_backfill_specs();
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
    .bind(usize_to_i32(plans.len()))
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

fn background_factor_task_id() -> String {
    let ts = chrono::Utc::now().format("fs-%Y%m%d-%H%M%S%3f");
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{}-{}", ts, &suffix[..8])
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

async fn run_phase7_financial_quality_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7FinancialQualityBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_financial_quality_backfill_specs();
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

async fn run_phase7_event_window_alpha_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &Phase7EventWindowAlphaBackfillPlan,
) -> Result<Phase7BackfillCompletion, String> {
    let specs = phase7_event_window_alpha_backfill_specs();
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
    let total_steps = plans.len().max(1);
    let mut combo_rows = 0usize;

    for (index, plan) in plans.iter().enumerate() {
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
        let rows = execute_phase7_alpha_blend_backfill(db, plan).await?;
        combo_rows = combo_rows.saturating_add(rows);
        update_factor_backfill_progress(db, task_id, index + 1, total_steps, combo_rows).await?;
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

fn phase7_combo_required_factor_count(
    specs: &[SetBasedFactorSpec],
    plan: &SetBasedFactorBackfillPlan,
) -> i64 {
    match plan.combo_method {
        "weighted_event_earnings" | "weighted_event_window_earnings" => 1,
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

fn phase7_alpha_blend_required_source_count(plan: &Phase7AlphaBlendBackfillPlan) -> i64 {
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

fn factor_backfill_profile_metrics(
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

fn usize_to_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn factor_backfill_combo_weights_json(
    specs: &[SetBasedFactorSpec],
) -> Result<serde_json::Value, String> {
    let weights = specs
        .iter()
        .map(|spec| (spec.factor_code.to_string(), spec.weight))
        .collect::<HashMap<_, _>>();
    serde_json::to_value(weights).map_err(|error| format!("Failed to encode weights: {}", error))
}

fn alpha_blend_source_records_json(
    sources: &[Phase7AlphaBlendSourcePlan],
) -> Result<serde_json::Value, String> {
    serde_json::to_value(sources).map_err(|error| format!("Failed to encode sources: {}", error))
}

fn alpha_blend_weights_json(
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

fn phase7_factor_backfill_sql(spec: &Phase7BackfillFactorSpec) -> String {
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
        Phase7BackfillFactorKind::EventLatest {
            source_table,
            value_expression,
            higher_is_better,
        } => phase7_event_latest_backfill_sql(source_table, value_expression, higher_is_better),
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
        Phase7BackfillFactorKind::FinancialAnnualChange {
            source_column,
            mode,
        } => phase7_financial_annual_change_backfill_sql(source_column, mode),
    }
}

fn phase7_reversal_backfill_sql(period: i32) -> String {
    format!(
        "WITH priced AS (
            SELECT
                symbol,
                trade_date,
                close::double precision AS close,
                LAG(close::double precision, {period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close
            FROM market_stock_daily_bar
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

fn phase7_downside_volatility_backfill_sql(period: i32) -> String {
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
                FROM market_stock_daily_bar
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

fn phase7_amihud_backfill_sql(period: i32) -> String {
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
                FROM market_stock_daily_bar
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

fn phase7_amount_intensity_backfill_sql(period: i32) -> String {
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
            FROM market_stock_daily_bar
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

fn phase7_relative_momentum_backfill_sql(period: i32, industry_relative: bool) -> String {
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
                FROM market_stock_daily_bar
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

fn phase7_financial_latest_backfill_sql(
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

fn phase7_industry_relative_financial_latest_backfill_sql(
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

fn phase7_financial_annual_change_backfill_sql(
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

fn phase7_daily_basic_latest_backfill_sql(
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

fn phase7_moneyflow_backfill_sql(
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
            JOIN market_stock_daily_bar bar
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

fn phase7_event_latest_backfill_sql(
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
        symbols AS (
            SELECT DISTINCT symbol
            FROM {source_table}
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                event.available_at,
                event.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN LATERAL (
                SELECT
                    available_at,
                    {value_expression} AS raw_value
                FROM {source_table} event
                WHERE event.symbol = symbols.symbol
                  AND event.available_at <= td.trade_date
                ORDER BY event.available_at DESC, event.end_date DESC, event.created_at DESC
                LIMIT 1
            ) event ON true
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

fn phase7_event_window_backfill_sql(
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

fn phase7_combo_backfill_sql() -> &'static str {
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

fn phase7_alpha_blend_backfill_sql(combo_method: &str) -> &'static str {
    match combo_method {
        "weighted_combo_optional_overlay" => phase7_optional_overlay_blend_backfill_sql(),
        _ => phase7_strict_alpha_blend_backfill_sql(),
    }
}

fn phase7_strict_alpha_blend_backfill_sql() -> &'static str {
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

fn phase7_optional_overlay_blend_backfill_sql() -> &'static str {
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

async fn upsert_factor_values(
    db: &sqlx::PgPool,
    output: &FactorOutput,
    version: &str,
    store_normalized: bool,
) -> Result<usize, String> {
    let mut saved = 0usize;

    for chunk in output.values.chunks(2_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO factor_value \
             (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at) ",
        );

        builder.push_values(chunk, |mut row, fv| {
            let normalized_value = if store_normalized {
                Some(fv.value)
            } else {
                None::<f64>
            };
            row.push_bind(&output.name)
                .push_bind(version)
                .push_bind(&fv.symbol)
                .push_bind(fv.date)
                .push_bind(fv.value)
                .push_bind(normalized_value)
                .push_bind(fv.available_at.unwrap_or(fv.date));
        });

        builder.push(
            " ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET \
              raw_value = EXCLUDED.raw_value, \
              normalized_value = EXCLUDED.normalized_value, \
              available_at = EXCLUDED.available_at, \
              created_at = NOW()",
        );

        let result = builder.build().execute(db).await.map_err(|error| {
            format!(
                "Failed to upsert factor values for {}: {}",
                output.name, error
            )
        })?;
        saved += result.rows_affected() as usize;
    }

    Ok(saved)
}

// ─── Evaluate all factors (from DB) ────────────────────────

#[derive(Debug, Deserialize)]
pub struct EvaluateAllRequest {
    #[serde(default = "default_horizon")]
    #[allow(dead_code)]
    pub horizon: i16,
    #[serde(default)]
    #[allow(dead_code)]
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Enable industry/size neutralization before evaluation
    #[serde(default)]
    pub neutralize: Option<bool>,
    #[serde(default)]
    pub neutralize_industry: Option<bool>,
    #[serde(default)]
    pub neutralize_size: Option<bool>,
}

fn default_horizon() -> i16 {
    1
}

pub async fn evaluate_all_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateAllRequest>,
) -> impl IntoResponse {
    let end_d = req.end_date.as_deref().unwrap_or("20250509");
    let start_d = req.start_date.as_deref().unwrap_or("20230101");

    // Optional neutralization config
    let do_neutralize = req.neutralize.unwrap_or(false);
    let do_ind = req.neutralize_industry.unwrap_or(true);
    let do_sz = req.neutralize_size.unwrap_or(false);

    // Pre-load industry map + size proxy (once, reused for all factors)
    let neut_config: Option<NeutralizeConfig> = if do_neutralize {
        let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'",
        )
        .fetch_all(&state.db)
        .await
        .ok()
        .unwrap_or_default();
        let industries: HashMap<String, String> = ind_rows
            .into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect();

        let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
        if do_sz {
            let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, amount FROM market_stock_daily_bar
                 WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
                 ORDER BY symbol, trade_date",
            )
            .bind(start_d)
            .bind(end_d)
            .fetch_all(&state.db)
            .await;
            if let Ok(rows) = amt_rows {
                for (sym, date, amt) in rows {
                    if let Some(a) = amt {
                        let a_val: f64 = a.try_into().unwrap_or(0.0);
                        if a_val > 0.0 {
                            size_proxy.entry(sym).or_default().push((date, a_val));
                        }
                    }
                }
            }
        }
        Some(NeutralizeConfig {
            industries,
            size_proxy,
        })
    } else {
        None
    };

    // Get all factor versions
    let all_factors: Vec<(String, String)> = match sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT factor_code, factor_version FROM factor_value ORDER BY factor_code",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => return Json(json!({"code":1,"message":format!("{}",e)})),
    };

    let mut results = Vec::new();

    for (code, ver) in &all_factors {
        // Load factor values
        let fv_rows =
            sqlx::query_as::<_, (String, chrono::NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
             FROM factor_value
             WHERE factor_code=$1 AND factor_version=$2
               AND trade_date>=$3::date AND trade_date<=$4::date
             ORDER BY trade_date, symbol",
            )
            .bind(code)
            .bind(ver)
            .bind(start_d)
            .bind(end_d)
            .fetch_all(&state.db)
            .await;

        let fv_rows = match fv_rows {
            Ok(r) => r,
            Err(e) => {
                results.push(json!({"factor":code,"version":ver,"error":e.to_string()}));
                continue;
            }
        };

        // Build forward returns
        let symbols: Vec<String> = fv_rows
            .iter()
            .map(|(s, _, _)| s.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        let bars = match load_bars(&state.db, &symbols, start_d, end_d, 2).await {
            Ok(b) => b,
            Err(e) => {
                results.push(json!({"factor":code,"version":ver,"error":e.to_string()}));
                continue;
            }
        };

        let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
        for (sym, sym_bars) in &bars {
            for i in 0..sym_bars.len() - 1 {
                let c: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
                let c1: f64 = sym_bars[i + 1].close.try_into().unwrap_or(f64::NAN);
                if c > 0.0 {
                    forward_returns.insert((sym.clone(), sym_bars[i].trade_date), (c1 - c) / c);
                }
            }
        }

        // Create FactorOutput
        let values: Vec<FactorValue> = fv_rows
            .iter()
            .filter_map(|(sym, date, val)| {
                val.and_then(|v| {
                    use rust_decimal::prelude::ToPrimitive;
                    v.to_f64().map(|fv| FactorValue {
                        symbol: sym.clone(),
                        date: *date,
                        value: fv,
                        available_at: None,
                    })
                })
            })
            .collect();

        if values.len() < 100 {
            continue;
        }

        let output = FactorOutput {
            name: code.clone(),
            values,
            metadata: FactorMetadata {
                factor_name: code.clone(),
                category: FactorCategory::PriceVolume,
                version: ver.clone(),
                params: serde_json::json!({}),
                computed_at: chrono::Utc::now(),
                symbol_count: 0,
                date_count: 0,
                coverage_ratio: 0.0,
                mean: f64::NAN,
                std: f64::NAN,
                min: f64::NAN,
                max: f64::NAN,
            },
        };

        // Optional: neutralize before evaluation
        let output = if let Some(ref cfg) = neut_config {
            use quant_factor::neutralize::neutralize;
            let (neut, _res) = neutralize(&output, cfg, do_ind, do_sz);
            neut
        } else {
            output
        };

        let evaluation = evaluate(&output, &forward_returns, 5);
        results.push(
            serde_json::to_value(&evaluation).unwrap_or(json!({"error":"serialization failed"})),
        );
    }

    Json(json!({"code":0,"data":{"evaluations":results,"count":results.len()}}))
}

/// POST /api/v1/quant/factors/evaluate-all/background
///
/// 后台评估所有因子 IC/ICIR，立即返回 task_id。
pub async fn evaluate_all_factors_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateAllRequest>,
) -> impl IntoResponse {
    let task_id = chrono::Utc::now().format("ev-%Y%m%d-%H%M%S%3f").to_string();
    let start_d = req.start_date.clone();
    let end_d = req.end_date.clone();
    let do_neutralize = req.neutralize.unwrap_or(false);
    let do_ind = req.neutralize_industry.unwrap_or(true);
    let do_sz = req.neutralize_size.unwrap_or(false);
    let horizon = req.horizon as usize;

    info!(task_id = %task_id, horizon = horizon, "后台评估所有因子");

    let _ = sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, 'evaluate_all', 'factor', 'running')"
    ).bind(&task_id).execute(&state.db).await;

    let state = state.clone();
    let tid = task_id.clone();

    tokio::spawn(async move {
        let result: Result<usize, String> = async {
            let start = start_d.as_deref().unwrap_or("20160101");
            let end = end_d.as_deref().unwrap_or("20260511");
            let do_neut = do_neutralize;
            let di = do_ind;
            let ds = do_sz;

            let neut_config: Option<NeutralizeConfig> = if do_neut {
                let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
                    "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'"
                ).fetch_all(&state.db).await.ok().unwrap_or_default();
                let industries: HashMap<String, String> = ind_rows.into_iter()
                    .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
                    .collect();
                let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
                if ds {
                    let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                        "SELECT symbol, trade_date, amount FROM market_stock_daily_bar
                         WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0 ORDER BY symbol, trade_date"
                    ).bind(start).bind(end).fetch_all(&state.db).await;
                    if let Ok(rows) = amt_rows {
                        for (sym, date, amt) in rows {
                            if let Some(a) = amt {
                                let a_val: f64 = a.try_into().unwrap_or(0.0);
                                if a_val > 0.0 { size_proxy.entry(sym).or_default().push((date, a_val)); }
                            }
                        }
                    }
                }
                Some(NeutralizeConfig { industries, size_proxy })
            } else { None };

            let all_factors: Vec<(String, String)> = sqlx::query_as::<_, (String, String)>(
                "SELECT DISTINCT factor_code, factor_version FROM factor_value ORDER BY factor_code"
            ).fetch_all(&state.db).await.map_err(|e| e.to_string())?;

            // Load forward returns ONCE for all factors — support multi-horizon
            // Load close prices grouped by symbol, compute N-day forward return
            let fwd_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, close FROM market_stock_daily_bar
                 WHERE trade_date >= $1::date AND trade_date <= $2::date
                   AND close > 0
                 ORDER BY symbol, trade_date"
            ).bind(start).bind(end).fetch_all(&state.db).await
              .map_err(|e| format!("Failed to load forward returns: {}", e))?;

            info!(fwd_rows = fwd_rows.len(), horizon = horizon, "Loaded close prices");

            // Group close prices by symbol: symbol -> [(date, close)]
            let mut close_by_sym: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
            for (sym, date, close) in &fwd_rows {
                if let Some(c) = close {
                    let cf: f64 = (*c).try_into().unwrap_or(0.0);
                    if cf > 0.0 {
                        close_by_sym.entry(sym.clone()).or_default().push((*date, cf));
                    }
                }
            }

            // Build N-day forward returns: date T -> (close_T+N - close_T) / close_T
            let mut fwd_map: HashMap<NaiveDate, HashMap<String, f64>> = HashMap::new();
            for (sym, prices) in &close_by_sym {
                for i in 0..prices.len().saturating_sub(horizon) {
                    let (date, close_t) = prices[i];
                    let (_date_n, close_n) = prices[i + horizon];
                    let ret = (close_n - close_t) / close_t;
                    fwd_map.entry(date).or_default().insert(sym.clone(), ret);
                }
            }
            info!(fwd_dates = fwd_map.len(), "Forward return map built");

            let mut count = 0usize;
            for (code, ver) in &all_factors {
                info!(factor = %code, version = %ver, "Evaluating factor");

                let fv_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                    "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
                     FROM factor_value WHERE factor_code=$1 AND factor_version=$2
                     AND trade_date >= $3::date AND trade_date <= $4::date ORDER BY symbol, trade_date"
                ).bind(code).bind(ver).bind(start).bind(end)
                  .fetch_all(&state.db).await.unwrap_or_default();

                info!(factor = %code, fv_rows = fv_rows.len(), "Factor values loaded");

                if fv_rows.len() < 100 { continue; }

                let mut values_map: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
                for (sym, date, val) in &fv_rows {
                    if let Some(v) = val {
                        let vf: f64 = (*v).try_into().unwrap_or(0.0);
                        values_map.entry(*date).or_default().push((sym.clone(), vf));
                    }
                }

                let mut output = FactorOutput {
                    name: code.clone(),
                    values: vec![],
                    metadata: FactorMetadata {
                        factor_name: code.clone(),
                        category: FactorCategory::PriceVolume,
                        version: ver.clone(),
                        params: json!({}),
                        computed_at: chrono::Utc::now(),
                        symbol_count: 0,
                        date_count: 0,
                        coverage_ratio: 0.0,
                        mean: 0.0, std: 0.0, min: 0.0, max: 0.0,
                    },
                };
                let mut forward_returns: HashMap<(String, NaiveDate), f64> = HashMap::new();
                let dates: Vec<NaiveDate> = {
                    let mut ds: Vec<NaiveDate> = values_map.keys().copied().collect();
                    ds.sort(); ds
                };
                for &date in &dates {
                    if let (Some(vals), Some(fwds)) = (values_map.get(&date), fwd_map.get(&date)) {
                        for (sym, val) in vals {
                            let fv = FactorValue { symbol: sym.clone(), date, value: *val, available_at: None };
                            output.values.push(fv);
                            if let Some(fwd) = fwds.get(sym) {
                                forward_returns.insert((sym.clone(), date), *fwd);
                            }
                        }
                    }
                }

                if let Some(ref cfg) = neut_config {
                    use quant_factor::neutralize::neutralize;
                    let (neut, _) = neutralize(&output, cfg, di, ds);
                    output = neut;
                }

                let evaluation = evaluate(&output, &forward_returns, 5);
                let ic_json = serde_json::to_value(&evaluation.ic_series).unwrap_or(json!([]));
                let qr_json = serde_json::to_value(&evaluation.quantile_returns).unwrap_or(json!([]));
                let insert_result = sqlx::query(
                    "INSERT INTO factor_evaluation (factor_code, factor_version, horizon, start_date, end_date,
                     mean_ic, ic_ir, mean_rank_ic, rank_ic_ir, ic_series, rank_ic_series,
                     quantile_spread, quantile_returns, period_count, symbol_count, total_pairs)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,0,0)
                     ON CONFLICT (factor_code, factor_version, horizon, start_date, end_date) DO UPDATE SET
                     mean_ic=EXCLUDED.mean_ic, ic_ir=EXCLUDED.ic_ir,
                     mean_rank_ic=EXCLUDED.mean_rank_ic, rank_ic_ir=EXCLUDED.rank_ic_ir,
                     ic_series=EXCLUDED.ic_series, rank_ic_series=EXCLUDED.rank_ic_series,
                     quantile_spread=EXCLUDED.quantile_spread, quantile_returns=EXCLUDED.quantile_returns,
                     period_count=EXCLUDED.period_count"
                )
                .bind(code).bind(ver)
                .bind(horizon as i32)
                .bind(evaluation.date_range.0).bind(evaluation.date_range.1)
                .bind(evaluation.mean_ic).bind(evaluation.ic_ir)
                .bind(evaluation.mean_rank_ic).bind(evaluation.rank_ic_ir)
                .bind(&ic_json).bind(&ic_json)
                .bind(evaluation.quantile_spread).bind(&qr_json)
                .bind(evaluation.period_count as i32)
                .execute(&state.db).await;
                if let Err(ref e) = insert_result {
                    tracing::error!(factor = %code, error = %e, "Failed to insert evaluation");
                }
                count += 1;
            }
            Ok(count)
        }.await;

        match result {
            Ok(count) => {
                info!(task_id = %tid, count = count, "后台评估完成");
                let _ = sqlx::query("UPDATE data_sync_task SET status='completed', total_count=$1, progress=100, completed_at=now() WHERE task_id=$2")
                    .bind(count as i32).bind(&tid).execute(&state.db).await;
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "后台评估失败");
                let _ = sqlx::query("UPDATE data_sync_task SET status='failed', completed_at=now() WHERE task_id=$1")
                    .bind(&tid).execute(&state.db).await;
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": task_id, "status": "running"}}))
}

// ─── Combine factors ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CombineFactorsRequest {
    pub combo_name: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub factors: Vec<FactorRef>,
    #[serde(default = "default_combine_method")]
    pub method: String,
    #[serde(default = "default_horizon")]
    pub horizon: i16,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FactorRef {
    pub factor_code: String,
    #[serde(default = "default_version")]
    pub factor_version: String,
}

fn default_combine_method() -> String {
    "icir_weighted".to_string()
}

pub async fn combine_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CombineFactorsRequest>,
) -> impl IntoResponse {
    let factors: Vec<(String, String)> = req
        .factors
        .iter()
        .map(|f| (f.factor_code.clone(), f.factor_version.clone()))
        .collect();

    let method = match req.method.as_str() {
        "equal_weight" => CombineMethod::EqualWeight,
        _ => CombineMethod::IcirWeighted,
    };

    let weights = match compute_weights(&state.db, &factors, method, req.horizon).await {
        Ok(w) => w,
        Err(e) => return Json(json!({"code":1,"message":e})),
    };

    let sd = req
        .start_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
    let ed = req
        .end_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());

    let inserted =
        match combine_and_persist(&state.db, &req.combo_name, &req.version, &weights, sd, ed).await
        {
            Ok(n) => n,
            Err(e) => return Json(json!({"code":1,"message":e})),
        };

    let weights_json: Vec<serde_json::Value> = weights
        .iter()
        .map(|w| {
            json!({
                "factor_code": w.factor_code,
                "factor_version": w.factor_version,
                "weight": w.weight,
            })
        })
        .collect();

    Json(
        json!({"code":0,"data":{"combo_name":req.combo_name,"version":req.version,"weights":weights_json,"inserted":inserted}}),
    )
}

// ─── Helpers ──────────────────────────────────────────────────────

/// Parse factor string like "mom_20d" → ("momentum", 20) or "turn_5d" → ("turnover", 5)
fn parse_factor(name: &str) -> Option<(&'static str, usize)> {
    let name = name.strip_suffix("_std").unwrap_or(name);
    if let Some(rest) = name.strip_prefix("mom_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("momentum", period))
    } else if let Some(rest) = name.strip_prefix("vol_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("volatility", period))
    } else if let Some(rest) = name.strip_prefix("downvol_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("downside_volatility", period))
    } else if let Some(rest) = name.strip_prefix("rev_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("reversal", period))
    } else if let Some(rest) = name.strip_prefix("turn_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("turnover", period))
    } else if let Some(rest) = name.strip_prefix("amihud_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amihud_illiquidity", period))
    } else if let Some(rest) = name.strip_prefix("amt_intensity_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amount_intensity", period))
    } else if let Some(rest) = name.strip_prefix("rsi_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("rsi", period))
    } else if let Some(rest) = name.strip_prefix("bb_pos_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("bb_position", period))
    } else if let Some(rest) = name.strip_prefix("atr_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("atr", period))
    } else if let Some(rest) = name.strip_prefix("amp_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amplitude", period))
    } else if let Some(rest) = name.strip_prefix("vp_corr_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("vol_price_corr", period))
    } else if let Some(rest) = name.strip_prefix("skew_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("skewness", period))
    } else if let Some(rest) = name.strip_prefix("maxdd_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("max_drawdown", period))
    } else {
        None
    }
}

/// Load daily bars from PostgreSQL
async fn load_bars(
    pool: &sqlx::PgPool,
    symbols: &[String],
    start_date: &str,
    end_date: &str,
    min_records: usize,
) -> Result<HashMap<String, Vec<DailyBar>>, Box<dyn std::error::Error>> {
    let mut result: HashMap<String, Vec<DailyBar>> = HashMap::new();

    for sym in symbols {
        let rows = sqlx::query_as::<_, (String, chrono::NaiveDate, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, rust_decimal::Decimal, rust_decimal::Decimal)>(
            "SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
             FROM market_stock_daily_bar
             WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
             ORDER BY trade_date ASC"
        )
        .bind(sym)
        .bind(start_date)
        .bind(end_date)
        .fetch_all(pool)
        .await?;

        if rows.len() < min_records {
            tracing::warn!(
                "{} only has {} records (need {})",
                sym,
                rows.len(),
                min_records
            );
        }

        let bars: Vec<DailyBar> = rows
            .into_iter()
            .map(|(s, d, o, h, l, c, pc, cp, v, a)| DailyBar {
                symbol: s,
                trade_date: d,
                open: o,
                high: h,
                low: l,
                close: c,
                pre_close: pc,
                change_pct: cp,
                volume: v,
                amount: a,
            })
            .collect();

        result.insert(sym.to_string(), bars);
    }

    Ok(result)
}

// ─── Factor Neutralization ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NeutralizeRequest {
    pub factor: String,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub do_industry: bool,
    #[serde(default)]
    pub do_size: bool,
}

pub async fn neutralize_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<NeutralizeRequest>,
) -> impl IntoResponse {
    use quant_factor::neutralize::{neutralize, NeutralizeConfig};

    // 1. Load factor values
    let rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, raw_value FROM factor_value
         WHERE factor_code = $1 AND factor_version = '1.0.0'
           AND trade_date >= $2::date AND trade_date <= $3::date
         ORDER BY trade_date, symbol",
    )
    .bind(&req.factor)
    .bind(&req.start_date)
    .bind(&req.end_date)
    .fetch_all(&state.db)
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => return Json(json!({"code":1,"message":format!("load factor: {}",e)})),
    };

    let values: Vec<FactorValue> = rows
        .into_iter()
        .filter_map(|(sym, date, raw)| {
            raw.map(|r| FactorValue {
                symbol: sym,
                date,
                value: r.try_into().unwrap_or(f64::NAN),
                available_at: None,
            })
        })
        .collect();

    if values.is_empty() {
        return Json(json!({"code":1,"message":"no factor values found"}));
    }

    let output = FactorOutput {
        name: req.factor.clone(),
        values,
        metadata: FactorMetadata {
            factor_name: req.factor.clone(),
            category: FactorCategory::PriceVolume,
            version: "1.0.0".into(),
            params: json!({}),
            computed_at: chrono::Utc::now(),
            symbol_count: 0,
            date_count: 0,
            coverage_ratio: 0.0,
            mean: 0.0,
            std: 0.0,
            min: 0.0,
            max: 0.0,
        },
    };

    // 2. Load industry map
    let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'",
    )
    .fetch_all(&state.db)
    .await;

    let industries: HashMap<String, String> = match ind_rows {
        Ok(r) => r
            .into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect(),
        Err(e) => return Json(json!({"code":1,"message":format!("load industries: {}",e)})),
    };

    // 3. Load size proxy (log daily amount)
    let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, amount FROM market_stock_daily_bar
         WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
         ORDER BY symbol, trade_date",
    )
    .bind(&req.start_date)
    .bind(&req.end_date)
    .fetch_all(&state.db)
    .await;

    let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    if let Ok(rows) = amt_rows {
        for (sym, date, amt) in rows {
            if let Some(a) = amt {
                let a_val: f64 = a.try_into().unwrap_or(0.0);
                if a_val > 0.0 {
                    size_proxy.entry(sym).or_default().push((date, a_val));
                }
            }
        }
    }

    let config = NeutralizeConfig {
        industries,
        size_proxy,
    };

    // 4. Neutralize
    let (neut_output, result) = neutralize(&output, &config, req.do_industry, req.do_size);

    // 5. Save neutralized values (back to original factor's neutralized_value column)
    let mut saved = 0usize;
    for chunk in neut_output.values.chunks(500) {
        let mut tx = match state.db.begin().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        for fv in chunk {
            if !fv.value.is_finite() {
                continue;
            }
            let res = sqlx::query(
                "UPDATE factor_value SET neutralized_value = $4
                 WHERE factor_code = $1 AND factor_version = '1.0.0'
                   AND symbol = $2 AND trade_date = $3",
            )
            .bind(&req.factor) // original factor name
            .bind(&fv.symbol)
            .bind(fv.date)
            .bind(fv.value)
            .execute(&mut *tx)
            .await;
            if let Ok(r) = res {
                saved += r.rows_affected() as usize;
            }
        }
        let _ = tx.commit().await;
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": neut_output.name,
            "original_count": result.original_count,
            "neutralized_count": result.neutralized_count,
            "saved": saved,
            "industry_neutral": result.industry_neutral,
            "size_neutral": result.size_neutral,
            "sample_values": &neut_output.values[..neut_output.values.len().min(5)]
                .iter().map(|v| json!({"symbol":v.symbol,"date":v.date,"value":v.value})).collect::<Vec<_>>(),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_factor_definition_request_defaults_and_trims_fields() {
        let req = RegisterFactorDefinitionRequest {
            factor_code: " mom_5d_std ".to_string(),
            version: " definition-api-v1 ".to_string(),
            name: " 5 日动量标准化 ".to_string(),
            category: " price_volume ".to_string(),
            frequency: None,
            dependencies: None,
            parameters: None,
            status: None,
        };

        let definition = req.into_definition().expect("valid definition");

        assert_eq!(definition.factor_code, "mom_5d_std");
        assert_eq!(definition.version, "definition-api-v1");
        assert_eq!(definition.name, "5 日动量标准化");
        assert_eq!(definition.category, "price_volume");
        assert_eq!(definition.frequency, "daily");
        assert_eq!(definition.dependencies, json!([]));
        assert_eq!(definition.parameters, json!({}));
        assert_eq!(definition.status, "active");
    }

    #[test]
    fn register_factor_definition_request_rejects_required_blank_fields() {
        let req = RegisterFactorDefinitionRequest {
            factor_code: " ".to_string(),
            version: "v1".to_string(),
            name: "name".to_string(),
            category: "price_volume".to_string(),
            frequency: None,
            dependencies: None,
            parameters: None,
            status: None,
        };

        let err = req.into_definition().unwrap_err();

        assert!(err.contains("factor_code"));
    }

    #[test]
    fn financial_indicator_factor_uses_announcement_date_as_pit_date() {
        let row = FinancialIndicatorRow {
            symbol: "000001.SZ".to_string(),
            ann_date: NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 3, 31).unwrap(),
            value: Decimal::new(1234, 4),
        };

        let factor_value = row.to_factor_value();

        assert_eq!(factor_value.date, row.ann_date);
        assert_eq!(factor_value.available_at, row.ann_date);
        assert_ne!(factor_value.date, row.end_date);
        assert_eq!(factor_value.value, 0.1234);
    }

    #[test]
    fn parse_factor_supports_phase7_alpha_sources() {
        assert_eq!(parse_factor("rev_5d"), Some(("reversal", 5)));
        assert_eq!(
            parse_factor("downvol_20d"),
            Some(("downside_volatility", 20))
        );
        assert_eq!(parse_factor("amihud_20d"), Some(("amihud_illiquidity", 20)));
        assert_eq!(
            parse_factor("amt_intensity_20d"),
            Some(("amount_intensity", 20))
        );
    }

    #[test]
    fn background_factor_task_id_is_unique_for_bursty_requests() {
        let mut ids = std::collections::HashSet::new();

        for _ in 0..100 {
            let task_id = background_factor_task_id();
            assert!(task_id.starts_with("fs-"));
            assert!(ids.insert(task_id));
        }
    }

    #[test]
    fn phase7_price_volume_backfill_request_builds_default_plan() {
        let req = Phase7PriceVolumeBackfillRequest {
            start_date: Some(" 2016-02-01 ".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        };

        let plan = req.into_plan().expect("valid phase7 plan");

        assert_eq!(
            plan.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).unwrap()
        );
        assert_eq!(plan.end_date, NaiveDate::from_ymd_opt(2026, 5, 11).unwrap());
        assert_eq!(plan.version, "1.0.0");
        assert_eq!(plan.combo_name, "phase7_price_volume_expanded_v1");
        assert_eq!(plan.task_type, "phase7_price_volume_backfill");
        assert_eq!(plan.source, "factor");
        assert!(plan.task_type.len() <= 64);
        assert!(plan.source.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 0);
    }

    #[test]
    fn phase7_request_builds_generic_set_based_backfill_contract() {
        let req = Phase7PriceVolumeBackfillRequest {
            start_date: Some("20160201".to_string()),
            end_date: Some("20260511".to_string()),
            version: Some("phase7j-test".to_string()),
            combo_name: Some("phase7_price_volume_expanded_v1_test".to_string()),
            statement_timeout_ms: Some(120_000),
        };

        let plan = req.into_plan().expect("valid set-based plan");

        assert_eq!(plan.bundle_name, "phase7_price_volume_expanded_v1");
        assert_eq!(plan.category, "price_volume");
        assert_eq!(plan.phase, "7-J");
        assert_eq!(plan.combo_method, "equal_weight");
        assert_eq!(plan.experiment_type, "phase7_factor_backfill_profile");
        assert_eq!(plan.dependencies, &["market_stock_daily_bar"]);
        assert_eq!(plan.statement_timeout_ms, 120_000);
    }

    #[test]
    fn phase7_financial_quality_backfill_request_builds_daily_pit_plan() {
        let req = Phase7FinancialQualityBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("phase7f-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid financial quality plan");

        assert_eq!(plan.bundle_name, "phase7_financial_quality_v1");
        assert_eq!(plan.combo_name, "phase7_financial_quality_v1");
        assert_eq!(plan.task_type, "phase7_financial_quality_backfill");
        assert_eq!(plan.category, "fundamental");
        assert_eq!(
            plan.dependencies,
            &["market_financial_indicator", "market_trade_calendar"]
        );
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_industry_residual_quality_backfill_request_builds_daily_pit_plan() {
        let req = Phase7IndustryResidualQualityBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("phase7ag-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req
            .into_plan()
            .expect("valid industry-residual quality plan");

        assert_eq!(plan.bundle_name, "phase7_industry_residual_quality_v1");
        assert_eq!(plan.combo_name, "phase7_industry_residual_quality_v1");
        assert_eq!(plan.task_type, "phase7_industry_residual_quality_backfill");
        assert_eq!(plan.category, "fundamental_residual");
        assert_eq!(plan.phase, "7-AG/7-J");
        assert_eq!(
            plan.dependencies,
            &[
                "market_financial_indicator",
                "market_trade_calendar",
                "market_stock"
            ]
        );
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_relative_strength_backfill_request_builds_market_industry_plan() {
        let req = Phase7RelativeStrengthBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("phase7rel-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid relative strength plan");

        assert_eq!(plan.bundle_name, "phase7_relative_strength_v1");
        assert_eq!(plan.combo_name, "phase7_relative_strength_v1");
        assert_eq!(plan.task_type, "phase7_relative_strength_backfill");
        assert_eq!(plan.category, "relative_strength");
        assert_eq!(
            plan.dependencies,
            &["market_stock_daily_bar", "market_stock"]
        );
        assert_eq!(plan.phase, "7-B/7-J");
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_quality_relative_strength_request_builds_combo_only_plan() {
        let req = Phase7QualityRelativeStrengthBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(0),
        };

        let plan = req.into_plan().expect("valid composite combo plan");

        assert_eq!(plan.bundle_name, "phase7_quality_relative_strength_v1");
        assert_eq!(plan.combo_name, "phase7_quality_relative_strength_v1");
        assert_eq!(plan.task_type, "phase7_quality_relative_strength_backfill");
        assert_eq!(plan.category, "composite_alpha");
        assert_eq!(plan.combo_method, "quality_60_relative_strength_40");
        assert!(plan.dependencies.contains(&"factor_value"));
    }

    #[test]
    fn phase7_growth_recovery_backfill_request_builds_daily_pit_plan() {
        let req = Phase7GrowthRecoveryBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("phase7gr-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid growth recovery plan");

        assert_eq!(plan.bundle_name, "phase7_growth_recovery_v1");
        assert_eq!(plan.combo_name, "phase7_growth_recovery_v1");
        assert_eq!(plan.task_type, "phase7_growth_recovery_backfill");
        assert_eq!(plan.category, "fundamental_growth");
        assert_eq!(
            plan.dependencies,
            &["market_financial_indicator", "market_trade_calendar"]
        );
        assert_eq!(plan.phase, "7-B/7-J");
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_valuation_backfill_request_builds_daily_basic_plan() {
        let req = Phase7ValuationBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid valuation plan");

        assert_eq!(plan.bundle_name, "phase7_valuation_v1");
        assert_eq!(plan.combo_name, "phase7_valuation_v1");
        assert_eq!(plan.task_type, "phase7_valuation_backfill");
        assert_eq!(plan.category, "valuation");
        assert_eq!(plan.dependencies, &["market_stock_daily_basic"]);
        assert_eq!(plan.phase, "7-B/7-J");
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_moneyflow_backfill_request_builds_moneyflow_plan() {
        let req = Phase7MoneyflowBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid moneyflow plan");

        assert_eq!(plan.bundle_name, "phase7_moneyflow_v1");
        assert_eq!(plan.combo_name, "phase7_moneyflow_v1");
        assert_eq!(plan.task_type, "phase7_moneyflow_backfill");
        assert_eq!(plan.category, "moneyflow_alpha");
        assert_eq!(
            plan.dependencies,
            &["market_stock_moneyflow", "market_stock_daily_bar"]
        );
        assert_eq!(plan.phase, "7-B/7-J");
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_alpha_blend_request_builds_weighted_combo_plan() {
        let req = Phase7AlphaBlendBackfillRequest {
            start_date: Some("2016-04-05".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("1.0.0".to_string()),
            combo_name: Some("phase7_value_quality_growth_rel_v1".to_string()),
            statement_timeout_ms: Some(180_000),
            allow_signed_weights: false,
            sources: vec![
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_valuation_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: 0.30,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_financial_quality_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: 0.30,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_growth_recovery_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: 0.30,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_relative_strength_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: 0.10,
                },
            ],
        };

        let plan = req.into_plan().expect("valid alpha blend plan");

        assert_eq!(plan.combo_name, "phase7_value_quality_growth_rel_v1");
        assert_eq!(plan.bundle_name, "phase7_alpha_blend_v1");
        assert_eq!(plan.task_type, "phase7_alpha_blend_backfill");
        assert_eq!(plan.combo_method, "weighted_combo_blend");
        assert_eq!(plan.source_combos.len(), 4);
        assert!(
            (plan
                .source_combos
                .iter()
                .map(|source| source.weight)
                .sum::<f64>()
                - 1.0)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn phase7_alpha_blend_request_allows_explicit_signed_weights() {
        let req = Phase7AlphaBlendBackfillRequest {
            start_date: Some("2016-04-05".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("1.0.0".to_string()),
            combo_name: Some("phase7_quality_moneyflow_confirm_v1".to_string()),
            statement_timeout_ms: None,
            allow_signed_weights: true,
            sources: vec![
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_financial_quality_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: 0.90,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_moneyflow_v1".to_string(),
                    version: Some("1.0.0".to_string()),
                    weight: -0.10,
                },
            ],
        };

        let plan = req.into_plan().expect("valid signed alpha blend plan");

        assert_eq!(plan.combo_name, "phase7_quality_moneyflow_confirm_v1");
        assert_eq!(plan.combo_method, "weighted_combo_blend_signed");
        assert!(
            (plan
                .source_combos
                .iter()
                .map(|source| source.weight.abs())
                .sum::<f64>()
                - 1.0)
                .abs()
                < 1e-9
        );
        assert!(plan.source_combos.iter().any(|source| source.weight < 0.0));
    }

    #[test]
    fn phase7_alpha_blend_profiles_request_builds_default_weight_grid() {
        let req = Phase7AlphaBlendProfilesBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2016-02-05".to_string()),
            version: None,
            profile_names: None,
            statement_timeout_ms: Some(0),
        };

        let plans = req.into_plans().expect("valid profile plans");
        let combo_names = plans
            .iter()
            .map(|plan| plan.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(plans.len(), 13);
        assert!(combo_names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(combo_names.contains("phase7_blend_value_tilt_v1"));
        assert!(combo_names.contains("phase7_blend_quality_growth_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert!(combo_names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(combo_names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        for plan in plans {
            assert_eq!(plan.task_type, "phase7_alpha_blend_profiles_backfill");
            if plan.combo_name == "phase7_quality_event_window_overlay_v1" {
                assert_eq!(plan.combo_method, "weighted_combo_optional_overlay");
            } else {
                assert_eq!(plan.combo_method, "weighted_combo_blend_profile");
            }
            assert!(plan.source_combos.len() >= 2);
            assert!(
                (plan
                    .source_combos
                    .iter()
                    .map(|source| source.weight)
                    .sum::<f64>()
                    - 1.0)
                    .abs()
                    < 1e-9
            );
        }
    }

    #[test]
    fn phase7_price_volume_specs_are_equal_weighted_combo_inputs() {
        let specs = phase7_price_volume_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "rev_5d_std",
                "rev_20d_std",
                "downvol_20d_std",
                "amihud_20d_std",
                "amt_intensity_20d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.2).abs() < 1e-12));
    }

    #[test]
    fn phase7_financial_quality_specs_include_quality_growth_and_low_leverage() {
        let specs = phase7_financial_quality_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "fin_roe_daily_std",
                "fin_roa_daily_std",
                "fin_gross_margin_daily_std",
                "fin_netprofit_margin_daily_std",
                "fin_current_ratio_daily_std",
                "fin_debt_to_assets_daily_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
    }

    #[test]
    fn phase7_relative_strength_specs_include_market_and_industry_momentum() {
        let specs = phase7_relative_strength_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "mkt_rel_mom_20d_std",
                "mkt_rel_mom_60d_std",
                "ind_rel_mom_20d_std",
                "ind_rel_mom_60d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.25).abs() < 1e-12));
    }

    #[test]
    fn phase7_quality_relative_strength_specs_blend_quality_and_relative_strength() {
        let specs = phase7_quality_relative_strength_combo_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(specs.len(), 10);
        assert!(codes.contains(&"fin_roe_daily_std"));
        assert!(codes.contains(&"fin_debt_to_assets_daily_std"));
        assert!(codes.contains(&"mkt_rel_mom_60d_std"));
        assert!(codes.contains(&"ind_rel_mom_60d_std"));
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.10).abs() < 1e-12));
    }

    #[test]
    fn phase7_growth_recovery_specs_focus_on_non_momentum_alpha_sources() {
        let specs = phase7_growth_recovery_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "fin_eps_yoy_recovery_std",
                "fin_roe_yoy_delta_std",
                "fin_gross_margin_yoy_delta_std",
                "fin_netprofit_margin_yoy_delta_std",
                "fin_debt_to_assets_yoy_improve_std",
                "fin_current_ratio_yoy_delta_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
    }

    #[test]
    fn phase7_valuation_specs_use_daily_basic_value_fields() {
        let specs = phase7_valuation_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "val_pe_ttm_low_std",
                "val_pb_low_std",
                "val_ps_ttm_low_std",
                "val_dividend_yield_ttm_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
    }

    #[test]
    fn phase7_moneyflow_specs_focus_on_fund_flow_alpha_sources() {
        let specs = phase7_moneyflow_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "mf_net_amount_5d_std",
                "mf_net_amount_20d_std",
                "mf_elg_net_amount_5d_std",
                "mf_lg_elg_net_amount_20d_std",
                "mf_small_sell_pressure_20d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.2).abs() < 1e-12));
    }

    #[test]
    fn phase7_event_alpha_specs_focus_on_pit_earnings_events() {
        let specs = phase7_event_alpha_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "event_forecast_change_mid_std",
                "event_forecast_profit_floor_std",
                "event_express_yoy_dedu_np_std",
                "event_express_roe_std",
                "event_disclosure_early_days_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
    }

    #[test]
    fn phase7_event_window_alpha_plan_uses_decayed_event_combo_metadata() {
        let req = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(300_000),
        };

        let plan = req.into_plan().expect("valid event-window alpha plan");

        assert_eq!(plan.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(plan.bundle_name, "phase7_event_window_earnings_v1");
        assert_eq!(plan.combo_method, "weighted_event_window_earnings");
        assert_eq!(plan.phase, "7-Y2");
        assert_eq!(plan.category, "event_alpha");
        assert_eq!(plan.statement_timeout_ms, 300_000);
    }

    #[test]
    fn phase7_event_window_alpha_specs_use_available_event_sources_only() {
        let specs = phase7_event_window_alpha_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "event_window_forecast_change_20d_decay_std",
                "event_window_forecast_profit_floor_20d_decay_std",
                "event_window_express_roe_20d_decay_std",
                "event_window_disclosure_early_days_20d_decay_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(
            !codes
                .iter()
                .any(|code| code.contains("yoy_dedu_np")),
            "ordinary Tushare express data currently leaves yoy_dedu_np empty, so the first event-window bundle should not spend weight there"
        );
    }

    #[test]
    fn phase7_event_surprise_alpha_plan_uses_segmented_event_combo_metadata() {
        let req = Phase7EventSurpriseBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(300_000),
        };

        let plan = req.into_plan().expect("valid event-surprise alpha plan");

        assert_eq!(plan.combo_name, "phase7_event_surprise_v1");
        assert_eq!(plan.bundle_name, "phase7_event_surprise_v1");
        assert_eq!(plan.combo_method, "weighted_event_surprise");
        assert_eq!(plan.phase, "7-Y4");
        assert_eq!(plan.category, "event_alpha");
        assert_eq!(plan.statement_timeout_ms, 300_000);
        assert!(plan.dependencies.contains(&"market_stock_forecast"));
        assert!(plan.dependencies.contains(&"market_stock_express"));
        assert!(plan.dependencies.contains(&"market_stock_disclosure_date"));
    }

    #[test]
    fn phase7_event_surprise_alpha_specs_use_segmented_surprise_only() {
        let specs = phase7_event_surprise_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "event_forecast_surprise_bucket_std",
                "event_forecast_profit_floor_sign_std",
                "event_express_roe_bucket_std",
                "event_disclosure_timing_bucket_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(
            !codes.iter().any(|code| code.contains("yoy_dedu_np")),
            "event surprise bundle should avoid empty ordinary express yoy_dedu_np fields"
        );
    }

    #[test]
    fn phase7_event_surprise_alpha_sql_uses_case_buckets_and_pit_carry_forward() {
        let specs = phase7_event_surprise_backfill_specs();
        let forecast = specs
            .iter()
            .find(|spec| spec.factor_code == "event_forecast_surprise_bucket_std")
            .expect("forecast surprise spec");
        let express = specs
            .iter()
            .find(|spec| spec.factor_code == "event_express_roe_bucket_std")
            .expect("express surprise spec");
        let disclosure = specs
            .iter()
            .find(|spec| spec.factor_code == "event_disclosure_timing_bucket_std")
            .expect("disclosure surprise spec");

        let forecast_sql = phase7_factor_backfill_sql(forecast);
        let express_sql = phase7_factor_backfill_sql(express);
        let disclosure_sql = phase7_factor_backfill_sql(disclosure);

        assert!(forecast_sql.contains("market_stock_forecast"));
        assert!(forecast_sql.contains("CASE"));
        assert!(forecast_sql.contains("WHEN ((COALESCE(event.p_change_min, event.p_change_max)"));
        assert!(forecast_sql.contains("COALESCE(event.p_change_min, event.p_change_max)"));
        assert!(forecast_sql.contains("COALESCE(event.net_profit_min::double precision, 0.0)"));
        assert!(forecast_sql.contains("event.available_at <= td.trade_date"));
        assert!(forecast_sql
            .contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));

        assert!(express_sql.contains("market_stock_express"));
        assert!(express_sql.contains("COALESCE(event.diluted_roe::double precision, 0.0)"));
        assert!(express_sql.contains("CASE"));
        assert!(express_sql.contains("event.available_at <= td.trade_date"));

        assert!(disclosure_sql.contains("market_stock_disclosure_date"));
        assert!(disclosure_sql.contains("event.pre_date"));
        assert!(disclosure_sql.contains("event.actual_date"));
        assert!(disclosure_sql.contains("CASE"));
    }

    #[test]
    fn set_based_factor_backfill_job_wraps_specs_plan_and_sql_builder() {
        let req = Phase7PriceVolumeBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-11".to_string()),
            version: Some("phase7j-test".to_string()),
            combo_name: Some("phase7_price_volume_expanded_v1_test".to_string()),
            statement_timeout_ms: Some(60_000),
        };
        let plan = req.into_plan().expect("valid set-based plan");
        let specs = phase7_price_volume_backfill_specs();
        let job = SetBasedFactorBackfillJob::new(&plan, &specs, phase7_factor_backfill_sql);

        assert_eq!(job.total_steps(), specs.len() + 1);
        assert_eq!(job.plan.combo_method, "equal_weight");
        assert_eq!(job.specs[0].factor_code, "rev_5d_std");
        assert!((job.factor_sql)(&job.specs[0]).contains("market_stock_daily_bar"));
    }

    #[test]
    fn phase7_backfill_sql_uses_set_based_rank_and_complete_window_frames() {
        let specs = phase7_price_volume_backfill_specs();
        let downvol = specs
            .iter()
            .find(|spec| spec.factor_code == "downvol_20d_std")
            .expect("downvol spec");
        let sql = phase7_factor_backfill_sql(downvol);

        assert!(sql.contains("market_stock_daily_bar"));
        assert!(sql.contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));
        assert!(sql.contains("ROWS BETWEEN 19 PRECEDING AND CURRENT ROW"));
        assert!(sql.contains("ON CONFLICT (factor_code, factor_version, symbol, trade_date)"));

        let combo_sql = phase7_combo_backfill_sql();
        assert!(combo_sql.contains("multi_factor_value"));
        assert!(combo_sql.contains("HAVING COUNT(DISTINCT fv.factor_code) >= $6"));
        assert!(combo_sql.contains("/ NULLIF(SUM(weights.weight), 0.0) AS raw_score"));
    }

    #[test]
    fn phase7_financial_quality_sql_uses_daily_pit_carry_forward() {
        let specs = phase7_financial_quality_backfill_specs();
        let roe = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_roe_daily_std")
            .expect("roe spec");
        let debt = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_debt_to_assets_daily_std")
            .expect("debt spec");

        let roe_sql = phase7_factor_backfill_sql(roe);
        let debt_sql = phase7_factor_backfill_sql(debt);

        assert!(roe_sql.contains("market_financial_indicator"));
        assert!(roe_sql.contains("JOIN LATERAL"));
        assert!(roe_sql.contains("fi.ann_date <= td.trade_date"));
        assert!(roe_sql.contains("available_at"));
        assert!(roe_sql.contains("ORDER BY raw_value) AS normalized_value"));
        assert!(debt_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
    }

    #[test]
    fn phase7_industry_residual_quality_sql_removes_same_industry_mean() {
        let specs = phase7_industry_residual_quality_backfill_specs();
        let roe = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_roe_indrel_daily_std")
            .expect("industry residual roe spec");
        let debt = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_debt_to_assets_indrel_daily_std")
            .expect("industry residual debt spec");

        let roe_sql = phase7_factor_backfill_sql(roe);
        let debt_sql = phase7_factor_backfill_sql(debt);

        assert!(roe_sql.contains("JOIN market_stock ms ON ms.symbol = symbols.symbol"));
        assert!(roe_sql.contains("COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry"));
        assert!(roe_sql.contains("PARTITION BY trade_date, industry"));
        assert!(roe_sql.contains("raw_value - AVG(raw_value)"));
        assert!(roe_sql.contains("fi.ann_date <= td.trade_date"));
        assert!(roe_sql.contains("ORDER BY raw_value) AS normalized_value"));
        assert!(debt_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
    }

    #[test]
    fn phase7_relative_strength_sql_uses_market_stock_and_relative_baseline() {
        let specs = phase7_relative_strength_backfill_specs();
        let market = specs
            .iter()
            .find(|spec| spec.factor_code == "mkt_rel_mom_20d_std")
            .expect("market relative spec");
        let industry = specs
            .iter()
            .find(|spec| spec.factor_code == "ind_rel_mom_20d_std")
            .expect("industry relative spec");

        let market_sql = phase7_factor_backfill_sql(market);
        let industry_sql = phase7_factor_backfill_sql(industry);

        assert!(market_sql.contains("JOIN market_stock ms ON ms.symbol = bars.symbol"));
        assert!(market_sql.contains("AVG(stock_return) AS baseline_return"));
        assert!(market_sql.contains("sr.stock_return - bl.baseline_return AS raw_value"));
        assert!(
            market_sql.contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)")
        );
        assert!(industry_sql.contains("COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry"));
        assert!(industry_sql.contains("GROUP BY trade_date, industry"));
        assert!(
            industry_sql.contains("bl.trade_date = sr.trade_date AND bl.industry = sr.industry")
        );
    }

    #[test]
    fn phase7_growth_recovery_sql_uses_ann_date_pit_and_same_quarter_yoy() {
        let specs = phase7_growth_recovery_backfill_specs();
        let eps = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_eps_yoy_recovery_std")
            .expect("eps yoy spec");
        let debt = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_debt_to_assets_yoy_improve_std")
            .expect("debt improvement spec");

        let eps_sql = phase7_factor_backfill_sql(eps);
        let debt_sql = phase7_factor_backfill_sql(debt);

        assert!(eps_sql.contains("market_financial_indicator"));
        assert!(eps_sql.contains("fi.ann_date <= td.trade_date"));
        assert!(eps_sql.contains("prev.end_date = (latest.end_date - INTERVAL '1 year')::date"));
        assert!(eps_sql.contains("available_at"));
        assert!(eps_sql
            .contains("(latest.raw_value - prev.raw_value) / NULLIF(ABS(prev.raw_value), 0.0)"));
        assert!(debt_sql.contains("prev.raw_value - latest.raw_value AS raw_value"));
    }

    #[test]
    fn phase7_valuation_sql_uses_daily_basic_and_value_direction() {
        let specs = phase7_valuation_backfill_specs();
        let pe = specs
            .iter()
            .find(|spec| spec.factor_code == "val_pe_ttm_low_std")
            .expect("pe spec");
        let dividend = specs
            .iter()
            .find(|spec| spec.factor_code == "val_dividend_yield_ttm_std")
            .expect("dividend spec");

        let pe_sql = phase7_factor_backfill_sql(pe);
        let dividend_sql = phase7_factor_backfill_sql(dividend);

        assert!(pe_sql.contains("market_stock_daily_basic"));
        assert!(pe_sql.contains("pe_ttm::double precision AS raw_value"));
        assert!(pe_sql.contains("raw_value > 0.0"));
        assert!(pe_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
        assert!(dividend_sql.contains("dv_ttm::double precision AS raw_value"));
        assert!(dividend_sql.contains("ORDER BY raw_value) AS normalized_value"));
    }

    #[test]
    fn phase7_moneyflow_sql_uses_moneyflow_pit_and_amount_normalization() {
        let specs = phase7_moneyflow_backfill_specs();
        let net = specs
            .iter()
            .find(|spec| spec.factor_code == "mf_net_amount_20d_std")
            .expect("net moneyflow spec");
        let pressure = specs
            .iter()
            .find(|spec| spec.factor_code == "mf_small_sell_pressure_20d_std")
            .expect("small sell pressure spec");

        let net_sql = phase7_factor_backfill_sql(net);
        let pressure_sql = phase7_factor_backfill_sql(pressure);

        assert!(net_sql.contains("market_stock_moneyflow mf"));
        assert!(net_sql.contains("JOIN market_stock_daily_bar bar"));
        assert!(net_sql.contains("mf.net_mf_amount::double precision"));
        assert!(net_sql.contains("SUM(flow_amount) OVER"));
        assert!(net_sql.contains("SUM(traded_amount) OVER"));
        assert!(net_sql.contains("ROWS BETWEEN 19 PRECEDING AND CURRENT ROW"));
        assert!(net_sql.contains("normalized_value, trade_date"));
        assert!(pressure_sql.contains("mf.sell_sm_amount::double precision"));
        assert!(pressure_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
    }

    #[test]
    fn phase7_event_alpha_sql_uses_available_at_pit_carry_forward() {
        let specs = phase7_event_alpha_backfill_specs();
        let plan = Phase7EventAlphaBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid event alpha plan");
        let forecast = specs
            .iter()
            .find(|spec| spec.factor_code == "event_forecast_change_mid_std")
            .expect("forecast event spec");
        let disclosure = specs
            .iter()
            .find(|spec| spec.factor_code == "event_disclosure_early_days_std")
            .expect("disclosure event spec");

        let forecast_sql = phase7_factor_backfill_sql(forecast);
        let disclosure_sql = phase7_factor_backfill_sql(disclosure);

        assert!(forecast_sql.contains("market_stock_forecast"));
        assert!(forecast_sql.contains("event.available_at <= td.trade_date"));
        assert!(forecast_sql.contains("ORDER BY event.available_at DESC"));
        assert!(forecast_sql
            .contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));
        assert!(forecast_sql.contains("WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0"));
        assert!(forecast_sql.contains("normalized_value, available_at"));
        assert!(disclosure_sql.contains("market_stock_disclosure_date"));
        assert!(disclosure_sql.contains("event.pre_date - event.actual_date"));
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);
    }

    #[test]
    fn phase7_event_window_sql_limits_event_life_and_decays_signal() {
        let spec = Phase7BackfillFactorSpec {
            factor_code: "event_window_forecast_change_20d_decay_std",
            name: "Phase 7 20d decayed forecast event window rank",
            period: 20,
            kind: Phase7BackfillFactorKind::EventWindow {
                source_table: "market_stock_forecast",
                value_expression: "(COALESCE(event.p_change_min, event.p_change_max)::double precision + COALESCE(event.p_change_max, event.p_change_min)::double precision) / 2.0",
                higher_is_better: true,
                window_days: 20,
                decay_days: 20,
            },
            weight: 1.0,
        };

        let sql = phase7_factor_backfill_sql(&spec);

        assert!(sql.contains("market_stock_forecast"));
        assert!(sql.contains("FROM market_stock_forecast event"));
        assert!(sql.contains("td.trade_date >= events.available_at"));
        assert!(sql.contains("td.trade_date <= events.available_at + INTERVAL '20 days'"));
        assert!(!sql.contains("JOIN LATERAL"));
        assert!(sql.contains(
            "GREATEST(0.0, 1.0 - ((td.trade_date - events.available_at)::double precision / 20.0))"
        ));
        assert!(sql.contains("event_raw_value * decay_weight AS raw_value"));
        assert!(sql.contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));
    }

    #[test]
    fn phase7_event_window_combo_allows_sparse_event_days() {
        let specs = phase7_event_window_alpha_backfill_specs();
        let plan = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid event-window alpha plan");

        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);
    }

    #[test]
    fn phase7_alpha_blend_sql_combines_existing_combo_scores_by_weight() {
        let sql = phase7_alpha_blend_backfill_sql("weighted_combo_blend");

        assert!(sql.contains("jsonb_to_recordset($3::jsonb)"));
        assert!(sql.contains("FROM multi_factor_value mfv"));
        assert!(sql.contains("SUM(mfv.normalized_score::double precision * sources.weight)"));
        assert!(sql.contains("HAVING COUNT(DISTINCT (sources.combo_name, sources.version)) = $6"));
    }

    #[test]
    fn phase7_alpha_blend_sql_supports_optional_sparse_event_overlay() {
        let sql = phase7_alpha_blend_backfill_sql("weighted_combo_optional_overlay");

        assert!(sql.contains("jsonb_array_elements($3::jsonb) WITH ORDINALITY"));
        assert!(sql.contains("required_sources AS"));
        assert!(sql.contains("optional_sources AS"));
        assert!(sql.contains("LEFT JOIN multi_factor_value optional_mfv"));
        assert!(sql.contains("COALESCE(SUM(optional_mfv.normalized_score::double precision * optional_sources.weight), 0.0)"));
        assert!(sql.contains(
            "HAVING COUNT(DISTINCT (required_sources.combo_name, required_sources.version)) = $6"
        ));
    }

    #[test]
    fn phase7_optional_overlay_requires_only_the_base_source() {
        let req = Phase7AlphaBlendProfilesBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2016-02-05".to_string()),
            version: None,
            profile_names: Some(vec!["quality_event_window_overlay_5pct".to_string()]),
            statement_timeout_ms: Some(0),
        };
        let plans = req.into_plans().expect("valid optional overlay profile");
        let plan = plans.first().expect("one selected profile");

        assert_eq!(plan.combo_name, "phase7_quality_event_window_overlay_v1");
        assert_eq!(plan.combo_method, "weighted_combo_optional_overlay");
        assert_eq!(plan.source_combos.len(), 2);
        assert_eq!(phase7_alpha_blend_required_source_count(plan), 1);
    }

    #[test]
    fn phase7_backfill_profile_metrics_reports_throughput() {
        let report = SetBasedFactorBackfillReport {
            factor_rows: 23_736,
            combo_rows: 4_746,
            factor_rows_by_code: vec![
                ("rev_5d_std".to_string(), 4_752),
                ("rev_20d_std".to_string(), 4_746),
            ],
        };

        let metrics = factor_backfill_profile_metrics(&report, 2_000);

        assert_eq!(metrics["factor_rows"], 23_736);
        assert_eq!(metrics["combo_rows"], 4_746);
        assert_eq!(metrics["total_rows"], 28_482);
        assert_eq!(metrics["elapsed_ms"], 2_000);
        assert_eq!(metrics["rows_per_sec"], 14_241.0);
        assert_eq!(metrics["factor_rows_by_code"]["rev_5d_std"], 4_752);
    }

    #[test]
    fn phase7_backfill_completion_status_keeps_cancelled_out_of_failed_path() {
        let report = SetBasedFactorBackfillReport {
            factor_rows: 0,
            combo_rows: 0,
            factor_rows_by_code: vec![],
        };
        assert_eq!(
            SetBasedFactorBackfillCompletion::Cancelled {
                report: report.clone(),
                elapsed_ms: 12_345,
            }
            .experiment_status(),
            "partial"
        );
        assert_eq!(
            SetBasedFactorBackfillCompletion::Cancelled {
                report: report.clone(),
                elapsed_ms: 12_345,
            }
            .task_status(),
            "cancelled"
        );
        assert_eq!(
            SetBasedFactorBackfillCompletion::Completed {
                report,
                elapsed_ms: 0,
            }
            .task_status(),
            "completed"
        );
    }

    #[test]
    fn parse_financial_factor_supports_quality_and_liquidity_sources() {
        let eps = parse_financial_factor("eps").expect("eps factor");
        assert_eq!(eps.code, "fin_eps");
        assert_eq!(eps.source_column, "eps");

        let current = parse_financial_factor("current_ratio").expect("current ratio factor");
        assert_eq!(current.code, "fin_current_ratio");
        assert_eq!(current.source_column, "current_ratio");

        let quick = parse_financial_factor("quick_ratio").expect("quick ratio factor");
        assert_eq!(quick.code, "fin_quick_ratio");
        assert_eq!(quick.source_column, "quick_ratio");
    }
}
