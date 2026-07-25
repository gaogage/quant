//! Factor API routes — compute, standardize, and evaluate factors
//!
//! 模块树拆分（DDD Step 3c）：
//! - `mod.rs`：共享类型 + 共享辅助函数 + `pub use` re-export
//! - `crud.rs`：因子 CRUD / sync / evaluate / combine / neutralize
//! - `icir_materialize.rs`：PIT 滚动 ICIR combo 物化
//! - `backfill.rs`：30+ 因子族 backfill 实现

use chrono::NaiveDate;
use quant_factor::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

// ─── Shared request/response types ─────────────────────────────
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

pub(crate) fn default_n_quantiles() -> usize {
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

pub(crate) fn default_version() -> String {
    "1.0.0".to_string()
}
pub(crate) fn default_chunk_size() -> usize {
    100
}

// ─── Shared helpers ─────────────────────────────────────────────
pub(crate) fn trim_or_default(value: Option<String>, default: &str, field: &str) -> Result<String, String> {
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

pub(crate) fn parse_phase7_backfill_date(
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

// ─── Factor definition shared types & helpers ─────────────────

#[derive(Debug)]
pub(crate) struct FactorDefinitionInput {
    pub(crate) factor_code: String,
    pub(crate) version: String,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) frequency: String,
    pub(crate) dependencies: serde_json::Value,
    pub(crate) parameters: serde_json::Value,
    pub(crate) status: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct FactorDefinitionRecord {
    pub(crate) factor_id: i64,
    pub(crate) factor_code: String,
    pub(crate) version: String,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) frequency: String,
    pub(crate) dependencies: serde_json::Value,
    pub(crate) parameters: serde_json::Value,
    pub(crate) status: String,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
}

pub(crate) type FactorDefinitionRow = (
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

pub(crate) fn trim_required(value: String, field: &str) -> Result<String, String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(format!("{} is required", field))
    } else {
        Ok(value)
    }
}

pub(crate) fn trim_optional(value: Option<String>, default_value: &str) -> String {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .unwrap_or_else(|| default_value.to_string())
}

pub(crate) fn normalize_filter(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

pub(crate) fn normalize_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(100).clamp(1, 500)
}

pub(crate) fn factor_definition_from_row(row: FactorDefinitionRow) -> FactorDefinitionRecord {
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

pub(crate) async fn upsert_factor_definition_input(
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

pub(crate) async fn upsert_factor_definition(
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

// ─── Background task shared helpers ────────────────────────────
pub(crate) fn background_factor_task_id() -> String {
    let ts = chrono::Utc::now().format("fs-%Y%m%d-%H%M%S%3f");
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{}-{}", ts, &suffix[..8])
}

pub(crate) fn usize_to_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

// ─── Submodules ─────────────────────────────────────────────────
mod crud;
mod icir_materialize;
mod backfill;

pub use crud::*;
pub use icir_materialize::*;
pub use backfill::*;

// ─── Tests ─────────────────────────────────────────────────────
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
    fn rolling_pit_evaluation_request_defaults_to_full_canonical_start() {
        let req = EvaluateRollingPitRequest {
            start_date: None,
            end_date: Some("2026-06-15".to_string()),
            version: "1.0.0".to_string(),
            horizon: 20,
            train_lookback_days: None,
            max_windows: None,
            factor_codes: None,
        };

        let plan = req.into_plan().expect("valid rolling PIT plan");

        assert_eq!(
            plan.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 1).unwrap()
        );
        assert_eq!(plan.end_date, NaiveDate::from_ymd_opt(2026, 6, 15).unwrap());
        assert_eq!(plan.horizon, 20);
        assert_eq!(plan.train_lookback_days, 756);
    }

    #[test]
    fn rolling_pit_evaluation_request_rejects_short_lookback() {
        let req = EvaluateRollingPitRequest {
            start_date: Some("20140101".to_string()),
            end_date: Some("20141231".to_string()),
            version: "1.0.0".to_string(),
            horizon: 20,
            train_lookback_days: Some(25),
            max_windows: None,
            factor_codes: None,
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("train_lookback_days"));
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
    fn phase7_financial_quality_change_backfill_request_builds_p37_pit_plan() {
        let req = Phase7FinancialQualityChangeBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-17".to_string()),
            version: Some("phase7fq-change-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req
            .into_plan()
            .expect("valid financial quality change plan");

        assert_eq!(plan.bundle_name, "phase7_financial_quality_change_v1");
        assert_eq!(plan.combo_name, "phase7_financial_quality_change_v1");
        assert_eq!(plan.task_type, "phase7_financial_quality_change_backfill");
        assert_eq!(plan.category, "fundamental_change");
        assert_eq!(plan.phase, "7-P3.7");
        assert_eq!(
            plan.dependencies,
            &["market_financial_indicator", "market_trade_calendar"]
        );
        assert_eq!(plan.combo_method, "equal_weight_fq_change");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_earnings_recovery_persistence_backfill_request_builds_p37_pit_plan() {
        let req = Phase7EarningsRecoveryPersistenceBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-17".to_string()),
            version: Some("phase7-earn-persist-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req
            .into_plan()
            .expect("valid earnings recovery persistence plan");

        assert_eq!(plan.bundle_name, "phase7_earnings_recovery_persistence_v1");
        assert_eq!(plan.combo_name, "phase7_earnings_recovery_persistence_v1");
        assert_eq!(
            plan.task_type,
            "phase7_earnings_recovery_persistence_backfill"
        );
        assert_eq!(plan.category, "earnings_recovery_persistence");
        assert_eq!(plan.phase, "7-P3.7");
        assert_eq!(
            plan.dependencies,
            &["market_financial_indicator", "market_trade_calendar"]
        );
        assert_eq!(plan.combo_method, "equal_weight_earn_persist");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
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
    fn phase7_cashflow_quality_backfill_request_builds_pit_plan() {
        let req = Phase7CashflowQualityBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-26".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid cashflow quality plan");

        assert_eq!(plan.bundle_name, "phase7_cashflow_quality_v1");
        assert_eq!(plan.combo_name, "phase7_cashflow_quality_v1");
        assert_eq!(plan.task_type, "phase7_cashflow_quality_backfill");
        assert_eq!(plan.category, "cashflow_quality");
        assert_eq!(
            plan.dependencies,
            &["market_stock_cashflow", "market_trade_calendar"]
        );
        assert_eq!(plan.phase, "7-FF/7-J");
        assert_eq!(plan.statement_timeout_ms, 180_000);
    }

    #[test]
    fn phase7_dividend_quality_backfill_request_builds_pit_plan() {
        let req = Phase7DividendQualityBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-26".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: Some(180_000),
        };

        let plan = req.into_plan().expect("valid dividend quality plan");

        assert_eq!(plan.bundle_name, "phase7_dividend_quality_v1");
        assert_eq!(plan.combo_name, "phase7_dividend_quality_v1");
        assert_eq!(plan.task_type, "phase7_dividend_quality_backfill");
        assert_eq!(plan.category, "dividend_quality");
        assert_eq!(
            plan.dependencies,
            &["market_stock_dividend", "market_trade_calendar"]
        );
        assert_eq!(plan.phase, "7-FF/7-J");
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
    fn phase7_alpha_blend_rejects_industry_prosperity_without_dedicated_market_scope_builder() {
        let req = Phase7AlphaBlendBackfillRequest {
            start_date: None,
            end_date: None,
            version: None,
            combo_name: Some("phase7_industry_prosperity_proxy_v1".to_string()),
            statement_timeout_ms: None,
            allow_signed_weights: false,
            sources: vec![
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_financial_quality_change_v1".to_string(),
                    version: None,
                    weight: 0.5,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_event_surprise_v1".to_string(),
                    version: None,
                    weight: 0.5,
                },
            ],
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("phase7_industry_membership_market_scope_gate_v1"));
        assert!(err.contains("dedicated market-scope factor builder"));
        assert!(err.contains("main_chinext_non_st"));
    }

    #[test]
    fn phase7_alpha_blend_rejects_industry_prosperity_source_combo() {
        let req = Phase7AlphaBlendBackfillRequest {
            start_date: None,
            end_date: None,
            version: None,
            combo_name: Some("phase7_generic_low_corr_test_v1".to_string()),
            statement_timeout_ms: None,
            allow_signed_weights: false,
            sources: vec![
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_industry_prosperity_proxy_v1".to_string(),
                    version: None,
                    weight: 0.5,
                },
                Phase7AlphaBlendSourceRequest {
                    combo_name: "phase7_event_surprise_v1".to_string(),
                    version: None,
                    weight: 0.5,
                },
            ],
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("phase7_industry_prosperity_proxy_v1"));
        assert!(err.contains("phase7_industry_membership_market_scope_gate_v1"));
        assert!(err.contains("generic alpha blend cannot enforce excluded markets"));
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

        assert_eq!(plans.len(), 34);
        assert!(combo_names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(combo_names.contains("phase7_blend_value_tilt_v1"));
        assert!(combo_names.contains("phase7_blend_quality_growth_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert!(combo_names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(combo_names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_cashflow_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_dividend_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_cashflow_dividend_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(combo_names.contains("phase7_fq_change_event_surprise_sleeve_05pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_event_surprise_sleeve_10pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_event_surprise_sleeve_15pct_v1"));
        assert!(combo_names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(combo_names.contains("phase7_quality_event_post_return_curve_overlay_v1"));
        assert!(combo_names.contains("phase7_fq_change_supply_float_sleeve_05pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_supply_float_sleeve_10pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_supply_float_sleeve_15pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_unlock_pressure_sleeve_05pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_unlock_pressure_sleeve_10pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_unlock_pressure_sleeve_15pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_forecast_revision_sleeve_05pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_forecast_revision_sleeve_10pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_forecast_revision_sleeve_15pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_shareholder_structure_sleeve_05pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_shareholder_structure_sleeve_10pct_v1"));
        assert!(combo_names.contains("phase7_fq_change_shareholder_structure_sleeve_15pct_v1"));
        assert!(combo_names.contains("phase7_quality_event_reaction_segments_overlay_v1"));
        assert!(combo_names.contains("phase7_quality_event_reaction_reversal_overlay_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        for plan in plans {
            assert_eq!(plan.task_type, "phase7_alpha_blend_profiles_backfill");
            if matches!(
                plan.combo_name.as_str(),
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
    fn phase7_alpha_blend_profiles_request_preserves_shareholder_source_version() {
        let req = Phase7AlphaBlendProfilesBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2016-02-05".to_string()),
            version: None,
            profile_names: Some(vec![
                "phase7_fq_change_shareholder_structure_sleeve_10pct_v1".to_string(),
            ]),
            statement_timeout_ms: Some(0),
        };

        let plans = req.into_plans().expect("valid selected profile plans");

        assert_eq!(plans.len(), 1);
        let plan = plans.first().expect("selected shareholder sleeve plan");
        assert_eq!(
            plan.combo_name,
            "phase7_fq_change_shareholder_structure_sleeve_10pct_v1"
        );
        assert_eq!(plan.combo_method, "weighted_combo_optional_overlay");
        assert!(plan.source_combos.iter().any(|source| {
            source.combo_name == "shareholder_structure"
                && source.version == "p321d-shareholder-low-fanout-v1"
                && (source.weight - 0.10).abs() < 1e-9
        }));
    }

    #[test]
    fn phase7_alpha_blend_profiles_backfill_counts_quarterly_segments() {
        let req = Phase7AlphaBlendProfilesBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: None,
            profile_names: Some(vec![
                "phase7_fq_change_unlock_pressure_sleeve_05pct_v1".to_string(),
                "phase7_fq_change_unlock_pressure_sleeve_10pct_v1".to_string(),
                "phase7_fq_change_unlock_pressure_sleeve_15pct_v1".to_string(),
            ]),
            statement_timeout_ms: Some(0),
        };

        let plans = req.into_plans().expect("valid selected profile plans");

        assert_eq!(plans.len(), 3);
        assert_eq!(alpha_blend_profile_backfill_total_steps(&plans), 114);
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
    fn phase7_financial_quality_change_specs_capture_acceleration_not_level() {
        let specs = phase7_financial_quality_change_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "fin_roe_yoy_accel_std",
                "fin_roa_yoy_accel_std",
                "fin_gross_margin_yoy_accel_std",
                "fin_netprofit_margin_yoy_accel_std",
                "fin_current_ratio_yoy_accel_std",
                "fin_debt_to_assets_yoy_decel_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs
            .iter()
            .all(|spec| (spec.weight - 1.0 / 6.0).abs() < 1e-12));
        assert!(specs.iter().all(|spec| matches!(
            spec.kind,
            Phase7BackfillFactorKind::FinancialAnnualAcceleration { .. }
        )));
    }

    #[test]
    fn phase7_earnings_recovery_persistence_specs_capture_multi_period_recovery() {
        let specs = phase7_earnings_recovery_persistence_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "fin_eps_yoy_recovery_persist_std",
                "fin_roe_yoy_recovery_persist_std",
                "fin_roa_yoy_recovery_persist_std",
                "fin_gross_margin_yoy_recovery_persist_std",
                "fin_netprofit_margin_yoy_recovery_persist_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.2).abs() < 1e-12));
        assert!(specs.iter().all(|spec| matches!(
            spec.kind,
            Phase7BackfillFactorKind::FinancialAnnualPersistence { .. }
        )));
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
    fn phase7_moneyflow_congestion_specs_interact_flow_with_capacity_crowding() {
        let specs = phase7_moneyflow_congestion_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "mf_net_inflow_low_crowding_20d_std",
                "mf_elg_inflow_low_crowding_10d_std",
                "mf_lg_elg_inflow_low_crowding_20d_std",
                "mf_small_order_relief_low_crowding_20d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.25).abs() < 1e-12));
        assert!(specs.iter().all(|spec| matches!(
            spec.kind,
            Phase7BackfillFactorKind::MoneyflowCongestionInteraction { .. }
        )));
    }

    #[test]
    fn phase7_moneyflow_congestion_backfill_request_builds_p38_pit_plan() {
        let req = Phase7MoneyflowCongestionBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-17".to_string()),
            version: Some("phase7-mf-congestion-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid moneyflow congestion plan");

        assert_eq!(
            plan.bundle_name,
            "phase7_moneyflow_congestion_interaction_v1"
        );
        assert_eq!(
            plan.combo_name,
            "phase7_moneyflow_congestion_interaction_v1"
        );
        assert_eq!(plan.task_type, "phase7_moneyflow_congestion_backfill");
        assert_eq!(plan.category, "moneyflow_congestion_alpha");
        assert_eq!(plan.phase, "7-P3.8");
        assert_eq!(
            plan.dependencies,
            &[
                "market_stock_moneyflow",
                "market_stock_daily_bar_adj",
                "market_stock_daily_basic",
            ]
        );
        assert_eq!(plan.combo_method, "equal_weight_mf_congest");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_supply_float_shock_specs_are_pre_registered_supply_proxy() {
        let specs = phase7_supply_float_shock_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "float_share_growth_20d_inverse_std",
                "total_share_growth_60d_inverse_std",
                "free_share_churn_120d_inverse_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs
            .iter()
            .all(|spec| matches!(spec.kind, Phase7BackfillFactorKind::SupplyFloatShock { .. })));
    }

    #[test]
    fn phase7_supply_float_shock_backfill_request_builds_p311_pit_plan() {
        let req = Phase7SupplyFloatShockBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-17".to_string()),
            version: Some("phase7-supply-float-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid supply float shock plan");

        assert_eq!(plan.bundle_name, "phase7_supply_float_shock_v1");
        assert_eq!(plan.combo_name, "phase7_supply_float_shock_v1");
        assert_eq!(plan.task_type, "phase7_supply_float_shock_backfill");
        assert_eq!(plan.category, "supply_float_shock_alpha");
        assert_eq!(plan.phase, "7-P3.11");
        assert_eq!(plan.dependencies, &["market_stock_daily_basic"]);
        assert_eq!(plan.combo_method, "equal_weight_supply_float");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_liquidity_quality_backfill_request_builds_p316_broad_base_plan() {
        let req = Phase7LiquidityQualityBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-17".to_string()),
            version: Some("phase7-liq-quality-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid liquidity quality plan");

        assert_eq!(plan.bundle_name, "phase7_liquidity_quality_v1");
        assert_eq!(plan.combo_name, "phase7_liquidity_quality_v1");
        assert_eq!(plan.task_type, "phase7_liquidity_quality_backfill");
        assert_eq!(plan.category, "liquidity_quality_alpha");
        assert_eq!(plan.phase, "7-P3.16");
        assert_eq!(
            plan.dependencies,
            &["market_stock_daily_bar_adj", "market_stock_daily_basic"]
        );
        assert_eq!(plan.combo_method, "weighted_liquidity_quality");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_liquidity_quality_specs_are_broad_base_not_moneyflow_or_static_industry() {
        let specs = phase7_liquidity_quality_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "liq_impact_improve_20v120_std",
                "liq_amount_trend_20v120_std",
                "liq_amount_stability_60d_std",
                "liq_turnover_stability_60d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.25).abs() < 1e-12));
        assert!(specs
            .iter()
            .all(|spec| matches!(spec.kind, Phase7BackfillFactorKind::LiquidityQuality { .. })));
    }

    #[test]
    fn phase7_liquidity_quality_sql_uses_only_pit_daily_market_data() {
        let specs = phase7_liquidity_quality_backfill_specs();
        let impact = specs
            .iter()
            .find(|spec| spec.factor_code == "liq_impact_improve_20v120_std")
            .expect("liquidity impact improvement spec");
        let turnover = specs
            .iter()
            .find(|spec| spec.factor_code == "liq_turnover_stability_60d_std")
            .expect("turnover stability spec");

        let impact_sql = phase7_factor_backfill_sql(impact);
        let turnover_sql = phase7_factor_backfill_sql(turnover);

        assert!(impact_sql.contains("market_stock_daily_bar_adj bar"));
        assert!(impact_sql.contains("JOIN market_stock_daily_basic basic"));
        assert!(impact_sql.contains("bar.trade_date <= $4"));
        assert!(impact_sql.contains("bar.trade_date >= ($3::date - INTERVAL '180 days')"));
        assert!(impact_sql.contains("LAG(close::double precision)"));
        assert!(impact_sql.contains("ROWS BETWEEN 19 PRECEDING AND CURRENT ROW"));
        assert!(impact_sql.contains("ROWS BETWEEN 119 PRECEDING AND CURRENT ROW"));
        assert!(impact_sql.contains("LN((long_illiq + 1e-12) / (short_illiq + 1e-12))"));
        assert!(turnover_sql.contains("circ_mv"));
        assert!(turnover_sql.contains("STDDEV_SAMP(turnover_proxy) OVER"));
        assert!(turnover_sql.contains("-ABS((turnover_proxy - avg_turnover_60)"));
        for sql in [impact_sql.as_str(), turnover_sql.as_str()] {
            assert!(!sql.contains("market_stock ms"));
            assert!(!sql.contains("industry"));
            assert!(!sql.contains("market_stock_moneyflow"));
            assert!(!sql.contains("market_stock_block_trade"));
            assert!(!sql.contains("market_stock_share_float"));
        }
    }

    #[test]
    fn phase7_market_residual_risk_backfill_request_builds_p317_broad_base_plan() {
        let req = Phase7MarketResidualRiskBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: Some("phase7-market-resid-risk-test".to_string()),
            combo_name: None,
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid market residual risk plan");

        assert_eq!(plan.bundle_name, "phase7_market_residual_risk_v1");
        assert_eq!(plan.combo_name, "phase7_market_residual_risk_v1");
        assert_eq!(plan.task_type, "phase7_market_residual_risk_backfill");
        assert_eq!(plan.category, "market_residual_risk_alpha");
        assert_eq!(plan.phase, "7-P3.17");
        assert_eq!(
            plan.dependencies,
            &["market_stock_daily_bar_adj", "market_index_daily_bar"]
        );
        assert_eq!(plan.combo_method, "weighted_market_residual");
        assert!(
            plan.combo_method.len() <= 32,
            "multi_factor_weight.method is varchar(32)"
        );
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_market_residual_risk_specs_are_low_correlation_broad_base() {
        let specs = phase7_market_residual_risk_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(
            codes,
            vec![
                "mkt_low_beta_120d_std",
                "mkt_downside_beta_120d_std",
                "resid_low_vol_120d_std",
                "resid_reversal_20d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs.iter().all(|spec| (spec.weight - 0.25).abs() < 1e-12));
        assert!(specs.iter().all(|spec| matches!(
            spec.kind,
            Phase7BackfillFactorKind::MarketResidualRisk { .. }
        )));
    }

    #[test]
    fn phase7_market_residual_risk_sql_is_pit_and_uses_market_index_only() {
        let specs = phase7_market_residual_risk_backfill_specs();
        let low_beta = specs
            .iter()
            .find(|spec| spec.factor_code == "mkt_low_beta_120d_std")
            .expect("low beta spec");
        let residual_reversal = specs
            .iter()
            .find(|spec| spec.factor_code == "resid_reversal_20d_std")
            .expect("residual reversal spec");

        let low_beta_sql = phase7_factor_backfill_sql(low_beta);
        let residual_reversal_sql = phase7_factor_backfill_sql(residual_reversal);

        assert!(low_beta_sql.contains("market_stock_daily_bar_adj bar"));
        assert!(low_beta_sql.contains("market_index_daily_bar"));
        assert!(low_beta_sql.contains("idx.symbol = '000300.SH'"));
        assert!(low_beta_sql.contains("bar.trade_date <= $4"));
        assert!(low_beta_sql.contains("idx.trade_date <= $4"));
        assert!(low_beta_sql.contains("ROWS BETWEEN 119 PRECEDING AND CURRENT ROW"));
        assert!(low_beta_sql.contains("REGR_SLOPE(stock_return, market_return)"));
        assert!(low_beta_sql.contains("trade_date AS available_at"));
        assert!(residual_reversal_sql.contains("AVG(residual_return) OVER"));
        assert!(residual_reversal_sql.contains("-residual_mean_20"));
        for sql in [low_beta_sql.as_str(), residual_reversal_sql.as_str()] {
            assert!(!sql.contains("market_stock ms"));
            assert!(!sql.contains("industry"));
            assert!(!sql.contains("market_stock_moneyflow"));
            assert!(!sql.contains("market_stock_block_trade"));
            assert!(!sql.contains("market_stock_share_float"));
            assert!(!sql.contains("model_prediction"));
        }
    }

    #[test]
    fn phase7_industry_prosperity_request_requires_market_scope_gate() {
        let req = Phase7IndustryProsperityBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: Some("phase7-industry-prosperity-test".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("phase7_industry_membership_market_scope_gate_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_industry_prosperity_request_builds_p318g_market_scope_plan() {
        let req = Phase7IndustryProsperityBackfillRequest {
            start_date: Some("2017-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: Some("phase7-industry-prosperity-test".to_string()),
            combo_name: None,
            alpha_admission_gate_id: Some(
                "phase7_industry_membership_market_scope_gate_v1".to_string(),
            ),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid industry prosperity plan");

        assert_eq!(plan.combo_name, "phase7_industry_prosperity_proxy_v1");
        assert_eq!(plan.bundle_name, "phase7_industry_prosperity_proxy_v1");
        assert_eq!(plan.task_type, "phase7_industry_prosperity_backfill");
        assert_eq!(plan.category, "industry_prosperity_alpha");
        assert_eq!(plan.phase, "7-P3.18G");
        assert_eq!(
            plan.dependencies,
            &[
                "market_stock_industry_membership_pit",
                "market_stock_daily_bar_adj",
                "market_stock_daily_basic",
                "market_stock",
                "market_trade_calendar"
            ]
        );
        assert_eq!(plan.combo_method, "weighted_ind_prosperity");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 240_000);
    }

    #[test]
    fn phase7_industry_prosperity_sql_uses_pit_membership_and_main_chinext_scope() {
        let specs = phase7_industry_prosperity_backfill_specs();
        let sql = phase7_factor_backfill_sql(&specs[0]);

        assert!(sql.contains("market_stock_industry_membership_pit membership"));
        assert!(sql.contains("WHEN universe.trade_date < DATE '2021-12-13'"));
        assert!(sql.contains("THEN 'SW2014'"));
        assert!(sql.contains("ELSE 'SW2021'"));
        assert!(sql.contains("membership.available_at <= universe.trade_date"));
        assert!(sql.contains("membership.exit_available_at > universe.trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));
        assert!(!sql.contains("market_stock.industry"));
        assert!(!sql.contains("stock_basic.industry"));
        assert!(!sql.contains("concept_detail"));
        assert!(!sql.contains("model_prediction"));
    }

    #[test]
    fn phase7_industry_prosperity_multi_sql_writes_all_atoms_with_single_pit_universe() {
        let sql = phase7_industry_prosperity_multi_backfill_sql();

        assert!(sql.contains("market_stock_industry_membership_pit membership"));
        assert_eq!(sql.matches("WITH eligible_universe AS").count(), 1);
        assert!(sql.contains("WHEN universe.trade_date < DATE '2021-12-13'"));
        assert!(sql.contains("THEN 'SW2014'"));
        assert!(sql.contains("ELSE 'SW2021'"));
        assert!(sql.contains("membership.available_at <= universe.trade_date"));
        assert!(sql.contains("membership.exit_available_at > universe.trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("PARTITION BY factor_code, trade_date ORDER BY raw_value"));
        assert!(sql.contains("RETURNING factor_code"));
        assert!(!sql.contains("market_stock.industry"));
        assert!(!sql.contains("stock_basic.industry"));
        assert!(!sql.contains("concept_detail"));
        assert!(!sql.contains("model_prediction"));
    }

    #[test]
    fn phase7_futures_price_chain_request_requires_coverage_gate() {
        let req = Phase7FuturesPriceChainBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: Some("p319q-sw2021-l1-price-chain-v1".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("futures_price_chain_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_futures_price_chain_request_builds_p319q_plan() {
        let req = Phase7FuturesPriceChainBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-18".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some("futures_price_chain_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid futures price-chain plan");

        assert_eq!(plan.combo_name, "futures_price_chain");
        assert_eq!(plan.version, "p319q-sw2021-l1-price-chain-v1");
        assert_eq!(plan.bundle_name, "futures_price_chain");
        assert_eq!(plan.task_type, "phase7_futures_price_chain_backfill");
        assert_eq!(plan.category, "futures_price_chain_alpha");
        assert_eq!(plan.phase, "7-P3.19Q");
        assert_eq!(plan.combo_method, "weighted_futures_price_chain");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 240_000);
        assert!(plan
            .dependencies
            .contains(&"market_futures_product_exposure_mapping_pit"));
        assert!(plan
            .dependencies
            .contains(&"market_stock_industry_membership_pit"));
        assert!(plan.dependencies.contains(&"market_stock_name_history"));
    }

    #[test]
    fn phase7_futures_price_chain_sql_is_pit_and_market_scope_gated() {
        let product_sql = phase7_futures_price_chain_product_signal_backfill_sql();
        let sql = phase7_futures_price_chain_combo_backfill_sql();

        assert!(product_sql.contains("market_futures_daily"));
        assert!(product_sql.contains("market_futures_warehouse_receipt"));
        assert!(product_sql.contains("market_futures_holding_rank"));
        assert!(product_sql.contains("INSERT INTO market_futures_product_signal_pit"));
        assert!(product_sql.contains("available_at >= trade_date"));
        assert!(product_sql
            .contains("ON CONFLICT (signal_code, source_version, product_symbol, trade_date)"));
        assert!(sql.contains("FROM market_futures_product_signal_pit"));
        assert!(sql.contains("market_futures_product_exposure_mapping_pit mapping"));
        assert!(sql.contains("eligible_universe AS MATERIALIZED"));
        assert!(sql.contains("stock_membership AS MATERIALIZED"));
        assert!(sql.contains("industry_signal AS MATERIALIZED"));
        assert!(sql.contains("JOIN market_stock_daily_bar bar"));
        assert!(sql.contains("bar.trade_date BETWEEN $4 AND $5"));
        assert!(sql.contains("basic.trade_date BETWEEN $4 AND $5"));
        assert!(!sql.contains("JOIN market_stock_daily_bar_adj bar"));
        assert!(sql.contains("ms.list_date <= bar.trade_date"));
        assert!(sql.contains("ms.delist_date >= bar.trade_date"));
        assert!(sql.contains("FROM market_stock_name_history st_name"));
        assert!(sql.contains("st_name.start_date <= bar.trade_date"));
        assert!(!sql.contains("ms.list_status = 'L'"));
        assert!(!sql.contains("ms.is_st"));
        assert!(sql.contains("mapping.available_at <= td.trade_date"));
        assert!(sql.contains("td.trade_date >= signal_window.available_at"));
        assert!(sql.contains("available_at <= trade_date"));
        assert!(sql.contains("market_stock_industry_membership_pit membership"));
        assert!(sql.contains("WHEN universe.trade_date < DATE '2021-12-13'"));
        assert!(sql.contains("THEN 'SW2014'"));
        assert!(sql.contains("ELSE 'SW2021'"));
        assert!(sql.contains("membership.available_at <= universe.trade_date"));
        assert!(sql.contains("membership.exit_available_at > universe.trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("PARTITION BY factor_code, trade_date ORDER BY raw_value"));
        assert!(sql.contains("jsonb_each_text($3::jsonb)"));
        assert!(sql.contains("INSERT INTO multi_factor_value"));
        assert!(sql.contains("DELETE FROM multi_factor_value"));
        assert!(sql.contains("trade_date BETWEEN $4 AND $5"));
        assert!(sql.contains("HAVING COUNT(DISTINCT ranked.factor_code) >= $6"));
        assert!(!sql.contains("INSERT INTO factor_value"));
        assert!(!sql.contains("model_prediction"));
        assert!(!sql.contains("future_return"));
    }

    #[test]
    fn phase7_futures_price_chain_combo_allows_sparse_supply_atoms() {
        let specs = phase7_futures_price_chain_backfill_specs();
        let plan = SetBasedFactorBackfillPlan {
            start_date: NaiveDate::from_ymd_opt(2014, 1, 3).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2014, 3, 31).unwrap(),
            version: "p319q-sw2021-l1-price-chain-v1".to_string(),
            combo_name: "futures_price_chain".to_string(),
            statement_timeout_ms: 0,
            task_type: "phase7_futures_price_chain_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "futures_price_chain",
            category: "futures_price_chain_alpha",
            phase: "7-P3.19Q",
            dependencies: &["market_futures_daily"],
            combo_method: "weighted_futures_price_chain",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        };

        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);
    }

    #[test]
    fn phase7_equity_pledge_request_requires_coverage_gate() {
        let req = Phase7EquityPledgePressureBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: Some("p320f-equity-pledge-pressure-v1".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("equity_pledge_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_equity_pledge_request_builds_p320f_plan() {
        let req = Phase7EquityPledgePressureBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some("equity_pledge_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid equity pledge pressure plan");

        assert_eq!(plan.combo_name, "equity_pledge_pressure");
        assert_eq!(plan.version, "p320f-equity-pledge-pressure-v1");
        assert_eq!(plan.bundle_name, "equity_pledge_pressure");
        assert_eq!(plan.task_type, "phase7_equity_pledge_pressure_backfill");
        assert_eq!(plan.category, "equity_pledge_pressure_alpha");
        assert_eq!(plan.phase, "7-P3.20F");
        assert_eq!(plan.combo_method, "weighted_equity_pledge_pressure");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 240_000);
        assert!(plan.dependencies.contains(&"market_stock_pledge_stat"));
        assert!(plan.dependencies.contains(&"market_stock_pledge_detail"));
        assert!(plan.dependencies.contains(&"market_stock_name_history"));
    }

    #[test]
    fn phase7_equity_pledge_sql_is_pit_and_market_scope_gated() {
        let sql = phase7_equity_pledge_pressure_backfill_sql();

        assert!(sql.contains("market_stock_pledge_stat"));
        assert!(sql.contains("JOIN LATERAL"));
        assert!(sql.contains("stat.available_at <= universe.trade_date"));
        assert!(sql.contains("stat.end_date <= universe.trade_date"));
        assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("FROM market_stock_name_history st_name"));
        assert!(sql.contains("st_name.start_date <= bar.trade_date"));
        assert!(sql.contains("COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date"));
        assert!(sql.contains("pledge_ratio BETWEEN 0.0 AND 100.0"));
        assert!(sql.contains("ORDER BY raw_value"));
        assert!(sql.contains("INSERT INTO multi_factor_value"));
        assert!(sql.contains("DELETE FROM multi_factor_value"));
        assert!(sql.contains("available_at <= trade_date"));
        assert!(!sql.contains("model_prediction"));
        assert!(!sql.contains("future_return"));
    }

    #[test]
    fn phase7_shareholder_structure_request_requires_strict_low_fanout_gate() {
        let req = Phase7ShareholderStructureBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: Some("p321d-shareholder-low-fanout-v1".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("shareholder_structure_low_fanout_strict_pit_gate_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_shareholder_structure_request_builds_p321d_plan() {
        let req = Phase7ShareholderStructureBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some(
                "shareholder_structure_low_fanout_strict_pit_gate_v1".to_string(),
            ),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid shareholder structure plan");

        assert_eq!(plan.combo_name, "shareholder_structure");
        assert_eq!(plan.version, "p321d-shareholder-low-fanout-v1");
        assert_eq!(plan.bundle_name, "shareholder_structure");
        assert_eq!(plan.task_type, "phase7_shareholder_structure_backfill");
        assert_eq!(plan.category, "shareholder_structure_alpha");
        assert_eq!(plan.phase, "7-P3.21D");
        assert_eq!(plan.combo_method, "weighted_shareholder_structure");
        assert_eq!(plan.statement_timeout_ms, 240_000);
        assert!(plan.dependencies.contains(&"market_stock_holder_number"));
        assert!(plan.dependencies.contains(&"market_stock_holder_trade"));
        assert!(plan.dependencies.contains(&"market_stock_name_history"));
    }

    #[test]
    fn phase7_shareholder_structure_sql_is_strict_pit_low_fanout_only() {
        let sql = phase7_shareholder_structure_backfill_sql();

        assert!(sql.contains("market_stock_holder_number"));
        assert!(sql.contains("market_stock_holder_trade"));
        assert!(sql.contains("holder_number_candidates AS MATERIALIZED"));
        assert!(sql.contains("SELECT DISTINCT ON (hn.symbol)"));
        assert!(sql.contains("holder_number_segment_events AS MATERIALIZED"));
        assert!(sql.contains("hn.available_at >= hn.ann_date"));
        assert!(sql.contains("hn.available_at >= hn.end_date"));
        assert!(sql.contains("hn.holder_num > 0"));
        assert!(!sql.contains("LAG(hn.holder_num"));
        assert!(sql.contains("trade.available_at <= universe.trade_date"));
        assert!(sql.contains("trade.available_at >= trade.ann_date"));
        assert!(sql.contains("trade.after_ratio >= 0"));
        assert!(sql.contains("trade.close_date >= trade.begin_date"));
        assert!(sql.contains("available_at <= trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("percent_rank() OVER"));
        assert!(sql.contains("jsonb_each_text($3::jsonb)"));
        assert!(!sql.contains("market_stock_top10_holders"));
        assert!(!sql.contains("market_stock_top10_float_holders"));
        assert!(!sql.contains("future_return"));
        assert!(!sql.contains("model_prediction"));
    }

    #[test]
    fn phase7_margin_detail_request_requires_coverage_gate() {
        let req = Phase7MarginDetailBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: Some("p322e-margin-leverage-v1".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("margin_detail_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_margin_detail_request_builds_p322e_plan() {
        let req = Phase7MarginDetailBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-23".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some("margin_detail_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid margin detail plan");

        assert_eq!(plan.combo_name, "margin_detail_leverage_crowding");
        assert_eq!(plan.version, "p322e-margin-leverage-v1");
        assert_eq!(plan.bundle_name, "margin_detail");
        assert_eq!(plan.task_type, "phase7_margin_detail_backfill");
        assert_eq!(plan.category, "margin_detail_leverage_crowding_alpha");
        assert_eq!(plan.phase, "7-P3.22E");
        assert_eq!(plan.combo_method, "weighted_margin_detail");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 240_000);
        assert!(plan.dependencies.contains(&"market_stock_margin_detail"));
        assert!(plan.dependencies.contains(&"market_stock_daily_bar"));
        assert!(plan.dependencies.contains(&"market_stock_daily_basic"));
        assert!(plan.dependencies.contains(&"market_stock_name_history"));
    }

    #[test]
    fn phase7_margin_detail_sql_is_pit_and_market_scope_gated() {
        let sql = phase7_margin_detail_backfill_sql();

        assert!(sql.contains("market_stock_margin_detail"));
        assert!(sql.contains("rolling.available_at <= universe.trade_date"));
        assert!(sql.contains("md.available_at > md.trade_date"));
        assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("FROM market_stock_name_history st_name"));
        assert!(sql.contains("st_name.start_date <= bar.trade_date"));
        assert!(sql.contains("SUM(md.rzmre::double precision)"));
        assert!(sql.contains("LAG(md.rzye::double precision, 20)"));
        assert!(sql.contains("-SUM(COALESCE(md.rqmcl::double precision, 0.0))"));
        assert!(sql.contains("ORDER BY financing_buy_intensity_20d"));
        assert!(sql.contains("ORDER BY financing_balance_chg_20d"));
        assert!(sql.contains("ORDER BY short_sell_pressure_relief_20d"));
        assert!(sql.contains("INSERT INTO multi_factor_value"));
        assert!(sql.contains("DELETE FROM multi_factor_value"));
        assert!(sql.contains("available_at <= trade_date"));
        assert!(!sql.contains("INSERT INTO factor_value"));
        assert!(!sql.contains("model_prediction"));
        assert!(!sql.contains("future_return"));
    }

    #[test]
    fn phase7_margin_detail_sql_uses_wide_signal_scoring_shape() {
        let sql = phase7_margin_detail_backfill_sql();

        assert!(sql.contains("latest_signals AS"));
        assert!(sql.contains("ranked_signals AS"));
        assert!(sql.contains("weight_params AS"));
        assert!(sql.contains("rolling.available_at = universe.trade_date"));
        assert!(sql.contains("financing_buy_intensity_rank"));
        assert!(sql.contains("financing_balance_chg_rank"));
        assert!(sql.contains("short_sell_pressure_relief_rank"));
        assert!(sql.contains("GREATEST("));
        assert!(!sql.contains("universe.trade_date - INTERVAL '10 days'"));
        assert!(!sql.contains("CROSS JOIN LATERAL"));
        assert!(!sql.contains("latest_raw AS"));
        assert!(!sql.contains("COUNT(DISTINCT ranked.factor_code)"));
    }

    #[test]
    fn phase7_analyst_revision_request_requires_coverage_gate() {
        let req = Phase7AnalystRevisionBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-24".to_string()),
            version: Some("p323f-akshare-cninfo-revision-v1".to_string()),
            combo_name: None,
            alpha_admission_gate_id: None,
            universe_profile: Some("listed_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let err = req.into_plan().unwrap_err();

        assert!(err.contains("analyst_revision_coverage_ready_v1"));
        assert!(err.contains("main_chinext_non_st"));
        assert!(err.contains("factor builder"));
    }

    #[test]
    fn phase7_analyst_revision_request_builds_p323f_plan() {
        let req = Phase7AnalystRevisionBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-24".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some("analyst_revision_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: Some(240_000),
        };

        let plan = req.into_plan().expect("valid analyst revision plan");

        assert_eq!(plan.combo_name, "multi_vendor_analyst_revision");
        assert_eq!(plan.version, "p323f-akshare-cninfo-revision-v1");
        assert_eq!(plan.bundle_name, "multi_vendor_analyst_revision");
        assert_eq!(plan.task_type, "phase7_analyst_revision_backfill");
        assert_eq!(plan.category, "analyst_revision_alpha");
        assert_eq!(plan.phase, "7-P3.23F");
        assert_eq!(plan.combo_method, "weighted_analyst_revision");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(plan.statement_timeout_ms, 240_000);
        assert!(plan
            .dependencies
            .contains(&"market_vendor_analyst_revision_raw"));
        assert!(plan.dependencies.contains(&"market_stock_daily_bar"));
        assert!(plan.dependencies.contains(&"market_stock_daily_basic"));
        assert!(plan.dependencies.contains(&"market_stock_name_history"));
    }

    #[test]
    fn phase7_analyst_revision_specs_are_low_dimensional_and_sparse_combo() {
        let specs = phase7_analyst_revision_backfill_specs();
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();
        let plan = Phase7AnalystRevisionBackfillRequest {
            start_date: Some("2014-01-03".to_string()),
            end_date: Some("2026-06-24".to_string()),
            version: None,
            combo_name: None,
            alpha_admission_gate_id: Some("analyst_revision_coverage_ready_v1".to_string()),
            universe_profile: Some("main_chinext_non_st".to_string()),
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid analyst revision plan");

        assert_eq!(
            codes,
            vec![
                "ar_rating_change_net_20d_std",
                "ar_upgrade_event_20d_std",
                "ar_downgrade_pressure_20d_std",
                "ar_bullish_first_rating_60d_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        assert!(specs
            .iter()
            .all(|spec| matches!(spec.kind, Phase7BackfillFactorKind::AnalystRevision { .. })));
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);
    }

    #[test]
    fn phase7_analyst_revision_sql_is_pit_vendor_aware_and_market_scope_gated() {
        let specs = phase7_analyst_revision_backfill_specs();
        let net = specs
            .iter()
            .find(|spec| spec.factor_code == "ar_rating_change_net_20d_std")
            .expect("net revision spec");
        let first_rating = specs
            .iter()
            .find(|spec| spec.factor_code == "ar_bullish_first_rating_60d_std")
            .expect("bullish first-rating spec");

        let net_sql = phase7_factor_backfill_sql(net);
        let first_sql = phase7_factor_backfill_sql(first_rating);

        assert!(net_sql.contains("market_vendor_analyst_revision_raw raw"));
        assert!(net_sql.contains("raw.vendor = 'akshare'"));
        assert!(net_sql.contains("raw.vendor_endpoint = 'stock_rank_forecast_cninfo'"));
        assert!(net_sql.contains("raw.available_at <= $4"));
        assert!(net_sql.contains("raw.source_published_at IS NOT NULL"));
        assert!(net_sql.contains("raw.rating_previous IS NOT NULL"));
        assert!(net_sql.contains("raw.rating_change IS NOT NULL"));
        assert!(net_sql.contains("LEFT(ms.symbol, 6) = raw.symbol"));
        assert!(net_sql.contains("events.available_at <= universe.trade_date"));
        assert!(net_sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(net_sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(net_sql.contains("FROM market_stock_name_history st_name"));
        assert!(net_sql.contains("WHEN raw.rating_change = '调高' THEN 1.0"));
        assert!(net_sql.contains("WHEN raw.rating_change = '调低' THEN -1.0"));
        assert!(first_sql.contains("bullish_first_rating_score"));
        assert!(first_sql.contains("raw.is_first_rating = '是首次评级'"));
        assert!(!net_sql.contains("future_return"));
        assert!(!net_sql.contains("model_prediction"));
    }

    #[test]
    fn phase7_analyst_revision_multi_sql_writes_all_atoms_with_single_pit_universe() {
        let sql = phase7_analyst_revision_multi_backfill_sql();

        assert_eq!(sql.matches("eligible_universe AS MATERIALIZED").count(), 1);
        assert!(sql.contains("market_vendor_analyst_revision_raw raw"));
        assert!(sql.contains("raw.vendor = 'akshare'"));
        assert!(sql.contains("raw.vendor_endpoint = 'stock_rank_forecast_cninfo'"));
        assert!(sql.contains("raw.available_at <= $7"));
        assert!(sql.contains("raw.source_published_at IS NOT NULL"));
        assert!(sql.contains("raw.rating_previous IS NOT NULL"));
        assert!(sql.contains("raw.rating_change IS NOT NULL"));
        assert!(sql.contains("LEFT(ms.symbol, 6) = raw.symbol"));
        assert!(sql.contains("events.available_at <= universe.trade_date"));
        assert!(sql.contains("ms.market IN ('主板', '创业板')"));
        assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
        assert!(sql.contains("FROM market_stock_name_history st_name"));
        assert!(sql.contains("PARTITION BY factor_code, trade_date ORDER BY raw_value"));
        assert!(sql.contains("SELECT $1::varchar AS factor_code"));
        assert!(sql.contains("SELECT $2::varchar AS factor_code"));
        assert!(sql.contains("SELECT $3::varchar AS factor_code"));
        assert!(sql.contains("SELECT $4::varchar AS factor_code"));
        assert!(sql.contains("RETURNING factor_code"));
        assert!(!sql.contains("future_return"));
        assert!(!sql.contains("model_prediction"));
    }

    #[test]
    fn phase7_supply_float_shock_sql_uses_actual_daily_basic_share_fields() {
        let specs = phase7_supply_float_shock_backfill_specs();
        let float_growth = specs
            .iter()
            .find(|spec| spec.factor_code == "float_share_growth_20d_inverse_std")
            .expect("float growth supply spec");

        let total_growth = specs
            .iter()
            .find(|spec| spec.factor_code == "total_share_growth_60d_inverse_std")
            .expect("total growth supply spec");
        let free_churn = specs
            .iter()
            .find(|spec| spec.factor_code == "free_share_churn_120d_inverse_std")
            .expect("free-share churn supply spec");

        let sql = phase7_factor_backfill_sql(float_growth);
        let total_sql = phase7_factor_backfill_sql(total_growth);
        let free_sql = phase7_factor_backfill_sql(free_churn);

        assert!(sql.contains("market_stock_daily_basic"));
        assert!(sql.contains("float_share"));
        assert!(total_sql.contains("total_share"));
        assert!(free_sql.contains("free_share"));
        assert!(sql.contains("available_at"));
        assert!(!sql.contains("market_stock_daily_bar"));
        assert!(!total_sql.contains("market_stock_daily_bar"));
        assert!(!free_sql.contains("market_stock_daily_bar"));
        assert!(!sql.contains("circ_mv"));
        assert!(!sql.contains("total_mv"));
        assert!(!sql.contains("close"));
        assert!(!sql.contains("market_stock ms"));
        assert!(!sql.contains("industry"));
    }

    #[test]
    fn phase7_moneyflow_congestion_sql_penalizes_crowded_flow_without_future_data() {
        let specs = phase7_moneyflow_congestion_backfill_specs();
        let net = specs
            .iter()
            .find(|spec| spec.factor_code == "mf_net_inflow_low_crowding_20d_std")
            .expect("net low-crowding spec");
        let small = specs
            .iter()
            .find(|spec| spec.factor_code == "mf_small_order_relief_low_crowding_20d_std")
            .expect("small-order relief spec");

        let net_sql = phase7_factor_backfill_sql(net);
        let small_sql = phase7_factor_backfill_sql(small);

        assert!(net_sql.contains("market_stock_moneyflow mf"));
        assert!(net_sql.contains("JOIN market_stock_daily_bar_adj bar"));
        assert!(net_sql.contains("JOIN market_stock_daily_basic basic"));
        assert!(net_sql.contains("mf.trade_date <= $4"));
        assert!(net_sql.contains("mf.trade_date >= ($3::date - INTERVAL '180 days')"));
        assert!(net_sql.contains("ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING"));
        assert!(net_sql.contains("SUM(flow_amount) OVER"));
        assert!(net_sql.contains("SUM(traded_amount) OVER"));
        assert!(net_sql.contains("AVG(amount_crowding) OVER"));
        assert!(net_sql.contains("SUM(traded_amount) OVER"));
        assert!(net_sql.contains("SUM(float_market_value) OVER"));
        assert!(net_sql.contains("flow_intensity"));
        assert!(net_sql.contains("crowding_penalty"));
        assert!(net_sql.contains("flow_intensity / (1.0 + crowding_penalty) AS raw_value"));
        assert!(net_sql.contains("normalized_value, trade_date"));
        assert!(small_sql.contains("mf.buy_sm_amount::double precision"));
        assert!(small_sql.contains("mf.sell_sm_amount::double precision"));
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
        let specs = phase7_event_window_alpha_backfill_specs_for_days(20);
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
    fn phase7_event_window_decay_variants_use_distinct_combo_metadata_and_window_lengths() {
        let short_req = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: Some("phase7_event_window_earnings_10d_v1".to_string()),
            statement_timeout_ms: None,
        };
        let long_req = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: Some("phase7_event_window_earnings_40d_v1".to_string()),
            statement_timeout_ms: None,
        };

        let short_plan = short_req
            .into_plan()
            .expect("valid short event-window plan");
        let long_plan = long_req.into_plan().expect("valid long event-window plan");
        let short_specs = phase7_event_window_alpha_backfill_specs_for_plan(&short_plan);
        let long_specs = phase7_event_window_alpha_backfill_specs_for_plan(&long_plan);

        assert_eq!(short_plan.combo_name, "phase7_event_window_earnings_10d_v1");
        assert_eq!(
            short_plan.bundle_name,
            "phase7_event_window_earnings_10d_v1"
        );
        assert_eq!(long_plan.combo_name, "phase7_event_window_earnings_40d_v1");
        assert_eq!(long_plan.bundle_name, "phase7_event_window_earnings_40d_v1");
        assert!(short_specs
            .iter()
            .all(|spec| spec.factor_code.contains("_10d_decay_std")));
        assert!(long_specs
            .iter()
            .all(|spec| spec.factor_code.contains("_40d_decay_std")));

        for spec in short_specs {
            match spec.kind {
                Phase7BackfillFactorKind::EventWindow {
                    window_days,
                    decay_days,
                    ..
                } => {
                    assert_eq!(window_days, 10);
                    assert_eq!(decay_days, 10);
                }
                _ => panic!("expected event-window factor kind"),
            }
        }
        for spec in long_specs {
            match spec.kind {
                Phase7BackfillFactorKind::EventWindow {
                    window_days,
                    decay_days,
                    ..
                } => {
                    assert_eq!(window_days, 40);
                    assert_eq!(decay_days, 40);
                }
                _ => panic!("expected event-window factor kind"),
            }
        }
    }

    #[test]
    fn phase7_event_post_return_curve_plan_uses_pit_return_curve_metadata() {
        let req = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: Some("phase7_event_post_return_curve_20d_v1".to_string()),
            statement_timeout_ms: Some(300_000),
        };

        let plan = req.into_plan().expect("valid event post-return curve plan");
        let specs = phase7_event_window_alpha_backfill_specs_for_plan(&plan);
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();
        let total_weight = specs.iter().map(|spec| spec.weight).sum::<f64>();

        assert_eq!(plan.combo_name, "phase7_event_post_return_curve_20d_v1");
        assert_eq!(plan.bundle_name, "phase7_event_post_return_curve_20d_v1");
        assert_eq!(plan.combo_method, "weighted_event_post_return_curve");
        assert_eq!(plan.phase, "7-FB");
        assert_eq!(
            codes,
            vec![
                "event_post_return_forecast_20d_indrel_std",
                "event_post_return_express_20d_indrel_std",
            ]
        );
        assert!((total_weight - 1.0).abs() < 1e-12);
        for spec in specs {
            match spec.kind {
                Phase7BackfillFactorKind::EventPostReturnCurve {
                    window_days,
                    industry_relative,
                    ..
                } => {
                    assert_eq!(window_days, 20);
                    assert!(industry_relative);
                }
                _ => panic!("expected event post-return curve factor kind"),
            }
        }
    }

    #[test]
    fn phase7_event_post_return_curve_sql_uses_only_visible_event_and_price_history() {
        let specs = phase7_event_post_return_curve_backfill_specs_for_days(20);
        let forecast = specs
            .iter()
            .find(|spec| spec.factor_code == "event_post_return_forecast_20d_indrel_std")
            .expect("forecast post-return spec");
        let sql = phase7_factor_backfill_sql(forecast);

        assert!(sql.contains("event.available_at <= $4"));
        assert!(sql.contains("td.trade_date >= events.available_at"));
        assert!(sql.contains("anchor_bar.trade_date <= events.available_at"));
        assert!(sql.contains("current_bar.trade_date = td.trade_date"));
        assert!(sql.contains("PARTITION BY trade_date, industry"));
        assert!(
            !sql.contains("current_bar.trade_date > td.trade_date"),
            "post-return feature must not read future price data"
        );
    }

    #[test]
    fn phase7_event_reaction_segment_specs_split_early_and_late_windows() {
        let req = Phase7EventWindowAlphaBackfillRequest {
            start_date: Some("2016-02-01".to_string()),
            end_date: Some("2026-05-19".to_string()),
            version: None,
            combo_name: Some("phase7_event_reaction_segments_20d_v1".to_string()),
            statement_timeout_ms: Some(300_000),
        };

        let plan = req.into_plan().expect("valid event reaction segment plan");
        let specs = phase7_event_window_alpha_backfill_specs_for_plan(&plan);
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();

        assert_eq!(plan.combo_name, "phase7_event_reaction_segments_20d_v1");
        assert_eq!(plan.bundle_name, "phase7_event_reaction_segments_20d_v1");
        assert_eq!(plan.combo_method, "weighted_event_post_return_curve");
        assert_eq!(plan.phase, "7-FC");
        assert_eq!(
            codes,
            vec![
                "event_reaction_forecast_1_5d_indrel_std",
                "event_reaction_forecast_6_20d_indrel_std",
                "event_reaction_express_1_5d_indrel_std",
                "event_reaction_express_6_20d_indrel_std",
            ]
        );

        let windows = specs
            .iter()
            .map(|spec| match spec.kind {
                Phase7BackfillFactorKind::EventPostReturnCurve {
                    min_event_age_days,
                    max_event_age_days,
                    higher_is_better,
                    ..
                } => (min_event_age_days, max_event_age_days, higher_is_better),
                _ => panic!("expected segmented event post-return kind"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            windows,
            vec![(1, 5, true), (6, 20, true), (1, 5, true), (6, 20, true)]
        );
    }

    #[test]
    fn phase7_event_reaction_reversal_specs_rank_negative_reactions_higher() {
        let specs = phase7_event_reaction_reversal_backfill_specs_for_days(20);
        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();

        assert_eq!(
            codes,
            vec![
                "event_reaction_reversal_forecast_1_5d_indrel_std",
                "event_reaction_reversal_forecast_6_20d_indrel_std",
                "event_reaction_reversal_express_1_5d_indrel_std",
                "event_reaction_reversal_express_6_20d_indrel_std",
            ]
        );
        assert!(specs.iter().all(|spec| match spec.kind {
            Phase7BackfillFactorKind::EventPostReturnCurve {
                higher_is_better, ..
            } => !higher_is_better,
            _ => false,
        }));
    }

    #[test]
    fn phase7_event_reaction_segment_sql_enforces_segment_age_bounds() {
        let specs = phase7_event_reaction_segment_backfill_specs_for_days(20);
        let late = specs
            .iter()
            .find(|spec| spec.factor_code == "event_reaction_forecast_6_20d_indrel_std")
            .expect("late forecast segment");
        let sql = phase7_factor_backfill_sql(late);

        assert!(sql.contains("event.available_at <= $4"));
        assert!(sql.contains("td.trade_date >= events.available_at + INTERVAL '6 days'"));
        assert!(sql.contains("td.trade_date <= events.available_at + INTERVAL '20 days'"));
        assert!(sql.contains("anchor_bar.trade_date <= events.available_at"));
        assert!(sql.contains("current_bar.trade_date = td.trade_date"));
        assert!(
            !sql.contains("current_bar.trade_date > td.trade_date"),
            "segmented event reaction feature must not read future price data"
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
        assert!(forecast_sql.contains("event.available_at <= $4"));
        assert!(forecast_sql.contains("td.trade_date >= event_intervals.available_at"));
        assert!(forecast_sql
            .contains("percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)"));

        assert!(express_sql.contains("market_stock_express"));
        assert!(express_sql.contains("COALESCE(event.diluted_roe::double precision, 0.0)"));
        assert!(express_sql.contains("CASE"));
        assert!(express_sql.contains("event.available_at <= $4"));
        assert!(express_sql.contains("td.trade_date >= event_intervals.available_at"));

        assert!(disclosure_sql.contains("market_stock_disclosure_date"));
        assert!(disclosure_sql.contains("event.pre_date"));
        assert!(disclosure_sql.contains("event.actual_date"));
        assert!(disclosure_sql.contains("CASE"));
    }

    #[test]
    fn phase7_event_surprise_latest_sql_uses_pit_event_intervals_without_lateral_scan() {
        let specs = phase7_event_surprise_backfill_specs();
        let forecast = specs
            .iter()
            .find(|spec| spec.factor_code == "event_forecast_surprise_bucket_std")
            .expect("forecast surprise spec");

        let sql = phase7_factor_backfill_sql(forecast);

        assert!(sql.contains("event.available_at IS NOT NULL"));
        assert!(sql.contains("event.available_at <= $4"));
        assert!(sql.contains("ROW_NUMBER() OVER ("));
        assert!(sql.contains("PARTITION BY event.symbol, event.available_at"));
        assert!(sql.contains("LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at)"));
        assert!(sql.contains("td.trade_date >= event_intervals.available_at"));
        assert!(sql.contains("td.trade_date < COALESCE(event_intervals.next_available_at, $4 + 1)"));
        assert!(sql.contains("td.trade_date BETWEEN $3 AND $4"));
        assert!(!sql.contains("JOIN LATERAL"));
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
        assert!((job.factor_sql)(&job.specs[0]).contains("market_stock_daily_bar_adj"));
    }

    #[test]
    fn phase7_backfill_sql_uses_set_based_rank_and_complete_window_frames() {
        let specs = phase7_price_volume_backfill_specs();
        let downvol = specs
            .iter()
            .find(|spec| spec.factor_code == "downvol_20d_std")
            .expect("downvol spec");
        let sql = phase7_factor_backfill_sql(downvol);

        assert!(sql.contains("market_stock_daily_bar_adj"));
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
    fn phase7_financial_quality_change_sql_uses_prior_disclosed_yoy_acceleration_without_future_data(
    ) {
        let specs = phase7_financial_quality_change_backfill_specs();
        let roe = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_roe_yoy_accel_std")
            .expect("roe acceleration spec");
        let debt = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_debt_to_assets_yoy_decel_std")
            .expect("debt deceleration spec");

        let roe_sql = phase7_factor_backfill_sql(roe);
        let debt_sql = phase7_factor_backfill_sql(debt);

        assert!(roe_sql.contains("market_financial_indicator"));
        assert!(roe_sql.contains("latest.ann_date <= $4"));
        assert!(
            roe_sql.contains("latest_prev.end_date = (latest.end_date - INTERVAL '1 year')::date")
        );
        assert!(roe_sql.contains("prior_latest.ann_date < latest.ann_date"));
        assert!(roe_sql
            .contains("prior_prev.end_date = (prior_latest.end_date - INTERVAL '1 year')::date"));
        assert!(roe_sql.contains("latest_yoy.raw_value - prior_yoy.raw_value AS raw_value"));
        assert!(roe_sql.contains("GREATEST(latest.ann_date, latest_prev.ann_date, prior_latest.ann_date, prior_prev.ann_date)"));
        assert!(roe_sql.contains("WHERE available_at <= $4"));
        assert!(roe_sql.contains("LEAD(available_at) OVER"));
        assert!(roe_sql.contains("td.trade_date >= yi.available_at"));
        assert!(roe_sql.contains("td.trade_date < yi.next_available_at"));
        assert!(roe_sql.contains("available_at"));
        assert!(debt_sql.contains("prior_yoy.raw_value - latest_yoy.raw_value AS raw_value"));
    }

    #[test]
    fn phase7_earnings_recovery_persistence_sql_uses_three_disclosed_yoy_points() {
        let specs = phase7_earnings_recovery_persistence_backfill_specs();
        let roe = specs
            .iter()
            .find(|spec| spec.factor_code == "fin_roe_yoy_recovery_persist_std")
            .expect("roe persistence spec");

        let sql = phase7_factor_backfill_sql(roe);

        assert!(sql.contains("market_financial_indicator"));
        assert!(sql.contains("latest.ann_date <= $4"));
        assert!(sql.contains("annual_yoy_points AS"));
        assert!(sql.contains("current_report.ann_date AS current_ann_date"));
        assert!(sql.contains(
            "previous_report.end_date = (current_report.end_date - INTERVAL '1 year')::date"
        ));
        assert!(sql.contains(
            "GREATEST(current_report.ann_date, previous_report.ann_date) AS yoy_available_at"
        ));
        assert!(sql.contains("sequenced_yoy AS"));
        assert!(sql.contains("LAG(raw_value, 1) OVER"));
        assert!(sql.contains("LAG(raw_value, 2) OVER"));
        assert!(sql
            .contains("yoy_value + 0.5 * prior_yoy_value + 0.25 * second_yoy_value AS raw_value"));
        assert!(sql.contains(
            "GREATEST(yoy_available_at, prior_yoy_available_at, second_yoy_available_at) AS available_at"
        ));
        assert!(sql.contains("WHERE available_at <= $4"));
        assert!(sql.contains("LEAD(available_at) OVER"));
        assert!(sql.contains("td.trade_date >= yi.available_at"));
        assert!(sql.contains("td.trade_date < yi.next_available_at"));
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
        assert!(net_sql.contains("JOIN market_stock_daily_bar_adj bar"));
        assert!(net_sql.contains("mf.net_mf_amount::double precision"));
        assert!(net_sql.contains("SUM(flow_amount) OVER"));
        assert!(net_sql.contains("SUM(traded_amount) OVER"));
        assert!(net_sql.contains("ROWS BETWEEN 19 PRECEDING AND CURRENT ROW"));
        assert!(net_sql.contains("normalized_value, trade_date"));
        assert!(pressure_sql.contains("mf.sell_sm_amount::double precision"));
        assert!(pressure_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
    }

    #[test]
    fn phase7_cashflow_quality_sql_uses_interval_pit_carry_forward() {
        let specs = phase7_cashflow_quality_backfill_specs();
        let ocf_cover = specs
            .iter()
            .find(|spec| spec.factor_code == "cf_ocf_to_profit_latest_std")
            .expect("ocf cover spec");
        let gap = specs
            .iter()
            .find(|spec| spec.factor_code == "cf_ocf_profit_gap_latest_std")
            .expect("cashflow profit gap spec");

        let ocf_sql = phase7_factor_backfill_sql(ocf_cover);
        let gap_sql = phase7_factor_backfill_sql(gap);

        assert!(ocf_sql.contains("market_stock_cashflow"));
        assert!(ocf_sql.contains("DISTINCT ON (cf.symbol, cf.available_at)"));
        assert!(ocf_sql.contains("LEAD(available_at) OVER"));
        assert!(ocf_sql
            .contains("ORDER BY cf.symbol, cf.available_at, cf.end_date DESC, cf.ann_date DESC"));
        assert!(ocf_sql.contains("td.trade_date >= vi.available_at"));
        assert!(ocf_sql
            .contains("vi.next_available_at IS NULL OR td.trade_date < vi.next_available_at"));
        assert!(!ocf_sql.contains("JOIN LATERAL"));
        assert!(ocf_sql.contains("n_cashflow_act::double precision"));
        assert!(ocf_sql.contains("net_profit::double precision"));
        assert!(ocf_sql.contains("available_at"));
        assert!(ocf_sql.contains("ORDER BY raw_value) AS normalized_value"));
        assert!(gap_sql.contains("n_cashflow_act::double precision - net_profit::double precision"));
    }

    #[test]
    fn phase7_dividend_quality_sql_uses_only_announced_history_window() {
        let specs = phase7_dividend_quality_backfill_specs();
        let years = specs
            .iter()
            .find(|spec| spec.factor_code == "div_paid_years_4y_std")
            .expect("paid years spec");
        let stability = specs
            .iter()
            .find(|spec| spec.factor_code == "div_stability_4y_std")
            .expect("dividend stability spec");

        let years_sql = phase7_factor_backfill_sql(years);
        let stability_sql = phase7_factor_backfill_sql(stability);

        assert!(years_sql.contains("market_stock_dividend div"));
        assert!(years_sql.contains("div.ann_date <= td.trade_date"));
        assert!(years_sql.contains("div.end_date >= (td.trade_date - INTERVAL '4 years')::date"));
        assert!(years_sql.contains("COUNT(DISTINCT div.end_date)"));
        assert!(years_sql.contains("MAX(div.ann_date) AS available_at"));
        assert!(years_sql.contains("ORDER BY raw_value) AS normalized_value"));
        assert!(stability_sql.contains("STDDEV_POP(cash_div_value)"));
        assert!(stability_sql.contains("ORDER BY raw_value DESC) AS normalized_value"));
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
        assert!(forecast_sql.contains("event.available_at <= $4"));
        assert!(forecast_sql.contains("LEAD(available_at) OVER"));
        assert!(forecast_sql.contains("td.trade_date >= event_intervals.available_at"));
        assert!(!forecast_sql.contains("JOIN LATERAL"));
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
        let specs = phase7_event_window_alpha_backfill_specs_for_days(20);
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
    fn yearly_backfill_segments_split_cross_year_ranges() {
        let start = NaiveDate::from_ymd_opt(2023, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 19).unwrap();

        let segments = yearly_backfill_segments(start, end);

        assert_eq!(
            segments,
            vec![
                (
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 6, 19).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn quarterly_backfill_segments_split_cross_quarter_ranges() {
        let start = NaiveDate::from_ymd_opt(2023, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 5, 15).unwrap();

        let segments = quarterly_backfill_segments(start, end);

        assert_eq!(
            segments,
            vec![
                (
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 3, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 4, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 6, 30).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 7, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 9, 30).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 10, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2024, 3, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2024, 5, 15).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn monthly_backfill_segments_split_cross_month_ranges() {
        let start = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 3, 8).unwrap();

        let segments = monthly_backfill_segments(start, end);

        assert_eq!(
            segments,
            vec![
                (
                    NaiveDate::from_ymd_opt(2023, 1, 15).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 1, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 2, 28).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 3, 8).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn margin_detail_backfill_segments_use_quarter_boundaries() {
        let start = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 8, 8).unwrap();

        let segments = margin_detail_backfill_segments(start, end);

        assert_eq!(
            segments,
            vec![
                (
                    NaiveDate::from_ymd_opt(2023, 1, 15).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 3, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 4, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 6, 30).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2023, 7, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 8, 8).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn phase7_event_surprise_combo_allows_sparse_segment_days() {
        let specs = phase7_event_surprise_backfill_specs();
        let plan = Phase7EventSurpriseBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid event-surprise alpha plan");

        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);
    }

    #[test]
    fn phase7_forecast_revision_surprise_uses_prior_visible_forecast_only() {
        let specs = phase7_forecast_revision_surprise_backfill_specs();
        let plan = Phase7ForecastRevisionSurpriseBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid forecast revision surprise plan");

        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();

        assert_eq!(plan.combo_name, "phase7_forecast_revision_surprise_v1");
        assert_eq!(plan.bundle_name, "phase7_forecast_revision_surprise_v1");
        assert_eq!(plan.combo_method, "weighted_forecast_revision");
        assert_eq!(plan.phase, "7-P3.11");
        assert_eq!(
            codes,
            vec![
                "forecast_pchange_revision_delta_120d_std",
                "forecast_profit_mid_revision_pct_120d_std",
                "forecast_type_upgrade_120d_std",
            ]
        );
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);

        let sql = phase7_factor_backfill_sql(&specs[0]);
        assert!(sql.contains("FROM market_stock_forecast latest"));
        assert!(sql.contains("JOIN LATERAL"));
        assert!(sql.contains("previous.symbol = latest.symbol"));
        assert!(sql.contains("previous.end_date = latest.end_date"));
        assert!(sql.contains("previous.available_at < latest.available_at"));
        assert!(sql.contains("GREATEST(latest.available_at, previous.available_at)"));
        assert!(sql.contains("td.trade_date <= event_intervals.available_at + INTERVAL '120 days'"));
        assert!(sql.contains("LEAD(available_at) OVER"));
    }

    #[test]
    fn phase7_repurchase_supply_shock_uses_pit_event_latest_source() {
        let specs = phase7_repurchase_supply_shock_backfill_specs();
        let plan = Phase7RepurchaseSupplyShockBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid repurchase supply shock plan");

        assert!(specs
            .iter()
            .any(|spec| spec.factor_code == "repurchase_amount_log_latest_std"));
        assert_eq!(plan.combo_name, "phase7_repurchase_supply_shock_v1");
        assert_eq!(plan.bundle_name, "phase7_repurchase_supply_shock_v1");
        assert_eq!(
            plan.dependencies,
            &["market_stock_repurchase", "market_trade_calendar"]
        );
        assert_eq!(plan.combo_method, "weighted_repurchase_supply_shock");
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);

        let sql = phase7_factor_backfill_sql(&specs[0]);
        assert!(sql.contains("market_stock_repurchase"));
        assert!(sql.contains("event.available_at <= $4"));
        assert!(sql.contains("LEAD(available_at) OVER"));
        assert!(sql.contains("td.trade_date >= event_intervals.available_at"));
        assert!(sql.contains("normalized_value, available_at"));
    }

    #[test]
    fn phase7_block_trade_supply_demand_uses_pit_windowed_trade_events() {
        let specs = phase7_block_trade_supply_demand_backfill_specs();
        let plan = Phase7BlockTradeSupplyDemandBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid block-trade supply-demand plan");

        let codes = specs
            .iter()
            .map(|spec| spec.factor_code)
            .collect::<Vec<_>>();

        assert_eq!(plan.combo_name, "phase7_block_trade_supply_demand_v1");
        assert_eq!(plan.bundle_name, "phase7_block_trade_supply_demand_v1");
        assert_eq!(plan.phase, "7-P3.15");
        assert_eq!(
            plan.dependencies,
            &[
                "market_stock_block_trade",
                "market_stock_daily_bar_adj",
                "market_trade_calendar"
            ]
        );
        assert_eq!(plan.combo_method, "weighted_block_trade_sd");
        assert!(plan.combo_method.len() <= 32);
        assert_eq!(
            codes,
            vec![
                "block_trade_inst_buy_20d_decay_std",
                "block_trade_inst_sell_inverse_20d_decay_std",
                "block_trade_premium_20d_decay_std",
            ]
        );
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);

        let sql = phase7_factor_backfill_sql(&specs[2]);
        assert!(sql.contains("FROM market_stock_block_trade event"));
        assert!(sql.contains("event.ts_code AS symbol"));
        assert!(sql.contains("LEFT JOIN market_stock_daily_bar_adj bar"));
        assert!(sql.contains("event.available_at > event.trade_date"));
        assert!(sql.contains("event.available_at <= $4"));
        assert!(sql.contains("td.trade_date >= events.available_at"));
        assert!(sql.contains("SUM(event_raw_value * decay_weight) AS raw_value"));
        assert!(sql.contains("event.price::double precision / NULLIF(bar.close::double precision"));
    }

    #[test]
    fn phase7_unlock_supply_pressure_uses_float_date_window_and_daily_pit() {
        let specs = phase7_unlock_supply_pressure_backfill_specs();
        let plan = Phase7UnlockSupplyPressureBackfillRequest {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-05-31".to_string()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("valid unlock supply pressure plan");

        assert_eq!(plan.combo_name, "phase7_unlock_supply_pressure_v1");
        assert_eq!(plan.bundle_name, "phase7_unlock_supply_pressure_v1");
        assert_eq!(
            plan.dependencies,
            &[
                "market_stock_share_float",
                "market_stock_daily_bar_adj",
                "market_trade_calendar"
            ]
        );
        assert_eq!(plan.combo_method, "weighted_unlock_supply_pressure");
        assert_eq!(phase7_combo_required_factor_count(&specs, &plan), 1);

        let sql = phase7_factor_backfill_sql(&specs[0]);
        assert!(sql.contains("market_stock_share_float event"));
        assert!(sql.contains("event.available_at <= universe.trade_date"));
        assert!(sql.contains("event.float_date >= universe.trade_date"));
        assert!(sql.contains("event.float_date <= universe.trade_date + INTERVAL '30 days'"));
        assert!(sql.contains("COALESCE(SUM(COALESCE(event.float_ratio"));
        assert!(sql.contains("-raw_unlock_ratio"));
        assert!(sql.contains("market_stock_daily_bar_adj universe"));
        assert!(sql.contains("normalized_value, available_at"));
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
        for profile_name in [
            "quality_event_window_overlay_5pct",
            "quality_event_post_return_curve_overlay_5pct",
            "fq_change_event_surprise_sleeve_05pct",
            "fq_change_event_surprise_sleeve_10pct",
            "fq_change_event_surprise_sleeve_15pct_boundary",
            "fq_change_supply_float_sleeve_05pct",
            "fq_change_supply_float_sleeve_10pct",
            "fq_change_supply_float_sleeve_15pct_boundary",
            "fq_change_forecast_revision_sleeve_05pct",
            "fq_change_forecast_revision_sleeve_10pct",
            "fq_change_forecast_revision_sleeve_15pct_boundary",
            "quality_event_reaction_segments_overlay_5pct",
            "quality_event_reaction_reversal_overlay_5pct",
        ] {
            let req = Phase7AlphaBlendProfilesBackfillRequest {
                start_date: Some("2016-02-01".to_string()),
                end_date: Some("2016-02-05".to_string()),
                version: None,
                profile_names: Some(vec![profile_name.to_string()]),
                statement_timeout_ms: Some(0),
            };
            let plans = req.into_plans().expect("valid optional overlay profile");
            let plan = plans.first().expect("one selected profile");

            assert_eq!(plan.combo_method, "weighted_combo_optional_overlay");
            assert_eq!(plan.source_combos.len(), 2);
            assert_eq!(phase7_alpha_blend_required_source_count(plan), 1);
        }
    }

    #[test]
    fn phase7_cashflow_dividend_confirmation_profiles_keep_sparse_sources_optional() {
        for profile_name in [
            "quality_dividend_confirm_5pct",
            "quality_cashflow_dividend_confirm_10pct",
        ] {
            let req = Phase7AlphaBlendProfilesBackfillRequest {
                start_date: Some("2016-02-01".to_string()),
                end_date: Some("2016-02-05".to_string()),
                version: None,
                profile_names: Some(vec![profile_name.to_string()]),
                statement_timeout_ms: Some(0),
            };
            let plans = req.into_plans().expect("valid cashflow/dividend profile");
            let plan = plans.first().expect("one selected profile");

            assert_eq!(plan.combo_method, "weighted_combo_optional_overlay");
            assert_eq!(
                plan.source_combos.first().expect("base source").combo_name,
                "phase7_financial_quality_v1"
            );
            assert_eq!(phase7_alpha_blend_required_source_count(plan), 1);
        }
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

