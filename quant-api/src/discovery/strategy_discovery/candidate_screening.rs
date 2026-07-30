//! BC4 策略发现 / 候选筛选门禁：优化结果分级与排序。
//!
//! 由 phase7.rs 拆出（DDD 重构 R8 批次1），纯 move，零行为变更。

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateTargets {
    pub min_annual_return: Decimal,
    pub min_excess_return: Decimal,
    pub min_sharpe: Decimal,
    pub min_sortino: Decimal,
    pub max_drawdown: Decimal,
}

impl Default for CandidateTargets {
    fn default() -> Self {
        Self {
            min_annual_return: Decimal::new(15, 2),
            min_excess_return: Decimal::ZERO,
            min_sharpe: Decimal::ONE,
            min_sortino: Decimal::new(15, 1),
            max_drawdown: Decimal::new(35, 2),
        }
    }
}

impl CandidateTargets {
    pub fn classify(&self, metrics: &CandidateMetrics) -> CandidateType {
        if metrics.annual_return >= self.min_annual_return
            && metrics.excess_return > self.min_excess_return
            && metrics.sharpe > self.min_sharpe
            && metrics.sortino >= self.min_sortino
            && metrics.max_drawdown < self.max_drawdown
        {
            CandidateType::Professional
        } else if metrics.annual_return >= self.min_annual_return
            && metrics.excess_return > self.min_excess_return
        {
            CandidateType::ReviewRequired
        } else if metrics.annual_return >= Decimal::ZERO
            && metrics.max_drawdown <= Decimal::new(35, 2)
        {
            CandidateType::Defensive
        } else {
            CandidateType::Research
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateType {
    #[serde(rename = "professional_candidate")]
    Professional,
    #[serde(rename = "review_required")]
    ReviewRequired,
    #[serde(rename = "defensive_candidate")]
    Defensive,
    #[serde(rename = "research_candidate")]
    Research,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateMetrics {
    pub annual_return: Decimal,
    pub excess_return: Decimal,
    pub sharpe: Decimal,
    pub sortino: Decimal,
    pub max_drawdown: Decimal,
    pub total_return: Decimal,
    pub benchmark_return: Decimal,
    pub turnover: Decimal,
    pub num_trades: u64,
    pub execution_schedule_expired_count: u64,
    #[serde(default)]
    pub execution_schedule_roll_forward_count: u64,
    pub max_execution_target_gap: Decimal,
    pub final_cash_weight: Decimal,
    #[serde(default)]
    pub final_target_gross_exposure: Decimal,
    #[serde(default)]
    pub final_actual_gross_exposure: Decimal,
    #[serde(default)]
    pub final_unfilled_target_gap: Decimal,
    #[serde(default = "default_candidate_execution_fill_ratio")]
    pub final_execution_fill_ratio: Decimal,
}

fn default_candidate_execution_fill_ratio() -> Decimal {
    Decimal::ONE
}

impl Default for CandidateMetrics {
    fn default() -> Self {
        Self {
            annual_return: Decimal::ZERO,
            excess_return: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            sortino: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            total_return: Decimal::ZERO,
            benchmark_return: Decimal::ZERO,
            turnover: Decimal::ZERO,
            num_trades: 0,
            execution_schedule_expired_count: 0,
            execution_schedule_roll_forward_count: 0,
            max_execution_target_gap: Decimal::ZERO,
            final_cash_weight: Decimal::ZERO,
            final_target_gross_exposure: Decimal::ZERO,
            final_actual_gross_exposure: Decimal::ZERO,
            final_unfilled_target_gap: Decimal::ZERO,
            final_execution_fill_ratio: Decimal::ONE,
        }
    }
}

impl CandidateMetrics {
    pub fn from_optimization_metrics(metrics: &Value) -> Self {
        Self {
            annual_return: decimal_field(metrics, "annual_return_pct"),
            excess_return: decimal_field(metrics, "excess_return_pct"),
            sharpe: decimal_field(metrics, "sharpe_ratio"),
            sortino: decimal_field(metrics, "sortino_ratio"),
            max_drawdown: decimal_field(metrics, "max_drawdown_pct"),
            total_return: decimal_field(metrics, "total_return_pct"),
            benchmark_return: decimal_field(metrics, "benchmark_return_pct"),
            turnover: decimal_field(metrics, "turnover"),
            num_trades: metrics
                .get("num_trades")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| decimal_field(metrics, "num_trades").to_u64().unwrap_or(0)),
            execution_schedule_expired_count: metrics
                .get("execution_schedule_expired_count")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| {
                    decimal_field(metrics, "execution_schedule_expired_count")
                        .to_u64()
                        .unwrap_or(0)
                }),
            execution_schedule_roll_forward_count: metrics
                .get("execution_schedule_roll_forward_count")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| {
                    decimal_field(metrics, "execution_schedule_roll_forward_count")
                        .to_u64()
                        .unwrap_or(0)
                }),
            max_execution_target_gap: decimal_field(metrics, "max_execution_target_gap_pct"),
            final_cash_weight: decimal_field(metrics, "final_cash_weight_pct"),
            final_target_gross_exposure: decimal_field(metrics, "final_target_gross_exposure_pct"),
            final_actual_gross_exposure: decimal_field(metrics, "final_actual_gross_exposure_pct"),
            final_unfilled_target_gap: decimal_field(metrics, "final_unfilled_target_gap_pct"),
            final_execution_fill_ratio: decimal_field(metrics, "final_execution_fill_ratio"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateScreeningRow {
    pub candidate_id: String,
    pub candidate_type: CandidateType,
    pub metrics: CandidateMetrics,
    pub robustness_status: Option<String>,
    pub optimization_task_id: Option<String>,
    pub backtest_task_id: Option<String>,
    pub parameters: Value,
}

pub fn screen_optimization_results(
    results: &[Value],
    targets: &CandidateTargets,
) -> Vec<CandidateScreeningRow> {
    let mut rows: Vec<_> = results
        .iter()
        .map(|item| {
            let best_trial = item.get("best_trial").unwrap_or(&Value::Null);
            let metrics = CandidateMetrics::from_optimization_metrics(
                best_trial.get("metrics").unwrap_or(&Value::Null),
            );
            CandidateScreeningRow {
                candidate_id: item
                    .get("candidate_id")
                    .and_then(Value::as_str)
                    .or_else(|| item.get("optimization_task_id").and_then(Value::as_str))
                    .unwrap_or("unknown_candidate")
                    .to_string(),
                candidate_type: targets.classify(&metrics),
                metrics,
                robustness_status: item
                    .pointer("/robustness/data/status")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                optimization_task_id: item
                    .get("optimization_task_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                backtest_task_id: best_trial
                    .get("backtest_task_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                parameters: best_trial.get("parameters").cloned().unwrap_or(Value::Null),
            }
        })
        .collect();

    rows.sort_by(candidate_row_order);
    rows
}

fn candidate_row_order(
    left: &CandidateScreeningRow,
    right: &CandidateScreeningRow,
) -> std::cmp::Ordering {
    candidate_type_rank(left.candidate_type)
        .cmp(&candidate_type_rank(right.candidate_type))
        .then_with(|| right.metrics.annual_return.cmp(&left.metrics.annual_return))
        .then_with(|| right.metrics.sharpe.cmp(&left.metrics.sharpe))
        .then_with(|| right.metrics.sortino.cmp(&left.metrics.sortino))
        .then_with(|| right.metrics.excess_return.cmp(&left.metrics.excess_return))
        .then_with(|| left.metrics.max_drawdown.cmp(&right.metrics.max_drawdown))
}

fn candidate_type_rank(candidate_type: CandidateType) -> u8 {
    match candidate_type {
        CandidateType::Professional => 0,
        CandidateType::ReviewRequired => 1,
        CandidateType::Defensive => 2,
        CandidateType::Research => 3,
    }
}

pub(super) fn decimal_field(value: &Value, field: &str) -> Decimal {
    value
        .get(field)
        .and_then(|field_value| match field_value {
            Value::String(raw) => raw.parse().ok(),
            Value::Number(number) => number.to_string().parse().ok(),
            _ => None,
        })
        .unwrap_or(Decimal::ZERO)
}

pub(super) fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

