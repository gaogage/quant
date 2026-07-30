//! Factor data types - input/output containers

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Daily bar input for factor computation (price-volume data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyBar {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub pre_close: Option<Decimal>,
    pub change_pct: Option<Decimal>,
    pub volume: Decimal,
    pub amount: Decimal,
}

/// Input bundle for factor computation
#[derive(Debug, Clone)]
pub struct FactorInput {
    /// All daily bars, keyed by symbol, sorted by date ascending
    pub bars: HashMap<String, Vec<DailyBar>>,
    /// Trade calendar (sorted ascending)
    pub trade_dates: Vec<NaiveDate>,
}

/// A single factor value for one symbol on one date
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorValue {
    pub symbol: String,
    pub date: NaiveDate,
    pub value: f64,
    pub available_at: Option<NaiveDate>,
}

/// Output of factor computation - vector of (symbol, date, value)
#[derive(Debug, Clone)]
pub struct FactorOutput {
    pub name: String,
    pub values: Vec<FactorValue>,
    /// Metadata about the computation
    pub metadata: FactorMetadata,
}

// ─── DDD Step 5a：PIT 类型门禁 ─────────────────────────────────────
//
// 目的：从类型层杜绝"用了未来函数泄露的脏因子数据"。`PitSeries<T>` 只能由
// `RawSeries::from_pit_with_available_at` 构造（要求传入 `available_at` PIT 时间戳），
// 进入计算管线的因子数据必须携带 PIT 印记。
//
// 设计原则：
// - `RawSeries<T>`：DB 加载的原始数据，未校验，crate 外不可构造（new 为 pub(crate)）。
// - `PitSeries<T>`：PIT 校验通过的数据，携带 `available_at`（因子可用日期），
//   唯一公开出口。`available_at > 评估日` 的数据视为未来函数，门禁处过滤。
// - 构造器私有化 + `from_pit_with_available_at` 唯一出口，编译期强制 PIT 语义。
//
// 语义说明（Step 5a 接入决策，2026-07-26）：
// 原设计用 `data_version_id` 做 PIT 印记，但实测发现因子数据管线 dv_id 完全未
// 落地（factor_value 12亿行0.04%、multi_factor_value 1.3亿行0%、factor_evaluation
// 0%），所有 INSERT 不写 dv_id 列，且历史数据源行情批次未记录无法追溯重建。
// 改用 `available_at`（因子原生 PIT 时间戳，100% 填充，语义正确无未来函数泄露）。

/// DB 加载的原始因子序列，尚未做 PIT 校验。
///
/// 仅 `quant-factor` crate 内部可构造（DB 查询结果直接组装），外部拿到后必须
/// 调 `from_pit_with_available_at` 升级为 `PitSeries` 才能进入计算管线。
#[derive(Debug, Clone)]
pub struct RawSeries<T> {
    values: Vec<T>,
}

impl<T> RawSeries<T> {
    /// crate 内部构造：DB 加载后组装。
    // Step 2 骨架方法，待 R3/R4 接入计算管线后启用。
    #[allow(dead_code)]
    pub(crate) fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    /// 只读访问底层值。
    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    /// 转移出底层值（供需要原始 Vec 的场景）。
    pub fn into_inner(self) -> Vec<T> {
        self.values
    }

    /// 唯一公开出口：把原始序列升级为 PIT 校验通过的序列。
    ///
    /// 调用方负责确保 `available_at` 是因子数据的真实可用日期（来自 DB 的
    /// `available_at` 列或因子计算时的 `trade_date`）。`available_at > 评估日`
    /// 的数据视为未来函数，调用方在门禁处过滤。
    pub fn from_pit_with_available_at(
        self,
        available_at: impl Into<Option<chrono::NaiveDate>>,
    ) -> PitSeries<T> {
        let avail = available_at.into();
        PitSeries {
            values: self.values,
            available_at: avail,
        }
    }
}

/// PIT 校验通过的因子序列，携带 `available_at` 追溯印记。
///
/// 类型门禁：只能由 `RawSeries::from_pit_with_available_at` 构造。
/// `available_at` 是因子数据的 PIT 时间戳（因子可用日期），
/// `available_at > 评估日` 的数据视为未来函数泄露。
#[derive(Debug, Clone)]
pub struct PitSeries<T> {
    values: Vec<T>,
    /// 该序列的 PIT 可用日期（因子可用日期，非空时用于 PIT 门禁）。
    ///
    /// None 表示 PIT 时间戳未知（如内存计算路径未填充），此时门禁降级为
    /// 无校验（与旧 FactorOutput 行为一致，向后兼容）。
    pub available_at: Option<chrono::NaiveDate>,
}

impl<T> PitSeries<T> {
    /// 只读访问底层值。
    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    /// 转移出底层值。
    pub fn into_inner(self) -> Vec<T> {
        self.values
    }

    /// 底层值数量。
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// PIT 校验通过的因子输出，替代裸 `FactorOutput` 进入计算管线。
///
/// `values` 为 `PitSeries<FactorValue>`，携带 `available_at` 印记。
#[derive(Debug, Clone)]
pub struct PitFactorOutput {
    pub name: String,
    pub values: PitSeries<FactorValue>,
    pub metadata: FactorMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorMetadata {
    pub factor_name: String,
    pub category: FactorCategory,
    pub version: String,
    pub params: serde_json::Value,
    pub computed_at: chrono::DateTime<chrono::Utc>,
    pub symbol_count: usize,
    pub date_count: usize,
    pub coverage_ratio: f64,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorCategory {
    PriceVolume,
    Fundamental,
    Sentiment,
    Alternative,
}

impl std::fmt::Display for FactorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::PriceVolume => "price_volume",
            Self::Fundamental => "fundamental",
            Self::Sentiment => "sentiment",
            Self::Alternative => "alternative",
        };
        write!(f, "{}", s)
    }
}

/// Standardization method for cross-sectional normalization
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandardizeMethod {
    /// Z-score: (x - mean) / std
    ZScore,
    /// Rank-based: percentile rank [0, 1]
    Rank,
    /// Winsorize then z-score: clip at ±N sigma, then z-score
    Winsorized(f64),
}

/// Evaluation metrics for a factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorEvaluation {
    pub factor_name: String,
    pub date_range: (NaiveDate, NaiveDate),
    /// Average IC (Information Coefficient)
    pub mean_ic: f64,
    /// IC-IR (IC / std(IC))
    pub ic_ir: f64,
    /// Rank IC average
    pub mean_rank_ic: f64,
    /// Rank IC-IR
    pub rank_ic_ir: f64,
    /// IC time series (date -> IC)
    pub ic_series: Vec<(NaiveDate, f64)>,
    /// Rank IC time series (date -> Rank IC)
    pub rank_ic_series: Vec<(NaiveDate, f64)>,
    /// Quantile return spread (top group - bottom group)
    pub quantile_spread: f64,
    /// Per-quantile average forward returns
    pub quantile_returns: Vec<f64>,
    /// Number of periods evaluated
    pub period_count: usize,
}

#[cfg(test)]
mod pit_tests {
    use super::*;

    #[test]
    fn raw_series_upgrades_to_pit_with_available_at() {
        let avail = NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        let raw = RawSeries::new(vec![FactorValue {
            symbol: "000001".into(),
            date: avail,
            value: 1.5,
            available_at: Some(avail),
        }]);
        let pit = raw.from_pit_with_available_at(Some(avail));
        assert_eq!(pit.available_at, Some(avail));
        assert_eq!(pit.len(), 1);
        assert!(!pit.is_empty());
    }

    #[test]
    fn none_available_at_degrades_gracefully() {
        // 内存计算路径未填充 available_at 时，None 降级为无 PIT 校验
        // （与旧 FactorOutput 行为一致，向后兼容）
        let raw: RawSeries<FactorValue> = RawSeries::new(vec![]);
        let pit = raw.from_pit_with_available_at(None);
        assert_eq!(pit.available_at, None);
        assert!(pit.is_empty());
    }

    #[test]
    fn pit_series_into_inner_yields_values() {
        let avail = NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        let raw = RawSeries::new(vec![
            FactorValue {
                symbol: "000001".into(),
                date: avail,
                value: 1.0,
                available_at: Some(avail),
            },
            FactorValue {
                symbol: "000002".into(),
                date: avail,
                value: 2.0,
                available_at: Some(avail),
            },
        ]);
        let pit = raw.from_pit_with_available_at(Some(avail));
        let vals = pit.into_inner();
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0].symbol, "000001");
    }
}
