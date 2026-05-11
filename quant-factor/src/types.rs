//! Factor data types — input/output containers

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
}

/// Output of factor computation — vector of (symbol, date, value)
#[derive(Debug, Clone)]
pub struct FactorOutput {
    pub name: String,
    pub values: Vec<FactorValue>,
    /// Metadata about the computation
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
    /// Quantile return spread (top group - bottom group)
    pub quantile_spread: f64,
    /// Per-quantile average forward returns
    pub quantile_returns: Vec<f64>,
    /// Number of periods evaluated
    pub period_count: usize,
}
