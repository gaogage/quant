//! Price-volume factors: momentum, volatility, turnover
//!
//! Each factor is implemented as a standalone function for testability,
//! plus a Factor struct that implements the core trait.

use chrono::NaiveDate;
use rust_decimal::Decimal;

use crate::types::*;

// ─── Momentum Factor ─────────────────────────────────────────────

/// Momentum factor: (close_t - close_{t-n}) / close_{t-n}
///
/// # Parameters
/// - `period`: lookback window in trading days (default: 20)
pub struct MomentumFactor {
    pub period: usize,
}

impl MomentumFactor {
    pub fn new(period: usize) -> Self {
        Self { period }
    }

    /// Compute raw momentum values for all symbols
    pub fn compute(&self, input: &FactorInput) -> FactorOutput {
        let mut values = Vec::new();

        for (symbol, bars) in &input.bars {
            // Need at least period+1 bars
            if bars.len() <= self.period {
                continue;
            }

            for i in self.period..bars.len() {
                let close_t = bars[i].close;
                let close_tn = bars[i - self.period].close;

                if close_tn.is_zero() {
                    continue;
                }

                let raw: Decimal = (close_t - close_tn) / close_tn;
                let val: f64 = raw.try_into().unwrap_or(f64::NAN);

                values.push(FactorValue {
                    symbol: symbol.clone(),
                    date: bars[i].trade_date,
                    value: val,
                });
            }
        }

        let meta = build_metadata(&values, "momentum", FactorCategory::PriceVolume, "1.0.0",
            serde_json::json!({"period": self.period}));

        FactorOutput {
            name: format!("mom_{}d", self.period),
            values,
            metadata: meta,
        }
    }
}

// ─── Volatility Factor ────────────────────────────────────────────

/// Historical volatility: std of daily returns over a lookback window
///
/// Daily return = ln(close_t / close_{t-1})
/// Volatility = std(returns) * sqrt(252) (annualized)
pub struct VolatilityFactor {
    pub period: usize,
}

impl VolatilityFactor {
    pub fn new(period: usize) -> Self {
        Self { period }
    }

    pub fn compute(&self, input: &FactorInput) -> FactorOutput {
        let mut values = Vec::new();

        for (symbol, bars) in &input.bars {
            if bars.len() <= self.period + 1 {
                continue;
            }

            // Pre-compute log returns
            let mut log_returns: Vec<f64> = Vec::with_capacity(bars.len() - 1);
            for i in 1..bars.len() {
                let close_t: f64 = bars[i].close.try_into().unwrap_or(f64::NAN);
                let close_t1: f64 = bars[i - 1].close.try_into().unwrap_or(f64::NAN);
                if close_t1 > 0.0 {
                    log_returns.push((close_t / close_t1).ln());
                } else {
                    log_returns.push(0.0);
                }
            }

            for i in self.period..bars.len() {
                let start = i - self.period;
                let end = i;
                if end > log_returns.len() {
                    break;
                }

                let slice = &log_returns[start..end];
                let n = slice.len() as f64;
                if n < 2.0 {
                    continue;
                }
                let mean: f64 = slice.iter().sum::<f64>() / n;
                let variance: f64 = slice.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);

                // Annualized: sqrt(variance * 252)
                let annual_vol = (variance * 252.0).sqrt();

                values.push(FactorValue {
                    symbol: symbol.clone(),
                    date: bars[i].trade_date,
                    value: annual_vol,
                });
            }
        }

        let meta = build_metadata(&values, "volatility", FactorCategory::PriceVolume, "1.0.0",
            serde_json::json!({"period": self.period, "annualized": true}));

        FactorOutput {
            name: format!("vol_{}d", self.period),
            values,
            metadata: meta,
        }
    }
}

// ─── Turnover Factor ──────────────────────────────────────────────

/// Average turnover rate over a lookback window
///
/// Daily turnover = volume / float_shares (approximated)
/// Here we use volume / volume_ma as a proxy for turnover intensity
pub struct TurnoverFactor {
    pub period: usize,
}

impl TurnoverFactor {
    pub fn new(period: usize) -> Self {
        Self { period }
    }

    pub fn compute(&self, input: &FactorInput) -> FactorOutput {
        let mut values = Vec::new();

        for (symbol, bars) in &input.bars {
            if bars.len() <= self.period {
                continue;
            }

            for i in self.period..bars.len() {
                let slice = &bars[i - self.period..i];
                let avg_volume: Decimal = slice.iter()
                    .map(|b| b.volume)
                    .sum::<Decimal>()
                    / Decimal::from(self.period as u64);

                let current_vol: f64 = bars[i].volume.try_into().unwrap_or(f64::NAN);
                let avg_vol: f64 = avg_volume.try_into().unwrap_or(f64::NAN);

                let turnover = if avg_vol > 0.0 {
                    current_vol / avg_vol
                } else {
                    f64::NAN
                };

                values.push(FactorValue {
                    symbol: symbol.clone(),
                    date: bars[i].trade_date,
                    value: turnover,
                });
            }
        }

        let meta = build_metadata(&values, "turnover", FactorCategory::PriceVolume, "1.0.0",
            serde_json::json!({"period": self.period}));

        FactorOutput {
            name: format!("turn_{}d", self.period),
            values,
            metadata: meta,
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────

pub use crate::standardize::helpers::build_metadata_inner;

fn build_metadata(
    values: &[FactorValue],
    name: &str,
    category: FactorCategory,
    version: &str,
    params: serde_json::Value,
) -> FactorMetadata {
    let n = values.len() as f64;
    if n == 0.0 {
        return FactorMetadata {
            factor_name: name.to_string(),
            category,
            version: version.to_string(),
            params,
            computed_at: chrono::Utc::now(),
            symbol_count: 0,
            date_count: 0,
            coverage_ratio: 0.0,
            mean: f64::NAN,
            std: f64::NAN,
            min: f64::NAN,
            max: f64::NAN,
        };
    }

    let valid: Vec<f64> = values.iter().map(|v| v.value).filter(|x| x.is_finite()).collect();
    let m = valid.len() as f64;
    let mean = valid.iter().sum::<f64>() / m;
    let variance = valid.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / m;
    let std = variance.sqrt();
    let min = valid.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = valid.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let symbols: std::collections::HashSet<&str> = values.iter().map(|v| v.symbol.as_str()).collect();
    let dates: std::collections::HashSet<NaiveDate> = values.iter().map(|v| v.date).collect();

    FactorMetadata {
        factor_name: name.to_string(),
        category,
        version: version.to_string(),
        params,
        computed_at: chrono::Utc::now(),
        symbol_count: symbols.len(),
        date_count: dates.len(),
        coverage_ratio: m / n,
        mean,
        std,
        min,
        max,
    }
}

// ─── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use rust_decimal::Decimal;

    fn make_bars(symbol: &str, closes: &[f64], volumes: &[f64]) -> Vec<DailyBar> {
        let base_date = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
        closes.iter().enumerate().map(|(i, c)| {
            let date = base_date + chrono::Duration::days(i as i64);
            DailyBar {
                symbol: symbol.to_string(),
                trade_date: date,
                open: Decimal::from_f64_retain(*c * 0.99).unwrap(),
                high: Decimal::from_f64_retain(*c * 1.02).unwrap(),
                low: Decimal::from_f64_retain(*c * 0.98).unwrap(),
                close: Decimal::from_f64_retain(*c).unwrap(),
                pre_close: Some(Decimal::from_f64_retain(if i == 0 { *c } else { closes[i-1] }).unwrap()),
                change_pct: None,
                volume: Decimal::from_f64_retain(volumes[i]).unwrap(),
                amount: Decimal::from_f64_retain(*c * volumes[i]).unwrap(),
            }
        }).collect()
    }

    #[test]
    fn test_momentum_factor() {
        let factor = MomentumFactor::new(5);
        let bars = make_bars("000001.SZ", &[10.0, 10.5, 11.0, 10.8, 11.5, 12.0, 12.5, 13.0], &[1000.0; 8]);
        let mut input_bars = HashMap::new();
        input_bars.insert("000001.SZ".to_string(), bars);
        let input = FactorInput { bars: input_bars, trade_dates: vec![] };

        let output = factor.compute(&input);
        assert_eq!(output.name, "mom_5d");
        // mom at day 5: (12.0 - 10.0)/10.0 = 0.20
        let d5 = &output.values[0];
        assert!((d5.value - 0.20).abs() < 0.001);
        // mom at day 6: (12.5 - 10.5)/10.5 ≈ 0.1905
        let d6 = &output.values[1];
        assert!((d6.value - 0.190476).abs() < 0.01);
        // mom at day 7: (13.0 - 11.0)/11.0 ≈ 0.1818
        let d7 = &output.values[2];
        assert!((d7.value - 0.181818).abs() < 0.01);
    }

    #[test]
    fn test_volatility_factor() {
        let factor = VolatilityFactor::new(5);
        let bars = make_bars("000001.SZ",
            &[10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8],
            &[1000.0; 9]);
        let mut input_bars = HashMap::new();
        input_bars.insert("000001.SZ".to_string(), bars);
        let input = FactorInput { bars: input_bars, trade_dates: vec![] };

        let output = factor.compute(&input);
        assert_eq!(output.name, "vol_5d");
        assert!(!output.values.is_empty());
        // All returns are positive (~0.0099), so vol should be small
        assert!(output.metadata.mean < 0.5);
    }

    #[test]
    fn test_turnover_factor() {
        let factor = TurnoverFactor::new(5);
        let vol: Vec<f64> = vec![1000.0, 1200.0, 1100.0, 1300.0, 900.0, 1500.0, 800.0, 2000.0];
        let bars = make_bars("000001.SZ", &[10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0], &vol);
        let mut input_bars = HashMap::new();
        input_bars.insert("000001.SZ".to_string(), bars);
        let input = FactorInput { bars: input_bars, trade_dates: vec![] };

        let output = factor.compute(&input);
        assert_eq!(output.name, "turn_5d");
        assert_eq!(output.values.len(), 3);
        // day 5: 1500 / mean(1000,1200,1100,1300,900) = 1500/1100 ≈ 1.364
        assert!((output.values[0].value - 1.364).abs() < 0.01);
    }
}
