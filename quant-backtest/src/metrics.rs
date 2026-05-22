//! 回测指标计算
//!
//! 绝对收益、相对收益、风险指标。

use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn default_execution_fill_ratio() -> Decimal {
    Decimal::one()
}

/// 回测绩效指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestMetrics {
    pub total_return: Decimal,
    pub total_return_pct: Decimal,
    pub annual_return_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub annual_volatility_pct: Decimal,
    pub annualized_volatility: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub calmar_ratio: Decimal,
    pub max_drawdown_duration_days: usize,
    pub benchmark_return_pct: Decimal,
    pub excess_return_pct: Decimal,
    pub information_ratio: Decimal,
    pub tracking_error_pct: Decimal,
    pub turnover: Decimal,
    pub num_trades: usize,
    pub win_rate_pct: Decimal,
    pub profit_factor: Decimal,
    pub execution_schedule_expired_count: usize,
    #[serde(default)]
    pub execution_schedule_roll_forward_count: usize,
    pub max_execution_target_gap_pct: Decimal,
    pub final_cash_weight_pct: Decimal,
    #[serde(default)]
    pub final_target_gross_exposure_pct: Decimal,
    #[serde(default)]
    pub final_actual_gross_exposure_pct: Decimal,
    #[serde(default)]
    pub final_unfilled_target_gap_pct: Decimal,
    #[serde(default = "default_execution_fill_ratio")]
    pub final_execution_fill_ratio: Decimal,
}

impl Default for BacktestMetrics {
    fn default() -> Self {
        Self {
            total_return: Decimal::zero(),
            total_return_pct: Decimal::zero(),
            annual_return_pct: Decimal::zero(),
            max_drawdown_pct: Decimal::zero(),
            annual_volatility_pct: Decimal::zero(),
            annualized_volatility: Decimal::zero(),
            sharpe_ratio: Decimal::zero(),
            sortino_ratio: Decimal::zero(),
            calmar_ratio: Decimal::zero(),
            max_drawdown_duration_days: 0,
            benchmark_return_pct: Decimal::zero(),
            excess_return_pct: Decimal::zero(),
            information_ratio: Decimal::zero(),
            tracking_error_pct: Decimal::zero(),
            turnover: Decimal::zero(),
            num_trades: 0,
            win_rate_pct: Decimal::zero(),
            profit_factor: Decimal::zero(),
            execution_schedule_expired_count: 0,
            execution_schedule_roll_forward_count: 0,
            max_execution_target_gap_pct: Decimal::zero(),
            final_cash_weight_pct: Decimal::zero(),
            final_target_gross_exposure_pct: Decimal::zero(),
            final_actual_gross_exposure_pct: Decimal::zero(),
            final_unfilled_target_gap_pct: Decimal::zero(),
            final_execution_fill_ratio: Decimal::one(),
        }
    }
}

impl BacktestMetrics {
    pub fn compute(nav: &[Decimal], bm_nav: &[Decimal], initial_capital: Decimal) -> Self {
        let n = nav.len();
        if n < 2 {
            return Self::empty();
        }

        // 总收益
        let total_return = nav[n - 1] - initial_capital;
        let total_return_pct = total_return / initial_capital;

        // 年化
        let years = n as f64 / 252.0;
        let final_ratio = if initial_capital.is_zero() {
            0.0
        } else {
            nav[n - 1].to_f64().unwrap_or(0.0) / initial_capital.to_f64().unwrap_or(1.0)
        };
        let annual_return_pct = if years > 0.0 && final_ratio > 0.0 {
            let ar = final_ratio.powf(1.0 / years) - 1.0;
            Decimal::from_f64(ar).unwrap_or_default()
        } else {
            Decimal::zero()
        };

        // 日收益率
        let daily: Vec<Decimal> = nav.windows(2).map(|w| (w[1] - w[0]) / w[0]).collect();

        // 波动率
        let annual_vol = if daily.len() <= 1 {
            Decimal::zero()
        } else {
            let mean = daily.iter().sum::<Decimal>() / Decimal::from(daily.len());
            let variance = daily
                .iter()
                .map(|x| (*x - mean) * (*x - mean))
                .sum::<Decimal>()
                / Decimal::from(daily.len() - 1);
            let daily_vol =
                Decimal::from_f64(variance.to_f64().unwrap_or(0.0).sqrt()).unwrap_or_default();
            daily_vol * Decimal::from_f64(252.0_f64.sqrt()).unwrap()
        };

        // Sortino uses downside deviation only. A strictly non-decreasing NAV has
        // effectively unbounded downside-adjusted return, capped for storage.
        let downside_vol = if daily.len() <= 1 {
            Decimal::zero()
        } else {
            let downside_sum = daily
                .iter()
                .filter(|value| **value < Decimal::zero())
                .map(|value| *value * *value)
                .sum::<Decimal>();
            if downside_sum.is_zero() {
                Decimal::zero()
            } else {
                let variance = downside_sum / Decimal::from(daily.len() - 1);
                let daily_downside =
                    Decimal::from_f64(variance.to_f64().unwrap_or(0.0).sqrt()).unwrap_or_default();
                daily_downside * Decimal::from_f64(252.0_f64.sqrt()).unwrap()
            }
        };

        // 夏普
        let sharpe = if annual_vol.is_zero() {
            Decimal::zero()
        } else {
            annual_return_pct / annual_vol
        };
        let sortino = if downside_vol.is_zero() {
            if annual_return_pct > Decimal::zero() {
                Decimal::new(999, 0)
            } else {
                Decimal::zero()
            }
        } else {
            annual_return_pct / downside_vol
        };

        // 最大回撤
        let mut max_dd: Decimal = Decimal::zero();
        let mut peak = nav[0];
        let mut current_drawdown_duration_days = 0usize;
        let mut max_drawdown_duration_days = 0usize;
        for v in nav {
            if *v >= peak {
                peak = *v;
                current_drawdown_duration_days = 0;
            } else {
                current_drawdown_duration_days += 1;
                max_drawdown_duration_days =
                    max_drawdown_duration_days.max(current_drawdown_duration_days);
            }
            let dd = (peak - *v) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
        let calmar_ratio = if max_dd.is_zero() {
            if annual_return_pct > Decimal::zero() {
                Decimal::new(999, 0)
            } else {
                Decimal::zero()
            }
        } else {
            annual_return_pct / max_dd
        };

        // 基准
        let bm_return = if !bm_nav.is_empty() && !bm_nav[0].is_zero() {
            (bm_nav[bm_nav.len() - 1] - bm_nav[0]) / bm_nav[0]
        } else {
            Decimal::zero()
        };
        let excess = total_return_pct - bm_return;

        Self {
            total_return,
            total_return_pct,
            annual_return_pct,
            max_drawdown_pct: max_dd,
            annual_volatility_pct: annual_vol,
            annualized_volatility: annual_vol,
            sharpe_ratio: sharpe,
            sortino_ratio: sortino,
            calmar_ratio,
            max_drawdown_duration_days,
            benchmark_return_pct: bm_return,
            excess_return_pct: excess,
            information_ratio: Decimal::zero(),
            tracking_error_pct: Decimal::zero(),
            turnover: Decimal::zero(),
            num_trades: 0,
            win_rate_pct: Decimal::zero(),
            profit_factor: Decimal::zero(),
            execution_schedule_expired_count: 0,
            execution_schedule_roll_forward_count: 0,
            max_execution_target_gap_pct: Decimal::zero(),
            final_cash_weight_pct: Decimal::zero(),
            final_target_gross_exposure_pct: Decimal::zero(),
            final_actual_gross_exposure_pct: Decimal::zero(),
            final_unfilled_target_gap_pct: Decimal::zero(),
            final_execution_fill_ratio: Decimal::one(),
        }
    }

    fn empty() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(raw: &str) -> Decimal {
        raw.parse().expect("valid decimal")
    }

    #[test]
    fn computes_sortino_ratio_from_downside_returns() {
        let nav = vec![d("100"), d("103"), d("101"), d("106"), d("104"), d("110")];
        let bm_nav = vec![d("100"), d("101"), d("100"), d("102"), d("101"), d("103")];

        let metrics = BacktestMetrics::compute(&nav, &bm_nav, d("100"));

        assert!(metrics.sortino_ratio > Decimal::ZERO);
        assert!(metrics.sortino_ratio > metrics.sharpe_ratio);
        assert!(metrics.calmar_ratio > Decimal::ZERO);
        assert!(metrics.max_drawdown_duration_days > 0);
    }
}
