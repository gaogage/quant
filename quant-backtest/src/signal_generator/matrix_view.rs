//! MatrixView trait：统一三联体函数的数据源访问接口（R9）。
//!
//! 背景：pit_alpha.rs 有 10 组三联体（fn X / X_from_matrix / X_from_stats_matrix），
//! 逻辑同构仅数据源类型不同。本 trait 抽取 6 个公共数据访问方法，让三联体合并为
//! 单一泛型函数，消除 40+ #[allow(dead_code)] 重复。
//!
//! 9 组纯数据源切换三联体可合并；rank_candidates_for_capacity 不可（三版本逻辑不同）。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;

use super::pit_alpha::{
    average_abs_correlation_to_reference, fractional_kelly_weight,
    pearson_correlation, sample_volatility, trailing_returns, ScoreDateReturnRiskMatrix,
    ScoreDateReturnRiskStatsMatrix,
};

/// 数据源统一访问接口。两个矩阵类型 + HashMap 适配器实现它。
///
/// 方法签名对齐矩阵类型的现有方法（score_day + symbol 入参）。
/// HashMap 适配器用冻结的 lookback_days 替代 config.*_lookback_days。
// 批次 1 仅 kelly 用 fractional_kelly_weight；其余方法批次 2/3 接入后启用，暂 allow dead_code。
#[allow(dead_code)]
pub(crate) trait MatrixView {
    /// 某 symbol 在 score_day 的累计收益率。
    fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64>;
    /// 某 symbol 在 score_day 的样本波动率。
    fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64>;
    /// 某 symbol 的分数 Kelly 权重。
    fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64>;
    /// 两 symbol 间的 Pearson 相关系数。
    fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64>;
    /// 某 symbol 相对参考集的平均绝对相关。
    fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64>;
    /// 协方差集中度惩罚（≥1.0）。
    fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64;
}

// Arc<T> 透传：让 Option<&Arc<ScoreDateReturnRiskMatrix>> 也能用 MatrixView。
impl<T: MatrixView + ?Sized> MatrixView for Arc<T> {
    fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        (**self).total_return(score_day, symbol)
    }
    fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        (**self).sample_volatility(score_day, symbol)
    }
    fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        (**self).fractional_kelly_weight(score_day, symbol, fraction)
    }
    fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64> {
        (**self).pearson_correlation(score_day, left, right)
    }
    fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        (**self).average_abs_correlation_to_reference(score_day, symbol, reference_symbols)
    }
    fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        (**self).covariance_concentration_penalty(score_day, symbol, symbols)
    }
}

// ScoreDateReturnRiskMatrix 转发现有同名方法。
impl MatrixView for ScoreDateReturnRiskMatrix {
    fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        ScoreDateReturnRiskMatrix::total_return(self, score_day, symbol)
    }
    fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        ScoreDateReturnRiskMatrix::sample_volatility(self, score_day, symbol)
    }
    fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        ScoreDateReturnRiskMatrix::fractional_kelly_weight(self, score_day, symbol, fraction)
    }
    fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64> {
        ScoreDateReturnRiskMatrix::pearson_correlation(self, score_day, left, right)
    }
    fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        ScoreDateReturnRiskMatrix::average_abs_correlation_to_reference(
            self,
            score_day,
            symbol,
            reference_symbols,
        )
    }
    fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        ScoreDateReturnRiskMatrix::covariance_concentration_penalty(self, score_day, symbol, symbols)
    }
}

// ScoreDateReturnRiskStatsMatrix 转发现有同名方法。
impl MatrixView for ScoreDateReturnRiskStatsMatrix {
    fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        ScoreDateReturnRiskStatsMatrix::total_return(self, score_day, symbol)
    }
    fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        ScoreDateReturnRiskStatsMatrix::sample_volatility(self, score_day, symbol)
    }
    fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        ScoreDateReturnRiskStatsMatrix::fractional_kelly_weight(self, score_day, symbol, fraction)
    }
    fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64> {
        ScoreDateReturnRiskStatsMatrix::pearson_correlation(self, score_day, left, right)
    }
    fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        ScoreDateReturnRiskStatsMatrix::average_abs_correlation_to_reference(
            self,
            score_day,
            symbol,
            reference_symbols,
        )
    }
    fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        ScoreDateReturnRiskStatsMatrix::covariance_concentration_penalty(
            self,
            score_day,
            symbol,
            symbols,
        )
    }
}

/// base 版适配器：把 return_history HashMap 适配为 MatrixView。
///
/// 构造时冻结单一 lookback_days（与矩阵键值映射一致：每个矩阵按特定 lookback 构建）。
/// 适配器方法内部调现有自由函数，用冻结的 lookback 替代 config.*_lookback_days。
pub(crate) struct ReturnHistoryMatrixView<'a> {
    return_history: &'a HashMap<String, Vec<(NaiveDate, f64)>>,
    lookback_days: usize,
}

impl<'a> ReturnHistoryMatrixView<'a> {
    pub(crate) fn new(
        return_history: &'a HashMap<String, Vec<(NaiveDate, f64)>>,
        lookback_days: usize,
    ) -> Self {
        Self {
            return_history,
            lookback_days,
        }
    }
}

impl<'a> MatrixView for ReturnHistoryMatrixView<'a> {
    fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        let returns = trailing_returns(self.return_history, symbol, score_day, self.lookback_days);
        if returns.is_empty() {
            None
        } else {
            Some(returns.iter().fold(1.0_f64, |acc, r| acc * (1.0 + r)))
        }
    }
    fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        let returns = trailing_returns(self.return_history, symbol, score_day, self.lookback_days);
        sample_volatility(&returns)
    }
    fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        let returns = trailing_returns(self.return_history, symbol, score_day, self.lookback_days);
        fractional_kelly_weight(&returns, fraction)
    }
    fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64> {
        let left_returns = trailing_returns(self.return_history, left, score_day, self.lookback_days);
        let right_returns =
            trailing_returns(self.return_history, right, score_day, self.lookback_days);
        pearson_correlation(&left_returns, &right_returns)
    }
    fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        average_abs_correlation_to_reference(
            symbol,
            reference_symbols,
            self.return_history,
            score_day,
            self.lookback_days,
        )
    }
    fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        // base 自由函数收 config（用 risk_budget_lookback_days）；适配器用冻结 lookback 等价。
        // 重新实现以避免依赖 config：等价于自由函数内部逻辑（trailing_returns + pearson 相关矩阵）。
        let own_returns =
            trailing_returns(self.return_history, symbol, score_day, self.lookback_days);
        if own_returns.len() < 3 {
            return 1.0;
        }
        let mut sum_abs = 0.0_f64;
        let mut count = 0usize;
        for other in symbols {
            if other.as_str() == symbol {
                continue;
            }
            let other_returns =
                trailing_returns(self.return_history, other, score_day, self.lookback_days);
            if let Some(corr) = pearson_correlation(&own_returns, &other_returns) {
                sum_abs += corr.abs();
                count += 1;
            }
        }
        if count == 0 {
            1.0
        } else {
            (sum_abs / count as f64).max(0.0) + 1.0
        }
    }
}
