//! 任务80 C类可配项 helper（写死值 env 化，未配置时回退原写死值）。
//!
//! 性能约定：本模块的取值函数均用 [`std::sync::OnceLock`] 缓存——
//! 年化天数在回测/调仓内循环高频取用，避免每笔交易重复读进程环境。

/// 年化交易日基数（Sharpe/年化波动/年化收益外推，A 股惯例 252）。
///
/// quant-backtest metrics 与 quant-api mvo_engine/mvo_weights 共用同一取值口径。
pub fn annualization_days() -> f64 {
    // 任务80: C类特许 → env 化（默认=原写死值）
    static CACHED: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("ANNUALIZATION_DAYS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(252.0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn annualization_days_defaults_to_252() {
        // env 未配置（CI 环境）时恒为 252.0
        assert_eq!(annualization_days(), 252.0);
    }
}
