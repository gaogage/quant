//! 数据完整性验证 — 所有回测/模拟回放的强制前置检查。
//!
//! 依据: docs/standards/data-completeness-before-evaluation.md
//! 核心原则: 数据不齐 = 绩效评估无意义。缺失或异常数据直接终止，不产出虚假结果。

use chrono::NaiveDate;
use std::collections::HashMap;
use tracing::{info, warn};

/// Validate A-share equity curve for outliers and flat segments.
/// Returns Err on fatal issues (outliers, eval-period flat streaks > 5 days).
/// Warns on training-period flat streaks.
pub fn validate_equity_curve(
    a_nav: &[(NaiveDate, f64)],
    eval_start: NaiveDate,
) -> Result<(), String> {
    if a_nav.len() < 252 {
        return Err(format!(
            "权益曲线仅{}个数据点, 需要至少1年(252个交易日)",
            a_nav.len()
        ));
    }

    let mut prev_v: Option<(NaiveDate, f64)> = None;
    let mut flat_streak = 0u32;
    let mut max_eval_flat = 0u32;
    let mut max_train_flat = 0u32;

    for (d, v) in a_nav {
        if let Some((pd, pv)) = prev_v {
            let daily_ret = (v - pv) / pv;
            // 异常跳变检测
            if daily_ret.abs() > 0.5 {
                return Err(format!(
                    "数据异常: 权益曲线 {} → {} 单日收益 {:.1}% 超过50%阈值, 数据有拼接错误",
                    pd.format("%Y-%m-%d"),
                    d.format("%Y-%m-%d"),
                    daily_ret * 100.0
                ));
            }
            // 平坦段检测
            if (v - pv).abs() < 1e-9 {
                flat_streak += 1;
                if *d >= eval_start {
                    max_eval_flat = max_eval_flat.max(flat_streak);
                } else {
                    max_train_flat = max_train_flat.max(flat_streak);
                }
            } else {
                flat_streak = 0;
            }
        }
        prev_v = Some((*d, *v));
    }

    if max_eval_flat > 5 {
        return Err(format!(
            "数据异常: 评估期内权益曲线存在连续{}个交易日净值不变, 请先修复数据源",
            max_eval_flat
        ));
    }
    if max_train_flat > 100 {
        warn!(
            "训练窗口存在较长平坦期(最大{}天), 可能是策略未运行的空白期",
            max_train_flat
        );
    }

    info!(
        "权益曲线验证通过: {}点, 评估期最大平坦={}, 训练期最大平坦={}",
        a_nav.len(),
        max_eval_flat,
        max_train_flat
    );
    Ok(())
}

/// Check equity curve and ETF price data coverage.
pub fn validate_data_coverage(
    a_nav: &[(NaiveDate, f64)],
    etf_prices: &HashMap<String, HashMap<NaiveDate, f64>>,
    etf_symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), String> {
    let a_start = a_nav
        .first()
        .map(|(d, _)| *d)
        .unwrap_or(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
    let a_end = a_nav
        .last()
        .map(|(d, _)| *d)
        .unwrap_or(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());

    if a_start > start_date + chrono::Duration::days(30) {
        return Err(format!(
            "数据覆盖不足: 权益曲线始于 {}, 无法覆盖评估期起始 {}",
            a_start.format("%Y-%m-%d"),
            start_date.format("%Y-%m-%d")
        ));
    }
    if a_end < end_date - chrono::Duration::days(30) {
        return Err(format!(
            "数据覆盖不足: 权益曲线止于 {}, 但评估要求到 {}",
            a_end.format("%Y-%m-%d"),
            end_date.format("%Y-%m-%d")
        ));
    }

    for sym in etf_symbols {
        match etf_prices.get(sym.as_str()) {
            Some(prices) => {
                let max_d = prices
                    .keys()
                    .max()
                    .copied()
                    .unwrap_or(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
                if max_d < end_date - chrono::Duration::days(30) {
                    return Err(format!(
                        "数据覆盖不足: ETF {} 数据止于 {}, 但评估要求到 {}",
                        sym,
                        max_d.format("%Y-%m-%d"),
                        end_date.format("%Y-%m-%d")
                    ));
                }
                let min_d = prices
                    .keys()
                    .min()
                    .copied()
                    .unwrap_or(NaiveDate::from_ymd_opt(2099, 1, 1).unwrap());
                if min_d > start_date {
                    warn!(
                        "ETF {} 数据从 {} 开始, 训练窗口无此资产",
                        sym,
                        min_d.format("%Y-%m-%d")
                    );
                }
            }
            None => return Err(format!("数据缺失: ETF {} 无价格数据", sym)),
        }
    }

    info!(
        "数据覆盖检查通过: 权益曲线{}-{}, {}个ETF",
        a_start.format("%Y-%m-%d"),
        a_end.format("%Y-%m-%d"),
        etf_symbols.len()
    );
    Ok(())
}

/// Check monthly training data has at least 12 months.
pub fn validate_training_data(
    monthly_rets: &[(String, NaiveDate, Vec<f64>)],
    etf_count: usize,
    lookback: usize,
) -> Result<(), String> {
    if monthly_rets.len() < 12 {
        return Err(format!(
            "训练数据不足: 仅有{}个月度收益, 需要至少12个月",
            monthly_rets.len()
        ));
    }

    // 检查是否有足够的月度数据在第一个有效调仓季之前
    let first_valid_q = monthly_rets.iter().position(|(m, _, rets)| {
        let parts: Vec<&str> = m.split('-').collect();
        let is_q = parts.len() >= 2 && matches!(parts[1], "01" | "04" | "07" | "10");
        let etf_valid = rets.iter().skip(1).filter(|&&x| x != 0.0).count() >= 2;
        is_q && etf_valid
    });

    match first_valid_q {
        Some(idx) if idx < lookback => {
            warn!(
                "第一个可用季度月({})仅有{}个月前置数据, MVO将在之后启动",
                monthly_rets[idx].0, idx
            );
        }
        Some(idx) => {
            info!(
                "训练数据充足: 第一个可用季度月={} ({}个月前置)",
                monthly_rets[idx].0, idx
            );
        }
        None => {
            warn!("未找到有效季度调仓月, 检查ETF数据是否充足");
        }
    }

    info!(
        "训练数据校验: {}个月度收益, {}个ETF",
        monthly_rets.len(),
        etf_count
    );
    Ok(())
}
