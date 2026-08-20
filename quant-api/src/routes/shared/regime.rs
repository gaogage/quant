//! 体制识别模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`detect_regime_exposure`]：Trailing 12-month CSI300 return，深熊降仓
//! - [`detect_regime_exposure_cached`]：内存缓存版本（mvo_simulate 性能优化）
//!
//! 原位置：scheduler.rs:3975-4052。

use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::debug;

/// 体制检测：Trailing 12-month CSI300 return。
/// 深熊（12月跌 >10%）：仓位降至 60%，规避系统性风险。
/// 其余时间：满仓，让 LW-MVO 自主调配。
pub async fn detect_regime_exposure(
    db: &PgPool,
    date: NaiveDate,
    deep_bear_threshold: f64,
    deep_bear_exposure: f64,
) -> f64 {
    // 真正的 trailing-12m 回报 = 最新收盘 / 252日前收盘 - 1。
    // （旧实现用 MAX/MIN-1，永远为正 → 降仓从不触发，2015股灾/2018熊市全程满仓）
    let trail: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (
            SELECT trade_date, close::double precision AS close FROM market_index_daily_bar
            WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252
        )
        SELECT (SELECT close FROM dates ORDER BY trade_date DESC LIMIT 1)
             / NULLIF((SELECT close FROM dates ORDER BY trade_date ASC LIMIT 1), 0) - 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .and_then(|(v,)| v);

    match trail {
        Some(t) if t < deep_bear_threshold => {
            debug!(
                "[Regime] DEEP BEAR: 12m return={:.1}%, exposure={:.0}%",
                t * 100.0, deep_bear_exposure * 100.0
            );
            deep_bear_exposure
        }
        _ => 1.00, // 满仓
    }
}

/// detect_regime_exposure 的内存缓存版本(mvo_simulate 性能优化)。
///
/// 从预加载的 csi300_map(000300.SH 日线 HashMap<NaiveDate, f64>)取最近 252 个交易日
/// 算 trailing 12m return,逻辑同 detect_regime_exposure 但不查 DB。
///
/// mvo_simulate 循环前一次性预加载整个区间,循环内每天调此函数 O(1) 查内存,
/// 省 ~N 次 DB 往返(N = 模拟天数)。
pub fn detect_regime_exposure_cached(
    csi300_map: &std::collections::HashMap<NaiveDate, f64>,
    date: NaiveDate,
    deep_bear_threshold: f64,
    deep_bear_exposure: f64,
) -> f64 {
    // 取不晚于 date 的最近 252 个交易日收盘价(对齐 SQL 的 trade_date <= $1 ORDER BY DESC LIMIT 252)
    let mut recent: Vec<(NaiveDate, f64)> = csi300_map
        .iter()
        .filter(|(d, _)| **d <= date)
        .map(|(d, c)| (*d, *c))
        .collect();
    if recent.len() < 2 {
        return 1.00; // 数据不足,默认满仓(对齐 SQL 不足时返回 1.00)
    }
    recent.sort_by(|a, b| b.0.cmp(&a.0)); // trade_date DESC
    recent.truncate(252);
    // 最新收盘 / 252日前收盘 - 1(对齐 SQL:DESC LIMIT 1 是最新,ASC LIMIT 1 是最早)
    let latest = recent.first().map(|(_, c)| *c).unwrap_or(0.0);
    let earliest = recent.last().map(|(_, c)| *c).unwrap_or(0.0);
    let trail = if earliest > 0.0 {
        Some(latest / earliest - 1.0)
    } else {
        None
    };
    match trail {
        Some(t) if t < deep_bear_threshold => {
            debug!(
                "[Regime] DEEP BEAR: 12m return={:.1}%, exposure={:.0}%",
                t * 100.0, deep_bear_exposure * 100.0
            );
            deep_bear_exposure
        }
        _ => 1.00, // 满仓
    }
}

/// bear_window_guard_v2 regime policy（从 quant-backtest capacity_budget.rs:1877 移植）。
///
/// 126 天 lookback，3 信号（return + volatility + drawdown），5 档分级：
/// - HighVolatility（vol≥0.28）→ 0.58
/// - Bear（ret≤-0.03 OR dd≥0.14）→ 0.72
/// - Bull/Sideways/Mixed → 1.00（满仓）
///
/// 比 trailing-12m 更前瞻：短窗口（126 vs 252）+ drawdown 早触发 + 分级减仓（非一刀切 0.6）。
/// 阈值来源：capacity_budget.rs:1917-1929 quality_bear_window_guard_v2。
/// 数据源同 trailing-12m（CSI300 close），口径一致，无后复权差异。
/// bwgv2 阈值配置（阶段1.2 配置化，可经 strategy_config 调优，WFA 网格搜索）。
/// 默认值对齐 capacity_budget.rs:1917-1929 quality_bear_window_guard_v2。
#[derive(Debug, Clone)]
pub struct Bwgv2Config {
    pub lookback_days: usize,
    pub high_vol_threshold: f64,
    pub bear_return_threshold: f64,
    pub bear_drawdown_threshold: f64,
}
impl Default for Bwgv2Config {
    fn default() -> Self {
        Self {
            lookback_days: 126,
            high_vol_threshold: 0.28,
            bear_return_threshold: -0.03,
            bear_drawdown_threshold: 0.14,
        }
    }
}

pub fn detect_regime_exposure_bwgv2(
    csi300_map: &std::collections::HashMap<NaiveDate, f64>,
    date: NaiveDate,
    cfg: &Bwgv2Config,
) -> f64 {
    // 取不晚于 date 的最近 cfg.lookback_days 个交易日收盘价（升序）
    let mut recent: Vec<(NaiveDate, f64)> = csi300_map
        .iter()
        .filter(|(d, _)| **d <= date)
        .map(|(d, c)| (*d, *c))
        .collect();
    if recent.len() < 20 {
        return 1.00; // min_observations=20，不足默认满仓
    }
    recent.sort_by(|a, b| a.0.cmp(&b.0)); // 升序（旧→新）
    let start = recent.len().saturating_sub(cfg.lookback_days);
    let window = &recent[start..];
    // 日收益序列
    let rets: Vec<f64> = window
        .windows(2)
        .map(|w| if w[0].1 != 0.0 { w[1].1 / w[0].1 - 1.0 } else { 0.0 })
        .collect();
    if rets.len() < 20 {
        return 1.00;
    }
    // 三信号
    let total_return: f64 = rets.iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    let variance: f64 = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rets.len() as f64;
    let volatility = variance.sqrt() * (252.0_f64).sqrt();
    // 路径最大回撤
    let mut nav = 1.0;
    let mut peak = 1.0;
    let mut max_dd = 0.0;
    for r in &rets {
        nav *= 1.0 + r;
        if nav > peak {
            peak = nav;
        }
        if peak > 0.0 {
            let dd = 1.0 - nav / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    // 优先级短路判定（对齐 pit_alpha.rs:1025-1039 + capacity_budget:1917-1929）
    let exposure = if volatility >= cfg.high_vol_threshold {
        0.58 // HighVolatility
    } else if total_return <= cfg.bear_return_threshold || max_dd >= cfg.bear_drawdown_threshold {
        0.72 // Bear
    } else {
        1.00 // Bull/Sideways/Mixed 都满仓
    };
    debug!(
        "[Regime] BWGV2: 6m ret={:.1}% vol={:.1}% dd={:.1}% → exposure={:.0}%",
        total_return * 100.0,
        volatility * 100.0,
        max_dd * 100.0,
        exposure * 100.0
    );
    exposure
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── detect_regime_exposure_cached 测试 ──

    #[test]
    fn detect_regime_exposure_cached_deep_bear_triggers() {
        use std::collections::HashMap;
        use chrono::NaiveDate;
        let mut map = HashMap::new();
        let start = NaiveDate::from_ymd_opt(2022, 1, 3).unwrap();
        for i in 0..300_i64 {
            let d = start + chrono::Duration::days(i);
            // 从 4000 线性跌到 3000 → trailing 12m ≈ -25%
            let price = 4000.0 - (i as f64) * (1000.0 / 300.0);
            map.insert(d, price);
        }
        let latest = *map.keys().max().unwrap();
        let exposure = detect_regime_exposure_cached(&map, latest, -0.10, 0.60);
        assert!((exposure - 0.60).abs() < 0.001,
            "deep bear 应触发降仓 0.60, 实际: {}", exposure);
    }

    #[test]
    fn detect_regime_exposure_cached_bull_keeps_full() {
        use std::collections::HashMap;
        use chrono::NaiveDate;
        let mut map = HashMap::new();
        let start = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        for i in 0..300_i64 {
            let d = start + chrono::Duration::days(i);
            let price = 3000.0 + (i as f64) * (1000.0 / 300.0); // 涨
            map.insert(d, price);
        }
        let latest = *map.keys().max().unwrap();
        let exposure = detect_regime_exposure_cached(&map, latest, -0.10, 0.60);
        assert!((exposure - 1.00).abs() < 0.001,
            "牛市应满仓 1.00, 实际: {}", exposure);
    }

    #[test]
    fn detect_regime_exposure_cached_insufficient_data_defaults_full() {
        use std::collections::HashMap;
        use chrono::NaiveDate;
        let mut map = HashMap::new();
        map.insert(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(), 3500.0);
        let exposure = detect_regime_exposure_cached(
            &map,
            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            -0.10,
            0.60,
        );
        assert!((exposure - 1.00).abs() < 0.001, "数据不足应默认满仓, 实际: {}", exposure);
    }
}
