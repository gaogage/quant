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
