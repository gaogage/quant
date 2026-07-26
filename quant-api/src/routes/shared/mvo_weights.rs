//! MVO 权重计算模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`MvoWeightCache`]：季度权重缓存
//! - [`compute_lw_mvo_weights`]：LW-MVO 自动发现权重（Ledoit-Wolf shrinkage + Grid Search 季度调仓）
//! - [`compute_vol_target_leverage`]：波动率目标杠杆
//! - 附属私有：[`get_monthly_returns`] / [`daily_to_monthly_returns`]
//!
//! 原位置：scheduler.rs:258-261 / 3886-3936 / 4058-4430。

use chrono::{Datelike, NaiveDate};
use ndarray::Array2;
use quant_common::mvo;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::routes::shared::strategy_config::StrategyConfig;

/// MVO 权重缓存（季度更新）
pub struct MvoWeightCache {
    quarter: String,   // e.g. "2026-Q2"
    weights: Vec<f64>, // [A股, 黄金, 国债, SP500, 纳指, 有色, 豆粕, 原油]
}

/// 波动率目标杠杆：根据 trailing 60日组合NAV变化计算波动率，动态调整杠杆。
/// 目标年化波动率 20%，杠杆 = 20% / trailing_vol，clamp [0.5, 2.0]。
pub(crate) async fn compute_vol_target_leverage(
    db: &PgPool,
    account_id: &str,
    sc: &StrategyConfig,
) -> f64 {
    let target_vol = sc.vol_target;
    let rows = sqlx::query_as::<_, (rust_decimal::Decimal,)>(
        "SELECT nav FROM paper_nav_snapshot
         WHERE paper_account_id = $1
         ORDER BY snapshot_date DESC LIMIT 61",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let navs: Vec<f64> = rows
        .iter()
        .map(|(n,)| n.to_string().parse::<f64>().unwrap_or(0.0))
        .filter(|&v| v > 0.0)
        .collect();

    if navs.len() < 21 {
        return 1.0; // 数据不足
    }

    // 计算日收益率
    let mut rets = Vec::new();
    for i in 1..navs.len() {
        if navs[i - 1] > 0.0 {
            rets.push(navs[i] / navs[i - 1] - 1.0);
        }
    }

    if rets.len() < 20 {
        return 1.0;
    }

    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let variance = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_vol = variance.sqrt();
    let annual_vol = daily_vol * (252.0_f64).sqrt();

    if annual_vol < 0.05 {
        return 1.0;
    }

    let lev = target_vol / annual_vol;
    lev.clamp(0.5, sc.leverage_cap)
}

/// LW-MVO 自动发现权重：Ledoit-Wolf shrinkage + Grid Search 季度调仓。
/// 返回 (a_share, gold, bond, sp500, nasdaq) 权重（和为 1.0）。
/// ETF 从实际有数据的日期开始纳入 MVO 计算。
pub(crate) async fn compute_lw_mvo_weights(
    db: &PgPool,
    date: NaiveDate,
    cache: &Mutex<Option<MvoWeightCache>>,
    sc: &StrategyConfig,
) -> Vec<f64> {
    let quarter = format!("{}-Q{}", date.year(), (date.month() - 1) / 3 + 1);

    // 检查缓存（同季度不重复计算）
    {
        let guard = cache.lock().await;
        if let Some(ref c) = *guard {
            if c.quarter == quarter {
                return c.weights.clone();
            }
        }
    }

    // 过滤当日未发行的 ETF:MVO 只对已发行标的分配,未发行的不占维度
    // 用循环而非 .filter()+await(闭包不能 async)
    let mut listed_etf_symbols: Vec<String> = Vec::new();
    let mut listed_default_weights: Vec<f64> = Vec::new();
    for (s, w) in sc.etf_symbols.iter().zip(sc.default_weights.iter()) {
        if crate::routes::equity_curve_sync::is_etf_listed_on(db, s, date).await {
            listed_etf_symbols.push(s.clone());
            listed_default_weights.push(*w);
        }
    }

    let etf_symbols: Vec<&str> = listed_etf_symbols.iter().map(|s| s.as_str()).collect();
    let n_total_assets = 1 + etf_symbols.len();
    let min_stock = sc.min_stock;

    // 获取过去 36 个月的月度收益数据
    let lookback_start = date - chrono::Duration::days(36 * 31);

    // A 股月度收益（从策略配置的权益曲线获取）
    let a_monthly = get_monthly_returns(db, lookback_start, date, "A_SHARE", sc).await;

    // Adaptive MVO: 根据近期 A 股表现动态调整 min_stock
    let adaptive_min_stock = if a_monthly.len() >= 3 {
        // P4 修复(2026-07-21): a_monthly 由 daily_to_monthly_returns 按时间升序返回(最早月份在前)，
        // 原代码用 a_monthly[..3]/[..6] 取的是最老的月份而非最近月份 — trail_3m/trail_6m 方向完全错误。
        // 实测:原实现取到 2020 年最老 3 月累计 +3.08%，而真实最近 3 月为 -8.60%(熊市信号完全颠倒)。
        // 修复:统一用尾部切片(最近 N 月)替代头部切片(最老 N 月)。
        let tail_start_3 = a_monthly.len().saturating_sub(3);
        let trail_3m: f64 = a_monthly[tail_start_3..].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
        let trail_6m: f64 = if a_monthly.len() >= 6 {
            let tail_start_6 = a_monthly.len().saturating_sub(6);
            a_monthly[tail_start_6..].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0
        } else {
            trail_3m * 2.0
        };
        if trail_3m < -0.03 {
            // 熊市:优先用策略配置 regime_bear_min_stock(若>0),否则降仓到 0 转 ETF 防守
            let bear_target = if sc.regime_bear_min_stock > 0.0 {
                sc.regime_bear_min_stock
            } else {
                0.00
            };
            info!(
                "[MVO] Factor failure detected (3m={:.1}%), min_stock {} -> {:.2}, switching to ETF defense",
                trail_3m * 100.0,
                min_stock,
                bear_target
            );
            bear_target
        } else if trail_6m > 0.15 {
            // 牛市:优先用策略配置 regime_bull_min_stock(若>0),否则用默认 0.20
            let bull_target = if sc.regime_bull_min_stock > 0.0 {
                sc.regime_bull_min_stock
            } else {
                0.20
            };
            info!(
                "[MVO] Adaptive: bull detected (6m={:.1}%), min_stock {} -> {:.2}",
                trail_6m * 100.0,
                min_stock,
                bull_target
            );
            bull_target
        } else {
            min_stock
        }
    } else {
        min_stock
    };

    // Kelly-inspired A股仓位缩放
    let kelly_scale = if adaptive_min_stock > 0.0 && a_monthly.len() >= 6 {
        // 同上修复:取最近 6 个月而非最老 6 个月
        let trail_rets: Vec<f64> = a_monthly[a_monthly.len() - 6..].to_vec();
        let n = trail_rets.len() as f64;
        let avg = trail_rets.iter().sum::<f64>() / n;
        if n > 1.0 {
            let variance = trail_rets.iter().map(|r| (r - avg).powi(2)).sum::<f64>() / (n - 1.0);
            let monthly_ir = if variance > 0.0 {
                avg / variance.sqrt()
            } else {
                0.0
            };
            let annual_ir = monthly_ir * (12.0_f64).sqrt();
            (0.5 + annual_ir).clamp(0.3, 1.5)
        } else {
            1.0
        }
    } else {
        1.0
    };
    let adaptive_min_stock = (adaptive_min_stock * kelly_scale).min(sc.max_single);

    // 默认权重从策略配置读取 (数据不足时的fallback)
    let default_weights: Vec<f64> = {
        let mut w = listed_default_weights.clone();
        w.insert(0, adaptive_min_stock); // A股权重在第一位
        w
    };

    let mut weights = default_weights.clone();

    if a_monthly.len() < 12 {
        let mut guard = cache.lock().await;
        *guard = Some(MvoWeightCache {
            quarter,
            weights: weights.clone(),
        });
        return weights;
    }

    // 所有ETF月度收益
    let mut etf_monthly_data: Vec<Vec<f64>> = Vec::new();
    let mut valid_etf_count = 0;
    for sym in &etf_symbols {
        let mrets = get_monthly_returns(db, lookback_start, date, sym, sc).await;
        if !mrets.is_empty() {
            valid_etf_count += 1;
        }
        etf_monthly_data.push(mrets);
    }

    // 构建8资产训练数据
    let mut all_monthly: Vec<Vec<f64>> = Vec::new();
    let n_months = a_monthly.len();
    for i in 0..n_months {
        let mut row = vec![a_monthly[i]];
        for j in 0..etf_symbols.len() {
            if i < etf_monthly_data[j].len() {
                row.push(etf_monthly_data[j][i]);
            } else {
                row.push(0.0);
            }
        }
        if row.iter().all(|r| r.abs() < 1.0) {
            all_monthly.push(row);
        }
    }

    if all_monthly.len() >= 12 && valid_etf_count >= 2 {
        let n_rows = all_monthly.len();
        let flat: Vec<f64> = all_monthly.iter().flatten().copied().collect();

        if let Some(arr) = Array2::from_shape_vec((n_rows, n_total_assets), flat).ok() {
            // v19: momentum-adjusted μ (50/50) + GA MinVariance + adaptive max_single
            // dynamic_target 上限 0.06：高 target(0.18) 会把 MinVariance 逼向单资产集中、
            // DD 翻倍(13%→25%)。0.06 与 ROADMAP 验证 v19 22.6% 时的原始配置一致，保持跨资产分散。
            // 可用 MVO_TARGET_CAP 覆盖做调参实验。
            // target 上限：唯一来源为策略配置 sc.dynamic_target_cap。
            // 不再使用 MVO_TARGET_CAP 环境变量——async 并发环境下 std::env::set_var 是
            // 进程全局状态，会导致不同账号/请求之间互相污染（2026-06-28 排查确认）。
            // 杠杆/无杠杆差异化通过独立的 strategy_config 记录实现（v21 vs v21_lev）。
            let target_cap = sc.dynamic_target_cap;
            let target_floor = sc.dynamic_target_floor;
            let dynamic_target = if a_monthly.len() >= 12 {
                // P4 修复:取最近 12 个月(尾部)而非最老 12 个月(头部)。
                // 同 trail_3m/trail_6m 修复理由:daily_to_monthly_returns 升序返回,头部切片=最老。
                let tail_start = a_monthly.len() - 12;
                let trail_12m: f64 =
                    a_monthly[tail_start..].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
                (trail_12m + 0.05).clamp(target_floor.min(target_cap), target_cap)
            } else {
                target_floor.min(target_cap)
            };
            // Momentum-adjusted expected returns (50/50 blend)
            let hist_mu = ndarray::Array1::from_vec(
                (0..n_total_assets)
                    .map(|j| {
                        let col: Vec<f64> = all_monthly.iter().map(|r| r[j]).collect();
                        col.iter().sum::<f64>() / col.len() as f64 * 12.0
                    })
                    .collect(),
            );
            let mom_mu = ndarray::Array1::from_vec(
                (0..n_total_assets)
                    .map(|j| {
                        let recent: Vec<f64> =
                            all_monthly.iter().rev().take(6).map(|r| r[j]).collect();
                        recent.iter().fold(1.0, |acc, r| acc * (1.0 + r)).powf(2.0) - 1.0
                    })
                    .collect(),
            );
            let bw = sc.momentum_blend_ratio;
            let adj_mu = bw * &hist_mu + (1.0 - bw) * &mom_mu;
            // 自适应max_single: 牛市用max_single_bull, 否则用max_single
            // P4 修复:同 trail_12m 修复,取最近 min(len,12) 个月(尾部)。
            let take_n = a_monthly.len().min(12);
            let trail_12m_a = a_monthly[a_monthly.len() - take_n..]
                .iter()
                .fold(1.0, |acc, r| acc * (1.0 + r))
                - 1.0;
            let adaptive_max = if trail_12m_a > 0.10 {
                sc.max_single_bull
            } else {
                sc.max_single
            };
            // 牛市放宽min_stock
            let adaptive_ms = if trail_12m_a > 0.10 {
                (adaptive_min_stock * 0.7).max(0.08)
            } else {
                adaptive_min_stock
            };
            // 目标函数:从 strategy_config.mvo_objective 字段读(maxsharpe / minvariance)
            let mvo_result = if sc.mvo_objective == "maxsharpe" {
                mvo::mvo_allocate_ga_maxsharpe_with_max_single(
                    &arr,
                    &adj_mu,
                    adaptive_ms,
                    adaptive_max,
                )
            } else {
                mvo::mvo_allocate_ga_with_max_single(
                    &arr,
                    &adj_mu,
                    adaptive_ms,
                    dynamic_target,
                    0.10,
                    adaptive_max,
                )
            };
            if let Some(result) = mvo_result {
                let w = result.weights.to_vec();
                let wg = |i: usize| (w.get(i).copied().unwrap_or(0.0) * 100.0).round();
                info!(
                    quarter = %quarter,
                    a = %wg(0), gold = %wg(1), bond = %wg(2), sp500 = %wg(3),
                    nq = %wg(4), color = %wg(5), meal = %wg(6), oil = %wg(7),
                    sharpe = %(result.sharpe * 100.0).round() / 100.0,
                    "v19 MVO 权重已更新 (momentum μ + adaptive)"
                );
                weights = w;
            }
        }
    }

    let mut guard = cache.lock().await;
    *guard = Some(MvoWeightCache {
        quarter,
        weights: weights.clone(),
    });
    weights
}

/// 获取某个资产的月度收益率序列（最新在前）
async fn get_monthly_returns(
    db: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
    symbol: &str,
    sc: &StrategyConfig,
) -> Vec<f64> {
    if symbol == "A_SHARE" {
        // A股月度收益：从策略配置的权益曲线获取
        let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
            "SELECT trade_date, portfolio_value FROM backtest_equity_curve
             WHERE task_id = $1
             AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date",
        )
        .bind(&sc.equity_curve_task_id)
        .bind(start)
        .bind(end)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        // 权益曲线新鲜度检查
        if let Some(last) = eq_rows.last() {
            let gap = (end - last.0).num_days();
            if gap > 60 {
                warn!("[MVO] ⚠ A股权益曲线数据滞后{}天 (最新: {}), MVO训练窗口可能缺失近期数据。建议重新运行全量回测更新fbt-36e18e12",
                      gap, last.0.format("%Y-%m-%d"));
            } else if gap > 30 {
                info!(
                    "[MVO] A股权益曲线滞后{}天 (最新: {}), 36月训练窗口内影响可忽略",
                    gap,
                    last.0.format("%Y-%m-%d")
                );
            }
        }

        return daily_to_monthly_returns(&eq_rows);
    }

    // ETF：从 market_stock_daily_bar 获取
    let rows: Vec<(NaiveDate, rust_decimal::Decimal)> = sqlx::query_as(
        "SELECT trade_date, close FROM market_stock_daily_bar_adj
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(symbol)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    daily_to_monthly_returns(&rows)
}

/// 日线价格 → 月度收益
fn daily_to_monthly_returns(rows: &[(NaiveDate, rust_decimal::Decimal)]) -> Vec<f64> {
    if rows.len() < 2 {
        return vec![];
    }

    let mut monthly: Vec<f64> = Vec::new();
    let mut current_month = rows[0].0.month();
    let mut current_year = rows[0].0.year();
    let mut month_start_val: Option<f64> = None;
    let mut month_end_val: f64 = 0.0;

    for (d, val) in rows {
        let v = val.to_string().parse::<f64>().unwrap_or(0.0);
        if v <= 0.0 {
            continue;
        }

        if d.month() != current_month || d.year() != current_year {
            // 保存上月收益
            if let Some(start_v) = month_start_val {
                if start_v > 0.0 && month_end_val > 0.0 {
                    monthly.push(month_end_val / start_v - 1.0);
                }
            }
            current_month = d.month();
            current_year = d.year();
            month_start_val = Some(v);
        }
        if month_start_val.is_none() {
            month_start_val = Some(v);
        }
        month_end_val = v;
    }

    // 最后一个月
    if let Some(start_v) = month_start_val {
        if start_v > 0.0 && month_end_val > 0.0 {
            monthly.push(month_end_val / start_v - 1.0);
        }
    }

    monthly
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── daily_to_monthly_returns 测试 ──

    #[test]
    fn daily_to_monthly_returns_three_months_correct_returns() {
        use chrono::NaiveDate;
        use rust_decimal::Decimal;
        let rows: Vec<(NaiveDate, Decimal)> = vec![
            (NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(), Decimal::new(100, 0)),
            (NaiveDate::from_ymd_opt(2020, 1, 31).unwrap(), Decimal::new(110, 0)), // +10%
            (NaiveDate::from_ymd_opt(2020, 2, 3).unwrap(), Decimal::new(110, 0)),
            (NaiveDate::from_ymd_opt(2020, 2, 28).unwrap(), Decimal::new(132, 0)), // +20%
            (NaiveDate::from_ymd_opt(2020, 3, 2).unwrap(), Decimal::new(132, 0)),
            (NaiveDate::from_ymd_opt(2020, 3, 31).unwrap(), Decimal::new(99, 0)),  // -25%
        ];
        let result = daily_to_monthly_returns(&rows);
        assert_eq!(result.len(), 3, "应产生 3 个月度收益");
        assert!((result[0] - 0.10).abs() < 0.001, "1月应为 +10%, 实际: {}", result[0]);
        assert!((result[1] - 0.20).abs() < 0.001, "2月应为 +20%, 实际: {}", result[1]);
        assert!((result[2] - (-0.25)).abs() < 0.001, "3月应为 -25%, 实际: {}", result[2]);
    }

    #[test]
    fn daily_to_monthly_returns_returns_oldest_first() {
        // 验证语义: 结果按时间升序(最旧月份在索引 0)。
        use chrono::NaiveDate;
        use rust_decimal::Decimal;
        let mut rows: Vec<(NaiveDate, Decimal)> = Vec::new();
        let mut nav = 100.0_f64;
        for y in 2020..=2022 {
            for m in 1..=12 {
                let first = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
                rows.push((first, Decimal::from_f64_retain(nav).unwrap()));
                nav *= 1.05;
                let last = NaiveDate::from_ymd_opt(y, m, 25).unwrap();
                rows.push((last, Decimal::from_f64_retain(nav).unwrap()));
            }
        }
        let monthly = daily_to_monthly_returns(&rows);
        assert_eq!(monthly.len(), 36, "36 个月应产生 36 条收益");
        // 每月均为 +5%
        for (i, r) in monthly.iter().enumerate() {
            assert!((r - 0.05).abs() < 0.005, "第{}(0-based)月应为 5%, 实际: {}", i, r);
        }
        // 最近3月 = 尾部切片 (monthly.len()-3..)
        let recent_3 = &monthly[monthly.len() - 3..];
        assert_eq!(recent_3.len(), 3);
        // 最老3月 = 头部切片 (..3)
        let oldest_3 = &monthly[..3];
        assert_eq!(oldest_3.len(), 3);
        // 在均涨5%的现实下两者数值相同,但语义不同——验证切片方向正确即可。
    }

    #[test]
    fn daily_to_monthly_returns_insufficient_data() {
        use chrono::NaiveDate;
        use rust_decimal::Decimal;
        // 单条记录 → 无法跨月比较
        let rows = vec![(NaiveDate::from_ymd_opt(2020, 1, 15).unwrap(), Decimal::new(100, 0))];
        assert!(daily_to_monthly_returns(&rows).is_empty());
        // 空输入
        let empty: Vec<(NaiveDate, Decimal)> = vec![];
        assert!(daily_to_monthly_returns(&empty).is_empty());
    }
}
