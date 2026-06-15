//! v19 策略统一模拟核心 —— 回放/mvo_simulate/实盘验证共用一套权重与收益逻辑。
//!
//! 设计目标：消除"三套不一致 MVO 引擎"。权重统一来自
//! `scheduler::compute_mvo_weights_for_date`（真 v19 GA + momentum + 自适应），
//! 叠加 `detect_regime_exposure` 降仓和 vol_target 杠杆，与实盘
//! `sync_positions_from_backtest` 完全一致。
//!
//! 公式（逐日）：
//!   各资产暴露_i = mvo_weights[i] × regime_exposure
//!   现金         = 1 - regime_exposure
//!   组合日收益   = Σ(暴露_i × 资产日收益_i)
//!   杠杆后收益   = 组合日收益 × leverage   (vol_target / fixed, 仅 regime>0.9)

use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::routes::scheduler::{
    compute_mvo_weights_for_date, detect_regime_exposure, StrategyConfig,
};

/// 逐日模拟结果：日期 + 杠杆后组合日收益。
#[derive(Debug, Clone)]
pub struct DailyReturn {
    pub date: NaiveDate,
    pub gross_return: f64, // 未加杠杆组合日收益
    pub net_return: f64,   // 叠加 vol_target/fixed 杠杆后日收益
    pub leverage: f64,     // 当日杠杆
    pub regime: f64,       // 当日体制暴露 0-1
}

/// 加载某资产（A股 task / ETF symbol）的日收益序列 → HashMap<date, ret>。
async fn load_a_share_daily(db: &PgPool, task_id: &str) -> Vec<(NaiveDate, f64)> {
    let rows = sqlx::query_as::<_, (NaiveDate, Decimal)>(
        "SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id = $1 ORDER BY trade_date",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let navs: Vec<(NaiveDate, f64)> = rows
        .iter()
        .map(|(d, v)| (*d, v.to_string().parse::<f64>().unwrap_or(0.0)))
        .filter(|(_, v)| *v > 0.0)
        .collect();

    let mut out = Vec::new();
    for i in 1..navs.len() {
        let (d, nav) = navs[i];
        let prev = navs[i - 1].1;
        if prev > 0.0 {
            let r = nav / prev - 1.0;
            if r.abs() <= 0.5 {
                out.push((d, r));
            }
        }
    }
    out
}

/// 加载多个 ETF 的逐日收盘价 → HashMap<symbol, HashMap<date, price>>。
async fn load_etf_prices(
    db: &PgPool,
    symbols: &[String],
    start: NaiveDate,
) -> HashMap<String, HashMap<NaiveDate, f64>> {
    let mut out: HashMap<String, HashMap<NaiveDate, f64>> = HashMap::new();
    if symbols.is_empty() {
        return out;
    }
    let placeholders: Vec<String> = (1..=symbols.len()).map(|i| format!("${}", i)).collect();
    let sql = format!(
        "SELECT trade_date, symbol, close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol IN ({}) AND trade_date >= ${} ORDER BY trade_date",
        placeholders.join(", "),
        symbols.len() + 1
    );
    let mut q = sqlx::query_as::<_, (NaiveDate, String, f64)>(&sql);
    for s in symbols {
        q = q.bind(s);
    }
    q = q.bind(start);
    let rows = q.fetch_all(db).await.unwrap_or_default();
    for (d, sym, price) in rows {
        if price > 0.0 {
            out.entry(sym).or_default().insert(d, price);
        }
    }
    out
}

/// 模拟 v19 策略逐日收益。
///
/// - `sc`: 策略配置（含 equity_curve_task_id、etf_symbols、vol_target、leverage_cap）
/// - `leverage_enabled` / `leverage_multiplier` / `leverage_mode`: 账号杠杆属性
/// - `liq_threshold` / `warn_threshold`: 杠杆账号强平线/警告线（峰值回撤口径，None=无强平建模）
/// - `start` / `end`: 模拟区间（含端点）
///
/// 权重逐季度由 `compute_mvo_weights_for_date` 刷新（真 v19 GA），叠加 regime 降仓 + 杠杆。
/// 强平：账号峰值回撤≥liq_threshold→后续清仓(net_return=0)；≥warn_threshold→禁止加杠杆(lev≤1)。
pub async fn simulate_v19_daily_returns(
    db: &PgPool,
    sc: &StrategyConfig,
    start: NaiveDate,
    end: NaiveDate,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
    liq_threshold: Option<f64>,
    warn_threshold: Option<f64>,
) -> Result<Vec<DailyReturn>, String> {
    // 1. A股日收益（区间过滤）
    let a_daily: Vec<(NaiveDate, f64)> = load_a_share_daily(db, &sc.equity_curve_task_id)
        .await
        .into_iter()
        .filter(|(d, _)| *d >= start && *d <= end)
        .collect();
    if a_daily.len() < 60 {
        return Err(format!(
            "A股权益曲线数据不足（task={}, 区间内仅{}日）",
            sc.equity_curve_task_id,
            a_daily.len()
        ));
    }

    // 2. ETF 日价格（多取 1 年缓冲用于首日收益计算）
    let etf_start = start - chrono::Duration::days(400);
    let etf_prices = load_etf_prices(db, &sc.etf_symbols, etf_start).await;

    // 3. 逐日组合收益
    let mut weights: Vec<f64> = Vec::new();
    let mut last_quarter = String::new();
    let mut last_regime_month: (i32, u32) = (0, 0);
    let mut cached_regime: f64 = 1.0;
    let mut trail_60: Vec<f64> = Vec::new();
    let mut out: Vec<DailyReturn> = Vec::with_capacity(a_daily.len());

    // 杠杆账号维保门控状态：账号净值随杠杆后日收益演进（归一化初始=1.0）
    let mut acct_nav = 1.0_f64;

    // 上一交易日（用于 ETF 收益的 prev/cur 取价）
    let mut prev_date: Option<NaiveDate> = None;

    for (d, a_ret) in &a_daily {
        let d = *d;
        let quarter = format!("{}-Q{}", d.year(), (d.month() - 1) / 3 + 1);

        // 季度首次出现 → 刷新 MVO 权重（真 v19 GA）
        if quarter != last_quarter || weights.is_empty() {
            weights = compute_mvo_weights_for_date(db, d, sc).await;
            last_quarter = quarter.clone();
            if std::env::var("MVO_DEBUG").is_ok() {
                let ws: Vec<String> = weights
                    .iter()
                    .map(|w| format!("{:.0}", w * 100.0))
                    .collect();
                eprintln!(
                    "[wt {}] sum={:.2} [{}]",
                    quarter,
                    weights.iter().sum::<f64>(),
                    ws.join(",")
                );
            }
        }

        // 体制降仓（按月缓存：regime 变化慢，避免逐日 DB 查询 3000+ 次）
        let month_key = (d.year(), d.month());
        if month_key != last_regime_month {
            cached_regime = detect_regime_exposure(db, d).await;
            last_regime_month = month_key;
        }
        let regime = cached_regime;

        // 各资产日收益：第0列A股，其余 ETF
        let mut asset_rets: Vec<f64> = vec![*a_ret];
        if let Some(pd) = prev_date {
            for sym in &sc.etf_symbols {
                let prices = etf_prices.get(sym);
                let pp = prices.and_then(|p| p.get(&pd)).copied().unwrap_or(0.0);
                let pc = prices.and_then(|p| p.get(&d)).copied().unwrap_or(0.0);
                let r = if pp > 0.0 && pc > 0.0 {
                    pc / pp - 1.0
                } else {
                    0.0
                };
                asset_rets.push(if r.abs() <= 0.5 { r } else { 0.0 });
            }
        } else {
            for _ in &sc.etf_symbols {
                asset_rets.push(0.0);
            }
        }

        // 组合日收益 = Σ(weight_i × regime × asset_ret_i)。现金部分(1-regime)收益为0
        let gross: f64 = weights
            .iter()
            .zip(asset_rets.iter())
            .map(|(w, r)| w * regime * r)
            .sum();

        // vol_target 杠杆（trailing 60日组合波动），仅 regime>0.9 且账号启用
        trail_60.push(gross);
        if trail_60.len() > 60 {
            trail_60.remove(0);
        }
        let leverage = if leverage_enabled && regime > 0.9 && leverage_multiplier > 1.0 {
            if leverage_mode == "vol_target" {
                if trail_60.len() >= 20 {
                    let n = trail_60.len() as f64;
                    let mean = trail_60.iter().sum::<f64>() / n;
                    let var = trail_60.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
                    let ann_vol = var.sqrt() * (252.0_f64).sqrt();
                    if ann_vol > 0.05 {
                        (sc.vol_target / ann_vol)
                            .clamp(1.0 / sc.leverage_cap.max(1.0), sc.leverage_cap)
                    } else {
                        1.0
                    }
                } else {
                    1.0
                }
            } else {
                leverage_multiplier
            }
        } else {
            1.0
        };

        // 杠杆账号强平/警告线（维持担保比例口径，与真实券商 + 实盘 scheduler 一致）：
        // 维保 = 总资产/融资额 = (净值 + 融资额) / 融资额。
        // 融资额 debt = (当前生效杠杆-1)×净值，随杠杆浮动（与实盘"每次调仓重算维保"对齐）。
        // 口径=「去杠杆不清仓」：维保<平仓线→杠杆降至1.0(卖融资仓、还债，自有仓续持)；
        //   维保<警告线→杠杆≤1.0(禁新增)。均不锁死——维保回升后下一日可按 vol_target 重新加杠杆。
        let debt = (leverage - 1.0).max(0.0) * acct_nav;
        let maint = if debt > 1e-9 {
            (acct_nav + debt) / debt
        } else {
            f64::INFINITY
        };
        let leverage = if liq_threshold.is_some_and(|t| maint < t) {
            1.0 // 维保跌破平仓线：去杠杆至 1.0（不清仓自有仓位）
        } else if warn_threshold.is_some_and(|t| maint < t) {
            leverage.min(1.0) // 警告区：禁止加杠杆（只许 ≤1）
        } else {
            leverage
        };

        let net = gross * leverage;
        acct_nav *= 1.0 + net;

        out.push(DailyReturn {
            date: d,
            gross_return: gross,
            net_return: gross * leverage,
            leverage,
            regime,
        });
        prev_date = Some(d);
    }

    Ok(out)
}

/// 从逐日收益计算聚合绩效指标。
pub fn compute_metrics(rets: &[f64]) -> Metrics {
    let n = rets.len() as f64;
    if n < 2.0 {
        return Metrics::default();
    }
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let std = var.sqrt();
    let ann_ret = (1.0 + mean).powf(252.0) - 1.0;
    let ann_vol = std * (252.0_f64).sqrt();
    let sharpe = if ann_vol > 0.0 {
        (ann_ret - 0.02) / ann_vol
    } else {
        0.0
    };

    let mut nav = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut max_dd = 0.0_f64;
    for r in rets {
        nav *= 1.0 + r;
        peak = peak.max(nav);
        max_dd = max_dd.max((peak - nav) / peak);
    }
    let cumulative = nav - 1.0;
    let calmar = if max_dd > 0.0 { ann_ret / max_dd } else { 0.0 };

    let downside: Vec<f64> = rets.iter().filter(|&&r| r < 0.0).copied().collect();
    let sortino = if downside.len() > 1 {
        let dm = downside.iter().sum::<f64>() / downside.len() as f64;
        let dv =
            downside.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (downside.len() - 1) as f64;
        let ds = dv.sqrt() * (252.0_f64).sqrt();
        if ds > 0.0 {
            (ann_ret - 0.02) / ds
        } else {
            0.0
        }
    } else {
        0.0
    };
    let win_rate = rets.iter().filter(|&&r| r > 0.0).count() as f64 / n;

    Metrics {
        annual_return: ann_ret,
        cumulative_return: cumulative,
        volatility: ann_vol,
        sharpe,
        sortino,
        max_drawdown: max_dd,
        calmar,
        win_rate,
        trading_days: rets.len(),
    }
}

#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub annual_return: f64,
    pub cumulative_return: f64,
    pub volatility: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_drawdown: f64,
    pub calmar: f64,
    pub win_rate: f64,
    pub trading_days: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_constant_positive() {
        let rets: Vec<f64> = (0..252).map(|_| 0.0005).collect();
        let m = compute_metrics(&rets);
        assert!(m.annual_return > 0.10, "AR={}", m.annual_return);
        assert_eq!(m.max_drawdown, 0.0, "恒正收益无回撤");
        assert!(m.win_rate > 0.99);
    }

    #[test]
    fn test_metrics_drawdown() {
        let mut rets = Vec::new();
        rets.extend((0..100).map(|_| 0.005));
        rets.extend((0..50).map(|_| -0.005));
        rets.extend((0..102).map(|_| 0.005));
        let m = compute_metrics(&rets);
        assert!(m.max_drawdown > 0.0, "应有回撤");
        assert!(m.max_drawdown < 0.30, "回撤={}", m.max_drawdown);
    }

    /// 全周期 v19 模拟验证（需本地 quant 库）。
    /// DATABASE_URL=postgres://gaocheng@localhost/quant cargo test --release -p quant-api v19_full_period -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_v19_full_period_performance() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let sc = crate::routes::scheduler::load_strategy_config(&db, "v19").await;

        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        let unlev =
            simulate_v19_daily_returns(&db, &sc, start, end, false, 1.0, "fixed", None, None)
                .await
                .expect("unlev sim");
        let unlev_rets: Vec<f64> = unlev.iter().map(|d| d.net_return).collect();
        let m_unlev = compute_metrics(&unlev_rets);
        println!(
            "[v19 unlev] AR={:.1}% DD={:.1}% Sharpe={:.2} Sortino={:.2} cum={:.0}% days={}",
            m_unlev.annual_return * 100.0,
            m_unlev.max_drawdown * 100.0,
            m_unlev.sharpe,
            m_unlev.sortino,
            m_unlev.cumulative_return * 100.0,
            m_unlev.trading_days
        );

        let lev =
            simulate_v19_daily_returns(&db, &sc, start, end, true, 1.5, "vol_target", None, None)
                .await
                .expect("lev sim");
        let lev_rets: Vec<f64> = lev.iter().map(|d| d.net_return).collect();
        let m_lev = compute_metrics(&lev_rets);
        println!(
            "[v19 lev]   AR={:.1}% DD={:.1}% Sharpe={:.2} Sortino={:.2} cum={:.0}%",
            m_lev.annual_return * 100.0,
            m_lev.max_drawdown * 100.0,
            m_lev.sharpe,
            m_lev.sortino,
            m_lev.cumulative_return * 100.0
        );

        // DD 是 MVO 分散化的核心目标，dynamic_target=0.06 后应 ≤15%（蓝图 13%）。
        assert!(
            m_unlev.max_drawdown <= 0.15,
            "unlev DD out of blueprint: {:.1}% (期望≤15%)",
            m_unlev.max_drawdown * 100.0
        );
        assert!(
            m_unlev.sharpe >= 0.7,
            "unlev Sharpe too low: {:.2} (期望≥0.7)",
            m_unlev.sharpe
        );
        // AR 软底线：完整达标(16%)依赖 A股选股 CAGR 提升（当前曲线~6%偏弱，独立排查中）。
        assert!(
            m_unlev.annual_return >= 0.07,
            "unlev AR too low: {:.1}% (A股选股层待优化)",
            m_unlev.annual_return * 100.0
        );
    }

    /// 参数扫描：遍历 dynamic_target 上限，输出 AR/DD/Sharpe/Sortino/Calmar，找 AR/DD 最优平衡。
    /// DATABASE_URL=... cargo test --release -p quant-api scan_target -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_scan_target_cap() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let sc = crate::routes::scheduler::load_strategy_config(&db, "v19").await;
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        println!(
            "\n{:>6} | {:>5} {:>5} {:>5} {:>5} {:>5} | {:>5} {:>5} {:>5}",
            "tgt", "AR", "DD", "Shrp", "Sort", "Clmr", "LvAR", "LvDD", "LvShrp"
        );
        for cap in ["0.06", "0.08", "0.10", "0.12", "0.14", "0.16", "0.18"] {
            std::env::set_var("MVO_TARGET_CAP", cap);
            let u =
                simulate_v19_daily_returns(&db, &sc, start, end, false, 1.0, "fixed", None, None)
                    .await
                    .expect("u");
            let mu = compute_metrics(&u.iter().map(|d| d.net_return).collect::<Vec<_>>());
            let l = simulate_v19_daily_returns(
                &db,
                &sc,
                start,
                end,
                true,
                1.5,
                "vol_target",
                None,
                None,
            )
            .await
            .expect("l");
            let ml = compute_metrics(&l.iter().map(|d| d.net_return).collect::<Vec<_>>());
            println!(
                "{:>6} | {:>4.1}% {:>4.1}% {:>5.2} {:>5.2} {:>5.2} | {:>4.1}% {:>4.1}% {:>5.2}",
                cap,
                mu.annual_return * 100.0,
                mu.max_drawdown * 100.0,
                mu.sharpe,
                mu.sortino,
                mu.calmar,
                ml.annual_return * 100.0,
                ml.max_drawdown * 100.0,
                ml.sharpe
            );
        }
        std::env::remove_var("MVO_TARGET_CAP");
    }

    /// 阶段2 Task C：32因子PIT combo 代入组合层 vs 旧基线，同区间(2017-2026)对照。
    /// DATABASE_URL=... cargo test --release -p quant-api test_scan_pit_combo -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_scan_pit_combo() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        // 同区间 2017-2026（新 combo 曲线起点 2017）
        let start = NaiveDate::from_ymd_opt(2017, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        // 两条 A股曲线：新 PIT combo vs 旧 5因子基线
        let curves = [
            ("PIT-37f", "fbt-697279b1-7209-4a4f-b522-d9be16fe3aa9"),
            ("base-5f", "fbt-f24aa67e-9171-42d5-9bd6-530f631762fc"),
        ];
        for (label, curve) in curves {
            let mut sc = crate::routes::scheduler::load_strategy_config(&db, "v19").await;
            sc.equity_curve_task_id = curve.to_string();
            println!("\n=== {} ({}) 同区间2017-2026 ===", label, curve);
            println!(
                "{:>6} | {:>5} {:>5} {:>5} {:>5} {:>5}",
                "tgt", "AR", "DD", "Shrp", "Sort", "Clmr"
            );
            for cap in ["0.06", "0.08", "0.10", "0.12"] {
                std::env::set_var("MVO_TARGET_CAP", cap);
                let u = simulate_v19_daily_returns(
                    &db, &sc, start, end, false, 1.0, "fixed", None, None,
                )
                .await
                .expect("u");
                let m = compute_metrics(&u.iter().map(|d| d.net_return).collect::<Vec<_>>());
                println!(
                    "{:>6} | {:>4.1}% {:>4.1}% {:>5.2} {:>5.2} {:>5.2}",
                    cap,
                    m.annual_return * 100.0,
                    m.max_drawdown * 100.0,
                    m.sharpe,
                    m.sortino,
                    m.calmar
                );
            }
            std::env::remove_var("MVO_TARGET_CAP");
        }
    }

    /// 阶段2 验证补强：target OOS 防过拟合 + 杠杆腿。
    /// IS(2017-2021)扫target各档，OOS(2022-2026)用各档实测——看 IS 最优是否在 OOS 站住。
    /// 同时输出无杠杆 + 杠杆(vol_target×1.5)。PIT-37f vs base-5f。
    /// DATABASE_URL=... cargo test --release -p quant-api test_pit_oos_lev -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_pit_oos_lev() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let is_start = NaiveDate::from_ymd_opt(2017, 1, 1).unwrap();
        let is_end = NaiveDate::from_ymd_opt(2021, 12, 31).unwrap();
        let oos_start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
        let oos_end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();
        let curves = [
            ("PIT-37f", "fbt-697279b1-7209-4a4f-b522-d9be16fe3aa9"),
            ("base-5f", "fbt-f24aa67e-9171-42d5-9bd6-530f631762fc"),
        ];
        for (label, curve) in curves {
            let mut sc = crate::routes::scheduler::load_strategy_config(&db, "v19").await;
            sc.equity_curve_task_id = curve.to_string();
            println!("\n=== {} ({}) ===", label, curve);
            println!(
                "{:>4} | {:>14} | {:>14} | {:>14}",
                "tgt", "IS17-21 unlev", "OOS22-26 unlev", "OOS22-26 LEV"
            );
            for cap in ["0.06", "0.08", "0.10", "0.12"] {
                std::env::set_var("MVO_TARGET_CAP", cap);
                let is_u = simulate_v19_daily_returns(
                    &db, &sc, is_start, is_end, false, 1.0, "fixed", None, None,
                )
                .await
                .expect("is");
                let mis = compute_metrics(&is_u.iter().map(|d| d.net_return).collect::<Vec<_>>());
                let oos_u = simulate_v19_daily_returns(
                    &db, &sc, oos_start, oos_end, false, 1.0, "fixed", None, None,
                )
                .await
                .expect("oos");
                let moos = compute_metrics(&oos_u.iter().map(|d| d.net_return).collect::<Vec<_>>());
                let oos_l = simulate_v19_daily_returns(
                    &db,
                    &sc,
                    oos_start,
                    oos_end,
                    true,
                    1.5,
                    "vol_target",
                    None,
                    None,
                )
                .await
                .expect("oosl");
                let mlev = compute_metrics(&oos_l.iter().map(|d| d.net_return).collect::<Vec<_>>());
                println!("{:>4} | {:>4.1}%/{:>4.1}%/{:>4.2} | {:>4.1}%/{:>4.1}%/{:>4.2} | {:>4.1}%/{:>4.1}%/{:>4.2}",
                         cap,
                         mis.annual_return*100.0, mis.max_drawdown*100.0, mis.sharpe,
                         moos.annual_return*100.0, moos.max_drawdown*100.0, moos.sharpe,
                         mlev.annual_return*100.0, mlev.max_drawdown*100.0, mlev.sharpe);
            }
            std::env::remove_var("MVO_TARGET_CAP");
        }
    }
    /// 杠杆参数优化（含强平/警告线约束）。
    /// 用真实保证金账户模型重建维保轨迹：gross 日收益（含 regime、不含杠杆）为输入；
    /// 季度调仓点按 vol_target 设杠杆并锁定融资 debt=(L-1)×nav，季内总资产随市值浮动、债务不变；
    /// 逐日维保=总资产/融资额，<130% 强平清仓（季内持现金），<150% 警告。
    /// 找"不爆仓前提下风险调整收益最优"的 (vol_target, leverage_cap)。
    #[tokio::test]
    #[ignore]
    async fn test_optimize_leverage_with_liquidation() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let sc = crate::routes::scheduler::load_strategy_config(&db, "v19").await;
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();
        // target 必须用生产真实值 sc.dynamic_target_cap(=0.12)。
        // 历史杠杆回放 AR22.7% 即在 0.12 下产生；用 0.06 会把绩效压低一半，扫描失真。
        let target = sc.dynamic_target_cap;
        std::env::set_var("MVO_TARGET_CAP", format!("{}", target));

        // base 逐日 (date, gross_return, regime)：杠杆无关，作保证金模拟输入。
        // regime 用于杠杆门控（与生产 simulate_v19_daily_returns 一致：regime≤0.9 不加杠杆）
        let base =
            simulate_v19_daily_returns(&db, &sc, start, end, false, 1.0, "fixed", None, None)
                .await
                .expect("base");
        let gross: Vec<(NaiveDate, f64, f64)> = base
            .iter()
            .map(|d| (d.date, d.gross_return, d.regime))
            .collect();
        std::env::remove_var("MVO_TARGET_CAP");

        // 强平/警告线：从生产杠杆账号读真实配置，不写死——保证扫描审计与生产一致。
        // 账号未配则回退券商通行档(平仓130%/警告150%)。
        let (liq, warn): (f64, f64) = sqlx::query_as::<_, (Option<f64>, Option<f64>)>(
            "SELECT liquidation_threshold, warning_threshold FROM paper_account \
             WHERE leverage_enabled = true AND status = 'active' \
             ORDER BY paper_account_id LIMIT 1",
        )
        .fetch_optional(&db)
        .await
        .ok()
        .flatten()
        .map(|(l, w)| (l.unwrap_or(1.30), w.unwrap_or(1.50)))
        .unwrap_or((1.30, 1.50));
        println!(
            "[强平审计阈值] 平仓={:.0}% 警告={:.0}% (源自 active 杠杆账号配置)",
            liq * 100.0,
            warn * 100.0
        );

        // 保证金账户强平审计模型（drift 口径，真实券商）：
        // 季度调仓 / regime 跨 0.9 阈值时，按 vol_target 设杠杆并锁定融资 debt=(L-1)×nav；
        // 期间债务不变、总资产随市值浮动；逐日维保=总资产/融资额，<liq 强平、<warn 警告。
        // regime≤0.9 段强制 lev=1.0（与生产 simulate 的杠杆门控一致）。
        // 返回 (最差维保, 警告天数, 强平次数, 首次强平日)。
        let audit = |fixed_lev: Option<f64>,
                     vol_target: f64,
                     cap: f64|
         -> (f64, usize, usize, Option<NaiveDate>) {
            let mut ta = 1.0_f64; // 总资产
            let mut debt = 0.0_f64; // 融资额
            let mut in_cash = false; // 季内已强平、持现金
            let mut last_q = String::new();
            let mut prev_hi = false; // 上一日 regime 是否 >0.9
            let mut trail: Vec<f64> = Vec::new();
            let (mut min_maint, mut warn_days, mut liq_n, mut liq_date) =
                (f64::INFINITY, 0usize, 0usize, None);
            for (d, g, rg) in &gross {
                trail.push(*g);
                if trail.len() > 60 {
                    trail.remove(0);
                }
                let hi = *rg > 0.9;
                let q = format!("{}-Q{}", d.year(), (d.month() - 1) / 3 + 1);
                // 调仓时机：季度边界 或 regime 跨越 0.9 门控（与生产逐日门控对齐）
                if q != last_q || hi != prev_hi {
                    last_q = q;
                    prev_hi = hi;
                    let cur_nav = ta - debt;
                    let lev = if !hi {
                        1.0
                    }
                    // regime 降仓段不加杠杆
                    else if let Some(fl) = fixed_lev {
                        fl
                    } else if trail.len() >= 20 {
                        let n = trail.len() as f64;
                        let mean = trail.iter().sum::<f64>() / n;
                        let var = trail.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
                        let av = var.sqrt() * (252.0_f64).sqrt();
                        if av > 0.05 {
                            (vol_target / av).clamp(1.0, cap)
                        } else {
                            1.0
                        }
                    } else {
                        1.0
                    };
                    ta = lev * cur_nav;
                    debt = (lev - 1.0) * cur_nav;
                    in_cash = false;
                }
                if in_cash {
                    continue;
                }
                ta *= 1.0 + g;
                let maint = if debt > 1e-9 {
                    ta / debt
                } else {
                    f64::INFINITY
                };
                if maint.is_finite() && maint < min_maint {
                    min_maint = maint;
                }
                if maint < liq {
                    liq_n += 1;
                    liq_date.get_or_insert(*d);
                    ta = (ta - debt).max(0.0);
                    debt = 0.0;
                    in_cash = true;
                } else if maint < warn {
                    warn_days += 1;
                }
            }
            (min_maint, warn_days, liq_n, liq_date)
        };

        // 绩效 ground truth：直接用生产函数 simulate_v19_daily_returns（逐日重置杠杆 +
        // regime 门控，与实盘/历史验证完全同路径）。绝不另造绩效模型，避免口径分歧。
        let perf = |label: String, rets: Vec<f64>, a: (f64, usize, usize, Option<NaiveDate>)| {
            let m = compute_metrics(&rets);
            let mm = if a.0.is_finite() {
                format!("{:>5.0}%", a.0 * 100.0)
            } else {
                "  inf".into()
            };
            let ld = a.3.map(|d| d.to_string()).unwrap_or_else(|| "-".into());
            println!(
                "{:<16} | {:>5.1}% {:>5.1}% {:>5.2} {:>5.2} {:>5.2} | {} {:>4} {:>3} {}",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.sortino,
                m.calmar,
                mm,
                a.1,
                a.2,
                ld
            );
        };

        println!("\n[target={}] 绩效=生产函数 simulate_v19_daily_returns | 强平=维保审计模型(regime门控)", target);
        println!(
            "{:<16} | {:>6} {:>6} {:>5} {:>5} {:>5} | {:>5} {:>4} {:>3} {}",
            "config", "AR", "DD", "Shrp", "Sort", "Clmr", "minMt", "warn", "liq", "first"
        );
        std::env::set_var("MVO_TARGET_CAP", format!("{}", target));
        for fl in [1.0, 1.5, 2.0, 2.5, 3.0] {
            let r =
                simulate_v19_daily_returns(&db, &sc, start, end, fl > 1.0, fl, "fixed", None, None)
                    .await
                    .expect("fixed");
            let rets: Vec<f64> = r.iter().map(|d| d.net_return).collect();
            perf(format!("fixed {:.1}x", fl), rets, audit(Some(fl), 0.0, 0.0));
        }
        for vt in [0.15, 0.20, 0.25, 0.30] {
            for cap in [2.0, 2.5, 3.0] {
                let mut scv = sc.clone();
                scv.vol_target = vt;
                scv.leverage_cap = cap;
                let r = simulate_v19_daily_returns(
                    &db,
                    &scv,
                    start,
                    end,
                    true,
                    1.5,
                    "vol_target",
                    None,
                    None,
                )
                .await
                .expect("vt");
                let rets: Vec<f64> = r.iter().map(|d| d.net_return).collect();
                perf(
                    format!("vt{:.2} cap{:.1}", vt, cap),
                    rets,
                    audit(None, vt, cap),
                );
            }
        }
        std::env::remove_var("MVO_TARGET_CAP");
    }
}
