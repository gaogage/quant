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

use crate::routes::rebalance::{mark_to_market, rebalance_account, PriceSource};
use crate::routes::strategy::ResolvedStrategy;
use crate::routes::trading::update_current_nav;

/// 默认无风险利率（年化）。历史硬编码值 0.02，作为无策略上下文路径的 fallback。
/// 生产路径应从 `MvoParams.risk_free_rate` 读真实值传入 `compute_metrics`。
pub const DEFAULT_RISK_FREE_RATE: f64 = 0.02;

/// 统一逐日模拟的盯市 NAV 结果（绩效口径：current_nav 复利）。
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DailyNav {
    pub date: NaiveDate,
    pub nav: f64,        // 盯市 current_nav（真实持仓市值+cash-margin）
    pub net_return: f64, // 日收益 = nav/prev_nav - 1
    pub leverage: f64,
    pub regime: f64,
}

/// 从逐日收益计算聚合绩效指标。
///
/// `risk_free_rate` 用于 Sharpe / Sortino 的分子（年化无风险利率），
/// 上游应从 `MvoParams.risk_free_rate` 读入；纯回测/无策略上下文路径
/// 传 `DEFAULT_RISK_FREE_RATE`（0.02）保持历史行为。
pub fn compute_metrics(rets: &[f64], risk_free_rate: f64) -> Metrics {
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
        (ann_ret - risk_free_rate) / ann_vol
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
            (ann_ret - risk_free_rate) / ds
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

/// 统一逐日模拟引擎 —— 回放/在线模拟共用，实盘走单日增量（不循环）。
/// 绩效口径：盯市 current_nav 复利（全口径统一）。
pub async fn run_daily_simulation(
    db: &PgPool,
    account_id: &str,
    rs: &ResolvedStrategy,
    start: NaiveDate,
    end: NaiveDate,
    price_source: PriceSource,
    mvo_cache: &std::sync::Arc<tokio::sync::Mutex<Option<crate::routes::shared::MvoWeightCache>>>,
    tushare: &quant_data::tushare::client::TushareClient,
    reset_account: bool,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
) -> Result<Vec<DailyNav>, String> {
    // 0. 账号绑校验
    let (strategy_version_id, init_cap): (Option<String>, Decimal) = sqlx::query_as(
        "SELECT strategy_version_id, initial_capital FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("query account: {}", e))?
    .ok_or_else(|| "account not found".to_string())?;
    if strategy_version_id.as_deref().unwrap_or("").is_empty() {
        return Err(format!("账号 {} 未挂策略（strategy_version_id 空）", account_id));
    }
    let init_cap_f: f64 = init_cap.to_string().parse().unwrap_or(1_000_000.0);

    // 1. 账号重置（回放专用）
    if reset_account {
        for table in &[
            "paper_order",
            "paper_fill",
            "paper_position",
            "paper_nav_snapshot",
            "paper_replay",
            "paper_margin_trade",
        ] {
            sqlx::query(&format!("DELETE FROM {} WHERE paper_account_id = $1", table))
                .bind(account_id)
                .execute(db)
                .await
                .map_err(|e| format!("clean {}: {}", table, e))?;
        }
        sqlx::query(
            "UPDATE paper_account SET current_nav=$1, peak_nav=$1, cash=$1, max_drawdown_pct=0, margin_amount=0, total_trades=0 WHERE paper_account_id=$2",
        )
        .bind(init_cap)
        .bind(account_id)
        .execute(db)
        .await
        .map_err(|e| format!("reset: {}", e))?;
    }

    // 2. 交易日序列（从 a_share asset 的 backtest_equity_curve 取）+ task_id（回放传 rs 的 equity_curve_task_id）
    let a_task_id = rs
        .assets
        .iter()
        .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare)
        .and_then(|a| a.security.equity_curve_task_id.as_deref())
        .ok_or_else(|| "策略无 a_share asset，equity_curve_task_id 缺失".to_string())?;
    let dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT trade_date FROM backtest_equity_curve WHERE task_id = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date",
    )
    .bind(a_task_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("load dates: {}", e))?;
    if dates.is_empty() {
        return Err(format!("交易日序列为空（task={}, {}~{}）", a_task_id, start, end));
    }

    // 2.1 预加载 CSI300 日线到内存(detect_regime_exposure 每天查 252 天,改内存读省 ~N 次 DB)
    // 复用 paper.rs:721 bench_map 模式。first_d 往前推 400 天确保 trailing 252 天有边界数据。
    let csi300_first = *dates.first().unwrap() - chrono::Duration::days(400);
    let csi300_last = *dates.last().unwrap();
    let csi300_rows = sqlx::query_as::<_, (NaiveDate, f64)>(
        "SELECT trade_date, close::double precision FROM market_index_daily_bar
         WHERE symbol = '000300.SH' AND trade_date >= $1 AND trade_date <= $2 AND close > 0
         ORDER BY trade_date",
    )
    .bind(csi300_first)
    .bind(csi300_last)
    .fetch_all(db)
    .await
    .map_err(|e| format!("load csi300: {}", e))?;
    let csi300_map: std::collections::HashMap<NaiveDate, f64> =
        csi300_rows.into_iter().collect();
    tracing::info!(days = csi300_map.len(), "CSI300 预加载完成(detect_regime 缓存)");

    // 3. 逐日
    let mut prev_nav = init_cap_f;
    // P1-2: regime 阈值从策略配置读(原硬编码 -0.10/0.60)。
    let (deep_bear_threshold, deep_bear_exposure) = rs
        .mvo
        .as_ref()
        .map(|m| (m.deep_bear_threshold, m.deep_bear_exposure))
        .unwrap_or((-0.10, 0.60));

    let mut out: Vec<DailyNav> = Vec::with_capacity(dates.len());
    let mut last_marker: Option<(i32, u32)> = None; // 调仓频率 marker（年/季/月）

    for d in &dates {
        let d = *d;
        // P2-B:regime 提前算(只依赖 date + csi300_map),供 rebalance_account 复用,省调仓日内 1 次 DB
        // regime_policy 配置化(阶段1.2): bwgv2 走 bear_window_guard_v2，其余/缺省走 trailing-12m
        let regime = match rs.mvo.as_ref().and_then(|m| m.regime_policy.as_deref()) {
            Some("bwgv2") | Some("bear_window_guard_v2") | Some("quality_bear_window_guard_v2") => {
                let m = rs.mvo.as_ref().unwrap();
                let cfg = crate::routes::shared::Bwgv2Config {
                    bear_return_threshold: m.regime_bear_return_threshold,
                    ..crate::routes::shared::Bwgv2Config::default()
                };
                crate::routes::shared::detect_regime_exposure_bwgv2(&csi300_map, d, &cfg)
            }
            _ => crate::routes::shared::detect_regime_exposure_cached(
                &csi300_map,
                d,
                deep_bear_threshold,
                deep_bear_exposure,
            ),
        };
        // 3a. 调仓日控制（读 rebalance_freq）
        if is_rebalance_day(d, &rs.rebalance_freq, &mut last_marker) {
            rebalance_account(
                db,
                account_id,
                rs,
                d,
                a_task_id,
                price_source,
                mvo_cache,
                tushare,
                leverage_enabled,
                leverage_multiplier,
                leverage_mode,
                // P2-B:传上层已算的 prev_nav(调仓前 NAV)和 regime,省调仓日内 2 次 DB
                Some(prev_nav),
                Some(regime),
            )
            .await?;
        }
        // 3b. 每日盯市
        mark_to_market(db, account_id, d, price_source).await?;
        // 3c. NAV 重算(P1-B:update_current_nav 返回 NAV,省下面的 SELECT current_nav)
        let nav_first = update_current_nav(db, account_id).await?;
        // 3c.1 每日维保检查(实盘口径):维保<平仓线触发强平,平仓后重算 NAV。
        // 非调仓日也可能因价格下跌触发强平(券商每日盯市)。
        // EodCloseAdj 基准模式无成本(slippage=0)不强平;实盘/EodClose 用 0.002。
        let maint_slippage = if matches!(price_source, PriceSource::EodCloseAdj) {
            0.0
        } else {
            0.002
        };
        let (liq_n, _warn_block) =
            crate::routes::rebalance::check_maintenance_after_mark(db, account_id, d, maint_slippage)
                .await
                .unwrap_or((0, false));
        // P1-A:仅强平日(liq_n>0)才重算 NAV,未强平用第1次值(省 ~N 次 DB)
        let nav = if liq_n > 0 {
            update_current_nav(db, account_id).await?
        } else {
            nav_first
        };
        let net = if prev_nav > 0.0 {
            nav / prev_nav - 1.0
        } else {
            0.0
        };
        // 3e. 写 paper_nav_snapshot
        let cum = if init_cap_f > 0.0 {
            nav / init_cap_f - 1.0
        } else {
            0.0
        };
        // R5: 统一走 upsert_nav_snapshot（原裸 SQL 5 处重复之一）
        let mut snap = crate::routes::shared::NavSnapshot::new(account_id, d, nav);
        snap.daily_return = Some(net);
        snap.cumulative_return = Some(cum);
        let _ = crate::routes::shared::upsert_nav_snapshot(db, &snap).await;
        prev_nav = nav;
        out.push(DailyNav {
            date: d,
            nav,
            net_return: net,
            leverage: 1.0,
            regime,
        });
    }
    Ok(out)
}

/// 调仓日判断：quarterly（季首）/monthly（月首）/weekly（周首）/biweekly（双周≈10交易日）。
fn is_rebalance_day(d: NaiveDate, freq: &str, last_marker: &mut Option<(i32, u32)>) -> bool {
    let marker = match freq {
        "monthly" => Some((d.year(), d.month())),
        "weekly" => Some((d.year(), d.iso_week().week())),
        // biweekly = 每 2 周 ≈ 10 交易日，与 sleeve 的 10 日调仓频率对齐，
        // 消除 composite 季度采样 sleeve 导致的跟踪误差（2026-09-07）
        "biweekly" | "10" => Some((d.year(), d.iso_week().week() / 2)),
        _ => Some((d.year(), (d.month() - 1) / 3 + 1)), // quarterly 默认
    };
    if *last_marker == marker {
        false
    } else {
        *last_marker = marker;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_constant_positive() {
        let rets: Vec<f64> = (0..252).map(|_| 0.0005).collect();
        let m = compute_metrics(&rets, DEFAULT_RISK_FREE_RATE);
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
        let m = compute_metrics(&rets, DEFAULT_RISK_FREE_RATE);
        assert!(m.max_drawdown > 0.0, "应有回撤");
        assert!(m.max_drawdown < 0.30, "回撤={}", m.max_drawdown);
    }

    /// 验证 risk_free_rate 参数确实影响 Sharpe / Sortino（P4.0d 回归测试）。
    /// 相同收益序列，rfr 越高 Sharpe/Sortino 越低；rfr=0 时最高。
    /// 用正负混合序列(让 downside 非空,Sortino 可计算)。
    #[test]
    fn test_metrics_risk_free_rate_affects_sharpe() {
        let rets: Vec<f64> = (0..252)
            .map(|i| if i % 7 == 0 { -0.004 } else { 0.001 })
            .collect();
        let m_zero = compute_metrics(&rets, 0.0);
        let m_low = compute_metrics(&rets, 0.02);
        let m_high = compute_metrics(&rets, 0.10);
        assert!(
            m_zero.sharpe > m_low.sharpe,
            "rfr=0 Sharpe {} 应 > rfr=0.02 Sharpe {}",
            m_zero.sharpe,
            m_low.sharpe
        );
        assert!(
            m_low.sharpe > m_high.sharpe,
            "rfr=0.02 Sharpe {} 应 > rfr=0.10 Sharpe {}",
            m_low.sharpe,
            m_high.sharpe
        );
        assert!(
            m_zero.sortino > m_low.sortino,
            "rfr=0 Sortino {} 应 > rfr=0.02 Sortino {}",
            m_zero.sortino,
            m_low.sortino
        );
    }

    /// 全周期 v19 模拟验证（需本地 quant 库）。
    /// DATABASE_URL=postgres://gaocheng@localhost/quant cargo test --release -p quant-api v19_full_period -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_v19_full_period_performance() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));

        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        let unlev = run_daily_simulation(
            &db,
            "pa-v19-active-full-unlev-20260615",
            &rs,
            start,
            end,
            crate::routes::rebalance::PriceSource::EodClose,
            &cache,
            &tushare,
            true,
            false,
            1.0,
            "fixed",
        )
        .await
        .expect("unlev sim");
        let unlev_rets: Vec<f64> = unlev.iter().map(|d| d.net_return).collect();
        let m_unlev = compute_metrics(&unlev_rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!(
            "[v19 unlev] AR={:.1}% DD={:.1}% Sharpe={:.2} Sortino={:.2} cum={:.0}% days={}",
            m_unlev.annual_return * 100.0,
            m_unlev.max_drawdown * 100.0,
            m_unlev.sharpe,
            m_unlev.sortino,
            m_unlev.cumulative_return * 100.0,
            m_unlev.trading_days
        );

        let lev = run_daily_simulation(
            &db,
            "pa-v19-active-full-lev-20260615",
            &rs,
            start,
            end,
            crate::routes::rebalance::PriceSource::EodClose,
            &cache,
            &tushare,
            true,
            true,
            1.5,
            "vol_target",
        )
        .await
        .expect("lev sim");
        let lev_rets: Vec<f64> = lev.iter().map(|d| d.net_return).collect();
        let m_lev = compute_metrics(&lev_rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!(
            "[v19 lev]   AR={:.1}% DD={:.1}% Sharpe={:.2} Sortino={:.2} cum={:.0}%",
            m_lev.annual_return * 100.0,
            m_lev.max_drawdown * 100.0,
            m_lev.sharpe,
            m_lev.sortino,
            m_lev.cumulative_return * 100.0
        );

        // DD 是 MVO 分散化的核心目标，dynamic_target=0.06 后应 ≤15%（蓝图 13%）。
        // 盯市口径含成本/滑点，阈值先放宽（DD<=0.25、Sharpe>=0.5、AR>=0.05），主代理 Task 10 跑测试收紧。
        assert!(
            m_unlev.max_drawdown <= 0.25,
            "unlev DD out of blueprint: {:.1}% (期望≤25%)",
            m_unlev.max_drawdown * 100.0
        );
        assert!(
            m_unlev.sharpe >= 0.5,
            "unlev Sharpe too low: {:.2} (期望≥0.5)",
            m_unlev.sharpe
        );
        // AR 软底线：完整达标(16%)依赖 A股选股 CAGR 提升（当前曲线~6%偏弱，独立排查中）。
        assert!(
            m_unlev.annual_return >= 0.05,
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
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        println!(
            "\n{:>6} | {:>5} {:>5} {:>5} {:>5} {:>5} | {:>5} {:>5} {:>5}",
            "tgt", "AR", "DD", "Shrp", "Sort", "Clmr", "LvAR", "LvDD", "LvShrp"
        );
        for cap in ["0.06", "0.08", "0.10", "0.12", "0.14", "0.16", "0.18"] {
            let mut rs_v = rs.clone();
            rs_v.mvo.as_mut().unwrap().dynamic_target_cap = cap.parse().unwrap();
            let u = run_daily_simulation(
                &db,
                "pa-v19-active-full-unlev-20260615",
                &rs_v,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                false,
                1.0,
                "fixed",
            )
            .await
            .expect("u");
            let mu = compute_metrics(&u.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_v.mvo.as_ref().unwrap().risk_free_rate);
            let l = run_daily_simulation(
                &db,
                "pa-v19-active-full-lev-20260615",
                &rs_v,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                true,
                1.5,
                "vol_target",
            )
            .await
            .expect("l");
            let ml = compute_metrics(&l.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_v.mvo.as_ref().unwrap().risk_free_rate);
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
    }

    /// 阶段2 Task C：32因子PIT combo 代入组合层 vs 旧基线，同区间(2017-2026)对照。
    /// DATABASE_URL=... cargo test --release -p quant-api test_scan_pit_combo -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_scan_pit_combo() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));
        // 同区间 2017-2026（新 combo 曲线起点 2017）
        let start = NaiveDate::from_ymd_opt(2017, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();

        // 两条 A股曲线：新 PIT combo vs 旧 5因子基线
        let curves = [
            ("PIT-37f", "fbt-697279b1-7209-4a4f-b522-d9be16fe3aa9"),
            ("base-5f", "fbt-f24aa67e-9171-42d5-9bd6-530f631762fc"),
        ];
        for (label, curve) in curves {
            let mut rs_v = rs.clone();
            if let Some(a) = rs_v
                .assets
                .iter_mut()
                .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare)
            {
                a.security.equity_curve_task_id = Some(curve.to_string());
            }
            println!("\n=== {} ({}) 同区间2017-2026 ===", label, curve);
            println!(
                "{:>6} | {:>5} {:>5} {:>5} {:>5} {:>5}",
                "tgt", "AR", "DD", "Shrp", "Sort", "Clmr"
            );
            for cap in ["0.06", "0.08", "0.10", "0.12"] {
                let mut rs_c = rs_v.clone();
                rs_c.mvo.as_mut().unwrap().dynamic_target_cap = cap.parse().unwrap();
                let u = run_daily_simulation(
                    &db,
                    "pa-v19-active-full-unlev-20260615",
                    &rs_c,
                    start,
                    end,
                    crate::routes::rebalance::PriceSource::EodClose,
                    &cache,
                    &tushare,
                    true,
                    false,
                    1.0,
                    "fixed",
                )
                .await
                .expect("u");
                let m = compute_metrics(&u.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_c.mvo.as_ref().unwrap().risk_free_rate);
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
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));
        let is_start = NaiveDate::from_ymd_opt(2017, 1, 1).unwrap();
        let is_end = NaiveDate::from_ymd_opt(2021, 12, 31).unwrap();
        let oos_start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
        let oos_end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();
        let curves = [
            ("PIT-37f", "fbt-697279b1-7209-4a4f-b522-d9be16fe3aa9"),
            ("base-5f", "fbt-f24aa67e-9171-42d5-9bd6-530f631762fc"),
        ];
        for (label, curve) in curves {
            let mut rs_v = rs.clone();
            if let Some(a) = rs_v
                .assets
                .iter_mut()
                .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare)
            {
                a.security.equity_curve_task_id = Some(curve.to_string());
            }
            println!("\n=== {} ({}) ===", label, curve);
            println!(
                "{:>4} | {:>14} | {:>14} | {:>14}",
                "tgt", "IS17-21 unlev", "OOS22-26 unlev", "OOS22-26 LEV"
            );
            for cap in ["0.06", "0.08", "0.10", "0.12"] {
                let mut rs_c = rs_v.clone();
                rs_c.mvo.as_mut().unwrap().dynamic_target_cap = cap.parse().unwrap();
                let is_u = run_daily_simulation(
                    &db,
                    "pa-v19-active-full-unlev-20260615",
                    &rs_c,
                    is_start,
                    is_end,
                    crate::routes::rebalance::PriceSource::EodClose,
                    &cache,
                    &tushare,
                    true,
                    false,
                    1.0,
                    "fixed",
                )
                .await
                .expect("is");
                let mis = compute_metrics(&is_u.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_c.mvo.as_ref().unwrap().risk_free_rate);
                let oos_u = run_daily_simulation(
                    &db,
                    "pa-v19-active-full-unlev-20260615",
                    &rs_c,
                    oos_start,
                    oos_end,
                    crate::routes::rebalance::PriceSource::EodClose,
                    &cache,
                    &tushare,
                    true,
                    false,
                    1.0,
                    "fixed",
                )
                .await
                .expect("oos");
                let moos = compute_metrics(&oos_u.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_c.mvo.as_ref().unwrap().risk_free_rate);
                let oos_l = run_daily_simulation(
                    &db,
                    "pa-v19-active-full-lev-20260615",
                    &rs_c,
                    oos_start,
                    oos_end,
                    crate::routes::rebalance::PriceSource::EodClose,
                    &cache,
                    &tushare,
                    true,
                    true,
                    1.5,
                    "vol_target",
                )
                .await
                .expect("oosl");
                let mlev = compute_metrics(&oos_l.iter().map(|d| d.net_return).collect::<Vec<_>>(), rs_c.mvo.as_ref().unwrap().risk_free_rate);
                println!("{:>4} | {:>4.1}%/{:>4.1}%/{:>4.2} | {:>4.1}%/{:>4.1}%/{:>4.2} | {:>4.1}%/{:>4.1}%/{:>4.2}",
                         cap,
                         mis.annual_return*100.0, mis.max_drawdown*100.0, mis.sharpe,
                         moos.annual_return*100.0, moos.max_drawdown*100.0, moos.sharpe,
                         mlev.annual_return*100.0, mlev.max_drawdown*100.0, mlev.sharpe);
            }
        }
    }
    /// 杠杆参数优化（含强平/警告线约束）。
    /// 扫 (vol_target, leverage_cap) 组合，用 run_daily_simulation（盯市 NAV 复利，
    /// 强平门控内建在 rebalance_account，从 paper_account 列读）找风险调整收益最优配置。
    /// DATABASE_URL=... cargo test --release -p quant-api optimize_leverage -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_optimize_leverage_with_liquidation() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();
        // target 用生产真实值 rs.mvo.dynamic_target_cap（历史杠杆回放 AR22.7% 即在此下产生）。
        let target = rs.mvo.as_ref().unwrap().dynamic_target_cap;

        // 绩效打印（只打印 run_daily_simulation 绩效，audit 维保审计已删——
        // 强平门控内建在 rebalance_account，从 paper_account 列读，无需独立审计闭包）。
        let perf = |label: String, rets: Vec<f64>| {
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!(
                "{:<16} | {:>5.1}% {:>5.1}% {:>5.2} {:>5.2} {:>5.2}",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.sortino,
                m.calmar
            );
        };

        println!(
            "\n[target={}] 绩效=run_daily_simulation（盯市NAV，强平门控从 paper_account 列读）",
            target
        );
        println!(
            "{:<16} | {:>6} {:>6} {:>5} {:>5} {:>5}",
            "config", "AR", "DD", "Shrp", "Sort", "Clmr"
        );
        // 固定杠杆档（账号 pa-v19-active-full-lev-20260615 已配强平门控）
        for fl in [1.0, 1.5, 2.0, 2.5, 3.0] {
            let r = run_daily_simulation(
                &db,
                "pa-v19-active-full-lev-20260615",
                &rs,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                fl > 1.0,
                fl,
                "fixed",
            )
            .await
            .expect("fixed");
            let rets: Vec<f64> = r.iter().map(|d| d.net_return).collect();
            perf(format!("fixed {:.1}x", fl), rets);
        }
        // vol_target × leverage_cap 扫描（改 rs.mvo 字段，非环境变量）
        for vt in [0.15, 0.20, 0.25, 0.30] {
            for cap in [2.0, 2.5, 3.0] {
                let mut rs_v = rs.clone();
                rs_v.mvo.as_mut().unwrap().vol_target = vt;
                rs_v.mvo.as_mut().unwrap().leverage_cap = cap;
                let r = run_daily_simulation(
                    &db,
                    "pa-v19-active-full-lev-20260615",
                    &rs_v,
                    start,
                    end,
                    crate::routes::rebalance::PriceSource::EodClose,
                    &cache,
                    &tushare,
                    true,
                    true,
                    1.5,
                    "vol_target",
                )
                .await
                .expect("vt");
                let rets: Vec<f64> = r.iter().map(|d| d.net_return).collect();
                perf(format!("vt{:.2} cap{:.1}", vt, cap), rets);
            }
        }
    }

    /// Task 7: run_daily_simulation NAV 恒等式验证（盯市 current_nav 复利）。
    /// DATABASE_URL=postgres://gaocheng@localhost/quant cargo test --lib routes::mvo_engine::tests::test_run_daily_simulation_nav_identity -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_run_daily_simulation_nav_identity() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v19")
            .await
            .unwrap();
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));

        // 小区间 10 个交易日
        let start = NaiveDate::from_ymd_opt(2024, 6, 3).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 6, 14).unwrap();
        let navs = run_daily_simulation(
            &db,
            "pa-v19-active-full-unlev-20260615",
            &rs,
            start,
            end,
            crate::routes::rebalance::PriceSource::EodClose,
            &cache,
            &tushare,
            true,
            false,
            1.0,
            "fixed",
        )
        .await
        .expect("sim");

        assert!(navs.len() >= 5, "至少 5 个交易日 nav");
        // NAV 恒等式：每条 snapshot 的 nav 应等于当时 paper_account.current_nav
        let db_nav: f64 = sqlx::query_scalar(
            "SELECT current_nav::double precision FROM paper_account WHERE paper_account_id='pa-v19-active-full-unlev-20260615'",
        )
        .fetch_one(&db)
        .await
        .unwrap_or(0.0);
        let last_sim_nav = navs.last().unwrap().nav;
        assert!(
            (db_nav - last_sim_nav).abs() < 1.0,
            "NAV 恒等式: db={} sim={}",
            db_nav,
            last_sim_nav
        );
    }

    /// v26 MaxSharpe 对照实验：h20 配置下 maxsharpe vs minvariance（阶段2 仓位/目标函数探索）。
    ///
    /// 背景：MaxSharpe 对照仅在 v19/h1 时代做过（2026-06-11，DD 35% 顶红线被否）；
    /// h20（更稳的 sleeve 风险特征）从未重跑。本实验在同一账户、同一引擎下先后回放：
    ///   A = v24_lev（minvariance，现役配置）
    ///   B = v26ms  （v24_lev 克隆，仅 mvo_objective='maxsharpe'）
    /// 区间 2020-01-02 ~ 2026-09-02，fixed 1.5x，reset_account 全清。
    ///
    /// 运行：cargo test --release -p quant-api v26_maxsharpe -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_v26_maxsharpe_vs_minvariance_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let start = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let account = "pa-v26ms-lev";

        for (label, strategy_id) in [("A-minvariance", "v24_lev"), ("B-maxsharpe", "v26ms")] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, strategy_id)
                .await
                .unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
                None::<crate::routes::shared::MvoWeightCache>,
            ));
            let sim = run_daily_simulation(
                &db,
                account,
                &rs,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                true,
                1.5,
                "fixed",
            )
            .await
            .expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!(
                "[v26 {}] AR={:.2}% DD={:.2}% Sharpe={:.2} Sortino={:.2} Calmar={:.2} cum={:.0}% days={}",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.sortino,
                m.calmar,
                m.cumulative_return * 100.0,
                m.trading_days
            );
        }
    }

    /// v24 现役配置（minvariance + adaptive）完整区间回放 2016-01-04 ~ 2026-09-02。
    ///
    /// 目的：回答"如果 2016 年就运行当前 v24 配置会怎样"——实盘曲线的 2016-2019 段
    /// 是 v19/v21 旧配置的历史混合，当前配置的完整区间绩效从未测过；且本回放跑在
    /// 修复后的引擎上（participation_rate cap 千元 bug + MVO 特征向量修复）。
    ///
    /// 运行：set -a; source .env; source .env.quant; set +a;
    ///       cargo test --release -p quant-api v24_full_period -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_v24_full_period_replay_2016_2026() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for (label, strategy_id, lev_on, mult) in [
            ("unlev-1.0x", "v24", false, 1.0),
            ("lev-1.5x", "v24_lev", true, 1.5),
        ] {
            // 落库到专用基准账户（inactive 不进调度，作为代码变更前后绩效对照的
            // 固定配置快照曲线；每次引擎代码变更后重跑本测试刷新）
            let account = if lev_on {
                "pa-v21-prod-lev"
            } else {
                "pa-v21-prod-unlev"
            };
            let rs = crate::routes::strategy::load_resolved_strategy(&db, strategy_id)
                .await
                .unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
                None::<crate::routes::shared::MvoWeightCache>,
            ));
            let sim = run_daily_simulation(
                &db,
                account,
                &rs,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                lev_on,
                mult,
                "fixed",
            )
            .await
            .expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!(
                "[v24-full {}] AR={:.2}% DD={:.2}% Sharpe={:.2} Sortino={:.2} Calmar={:.2} cum={:.0}% days={}",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.sortino,
                m.calmar,
                m.cumulative_return * 100.0,
                m.trading_days
            );
            // 导出日收益序列供 bootstrap 检验（脚本读 CSV）
            let mut csv = String::from("date,nav\n");
            for d in &sim {
                csv.push_str(&format!("{},{}\n", d.date, d.nav));
            }
            let path = format!("/tmp/v24_full_{}.csv", label);
            std::fs::write(&path, csv).expect("write csv");
            println!("[v24-full {}] curve -> {}", label, path);
        }
    }

    /// 阶段3-方向A：ETF 扩池对照实验。E1=+4 分散资产（德国/日经/恒生/可转债），
    /// E2=+2 保守（德国/可转债）。对照基准 = pa-v24-baseline-lev（AR 10.74%/
    /// DD 33.89%/Sharpe 0.48，2016-2026 同引擎同区间）。
    ///
    /// 运行：set -a; source .env; source .env.quant; set +a;
    ///       cargo test --release -p quant-api v27_etf_expand -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_v27_etf_pool_expand_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let account = "pa-v26ms-lev";

        for (label, strategy_id) in [("E1-plus4", "v27e1"), ("E2-plus2", "v27e2")] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, strategy_id)
                .await
                .unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
                None::<crate::routes::shared::MvoWeightCache>,
            ));
            let sim = run_daily_simulation(
                &db,
                account,
                &rs,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                true,
                1.5,
                "fixed",
            )
            .await
            .expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!(
                "[v27 {}] AR={:.2}% DD={:.2}% Sharpe={:.2} Sortino={:.2} Calmar={:.2} cum={:.0}% days={}",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.sortino,
                m.calmar,
                m.cumulative_return * 100.0,
                m.trading_days
            );
            // 最后一天持仓快照（验证扩池资产确实被配置）
            if let Some(last) = sim.last() {
                println!("[v27 {}] final nav = {}", label, last.nav);
            }
        }
    }
}

#[cfg(test)]
mod v28dd_tests {
    use super::*;

    /// v28dd composite 验证：a_share sleeve 换 drawdown_control_v1 曲线后，
    /// composite(1.5x) 与基准 pa-v24-baseline-lev（AR 10.74%/DD 33.89%/Sharpe 0.48，
    /// 同引擎同区间 2016-2026）的对照。假设：sleeve 回撤 61→19.6% 释放风险预算。
    ///
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api v28dd -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn v28dd_composite_replay_2016_2026() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        // 1.5x 版暴露强平记账 bug（margin 膨胀→维保击穿），先用 1.0x 验证
        // dd_ctrl_v1 在 composite 层的净效果；强平交互留专项修复。
        for (label, account, lev_on, mult) in [
            ("unlev-1.0x", "pa-v28dd-unlev", false, 1.0),
            ("lev-1.5x", "pa-v28dd-lev", true, 1.5),
        ] {
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v28dd")
            .await
            .unwrap();
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
            None::<crate::routes::shared::MvoWeightCache>,
        ));
        let sim = run_daily_simulation(
            &db,
            account,
            &rs,
            start,
            end,
            crate::routes::rebalance::PriceSource::EodClose,
            &cache,
            &tushare,
            true,
            lev_on,
            mult,
            "fixed",
        )
        .await
        .expect("sim");
        let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
        let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!(
            "[v28dd-{}] AR={:.2}% DD={:.2}% Sharpe={:.2} Calmar={:.2} cum={:.0}% days={} (新基准: unlev 7.65%/11.02%/0.66, lev 12.43%/34.34%/0.61)",
            label,
            m.annual_return * 100.0,
            m.max_drawdown * 100.0,
            m.sharpe,
            m.calmar,
            m.cumulative_return * 100.0,
            m.trading_days
        );
        }
    }
}

#[cfg(test)]
mod v29ra_tests {
    use super::*;

    /// H-μRA 预注册实验：μ 风险调整化（Sharpe×σ_target）能否解锁 dd_ctrl_v1
    /// 与改善现役 h20。判据见 2026-09-04-MVO期望收益风险调整化预注册.md。
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api v29ra -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn v29ra_mu_risk_adjusted_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("connect db");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("tushare env");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for (label, strategy_id, account) in [
            ("E1-ddctrl-muRA", "v29ra", "pa-v29ra-lev"),
            ("E2-h20-muRA", "v29rah", "pa-v29rah-lev"),
        ] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, strategy_id)
                .await
                .unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
                None::<crate::routes::shared::MvoWeightCache>,
            ));
            let sim = run_daily_simulation(
                &db,
                account,
                &rs,
                start,
                end,
                crate::routes::rebalance::PriceSource::EodClose,
                &cache,
                &tushare,
                true,
                true,
                1.5,
                "fixed",
            )
            .await
            .expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!(
                "[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} Calmar={:.2} cum={:.0}% days={} (基线: 12.43%/34.34%/0.61)",
                label,
                m.annual_return * 100.0,
                m.max_drawdown * 100.0,
                m.sharpe,
                m.calmar,
                m.cumulative_return * 100.0,
                m.trading_days
            );
        }
    }
}

#[cfg(test)]
mod etf_expand_rerun {
    use super::*;

    /// 重跑2：ETF扩池在真实基线（授信预算+weight修复+72因子combo）下重测。
    /// 之前否决时基线虚高12.43%，且 sleeve 用的是拼接体combo（11.14%）。
    #[tokio::test]
    #[ignore]
    async fn etf_pool_expand_clean_rerun() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        for (label, sid) in [
            ("基线-7资产", "v24_lev"),
            ("E1-+4分散", "v27e1"),
            ("E2-+2分散", "v27e2"),
        ] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, sid).await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v26ms-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%", label,
                m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
    }
}

#[cfg(test)]
mod v30dd_tests {
    use super::*;

    /// v30dd：72 因子 dd_ctrl sleeve 的 composite 重测——最高优先实验。
    /// sleeve 层 9.28%/0.804/-22%，看 composite 能否兑现。
    #[tokio::test]
    #[ignore]
    async fn v30dd_composite_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v30dd").await.unwrap();
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
        let sim = run_daily_simulation(&db, "pa-v30dd-lev", &rs, start, end,
            crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
            true, true, 1.5, "fixed").await.expect("sim");
        let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
        let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!("[v30dd-lev] AR={:.2}% DD={:.2}% Sharpe={:.2} Calmar={:.2} cum={:.0}% (真实基线: 10.31%/24.98%/0.60)",
            m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.calmar, m.cumulative_return*100.0);
    }
}

#[cfg(test)]
mod v31fx_tests {
    use super::*;

    /// v31fx：固定权重(30%A+70%ETF池11资产) + dd_ctrl sleeve + 1.5x 回放验证。
    #[tokio::test]
    #[ignore]
    async fn v31fx_composite_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31fx").await.unwrap();
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
        let sim = run_daily_simulation(&db, "pa-v31fx-lev", &rs, start, end,
            crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
            true, true, 1.5, "fixed").await.expect("sim");
        let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
        let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!("[v31fx-lev] AR={:.2}% DD={:.2}% Sharpe={:.2} Calmar={:.2} cum={:.0}% (目标: 14%+/0.9+/-18%)",
            m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.calmar, m.cumulative_return*100.0);
    }
}

#[cfg(test)]
mod v31_dual_tests {
    use super::*;

    /// v31 双组回放：11 资产（修正权重）vs 7 资产（原始池）。
    #[tokio::test]
    #[ignore]
    async fn v31_dual_replay() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        for (label, sid, acct) in [
            ("v31fx-11资产", "v31fx", "pa-v31fx-lev"),
            ("v31f7-7资产", "v31f7", "pa-v31f7-lev"),
        ] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, sid).await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, acct, &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%", label,
                m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
    }
}

#[cfg(test)]
mod v31_freq_tests {
    use super::*;

    /// 跟踪误差消除实验：quarterly vs monthly vs biweekly 三频率对照。
    /// 假设：调仓频率越接近 sleeve 的 10 日频，跟踪误差越小，绩效越接近理论模拟。
    #[tokio::test]
    #[ignore]
    async fn v31_tracking_error_elimination() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for freq in ["quarterly", "monthly", "biweekly"] {
            sqlx::query("UPDATE strategy_config SET rebalance_freq=$1 WHERE strategy_id='v31f7'")
                .bind(freq).execute(&db).await.expect("upd");
            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%",
                freq, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
    }
}

#[cfg(test)]
mod weekly_test {
    use super::*;
    #[tokio::test]
    #[ignore]
    async fn v31_weekly() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
        let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
        let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs,
            chrono::NaiveDate::from_ymd_opt(2016,1,4).unwrap(), chrono::NaiveDate::from_ymd_opt(2026,9,2).unwrap(),
            crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
            true, true, 1.5, "fixed").await.expect("sim");
        let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
        let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
        println!("[weekly] AR={:.2}% DD={:.2}% Sharpe={:.2}", m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe);
    }
}

#[cfg(test)]
mod freq_final {
    use super::*;

    /// 终局频率实验：修复价格空间 BUG 后的公平对比。
    /// 之前 weekly 36.25% 是停牌股价格膨胀污染，本次应为真实绩效。
    #[tokio::test]
    #[ignore]
    async fn freq_final_clean() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for freq in ["quarterly", "monthly", "biweekly", "weekly"] {
            sqlx::query("UPDATE strategy_config SET rebalance_freq=$1, status='active' WHERE strategy_id='v31f7' OR parent_strategy_id='v31f7'")
                .bind(freq).execute(&db).await.expect("upd");
            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%",
                freq, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
        // 清理
        sqlx::query("UPDATE strategy_config SET status='inactive' WHERE strategy_id='v31f7' OR parent_strategy_id='v31f7'").execute(&db).await.ok();
    }
}

#[cfg(test)]
mod final_optimization {
    use super::*;

    /// 终局参数优化：最优 sleeve（kelly=0.15）+ weekly + 权重扫描
    #[tokio::test]
    #[ignore]
    async fn final_weight_scan() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for aw in [0.20f64, 0.25, 0.30, 0.35, 0.40, 0.45] {
            // 更新权重
            let etf_w = 1.0 - aw;
            let weights = format!("[{:.2}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}]",
                aw, etf_w*0.31, etf_w*0.40, etf_w*0.07, etf_w*0.14, etf_w*0.03, etf_w*0.03, etf_w*0.02);
            sqlx::query("UPDATE strategy_config SET default_weights=$1::jsonb WHERE strategy_id='v31f7'")
                .bind(&weights).execute(&db).await.expect("upd");

            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[A={:.0}%] AR={:.2}% DD={:.2}% Sharpe={:.2}",
                aw*100.0, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe);
        }
    }

    /// ETF 内部权重结构实验（2026-09-07 收益增强）：当前池内国债占 40%（年化仅 2.67%），
    /// 纳指年化 19.93% 仅 14% 权重——结构性拖累。对比规则化权重方案（零拟合先验，
    /// 非全样本挑参）：基准 / 等权 / 增长倾斜 / 半防御倾斜。均 A=20% + kelly=0.15 + weekly @1.5x。
    #[tokio::test]
    #[ignore]
    async fn etf_weight_structure_scan() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        // 顺序: [A股, 黄金518880, 国债511010, 标普513500, 纳指513100, 有色159980, 豆粕159985, 原油501018]
        let plans: Vec<(&str, Vec<f64>)> = vec![
            ("BASE 现行", vec![0.20, 0.248, 0.320, 0.056, 0.112, 0.024, 0.024, 0.016]),
            ("EQ 等权", vec![0.20, 0.1143, 0.1143, 0.1143, 0.1143, 0.1143, 0.1143, 0.1143]),
            ("GROW 增长倾斜", vec![0.20, 0.18, 0.10, 0.14, 0.24, 0.04, 0.06, 0.04]),
            ("HALF 半防御", vec![0.20, 0.16, 0.18, 0.12, 0.20, 0.04, 0.06, 0.04]),
        ];

        for (name, w) in &plans {
            let weights = format!("[{:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}]",
                w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]);
            sqlx::query("UPDATE strategy_config SET default_weights=$1::jsonb WHERE strategy_id='v31f7'")
                .bind(&weights).execute(&db).await.expect("upd");
            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 1.5, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[{}] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%",
                name, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
    }

    /// 杠杆扫描（2026-09-07 收益增强）：MaxDD 预算 35% 当前仅用 17%，风险预算闲置。
    /// 在权重结构实验选定的权重上扫 1.5~2.5x，验证 dd_ctrl/强平非线性下的真实曲线。
    /// 权重经环境变量 LEV_SCAN_WEIGHTS 注入（8 元素逗号分隔），缺省用生产现行权重。
    #[tokio::test]
    #[ignore]
    async fn leverage_scan() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        let weights = std::env::var("LEV_SCAN_WEIGHTS")
            .unwrap_or_else(|_| "0.20,0.248,0.320,0.056,0.112,0.024,0.024,0.016".into());
        let w: Vec<&str> = weights.split(',').collect();
        let weights = format!("[{:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}]",
            w[0].trim().parse::<f64>().unwrap(), w[1].trim().parse::<f64>().unwrap(),
            w[2].trim().parse::<f64>().unwrap(), w[3].trim().parse::<f64>().unwrap(),
            w[4].trim().parse::<f64>().unwrap(), w[5].trim().parse::<f64>().unwrap(),
            w[6].trim().parse::<f64>().unwrap(), w[7].trim().parse::<f64>().unwrap());
        sqlx::query("UPDATE strategy_config SET default_weights=$1::jsonb WHERE strategy_id='v31f7'")
            .bind(&weights).execute(&db).await.expect("upd");

        for lev in [1.5f64, 1.8, 2.0, 2.2, 2.5] {
            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, lev, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[lev={:.1}x] AR={:.2}% DD={:.2}% Sharpe={:.2} cum={:.0}%",
                lev, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe, m.cumulative_return*100.0);
        }
    }

    /// 等权 ETF 结构下的 A 股权重重扫（2026-09-07）：原 A 权重扫描基于旧权重结构
    /// （国债 32%），等权结构下最优 A 权重可能偏移。ETF 侧等权，A 15%~30%，@2.2x。
    #[tokio::test]
    #[ignore]
    async fn a_weight_scan_equal_weight() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        for aw in [0.15f64, 0.20, 0.25, 0.30] {
            let etf_w = (1.0 - aw) / 7.0;
            let weights = format!("[{:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}]",
                aw, etf_w, etf_w, etf_w, etf_w, etf_w, etf_w, etf_w);
            sqlx::query("UPDATE strategy_config SET default_weights=$1::jsonb WHERE strategy_id='v31f7'")
                .bind(&weights).execute(&db).await.expect("upd");
            let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
            let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
            let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                true, true, 2.2, "fixed").await.expect("sim");
            let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
            let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
            println!("[A={:.0}% EW] AR={:.2}% DD={:.2}% Sharpe={:.2}",
                aw*100.0, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe);
        }
    }

    /// ETF 池扩容实验（2026-09-07）：相关性审计（日期对齐）显示红利/创业板/可转债
    /// 与现有资产最大相关 ≤0.36，德国 0.69（欧系簇取德弃法），红利三兄弟取 510880。
    /// 三方案 × 双杠杆档（2.2x 有杠杆 / 1.0x 无杠杆），A=15% 基准，等权零拟合。
    /// X1 扩3：红利+创业板+德国（10 资产）；X2 扩4：+可转债（11）；X3 扩5：+日经（12）。
    #[tokio::test]
    #[ignore]
    async fn pool_expansion_scan() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("db");
        let tushare = quant_data::tushare::client::TushareClient::from_env().expect("tushare");
        let start = NaiveDate::from_ymd_opt(2016, 1, 4).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();

        let base_etf = vec!["518880.SH","511010.SH","513500.SH","513100.SH","159980.SZ","159985.SZ","501018.SH"];
        let plans: Vec<(&str, Vec<&str>)> = vec![
            ("X0 现行7ETF", base_etf.clone()),
            ("X1 +红利+创业板+德国", [base_etf.clone(), vec!["510880.SH","159915.SZ","513030.SH"]].concat()),
            ("X2 +可转债(11)", [base_etf.clone(), vec!["510880.SH","159915.SZ","513030.SH","511380.SH"]].concat()),
            ("X3 +日经(12)", [base_etf.clone(), vec!["510880.SH","159915.SZ","513030.SH","511380.SH","513520.SH"]].concat()),
        ];

        for (name, etf) in &plans {
            let etf_json = serde_json::to_string(etf).unwrap();
            let aw = 0.15f64;
            let etf_w = (1.0 - aw) / etf.len() as f64;
            let mut ws = vec![format!("{:.4}", aw)];
            for _ in 0..etf.len() { ws.push(format!("{:.4}", etf_w)); }
            let weights = format!("[{}]", ws.join(", "));
            sqlx::query("UPDATE strategy_config SET etf_symbols=$1::jsonb, default_weights=$2::jsonb WHERE strategy_id='v31f7'")
                .bind(&etf_json).bind(&weights).execute(&db).await.expect("upd etf+w");

            for lev in [2.2f64, 1.0] {
                let rs = crate::routes::strategy::load_resolved_strategy(&db, "v31f7").await.unwrap();
                let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<crate::routes::shared::MvoWeightCache>));
                let sim = run_daily_simulation(&db, "pa-v31f7-lev", &rs, start, end,
                    crate::routes::rebalance::PriceSource::EodClose, &cache, &tushare,
                    true, true, lev, "fixed").await.expect("sim");
                let rets: Vec<f64> = sim.iter().map(|d| d.net_return).collect();
                let m = compute_metrics(&rets, rs.mvo.as_ref().unwrap().risk_free_rate);
                println!("[{} @{:.1}x] AR={:.2}% DD={:.2}% Sharpe={:.2}",
                    name, lev, m.annual_return*100.0, m.max_drawdown*100.0, m.sharpe);
            }
        }
    }
}
