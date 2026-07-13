//! 共享建仓模块 —— 回放与实盘走同一套选股→建仓→盯市链路。
//! 唯一差异:价格源(PriceSource)。选股统一为"读某 task_id 的 backtest_position 当日截面"
//! (实盘 task_id 由 generate_paper_signals_for_all 的 run-factor 产生并传入,
//!  回放 task_id 用 rs 的 a_share equity_curve_task_id 由调用方传入)。
//!
//! NAV 恒等式:current_nav = 持仓市值 + cash - margin_amount(经 trading::update_current_nav)
//!
//! 绩效口径(spec §2.1/§3.4):回放绩效基于真实持仓盯市后的 current_nav 复利,
//! 不再用 backtest_equity_curve 的收益率累乘。rebalance_account 做目标持仓驱动增量调仓。

#![allow(dead_code, unused_imports)]

use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;

use quant_data::tushare::client::TushareClient;

/// 价格源(回放/实盘唯一差异)
#[derive(Clone, Copy)]
pub enum PriceSource {
    /// 回放:日终收盘价(market_stock_daily_bar_adj)
    EodClose,
    /// 实盘:盘中实时价(Tushare)
    Intraday,
}

/// 选股结果(当日某 A 股个股的截面持仓)
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub market_value: Decimal,
}

/// 统一选股:读指定 task_id 的 backtest_position 当日截面。
/// task_id 来源:实盘=run-factor 当日产生;回放=rs 的 a_share asset equity_curve_task_id(调用方传入)。
pub async fn select_positions(
    db: &PgPool,
    task_id: &str,
    date: NaiveDate,
) -> Result<Vec<Position>, String> {
    let rows = sqlx::query_as::<_, (String, Decimal, Decimal)>(
        "SELECT symbol, COALESCE(quantity,0), COALESCE(market_value,0)
         FROM backtest_position
         WHERE task_id = $1 AND position_date = $2
           AND quantity > 0 AND market_value > 0
         ORDER BY market_value DESC",
    )
    .bind(task_id)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("select_positions: {}", e))?;
    Ok(rows
        .into_iter()
        .map(|(symbol, quantity, market_value)| Position {
            symbol,
            quantity,
            market_value,
        })
        .collect())
}

// rebalance_account 与 mark_to_market 由 Task 5 在此追加

use crate::routes::scheduler::{
    a_share_trade_block_reason, compute_lw_mvo_weights, compute_vol_target_leverage,
    detect_regime_exposure, fetch_intraday_etf_prices, resolved_to_legacy_sc,
    send_quality_alert, MvoWeightCache, StrategyConfig,
};
use crate::routes::strategy::ResolvedStrategy;
use crate::routes::trading::{execute_simulated_trade, try_auto_repay, update_current_nav};
use std::collections::HashMap;
use uuid::Uuid;

fn short_id() -> String {
    Uuid::new_v4()
        .to_string()
        .split('-')
        .next()
        .unwrap()
        .to_string()
}

/// 共享建仓:回放(EodClose)/实盘(Intraday)走同一套资金→权重→体制→杠杆→维保→建仓→NAV。
/// 目标持仓驱动增量调仓:读当前持仓,算 delta(买不足/卖多余/清不在目标集的),经 trading 落计划+实际交易记录(含滑点)。
/// task_id:实盘=run-factor 当日产生;回放=rs 的 a_share asset equity_curve_task_id(由调用方传入)。
/// 返回建仓笔数。
pub async fn rebalance_account(
    db: &PgPool,
    account_id: &str,
    rs: &ResolvedStrategy,
    date: NaiveDate,
    task_id: &str,
    price_source: PriceSource,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    tushare: &TushareClient,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
    // P2-B:上层已算的值传入,避免重复查 DB(回放 mvo_engine 传 Some,实盘传 None 自查)
    preloaded_nav: Option<f64>,
    preloaded_regime: Option<f64>,
) -> Result<usize, String> {
    // 桥接:ResolvedStrategy → 平铺 StrategyConfig(MVO 函数/sc_slippage_pct 仍接 &StrategyConfig)。
    // task_id 保留参数:实盘由调用方传 run-factor 当日 task;回放传 rs 的 a_share equity_curve_task_id。
    let sc = resolved_to_legacy_sc(rs)?;
    // 1. 资金基准:统一用 current_nav(非 initial_capital);NULL 则降级 initial_capital
    // P2-B:上层 mvo_engine 已算过 NAV 时直接用,省 1 次 DB
    let capital_f = if let Some(nav) = preloaded_nav {
        nav
    } else {
        let current_nav: Decimal = sqlx::query_scalar(
            "SELECT COALESCE(current_nav, initial_capital) FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_one(db)
        .await
        .map_err(|e| format!("nav query: {}", e))?;
        current_nav.to_string().parse::<f64>().unwrap_or(0.0)
    };
    if capital_f <= 0.0 {
        return Err(format!("账号 {} current_nav<=0,拒绝建仓", account_id));
    }
    let current_nav = Decimal::from_f64_retain(capital_f).unwrap_or(Decimal::ZERO);

    // 2. MVO 权重 + 体制(共享)
    let mvo_weights = compute_lw_mvo_weights(db, date, mvo_cache, &sc).await;
    // P2-B:上层 mvo_engine 已用 csi300_map 算过 regime 时直接用,省 1 次 DB
    let regime = if let Some(r) = preloaded_regime {
        r
    } else {
        detect_regime_exposure(db, date, sc.deep_bear_threshold, sc.deep_bear_exposure).await
    };
    let mvo_a_pct = mvo_weights.get(0).copied().unwrap_or(0.0) * regime;

    // 3. 选股(读 task_id 的 backtest_position 当日截面)
    let positions = select_positions(db, task_id, date).await?;
    let total_stock_mv: Decimal = positions.iter().map(|p| p.market_value).sum();
    let a_share_capital =
        current_nav * Decimal::from_f64_retain(mvo_a_pct).unwrap_or(Decimal::ZERO);
    let base_scale = if total_stock_mv > Decimal::ZERO {
        a_share_capital / total_stock_mv
    } else {
        Decimal::ZERO
    };

    // 4. 杠杆(共享 compute_vol_target_leverage)
    let leverage_mult = if leverage_enabled && regime > sc.leverage_regime_threshold && leverage_multiplier > 1.0 {
        if leverage_mode == "vol_target" {
            Decimal::from_f64_retain(compute_vol_target_leverage(db, account_id, &sc).await)
                .unwrap_or(Decimal::ONE)
        } else {
            Decimal::from_f64_retain(leverage_multiplier).unwrap_or(Decimal::ONE)
        }
    } else {
        Decimal::ONE
    };

    // 5. 维保门控(实盘口径):维保<警戒线禁买(跳过建仓段),平仓由每日盯市 check_maintenance_after_mark 处理。
    // 建仓前的预防:若当前已警戒状态,本次 rebalance 不新买入(只允许调仓段卖出的减仓)。
    // P2-D:维保 ratio + 阈值合并为 1 条 SQL(原 maintenance_ratio + SELECT thr 两次 DB)
    let mut warn_no_buy = false;
    if leverage_enabled {
        let maint_info: Option<(f64, f64, f64)> = sqlx::query_as(
            "SELECT
                CASE WHEN COALESCE(margin_amount,0) < 0.000001 THEN 'Infinity'::float
                     ELSE (COALESCE((SELECT SUM(market_value) FROM paper_position WHERE paper_account_id=$1),0)
                           + COALESCE(cash,0)) / COALESCE(margin_amount,1)::float END,
                COALESCE(liquidation_threshold, 1.3),
                COALESCE(warning_threshold, 1.5)
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        if let Some((maint, liq_thr, warn_thr)) = maint_info {
            if !maint.is_infinite() {
                if maint < liq_thr {
                    // 平仓线:强平(正常由每日检查处理,此处兜底)
                    let _ = force_liquidation(db, account_id, date, warn_thr + 0.05, sc_slippage_pct(&sc)).await;
                    warn_no_buy = true;
                    warn!("[MAINT] date={} acct={} 维保{:.3}<平仓线{} 强平+禁买", date, account_id, maint, liq_thr);
                } else if maint < warn_thr {
                    warn_no_buy = true;
                    warn!("[MAINT] date={} acct={} 维保{:.3}<警戒线{} 禁买", date, account_id, maint, warn_thr);
                }
            }
        }
    }
    let scale = base_scale * leverage_mult;

    // 6. 读当前持仓(增量调仓基础)
    let current_positions: HashMap<String, (Decimal, Decimal)> = sqlx::query_as::<
        _,
        (String, Decimal, Decimal),
    >(
        "SELECT symbol, quantity, COALESCE(avg_cost,0) FROM paper_position WHERE paper_account_id=$1 AND quantity>0",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("cur pos: {}", e))?
    .into_iter()
    .map(|(s, q, c)| (s, (q, c)))
    .collect();

    // 7. A 股目标持仓 → 增量调仓(买不足/卖多余)
    let slippage = sc_slippage_pct(&sc);
    let mut n = 0usize;
    let mut target_symbols: std::collections::HashSet<String> = std::collections::HashSet::new();
    // P2-A:批量预加载当日 A 股停牌/涨跌停状态(替代循环内逐股查 2 次 DB)
    let block_symbols: Vec<String> = positions.iter().map(|p| p.symbol.clone()).collect();
    let trade_block_map =
        crate::routes::scheduler::preload_trade_block_map(db, date, &block_symbols).await;
    for p in &positions {
        if warn_no_buy {
            // 警戒禁买:不建仓,仅记录目标集(清仓段仍可减仓)
            target_symbols.insert(p.symbol.clone());
            continue;
        }
        if p.quantity <= Decimal::ZERO || p.market_value <= Decimal::ZERO {
            continue;
        }
        let price = p.market_value / p.quantity;
        if price <= Decimal::ZERO {
            continue;
        }
        target_symbols.insert(p.symbol.clone());
        // A 股交易阻断(停牌/涨跌停)——回放用批量预加载的 trade_block_map 内存查(P2-A)
        if let Some(reason) = trade_block_map.get(&p.symbol) {
            warn!("[rebalance] 跳过 A股交易: {} {}", p.symbol, reason);
            send_quality_alert(db, &[format!("{}: {}", account_id, reason)]).await;
            continue;
        }
        let target_qty = p.quantity * scale;
        let cur_qty = current_positions
            .get(&p.symbol)
            .map(|(q, _)| *q)
            .unwrap_or(Decimal::ZERO);
        let delta = target_qty - cur_qty;
        if delta.abs() < Decimal::new(1, 2) {
            // |delta| < 0.01,忽略
            continue;
        }
        let (side, qty) = if delta > Decimal::ZERO {
            ("buy", delta)
        } else {
            ("sell", -delta)
        };
        let target_value = qty * price;
        if side == "buy" && target_value < Decimal::ONE {
            continue;
        }
        let trade = crate::routes::trading::PlannedTrade {
            account_id: account_id.to_string(),
            symbol: p.symbol.clone(),
            side: side.into(),
            target_quantity: qty,
            target_price: price,
            price_upper_limit: None,
            price_lower_limit: None,
            slippage_pct: slippage,
            target_value,
            reason: Some(format!("调仓 A股 regime={:.0}%", regime * 100.0)),
            strategy_version_id: Some(sc.strategy_id.clone()),
        };
        if execute_simulated_trade(db, &trade).await.is_ok() {
            // 实际成交价(含滑点)用于更新持仓
            let slip_d = Decimal::from_f64_retain(slippage).unwrap_or(Decimal::ZERO);
            let mult = if side == "sell" {
                Decimal::ONE - slip_d
            } else {
                Decimal::ONE + slip_d
            };
            let fill_price = price * mult;
            apply_fill_to_position(db, account_id, &p.symbol, side, qty, fill_price).await;
            n += 1;
        }
    }

    // 7b. 清仓:当前持仓中【不在 A 股目标集 且 属于 A 股】的 symbol。
    // ETF 不在此清——ETF 减仓由 ETF 段增量调仓处理(delta=target_qty-cur_qty)。
    // 若在此清 ETF,清仓段改 DB 但 current_positions 内存快照不更新,
    // ETF 段会读到旧 cur_qty 导致 delta 反向(应 buy 却 sell),持仓 quantity 错乱、nav 跳变。
    for (sym, (qty, _)) in &current_positions {
        // 仅清 A股:排除策略配置的 ETF 列表(ETF 由 ETF 段管理)
        if sc.etf_symbols.iter().any(|e| e == sym) {
            continue;
        }
        if !target_symbols.contains(sym) && *qty > Decimal::ZERO {
            let price = fetch_eod_price(db, sym, date).await;
            if price <= 0.0 {
                continue;
            }
            let trade = crate::routes::trading::PlannedTrade {
                account_id: account_id.to_string(),
                symbol: sym.clone(),
                side: "sell".into(),
                target_quantity: *qty,
                target_price: Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO),
                price_upper_limit: None,
                price_lower_limit: None,
                slippage_pct: slippage,
                target_value: *qty * Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO),
                reason: Some("清仓(A股不在目标集)".into()),
                strategy_version_id: Some(sc.strategy_id.clone()),
            };
            if execute_simulated_trade(db, &trade).await.is_ok() {
                let slip_d = Decimal::from_f64_retain(slippage).unwrap_or(Decimal::ZERO);
                let fill_price = Decimal::from_f64_retain(price).unwrap_or(Decimal::ZERO)
                    * (Decimal::ONE - slip_d);
                apply_fill_to_position(db, account_id, sym, "sell", *qty, fill_price).await;
                n += 1;
            }
        }
    }

    // 8. ETF 建仓(按 price_source 取价:EodClose 从 DB / Intraday 从 tushare HashMap)
    // etf_symbols 必须与 mvo_weights 同源:只含当日已发行 ETF(compute_lw_mvo_weights 已过滤),
    // 否则 mvo_weights.get(i+1) 索引会与全量 sc.etf_symbols 错位。
    // P2-C:批量预加载 ETF 上市状态 + 当日收盘价(替代循环内逐个查 DB)
    let etf_listed_map = preload_etf_listed_map(db, &sc.etf_symbols, date).await;
    let mut listed_etf_symbols: Vec<String> = Vec::new();
    for s in &sc.etf_symbols {
        if etf_listed_map.get(s).copied().unwrap_or(false) {
            listed_etf_symbols.push(s.clone());
        }
    }
    let etf_allocations = build_etf_allocations(&mvo_weights, regime, &listed_etf_symbols);
    // P2-C:批量预加载 ETF 当日收盘价(EodClose 模式,替代 fetch_etf_price 逐个查)
    let etf_eod_prices: HashMap<String, f64> = if matches!(price_source, PriceSource::EodClose) {
        preload_etf_eod_prices(db, &listed_etf_symbols, date).await
    } else {
        HashMap::new()
    };
    let intraday_prices: HashMap<String, f64> = if matches!(price_source, PriceSource::Intraday) {
        let syms: Vec<String> = etf_allocations
            .iter()
            .map(|(s, _)| s.to_string())
            .collect();
        fetch_intraday_etf_prices(tushare, &syms, date, db).await
    } else {
        HashMap::new()
    };
    for (etf_symbol, alloc_pct) in &etf_allocations {
        if warn_no_buy {
            continue; // 警戒禁买:不建仓 ETF(减仓由增量 delta 自然处理)
        }
        if *alloc_pct <= 0.0 {
            continue;
        }
        // ETF 目标市值也应用 leverage_mult(与 A 股段 scale 口径一致)。
        // 杠杆是账号级配置,放大整个组合(A股+ETF),而非只放大 A 股 11%。
        // 修复前:alloc_amount = current_nav × alloc_pct(不放大)→ 杠杆只对 A股生效,总 nav 几乎不变。
        let alloc_amount =
            current_nav * Decimal::from_f64_retain(*alloc_pct).unwrap_or(Decimal::ZERO) * leverage_mult;
        if alloc_amount <= Decimal::ZERO {
            continue;
        }
        // P2-C:EodClose 模式优先用预加载的 etf_eod_prices,Intraday 模式用 intraday_prices
        let price_val = if matches!(price_source, PriceSource::EodClose) {
            etf_eod_prices.get(etf_symbol.as_str()).copied().unwrap_or(0.0)
        } else {
            fetch_etf_price(db, etf_symbol.as_str(), date, price_source, &intraday_prices).await
        };
        let price = Decimal::from_f64_retain(price_val).unwrap_or(Decimal::ONE);
        if price <= Decimal::ZERO {
            continue;
        }
        let target_qty = alloc_amount / price;
        let cur_qty = current_positions
            .get(etf_symbol.as_str())
            .map(|(q, _)| *q)
            .unwrap_or(Decimal::ZERO);
        let delta = target_qty - cur_qty;
        if delta.abs() < Decimal::new(1, 2) {
            continue;
        }
        let (side, qty) = if delta > Decimal::ZERO {
            ("buy", delta)
        } else {
            ("sell", -delta)
        };
        let target_value = qty * price;
        let trade = crate::routes::trading::PlannedTrade {
            account_id: account_id.to_string(),
            symbol: etf_symbol.to_string(),
            side: side.into(),
            target_quantity: qty,
            target_price: price,
            price_upper_limit: None,
            price_lower_limit: None,
            slippage_pct: slippage,
            target_value,
            reason: Some(format!("调仓 ETF w={:.1}%", *alloc_pct * 100.0)),
            strategy_version_id: Some(sc.strategy_id.clone()),
        };
        if execute_simulated_trade(db, &trade).await.is_ok() {
            let slip_d = Decimal::from_f64_retain(slippage).unwrap_or(Decimal::ZERO);
            let mult = if side == "sell" {
                Decimal::ONE - slip_d
            } else {
                Decimal::ONE + slip_d
            };
            let fill_price = price * mult;
            apply_fill_to_position(db, account_id, etf_symbol.as_str(), side, qty, fill_price).await;
            n += 1;
        }
    }

    // 建仓 0 笔 + A股选股空 + 账号当前无持仓 → 报错(不产出假绩效)
    if n == 0 && positions.is_empty() && current_positions.is_empty() {
        let msg = format!("建仓 0 笔:策略 {} 当日已发行标的均无建仓,可能权益曲线/ETF价格数据缺失", rs.strategy_id);
        warn!("[rebalance] {}", msg);
        crate::routes::scheduler::send_quality_alert(db, &[msg.clone()]).await;
        return Err(msg);
    }

    // 9. NAV 重算:统一走 trading::update_current_nav(正确口径:持仓市值+cash-margin)
    update_current_nav(db, account_id).await?;
    try_auto_repay(db, account_id).await.ok();
    Ok(n)
}

/// 维保比例(维持担保比例) = (持仓市值 + cash) / margin。margin=0 返回 ∞(无融资不限制)。
async fn maintenance_ratio(db: &PgPool, account_id: &str) -> f64 {
    let row: Option<(rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal)> =
        sqlx::query_as(
            "SELECT (SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id=$1),
                    COALESCE(cash,0), COALESCE(margin_amount,0)
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
    match row {
        Some((mv, cash, margin)) => {
            let margin_f = margin.to_string().parse::<f64>().unwrap_or(0.0);
            if margin_f < 1e-6 {
                return f64::INFINITY; // 无融资
            }
            let total = (mv + cash).to_string().parse::<f64>().unwrap_or(0.0);
            total / margin_f
        }
        None => f64::INFINITY,
    }
}

/// 强制平仓:维保 < 平仓线时,卖出持仓还款直至维保恢复到 target_maint。
/// 平仓顺序:优先 ETF(流动性好),再 A股;每只按市值比例卖。返回平仓笔数 + 是否清空。
async fn force_liquidation(
    db: &PgPool,
    account_id: &str,
    date: NaiveDate,
    target_maint: f64,
    slippage: f64,
) -> Result<usize, String> {
    let mut n = 0usize;
    loop {
        let maint = maintenance_ratio(db, account_id).await;
        if maint >= target_maint || maint.is_infinite() {
            break; // 维保恢复或无融资
        }
        // 计算需还款额:使 maint = total_assets / (margin - repay) = target_maint
        // repay = margin - total_assets / target_maint
        let (mv, cash, margin): (rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal) =
            sqlx::query_as(
                "SELECT (SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id=$1),
                        COALESCE(cash,0), COALESCE(margin_amount,0)
                 FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(account_id)
            .fetch_one(db)
            .await
            .map_err(|e| format!("liq query: {}", e))?;
        let margin_f = margin.to_string().parse::<f64>().unwrap_or(0.0);
        let total_f = (mv + cash).to_string().parse::<f64>().unwrap_or(0.0);
        let need_repay = margin_f - total_f / target_maint;
        if need_repay <= 1.0 {
            break;
        }
        // 取持仓中市值最大的一只卖出(足以还款的比例)
        let pos: Option<(String, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal)> =
            sqlx::query_as(
                "SELECT symbol, quantity, market_price, market_value
                 FROM paper_position WHERE paper_account_id=$1 AND quantity>0
                 ORDER BY market_value DESC LIMIT 1",
            )
            .bind(account_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("liq pos: {}", e))?;
        let (sym, qty, price, mv_sym) = match pos {
            Some(p) => p,
            None => break, // 无持仓可平
        };
        // 卖出数量 = need_repay / price(含滑点卖出价),不超过持仓
        let slip_d = rust_decimal::Decimal::from_f64_retain(slippage).unwrap_or(rust_decimal::Decimal::ZERO);
        let sell_price = price * (rust_decimal::Decimal::ONE - slip_d);
        if sell_price <= rust_decimal::Decimal::ZERO {
            break;
        }
        let need_repay_d = rust_decimal::Decimal::from_f64_retain(need_repay).unwrap_or(rust_decimal::Decimal::ZERO);
        let mut sell_qty = need_repay_d / sell_price;
        if sell_qty > qty {
            sell_qty = qty; // 清空该只
        }
        if sell_qty <= rust_decimal::Decimal::new(1, 2) {
            break; // 量太小,避免死循环
        }
        let trade = crate::routes::trading::PlannedTrade {
            account_id: account_id.to_string(),
            symbol: sym.clone(),
            side: "sell".into(),
            target_quantity: sell_qty,
            target_price: price,
            price_upper_limit: None,
            price_lower_limit: None,
            slippage_pct: slippage,
            target_value: sell_qty * price,
            reason: Some(format!("强平(维保{:.2}<平仓线)", maint)),
            strategy_version_id: None,
        };
        if execute_simulated_trade(db, &trade).await.is_ok() {
            apply_fill_to_position(db, account_id, &sym, "sell", sell_qty, sell_price).await;
            // 卖出后 cash += sell_qty*sell_price,主动还款降低 margin
            let repay_amount = sell_qty * sell_price;
            let _ = sqlx::query(
                "UPDATE paper_account SET cash = GREATEST(cash - $2, 0),
                     margin_amount = GREATEST(margin_amount - $2, 0)
                 WHERE paper_account_id = $1",
            )
            .bind(account_id)
            .bind(repay_amount)
            .execute(db)
            .await;
            update_current_nav(db, account_id).await?;
            n += 1;
            warn!(
                "[FORCE_LIQ] date={} acct={} sym={} qty={} 维保{:.3}→平仓还款{:.0}",
                date, account_id, sym, sell_qty, maint, repay_amount
            );
        } else {
            break;
        }
        if n > 50 {
            warn!("[FORCE_LIQ] date={} 平仓超50笔,中止防死循环", date);
            break;
        }
    }
    Ok(n)
}

/// 每日盯市后维保检查:维保<平仓线触发强平,维保<警戒线标记禁买。
/// 返回 (强平笔数, 是否警戒禁买)。
pub async fn check_maintenance_after_mark(
    db: &PgPool,
    account_id: &str,
    date: NaiveDate,
    slippage: f64,
) -> Result<(usize, bool), String> {
    // 读维保阈值 + 杠杆配置(合并为 1 条 SELECT,原 2 条查同一行 paper_account)
    let (liq_thr, warn_thr, leverage_enabled, _): (f64, f64, bool, Option<f64>) =
        sqlx::query_as(
            "SELECT COALESCE(liquidation_threshold, 1.3), COALESCE(warning_threshold, 1.5),
                    leverage_enabled, leverage_multiplier
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("thr/lev: {}", e))?
        .unwrap_or((1.3, 1.5, false, None));
    if !leverage_enabled {
        return Ok((0, false)); // 无杠杆不检查维保
    }
    let maint = maintenance_ratio(db, account_id).await;
    if maint.is_infinite() {
        return Ok((0, false));
    }
    // 平仓线:强平至维保恢复到 warn_thr + 缓冲(0.05)
    let mut liq_n = 0;
    if maint < liq_thr {
        liq_n = force_liquidation(db, account_id, date, warn_thr + 0.05, slippage).await?;
    }
    let maint_after = maintenance_ratio(db, account_id).await;
    let warn = !maint_after.is_infinite() && maint_after < warn_thr;
    Ok((liq_n, warn))
}

/// 每日盯市:用当日收盘价重算持仓 market_price/market_value,使 update_current_nav 反映真实市值。
/// 回放逐日复利的必要步骤(否则 NAV 停在建仓日不动)。
pub async fn mark_to_market(db: &PgPool, account_id: &str, date: NaiveDate) -> Result<(), String> {
    // DISTINCT ON 取每个 symbol 不晚于 date 的最近收盘价(当日缺失时降级最近可用)
    sqlx::query(
        "UPDATE paper_position pp SET
             market_price = sub.close, market_value = pp.quantity * sub.close
         FROM (
             SELECT DISTINCT ON (symbol) symbol, close::numeric AS close
             FROM market_stock_daily_bar_adj
             WHERE trade_date <= $1
               AND symbol IN (SELECT symbol FROM paper_position WHERE paper_account_id = $2)
             ORDER BY symbol, trade_date DESC
         ) sub
         WHERE pp.symbol = sub.symbol AND pp.paper_account_id = $2",
    )
    .bind(date)
    .bind(account_id)
    .execute(db)
    .await
    .map_err(|e| format!("mtm: {}", e))?;
    Ok(())
}

/// 成交后更新持仓:买(qty+=,avg_cost 移动加权);卖(qty-=,qty→0 删行)。
/// 同步流转 cash/margin:买扣 cash(不足自动融资 margin+=缺口);卖 cash+=fill_amount(末尾 try_auto_repay 归还多余融资)。
/// 这是 NAV 复利成立的前提——否则 current_nav = 持仓市值+cash-margin 会因 cash 不动而虚高。
async fn apply_fill_to_position(
    db: &PgPool,
    account_id: &str,
    sym: &str,
    side: &str,
    qty: Decimal,
    fill_price: Decimal,
) {
    let fill_amount = qty * fill_price;
    if side == "buy" {
        // 持仓:移动加权 avg_cost
        let _ = sqlx::query(
            "INSERT INTO paper_position (paper_position_id, paper_account_id, symbol, quantity, avg_cost, market_price, market_value)
             VALUES ($1, $2, $3, $4, $5, $5, $4*$5)
             ON CONFLICT (paper_account_id, symbol) DO UPDATE SET
                 avg_cost = (paper_position.avg_cost * paper_position.quantity + EXCLUDED.avg_cost * EXCLUDED.quantity)
                            / (paper_position.quantity + EXCLUDED.quantity),
                 quantity = paper_position.quantity + EXCLUDED.quantity,
                 market_price = EXCLUDED.market_price,
                 market_value = (paper_position.quantity + EXCLUDED.quantity) * EXCLUDED.market_price",
        )
        .bind(format!("pp-{}", short_id()))
        .bind(account_id)
        .bind(sym)
        .bind(qty)
        .bind(fill_price)
        .execute(db)
        .await;
        // 资金:扣 cash,不足自动融资(margin += 缺口)。单 SQL 保证原子。
        let _ = sqlx::query(
            "UPDATE paper_account SET
                 cash = CASE WHEN COALESCE(cash,0) >= $2 THEN cash - $2 ELSE 0 END,
                 margin_amount = COALESCE(margin_amount,0) + GREATEST($2 - COALESCE(cash,0), 0)
             WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .bind(fill_amount)
        .execute(db)
        .await;
    } else {
        // sell
        let _ = sqlx::query(
            "UPDATE paper_position SET quantity = quantity - $3,
                 market_value = (quantity - $3) * market_price
             WHERE paper_account_id = $1 AND symbol = $2 AND quantity >= $3",
        )
        .bind(account_id)
        .bind(sym)
        .bind(qty)
        .execute(db)
        .await;
        // qty 归零的行删除(保持持仓表干净)
        let _ =
            sqlx::query("DELETE FROM paper_position WHERE paper_account_id = $1 AND quantity <= 0")
                .bind(account_id)
                .execute(db)
                .await;
        // 资金回流:cash += fill_amount(末尾 try_auto_repay 会把超出 reserve 的部分还给 margin)
        let _ = sqlx::query(
            "UPDATE paper_account SET cash = COALESCE(cash,0) + $2 WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .bind(fill_amount)
        .execute(db)
        .await;
    }
}

/// 策略级滑点(从 strategy_config.slippage_pct 字段读,默认 0.002=20bp)
fn sc_slippage_pct(sc: &StrategyConfig) -> f64 {
    sc.slippage_pct
}

fn build_etf_allocations(
    mvo_weights: &[f64],
    regime: f64,
    etf_symbols: &[String],
) -> Vec<(String, f64)> {
    let mut v: Vec<(String, f64)> = etf_symbols
        .iter()
        .enumerate()
        .map(|(i, sym)| {
            // mvo_weights[0]=A股, mvo_weights[1..]=已发行 ETF 权重(顺序与 etf_symbols 一致)
            (sym.clone(), mvo_weights.get(i + 1).copied().unwrap_or(0.0) * regime)
        })
        .collect();
    // 现金段:regime < 1 时补银华日利
    if 1.0 - regime > 0.01 {
        v.push(("511880.SH".to_string(), 1.0 - regime));
    }
    v
}

/// 取 ETF 价:EodClose 从 market_stock_daily_bar_adj;Intraday 从预取的实时价 HashMap
async fn fetch_etf_price(
    db: &PgPool,
    symbol: &str,
    date: NaiveDate,
    price_source: PriceSource,
    intraday_prices: &HashMap<String, f64>,
) -> f64 {
    if matches!(price_source, PriceSource::Intraday) {
        if let Some(p) = intraday_prices.get(symbol) {
            if *p > 0.0 {
                return *p;
            }
        }
    }
    // EodClose 或 Intraday fallback:取不晚于 date 的最近收盘价
    fetch_eod_price(db, symbol, date).await
}

/// 取某 symbol 不晚于 date 的最近收盘价(EodClose 口径)
async fn fetch_eod_price(db: &PgPool, symbol: &str, date: NaiveDate) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol=$1 AND trade_date<=$2 ORDER BY trade_date DESC LIMIT 1",
    )
    .bind(symbol)
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .unwrap_or(1.0)
}

/// P2-C:批量预加载 ETF 当日收盘价(EodClose 口径)。
///
/// 替代循环内逐个 fetch_eod_price(每个 ETF 1 次 DB)。用 DISTINCT ON 取每个 symbol
/// 不晚于 date 的最近收盘价,一次查全部。
async fn preload_etf_eod_prices(
    db: &PgPool,
    symbols: &[String],
    date: NaiveDate,
) -> HashMap<String, f64> {
    if symbols.is_empty() {
        return HashMap::new();
    }
    let rows: Vec<(String, f64)> = sqlx::query_as(
        "SELECT DISTINCT ON (symbol) symbol, close::double precision
         FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1) AND trade_date <= $2 AND close > 0
         ORDER BY symbol, trade_date DESC",
    )
    .bind(symbols)
    .bind(date)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    rows.into_iter().collect()
}

/// P2-C:批量预加载 ETF 上市状态(是否在 date 当日已发行)。
///
/// 替代循环内逐个 is_etf_listed_on(每个 ETF 1-2 次 DB)。
/// 对齐 equity_curve_sync.rs:24 is_etf_listed_on 逻辑:优先 list_date,fallback MIN(trade_date)。
/// 一次查所有 ETF 的 list_date + 首发行情日,返回 HashMap<symbol, is_listed>。
async fn preload_etf_listed_map(
    db: &PgPool,
    symbols: &[String],
    date: NaiveDate,
) -> HashMap<String, bool> {
    if symbols.is_empty() {
        return HashMap::new();
    }
    // 一次查 list_date + 首发行情日(MIN(trade_date) fallback)
    let rows: Vec<(String, Option<NaiveDate>, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT m.symbol, m.list_date,
                (SELECT MIN(trade_date) FROM market_stock_daily_bar_adj b WHERE b.symbol = m.symbol) AS first_bar
         FROM market_stock m
         WHERE m.symbol = ANY($1)",
    )
    .bind(symbols)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut map: HashMap<String, bool> = HashMap::new();
    for (sym, list_date, first_bar) in rows {
        let listed = if let Some(ld) = list_date {
            ld <= date
        } else {
            first_bar.map(|f| f <= date).unwrap_or(false)
        };
        map.insert(sym, listed);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_etf_allocations_dynamic_symbols() {
        let weights = vec![0.12, 0.22, 0.28, 0.05]; // A股 + 2 ETF
        let symbols = vec!["518880.SH".to_string(), "511010.SH".to_string()];
        let allocs = build_etf_allocations(&weights, 1.0, &symbols);
        assert_eq!(allocs.len(), 2);
        assert_eq!(allocs[0].0, "518880.SH");
        assert!((allocs[0].1 - 0.22).abs() < 1e-9);
        assert_eq!(allocs[1].0, "511010.SH");
        assert!((allocs[1].1 - 0.28).abs() < 1e-9);
    }

    #[test]
    fn test_sc_slippage_pct_reads_field() {
        let sc = StrategyConfig {
            strategy_id: "test".into(),
            name: "test".into(),
            etf_symbols: vec![],
            equity_curve_task_id: String::new(),
            min_stock: 0.0,
            max_single: 0.0,
            max_single_bull: 0.0,
            momentum_blend_ratio: 0.0,
            ga_population: 0,
            ga_generations: 0,
            vol_target: 0.0,
            leverage_cap: 0.0,
            default_weights: vec![],
            regime_bull_min_stock: 0.0,
            regime_bear_min_stock: 0.0,
            deep_bear_threshold: -0.10,
            deep_bear_exposure: 0.60,
            signal_source: String::new(),
            prediction_blend_weight: 0.0,
            combo_name: String::new(),
            top_n: 0,
            prediction_set_id: None,
            dynamic_target_cap: 0.0,
            dynamic_target_floor: 0.0,
            score_direction: String::new(),
            candidate_tier: String::new(),
            leverage_regime_threshold: 0.9,
            slippage_pct: 0.005,
            mvo_objective: "minvariance".into(),
        };
        assert!((sc_slippage_pct(&sc) - 0.005).abs() < 1e-12);
    }
}
