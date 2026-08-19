//! 实盘绩效日报与快照模块（DDD R10/Step 6c-3：从 scheduler 上帝模块迁出）。
//!
//! 原属 scheduler.rs，职责是「写 NAV 快照 + 回测偏离对比 + 钉钉日报推送」。
//! 与调度编排无关，迁出后 scheduler 只管任务生命周期与 CRON。
//!
//! 三个入口：
//! - [`push_daily_performance_report`]：EOD 日报（偏离对比 + 快照写入 + 钉钉推送）
//! - [`snapshot_positions_for_all_accounts`]：调仓后即时快照（仅写快照，防 EOD 失败空档）
//! - [`push_dingtalk_for_all_accounts`]：持仓摘要钉钉推送
//!
//! 依赖关系：零 scheduler 内部私有函数依赖，仅用 shared::upsert_nav_snapshot /
//! shared::send_quality_alert / dingtalk / sync::market_data。

use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::routes::shared::{NavSnapshot, send_quality_alert, upsert_nav_snapshot};

/// T+1 补盯市后的快照重算(不推钉钉)。
///
/// 20:00 EOD 时 Tushare fund_daily 常无当日数据(实测 0 rows),ETF 持仓的日终
/// 盯市被迫落在前日收盘;次日 9:00 T+1 补同步后 ETF 日线就绪,调用方先
/// mark_to_market(date) + update_current_nav,再调本函数按当前 NAV 重写 date 的
/// snapshot(nav/daily_return/cumulative_return),补齐真实日终口径。
/// 幂等:ON CONFLICT 覆盖,重复调用无副作用。
pub async fn refresh_eod_snapshot(db: &PgPool, date: NaiveDate) {
    let accounts: Vec<(String, f64, f64, f64)> = sqlx::query_as(
        "SELECT paper_account_id,
                COALESCE(current_nav, initial_capital)::double precision,
                COALESCE(initial_capital, 0)::double precision,
                COALESCE(cash, 0)::double precision
         FROM paper_account WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    for (account_id, nav, init_cap, cash) in &accounts {
        let prev_nav: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT nav FROM paper_nav_snapshot
             WHERE paper_account_id = $1 AND snapshot_date < $2
             ORDER BY snapshot_date DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(date)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let prev_nav_f = prev_nav
            .map(|(d,)| d.to_string().parse::<f64>().unwrap_or(*nav))
            .unwrap_or(*init_cap);

        let pos: Option<(i64, rust_decimal::Decimal)> = sqlx::query_as(
            "SELECT COUNT(*)::bigint, COALESCE(SUM(market_value), 0)
             FROM paper_position WHERE paper_account_id = $1 AND quantity > 0",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let (position_count, market_value) =
            pos.unwrap_or((0, rust_decimal::Decimal::ZERO));

        let mut snap = NavSnapshot::new(account_id.clone(), date, *nav);
        snap.cash = *cash;
        snap.market_value = market_value.to_string().parse::<f64>().unwrap_or(0.0);
        snap.position_count = position_count as i32;
        if prev_nav_f > 0.0 {
            snap.daily_return = Some((*nav - prev_nav_f) / prev_nav_f);
        }
        if *init_cap > 0.0 {
            snap.cumulative_return = Some((*nav - *init_cap) / *init_cap);
        }
        if let Err(e) = upsert_nav_snapshot(db, &snap).await {
            warn!("[report] T+1 补盯市 snapshot 重算失败 {} {}: {}", account_id, date, e);
        }
    }
    info!("[report] T+1 补盯市 snapshot 重算完成({} 账户, {})", accounts.len(), date);
}

pub async fn push_daily_performance_report(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    use super::dingtalk;

    // P0 双保险:日报生成前强制复权因子兜底。即使 EOD 中段失败(如 daily_basic 卡死),
    // 日报前再补一次,确保 adj 视图不退化,回测偏离计算不被污染数据干扰。
    let dv_adj_id = format!("dv-adj-report-{}", date.format("%Y%m%d"));
    crate::routes::sync::market_data::backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

    let accounts = sqlx::query_as::<_, (String, String, Option<String>, f64, f64, f64, f64, i32)>(
        "SELECT paper_account_id, name, dingtalk_webhook_url,
                COALESCE(current_nav, initial_capital)::double precision,
                initial_capital::double precision,
                COALESCE(peak_nav, initial_capital)::double precision,
                COALESCE(max_drawdown_pct, 0)::double precision,
                COALESCE(total_trades, 0)
         FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("account query: {}", e))?;

    for (account_id, name, webhook, nav, init_cap, peak_nav, max_dd, total_trades) in &accounts {
        // 1. 昨日 snapshot(算当日收益率) + 持仓数
        let prev_nav: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT nav FROM paper_nav_snapshot WHERE paper_account_id = $1 AND snapshot_date < $2
             ORDER BY snapshot_date DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(date)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let prev_nav_f = prev_nav
            .map(|(d,)| d.to_string().parse::<f64>().unwrap_or(*nav))
            .unwrap_or(*init_cap);
        let daily_return = if prev_nav_f > 0.0 {
            (*nav - prev_nav_f) / prev_nav_f
        } else {
            0.0
        };
        let cumulative_return = if *init_cap > 0.0 {
            (*nav - *init_cap) / *init_cap
        } else {
            0.0
        };

        let pos_row: Option<(i64, rust_decimal::Decimal, rust_decimal::Decimal)> = sqlx::query_as(
            "SELECT COUNT(*)::bigint,
                    COALESCE(SUM(quantity * COALESCE(market_price, avg_cost)), 0),
                    COALESCE((SELECT cash FROM paper_account WHERE paper_account_id = $1), 0)
             FROM paper_position WHERE paper_account_id = $1 AND quantity > 0",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let (position_count, market_value, cash) = pos_row.unwrap_or((0, rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO));

        // 2. 当日成交汇总(买/卖笔数+金额)。paper_order 无 price 列，成交金额用 target_value
        // (下单时已按 target_price 算好的目标金额，比 quantity*target_price 更贴近实际口径)。
        let trade_row: Option<(i64, i64, f64, f64)> = sqlx::query_as(
            "SELECT
                COUNT(*) FILTER (WHERE side='buy')::bigint,
                COUNT(*) FILTER (WHERE side='sell')::bigint,
                COALESCE(SUM(target_value) FILTER (WHERE side='buy'), 0)::double precision,
                COALESCE(SUM(target_value) FILTER (WHERE side='sell'), 0)::double precision
             FROM paper_order WHERE paper_account_id = $1 AND DATE(created_at) = $2 AND status = 'filled'",
        )
        .bind(account_id)
        .bind(date)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let (buy_n, sell_n, buy_amt, sell_amt) = trade_row.unwrap_or((0, 0, 0.0, 0.0));
        let today_trades = buy_n + sell_n;

        // 3. 回测基准对比：用近 10 个自然日(约 5-7 交易日)滚动窗口，实盘与回测各自算窗口内累计收益之差。
        // 口径必须对齐同一时间窗——不能拿"自 initial_capital 的实盘复利"比"回测自 2014 年起的收益"，
        // 也不能用 paper_nav_snapshot 里最早一条记录做锚点(历史回放遗留可回溯到 2020 年，非 v24
        // 真正上线日)。滚动窗口天然规避这两个陷阱，且更贴合"近期漂移检测"的监控意图。
        //
        // P1 修复:对标基准从"纯 A 股选股曲线"改为"composite 多资产合成曲线"(消除结构性偏差),
        // 且 bt_ret 按账号 leverage_multiplier 放大(与实盘杠杆口径对齐)。composite 曲线缺失时
        // 回退旧的 A 股曲线逻辑(向后兼容)。
        let acct_meta: Option<(Option<String>, f64)> = sqlx::query_as(
            "SELECT strategy_version_id, COALESCE(leverage_multiplier, 1.0)::double precision \
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let (strategy_version_id, leverage_multiplier) = acct_meta.unwrap_or((None, 1.0));
        let mut backtest_deviation: Option<f64> = None;
        if let Some(ref sv_id) = strategy_version_id {
            // 优先读 composite 合成曲线(偏离监控正确对标);缺失则回退 A 股曲线
            let window_start = date - chrono::Duration::days(10);
            // 实盘窗口起点 NAV：窗口内最早一条 snapshot(若窗口内暂无历史,退化为今日,偏离记为 0)
            let live_anchor: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
                "SELECT nav FROM paper_nav_snapshot
                 WHERE paper_account_id = $1 AND snapshot_date >= $2 AND snapshot_date < $3
                 ORDER BY snapshot_date ASC LIMIT 1",
            )
            .bind(account_id)
            .bind(window_start)
            .bind(date)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
            if let Some((anchor_nav,)) = live_anchor {
                let anchor_nav_f = anchor_nav.to_string().parse::<f64>().unwrap_or(0.0);
                if anchor_nav_f > 0.0 {
                    // 优先:composite 合成曲线(strategy_id = sv_id)
                    // 先检测 composite 是否有当日数据:盘后 Tushare 日线常延迟到次日 09:00 T+1 才入库,
                    // 若 composite 缺当日(只有 <=昨日),用昨日 bt_end 对比今日 live_end 会造成窗口错位误报。
                    // 此时跳过偏离计算,日报显示"数据未就绪",避免 -5% 级假性偏离告警。
                    let composite_has_today: Option<(i64,)> = sqlx::query_as(
                        "SELECT COUNT(*)::bigint FROM backtest_composite_equity_curve
                         WHERE strategy_id=$1 AND trade_date = $2",
                    )
                    .bind(sv_id)
                    .bind(date)
                    .fetch_optional(db)
                    .await
                    .ok()
                    .flatten();
                    let composite_ready = composite_has_today.map(|(c,)| c > 0).unwrap_or(false);
                    // composite 缺当日数据时跳过偏离计算(A 股回退曲线同源于日线数据,也会缺当日,
                    // 强行回退仍会窗口错位误报)。backtest_deviation 保持 None,日报显示"数据未就绪"。
                    let composite_row: Option<(rust_decimal::Decimal, rust_decimal::Decimal)> = if composite_ready {
                        sqlx::query_as(
                            "SELECT
                                (SELECT portfolio_value FROM backtest_composite_equity_curve WHERE strategy_id=$1 AND trade_date >= $2 ORDER BY trade_date ASC LIMIT 1),
                                (SELECT portfolio_value FROM backtest_composite_equity_curve WHERE strategy_id=$1 AND trade_date <= $3 ORDER BY trade_date DESC LIMIT 1)",
                        )
                        .bind(sv_id)
                        .bind(window_start)
                        .bind(date)
                        .fetch_optional(db)
                    .await
                    .ok()
                    .flatten()
                    } else {
                        None
                    };
                    let bt_row = composite_row;

                    if let Some((bt_start, bt_end)) = bt_row {
                        let bt_start_f = bt_start.to_string().parse::<f64>().unwrap_or(0.0);
                        let bt_end_f = bt_end.to_string().parse::<f64>().unwrap_or(0.0);
                        if bt_start_f > 0.0 {
                            let live_ret = (*nav - anchor_nav_f) / anchor_nav_f;
                            // composite 曲线本身无杠杆,按账号 leverage_multiplier 放大回测收益
                            let bt_ret = (bt_end_f / bt_start_f - 1.0) * leverage_multiplier;
                            backtest_deviation = Some(live_ret - bt_ret);
                        }
                    }
                }
            }
        }

        // 4. 写当日 snapshot(实盘路径此前从不写，此处首次补齐)。
        // R5: 统一走 upsert_nav_snapshot（原裸 SQL 5 处重复之一）。
        // 注意不显式传 strategy_version_id：该字段有 FK -> strategy_version 表，
        // 而 v23/v24 等复合策略版本号不在该表中（该表仅存 phase7-professional-v1 等底层版本）。
        // 显式传入不存在的值会触发 FK 约束，静默失败。
        let mut snap = NavSnapshot::new(account_id, date, *nav);
        snap.cash = cash.to_string().parse::<f64>().unwrap_or(0.0);
        snap.market_value = market_value.to_string().parse::<f64>().unwrap_or(0.0);
        snap.position_count = position_count as i32;
        snap.daily_return = Some(daily_return);
        snap.cumulative_return = Some(cumulative_return);
        snap.max_drawdown = Some(*max_dd);
        snap.trade_count = Some(today_trades as i32);
        if let Err(e) = upsert_nav_snapshot(db, &snap).await {
            warn!("[report] {} snapshot 写入失败: {}", name, e);
        }

        // 5. 偏离 >2% 告警
        if let Some(dev) = backtest_deviation {
            if dev.abs() > 0.02 {
                send_quality_alert(
                    db,
                    &[format!(
                        "{}: 实盘累计收益 {:.2}% 与回测基准偏离 {:.2}%（超过 2% 阈值）",
                        name,
                        cumulative_return * 100.0,
                        dev * 100.0
                    )],
                )
                .await;
            }
        }

        // 6. 推送钉钉日报
        let webhook_url = match webhook {
            Some(u) if !u.is_empty() => u.clone(),
            _ => match dingtalk::build_dingtalk_webhook_url() {
                Some(u) => u,
                None => continue,
            },
        };
        // 当日涨跌箭头+颜色标记（突出显示当日表现）
        // 中国股市习惯：涨红跌绿（与欧美相反）
        let (daily_arrow, daily_color, daily_sign) = if daily_return >= 0.0 {
            ("📈", "🔴", "+")
        } else {
            ("📉", "🟢", "")
        };

        // 当日交易明细摘要（买卖前 5 笔，超 10 笔折叠）
        let trades = fetch_today_trades(db, account_id, date).await.unwrap_or_default();
        let trade_detail = if trades.is_empty() {
            String::new()
        } else {
            let buys: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "buy").collect();
            let sells: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "sell").collect();
            let mut detail = String::new();
            if !buys.is_empty() {
                let shown: Vec<String> = buys.iter().take(5).map(|t| format!("{} {}", t.symbol, t.name)).collect();
                let suffix = if buys.len() > 5 { format!(" 等 {} 笔", buys.len()) } else { String::new() };
                detail.push_str(&format!("  买: {}{}\n", shown.join(" | "), suffix));
            }
            if !sells.is_empty() {
                let shown: Vec<String> = sells.iter().take(5).map(|t| format!("{} {}", t.symbol, t.name)).collect();
                let suffix = if sells.len() > 5 { format!(" 等 {} 笔", sells.len()) } else { String::new() };
                detail.push_str(&format!("  卖: {}{}\n", shown.join(" | "), suffix));
            }
            detail
        };

        let text = format!(
            "## {} 实盘绩效日报 — {}  \n\n\
             **日期**: {}  \n\n\
             **当日收益**: {}{}{:.2}% | **累计收益**: {:.2}%  \n\
             **当前 NAV**: ¥{:.2} | **历史峰值**: ¥{:.2} | **最大回撤**: {:.2}%  \n\
             **当日调仓**: 买入 {} 笔(¥{:.0}) / 卖出 {} 笔(¥{:.0})  \n\
             **累计成交**: {} 笔  \n\
             {}\
             {}\n\n\
             > 自动生成于 {}",
            daily_arrow,
            name,
            date.format("%Y-%m-%d"),
            daily_color,
            daily_sign,
            daily_return.abs() * 100.0,
            cumulative_return * 100.0,
            nav,
            peak_nav,
            max_dd * 100.0,
            buy_n,
            buy_amt,
            sell_n,
            sell_amt,
            total_trades,
            trade_detail,
            match backtest_deviation {
                Some(dev) if dev.abs() > 0.02 => {
                    format!("**⚠️ 回测偏离**: {:.2}%（超 2% 阈值）  \n", dev * 100.0)
                }
                Some(dev) => format!("**回测偏离**: {:.2}%（正常范围）  \n", dev * 100.0),
                None => "**回测偏离**: 当日行情同步中，次日 9:00 T+1 补齐  \n".to_string(),
            },
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        );
        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, "实盘绩效日报", &text).await
        {
            warn!("[dingtalk] {} 日报推送失败: {}", name, e);
        } else {
            info!("[dingtalk] {} 日报推送成功", name);
        }
    }
    Ok(())
}

/// Public wrapper，供 API 端点调用。
pub async fn push_dingtalk_for_all_accounts_public(
    db: &PgPool,
    date: NaiveDate,
) -> Result<(), String> {
    push_dingtalk_for_all_accounts(db, date).await
}

/// P2-3: 调仓完成后立即写一份 `paper_nav_snapshot`(不等 16:00 EOD)。
///
/// 只做「写快照」这一件事，不做回测偏离对比/钉钉日报(那是 EOD `push_daily_performance_report`
/// 的职责)。同一天 EOD 会用收盘价 ON CONFLICT DO UPDATE 覆盖此处的调仓后快照，两者互不冲突——
/// 意义在于:即使 EOD 任务当天失败，调仓后的持仓状态也已经落库，不会完全空档。
pub async fn snapshot_positions_for_all_accounts(db: &PgPool, date: NaiveDate) {
    let accounts = sqlx::query_as::<_, (String, f64, f64)>(
        "SELECT paper_account_id, COALESCE(current_nav, initial_capital)::double precision,
                initial_capital::double precision
         FROM paper_account WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    for (account_id, nav, init_cap) in &accounts {
        let prev_nav: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT nav FROM paper_nav_snapshot WHERE paper_account_id = $1 AND snapshot_date < $2
             ORDER BY snapshot_date DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(date)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let prev_nav_f = prev_nav
            .map(|(d,)| d.to_string().parse::<f64>().unwrap_or(*nav))
            .unwrap_or(*init_cap);
        let daily_return = if prev_nav_f > 0.0 {
            (*nav - prev_nav_f) / prev_nav_f
        } else {
            0.0
        };
        let cumulative_return = if *init_cap > 0.0 {
            (*nav - *init_cap) / *init_cap
        } else {
            0.0
        };

        let pos_row: Option<(i64, rust_decimal::Decimal, rust_decimal::Decimal)> = sqlx::query_as(
            "SELECT COUNT(*)::bigint,
                    COALESCE(SUM(quantity * COALESCE(market_price, avg_cost)), 0),
                    COALESCE((SELECT cash FROM paper_account WHERE paper_account_id = $1), 0)
             FROM paper_position WHERE paper_account_id = $1 AND quantity > 0",
        )
        .bind(account_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        let (position_count, market_value, cash) =
            pos_row.unwrap_or((0, rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO));

        let today_trades: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_order
             WHERE paper_account_id = $1 AND DATE(created_at) = $2 AND status = 'filled'",
        )
        .bind(account_id)
        .bind(date)
        .fetch_one(db)
        .await
        .unwrap_or(0);

        // R5: 统一走 upsert_nav_snapshot（原裸 SQL 5 处重复之一）。
        // strategy_version_id 不显式传(同 push_daily_performance_report 的 FK 踩坑说明)。
        let mut snap = NavSnapshot::new(account_id, date, *nav);
        snap.cash = cash.to_string().parse::<f64>().unwrap_or(0.0);
        snap.market_value = market_value.to_string().parse::<f64>().unwrap_or(0.0);
        snap.position_count = position_count as i32;
        snap.daily_return = Some(daily_return);
        snap.cumulative_return = Some(cumulative_return);
        snap.trade_count = Some(today_trades as i32);
        if let Err(e) = upsert_nav_snapshot(db, &snap).await {
            warn!("[scheduler] {} 调仓后快照写入失败: {}", account_id, e);
        }
    }
}

async fn push_dingtalk_for_all_accounts(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    use super::dingtalk;
    use serde_json::{json, Value};

    let accounts = sqlx::query_as::<_, (String, String, String, Option<String>, Option<f64>, Option<f64>, Option<f64>)>(
        "SELECT paper_account_id, name, account_type, dingtalk_webhook_url, current_nav::double precision,
                COALESCE(cash, initial_capital)::double precision, COALESCE(margin_amount,0)::double precision
         FROM paper_account WHERE status='active' AND account_type='simulated' AND user_id IS NOT NULL",
    ).fetch_all(db).await.map_err(|e| format!("acct: {}", e))?;

    for (id, name, acct_type, webhook, nav, cash, margin) in &accounts {
        let webhook_url = match webhook {
            Some(u) if !u.is_empty() => u.clone(),
            _ => match dingtalk::build_dingtalk_webhook_url() {
                Some(u) => u,
                None => {
                    warn!("[dingtalk] {} 无 webhook", name);
                    continue;
                }
            },
        };

        // Load positions with stock names (ETFs get friendly names via CASE)
        let pos_rows = sqlx::query_as::<_, (String, Option<f64>, Option<f64>, Option<String>)>(
            "SELECT pp.symbol, pp.quantity::double precision,
                    COALESCE(pp.market_price,pp.avg_cost)::double precision,
                    COALESCE(ms.name,
                      CASE pp.symbol
                        WHEN '518880.SH' THEN '黄金ETF'
                        WHEN '511010.SH' THEN '国债ETF'
                        WHEN '513500.SH' THEN '标普500ETF'
                        WHEN '513100.SH' THEN '纳指ETF'
                        ELSE NULL END,
                      pp.symbol)
             FROM paper_position pp
             LEFT JOIN market_stock ms ON ms.symbol = pp.symbol
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
             ORDER BY pp.quantity*COALESCE(pp.market_price,pp.avg_cost) DESC",
        )
        .bind(id)
        .fetch_all(db)
        .await
        .map_err(|e| format!("pos: {}", e))?;

        let positions: Vec<Value> = pos_rows
            .iter()
            .filter_map(|(s, q, p, n)| {
                let q = q.unwrap_or(0.0);
                let p = p.unwrap_or(0.0);
                if q <= 0.0 {
                    None
                } else {
                    Some(json!({
                        "symbol":s, "name": n.as_deref().unwrap_or(s),
                        "quantity":q, "current_price":p, "market_value":q*p
                    }))
                }
            })
            .collect();

        // 资产大类分布：ETF 按品种单独列出，A 股汇总
        let class_rows = sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT CASE
                      WHEN pp.symbol = '518880.SH' THEN '黄金ETF'
                      WHEN pp.symbol = '511010.SH' THEN '国债ETF'
                      WHEN pp.symbol = '513500.SH' THEN '美股标普ETF'
                      WHEN pp.symbol = '513100.SH' THEN '美股纳指ETF'
                      WHEN pp.symbol = '159980.SZ' THEN '有色ETF'
                      WHEN pp.symbol = '159985.SZ' THEN '豆粕ETF'
                      WHEN pp.symbol = '501018.SH' THEN '原油LOF'
                      WHEN pp.symbol = '511880.SH' THEN '货币基金'
                      ELSE 'A股' END AS asset_class,
                    SUM(pp.quantity * COALESCE(pp.market_price, pp.avg_cost))::double precision AS mv
             FROM paper_position pp
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
             GROUP BY 1
             ORDER BY SUM(2) DESC",
        ).bind(id).fetch_all(db).await.unwrap_or_default();

        let total_nav = nav.unwrap_or(0.0);
        let cash_val = cash.unwrap_or(total_nav);
        let margin_val = margin.unwrap_or(0.0);
        let mv: f64 = positions
            .iter()
            .filter_map(|p| p.get("market_value").and_then(|v| v.as_f64()))
            .sum();
        let net_worth = mv + cash_val - margin_val; // 净资产 = 持仓市值 + 现金 - 融资金额

        let mut class_breakdown: Vec<Value> = class_rows
            .iter()
            .map(|(cls, v)| {
                let val = v.unwrap_or(0.0);
                let pct = if total_nav > 0.0 {
                    val / total_nav * 100.0
                } else {
                    0.0
                };
                // 字段名对齐 dingtalk::build_position_summary_notification 渲染方(name/pct)
                json!({"name": cls, "pct": (pct*100.0).round()/100.0})
            })
            .collect();
        // 现金单独列出
        if cash_val > 1.0 {
            let cash_pct = if net_worth > 0.0 {
                cash_val / net_worth * 100.0
            } else {
                0.0
            };
            class_breakdown.push(json!({"name": "现金", "pct": (cash_pct*100.0).round()/100.0}));
        }
        let init_row = sqlx::query_as::<_, (Option<f64>, Option<f64>)>(
            "SELECT initial_capital::double precision, max_drawdown_pct::double precision FROM paper_account WHERE paper_account_id=$1"
        ).bind(id).fetch_optional(db).await.map_err(|e| format!("init: {}", e))?.unwrap_or((Some(total_nav), Some(0.0)));

        let init = init_row.0.unwrap_or(total_nav);
        let cum_ret = if init > 0.0 {
            (total_nav - init) / init
        } else {
            0.0
        };
        let mdd = init_row.1.unwrap_or(0.0);

        let text = dingtalk::build_position_summary_notification(
            name,
            acct_type,
            &date.format("%Y-%m-%d").to_string(),
            total_nav,
            cash_val,
            margin_val,
            mv,
            net_worth,
            &positions,
            cum_ret,
            mdd,
            &class_breakdown,
        );

        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, "持仓摘要", &text).await
        {
            warn!("[dingtalk] {} 发送失败: {}", name, e);
        } else {
            info!("[dingtalk] {} 推送成功", name);
        }
    }
    Ok(())
}

/// 当日交易明细行（共用数据结构，日报与交易明细通知复用）。
struct TradeRow {
    symbol: String,
    side: String,
    quantity: f64,
    amount: f64,
    reason: String,
    name: String,
}

/// 查询某账户当日已成交订单明细（symbol/side/数量/金额/理由/名称）。
/// 名称解析复用持仓摘要的 CASE 映射 + market_stock LEFT JOIN。
async fn fetch_today_trades(
    db: &PgPool,
    account_id: &str,
    date: NaiveDate,
) -> Result<Vec<TradeRow>, String> {
    let rows: Vec<(String, String, f64, f64, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT po.symbol, po.side,
                po.quantity::double precision,
                po.target_value::double precision,
                po.reason,
                COALESCE(ms.name,
                  CASE po.symbol
                    WHEN '518880.SH' THEN '黄金ETF'
                    WHEN '511010.SH' THEN '国债ETF'
                    WHEN '513500.SH' THEN '标普500ETF'
                    WHEN '513100.SH' THEN '纳指ETF'
                    WHEN '159980.SZ' THEN '有色ETF'
                    WHEN '159985.SZ' THEN '豆粕ETF'
                    WHEN '501018.SH' THEN '原油LOF'
                    WHEN '511880.SH' THEN '货币基金'
                    ELSE NULL END,
                  po.symbol)
         FROM paper_order po
         LEFT JOIN market_stock ms ON ms.symbol = po.symbol
         WHERE po.paper_account_id = $1 AND DATE(po.created_at) = $2 AND po.status = 'filled'
         ORDER BY po.side, po.target_value DESC",
    )
    .bind(account_id)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch_today_trades: {}", e))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, side, quantity, amount, reason, name)| TradeRow {
            symbol,
            side,
            quantity,
            amount,
            reason: reason.unwrap_or_default(),
            name: name.unwrap_or_else(|| String::new()),
        })
        .collect())
}

/// 调仓后推送「今日交易明细」钉钉通知（每账户独立，含买卖明细+理由）。
pub async fn push_dingtalk_trade_detail_notification(
    db: &PgPool,
    date: NaiveDate,
) -> Result<(), String> {
    use super::dingtalk;

    let accounts = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT paper_account_id, name, dingtalk_webhook_url
         FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated' AND user_id IS NOT NULL",
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("trade detail acct: {}", e))?;

    for (account_id, name, webhook) in &accounts {
        let trades = fetch_today_trades(db, account_id, date).await.unwrap_or_default();
        if trades.is_empty() {
            continue; // 无成交跳过（不推送空通知）
        }

        let webhook_url = match webhook {
            Some(u) if !u.is_empty() => u.clone(),
            _ => match dingtalk::build_dingtalk_webhook_url() {
                Some(u) => u,
                None => continue,
            },
        };

        let buys: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "buy").collect();
        let sells: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "sell").collect();
        let total = trades.len();

        // 统一表格：按方向分组（买入在前），每行含交易方向列，表格对齐
        let mut rows = Vec::new();
        for t in &buys {
            rows.push(format!(
                "| 买入 | {} | {} | {:.0} | ¥{:.0} | {} |",
                t.symbol, t.name, t.quantity, t.amount, t.reason
            ));
        }
        for t in &sells {
            rows.push(format!(
                "| 卖出 | {} | {} | {:.0} | ¥{:.0} | {} |",
                t.symbol, t.name, t.quantity, t.amount, t.reason
            ));
        }

        let text = format!(
            "## 📋 今日交易明细 - {}  \n\n\
             **日期**: {}  |  **总成交**: {} 笔（买入 {} / 卖出 {}）  \n\n\
             | 方向 | 标的 | 名称 | 数量 | 金额 | 理由 |\n\
             |:----:|:-----|:-----|-----:|-----:|:-----|\n\
             {}\n\n\
             > 调仓于 14:40 执行",
            name,
            date.format("%Y-%m-%d"),
            total,
            buys.len(),
            sells.len(),
            rows.join("\n"),
        );

        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, "今日交易明细", &text).await
        {
            warn!("[dingtalk] {} 交易明细推送失败: {}", name, e);
        } else {
            info!("[dingtalk] {} 交易明细推送成功", name);
        }
    }
    Ok(())
}

#[cfg(test)]
mod daily_report_tests {
    use super::*;

    /// P2-1 集成测试(接真实生产库,验证 push_daily_performance_report 不 panic
    /// 且正确写入 paper_nav_snapshot)。运行: cargo test --lib -- --ignored push_daily_performance_report_writes_snapshot
    #[tokio::test]
    #[ignore]
    async fn push_daily_performance_report_writes_snapshot() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");

        let date = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let result = push_daily_performance_report(&db, date).await;
        assert!(result.is_ok(), "push_daily_performance_report 失败: {:?}", result.err());

        // 验证两个 v24 生产账号的当日 snapshot 已写入
        let rows: Vec<(String, rust_decimal::Decimal, Option<rust_decimal::Decimal>)> = sqlx::query_as(
            "SELECT paper_account_id, nav, cumulative_return FROM paper_nav_snapshot
             WHERE paper_account_id IN ('pa-v21-prod-lev','pa-v21-prod-unlev') AND snapshot_date = $1",
        )
        .bind(date)
        .fetch_all(&db)
        .await
        .expect("query snapshot");
        assert_eq!(rows.len(), 2, "两个生产账号都应有 7/20 snapshot");
        for (acct_id, nav, cum_ret) in &rows {
            let nav_f = nav.to_string().parse::<f64>().unwrap();
            assert!(nav_f > 0.0, "{} nav 应 > 0", acct_id);
            assert!(cum_ret.is_some(), "{} cumulative_return 应已计算", acct_id);
        }
    }
}
