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

use crate::routes::shared::{
    send_quality_alert, upsert_nav_snapshot, NavSnapshot, PaperAccountRepository,
    PaperPositionRepository, PgPaperAccountRepo, PgPaperPositionRepo,
};

/// T+1 补盯市后的快照重算(不推钉钉)。
///
/// 20:00 EOD 时 Tushare fund_daily 常无当日数据(实测 0 rows),ETF 持仓的日终
/// 盯市被迫落在前日收盘;次日 9:00 T+1 补同步后 ETF 日线就绪,调用方先
/// mark_to_market(date) + update_current_nav,再调本函数按当前 NAV 重写 date 的
/// snapshot(nav/daily_return/cumulative_return),补齐真实日终口径。
/// 幂等:ON CONFLICT 覆盖,重复调用无副作用。
pub async fn refresh_eod_snapshot(db: &PgPool, date: NaiveDate) {
    // 仅处理「snapshot 缺失或 daily_return IS NULL」的账户(即昨日日终数据不完整日):
    // 无条件重写会把完整日的 trade_count 等 upsert 覆盖为 NULL(NavSnapshot 未填的字段)。
    let accounts: Vec<(String, f64, f64, f64)> = sqlx::query_as(
        "SELECT pa.paper_account_id,
                COALESCE(pa.current_nav, pa.initial_capital)::double precision,
                COALESCE(pa.initial_capital, 0)::double precision,
                COALESCE(pa.cash, 0)::double precision
         FROM paper_account pa
         LEFT JOIN paper_nav_snapshot s
                ON s.paper_account_id = pa.paper_account_id AND s.snapshot_date = $1
         WHERE pa.status = 'active' AND pa.account_type = 'simulated'
           AND (s.paper_account_id IS NULL OR s.daily_return IS NULL)",
    )
    .bind(date)
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
        let (position_count, market_value) = pos.unwrap_or((0, rust_decimal::Decimal::ZERO));

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
            warn!(
                "[report] T+1 补盯市 snapshot 重算失败 {} {}: {}",
                account_id, date, e
            );
        }
    }
    info!(
        "[report] T+1 补盯市 snapshot 重算完成({} 账户, {})",
        accounts.len(),
        date
    );
}

/// EOD 日报(全部 active 账户):偏离对比 + 快照写入 + 钉钉推送。
pub async fn push_daily_performance_report(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    report_for_accounts(db, date, None, false).await
}

/// 次日 9:00 T+1 补发:仅指定账户(昨日日终数据缺失、daily_return 为 NULL 的账户),
/// 标题带"(补发)"。数据已在 T+1 补盯市流程中补齐,此处重算并推送昨日日报。
pub async fn resend_daily_performance_report(
    db: &PgPool,
    date: NaiveDate,
    only_accounts: &[String],
) -> Result<(), String> {
    report_for_accounts(db, date, Some(only_accounts), true).await
}

async fn report_for_accounts(
    db: &PgPool,
    date: NaiveDate,
    only_accounts: Option<&[String]>,
    resend: bool,
) -> Result<(), String> {
    use super::dingtalk;

    // P0 双保险:日报生成前强制复权因子兜底。即使 EOD 中段失败(如 daily_basic 卡死),
    // 日报前再补一次,确保 adj 视图不退化,回测偏离计算不被污染数据干扰。
    let dv_adj_id = format!("dv-adj-report-{}", date.format("%Y%m%d"));
    crate::routes::sync::market_data::backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

    let accounts = match only_accounts {
        Some(list) => {
            sqlx::query_as::<_, (String, String, Option<String>, f64, f64, f64, f64, i32)>(
                "SELECT paper_account_id, name, dingtalk_webhook_url,
                        COALESCE(current_nav, initial_capital)::double precision,
                        initial_capital::double precision,
                        COALESCE(peak_nav, initial_capital)::double precision,
                        COALESCE(max_drawdown_pct, 0)::double precision,
                        COALESCE(total_trades, 0)
                 FROM paper_account
                 WHERE status = 'active' AND account_type = 'simulated'
                   AND paper_account_id = ANY($1)",
            )
            .bind(list)
            .fetch_all(db)
            .await
            .map_err(|e| format!("account query: {}", e))?
        }
        None => sqlx::query_as::<_, (String, String, Option<String>, f64, f64, f64, f64, i32)>(
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
        .map_err(|e| format!("account query: {}", e))?,
    };

    for (account_id, name, webhook, nav, init_cap, peak_nav, max_dd, total_trades) in &accounts {
        // 0. 日终数据完整性:持仓 symbol 当日 bar 是否齐全(如 Tushare fund_delay 延迟)。
        // 不完整时当日收益显示 --(snapshot.daily_return 置 NULL),次日 9:00 T+1 补发。
        let missing_bars: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_position pp
             WHERE pp.paper_account_id = $1 AND pp.quantity > 0
               AND NOT EXISTS (SELECT 1 FROM market_stock_daily_bar b
                               WHERE b.symbol = pp.symbol AND b.trade_date = $2)",
        )
        .bind(account_id)
        .bind(date)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
        let data_complete = missing_bars == 0;

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
        // 日终数据不完整时不产出当日收益(NULL):NAV 仍是昨收估值口径,算出来的
        // daily_return 不是当日真实涨跌,宁缺毋滥,次日 9:00 补发。
        let daily_return: Option<f64> = if !data_complete {
            None
        } else if prev_nav_f > 0.0 {
            Some((*nav - prev_nav_f) / prev_nav_f)
        } else {
            Some(0.0)
        };
        let cumulative_return = if *init_cap > 0.0 {
            (*nav - *init_cap) / *init_cap
        } else {
            0.0
        };

        let pos_row: Option<(i64, rust_decimal::Decimal, rust_decimal::Decimal)> =
            PgPaperPositionRepo::new(db)
                .find_summary(account_id)
                .await
                .ok()
                .map(|s| (s.position_count, s.market_value, s.cash));
        let (position_count, market_value, cash) =
            pos_row.unwrap_or((0, rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO));

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
        let acct_meta: Option<(Option<String>, f64)> = PgPaperAccountRepo::new(db)
            .find_strategy_and_leverage(account_id)
            .await
            .ok()
            .flatten();
        let (strategy_version_id, leverage_multiplier) = acct_meta.unwrap_or((None, 1.0));
        let mut backtest_deviation: Option<f64> = None;
        // 绩效视角补充（2026-09-19 用户定版）：同 10 日窗口的实盘 vs 沪深300 超额——
        // 与回测偏离互补（偏离=执行保真度监控抓执行/数据事故，超额=策略 alpha 视角）。
        let mut benchmark_excess: Option<f64> = None;
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
                    let composite_row: Option<(rust_decimal::Decimal, rust_decimal::Decimal)> =
                        if composite_ready {
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

                            // 同窗口沪深300 超额（绩效视角；指数日线经 EOD 同步入库，
                            // 窗口首尾取最近可用交易日，不强制当日就绪——与 composite 的
                            // 当日就绪检测语义不同：超额是展示信息非告警，允许口径微滞后）
                            let bench_row: Option<
                                (rust_decimal::Decimal, rust_decimal::Decimal),
                            > = sqlx::query_as(
                                "SELECT
                                    (SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date >= $1 ORDER BY trade_date ASC LIMIT 1),
                                    (SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $2 ORDER BY trade_date DESC LIMIT 1)",
                            )
                            .bind(window_start)
                            .bind(date)
                            .fetch_optional(db)
                            .await
                            .ok()
                            .flatten();
                            if let Some((b_start, b_end)) = bench_row {
                                let b_start_f = b_start.to_string().parse::<f64>().unwrap_or(0.0);
                                let b_end_f = b_end.to_string().parse::<f64>().unwrap_or(0.0);
                                if b_start_f > 0.0 {
                                    let bench_ret = b_end_f / b_start_f - 1.0;
                                    benchmark_excess = Some(live_ret - bench_ret);
                                }
                            }
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
        // daily_return=None(数据不完整)时写 NULL:9:00 T+1 以此识别需补发的账户
        snap.daily_return = daily_return;
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
        // 中国股市习惯：涨红跌绿（与欧美相反）;数据不完整时无当日收益,中性图标
        let (daily_arrow, daily_color, daily_sign) = match daily_return {
            Some(dr) if dr >= 0.0 => ("📈", "🔴", "+"),
            Some(_) => ("📉", "🟢", ""),
            None => ("⏸", "⚪", ""),
        };

        // 当日交易明细摘要（买卖前 5 笔，超 10 笔折叠）
        let trades = fetch_today_trades(db, account_id, date)
            .await
            .unwrap_or_default();
        let trade_detail = if trades.is_empty() {
            String::new()
        } else {
            let buys: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "buy").collect();
            let sells: Vec<&TradeRow> = trades.iter().filter(|t| t.side == "sell").collect();
            let mut detail = String::new();
            if !buys.is_empty() {
                let shown: Vec<String> = buys
                    .iter()
                    .take(5)
                    .map(|t| format!("{} {}", t.symbol, t.name))
                    .collect();
                let suffix = if buys.len() > 5 {
                    format!(" 等 {} 笔", buys.len())
                } else {
                    String::new()
                };
                detail.push_str(&format!("  买: {}{}\n", shown.join(" | "), suffix));
            }
            if !sells.is_empty() {
                let shown: Vec<String> = sells
                    .iter()
                    .take(5)
                    .map(|t| format!("{} {}", t.symbol, t.name))
                    .collect();
                let suffix = if sells.len() > 5 {
                    format!(" 等 {} 笔", sells.len())
                } else {
                    String::new()
                };
                detail.push_str(&format!("  卖: {}{}\n", shown.join(" | "), suffix));
            }
            detail
        };

        // 当前回撤 = (峰值 - 当前 NAV)/峰值,与全周期 max_dd 分开标注避免误读
        let current_dd = if *peak_nav > 0.0 {
            (peak_nav - nav) / peak_nav
        } else {
            0.0
        };
        // 当日收益行:数据不完整时显示 --(宁缺毋滥,次日 09:00 T+1 补发)
        let daily_line = match daily_return {
            Some(dr) => format!("{}{}{:.2}%", daily_color, daily_sign, dr.abs() * 100.0),
            None => format!(
                "{}--(日终数据不完整:{} 持仓缺日线,次日 09:00 补发)",
                daily_color, missing_bars
            ),
        };
        let report_title = if resend {
            "实盘绩效日报(补发)"
        } else {
            "实盘绩效日报"
        };
        let text = format!(
            "## {} {} — {}  \n\n\
             **日期**: {}  \n\n\
             **当日收益**: {} | **累计收益**: {:.2}%  \n\
             **当前 NAV**: ¥{:.2} | **历史峰值**: ¥{:.2} | **当前回撤**: {:.2}% | **历史最大回撤**: {:.2}%  \n\
             **当日调仓**: 买入 {} 笔(¥{:.0}) / 卖出 {} 笔(¥{:.0})  \n\
             **累计成交**: {} 笔  \n\
             {}\
             {}\
             {}\n\n\
             > 自动生成于 {}",
            daily_arrow,
            name,
            report_title,
            date.format("%Y-%m-%d"),
            daily_line,
            cumulative_return * 100.0,
            nav,
            peak_nav,
            current_dd * 100.0,
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
            match benchmark_excess {
                Some(ex) => format!(
                    "**超额(vs 沪深300·10日)**: {}{:.2}%  \n",
                    if ex >= 0.0 { "+" } else { "" },
                    ex * 100.0
                ),
                None => String::new(),
            },
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        );
        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, report_title, &text).await {
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

        let pos_row: Option<(i64, rust_decimal::Decimal, rust_decimal::Decimal)> =
            PgPaperPositionRepo::new(db)
                .find_summary(account_id)
                .await
                .ok()
                .map(|s| (s.position_count, s.market_value, s.cash));
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
            name: name.unwrap_or_default(),
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
        let trades = fetch_today_trades(db, account_id, date)
            .await
            .unwrap_or_default();
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

/// report 域写库测试互斥锁（2026-09-22 修）：refresh_eod_snapshot /
/// snapshot_positions_for_all_accounts 是**全库 active 扫描**的产品函数，
/// 并行时会顺带给其它测试的中间态账户补快照（如 resend 测试的 rsA
/// 处于已建未写完整快照时被补 2027-06-15 行 → prev_nav 错乱 →
/// daily_return 断言失败）。report 域四个写库测试必须串行。
#[cfg(test)]
static REPORT_WRITE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod daily_report_tests {
    use super::*;

    /// P2-1 集成测试(接真实生产库,验证 push_daily_performance_report 不 panic
    /// 且正确写入 paper_nav_snapshot)。运行: cargo test --lib -- --ignored push_daily_performance_report_writes_snapshot
    #[tokio::test]
    #[ignore = "DB 集成测试(本机 PG),显式跑: cargo test -- --ignored"]
    async fn push_daily_performance_report_writes_snapshot() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");

        let date = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let result = push_daily_performance_report(&db, date).await;
        assert!(
            result.is_ok(),
            "push_daily_performance_report 失败: {:?}",
            result.err()
        );

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

    // ── 以下为非 ignored 常跑测试（Application 层覆盖率专项 2026-09-20）──
    //
    // push_daily_performance_report / resend / push_dingtalk_* 族会触发
    // backfill_adj_factor_for_date(Tushare 兜底) 与钉钉推送，不在直调范围；
    // 此处只覆盖私有查询函数 fetch_today_trades 与 refresh_eod_snapshot 的
    // 「全账户快照完整 → 空集」安全路径。

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 动态找一个「active 账户 + 有成交」的日期（数据只增不减，断言稳健）。
    async fn an_account_day_with_filled_orders(db: &sqlx::PgPool) -> (String, chrono::NaiveDate) {
        let row: Option<(String, chrono::NaiveDate)> = sqlx::query_as(
            "SELECT po.paper_account_id, DATE(po.created_at)
             FROM paper_order po
             JOIN paper_account pa ON pa.paper_account_id = po.paper_account_id
             WHERE po.status = 'filled'
               AND pa.status = 'active' AND pa.account_type = 'simulated'
             ORDER BY po.created_at DESC LIMIT 1",
        )
        .fetch_optional(db)
        .await
        .expect("query filled orders");
        row.expect("active 账户应存在历史成交（空库时此守卫失败提醒数据缺失）")
    }

    #[tokio::test]
    async fn fetch_today_trades_returns_filled_rows_with_sides() {
        let db = test_db().await;
        let (account_id, date) = an_account_day_with_filled_orders(&db).await;

        let trades = fetch_today_trades(&db, &account_id, date)
            .await
            .expect("fetch_today_trades ok");
        assert!(!trades.is_empty(), "{account_id} 在 {date} 应有成交明细");
        for t in &trades {
            assert!(!t.symbol.is_empty());
            assert!(
                t.side == "buy" || t.side == "sell",
                "side 只能是 buy/sell: {}",
                t.side
            );
            assert!(t.quantity > 0.0, "成交数量应 > 0");
        }
    }

    #[tokio::test]
    async fn fetch_today_trades_empty_for_day_without_orders() {
        let db = test_db().await;
        let (account_id, _) = an_account_day_with_filled_orders(&db).await;
        // 2019-06-15 早于全部历史单（最早为 SH 2020-01-02 回放单）。注意勿选
        // 2020-01-01：历史回放单 created_at=业务日 SH 零点（=UTC 前日 16:00），
        // sqlx session 时区 UTC 下 DATE() 比 psql（Asia/Shanghai）少一天，
        // 2020-01-01 在 UTC 视角恰好命中 2020-01-02 的回放单（实证 30 笔）。
        let trades = fetch_today_trades(
            &db,
            &account_id,
            chrono::NaiveDate::from_ymd_opt(2019, 6, 15).unwrap(),
        )
        .await
        .expect("fetch_today_trades ok");
        assert!(trades.is_empty(), "无成交日应返回空明细");
    }

    /// 已完整的快照日（所有 active simulated 账户 daily_return 非空）：
    /// refresh_eod_snapshot 命中账户集为空，不产生任何写入。
    /// 用「调用前后快照指纹不变」+「命中数为 0」双守卫防数据漂移误写。
    #[tokio::test]
    async fn refresh_eod_snapshot_is_noop_when_day_already_complete() {
        let _report_write_guard = super::REPORT_WRITE_TEST_LOCK.lock().await;
        let db = test_db().await;
        let date = chrono::NaiveDate::from_ymd_opt(2026, 9, 18).unwrap();

        // 守卫：该日无「缺失或 daily_return IS NULL」的 active 账户（与函数内谓词一致）
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_account pa
             LEFT JOIN paper_nav_snapshot s
                    ON s.paper_account_id = pa.paper_account_id AND s.snapshot_date = $1
             WHERE pa.status = 'active' AND pa.account_type = 'simulated'
               -- 排除 zzz 测试账户（2026-09-21 修：fifth_batch 并行造 zzz 账户时，
               -- 「已建未写快照」中间态会被本守卫扫到，属窗口竞态非数据不完整）
               AND pa.paper_account_id NOT LIKE 'zzz%'
               AND (s.paper_account_id IS NULL OR s.daily_return IS NULL)",
        )
        .bind(date)
        .fetch_one(&db)
        .await
        .unwrap_or(-1);
        assert_eq!(pending, 0, "守卫失败：{date} 存在待补账户，换完整日期再测");

        async fn day_fingerprint(db: &sqlx::PgPool, date: NaiveDate) -> String {
            // 排除 zzz 账户（2026-09-22 修，与守卫同口径）：产品 refresh_eod_snapshot
            // 不识测试前缀，会顺带给并行中「已建无快照」的 zzz 账户补当日快照行
            // （并行实证 COUNT 4→7、事后被各测试 cleanup 删除故查无实据）——
            // 本测试验证「完整日的真实账户无写入」，指纹口径须与之一致
            sqlx::query_scalar::<_, String>(
                "SELECT COUNT(*)::text || ':' || COALESCE(MAX(nav::text),'') || ':' \
                 || COUNT(daily_return)::text
                 FROM paper_nav_snapshot WHERE snapshot_date = $1 \
                 AND paper_account_id NOT LIKE 'zzz%'",
            )
            .bind(date)
            .fetch_one(db)
            .await
            .unwrap_or_default()
        }
        let before = day_fingerprint(&db, date).await;

        refresh_eod_snapshot(&db, date).await; // 空集路径：不 panic、不写入

        let after = day_fingerprint(&db, date).await;
        assert_eq!(before, after, "完整日的快照不得被重写");
    }
}

// ── 第五批覆盖率测试：快照重算/调仓后快照/日报补发/交易明细推送 ──
// 模式沿用 trading.rs db_second_batch：真实本机 PG，zzz_test_ 前缀独占键 +
// 前置+结尾精确键清理。钉钉推送用 loopback 无服务地址（127.0.0.1:1）触发
// 发送失败 warn 分支，绝不外发；远未来日期（2027-06-15）确保生产账户当日在
// 该测试视角下无快照/无成交，写入行结尾按精确键恢复原状。
#[cfg(test)]
mod fifth_batch {
    use super::*;

    /// 测试专用远未来日期：生产账户在该日无快照、无成交，写入可精确恢复。
    /// 各测试用不同日期（cargo test 并行），守卫与恢复互不交叉删除。
    const FUTURE_DAY: NaiveDate = chrono::NaiveDate::from_ymd_opt(2027, 6, 15).unwrap();
    const FUTURE_DAY_2: NaiveDate = chrono::NaiveDate::from_ymd_opt(2027, 6, 16).unwrap();
    const FUTURE_DAY_3: NaiveDate = chrono::NaiveDate::from_ymd_opt(2027, 6, 17).unwrap();
    const PREV_DAY: NaiveDate = chrono::NaiveDate::from_ymd_opt(2027, 6, 14).unwrap();

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    async fn cleanup_account(db: &sqlx::PgPool, account_id: &str) {
        for sql in [
            "DELETE FROM paper_fill WHERE paper_account_id = $1",
            "DELETE FROM paper_order WHERE paper_account_id = $1",
            "DELETE FROM paper_margin_trade WHERE paper_account_id = $1",
            "DELETE FROM paper_position WHERE paper_account_id = $1",
            "DELETE FROM paper_nav_snapshot WHERE paper_account_id = $1",
            // 账户行本身也要删（2026-09-21 修：残留 active simulated 账户会让
            // daily_report_tests 的"待补账户"守卫误炸）
            "DELETE FROM paper_account WHERE paper_account_id = $1",
        ] {
            let _ = sqlx::query(sql).bind(account_id).execute(db).await;
        }
        let _ = sqlx::query("DELETE FROM paper_account WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(db)
            .await;
    }

    /// 恢复远未来日的快照原状：删除非 zzz 账户在该日被本测试写入的行
    /// （前置守卫已验证该日原本 0 行）。
    async fn restore_future_day(db: &sqlx::PgPool, day: NaiveDate, zzz_accounts: &[&str]) {
        let _ = sqlx::query(
            "DELETE FROM paper_nav_snapshot
             WHERE snapshot_date = $1
               AND paper_account_id NOT IN (SELECT unnest($2::varchar[]))",
        )
        .bind(day)
        .bind(zzz_accounts)
        .execute(db)
        .await;
    }

    /// 造 zzz 模拟账户：current_nav/cash 参数化，user_id/webhook 供推送路径筛选。
    async fn create_zzz_account(
        db: &sqlx::PgPool,
        account_id: &str,
        current_nav: f64,
        cash: f64,
        user_id: Option<&str>,
    ) {
        cleanup_account(db, account_id).await;
        sqlx::query(
            "INSERT INTO paper_account
               (paper_account_id, name, initial_capital, cash, status, account_type,
                signal_source, current_nav, dingtalk_webhook_url, user_id)
             VALUES ($1, 'zzz 第五批日报测试', 100000, $2, 'active', 'simulated',
                'factor', $3, 'http://127.0.0.1:1/zzz', $4)",
        )
        .bind(account_id)
        .bind(cash)
        .bind(current_nav)
        .bind(user_id)
        .execute(db)
        .await
        .expect("insert zzz paper_account");
    }

    async fn insert_position(db: &sqlx::PgPool, account_id: &str, symbol: &str) {
        sqlx::query(
            "INSERT INTO paper_position
               (paper_position_id, paper_account_id, symbol, quantity, avg_cost,
                market_price, market_value)
             VALUES ($3, $1, $2, 1000, 12, 12, 12000)
             ON CONFLICT (paper_account_id, symbol) DO NOTHING",
        )
        .bind(account_id)
        .bind(symbol)
        .bind(format!("pp-zzz-r5-{}", symbol))
        .execute(db)
        .await
        .expect("insert zzz position");
    }

    /// 造前日快照（当日收益率的 prev 基准）。
    async fn insert_prev_snapshot(db: &sqlx::PgPool, account_id: &str, nav: f64) {
        let mut snap = crate::routes::shared::NavSnapshot::new(account_id, PREV_DAY, nav);
        snap.daily_return = Some(0.01);
        crate::routes::shared::upsert_nav_snapshot(db, &snap)
            .await
            .expect("prev snapshot");
    }

    /// 造当日已成交订单（created_at 取 date 正午 UTC：UTC/SH 双时区视角 DATE() 一致）。
    async fn insert_filled_order(
        db: &sqlx::PgPool,
        account_id: &str,
        order_id: &str,
        side: &str,
        target_value: f64,
        day: NaiveDate, // 2026-09-21 修：原写死 FUTURE_DAY，快照日为 FUTURE_DAY_2 时计数恒 0
    ) {
        let created: chrono::DateTime<chrono::Utc> =
            chrono::TimeZone::from_utc_datetime(&chrono::Utc, &day.and_hms_opt(12, 0, 0).unwrap());
        sqlx::query(
            "INSERT INTO paper_order
               (order_id, paper_account_id, symbol, side, order_type, quantity,
                status, target_value, created_at)
             VALUES ($1, $2, 'ZZZR01.SH', $3, 'market', 100, 'filled', $4, $5)",
        )
        .bind(order_id)
        .bind(account_id)
        .bind(side)
        .bind(target_value)
        .bind(created)
        .execute(db)
        .await
        .expect("insert zzz order");
    }

    #[tokio::test]
    async fn refresh_eod_snapshot_backfills_missing_daily_return() {
        let _report_write_guard = super::REPORT_WRITE_TEST_LOCK.lock().await;
        let db = test_db().await;
        let account_id = "zzz_test_api5_eod";
        // 守卫：远未来日原本无任何快照（有则说明环境异常，换日期再测）
        // 前置精确清理（2026-09-21 补：panic 残留防连锁失败，删 zzz 账户该日快照）
        sqlx::query("DELETE FROM paper_nav_snapshot WHERE snapshot_date = '2027-06-15'")
            .execute(&db)
            .await
            .unwrap();
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_nav_snapshot WHERE snapshot_date = '2027-06-15'",
        )
        .fetch_one(&db)
        .await
        .unwrap_or(-1);
        assert_eq!(existing, 0, "守卫失败：2027-06-15 已存在快照");

        // zzz 账户：nav=110000（current_nav），前日快照 105000，持仓 1 笔
        create_zzz_account(&db, account_id, 110_000.0, 5_000.0, None).await;
        insert_prev_snapshot(&db, account_id, 105_000.0).await;
        insert_position(&db, account_id, "ZZZR01.SH").await;

        refresh_eod_snapshot(&db, FUTURE_DAY).await;

        // 当日快照补齐：daily_return=(110000-105000)/105000，cumulative=(110000-100000)/100000
        let (nav, daily, cum, pos_n, mv, cash): (f64, Option<f64>, Option<f64>, i32, f64, f64) =
            sqlx::query_as(
                "SELECT nav::double precision, daily_return::double precision,
                    cumulative_return::double precision, position_count,
                    market_value::double precision, cash::double precision
             FROM paper_nav_snapshot
             WHERE paper_account_id = $1 AND snapshot_date = '2027-06-15'",
            )
            .bind(account_id)
            .fetch_one(&db)
            .await
            .expect("zzz snapshot row");
        assert_eq!(nav, 110_000.0);
        let daily = daily.expect("daily_return 应补齐");
        assert!(
            (daily - 5_000.0 / 105_000.0).abs() < 1e-9,
            "日收益: {daily}"
        );
        assert!((cum.unwrap_or(0.0) - 0.1).abs() < 1e-9, "累计收益 10%");
        assert_eq!(pos_n, 1);
        assert_eq!(mv, 12_000.0);
        assert_eq!(cash, 5_000.0);

        cleanup_account(&db, account_id).await;
        restore_future_day(&db, FUTURE_DAY, &[account_id]).await;
    }

    #[tokio::test]
    async fn snapshot_after_rebalance_writes_trade_count_and_returns() {
        let _report_write_guard = super::REPORT_WRITE_TEST_LOCK.lock().await;
        let db = test_db().await;
        let account_id = "zzz_test_api5_snap";
        // 前置精确清理（2026-09-21 补：panic 残留防连锁失败，删 zzz 账户该日快照）
        sqlx::query("DELETE FROM paper_nav_snapshot WHERE snapshot_date = '2027-06-16'")
            .execute(&db)
            .await
            .unwrap();
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_nav_snapshot WHERE snapshot_date = '2027-06-16'",
        )
        .fetch_one(&db)
        .await
        .unwrap_or(-1);
        assert_eq!(existing, 0, "守卫失败：2027-06-16 已存在快照");

        create_zzz_account(&db, account_id, 110_000.0, 5_000.0, None).await;
        insert_prev_snapshot(&db, account_id, 100_000.0).await;
        insert_position(&db, account_id, "ZZZR02.SH").await;
        insert_filled_order(
            &db,
            account_id,
            "po-zzz-r5-buy1",
            "buy",
            1_000.0,
            FUTURE_DAY_2,
        )
        .await;
        insert_filled_order(
            &db,
            account_id,
            "po-zzz-r5-sell1",
            "sell",
            800.0,
            FUTURE_DAY_2,
        )
        .await;

        snapshot_positions_for_all_accounts(&db, FUTURE_DAY_2).await;

        let (daily, cum, trade_n, pos_n): (Option<f64>, Option<f64>, Option<i32>, i32) =
            sqlx::query_as(
                "SELECT daily_return::double precision, cumulative_return::double precision,
                        trade_count, position_count
                 FROM paper_nav_snapshot
                 WHERE paper_account_id = $1 AND snapshot_date = '2027-06-16'",
            )
            .bind(account_id)
            .fetch_one(&db)
            .await
            .expect("zzz snapshot row");
        // 调仓后即时快照：日收益恒有值（10%），成交 2 笔
        assert!(
            (daily.unwrap_or(0.0) - 0.1).abs() < 1e-9,
            "日收益: {daily:?}"
        );
        assert!((cum.unwrap_or(0.0) - 0.1).abs() < 1e-9);
        assert_eq!(trade_n, Some(2), "当日 filled 订单计数");
        assert_eq!(pos_n, 1);

        cleanup_account(&db, account_id).await;
        restore_future_day(&db, FUTURE_DAY_2, &[account_id]).await;
    }

    #[tokio::test]
    async fn resend_daily_report_handles_complete_and_incomplete_days() {
        let _report_write_guard = super::REPORT_WRITE_TEST_LOCK.lock().await;
        let db = test_db().await;
        // 完整账户：持仓 symbol 当日有 bar → daily_return 有值
        let acct_a = "zzz_test_api5_rsA";
        // 不完整账户：持仓 symbol 当日无 bar → daily_return 置 NULL（次日 T+1 补发语义）
        let acct_b = "zzz_test_api5_rsB";
        // 前置全删远未来日（上次 panic 残留防连锁）
        sqlx::query("DELETE FROM paper_nav_snapshot WHERE snapshot_date = '2027-06-17'")
            .execute(&db)
            .await
            .unwrap();
        for a in [acct_a, acct_b] {
            cleanup_account(&db, a).await;
        }
        create_zzz_account(&db, acct_a, 110_000.0, 5_000.0, None).await;
        create_zzz_account(&db, acct_b, 108_000.0, 6_000.0, None).await;
        // v24 策略引用：composite 远未来日无数据 → 偏离计算跳过（"数据未就绪"分支）
        sqlx::query(
            "UPDATE paper_account SET strategy_version_id = 'v24' WHERE paper_account_id = ANY($1)",
        )
        .bind(vec![acct_a, acct_b])
        .execute(&db)
        .await
        .expect("set strategy");
        for a in [acct_a, acct_b] {
            insert_prev_snapshot(&db, a, 100_000.0).await;
        }
        insert_position(&db, acct_a, "ZZZR03.SH").await;
        insert_position(&db, acct_b, "ZZZR04.SH").await;
        // acct_a 的持仓当日有日线（数据完整）；acct_b 无 → 不完整
        sqlx::query(
            "INSERT INTO market_stock_daily_bar (symbol, trade_date, close, source)
             VALUES ('ZZZR03.SH', '2027-06-17', 12.5, 'zzz-test')
             ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .execute(&db)
        .await
        .expect("insert zzz bar");

        // 补发入口：只处理指定账户（绝不触碰生产账户）
        resend_daily_performance_report(
            &db,
            FUTURE_DAY_3,
            &[acct_a.to_string(), acct_b.to_string()],
        )
        .await
        .expect("resend ok");

        let (daily_a, cum_a): (Option<f64>, Option<f64>) = sqlx::query_as(
            "SELECT daily_return::double precision, cumulative_return::double precision
             FROM paper_nav_snapshot WHERE paper_account_id = $1 AND snapshot_date = '2027-06-17'",
        )
        .bind(acct_a)
        .fetch_one(&db)
        .await
        .expect("snapshot A");
        assert!((daily_a.expect("完整日 daily_return 应有值") - 0.1).abs() < 1e-9);
        assert!((cum_a.unwrap_or(0.0) - 0.1).abs() < 1e-9);

        let (daily_b, cum_b): (Option<f64>, Option<f64>) = sqlx::query_as(
            "SELECT daily_return::double precision, cumulative_return::double precision
             FROM paper_nav_snapshot WHERE paper_account_id = $1 AND snapshot_date = '2027-06-17'",
        )
        .bind(acct_b)
        .fetch_one(&db)
        .await
        .expect("snapshot B");
        assert!(
            daily_b.is_none(),
            "日终数据不完整 → daily_return NULL（T+1 补发标记）"
        );
        assert!((cum_b.unwrap_or(0.0) - 0.08).abs() < 1e-9, "累计收益 8%");

        for a in [acct_a, acct_b] {
            cleanup_account(&db, a).await;
        }
        let _ = sqlx::query(
            "DELETE FROM market_stock_daily_bar WHERE symbol = 'ZZZR03.SH' AND source = 'zzz-test'",
        )
        .execute(&db)
        .await;
        // report_for_accounts 开头的复权因子兜底会在该日注册 data_version 行（ZZZR03.SH
        // 无历史因子，无 factor 实际写入），精确清掉避免残留
        let _ = sqlx::query(
            "DELETE FROM data_version WHERE data_version_id = 'dv-adj-report-20270617'",
        )
        .execute(&db)
        .await;
    }

    #[tokio::test]
    async fn trade_detail_notification_pushes_only_accounts_with_fills() {
        let db = test_db().await;
        // user 账户（推送筛选条件 user_id IS NOT NULL），webhook 指向 loopback 无服务端口
        let acct_with = "zzz_test_api5_td1";
        let acct_without = "zzz_test_api5_td2";
        create_zzz_account(&db, acct_with, 110_000.0, 5_000.0, Some("admin")).await;
        create_zzz_account(&db, acct_without, 100_000.0, 5_000.0, Some("admin")).await;
        // 当日成交：2 买 1 卖（远未来日，生产账户当日必然无成交 → 不触发真实推送）
        insert_filled_order(
            &db,
            acct_with,
            "po-zzz-r5-td-b1",
            "buy",
            1_200.0,
            FUTURE_DAY,
        )
        .await;
        insert_filled_order(&db, acct_with, "po-zzz-r5-td-b2", "buy", 900.0, FUTURE_DAY).await;
        insert_filled_order(&db, acct_with, "po-zzz-r5-td-s1", "sell", 700.0, FUTURE_DAY).await;

        // 无成交账户走 continue 分支，有成交账户组装表格并推送（loopback 失败仅 warn）
        push_dingtalk_trade_detail_notification(&db, FUTURE_DAY)
            .await
            .expect("trade detail ok");

        cleanup_account(&db, acct_with).await;
        cleanup_account(&db, acct_without).await;
    }
}
