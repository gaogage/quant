//! 内置调度器 — 交易日盘中实时数据同步 + 交易信号生成 + 收盘钉钉推送。
//!
//! 盘中 (9:30-15:00): 每 10 分钟同步实时数据 → 检查信号 → 模拟交易
//! 收盘 (15:30):     推送钉钉持仓摘要 (每日一次)
//! 历史批量同步:     API 手动触发 (quant-sync-daily)
//!
//! 启动时通过 tokio::spawn 在后台运行，每 60 秒检查一次。

use chrono::{Local, NaiveDate, Timelike};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

fn short_id() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(12).collect()
}

struct DailyState {
    date: Option<NaiveDate>,
    last_sync_minute: Option<u32>,    // 上次盘中数据同步的分钟数
    signals_generated: bool,           // 今日是否已生成交易信号
    dingtalk_sent: bool,               // 今日是否已推送钉钉
    cleanup_done: bool,                // 今日是否已完成过期数据清理
}

/// 启动后台调度器。
pub fn start_scheduler(db: PgPool, port: u16) {
    tokio::spawn(async move {
        let state = Arc::new(Mutex::new(DailyState {
            date: None,
            last_sync_minute: None,
            signals_generated: false,
            dingtalk_sent: false,
            cleanup_done: false,
        }));
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        info!("[scheduler] 已启动: 盘中9:30-15:00实时同步+交易, 15:30钉钉推送");

        loop {
            interval.tick().await;
            if let Err(e) = run_tick(&db, &state, port).await {
                error!("[scheduler] 任务失败: {}", e);
            }
        }
    });
}

async fn run_tick(db: &PgPool, state: &Arc<Mutex<DailyState>>, port: u16) -> Result<(), String> {
    let now = Local::now();
    let today = now.date_naive();
    let hour = now.time().hour();
    let minute = now.time().minute();

    // 日期切换：重置状态
    {
        let mut st = state.lock().await;
        if st.date != Some(today) {
            st.date = Some(today);
            st.last_sync_minute = None;
            st.signals_generated = false;
            st.dingtalk_sent = false;
            st.cleanup_done = false;
        }
    }

    // 非交易日跳过
    if !is_trading_day(db, today).await? {
        return Ok(());
    }

    // ── 盘中 (9:30 - 15:00): 实时数据同步 + 交易信号 ──
    if hour >= 9 && (hour < 15 || (hour == 15 && minute == 0)) {
        let current_minute_slot = minute / 10; // 10 分钟粒度

        let should_sync = {
            let st = state.lock().await;
            // 9:30-9:40 首次同步 + 生成信号
            if hour == 9 && minute >= 30 && st.last_sync_minute.is_none() {
                true
            } else {
                st.last_sync_minute.map_or(true, |last| {
                    // 距上次同步 >= 10 分钟
                    let elapsed = if current_minute_slot >= last {
                        current_minute_slot - last
                    } else {
                        // 跨小时
                        (60 / 10) - last + current_minute_slot
                    };
                    elapsed >= 1
                })
            }
        };

        if should_sync {
            info!("[scheduler] 盘中数据同步 {}:{:02}", hour, minute);

            // 数据同步
            sync_intraday_data(port, today).await?;

            // 更新同步时间
            {
                let mut st = state.lock().await;
                st.last_sync_minute = Some(current_minute_slot);
            }

            // 首次同步后生成交易信号
            let should_generate = {
                let st = state.lock().await;
                !st.signals_generated
            };

            if should_generate {
                info!("[scheduler] 生成交易信号...");
                match generate_paper_signals_for_all(db, port, today).await {
                    Ok(_) => {
                        let mut st = state.lock().await;
                        st.signals_generated = true;
                    }
                    Err(e) => warn!("[scheduler] 信号生成失败: {}", e),
                }
            }
        }
    }

    // ── 收盘后 (15:30): 钉钉推送持仓摘要 (每日一次) ──
    if hour == 15 && minute >= 30 {
        let should_push = {
            let st = state.lock().await;
            !st.dingtalk_sent
        };

        if should_push {
            info!("[scheduler] 收盘钉钉推送...");
            match push_dingtalk_for_all_accounts(db, today).await {
                Ok(_) => {
                    let mut st = state.lock().await;
                    st.dingtalk_sent = true;
                }
                Err(e) => warn!("[scheduler] 钉钉推送失败: {}", e),
            }
        }
    }

    // ── 收盘后 (16:00): 清理 7 天前过期回测数据 (每日一次) ──
    if hour >= 16 {
        let should_cleanup = {
            let st = state.lock().await;
            !st.cleanup_done
        };

        if should_cleanup {
            info!("[scheduler] 清理过期回测数据 (7天前, is_kept=false)...");
            match super::cleanup::clean_expired_backtests(db).await {
                Ok((count, freed)) => {
                    let mut st = state.lock().await;
                    st.cleanup_done = true;
                    if count > 0 {
                        info!(
                            "[scheduler] 已清理 {} 个过期回测任务, 释放约 {}",
                            count,
                            super::cleanup::format_bytes(freed)
                        );
                    }
                }
                Err(e) => warn!("[scheduler] 过期数据清理失败: {}", e),
            }
        }
    }

    Ok(())
}

async fn is_trading_day(db: &PgPool, date: NaiveDate) -> Result<bool, String> {
    let row = sqlx::query_as::<_, (Option<bool>,)>(
        "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1 LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("calendar: {}", e))?;
    Ok(row.and_then(|(v,)| v).unwrap_or(false))
}

/// 盘中实时数据同步 (指数 + ETF)
async fn sync_intraday_data(port: u16, date: NaiveDate) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    let base = format!("http://localhost:{}", port);
    let client = reqwest::Client::new();

    // 指数日线
    let _ = client
        .post(format!("{}/api/v1/quant/data/sync/index-daily", base))
        .json(&serde_json::json!({
            "index_codes": ["000300.SH"],
            "start_date": date_str, "end_date": date_str
        }))
        .send().await;

    // ETF 日线
    let _ = client
        .post(format!("{}/api/v1/quant/data/sync/fund-daily", base))
        .json(&serde_json::json!({
            "symbols": ["518880.SH","511010.SH","513100.SH","513500.SH"],
            "start_date": date_str, "end_date": date_str
        }))
        .send().await;

    Ok(())
}

/// 为所有活跃模拟账号生成交易信号。
async fn generate_paper_signals_for_all(
    db: &PgPool, port: u16, date: NaiveDate,
) -> Result<(), String> {
    let accounts = sqlx::query_as::<_, (String, String)>(
        "SELECT paper_account_id, name FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db).await
    .map_err(|e| format!("account query: {}", e))?;

    if accounts.is_empty() { return Ok(()); }

    for (account_id, name) in &accounts {
        info!("[paper] {} ({})", name, account_id);

        // 检查今日是否已有交易
        let done: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM paper_order
             WHERE paper_account_id = $1 AND DATE(created_at) = $2",
        ).bind(account_id).bind(date).fetch_one(db).await
        .map_err(|e| format!("count: {}", e))?;

        if done.0 > 0 { continue; }

        let start = (date - chrono::Duration::days(30)).format("%Y%m%d").to_string();
        let end = date.format("%Y%m%d").to_string();
        let client = reqwest::Client::new();
        let base = format!("http://localhost:{}", port);

        let resp = client
            .post(format!("{}/api/v1/quant/backtests/run-factor", base))
            .json(&serde_json::json!({
                "combo_name": "phase7_price_volume_expanded_v1", "version": "1.0.0",
                "strategy_version_id": "phase7-professional-v1",
                "data_version_id": "research-full-2016-2026-20260515",
                "top_n": 15, "rebalance": "monthly", "max_position_pct": 0.10,
                "max_gross_exposure": 0.95, "score_direction": "descending",
                "portfolio_method": "heuristic", "benchmark": "000300.SH",
                "skip_top_pct": 0.0, "entry_delay": 1,
                "universe_profile": "main_board_non_st",
                "start_date": start, "end_date": end
            })).send().await;

        let task_id = match resp {
            Ok(r) => r.json::<serde_json::Value>().await.ok()
                .and_then(|v| v.get("data").and_then(|d| d.get("task_id"))
                .and_then(|t| t.as_str()).map(str::to_string)),
            Err(_) => continue,
        };
        let Some(task_id) = task_id else { continue };

        match sync_positions_from_backtest(db, account_id, &task_id, date).await {
            Ok(n) => info!("[paper] {} 同步 {} 个持仓", name, n),
            Err(e) => error!("[paper] {} 持仓同步失败: {}", name, e),
        }
    }
    Ok(())
}

async fn sync_positions_from_backtest(
    db: &PgPool, account_id: &str, task_id: &str, date: NaiveDate,
) -> Result<usize, String> {
    let positions = sqlx::query_as::<_, (String, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, quantity, market_value FROM backtest_position
         WHERE task_id = $1 AND position_date = (SELECT MAX(position_date) FROM backtest_position WHERE task_id = $1)
         ORDER BY market_value DESC",
    ).bind(task_id).fetch_all(db).await.map_err(|e| format!("pos: {}", e))?;

    if positions.is_empty() { return Ok(0); }

    // Get initial capital
    let (initial_cap,): (rust_decimal::Decimal,) = sqlx::query_as(
        "SELECT initial_capital FROM paper_account WHERE paper_account_id = $1"
    ).bind(account_id).fetch_one(db).await.map_err(|e| format!("cap: {}", e))?;

    // ── MVO Regime Detection ──
    let (mvo_a_pct, mvo_gold_pct, mvo_bond_pct, mvo_sp500_pct, mvo_nq_pct) =
        compute_mvo_allocation(db, date).await;

    let a_share_capital = initial_cap * rust_decimal::Decimal::from_f64_retain(mvo_a_pct).unwrap_or(rust_decimal::Decimal::from_f64_retain(0.25).unwrap());
    let total_stock_mv: rust_decimal::Decimal = positions.iter()
        .filter_map(|(_, _, mv)| *mv)
        .sum();
    let scale = if total_stock_mv > rust_decimal::Decimal::ZERO {
        a_share_capital / total_stock_mv
    } else {
        rust_decimal::Decimal::ONE
    };

    // Create A-share positions (scaled by MVO weight)
    for (symbol, qty, mkt_val) in &positions {
        let q = qty.unwrap_or(rust_decimal::Decimal::ZERO);
        let m = mkt_val.unwrap_or(rust_decimal::Decimal::ZERO);
        if q <= rust_decimal::Decimal::ZERO { continue; }
        let price = if q > rust_decimal::Decimal::ZERO { m / q } else { rust_decimal::Decimal::ZERO };
        let scaled_q = q * scale;
        let scaled_m = m * scale;

        let oid = format!("po-{}", short_id());
        sqlx::query("INSERT INTO paper_order (order_id,paper_account_id,symbol,side,order_type,quantity,limit_price,status,strategy_version_id) VALUES ($1,$2,$3,'buy','market',$4,$5,'pending','phase7-professional-v1')")
            .bind(&oid).bind(account_id).bind(symbol).bind(scaled_q).bind(price).execute(db).await.map_err(|e|format!("order:{}",e))?;
        let fid = format!("pf-{}", short_id());
        sqlx::query("INSERT INTO paper_fill (fill_id,order_id,paper_account_id,symbol,fill_time,side,quantity,price,amount) VALUES ($1,$2,$3,$4,now(),'buy',$5,$6,$7)")
            .bind(&fid).bind(&oid).bind(account_id).bind(symbol).bind(scaled_q).bind(price).bind(scaled_m).execute(db).await.map_err(|e|format!("fill:{}",e))?;
        sqlx::query("UPDATE paper_order SET status='filled' WHERE order_id=$1").bind(&oid).execute(db).await.map_err(|e|format!("upd:{}",e))?;
        let pid = format!("pp-{}", short_id());
        sqlx::query("INSERT INTO paper_position (paper_position_id,paper_account_id,symbol,quantity,avg_cost,market_price,market_value,target_weight) VALUES ($1,$2,$3,$4,$5,$5,$6,$7) ON CONFLICT (paper_account_id,symbol) DO UPDATE SET quantity=EXCLUDED.quantity,market_price=EXCLUDED.market_price,market_value=EXCLUDED.market_value,avg_cost=EXCLUDED.avg_cost")
            .bind(&pid).bind(account_id).bind(symbol).bind(scaled_q).bind(price).bind(scaled_m).bind(rust_decimal::Decimal::from_f64_retain(mvo_a_pct / positions.len() as f64).unwrap_or(rust_decimal::Decimal::ZERO)).execute(db).await.map_err(|e|format!("pos:{}",e))?;
    }

    // Create ETF positions (MVO allocation)
    let etf_allocations = vec![
        ("518880.SH", "黄金ETF", mvo_gold_pct),
        ("511010.SH", "国债ETF", mvo_bond_pct),
        ("513500.SH", "标普500", mvo_sp500_pct),
        ("513100.SH", "纳指ETF", mvo_nq_pct),
    ];

    for (etf_symbol, _etf_name, alloc_pct) in &etf_allocations {
        if *alloc_pct <= 0.0 { continue; }
        let alloc_amount = initial_cap * rust_decimal::Decimal::from_f64_retain(*alloc_pct).unwrap_or(rust_decimal::Decimal::ZERO);
        if alloc_amount <= rust_decimal::Decimal::ZERO { continue; }

        // Get latest ETF price
        let price_row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
            "SELECT close FROM market_stock_daily_bar WHERE symbol = $1 ORDER BY trade_date DESC LIMIT 1"
        ).bind(etf_symbol).fetch_optional(db).await.map_err(|e| format!("etf price: {}", e))?;

        let price = price_row.and_then(|(p,)| p).unwrap_or(rust_decimal::Decimal::ONE);
        let qty = if price > rust_decimal::Decimal::ZERO { alloc_amount / price } else { rust_decimal::Decimal::ZERO };

        let oid = format!("po-{}", short_id());
        sqlx::query("INSERT INTO paper_order (order_id,paper_account_id,symbol,side,order_type,quantity,limit_price,status,strategy_version_id) VALUES ($1,$2,$3,'buy','market',$4,$5,'pending','phase7-professional-v1')")
            .bind(&oid).bind(account_id).bind(etf_symbol).bind(qty).bind(price).execute(db).await.map_err(|e|format!("etf order:{}",e))?;
        let fid = format!("pf-{}", short_id());
        sqlx::query("INSERT INTO paper_fill (fill_id,order_id,paper_account_id,symbol,fill_time,side,quantity,price,amount) VALUES ($1,$2,$3,$4,now(),'buy',$5,$6,$7)")
            .bind(&fid).bind(&oid).bind(account_id).bind(etf_symbol).bind(qty).bind(price).bind(alloc_amount).execute(db).await.map_err(|e|format!("etf fill:{}",e))?;
        sqlx::query("UPDATE paper_order SET status='filled' WHERE order_id=$1").bind(&oid).execute(db).await.map_err(|e|format!("upd:{}",e))?;
        let pid = format!("pp-{}", short_id());
        sqlx::query("INSERT INTO paper_position (paper_position_id,paper_account_id,symbol,quantity,avg_cost,market_price,market_value,target_weight) VALUES ($1,$2,$3,$4,$5,$5,$6,$7) ON CONFLICT (paper_account_id,symbol) DO UPDATE SET quantity=EXCLUDED.quantity,market_price=EXCLUDED.market_price,market_value=EXCLUDED.market_value,avg_cost=EXCLUDED.avg_cost")
            .bind(&pid).bind(account_id).bind(etf_symbol).bind(qty).bind(price).bind(alloc_amount).bind(rust_decimal::Decimal::from_f64_retain(*alloc_pct).unwrap_or(rust_decimal::Decimal::ZERO)).execute(db).await.map_err(|e|format!("etf pos:{}",e))?;
    }

    sqlx::query("UPDATE paper_account SET cash=initial_capital-(SELECT COALESCE(SUM(quantity*avg_cost),0) FROM paper_position WHERE paper_account_id=$1), current_nav=initial_capital-(SELECT COALESCE(SUM(quantity*avg_cost),0) FROM paper_position WHERE paper_account_id=$1)+(SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id=$1), total_trades=(SELECT COUNT(*) FROM paper_order WHERE paper_account_id=$1) WHERE paper_account_id=$1")
        .bind(account_id).execute(db).await.map_err(|e|format!("acct:{}",e))?;

    Ok(positions.len() + etf_allocations.iter().filter(|(_,_,p)| *p > 0.0).count())
}

/// PIT-compliant MVO allocation: regime detection from trailing 1-year A-share return.
/// Returns (a_share, gold, bond, sp500, nasdaq) percentages.
async fn compute_mvo_allocation(db: &PgPool, date: NaiveDate) -> (f64, f64, f64, f64, f64) {
    // Get trailing 1-year HS300 return
    let trail: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (
            SELECT trade_date, close::double precision FROM market_index_daily_bar
            WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252
        ) SELECT (MAX(close)/MIN(close) - 1) FROM dates"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    match trail {
        Some(t) if t > 0.15 => (0.25, 0.35, 0.15, 0.00, 0.25), // Bull: A 25% Gold 35% Bond 15% NASDAQ 25%
        Some(t) if t < -0.05 => (0.08, 0.10, 0.72, 0.00, 0.10), // Bear: heavy bonds
        _ =>                      (0.15, 0.30, 0.30, 0.05, 0.20), // Normal
    }
}

/// 收盘后推送钉钉持仓摘要（所有活跃模拟账号）。
async fn push_dingtalk_for_all_accounts(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    use super::dingtalk;
    use serde_json::{json, Value};

    let accounts = sqlx::query_as::<_, (String, String, String, Option<String>, Option<f64>)>(
        "SELECT paper_account_id, name, account_type, dingtalk_webhook_url, current_nav::double precision
         FROM paper_account WHERE status='active' AND account_type='simulated'",
    ).fetch_all(db).await.map_err(|e| format!("acct: {}", e))?;

    for (id, name, acct_type, webhook, nav) in &accounts {
        let webhook_url = match webhook {
            Some(u) if !u.is_empty() => u.clone(),
            _ => match dingtalk::build_dingtalk_webhook_url() {
                Some(u) => u,
                None => { warn!("[dingtalk] {} 无 webhook", name); continue; }
            }
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
        ).bind(id).fetch_all(db).await.map_err(|e| format!("pos: {}", e))?;

        let positions: Vec<Value> = pos_rows.iter().filter_map(|(s,q,p,n)| {
            let q=q.unwrap_or(0.0); let p=p.unwrap_or(0.0);
            if q<=0.0 {None} else {Some(json!({
                "symbol":s, "name": n.as_deref().unwrap_or(s),
                "quantity":q, "current_price":p, "market_value":q*p
            }))}
        }).collect();

        // MVO 资产大类分布：ETF 单独列出，A 股汇总
        let class_rows = sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT CASE
                      WHEN pp.symbol = '518880.SH' THEN '黄金ETF'
                      WHEN pp.symbol = '511010.SH' THEN '国债ETF'
                      WHEN pp.symbol = '513500.SH' THEN '美股标普ETF'
                      WHEN pp.symbol = '513100.SH' THEN '美股纳指ETF'
                      ELSE 'A股' END,
                    SUM(pp.quantity * COALESCE(pp.market_price, pp.avg_cost))::double precision
             FROM paper_position pp
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
             GROUP BY 1
             ORDER BY SUM(2) DESC",
        ).bind(id).fetch_all(db).await.unwrap_or_default();

        // A股内部板块细分
        let a_sub_rows = sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT CONCAT('  A股-', COALESCE(NULLIF(ms.market,''), NULLIF(ms.exchange,''), '其他')),
                    SUM(pp.quantity * COALESCE(pp.market_price, pp.avg_cost))::double precision
             FROM paper_position pp
             LEFT JOIN market_stock ms ON ms.symbol = pp.symbol
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
               AND pp.symbol NOT IN ('518880.SH','511010.SH','513500.SH','513100.SH')
             GROUP BY 1 ORDER BY SUM(2) DESC",
        ).bind(id).fetch_all(db).await.unwrap_or_default();

        let total_mv: f64 = class_rows.iter().filter_map(|(_, v)| *v).sum();
        let mut class_breakdown: Vec<Value> = class_rows.iter().map(|(cls, v)| {
            let val = v.unwrap_or(0.0);
            let pct = if total_mv > 0.0 { val / total_mv * 100.0 } else { 0.0 };
            json!({"class": cls, "market_value": val, "weight_pct": (pct*100.0).round()/100.0})
        }).collect();
        // Append A-share sub-breakdown
        for (cls, v) in &a_sub_rows {
            let val = v.unwrap_or(0.0);
            let pct = if total_mv > 0.0 { val / total_mv * 100.0 } else { 0.0 };
            class_breakdown.push(json!({"class": cls, "market_value": val, "weight_pct": (pct*100.0).round()/100.0}));
        }

        let total_nav = nav.unwrap_or(0.0);
        let mv: f64 = positions.iter().filter_map(|p| p.get("market_value").and_then(|v| v.as_f64())).sum();
        let cash = total_nav - mv;
        let init_row = sqlx::query_as::<_, (Option<f64>, Option<f64>)>(
            "SELECT initial_capital::double precision, max_drawdown_pct::double precision FROM paper_account WHERE paper_account_id=$1"
        ).bind(id).fetch_optional(db).await.map_err(|e| format!("init: {}", e))?.unwrap_or((Some(total_nav), Some(0.0)));

        let init = init_row.0.unwrap_or(total_nav);
        let cum_ret = if init>0.0 {(total_nav-init)/init} else {0.0};
        let mdd = init_row.1.unwrap_or(0.0);

        let text = dingtalk::build_position_summary_notification(
            name, acct_type, &date.format("%Y-%m-%d").to_string(),
            total_nav, cash, &positions, cum_ret, mdd, &class_breakdown,
        );

        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, "持仓摘要", &text).await {
            warn!("[dingtalk] {} 发送失败: {}", name, e);
        } else {
            info!("[dingtalk] {} 推送成功", name);
        }
    }
    Ok(())
}
