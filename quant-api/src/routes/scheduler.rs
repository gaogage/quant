//! 内置调度器 — v15 日频量化交易。
//!
//! 14:45 (收盘前): 获取当日行情 → 回测 → MVO → 调仓 → 立即推送钉钉
//! 16:00 (收盘后): 同步日终行情数据到历史表 → 清理过期回测数据
//!
//! 日频交易不需要盘中实时行情，每天只在收盘前交易一次。
//! MVO 策略: Ledoit-Wolf + Grid Search 季度调仓 (自动发现权重)
//! 杠杆: 波动率目标 (vol_target, 20%年化波动率目标)
//! 启动时通过 tokio::spawn 在后台运行，每 60 秒检查一次。

use chrono::{Datelike, Local, NaiveDate, Timelike};
use ndarray::Array2;
use quant_common::mvo;
use quant_data::tushare::client::TushareClient;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

fn short_id() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(12).collect()
}

struct DailyState {
    date: Option<NaiveDate>,
    traded_today: bool,        // 今日是否已完成调仓 (14:40+)
    eod_synced_today: bool,    // 今日是否已完成日终数据同步 (16:00)
    yesterday_synced: bool,    // 昨日日线是否已完成 T+1 同步 (次日9:00)
    cleanup_done: bool,
}

/// MVO 权重缓存（季度更新）
struct MvoWeightCache {
    quarter: String,              // e.g. "2026-Q2"
    weights: Vec<f64>,            // [A股, 黄金, 国债, SP500, 纳指]
}

/// 启动后台调度器。
pub fn start_scheduler(db: PgPool, tushare: TushareClient, port: u16) {
    tokio::spawn(async move {
        let tushare = Arc::new(tushare);
        let state = Arc::new(Mutex::new(DailyState {
            date: None,
            traded_today: false,
            eod_synced_today: false,
            yesterday_synced: false,
            cleanup_done: false,
        }));
        let mvo_cache: Arc<Mutex<Option<MvoWeightCache>>> = Arc::new(Mutex::new(None));
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        info!("[scheduler] v18 已启动 (momentum μ + ms=12%): 14:40调仓 | 16:00 EOD | 9:00 T+1数据补同步");

        loop {
            interval.tick().await;
            if let Err(e) = run_tick(&db, &tushare, &state, &mvo_cache, port).await {
                error!("[scheduler] 任务失败: {}", e);
            }
        }
    });
}

/// 获取当前日期对应的最优 WFA 参数（从已完成的实验中提取）
async fn get_current_wfa_params(db: &PgPool, date: NaiveDate) -> Result<serde_json::Value, String> {
    let row = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT parameters FROM wfa_strategy_params
         WHERE test_start <= $1 AND test_end >= $1
         ORDER BY score DESC NULLS LAST LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("wfa params: {e}"))?;

    Ok(row.map(|(p,)| p).unwrap_or_default())
}

async fn run_tick(db: &PgPool, tushare: &TushareClient, state: &Arc<Mutex<DailyState>>, mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>, port: u16) -> Result<(), String> {
    let now = Local::now();
    let today = now.date_naive();
    let hour = now.time().hour();
    let minute = now.time().minute();

    // 日期切换：重置状态
    {
        let mut st = state.lock().await;
        if st.date != Some(today) {
            st.date = Some(today);
            st.traded_today = false;
            st.eod_synced_today = false;
            st.yesterday_synced = false;
            st.cleanup_done = false;
        }
    }

    // 非交易日跳过
    if !is_trading_day(db, today).await? {
        return Ok(());
    }

    // ── 14:40~15:00 (收盘前): 用当日行情调仓 + 立即推送钉钉 (每日一次) ──
    if hour == 14 && minute >= 40 {
        let should_trade = {
            let st = state.lock().await;
            !st.traded_today
        };

        if should_trade {
            {
                let mut st = state.lock().await;
                st.traded_today = true;
            }
            info!("[scheduler] 14:45 日频调仓 (v16 LW-MVO 7-asset)...");
            // 先同步当日行情数据
            sync_daily_data_for_today(db, tushare, today).await?;
            // 生成信号 + 调仓
            match generate_paper_signals_for_all(db, mvo_cache, port, today).await {
                Ok(_) => {
                    info!("[scheduler] 调仓完成, 推送钉钉...");
                    match push_dingtalk_for_all_accounts(db, today).await {
                        Ok(_) => info!("[scheduler] 钉钉推送完成"),
                        Err(e) => warn!("[scheduler] 钉钉推送失败: {}", e),
                    }
                }
                Err(e) => warn!("[scheduler] 调仓失败: {}", e),
            }
        }
    }

    // ── 16:00 (收盘后): 同步日终行情数据到历史表 ──
    if hour >= 16 {
        let should_sync = {
            let st = state.lock().await;
            !st.eod_synced_today
        };

        if should_sync {
            {
                let mut st = state.lock().await;
                st.eod_synced_today = true;
            }
            info!("[scheduler] 16:00 日终数据同步...");
            if let Err(e) = sync_eod_data(db, tushare, today).await {
                warn!("[scheduler] 日终数据同步失败: {}", e);
            }
        }
    }

    // ── 次日 9:00: 同步昨日日线 (Tushare T+1) + 因子重算 ──
    if hour == 9 && minute < 5 {
        let should_sync_yesterday = {
            let st = state.lock().await;
            !st.yesterday_synced
        };
        if should_sync_yesterday {
            {
                let mut st = state.lock().await;
                st.yesterday_synced = true;
            }
            let yesterday = today - chrono::Duration::days(1);
            let yesterday_str = yesterday.format("%Y%m%d").to_string();
            info!("[scheduler] 9:00 T+1 补同步昨日日线 {} + 因子重算...", yesterday_str);
            let empty: Vec<String> = vec![];

            // 昨日日线 (T+1 数据应已就绪) — 直接调用
            let _ = quant_data::sync::sync_daily_bars(db, tushare, &empty, &yesterday_str, &yesterday_str, &format!("dv-t1-{}", yesterday_str)).await;

            // 等日线同步完成
            tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;

            // 因子回填仍在后台运行(spawn), 此处通过现有的 factor backfill API 保持异步
            // 由于因子回填依赖 State, 需要一个简单的触发机制
            // 使用 tokio::spawn 异步触发, 不阻塞

            info!("[scheduler] T+1 补同步完成 ({})", yesterday_str);
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

    // ── 季度 WFA 自动优化 (每季度第一个交易日) ──
    let is_first_trading_day_of_quarter = {
        let m = today.month();
        let is_q_start = matches!(m, 1 | 4 | 7 | 10);
        if !is_q_start {
            false
        } else {
            // 检查是否是该季度第一个交易日
            sqlx::query_as::<_, (Option<bool>,)>(
                "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1",
            )
            .bind(today)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v)
            .unwrap_or(false)
        }
    };

    if is_first_trading_day_of_quarter && hour >= 16 {
        let quarter = format!("{}-Q{}", today.year(), (today.month() - 1) / 3 + 1);
        match check_and_trigger_wfa(db, port, &quarter, today).await {
            Ok(Some(msg)) => info!("[scheduler] WFA: {}", msg),
            Ok(None) => {}
            Err(e) => warn!("[scheduler] WFA check failed: {}", e),
        }
    }

    Ok(())
}

/// 检查是否需要触发 WFA 优化，如果需要则启动实验。
/// 返回 Some(msg) 表示执行了操作，None 表示跳过。
async fn check_and_trigger_wfa(
    db: &PgPool, port: u16, quarter: &str, today: NaiveDate,
) -> Result<Option<String>, String> {
    // 检查已有该季度的 WFA 参数
    let existing = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM wfa_strategy_params WHERE test_start <= $1 AND test_end >= $1",
    )
    .bind(today)
    .fetch_one(db)
    .await
    .map_err(|e| format!("wfa query: {e}"))?;

    if existing.0 > 0 {
        return Ok(None); // 已有参数，跳过
    }

    // 检查是否有正在运行的实验
    let running = sqlx::query_as::<_, (String,)>(
        "SELECT experiment_run_id FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status IN ('running', 'queued')
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("exp query: {e}"))?;

    if running.is_some() {
        // 检查是否可以从已完成的实验提取参数
        try_extract_wfa_params(db).await?;
        return Ok(Some("已有实验运行中，已尝试提取参数".into()));
    }

    // 检查最近一次实验完成时间（避免过于频繁）
    let recent = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>,)>(
        "SELECT created_at FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status = 'completed'
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("recent query: {e}"))?;

    if let Some((last_time,)) = recent {
        let days_since = (chrono::Utc::now() - last_time).num_days();
        if days_since < 30 {
            // 尝试从最近的实验提取参数
            try_extract_wfa_params(db).await?;
            return Ok(Some(format!("最近实验 {} 天前，跳过新建", days_since)));
        }
    }

    // 启动新的 WFA 实验
    let client = reqwest::Client::new();
    let base = format!("http://localhost:{}", port);
    let resp = client
        .post(format!("{}/api/v1/quant/optimizations/phase7-oos-walk-forward-discovery", base))
        .json(&serde_json::json!({
            "data_version_id": "research-full-2016-2026-20260515",
            "strategy_version_id": "phase7-professional-v1",
            "search_profile": "professional_simple_heuristic_discovery_default",
            "start_date": "20160201",
            "end_date": today.format("%Y%m%d").to_string(),
            "oos_top_n": 2,
            "execution_mode": "background",
        }))
        .send()
        .await
        .map_err(|e| format!("HTTP: {e}"))?;

    if resp.status().is_success() {
        Ok(Some(format!("新 WFA 实验已启动 (quarter={})", quarter)))
    } else {
        Err(format!("WFA 启动失败: {}", resp.status()))
    }
}

/// 从已完成的 WFA 实验中提取最优参数
async fn try_extract_wfa_params(db: &PgPool) -> Result<(), String> {
    // 找到最近完成的实验
    let exp_id = sqlx::query_as::<_, (String,)>(
        "SELECT experiment_run_id FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status = 'completed'
         ORDER BY completed_at DESC NULLS LAST LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("exp: {e}"))?
    .map(|(id,)| id);

    let Some(exp_id) = exp_id else { return Ok(()); };

    // 检查是否已提取过
    let already = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM wfa_strategy_params WHERE experiment_run_id = $1",
    )
    .bind(&exp_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("check: {e}"))?;

    if already.0 > 0 {
        return Ok(());
    }

    // 获取该实验的所有 optimization tasks
    let tasks = sqlx::query_as::<_, (String, i32)>(
        "SELECT o.optimization_task_id, (o.walk_forward_config->>'window_index')::int
         FROM optimization_task o
         WHERE o.walk_forward_config->>'experiment_run_id' = $1
           AND o.status = 'completed'
         ORDER BY (o.walk_forward_config->>'window_index')::int",
    )
    .bind(&exp_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("tasks: {e}"))?;

    let mut extracted = 0;
    for (task_id, window_idx) in &tasks {
        // 找 best_trial
        let trial = sqlx::query_as::<_, (String, Option<serde_json::Value>, Option<rust_decimal::Decimal>)>(
            "SELECT t.trial_id, t.parameters, t.score
             FROM optimization_trial t
             WHERE t.optimization_task_id = $1 AND t.status = 'completed'
             ORDER BY t.score DESC NULLS LAST LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("trial: {e}"))?;

        if let Some((trial_id, Some(params), score)) = trial {
            // 从 walk_forward_config 获取窗口日期
            let wf: Option<serde_json::Value> = sqlx::query_as::<_, (Option<serde_json::Value>,)>(
                "SELECT walk_forward_config FROM optimization_task WHERE optimization_task_id = $1",
            )
            .bind(task_id)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v);

            let test_start = wf.as_ref()
                .and_then(|w| w.get("test_start").and_then(|v| v.as_str()))
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
            let test_end = wf.as_ref()
                .and_then(|w| w.get("test_end").and_then(|v| v.as_str()))
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

            if let (Some(ts), Some(te)) = (test_start, test_end) {
                sqlx::query(
                    "INSERT INTO wfa_strategy_params (experiment_run_id, window_index, test_start, test_end, parameters, score)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (experiment_run_id, window_index) DO UPDATE
                     SET parameters = EXCLUDED.parameters, score = EXCLUDED.score",
                )
                .bind(&exp_id)
                .bind(window_idx)
                .bind(ts)
                .bind(te)
                .bind(&params)
                .bind(score.map(|s| s.to_string().parse::<f64>().unwrap_or(0.0)))
                .execute(db)
                .await
                .map_err(|e| format!("insert wfa: {e}"))?;
                extracted += 1;
                info!("[scheduler] WFA 参数已提取: window={} trial={}", window_idx, &trial_id[..32.min(trial_id.len())]);
            }
        }
    }

    if extracted > 0 {
        info!("[scheduler] WFA 参数提取完成: {} 个窗口", extracted);
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

/// 14:45 调仓前获取当日行情 + 停牌/涨跌停数据
async fn sync_daily_data_for_today(db: &PgPool, tushare: &TushareClient, date: NaiveDate) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    let empty: Vec<String> = vec![];

    // 停牌数据 (直接调用)
    let _ = quant_data::sync::sync_suspension(db, tushare, &date_str).await;

    // A股日线 (直接调用, symbols空→函数内自动获取全量)
    let dv_id = format!("dv-{}", date_str);
    let _ = quant_data::sync::sync_daily_bars(db, tushare, &empty, &date_str, &date_str, &dv_id).await;

    // 指数日线
    let index_codes = vec!["000300.SH".to_string()];
    let _ = quant_data::sync::sync_index_daily(db, tushare, &index_codes, &date_str, &date_str, &format!("idx-{}", date_str)).await;

    // ETF 日线 (v16: 7资产)
    let etf_symbols = vec!["518880.SH".into(),"511010.SH".into(),"513100.SH".into(),"513500.SH".into(),"159980.SZ".into(),"159985.SZ".into()];
    let _ = quant_data::sync::sync_fund_daily(db, tushare, &etf_symbols, &date_str, &date_str, &format!("etf-{}", date_str)).await;

    info!("[scheduler] 当日行情+停牌同步完成 ({})", date_str);
    Ok(())
}

/// 16:00 日终数据同步 (直接调用内部函数)
async fn sync_eod_data(db: &PgPool, tushare: &TushareClient, date: NaiveDate) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    let empty: Vec<String> = vec![];

    // 日线基础指标 (Tushare T+1, 尝试同步)
    let _ = quant_data::sync::sync_daily_basic(db, tushare, &empty, &date_str, &date_str, &format!("dv-basic-eod-{}", date_str)).await;

    // 复权因子 (直接调用)
    let _ = quant_data::sync::sync_adj_factor(db, tushare, &empty, &date_str, &date_str, &format!("dv-adj-eod-{}", date_str)).await;

    info!("[scheduler] 16:00 EOD 同步 (日线/因子由次日9:00 T+1补同步) ({})", date_str);

    // ── 涨跌停数据同步 ──
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    let limit_ok = sync_limit_with_retry(db, tushare, &date_str).await;
    if !limit_ok {
        warn!("[scheduler] ⚠ 涨跌停数据同步失败 (已重试)");
    }

    // ── ML预测数据检查+补齐 ──
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let pred_ok = ensure_prediction_coverage(db, tushare, date).await;
    if !pred_ok {
        warn!("[scheduler] ⚠ ML预测数据补齐失败, v16将降级为纯因子选股");
    }

    // 数据完整性检查
    run_data_quality_check(db).await;

    Ok(())
}

/// 数据完整性检查: 从配置的起始日期到今天, 检查所有核心表是否有缺口
async fn run_data_quality_check(db: &PgPool) {
    let start_date = sqlx::query_as::<_, (String,)>(
        "SELECT config_value FROM data_quality_config WHERE config_key = 'data_start_date'"
    )
    .fetch_optional(db).await
    .ok().flatten()
    .map(|(v,)| v)
    .unwrap_or_else(|| "2006-01-01".to_string());

    let today = chrono::Utc::now().date_naive();

    // 检查各表最后数据日期
    // 复权因子Tushare更新频率低(~月更), 使用30天阈值; 其他表使用2天阈值
    let checks: Vec<(&str, &str, Option<&str>, i64)> = vec![
        ("A股日线", "market_stock_daily_bar d",
         Some("d.symbol NOT IN (SELECT symbol FROM market_stock_suspension WHERE trade_date = d.trade_date AND suspend_type = 'S')"), 2), // Tushare T+1, 允许1天gap
        ("复权因子", "market_adjustment_factor", None, 30), // Tushare月更, 30天阈值
        ("日线基础", "market_stock_daily_basic", None, 2),
        ("因子(pv)", "multi_factor_value",
         Some("combo_name = 'phase7_price_volume_expanded_v1'"), 2),
        ("CSI300指数", "market_index_daily_bar",
         Some("symbol = '000300.SH'"), 2),
        ("ML预测", "model_prediction",
         None, 5),  // 不限制特定prediction_set, 检查全表最新日期
        // 涨跌停: Tushare免费版限流1次/分钟, 数据积累缓慢, 阈值设高避免频繁告警
        ("涨跌停", "market_stock_limit", None, 90),
    ];

    let mut gaps: Vec<String> = Vec::new();
    for (name, table, exclude_filter, max_gap) in &checks {
        let where_sql = exclude_filter.unwrap_or("TRUE");
        let sql = format!(
            "SELECT MAX(trade_date)::text FROM {} WHERE {}", table, where_sql
        );
        let max_date: Option<(String,)> = sqlx::query_as(&sql).fetch_optional(db).await.ok().flatten();

        if let Some((max_d,)) = max_date {
            if let (Ok(max_dt), Ok(today_dt)) = (
                NaiveDate::parse_from_str(&max_d, "%Y-%m-%d"),
                NaiveDate::parse_from_str(&today.format("%Y-%m-%d").to_string(), "%Y-%m-%d")
            ) {
                let gap_days = (today_dt - max_dt).num_days();
                if gap_days > *max_gap {
                    gaps.push(format!("{}: 最新={}, 缺口={}天 (阈值{}天)", name, max_d, gap_days, max_gap));
                }
            }
        } else {
            // 低优先级数据源(涨跌停等)无数据仅warn, 不触发告警
            if *max_gap >= 30 {
                warn!("[数据质量] {}: 无数据 (低优先级, 不告警)", name);
            } else {
                gaps.push(format!("{}: 无数据", name));
            }
        }
    }

    if !gaps.is_empty() {
        let msg = format!("[数据质量] 发现 {} 个缺口:\n{}", gaps.len(), gaps.join("\n"));
        warn!("{}", msg);
        // 钉钉报告
        send_quality_alert(db, &gaps).await;
    } else {
        info!("[数据质量] 全部数据完整 ({} → {})", start_date, today);
    }

    // 更新最后检查时间
    let _ = sqlx::query(
        "INSERT INTO data_quality_config (config_key, config_value, description) VALUES ('last_quality_check', $1, '最后质量检查日期') ON CONFLICT (config_key) DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = NOW()"
    )
    .bind(today.format("%Y-%m-%d").to_string())
    .execute(db).await;
}

/// 涨跌停数据同步 (直接调用, 带重试)
async fn sync_limit_with_retry(db: &PgPool, tushare: &TushareClient, date_str: &str) -> bool {
    let d = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d").unwrap();
    let _ = sqlx::query("DELETE FROM market_stock_limit WHERE trade_date = $1")
        .bind(d).execute(db).await;

    // 第一次尝试
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => { info!("[scheduler] 涨跌停同步成功 ({} 条)", n); return true; }
        Err(e) => warn!("[scheduler] 涨跌停首次失败: {}, 65秒后重试...", e),
    }

    // 重试
    tokio::time::sleep(tokio::time::Duration::from_secs(65)).await;
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => { info!("[scheduler] 涨跌停重试成功 ({} 条)", n); return true; }
        Err(e) => { warn!("[scheduler] 涨跌停重试仍失败: {}", e); false }
    }
}

/// 确保ML预测数据覆盖到当前日期
/// 策略: gap≤1天→正常; gap>1天→触发后台训练生成; 无法生成→降级纯因子
/// ML预测数据检查+补齐 (60天间隔, ML训练通过内部HTTP触发——真异步长任务)
async fn ensure_prediction_coverage(db: &PgPool, _tushare: &TushareClient, date: chrono::NaiveDate) -> bool {
    let latest_training: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(training_end_date) FROM prediction_set WHERE status = 'ready' AND training_end_date IS NOT NULL"
    ).fetch_optional(db).await.ok().flatten();

    if let Some((last_train,)) = latest_training {
        let days_since = (date - last_train).num_days();
        if days_since < 60 {
            info!("[scheduler] ML训练跳过 (最新训练={}, 距今{}天 < 60天)", last_train, days_since);
            return check_prediction_available(db, date).await;
        }
        info!("[scheduler] ML训练触发 (最新训练={}, 距今{}天 >= 60天)", last_train, days_since);
    } else {
        info!("[scheduler] 首次ML训练");
    }

    // 验证依赖
    if !verify_training_dependencies(db, date).await {
        warn!("[scheduler] ⚠ ML训练依赖数据不全, 跳过");
        return check_prediction_available(db, date).await;
    }

    // ML训练是复杂异步任务, 通过内部HTTP + tokio::spawn触发, 不阻塞scheduler
    info!("[scheduler] 🚀 触发ML预测训练 (后台异步)");
    let url = format!("http://localhost:{}", std::env::var("PORT").unwrap_or_else(|_| "8080".into()));
    let payload = serde_json::json!({
        "model_code": "nlqr_mr", "model_version": "1.0.0",
        "model_version_id": "mdl-p7-wf-wide-qgvrel-h60-v1",
        "data_version_id": "research-full-2016-2026-20260515",
        "feature_set_version_id": "phase7-wide-qgvrel-v1",
        "training_dataset_id": "phase7-wf-wide-qgvrel-h60-v1",
        "prediction_start_date": date.format("%Y%m%d").to_string(),
        "prediction_end_date": (date + chrono::Duration::days(63)).format("%Y%m%d").to_string(),
        "train_lookback_days": 756, "prediction_step_days": 63,
        "label_horizon_days": 20, "min_training_samples": 200,
        "max_windows": 20, "bucket_count": 10, "min_samples_per_bucket": 100,
        "factors": [
            {"factor_code": "rev_5d_std", "factor_version": "1.0.0"},
            {"factor_code": "rev_20d_std", "factor_version": "1.0.0"},
            {"factor_code": "downvol_20d_std", "factor_version": "1.0.0"},
            {"factor_code": "amihud_20d_std", "factor_version": "1.0.0"}
        ]
    });
    tokio::spawn(async move {
        let _ = reqwest::Client::new()
            .post(format!("{}/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker", url))
            .json(&payload).send().await;
    });

    check_prediction_available(db, date).await
}

/// 检查prediction数据是否覆盖当前日期
async fn check_prediction_available(db: &PgPool, date: chrono::NaiveDate) -> bool {
    let count: i64 = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM model_prediction mp
         JOIN prediction_set ps ON ps.prediction_set_id = mp.prediction_set_id AND ps.status = 'ready'
         WHERE mp.trade_date = $1"
    ).bind(date).fetch_optional(db).await.ok().flatten().map(|(c,)| c).unwrap_or(0);
    count > 0
}

/// 验证ML训练依赖的所有数据是否就绪
async fn verify_training_dependencies(db: &PgPool, date: chrono::NaiveDate) -> bool {
    let today = date;
    let threshold = today - chrono::Duration::days(2); // 2天内都算就绪

    // A股日线
    let daily_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar WHERE trade_date >= $1"
    ).bind(threshold).fetch_optional(db).await.ok().flatten().map(|(d,)| d >= threshold).unwrap_or(false);

    // 复权因子 (30天阈值, 因为Tushare月更)
    let adj_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_adjustment_factor"
    ).fetch_optional(db).await.ok().flatten().map(|(d,)| (today - d).num_days() < 60).unwrap_or(false);

    // 因子值 (pv combo)
    let factor_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM multi_factor_value WHERE combo_name = 'phase7_price_volume_expanded_v1' AND trade_date >= $1"
    ).bind(threshold).fetch_optional(db).await.ok().flatten().map(|(d,)| d >= threshold).unwrap_or(false);

    let all_ok = daily_ok && adj_ok && factor_ok;
    info!("[scheduler] ML训练依赖检查: daily={} adj={} factor={} → {}", daily_ok, adj_ok, factor_ok, if all_ok {"OK"} else {"MISSING"});
    all_ok
}

/// 发送钉钉告警 (独立于账号体系, 直接使用webhook)
async fn send_dingtalk_alert(db: &PgPool, msg: &str) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    ).fetch_all(db).await.unwrap_or_default();

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": "ML训练告警", "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}
async fn send_quality_alert(db: &PgPool, gaps: &[String]) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    )
    .fetch_all(db).await.unwrap_or_default();

    if accounts.is_empty() { return; }

    let gap_text = gaps.join("\n- ");
    let msg = format!("## ⚠️ 数据质量告警\n\n发现 {} 个数据缺口:\n- {}\n\n请检查数据同步状态。", gaps.len(), gap_text);

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": "数据质量告警", "text": msg}
            });
            let _ = reqwest::Client::new()
                .post(url)
                .json(&payload)
                .send().await;
        }
    }
}

/// 为所有活跃模拟账号生成交易信号（使用 LW-MVO 自动发现权重）。
async fn generate_paper_signals_for_all(
    db: &PgPool, mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>, port: u16, date: NaiveDate,
) -> Result<(), String> {
    let accounts = sqlx::query_as::<_, (String, String, bool, String, f64, String)>(
        "SELECT paper_account_id, name, COALESCE(leverage_enabled, false), COALESCE(signal_source, 'factor'), COALESCE(leverage_multiplier, 1.0), COALESCE(leverage_mode, 'fixed') FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db).await
    .map_err(|e| format!("account query: {}", e))?;

    if accounts.is_empty() { return Ok(()); }

    for (account_id, name, leverage_enabled, signal_source, leverage_multiplier, leverage_mode) in &accounts {
        let leverage_enabled = *leverage_enabled;
        let leverage_multiplier = *leverage_multiplier;
        let leverage_mode = leverage_mode.as_str();
        let signal_source = signal_source.as_str();
        info!("[paper] {} ({}) leverage={}x mode={} signal={}", name, account_id, leverage_multiplier, leverage_mode, signal_source);

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

        // 获取当前 WFA 最优参数（如有），合并到默认参数
        let wfa_params = get_current_wfa_params(db, date).await.unwrap_or_default();

        let combo_name = wfa_params.get("combo_name")
            .and_then(|v| v.as_str())
            .unwrap_or("phase7_price_volume_expanded_v1");
        let top_n = wfa_params.get("top_n")
            .and_then(|v| v.as_u64()).unwrap_or(15) as usize;
        let portfolio_method = wfa_params.get("portfolio_method")
            .and_then(|v| v.as_str()).unwrap_or("heuristic");
        let score_direction = wfa_params.get("score_direction")
            .and_then(|v| v.as_str()).unwrap_or("descending");
        let max_pos = wfa_params.get("max_position_pct")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.10);
        let skip_top = wfa_params.get("skip_top_pct")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let max_exposure = wfa_params.get("max_gross_exposure")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.95);
        let stop_loss = wfa_params.get("stop_loss_pct")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());
        let event_gate_combo = wfa_params.get("event_gate_combo_name")
            .and_then(|v| v.as_str());
        let event_gate_mode = wfa_params.get("event_gate_mode")
            .and_then(|v| v.as_str());
        let event_gate_score_dir = wfa_params.get("event_gate_score_direction")
            .and_then(|v| v.as_str());
        let risk_filter = wfa_params.get("candidate_risk_filter")
            .and_then(|v| v.as_str());
        let max_corr = wfa_params.get("max_pairwise_correlation")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());
        let rebalance_freq = wfa_params.get("rebalance")
            .and_then(|v| v.as_str()).unwrap_or("monthly");
        // WFA 高级风控参数
        let vol_control = wfa_params.get("portfolio_volatility_control").and_then(|v| v.as_str());
        let dd_control = wfa_params.get("portfolio_drawdown_control").and_then(|v| v.as_str());
        let risk_contribution = wfa_params.get("risk_contribution_control").and_then(|v| v.as_str());
        let partial_rebalance = wfa_params.get("partial_rebalance_ratio")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());
        let risk_budget_days = wfa_params.get("risk_budget_lookback_days")
            .and_then(|v| v.as_u64());
        let event_gate_min = wfa_params.get("event_gate_min_score")
            .and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());

        let wfa_used = wfa_params.as_object().map(|o| !o.is_empty()).unwrap_or(false);
        if wfa_used {
            info!("[paper] WFA params: combo={} top_n={} method={}", combo_name, top_n, portfolio_method);
        }

        let mut body = serde_json::json!({
            "combo_name": combo_name, "version": "1.0.0",
            "strategy_version_id": "phase7-professional-v1",
            "data_version_id": "research-full-2016-2026-20260515",
            "top_n": top_n, "rebalance": rebalance_freq, "max_position_pct": max_pos,
            "max_gross_exposure": max_exposure, "score_direction": score_direction,
            "portfolio_method": portfolio_method, "benchmark": "000300.SH",
            "skip_top_pct": skip_top, "entry_delay": 1,
            "universe_profile": "main_board_non_st",
            "start_date": start, "end_date": end
        });
        if let Some(sl) = stop_loss { body["stop_loss_pct"] = serde_json::json!(sl); }
        if let Some(eg) = event_gate_combo { body["event_gate_combo_name"] = serde_json::json!(eg); }
        if let Some(em) = event_gate_mode { body["event_gate_mode"] = serde_json::json!(em); }
        if let Some(es) = event_gate_score_dir { body["event_gate_score_direction"] = serde_json::json!(es); }
        if let Some(rf) = risk_filter { body["candidate_risk_filter"] = serde_json::json!(rf); }
        if let Some(mc) = max_corr { body["max_pairwise_correlation"] = serde_json::json!(mc); }
        if let Some(vc) = vol_control { body["portfolio_volatility_control"] = serde_json::json!(vc); }
        if let Some(dc) = dd_control { body["portfolio_drawdown_control"] = serde_json::json!(dc); }
        if let Some(rc) = risk_contribution { body["risk_contribution_control"] = serde_json::json!(rc); }
        if let Some(pr) = partial_rebalance { body["partial_rebalance_ratio"] = serde_json::json!(pr); }
        if let Some(rbd) = risk_budget_days { body["risk_budget_lookback_days"] = serde_json::json!(rbd); }
        if let Some(egm) = event_gate_min { body["event_gate_min_score"] = serde_json::json!(egm); }

        // 信号源路由: factor(默认) / prediction(ML) / prediction_blend(v16: 因子+ML混合)
        let is_prediction = signal_source == "prediction";
        let is_prediction_blend = signal_source == "prediction_blend";

        // v16: 动态选择最新的 ready prediction set, 检查数据是否覆盖当前日期
        let prediction_set_id = if is_prediction || is_prediction_blend {
            // PIT合规: 训练数据结束日期 < 预测日期, 选训练数据最新的模型
            let best: Option<(String,)> = sqlx::query_as(
                "SELECT ps.prediction_set_id FROM prediction_set ps
                 WHERE ps.status = 'ready'
                   AND ps.training_end_date IS NOT NULL
                   AND ps.training_end_date < $1           -- PIT: 训练数据必须在预测日期之前
                   AND ps.start_date <= $1 AND ps.end_date >= $1  -- 预测覆盖日期
                 ORDER BY ps.training_end_date DESC, ps.created_at DESC  -- 最近训练的优先
                 LIMIT 1"
            ).bind(date).fetch_optional(db).await.ok().flatten();

            match best {
                Some((pid,)) => {
                    info!("[paper] v16 prediction set: {} (覆盖{})", pid, date);
                    Some(pid)
                }
                None => {
                    warn!("[paper] ⚠ 无prediction set覆盖{}, v16降级为纯因子选股", date);
                    None
                }
            }
        } else {
            None
        };

        let resp = if is_prediction_blend {
            // v16: 因子+ML混合 — run-factor + prediction_blend
            let mut blend_body = body.clone();
            if let Some(ref pid) = prediction_set_id {
                blend_body["prediction_set_id"] = serde_json::json!(pid);
                blend_body["prediction_blend_weight"] = serde_json::json!(0.5);
            }
            // v16 专用参数
            if !wfa_used {
                blend_body["top_n"] = serde_json::json!(30);
                blend_body["rebalance"] = serde_json::json!("biweekly");
                blend_body["kelly_fraction"] = serde_json::json!(0.25);
                blend_body["score_candidate_pool_size"] = serde_json::json!(200);
            }
            info!("[paper] v16 prediction_blend: set={:?}", prediction_set_id);
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&blend_body).send().await
        } else if is_prediction {
            client
                .post(format!("{}/api/v1/quant/backtests/run-prediction", base))
                .json(&serde_json::json!({
                    "prediction_set_id": prediction_set_id.as_ref(),
                    "strategy_version_id": "phase7-professional-v1",
                    "data_version_id": "research-full-2016-2026-20260515",
                    "top_n": 15, "rebalance": "monthly", "max_position_pct": 0.10,
                    "max_gross_exposure": 0.95, "score_direction": "descending",
                    "portfolio_method": "heuristic", "benchmark": "000300.SH",
                    "skip_top_pct": 0.0, "entry_delay": 1,
                    "universe_profile": "main_board_non_st",
                    "start_date": start, "end_date": end
                })).send().await
        } else {
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&body).send().await
        };

        let task_id = match resp {
            Ok(r) => r.json::<serde_json::Value>().await.ok()
                .and_then(|v| v.get("data").and_then(|d| d.get("task_id"))
                .and_then(|t| t.as_str()).map(str::to_string)),
            Err(_) => continue,
        };
        let Some(task_id) = task_id else { continue };

        match sync_positions_from_backtest(db, account_id, &task_id, mvo_cache, date, leverage_enabled, leverage_multiplier, leverage_mode).await {
            Ok(n) => info!("[paper] {} 同步 {} 个持仓", name, n),
            Err(e) => error!("[paper] {} 持仓同步失败: {}", name, e),
        }
    }
    Ok(())
}

/// 波动率目标杠杆：根据 trailing 60日组合NAV变化计算波动率，动态调整杠杆。
/// 目标年化波动率 20%，杠杆 = 20% / trailing_vol，clamp [0.5, 2.0]。
async fn compute_vol_target_leverage(db: &PgPool, account_id: &str) -> f64 {
    let target_vol = 0.20;
    let rows = sqlx::query_as::<_, (rust_decimal::Decimal,)>(
        "SELECT nav FROM paper_nav_snapshot
         WHERE paper_account_id = $1
         ORDER BY snapshot_date DESC LIMIT 61",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let navs: Vec<f64> = rows.iter()
        .map(|(n,)| n.to_string().parse::<f64>().unwrap_or(0.0))
        .filter(|&v| v > 0.0)
        .collect();

    if navs.len() < 21 {
        return 1.0; // 数据不足
    }

    // 计算日收益率
    let mut rets = Vec::new();
    for i in 1..navs.len() {
        if navs[i-1] > 0.0 {
            rets.push(navs[i] / navs[i-1] - 1.0);
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
    lev.clamp(0.5, 2.0)
}

async fn sync_positions_from_backtest(
    db: &PgPool, account_id: &str, task_id: &str,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    date: NaiveDate,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
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

    // ── LW-MVO 自动发现权重（季度调仓，同季度复用缓存）──
    let mvo_weights = compute_lw_mvo_weights(db, date, mvo_cache, 0.12).await;

    // ── 体制检测 + 降仓 ──
    let regime_exposure = detect_regime_exposure(db, date).await;
    let mvo_a_pct = mvo_weights[0] * regime_exposure;
    let mvo_gold_pct = mvo_weights[1] * regime_exposure;
    let mvo_bond_pct = mvo_weights[2] * regime_exposure;
    let mvo_sp500_pct = mvo_weights[3] * regime_exposure;
    let mvo_nq_pct = mvo_weights[4] * regime_exposure;
    let mvo_color_pct = mvo_weights.get(5).copied().unwrap_or(0.03) * regime_exposure;
    let mvo_meal_pct = mvo_weights.get(6).copied().unwrap_or(0.03) * regime_exposure;
    let cash_pct = 1.0 - regime_exposure; // 现金/货币基金

    if regime_exposure < 0.99 {
        info!("[Regime] 降仓至 {:.0}%, 现金 {:.0}%", regime_exposure * 100.0, cash_pct * 100.0);
    }

    let a_share_capital = initial_cap * rust_decimal::Decimal::from_f64_retain(mvo_a_pct).unwrap_or(rust_decimal::Decimal::from_f64_retain(0.25).unwrap());
    let total_stock_mv: rust_decimal::Decimal = positions.iter()
        .filter_map(|(_, _, mv)| *mv)
        .sum();
    let base_scale = if total_stock_mv > rust_decimal::Decimal::ZERO {
        a_share_capital / total_stock_mv
    } else {
        rust_decimal::Decimal::ONE
    };

    // 杠杆：regime green(>0.9) + leverage_enabled → 使用配置的倍率
    let leverage_mult = if leverage_enabled && regime_exposure > 0.9 && leverage_multiplier > 1.0 {
        if leverage_mode == "vol_target" {
            // 波动率目标杠杆: 目标20%年化波动率, 根据trailing 60日实际波动率动态调整
            let vol_lev = compute_vol_target_leverage(db, account_id).await;
            info!("[paper] Vol-target leverage {:.2}x applied for {}", vol_lev, account_id);
            rust_decimal::Decimal::from_f64_retain(vol_lev).unwrap_or(rust_decimal::Decimal::ONE)
        } else {
            info!("[paper] Fixed leverage {}x applied for {}", leverage_multiplier, account_id);
            rust_decimal::Decimal::from_f64_retain(leverage_multiplier).unwrap_or(rust_decimal::Decimal::ONE)
        }
    } else {
        rust_decimal::Decimal::ONE
    };
    let scale = base_scale * leverage_mult;

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

    // v16: 7资产MVO Grid Search统一优化 (精简相关性冗余)
    let mut etf_allocations = vec![
        ("518880.SH", "黄金ETF", mvo_gold_pct),
        ("511010.SH", "国债ETF", mvo_bond_pct),
        ("513500.SH", "标普500", mvo_sp500_pct),
        ("513100.SH", "纳指ETF", mvo_nq_pct),
        ("159980.SZ", "有色ETF", mvo_color_pct),
        ("159985.SZ", "豆粕ETF", mvo_meal_pct),
    ];
    // 体制降仓时加入货币基金
    if cash_pct > 0.01 {
        etf_allocations.push(("511880.SH", "银华日利(现金)", cash_pct));
    }

    for (etf_symbol, _etf_name, alloc_pct) in &etf_allocations {
        if *alloc_pct <= 0.0 { continue; }
        let alloc_amount = initial_cap * rust_decimal::Decimal::from_f64_retain(*alloc_pct).unwrap_or(rust_decimal::Decimal::ZERO);
        if alloc_amount <= rust_decimal::Decimal::ZERO { continue; }

        // Get latest ETF price
        let price_row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
            "SELECT close FROM market_stock_daily_bar_adj WHERE symbol = $1 ORDER BY trade_date DESC LIMIT 1"
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

/// 体制检测：Trailing 12-month CSI300 return。
/// 深熊（12月跌 >10%）：仓位降至 60%，规避系统性风险。
/// 其余时间：满仓，让 LW-MVO 自主调配。
async fn detect_regime_exposure(db: &PgPool, date: NaiveDate) -> f64 {
    let trail: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (
            SELECT trade_date, close::double precision FROM market_index_daily_bar
            WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252
        ) SELECT (MAX(close)/MIN(close) - 1) FROM dates"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    match trail {
        Some(t) if t < -0.10 => {
            info!("[Regime] DEEP BEAR: 12m return={:.1}%, exposure=60%", t * 100.0);
            0.60
        }
        _ => 1.00, // 满仓
    }
}

/// v17: CSI300体制检测，返回动态min_stock
/// bull(MA60>MA250, trailing12m>10%): 35% | neutral: 25% | bear(trailing12m<-15%): 0%
async fn get_regime_min_stock(db: &PgPool, date: NaiveDate) -> f64 {
    // MA60
    let ma60: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT AVG(close::double precision) FROM (
            SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 60
        ) sub"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    // MA250
    let ma250: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT AVG(close::double precision) FROM (
            SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 250
        ) sub"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    // trailing 12m return
    let trail_12m: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (SELECT trade_date, close::double precision FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252)
         SELECT (MAX(close)/MIN(close) - 1) FROM dates"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    match (ma60, ma250, trail_12m) {
        (Some(m60), Some(m250), Some(t12)) if t12 > 0.10 && m60 > m250 => {
            info!("[Regime] BULL: min_stock=35% (t12m={:.1}%)", t12*100.0);
            0.35
        }
        (_, _, Some(t12)) if t12 < -0.15 => {
            info!("[Regime] BEAR: min_stock=0% (t12m={:.1}%)", t12*100.0);
            0.00
        }
        _ => {
            // neutral: 25% min stock (P2最优参数)
            0.25
        }
    }
}

/// v17: ETF MA200 趋势过滤。
/// 对每个 ETF，若最新收盘价 < MA200，将其权重归零，剩余权重重新归一化。
/// 仅当至少有一个 ETF 被过滤时才返回新权重，否则返回空 vec（调用方使用原权重）。
async fn apply_etf_trend_filter(
    db: &PgPool,
    date: NaiveDate,
    etf_symbols: &[&str],
    original_weights: &[f64],
) -> Vec<f64> {
    let n = original_weights.len();
    if n == 0 || etf_symbols.is_empty() {
        return vec![];
    }
    let mut filtered = original_weights.to_vec();
    let mut any_filtered = false;

    for (ei, sym) in etf_symbols.iter().enumerate() {
        // ETF 权重在索引 ei+1 (A股在索引0)
        let wi = ei + 1;
        if wi >= n || filtered[wi] <= 0.0 {
            continue;
        }
        // 获取最近 200 个交易日的收盘价
        let rows = sqlx::query_as::<_, (rust_decimal::Decimal,)>(
            "SELECT close FROM market_stock_daily_bar_adj
             WHERE symbol = $1 AND trade_date <= $2
             ORDER BY trade_date DESC LIMIT 200",
        )
        .bind(*sym)
        .bind(date)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        if rows.len() >= 200 {
            let sum: f64 = rows.iter().map(|(c,)| {
                c.to_string().parse::<f64>().unwrap_or(0.0)
            }).sum();
            let ma200 = sum / rows.len() as f64;
            let latest: f64 = rows[0].0.to_string().parse::<f64>().unwrap_or(0.0);

            if latest < ma200 {
                let etf_name = match *sym {
                    "518880.SH" => "黄金",
                    "511010.SH" => "国债",
                    "513500.SH" => "SP500",
                    "513100.SH" => "纳指",
                    "159980.SZ" => "有色",
                    "159985.SZ" => "豆粕",
                    _ => *sym,
                };
                info!(
                    "[ETF Trend] {} ({}) 跌破MA200 ({:.3} < {:.3}), 权重 {:.0}% → 0%",
                    etf_name, sym, latest, ma200, filtered[wi] * 100.0
                );
                filtered[wi] = 0.0;
                any_filtered = true;
            }
        }
    }

    if any_filtered {
        let remaining_sum: f64 = filtered.iter().sum();
        if remaining_sum > 0.0 {
            for w in &mut filtered {
                *w /= remaining_sum;
            }
        }
        info!(
            "[ETF Trend] 过滤后权重: A股={:.0}% 黄金={:.0}% 国债={:.0}% SP500={:.0}% 纳指={:.0}% 有色={:.0}% 豆粕={:.0}%",
            filtered[0] * 100.0, filtered[1] * 100.0, filtered[2] * 100.0,
            filtered[3] * 100.0, filtered[4] * 100.0,
            filtered.get(5).copied().unwrap_or(0.0) * 100.0,
            filtered.get(6).copied().unwrap_or(0.0) * 100.0,
        );
        filtered
    } else {
        vec![]
    }
}

/// LW-MVO 自动发现权重：Ledoit-Wolf shrinkage + Grid Search 季度调仓。
/// 返回 (a_share, gold, bond, sp500, nasdaq) 权重（和为 1.0）。
/// ETF 从实际有数据的日期开始纳入 MVO 计算。
async fn compute_lw_mvo_weights(
    db: &PgPool,
    date: NaiveDate,
    cache: &Mutex<Option<MvoWeightCache>>,
    min_stock: f64,
) -> Vec<f64> {
    // v18: min_stock=12% (P7超参数优化最优值)
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

    // v16: 7资产MVO — 精简后相关性独立的资产池
    // 国债ETF+十年国债(corr=0.865)合并保留国债ETF
    // 德国ETF移除(冗余), 银华日利仅在体制降仓时加入
    let etf_symbols = ["518880.SH", "511010.SH", "513500.SH", "513100.SH", "159980.SZ", "159985.SZ"];
    // 对应: 黄金, 国债, SP500, NASDAQ, 有色, 豆粕
    let n_total_assets = 1 + etf_symbols.len(); // A股 + 6 ETFs = 7

    // 获取过去 36 个月的月度收益数据
    let lookback_start = date - chrono::Duration::days(36 * 31); // ~3 years

    // A 股月度收益（从 backtest_equity_curve 获取）
    let a_monthly = get_monthly_returns(db, lookback_start, date, "A_SHARE").await;

    // Adaptive MVO: 根据近期 A 股表现动态调整 min_stock
    let adaptive_min_stock = if a_monthly.len() >= 3 {
        let trail_3m: f64 = a_monthly[..3].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
        let trail_6m: f64 = if a_monthly.len() >= 6 {
            a_monthly[..6].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0
        } else {
            trail_3m * 2.0
        };
        if trail_3m < -0.03 {
            info!("[MVO] Factor failure detected (3m={:.1}%), min_stock {} -> 0.00, switching to ETF defense", trail_3m * 100.0, min_stock);
            0.00
        } else if trail_6m > 0.15 {
            info!("[MVO] Adaptive: bull detected (6m={:.1}%), min_stock {} -> 0.20", trail_6m * 100.0, min_stock);
            0.20
        } else {
            min_stock
        }
    } else {
        min_stock
    };

    // Kelly-inspired A股仓位缩放
    let kelly_scale = if adaptive_min_stock > 0.0 && a_monthly.len() >= 6 {
        let trail_rets: Vec<f64> = a_monthly[..6].to_vec();
        let n = trail_rets.len() as f64;
        let avg = trail_rets.iter().sum::<f64>() / n;
        if n > 1.0 {
            let variance = trail_rets.iter().map(|r| (r - avg).powi(2)).sum::<f64>() / (n - 1.0);
            let monthly_ir = if variance > 0.0 { avg / variance.sqrt() } else { 0.0 };
            let annual_ir = monthly_ir * (12.0_f64).sqrt();
            (0.5 + annual_ir).clamp(0.3, 1.5)
        } else {
            1.0
        }
    } else {
        1.0
    };
    let adaptive_min_stock = (adaptive_min_stock * kelly_scale).min(0.75);

    // 7资产默认权重 (数据不足时的fallback): A股,黄金,国债,SP500,NASDAQ,有色,豆粕
    let default_weights = vec![adaptive_min_stock, 0.25, 0.35, 0.05, 0.15, 0.03, 0.03, 0.14 - adaptive_min_stock];

    let mut weights = default_weights.clone();

    if a_monthly.len() < 12 {
        let mut guard = cache.lock().await;
        *guard = Some(MvoWeightCache { quarter, weights: weights.clone() });
        return weights;
    }

    // 所有ETF月度收益
    let mut etf_monthly_data: Vec<Vec<f64>> = Vec::new();
    let mut valid_etf_count = 0;
    for sym in &etf_symbols {
        let mrets = get_monthly_returns(db, lookback_start, date, sym).await;
        if !mrets.is_empty() { valid_etf_count += 1; }
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
            // v16m: momentum-adjusted μ (60%历史均值 + 40%近期动量) + dynamic return target
            let dynamic_target = if a_monthly.len() >= 12 {
                let trail_12m: f64 = a_monthly[..12].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
                (trail_12m + 0.05).clamp(0.08, 0.18)
            } else {
                0.12
            };
            // Momentum-adjusted expected returns
            let hist_mu = ndarray::Array1::from_vec(
                (0..n_total_assets).map(|j| {
                    let col: Vec<f64> = all_monthly.iter().map(|r| r[j]).collect();
                    col.iter().sum::<f64>() / col.len() as f64 * 12.0
                }).collect()
            );
            let mom_mu = ndarray::Array1::from_vec(
                (0..n_total_assets).map(|j| {
                    let recent: Vec<f64> = all_monthly.iter().rev().take(6).map(|r| r[j]).collect();
                    recent.iter().fold(1.0, |acc, r| acc * (1.0 + r)).powf(2.0) - 1.0
                }).collect()
            );
            let adj_mu = 0.6 * &hist_mu + 0.4 * &mom_mu;
            if let Some(result) = mvo::mvo_allocate_with_custom_mu(&arr, &adj_mu, adaptive_min_stock, dynamic_target, 0.10) {
                let w = result.weights.to_vec();
                info!(
                    quarter = %quarter,
                    a = %(w[0] * 100.0).round(),
                    gold = %(w[1] * 100.0).round(),
                    bond = %(w[2] * 100.0).round(),
                    sp500 = %(w[3] * 100.0).round(),
                    nq = %(w[4] * 100.0).round(),
                    color = %(w[5] * 100.0).round(),
                    meal = %(w[6] * 100.0).round(),
                    sharpe = %(result.sharpe * 100.0).round() / 100.0,
                    "v18 7-asset MVO 权重已更新 (momentum μ + ms=12%)"
                );
                weights = w;
            }
        }
    }

    let mut guard = cache.lock().await;
    *guard = Some(MvoWeightCache { quarter, weights: weights.clone() });
    weights
}

/// 从 multi_factor_value 获取因子的月度收益（top-15等权组合的近似月收益）
async fn get_factor_monthly_returns(
    db: &PgPool, start: NaiveDate, end: NaiveDate, combo_name: &str,
) -> Vec<f64> {
    // 取每个交易日前15只股票的因子得分，用等权日收益近似
    let rows = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT DISTINCT trade_date FROM multi_factor_value
         WHERE combo_name = $1 AND version = '1.0.0'
         AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(combo_name)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    if rows.len() < 2 { return vec![]; }

    // 使用 backtest_equity_curve 中匹配 combo_name 的任务
    let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
        "SELECT bec.trade_date, bec.portfolio_value FROM backtest_equity_curve bec
         JOIN backtest_task bt ON bec.task_id = bt.task_id
         WHERE bt.parameters->>'combo_name' = $1
         AND bec.trade_date >= $2 AND bec.trade_date <= $3
         ORDER BY bec.trade_date",
    )
    .bind(combo_name)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    daily_to_monthly_returns(&eq_rows)
}

/// 获取某个资产的月度收益率序列（最新在前）
async fn get_monthly_returns(
    db: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
    symbol: &str,
) -> Vec<f64> {
    if symbol == "A_SHARE" {
        // A 股月度收益：从 multi_factor_value 混合 price_volume + financial_quality
        // 直接使用因子得分计算月收益（避免依赖特定回测task_id）
        let pv_monthly = get_factor_monthly_returns(db, start, end, "phase7_price_volume_expanded_v1").await;
        let fq_monthly = get_factor_monthly_returns(db, start, end, "phase7_financial_quality_v1").await;

        if !pv_monthly.is_empty() && !fq_monthly.is_empty() {
            let n = pv_monthly.len().min(fq_monthly.len());
            return (0..n).map(|i| pv_monthly[i] * 0.5 + fq_monthly[i] * 0.5).collect();
        }
        return pv_monthly;
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

// ── v17 Historical Replay ──────────────────────────────────────────

/// v17 完整策略历史回放结果
#[derive(Debug, serde::Serialize)]
pub struct ReplayResult {
    pub start_date: String,
    pub end_date: String,
    pub trading_days: usize,
    pub annual_return_pct: f64,
    pub cumulative_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub volatility_pct: f64,
    pub calmar_ratio: f64,
    pub win_rate_pct: f64,
    pub yearly_returns: Vec<YearlyReturn>,
    pub benchmarks: Benchmarks,
    pub mvo_start_date: String,
    pub mvo_trading_days: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct YearlyReturn {
    pub year: String,
    pub return_pct: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct Benchmarks {
    pub csi300: BenchmarkMetrics,
    pub sp500: BenchmarkMetrics,
    pub gold: BenchmarkMetrics,
}

#[derive(Debug, serde::Serialize)]
pub struct BenchmarkMetrics {
    pub annual_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub yearly_returns: Vec<YearlyReturn>,
}

/// 运行 v17 完整策略历史回放（与生产 scheduler 相同的逻辑）。
/// 逐日迭代所有交易日，在每个季度调仓日运行 compute_lw_mvo_weights，
/// 计算逐日 NAV 和完整绩效指标。
pub async fn run_historical_replay(
    db: &PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
    strategy: &str,
    leverage_mode: &str,
    leverage_multiplier: f64,
    min_stock_override: Option<f64>,
    objective: &str,
    rebalance: &str,
    fixed_return_target: Option<f64>,
    trend_boost: bool,
    vol_budget: bool,
    adaptive_vol_target: bool,
    leverage_cap: f64,
    extra_etfs: &[String],
    momentum_blend_ratio: f64,
    cov_method: &str,
) -> Result<ReplayResult, String> {
    let is_v17 = strategy == "v17";
    let use_max_sharpe = objective == "max_sharpe";
    let use_ewma = objective == "ewma";
    let use_momentum = objective == "momentum";
    let use_bl = objective == "black_litterman";
    let is_monthly = rebalance == "monthly";
    let use_vol_target = leverage_mode == "vol_target";
    let fixed_lev = if leverage_mode == "fixed" { leverage_multiplier.max(1.0) } else { 1.0 };
    let start_str = start_date.format("%Y-%m-%d").to_string();
    let end_str = end_date.format("%Y-%m-%d").to_string();
    info!("[replay] v17 历史回放: {} ~ {}", start_str, end_str);

    // 1. 加载交易日历（含36个月前置训练窗口）
    let pre_start = start_date - chrono::Duration::days(36 * 31); // ~3 years before
    let all_tdates: Vec<NaiveDate> = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT DISTINCT trade_date FROM market_trade_calendar WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true ORDER BY trade_date",
    )
    .bind(pre_start).bind(end_date)
    .fetch_all(db).await.map_err(|e| format!("交易日历: {e}"))?
    .into_iter().map(|(d,)| d).collect();

    let tdates: Vec<NaiveDate> = all_tdates.iter()
        .filter(|d| **d >= start_date).copied().collect();

    if tdates.len() < 252 { return Err("交易日不足1年".into()); }

    // 2. 加载 A 股权益曲线 — 因子选股回测(2009-2026全覆盖)
    let eq_task_id = "fbt-36e18e12-effc-40fe-9fc0-d539a336bf2e"; // 干净全量回测(2006-2026, adj_factor黑名单已排除)
    let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
        "SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id = $1 ORDER BY trade_date",
    ).bind(eq_task_id).fetch_all(db).await
    .map_err(|e| format!("A股权益曲线: {e}"))?;

    let a_nav: Vec<(NaiveDate, f64)> = eq_rows.iter()
        .map(|(d, v)| (*d, v.to_string().parse::<f64>().unwrap_or(0.0)))
        .filter(|(_, v)| *v > 0.0).collect();

    // 3. 加载 ETF 价格
    let mut etf_symbols = vec![
        "518880.SH".to_string(), "511010.SH".to_string(), "513500.SH".to_string(),
        "513100.SH".to_string(), "159980.SZ".to_string(), "159985.SZ".to_string(),
    ];
    for e in extra_etfs { if !etf_symbols.contains(e) { etf_symbols.push(e.clone()); } }
    let etf_count = etf_symbols.len();
    let etf_start = start_date - chrono::Duration::days(3 * 365); // 3年缓冲用于MA200

    let placeholders: Vec<String> = (1..=etf_count).map(|i| format!("${}", i)).collect();
    let in_clause = placeholders.join(", ");
    let sql = format!(
        "SELECT trade_date, symbol, close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol IN ({}) AND trade_date >= ${} ORDER BY trade_date",
        in_clause, etf_count + 1
    );
    let mut query = sqlx::query_as::<_, (NaiveDate, String, f64)>(&sql);
    for sym in &etf_symbols { query = query.bind(sym); }
    query = query.bind(etf_start);
    let etf_rows = query.fetch_all(db).await.map_err(|e| format!("ETF数据: {e}"))?;

    use std::collections::HashMap;
    let mut etf_prices: HashMap<String, HashMap<NaiveDate, f64>> = HashMap::new();
    for (d, sym, price) in &etf_rows {
        etf_prices.entry(sym.clone()).or_default().insert(*d, *price);
    }

    // 4. 加载基准数据
    async fn load_benchmark(
        db: &PgPool, sym: &str, table: &str,
        start_date: NaiveDate, end_date: NaiveDate, tdates: &[NaiveDate],
    ) -> Result<(Vec<f64>, Vec<NaiveDate>), String> {
        let sql = format!(
            "SELECT trade_date, close::double precision FROM {} WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date", table
        );
        let rows: Vec<(NaiveDate, f64)> = sqlx::query_as(&sql)
            .bind(sym).bind(start_date).bind(end_date)
            .fetch_all(db).await
            .map_err(|e: sqlx::Error| format!("基准{}: {e}", sym))?;
        let prices: HashMap<NaiveDate, f64> = rows.into_iter().collect();
        let mut rets = Vec::new(); let mut ret_dates = Vec::new();
        for di in 1..tdates.len() {
            let d = tdates[di]; let pd = tdates[di-1];
            if let (Some(&cp), Some(&pp)) = (prices.get(&d), prices.get(&pd)) {
                if pp > 0.0 && cp > 0.0 { rets.push(cp/pp - 1.0); ret_dates.push(d); }
            }
        }
        Ok((rets, ret_dates))
    }

    let (csi_rets, csi_dates) = load_benchmark(db, "000300.SH", "market_index_daily_bar", start_date, end_date, &tdates).await?;
    let (sp500_rets, sp500_dates) = load_benchmark(db, "513500.SH", "market_stock_daily_bar_adj", start_date, end_date, &tdates).await?;
    let (gold_rets, gold_dates) = load_benchmark(db, "518880.SH", "market_stock_daily_bar_adj", start_date, end_date, &tdates).await?;

    // 5. 构建日收益率 (含前置训练窗口, A股 + 6 ETFs)
    let mut daily_returns: Vec<(NaiveDate, Vec<f64>)> = Vec::new();
    for di in 1..all_tdates.len() {
        let d = all_tdates[di]; let pd = all_tdates[di-1];
        // A股收益: 使用因子选股回测权益曲线(fbt-dc4144c1, 覆盖2009-2026)
        let ap = a_nav.iter().find(|(td, _)| *td == pd).map(|(_, v)| *v);
        let ac = a_nav.iter().find(|(td, _)| *td == d).map(|(_, v)| *v);
        let (pp, cp) = match (ap, ac) {
            (Some(p1), Some(p2)) if p1 > 0.0 => (p1, p2),
            _ => continue,
        };
        let ar = cp/pp - 1.0;
        if ar.abs() > 0.5 { continue; }
        let mut row = vec![ar];
        let mut valid = true;
        for sym in &etf_symbols {
            let prices = etf_prices.get(sym.as_str());
            let ep = prices.and_then(|p| p.get(&pd)).copied().unwrap_or(0.0);
            let ec = prices.and_then(|p| p.get(&d)).copied().unwrap_or(0.0);
            if ep > 0.0 && ec > 0.0 {
                let r = ec/ep - 1.0;
                if r.abs() > 0.5 { valid = false; }
                row.push(r);
            } else { row.push(0.0); }
        }
        if valid { daily_returns.push((d, row)); }
    }
    let n_assets = 1 + etf_count;

    // 6. 月度聚合
    let mut monthly_rets: Vec<(String, NaiveDate, Vec<f64>)> = Vec::new();
    let mut cm: Option<(String, NaiveDate, Vec<f64>)> = None;
    for (d, rets) in &daily_returns {
        let mk = format!("{}-{:02}", d.format("%Y"), d.month());
        match &mut cm {
            Some((m, _, cum)) if *m == mk => {
                for j in 0..n_assets { cum[j] = (1.0+cum[j])*(1.0+rets[j])-1.0; }
            }
            _ => {
                if let Some((m, ld, cum)) = cm.take() { monthly_rets.push((m, ld, cum)); }
                cm = Some((mk, *d, rets.clone()));
            }
        }
    }
    if let Some((m, ld, cum)) = cm { monthly_rets.push((m, ld, cum)); }

    // ═══ 数据完整性检查 ═══
    crate::routes::data_validation::validate_equity_curve(&a_nav, start_date)?;
    crate::routes::data_validation::validate_data_coverage(
        &a_nav, &etf_prices, &etf_symbols, start_date, end_date)?;
    crate::routes::data_validation::validate_training_data(&monthly_rets, etf_count, 36)?;

    // 7. 逐日回放: 季度调仓时直接调用 MVO 模块
    let lookback: usize = 36;
    let mut weights: Vec<f64> = vec![0.10, 0.25, 0.35, 0.05, 0.15, 0.05, 0.05]; // A股,黄金,国债,SP500,纳指,有色,豆粕
    let mut last_q = String::new();
    let mut mvo_daily_rets: Vec<f64> = Vec::new();
    let mut mvo_start_idx = 0usize;
    let mut found_start = false;

    for (di, (d, rets)) in daily_returns.iter().enumerate() {
        let q_key = format!("{}-Q{}", d.format("%Y"), (d.month()-1)/3 + 1);
        let should_rebalance = if is_monthly {
            q_key != last_q  // every month
        } else {
            let is_q_month = matches!(d.month(), 1 | 4 | 7 | 10);
            is_q_month && q_key != last_q
        };

        if should_rebalance {
            let mk = format!("{}-{:02}", d.format("%Y"), d.month());
            if let Some(mi) = monthly_rets.iter().position(|(m, _, _)| m.as_str() >= mk.as_str()) {
                if mi >= lookback {
                    last_q = q_key.clone();
                    // 直接用MVO模块计算权重
                    let train: Vec<Vec<f64>> = monthly_rets[mi - lookback..mi]
                        .iter().map(|(_, _, r)| r.clone()).collect();
                    let n_months = train.len();
                    let flat: Vec<f64> = train.iter().flatten().copied().collect();
                    if let Some(arr) = Array2::from_shape_vec((n_months, n_assets), flat).ok() {
                        let v16_min = min_stock_override.unwrap_or(0.12); // v18 default
                        let regime_ms = if is_v17 { get_regime_min_stock(db, *d).await } else { v16_min };
                        let regime_exposure = detect_regime_exposure(db, *d).await;

                        let mvo_result = if is_v17 {
                            mvo::mvo_allocate_sortino_n(&arr, regime_ms, 0.06, 0.10)
                        } else if use_max_sharpe {
                            mvo::mvo_allocate(&arr, regime_ms)
                        } else if use_bl {
                            // P3: Black-Litterman approximated (60% hist + 40% equal-weight prior)
                            let prev_12: Vec<f64> = monthly_rets[mi-12..mi].iter()
                                .map(|(_, _, r)| r[0]).collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            let hist_mu = ndarray::Array1::from_vec(
                                (0..n_assets).map(|j| {
                                    let col: Vec<f64> = train.iter().map(|r| r[j]).collect();
                                    col.iter().sum::<f64>() / col.len() as f64 * 12.0
                                }).collect()
                            );
                            let prior_mu = ndarray::Array1::from_vec(vec![0.12_f64; n_assets]); // equal-weight prior
                            let bl_mu = 0.6 * &hist_mu + 0.4 * &prior_mu;
                            mvo::mvo_allocate_with_custom_mu(&arr, &bl_mu, regime_ms, dynamic_target, 0.10)
                        } else if use_momentum {
                            // v16m: momentum-adjusted expected returns
                            let prev_12: Vec<f64> = monthly_rets[mi-12..mi].iter()
                                .map(|(_, _, r)| r[0]).collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            // Blend: 60% historical mean + 40% recent momentum (6m annualized)
                            let hist_mu = ndarray::Array1::from_vec(
                                (0..n_assets).map(|j| {
                                    let col: Vec<f64> = train.iter().map(|r| r[j]).collect();
                                    col.iter().sum::<f64>() / col.len() as f64 * 12.0
                                }).collect()
                            );
                            let mom_mu = ndarray::Array1::from_vec(
                                (0..n_assets).map(|j| {
                                    let recent: Vec<f64> = train.iter().rev().take(6).map(|r| r[j]).collect();
                                    let cum: f64 = recent.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                                    cum.powf(2.0) - 1.0  // 6m → annualized
                                }).collect()
                            );
                            let bw = momentum_blend_ratio;
                            let adj_mu = bw * &hist_mu + (1.0 - bw) * &mom_mu;
                            let cm: mvo::CovMethod = cov_method.parse().unwrap_or(mvo::CovMethod::LinearLW);
                            mvo::mvo_allocate_with_cov_method(&arr, &adj_mu, regime_ms, dynamic_target, 0.10, cm)
                        } else if use_ewma {
                            // Exp B: EWMA covariance (λ=0.94) with dynamic target
                            let prev_12: Vec<f64> = monthly_rets[mi-12..mi].iter()
                                .map(|(_, _, r)| r[0]).collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            mvo::mvo_allocate_ewma_n(&arr, regime_ms, dynamic_target, 0.10, 0.94)
                        } else {
                            // v16 baseline with experiment options
                            let prev_12: Vec<f64> = monthly_rets[mi-12..mi].iter()
                                .map(|(_, _, r)| r[0]).collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let return_target = fixed_return_target.unwrap_or(
                                (cum - 1.0 + 0.05).clamp(0.08, 0.18)
                            );
                            // Exp: trend_boost — 趋势确认后提升min_stock
                            let effective_min = if trend_boost {
                                let trail_3m: f64 = prev_12.iter().rev().take(3).fold(1.0, |acc, r| acc * (1.0+r)) - 1.0;
                                // 简化的MA判断: 近3月累计正收益=上升趋势
                                if trail_3m > 0.0 && cum > 0.0 { 0.20_f64.max(regime_ms) } else { regime_ms }
                            } else { regime_ms };
                            // Exp: vol_budget — 逆波动率风险预算fallback(在MVO失败时用)
                            let _use_vol_budget = vol_budget;
                            mvo::mvo_allocate_with_target_n(&arr, effective_min, return_target, 0.10)
                        };

                        if let Some(result) = mvo_result {
                            let mut w = result.weights.to_vec();
                            // v17 only: ETF MA200 趋势过滤
                            if is_v17 {
                                for (ei, sym) in etf_symbols.iter().enumerate() {
                                    let wi = ei + 1;
                                    if wi >= w.len() || w[wi] <= 0.0 { continue; }
                                    if let Some(prices) = etf_prices.get(sym.as_str()) {
                                        let mut sorted_dates: Vec<NaiveDate> = prices.keys().copied().collect();
                                        sorted_dates.sort();
                                        let recent: Vec<f64> = sorted_dates.iter()
                                            .filter(|&&pd| pd <= *d).rev().take(200)
                                            .map(|pd| prices.get(pd).copied().unwrap_or(0.0)).collect();
                                        if recent.len() >= 200 {
                                            let ma200: f64 = recent.iter().sum::<f64>() / recent.len() as f64;
                                            if recent[0] < ma200 && recent[0] > 0.0 { w[wi] = 0.0; }
                                        }
                                    }
                                }
                            }
                            let w_sum: f64 = w.iter().sum();
                            if w_sum > 0.0 { for w_i in w.iter_mut() { *w_i /= w_sum; } }
                            // Exp: vol_budget — 用逆波动率权重做30%混合,增加分散化
                            if vol_budget && train.len() >= 12 {
                                let mut iv_w = vec![0.0f64; n_assets];
                                let mut iv_sum = 0.0f64;
                                for j in 0..n_assets {
                                    let col: Vec<f64> = train.iter().map(|r| r[j]).collect();
                                    let n = col.len() as f64;
                                    let mean = col.iter().sum::<f64>() / n;
                                    let var = col.iter().map(|x| (x-mean).powi(2)).sum::<f64>() / (n-1.0);
                                    iv_w[j] = 1.0 / (var.sqrt() + 0.01);
                                    iv_sum += iv_w[j];
                                }
                                if iv_sum > 0.0 { for j in 0..n_assets { iv_w[j] /= iv_sum; } }
                                for j in 0..n_assets { w[j] = 0.7 * w[j] + 0.3 * iv_w[j]; }
                                let w_sum3: f64 = w.iter().sum();
                                if w_sum3 > 0.0 { for w_i in w.iter_mut() { *w_i /= w_sum3; } }
                            }
                            for w_i in w.iter_mut() { *w_i *= regime_exposure; }
                            let w_sum2: f64 = w.iter().sum();
                            if w_sum2 > 0.0 { for w_i in w.iter_mut() { *w_i /= w_sum2; } }
                            weights = w;
                            if !found_start { mvo_start_idx = di; found_start = true; }
                        }
                    }
                }
            }
        }

        let mvo_ret: f64 = weights.iter().zip(rets.iter()).map(|(w, r)| w * r).sum();
        mvo_daily_rets.push(mvo_ret);
    }

    if mvo_daily_rets.len() < 60 { return Err("MVO收益序列不足".into()); }

    // 8. 应用杠杆 (vol_target or fixed)
    let mut leveraged_rets: Vec<f64> = Vec::with_capacity(mvo_daily_rets.len());
    let mut trail_60: Vec<f64> = Vec::new();
    // 预计算CSI300 regime用于自适应vol target
    let mut csi_trail_12m: Vec<f64> = Vec::with_capacity(csi_rets.len());
    let mut csi_cum = 1.0f64;
    for (i, r) in csi_rets.iter().enumerate() {
        csi_cum *= 1.0 + r;
        if i >= 252 {
            csi_trail_12m.push(csi_cum / csi_trail_12m[i - 252] - 1.0);
        } else {
            csi_trail_12m.push(0.0);
        }
    }

    for (i, &mvo_r) in mvo_daily_rets.iter().enumerate() {
        trail_60.push(mvo_r);
        if trail_60.len() > 60 { trail_60.remove(0); }
        let lev = if use_vol_target && trail_60.len() >= 20 {
            let n = trail_60.len() as f64;
            let m = trail_60.iter().sum::<f64>() / n;
            let v = if n > 1.0 { trail_60.iter().map(|r| (r - m).powi(2)).sum::<f64>() / (n - 1.0) } else { 0.0 };
            let ann_vol = v.sqrt() * (252.0_f64).sqrt();
            // P0: 自适应波动率目标 (bull=25%, bear=15%, neutral=20%)
            let target_vol = if adaptive_vol_target && i < csi_trail_12m.len() {
                let t12 = csi_trail_12m[i];
                if t12 > 0.10 { 0.25 } else if t12 < -0.15 { 0.15 } else { 0.20 }
            } else { 0.20 };
            let cap = leverage_cap;
            if ann_vol > 0.05 { (target_vol / ann_vol).clamp(0.5, cap) } else { 1.0 }
        } else if fixed_lev > 1.0 { fixed_lev } else { 1.0 };
        leveraged_rets.push(mvo_r * lev);
    }

    // 9. 计算绩效指标 (使用杠杆后收益)
    let final_rets = if use_vol_target || fixed_lev > 1.0 { &leveraged_rets } else { &mvo_daily_rets };
    let mvo_rets = &final_rets[mvo_start_idx..];
    let mvo_start_date = daily_returns[mvo_start_idx].0.format("%Y-%m-%d").to_string();
    let n = mvo_rets.len() as f64;

    let mean = mvo_rets.iter().sum::<f64>() / n;
    let var = mvo_rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_vol = var.sqrt();
    let ann_ret = (1.0 + mean).powf(252.0) - 1.0;
    let ann_vol = daily_vol * (252.0_f64).sqrt();
    let rf = 0.02;
    let sharpe = if ann_vol > 0.0 { (ann_ret - rf) / ann_vol } else { 0.0 };

    let mut nav = 1.0_f64; let mut peak = 1.0_f64; let mut max_dd = 0.0_f64;
    for r in mvo_rets { nav *= 1.0 + r; peak = peak.max(nav); max_dd = max_dd.max((peak-nav)/peak); }
    let cumulative = nav - 1.0;
    let calmar = if max_dd > 0.0 { ann_ret / max_dd } else { 0.0 };

    let down: Vec<f64> = mvo_rets.iter().filter(|&&r| r < 0.0).copied().collect();
    let down_std = if down.len() > 1 {
        let dm = down.iter().sum::<f64>() / down.len() as f64;
        (down.iter().map(|r| (r-dm).powi(2)).sum::<f64>() / (down.len()-1) as f64).sqrt()
    } else { 0.0 };
    let ann_down = down_std * (252.0_f64).sqrt();
    let sortino = if ann_down > 0.0 { (ann_ret - rf) / ann_down } else { 0.0 };
    let wr = mvo_rets.iter().filter(|&&r| r > 0.0).count() as f64 / mvo_rets.len() as f64;

    // 9. 逐年收益
    let mut yearly: Vec<YearlyReturn> = Vec::new();
    let mut yn: f64 = 1.0; let mut ys: f64 = 1.0; let mut py: Option<String> = None;
    for (idx, &r) in mvo_rets.iter().enumerate() {
        let di = mvo_start_idx + idx;
        let yr = daily_returns[di].0.format("%Y").to_string();
        if py.as_deref() != Some(&yr) {
            if let Some(ref p) = py {
                let ret_pct: f64 = ((yn/ys - 1.0) * 1000.0).round() / 10.0;
                yearly.push(YearlyReturn { year: p.clone(), return_pct: ret_pct });
            }
            ys = yn; py = Some(yr);
        }
        yn *= 1.0 + r;
    }
    if let Some(p) = py {
        let ret_pct: f64 = ((yn/ys - 1.0) * 1000.0).round() / 10.0;
        yearly.push(YearlyReturn { year: p, return_pct: ret_pct });
    }

    // 10. 基准指标
    fn bench_metrics_with_yearly(rets: &[f64], dates: &[NaiveDate]) -> BenchmarkMetrics {
        let n = rets.len() as f64; let rf = 0.02;
        if n < 60.0 { return BenchmarkMetrics { annual_return_pct:0.,max_drawdown_pct:0.,sharpe_ratio:0.,sortino_ratio:0.,yearly_returns:vec![] }; }
        let mean = rets.iter().sum::<f64>()/n;
        let ann_ret = (1.0+mean).powf(252.0)-1.0;
        let var = rets.iter().map(|r|(r-mean).powi(2)).sum::<f64>()/(n-1.0);
        let ann_vol = var.sqrt()*(252.0_f64).sqrt();
        let sharpe = if ann_vol>0.0 {(ann_ret-rf)/ann_vol} else {0.0};
        let mut nav: f64 = 1.0; let mut peak: f64 = 1.0; let mut mdd: f64 = 0.0;
        for r in rets { nav *= 1.0 + r; peak = peak.max(nav); mdd = mdd.max((peak - nav) / peak); }
        let down:Vec<f64> = rets.iter().filter(|&&r|r<0.0).copied().collect();
        let ds = if down.len()>1 {
            let dm=down.iter().sum::<f64>()/down.len() as f64;
            (down.iter().map(|r|(r-dm).powi(2)).sum::<f64>()/(down.len()-1)as f64).sqrt()*(252.0_f64).sqrt()
        } else {0.01};
        let sortino = if ds>0.0 {(ann_ret-rf)/ds} else {0.0};
        let mut yearly = Vec::new();
        let mut yn: f64 = 1.0; let mut ys: f64 = 1.0; let mut py: Option<String> = None;
        for (i, r) in rets.iter().enumerate() {
            let yr = dates[i].format("%Y").to_string();
            if py.as_deref() != Some(&yr) {
                if let Some(ref p) = py {
                    let ret_pct: f64 = ((yn/ys - 1.0) * 1000.0).round() / 10.0;
                    yearly.push(YearlyReturn { year: p.clone(), return_pct: ret_pct });
                }
                ys = yn; py = Some(yr);
            }
            yn *= 1.0 + r;
        }
        if let Some(p) = py {
            let ret_pct: f64 = ((yn/ys - 1.0) * 1000.0).round() / 10.0;
            yearly.push(YearlyReturn { year: p, return_pct: ret_pct });
        }
        let ar_pct: f64 = (ann_ret * 1000.0).round() / 10.0;
        let dd_pct: f64 = (mdd * 1000.0).round() / 10.0;
        let sr_val: f64 = (sharpe * 100.0).round() / 100.0;
        let so_val: f64 = (sortino * 100.0).round() / 100.0;
        BenchmarkMetrics {
            annual_return_pct: ar_pct,
            max_drawdown_pct: dd_pct,
            sharpe_ratio: sr_val,
            sortino_ratio: so_val,
            yearly_returns: yearly,
        }
    }

    let benchmarks = Benchmarks {
        csi300: bench_metrics_with_yearly(&csi_rets, &csi_dates),
        sp500: bench_metrics_with_yearly(&sp500_rets, &sp500_dates),
        gold: bench_metrics_with_yearly(&gold_rets, &gold_dates),
    };

    info!("[replay] v17 回放完成: AR={:.1}% DD={:.1}% SR={:.2} SO={:.2}",
        ann_ret*100.0, max_dd*100.0, sharpe, sortino);

    let ar_pct: f64 = (ann_ret * 1000.0).round() / 10.0;
    let cum_pct: f64 = (cumulative * 1000.0).round() / 10.0;
    let dd_pct: f64 = (max_dd * 1000.0).round() / 10.0;
    let sr: f64 = (sharpe * 100.0).round() / 100.0;
    let so: f64 = (sortino * 100.0).round() / 100.0;
    let vol_pct: f64 = (ann_vol * 1000.0).round() / 10.0;
    let cal: f64 = (calmar * 100.0).round() / 100.0;
    let wr_pct: f64 = (wr * 1000.0).round() / 10.0;

    Ok(ReplayResult {
        start_date: tdates.first().map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
        end_date: tdates.last().map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
        trading_days: tdates.len(),
        annual_return_pct: ar_pct,
        cumulative_return_pct: cum_pct,
        max_drawdown_pct: dd_pct,
        sharpe_ratio: sr,
        sortino_ratio: so,
        volatility_pct: vol_pct,
        calmar_ratio: cal,
        win_rate_pct: wr_pct,
        yearly_returns: yearly,
        benchmarks,
        mvo_start_date,
        mvo_trading_days: mvo_rets.len(),
    })
}
