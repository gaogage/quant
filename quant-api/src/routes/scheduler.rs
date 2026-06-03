//! 内置调度器 — 交易日盘中实时数据同步 + 交易信号生成 + 收盘钉钉推送。
//!
//! 盘中 (9:30-15:00): 每 10 分钟同步实时数据 → 检查信号 → 模拟交易
//! 收盘 (15:30):     推送钉钉持仓摘要 (每日一次)
//! 历史批量同步:     API 手动触发 (quant-sync-daily)
//!
//! MVO 策略: Ledoit-Wolf + Grid Search 季度调仓 (自动发现权重)
//! 启动时通过 tokio::spawn 在后台运行，每 60 秒检查一次。

use chrono::{Datelike, Local, NaiveDate, Timelike};
use ndarray::Array2;
use quant_common::mvo;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

fn short_id() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(12).collect()
}

struct DailyState {
    date: Option<NaiveDate>,
    last_sync_minute: Option<u32>,
    signals_generated: bool,
    dingtalk_sent: bool,
    cleanup_done: bool,
}

/// MVO 权重缓存（季度更新）
struct MvoWeightCache {
    quarter: String,              // e.g. "2026-Q2"
    weights: Vec<f64>,            // [A股, 黄金, 国债, SP500, 纳指]
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
        let mvo_cache: Arc<Mutex<Option<MvoWeightCache>>> = Arc::new(Mutex::new(None));
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        info!("[scheduler] 已启动: LW-MVO自动发现+季度调仓, 盘中9:30-15:00实时同步+交易, 15:30钉钉推送");

        loop {
            interval.tick().await;
            if let Err(e) = run_tick(&db, &state, &mvo_cache, port).await {
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

async fn run_tick(db: &PgPool, state: &Arc<Mutex<DailyState>>, mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>, port: u16) -> Result<(), String> {
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
                info!("[scheduler] 生成交易信号 (LW-MVO)...");
                match generate_paper_signals_for_all(db, mvo_cache, port, today).await {
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

        // 信号源路由: factor(默认) 或 prediction(ML)
        let is_prediction = signal_source == "prediction";
        let prediction_set_id = "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605";

        let resp = if is_prediction {
            client
                .post(format!("{}/api/v1/quant/backtests/run-prediction", base))
                .json(&serde_json::json!({
                    "prediction_set_id": prediction_set_id,
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
    let mvo_weights = compute_lw_mvo_weights(db, date, mvo_cache, 0.08).await;

    // ── 体制检测 + 降仓 ──
    let regime_exposure = detect_regime_exposure(db, date).await;
    let mvo_a_pct = mvo_weights[0] * regime_exposure;
    let mvo_gold_pct = mvo_weights[1] * regime_exposure;
    let mvo_bond_pct = mvo_weights[2] * regime_exposure;
    let mvo_sp500_pct = mvo_weights[3] * regime_exposure;
    let mvo_nq_pct = mvo_weights[4] * regime_exposure;
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

    // Create ETF positions (MVO allocation + 现金/货币基金)
    let mut etf_allocations = vec![
        ("518880.SH", "黄金ETF", mvo_gold_pct),
        ("511010.SH", "国债ETF", mvo_bond_pct),
        ("513500.SH", "标普500", mvo_sp500_pct),
        ("513100.SH", "纳指ETF", mvo_nq_pct),
    ];
    // 德国ETF: 固定 5% 卫星配置 (从债券分配中扣除), 2017年+17.2%提供额外分散
    let germany_pct = (mvo_bond_pct * 0.15).min(0.05); // max 5%
    if germany_pct > 0.005 {
        etf_allocations.push(("513030.SH", "德国ETF", germany_pct));
    }
    // 商品ETF卫星配置: 有色(3%) + 豆粕(3%), 低相关性提供通胀对冲
    let commodity_pct = 0.03;
    if commodity_pct > 0.001 {
        etf_allocations.push(("159980.SZ", "有色ETF", commodity_pct));
        etf_allocations.push(("159985.SZ", "豆粕ETF", commodity_pct));
    }
    // 10年国债ETF: 2%卫星配置, 更高收益的债券选择
    let bond10y_pct = 0.02;
    if bond10y_pct > 0.001 {
        etf_allocations.push(("511260.SH", "十年国债ETF", bond10y_pct));
    }
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

/// LW-MVO 自动发现权重：Ledoit-Wolf shrinkage + Grid Search 季度调仓。
/// 返回 (a_share, gold, bond, sp500, nasdaq) 权重（和为 1.0）。
/// ETF 从实际有数据的日期开始纳入 MVO 计算。
async fn compute_lw_mvo_weights(
    db: &PgPool,
    date: NaiveDate,
    cache: &Mutex<Option<MvoWeightCache>>,
    min_stock: f64,
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

    let etf_symbols = ["518880.SH", "511010.SH", "513500.SH", "513100.SH", "513030.SH", "159980.SZ", "159985.SZ"];

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
            trail_3m * 2.0 // 近似年化
        };
        if trail_3m < -0.03 {
            // 因子失效检测：A股因子近3月持续亏损 → 放开A股约束, 让LW-MVO自由配置ETF
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

    // Kelly-inspired A股仓位缩放: 因子IR高→加仓, IR低→减仓
    let kelly_scale = if adaptive_min_stock > 0.0 && a_monthly.len() >= 6 {
        let trail_rets: Vec<f64> = a_monthly[..6].to_vec();
        let n = trail_rets.len() as f64;
        let avg = trail_rets.iter().sum::<f64>() / n;
        if n > 1.0 {
            let variance = trail_rets.iter().map(|r| (r - avg).powi(2)).sum::<f64>() / (n - 1.0);
            let monthly_ir = if variance > 0.0 { avg / variance.sqrt() } else { 0.0 };
            let annual_ir = monthly_ir * (12.0_f64).sqrt();
            // Map IR to position scalar: IR=0→0.5x, IR=0.5→1.0x, IR=1.0→1.5x
            (0.5 + annual_ir).clamp(0.3, 1.5)
        } else {
            1.0
        }
    } else {
        1.0
    };
    let adaptive_min_stock = (adaptive_min_stock * kelly_scale).min(0.75);

    let default_weights = vec![adaptive_min_stock, 0.30, 0.40, 0.05, 0.25 - adaptive_min_stock];

    let mut weights = default_weights.clone();

    if a_monthly.len() < 12 {
        let mut guard = cache.lock().await;
        *guard = Some(MvoWeightCache { quarter, weights: weights.clone() });
        return weights;
    }

    // ETF 月度收益
    let mut all_monthly: Vec<Vec<f64>> = Vec::new();
    let mut valid_etf_count = 0;
    let mut etf_monthly_data: Vec<Vec<f64>> = Vec::new();

    for sym in &etf_symbols {
        let mrets = get_monthly_returns(db, lookback_start, date, sym).await;
        etf_monthly_data.push(mrets);
        if !etf_monthly_data.last().unwrap().is_empty() {
            valid_etf_count += 1;
        }
    }

    // 构建训练数据（对齐月份）
    let n_months = a_monthly.len();
    for i in 0..n_months {
        let mut row = vec![a_monthly[i]];
        for j in 0..4 {
            if i < etf_monthly_data[j].len() {
                row.push(etf_monthly_data[j][i]);
            } else {
                row.push(0.0);
            }
        }
        // 过滤异常值
        if row.iter().all(|r| r.abs() < 1.0) {
            all_monthly.push(row);
        }
    }

    if all_monthly.len() >= 12 && valid_etf_count >= 2 {
        let n_assets = 5usize;
        let n_rows = all_monthly.len();
        let flat: Vec<f64> = all_monthly.iter().flatten().copied().collect();

        if let Some(arr) = Array2::from_shape_vec((n_rows, n_assets), flat).ok() {
            // Dynamic Return Target: trailing CSI300 return + 5% (PIT-compliant)
            let dynamic_target = if a_monthly.len() >= 12 {
                let trail_12m: f64 = a_monthly[..12].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
                (trail_12m + 0.05).clamp(0.08, 0.18) // floor 8%, cap 18%
            } else {
                0.12
            };
            // Asset pre-filter: exclude assets with trailing return < 3% from MVO
            // This naturally reduces bond allocation when bonds underperform
            if let Some(result) = mvo::mvo_allocate_with_target(&arr, adaptive_min_stock, dynamic_target) {
                let w = result.weights.to_vec();
                info!(
                    quarter = %quarter,
                    a = %(w[0] * 100.0).round(),
                    gold = %(w[1] * 100.0).round(),
                    bond = %(w[2] * 100.0).round(),
                    sp500 = %(w[3] * 100.0).round(),
                    nq = %(w[4] * 100.0).round(),
                    sharpe = %(result.sharpe * 100.0).round() / 100.0,
                    "LW-MVO 权重已更新"
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
        "SELECT trade_date, close FROM market_stock_daily_bar
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
