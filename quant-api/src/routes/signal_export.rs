//! PTrade 实盘信号生成器（B1' 文件桥, 2026-09-11）
//!
//! 23:30 定时任务(scheduled_task_config: ptrade_signal_export):
//! 数据门禁 → run-factor 当日截面 → 目标权重(与模拟盘同源组件) → JSON(定版契约)
//! → 本地留存 + scp 推送 gaocheng-center → 钉钉通知。
//!
//! 契约: docs/projects/quant/PTrade实盘对接设计方案.md §3.1/§3.2
//! 执行端: docs/projects/quant/ptrade/file_executor_v1.py(七重校验)
//!
//! 同源保证: 目标组装调用 rebalance.rs / shared.rs 的既有共享组件
//! (compute_lw_mvo_weights / detect_regime_exposure / compute_leverage_mult /
//!  select_positions / build_etf_allocations), 与 rebalance_account 相同口径:
//!   A股权重 = mvo_weights[0] × regime × 截面weight × leverage_d
//!   ETF权重 = (mvo_weights[k+1] × regime 或 1-regime现金段) × leverage_d
//! run-factor body 构造复刻 scheduler.rs 模拟盘段(WFA 参数 + signal_source 路由),
//! 改动需两处同步——对齐点标注 [ALIGN: scheduler.rs generate_paper_signals_for_all]。

use crate::routes::rebalance::{build_etf_allocations, compute_leverage_mult, select_positions};
use crate::routes::shared::{
    compute_lw_mvo_weights, detect_regime_exposure, resolved_to_legacy_sc, send_dingtalk_alert,
    MvoWeightCache, StrategyConfig,
};
use sha1::{Digest, Sha1};
use sqlx::PgPool;
use tracing::{error, warn};
use std::path::PathBuf;

/// scp 目标(Windows 侧信号目录; ssh config 别名 gaocheng-center = Tailscale 100.70.30.64)。
const SCP_TARGET: &str = "gaocheng-center:D:/ptrade_sync/signals/";
/// 本地留存目录(env 可覆盖; 默认 /tmp, 容器/裸跑均可写, 仅 debug 用途——
/// Windows 侧与邮件回报互为备份)。
fn local_signal_dir() -> PathBuf {
    std::env::var("PTRADE_SIGNAL_DIR")
        .unwrap_or_else(|_| "/tmp/quant_signals".to_string())
        .into()
}

/// 定时任务入口(scheduler.rs "ptrade_signal_export" 分支调用)。
/// params: {"accounts": ["pa-xxx"]} 或 {"account": "pa-xxx"}; 空 = 拒绝(不猜默认账户,
/// 防止误对非 PTrade 账户发实盘信号)。
pub async fn run_ptrade_signal_export(db: &PgPool, params: &serde_json::Value) {
    let accounts: Vec<String> = params
        .get("accounts")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .or_else(|| {
            params
                .get("account")
                .and_then(|v| v.as_str())
                .map(|s| vec![s.to_string()])
        })
        .unwrap_or_default();
    if accounts.is_empty() {
        send_dingtalk_alert(
            db,
            "⚠️ [PTrade信号] scheduled_task_config 未配置 accounts 参数,信号未生成(防误发)",
        )
        .await;
        return;
    }
    for account_id in &accounts {
        match export_signal_for_account(db, account_id).await {
            Ok(msg) => send_dingtalk_alert(db, &msg).await,
            Err(e) => {
                error!("[PTrade信号] 账户 {} 信号生成失败: {}", account_id, e);
                send_dingtalk_alert(
                    db,
                    &format!("⛔ [PTrade信号] 账户 {} 信号生成失败,当日实盘无信号(降级为持有不动):\n{}", account_id, e),
                )
                .await;
            }
        }
    }
}

async fn export_signal_for_account(db: &PgPool, account_id: &str) -> Result<String, String> {
    // 2026-09-14 修复: 用最近交易日而非自然日——周末/节假日补跑时(容器重启/延迟),
    // now() 是非交易日, run-factor 当日截面为空 → A股 sleeve 缺失的残缺信号
    // (09-14 事故: 周六 07:01 补跑生成仅 7 ETF 的降级信号, 若执行将清空 A 股持仓)。
    let date = latest_trading_day(db).await?;

    // 1. 账户与策略加载(与模拟盘调仓同路径: load_account → resolved → production 门禁)
    let loaded = crate::routes::account::load_account(db, account_id).await?;
    let name = loaded.name().to_string();
    let strategy_version_id = loaded.strategy_version_id().to_string();
    if strategy_version_id.is_empty() {
        return Err(format!("账号 {} 未配置 strategy_version_id", account_id));
    }
    let (leverage_enabled, leverage_multiplier, leverage_mode) = loaded.leverage_params();
    let rs = crate::routes::strategy::load_resolved_strategy(db, &strategy_version_id)
        .await?
        .promote_to_production(db)
        .await
        .map_err(|e| format!("实盘门禁拦截: {}", e))?;
    let sc = resolved_to_legacy_sc(&rs)?;

    // 2. 数据门禁(与模拟盘同款 BlockRequiredYellow; 失败=不导出, 宁缺毋假)
    crate::routes::sync::check_paper_account_data_readiness(
        db,
        account_id,
        None,
        crate::routes::sync::DataReadinessGate::BlockRequiredYellow,
        "ptrade_signal_export",
    )
    .await
    .map_err(|e| format!("数据门禁未通过: {}", e))?;

    // 3. run-factor 当日截面(夜间预备链 23:20 已就绪因子; 与模拟盘 T 日 09:35 同数据日)
    let body = build_run_factor_body(db, &sc, date).await;
    let task_id = run_factor_task(&body).await?;

    // 3b. 截面查询 + 质量门禁: 策略含 A股 sleeve 而截面为空 = 数据异常, 拒绝导出
    // (宁可无信号也不要残缺信号——残缺信号执行会清空 A 股持仓, 09-14 事故根因之二)
    let positions = select_positions(db, &task_id, date).await?;
    let has_a_share_sleeve = rs
        .assets
        .iter()
        .any(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare);
    if has_a_share_sleeve && positions.is_empty() {
        return Err(format!(
            "当日截面为空(task={} date={}): A股 sleeve 缺失将生成清仓信号, 拒绝导出",
            task_id, date
        ));
    }

    // 4. 目标权重(与 rebalance_account 同组件同口径)
    let cache = std::sync::Arc::new(tokio::sync::Mutex::new(None::<MvoWeightCache>));
    let mvo_weights = compute_lw_mvo_weights(db, date, &cache, &sc).await;
    let regime =
        detect_regime_exposure(db, date, sc.deep_bear_threshold, sc.deep_bear_exposure).await;
    let leverage_d = compute_leverage_mult(
        db,
        account_id,
        &sc,
        regime,
        leverage_enabled,
        leverage_multiplier,
        &leverage_mode,
    )
    .await;
    let mvo_a_pct = mvo_weights.first().copied().unwrap_or(0.0) * regime;

    let mut targets: Vec<(String, f64)> = Vec::new();
    // A股 sleeve(口径复刻 rebalance_account §7):
    //   目标权重(占NAV) = mvo_a_pct × weight_d × lev
    //   weight_d = 截面weight(>0) | 兜底 mv/total_mv × mvo_a_pct(weight=0 历史截面)
    // positions 已在 3b 查得并通过质量门禁
    let total_stock_mv: f64 = positions
        .iter()
        .map(|p| p.market_value.to_string().parse::<f64>().unwrap_or(0.0))
        .sum();
    let lev = leverage_d_f64(&leverage_d);
    for p in &positions {
        let w_pct = p.weight.to_string().parse::<f64>().unwrap_or(0.0);
        let mv_pct = p.market_value.to_string().parse::<f64>().unwrap_or(0.0);
        let weight_d = if p.weight > rust_decimal::Decimal::ZERO {
            w_pct
        } else if total_stock_mv > 0.0 {
            mv_pct / total_stock_mv * mvo_a_pct
        } else {
            0.0
        };
        targets.push((p.symbol.clone(), mvo_a_pct * weight_d * lev));
    }
    // ETF sleeve: build_etf_allocations 输出已含 regime 缩放与现金段(511880), 再乘 lev
    // (与 rebalance_account §8 alloc_amount = nav × alloc_pct × leverage_d 同口径)
    let etf_listed_map =
        crate::routes::rebalance::preload_etf_listed_map(db, &sc.etf_symbols, date).await;
    let listed: Vec<String> = sc
        .etf_symbols
        .iter()
        .filter(|s| etf_listed_map.get(*s).copied().unwrap_or(false))
        .cloned()
        .collect();
    for (sym, w) in build_etf_allocations(&mvo_weights, regime, &listed) {
        targets.push((sym, w * lev));
    }

    // 5. 信号 JSON(定版契约 §3.1: symbol .SS/.SZ, trade_date YYYYMMDD, checksum 全payload)
    let trade_date = next_trading_day(db, date).await?;
    let nav_dec: rust_decimal::Decimal = sqlx::query_scalar(
        "SELECT COALESCE(current_nav, initial_capital) FROM paper_account WHERE paper_account_id=$1",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("nav: {}", e))?;
    let nav: f64 = nav_dec.to_string().parse().unwrap_or(0.0);

    let mut signal = serde_json::json!({
        "version": 1,
        "signal_id": format!("{}_{}_001", rs.strategy_id, trade_date.format("%Y%m%d")),
        "account_type": if leverage_enabled { "margin" } else { "cash" },
        "trade_date": trade_date.format("%Y%m%d").to_string(),
        "generated_at": chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        "execute_window": {"start": "09:35", "end": "10:30"},
        "nav_estimate": (nav * 100.0).round() / 100.0,
        "target_positions": targets
            .iter()
            .map(|(sym, w)| serde_json::json!({
                "symbol": to_ptrade_symbol(sym),
                "target_weight": (*w * 10000.0).round() / 10000.0,
                "side": "auto",
            }))
            .collect::<Vec<_>>(),
        "total_target_weight": (targets.iter().map(|(_, w)| *w).sum::<f64>() * 10000.0).round() / 10000.0,
    });
    let checksum = canonical_sha1(&signal);
    signal["checksum"] = serde_json::json!(checksum);

    // 6. 本地留存 + scp 推送(失败重试 1 次; 推送失败=当日实盘无信号, 钉钉已告警)
    let dir = local_signal_dir();
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {}", dir.display(), e))?;
    let fname = format!("signal_{}_001.json", trade_date.format("%Y%m%d"));
    let local = dir.join(&fname);
    std::fs::write(&local, serde_json::to_string_pretty(&signal).unwrap())
        .map_err(|e| format!("write {}: {}", local.display(), e))?;
    scp_push(&local).await?;

    let n_etf = listed.len() + 1; // +现金段按需
    Ok(format!(
        "📤 [PTrade信号] {}({}) 已生成并推送 → {} 执行\nA股 {} 只 + ETF {} 只, 权重和 {:.3}, regime {:.2}, lev {:.2}x\nsignal_id={} checksum={:.8}…",
        name, account_id, trade_date.format("%Y-%m-%d"), positions.len(), n_etf,
        signal["total_target_weight"].as_f64().unwrap_or(0.0), regime,
        leverage_d_f64(&leverage_d), signal["signal_id"].as_str().unwrap_or(""), checksum
    ))
}

// ── 内部组件 ──────────────────────────────────────────────────────

/// quant 内部 .SH 后缀 → PTrade .SS; .SZ 不变。(契约: §3.1 symbol 统一 PTrade 风格)
pub(crate) fn to_ptrade_symbol(sym: &str) -> String {
    if let Some(code) = sym.strip_suffix(".SH") {
        format!("{}.SS", code)
    } else {
        sym.to_string()
    }
}

/// sha1(规范化 JSON): 与 Python 生成端/执行端三方一致——
/// json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',',':'))。
/// serde_json::Value 默认 BTreeMap 键序 == sort_keys; 紧凑序列化 == separators;
/// 不转义非 ASCII == ensure_ascii=False。勿启用 preserve_order feature(会破坏键序)。
pub(crate) fn canonical_sha1(payload: &serde_json::Value) -> String {
    let canonical = serde_json::to_string(payload).unwrap_or_default();
    let mut h = Sha1::new();
    h.update(canonical.as_bytes());
    let out = h.finalize();
    out.iter().map(|b| format!("{:02x}", b)).collect()
}

async fn next_trading_day(db: &PgPool, after: chrono::NaiveDate) -> Result<chrono::NaiveDate, String> {
    let row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT trade_date FROM market_trade_calendar
         WHERE is_open = true AND trade_date > $1 ORDER BY trade_date LIMIT 1",
    )
    .bind(after)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("calendar: {}", e))?;
    row.map(|d| d.0)
        .ok_or_else(|| "交易日历无未来交易日".to_string())
}

/// 最近已到交易日(<= today): 信号截面与因子均以此日为准, 周末/节假日补跑不退化。
async fn latest_trading_day(db: &PgPool) -> Result<chrono::NaiveDate, String> {
    let row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT trade_date FROM market_trade_calendar
         WHERE is_open = true AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 1",
    )
    .bind(chrono::Local::now().date_naive())
    .fetch_optional(db)
    .await
    .map_err(|e| format!("calendar: {}", e))?;
    row.map(|d| d.0).ok_or_else(|| "交易日历无数据".to_string())
}

async fn scp_push(local: &PathBuf) -> Result<(), String> {
    let arg = local.to_string_lossy().to_string();
    for attempt in 0..2 {
        let st = tokio::process::Command::new("scp")
            .arg("-o").arg("BatchMode=yes").arg("-o").arg("ConnectTimeout=15")
            .arg("-o").arg("StrictHostKeyChecking=accept-new")
            .arg(&arg).arg(SCP_TARGET)
            .output()
            .await
            .map_err(|e| format!("scp spawn: {}", e))?;
        if st.status.success() {
            return Ok(());
        }
        let err = String::from_utf8_lossy(&st.stderr);
        warn!("[PTrade信号] scp 第{}次失败: {}", attempt + 1, err.trim());
        if attempt == 1 {
            return Err(format!("scp 推送失败(重试后): {}", err.trim()));
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    unreachable!()
}

/// [ALIGN: scheduler.rs generate_paper_signals_for_all] run-factor 请求体构造。
/// 复刻模拟盘 WFA 参数合并 + signal_source 路由; 两处需同步改。
async fn build_run_factor_body(
    db: &PgPool,
    sc: &StrategyConfig,
    date: chrono::NaiveDate,
) -> serde_json::Value {
    let start = (date - chrono::Duration::days(180)).format("%Y%m%d").to_string();
    let end = date.format("%Y%m%d").to_string();
    let wfa = crate::routes::scheduler::get_current_wfa_params(db, date)
        .await
        .unwrap_or_default();
    let g = |k: &str| wfa.get(k).and_then(|v| v.as_str());
    let body = serde_json::json!({
        "combo_name": g("combo_name").unwrap_or(sc.combo_name.as_str()),
        "version": "1.0.0",
        "strategy_version_id": "phase7-professional-v1",
        "data_version_id": crate::routes::scheduler::get_latest_data_version(db).await,
        "top_n": wfa.get("top_n").and_then(|v| v.as_u64()).unwrap_or(sc.top_n as u64),
        "rebalance": g("rebalance").unwrap_or("40"),
        "max_position_pct": g("max_position_pct").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.10),
        "max_gross_exposure": g("max_gross_exposure").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.95),
        "score_direction": g("score_direction").unwrap_or("descending"),
        "portfolio_method": g("portfolio_method").unwrap_or("heuristic"),
        "benchmark": "000300.SH",
        "skip_top_pct": g("skip_top_pct").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0),
        "entry_delay": 1,
        "universe_profile": "main_board_non_st",
        "start_date": start,
        "end_date": end,
    });
    // 可选风控参数透传(WFA 命中才写; 与 scheduler 段一致)
    let mut body = body;
    for (wfa_key, body_key) in [
        ("stop_loss_pct", "stop_loss_pct"),
        ("event_gate_combo_name", "event_gate_combo_name"),
        ("event_gate_mode", "event_gate_mode"),
        ("event_gate_score_direction", "event_gate_score_direction"),
        ("candidate_risk_filter", "candidate_risk_filter"),
        ("portfolio_volatility_control", "portfolio_volatility_control"),
        ("portfolio_drawdown_control", "portfolio_drawdown_control"),
        ("risk_contribution_control", "risk_contribution_control"),
    ] {
        if let Some(v) = g(wfa_key) {
            body[body_key] = serde_json::json!(v);
        }
    }
    for (wfa_key, body_key) in [
        ("max_pairwise_correlation", "max_pairwise_correlation"),
        ("partial_rebalance_ratio", "partial_rebalance_ratio"),
        ("event_gate_min_score", "event_gate_min_score"),
    ] {
        if let Some(v) = g(wfa_key) {
            body[body_key] = serde_json::json!(v);
        }
    }
    body
}

async fn run_factor_task(body: &serde_json::Value) -> Result<String, String> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(1800))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .post(format!("http://localhost:{}/api/v1/quant/backtests/run-factor", port))
        .json(body)
        .send()
        .await
        .map_err(|e| format!("run-factor http: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!("run-factor http {}", resp.status()));
    }
    let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    v.get("data")
        .and_then(|d| d.get("task_id"))
        .and_then(|t| t.as_str())
        .map(String::from)
        .ok_or_else(|| "run-factor 未返回 task_id".to_string())
}

fn leverage_d_f64(d: &rust_decimal::Decimal) -> f64 {
    d.to_string().parse().unwrap_or(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// checksum 三方对齐(Python 生成端 make_test_signal.py / 执行端 file_executor_v1.py):
    /// 基准值由 Python json.dumps(sort_keys, ensure_ascii=False, separators) 预计算。
    #[test]
    fn canonical_sha1_matches_python_reference() {
        let payload = serde_json::json!({
            "version": 1,
            "signal_id": "t1",
            "account_type": "cash",
            "trade_date": "20260914",
            "generated_at": "2026-09-11T23:30:00",
            "nav_estimate": 1000000.0,
            "target_positions": [{"symbol": "511010.SS", "target_weight": 0.5, "side": "auto"}],
            "total_target_weight": 0.5
        });
        assert_eq!(
            canonical_sha1(&payload),
            "3d4d77a043c87ab594b431aa73c5b8f6e7532528"
        );
    }

    #[test]
    fn symbol_mapping_sh_to_ss() {
        assert_eq!(to_ptrade_symbol("511880.SH"), "511880.SS");
        assert_eq!(to_ptrade_symbol("600519.SH"), "600519.SS");
        assert_eq!(to_ptrade_symbol("159915.SZ"), "159915.SZ");
        assert_eq!(to_ptrade_symbol("000001.SZ"), "000001.SZ");
    }
}
