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
    compute_lw_mvo_weights, detect_regime_exposure, load_etf_premium_map, resolved_to_legacy_sc,
    send_dingtalk_alert, MvoWeightCache, PaperAccountRepository, PgPaperAccountRepo,
    StrategyConfig,
};
use sha1::{Digest, Sha1};
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, warn};

/// 信号最小权重(占 NAV): 低于此值的 A股目标不进信号。
/// 执行端一手价格上界按 ~5000 元覆盖(0.1% × 500 万 NAV), 消除不足一手的
/// 确定性碎单(2026-09-18: 002955 目标 1001 元被引擎拒单计入"失败"的源头治理)。
pub(crate) const MIN_SIGNAL_WEIGHT: f64 = 0.001;

/// PTrade 执行通道配置(DB 表 ptrade_channel_config, 未来页面化管理)。
/// quant 账户 → Windows 信号目录映射: 生产账号 D:/ptrade_sync, 仿真账号 D:/ptrade_sync_test。
/// (2026-09-18: 去掉未读的 account_id/is_production 字段, 查询列同步收窄——
/// 生产/仿真差异化处理需求出现时再随用例加回。)
#[derive(sqlx::FromRow)]
pub struct PtradeChannel {
    pub channel_name: String,
    pub scp_target: String,
    pub enabled: bool,
}

/// 本地留存目录(按账户分子目录防双通道同名覆盖; env 可覆盖根目录)。
fn local_signal_dir(account_id: &str) -> PathBuf {
    let root =
        std::env::var("PTRADE_SIGNAL_DIR").unwrap_or_else(|_| "/tmp/quant_signals".to_string());
    PathBuf::from(root).join(account_id)
}

async fn load_channel(db: &PgPool, account_id: &str) -> Result<PtradeChannel, String> {
    let ch: Option<PtradeChannel> = sqlx::query_as(
        "SELECT channel_name, scp_target, enabled
         FROM ptrade_channel_config WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("channel config: {}", e))?;
    match ch {
        Some(c) if c.enabled => Ok(c),
        Some(_) => Err(format!("通道 {} 已停用(enabled=false)", account_id)),
        None => Err(format!(
            "账户 {} 无 ptrade_channel_config 配置(未配通道目录,拒绝推送防误发)",
            account_id
        )),
    }
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
                    &format!(
                        "⛔ [PTrade信号] 账户 {} 信号生成失败,当日实盘无信号(降级为持有不动):\n{}",
                        account_id, e
                    ),
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

    // 0. 执行通道配置(DB: ptrade_channel_config——生产/仿真各自目录, 未来页面化管理)
    let channel = load_channel(db, account_id).await?;

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

    // 2b. 物化新鲜度门禁(任务71, 2026-09-21): T 日 combo 截面必须由今晚
    // (回填完成后)的物化产生。夜间链 fire-and-forget 时期物化吃 T-1 因子,
    // 信号截面静默滞后一交易日(multi_factor_value created_at 实锤)——本门禁
    // 是依赖编排(wait_for_factor_backfill)失效时的兜底: 物化先于回填完成/
    // 未物化/链未跑, 三种残缺一律拒绝导出(宁缺毋假, 与截面空拒绝同哲学)。
    let (combo_cnt, combo_min_created): (i64, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as(
            "SELECT COUNT(*), MIN(created_at) FROM multi_factor_value
             WHERE combo_name = $1 AND trade_date = $2",
        )
        .bind(&sc.combo_name)
        .bind(date)
        .fetch_one(db)
        .await
        .map_err(|e| format!("combo 截面查询: {}", e))?;
    let today_local = chrono::Local::now().date_naive();
    // 非交易日分支（2026-09-26）：假日/周末无新截面产生——截面的"最新物化"就是
    // 最近交易日夜链的产物（天然早于今天的任何回填重跑），按旧口径必被误判
    // "旧因子物化"（2026-09-25 中秋假日 23:00 导出失败实证：9/24 截面行写入
    // 9/24 22:30 < 假日重跑回填 9/25 22:10 → 拒绝）。非交易日改验"截面存在 +
    // 写入时刻属于最近交易日的夜间链时段"（>= 该日 21:00 本地），通过即放行。
    let is_trade_today: bool = sqlx::query_scalar(
        "SELECT is_open FROM market_trade_calendar
         WHERE exchange = 'SSE' AND trade_date = $1",
    )
    .bind(today_local)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("交易日历查询(物化门禁): {}", e))?
    .flatten()
    .unwrap_or(true); // 日历缺失保持原口径
    let mut skip_freshness_gate = false;
    if !is_trade_today {
        let last_trade: Option<(chrono::NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE exchange = 'SSE' AND is_open = true AND trade_date < $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(today_local)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("最近交易日查询(物化门禁): {}", e))?;
        if let Some((last_trade_date,)) = last_trade {
            use chrono::TimeZone;
            let night_chain_start_utc = last_trade_date
                .and_hms_opt(21, 0, 0)
                .and_then(|nd| chrono::Local.from_local_datetime(&nd).single())
                .map(|dt| dt.with_timezone(&chrono::Utc));
            if let Some(night_start) = night_chain_start_utc {
                if let Some(min_created) = combo_min_created {
                    if min_created >= night_start {
                        tracing::info!(
                            "[PTrade信号] 非交易日({})放行: 截面{}物化于{}(最近交易日{}夜链产物)",
                            today_local.format("%Y-%m-%d"),
                            date,
                            min_created.format("%m-%d %H:%M"),
                            last_trade_date.format("%m-%d")
                        );
                        skip_freshness_gate = true;
                    }
                }
            }
        }
        // 夜链产物校验不通过则落入原口径（由其给出具体拒绝理由）
    }
    if !skip_freshness_gate {
        let day_start_utc = today_local
            .and_hms_opt(0, 0, 0)
            .and_then(|nd| {
                use chrono::TimeZone;
                chrono::Local
                    .from_local_datetime(&nd)
                    .single()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            })
            .ok_or_else(|| "时区转换失败(物化门禁)".to_string())?;
        let backfill_done: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT MAX(completed_at) FROM data_sync_task
         WHERE task_type LIKE '%backfill%' AND created_at >= $1
           AND completed_at IS NOT NULL",
        )
        .bind(day_start_utc)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("回填任务查询: {}", e))?
        .flatten();
        verify_combo_materialization_freshness(
            (combo_cnt, combo_min_created),
            backfill_done,
            &sc.combo_name,
            date,
        )?;
    } // !skip_freshness_gate

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
    // 最小权重过滤(2026-09-18 碎单治理): 占 NAV 低于 0.1%(50 万账户即 500 元)的
    // 目标在执行端必然不足一手(引擎确定性不委托, 0918 实例 002955 目标 1001 元),
    // 只产生 failed 噪音与碎股残留。从信号源头剔除; 已持仓标的被剔除=目标 0 全卖,
    // 忠实于"目标≈0"的策略意图且清掉碎股残留, 差异上限 0.1%×NAV 可忽略。
    let mut n_filtered = 0usize;
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
        let w_final = mvo_a_pct * weight_d * lev;
        if w_final.abs() < MIN_SIGNAL_WEIGHT {
            n_filtered += 1;
            continue;
        }
        targets.push((p.symbol.clone(), w_final));
    }
    if n_filtered > 0 {
        warn!(
            "[PTrade信号] 最小权重过滤: {} 只 A股标的权重 < {:.1}% 未入信号(碎单源头治理)",
            n_filtered,
            MIN_SIGNAL_WEIGHT * 100.0
        );
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
    // ETF 溢价门禁·方向感知(2026-09-17): 溢价 > +gate 禁买可卖(高溢价买入承受回归
    // 损失, 卖出占便宜); 折价 < -gate 禁卖可买; 区间内正常双向。阈值策略层配置
    // (strategy_config.etf_premium_gate, 默认 0.10)。数据缺失放行(降级取向)。
    let premium_gate = sc.etf_premium_gate;
    let premium_map: HashMap<String, crate::routes::shared::EtfPremium> =
        load_etf_premium_map(db, date, &listed, premium_gate).await;
    let gate_hit: Vec<String> = premium_map
        .iter()
        .filter(|(_, p)| !p.is_free())
        .map(|(s, _)| s.clone())
        .collect();
    if !gate_hit.is_empty() {
        let detail = gate_hit
            .iter()
            .map(|s| {
                let p = &premium_map[s];
                format!(
                    "{} 溢价{:+.1}%→{}",
                    to_ptrade_symbol(s),
                    p.premium_pct.unwrap_or(0.0) * 100.0,
                    if p.blocks_side("buy") {
                        "禁买可卖"
                    } else {
                        "禁卖可买"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        warn!(
            "[PTrade信号] 溢价门禁命中 {} 只 ETF(阈值 {:.0}%): {}",
            gate_hit.len(),
            premium_gate * 100.0,
            detail
        );
    }
    // 溢价存量退出 overlay(2026-09-18 回测定版, 与 rebalance_account 同源组件):
    // 持有 + 溢价>gate → 目标置 0(清仓, 高溢价持有负期望); 未持有 + >=gate/2 →
    // 滞回不买回; <gate/2 恢复; 折价豁免(正期望, 禁卖门禁已保护)。
    // 状态由 mirror 账户持仓承载(live 账户未激活时持仓空集=按未持有处理, 天然安全)。
    let etf_holding: std::collections::HashSet<String> = sqlx::query_scalar(
        "SELECT symbol FROM paper_position WHERE paper_account_id = $1 AND quantity > 0",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .unwrap_or_default()
    .into_iter()
    .collect();
    let etf_allocs = build_etf_allocations(&mvo_weights, regime, &listed);
    let etf_allocs = crate::routes::shared::apply_premium_exit_overlay(
        &etf_allocs,
        &premium_map,
        &etf_holding,
        premium_gate,
    );
    for (sym, w) in etf_allocs {
        targets.push((sym, w * lev));
    }

    // 5. 信号 JSON(定版契约 §3.1: symbol .SS/.SZ, trade_date YYYYMMDD, checksum 全payload)
    let trade_date = next_trading_day(db, date).await?;
    let nav_dec: rust_decimal::Decimal = PgPaperAccountRepo::new(db)
        .find_current_nav_or_capital(account_id)
        .await
        .map_err(|e| format!("nav: {}", e))?
        .ok_or_else(|| format!("nav: 账号 {} 不存在", account_id))?;
    let nav: f64 = nav_dec.to_string().parse().unwrap_or(0.0);

    let mut signal = serde_json::json!({
        "version": 1,
        "signal_id": format!("{}_{}_001", rs.strategy_id, trade_date.format("%Y%m%d")),
        "account_type": if leverage_enabled { "margin" } else { "cash" },
        "trade_date": trade_date.format("%Y%m%d").to_string(),
        "generated_at": chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        // 主窗口保持 09:35-10:30(正常标的); 门禁命中标的的交易推迟到复牌后的
        // 延迟窗口(10:30 溢价停牌复牌, 留 5 分钟行情稳定), 执行器按 blocked_side
        // 在延迟窗口只执行允许方向(高溢价→只卖, 高折价→只买)。
        "execute_window": {"start": "09:35", "end": "10:30"},
        "deferred_execute_window": {"start": "10:35", "end": "11:30"},
        "nav_estimate": (nav * 100.0).round() / 100.0,
        "target_positions": targets
            .iter()
            .map(|(sym, w)| {
                let mut item = serde_json::json!({
                    "symbol": to_ptrade_symbol(sym),
                    "target_weight": (*w * 10000.0).round() / 10000.0,
                    "side": "auto",
                });
                // blocked_side 协议(2026-09-17 方向感知): "buy"=禁买可卖, "sell"=禁卖可买。
                // target_weight 保留策略原值(回放对账), 执行端按方向跳过对应委托。
                if let Some(p) = premium_map.get(sym) {
                    let side = if p.blocks_side("buy") {
                        "buy"
                    } else if p.blocks_side("sell") {
                        "sell"
                    } else {
                        ""
                    };
                    if !side.is_empty() {
                        item["blocked_side"] = serde_json::json!(side);
                        item["premium_pct"] = serde_json::json!(
                            (p.premium_pct.unwrap_or(0.0) * 100.0 * 10.0).round() / 10.0
                        );
                    }
                }
                item
            })
            .collect::<Vec<_>>(),
        "total_target_weight": (targets.iter().map(|(_, w)| *w).sum::<f64>() * 10000.0).round() / 10000.0,
    });
    let checksum = canonical_sha1(&signal);
    signal["checksum"] = serde_json::json!(checksum);

    // 6. 本地留存 + scp 推送(失败重试 1 次; 推送失败=当日实盘无信号, 钉钉已告警)
    let dir = local_signal_dir(account_id);
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {}", dir.display(), e))?;
    let fname = format!("signal_{}_001.json", trade_date.format("%Y%m%d"));
    let local = dir.join(&fname);
    std::fs::write(&local, serde_json::to_string_pretty(&signal).unwrap())
        .map_err(|e| format!("write {}: {}", local.display(), e))?;
    scp_push(&local, &channel.scp_target).await?;

    let n_etf = listed.len() + 1; // +现金段按需
    let gate_msg = if gate_hit.is_empty() {
        String::new()
    } else {
        format!(
            "\n🚫 溢价门禁 {} 只(阈值{:.0}%): {}",
            gate_hit.len(),
            premium_gate * 100.0,
            gate_hit
                .iter()
                .map(|s| {
                    let p = &premium_map[s];
                    format!(
                        "{}({:+.1}%→{})",
                        to_ptrade_symbol(s),
                        p.premium_pct.unwrap_or(0.0) * 100.0,
                        if p.blocks_side("buy") {
                            "禁买"
                        } else {
                            "禁卖"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    Ok(format!(
        "📤 [PTrade信号] {}({}) 已生成并推送[{}] → {} 执行\nA股 {} 只 + ETF {} 只, 权重和 {:.3}, regime {:.2}, lev {:.2}x{}\nsignal_id={} checksum={:.8}…",
        name, account_id, channel.channel_name, trade_date.format("%Y-%m-%d"), positions.len(), n_etf,
        signal["total_target_weight"].as_f64().unwrap_or(0.0), regime,
        leverage_d_f64(&leverage_d), gate_msg,
        signal["signal_id"].as_str().unwrap_or(""), checksum
    ))
}

// ── 内部组件 ──────────────────────────────────────────────────────

/// 物化新鲜度判定的纯逻辑(任务71): 与 DB 查询分离以便单测三分支。
/// - 截面日无 combo 行 → 拒绝(夜间链未物化)
/// - 当日无已完成回填任务 → 拒绝(夜间链未跑, 截面新鲜度无从谈起)
/// - combo 行最早写入时刻 <= 回填完成时刻 → 拒绝(物化先于回填完成=旧因子物化,
///   即 fire-and-forget 时期每晚的真实形态)
pub(crate) fn verify_combo_materialization_freshness(
    combo_row: (i64, Option<chrono::DateTime<chrono::Utc>>),
    backfill_done: Option<chrono::DateTime<chrono::Utc>>,
    combo_name: &str,
    date: chrono::NaiveDate,
) -> Result<(), String> {
    let (combo_cnt, combo_min_created) = combo_row;
    let min_created = combo_min_created.ok_or_else(|| {
        format!(
            "combo {} 截面日 {} 无物化行(夜间链物化未覆盖), 拒绝导出",
            combo_name, date
        )
    })?;
    let _ = combo_cnt; // 行数为 0 时 MIN 必为 None, 上面已拦截; 计数仅日志价值
    let backfill_at = backfill_done.ok_or_else(|| {
        format!(
            "当日无已完成因子回填任务(夜间链未跑?), combo {} 截面新鲜度无法确认, 拒绝导出",
            combo_name
        )
    })?;
    if min_created <= backfill_at {
        return Err(format!(
            "combo {} 截面 {} 行写入({})早于因子回填完成({})=旧因子物化, 拒绝导出",
            combo_name,
            date,
            min_created.format("%Y-%m-%d %H:%M"),
            backfill_at.format("%H:%M")
        ));
    }
    Ok(())
}

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

async fn next_trading_day(
    db: &PgPool,
    after: chrono::NaiveDate,
) -> Result<chrono::NaiveDate, String> {
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

async fn scp_push(local: &std::path::Path, scp_target: &str) -> Result<(), String> {
    let arg = local.to_string_lossy().to_string();
    for attempt in 0..2 {
        let st = tokio::process::Command::new("scp")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=15")
            .arg("-o")
            .arg("StrictHostKeyChecking=accept-new")
            .arg(&arg)
            .arg(scp_target)
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
    let start = (date - chrono::Duration::days(180))
        .format("%Y%m%d")
        .to_string();
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
        (
            "portfolio_volatility_control",
            "portfolio_volatility_control",
        ),
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
        .post(format!(
            "http://localhost:{}/api/v1/quant/backtests/run-factor",
            port
        ))
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

    /// 最小权重过滤语义(2026-09-18 碎单源头治理): 0.1% 边界恰在阈值上保留,
    /// 0.02%(0918 实例 002955 的实际权重)被剔除——执行端不足一手的确定性碎单
    /// 从信号源头消除, 权重和只含幸存标的(checksum 随之自洽)。
    #[test]
    #[allow(clippy::assertions_on_constants)] // 文档性常量断言：语义锁定阈值边界
    fn min_signal_weight_threshold_semantics() {
        assert_eq!(
            MIN_SIGNAL_WEIGHT, 0.001,
            "阈值 0.1% 定版, 改动需评审碎单边界"
        );
        // 0918 实例复刻: 0.0002 权重(目标 ~1001 元/500 万 NAV)必被过滤
        assert!(0.0002_f64 < MIN_SIGNAL_WEIGHT);
        // 正常最小持仓权重(等权 1/50 = 2%)远在阈值之上不受影响
        assert!(0.02_f64 > MIN_SIGNAL_WEIGHT);
    }

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

    /// 物化新鲜度门禁三分支(任务71): 2026-09-21 事故实锤时间线复刻——
    /// 物化 22:11:25 落库, 回填 22:12~22:35 完成, 旧序下 combo 写入早于回填
    /// 完成 → 必拒; 修正编排后(等待回填→物化) → 必过。
    #[test]
    fn combo_materialization_freshness_gate_branches() {
        use chrono::TimeZone;
        let date = chrono::NaiveDate::from_ymd_opt(2026, 9, 21).unwrap();
        let mat_at = chrono::Utc
            .with_ymd_and_hms(2026, 9, 21, 14, 11, 25)
            .unwrap(); // 22:11:25 +08
        let backfill_at = chrono::Utc
            .with_ymd_and_hms(2026, 9, 21, 14, 35, 33)
            .unwrap(); // 22:35:33 +08

        // 分支1: 截面日无物化行(MIN=NULL) → 拒
        let err = verify_combo_materialization_freshness((0, None), Some(backfill_at), "c1", date)
            .unwrap_err();
        assert!(err.contains("无物化行"), "分支1 文案: {}", err);

        // 分支2: 当日无已完成回填任务 → 拒
        let err = verify_combo_materialization_freshness((5919, Some(mat_at)), None, "c1", date)
            .unwrap_err();
        assert!(err.contains("夜间链未跑"), "分支2 文案: {}", err);

        // 分支3: 事故形态——物化(22:11:25)早于回填完成(22:35:33) → 拒
        let err = verify_combo_materialization_freshness(
            (5919, Some(mat_at)),
            Some(backfill_at),
            "c1",
            date,
        )
        .unwrap_err();
        assert!(err.contains("旧因子物化"), "分支3 文案: {}", err);

        // 分支4: 修正编排后——物化(22:36:10)晚于回填完成(22:35:33) → 过
        let fixed_mat = chrono::Utc
            .with_ymd_and_hms(2026, 9, 21, 14, 36, 10)
            .unwrap();
        assert!(verify_combo_materialization_freshness(
            (5919, Some(fixed_mat)),
            Some(backfill_at),
            "c1",
            date
        )
        .is_ok());
    }

    /// blocked_side 协议字段参与 checksum 的双端对齐(2026-09-17 方向感知门禁):
    /// Python 端 json.dumps(sort_keys, ensure_ascii=False, separators) 预计算基准。
    #[test]
    fn canonical_sha1_with_blocked_fields() {
        let payload = serde_json::json!({
            "version": 1, "signal_id": "t2", "account_type": "cash",
            "trade_date": "20260918", "generated_at": "2026-09-17T23:30:00",
            "nav_estimate": 5000000.0,
            "execute_window": {"start": "09:35", "end": "10:30"},
            "deferred_execute_window": {"start": "10:35", "end": "11:30"},
            "target_positions": [
                {"symbol": "511010.SS", "target_weight": 0.1214, "side": "auto"},
                {"symbol": "513100.SS", "target_weight": 0.1214, "side": "auto",
                 "blocked_side": "buy", "premium_pct": 13.6},
            ],
            "total_target_weight": 0.2428
        });
        assert_eq!(
            canonical_sha1(&payload),
            "61e52b7e48baa3bb0ce6b7549e5313acefdc13b8"
        );
    }

    /// 基金净值历史回补(一次性, 2026-09-17 溢价门禁一期)。
    /// 活跃策略 ETF 并集回补 2 年(sync_fund_nav 无历史自动回补 730 天)。
    /// 运行:
    ///   set -a; source ../.env; source ../.env.quant; set +a;
    ///   cargo test --release -p quant-api fund_nav_backfill -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn fund_nav_backfill() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let cfg = quant_data::tushare::client::TushareConfig {
            token: std::env::var("TUSHARE_TOKEN").expect("TUSHARE_TOKEN"),
            rate_limit_per_minute: 60,
            ..Default::default()
        };
        let client = quant_data::tushare::client::TushareClient::new(cfg).expect("client");

        let etfs = crate::routes::strategy_query::load_active_etf_symbols_union(&db).await;
        println!("[fund_nav_backfill] 回补标的: {:?}", etfs);
        let n = quant_data::sync::sync_fund_nav(&db, &client, &etfs)
            .await
            .expect("sync_fund_nav");
        println!("[fund_nav_backfill] 完成, 新增 {} 行", n);

        // 验证: 每标的最近净值
        let latest = quant_data::repository::latest_fund_navs(&db, &etfs)
            .await
            .expect("latest_fund_navs");
        for (sym, (d, v)) in &latest {
            println!("[fund_nav_backfill] {} nav_date={} unit_nav={}", sym, d, v);
        }
        assert_eq!(latest.len(), etfs.len(), "部分标的无净值");
    }

    /// 溢价门禁查询实测(2026-09-17 回放零触发排障):
    /// 2020-04-20 南方原油溢价 63% 应命中 block_buy。
    /// 运行: cargo test --release -p quant-api premium_gate_probe -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn premium_gate_probe() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let date = chrono::NaiveDate::from_ymd_opt(2020, 4, 20).unwrap();
        let etfs = vec!["501018.SH".to_string()];
        let m = load_etf_premium_map(&db, date, &etfs, 0.10).await;
        println!("[premium_gate_probe] map = {:?}", m);
        assert!(
            m.contains_key("501018.SH"),
            "map 缺 501018(查询失败或数据缺)"
        );
        let p = &m["501018.SH"];
        println!(
            "[premium_gate_probe] premium={:?} block_buy={} block_sell={}",
            p.premium_pct, p.block_buy, p.block_sell
        );
        assert!(p.block_buy, "63% 溢价未触发 block_buy");
    }

    /// dividend 送转字段回填(一次性, 2026-09-17 公司行动调整):
    /// 对 2019-06 以来复权因子跳变过的股票重拉 dividend(stk_div 等字段旧白名单遗漏)。
    /// sync_dividend 全历史分页拉取, upsert 覆盖旧行。
    /// 运行: set -a; source .env; source .env.quant; set +a;
    ///   cargo test --release -p quant-api dividend_stkdiv_backfill -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn dividend_stkdiv_backfill() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let cfg = quant_data::tushare::client::TushareConfig {
            token: std::env::var("TUSHARE_TOKEN").expect("TUSHARE_TOKEN"),
            rate_limit_per_minute: 60,
            ..Default::default()
        };
        let client = quant_data::tushare::client::TushareClient::new(cfg).expect("client");

        let symbols: Vec<String> = sqlx::query_scalar(
            r#"WITH f AS (SELECT symbol, adj_factor,
                        LAG(adj_factor) OVER (PARTITION BY symbol ORDER BY trade_date) AS pf
                        FROM market_adjustment_factor WHERE trade_date >= '2019-06-01')
               SELECT DISTINCT symbol FROM f
               WHERE pf IS NOT NULL AND ABS(adj_factor/pf - 1) > 0.05"#,
        )
        .fetch_all(&db)
        .await
        .expect("跳变标的查询");
        println!(
            "[dividend_backfill] 重拉 {} 只标的的 dividend(全历史, 限速约50分钟)",
            symbols.len()
        );
        let dv = format!(
            "div-stkdiv-backfill-{}",
            chrono::Local::now().format("%Y%m%d%H%M")
        );
        let n =
            quant_data::sync::sync_dividend(&db, &client, &symbols, "20190601", "20260917", &dv)
                .await
                .expect("sync_dividend");
        println!("[dividend_backfill] 完成 {} 行", n);
        // 验证: 送转字段非空
        let filled: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM market_stock_dividend WHERE stk_div IS NOT NULL AND div_proc='实施'",
        )
        .fetch_one(&db)
        .await
        .expect("验证查询");
        println!("[dividend_backfill] stk_div 非空实施记录 {} 条", filled);
        assert!(filled > 1000, "送转字段回填异常");
    }

    /// fund_div 基金分红回填(一次性, 2026-09-17 复权体系 P0-2):
    /// 活跃策略 ETF 并集全历史分红(fund_div 数据量小)。
    /// 运行: set -a; source .env; source .env.quant; set +a;
    ///   cargo test --release -p quant-api fund_div_backfill -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn fund_div_backfill() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let cfg = quant_data::tushare::client::TushareConfig {
            token: std::env::var("TUSHARE_TOKEN").expect("TUSHARE_TOKEN"),
            rate_limit_per_minute: 60,
            ..Default::default()
        };
        let client = quant_data::tushare::client::TushareClient::new(cfg).expect("client");
        let etfs = crate::routes::strategy_query::load_active_etf_symbols_union(&db).await;
        println!("[fund_div_backfill] 标的: {:?}", etfs);
        let n = quant_data::sync::sync_fund_div(&db, &client, &etfs)
            .await
            .expect("sync_fund_div");
        println!("[fund_div_backfill] 完成 {} 条", n);
        for (s, e, c) in sqlx::query_as::<_, (String, chrono::NaiveDate, rust_decimal::Decimal)>(
            "SELECT symbol, ex_date, div_cash FROM market_fund_div ORDER BY ex_date DESC LIMIT 8",
        )
        .fetch_all(&db)
        .await
        .expect("验证查询")
        {
            println!("[fund_div_backfill] {} ex={} div_cash={}", s, e, c);
        }
    }
}

// ══ 第六批覆盖专项(2026-09-22, 调度域·信号导出) ══
// 靶点: 通道配置三分支(load_channel) / 交易日历取日(next_trading_day /
// latest_trading_day) / run-factor 请求体构造(build_run_factor_body 默认值
// 全量 + WFA 参数合并与风控键透传) / 物化新鲜度门禁相等边界。不直调:
// run_ptrade_signal_export(空参会向生产钉钉群发告警)、export_signal_for_account
// (HTTP 自调用 8080 生产容器 + scp 外推 + 写生产信号文件)、run_factor_task(触发
// 真实回测)、scp_push(外部网络 + 45s 级超时分支)。
#[cfg(test)]
mod sixth_batch {
    use super::*;

    async fn test_db() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        PgPool::connect(&url).await.expect("test db connect")
    }

    /// 测试用 StrategyConfig 字面量(与 scheduler.rs tests 模块同款,
    /// 避免触发 panic 版 Default)。
    fn sc_literal() -> StrategyConfig {
        StrategyConfig {
            etf_premium_gate: 0.10,
            allocation_mode: None,
            mu_estimation: None,
            strategy_id: "zzz_test_sixth".into(),
            name: "zzz_test_sixth".into(),
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
            slippage_pct: 0.002,
            mvo_objective: "minvariance".into(),
            kelly_fraction: 0.25,
            score_candidate_pool_size: 200,
            regime_policy: None,
            regime_bear_return_threshold: -0.03,
        }
    }

    /// 通道配置独占键清理(前/后置, 仅删本测试行)。
    async fn cleanup_channel(db: &PgPool) {
        let _ = sqlx::query("DELETE FROM ptrade_channel_config WHERE paper_account_id = $1")
            .bind("zzz_test_sixth_channel_acct")
            .execute(db)
            .await;
    }

    /// 通道配置三分支: 无配置行拒绝(防误发) / 停用拒绝 / 启用原样返回。
    #[tokio::test]
    async fn load_channel_missing_disabled_and_enabled_branches() {
        let db = test_db().await;
        // 前置 + 结尾精确清理(仅删本测试独占键行)
        cleanup_channel(&db).await;

        // 分支1: 无配置行 → 拒绝(未配通道目录, 防误发)
        let err = load_channel(&db, "zzz_test_sixth_channel_acct")
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("无 ptrade_channel_config 配置"), "{err}");

        // 分支2: enabled=false → 拒绝
        sqlx::query(
            "INSERT INTO ptrade_channel_config (paper_account_id, channel_name, scp_target, enabled)
             VALUES ($1, 'zzz_test_channel', 'zzz_test_host:/tmp/zzz', false)
             ON CONFLICT (paper_account_id) DO UPDATE
               SET enabled = false, channel_name = EXCLUDED.channel_name,
                   scp_target = EXCLUDED.scp_target",
        )
        .bind("zzz_test_sixth_channel_acct")
        .execute(&db)
        .await
        .expect("insert disabled channel");
        let err = load_channel(&db, "zzz_test_sixth_channel_acct")
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("已停用"), "{err}");

        // 分支3: enabled=true → 原样返回
        sqlx::query("UPDATE ptrade_channel_config SET enabled = true WHERE paper_account_id = $1")
            .bind("zzz_test_sixth_channel_acct")
            .execute(&db)
            .await
            .expect("enable channel");
        let channel = load_channel(&db, "zzz_test_sixth_channel_acct")
            .await
            .expect("启用通道应返回配置");
        assert_eq!(channel.channel_name, "zzz_test_channel");
        assert_eq!(channel.scp_target, "zzz_test_host:/tmp/zzz");
        assert!(channel.enabled);

        cleanup_channel(&db).await;
    }

    /// 交易日历取日: 下一交易日跳过周末 / 超出日历视野报错 / 最近已到交易日
    /// 必须开市且不晚于今天。
    #[tokio::test]
    async fn trading_day_helpers_wrap_calendar_and_error_beyond_horizon() {
        let db = test_db().await;
        // 2026-09-18(周五)开市 → 下一交易日为 2026-09-21(周一), 周末被跳过
        let next = next_trading_day(&db, chrono::NaiveDate::from_ymd_opt(2026, 9, 18).unwrap())
            .await
            .expect("2026-09-18 后必有下一交易日");
        assert_eq!(next, chrono::NaiveDate::from_ymd_opt(2026, 9, 21).unwrap());

        // 日历只配到 2026 年末 → 2100 年无未来交易日(错误路径)
        let err = next_trading_day(&db, chrono::NaiveDate::from_ymd_opt(2100, 1, 1).unwrap())
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("无未来交易日"), "{err}");

        // 最近已到交易日(<= today): 动态断言, 数据增长/任意日期运行均不失效
        let latest = latest_trading_day(&db).await.expect("交易日历应有数据");
        let today = chrono::Local::now().date_naive();
        assert!(
            latest <= today,
            "最近交易日不应晚于今天: {latest} > {today}"
        );
        let is_open: Option<bool> = sqlx::query_scalar(
            "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1 LIMIT 1",
        )
        .bind(latest)
        .fetch_optional(&db)
        .await
        .ok()
        .flatten()
        .flatten();
        assert_eq!(is_open, Some(true), "最近已到交易日必须开市: {latest}");
    }

    /// WFA 参数行独占键清理(前/后置)。
    async fn cleanup_wfa_row(db: &PgPool) {
        let _ = sqlx::query(
            "DELETE FROM wfa_strategy_params WHERE experiment_run_id = 'zzz_test_sixth_exp'",
        )
        .execute(db)
        .await;
    }

    /// WFA 窗口外日期 → 全部字段回落默认值, 风控可选键不写入。
    #[tokio::test]
    async fn build_run_factor_body_defaults_when_wfa_out_of_window() {
        let db = test_db().await;
        let sc = StrategyConfig {
            combo_name: "zzz_test_sixth_combo".into(),
            top_n: 12,
            ..sc_literal()
        };
        // 2010 年远早于 wfa_strategy_params 覆盖窗(2019-01 起) → wfa 空
        let date = chrono::NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let body = build_run_factor_body(&db, &sc, date).await;

        assert_eq!(body["combo_name"], "zzz_test_sixth_combo");
        assert_eq!(body["version"], "1.0.0");
        assert_eq!(body["strategy_version_id"], "phase7-professional-v1");
        assert_eq!(body["top_n"].as_u64(), Some(12));
        assert_eq!(body["rebalance"], "40");
        assert_eq!(body["max_position_pct"].as_f64(), Some(0.10));
        assert_eq!(body["max_gross_exposure"].as_f64(), Some(0.95));
        assert_eq!(body["score_direction"], "descending");
        assert_eq!(body["portfolio_method"], "heuristic");
        assert_eq!(body["benchmark"], "000300.SH");
        assert_eq!(body["skip_top_pct"].as_f64(), Some(0.0));
        assert_eq!(body["entry_delay"].as_i64(), Some(1));
        assert_eq!(body["universe_profile"], "main_board_non_st");
        // 窗口: 当日截面往前 180 天(与模拟盘 [ALIGN] 口径一致)
        assert_eq!(
            body["start_date"],
            (date - chrono::Duration::days(180))
                .format("%Y%m%d")
                .to_string()
        );
        assert_eq!(body["end_date"], date.format("%Y%m%d").to_string());
        // data_version_id 动态取最新, 只断言非空
        assert!(
            body["data_version_id"]
                .as_str()
                .is_some_and(|v| !v.is_empty()),
            "data_version_id 不应为空"
        );
        // WFA 未命中 → 风控可选键一律不写入
        for key in [
            "stop_loss_pct",
            "event_gate_combo_name",
            "candidate_risk_filter",
            "max_pairwise_correlation",
            "portfolio_volatility_control",
        ] {
            assert!(body.get(key).is_none(), "未命中 WFA 不应写 {key}");
        }
    }

    /// WFA 命中 → combo/top_n/rebalance 覆盖默认值, 字符串型风控参数按原样
    /// 透传(执行端自行解析), WFA 未覆盖的键回落默认。
    #[tokio::test]
    async fn build_run_factor_body_merges_wfa_params_and_risk_passthrough() {
        let db = test_db().await;
        cleanup_wfa_row(&db).await;
        // 生产覆盖窗止于 2026-01-28, 2026-03 窗口为空档 → zzz 行独占命中;
        // score 置 999 保证即便生产补了同窗行也以本行为准
        sqlx::query(
            "INSERT INTO wfa_strategy_params (experiment_run_id, window_index, test_start,
                 test_end, parameters, score)
             VALUES ('zzz_test_sixth_exp', 0, '2026-03-01', '2026-03-31', $1, 999.0)
             ON CONFLICT (experiment_run_id, window_index) DO UPDATE
               SET parameters = EXCLUDED.parameters, score = EXCLUDED.score",
        )
        .bind(serde_json::json!({
            "combo_name": "zzz_test_sixth_wfa_combo",
            "top_n": 7,
            "rebalance": "20",
            "max_position_pct": "0.25",
            "stop_loss_pct": "0.08",
            "max_pairwise_correlation": "0.5"
        }))
        .execute(&db)
        .await
        .expect("insert zzz wfa row");

        let sc = StrategyConfig {
            combo_name: "zzz_test_sixth_combo".into(),
            top_n: 12,
            ..sc_literal()
        };
        let body = build_run_factor_body(
            &db,
            &sc,
            chrono::NaiveDate::from_ymd_opt(2026, 3, 15).unwrap(),
        )
        .await;

        assert_eq!(body["combo_name"], "zzz_test_sixth_wfa_combo");
        assert_eq!(body["top_n"].as_u64(), Some(7));
        assert_eq!(body["rebalance"], "20");
        // 字符串数值字段: 解析为 f64 后写入
        assert_eq!(body["max_position_pct"].as_f64(), Some(0.25));
        // 纯透传字段: 保持字符串形态(与 scheduler 模拟盘段一致)
        assert_eq!(body["stop_loss_pct"], "0.08");
        assert_eq!(body["max_pairwise_correlation"], "0.5");
        // WFA 未覆盖的键回落默认
        assert_eq!(body["max_gross_exposure"].as_f64(), Some(0.95));

        cleanup_wfa_row(&db).await;
    }

    /// 物化新鲜度门禁相等边界: 写入时刻 == 回填完成时刻在语义上仍是
    /// 「回填完成前写入」(<=) → 拒绝(任务71 定版语义的边界锁定)。
    #[test]
    fn combo_materialization_freshness_equal_boundary_rejects() {
        use chrono::TimeZone;
        let t = chrono::Utc
            .with_ymd_and_hms(2026, 9, 21, 14, 30, 0)
            .unwrap();
        let err = verify_combo_materialization_freshness(
            (10, Some(t)),
            Some(t),
            "zzz_test_combo",
            chrono::NaiveDate::from_ymd_opt(2026, 9, 21).unwrap(),
        )
        .map(|_| ())
        .unwrap_err();
        assert!(err.contains("旧因子物化"), "相等时刻应拒绝: {err}");
    }

    /// 本地留存目录按账户分子目录(根目录可被 PTRADE_SIGNAL_DIR 覆盖,
    /// 只对账户段断言避免环境耦合)。
    #[test]
    fn local_signal_dir_segments_by_account_id() {
        let path = local_signal_dir("zzz_test_sixth_acct");
        assert!(path.ends_with("zzz_test_sixth_acct"), "{path:?}");
    }

    /// Decimal → f64 换算: 正常值精确换算, 零值保持零。
    #[test]
    fn leverage_d_f64_converts_decimal() {
        let d = rust_decimal::Decimal::new(125, 2); // 1.25
        assert!((leverage_d_f64(&d) - 1.25).abs() < 1e-12);
        assert_eq!(leverage_d_f64(&rust_decimal::Decimal::ZERO), 0.0);
    }
}
