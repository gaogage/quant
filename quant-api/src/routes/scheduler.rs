//! 内置调度器 — v15 日频量化交易。
//!
//! 09:35 (开盘后早间): 前置数据校验 → 生成信号 → 调仓 → 钉钉推送
//! 21:30 (盘后数据就绪, 任务73 前移): 同步日终行情数据到历史表 → 清理过期回测数据(16:00-16:30)
//!
//! 工作时间定版(任务73, 2026-09-22 用户裁决): 非工作段 = 00:00-08:30 /
//! 16:30-21:00 / 非交易日全天(可部署/关机休眠); 其余为工作段,定时任务
//! 执行和完成必须落在工作段内。
//! 日频交易不需要盘中实时行情，每天只在开盘后调仓一次。
//! MVO 策略: Ledoit-Wolf + Grid Search 季度调仓 (自动发现权重)
//! 杠杆: 波动率目标 (vol_target, 20%年化波动率目标)
//! 启动时通过 tokio::spawn 在后台运行，每 60 秒检查一次。

use super::sync::{check_paper_account_data_readiness, DataReadinessGate};
use chrono::{Datelike, Local, NaiveDate, Timelike};
use quant_data::tushare::client::TushareClient;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::routes::shared::{
    compute_lw_mvo_weights, resolved_to_legacy_sc, send_dingtalk_alert, send_dingtalk_alert_titled,
    send_quality_alert, MvoWeightCache, PaperAccountRepository, PgPaperAccountRepo, StrategyConfig,
};
use crate::routes::strategy::{AssetClass, ResolvedStrategy};

/// 因子回填窗口（天）：覆盖 T+1 延迟 + 周末缺口 + 断档自愈余量。
/// 2026-09-18 7→14: 7-01~07-08 管道断档 6 个交易日(8 自然日)恰好滑出 7 天窗口,
/// 之后每晚回填永远刷不到断档期(永久留洞)。14 天让两周内断档可自愈;
/// 更长断档靠 data_quality 滞缓告警 + 手动区间回填(warmup 已修复必成功)。
const BACKFILL_WINDOW_DAYS: chrono::Duration = chrono::Duration::days(14);

/// v24 因子全量回填路由清单（覆盖白名单全部 14 活跃因子）。
/// 新增因子类别时只需在此处添加一行，factor_backfill 任务和 T+1 补偿同步共用。
/// (route_path_segment, label)：route_path_segment 拼接到 `/api/v1/quant/factors/{seg}/background`。
/// P4-1: DB 驱动的回填路由 — 当 `factor_backfill_route` 表启用路由为空时作为 fallback。
/// 新增因子类别只需在 DB 表 INSERT 一行即可（无需改代码 + 重新编译部署）。
const V24_BACKFILL_ROUTES_FALLBACK: &[(&str, &str)] = &[
    ("phase7-price-volume-backfill", "量价+amihud"),
    ("p42b-defensive-low-vol-quality-backfill", "防御低波质量"),
    ("phase7-financial-quality-backfill", "财务质量"),
    ("phase7-financial-quality-change-backfill", "财务质量变化"),
    ("phase7-growth-recovery-backfill", "质量增长恢复"),
    ("phase7-moneyflow-backfill", "资金流"),
    ("phase7-moneyflow-congestion-backfill", "资金流拥挤度"),
    (
        "phase7-forecast-revision-surprise-backfill",
        "分析师预测修正",
    ),
    ("phase7-market-residual-risk-backfill", "市场残差风险"),
    ("phase7-block-trade-supply-demand-backfill", "大宗交易"),
    ("phase7-repurchase-supply-shock-backfill", "回购"),
    ("phase7-liquidity-quality-backfill", "流动性质量"),
];

/// 获取本服务 API base URL（供 scheduler 内部 HTTP 自调用）。
/// 统一抽取，消除 `http://localhost:{port}` 重复硬编码。
fn self_api_base() -> String {
    format!(
        "http://localhost:{}",
        std::env::var("PORT").unwrap_or_else(|_| "8080".into())
    )
}

/// 触发 v24 全量因子回填路由（factor_backfill 任务和 T+1 补偿同步共用）。
/// 幂等：每个路由独立 POST，失败仅 warn 不中断后续路由。
///
/// P4-1: 路由清单从 `factor_backfill_route` 表读取（按 priority 升序，仅 enabled=true）。
/// 表为空时回退到 `V24_BACKFILL_ROUTES_FALLBACK` 常量，保证 DB 异常时链路不中断。
/// 新增因子类别只需 INSERT 一行到 factor_backfill_route，无需改代码。
async fn trigger_v24_backfill_routes(db: &PgPool, start_date: &str, end_date: &str) {
    let api_base = self_api_base();
    // 任务79: 连接超时 5s 防自身 API 端口不可达时串行拖慢整链（实测 12 条路由
    // 对不可达端口每条 ~1.3s 双栈尝试，无连接超时时依赖单路由 10s 总超时兜底）。
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    // P4-1: 优先从 DB 读取路由清单，表空时回退硬编码常量。
    let routes: Vec<(String, String)> = match sqlx::query_as::<_, (String, String)>(
        "SELECT route_name, label FROM factor_backfill_route
             WHERE enabled = true ORDER BY priority ASC, route_name ASC",
    )
    .fetch_all(db)
    .await
    {
        Ok(rows) if !rows.is_empty() => rows,
        Ok(_) => {
            warn!(
                    "[scheduler] factor_backfill: factor_backfill_route 表无启用路由，回退到硬编码常量 ({} 条)",
                    V24_BACKFILL_ROUTES_FALLBACK.len()
                );
            V24_BACKFILL_ROUTES_FALLBACK
                .iter()
                .map(|(r, l)| (r.to_string(), l.to_string()))
                .collect()
        }
        Err(e) => {
            warn!(
                    "[scheduler] factor_backfill: 读取 factor_backfill_route 失败 ({}), 回退到硬编码常量",
                    e
                );
            V24_BACKFILL_ROUTES_FALLBACK
                .iter()
                .map(|(r, l)| (r.to_string(), l.to_string()))
                .collect()
        }
    };

    info!(
        "[scheduler] factor_backfill: 触发 {} 类因子回填 {}~{}",
        routes.len(),
        start_date,
        end_date
    );
    for (route, label) in &routes {
        let url = format!("{}/api/v1/quant/factors/{}/background", api_base, route);
        let result = client
            .post(&url)
            .json(&serde_json::json!({
                "start_date": start_date,
                "end_date": end_date
            }))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                info!("[scheduler] factor_backfill {} ok", label);
            }
            Ok(resp) => {
                warn!(
                    "[scheduler] factor_backfill {} HTTP {}: {}",
                    label,
                    resp.status().as_u16(),
                    resp.text().await.unwrap_or_default()
                );
            }
            Err(e) => {
                warn!("[scheduler] factor_backfill {} 请求失败: {}", label, e);
            }
        }
    }
    // 静默 db 引用，保持签名统一供未来扩展（如读 DB 配置覆盖路由清单）。
    let _ = db;
}

/// 等待 `since` 之后创建的因子回填任务全部到达终态(任务71, 2026-09-21)。
///
/// 依赖编排修复: 原 nightly_signal_prep 对回填路由 fire-and-forget(POST /background
/// 受理即返回), PIT 物化紧随其后执行, 吃到的是未回填的 factor_value——实锤
/// 2026-09-21: multi_factor_value T 日行 created_at=22:11:25 早于全部 12 类回填
/// 完成(22:12~22:35), 每晚信号截面实际由 T-1 交易日因子合成(静默滞后一天)。
///
/// 轮询 data_sync_task 中 `since` 后创建的 `*backfill*` 任务直至全部终态
/// (completed/failed/partial/timeout/cancelled); 超时硬上限防链悬挂——超时后
/// 照常继续后续步骤, 残缺物化由 ptrade_signal_export 的物化新鲜度门禁兜底拒发。
async fn wait_for_factor_backfill(
    db: &PgPool,
    since: chrono::DateTime<chrono::Utc>,
    timeout: std::time::Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let pending: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM data_sync_task
             WHERE created_at >= $1 AND task_type LIKE '%backfill%'
               AND status IN ('pending','running','cancel_requested')",
        )
        .bind(since)
        .fetch_one(db)
        .await
        .unwrap_or((0,));
        if pending.0 == 0 {
            info!(
                "[夜间预备] 因子回填全部终态(等待至 {})",
                chrono::Local::now().format("%H:%M:%S")
            );
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(
                "[夜间预备] 因子回填等待超时({}s), 仍有 {} 个未终态任务, 继续后续步骤(物化门禁兜底)",
                timeout.as_secs(),
                pending.0
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

/// 获取最新 EOD 数据版本（动态，确保回测使用最新数据而非硬编码的旧版本）
pub(crate) async fn get_latest_data_version(db: &PgPool) -> String {
    // R11: 转调 PgDataVersionRegistry 集中化（原内联 SQL 收敛到 versioning 模块）。
    use quant_data::versioning::PgDataVersionRegistry;
    PgDataVersionRegistry::new(db).latest_eod_version_id().await
}

struct DailyState {
    date: Option<NaiveDate>,
    traded_today: bool,     // 今日是否已完成调仓 (14:40+)
    eod_synced_today: bool, // 今日是否已完成日终数据同步 (21:30, 任务73 前移)
    yesterday_synced: bool, // 昨日日线是否已完成 T+1 同步 (次日9:00)
    cleanup_done: bool,
    report_pushed: bool, // 今日是否已推送实盘绩效日报 (16:00 EOD 后)
}

/// ⚠️ cron crate 数字 DOW 是 **1=Sunday** 语义（2026-09-11 实测：`1-5` 的触发日为
/// 周日~周四，周五漏跑），与标准 Vixie cron（1=Monday）不同。
/// 周字段一律用名字（MON-FRI/SAT/SUN），禁止数字区间——scheduled_task_config
/// 已全量迁移 MON-FRI，新增任务必须遵守。
fn normalize_cron_expr(expr: &str) -> String {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() == 5 {
        format!("0 {}", parts.join(" "))
    } else {
        parts.join(" ")
    }
}

fn scheduled_task_time_minutes(expr: &str) -> Result<u32, String> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    let (minute_idx, hour_idx) = match parts.len() {
        5 => (0, 1),
        6 | 7 => (1, 2),
        _ => return Err(format!("CRON 表达式 '{}' 字段数不是 5/6/7", expr)),
    };
    let minute = parts[minute_idx]
        .parse::<u32>()
        .map_err(|_| format!("CRON 表达式 '{}' 分字段不是固定数字", expr))?;
    let hour = parts[hour_idx]
        .parse::<u32>()
        .map_err(|_| format!("CRON 表达式 '{}' 时字段不是固定数字", expr))?;
    if minute > 59 || hour > 23 {
        return Err(format!("CRON 表达式 '{}' 时/分超出范围", expr));
    }
    Ok(hour * 60 + minute)
}

fn is_eod_sync_window(hour: u32, minute: u32) -> bool {
    // 21:30 窗口（2026-09-22 任务73 前移，原 22:00）：Tushare fund_daily 当日就绪率
    // 不稳定(2026-08 实测约 5/7 交易日 20:00 前就绪,8/14/8/19 延迟到次日;8/19 当晚
    // 22:00 仍 0 rows)。前移动机：全链收尾提前 30 分钟,为周五 23:20 rolling IC 与
    // 0 点关机窗口留余量(工作时间定版:00:00-08:30/16:30-21:00/非交易日全天为非工作段)。
    // fund_daily 21:30 就绪率较 22:00 略降的残留,由 akshare(东财)兜底与 9:00 T+1
    // 补盯市闭环承接,告警观察一周后复评。
    hour == 21 && (30..40).contains(&minute)
}

fn pre_trade_factor_combo(sc: &StrategyConfig) -> &str {
    let combo = sc.combo_name.trim();
    if combo.is_empty() {
        "full_pit_icir_37f"
    } else {
        combo
    }
}
fn market_level_freshness_dataset(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("moneyflow_hsgt"),
        _ => None,
    }
}

fn market_level_freshness_task_slug(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("hsgt"),
        _ => None,
    }
}

fn market_level_freshness_sync_payload(
    source: &str,
    latest_trade_date: Option<NaiveDate>,
    today: NaiveDate,
) -> Option<serde_json::Value> {
    let dataset = market_level_freshness_dataset(source)?;
    let task_slug = market_level_freshness_task_slug(source)?;
    let start_date = latest_trade_date
        .map(|date| (date + chrono::Duration::days(1)).min(today))
        .unwrap_or(today);
    Some(serde_json::json!({
        "dataset": dataset,
        "source": "tushare",
        "start_date": start_date.format("%Y%m%d").to_string(),
        "end_date": today.format("%Y%m%d").to_string(),
        "data_version_id": format!("dv-p315-{}-{}", task_slug, today.format("%Y%m%d")),
        "background": true,
        "reason": "scheduled_p315_market_level_regime_source_freshness"
    }))
}

fn market_level_freshness_sources(params: &serde_json::Value) -> Vec<String> {
    params
        .get("sources")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| {
            vec![
                "market_margin_regime".to_string(),
                "market_moneyflow_hsgt_regime".to_string(),
            ]
        })
}

async fn latest_market_level_trade_date(db: &PgPool, source: &str) -> Option<NaiveDate> {
    let sql = match source {
        "market_margin_regime" => "SELECT MAX(trade_date) FROM market_margin",
        "market_moneyflow_hsgt_regime" => "SELECT MAX(trade_date) FROM market_moneyflow_hsgt",
        _ => return None,
    };
    sqlx::query_scalar::<_, Option<NaiveDate>>(sql)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用 StrategyConfig 字面量(显式构造,避免触发 panic 版 Default)。
    fn test_strategy_config() -> StrategyConfig {
        StrategyConfig {
            etf_premium_gate: 0.10,
            allocation_mode: None,
            mu_estimation: None,
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
            slippage_pct: 0.002,
            mvo_objective: "minvariance".into(),
            kelly_fraction: 0.25,
            score_candidate_pool_size: 200,
            regime_policy: None,
            regime_bear_return_threshold: -0.03,
        }
    }

    #[test]
    fn normalize_cron_expr_accepts_existing_five_field_task_crons() {
        assert_eq!(normalize_cron_expr("0 9 * * 1-5"), "0 0 9 * * 1-5");
        assert_eq!(normalize_cron_expr("30 16 * * 1-5"), "0 30 16 * * 1-5");
        assert_eq!(normalize_cron_expr("0 30 16 * * 1-5"), "0 30 16 * * 1-5");
    }

    #[test]
    fn scheduled_task_time_supports_five_and_six_field_crons() {
        assert_eq!(
            scheduled_task_time_minutes("30 16 * * 1-5").expect("five field cron"),
            16 * 60 + 30
        );
        assert_eq!(
            scheduled_task_time_minutes("0 30 10 * * 1-5").expect("six field cron"),
            10 * 60 + 30
        );
        assert!(scheduled_task_time_minutes("not a cron").is_err());
    }

    #[test]
    fn market_level_freshness_payload_uses_latest_plus_one_range() {
        let payload = market_level_freshness_sync_payload(
            "market_margin_regime",
            NaiveDate::from_ymd_opt(2026, 6, 18),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        )
        .expect("margin payload");

        assert_eq!(payload["dataset"], "margin");
        assert_eq!(payload["source"], "tushare");
        assert_eq!(payload["start_date"], "20260619");
        assert_eq!(payload["end_date"], "20260620");
        assert_eq!(payload["background"], true);
        assert_eq!(payload["data_version_id"], "dv-p315-margin-20260620");
    }

    #[test]
    fn market_level_freshness_payload_supports_hsgt_and_rejects_static_industry() {
        let payload = market_level_freshness_sync_payload(
            "market_moneyflow_hsgt_regime",
            NaiveDate::from_ymd_opt(2026, 5, 29),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        )
        .expect("hsgt payload");

        assert_eq!(payload["dataset"], "moneyflow_hsgt");
        assert_eq!(payload["start_date"], "20260530");
        assert_eq!(
            market_level_freshness_sync_payload(
                "industry_prosperity_proxy",
                None,
                NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
            ),
            None
        );
    }

    #[test]
    fn eod_sync_window_does_not_replay_after_startup_late_in_day() {
        // EOD 于 21:30（任务73 前移，原 22:00）：fund_daily 晚到由 akshare 兜底 + 9:00 T+1 补
        assert!(is_eod_sync_window(21, 30));
        assert!(is_eod_sync_window(21, 39));
        assert!(!is_eod_sync_window(21, 40));
        assert!(!is_eod_sync_window(21, 29));
        assert!(!is_eod_sync_window(16, 0));
        assert!(!is_eod_sync_window(20, 0)); // 旧 20:00 窗口已废弃，防回归
        assert!(!is_eod_sync_window(22, 0)); // 旧 22:00 窗口已废弃（任务73 前移），防回归
        assert!(!is_eod_sync_window(23, 0));
    }

    #[test]
    fn pre_trade_factor_combo_uses_active_strategy_combo_not_price_volume_fallback() {
        // StrategyConfig::default() 已改 panic(配置必须从 DB 加载),测试手工构造。
        let strategy = StrategyConfig {
            combo_name: "full_pit_icir_37f".to_string(),
            ..test_strategy_config()
        };

        assert_eq!(pre_trade_factor_combo(&strategy), "full_pit_icir_37f");
    }

    #[test]
    fn pre_trade_factor_combo_empty_falls_back_to_canonical() {
        let strategy = StrategyConfig {
            combo_name: "  ".to_string(),
            ..test_strategy_config()
        };
        assert_eq!(pre_trade_factor_combo(&strategy), "full_pit_icir_37f");
    }

    #[test]
    fn scheduled_task_time_minutes_rejects_invalid_shapes_and_ranges() {
        // 字段数 4/8 均拒绝
        assert!(scheduled_task_time_minutes("30 16 * *").is_err());
        assert!(scheduled_task_time_minutes("0 30 16 * * 1-5 2026 extra").is_err());
        // 分/时非数字
        assert!(scheduled_task_time_minutes("x 9 * * 1-5").is_err());
        assert!(scheduled_task_time_minutes("30 y * * 1-5").is_err());
        // 超范围
        assert!(scheduled_task_time_minutes("60 9 * * 1-5").is_err());
        assert!(scheduled_task_time_minutes("0 24 * * 1-5").is_err());
        // 7 字段（含年份）可解析
        assert_eq!(
            scheduled_task_time_minutes("0 30 16 * * 1-5 2026").unwrap(),
            16 * 60 + 30
        );
    }

    #[test]
    fn normalize_cron_expr_passes_through_non_five_field_expressions() {
        // 非 5 字段表达式原样返回（仅空白规范化）
        assert_eq!(normalize_cron_expr("30 16 * *"), "30 16 * *");
        assert_eq!(normalize_cron_expr("0  30   16 * * 1-5"), "0 30 16 * * 1-5");
    }

    #[test]
    fn market_level_freshness_dataset_and_task_slug_mappings() {
        assert_eq!(
            market_level_freshness_dataset("market_margin_regime"),
            Some("margin")
        );
        assert_eq!(
            market_level_freshness_dataset("market_moneyflow_hsgt_regime"),
            Some("moneyflow_hsgt")
        );
        assert_eq!(market_level_freshness_dataset("industry_prosperity"), None);

        assert_eq!(
            market_level_freshness_task_slug("market_margin_regime"),
            Some("margin")
        );
        assert_eq!(
            market_level_freshness_task_slug("market_moneyflow_hsgt_regime"),
            Some("hsgt")
        );
        assert_eq!(market_level_freshness_task_slug("unknown_source"), None);
    }

    #[test]
    fn market_level_freshness_sources_parses_filters_and_defaults() {
        // 无 sources → 默认两源
        let defaults = market_level_freshness_sources(&serde_json::json!({}));
        assert_eq!(
            defaults,
            vec![
                "market_margin_regime".to_string(),
                "market_moneyflow_hsgt_regime".to_string(),
            ]
        );
        // 空数组 → 同样回退默认
        assert_eq!(
            market_level_freshness_sources(&serde_json::json!({"sources": []})),
            defaults
        );
        // 非字符串/空串/trim 混合过滤
        let parsed = market_level_freshness_sources(&serde_json::json!({
            "sources": ["  market_margin_regime ", "", 42, "market_moneyflow_hsgt_regime"]
        }));
        assert_eq!(
            parsed,
            vec![
                "market_margin_regime".to_string(),
                "market_moneyflow_hsgt_regime".to_string(),
            ]
        );
    }

    #[test]
    fn market_level_freshness_payload_without_latest_starts_from_today() {
        let payload = market_level_freshness_sync_payload(
            "market_margin_regime",
            None,
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        )
        .expect("payload");
        assert_eq!(payload["start_date"], "20260620");
        assert_eq!(payload["end_date"], "20260620");
    }

    // ── 连库只读（Application 层覆盖率专项 2026-09-20）──
    // 仅覆盖纯查询分支；run_tick/run_scheduled_tasks/validate_pre_trade_data/
    // sync_limit_with_retry 等会触发真实同步或调度循环，不直调。

    async fn test_db() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        PgPool::connect(&url).await.expect("test db connect")
    }

    #[tokio::test]
    async fn is_trading_day_distinguishes_open_and_closed_dates() {
        let db = test_db().await;
        let d = |m: u32, dd: u32| NaiveDate::from_ymd_opt(2026, m, dd).unwrap();
        assert_eq!(is_trading_day(&db, d(9, 18)).await, Ok(true), "周五开市");
        assert_eq!(is_trading_day(&db, d(9, 19)).await, Ok(false), "周六休市");
        assert_eq!(is_trading_day(&db, d(9, 20)).await, Ok(false), "周日休市");
    }

    #[tokio::test]
    async fn check_data_freshness_classifies_fresh_stale_and_missing() {
        let db = test_db().await;
        let d = |m: u32, dd: u32| NaiveDate::from_ymd_opt(2026, m, dd).unwrap();
        // 518880 数据到 2026-09-18；09-19（周六）视角 gap=0 → 新鲜（历史 gap 不随时间变）
        assert_eq!(
            check_data_freshness(&db, "518880.SH", d(9, 19), 1).await,
            None,
            "数据齐备应为 None"
        );
        // 「最后行情日 + 14 天」视角 → 必然落后多日（动态构造，数据增长不失效）
        let last_bar: Option<NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = '518880.SH'",
        )
        .fetch_one(&db)
        .await
        .ok()
        .flatten();
        let horizon = last_bar.unwrap_or_else(|| d(9, 18)) + chrono::Duration::days(14);
        let stale = check_data_freshness(&db, "518880.SH", horizon, 1)
            .await
            .expect("应报告落后");
        assert!(stale > 1, "落后交易日数应 > 1: {stale}");
        // 完全无数据的 symbol → 999 哨兵
        assert_eq!(
            check_data_freshness(&db, "zzz_test_no_such_symbol", d(9, 19), 1).await,
            Some(999)
        );
    }

    #[tokio::test]
    async fn check_factor_freshness_fresh_combo_and_unknown_combo() {
        let db = test_db().await;
        let d = |m: u32, dd: u32| NaiveDate::from_ymd_opt(2026, m, dd).unwrap();
        // v24 active combo 数据已覆盖到最近交易日 → 新鲜
        assert_eq!(
            check_factor_freshness(&db, "full_pit_icir_indneutral_val_v1", d(9, 19), 5).await,
            None
        );
        // 未知 combo → 999 哨兵
        assert_eq!(
            check_factor_freshness(&db, "zzz_test_no_such_combo", d(9, 19), 5).await,
            Some(999)
        );
    }

    #[tokio::test]
    async fn check_task_dependency_order_finds_no_missing_required_tasks() {
        let db = test_db().await;
        let issues = check_task_dependency_order(&db).await;
        assert!(
            !issues.iter().any(|i| i.contains("缺少必要定时任务")),
            "生产配置三件套应齐全: {issues:?}"
        );
        // 当前 factor_backfill_daily 09:05 不早于 T+1 完成（9:10 判定阈值 9:00）
        assert!(
            !issues
                .iter()
                .any(|i| i.contains("factor_backfill_daily") && i.contains("早于")),
            "factor_backfill 排序应合规: {issues:?}"
        );
    }

    #[tokio::test]
    async fn get_latest_data_version_returns_nonempty_version() {
        let db = test_db().await;
        let version = get_latest_data_version(&db).await;
        assert!(!version.is_empty(), "EOD 数据版本不应为空");
    }

    #[tokio::test]
    async fn latest_market_level_trade_date_reads_known_sources_only() {
        let db = test_db().await;
        let margin = latest_market_level_trade_date(&db, "market_margin_regime")
            .await
            .expect("margin 表有数据");
        assert!(
            margin >= NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            "margin 最新交易日过旧: {margin:?}"
        );
        let hsgt = latest_market_level_trade_date(&db, "market_moneyflow_hsgt_regime").await;
        assert!(hsgt.is_some(), "hsgt 表应有数据");
        assert_eq!(
            latest_market_level_trade_date(&db, "zzz_test_unknown_source").await,
            None
        );
    }

    #[tokio::test]
    async fn check_prediction_available_uses_latest_ready_predictions() {
        let db = test_db().await;
        // 动态取 ready 预测集覆盖的最新交易日（数据只增不减）
        let latest: Option<NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(mp.trade_date) FROM model_prediction mp \
             JOIN prediction_set ps ON ps.prediction_set_id = mp.prediction_set_id \
               AND ps.status = 'ready'",
        )
        .fetch_one(&db)
        .await
        .ok()
        .flatten();
        let latest = latest.unwrap_or_else(|| NaiveDate::from_ymd_opt(2026, 6, 15).unwrap());
        assert!(
            check_prediction_available(&db, latest).await,
            "ready 集最新交易日应有预测行 ({latest})"
        );
        // 远未来（超出任何 ML 预测视野）无预测
        assert!(
            !check_prediction_available(&db, NaiveDate::from_ymd_opt(2030, 1, 1).unwrap()).await
        );
    }

    #[tokio::test]
    async fn get_current_wfa_params_resolves_within_window_and_defaults_outside() {
        let db = test_db().await;
        // wfa_strategy_params 覆盖 2019-01-31 ~ 2026-01-28，窗口内命中最高分参数
        let inside = get_current_wfa_params(&db, NaiveDate::from_ymd_opt(2025, 6, 30).unwrap())
            .await
            .expect("窗口内查询 ok");
        assert!(!inside.is_null(), "窗口内应返回参数: {inside}");
        // 窗口外 → Value::default() 即 Null
        let outside = get_current_wfa_params(&db, NaiveDate::from_ymd_opt(2018, 1, 1).unwrap())
            .await
            .expect("窗口外查询 ok");
        assert!(outside.is_null(), "窗口外应回退 Null: {outside}");
    }
}
/// 不再依赖单一策略（v19）的 etf_symbols，确保多策略并行时所有策略 ETF 都被同步。
// R6: 策略配置查询函数已迁到 strategy_query.rs，此处 re-export 转发保持调用方零改动。
pub(crate) use crate::routes::strategy_query::{
    combo_horizon_from_name, load_active_combo_materialize_configs, load_active_etf_symbols_union,
    load_active_factor_combos, load_first_active_strategy_config, load_strategy_config,
};

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_data_quality_check(db: &PgPool) {
    // 手动触发口径: 调用者意图是"现在检查", 因子基准取 T 日(最严格)。
    // 若在回填前时段手动触发而想看回填前状态, 误报可由告警文案"基准T日"自明。
    crate::routes::data_quality::run_data_quality_check(
        db,
        crate::routes::data_quality::FactorFreshnessBaseline::LatestTradeDate,
    )
    .await;
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_equity_curve_update(db: &PgPool) {
    // 自动同步所有活跃账号关联策略的权益曲线(combo 去重)。
    // 消除硬编码 v19:v21/v21_lev/v23 等策略都会被同步。
    // 异步 spawn 不阻塞 scheduler tick(sleeve 回测全量重跑 ~75s/个,串行会卡 run_tick)。
    // 每日工作日 17:00 触发(原月度,v23 月中创建后 sleeve 滞后到下月才同步→门禁拦截)。
    let db_clone = db.clone();
    tokio::spawn(async move {
        let results =
            crate::routes::equity_curve_sync::sync_active_strategies_equity_curves(&db_clone).await;
        for r in &results {
            if r.status == "success" {
                info!(
                    "[scheduler] 权益曲线同步成功: {} task_id={:?} 更新策略 {:?}",
                    r.strategy_id, r.task_id, r.updated_strategy_ids
                );
            } else {
                warn!(
                    "[scheduler] 权益曲线同步失败: {} err={:?}",
                    r.strategy_id, r.error
                );
            }
        }
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_mvo_equity_curve_update(db: &PgPool, params: &serde_json::Value) {
    // 每日 21:00(EOD 数据全就绪后)重跑 MVO 动态后复权无成本基准曲线。
    // 策略参数不变则历史段幂等覆盖、追加最新交易日。全周期重跑 ~分钟级,spawn 不阻塞 tick。
    let strategy_id = params
        .get("strategy_id")
        .and_then(|v| v.as_str())
        .unwrap_or("v24")
        .to_string();
    let benchmark_account_id = params
        .get("benchmark_account_id")
        .and_then(|v| v.as_str())
        .unwrap_or("pa-v24-mvo-bench")
        .to_string();
    let start_date = params
        .get("start_date")
        .and_then(|v| v.as_str())
        .unwrap_or("20160104")
        .to_string();
    let end_date = chrono::Local::now()
        .date_naive()
        .format("%Y%m%d")
        .to_string();
    let leverage_multiplier = params
        .get("leverage_multiplier")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let db_clone = db.clone();
    tokio::spawn(async move {
        let req = crate::routes::historical_replay::MvoBenchmarkRequest {
            strategy_id,
            benchmark_account_id,
            start_date,
            end_date,
            leverage_multiplier,
        };
        match crate::routes::historical_replay::run_mvo_benchmark_sync(&db_clone, req).await {
            Ok(d) => info!("[scheduler] MVO 基准曲线同步成功: {}", d),
            Err(e) => warn!("[scheduler] MVO 基准曲线同步失败: {}", e),
        }
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_factor_backfill(db: &PgPool) {
    // v24 因子全量回填:覆盖所有 active 策略依赖的全部因子类别
    // （量价/财务/资金流/分析师/回购/大宗/流动性/市场风险）。
    // 幂等刷新 BACKFILL_WINDOW_DAYS 窗口，保证盘中调仓依赖的 factor_value/multi_factor_value 新鲜。
    // 可被 check_task_dependency_order 检查、可手动触发、可配 CRON。
    let today = chrono::Local::now().date_naive();
    let last_trade_date: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT trade_date FROM market_trade_calendar
                 WHERE is_open = true AND trade_date <= $1
                 ORDER BY trade_date DESC LIMIT 1",
    )
    .bind(today)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    let Some((sync_date,)) = last_trade_date else {
        warn!("[scheduler] factor_backfill: 无交易日历数据,跳过");
        return;
    };
    let sync_date_str = sync_date.format("%Y%m%d").to_string();
    let backfill_start = (sync_date - BACKFILL_WINDOW_DAYS)
        .format("%Y%m%d")
        .to_string();
    trigger_v24_backfill_routes(db, &backfill_start, &sync_date_str).await;
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_nightly_signal_prep(db: &PgPool, tushare: &TushareClient) {
    // 夜间信号预备链(2026-09-11; 任务73 2026-09-22 前移 21:45,原 22:10):
    // 独立触发不等 EOD 主链(主链被 forecast 拖到 00:00 的历史教训)。
    // bar 前置: EOD 主链 21:30 起跑,日线步骤 21:31 入库即满足。
    // 链: phase7 因子回补(正常 ~25m) → PIT 物化增量 → sleeve 曲线更新
    //      → fund_nav 净值增量(2026-09-17, ETF 溢价门禁数据源)
    //      → 收尾质量检查(T日基准, 任务71 2026-09-21)。
    // 预计 22:55 完成,为 23:00 信号生成(任务73 倒挂修复:原 23:30 导出晚于
    // 预备链最坏 23:45)与日终通知留窗。幂等,失败告警。
    // 任务71 依赖编排修正: 回补由 fire-and-forget 改为等待终态后再物化
    // (原实现在回填完成前物化, T 日 combo 行由 T-1 因子合成, 信号截面
    // 静默滞后一交易日——multi_factor_value created_at 实锤)。
    let db2 = db.clone();
    let tushare2 = tushare.clone();
    let date = chrono::Local::now().date_naive();
    tokio::spawn(async move {
        let t0 = std::time::Instant::now();
        let sd = (date - chrono::Duration::days(190))
            .format("%Y%m%d")
            .to_string();
        let ed = date.format("%Y%m%d").to_string();
        let trigger_started = chrono::Utc::now();
        trigger_v24_backfill_routes(&db2, &sd, &ed).await;
        // 受理→任务行落库毫秒级, 但防末条路由尚未落库时 COUNT=0 提前返回
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        // 超时上限 60m(21:45+60m=22:45, 任务73 由 95m 收紧): 正常 25m 内
        // 完成; 超时继续走物化, 信号侧物化新鲜度门禁拒发残缺截面(宁缺毋假),
        // 且为 23:00 信号导出留 15m 缓冲(倒挂修复的一部分)。
        wait_for_factor_backfill(
            &db2,
            trigger_started,
            std::time::Duration::from_secs(60 * 60),
        )
        .await;
        info!(
            "[夜间预备] phase7 因子回补完成 累计{}s",
            t0.elapsed().as_secs()
        );
        let wl: Option<Vec<String>> = sqlx::query_scalar(
                    "SELECT factor_whitelist FROM strategy_config WHERE strategy_id='v24' AND status='active'",
                )
                .fetch_optional(&db2)
                .await
                .ok()
                .flatten()
                .and_then(|v: serde_json::Value| v.as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()));
        let ind_neut: bool = sqlx::query_scalar(
                    "SELECT ind_neutral FROM combo_materialization_log WHERE combo_name='full_pit_icir_indneutral_val_v1' ORDER BY materialized_at DESC LIMIT 1",
                )
                .fetch_optional(&db2)
                .await
                .ok()
                .flatten()
                .unwrap_or(false);
        match crate::routes::factors::materialize_pit_combo_ext(
            &db2,
            &crate::routes::factors::PitComboMaterializeParams {
                combo_name: "full_pit_icir_indneutral_val_v1",
                factor_version: "1.0.0",
                horizon: 20,
                start_date: date - chrono::Duration::days(120),
                end_date: date,
                include_fundamentals: true,
                min_abs_ic_ir: None,
                factor_whitelist: wl.as_deref(),
                ind_neutral: ind_neut,
            },
        )
        .await
        {
            Ok(rows) => info!(
                "[夜间预备] PIT 物化 {} 行 累计{}s",
                rows,
                t0.elapsed().as_secs()
            ),
            Err(e) => {
                error!("[夜间预备] PIT 物化失败(次日9:30档兜底): {}", e);
            }
        }
        // 任务77(2026-09-23): nightly 链物化此前只覆盖 indneutral_val_v1,
        // 生产信号 combo(37f_h20_fund_v2 等)仅靠早间 09:30 档保鲜——
        // 新鲜度门禁首夜即拦截(早间截面由 T-1 因子合成, 行写入早于夜间
        // 回填完成, 且早间截面行数不完整 3893/5573)。此处追加遍历其余
        // active full_pit_icir* combo 的夜间物化(与 pit_combo_refresh
        // 同口径, 120d 增量窗口), 早间 09:30 档降级为兜底。
        let pit_combos: Vec<_> = load_active_combo_materialize_configs(&db2)
            .await
            .into_iter()
            .filter(|c| {
                c.combo_name.starts_with("full_pit_icir")
                    && c.combo_name != "full_pit_icir_indneutral_val_v1" // 前段已专门物化(ind_neutral 自举参数特殊)
            })
            .collect();
        for cfg in &pit_combos {
            // 显式 combo_horizon 列优先(2026-09-05: 名字推断曾把无 _h{N}
            // 后缀的 combo 判错 horizon), 与 pit_combo_refresh 同规则。
            let horizon = cfg
                .combo_horizon
                .unwrap_or_else(|| combo_horizon_from_name(&cfg.combo_name));
            match crate::routes::factors::materialize_pit_combo_ext(
                &db2,
                &crate::routes::factors::PitComboMaterializeParams {
                    combo_name: &cfg.combo_name,
                    factor_version: "1.0.0",
                    horizon,
                    start_date: date - chrono::Duration::days(120),
                    end_date: date,
                    include_fundamentals: cfg.include_fundamentals,
                    min_abs_ic_ir: None,
                    factor_whitelist: cfg.factor_whitelist.as_deref(),
                    // scheduler 夜间物化不做行业中性化(与保鲜档同规则)
                    ind_neutral: false,
                },
            )
            .await
            {
                Ok(rows) => info!(
                    "[夜间预备] PIT combo {} 夜间物化 {} 行 累计{}s",
                    cfg.combo_name,
                    rows,
                    t0.elapsed().as_secs()
                ),
                Err(e) => warn!(
                    "[夜间预备] PIT combo {} 夜间物化失败(次日9:30档兜底): {}",
                    cfg.combo_name, e
                ),
            }
        }
        // 2026-09-18: 原写法 `results => for r in results` 实为迭代
        // Result<SyncResult,String> —— Err 分支零次迭代被静默吞掉,
        // 曲线同步异常时只打"更新完成"。改为显式三分支。
        match crate::routes::equity_curve_sync::sync_strategy_equity_curve(
            &db2,
            "v24",
            date - chrono::Duration::days(10),
            date,
            false,
        )
        .await
        {
            Ok(r) if r.status != "success" => {
                warn!(
                    "[夜间预备] 曲线同步未成功 {} {}: {:?}",
                    r.strategy_id, r.status, r.error
                );
            }
            Ok(_) => {}
            Err(e) => warn!("[夜间预备] 曲线同步异常: {}", e),
        }
        info!(
            "[夜间预备] sleeve 曲线更新完成 累计{}s",
            t0.elapsed().as_secs()
        );
        // 基金净值增量(2026-09-17, ETF 溢价门禁数据源): 每标的增量秒级完成。
        // 门禁对缺数据降级放行, 此处失败不阻断 23:30 信号(告警留痕)。
        let etf_syms = load_active_etf_symbols_union(&db2).await;
        match quant_data::sync::sync_fund_nav(&db2, &tushare2, &etf_syms).await {
            Ok(n) => info!(
                "[夜间预备] fund_nav 净值同步 {} 行 累计{}s",
                n,
                t0.elapsed().as_secs()
            ),
            Err(e) => warn!("[夜间预备] fund_nav 净值同步失败(门禁将降级放行): {}", e),
        }
        match quant_data::sync::sync_fund_div(&db2, &tushare2, &etf_syms).await {
            Ok(n) => info!(
                "[夜间预备] fund_div 分红同步 {} 条 累计{}s",
                n,
                t0.elapsed().as_secs()
            ),
            Err(e) => warn!("[夜间预备] fund_div 分红同步失败(ETF分红不入账): {}", e),
        }
        // 收尾哨兵(任务71): 因子回填+物化后的全量质量检查, T日基准——
        // 22:01 EOD 尾那次是 T-1 基准(回填前时点), 真正的"信号前哨兵"
        // 在这里: 真滞缓在此报出, 23:30 信号前留人工处置窗。
        crate::routes::data_quality::run_data_quality_check(
            &db2,
            crate::routes::data_quality::FactorFreshnessBaseline::LatestTradeDate,
        )
        .await;
        info!(
            "[夜间预备] 链收尾质量检查完成(T日基准) 累计{}s",
            t0.elapsed().as_secs()
        );
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_ptrade_signal_export(
    db: &PgPool,
    tushare: &TushareClient,
    params: &serde_json::Value,
) {
    // PTrade 实盘信号生成(2026-09-11, B1' 文件桥): 23:30 触发。
    // 门禁→run-factor 截面→目标权重(与本文件模拟盘同源组件)→JSON→scp 推送。
    // 依赖夜间预备链(22:10)的因子/物化/曲线在 23:20 前就绪。
    // 详见 signal_export.rs 与 docs/projects/quant/PTrade实盘对接设计方案.md §5。
    let db2 = db.clone();
    let tushare2 = tushare.clone();
    let params2 = params.clone();
    tokio::spawn(async move {
        // 溢价门禁净值兜底(2026-09-17): 22:10 链失败/EOD 拖延时补拉,
        // 增量幂等秒级; 再失败则门禁降级放行(signal_export 内置)。
        let etf_syms = load_active_etf_symbols_union(&db2).await;
        if let Err(e) = quant_data::sync::sync_fund_nav(&db2, &tushare2, &etf_syms).await {
            warn!("[PTrade信号] fund_nav 兜底同步失败(门禁降级放行): {}", e);
        }
        if let Err(e) = quant_data::sync::sync_fund_div(&db2, &tushare2, &etf_syms).await {
            warn!("[PTrade信号] fund_div 兜底同步失败: {}", e);
        }
        crate::routes::signal_export::run_ptrade_signal_export(&db2, &params2).await;
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_ptrade_report_fetch(db: &PgPool) {
    // PTrade 实盘回报抓取(2026-09-11, B1' 回报回流链): 16:30 触发。
    // IMAP 拉当日 exec/heartbeat 邮件 → ptrade_execution_report 入库 → 钉钉日报。
    // 心跳缺失告警(区分"无交易"与"策略挂了/通道故障")。见 ptrade_report.rs。
    let db2 = db.clone();
    tokio::spawn(async move {
        crate::routes::ptrade_report::run_ptrade_report_fetch(&db2).await;
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_native_pv_increment(db: &PgPool) {
    // quant-factor 原生量价 7 因子夜间增量(2026-09-16 治本): 23:00 触发。
    // mom_5d/vol_20d/turn_20d/mom_20d/rsi_14d/amp_5d/bb_pos_20d 不在 phase7
    // 回填体系内, 历史 5-12/7-15/9-05 三次断供全靠手动测试补数。此任务分批
    // 增量计算(口径与 pv_std_backfill_2605 一致), 见 factors/native_pv.rs。
    let db2 = db.clone();
    tokio::spawn(async move {
        crate::routes::factors::run_native_pv_increment(&db2).await;
    });
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_pit_combo_refresh(db: &PgPool, params: &serde_json::Value) {
    // PIT 滚动 ICIR combo 数据保鲜：增量物化最近季度（幂等）。
    // 防止随交易日推移 combo 分数过时。依赖：因子已重算 + 滚动 IC 已评估。
    // 遍历所有 active 策略声明的 PIT combo（含 h1/h20），每个用 combo_name 推断的 horizon。
    // 模拟实盘盘中调仓依赖：所有激活账号策略用到的 combo 都需每日刷新到最新交易日。
    // phase7_price_volume_expanded_v1 等 non-ICIR combo 不走此路径（由 phase7 backfill 路由处理）。
    let ver = params
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("1.0.0");
    // 增量区间：默认最近一年（覆盖当前+上季度，幂等刷新）
    let refresh_start = chrono::Utc::now().date_naive() - chrono::Duration::days(370);
    let refresh_end = chrono::Utc::now().date_naive();
    let pit_combos: Vec<_> = load_active_combo_materialize_configs(db)
        .await
        .into_iter()
        .filter(|c| c.combo_name.starts_with("full_pit_icir"))
        .collect();
    if pit_combos.is_empty() {
        warn!("[scheduler] PIT combo 保鲜：无 active full_pit_icir* combo，跳过");
    }
    for cfg in &pit_combos {
        // 显式 combo_horizon 列优先（2026-09-05：indneutral_val_v1 名字无
        // _h{N} 后缀曾被推断为 1，而实际物化口径是 20——三段拼接根源）
        let horizon = cfg
            .combo_horizon
            .unwrap_or_else(|| combo_horizon_from_name(&cfg.combo_name));
        info!(
                    "[scheduler] PIT combo 保鲜: combo={} horizon={} include_fund={} whitelist={} 区间 {}~{}",
                    cfg.combo_name, horizon, cfg.include_fundamentals,
                    cfg.factor_whitelist.as_ref().map(|w| w.len()).unwrap_or(0), refresh_start, refresh_end
                );
        // 含基本面因子的 combo(如 v24 fund_v2)用 ext + include_fundamentals + factor_whitelist,
        // 否则用默认黑名单物化会丢失 fin_/mf_/north_ 因子。
        match crate::routes::factors::materialize_pit_combo_ext(
            db,
            &crate::routes::factors::PitComboMaterializeParams {
                combo_name: &cfg.combo_name,
                factor_version: ver,
                horizon,
                start_date: refresh_start,
                end_date: refresh_end,
                include_fundamentals: cfg.include_fundamentals,
                // min_abs_ic_ir: 保鲜不加阈值(白名单已筛)
                min_abs_ic_ir: None,
                factor_whitelist: cfg.factor_whitelist.as_deref(),
                // ind_neutral: scheduler 保鲜不做行业中性化(仅手动物化新 combo 时启用)
                ind_neutral: false,
            },
        )
        .await
        {
            Ok(rows) => info!(
                "[scheduler] PIT combo {} 保鲜完成: {} 行",
                cfg.combo_name, rows
            ),
            Err(e) => warn!("[scheduler] PIT combo {} 保鲜失败: {}", cfg.combo_name, e),
        }
    }
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_rolling_pit_eval(params: &serde_json::Value) {
    // rolling PIT IC 评估调度化（2026-09-21，rolling IC 专项发现 A 修复）：
    // factor_evaluation 此前无任何定时调用者，rolling 权重曾冻结 2.5 个月
    // （2026-06-30 后停更，与 sync_fund_adj 缺调度同构——能力存在但无人调用）。
    // IC 评估是 PIT combo 物化前置（ICIR 权重按 as-of 前最新评估），停更 =
    // combo 权重对新市场结构停止适应。建议 cron 排非交易日（周六晨）。
    // 走 background 路由异步执行不阻塞调度循环；horizon 20/60 各提交一遍
    // （factor_evaluation 现存这两个 horizon 的评估序列）。
    let api_base = self_api_base();
    let client = reqwest::Client::new();
    let end = chrono::Utc::now().date_naive();
    let lookback = params
        .get("lookback_days")
        .and_then(|v| v.as_i64())
        .unwrap_or(370);
    let start = end - chrono::Duration::days(lookback);
    for horizon in [20i16, 60i16] {
        let payload = serde_json::json!({
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
            "horizon": horizon,
        });
        match client
            .post(format!(
                "{}/api/v1/quant/factors/evaluate-rolling-pit/background",
                api_base
            ))
            .json(&payload)
            .timeout(std::time::Duration::from_secs(60))
            .send()
            .await
        {
            Ok(resp) => {
                info!(
                    "[scheduler] rolling PIT IC 评估任务已提交(horizon={} 区间 {}~{}): {}",
                    horizon,
                    start,
                    end,
                    resp.status()
                )
            }
            Err(e) => warn!(
                "[scheduler] rolling PIT IC 评估任务提交失败(horizon={}): {}",
                horizon, e
            ),
        }
    }
}

/// 调度分派臂（任务79 从 run_scheduled_tasks 拆出，语义等价搬运）。
pub(crate) async fn dispatch_market_level_source_freshness(
    db: &PgPool,
    params: &serde_json::Value,
) {
    let today = chrono::Utc::now().date_naive();
    let api_base = self_api_base();
    let client = reqwest::Client::new();
    for source in market_level_freshness_sources(params) {
        let latest = latest_market_level_trade_date(db, &source).await;
        let Some(payload) = market_level_freshness_sync_payload(&source, latest, today) else {
            warn!("[scheduler] P3.15 市场级源不支持自动同步: {}", source);
            continue;
        };
        info!(
            "[scheduler] P3.15 市场级源保鲜: source={} latest={:?}",
            source, latest
        );
        match client
            .post(format!("{}/api/v1/quant/data/sync-tasks", api_base))
            .json(&payload)
            .timeout(std::time::Duration::from_secs(600))
            .send()
            .await
        {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(result) => info!(
                    "[scheduler] P3.15 市场级源同步任务返回: source={} result={}",
                    source, result
                ),
                Err(error) => warn!(
                    "[scheduler] P3.15 市场级源同步响应解析失败: source={} error={}",
                    source, error
                ),
            },
            Err(error) => warn!(
                "[scheduler] P3.15 市场级源同步任务提交失败: source={} error={}",
                source, error
            ),
        }
    }
}

async fn run_scheduled_tasks(db: &PgPool, tushare: &TushareClient) {
    let now = chrono::Local::now();
    let tasks: Vec<(String, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT task_name, task_type, schedule_cron, params FROM scheduled_task_config
         WHERE enabled = true AND (next_run_at IS NULL OR next_run_at <= $1)
         ORDER BY next_run_at NULLS FIRST",
    )
    .bind(now)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    for (name, task_type, cron_expr, params) in &tasks {
        info!(
            "[scheduler] 定时任务触发: {} ({}) cron={}",
            name, task_type, cron_expr
        );
        // 未知 task_type 标记（2026-09-21 修复）：兜底臂原为 `_ => {}` 静默空转，
        // 而末尾状态硬编码 last_status='success'——新注册任务若运行在尚无对应分派
        // 分支的旧二进制上，会"绿着什么都不做"。实锤：rolling_pit_eval_weekly
        // 11:39:02 注册、11:39:44 被 catch-up 执行记 success，但当时镜像无该分派，
        // factor_evaluation 零新增（与 PIT 保鲜静默空转同一类缺陷）。
        let mut unhandled_task_type = false;
        match task_type.as_str() {
            "data_quality_check" => dispatch_data_quality_check(db).await,
            "equity_curve_update" => dispatch_equity_curve_update(db).await,
            "mvo_equity_curve_update" => dispatch_mvo_equity_curve_update(db, params).await,
            "factor_backfill" => dispatch_factor_backfill(db).await,
            "nightly_signal_prep" => dispatch_nightly_signal_prep(db, tushare).await,
            "ptrade_signal_export" => dispatch_ptrade_signal_export(db, tushare, params).await,
            "ptrade_report_fetch" => dispatch_ptrade_report_fetch(db).await,
            "native_pv_increment" => dispatch_native_pv_increment(db).await,
            "pit_combo_refresh" => dispatch_pit_combo_refresh(db, params).await,
            "rolling_pit_eval" => dispatch_rolling_pit_eval(params).await,
            "market_level_source_freshness" => {
                dispatch_market_level_source_freshness(db, params).await
            }
            _ => {
                // 无匹配分派分支：多为「DB 注册了新 task_type，但运行中的二进制还没
                // 这个分支」（新功能注册与部署之间的窗口），也可能是 task_type 拼写错。
                // 必须显式告警并把状态记成 unhandled，不能静默记 success。
                unhandled_task_type = true;
                warn!(
                    "[scheduler] ⚠️ 任务 {} 的 task_type='{}' 无对应分派分支——本次未执行任何动作。\
                     若该类型是新增功能，检查当前镜像是否已部署对应代码；否则检查 task_type 拼写",
                    name, task_type
                );
            }
        }

        // 根据 CRON 表达式计算下次运行时间
        let normalized_cron = normalize_cron_expr(cron_expr);
        let next = match cron::Schedule::from_str(&normalized_cron) {
            Ok(schedule) => schedule.upcoming(chrono::Local).next(),
            Err(e) => {
                warn!(
                    "[scheduler] 任务 {} 的 CRON 表达式 '{}' 规范化为 '{}' 后仍无效: {}，默认 1 天后",
                    name, cron_expr, normalized_cron, e
                );
                None
            }
        };
        let next_at = next.unwrap_or_else(|| now + chrono::Duration::days(1));
        // 状态必须如实反映本次分派结果：无分派分支时记 unhandled，避免「绿着空转」
        // 掩盖问题（历史教训：rolling_pit_eval 注册后 42s 被 catch-up 执行并记
        // success，实际当时镜像没有该分支，什么都没做）。
        let status = if unhandled_task_type {
            "unhandled"
        } else {
            "success"
        };
        let _ = sqlx::query(
            "UPDATE scheduled_task_config SET last_run_at = NOW(), run_count = run_count + 1,
             last_status = $1, next_run_at = $2 WHERE task_name = $3",
        )
        .bind(status)
        .bind(next_at)
        .bind(name)
        .execute(db)
        .await;
    }
}

/// 启动后台调度器。
pub fn start_scheduler(db: PgPool, tushare: TushareClient, port: u16) {
    let tushare = Arc::new(tushare);

    // 任务77(2026-09-23): 定时任务拾取拆独立循环——原主循环 run_tick().await 与
    // run_scheduled_tasks().await 串行, run_tick 窗口分支一阻塞(9-22 实锤 22:11-23:00
    // 阻塞 49 分钟)全部定时任务停摆(native_pv 22:15 档 23:00 才触发、signal_export
    // 23:00 档 23:30 才触发)。解耦后两循环互不拖累; 首查仍在启动时立即执行(重启后
    // 错过的当日任务立即补跑, 原语义保留)。任务体仍按原 match 分支执行, await 型
    // 分支会延迟本循环内的后续拾取(分钟级, 可容忍), spawn 化留观察后深化。
    {
        let db_sched = db.clone();
        let tushare_sched = tushare.clone();
        tokio::spawn(async move {
            run_scheduled_tasks(&db_sched, &tushare_sched).await;
            let mut sched_interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                sched_interval.tick().await;
                run_scheduled_tasks(&db_sched, &tushare_sched).await;
            }
        });
    }

    tokio::spawn(async move {
        let state = Arc::new(Mutex::new(DailyState {
            date: None,
            traded_today: false,
            eod_synced_today: false,
            yesterday_synced: false,
            cleanup_done: false,
            report_pushed: false,
        }));
        let mvo_cache: Arc<Mutex<Option<MvoWeightCache>>> = Arc::new(Mutex::new(None));

        // 从数据库加载策略配置（取第一个 active 复合策略，不再硬编码 v19）。
        // 无 active 策略时为 None，run_tick/validate_pre_trade_data 据此跳过策略相关校验。
        let strategy_config: Arc<Option<StrategyConfig>> =
            Arc::new(load_first_active_strategy_config(&db).await);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        match strategy_config.as_ref() {
            Some(sc) => info!(
                "[scheduler] {} 已启动 ({}): 09:35调仓(早间) | 21:30 EOD+夜间预备链 | 9:00 T+1数据补同步",
                sc.strategy_id, sc.name
            ),
            None => info!(
                "[scheduler] 已启动 (无 active 复合策略): 09:35调仓(早间) | 21:30 EOD+夜间预备链 | 9:00 T+1数据补同步"
            ),
        }

        loop {
            interval.tick().await;
            if let Err(e) = run_tick(
                &db,
                &tushare,
                &state,
                &mvo_cache,
                port,
                strategy_config.as_ref().as_ref(),
            )
            .await
            {
                error!("[scheduler] 任务失败: {}", e);
            }
        }
    });
}

/// 获取当前日期对应的最优 WFA 参数（从已完成的实验中提取）
pub(crate) async fn get_current_wfa_params(
    db: &PgPool,
    date: NaiveDate,
) -> Result<serde_json::Value, String> {
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

async fn run_tick(
    db: &PgPool,
    tushare: &TushareClient,
    state: &Arc<Mutex<DailyState>>,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    port: u16,
    sc_opt: Option<&StrategyConfig>,
) -> Result<(), String> {
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
            st.report_pushed = false;
        }
    }

    let is_trade = is_trading_day(db, today).await?;

    // ── 调仓窗口（09:35~10:30 开盘后早间执行, 2026-09-17 门禁调整后主窗口不变）──
    // 与实盘 B1' 信号时序对齐: T-1 夜间预备链产出信号, T 日开盘后执行。
    // 溢价门禁命中标的由执行端延迟窗口(10:35-11:30)处理, 主窗口不因此移动。
    // 绩效验证(回放 open 口径): lev 15.35%/1.002/-23.34% vs 收盘执行 15.14%/0.994,
    // 无折损且杠杆账户 Sharpe 破 1.0。窗口可经 REBALANCE_WINDOW_HOUR 覆盖(默认 9)。
    let reb_hour: u32 = std::env::var("REBALANCE_WINDOW_HOUR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9);
    let reb_min: u32 = 35;
    let in_reb_window = if reb_hour < 12 {
        (hour == reb_hour && minute >= reb_min) || (hour == reb_hour + 1 && minute <= 30)
    } else {
        hour == reb_hour && minute >= reb_min
    };
    if is_trade && in_reb_window {
        let should_trade = {
            let st = state.lock().await;
            !st.traded_today
        };

        if should_trade {
            {
                let mut st = state.lock().await;
                st.traded_today = true;
            }
            // 无 active 策略则跳过调仓（无调仓目标），不阻塞后续 EOD/T+1 同步。
            let sc = match sc_opt {
                Some(s) => s,
                None => {
                    warn!("[scheduler] 14:45 调仓跳过（无 active 复合策略）");
                    return Ok(());
                }
            };
            info!("[scheduler] 14:45 日频调仓 (v16 LW-MVO 7-asset)...");

            // ── 前置数据校验+自动修复 ──
            let data_errors = validate_pre_trade_data(db, tushare, today, sc).await;
            if !data_errors.is_empty() {
                let alert_msg = format!(
                    "⛔ [调仓拒绝] {} 数据异常，已尝试自动修复失败:\n{}",
                    today.format("%Y-%m-%d"),
                    data_errors.join("\n")
                );
                error!("{}", alert_msg);
                send_dingtalk_alert(db, &alert_msg).await;
                // 将 traded_today 重置，下次 tick 可以重试
                let mut st = state.lock().await;
                st.traded_today = false;
                return Ok(());
            }

            // 生成信号 + 调仓 (P2-3:调仓成功/失败均推送钉钉 + 调仓后写快照)
            // 记录调仓前订单数，用于检测是否产生了新交易
            let orders_before: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM paper_order WHERE DATE(created_at) = $1")
                    .bind(today)
                    .fetch_one(db)
                    .await
                    .unwrap_or(0);

            match generate_paper_signals_for_all(db, mvo_cache, port, today, sc, tushare, false)
                .await
            {
                Ok(_) => {
                    // 收集各账号当日交易统计（复用 admin.rs manual_rebalance 的模式）
                    let orders_after: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM paper_order WHERE DATE(created_at) = $1",
                    )
                    .bind(today)
                    .fetch_one(db)
                    .await
                    .unwrap_or(0);
                    let new_orders = orders_after - orders_before;

                    // 快照：调仓完成后立即写(不等 EOD,防止 14:45 到 16:00 之间统计字段悬空)
                    crate::routes::report::snapshot_positions_for_all_accounts(db, today).await;

                    if new_orders == 0 {
                        // 零信号场景：有活跃账号，但无任何订单生成(因子信号为空/数据门禁全跳过)
                        let msg = format!(
                            "## ⚠️ 调仓零信号  \n\n**日期**: {}  \n\
                             **结论**: 调仓链路完整执行但无订单生成  \n\
                             **可能原因**: A股 sleeve 因子信号为空(因子覆盖不足/数据滞后)  \n\
                             或所有活跃账号被数据门禁跳过(查日志 warn 行)  \n\n\
                             > 下次调仓时间: 下一交易日 14:45",
                            today.format("%Y-%m-%d"),
                        );
                        send_dingtalk_alert_titled(db, "调仓零信号", &msg).await;
                    } else {
                        // 成功且有交易：推送成交摘要 + 持仓摘要
                        let mut lines: Vec<String> = Vec::new();
                        let per_account: Vec<(String, i64, i64, f64, f64)> = sqlx::query_as(
                            "SELECT po.paper_account_id,
                                    COUNT(*) FILTER (WHERE po.side='buy')::bigint,
                                    COUNT(*) FILTER (WHERE po.side='sell')::bigint,
                                    COALESCE(SUM(po.target_value) FILTER (WHERE po.side='buy'), 0)::double precision,
                                    COALESCE(SUM(po.target_value) FILTER (WHERE po.side='sell'), 0)::double precision
                             FROM paper_order po
                             JOIN paper_account pa ON pa.paper_account_id = po.paper_account_id
                             WHERE pa.status = 'active' AND pa.account_type = 'simulated'
                               AND DATE(po.created_at) = $1 AND po.status = 'filled'
                             GROUP BY po.paper_account_id",
                        )
                        .bind(today)
                        .fetch_all(db)
                        .await
                        .unwrap_or_default();

                        for (aid, buy_n, sell_n, buy_amt, sell_amt) in &per_account {
                            let name: Option<String> = sqlx::query_scalar(
                                "SELECT name FROM paper_account WHERE paper_account_id = $1",
                            )
                            .bind(aid)
                            .fetch_optional(db)
                            .await
                            .ok()
                            .flatten();
                            let label = name.as_deref().unwrap_or(aid);
                            lines.push(format!(
                                "**{}** 买入 {} 笔(¥{:.0}) / 卖出 {} 笔(¥{:.0})",
                                label, buy_n, buy_amt, sell_n, sell_amt
                            ));
                        }
                        let msg = format!(
                            "## ✅ 调仓执行完成  \n\n**日期**: {}  \n**总订单**: {} 笔  \n\n{}\n\n> 自动生成于 14:45 调仓",
                            today.format("%Y-%m-%d"),
                            new_orders,
                            lines.join("  \n")
                        );
                        send_dingtalk_alert_titled(db, "调仓执行完成", &msg).await;

                        // 推送持仓摘要钉钉通知(asset class 分布 + 持仓明细)
                        info!("[scheduler] 调仓完成, 推送钉钉持仓摘要...");
                        match crate::routes::report::push_dingtalk_for_all_accounts_public(
                            db, today,
                        )
                        .await
                        {
                            Ok(_) => info!("[scheduler] 钉钉推送完成"),
                            Err(e) => warn!("[scheduler] 钉钉推送失败: {}", e),
                        }

                        // 推送今日交易明细钉钉通知（每账户买卖明细+理由）
                        match crate::routes::report::push_dingtalk_trade_detail_notification(
                            db, today,
                        )
                        .await
                        {
                            Ok(_) => info!("[scheduler] 交易明细推送完成"),
                            Err(e) => warn!("[scheduler] 交易明细推送失败: {}", e),
                        }
                    }
                }
                Err(e) => {
                    let msg = format!(
                        "## ⛔ 调仓执行失败  \n\n**日期**: {}  \n**错误**: {}\n\n> 请检查 quant-api 日志排查根因",
                        today.format("%Y-%m-%d"),
                        e
                    );
                    send_dingtalk_alert_titled(db, "调仓执行失败", &msg).await;
                    error!("[scheduler] 调仓失败: {}", e);
                }
            }
        }
    }

    // ── 21:30 (盘后数据就绪, 任务73 前移): 交易日EOD + 非交易日也执行数据同步 ──
    if is_eod_sync_window(hour, minute) {
        let should_sync = {
            let st = state.lock().await;
            !st.eod_synced_today
        };

        if should_sync {
            {
                let mut st = state.lock().await;
                st.eod_synced_today = true;
            }
            info!("[scheduler] 21:30 日终数据同步...");
            if let Err(e) = crate::routes::sync::sync_eod_data(db, tushare, today, is_trade).await {
                warn!("[scheduler] 日终数据同步失败: {}", e);
            }

            // 日报推送已在 sync_eod_data 内部提前完成（composite 合成后立即推，
            // 不等复权因子全量同步）。这里仅标记 report_pushed 状态。
            if is_trade {
                let mut st = state.lock().await;
                st.report_pushed = true;
            }
        }
    }

    // ── 每日 9:00: 同步上一个交易日日线 (Tushare T+1) + 因子重算 ──
    // 非交易日同样执行，周末/假日后自动回补缺失数据
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
            // 查询最近一个交易日（处理假期/周末间隔）
            let last_trade_date: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                "SELECT trade_date FROM market_trade_calendar WHERE is_open = true AND trade_date < $1 ORDER BY trade_date DESC LIMIT 1"
            ).bind(today).fetch_optional(db).await.ok().flatten();
            let sync_date = match last_trade_date {
                Some((d,)) => d,
                None => today - chrono::Duration::days(1),
            };
            let sync_date_str = sync_date.format("%Y%m%d").to_string();
            info!(
                "[scheduler] 9:00 T+1 补同步最近交易日 {} 日线 + 因子重算...",
                sync_date_str
            );

            // Step 1: 先同步事件数据，避免停牌/涨跌停门禁被“完成标记”误放行。
            if let Err(e) = quant_data::sync::sync_suspension(db, tushare, &sync_date_str).await {
                warn!("[scheduler] T+1 停牌同步失败: {}", e);
            }
            if !sync_limit_with_retry(db, tushare, &sync_date_str).await {
                warn!("[scheduler] T+1 涨跌停同步失败 (已重试)");
            }

            // Step 2: 同步最近交易日日线 + ETF日线 (T+1数据已就绪)
            let bar_dv = format!("dv-t1-{}", sync_date_str);
            let all_stocks: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
            )
            .fetch_all(db)
            .await
            .unwrap_or_default();
            match quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &all_stocks,
                &sync_date_str,
                &sync_date_str,
                &bar_dv,
            )
            .await
            {
                Ok(n) => info!("[scheduler] T+1 A股日线同步: {} 条", n),
                Err(e) => warn!("[scheduler] T+1 A股日线同步失败: {}", e),
            }
            let etf_symbols = load_active_etf_symbols_union(db).await;
            let _ = quant_data::sync::sync_fund_daily(
                db,
                tushare,
                &etf_symbols,
                &sync_date_str,
                &sync_date_str,
                &format!("etf-t1-{}", sync_date_str),
            )
            .await;
            let index_codes = vec!["000300.SH".to_string()];
            let _ = quant_data::sync::sync_index_daily(
                db,
                tushare,
                &index_codes,
                &sync_date_str,
                &sync_date_str,
                &format!("idx-t1-{}", sync_date_str),
            )
            .await;

            // Step 3: 验证日线数据已就绪 (实际查询DB确认, 非盲等)
            let mut retries = 0;
            let max_retries = 30; // 最多等5分钟 (30×10s)
            loop {
                let count: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM market_stock_daily_bar_adj WHERE trade_date = $1",
                )
                .bind(sync_date)
                .fetch_one(db)
                .await
                .unwrap_or((0,));
                if count.0 > 100 {
                    info!(
                        "[scheduler] T+1 日线数据已就绪: {} 条 (等待{}s)",
                        count.0,
                        retries * 10
                    );
                    break;
                }
                retries += 1;
                if retries >= max_retries {
                    warn!(
                        "[scheduler] T+1 日线数据等待超时({}s), 仅{}条, 因子回填可能不完整",
                        retries * 10,
                        count.0
                    );
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            }

            // Step 3b: T+1 补盯市。21:30 EOD 时 fund_daily 偶发无当日数据(前移后概率略增),
            // ETF 持仓的日终盯市被迫落在前日收盘。此处 ETF 日线已补齐,对 active 账户
            // 补 mark_to_market(上一交易日收盘) + NAV 重算。
            let remak_accounts: Vec<String> = PgPaperAccountRepo::new(db)
                .find_active_simulated_ids()
                .await
                .unwrap_or_default();
            for aid in &remak_accounts {
                if let Err(e) = crate::routes::rebalance::mark_to_market(
                    db,
                    aid,
                    sync_date,
                    crate::routes::rebalance::PriceSource::EodClose,
                )
                .await
                {
                    warn!("[scheduler] T+1 补盯市失败 {} {}: {}", aid, sync_date, e);
                    continue;
                }
                if let Err(e) = crate::routes::trading::update_current_nav(db, aid).await {
                    warn!("[scheduler] T+1 NAV 重算失败 {}: {}", aid, e);
                }
            }

            // Step 3c: 昨日日终数据不完整的账户(snapshot 缺失或 daily_return IS NULL,
            // 即昨日日报显示 --)→ 补盯市后数据已齐,refresh 重算 snapshot 并补发昨日绩效。
            let needs_resend: Vec<String> = sqlx::query_scalar(
                "SELECT pa.paper_account_id
                 FROM paper_account pa
                 LEFT JOIN paper_nav_snapshot s
                        ON s.paper_account_id = pa.paper_account_id AND s.snapshot_date = $1
                 WHERE pa.status = 'active' AND pa.account_type = 'simulated'
                   AND (s.paper_account_id IS NULL OR s.daily_return IS NULL)",
            )
            .bind(sync_date)
            .fetch_all(db)
            .await
            .unwrap_or_default();
            if !needs_resend.is_empty() {
                crate::routes::report::refresh_eod_snapshot(db, sync_date).await;
                // refresh 后 daily_return 仍 NULL 的(T+1 数据也缺)不补发,避免重复发 -- 日报
                let recovered: Vec<String> = sqlx::query_scalar(
                    "SELECT paper_account_id FROM paper_nav_snapshot \
                     WHERE snapshot_date = $1 AND daily_return IS NOT NULL \
                       AND paper_account_id = ANY($2)",
                )
                .bind(sync_date)
                .bind(&needs_resend)
                .fetch_all(db)
                .await
                .unwrap_or_default();
                if !recovered.is_empty() {
                    info!(
                        "[scheduler] T+1 补发昨日绩效: {} 账户 ({})",
                        recovered.len(),
                        sync_date
                    );
                    if let Err(e) = crate::routes::report::resend_daily_performance_report(
                        db, sync_date, &recovered,
                    )
                    .await
                    {
                        warn!("[scheduler] T+1 补发昨日绩效失败: {}", e);
                    }
                } else {
                    warn!(
                        "[scheduler] ⚠ {} 昨日日终数据 T+1 仍未补齐,跳过补发",
                        sync_date
                    );
                }
            }

            // Step 4: 日线就绪后才触发因子回填（覆盖v24全部因子类别）
            if retries < max_retries {
                info!("[scheduler] 触发因子回填 (依赖数据已就绪)");
                let backfill_start = (sync_date - BACKFILL_WINDOW_DAYS)
                    .format("%Y%m%d")
                    .to_string();
                trigger_v24_backfill_routes(db, &backfill_start, &sync_date_str).await;
                // 遍历所有 active 复合策略的 combo 做增量物化（不再依赖单一 v19 的 sc）。
                let active_combos: Vec<(String, String)> = sqlx::query_as(
                    "SELECT strategy_id, combo_name FROM strategy_config
                     WHERE status='active' AND strategy_type='composite'
                       AND combo_name IS NOT NULL AND combo_name <> ''
                       AND combo_name <> 'phase7_price_volume_expanded_v1'
                     ORDER BY strategy_id",
                )
                .fetch_all(db)
                .await
                .unwrap_or_default();
                for (sid, combo) in active_combos {
                    // T+1 combo 增量物化:用 ext + 读取策略配置的 include_fundamentals/factor_whitelist,
                    // 确保含基本面因子的 combo(如 v24 fund_v2)不会被默认黑名单过滤。
                    let inc_fund: bool = sqlx::query_scalar(
                        "SELECT COALESCE(include_fundamentals,false) FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                    )
                    .bind(&sid)
                    .fetch_optional(db)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(false);
                    let horizon_col: Option<i16> = sqlx::query_scalar(
                        "SELECT combo_horizon FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                    )
                    .bind(&sid)
                    .fetch_optional(db)
                    .await
                    .ok()
                    .flatten();
                    let whitelist: Option<Vec<String>> = {
                        let raw: Option<serde_json::Value> = sqlx::query_scalar(
                            "SELECT factor_whitelist FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                        )
                        .bind(&sid)
                        .fetch_optional(db)
                        .await
                        .ok()
                        .flatten();
                        raw.and_then(|v| {
                            v.as_array().map(|arr| {
                                arr.iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect()
                            })
                        })
                    };
                    // 增量物化口径继承（2026-09-07 事故修复）：按该 combo 在
                    // combo_materialization_log 的最近一次实跑口径继承 ind_neutral，
                    // 防止硬编码 false 覆盖手动物化确立的 t 口径造成信号漂移
                    //（9/07 实例：t 口径全量物化被 scheduler f 增量覆盖，sleeve 9.4%→0.15%）。
                    let ind_neut: bool = sqlx::query_scalar(
                        "SELECT ind_neutral FROM combo_materialization_log WHERE combo_name=$1 ORDER BY materialized_at DESC LIMIT 1",
                    )
                    .bind(&combo)
                    .fetch_optional(db)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(false);
                    match crate::routes::factors::materialize_pit_combo_ext(
                        db,
                        &crate::routes::factors::PitComboMaterializeParams {
                            combo_name: &combo,
                            factor_version: "1.0.0",
                            horizon: horizon_col.unwrap_or_else(|| combo_horizon_from_name(&combo)),
                            start_date: sync_date - chrono::Duration::days(7),
                            end_date: sync_date,
                            include_fundamentals: inc_fund,
                            // min_abs_ic_ir: T+1 保鲜不加阈值(白名单已筛)
                            min_abs_ic_ir: None,
                            factor_whitelist: whitelist.as_deref(),
                            ind_neutral: ind_neut,
                        },
                    )
                    .await
                    {
                        Ok(rows) => info!(
                            "[scheduler] T+1 PIT combo 增量物化完成 ({}={}): {} 行",
                            sid, combo, rows
                        ),
                        Err(e) => warn!(
                            "[scheduler] T+1 PIT combo 增量物化失败 ({}={}): {}",
                            sid, combo, e
                        ),
                    }
                }
            }

            info!("[scheduler] T+1 补同步完成 ({})", sync_date_str);
        }
    }

    // ── 收盘后 (16:00-16:30, 工作段A尾): 清理 7 天前过期回测数据 (每日一次) ──
    // 任务73 收窗：16:30-21:00 属非工作段(可部署/关机),清理必须在工作段内完成;
    // 错过当日窗口则次日再清(cleanup_done 为每日状态,过期清理晚一天无碍)。
    if hour == 16 && (0..30).contains(&minute) {
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

    // 任务73：季度 WFA 挪工作段 B(21:00-22:00, 原 16:00+)——非工作段定义下
    // 16:30 后可能部署/关机,长任务(分钟-小时级寻优)不得跨非工作段执行;
    // 次日 09:35 调仓前完成即可,与 EOD 链(21:30 起)数据依赖不冲突(读历史)。
    if is_first_trading_day_of_quarter && (21..22).contains(&hour) {
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
    db: &PgPool,
    port: u16,
    quarter: &str,
    today: NaiveDate,
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
        .post(format!(
            "{}/api/v1/quant/optimizations/phase7-oos-walk-forward-discovery",
            base
        ))
        .json(&serde_json::json!({
            "data_version_id": &get_latest_data_version(db).await,
            "strategy_version_id": "phase7-professional-v1",
            "search_profile": "professional_simple_heuristic_discovery",
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

    let Some(exp_id) = exp_id else {
        return Ok(());
    };

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
        let trial = sqlx::query_as::<
            _,
            (
                String,
                Option<serde_json::Value>,
                Option<rust_decimal::Decimal>,
            ),
        >(
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

            let test_start = wf
                .as_ref()
                .and_then(|w| w.get("test_start").and_then(|v| v.as_str()))
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
            let test_end = wf
                .as_ref()
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
                info!(
                    "[scheduler] WFA 参数已提取: window={} trial={}",
                    window_idx,
                    &trial_id[..32.min(trial_id.len())]
                );
            }
        }
    }

    if extracted > 0 {
        info!("[scheduler] WFA 参数提取完成: {} 个窗口", extracted);
    }
    Ok(())
}

/// 调仓前数据校验+自动修复。返回非空列表 = 校验/修复失败，拒绝调仓。
/// 调仓前数据校验+自动修复。返回非空列表 = 校验/修复失败，拒绝调仓。
/// pub: 手动触发调仓(admin::manual_rebalance)复用此前置校验。
pub async fn validate_pre_trade_data(
    db: &PgPool,
    tushare: &TushareClient,
    today: NaiveDate,
    sc: &StrategyConfig,
) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();
    let today_str = today.format("%Y%m%d").to_string();

    // 1. ETF 日线 — 每个标的必须覆盖到最近一个交易日
    for symbol in &sc.etf_symbols {
        let stale = check_data_freshness(db, symbol, today, 1).await;
        if let Some(gap_td) = stale {
            info!(
                "[pre-trade] {} 数据落后{}交易日, 尝试自动同步...",
                symbol, gap_td
            );
            let dv_id = format!("pre-trade-etf-{}-{}", symbol, today_str);
            let n = quant_data::sync::sync_fund_daily(
                db,
                tushare,
                std::slice::from_ref(symbol),
                &today_str,
                &today_str,
                &dv_id,
            )
            .await
            .unwrap_or(0);
            if n > 0 {
                info!("[pre-trade] {} 同步: {} 条", symbol, n);
            } else {
                let week_ago = (today - chrono::Duration::days(7))
                    .format("%Y%m%d")
                    .to_string();
                let dv2 = format!("pre-trade-etf-wk-{}-{}", symbol, today_str);
                let n2 = quant_data::sync::sync_fund_daily(
                    db,
                    tushare,
                    std::slice::from_ref(symbol),
                    &week_ago,
                    &today_str,
                    &dv2,
                )
                .await
                .unwrap_or(0);
                if n2 > 0 {
                    info!("[pre-trade] {} 周回补: {} 条", symbol, n2);
                } else {
                    errors.push(format!(
                        "ETF {}: 数据落后{}交易日, 自动同步失败",
                        symbol, gap_td
                    ));
                }
            }
        }
    }

    // 2. A股日线 — 抽查沪深主板
    for probe in &["000001.SZ", "600000.SH"] {
        let stale = check_data_freshness(db, probe, today, 1).await;
        if let Some(gap_td) = stale {
            info!(
                "[pre-trade] A股({})落后{}交易日, 自动同步...",
                probe, gap_td
            );
            let dv_id = format!("pre-trade-stock-{}", today_str);
            let recent_start = (today - chrono::Duration::days(7))
                .format("%Y%m%d")
                .to_string();
            match quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &[probe.to_string()],
                &recent_start,
                &today_str,
                &dv_id,
            )
            .await
            {
                Ok(n) if n > 0 => {
                    info!("[pre-trade] A股日线同步: {} 条", n);
                    break; // 成功一个就够
                }
                _ => errors.push(format!("A股日线({}): 数据落后, 自动同步失败", probe)),
            }
        }
    }

    // 3. 策略声明的 PIT combo — 缺了自动触发对应物化，不能用旧 PV combo 代替 full PIT。
    //    遍历所有 active 策略用到的 combo（含 composite + asset 子策略），逐个校验新鲜度+物化。
    //    模拟实盘盘中调仓依赖：每个激活账号的激活策略用到的 combo 都必须有当日因子数据。
    let mut active_combos = load_active_factor_combos(db).await;
    if active_combos.is_empty() {
        // 无 active 复合策略时兜底用传入 sc 的 combo（保持原行为，不应命中——start_scheduler 已跳过无策略）。
        let fallback = pre_trade_factor_combo(sc).to_string();
        active_combos.push(fallback);
    }
    for factor_combo in active_combos {
        if let Some(gap_td) = check_factor_freshness(db, &factor_combo, today, 2).await {
            info!(
                "[pre-trade] 因子({})落后{}交易日, 自动触发回填...",
                factor_combo, gap_td
            );
            let materialize_start = today - chrono::Duration::days(30);
            let trigger_ok = if factor_combo == "phase7_price_volume_expanded_v1" {
                let client = reqwest::Client::new();
                let backfill_start = materialize_start.format("%Y%m%d").to_string();
                let today_str_clone = today_str.clone();
                let api_base = self_api_base();
                client
                    .post(format!(
                        "{}/api/v1/quant/factors/phase7-price-volume-backfill/background",
                        api_base
                    ))
                    .json(
                        &serde_json::json!({"start_date": backfill_start, "end_date": today_str_clone}),
                    )
                    .timeout(std::time::Duration::from_secs(10))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
            } else {
                match crate::routes::factors::materialize_pit_combo(
                    db,
                    &factor_combo,
                    "1.0.0",
                    combo_horizon_from_name(&factor_combo),
                    materialize_start,
                    today,
                )
                .await
                {
                    Ok(rows) => {
                        info!(
                            "[pre-trade] PIT combo {} 物化完成: {} 行",
                            factor_combo, rows
                        );
                        true
                    }
                    Err(e) => {
                        warn!("[pre-trade] PIT combo {} 物化失败: {}", factor_combo, e);
                        false
                    }
                }
            };
            if trigger_ok {
                // 轮询等待因子计算完成
                for retry in 0..20 {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    if check_factor_freshness(db, &factor_combo, today, 2)
                        .await
                        .is_none()
                    {
                        info!(
                            "[pre-trade] 因子({})回填完成 (等待{}s)",
                            factor_combo,
                            (retry + 1) * 3
                        );
                        break;
                    }
                }
                // 再次检查
                if let Some(g) = check_factor_freshness(db, &factor_combo, today, 2).await {
                    errors.push(format!(
                        "因子({}): 自动回填后仍落后{}交易日, 请检查底层数据",
                        factor_combo, g
                    ));
                }
            } else {
                errors.push(format!("因子({}): 自动回填触发失败", factor_combo));
            }
        }
    }

    errors
}

/// 检查单个 symbol 的数据新鲜度。返回 Some(落后交易日数) 表示需要同步。
async fn check_data_freshness(
    db: &PgPool,
    symbol: &str,
    today: NaiveDate,
    max_gap: i64,
) -> Option<i64> {
    let max_row: Option<(chrono::NaiveDate,)> =
        sqlx::query_as("SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = $1")
            .bind(symbol)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    if let Some((max_dt,)) = max_row {
        // market_trade_calendar 存在重复行（历史同步多来源导致），用 DISTINCT trade_date 去重，
        // 否则 gap 虚高（每行重复 N 倍）误判数据落后。
        let gap: (i64,) = sqlx::query_as(
            "SELECT COUNT(DISTINCT trade_date) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((999,));
        if gap.0 > max_gap {
            Some(gap.0)
        } else {
            None
        }
    } else {
        Some(999) // 完全无数据
    }
}

/// 检查因子数据新鲜度
async fn check_factor_freshness(
    db: &PgPool,
    combo: &str,
    today: NaiveDate,
    max_gap: i64,
) -> Option<i64> {
    let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(trade_date) FROM multi_factor_value
             WHERE combo_name = $1
               AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(combo)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if let Some((max_dt,)) = max_row {
        // market_trade_calendar 存在重复行，用 DISTINCT trade_date 去重，否则 gap 虚高误判因子落后。
        let gap: (i64,) = sqlx::query_as(
            "SELECT COUNT(DISTINCT trade_date) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((999,));
        if gap.0 > max_gap {
            Some(gap.0)
        } else {
            None
        }
    } else {
        Some(999)
    }
}

pub(crate) async fn is_trading_day(db: &PgPool, date: NaiveDate) -> Result<bool, String> {
    let row = sqlx::query_as::<_, (Option<bool>,)>(
        "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1 LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("calendar: {}", e))?;
    Ok(row.and_then(|(v,)| v).unwrap_or(false))
}

/// v24 白名单 14 活跃因子（与 factor_backfill_route 表配置路由覆盖的因子集一致）。
/// P3-2: 提升为模块级常量，供 admin.rs 的 GET /api/v1/admin/factor-health 端点复用，
/// 避免与 run_data_quality_check 内部检测逻辑维护两份因子清单。
pub(crate) const V24_FACTOR_CODES: &[&str] = &[
    "amihud_20d_std",
    "liq_amount_trend_20v120_std",
    "defensive_lowvol_quality_daily_std",
    "fin_gross_margin_daily_std",
    "fin_debt_to_assets_yoy_improve_std",
    "fin_roa_yoy_accel_std",
    "mf_elg_inflow_low_crowding_10d_std",
    "forecast_type_upgrade_120d_std",
    "mkt_downside_beta_120d_std",
    "block_trade_inst_sell_inverse_20d_decay_std",
    "repurchase_amount_log_latest_std",
    "mf_elg_net_amount_5d_std",
    "mf_net_amount_5d_std",
    "repurchase_volume_log_latest_std",
];

/// 检查定时任务 CRON 配置的依赖顺序。
///
/// 规则：
/// - factor_backfill_daily 需要等日线同步完成后运行（9:00 T+1 或 16:00 EOD 之后）
/// - equity_curve_monthly 需要因子数据就绪后运行（factor_backfill 之后）
///
/// 内置 scheduler 同步时刻（北京时间）:
///   T+1 补同步: 9:00  |  EOD 同步: 16:00
pub async fn check_task_dependency_order(db: &PgPool) -> Vec<String> {
    let mut issues = Vec::new();

    // 读取所有启用任务的 CRON（格式: "分 时 日 月 周"）
    let tasks: Vec<(String, String)> = sqlx::query_as(
        "SELECT task_name, schedule_cron FROM scheduled_task_config WHERE enabled = true",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    // ── 调仓依赖的必要任务存在性检查 ──
    // 缺失/禁用则盘中调仓数据无保障,需提示用户配置(钉钉 + 前端 /tasks 页面双路展示)。
    let required_tasks: [(&str, &str); 3] = [
        ("factor_backfill_daily", "因子回填 phase7 量价因子"),
        ("pit_combo_refresh_daily", "PIT combo 保鲜(因子组合物化)"),
        ("data_quality_daily", "数据质量检查"),
    ];
    let configured: std::collections::HashSet<&str> =
        tasks.iter().map(|(n, _)| n.as_str()).collect();
    for (name, desc) in &required_tasks {
        if !configured.contains(name) {
            issues.push(format!(
                "缺少必要定时任务: {} ({}) — 盘中调仓数据可能无保障, 请在 /tasks 页面配置",
                name, desc
            ));
        }
    }

    // 简单解析 CRON 的时和分字段，兼容 DB 里常见的 5 字段 cron 与 cron crate 需要的 6 字段 cron。
    let mut task_times: Vec<(String, u32, u32)> = Vec::new(); // (name, hour, minute)
    for (name, cron_str) in &tasks {
        match scheduled_task_time_minutes(cron_str) {
            Ok(time_minutes) => {
                task_times.push((name.clone(), time_minutes / 60, time_minutes % 60))
            }
            Err(message) => {
                issues.push(format!("{}: {}", name, message));
            }
        }
    }

    // 内置 scheduler 同步完成时间（北京时间）
    // T+1 补同步: 9:00-9:10  |  EOD 同步: 16:00-16:10
    let t1_complete = (9, 10); // 9:10 BJT
    let eod_complete = (16, 10); // 16:10 BJT

    // 检查 factor_backfill_daily 的 CRON 时间
    for (name, hour, min) in &task_times {
        let time_minutes = hour * 60 + min;

        if name == "factor_backfill_daily" {
            let eod_min = eod_complete.0 * 60 + eod_complete.1;
            if time_minutes < eod_min && time_minutes < t1_complete.0 * 60 {
                issues.push(format!(
                    "factor_backfill_daily: CRON {}:{:02} (BJ) 早于日线同步完成 (16:10),
                     因子计算可能缺少当日日线数据",
                    hour, min
                ));
            }
        }

        if name == "equity_curve_monthly" {
            // 权益曲线依赖因子数据，应在 factor_backfill_daily 之后
            let factor_min = task_times
                .iter()
                .find(|(n, _, _)| n == "factor_backfill_daily")
                .map(|(_, h, m)| h * 60 + m);
            if let Some(fm) = factor_min {
                if time_minutes < fm {
                    issues.push(format!(
                        "equity_curve_monthly: CRON {}:{:02} (BJ) 早于 factor_backfill_daily ({}:{:02}),
                         权益曲线可能缺少因子数据", hour, min, fm / 60, fm % 60
                    ));
                }
            }
        }
    }

    issues
}

/// 涨跌停数据同步 (直接调用, 带重试)
pub(crate) async fn sync_limit_with_retry(
    db: &PgPool,
    tushare: &TushareClient,
    date_str: &str,
) -> bool {
    let d = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d").unwrap();
    let _ = sqlx::query("DELETE FROM market_stock_limit WHERE trade_date = $1")
        .bind(d)
        .execute(db)
        .await;

    // 第一次尝试
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => {
            info!("[scheduler] 涨跌停同步成功 ({} 条)", n);
            return true;
        }
        Err(e) => warn!("[scheduler] 涨跌停首次失败: {}, 65秒后重试...", e),
    }

    // 重试
    tokio::time::sleep(tokio::time::Duration::from_secs(65)).await;
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => {
            info!("[scheduler] 涨跌停重试成功 ({} 条)", n);
            true
        }
        Err(e) => {
            warn!("[scheduler] 涨跌停重试仍失败: {}", e);
            false
        }
    }
}

/// 确保ML预测数据覆盖到当前日期
/// 策略: gap≤1天→正常; gap>1天→触发后台训练生成; 无法生成→降级纯因子
/// ML预测数据检查+补齐 (60天间隔, ML训练通过内部HTTP触发——真异步长任务)
pub(crate) async fn ensure_prediction_coverage(
    db: &PgPool,
    _tushare: &TushareClient,
    date: chrono::NaiveDate,
    sc: &StrategyConfig,
) -> bool {
    let latest_training: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(training_end_date) FROM prediction_set WHERE status = 'ready' AND training_end_date IS NOT NULL"
    ).fetch_optional(db).await.ok().flatten();

    if let Some((last_train,)) = latest_training {
        let days_since = (date - last_train).num_days();
        if days_since < 60 {
            info!(
                "[scheduler] ML训练跳过 (最新训练={}, 距今{}天 < 60天)",
                last_train, days_since
            );
            return check_prediction_available(db, date).await;
        }
        info!(
            "[scheduler] ML训练触发 (最新训练={}, 距今{}天 >= 60天)",
            last_train, days_since
        );
    } else {
        info!("[scheduler] 首次ML训练");
    }

    // 验证依赖
    if !verify_training_dependencies(db, date, pre_trade_factor_combo(sc)).await {
        warn!("[scheduler] ⚠ ML训练依赖数据不全, 跳过");
        return check_prediction_available(db, date).await;
    }

    // ML训练是复杂异步任务, 通过内部HTTP + tokio::spawn触发, 不阻塞scheduler
    info!("[scheduler] 🚀 触发ML预测训练 (后台异步)");
    let url = self_api_base();
    let payload = serde_json::json!({
        "model_code": "nlqr", "model_version": "1.0.0",
        "model_version_id": "mdl-p7-wf-wide-qgvrel-h60-v1",
        "data_version_id": &get_latest_data_version(db).await,
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
    let db_clone = db.clone();
    tokio::spawn(async move {
        let _ = reqwest::Client::new()
            .post(format!(
                "{}/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker",
                url
            ))
            .json(&payload)
            .send()
            .await;
        // 训练完成后重建全市场预测集（合并 ETF + A 股数据）
        if let Err(e) = rebuild_full_universe_prediction_set(&db_clone).await {
            warn!("[scheduler] 全市场预测集重建失败: {}", e);
        }
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

/// 重建全市场预测集：合并最新ETF预测 + 最新A股个股预测为一个PIT合规的预测集。
/// 调度器60天自动训练后调用，确保数据持续更新。也可从 admin API 调用。
pub async fn rebuild_full_universe_prediction_set(db: &PgPool) -> Result<String, String> {
    let today = chrono::Utc::now().date_naive();
    let pred_start = today;
    let pred_end = today + chrono::Duration::days(63);
    let training_end = pred_start - chrono::Duration::days(1);

    // 新预测集 ID
    let pred_set_id = format!(
        "pred-full-1.0.0-nlq-wf-{}-{}",
        pred_start.format("%Y%m%d"),
        pred_end.format("%Y%m%d")
    );

    // 创建/更新 prediction_set（PIT 合规）
    sqlx::query(
        "INSERT INTO prediction_set (prediction_set_id, model_version_id, feature_set_version_id,
         data_version_id, start_date, end_date, training_end_date, prediction_hash, status, metadata)
         VALUES ($1, 'mdl-p7-wf-wide-qgvrel-h60-v1', 'phase7-wide-qgvrel-v1',
         'research-full-2016-2026-20260515', $2, $3, $4, 'full-universe-auto', 'ready', '{}'::jsonb)
         ON CONFLICT (prediction_set_id) DO UPDATE SET
           end_date = EXCLUDED.end_date, training_end_date = EXCLUDED.training_end_date, status = 'ready'"
    ).bind(&pred_set_id).bind(pred_start).bind(pred_end).bind(training_end)
     .execute(db).await.map_err(|e| format!("create set: {}", e))?;

    // 从最新ETF预测集复制 7 只核心 ETF
    let etf_src: Option<String> = sqlx::query_scalar(
        "SELECT prediction_set_id FROM prediction_set
         WHERE status = 'ready' AND training_end_date IS NOT NULL
           AND training_end_date < $1 AND start_date <= $1 AND end_date >= $1
         ORDER BY training_end_date DESC LIMIT 1",
    )
    .bind(today)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    if let Some(ref src) = etf_src {
        sqlx::query(
            "INSERT INTO model_prediction (prediction_set_id, trade_date, symbol, score, probability, available_at, created_at)
             SELECT $1, mp.trade_date, mp.symbol, mp.score, mp.probability, mp.available_at, now()
             FROM model_prediction mp WHERE mp.prediction_set_id = $2
               AND mp.symbol IN ('518880.SH','511010.SH','513500.SH','513100.SH','159980.SZ','159985.SZ','501018.SH')
             ON CONFLICT (prediction_set_id, trade_date, symbol) DO UPDATE SET score = EXCLUDED.score"
        ).bind(&pred_set_id).bind(src).execute(db).await.map_err(|e| format!("copy etf: {}", e))?;
    }

    // 从最新A股预测集复制个股
    let stock_src: Option<String> = sqlx::query_scalar(
        "SELECT prediction_set_id FROM model_prediction
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         GROUP BY prediction_set_id
         HAVING COUNT(DISTINCT symbol) >= 20
         ORDER BY MAX(trade_date) DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    if let Some(ref src) = stock_src {
        sqlx::query(
            "INSERT INTO model_prediction (prediction_set_id, trade_date, symbol, score, probability, available_at, created_at)
             SELECT $1, mp.trade_date, mp.symbol, mp.score, mp.probability, mp.available_at, now()
             FROM model_prediction mp WHERE mp.prediction_set_id = $2
               AND mp.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
             ON CONFLICT (prediction_set_id, trade_date, symbol) DO UPDATE SET score = EXCLUDED.score"
        ).bind(&pred_set_id).bind(src).execute(db).await.map_err(|e| format!("copy stock: {}", e))?;
    }

    let symbols: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT symbol) FROM model_prediction WHERE prediction_set_id = $1",
    )
    .bind(&pred_set_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    info!(
        "[scheduler] 全市场预测集已重建: {} ({} 符号, PIT={})",
        pred_set_id, symbols, training_end
    );
    Ok(pred_set_id)
}

/// 验证ML训练依赖的所有数据是否就绪
async fn verify_training_dependencies(
    db: &PgPool,
    date: chrono::NaiveDate,
    combo_name: &str,
) -> bool {
    let today = date;
    let threshold = today - chrono::Duration::days(2); // 2天内都算就绪

    // A股日线
    let daily_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar WHERE trade_date >= $1",
    )
    .bind(threshold)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| d >= threshold)
    .unwrap_or(false);

    // 复权因子 (30天阈值, 因为Tushare月更)
    let adj_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_adjustment_factor",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| (today - d).num_days() < 60)
    .unwrap_or(false);

    // 因子值 (策略声明 combo，PIT available_at 不晚于 trade_date)
    let factor_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name = $1
           AND trade_date >= $2
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(combo_name)
    .bind(threshold)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| d >= threshold)
    .unwrap_or(false);

    let all_ok = daily_ok && adj_ok && factor_ok;
    info!(
        "[scheduler] ML训练依赖检查: daily={} adj={} factor({})={} → {}",
        daily_ok,
        adj_ok,
        combo_name,
        factor_ok,
        if all_ok { "OK" } else { "MISSING" }
    );
    all_ok
}

/// 为所有活跃模拟账号生成交易信号（使用 LW-MVO 自动发现权重）。
/// pub: 手动触发调仓(admin::manual_rebalance)复用,不依赖 scheduler DailyState。
/// 内部 per-account 今日已交易检查保证幂等(今日已调仓账号跳过)。
pub async fn generate_paper_signals_for_all(
    db: &PgPool,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    port: u16,
    date: NaiveDate,
    _sc: &StrategyConfig,
    tushare: &TushareClient,
    skip_data_gate: bool,
) -> Result<(), String> {
    let accounts = PgPaperAccountRepo::new(db)
        .find_active_simulated_ids()
        .await
        .map_err(|e| format!("account query: {}", e))?;

    if accounts.is_empty() {
        return Ok(());
    }

    for account_id in &accounts {
        // R2 类型门禁：load_account 按 leverage_enabled 分派 Cash/Margin，
        // Cash 账号编译期保证无 leverage_config（杜绝无杠杆账户误融资）。
        let loaded = match crate::routes::account::load_account(db, account_id).await {
            Ok(l) => l,
            Err(e) => {
                warn!("[paper] 账号 {} 加载失败,跳过: {}", account_id, e);
                continue;
            }
        };
        let name = loaded.name().to_string();
        let strategy_version_id = loaded.strategy_version_id().to_string();
        if strategy_version_id.is_empty() {
            error!(
                "[paper] 账号 {} 未配置 strategy_version_id,跳过(配置错误,不阻塞其他账号)",
                account_id
            );
            continue;
        }
        // leverage_params: Cash 编译期返回 (false,1.0,"fixed"),Margin 取 LeverageConfig
        let (leverage_enabled, leverage_multiplier, leverage_mode) = loaded.leverage_params();
        let leverage_mode = leverage_mode.as_str();
        let rs =
            match crate::routes::strategy::load_resolved_strategy(db, &strategy_version_id).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("[paper] {} 策略加载失败,跳过: {}", account_id, e);
                    continue;
                }
            };
        // R1 实盘状态门禁：校验策略已回测过（equity_curve_task_id 对应回测曲线存在），
        // 未回测的策略拒绝上实盘。校验通过后推进到 Strategy<Production>，
        // 下游 deref 成 &ResolvedStrategy 透传（保持兼容，零调用点改动）。
        let rs = match rs.promote_to_production(db).await {
            Ok(prod) => prod,
            Err(e) => {
                error!(
                    "[paper] {} 实盘门禁拦截: {} (未回测策略不能上实盘,跳过)",
                    account_id, e
                );
                // 严重:未回测策略挂到 active 账号,说明配置错误。告警但不阻塞其他账号。
                send_dingtalk_alert(
                    db,
                    &format!(
                        "⚠️ 实盘门禁拦截: 账号 {} 挂载的策略 {} 未通过回测校验，已跳过调仓。\n{}",
                        account_id, strategy_version_id, e
                    ),
                )
                .await;
                continue;
            }
        };
        // 桥接出平铺 StrategyConfig 视图:run-factor body 取参(combo_name/top_n 等)继续用 sc。
        // 传给 sync_positions_from_backtest 时传 &rs(不再传 &sc)。
        let sc = resolved_to_legacy_sc(&rs)?;
        let a_share = rs
            .assets
            .iter()
            .find(|a| a.asset_class == AssetClass::AShare);
        let signal_source = a_share
            .map(|a| a.security.signal_source.as_str())
            .unwrap_or("fixed");
        info!(
            "[paper] {} ({}) strategy={} leverage={}x mode={} signal={}",
            name, account_id, rs.strategy_id, leverage_multiplier, leverage_mode, signal_source
        );

        // 检查今日是否已有交易
        let done: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM paper_order
             WHERE paper_account_id = $1 AND DATE(created_at) = $2",
        )
        .bind(account_id)
        .bind(date)
        .fetch_one(db)
        .await
        .map_err(|e| format!("count: {}", e))?;

        if done.0 > 0 {
            continue;
        }

        if !skip_data_gate {
            if let Err(e) = check_paper_account_data_readiness(
                db,
                account_id,
                None,
                DataReadinessGate::BlockRequiredYellow,
                "paper_intraday_trading",
            )
            .await
            {
                warn!("[paper] {} 数据门禁失败，跳过本次交易: {}", name, e);
                send_quality_alert(db, &[format!("{}: {}", name, e)]).await;
                continue;
            }
        } else {
            info!("[paper] {} 跳过数据门禁(历史重放模式)", name);
        }

        // 180 天窗口:覆盖 40 日频调仓 ≥2 个信号周期 + 信号前置数据需求
        // (~50 交易日),保证当日截面非空(2026-08-20 实测:90 天窗口 + 40 日频
        // signals=0 截面空;180 天窗口 signals=3、当日截面 11 只。
        // 历史背景:30 天短窗口曾迫使 rebalance=daily,P0 修复——非日频在短窗口
        // 当日无信号截面 → 误清仓)。
        let start = (date - chrono::Duration::days(180))
            .format("%Y%m%d")
            .to_string();
        let end = date.format("%Y%m%d").to_string();
        let client = reqwest::Client::new();
        let base = format!("http://localhost:{}", port);

        // 获取当前 WFA 最优参数（如有），合并到默认参数
        let wfa_params = get_current_wfa_params(db, date).await.unwrap_or_default();

        let combo_name = wfa_params
            .get("combo_name")
            .and_then(|v| v.as_str())
            .unwrap_or(sc.combo_name.as_str());
        let top_n = wfa_params
            .get("top_n")
            .and_then(|v| v.as_u64())
            .unwrap_or(sc.top_n as u64) as usize;
        let portfolio_method = wfa_params
            .get("portfolio_method")
            .and_then(|v| v.as_str())
            .unwrap_or("heuristic");
        let score_direction = wfa_params
            .get("score_direction")
            .and_then(|v| v.as_str())
            .unwrap_or("descending");
        let max_pos = wfa_params
            .get("max_position_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.10);
        let skip_top = wfa_params
            .get("skip_top_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let max_exposure = wfa_params
            .get("max_gross_exposure")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.95);
        let stop_loss = wfa_params
            .get("stop_loss_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let event_gate_combo = wfa_params
            .get("event_gate_combo_name")
            .and_then(|v| v.as_str());
        let event_gate_mode = wfa_params.get("event_gate_mode").and_then(|v| v.as_str());
        let event_gate_score_dir = wfa_params
            .get("event_gate_score_direction")
            .and_then(|v| v.as_str());
        let risk_filter = wfa_params
            .get("candidate_risk_filter")
            .and_then(|v| v.as_str());
        let max_corr = wfa_params
            .get("max_pairwise_correlation")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        // sleeve 调仓频率默认 40 交易日(2026-08-20 网格实验:indneutral_val_v1 组合 10/20/40 日频
        // 实测年化 8.3%/10.8%/10.4%,40 日频回撤 24.4% vs 20 日 31.4%,换手 69x vs 131x,
        // 风险调整后最优;原 v24 蓝图 task 参数为 rebalance="10")。
        // 历史注:2026-07-20 P0 修复曾强制 daily——因当时 30 天短窗口 + 非日频会导致
        // 当日无信号截面 → 误清仓。现窗口已拉长到 90 天,覆盖多个 10 日信号周期,
        // 当日截面恒非空(2026-08-20 实测:90 天窗口 + 10 日频,当日截面 8 只、
        // signals_count=6),daily 强制不再必要。日频换手 ~250x/年,摩擦成本吞掉
        // sleeve 全部仓位价值(全周期含成本回测三组全灭),10 日频降至 ~25x。
        let rebalance_freq = wfa_params
            .get("rebalance")
            .and_then(|v| v.as_str())
            .unwrap_or("40");
        // WFA 高级风控参数
        let vol_control = wfa_params
            .get("portfolio_volatility_control")
            .and_then(|v| v.as_str());
        let dd_control = wfa_params
            .get("portfolio_drawdown_control")
            .and_then(|v| v.as_str());
        let risk_contribution = wfa_params
            .get("risk_contribution_control")
            .and_then(|v| v.as_str());
        let partial_rebalance = wfa_params
            .get("partial_rebalance_ratio")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let risk_budget_days = wfa_params
            .get("risk_budget_lookback_days")
            .and_then(|v| v.as_u64());
        let event_gate_min = wfa_params
            .get("event_gate_min_score")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());

        let wfa_used = wfa_params
            .as_object()
            .map(|o| !o.is_empty())
            .unwrap_or(false);
        if wfa_used {
            info!(
                "[paper] WFA params: combo={} top_n={} method={}",
                combo_name, top_n, portfolio_method
            );
        }

        let mut body = serde_json::json!({
            "combo_name": combo_name, "version": "1.0.0",
            "strategy_version_id": "phase7-professional-v1",
            "data_version_id": &get_latest_data_version(db).await,
            "top_n": top_n, "rebalance": rebalance_freq, "max_position_pct": max_pos,
            "max_gross_exposure": max_exposure, "score_direction": score_direction,
            "portfolio_method": portfolio_method, "benchmark": "000300.SH",
            "skip_top_pct": skip_top, "entry_delay": 1,
            "universe_profile": "main_board_non_st",
            "start_date": start, "end_date": end
        });
        if let Some(sl) = stop_loss {
            body["stop_loss_pct"] = serde_json::json!(sl);
        }
        if let Some(eg) = event_gate_combo {
            body["event_gate_combo_name"] = serde_json::json!(eg);
        }
        if let Some(em) = event_gate_mode {
            body["event_gate_mode"] = serde_json::json!(em);
        }
        if let Some(es) = event_gate_score_dir {
            body["event_gate_score_direction"] = serde_json::json!(es);
        }
        if let Some(rf) = risk_filter {
            body["candidate_risk_filter"] = serde_json::json!(rf);
        }
        if let Some(mc) = max_corr {
            body["max_pairwise_correlation"] = serde_json::json!(mc);
        }
        if let Some(vc) = vol_control {
            body["portfolio_volatility_control"] = serde_json::json!(vc);
        }
        if let Some(dc) = dd_control {
            body["portfolio_drawdown_control"] = serde_json::json!(dc);
        }
        if let Some(rc) = risk_contribution {
            body["risk_contribution_control"] = serde_json::json!(rc);
        }
        if let Some(pr) = partial_rebalance {
            body["partial_rebalance_ratio"] = serde_json::json!(pr);
        }
        if let Some(rbd) = risk_budget_days {
            body["risk_budget_lookback_days"] = serde_json::json!(rbd);
        }
        if let Some(egm) = event_gate_min {
            body["event_gate_min_score"] = serde_json::json!(egm);
        }

        // 信号源路由: factor(默认) / prediction(ML) / prediction_blend(v16: 因子+ML混合)
        let is_prediction = signal_source == "prediction";
        let is_prediction_blend = signal_source == "prediction_blend";

        // v16: 动态选择预测集 — 优先用策略配置指定的，否则选最新 PIT 集
        let prediction_set_id = if is_prediction || is_prediction_blend {
            if let Some(ref pid) = sc.prediction_set_id {
                info!("[paper] 策略指定 prediction set: {}", pid);
                Some(pid.clone())
            } else {
                // PIT合规: 优先选包含A股个股预测的模型（v19策略需要），其次选最近训练的
                let best: Option<(String,)> = sqlx::query_as(
                    "SELECT ps.prediction_set_id FROM prediction_set ps
                 WHERE ps.status = 'ready'
                   AND ps.training_end_date IS NOT NULL
                   AND ps.training_end_date < $1           -- PIT: 训练数据必须在预测日期之前
                   AND ps.start_date <= $1 AND ps.end_date >= $1  -- 预测覆盖日期
                 ORDER BY ps.training_end_date DESC
                 LIMIT 1",
                )
                .bind(date)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();

                match best {
                    Some((pid,)) => {
                        info!("[paper] v16 prediction set: {} (覆盖{})", pid, date);
                        Some(pid)
                    }
                    None => {
                        warn!(
                            "[paper] ⚠ 无prediction set覆盖{}, v16降级为纯因子选股",
                            date
                        );
                        None
                    }
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
                blend_body["prediction_blend_weight"] =
                    serde_json::json!(sc.prediction_blend_weight);
            }
            // v16 专用参数(P1-4 配置化:从 sc 读,原硬编码 0.25/200)
            if !wfa_used {
                blend_body["top_n"] = serde_json::json!(sc.top_n);
                blend_body["rebalance"] = serde_json::json!("biweekly");
                blend_body["kelly_fraction"] = serde_json::json!(sc.kelly_fraction);
                blend_body["score_candidate_pool_size"] =
                    serde_json::json!(sc.score_candidate_pool_size);
            }
            info!("[paper] v16 prediction_blend: set={:?}", prediction_set_id);
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&blend_body)
                .send()
                .await
        } else if is_prediction {
            client
                .post(format!("{}/api/v1/quant/backtests/run-prediction", base))
                .json(&serde_json::json!({
                    "prediction_set_id": prediction_set_id.as_ref(),
                    "strategy_version_id": "phase7-professional-v1",
                    "data_version_id": &get_latest_data_version(db).await,
                    "top_n": 15, "rebalance": "monthly", "max_position_pct": 0.10,
                    "max_gross_exposure": 0.95, "score_direction": "descending",
                    "portfolio_method": "heuristic", "benchmark": "000300.SH",
                    "skip_top_pct": 0.0, "entry_delay": 1,
                    "universe_profile": "main_board_non_st",
                    "start_date": start, "end_date": end
                }))
                .send()
                .await
        } else {
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&body)
                .send()
                .await
        };

        let task_id = match resp {
            Ok(r) => r.json::<serde_json::Value>().await.ok().and_then(|v| {
                v.get("data")
                    .and_then(|d| d.get("task_id"))
                    .and_then(|t| t.as_str())
                    .map(str::to_string)
            }),
            Err(_) => continue,
        };
        let Some(task_id) = task_id else { continue };

        match sync_positions_from_backtest(
            db,
            account_id,
            &task_id,
            mvo_cache,
            date,
            &rs,
            tushare,
            leverage_enabled,
            leverage_multiplier,
            leverage_mode,
        )
        .await
        {
            Ok(n) => info!("[paper] {} 同步 {} 个持仓", name, n),
            Err(e) => error!("[paper] {} 持仓同步失败: {}", name, e),
        }
    }
    Ok(())
}
async fn sync_positions_from_backtest(
    db: &PgPool,
    account_id: &str,
    task_id: &str,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    date: NaiveDate,
    rs: &ResolvedStrategy,
    tushare: &TushareClient,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
) -> Result<usize, String> {
    // 建仓逻辑统一委托 rebalance_account(实盘=Intraday 价格源)。
    // NAV 由 rebalance_account 末尾的 update_current_nav 统一重算(正确口径:持仓市值+cash-margin)。
    // 旧的手写 NAV SQL(cash=initial_capital-SUM(...),忽略 margin)已删除。
    crate::routes::rebalance::rebalance_account(
        db,
        account_id,
        rs,
        date,
        task_id,
        crate::routes::rebalance::PriceSource::Intraday,
        mvo_cache,
        tushare,
        leverage_enabled,
        leverage_multiplier,
        leverage_mode,
        // P2-B:实盘无上层预算,传 None 让 rebalance_account 内部自查(保持旧行为)
        None,
        None,
    )
    .await
}
// get_regime_min_stock removed — was dead code (0 callers). Regime detection is now handled
// inside compute_lw_mvo_weights's regime-adaptive logic directly, using a_monthly trailing returns.
/// 公开版本：不依赖调度器 MvoWeightCache，用于回放等场景按日期独立计算 MVO 权重
#[allow(dead_code)]
pub async fn compute_mvo_weights_for_date(
    db: &PgPool,
    date: NaiveDate,
    sc: &StrategyConfig,
) -> Vec<f64> {
    let cache = tokio::sync::Mutex::new(None::<MvoWeightCache>);
    compute_lw_mvo_weights(db, date, &cache, sc).await
}

#[cfg(test)]
mod stale_factor_recompute_tests {
    use super::*;

    /// 停更因子重算（2026-09-05 数据已补齐后的管道触发）：
    /// 跑全部启用路由覆盖 2026-05-01 以来的因子缺口。
    /// 运行：set -a; source ../.env; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api stale_factor_recompute -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn stale_factor_recompute() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        // HTTP 自调用需要服务端口——直接用容器跑着的 8080
        std::env::set_var("PORT", "8080");
        trigger_v24_backfill_routes(&db, "20260501", "20260905").await;
        println!("[recompute] 全部路由已触发（异步后台执行，等几分钟后查因子新鲜度）");
    }
}

#[cfg(test)]
mod forecast_backfill_byday_tests {
    /// forecast 断档回补(2026-09-15): 8-26 起每日按 ann_date 单次拉取回补至 9-14。
    /// 运行: set -a; source ../.env.quant; set +a;
    ///       cargo test --release -p quant-api forecast_backfill_byday -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn forecast_backfill_byday() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let tok = std::env::var("TUSHARE_TOKEN_ALT").expect("TUSHARE_TOKEN_ALT");
        let cfg = quant_data::tushare::client::TushareConfig {
            token: tok,
            rate_limit_per_minute: 60,
            // 凭证配对: 充值 token 走官方域名(私有直连 IP 只认专属 token)
            base_url: std::env::var("TUSHARE_API_URL_ALT")
                .unwrap_or_else(|_| "http://api.tushare.pro".to_string()),
            fallback_token: None,
            ..Default::default()
        };
        let client = quant_data::tushare::client::TushareClient::new(cfg).expect("client");
        let mut d = chrono::NaiveDate::from_ymd_opt(2026, 8, 26).unwrap();
        let end = chrono::NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        while d <= end {
            let ds = d.format("%Y%m%d").to_string();
            match quant_data::sync::sync_forecast_by_day(
                &db,
                &client,
                &ds,
                &format!("fc-bf-{}", ds),
            )
            .await
            {
                Ok(n) => println!("[fc-bf] {} -> {} 条", ds, n),
                Err(e) => println!("[fc-bf] {} 失败: {}", ds, e),
            }
            d += chrono::Duration::days(1);
        }
        println!("[fc-bf] 回补完成");
    }
}

#[cfg(test)]
mod forecast_daily_tests {
    /// forecast 表补同步（2026-09-05：数据断在 4-29，充值 token 有权限）。
    /// 运行：TUSHARE_TOKEN_ALT=<token> cargo test --release -p quant-api forecast_backfill -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn forecast_backfill_apr_to_now() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let tok = std::env::var("TUSHARE_TOKEN_ALT").expect("TUSHARE_TOKEN_ALT（充值 token）");
        let cfg = quant_data::tushare::client::TushareConfig {
            token: tok,
            rate_limit_per_minute: 240,
            ..Default::default()
        };
        let tushare = quant_data::tushare::client::TushareClient::new(cfg).expect("client");
        let empty: Vec<String> = vec![];
        // 按月分块（forecast 单次上限同量级）
        let windows = [
            ("20260429", "20260531"),
            ("20260601", "20260630"),
            ("20260701", "20260731"),
            ("20260801", "20260831"),
            ("20260901", "20260905"),
        ];
        for (s, e) in windows {
            let n = quant_data::sync::sync_forecast(
                &db,
                &tushare,
                &empty,
                s,
                e,
                &format!("dv-fc-bf-{}", s),
            )
            .await
            .unwrap_or_else(|err| {
                println!("[fc-bf] {}..{} err: {}", s, e, err);
                0
            });
            println!("[fc-bf] {}..{} rows={}", s, e, n);
        }
    }
}

// ══ 第六批覆盖专项(2026-09-22, 调度域) ══
// 靶点: 167091e 新增的 wait_for_factor_backfill 轮询终态判定 + WFA 参数提取链
// (try_extract_wfa_params 全分支 / check_and_trigger_wfa 已有参数早退) + ML 训练
// 依赖检查(verify_training_dependencies) + compute_mvo_weights_for_date(fixed
// 模式纯计算路径)。不直调: run_tick / run_scheduled_tasks / trigger_v24_backfill_routes
// / validate_pre_trade_data / ensure_prediction_coverage / rebuild_full_universe_
// prediction_set——涉及 HTTP 自调用(8080 是生产容器)、真实 Tushare 同步、或以
// 生产 ID 写 prediction_set(详见批次报告)。
#[cfg(test)]
mod sixth_batch {
    use super::*;

    /// wait 三连测互斥锁：wait_for_factor_backfill 的候选查询扫全库 running/pending
    /// backfill 行，并行时 zero_timeout 造的未终态行会被邻居测试捞到，导致"全终态
    /// 立即返回"断言失败（实测 30.005s=一轮 sleep）——三测试必须串行
    /// （CLEANUP_TEST_LOCK 同款模式，任务79 修）。
    static WAIT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn test_db() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        PgPool::connect(&url).await.expect("test db connect")
    }

    /// 测试用 StrategyConfig 字面量(与 tests 模块同款, 避免触发 panic 版 Default)。
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

    #[test]
    fn self_api_base_always_points_to_localhost() {
        // 不读写 PORT 环境变量(进程级全局状态, 并行测试互扰), 只断言形态:
        // scheduler 内部自调用必须始终落在 localhost, 防止误改为外部地址。
        let base = self_api_base();
        assert!(base.starts_with("http://localhost:"), "形态异常: {base}");
        let port: u16 = base["http://localhost:".len()..]
            .parse()
            .expect("端口部分必须是数字");
        assert!(port > 0);
    }

    // ── wait_for_factor_backfill(任务71 新增): 轮询终态判定 ──
    // since 与 zzz 行 created_at 全部取未来时刻, 生产表不可能存在未来行 →
    // 计数结果完全由 zzz_test 行决定, 测试不依赖生产 data_sync_task 当时状态。

    const DST_PREFIX: &str = "zzz_test_sixth_dst";

    async fn cleanup_dst_rows(db: &PgPool) {
        let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id LIKE $1")
            .bind(format!("{}%", DST_PREFIX))
            .execute(db)
            .await;
    }

    async fn insert_dst_row(
        db: &PgPool,
        suffix: &str,
        task_type: &str,
        status: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query(
            "INSERT INTO data_sync_task (task_id, task_type, source, status, created_at)
             VALUES ($1, $2, 'tushare', $3, $4)
             ON CONFLICT (task_id) DO UPDATE
               SET status = EXCLUDED.status, created_at = EXCLUDED.created_at,
                   task_type = EXCLUDED.task_type",
        )
        .bind(format!("{}_{}", DST_PREFIX, suffix))
        .bind(task_type)
        .bind(status)
        .bind(created_at)
        .execute(db)
        .await
        .expect("insert zzz_test data_sync_task");
    }

    /// 全部终态(completed/failed/partial/timeout/cancelled)不计入轮询;
    /// 非 backfill 类型的 pending 行也不计入 → 应零等待立即返回。
    #[tokio::test]
    async fn wait_for_factor_backfill_returns_at_once_when_all_terminal() {
        let _wait_guard = WAIT_TEST_LOCK.lock().await;
        let db = test_db().await;
        cleanup_dst_rows(&db).await;
        let since = chrono::Utc::now() + chrono::Duration::minutes(60);
        let at = since + chrono::Duration::minutes(10);
        for (suffix, status) in [
            ("completed", "completed"),
            ("failed", "failed"),
            ("partial", "partial"),
            ("timeout", "timeout"),
            ("cancelled", "cancelled"),
        ] {
            insert_dst_row(&db, suffix, "zzz_test_sixth_fc_backfill", status, at).await;
        }
        // 非 backfill 的未终态行: task_type 不含 backfill 子串, 必须被过滤
        insert_dst_row(&db, "eod_pending", "zzz_test_sixth_eod_sync", "pending", at).await;

        let t0 = std::time::Instant::now();
        wait_for_factor_backfill(&db, since, std::time::Duration::from_secs(60)).await;
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "全终态应立即返回, 实际耗时 {:?}",
            t0.elapsed()
        );
        cleanup_dst_rows(&db).await;
    }

    /// pending/running/cancel_requested 三种未终态均计入; timeout=0 时首轮命中
    /// deadline 分支立即放行(不 sleep 30s)——超时后继续后续步骤由物化门禁兜底。
    #[tokio::test]
    async fn wait_for_factor_backfill_zero_timeout_bails_out_with_pending_rows() {
        let _wait_guard = WAIT_TEST_LOCK.lock().await;
        let db = test_db().await;
        cleanup_dst_rows(&db).await;
        let since = chrono::Utc::now() + chrono::Duration::minutes(60);
        let at = since + chrono::Duration::minutes(10);
        for (suffix, status) in [
            ("pending", "pending"),
            ("running", "running"),
            ("cancel_requested", "cancel_requested"),
        ] {
            insert_dst_row(&db, suffix, "zzz_test_sixth_fc_backfill", status, at).await;
        }

        let t0 = std::time::Instant::now();
        wait_for_factor_backfill(&db, since, std::time::Duration::ZERO).await;
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "零超时应经 deadline 分支立即返回(若误入 30s sleep 说明 deadline 判定回归), 实际 {:?}",
            t0.elapsed()
        );
        cleanup_dst_rows(&db).await;
    }

    /// created_at 早于 since 的未终态回填行不计入(只等 since 之后创建的任务)。
    #[tokio::test]
    async fn wait_for_factor_backfill_ignores_rows_created_before_since() {
        let _wait_guard = WAIT_TEST_LOCK.lock().await;
        let db = test_db().await;
        cleanup_dst_rows(&db).await;
        let since = chrono::Utc::now() + chrono::Duration::minutes(60);
        let before = since - chrono::Duration::minutes(10);
        insert_dst_row(
            &db,
            "stale_pending",
            "zzz_test_sixth_fc_backfill",
            "pending",
            before,
        )
        .await;

        let t0 = std::time::Instant::now();
        wait_for_factor_backfill(&db, since, std::time::Duration::from_secs(60)).await;
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "since 之前的行应被过滤, 立即返回, 实际 {:?}",
            t0.elapsed()
        );
        cleanup_dst_rows(&db).await;
    }

    // ── WFA 参数提取链: try_extract_wfa_params / check_and_trigger_wfa ──

    const WFA_EXP_ID: &str = "zzz_test_sixth_wfa_exp";

    async fn cleanup_wfa_fixtures(db: &PgPool) {
        // 顺序: 参数行 → 任务行(CASCADE 级联删 trial) → 实验行
        let _ = sqlx::query("DELETE FROM wfa_strategy_params WHERE experiment_run_id = $1")
            .bind(WFA_EXP_ID)
            .execute(db)
            .await;
        let _ = sqlx::query(
            "DELETE FROM optimization_task WHERE optimization_task_id LIKE 'zzz_test_sixth_opt%'",
        )
        .execute(db)
        .await;
        let _ = sqlx::query("DELETE FROM experiment_run WHERE experiment_run_id = $1")
            .bind(WFA_EXP_ID)
            .execute(db)
            .await;
    }

    /// 从最近 completed 实验提取最优 trial 参数: 仅带完整 test_start/test_end
    /// 的窗口入库; 每窗口取分最高 trial; 缺日期/无 trial 的窗口跳过; 二次调用
    /// 幂等(already>0 早退)。
    #[tokio::test]
    async fn try_extract_wfa_params_from_completed_experiment_best_trial_only() {
        let db = test_db().await;
        cleanup_wfa_fixtures(&db).await;

        // FK 引用真实存在的策略/数据版本行(仅引用, 不写这两张表)
        let sv: String =
            sqlx::query_scalar("SELECT strategy_version_id FROM strategy_version LIMIT 1")
                .fetch_one(&db)
                .await
                .expect("strategy_version 应有数据行供 FK 引用");
        let dv: String = sqlx::query_scalar("SELECT data_version_id FROM data_version LIMIT 1")
            .fetch_one(&db)
            .await
            .expect("data_version 应有数据行供 FK 引用");

        // 最新 completed 实验: completed_at 置未来 2 分钟, 压过生产任何已完成实验
        sqlx::query(
            "INSERT INTO experiment_run (experiment_run_id, experiment_type, config, status, completed_at)
             VALUES ($1, 'phase7_oos_walk_forward_discovery', '{}', 'completed', now() + interval '2 minutes')",
        )
        .bind(WFA_EXP_ID)
        .execute(&db)
        .await
        .expect("insert zzz_test experiment_run");

        // 窗口1: 完整日期 + 两个 completed trial(分低/分高) → 入库分高者
        sqlx::query(
            "INSERT INTO optimization_task (optimization_task_id, strategy_version_id,
                 data_version_id, search_method, search_space, objective,
                 walk_forward_config, status)
             VALUES ($1, $2, $3, 'grid_search', '{}'::jsonb, '{}'::jsonb, $4::jsonb, 'completed')",
        )
        .bind("zzz_test_sixth_opt_w1")
        .bind(&sv)
        .bind(&dv)
        .bind(serde_json::json!({
            "experiment_run_id": WFA_EXP_ID,
            "window_index": 1,
            "test_start": "2026-02-10",
            "test_end": "2026-02-28"
        }))
        .execute(&db)
        .await
        .expect("insert optimization_task w1");
        for (trial_id, idx, combo, score) in [
            ("zzz_test_sixth_trial_w1_lo", 0i32, "zzz_test_worst", 1.1f64),
            ("zzz_test_sixth_trial_w1_hi", 1, "zzz_test_best", 2.5),
        ] {
            sqlx::query(
                "INSERT INTO optimization_trial (trial_id, optimization_task_id, trial_index,
                     parameters, score, status)
                 VALUES ($1, 'zzz_test_sixth_opt_w1', $2, $3::jsonb, $4, 'completed')",
            )
            .bind(trial_id)
            .bind(idx)
            .bind(serde_json::json!({ "combo_name": combo, "top_n": 9 }))
            .bind(rust_decimal::Decimal::from_f64_retain(score))
            .execute(&db)
            .await
            .expect("insert optimization_trial");
        }

        // 窗口2: walk_forward_config 缺 test_start/test_end → 有 best trial 也不入库
        sqlx::query(
            "INSERT INTO optimization_task (optimization_task_id, strategy_version_id,
                 data_version_id, search_method, search_space, objective,
                 walk_forward_config, status)
             VALUES ($1, $2, $3, 'grid_search', '{}'::jsonb, '{}'::jsonb, $4::jsonb, 'completed')",
        )
        .bind("zzz_test_sixth_opt_w2")
        .bind(&sv)
        .bind(&dv)
        .bind(serde_json::json!({
            "experiment_run_id": WFA_EXP_ID,
            "window_index": 2
        }))
        .execute(&db)
        .await
        .expect("insert optimization_task w2");
        sqlx::query(
            "INSERT INTO optimization_trial (trial_id, optimization_task_id, trial_index,
                 parameters, score, status)
             VALUES ($1, 'zzz_test_sixth_opt_w2', 0, $2::jsonb, 3.5, 'completed')",
        )
        .bind("zzz_test_sixth_trial_w2")
        .bind(serde_json::json!({ "combo_name": "zzz_test_no_dates" }))
        .execute(&db)
        .await
        .expect("insert optimization_trial w2");

        // 窗口3: 无任何 trial → 跳过
        sqlx::query(
            "INSERT INTO optimization_task (optimization_task_id, strategy_version_id,
                 data_version_id, search_method, search_space, objective,
                 walk_forward_config, status)
             VALUES ($1, $2, $3, 'grid_search', '{}'::jsonb, '{}'::jsonb, $4::jsonb, 'completed')",
        )
        .bind("zzz_test_sixth_opt_w3")
        .bind(&sv)
        .bind(&dv)
        .bind(serde_json::json!({
            "experiment_run_id": WFA_EXP_ID,
            "window_index": 3,
            "test_start": "2026-03-10",
            "test_end": "2026-03-20"
        }))
        .execute(&db)
        .await
        .expect("insert optimization_task w3");

        // 首次提取: 仅窗口1 入库, 参数取分高 trial
        try_extract_wfa_params(&db).await.expect("首次提取应成功");
        let rows: Vec<(i32, NaiveDate, NaiveDate, serde_json::Value, Option<f64>)> =
            sqlx::query_as(
                "SELECT window_index, test_start, test_end, parameters, score
                 FROM wfa_strategy_params WHERE experiment_run_id = $1 ORDER BY window_index",
            )
            .bind(WFA_EXP_ID)
            .fetch_all(&db)
            .await
            .expect("查询提取结果");
        assert_eq!(
            rows.len(),
            1,
            "仅窗口1入库(窗口2缺日期/窗口3无trial): {rows:?}"
        );
        assert_eq!(rows[0].0, 1, "window_index: {rows:?}");
        assert_eq!(rows[0].1, NaiveDate::from_ymd_opt(2026, 2, 10).unwrap());
        assert_eq!(rows[0].2, NaiveDate::from_ymd_opt(2026, 2, 28).unwrap());
        assert_eq!(
            rows[0].3["combo_name"], "zzz_test_best",
            "应取分最高 trial: {rows:?}"
        );
        assert!(
            (rows[0].4.expect("score 应非空") - 2.5).abs() < 1e-9,
            "score 应为最高分 trial: {rows:?}"
        );

        // 二次提取: already>0 幂等早退, 行数不变
        try_extract_wfa_params(&db)
            .await
            .expect("二次提取应成功(幂等)");
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM wfa_strategy_params WHERE experiment_run_id = $1",
        )
        .bind(WFA_EXP_ID)
        .fetch_one(&db)
        .await
        .expect("计数提取结果");
        assert_eq!(n, 1, "幂等: 二次提取不应新增行");

        cleanup_wfa_fixtures(&db).await;
    }

    /// 当季已有覆盖日期的 WFA 参数 → 直接跳过(不查实验/不发 HTTP)。
    #[tokio::test]
    async fn check_and_trigger_wfa_skips_when_quarter_params_already_exist() {
        let db = test_db().await;
        // 生产 wfa_strategy_params 覆盖 2019-01-31~2026-01-28, 取窗口内日期验证早退
        let inside = NaiveDate::from_ymd_opt(2025, 6, 30).unwrap();
        let r = check_and_trigger_wfa(&db, 65535, "2025-Q3", inside).await;
        assert_eq!(r, Ok(None), "已有该季度参数应跳过: {r:?}");
    }

    /// ML 训练依赖检查: 新鲜数据全通过 / 未知 combo / 远未来日期三态。
    #[tokio::test]
    async fn verify_training_dependencies_fresh_missing_combo_and_far_future() {
        let db = test_db().await;
        // 正例日期动态取该 combo PIT 口径最新截面日(数据增长不失效)
        let mfv_max: Option<NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(trade_date) FROM multi_factor_value
             WHERE combo_name = 'full_pit_icir_indneutral_val_v1'
               AND COALESCE(available_at, trade_date) <= trade_date",
        )
        .fetch_one(&db)
        .await
        .ok()
        .flatten();
        let date = mfv_max.expect("v24 combo 应有物化数据");
        assert!(
            verify_training_dependencies(&db, date, "full_pit_icir_indneutral_val_v1").await,
            "日线/复权/因子三依赖均新鲜时应通过(date={date})"
        );
        // 反例1: 未知 combo → factor_ok=false
        assert!(
            !verify_training_dependencies(&db, date, "zzz_test_no_such_combo").await,
            "未知 combo 应判依赖缺失"
        );
        // 反例2: 远未来日期 → 日线/因子窗口全空, 复权因子超 60 天
        assert!(
            !verify_training_dependencies(
                &db,
                NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
                "full_pit_icir_indneutral_val_v1"
            )
            .await,
            "远未来日期应判依赖缺失"
        );
    }

    /// compute_mvo_weights_for_date: fixed 分配模式绕过 MVO 管线,
    /// default_weights 按比例归一原样返回(该路径不触 DB 计算查询)。
    #[tokio::test]
    async fn compute_mvo_weights_for_date_fixed_mode_returns_configured_weights() {
        let db = test_db().await;
        let sc = StrategyConfig {
            allocation_mode: Some("fixed".into()),
            etf_symbols: vec!["518880.SH".into(), "511010.SH".into(), "513100.SH".into()],
            default_weights: vec![0.30, 0.20, 0.20, 0.30],
            ..sc_literal()
        };
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
        let weights = compute_mvo_weights_for_date(&db, date, &sc).await;
        assert_eq!(weights.len(), 4, "A股 + 3 ETF: {weights:?}");
        for (got, want) in weights.iter().zip([0.30, 0.20, 0.20, 0.30]) {
            assert!(
                (got - want).abs() < 1e-9,
                "fixed 模式权重应按配置比例返回: {weights:?}"
            );
        }
        assert!(
            (weights.iter().sum::<f64>() - 1.0).abs() < 1e-9,
            "权重和应归一为 1: {weights:?}"
        );
    }
}

// ══ 第十二批覆盖专项(2026-09-23, 调度分派臂) ══
// 靶点: 任务79 从 run_scheduled_tasks 拆出的 11 个 dispatch_* 分派臂的可测分支。
// 直调的臂(经本地 HTTP 捕获服务 + PORT 环境变量注入, 详见 spawn_capture_server):
//   dispatch_rolling_pit_eval / dispatch_market_level_source_freshness
//   / dispatch_factor_backfill(载荷层)
// 参数组装链测输入侧(不直调, 理由见各测试): dispatch_pit_combo_refresh
//   / dispatch_mvo_equity_curve_update。
// 明确跳过直调的臂(与 60 个 ignored 工具型同类, 见批次报告):
//   dispatch_data_quality_check(真实全量质量检查, 滞缓时会真发钉钉告警)
//   / dispatch_equity_curve_update(spawn 真实同步全部活跃策略权益曲线, 写表)
//   / dispatch_nightly_signal_prep(spawn 整条夜间链: 回填+物化+Tushare 同步)
//   / dispatch_ptrade_signal_export(spawn 真实 Tushare fund_nav/div + 实盘信号导出)
//   / dispatch_ptrade_report_fetch(spawn IMAP 拉邮件 + 钉钉日报)
//   / dispatch_native_pv_increment(spawn 真实因子增量计算, 写 factor_value)
//   ——这六个壳的函数体是纯 spawn 转发, 无自有分支, 由第 15 号哨兵测试
//   scheduled_task_registered_types_all_have_dispatch_arms 从调度入口侧兜底。
#[cfg(test)]
mod twelfth_batch {
    use super::*;

    // ── 基建: PORT 环境变量互斥 ──
    // self_api_base() 读进程级 PORT 环境变量, 注入期间必须串行; 退出前恢复原值。
    // sixth_batch::self_api_base_always_points_to_localhost 只断言形态(localhost:数字),
    // 不依赖具体端口, 注入窗口对其无影响; 其余测试不读 PORT。
    static PORT_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// PORT 注入守卫: Drop 时恢复原值(原值缺省则移除), 防止污染并行测试。
    struct PortGuard(Option<String>);

    impl PortGuard {
        fn set(value: &str) -> Self {
            let old = std::env::var("PORT").ok();
            std::env::set_var("PORT", value);
            PortGuard(old)
        }
    }

    impl Drop for PortGuard {
        fn drop(&mut self) {
            match self.0.take() {
                Some(v) => std::env::set_var("PORT", v),
                None => std::env::remove_var("PORT"),
            }
        }
    }

    // ── 基建: 本地 HTTP 捕获服务 ──
    // 绑 127.0.0.1 随机端口, 记录收到的 POST(path + JSON body) 并立即回 200,
    // 让 dispatch_* 的内部 HTTP 自调用落到测试沙箱而非生产 8080, 且可精确断言载荷。
    // 支持同连接 keep-alive 多请求(hyper 连接池会复用)。
    #[derive(Debug, Clone)]
    struct CapturedPost {
        path: String,
        body: serde_json::Value,
    }

    struct CaptureServer {
        port: u16,
        posts: Arc<std::sync::Mutex<Vec<CapturedPost>>>,
        _task: tokio::task::JoinHandle<()>,
    }

    impl Drop for CaptureServer {
        fn drop(&mut self) {
            self._task.abort();
        }
    }

    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    async fn spawn_capture_server() -> CaptureServer {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 随机端口");
        let port = listener.local_addr().expect("local addr").port();
        let posts: Arc<std::sync::Mutex<Vec<CapturedPost>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let shared = posts.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    continue;
                };
                let shared = shared.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        // 持续读直到缓冲内出现一个完整请求(头 + Content-Length 字节体)
                        loop {
                            if let Some(header_end) = find_subsequence(&buf, b"\r\n\r\n") {
                                let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                                let content_length = head
                                    .lines()
                                    .find_map(|l| {
                                        let (k, v) = l.split_once(':')?;
                                        if k.eq_ignore_ascii_case("content-length") {
                                            v.trim().parse::<usize>().ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(0);
                                if buf.len() >= header_end + 4 + content_length {
                                    let body = buf[header_end + 4..header_end + 4 + content_length]
                                        .to_vec();
                                    let path = head
                                        .lines()
                                        .next()
                                        .and_then(|req_line| req_line.split_whitespace().nth(1))
                                        .unwrap_or("")
                                        .to_string();
                                    let body_json: serde_json::Value =
                                        serde_json::from_slice(&body)
                                            .unwrap_or(serde_json::Value::Null);
                                    shared.lock().unwrap().push(CapturedPost {
                                        path,
                                        body: body_json,
                                    });
                                    let resp: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 9\r\n\r\n{\"ok\":true}";
                                    let _ = sock.write_all(resp).await;
                                    // 消费已处理请求, 继续处理同连接的下一个(keep-alive)
                                    buf.drain(..header_end + 4 + content_length);
                                    break;
                                }
                            }
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                    }
                });
            }
        });
        CaptureServer {
            port,
            posts,
            _task: task,
        }
    }

    async fn test_db() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        PgPool::connect(&url).await.expect("test db connect")
    }

    // ── zzz 造数: strategy_config 独占键(无 FK, strategy_id 即主键) ──
    // 造数三测共用 zzz_test_sch12% 前缀清理, 并行互跑会互删对方的行——
    // 与 sixth_batch::WAIT_TEST_LOCK 同款互斥, 三测试必须串行。
    static SCH12_ZZZ_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn cleanup_sch12_strategy_rows(db: &PgPool) {
        let _ = sqlx::query("DELETE FROM strategy_config WHERE strategy_id LIKE $1")
            .bind("zzz_test_sch12%")
            .execute(db)
            .await;
    }

    async fn insert_sch12_strategy(
        db: &PgPool,
        strategy_id: &str,
        combo_name: &str,
        combo_horizon: Option<i16>,
        include_fundamentals: bool,
        whitelist: Option<&serde_json::Value>,
    ) {
        sqlx::query(
            "INSERT INTO strategy_config (strategy_id, name, combo_name, combo_horizon,
                 include_fundamentals, factor_whitelist)
             VALUES ($1, $1, $2, $3, $4, $5)",
        )
        .bind(strategy_id)
        .bind(combo_name)
        .bind(combo_horizon)
        .bind(include_fundamentals)
        .bind(whitelist)
        .execute(db)
        .await
        .expect("insert zzz_test_sch12 strategy_config");
    }

    // ── dispatch_rolling_pit_eval: lookback 默认/覆盖 + horizon 双提交载荷 ──

    /// 默认 params: 向 evaluate-rolling-pit/background 提交 horizon 20/60 两条,
    /// 区间 = [end-370, end](lookback_days 缺省 370)。
    #[tokio::test]
    async fn rolling_pit_eval_default_posts_dual_horizon_370d_window() {
        let _lock = PORT_ENV_LOCK.lock().await;
        let d0 = chrono::Utc::now().date_naive();
        let server = spawn_capture_server().await;
        let _port = PortGuard::set(&server.port.to_string());
        dispatch_rolling_pit_eval(&serde_json::json!({})).await;
        let d1 = chrono::Utc::now().date_naive();

        let posts = server.posts.lock().unwrap();
        assert_eq!(posts.len(), 2, "horizon 20/60 各提交一次: {posts:?}");
        let mut horizons: Vec<i64> = posts
            .iter()
            .map(|p| p.body["horizon"].as_i64().unwrap_or(-1))
            .collect();
        horizons.sort_unstable();
        assert_eq!(horizons, vec![20, 60], "双 horizon 载荷: {posts:?}");
        for p in posts.iter() {
            assert_eq!(
                p.path, "/api/v1/quant/factors/evaluate-rolling-pit/background",
                "端点路径: {p:?}"
            );
            let end = NaiveDate::parse_from_str(
                p.body["end_date"].as_str().unwrap_or_default(),
                "%Y%m%d",
            )
            .expect("end_date 应可解析");
            let start = NaiveDate::parse_from_str(
                p.body["start_date"].as_str().unwrap_or_default(),
                "%Y%m%d",
            )
            .expect("start_date 应可解析");
            // dispatch 内取 now 与测试取 now 可能跨午夜, 允许 {d0, d1} 两个值
            assert!(
                end == d0 || end == d1,
                "end_date 应为执行日(跨午夜容差): end={end} d0={d0} d1={d1}"
            );
            assert_eq!(
                start,
                end - chrono::Duration::days(370),
                "默认 lookback=370 天: {p:?}"
            );
        }
    }

    /// params.lookback_days=30: 窗口收窄为 [end-30, end], 覆盖默认值。
    #[tokio::test]
    async fn rolling_pit_eval_lookback_days_param_narrows_window() {
        let _lock = PORT_ENV_LOCK.lock().await;
        let server = spawn_capture_server().await;
        let _port = PortGuard::set(&server.port.to_string());
        dispatch_rolling_pit_eval(&serde_json::json!({ "lookback_days": 30 })).await;

        let posts = server.posts.lock().unwrap();
        assert_eq!(posts.len(), 2, "双 horizon 各一条: {posts:?}");
        for p in posts.iter() {
            let end = NaiveDate::parse_from_str(
                p.body["end_date"].as_str().unwrap_or_default(),
                "%Y%m%d",
            )
            .expect("end_date");
            let start = NaiveDate::parse_from_str(
                p.body["start_date"].as_str().unwrap_or_default(),
                "%Y%m%d",
            )
            .expect("start_date");
            assert_eq!(
                start,
                end - chrono::Duration::days(30),
                "lookback_days=30 应覆盖默认 370: {p:?}"
            );
        }
    }

    // ── dispatch_market_level_source_freshness: 源过滤 + 增量载荷 ──

    /// 不认识的 source: payload 构造返回 None → warn 后 continue, 不发任何 POST
    /// (必须注入沙箱端口验证——若该分支回归, 请求会打到生产 8080)。
    #[tokio::test]
    async fn market_level_freshness_unknown_source_posts_nothing() {
        let _lock = PORT_ENV_LOCK.lock().await;
        let server = spawn_capture_server().await;
        let _port = PortGuard::set(&server.port.to_string());
        let db = test_db().await;
        dispatch_market_level_source_freshness(
            &db,
            &serde_json::json!({ "sources": ["zzz_test_sch12_unknown_source"] }),
        )
        .await;
        let posts = server.posts.lock().unwrap();
        assert!(posts.is_empty(), "未知源应跳过不发同步任务: {posts:?}");
    }

    /// 默认两源(margin/hsgt): 各 POST 一条 sync-tasks 增量任务, start = min(表内
    /// 最新交易日+1, today), data_version_id 按 dv-p315-{slug}-{yyyymmdd} 生成。
    #[tokio::test]
    async fn market_level_freshness_known_sources_post_increment_windows() {
        let _lock = PORT_ENV_LOCK.lock().await;
        let db = test_db().await;
        let margin_max: Option<NaiveDate> =
            sqlx::query_scalar("SELECT MAX(trade_date) FROM market_margin")
                .fetch_one(&db)
                .await
                .ok()
                .flatten();
        let hsgt_max: Option<NaiveDate> =
            sqlx::query_scalar("SELECT MAX(trade_date) FROM market_moneyflow_hsgt")
                .fetch_one(&db)
                .await
                .ok()
                .flatten();

        let server = spawn_capture_server().await;
        let _port = PortGuard::set(&server.port.to_string());
        dispatch_market_level_source_freshness(&db, &serde_json::json!({})).await;

        let posts = server.posts.lock().unwrap();
        assert_eq!(posts.len(), 2, "默认两源各一条: {posts:?}");
        for p in posts.iter() {
            assert_eq!(p.path, "/api/v1/quant/data/sync-tasks", "端点路径: {p:?}");
        }
        for (p, dataset, slug, latest) in [
            (&posts[0], "margin", "margin", margin_max),
            // dataset 用完整 Tushare 接口名（moneyflow_hsgt），slug 仅用于 dv 标识
            (&posts[1], "moneyflow_hsgt", "hsgt", hsgt_max),
        ] {
            assert_eq!(p.body["dataset"], dataset, "数据集映射: {p:?}");
            assert_eq!(p.body["source"], "tushare", "同步源: {p:?}");
            assert_eq!(p.body["background"], true, "后台执行标志: {p:?}");
            assert_eq!(
                p.body["reason"], "scheduled_p315_market_level_regime_source_freshness",
                "触发原因标记: {p:?}"
            );
            let end_str = p.body["end_date"].as_str().unwrap_or_default().to_string();
            let end = NaiveDate::parse_from_str(&end_str, "%Y%m%d").expect("end_date");
            assert_eq!(
                p.body["data_version_id"],
                serde_json::json!(format!("dv-p315-{slug}-{end_str}")),
                "数据版本 id 格式: {p:?}"
            );
            let expected_start = latest
                .map(|m| (m + chrono::Duration::days(1)).min(end))
                .unwrap_or(end);
            assert_eq!(
                p.body["start_date"],
                serde_json::json!(expected_start.format("%Y%m%d").to_string()),
                "start = min(源表最新+1, today): latest={latest:?} {p:?}"
            );
        }
    }

    /// payload 纯函数 Some(latest) 分支: start = min(latest+1, today) —— 昨天→今天
    /// (clamp 截断), 40 天前→39 天前(+1 步进), 当天→今天(不越过 today)。
    #[test]
    fn market_level_freshness_payload_clamps_and_increments_start() {
        let today = NaiveDate::from_ymd_opt(2026, 6, 20).unwrap();
        let d = |n: i64| today - chrono::Duration::days(n);

        let p = market_level_freshness_sync_payload("market_margin_regime", Some(d(1)), today)
            .expect("payload");
        assert_eq!(p["start_date"], "20260620", "昨天的数据→从今天起拉");

        let p =
            market_level_freshness_sync_payload("market_moneyflow_hsgt_regime", Some(d(40)), today)
                .expect("payload");
        assert_eq!(
            p["start_date"], "20260512",
            "40 天前→+1 天步进(2026-05-11+1)"
        );

        let p = market_level_freshness_sync_payload("market_margin_regime", Some(today), today)
            .expect("payload");
        assert_eq!(
            p["start_date"], "20260620",
            "latest==today 时 min 截断, 不越界到明天"
        );
    }

    /// payload 纯函数顶层守卫: 未知 source → None(dispatch 的 continue 分支判定)。
    #[test]
    fn market_level_freshness_payload_rejects_unknown_source() {
        let today = NaiveDate::from_ymd_opt(2026, 6, 20).unwrap();
        assert!(market_level_freshness_sync_payload(
            "zzz_test_sch12_unknown_source",
            Some(today),
            today
        )
        .is_none());
        assert!(
            market_level_freshness_sync_payload("zzz_test_sch12_unknown_source", None, today)
                .is_none()
        );
    }

    // ── dispatch_factor_backfill: 14 天窗口 + DB 驱动路由 ──

    /// 正常分支(日历有数据): 向 factor_backfill_route 表的全部启用路由 POST,
    /// 窗口 = [最新开市日-14 天, 最新开市日](BACKFILL_WINDOW_DAYS 锚定为 14)。
    /// 经捕获服务拦截, 不触达生产 8080 的真实回填。
    #[tokio::test]
    async fn factor_backfill_posts_14d_window_to_enabled_db_routes() {
        assert_eq!(
            BACKFILL_WINDOW_DAYS.num_days(),
            14,
            "BACKFILL_WINDOW_DAYS 值锚定(改动需同步断言)"
        );
        let _lock = PORT_ENV_LOCK.lock().await;
        let db = test_db().await;
        let enabled_routes: Vec<String> = sqlx::query_scalar(
            "SELECT route_name FROM factor_backfill_route
             WHERE enabled = true ORDER BY priority ASC, route_name ASC",
        )
        .fetch_all(&db)
        .await
        .expect("读取启用回填路由");
        assert!(
            !enabled_routes.is_empty(),
            "生产 factor_backfill_route 应有启用路由(P4-1)"
        );

        let server = spawn_capture_server().await;
        let _port = PortGuard::set(&server.port.to_string());
        dispatch_factor_backfill(&db).await;

        // 锁内提取 owned 数据后立即释放（clippy await_holding_lock：std MutexGuard
        // 不得跨 await——日历对照查询挪到锁作用域外）
        let posts: Vec<_> = server.posts.lock().unwrap().clone();
        assert_eq!(
            posts.len(),
            enabled_routes.len(),
            "每条启用路由各一次 POST: {posts:?}"
        );
        let mut got_paths: Vec<&str> = posts.iter().map(|p| p.path.as_str()).collect();
        got_paths.sort_unstable();
        let mut want_paths: Vec<String> = enabled_routes
            .iter()
            .map(|r| format!("/api/v1/quant/factors/{r}/background"))
            .collect();
        want_paths.sort_unstable();
        assert_eq!(
            got_paths, want_paths,
            "路由清单应由 factor_backfill_route 表驱动(P4-1)"
        );

        let mut end_dates: Vec<&str> = posts
            .iter()
            .map(|p| p.body["end_date"].as_str().unwrap_or_default())
            .collect();
        end_dates.dedup();
        assert_eq!(end_dates.len(), 1, "全部路由共用同一窗口终点");
        let end = NaiveDate::parse_from_str(end_dates[0], "%Y%m%d").expect("end_date 应可解析");
        // 自洽断言(免跨午夜 flaky): end 即『<=end 的最新开市日』
        let sync_date: Option<(NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE is_open = true AND trade_date <= $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(end)
        .fetch_optional(&db)
        .await
        .expect("日历查询");
        assert_eq!(sync_date.map(|(d,)| d), Some(end), "end 应为最新开市日");
        for p in posts.iter() {
            let start = NaiveDate::parse_from_str(
                p.body["start_date"].as_str().unwrap_or_default(),
                "%Y%m%d",
            )
            .expect("start_date");
            assert_eq!(
                start,
                end - chrono::Duration::days(14),
                "backfill_start = 最新开市日 - 14 天: {p:?}"
            );
        }
    }

    /// 缺失分支的判定输入: dispatch 同款日历查询在『早于首行(1990-10-12)』的窗口
    /// 返回 None(→ warn 跳过); 正常窗口返回 Some 作对照。
    /// 不直调 dispatch 的理由: 生产日历不可清空, 而正常路径会真触发回填。
    #[tokio::test]
    async fn factor_backfill_empty_calendar_window_yields_no_sync_date() {
        let db = test_db().await;
        let empty: Option<(NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE is_open = true AND trade_date <= $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())
        .fetch_optional(&db)
        .await
        .expect("日历查询");
        assert!(empty.is_none(), "空窗口应查无开市日(缺失分支判定)");
        let normal: Option<(NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE is_open = true AND trade_date <= $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(chrono::Utc::now().date_naive())
        .fetch_optional(&db)
        .await
        .expect("日历查询");
        assert!(normal.is_some(), "当前窗口应查得到开市日(对照)");
    }

    /// trigger_v24_backfill_routes 容错链: API 端口不可达时逐路由 warn,
    /// 全部 12 条 refused 也不挂起(单路由失败不中断后续)。
    #[tokio::test]
    async fn backfill_routes_survive_unreachable_api_port() {
        let _lock = PORT_ENV_LOCK.lock().await;
        let _port = PortGuard::set("1");
        let db = test_db().await;
        let t0 = std::time::Instant::now();
        trigger_v24_backfill_routes(&db, "20260101", "20260102").await;
        // 12 条路由 × 双栈(localhost→::1+127.0.0.1)逐次 refused ≈1.3s/条,放宽到 20s
        // 上限防环境差异;真正要防的是无超时挂起(单路由 10s 总超时已兜底)。
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(20),
            "连接拒绝应快速失败不挂起, 实际 {:?}",
            t0.elapsed()
        );
    }

    /// fallback 常量锚定: DB 路由表异常时的最后防线, 至少 12 类因子、
    /// 路由段唯一且符合 URL 命名约定(无空格, -backfill 结尾)。
    #[test]
    fn backfill_routes_fallback_constant_shape() {
        assert!(
            V24_BACKFILL_ROUTES_FALLBACK.len() >= 12,
            "fallback 至少覆盖 12 类因子"
        );
        let routes: Vec<&str> = V24_BACKFILL_ROUTES_FALLBACK
            .iter()
            .map(|(r, _)| *r)
            .collect();
        let unique: std::collections::BTreeSet<&str> = routes.iter().copied().collect();
        assert_eq!(routes.len(), unique.len(), "路由段不得重复");
        for (route, label) in V24_BACKFILL_ROUTES_FALLBACK {
            assert!(!route.contains(' '), "路由段不得含空格: {route}");
            assert!(
                route.ends_with("-backfill"),
                "命名约定 -backfill 结尾: {route}"
            );
            assert!(!label.is_empty(), "label 不得为空: {route}");
        }
        assert!(
            routes.contains(&"phase7-price-volume-backfill"),
            "量价路由(最基础)不得被误删"
        );
    }

    // ── dispatch_pit_combo_refresh: 参数组装链(不直调, 见各测试说明) ──

    /// full_pit_icir 前缀过滤的非空性锚点(空集告警分支的反向回归): 生产 active
    /// combo 中恒有 full_pit_icir* → 保鲜循环不会空转; 非 full_pit 前缀(含 zzz
    /// 造的 phase7_* 行)必须被排除。2026-09-20 曾因 or_insert_with 漏赋
    /// combo_name 导致该过滤恒空集、保鲜静默空转近两个月——此测试防复发。
    /// 不直调 dispatch 的理由: 正常分支会真实物化生产 combo(写 multi_factor_value)。
    #[tokio::test]
    async fn pit_combo_refresh_filter_keeps_full_pit_prefix_combos_only() {
        let _zzz_guard = SCH12_ZZZ_LOCK.lock().await;
        let db = test_db().await;
        cleanup_sch12_strategy_rows(&db).await;
        insert_sch12_strategy(
            &db,
            "zzz_test_sch12_other",
            "phase7_zzz_test_sch12_v1",
            None,
            false,
            None,
        )
        .await;

        let configs = load_active_combo_materialize_configs(&db).await;
        let filtered: Vec<&str> = configs
            .iter()
            .filter(|c| c.combo_name.starts_with("full_pit_icir"))
            .map(|c| c.combo_name.as_str())
            .collect();
        assert!(
            filtered.len() >= 2,
            "生产应恒有 active full_pit_icir* combo(空集告警分支不触发): {filtered:?}"
        );
        assert!(
            configs
                .iter()
                .any(|c| c.combo_name == "phase7_zzz_test_sch12_v1"),
            "zzz 非 full_pit 行应进入配置读取(证明造数生效)"
        );
        assert!(
            !filtered.contains(&"phase7_zzz_test_sch12_v1"),
            "非 full_pit_icir 前缀必须被保鲜循环排除"
        );
        cleanup_sch12_strategy_rows(&db).await;
    }

    /// horizon 组装仲裁: 显式 combo_horizon 列优先, 列缺失才用名字推断兜底。
    /// zzz 行: 列=7 / 名字推断(_h3)=3 → 组装取 7;
    /// 生产 37f_h20_fund_v2: 列 NULL → 名字推断 —— 注意 rfind("_h") 后缀是
    /// "20_fund_v2", parse 失败兜底 1(而非直觉的 20), 如实锚定当前行为。
    #[tokio::test]
    async fn pit_combo_refresh_horizon_column_wins_over_name_inference() {
        let _zzz_guard = SCH12_ZZZ_LOCK.lock().await;
        let db = test_db().await;
        cleanup_sch12_strategy_rows(&db).await;
        insert_sch12_strategy(
            &db,
            "zzz_test_sch12_col",
            "full_pit_icir_zzz_test_sch12_h3",
            Some(7),
            false,
            None,
        )
        .await;

        let configs = load_active_combo_materialize_configs(&db).await;
        let zzz = configs
            .iter()
            .find(|c| c.combo_name == "full_pit_icir_zzz_test_sch12_h3")
            .expect("zzz combo 配置应被读取");
        assert_eq!(zzz.combo_horizon, Some(7), "显式列值: {zzz:?}");
        assert_eq!(
            combo_horizon_from_name("full_pit_icir_zzz_test_sch12_h3"),
            3,
            "名字推断 _h3 → 3"
        );
        let assembled = zzz
            .combo_horizon
            .unwrap_or_else(|| combo_horizon_from_name(&zzz.combo_name));
        assert_eq!(assembled, 7, "列值必须压过名字推断(dispatch 同款仲裁)");

        // 生产 combo 的组装结果(现网真实生效口径)
        let ind = configs
            .iter()
            .find(|c| c.combo_name == "full_pit_icir_indneutral_val_v1")
            .expect("生产 indneutral combo");
        assert_eq!(
            ind.combo_horizon,
            Some(20),
            "indneutral 显式列=20(2026-09-05 修复的口径)"
        );
        let f37 = configs
            .iter()
            .find(|c| c.combo_name == "full_pit_icir_37f_h20_fund_v2")
            .expect("生产 37f combo");
        assert!(
            f37.combo_horizon.is_some() && f37.combo_horizon == Some(20),
            "37f 显式列=20(任务79c 双修后现网配置): {f37:?}"
        );
        assert_eq!(
            combo_horizon_from_name("full_pit_icir_37f_h20_fund_v2"),
            20,
            "任务79c: 前导数字解析修复后, 名字推断与列值/命名意图三口径一致"
        );
        cleanup_sch12_strategy_rows(&db).await;
    }

    /// 同名 combo 多策略声明合并(dispatch 组装 include_fundamentals/whitelist 的
    /// 输入): include_fundamentals 任一 true 则 true, whitelist 取首个非空,
    /// horizon 取首个非 None —— 顺序无关。生产 indneutral combo 含基本面因子。
    #[tokio::test]
    async fn pit_combo_refresh_config_merges_fund_flag_and_whitelist() {
        let _zzz_guard = SCH12_ZZZ_LOCK.lock().await;
        let db = test_db().await;
        cleanup_sch12_strategy_rows(&db).await;
        let combo = "full_pit_icir_zzz_test_sch12_merge";
        let wl = serde_json::json!(["zzz_f1", "zzz_f2"]);
        insert_sch12_strategy(&db, "zzz_test_sch12_m_a", combo, None, false, None).await;
        insert_sch12_strategy(&db, "zzz_test_sch12_m_b", combo, Some(9), true, Some(&wl)).await;

        let configs = load_active_combo_materialize_configs(&db).await;
        let merged = configs
            .iter()
            .find(|c| c.combo_name == combo)
            .expect("合并后同名 combo 应只有一条");
        assert!(
            merged.include_fundamentals,
            "任一策略声明 true 则合并为 true(OR 语义): {merged:?}"
        );
        let got_wl = merged
            .factor_whitelist
            .as_ref()
            .expect("首个非空 whitelist 应被保留");
        assert_eq!(got_wl.len(), 2, "白名单两项: {got_wl:?}");
        assert_eq!(got_wl[0], "zzz_f1");
        assert_eq!(
            merged.combo_horizon,
            Some(9),
            "首个非 None horizon 应被保留(与插入顺序无关): {merged:?}"
        );

        let ind = configs
            .iter()
            .find(|c| c.combo_name == "full_pit_icir_indneutral_val_v1")
            .expect("生产 indneutral combo");
        assert!(
            ind.include_fundamentals,
            "生产 v24 主 combo 含基本面因子(fin_/mf_ 等): {ind:?}"
        );
        cleanup_sch12_strategy_rows(&db).await;
    }

    // ── dispatch_mvo_equity_curve_update: 输入契约(不直调) ──

    /// 生产注册的 mvo_equity_curve_update 任务 params 形状契约: strategy_id/
    /// benchmark_account_id/start_date 均为字符串且 start_date 可解析 %Y%m%d,
    /// 可选 leverage_multiplier 为正有限数, 策略 id 必须是 active 策略。
    /// 不直调 dispatch 的理由: params 解析内联进函数体且输出仅进 spawn 闭包,
    /// 唯一可观察通道是真实 MVO 基准重跑(写 backtest_mvo_equity_curve, 分钟级)。
    #[tokio::test]
    async fn mvo_equity_curve_update_registered_params_are_well_formed() {
        let db = test_db().await;
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT task_name, params FROM scheduled_task_config
             WHERE task_type = 'mvo_equity_curve_update' AND enabled = true",
        )
        .fetch_all(&db)
        .await
        .expect("读取 mvo 任务注册");
        assert!(!rows.is_empty(), "生产应注册 mvo 基准任务");

        let active_ids: Vec<String> =
            sqlx::query_scalar("SELECT strategy_id FROM strategy_config WHERE status = 'active'")
                .fetch_all(&db)
                .await
                .expect("读取 active 策略");
        for (name, params) in &rows {
            let sid = params["strategy_id"].as_str().unwrap_or_default();
            assert!(!sid.is_empty(), "{name}: strategy_id 应为非空字符串");
            assert!(
                active_ids.iter().any(|s| s == sid),
                "{name}: strategy_id={sid} 应是 active 策略, 否则 spawn 内 load 失败仅 warn"
            );
            let bench = params["benchmark_account_id"].as_str().unwrap_or_default();
            assert!(
                !bench.is_empty(),
                "{name}: benchmark_account_id 应为非空字符串"
            );
            let sd = params["start_date"].as_str().unwrap_or_default();
            assert_eq!(sd.len(), 8, "{name}: start_date 应为 yyyymmdd: {sd}");
            assert!(
                NaiveDate::parse_from_str(sd, "%Y%m%d").is_ok(),
                "{name}: start_date 应可解析: {sd}"
            );
            if let Some(lm) = params["leverage_multiplier"].as_f64() {
                assert!(
                    lm > 0.0 && lm.is_finite(),
                    "{name}: leverage_multiplier 应为正有限数"
                );
            }
        }
    }

    // ── 调度入口哨兵: 防『注册了无分派臂的任务类型而绿着空转』 ──

    /// run_scheduled_tasks 的 11 个分派臂(task79 拆分后全集)。新 task_type 注册
    /// 进 scheduled_task_config 而代码未部署对应分支时, 会被记 unhandled 而非
    /// 静默 success(2026-09-21 rolling_pit_eval 教训); 本哨兵在注册侧提前拦截。
    const KNOWN_DISPATCH_ARMS: &[&str] = &[
        "data_quality_check",
        "equity_curve_update",
        "mvo_equity_curve_update",
        "factor_backfill",
        "nightly_signal_prep",
        "ptrade_signal_export",
        "ptrade_report_fetch",
        "native_pv_increment",
        "pit_combo_refresh",
        "rolling_pit_eval",
        "market_level_source_freshness",
    ];

    #[tokio::test]
    async fn scheduled_task_registered_types_all_have_dispatch_arms() {
        let db = test_db().await;
        let registered: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT task_type FROM scheduled_task_config WHERE enabled = true",
        )
        .fetch_all(&db)
        .await
        .expect("读取启用的任务类型");
        assert!(!registered.is_empty());
        for task_type in &registered {
            assert!(
                KNOWN_DISPATCH_ARMS.contains(&task_type.as_str()),
                "任务类型 '{task_type}' 在 11 个分派臂中无对应分支——先部署分派代码再注册, \
                 否则该任务每轮被记 unhandled 空转"
            );
        }
    }
}
