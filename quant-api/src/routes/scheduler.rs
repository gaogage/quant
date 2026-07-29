//! 内置调度器 — v15 日频量化交易。
//!
//! 14:45 (收盘前): 获取当日行情 → 回测 → MVO → 调仓 → 立即推送钉钉
//! 16:00 (收盘后): 同步日终行情数据到历史表 → 清理过期回测数据
//!
//! 日频交易不需要盘中实时行情，每天只在收盘前交易一次。
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
    MvoWeightCache, StrategyConfig, compute_lw_mvo_weights,
    resolved_to_legacy_sc, send_quality_alert,
};
use crate::routes::strategy::{AssetClass, ResolvedStrategy};

/// 因子回填窗口（天）：覆盖 T+1 延迟 + 周末缺口，确保幂等刷新最近数据。
const BACKFILL_WINDOW_DAYS: chrono::Duration = chrono::Duration::days(7);

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
    ("phase7-forecast-revision-surprise-backfill", "分析师预测修正"),
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
async fn trigger_v24_backfill_routes(
    db: &PgPool,
    start_date: &str,
    end_date: &str,
) {
    let api_base = self_api_base();
    let client = reqwest::Client::new();

    // P4-1: 优先从 DB 读取路由清单，表空时回退硬编码常量。
    let routes: Vec<(String, String)> =
        match sqlx::query_as::<_, (String, String)>(
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

/// 获取最新 EOD 数据版本（动态，确保回测使用最新数据而非硬编码的旧版本）
pub(crate) async fn get_latest_data_version(db: &PgPool) -> String {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT data_version_id FROM data_version
         WHERE data_version_id LIKE 'dv-eod-%'
         ORDER BY end_date DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.map(|(d,)| d)
        .unwrap_or_else(|| "research-full-2016-2026-20260515".to_string())
}

struct DailyState {
    date: Option<NaiveDate>,
    traded_today: bool,     // 今日是否已完成调仓 (14:40+)
    eod_synced_today: bool, // 今日是否已完成日终数据同步 (16:00)
    yesterday_synced: bool, // 昨日日线是否已完成 T+1 同步 (次日9:00)
    cleanup_done: bool,
    report_pushed: bool,    // 今日是否已推送实盘绩效日报 (16:00 EOD 后)
}

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
    hour == 16 && minute < 10
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
        assert!(is_eod_sync_window(16, 0));
        assert!(is_eod_sync_window(16, 9));
        assert!(!is_eod_sync_window(16, 10));
        assert!(!is_eod_sync_window(16, 39));
        assert!(!is_eod_sync_window(17, 0));
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

    // ── daily_to_monthly_returns 测试 ──

    // ── detect_regime_exposure_cached 测试 ──

}
/// 不再依赖单一策略（v19）的 etf_symbols，确保多策略并行时所有策略 ETF 都被同步。
pub async fn load_active_etf_symbols_union(db: &PgPool) -> Vec<String> {
    let rows: Vec<(Option<serde_json::Value>,)> = sqlx::query_as(
        "SELECT etf_symbols
         FROM strategy_config
         WHERE status='active' AND strategy_type='composite' AND etf_symbols IS NOT NULL
         ORDER BY strategy_id",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (etf_json,) in rows {
        if let Some(arr) = etf_json.as_ref().and_then(|v| v.as_array()) {
            for sym in arr {
                if let Some(s) = sym.as_str() {
                    set.insert(s.to_string());
                }
            }
        }
    }
    set.into_iter().collect()
}

/// 从 combo_name 推断 PIT horizon：`full_pit_icir_37f_h20` → 20，无 `_hN` 后缀 → 1。
/// 用于 pit_combo_refresh 遍历所有 active combo 时为每个 combo 取正确 horizon。
fn combo_horizon_from_name(combo: &str) -> i16 {
    if let Some(idx) = combo.rfind("_h") {
        if let Ok(h) = combo[idx + 2..].parse::<i16>() {
            if h > 0 {
                return h;
            }
        }
    }
    1
}

/// 收集所有 active 策略（composite + asset 子策略）声明的 factor combo_name 去重列表。
/// 用于调仓前对所有活跃账号用到的因子物化做新鲜度校验+自动补全。
/// 模拟实盘盘中调仓依赖：每个激活账号的激活策略用到的 combo 都必须有当日因子数据。
pub async fn load_active_factor_combos(db: &PgPool) -> Vec<String> {
    let rows: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT combo_name
         FROM strategy_config
         WHERE status='active' AND combo_name IS NOT NULL AND btrim(combo_name) <> ''
         ORDER BY strategy_id",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (combo,) in rows {
        if let Some(c) = combo {
            let c = c.trim().to_string();
            if !c.is_empty() {
                set.insert(c);
            }
        }
    }
    set.into_iter().collect()
}

/// 活跃 combo 的物化配置(include_fundamentals + factor_whitelist)。
/// 从 strategy_config 读取:含基本面因子的 combo(如 v24 fund_v2)需 include_fundamentals=true
/// + factor_whitelist 去冗余白名单,否则用默认黑名单物化会丢失基本面因子。
/// 返回 (combo_name, include_fundamentals, factor_whitelist) 列表。
pub async fn load_active_combo_materialize_configs(
    db: &PgPool,
) -> Vec<(String, bool, Option<Vec<String>>)> {
    let rows: Vec<(Option<String>, Option<bool>, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT combo_name, include_fundamentals, factor_whitelist
         FROM strategy_config
         WHERE status='active' AND combo_name IS NOT NULL AND btrim(combo_name) <> ''",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut map: std::collections::BTreeMap<String, (bool, Option<Vec<String>>)> =
        std::collections::BTreeMap::new();
    for (combo, inc_fund, whitelist) in rows {
        if let Some(c) = combo {
            let c = c.trim().to_string();
            if c.is_empty() {
                continue;
            }
            // 同名 combo 可能多策略声明,合并:任一策略 include_fundamentals=true 则用 true;
            // factor_whitelist 取首个非空(同 combo 白名单应一致)。
            let inc = inc_fund.unwrap_or(false);
            let wl: Option<Vec<String>> = whitelist.and_then(|v| {
                v.as_array()
                    .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            });
            let entry = map.entry(c).or_insert((false, None));
            if inc {
                entry.0 = true;
            }
            if entry.1.is_none() && wl.is_some() {
                entry.1 = wl;
            }
        }
    }
    map.into_iter()
        .map(|(combo, (inc, wl))| (combo, inc, wl))
        .collect()
}


pub async fn load_strategy_config(db: &PgPool, strategy_id: &str) -> StrategyConfig {
    let row: Option<(serde_json::Value,)> = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT jsonb_build_object(
            'strategy_id', strategy_id,
            'name', name,
            'etf_symbols', etf_symbols,
            'equity_curve_task_id', equity_curve_task_id,
            'min_stock', min_stock,
            'max_single', max_single,
            'max_single_bull', max_single_bull,
            'momentum_blend_ratio', momentum_blend_ratio,
            'ga_population', ga_population,
            'ga_generations', ga_generations,
            'vol_target', vol_target,
            'leverage_cap', leverage_cap,
            'default_weights', default_weights,
            'regime_bull_min_stock', regime_bull_min_stock,
            'regime_bear_min_stock', regime_bear_min_stock,
            'deep_bear_threshold', deep_bear_threshold,
            'deep_bear_exposure', deep_bear_exposure,
            'kelly_fraction', kelly_fraction,
            'score_candidate_pool_size', score_candidate_pool_size,
            'signal_source', signal_source,
            'prediction_blend_weight', prediction_blend_weight,
            'combo_name', combo_name,
            'top_n', top_n,
            'prediction_set_id', prediction_set_id,
            'dynamic_target_cap', dynamic_target_cap,
            'dynamic_target_floor', dynamic_target_floor,
            'score_direction', score_direction,
            'candidate_tier', candidate_tier,
            'leverage_regime_threshold', leverage_regime_threshold,
            'slippage_pct', slippage_pct,
            'mvo_objective', mvo_objective
        ) FROM strategy_config WHERE strategy_id = $1 AND status = 'active'",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    match row {
        Some((v,)) => {
            let cfg: StrategyConfig = serde_json::from_value(v)
                .unwrap_or_else(|e| panic!("策略配置 {} 反序列化失败: {}", strategy_id, e));
            info!("[scheduler] 策略配置加载: {} (DB)", cfg.strategy_id);
            cfg
        }
        None => panic!(
            "[scheduler] 策略配置加载失败 strategy_id={},strategy_config 表无此 active 记录",
            strategy_id
        ),
    }
}

/// 加载第一个 active 复合策略配置（不再硬编码 v19）。
/// 用于 EOD 同步的 ML 预测覆盖检查（ensure_prediction_coverage 仍需单策略 sc）。
/// 无 active 复合策略时返回 None（调用方跳过 ML 检查，不 panic）。
pub async fn load_first_active_strategy_config(db: &PgPool) -> Option<StrategyConfig> {
    let sid: Option<String> = sqlx::query_scalar(
        "SELECT strategy_id FROM strategy_config
         WHERE status='active' AND strategy_type='composite'
         ORDER BY strategy_id LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    match sid {
        Some(sid) => Some(load_strategy_config(db, &sid).await),
        None => {
            warn!("[scheduler] 无 active 复合策略，跳过 ML 预测覆盖检查");
            None
        }
    }
}

async fn run_scheduled_tasks(db: &PgPool) {
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
        match task_type.as_str() {
            "data_quality_check" => {
                run_data_quality_check(db).await;
            }
            "equity_curve_update" => {
                // 自动同步所有活跃账号关联策略的权益曲线(combo 去重)。
                // 消除硬编码 v19:v21/v21_lev/v23 等策略都会被同步。
                // 异步 spawn 不阻塞 scheduler tick(sleeve 回测全量重跑 ~75s/个,串行会卡 run_tick)。
                // 每日工作日 17:00 触发(原月度,v23 月中创建后 sleeve 滞后到下月才同步→门禁拦截)。
                let db_clone = db.clone();
                tokio::spawn(async move {
                    let results =
                        crate::routes::equity_curve_sync::sync_active_strategies_equity_curves(
                            &db_clone,
                        )
                        .await;
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
            "factor_backfill" => {
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
                    continue;
                };
                let sync_date_str = sync_date.format("%Y%m%d").to_string();
                let backfill_start = (sync_date - BACKFILL_WINDOW_DAYS).format("%Y%m%d").to_string();
                trigger_v24_backfill_routes(db, &backfill_start, &sync_date_str).await;
            }
            "pit_combo_refresh" => {
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
                let combos = load_active_combo_materialize_configs(db).await;
                let pit_combos: Vec<(String, bool, Option<Vec<String>>)> = combos
                    .into_iter()
                    .filter(|(c, _, _)| c.starts_with("full_pit_icir"))
                    .collect();
                if pit_combos.is_empty() {
                    warn!("[scheduler] PIT combo 保鲜：无 active full_pit_icir* combo，跳过");
                }
                for (combo, include_fund, whitelist) in &pit_combos {
                    let horizon = combo_horizon_from_name(combo);
                    info!(
                        "[scheduler] PIT combo 保鲜: combo={} horizon={} include_fund={} whitelist={} 区间 {}~{}",
                        combo, horizon, include_fund, whitelist.as_ref().map(|w| w.len()).unwrap_or(0), refresh_start, refresh_end
                    );
                    // 含基本面因子的 combo(如 v24 fund_v2)用 ext + include_fundamentals + factor_whitelist,
                    // 否则用默认黑名单物化会丢失 fin_/mf_/north_ 因子。
                    match crate::routes::factors::materialize_pit_combo_ext(
                        db,
                        combo,
                        ver,
                        horizon,
                        refresh_start,
                        refresh_end,
                        *include_fund,
                        None,  // min_abs_ic_ir: 保鲜不加阈值(白名单已筛)
                        whitelist.as_deref(),
                    )
                    .await
                    {
                        Ok(rows) => info!("[scheduler] PIT combo {} 保鲜完成: {} 行", combo, rows),
                        Err(e) => warn!("[scheduler] PIT combo {} 保鲜失败: {}", combo, e),
                    }
                }
            }
            "market_level_source_freshness" => {
                let today = chrono::Utc::now().date_naive();
                let api_base = self_api_base();
                let client = reqwest::Client::new();
                for source in market_level_freshness_sources(params) {
                    let latest = latest_market_level_trade_date(db, &source).await;
                    let Some(payload) = market_level_freshness_sync_payload(&source, latest, today)
                    else {
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
            _ => {}
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
        let _ = sqlx::query(
            "UPDATE scheduled_task_config SET last_run_at = NOW(), run_count = run_count + 1,
             last_status = 'success', next_run_at = $1 WHERE task_name = $2",
        )
        .bind(next_at)
        .bind(name)
        .execute(db)
        .await;
    }
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
                "[scheduler] {} 已启动 ({}): 14:40调仓 | 16:00 EOD | 9:00 T+1数据补同步",
                sc.strategy_id, sc.name
            ),
            None => info!(
                "[scheduler] 已启动 (无 active 复合策略): 14:40调仓 | 16:00 EOD | 9:00 T+1数据补同步"
            ),
        }

        // 首次运行时检查定时任务
        run_scheduled_tasks(&db).await;

        loop {
            interval.tick().await;
            if let Err(e) =
                run_tick(&db, &tushare, &state, &mvo_cache, port, strategy_config.as_ref().as_ref()).await
            {
                error!("[scheduler] 任务失败: {}", e);
            }
            // 每小时检查一次定时任务
            let now = chrono::Local::now();
            if now.minute() == 0 {
                run_scheduled_tasks(&db).await;
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

    // ── 14:40~15:00 (收盘前): 交易日才调仓 ──
    if is_trade && hour == 14 && minute >= 40 {
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
            let orders_before: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM paper_order WHERE DATE(created_at) = $1",
            )
            .bind(today)
            .fetch_one(db)
            .await
            .unwrap_or(0);

            match generate_paper_signals_for_all(db, mvo_cache, port, today, sc, tushare).await {
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
                    snapshot_positions_for_all_accounts(db, today).await;

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
                        match push_dingtalk_for_all_accounts(db, today).await {
                            Ok(_) => info!("[scheduler] 钉钉推送完成"),
                            Err(e) => warn!("[scheduler] 钉钉推送失败: {}", e),
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

    // ── 16:00 (收盘后): 交易日EOD + 非交易日也执行数据同步 ──
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
            info!("[scheduler] 16:00 日终数据同步...");
            if let Err(e) = sync_eod_data(db, tushare, today).await {
                warn!("[scheduler] 日终数据同步失败: {}", e);
            }

            // P2-1: EOD 数据就绪后生成实盘绩效日报（写 paper_nav_snapshot 当日点 + 钉钉推送）。
            // 仅交易日才有当日调仓/NAV 变化，非交易日跳过（避免推送重复的静态快照）。
            if is_trade {
                {
                    let mut st = state.lock().await;
                    st.report_pushed = true;
                }
                if let Err(e) = push_daily_performance_report(db, today).await {
                    warn!("[scheduler] 实盘绩效日报生成失败: {}", e);
                }
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

            // Step 4: 日线就绪后才触发因子回填（覆盖v24全部因子类别）
            if retries < max_retries {
                info!("[scheduler] 触发因子回填 (依赖数据已就绪)");
                let backfill_start = (sync_date - BACKFILL_WINDOW_DAYS).format("%Y%m%d").to_string();
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
                                arr.iter().filter_map(|x| x.as_str().map(String::from)).collect()
                            })
                        })
                    };
                    match crate::routes::factors::materialize_pit_combo_ext(
                        db,
                        &combo,
                        "1.0.0",
                        combo_horizon_from_name(&combo),
                        sync_date - chrono::Duration::days(7),
                        sync_date,
                        inc_fund,
                        None,  // min_abs_ic_ir: T+1 保鲜不加阈值(白名单已筛)
                        whitelist.as_deref(),
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
                &[symbol.clone()],
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
                    &[symbol.clone()],
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
                        info!("[pre-trade] 因子({})回填完成 (等待{}s)", factor_combo, (retry + 1) * 3);
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

/// 16:00 日终数据同步 (直接调用内部函数)
async fn sync_eod_data(
    db: &PgPool,
    tushare: &TushareClient,
    date: NaiveDate,
) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    // etf_symbols 取所有 active 复合策略的并集（不再硬编码 v19，覆盖多策略 ETF）。
    let etf_symbols = load_active_etf_symbols_union(db).await;
    // ensure_prediction_coverage 的 ML 训练逻辑仍需单策略 sc（取第一个 active 复合策略为代表）。
    let sc = load_first_active_strategy_config(db).await;
    let all_stocks: Vec<String> = sqlx::query_scalar(
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    // ── 事件数据优先同步：即使后续 heavy EOD 任务失败，也不能让交易门禁缺停牌/涨跌停。──
    if let Err(e) = quant_data::sync::sync_suspension(db, tushare, &date_str).await {
        warn!("[scheduler] EOD 停牌数据同步失败: {}", e);
    }
    let limit_ok = sync_limit_with_retry(db, tushare, &date_str).await;
    if !limit_ok {
        warn!("[scheduler] ⚠ 涨跌停数据同步失败 (已重试)");
    }

    // ── 当日日线 + ETF日线（收盘后通常已可获取）──
    // P0 EOD 隔离:日线/ETF/指数/daily_basic/moneyflow/block_trade 均为"非关键路径",
    // 用 timeout 包裹防卡死。daily_basic 是已知慢点(历史曾 3 小时卡死 running 永不完成,
    // 导致后续复权因子同步+兜底未执行,adj 视图退化为 raw 价,回测曲线崩坏)。
    // 超时则 warn 并继续,确保复权因子+兜底(关键路径)不被前序步骤短路。
    const EOD_STEP_TIMEOUT_SECS: u64 = 1800; // 30 分钟

    // 注意:timeout 结果含 Box<dyn StdError>(非 Send),须在独立块内消费,
    // 避免变量跨越后续 await 点导致整个 spawn future 非 Send。
    {
        let daily_timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS),
            quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &all_stocks,
                &date_str,
                &date_str,
                &format!("dv-eod-{}", date_str),
            ),
        )
        .await;
        match daily_timeout {
            Ok(r) => {
                if let Err(e) = r {
                    warn!("[scheduler] EOD 日线同步失败: {}", e);
                }
            }
            Err(_) => warn!("[scheduler] ⚠ EOD 日线同步超时({}秒),跳过", EOD_STEP_TIMEOUT_SECS),
        }
    }
    let _ = quant_data::sync::sync_fund_daily(
        db,
        tushare,
        &etf_symbols,
        &date_str,
        &date_str,
        &format!("etf-eod-{}", date_str),
    )
    .await;
    let index_codes = vec!["000300.SH".to_string()];
    let _ = quant_data::sync::sync_index_daily(
        db,
        tushare,
        &index_codes,
        &date_str,
        &date_str,
        &format!("idx-eod-{}", date_str),
    )
    .await;

    // 日线基础指标(已知慢点,超时隔离)
    {
        let basic_timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS),
            quant_data::sync::sync_daily_basic(
                db,
                tushare,
                &all_stocks,
                &date_str,
                &date_str,
                &format!("dv-basic-eod-{}", date_str),
            ),
        )
        .await;
        match basic_timeout {
            Ok(r) => {
                if let Err(e) = r {
                    warn!("[scheduler] EOD daily_basic 同步失败: {}", e);
                }
            }
            Err(_) => warn!(
                "[scheduler] ⚠ EOD daily_basic 同步超时({}秒),跳过 - 复权因子兜底仍将执行",
                EOD_STEP_TIMEOUT_SECS
            ),
        }
    }

    // 资金流(market_stock_moneyflow): v24 mf_* 因子依赖。
    // 不在原 EOD 列表内,缺失会导致 moneyflow backfill completed 但无产出(静默降级)。
    let mf_n = {
        let mf_dv = format!("mf-eod-{}", date_str);
        let mf_fut = quant_data::sync::sync_moneyflow(
            db,
            tushare,
            &all_stocks,
            &date_str,
            &date_str,
            &mf_dv,
        );
        match tokio::time::timeout(tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS), mf_fut)
            .await
        {
            Ok(r) => r.unwrap_or(0),
            Err(_) => {
                warn!("[scheduler] ⚠ EOD 资金流同步超时,跳过");
                0
            }
        }
    };
    if mf_n > 0 {
        info!("[scheduler] EOD 资金流同步: {} 条", mf_n);
    }

    // 大宗交易(market_stock_block_trade): v24 block_trade_inst 因子依赖。
    let bt_n = {
        let bt_dv = format!("bt-eod-{}", date_str);
        let bt_fut = quant_data::sync::sync_block_trade(
            db,
            tushare,
            &bt_dv,
            &date_str,
            &date_str,
        );
        match tokio::time::timeout(tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS), bt_fut)
            .await
        {
            Ok(r) => r.unwrap_or(0),
            Err(_) => {
                warn!("[scheduler] ⚠ EOD 大宗交易同步超时,跳过");
                0
            }
        }
    };
    if bt_n > 0 {
        info!("[scheduler] EOD 大宗交易同步: {} 条", bt_n);
    }

    // ── 复权因子 + 兜底:关键路径,必须执行,不受前序步骤失败/超时影响 ──
    // (这是 P0 修复核心:7/20-7/21 daily_basic 卡死导致此处未执行,adj 视图退化,回测崩坏)
    let adj_n = quant_data::sync::sync_adj_factor(
        db,
        tushare,
        &all_stocks,
        &date_str,
        &date_str,
        &format!("dv-adj-eod-{}", date_str),
    )
    .await
    .unwrap_or(0);
    if adj_n > 0 {
        info!("[scheduler] EOD 复权因子同步: {} 条", adj_n);
    }

    // 复权因子完整性兜底:adj_factor 表是「每日全量快照」设计(正常≈bar行数,实测5192≈5190)。
    // 但 sync_adj_factor 按 symbol 逐只拉 Tushare(8并发,5201只),限流 200/分钟下部分超时会致
    // 当日只成功一部分(如 7/10 仅 1196/5189)。视图 market_stock_daily_bar_adj 用 LEFT JOIN +
    // COALESCE(adj_factor,1.0),缺失股票复权价退化为 raw 价,与前后日断层(10倍级),回测当日
    // 收益率/涨跌停全错乱。故 EOD 必须校验覆盖率并自动补全,而非仅告警。
    //
    // 补全原理:非除权日 adj_factor 恒等于前一交易日值(复权因子仅在除权除息日跳变)。
    // 对当日有 bar 但缺 adj_factor 的股票,用最近前一交易日的 adj_factor 前向填充 INSERT。
    // 这是数学正确的兜底——除权日当日 Tushare 必返回新值(不会缺),缺失的必是非除权日。
    let dv_adj_id = format!("dv-adj-eod-{}", date_str);
    backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

    // P1: 刷新 composite 回测曲线(偏离监控对标用)。复权兜底后执行,
    // 确保合成所用 ETF 复权价已补全。单策略失败不阻断其他。
    for sid in crate::routes::equity_curve_sync::collect_active_strategies(db).await {
        if let Err(e) =
            crate::routes::equity_curve_sync::sync_composite_equity_curve(db, &sid).await
        {
            warn!("[scheduler] composite 曲线合成失败 {}: {}", sid, e);
        }
    }

    info!(
        "[scheduler] 16:00 EOD 同步 (事件+当日日线+ETF+指数+基础指标+复权) ({})",
        date_str
    );

    // ── ML预测数据检查+补齐 ──
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let pred_ok = match sc.as_ref() {
        Some(sc) => ensure_prediction_coverage(db, tushare, date, sc).await,
        None => {
            warn!("[scheduler] ⚠ 无 active 策略，跳过 ML 预测覆盖检查");
            false
        }
    };
    if !pred_ok && sc.is_some() {
        warn!("[scheduler] ⚠ ML预测数据补齐失败, v16将降级为纯因子选股");
    }

    // 数据完整性检查
    run_data_quality_check(db).await;

    Ok(())
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

/// 数据完整性检查: 从配置的起始日期到今天, 检查所有核心表是否有缺口
async fn run_data_quality_check(db: &PgPool) {
    let today = chrono::Utc::now().date_naive();

    // 计算 A 股日线的交易日 gap
    let mut gaps: Vec<String> = Vec::new();
    let stock_max: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol LIKE '6%'",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if let Some((max_dt,)) = stock_max {
        let trading_days_behind: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((0,));
        if trading_days_behind.0 > 1 {
            gaps.push(format!(
                "A股日线: 最新={}, 落后{}个交易日",
                max_dt, trading_days_behind.0
            ));
        }
    }

    // 检查 ETF 日线（按策略配置的 ETF 列表逐个查）
    let etf_symbols: Vec<String> = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT etf_symbols FROM strategy_config WHERE status = 'active' ORDER BY updated_at DESC LIMIT 1"
    ).fetch_optional(db).await.ok().flatten()
        .and_then(|(v,)| serde_json::from_value::<Vec<String>>(v).ok())
        .unwrap_or_default();

    if !etf_symbols.is_empty() {
        for symbol in &etf_symbols {
            let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = $1",
            )
            .bind(symbol)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
            if let Some((max_dt,)) = max_row {
                let trading_gap: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
                ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((0,));
                if trading_gap.0 > 1 {
                    // ETF T+1，允许落后1个交易日
                    gaps.push(format!(
                        "ETF {}: 最新={}, 落后{}个交易日",
                        symbol, max_dt, trading_gap.0
                    ));
                }
            } else {
                gaps.push(format!("ETF {}: 无数据", symbol));
            }
        }
    }

    // 其他检查项
    let other_checks: Vec<(&str, &str, i64)> = vec![
        ("复权因子", "market_adjustment_factor", 30),
        ("ML预测", "model_prediction", 5),
    ];
    for (name, table, max_calendar_gap) in &other_checks {
        let max_row: Option<(String,)> =
            sqlx::query_as(&format!("SELECT MAX(trade_date)::text FROM {}", table))
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
        if let Some((max_d,)) = max_row {
            if let (Ok(max_dt), Ok(today_dt)) = (
                NaiveDate::parse_from_str(&max_d, "%Y-%m-%d"),
                NaiveDate::parse_from_str(&today.format("%Y-%m-%d").to_string(), "%Y-%m-%d"),
            ) {
                let gap = (today_dt - max_dt).num_days();
                if gap > *max_calendar_gap {
                    gaps.push(format!("{}: 最新={}, 缺口={}天", name, max_d, gap));
                }
            }
        }
    }

    // ── v24 因子数据健康检查：14 活跃因子每日覆盖率 ──
    // 任何因子滞后(最新数据日落后 >1 交易日)都会导致 combo 打分静默降级。
    // 检查每个因子最新 trade_date 是否覆盖到最近交易日。
    {
        let latest_trade: Option<(chrono::NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE is_open = true AND trade_date <= $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(today)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        if let Some((latest_td,)) = latest_trade {
            let mut stale_factors: Vec<String> = Vec::new();
            for code in V24_FACTOR_CODES {
                let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                    "SELECT MAX(trade_date) FROM factor_value
                     WHERE factor_code = $1 AND factor_version = '1.0.0'",
                )
                .bind(code)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
                match max_row {
                    Some((factor_max,)) if factor_max >= latest_td => {}
                    Some((factor_max,)) => {
                        let gap = (latest_td - factor_max).num_days();
                        stale_factors.push(format!("{}(最新{}天前)", code, gap));
                    }
                    None => {
                        stale_factors.push(format!("{}(无数据)", code));
                    }
                }
            }
            if !stale_factors.is_empty() {
                gaps.push(format!(
                    "v24因子滞缓({}/14): {}",
                    stale_factors.len(),
                    stale_factors.join(", ")
                ));
            }

            // ── P2-2: v24 因子覆盖率骤降检测 ──
            // 部分因子(回购/大宗交易/研报评级等)天然只覆盖部分标的,不能拿"全市场"做分母，
            // 否则天天误报。改用"该因子自身近 30 日单日最大覆盖数"做基准——覆盖率 =
            // 最新日覆盖数 / 近期最大覆盖数，<80% 说明相对自身正常水平出现骤降(真实数据缺口)，
            // 而非因子设计上的稀疏性。
            let window_start = latest_td - chrono::Duration::days(30);
            let coverage_rows: Vec<(String, chrono::NaiveDate, i64, i64)> = sqlx::query_as(
                "WITH per_day AS (
                    SELECT factor_code, trade_date, COUNT(DISTINCT symbol) AS day_cnt
                    FROM factor_value
                    WHERE factor_version = '1.0.0'
                      AND factor_code = ANY($1)
                      AND trade_date >= $2
                    GROUP BY factor_code, trade_date
                 ),
                 latest AS (
                    SELECT DISTINCT ON (factor_code) factor_code, trade_date, day_cnt
                    FROM per_day ORDER BY factor_code, trade_date DESC
                 )
                 SELECT l.factor_code, l.trade_date, l.day_cnt, MAX(p.day_cnt) AS recent_max
                 FROM latest l JOIN per_day p USING (factor_code)
                 GROUP BY l.factor_code, l.trade_date, l.day_cnt",
            )
            .bind(V24_FACTOR_CODES)
            .bind(window_start)
            .fetch_all(db)
            .await
            .unwrap_or_default();

            let mut low_coverage: Vec<String> = Vec::new();
            for (code, factor_latest_dt, latest_cnt, recent_max) in &coverage_rows {
                if *recent_max <= 0 {
                    continue;
                }
                let ratio = *latest_cnt as f64 / *recent_max as f64;
                if ratio < 0.8 {
                    low_coverage.push(format!(
                        "{}({}覆盖{}/{}={:.0}%)",
                        code,
                        factor_latest_dt,
                        latest_cnt,
                        recent_max,
                        ratio * 100.0
                    ));
                }
            }
            if !low_coverage.is_empty() {
                gaps.push(format!(
                    "v24因子覆盖率骤降({}/14, 较近30日最高<80%): {}",
                    low_coverage.len(),
                    low_coverage.join(", ")
                ));
            }
        }
    }

    // ── 策略依赖覆盖检查：验证所有活跃策略所需数据都有自动同步 ──
    {
        let signal_sources: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT signal_source, paper_account_id FROM paper_account WHERE status = 'active'"
        ).fetch_all(db).await.unwrap_or_default();
        for (signal_source, _account_id) in &signal_sources {
            let required: Vec<&str> = match signal_source.as_str() {
                "factor" => vec!["A股日线", "ETF日线", "因子(pv)", "CSI300"],
                "prediction" | "prediction_blend" => {
                    vec!["A股日线", "ETF日线", "因子(pv)", "CSI300", "ML预测"]
                }
                _ => vec!["A股日线", "ETF日线", "因子(pv)", "CSI300"],
            };
            for item in &required {
                let covered = match *item {
                    "A股日线" | "ETF日线" | "CSI300" | "停牌" | "涨跌停" | "复权因子" => {
                        true
                    } // scheduler 9:00/16:00 内置
                    "因子(pv)" => {
                        // 查 scheduled_task_config 确认 factor_backfill_daily 已配置且启用
                        // (T+1 9:00 run_tick 内联也会回填,但需有可查/可触发的任务保障)
                        let n: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM scheduled_task_config
                             WHERE task_name='factor_backfill_daily' AND enabled=true",
                        )
                        .fetch_one(db)
                        .await
                        .unwrap_or(0);
                        n > 0
                    }
                    "ML预测" => true,   // scheduler 16:00 EOD (60天检查)
                    "权益曲线" => true, // equity_curve_monthly 任务
                    _ => false,
                };
                if !covered {
                    gaps.push(format!(
                        "策略依赖缺失: signal={} 需要 {} 但无自动同步任务",
                        signal_source, item
                    ));
                }
            }
        }
    }

    // ── 定时任务依赖顺序检查 ──
    {
        let deps = check_task_dependency_order(db).await;
        for d in &deps {
            gaps.push(format!("任务依赖顺序异常: {}", d));
        }
    }

    if !gaps.is_empty() {
        let msg = format!(
            "[数据质量] 发现 {} 个缺口:\n{}",
            gaps.len(),
            gaps.join("\n")
        );
        warn!("{}", msg);
        send_quality_alert(db, &gaps).await;
    } else {
        info!("[数据质量] 全部数据完整, 检查日期={}", today);
    }

    let _ = sqlx::query(
        "INSERT INTO data_quality_config (config_key, config_value, description) VALUES ('last_quality_check', $1, '最后质量检查日期') ON CONFLICT (config_key) DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = NOW()"
    ).bind(today.format("%Y-%m-%d").to_string()).execute(db).await;
}

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
async fn sync_limit_with_retry(db: &PgPool, tushare: &TushareClient, date_str: &str) -> bool {
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
            return true;
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
async fn ensure_prediction_coverage(
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

/// 发送钉钉告警 (独立于账号体系, 直接使用webhook)
async fn send_dingtalk_alert(db: &PgPool, msg: &str) {
    send_dingtalk_alert_titled(db, "调仓告警", msg).await;
}

/// 带标题的钉钉告警(P2-3:调仓成功/失败/零信号复用同一推送通道，用标题区分场景)。
async fn send_dingtalk_alert_titled(db: &PgPool, title: &str, msg: &str) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    ).fetch_all(db).await.unwrap_or_default();

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": title, "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}
/// 复权因子当日完整性兜底:校验覆盖率→前向填充补全→再校验告警。
///
/// adj_factor 表是「每日全量快照」(正常行数≈当日 bar 行数)。sync_adj_factor 按 symbol 逐只
/// 拉 Tushare,部分超时会致当日只成功一部分。视图 COALESCE(adj_factor,1.0) 让缺失股票复权价
/// 退化为 raw 价,与前后日断层,回测当日收益率/涨跌停错乱。
///
/// 补全:对当日有 bar 但缺 adj_factor 的股票,取该 symbol 最近前一交易日的 adj_factor INSERT。
/// 数学正确——非除权日复权因子恒等于前值,缺失的必是非除权日(除权日 Tushare 必返回新值)。
///
/// 阈值:覆盖率<90%(adj_cnt < bar_cnt*0.9)才触发补全+告警,避免无谓写入。
pub(crate) async fn backfill_adj_factor_for_date(db: &PgPool, date: NaiveDate, dv_id: &str) {
    let date_str = date.format("%Y-%m-%d").to_string();
    let bar_cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT symbol) FROM market_stock_daily_bar WHERE trade_date = $1",
    )
    .bind(date)
    .fetch_one(db)
    .await
    .unwrap_or(0);
    let adj_cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT symbol) FROM market_adjustment_factor WHERE trade_date = $1",
    )
    .bind(date)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    if bar_cnt == 0 {
        // 当日无 bar(非交易日或日线未同步),跳过
        return;
    }

    // 覆盖率<90%:有缺失,前向填充补全
    if adj_cnt * 10 < bar_cnt * 9 {
        // 先确保 dv_id 在 data_version 表注册(market_adjustment_factor.data_version_id 有 FK 约束,
        // 未注册会导致后续 INSERT 静默失败 - unwrap_or(0) 吞错误)。ON CONFLICT 幂等。
        let _ = sqlx::query(
            "INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash) \
             VALUES ($1, $2, 'forward_fill', $3, $3, ARRAY['market_adjustment_factor'], '') \
             ON CONFLICT (data_version_id) DO NOTHING",
        )
        .bind(dv_id)
        .bind(format!("复权因子前向填充 {}", date_str))
        .bind(date)
        .execute(db)
        .await;
        // 用 LATERAL 取每只缺失股票最近前一交易日的 adj_factor,批量 INSERT
        let filled = sqlx::query(
            "INSERT INTO market_adjustment_factor (symbol, trade_date, adj_factor, source, data_version_id, created_at) \
             SELECT b.symbol, $1::date, prev.adj_factor, 'forward_fill', $2, NOW() \
             FROM (SELECT DISTINCT symbol FROM market_stock_daily_bar WHERE trade_date = $1) b \
             LEFT JOIN LATERAL ( \
                 SELECT a.adj_factor FROM market_adjustment_factor a \
                 WHERE a.symbol = b.symbol AND a.trade_date < $1 \
                 ORDER BY a.trade_date DESC LIMIT 1 \
             ) prev ON true \
             WHERE prev.adj_factor IS NOT NULL \
               AND NOT EXISTS ( \
                   SELECT 1 FROM market_adjustment_factor x \
                   WHERE x.symbol = b.symbol AND x.trade_date = $1 \
               ) \
             ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(date)
        .bind(dv_id)
        .execute(db)
        .await
        .map(|r| r.rows_affected())
        .unwrap_or(0);

        let adj_cnt_after: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT symbol) FROM market_adjustment_factor WHERE trade_date = $1",
        )
        .bind(date)
        .fetch_one(db)
        .await
        .unwrap_or(0);
        let pct = if bar_cnt > 0 { adj_cnt_after * 100 / bar_cnt } else { 100 };
        if filled > 0 {
            info!(
                "[scheduler] 复权因子前向填充: {} 补 {} 只 (adj {}→{} 覆盖率 {}%)",
                date_str, filled, adj_cnt, adj_cnt_after, pct
            );
        }
        // 补全后仍不足 90%:告警(可能前一交易日也大面积缺失,需人工查)
        if adj_cnt_after * 10 < bar_cnt * 9 {
            let msg = format!(
                "复权因子缺失: {} 当日 bar {} 只,前向填充后 adj_factor {} 只(覆盖率 {}%),复权价可能仍退化,请人工核查",
                date_str, bar_cnt, adj_cnt_after, pct
            );
            warn!("[scheduler] {}", msg);
            send_quality_alert(db, &[msg]).await;
        }
    } else {
        info!(
            "[scheduler] 复权因子校验通过: {} bar={} adj_factor={} (覆盖率 {}%)",
            date_str,
            bar_cnt,
            adj_cnt,
            if bar_cnt > 0 { adj_cnt * 100 / bar_cnt } else { 100 }
        );
    }
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
) -> Result<(), String> {
    let accounts = sqlx::query_as::<_, (String, String, bool, f64, String, Option<String>)>(
        "SELECT paper_account_id, name, COALESCE(leverage_enabled, false), COALESCE(leverage_multiplier, 1.0), COALESCE(leverage_mode, 'fixed'), strategy_version_id FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db).await
    .map_err(|e| format!("account query: {}", e))?;

    if accounts.is_empty() {
        return Ok(());
    }

    for (
        account_id,
        name,
        leverage_enabled,
        leverage_multiplier,
        leverage_mode,
        strategy_version_id,
    ) in &accounts
    {
        let leverage_enabled = *leverage_enabled;
        let leverage_multiplier = *leverage_multiplier;
        let leverage_mode = leverage_mode.as_str();
        // 账号挂策略(strategy_version_id) → 加载该策略配置;无配置则报错并跳过(不阻塞其他账号)
        let strategy_version_id = match strategy_version_id.as_deref() {
            Some(s) if !s.is_empty() => s,
            _ => {
                error!(
                    "[paper] 账号 {} 未配置 strategy_version_id,跳过(配置错误,不阻塞其他账号)",
                    account_id
                );
                continue;
            }
        };
        let rs = match crate::routes::strategy::load_resolved_strategy(db, strategy_version_id)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("[paper] {} 策略加载失败,跳过: {}", account_id, e);
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

        let start = (date - chrono::Duration::days(30))
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
        // P0修复(2026-07-20): 默认必须是 daily。实盘每日按 30 天滚动窗口调 run-factor,
        // 若沿用回测语义的 monthly(20 交易日)/biweekly 频率,调仓触发点大概率不落在"今天"这一
        // 边界上,导致 signals_count=0 → backtest_position 当日截面为空 → 目标持仓空集 →
        // rebalance_account 把所有 A 股持仓当"不在目标集"清仓,且不会买入任何新 A 股。
        // 已实测验证:30天窗口+monthly=0信号;30天窗口+daily=7信号(含今日截面)。
        let rebalance_freq = wfa_params
            .get("rebalance")
            .and_then(|v| v.as_str())
            .unwrap_or("daily");
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
/// 1. 写当日 `paper_nav_snapshot`（实盘调仓路径此前从不写该表，只有回放路径写，
///    导致 NAV 曲线在实盘期间断档）
/// 2. 对比回测基准（`equity_curve_task_id` 对应的 backtest_equity_curve），偏离 >2% 告警
/// 3. 汇总当日成交笔数/买卖金额，推送钉钉 markdown 日报
pub async fn push_daily_performance_report(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    use super::dingtalk;

    // P0 双保险:日报生成前强制复权因子兜底。即使 EOD 中段失败(如 daily_basic 卡死),
    // 日报前再补一次,确保 adj 视图不退化,回测偏离计算不被污染数据干扰。
    let dv_adj_id = format!("dv-adj-report-{}", date.format("%Y%m%d"));
    backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

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
        // 注意不显式传 strategy_version_id：该字段有 FK -> strategy_version 表，
        // 而 v23/v24 等复合策略版本号不在该表中（该表仅存 phase7-professional-v1 等底层版本）。
        // 显式传入不存在的值会触发 FK 约束，静默失败。
        let snap_id = format!("ns-{}", uuid::Uuid::new_v4());
        let snap_result = sqlx::query(
            "INSERT INTO paper_nav_snapshot
                (nav_snapshot_id, paper_account_id, snapshot_date, nav, cash, market_value,
                 position_count, daily_return, cumulative_return, max_drawdown, trade_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE SET
                nav = EXCLUDED.nav, cash = EXCLUDED.cash, market_value = EXCLUDED.market_value,
                position_count = EXCLUDED.position_count, daily_return = EXCLUDED.daily_return,
                cumulative_return = EXCLUDED.cumulative_return, max_drawdown = EXCLUDED.max_drawdown,
                trade_count = EXCLUDED.trade_count",
        )
        .bind(&snap_id)
        .bind(account_id)
        .bind(date)
        .bind(nav)
        .bind(cash.to_string().parse::<f64>().unwrap_or(0.0))
        .bind(market_value.to_string().parse::<f64>().unwrap_or(0.0))
        .bind(position_count as i32)
        .bind(daily_return)
        .bind(cumulative_return)
        .bind(*max_dd)
        .bind(today_trades as i32)
        .execute(db)
        .await;
        if let Err(e) = snap_result {
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
        let text = format!(
            "## 📈 实盘绩效日报 — {}  \n\n\
             **日期**: {}  \n\n\
             **当日收益**: {:.2}% | **累计收益**: {:.2}%  \n\
             **当前 NAV**: ¥{:.2} | **历史峰值**: ¥{:.2} | **最大回撤**: {:.2}%  \n\
             **当日调仓**: 买入 {} 笔(¥{:.0}) / 卖出 {} 笔(¥{:.0})  \n\
             **累计成交**: {} 笔  \n\
             {}\n\n\
             > 自动生成于 {}",
            name,
            date.format("%Y-%m-%d"),
            daily_return * 100.0,
            cumulative_return * 100.0,
            nav,
            peak_nav,
            max_dd * 100.0,
            buy_n,
            buy_amt,
            sell_n,
            sell_amt,
            total_trades,
            match backtest_deviation {
                Some(dev) if dev.abs() > 0.02 => {
                    format!("**⚠️ 回测偏离**: {:.2}%（超 2% 阈值）  \n", dev * 100.0)
                }
                Some(dev) => format!("**回测偏离**: {:.2}%（正常范围）  \n", dev * 100.0),
                None => "**回测偏离**: 数据未就绪（composite 曲线缺当日，待 T+1 补齐）  \n".to_string(),
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
async fn snapshot_positions_for_all_accounts(db: &PgPool, date: NaiveDate) {
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

        // strategy_version_id 不显式传(同 push_daily_performance_report 的 FK 踩坑说明)。
        let snap_id = format!("ns-{}", uuid::Uuid::new_v4());
        let snap_result = sqlx::query(
            "INSERT INTO paper_nav_snapshot
                (nav_snapshot_id, paper_account_id, snapshot_date, nav, cash, market_value,
                 position_count, daily_return, cumulative_return, trade_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE SET
                nav = EXCLUDED.nav, cash = EXCLUDED.cash, market_value = EXCLUDED.market_value,
                position_count = EXCLUDED.position_count, daily_return = EXCLUDED.daily_return,
                cumulative_return = EXCLUDED.cumulative_return, trade_count = EXCLUDED.trade_count",
        )
        .bind(&snap_id)
        .bind(account_id)
        .bind(date)
        .bind(nav)
        .bind(cash.to_string().parse::<f64>().unwrap_or(0.0))
        .bind(market_value.to_string().parse::<f64>().unwrap_or(0.0))
        .bind(position_count as i32)
        .bind(daily_return)
        .bind(cumulative_return)
        .bind(today_trades as i32)
        .execute(db)
        .await;
        if let Err(e) = snap_result {
            warn!("[scheduler] {} 调仓后快照写入失败: {}", account_id, e);
        }
    }
}

/// 收盘后推送钉钉持仓摘要（所有活跃模拟账号）。
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

