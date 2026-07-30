/// 数据同步路由
use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use chrono::{Duration, NaiveDate};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

#[derive(Debug, serde::Deserialize)]
pub struct AccountDataHealthReq {
    pub user_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]


struct StrategyHealthConfig {
    combo_name: String,
    equity_curve_task_id: String,
    prediction_set_id: Option<String>,
    etf_symbols: Vec<String>,
    signal_source: String,
    prediction_blend_weight: f64,
}

pub(crate) const DEFAULT_MVO_ETFS: &[&str] = &[
    "518880.SH",
    "511010.SH",
    "513500.SH",
    "513100.SH",
    "159980.SZ",
    "159985.SZ",
    "501018.SH",
];



#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventSyncSourceQuality {
    Official,
    AcceptedDerived,
    UnverifiedDerived,
    Missing,
    Unknown,
}



#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataReadinessGate {
    BlockRequiredRed,
    BlockRequiredYellow,
}



async fn write_data_readiness_audit_event(
    db: &sqlx::PgPool,
    account_id: &str,
    operation: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, $2, 'paper_account', $3, 'system', $4, $5)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(format!("data_readiness.{}", status))
    .bind(account_id)
    .bind(format!("{} data readiness {}", operation, status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据门禁审计失败: {}", error))
}



async fn write_data_health_check_audit_event(
    db: &sqlx::PgPool,
    entity_id: &str,
    status: &str,
    report: &Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, 'data_readiness.checked', 'data_health_check', $2, 'system', $3, $4)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(entity_id)
    .bind(format!("account data health check {}", status))
    .bind(report)
    .execute(db)
    .await
    .map(|_| ())
    .map_err(|error| format!("写入数据健康检查审计失败: {}", error))
}



pub async fn check_paper_account_data_readiness(
    db: &sqlx::PgPool,
    account_id: &str,
    range: Option<(NaiveDate, NaiveDate)>,
    gate: DataReadinessGate,
    operation: &str,
) -> Result<Value, String> {
    let account: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id
         FROM paper_account WHERE paper_account_id=$1 AND status='active'",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询账号数据门禁: {}", e))?;
    let (account_id, account_name, strategy_id) =
        account.ok_or_else(|| format!("账号不存在或未激活: {}", account_id))?;
    // 账号未挂策略 → 数据未就绪(不再 fallback "v19",配置化原则)。
    // 该账号在批量门禁中被跳过,不阻塞其他账号。
    let sid = strategy_id.ok_or_else(|| {
        format!("账号 {}({}) 未配置 strategy_version_id,数据未就绪", account_id, account_name)
    })?;

    let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> = sqlx::query_as(
        "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                signal_source, prediction_blend_weight
         FROM strategy_config WHERE strategy_id=$1 AND status='active'",
    )
    .bind(&sid)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("查询策略数据门禁: {}", e))?;

    let checks = if let Some((
        combo_name,
        equity_curve_task_id,
        prediction_set_id,
        etf_symbols,
        signal_source,
        prediction_blend_weight,
    )) = cfg
    {
        let cfg = StrategyHealthConfig {
            combo_name,
            equity_curve_task_id,
            prediction_set_id,
            etf_symbols: parse_etf_symbols(etf_symbols),
            signal_source,
            prediction_blend_weight,
        };
        check_account_deps(db, &account_name, &sid, &cfg, range, true, true).await
    } else {
        vec![check_item(
            &account_name,
            &sid,
            "策略配置",
            "red",
            "strategy_config 未找到激活配置",
            None,
            None,
            Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
        )]
    };

    let red = checks
        .iter()
        .filter(|check| check["level"] == "red")
        .count();
    let yellow = checks
        .iter()
        .filter(|check| check["level"] == "yellow")
        .count();
    let blocked = data_readiness_blocking_checks(&checks, gate);
    let blocking_items = blocked.len();
    let failure_message = if blocking_items == 0 {
        None
    } else {
        Some(data_readiness_failure_message(
            operation,
            &account_name,
            &sid,
            &blocked,
        ))
    };
    let passed = blocking_items == 0;
    let report = json!({
        "operation": operation,
        "mode": if range.is_some() { "range_coverage" } else { "freshness" },
        "account_id": account_id,
        "account": account_name,
        "strategy": sid,
        "gate": match gate {
            DataReadinessGate::BlockRequiredRed => "block_required_red",
            DataReadinessGate::BlockRequiredYellow => "block_required_yellow",
        },
        "passed": passed,
        "red": red,
        "yellow": yellow,
        "blocking_items": blocking_items,
        "checks": checks,
    });

    let audit_status = if passed { "passed" } else { "failed" };
    if let Err(error) =
        write_data_readiness_audit_event(db, &account_id, operation, audit_status, &report).await
    {
        let base_message = failure_message
            .clone()
            .unwrap_or_else(|| format!("{} 数据门禁审计失败", operation));
        return Err(format!("{}; {}", base_message, error));
    }

    if passed {
        Ok(report)
    } else {
        Err(failure_message.unwrap_or_else(|| format!("{} 数据门禁失败", operation)))
    }
}

/// POST /api/v1/quant/data/account-data-health
/// 遍历激活账号(模拟+实盘) → 其策略依赖的加工数据(combo因子/PIT combo/权益曲线/滚动IC) → 红黄绿。


pub async fn account_data_health(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AccountDataHealthReq>,
) -> impl IntoResponse {
    let db = &state.db;
    let range = match (req.start_date.as_deref(), req.end_date.as_deref()) {
        (Some(start), Some(end)) => match (parse_health_date(start), parse_health_date(end)) {
            (Ok(start), Ok(end)) if start <= end => Some((start, end)),
            (Ok(_), Ok(_)) => {
                return Json(json!({"code": 1, "message": "start_date 不能晚于 end_date"}));
            }
            (Err(message), _) | (_, Err(message)) => {
                return Json(json!({"code": 1, "message": message}));
            }
        },
        _ => None,
    };
    let deep = range.is_some();
    let mut checks: Vec<serde_json::Value> = Vec::new();

    // 活跃账号(可选按 user 过滤)
    let accounts: Vec<(String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT paper_account_id, name, strategy_version_id, COALESCE(leverage_enabled,false)
         FROM paper_account WHERE status='active'
           AND ($1::text IS NULL OR user_id = $1)
         ORDER BY name",
    )
    .bind(req.user_id.as_deref())
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let mut cfg_cache: BTreeMap<String, Option<StrategyHealthConfig>> = BTreeMap::new();
    let mut deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut common_deps_cache: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for (_acct_id, acct_name, strat_id, _lev) in &accounts {
        // 账号未挂策略 → 跳过(不再 fallback "v19",配置化原则)。
        // 健康检查不因单个无策略账号终止。
        let sid = match strat_id.as_deref() {
            Some(s) if !s.is_empty() => s,
            _ => {
                tracing::error!(
                    "[health] 账号 {} 未配置 strategy_version_id,跳过健康检查",
                    acct_name
                );
                continue;
            }
        };
        if !cfg_cache.contains_key(sid) {
            let cfg: Option<(String, String, Option<String>, Option<Value>, String, f64)> =
                sqlx::query_as(
                    "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols,
                        signal_source, prediction_blend_weight
                 FROM strategy_config WHERE strategy_id=$1 AND status='active'",
                )
                .bind(sid)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
            cfg_cache.insert(
                sid.to_string(),
                cfg.map(
                    |(
                        combo_name,
                        equity_curve_task_id,
                        prediction_set_id,
                        etf_symbols,
                        signal_source,
                        prediction_blend_weight,
                    )| {
                        StrategyHealthConfig {
                            combo_name,
                            equity_curve_task_id,
                            prediction_set_id,
                            etf_symbols: parse_etf_symbols(etf_symbols),
                            signal_source,
                            prediction_blend_weight,
                        }
                    },
                ),
            );
        }

        let Some(Some(cfg)) = cfg_cache.get(sid) else {
            checks.push(check_item(
                acct_name,
                sid,
                "策略配置",
                "red",
                "strategy_config 未找到激活配置",
                None,
                None,
                Some("缺少策略配置，无法自动判断应修复哪些数据".to_string()),
            ));
            continue;
        };

        let mut cached_checks = Vec::new();
        if let Some((start, end)) = range {
            let common_key = format!("{}|{}|{}", start, end, cfg.etf_symbols.join(","));
            if !common_deps_cache.contains_key(&common_key) {
                common_deps_cache.insert(
                    common_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, false)
                        .await,
                );
            }
            if let Some(common_checks) = common_deps_cache.get(&common_key) {
                cached_checks.extend(common_checks.iter().cloned());
            }

            let deps_key = format!(
                "{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or("")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, false, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        } else {
            let deps_key = format!(
                "{}|{}|{}|{}",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.prediction_set_id.as_deref().unwrap_or(""),
                cfg.etf_symbols.join(",")
            );
            if !deps_cache.contains_key(&deps_key) {
                deps_cache.insert(
                    deps_key.clone(),
                    check_account_deps(db, "__ACCOUNT__", "__STRATEGY__", cfg, range, true, true)
                        .await,
                );
            }
            if let Some(strategy_checks) = deps_cache.get(&deps_key) {
                cached_checks.extend(strategy_checks.iter().cloned());
            }
        }

        checks.extend(cached_checks.into_iter().map(|mut check| {
            check["account"] = json!(acct_name);
            check["strategy"] = json!(sid);
            if let Some(params) = check.get_mut("fix_params") {
                if params.get("strategy_id").and_then(|v| v.as_str()).is_some() {
                    params["strategy_id"] = json!(sid);
                }
            }
            check
        }));
    }

    let red = checks.iter().filter(|c| c["level"] == "red").count();
    let yellow = checks.iter().filter(|c| c["level"] == "yellow").count();
    let status = if red > 0 {
        "red"
    } else if yellow > 0 {
        "yellow"
    } else {
        "green"
    };
    let mut data = serde_json::json!({
        "mode": if deep {"range_coverage"} else {"freshness"},
        "accounts_checked": accounts.len(),
        "red": red,
        "yellow": yellow,
        "checks": checks
    });
    let audit_report = json!({
        "operation": "account_data_health",
        "status": status,
        "user_id": req.user_id,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "data": data.clone()
    });
    let audit_entity_id = audit_report
        .get("user_id")
        .and_then(|value| value.as_str())
        .unwrap_or("all_active_accounts");
    match write_data_health_check_audit_event(db, audit_entity_id, status, &audit_report).await {
        Ok(()) => data["audit_persisted"] = json!(true),
        Err(error) => {
            data["audit_persisted"] = json!(false);
            data["audit_error"] = json!(error);
        }
    }

    Json(serde_json::json!({
        "code": 0,
        "data": data
    }))
}



async fn latest_market_date(db: &sqlx::PgPool) -> NaiveDate {
    sqlx::query_scalar::<_, Option<NaiveDate>>(
        "SELECT GREATEST(
            (SELECT MAX(trade_date) FROM market_stock_daily_bar_adj),
            (SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH')
        )",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .flatten()
    .unwrap_or_else(|| chrono::Utc::now().date_naive())
}



fn strategy_needs_prediction(cfg: &StrategyHealthConfig) -> bool {
    matches!(
        cfg.signal_source.as_str(),
        "prediction" | "prediction_blend"
    ) && (cfg.signal_source == "prediction" || cfg.prediction_blend_weight > f64::EPSILON)
}



async fn resolve_live_prediction_set(
    db: &sqlx::PgPool,
    cfg: &StrategyHealthConfig,
    date: NaiveDate,
) -> Option<String> {
    if !strategy_needs_prediction(cfg) {
        return None;
    }
    if let Some(prediction_set_id) = cfg.prediction_set_id.as_ref() {
        return Some(prediction_set_id.clone());
    }
    sqlx::query_scalar(
        "SELECT ps.prediction_set_id FROM prediction_set ps
         WHERE ps.status = 'ready'
           AND ps.training_end_date IS NOT NULL
           AND ps.training_end_date < $1
           AND ps.start_date <= $1 AND ps.end_date >= $1
         ORDER BY ps.training_end_date DESC, ps.created_at DESC
         LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}



async fn expected_open_day_count(db: &sqlx::PgPool, start: NaiveDate, end: NaiveDate) -> i64 {
    let calendar_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_trade_calendar
         WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true",
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0);
    if calendar_count > 0 {
        calendar_count
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
        )
        .bind(start)
        .bind(end)
        .fetch_one(db)
        .await
        .unwrap_or(0)
    }
}



async fn event_completion_latest_source(
    db: &sqlx::PgPool,
    table: &str,
    task_type: &str,
) -> (Option<NaiveDate>, Option<String>, i64) {
    let table_sql = format!(
        "SELECT MAX(trade_date), COUNT(*)::int8
         FROM {table}
         WHERE trade_date = (SELECT MAX(trade_date) FROM {table})"
    );
    let (table_date, table_rows): (Option<NaiveDate>, i64) = sqlx::query_as(&table_sql)
        .fetch_one(db)
        .await
        .unwrap_or((None, 0));

    let task_row: Option<(NaiveDate, Option<String>, i64)> = sqlx::query_as(
        "SELECT end_date, source, COALESCE(success_count, total_count, 0)::int8
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
         ORDER BY end_date DESC, completed_at DESC NULLS LAST
         LIMIT 1",
    )
    .bind(task_type)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    match (table_date, task_row) {
        (Some(td), Some((task_date, source, rows))) if task_date >= td => {
            (Some(task_date), source, rows)
        }
        (Some(td), _) => (Some(td), None, table_rows),
        (None, Some((task_date, source, rows))) => (Some(task_date), source, rows),
        (None, None) => (None, None, 0),
    }
}



async fn event_completed_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT end_date)::int8 FROM data_sync_task
         WHERE task_type = $1 AND status = 'completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or(0)
}



async fn event_verified_day_count(
    db: &sqlx::PgPool,
    task_type: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> i64 {
    let rows: Vec<(NaiveDate, Option<String>)> = sqlx::query_as(
        "SELECT DISTINCT end_date, source
         FROM data_sync_task
         WHERE task_type=$1 AND status='completed'
           AND end_date >= $2 AND end_date <= $3",
    )
    .bind(task_type)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    rows.into_iter()
        .filter(|(date, source)| {
            matches!(
                event_sync_source_quality(task_type, source.as_deref(), *date),
                EventSyncSourceQuality::Official | EventSyncSourceQuality::AcceptedDerived
            )
        })
        .map(|(date, _)| date)
        .collect::<std::collections::BTreeSet<_>>()
        .len() as i64
}



async fn rolling_pit_ic_quarter_coverage(
    db: &sqlx::PgPool,
    horizon: i16,
    start: NaiveDate,
    end: NaiveDate,
) -> (i64, i64, Option<NaiveDate>, Option<NaiveDate>) {
    sqlx::query_as(
        "WITH quarters AS (
           SELECT MIN(trade_date) AS as_of
           FROM (SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
                 WHERE trade_date >= $2 AND trade_date <= $3) d
           GROUP BY date_trunc('quarter', trade_date)
         ),
         covered AS (
           SELECT q.as_of
           FROM quarters q
           WHERE EXISTS (
             SELECT 1
             FROM factor_evaluation fe
             WHERE fe.horizon = $1
               AND fe.end_date <= q.as_of
               AND fe.mean_ic IS NOT NULL
               AND fe.ic_ir IS NOT NULL
               AND fe.factor_code !~ '^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)'
           )
         )
         SELECT COUNT(q.as_of)::int8,
                COUNT(c.as_of)::int8,
                MIN(q.as_of) FILTER (WHERE c.as_of IS NULL),
                MAX(q.as_of) FILTER (WHERE c.as_of IS NULL)
         FROM quarters q
         LEFT JOIN covered c USING (as_of)",
    )
    .bind(horizon)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .unwrap_or((0, 0, None, None))
}

/// 检查单个账号策略依赖的数据。range=None 查新鲜度；range=Some 查区间覆盖率。


async fn check_account_deps(
    db: &sqlx::PgPool,
    acct: &str,
    sid: &str,
    cfg: &StrategyHealthConfig,
    range: Option<(NaiveDate, NaiveDate)>,
    include_common: bool,
    include_strategy: bool,
) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let last_mkt = latest_market_date(db).await;
    let repair_start = yyyymmdd(last_mkt - Duration::days(30));
    let repair_end = yyyymmdd(last_mkt);

    if include_strategy {
        out.push(check_item(
            acct,
            sid,
            "策略配置",
            "green",
            format!(
                "combo={}, curve={}, signal_source={}, prediction_set={}, blend_weight={}, ETF{}只",
                cfg.combo_name,
                cfg.equity_curve_task_id,
                cfg.signal_source,
                cfg.prediction_set_id.as_deref().unwrap_or("自动选择"),
                cfg.prediction_blend_weight,
                cfg.etf_symbols.len()
            ),
            None,
            None,
            None,
        ));
    }

    if let Some((start, end)) = range {
        let expected_days = expected_open_day_count(db, start, end).await;

        if include_common {
            let (a_expected_days, a_days, a_min_expected, a_min_symbols, a_weak_days): (
                i64,
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               LEFT JOIN market_stock_suspension susp
                 ON susp.symbol = s.symbol
                AND susp.trade_date = c.trade_date
                AND COALESCE(susp.suspend_type, 'S') = 'S'
               WHERE susp.symbol IS NULL
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_stock_daily_bar_adj
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_level = if a_days < a_expected_days || a_expected_days < expected_days {
                "red"
            } else if a_weak_days > 0 {
                "yellow"
            } else {
                "green"
            };
            let (a_fix_endpoint, a_fix_params) =
                if a_days < a_expected_days || a_expected_days < expected_days {
                    (
                        Some("/api/v1/quant/data/sync/daily/background"),
                        Some(json!({
                            "symbols": [],
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "data_version_id": format!("health-repair-daily-{}", yyyymmdd(end))
                        })),
                    )
                } else if a_weak_days > 0 {
                    (
                        Some("/api/v1/quant/data/sync/suspension/derive-from-daily"),
                        Some(json!({
                            "start_date": yyyymmdd(start),
                            "end_date": yyyymmdd(end),
                            "force_tushare": false
                        })),
                    )
                } else {
                    (None, None)
                };
            out.push(check_item(
                acct,
                sid,
                "A股日线区间覆盖",
                a_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市/停牌口径单日最少{}/{}只，低于90%天数{}",
                    start, end, expected_days, a_days, a_min_symbols, a_min_expected, a_weak_days
                ),
                a_fix_endpoint,
                a_fix_params,
                None,
            ));

            let (
                a_adj_expected_days,
                a_adj_days,
                a_adj_min_expected,
                a_adj_min_symbols,
                a_adj_weak_days,
            ): (i64, i64, i64, i64, i64) = sqlx::query_as(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             ),
             actual AS (
               SELECT trade_date, COUNT(DISTINCT symbol)::int8 AS actual_symbols
               FROM market_adjustment_factor
               WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                 AND trade_date >= $1 AND trade_date <= $2
               GROUP BY trade_date
             )
             SELECT COUNT(e.trade_date)::int8,
                    COUNT(a.trade_date)::int8,
                    COALESCE(MIN(e.expected_symbols), 0)::int8,
                    COALESCE(MIN(a.actual_symbols), 0)::int8,
                    COUNT(*) FILTER (
                      WHERE COALESCE(a.actual_symbols, 0) * 100 < e.expected_symbols * 90
                    )::int8
             FROM expected e
             LEFT JOIN actual a USING(trade_date)",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0, 0));
            let a_adj_level =
                if a_adj_days < a_adj_expected_days || a_adj_expected_days < expected_days {
                    "red"
                } else if a_adj_weak_days > 0 {
                    "yellow"
                } else {
                    "green"
                };
            out.push(check_item(
                acct,
                sid,
                "A股复权因子区间覆盖",
                a_adj_level,
                format!(
                    "{}~{} 期望{}个交易日，覆盖{}天；按上市/退市口径单日最少{}/{}只，低于90%天数{}",
                    start,
                    end,
                    expected_days,
                    a_adj_days,
                    a_adj_min_symbols,
                    a_adj_min_expected,
                    a_adj_weak_days
                ),
                Some("/api/v1/quant/data/sync/adj-factor/background"),
                Some(json!({
                    "symbols": [],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let (etf_expected, etf_actual, etf_bad, etf_confirmed_absent): (
                i64,
                i64,
                i64,
                i64,
            ) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
                 firsts AS (
                   SELECT symbol, MIN(trade_date) AS first_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                   GROUP BY symbol
                 ),
                 calendar AS (
                   SELECT DISTINCT trade_date
                   FROM market_trade_calendar
                   WHERE is_open = true
                     AND trade_date >= $2::date
                     AND trade_date <= $3::date
                 ),
                 expected AS (
                   SELECT s.symbol, c.trade_date
                   FROM symbols s
                   LEFT JOIN firsts f ON f.symbol = s.symbol
                   JOIN calendar c
                     ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
                 ),
                 actual AS (
                   SELECT DISTINCT symbol, trade_date
                   FROM market_stock_daily_bar_adj
                   WHERE symbol = ANY($1::text[])
                     AND trade_date >= $2 AND trade_date <= $3
                 ),
                 confirmed_absent AS (
                   SELECT e.symbol, e.trade_date
                   FROM expected e
                   JOIN data_sync_attempt attempt
                     ON attempt.source = 'fund_daily'
                    AND attempt.symbol = e.symbol
                    AND attempt.status = 'completed'
                    AND attempt.row_count = 0
                    AND attempt.start_date = e.trade_date
                    AND attempt.end_date = e.trade_date
                 )
                 SELECT COUNT(*)::int8,
                        COUNT(a.trade_date)::int8,
                        COUNT(DISTINCT e.symbol) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NULL
                        )::int8,
                        COUNT(*) FILTER (
                          WHERE a.trade_date IS NULL AND ca.trade_date IS NOT NULL
                        )::int8
                 FROM expected e
                 LEFT JOIN actual a ON a.symbol = e.symbol AND a.trade_date = e.trade_date
                 LEFT JOIN confirmed_absent ca ON ca.symbol = e.symbol AND ca.trade_date = e.trade_date",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0, 0));
            let etf_missing = (etf_expected - etf_actual).max(0);
            let etf_level = if etf_bad == 0 {
                "green"
            } else if etf_missing <= 5 {
                "yellow"
            } else {
                "red"
            };
            let (etf_fix_endpoint, etf_fix_params, etf_fix_reason) = if etf_level == "red" {
                (
                    Some("/api/v1/quant/data/sync/fund-daily"),
                    Some(json!({
                        "symbols": cfg.etf_symbols,
                        "start_date": yyyymmdd(start),
                        "end_date": yyyymmdd(end),
                        "data_version_id": format!("health-repair-etf-daily-{}", yyyymmdd(end))
                    })),
                    None,
                )
            } else if etf_level == "yellow" {
                (
                    None,
                    None,
                    Some(
                        "少量 ETF/QDII 日线缺口通常来自基金非交易日或上游空值；已尝试同步仍为空时不应静默补假价格"
                            .to_string(),
                    ),
                )
            } else {
                (None, None, None)
            };
            out.push(check_item(
                acct,
                sid,
                "ETF日线区间覆盖(MVO)",
                etf_level,
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口{}个 symbol-day/{}只ETF，其中{}个 symbol-day 已确认源端无行情",
                    cfg.etf_symbols.len(),
                    etf_expected,
                    etf_actual,
                    etf_missing,
                    etf_bad,
                    etf_confirmed_absent
                ),
                etf_fix_endpoint,
                etf_fix_params,
                etf_fix_reason,
            ));

            let (etf_adj_expected, etf_adj_actual, etf_adj_bad): (i64, i64, i64) = sqlx::query_as(
                "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
             firsts AS (
               SELECT symbol, MIN(trade_date) AS first_date
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
               GROUP BY symbol
             ),
             calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true
                 AND trade_date >= $2::date
                 AND trade_date <= $3::date
             ),
             expected AS (
               SELECT s.symbol, COUNT(DISTINCT c.trade_date)::int8 AS expected_days
               FROM symbols s
               LEFT JOIN firsts f ON f.symbol = s.symbol
               LEFT JOIN calendar c
                 ON c.trade_date >= GREATEST($2::date, COALESCE(f.first_date, $2::date))
               GROUP BY s.symbol
             ),
             actual AS (
               SELECT symbol, COUNT(DISTINCT trade_date)::int8 AS actual_days
               FROM market_adjustment_factor
               WHERE symbol = ANY($1::text[])
                 AND trade_date >= $2 AND trade_date <= $3
               GROUP BY symbol
             )
             SELECT COALESCE(SUM(e.expected_days), 0)::int8,
                    COALESCE(SUM(COALESCE(a.actual_days, 0)), 0)::int8,
                    COUNT(*) FILTER (WHERE COALESCE(a.actual_days, 0) < e.expected_days)::int8
             FROM expected e
             LEFT JOIN actual a ON a.symbol = e.symbol",
            )
            .bind(&cfg.etf_symbols)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or((0, 0, 0));
            out.push(check_item(
                acct,
                sid,
                "ETF复权因子区间覆盖(MVO)",
                if etf_adj_bad == 0 { "green" } else { "red" },
                format!(
                    "{}只ETF，期望{}个 symbol-day，覆盖{}，缺口ETF数{}",
                    cfg.etf_symbols.len(),
                    etf_adj_expected,
                    etf_adj_actual,
                    etf_adj_bad
                ),
                Some("/api/v1/quant/data/sync/fund-adj"),
                Some(json!({
                    "symbols": cfg.etf_symbols,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-etf-adj-{}", yyyymmdd(end))
                })),
                None,
            ));

            let stock_basic_days: i64 = sqlx::query_scalar(
                "WITH calendar AS (
               SELECT DISTINCT trade_date
               FROM market_trade_calendar
               WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
             ),
             expected AS (
               SELECT c.trade_date, COUNT(DISTINCT s.symbol)::int8 AS expected_symbols
               FROM calendar c
               JOIN market_stock s
                 ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                AND s.list_date IS NOT NULL
                AND s.list_date <= c.trade_date
                AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
               GROUP BY c.trade_date
             )
             SELECT COUNT(*)::int8 FROM expected WHERE expected_symbols > 0",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
            acct,
            sid,
            "股票基础信息区间覆盖",
            coverage_level(expected_days, stock_basic_days),
            format!(
                "market_stock 按 list_date/delist_date 可覆盖{}个交易日，期望{}天",
                stock_basic_days, expected_days
            ),
            Some("/api/v1/quant/data/sync/stock-basic"),
            Some(
                json!({"data_version_id": format!("health-repair-stock-basic-{}", yyyymmdd(end))}),
            ),
            None,
        ));

            let csi_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM market_index_daily_bar
             WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
            )
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "CSI300区间覆盖",
                coverage_level(expected_days, csi_days),
                format!("期望{}个交易日，覆盖{}天", expected_days, csi_days),
                Some("/api/v1/quant/data/sync/index-daily"),
                Some(json!({
                    "index_codes": ["000300.SH"],
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "data_version_id": format!("health-repair-csi300-{}", yyyymmdd(end))
                })),
                None,
            ));
        }

        if include_strategy {
            let (ic_quarters, ic_covered, ic_missing_min, ic_missing_max) =
                rolling_pit_ic_quarter_coverage(db, 20, start, end).await;
            let ic_level = coverage_level(ic_quarters, ic_covered);
            out.push(check_item(
                acct,
                sid,
                "滚动IC区间覆盖(PIT权重)",
                ic_level,
                format!(
                    "{}~{} 期望{}个季度as-of，PIT IC可用{}个；缺失as-of范围{}~{}",
                    start,
                    end,
                    ic_quarters,
                    ic_covered,
                    ic_missing_min
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string()),
                    ic_missing_max
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "无".to_string())
                ),
                Some("/api/v1/quant/factors/evaluate-rolling-pit/background"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "version": "1.0.0",
                    "horizon": 20,
                    "train_lookback_days": 756
                })),
                None,
            ));

            let combo_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM multi_factor_value
             WHERE combo_name=$1 AND version='1.0.0'
               AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            let combo_future_leak: bool = sqlx::query_scalar(
                "SELECT EXISTS(
               SELECT 1 FROM multi_factor_value
               WHERE combo_name=$1 AND version='1.0.0'
                 AND trade_date >= $2 AND trade_date <= $3
                 AND available_at > trade_date
               LIMIT 1
             )",
            )
            .bind(&cfg.combo_name)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(false);
            let combo_level = if combo_future_leak {
                "red"
            } else {
                coverage_level(expected_days, combo_days)
            };
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo区间覆盖({})", cfg.combo_name),
                combo_level,
                format!(
                    "期望{}个交易日，PIT覆盖{}天，future available_at={}",
                    expected_days, combo_days, combo_future_leak
                ),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({
                    "combo_name": cfg.combo_name,
                    "version": "1.0.0",
                    "horizon": 20,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            let curve_days: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT trade_date)::int8 FROM backtest_equity_curve
             WHERE task_id=$1 AND trade_date >= $2 AND trade_date <= $3",
            )
            .bind(&cfg.equity_curve_task_id)
            .bind(start)
            .bind(end)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线区间覆盖",
                coverage_level(expected_days, curve_days),
                format!(
                    "期望{}个交易日，覆盖{}天，task={}",
                    expected_days, curve_days, cfg.equity_curve_task_id
                ),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({
                    "name": "权益曲线",
                    "strategy_id": sid,
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end)
                })),
                None,
            ));

            if strategy_needs_prediction(cfg) && cfg.prediction_set_id.is_none() {
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    "red",
                    format!(
                        "strategy signal_source={} 需要 ML，但未固定 prediction_set_id；历史回放不可复现",
                        cfg.signal_source
                    ),
                    None,
                    None,
                    Some(
                        "历史回放必须绑定 PIT prediction_set_id；不能用运行时最新预测集替代"
                            .to_string(),
                    ),
                ));
            } else if let Some(pred_set) = cfg
                .prediction_set_id
                .as_deref()
                .filter(|_| strategy_needs_prediction(cfg))
            {
                let ps_ready: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $3
                   AND training_end_date IS NOT NULL
                   AND training_end_date < $2
                 )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                let ml_days: i64 = sqlx::query_scalar(
                    "SELECT COUNT(DISTINCT trade_date)::int8 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND COALESCE(available_at, trade_date) <= trade_date",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(0);
                let ml_future_leak: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                   SELECT 1 FROM model_prediction
                 WHERE prediction_set_id=$1 AND trade_date >= $2 AND trade_date <= $3
                   AND available_at > trade_date
                 LIMIT 1
                )",
                )
                .bind(pred_set)
                .bind(start)
                .bind(end)
                .fetch_one(db)
                .await
                .unwrap_or(false);
                out.push(check_item(
                    acct,
                    sid,
                    "ML预测集区间覆盖",
                    if !ps_ready || ml_future_leak {
                        "red"
                    } else {
                        coverage_level(expected_days, ml_days)
                    },
                    format!(
                        "期望{}个交易日，覆盖{}天，prediction_set_ready={}，future available_at={}，set={}",
                        expected_days, ml_days, ps_ready, ml_future_leak, pred_set
                    ),
                    None,
                    None,
                    Some(
                        "预测集由训练/预测流水线生成，不能用单点补数安全修复；需重建 PIT 预测集"
                            .to_string(),
                    ),
                ));
            }
        }

        if include_common {
            let suspension_completed =
                event_completed_day_count(db, "suspension_daily", start, end).await;
            let suspension_verified =
                event_verified_day_count(db, "suspension_daily", start, end).await;
            out.push(check_item(
                acct,
                sid,
                "停牌同步完成标记",
                coverage_level(expected_days, suspension_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；事件表空行和 derived 标记不能证明零停牌",
                    expected_days, suspension_verified, suspension_completed
                ),
                Some("/api/v1/quant/data/sync/suspension/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let limit_earliest = proven_limit_list_earliest_date();
            let limit_completed = event_completed_day_count(db, "limit_daily", start, end).await;
            let limit_verified = event_verified_day_count(db, "limit_daily", start, end).await;
            let limit_missing_source_note = if start < limit_earliest {
                format!(
                    "；{} 前 Tushare 不提供 limit_list_d，系统需用日线 close/pre_close 按交易规则派生",
                    limit_earliest
                )
            } else {
                String::new()
            };
            out.push(check_item(
                acct,
                sid,
                "涨跌停同步完成标记",
                coverage_level(expected_days, limit_verified),
                format!(
                    "期望{}个交易日可信完成，可信{}天/完成标记{}天；2019-11-28后必须有 Tushare 来源，事件表空行不能证明零涨跌停{}",
                    expected_days, limit_verified, limit_completed, limit_missing_source_note
                ),
                Some("/api/v1/quant/data/sync/limit/backfill"),
                Some(json!({
                    "start_date": yyyymmdd(start),
                    "end_date": yyyymmdd(end),
                    "force_tushare": true
                })),
                None,
            ));

            let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
            out.push(check_item(
                acct,
                sid,
                "数据同步调度任务",
                if scheduler_issues.is_empty() {
                    "green"
                } else {
                    "red"
                },
                if scheduler_issues.is_empty() {
                    "启用调度任务 CRON/依赖顺序检查通过".to_string()
                } else {
                    format!(
                        "调度任务问题{}项: {}",
                        scheduler_issues.len(),
                        scheduler_issues.join("；")
                    )
                },
                None,
                None,
                if scheduler_issues.is_empty() {
                    None
                } else {
                    Some(
                        "请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效"
                            .to_string(),
                    )
                },
            ));

            let st_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::int8 FROM market_stock_name_history
             WHERE start_date <= $1 AND (end_date IS NULL OR end_date >= $2)",
            )
            .bind(end)
            .bind(start)
            .fetch_one(db)
            .await
            .unwrap_or(0);
            out.push(check_item(
                acct,
                sid,
                "ST/名称历史",
                if st_count > 0 { "green" } else { "red" },
                format!("区间相交名称历史/ST记录{}条", st_count),
                Some("/api/v1/quant/data/sync/namechange"),
                Some(json!({})),
                None,
            ));
        }

        return out;
    }

    let active_stock_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::int8
         FROM market_stock s
         LEFT JOIN market_stock_suspension susp
           ON susp.symbol = s.symbol
          AND susp.trade_date = $1
          AND COALESCE(susp.suspend_type, 'S') = 'S'
         WHERE s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
           AND s.list_date IS NOT NULL
           AND s.list_date <= $1
           AND (s.delist_date IS NULL OR s.delist_date >= $1)
           AND susp.symbol IS NULL",
    )
    .bind(last_mkt)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    let (a_last, a_symbols): (Option<NaiveDate>, i64) = sqlx::query_as(
        "WITH latest AS (
           SELECT MAX(trade_date) AS trade_date FROM market_stock_daily_bar_adj
           WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         )
         SELECT latest.trade_date,
                COUNT(DISTINCT b.symbol)::int8
         FROM latest
         LEFT JOIN market_stock_daily_bar_adj b
           ON b.trade_date = latest.trade_date
          AND b.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         GROUP BY latest.trade_date",
    )
    .fetch_one(db)
    .await
    .unwrap_or((None, 0));
    let a_lag = a_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let a_partial = active_stock_count > 0 && a_symbols * 100 < active_stock_count * 80;
    out.push(check_item(
        acct,
        sid,
        "A股日线",
        if a_partial {
            "red"
        } else {
            lag_level(a_lag, 2, 7)
        },
        format!(
            "最新{}，落后{}天，最新日{}只/按上市退市停牌口径应有{}只",
            a_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            a_lag,
            a_symbols,
            active_stock_count
        ),
        Some("/api/v1/quant/data/sync/daily/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-daily-{}", repair_end)
        })),
        None,
    ));

    let (etf_present, etf_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_stock_daily_bar_adj
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_lag = etf_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF日线(MVO)",
        if etf_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_lag, 2, 7)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_lag
        ),
        Some("/api/v1/quant/data/sync/fund-daily"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-daily-{}", repair_end)
        })),
        None,
    ));

    let adj_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_adjustment_factor
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let adj_lag = adj_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "A股复权因子",
        lag_level(adj_lag, 30, 90),
        format!(
            "最新{}，落后{}天",
            adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            adj_lag
        ),
        Some("/api/v1/quant/data/sync/adj-factor/background"),
        Some(json!({
            "symbols": [],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-adj-{}", repair_end)
        })),
        None,
    ));

    let (etf_adj_present, etf_adj_last): (i64, Option<NaiveDate>) = sqlx::query_as(
        "WITH symbols AS (SELECT unnest($1::text[]) AS symbol),
         latest AS (
           SELECT symbol, MAX(trade_date) AS max_date
           FROM market_adjustment_factor
           WHERE symbol = ANY($1::text[])
           GROUP BY symbol
         )
         SELECT COUNT(latest.max_date)::int8, MIN(latest.max_date)
         FROM symbols LEFT JOIN latest USING(symbol)",
    )
    .bind(&cfg.etf_symbols)
    .fetch_one(db)
    .await
    .unwrap_or((0, None));
    let etf_adj_lag = etf_adj_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "ETF复权因子(MVO)",
        if etf_adj_present < cfg.etf_symbols.len() as i64 {
            "red"
        } else {
            lag_level(etf_adj_lag, 30, 90)
        },
        format!(
            "{}只ETF，全部最新最早{}，落后{}天",
            cfg.etf_symbols.len(),
            etf_adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            etf_adj_lag
        ),
        Some("/api/v1/quant/data/sync/fund-adj"),
        Some(json!({
            "symbols": cfg.etf_symbols,
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-etf-adj-{}", repair_end)
        })),
        None,
    ));

    let csi_last: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH'",
    )
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    let csi_lag = csi_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    out.push(check_item(
        acct,
        sid,
        "CSI300",
        lag_level(csi_lag, 2, 7),
        format!(
            "最新{}，落后{}天",
            csi_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            csi_lag
        ),
        Some("/api/v1/quant/data/sync/index-daily"),
        Some(json!({
            "index_codes": ["000300.SH"],
            "start_date": repair_start,
            "end_date": repair_end,
            "data_version_id": format!("health-repair-csi300-{}", repair_end)
        })),
        None,
    ));

    let stock_basic_level = if active_stock_count >= 3000 {
        "green"
    } else {
        "red"
    };
    out.push(check_item(
        acct,
        sid,
        "股票基础信息",
        stock_basic_level,
        format!("当前上市A股{}只", active_stock_count),
        Some("/api/v1/quant/data/sync/stock-basic"),
        Some(json!({"data_version_id": format!("health-repair-stock-basic-{}", repair_end)})),
        None,
    ));

    let st_count: i64 = sqlx::query_scalar("SELECT COUNT(*)::int8 FROM market_stock_name_history")
        .fetch_one(db)
        .await
        .unwrap_or(0);
    out.push(check_item(
        acct,
        sid,
        "ST/名称历史",
        if st_count > 0 { "green" } else { "red" },
        format!("名称历史/ST记录{}条", st_count),
        Some("/api/v1/quant/data/sync/namechange"),
        Some(json!({})),
        None,
    ));

    let (suspension_last, suspension_source, suspension_rows) =
        event_completion_latest_source(db, "market_stock_suspension", "suspension_daily").await;
    let suspension_lag = suspension_last
        .map(|d| (last_mkt - d).num_days())
        .unwrap_or(999);
    let suspension_source_level = suspension_last
        .map(|d| event_sync_source_level("suspension_daily", suspension_source.as_deref(), d))
        .unwrap_or("red");
    let suspension_level = if suspension_source_level == "green" {
        lag_level(suspension_lag, 2, 7)
    } else {
        suspension_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "停牌",
        suspension_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}",
            suspension_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            suspension_lag,
            suspension_source.as_deref().unwrap_or("无"),
            suspension_last
                .map(|d| event_sync_source_label(
                    "suspension_daily",
                    suspension_source.as_deref(),
                    d
                ))
                .unwrap_or("missing"),
            suspension_rows
        ),
        Some("/api/v1/quant/data/sync/suspension"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let (limit_last, limit_source, limit_rows) =
        event_completion_latest_source(db, "market_stock_limit", "limit_daily").await;
    let limit_lag = limit_last.map(|d| (last_mkt - d).num_days()).unwrap_or(999);
    let limit_source_level = limit_last
        .map(|d| event_sync_source_level("limit_daily", limit_source.as_deref(), d))
        .unwrap_or("red");
    let limit_level = if limit_source_level == "green" {
        lag_level(limit_lag, 2, 7)
    } else {
        limit_source_level
    };
    out.push(check_item(
        acct,
        sid,
        "涨跌停",
        limit_level,
        format!(
            "最新完成/事件日期{}，落后{}天，source={}，quality={}，rows={}；2019-11-28前历史需由日线按交易规则派生",
            limit_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string()),
            limit_lag,
            limit_source.as_deref().unwrap_or("无"),
            limit_last
                .map(|d| event_sync_source_label("limit_daily", limit_source.as_deref(), d))
                .unwrap_or("missing"),
            limit_rows
        ),
        Some("/api/v1/quant/data/sync/limit"),
        Some(json!({"trade_date": repair_end})),
        None,
    ));

    let scheduler_issues = crate::routes::scheduler::check_task_dependency_order(db).await;
    out.push(check_item(
        acct,
        sid,
        "数据同步调度任务",
        if scheduler_issues.is_empty() {
            "green"
        } else {
            "red"
        },
        if scheduler_issues.is_empty() {
            "启用调度任务 CRON/依赖顺序检查通过".to_string()
        } else {
            format!(
                "调度任务问题{}项: {}",
                scheduler_issues.len(),
                scheduler_issues.join("；")
            )
        },
        None,
        None,
        if scheduler_issues.is_empty() {
            None
        } else {
            Some("请在调度任务配置页修正 CRON 或依赖顺序；系统不会用错误调度生成绩效".to_string())
        },
    ));

    // PIT combo 物化新鲜度
    let combo_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name=$1 AND version='1.0.0'
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(&cfg.combo_name)
    .fetch_one(db)
    .await
    .ok()
    .flatten();
    match combo_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                format!("PIT combo物化({})", cfg.combo_name),
                lag_level(lag, 2, 7),
                format!("最新 {} (落后行情 {} 天)", d, lag),
                Some("/api/v1/quant/factors/materialize-pit-combo/background"),
                Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            format!("PIT combo物化({})", cfg.combo_name),
            "red",
            "combo 无任何 PIT 合规物化数据",
            Some("/api/v1/quant/factors/materialize-pit-combo/background"),
            Some(json!({"combo_name": cfg.combo_name, "version":"1.0.0", "horizon":20, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 权益曲线新鲜度
    let curve_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM backtest_equity_curve WHERE task_id=$1")
            .bind(&cfg.equity_curve_task_id)
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    match curve_last {
        Some(d) => {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "A股权益曲线",
                lag_level(lag, 4, 10),
                format!("最新 {} (落后 {} 天, task={})", d, lag, cfg.equity_curve_task_id),
                Some("/api/v1/admin/sync/repair"),
                Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
                None,
            ));
        }
        None => out.push(check_item(
            acct,
            sid,
            "A股权益曲线",
            "red",
            format!("曲线 {} 无数据", cfg.equity_curve_task_id),
            Some("/api/v1/admin/sync/repair"),
            Some(json!({"name": "权益曲线", "strategy_id": sid, "start_date": repair_start, "end_date": repair_end})),
            None,
        )),
    }

    // 滚动 IC 新鲜度
    let ic_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(end_date) FROM factor_evaluation WHERE horizon=20")
            .fetch_one(db)
            .await
            .ok()
            .flatten();
    if let Some(d) = ic_last {
        let lag = (last_mkt - d).num_days();
        out.push(check_item(
            acct,
            sid,
            "滚动IC窗口",
            if lag > 100 { "yellow" } else { "green" },
            format!("最新IC窗口 {} (距今 {} 天)", d, lag),
            Some("/api/v1/quant/factors/evaluate-all/background"),
            Some(json!({})),
            None,
        ));
    }

    // ML 预测集新鲜度(策略使用 prediction / prediction_blend 时为必需项)
    if strategy_needs_prediction(cfg) {
        let resolved_prediction_set = resolve_live_prediction_set(db, cfg, last_mkt).await;
        let Some(ps) = resolved_prediction_set.as_deref() else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!(
                    "strategy signal_source={} 需要 ML，但没有 PIT 合规预测集覆盖 {}",
                    cfg.signal_source, last_mkt
                ),
                None,
                None,
                Some("需要先运行 PIT 训练/预测流水线，不能降级成纯因子交易".to_string()),
            ));
            return out;
        };
        let ps_ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(
               SELECT 1 FROM prediction_set
                 WHERE prediction_set_id=$1
                   AND status='ready'
                   AND start_date <= $2
                   AND end_date >= $2
                 AND training_end_date IS NOT NULL
                 AND training_end_date < $2
             )",
        )
        .bind(ps)
        .bind(last_mkt)
        .fetch_one(db)
        .await
        .unwrap_or(false);
        if !ps_ready {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 未 ready 或未 PIT 覆盖 {}", ps, last_mkt),
                None,
                None,
                Some("需要重建或切换到覆盖当前交易日的 PIT 预测集".to_string()),
            ));
            return out;
        }
        let ml_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
            "SELECT MAX(trade_date) FROM model_prediction
             WHERE prediction_set_id=$1 AND COALESCE(available_at, trade_date) <= trade_date",
        )
        .bind(ps)
        .fetch_one(db)
        .await
        .ok()
        .flatten();
        if let Some(d) = ml_last {
            let lag = (last_mkt - d).num_days();
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                lag_level(lag, 2, 7),
                format!("最新 {} (落后 {} 天, set={})", d, lag, ps),
                None,
                None,
                Some(
                    "预测集由 PIT 训练/预测流水线生成；单点页面修复不能保证模型正确性".to_string(),
                ),
            ));
        } else {
            out.push(check_item(
                acct,
                sid,
                "ML预测集",
                "red",
                format!("prediction_set={} 无 PIT 合规预测行", ps),
                None,
                None,
                Some("需要重建 PIT 预测集".to_string()),
            ));
        }
    }
    out
}


