//! Admin API — user management, data sync, scheduled task management.
//! All endpoints require admin role.

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::{require_admin, UserContext};
use crate::AppState;
// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_datetime;
// ── User Management ─────────────────────────────────────────

/// GET /api/v1/admin/users — list all users
pub async fn list_users(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }
    let rows = sqlx::query(
        "SELECT user_id, username, display_name, email, role, status, last_login_at FROM quant_user ORDER BY created_at DESC"
    )
    .fetch_all(&state.db).await.unwrap_or_default();

    let list: Vec<serde_json::Value> = rows.iter().map(|r| {
        use sqlx::Row;
        serde_json::json!({
            "user_id": r.try_get::<String, _>("user_id").unwrap_or_default(),
            "username": r.try_get::<String, _>("username").unwrap_or_default(),
            "display_name": r.try_get::<Option<String>, _>("display_name").unwrap_or(None),
            "email": r.try_get::<Option<String>, _>("email").unwrap_or(None),
            "role": r.try_get::<String, _>("role").unwrap_or_default(),
            "status": r.try_get::<String, _>("status").unwrap_or_default(),
            "last_login_at": r.try_get::<Option<chrono::NaiveDateTime>, _>("last_login_at").ok().flatten().map(|t| t.to_string()),
        })
    }).collect();

    Json(serde_json::json!({"code": 0, "data": list})).into_response()
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub role: Option<String>,
}

/// POST /api/v1/admin/users — create user
pub async fn create_user(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<CreateUserRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let uid = Uuid::new_v4().simple().to_string(); // 32-char, fits VARCHAR(32)
    let hash = bcrypt::hash(&req.password, 12).unwrap_or_default();
    let role = req.role.as_deref().unwrap_or("user");

    match sqlx::query(
        "INSERT INTO quant_user (user_id, username, password_hash, display_name, email, role, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    )
    .bind(&uid).bind(&req.username).bind(&hash)
    .bind(&req.display_name).bind(&req.email).bind(role).bind(&admin.user_id)
    .execute(&state.db).await
    {
        Ok(_) => Json(serde_json::json!({"code": 0, "data": {"user_id": uid}})).into_response(),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("创建失败: {}", e)})).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub role: Option<String>,
    pub status: Option<String>,
}

/// PUT /api/v1/admin/users/:id — update user
pub async fn update_user(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(user_id): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let _ = sqlx::query(
        "UPDATE quant_user SET display_name = COALESCE($1, display_name),
         email = COALESCE($2, email), role = COALESCE($3, role),
         status = COALESCE($4, status), updated_by = $5, updated_at = NOW()
         WHERE user_id = $6",
    )
    .bind(&req.display_name)
    .bind(&req.email)
    .bind(&req.role)
    .bind(&req.status)
    .bind(&admin.user_id)
    .bind(&user_id)
    .execute(&state.db)
    .await;

    Json(serde_json::json!({"code": 0})).into_response()
}

#[derive(Debug, Deserialize)]
pub struct ResetPasswordRequest {
    pub new_password: String,
}

/// PUT /api/v1/admin/users/:id/password — reset user password
pub async fn reset_user_password(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(user_id): Path<String>,
    Json(req): Json<ResetPasswordRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }
    if req.new_password.len() < 4 {
        return Json(serde_json::json!({"code": 1, "message": "密码至少4位"})).into_response();
    }
    let hash = bcrypt::hash(&req.new_password, 12).unwrap_or_default();
    match sqlx::query(
        "UPDATE quant_user SET password_hash = $1, updated_by = $2, updated_at = NOW() WHERE user_id = $3"
    )
    .bind(&hash).bind(&admin.user_id).bind(&user_id)
    .execute(&state.db).await
    {
        Ok(r) if r.rows_affected() > 0 => {
            // 清除该用户所有会话，强制重新登录
            let _ = sqlx::query("DELETE FROM user_session WHERE user_id = $1").bind(&user_id).execute(&state.db).await;
            Json(serde_json::json!({"code": 0, "message": "密码已重置，用户需重新登录"})).into_response()
        }
        Ok(_) => Json(serde_json::json!({"code": 1, "message": "用户不存在"})).into_response(),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("重置失败: {}", e)})).into_response(),
    }
}

/// DELETE /api/v1/admin/users/:id — delete user
pub async fn delete_user(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }
    // 检查是否是超级管理员（通过 username 判断，user_id 是 UUID）
    let is_admin_user: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM quant_user WHERE user_id = $1 AND username = 'admin')",
    )
    .bind(&user_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();
    if is_admin_user.map(|(b,)| b).unwrap_or(false) {
        return Json(serde_json::json!({"code": 1, "message": "不能删除超级管理员"}))
            .into_response();
    }
    let _ = sqlx::query("DELETE FROM quant_user WHERE user_id = $1")
        .bind(&user_id)
        .execute(&state.db)
        .await;
    Json(serde_json::json!({"code": 0})).into_response()
}

// ── Scheduled Task Management ───────────────────────────────

/// GET /api/v1/admin/tasks — list scheduled tasks
pub async fn list_tasks(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let tasks: Vec<(String, String, bool, String, serde_json::Value, Option<chrono::DateTime<chrono::Utc>>, i32)> =
        sqlx::query_as("SELECT task_name, task_type, enabled, schedule_cron, params, last_run_at, run_count FROM scheduled_task_config ORDER BY task_name")
        .fetch_all(&state.db).await.unwrap_or_default();

    let list: Vec<serde_json::Value> = tasks
        .into_iter()
        .map(|(n, tt, en, cron, p, lr, rc)| {
            serde_json::json!({
                "task_name": n, "task_type": tt, "enabled": en, "schedule_cron": cron,
                "params": p, "last_run_at": fmt_datetime(lr), "run_count": rc
            })
        })
        .collect();

    Json(serde_json::json!({"code": 0, "data": list})).into_response()
}

#[derive(Debug, Deserialize)]
pub struct UpdateTaskRequest {
    pub enabled: Option<bool>,
    pub schedule_cron: Option<String>,
    pub params: Option<serde_json::Value>,
}

/// PUT /api/v1/admin/tasks/:name — update task
pub async fn update_task(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(name): Path<String>,
    Json(req): Json<UpdateTaskRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let _ = sqlx::query(
        "UPDATE scheduled_task_config SET enabled = COALESCE($1, enabled),
         schedule_cron = COALESCE($2, schedule_cron),
         params = COALESCE($3, params), updated_by = $4, updated_at = NOW()
         WHERE task_name = $5",
    )
    .bind(req.enabled)
    .bind(&req.schedule_cron)
    .bind(&req.params)
    .bind(&admin.user_id)
    .bind(&name)
    .execute(&state.db)
    .await;

    Json(serde_json::json!({"code": 0})).into_response()
}

/// POST /api/v1/admin/tasks/:name/run — manually trigger task
pub async fn run_task(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(name): Path<String>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    // Set next_run_at to NOW to trigger on next scheduler tick
    let _ = sqlx::query(
        "UPDATE scheduled_task_config SET next_run_at = NOW(), updated_by = $1, updated_at = NOW() WHERE task_name = $2"
    )
    .bind(&admin.user_id).bind(&name)
    .execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": format!("任务 {} 已触发", name)})).into_response()
}

// ── Data Sync Status ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PerformanceReportRequest {
    /// 日报日期(YYYYMMDD),不传则用今日
    pub date: Option<String>,
}

/// POST /api/v1/admin/performance-report — 手动触发实盘绩效日报(遍历所有 active 模拟盘)
///
/// 正常由 16:00 EOD scheduler 自动触发(scheduler.rs push_daily_performance_report)，
/// 此端点用于手动补发/重发(如当日 EOD 时段服务重启错过自动触发)。
pub async fn trigger_performance_report(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<PerformanceReportRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let date = req
        .date
        .as_deref()
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y%m%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive());

    match crate::routes::scheduler::push_daily_performance_report(&state.db, date).await {
        Ok(_) => Json(serde_json::json!({
            "code": 0,
            "message": format!("绩效日报已生成并推送 {}", date.format("%Y-%m-%d"))
        }))
        .into_response(),
        Err(e) => Json(serde_json::json!({
            "code": 1,
            "message": format!("绩效日报生成失败: {}", e)
        }))
        .into_response(),
    }
}

/// GET /api/v1/admin/sync/status — check data freshness
pub async fn sync_status(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    // T+1 数据在次日 9:00 自动同步（含周末），正常 gap ≤1 天。
    // 看板取第一个 active 复合策略为代表检查数据新鲜度（不再硬编码 v19）。
    let cfg = load_first_active_admin_strategy_config(&state.db).await;
    let today = admin_latest_market_date(&state.db).await;

    let mut results = Vec::new();

    let a_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
    )
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();
    let a_gap = admin_gap(today, a_last);
    results.push(admin_status_item(
        "A股日线",
        2,
        a_gap,
        a_gap <= 2,
        a_last.map(|d| format!("最新 {}", d)),
    ));

    let (etf_present, etf_last): (i64, Option<chrono::NaiveDate>) = sqlx::query_as(
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
    .fetch_one(&state.db)
    .await
    .unwrap_or((0, None));
    let etf_gap = admin_gap(today, etf_last);
    results.push(admin_status_item(
        "ETF日线(MVO)",
        2,
        etf_gap,
        etf_gap <= 2 && etf_present == cfg.etf_symbols.len() as i64,
        Some(format!(
            "{}只ETF，已覆盖{}只，全部最新最早{}",
            cfg.etf_symbols.len(),
            etf_present,
            etf_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string())
        )),
    ));

    let suspension_last =
        admin_event_completion_last_date(&state.db, "market_stock_suspension", "suspension_daily")
            .await;
    let suspension_gap = admin_gap(today, suspension_last);
    results.push(admin_status_item(
        "停牌",
        2,
        suspension_gap,
        suspension_gap <= 2,
        suspension_last.map(|d| format!("最新完成/事件日期 {}", d)),
    ));

    let adj_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_adjustment_factor
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
    )
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();
    let adj_gap = admin_gap(today, adj_last);
    results.push(admin_status_item(
        "A股复权因子",
        30,
        adj_gap,
        adj_gap <= 30,
        adj_last.map(|d| format!("最新 {}", d)),
    ));

    let (etf_adj_present, etf_adj_last): (i64, Option<chrono::NaiveDate>) = sqlx::query_as(
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
    .fetch_one(&state.db)
    .await
    .unwrap_or((0, None));
    let etf_adj_gap = admin_gap(today, etf_adj_last);
    results.push(admin_status_item(
        "ETF复权因子(MVO)",
        30,
        etf_adj_gap,
        etf_adj_gap <= 30 && etf_adj_present == cfg.etf_symbols.len() as i64,
        Some(format!(
            "{}只ETF，已覆盖{}只，全部最新最早{}",
            cfg.etf_symbols.len(),
            etf_adj_present,
            etf_adj_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string())
        )),
    ));

    let combo_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name = $1 AND version='1.0.0'
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(&cfg.combo_name)
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();
    let combo_gap = admin_gap(today, combo_last);
    results.push(admin_status_item(
        "因子(full PIT)",
        2,
        combo_gap,
        combo_gap <= 2,
        combo_last.map(|d| format!("combo={}，最新 {}", cfg.combo_name, d)),
    ));

    let csi_last: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT MAX(trade_date) FROM market_index_daily_bar WHERE symbol='000300.SH'",
    )
    .fetch_one(&state.db)
    .await
    .ok()
    .flatten();
    let csi_gap = admin_gap(today, csi_last);
    results.push(admin_status_item(
        "CSI300",
        2,
        csi_gap,
        csi_gap <= 2,
        csi_last.map(|d| format!("最新 {}", d)),
    ));

    let limit_last =
        admin_event_completion_last_date(&state.db, "market_stock_limit", "limit_daily").await;
    let limit_gap = admin_gap(today, limit_last);
    results.push(admin_status_item(
        "涨跌停",
        2,
        limit_gap,
        limit_gap <= 2,
        Some(format!(
            "最新完成/事件日期{}；2019-11-28前历史需由日线按交易规则派生",
            limit_last
                .map(|d| d.to_string())
                .unwrap_or_else(|| "无".to_string())
        )),
    ));

    let curve_last: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM backtest_equity_curve WHERE task_id=$1")
            .bind(&cfg.equity_curve_task_id)
            .fetch_one(&state.db)
            .await
            .ok()
            .flatten();
    let curve_gap = admin_gap(today, curve_last);
    results.push(admin_status_item(
        "权益曲线",
        10,
        curve_gap,
        curve_gap <= 10,
        curve_last.map(|d| {
            format!(
                "strategy={}，task={}，最新 {}",
                cfg.strategy_id, cfg.equity_curve_task_id, d
            )
        }),
    ));

    let active_stock_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::int8 FROM market_stock WHERE list_status='L' AND exchange IN ('SSE','SZSE')",
    )
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);
    results.push(admin_status_item(
        "股票基础信息",
        7,
        if active_stock_count >= 3000 { 0 } else { 999 },
        active_stock_count >= 3000,
        Some(format!("当前上市A股{}只", active_stock_count)),
    ));

    let st_count: i64 = sqlx::query_scalar("SELECT COUNT(*)::int8 FROM market_stock_name_history")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    results.push(admin_status_item(
        "ST/名称历史",
        30,
        if st_count > 0 { 0 } else { 999 },
        st_count > 0,
        Some(format!("名称历史/ST记录{}条", st_count)),
    ));

    // ── ML预测：检查调度器实际会选中的预测集（PIT 最新）──
    {
        let pid: Option<String> = cfg.prediction_set_id.clone().or_else(|| None);
        let pid = match pid {
            Some(pid) => Some(pid),
            None => sqlx::query_scalar(
                "SELECT ps.prediction_set_id FROM prediction_set ps
                     WHERE ps.status = 'ready'
                       AND ps.training_end_date IS NOT NULL AND ps.training_end_date < $1
                       AND ps.start_date <= $1 AND ps.end_date >= $1
                     ORDER BY ps.training_end_date DESC LIMIT 1",
            )
            .bind(today)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten(),
        };

        let (ml_gap, ml_healthy, ml_extra) = if let Some(ref pid) = pid {
            let latest: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                "SELECT MAX(trade_date) FROM model_prediction
                 WHERE prediction_set_id = $1 AND COALESCE(available_at, trade_date) <= trade_date",
            )
            .bind(pid)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
            let gap = latest.map(|(d,)| (today - d).num_days()).unwrap_or(999);

            let etf_count: (i64,) = sqlx::query_as(
                "SELECT COUNT(DISTINCT symbol) FROM model_prediction WHERE prediction_set_id = $1
                 AND symbol IN ('518880.SH','511010.SH','513500.SH','513100.SH','159980.SZ','159985.SZ','501018.SH')"
            ).bind(pid).fetch_optional(&state.db).await.ok().flatten().unwrap_or((0,));

            let a_stock_count: (i64,) = sqlx::query_as(
                "SELECT COUNT(DISTINCT symbol) FROM model_prediction WHERE prediction_set_id = $1
                 AND symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'",
            )
            .bind(pid)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten()
            .unwrap_or((0,));

            let future_rows: (i64,) = sqlx::query_as(
                "SELECT COUNT(*)::int8 FROM model_prediction
                 WHERE prediction_set_id = $1 AND COALESCE(available_at, trade_date) > trade_date",
            )
            .bind(pid)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten()
            .unwrap_or((0,));

            let healthy =
                gap <= 7 && etf_count.0 >= 7 && a_stock_count.0 >= 20 && future_rows.0 == 0;
            let info = format!(
                "{}, ETF{}/7, 个股{}条(需≥20), future_rows={}",
                pid, etf_count.0, a_stock_count.0, future_rows.0
            );
            (gap, healthy, info)
        } else {
            (999i64, false, "无可用预测集".to_string())
        };

        results.push(serde_json::json!({
            "name": "ML预测", "max_gap_days": 7i64,
            "current_gap_days": ml_gap, "healthy": ml_healthy,
            "extra": ml_extra,
            "repairable": true,
        }));
    }

    Json(serde_json::json!({"code": 0, "data": results})).into_response()
}

/// GET /api/v1/admin/tasks/check-deps
pub async fn check_task_deps(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }
    let issues = crate::routes::scheduler::check_task_dependency_order(&state.db).await;
    Json(serde_json::json!({"code": 0, "data": issues})).into_response()
}

// ── Data Repair ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RepairRequest {
    pub name: String,
    pub strategy_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]
struct AdminSyncStrategyConfig {
    strategy_id: String,
    combo_name: String,
    equity_curve_task_id: String,
    prediction_set_id: Option<String>,
    etf_symbols: Vec<String>,
}

const DEFAULT_MVO_ETFS: &[&str] = &[
    "518880.SH",
    "511010.SH",
    "513500.SH",
    "513100.SH",
    "159980.SZ",
    "159985.SZ",
    "501018.SH",
];

fn admin_default_mvo_etfs() -> Vec<String> {
    DEFAULT_MVO_ETFS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect()
}

fn admin_parse_etf_symbols(value: Option<serde_json::Value>) -> Vec<String> {
    let symbols = value
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(str::trim).map(str::to_string))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        admin_default_mvo_etfs()
    } else {
        symbols
    }
}

fn admin_parse_date(value: &str) -> Result<chrono::NaiveDate, String> {
    let trimmed = value.trim();
    chrono::NaiveDate::parse_from_str(trimmed, "%Y%m%d")
        .or_else(|_| chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d"))
        .map_err(|_| format!("日期格式无效: {}，需要 YYYYMMDD 或 YYYY-MM-DD", value))
}

fn admin_yyyymmdd(date: chrono::NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn admin_gap(base: chrono::NaiveDate, date: Option<chrono::NaiveDate>) -> i64 {
    date.map(|d| (base - d).num_days()).unwrap_or(999)
}

fn admin_status_item(
    name: &str,
    max_gap_days: i64,
    current_gap_days: i64,
    healthy: bool,
    extra: Option<String>,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "name": name,
        "max_gap_days": max_gap_days,
        "current_gap_days": current_gap_days,
        "healthy": healthy,
        "repairable": true,
    });
    if let Some(extra) = extra {
        value["extra"] = serde_json::json!(extra);
    }
    value
}

async fn admin_latest_market_date(db: &sqlx::PgPool) -> chrono::NaiveDate {
    sqlx::query_scalar::<_, Option<chrono::NaiveDate>>(
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

async fn admin_first_open_trade_date(
    db: &sqlx::PgPool,
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
) -> Option<chrono::NaiveDate> {
    sqlx::query_scalar::<_, chrono::NaiveDate>(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE exchange = 'SSE'
           AND is_open = true
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date ASC
         LIMIT 1",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

async fn load_admin_strategy_config(
    db: &sqlx::PgPool,
    strategy_id: &str,
) -> AdminSyncStrategyConfig {
    let row: Option<(String, String, Option<String>, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT combo_name, equity_curve_task_id, prediction_set_id, etf_symbols
         FROM strategy_config WHERE strategy_id=$1 AND status='active'",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    match row {
        Some((combo_name, equity_curve_task_id, prediction_set_id, etf_symbols)) => {
            AdminSyncStrategyConfig {
                strategy_id: strategy_id.to_string(),
                combo_name,
                equity_curve_task_id,
                prediction_set_id,
                etf_symbols: admin_parse_etf_symbols(etf_symbols),
            }
        }
        None => AdminSyncStrategyConfig {
            strategy_id: strategy_id.to_string(),
            combo_name: String::new(),
            equity_curve_task_id: String::new(),
            prediction_set_id: None,
            etf_symbols: admin_default_mvo_etfs(),
        },
    }
}

/// 加载第一个 active 复合策略(sync_status 看板用,不再硬编码 v19)。
/// 看板本质是抽样检查数据新鲜度,取任一 active 复合策略代表即可。
/// 若无 active 策略,返回带默认 ETF 的空配置(后续检查会因 combo/curve 为空而提示未就绪)。
async fn load_first_active_admin_strategy_config(db: &sqlx::PgPool) -> AdminSyncStrategyConfig {
    let row: Option<(String, String, String, Option<String>, Option<serde_json::Value>)> =
        sqlx::query_as(
            "SELECT strategy_id, combo_name, equity_curve_task_id, prediction_set_id, etf_symbols
             FROM strategy_config
             WHERE status='active' AND strategy_type='composite'
             ORDER BY strategy_id LIMIT 1",
        )
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
    match row {
        Some((strategy_id, combo_name, equity_curve_task_id, prediction_set_id, etf_symbols)) => {
            AdminSyncStrategyConfig {
                strategy_id,
                combo_name,
                equity_curve_task_id,
                prediction_set_id,
                etf_symbols: admin_parse_etf_symbols(etf_symbols),
            }
        }
        None => AdminSyncStrategyConfig {
            strategy_id: String::new(),
            combo_name: String::new(),
            equity_curve_task_id: String::new(),
            prediction_set_id: None,
            etf_symbols: admin_default_mvo_etfs(),
        },
    }
}

async fn admin_event_completion_last_date(
    db: &sqlx::PgPool,
    table: &str,
    task_type: &str,
) -> Option<chrono::NaiveDate> {
    let sql = format!(
        "SELECT GREATEST(
            (SELECT MAX(trade_date) FROM {}),
            (SELECT MAX(end_date) FROM data_sync_task
             WHERE task_type=$1 AND status='completed')
        )",
        table
    );
    sqlx::query_scalar::<_, Option<chrono::NaiveDate>>(&sql)
        .bind(task_type)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .flatten()
}

/// POST /api/v1/admin/sync/repair — repair a specific data source
pub async fn repair_sync(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<RepairRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let dv_id = uuid::Uuid::new_v4().simple().to_string();
    // 策略 ID 必传(不再 fallback 到 "v19",配置化原则)。
    // 与 portfolio.rs run_mvo_simulate 链路一致:请求未传则报错。
    let strategy_id = match req.strategy_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(sid) => sid,
        None => {
            return Json(serde_json::json!({
                "code": 1,
                "message": "repair 必传 strategy_id(不再默认 v19)"
            })).into_response();
        }
    };
    let cfg = load_admin_strategy_config(&state.db, strategy_id).await;
    let has_explicit_start_date = req.start_date.is_some();
    let target_date = match req.end_date.as_deref() {
        Some(value) => match admin_parse_date(value) {
            Ok(date) => date,
            Err(message) => {
                return Json(serde_json::json!({"code": 1, "message": message})).into_response();
            }
        },
        None => admin_latest_market_date(&state.db).await,
    };
    let start_date = match req.start_date.as_deref() {
        Some(value) => match admin_parse_date(value) {
            Ok(date) => date,
            Err(message) => {
                return Json(serde_json::json!({"code": 1, "message": message})).into_response();
            }
        },
        None => target_date - chrono::Duration::days(30),
    };
    if start_date > target_date {
        return Json(serde_json::json!({"code": 1, "message": "start_date 不能晚于 end_date"}))
            .into_response();
    }
    let today = admin_yyyymmdd(target_date);
    let recent_start = admin_yyyymmdd(start_date);

    let result: Json<serde_json::Value> = match req.name.as_str() {
        "A股日线" => {
            let symbols: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock
                 WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                   AND list_date IS NOT NULL
                   AND list_date <= $1::date
                   AND (delist_date IS NULL OR delist_date >= $2::date)
                 ORDER BY symbol",
            )
            .bind(target_date)
            .bind(start_date)
            .fetch_all(&state.db)
            .await
            .unwrap_or_default();
            match quant_data::sync::sync_daily_bars(
                &state.db,
                &state.tushare,
                &symbols,
                &recent_start,
                &today,
                &dv_id,
            )
            .await
            {
                Ok(n) => {
                    Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)}))
                }
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "ETF日线(原油)" | "ETF日线(MVO)" => {
            let symbols = cfg.etf_symbols.clone();
            match quant_data::sync::sync_fund_daily(
                &state.db,
                &state.tushare,
                &symbols,
                &recent_start,
                &today,
                &dv_id,
            )
            .await
            {
                Ok(n) => {
                    Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)}))
                }
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "停牌" => {
            match quant_data::sync::sync_suspension(&state.db, &state.tushare, &today).await {
                Ok(count) => Json(serde_json::json!({
                    "code": 0,
                    "message": format!("{} 停牌同步完成 {} 条；已记录完成标记，0条也代表该日已成功检查", today, count)
                })),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
            }
        }
        "复权因子" => {
            let symbols: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock
                 WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
                   AND list_date IS NOT NULL
                   AND list_date <= $1::date
                   AND (delist_date IS NULL OR delist_date >= $2::date)
                 ORDER BY symbol",
            )
            .bind(target_date)
            .bind(start_date)
            .fetch_all(&state.db)
            .await
            .unwrap_or_default();
            match quant_data::sync::sync_adj_factor(
                &state.db,
                &state.tushare,
                &symbols,
                &recent_start,
                &today,
                &dv_id,
            )
            .await
            {
                Ok(n) => {
                    Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)}))
                }
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "涨跌停" => {
            let limit_earliest = chrono::NaiveDate::from_ymd_opt(2019, 11, 28).unwrap();
            if start_date < limit_earliest {
                let derive_end = target_date.min(limit_earliest - chrono::Duration::days(1));
                if derive_end >= start_date {
                    match quant_data::sync::derive_limit_list_from_daily_bars(
                        &state.db,
                        &admin_yyyymmdd(start_date),
                        &admin_yyyymmdd(derive_end),
                    )
                    .await
                    {
                        Ok(count) if target_date < limit_earliest => {
                            return Json(serde_json::json!({
                                "code": 0,
                                "message": format!(
                                    "{}~{} 涨跌停已由日线派生 {} 条，并写入完成标记",
                                    admin_yyyymmdd(start_date),
                                    admin_yyyymmdd(derive_end),
                                    count
                                )
                            }))
                            .into_response();
                        }
                        Ok(_) => {}
                        Err(e) => {
                            return Json(serde_json::json!({"code": 1, "message": e}))
                                .into_response()
                        }
                    }
                }
            }
            match quant_data::sync::sync_limit_list(&state.db, &state.tushare, &today).await {
                Ok(count) => Json(serde_json::json!({
                    "code": 0,
                    "message": format!("{} 涨跌停同步完成 {} 条；已记录完成标记，0条也代表该日已成功检查", today, count)
                })),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
            }
        }
        "CSI300" => {
            let codes = vec!["000300.SH".to_string()];
            match quant_data::sync::sync_index_daily(
                &state.db,
                &state.tushare,
                &codes,
                &recent_start,
                &today,
                &dv_id,
            )
            .await
            {
                Ok(n) => {
                    Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)}))
                }
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "因子(pv)" | "因子(full PIT)" => {
            // 正式 canonical 为 full PIT combo，修复动作直接补 PIT 组合物化区间。
            let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
            let url = format!(
                "http://localhost:{}/api/v1/quant/factors/materialize-pit-combo/background",
                port
            );
            let payload = serde_json::json!({
                "combo_name": cfg.combo_name,
                "version": "1.0.0",
                "horizon": 20,
                "start_date": recent_start,
                "end_date": today
            });
            match reqwest::Client::new()
                .post(&url)
                .json(&payload)
                .send()
                .await
            {
                Ok(_) => Json(
                    serde_json::json!({"code": 0, "message": format!("PIT combo物化任务已触发: {} {}~{}", cfg.combo_name, recent_start, today)}),
                ),
                Err(e) => {
                    Json(serde_json::json!({"code": 1, "message": format!("触发失败: {}", e)}))
                }
            }
        }
        "ML预测" | "ML预测(活跃策略)" => {
            // 直接重建全市场预测集（秒级完成），同时后台触发 ML 训练保持 ETF 数据新鲜
            let db = state.db.clone();
            match crate::routes::scheduler::rebuild_full_universe_prediction_set(&db).await {
                Ok(pid) => {
                    // 后台异步触发 ML 训练（更新 ETF 预测，供下次重建使用）
                    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
                    let pred_date = chrono::Utc::now().date_naive();
                    let dv_id: String = sqlx::query_scalar(
                        "SELECT data_version_id FROM data_version WHERE data_version_id LIKE 'dv-eod-%' ORDER BY end_date DESC LIMIT 1"
                    ).fetch_optional(&state.db).await.ok().flatten()
                        .unwrap_or_else(|| "research-full-2016-2026-20260515".to_string());
                    let payload = serde_json::json!({
                        "model_code": "nlqr_mr", "model_version": "1.0.0",
                        "model_version_id": "mdl-p7-wf-wide-qgvrel-h60-v1",
                        "data_version_id": dv_id,
                        "feature_set_version_id": "phase7-wide-qgvrel-v1",
                        "training_dataset_id": "phase7-wf-wide-qgvrel-h60-v1",
                        "prediction_start_date": pred_date.format("%Y%m%d").to_string(),
                        "prediction_end_date": (pred_date + chrono::Duration::days(63)).format("%Y%m%d").to_string(),
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
                    let url = format!("http://localhost:{}/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker", port);
                    tokio::spawn(async move {
                        let _ = reqwest::Client::new()
                            .post(&url)
                            .json(&payload)
                            .send()
                            .await;
                    });
                    Json(
                        serde_json::json!({"code": 0, "message": format!("全市场预测集已重建: {}，请刷新页面", pid)}),
                    )
                }
                Err(e) => {
                    Json(serde_json::json!({"code": 1, "message": format!("重建失败: {}", e)}))
                }
            }
        }
        "权益曲线" => {
            // 触发 canonical full PIT 因子回测任务，并只更新目标策略配置。
            let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
            let end_date = today.clone();
            let requested_curve_start_date = if has_explicit_start_date {
                start_date
            } else {
                chrono::NaiveDate::from_ymd_opt(2017, 1, 3).unwrap()
            };
            let curve_start_date =
                admin_first_open_trade_date(&state.db, requested_curve_start_date, target_date)
                    .await
                    .unwrap_or(requested_curve_start_date);
            let curve_start_date = admin_yyyymmdd(curve_start_date);
            let data_version_id: Option<String> = sqlx::query_scalar(
                "SELECT data_version_id FROM data_version
                 WHERE data_version_id LIKE 'dv-v19-audit-ready-%'
                 ORDER BY COALESCE(end_date, DATE '1900-01-01') DESC, created_at DESC
                 LIMIT 1",
            )
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
            let data_version_id = match data_version_id {
                Some(id) => Some(id),
                None => sqlx::query_scalar(
                    "SELECT data_version_id FROM backtest_task WHERE task_id=$1 LIMIT 1",
                )
                .bind(&cfg.equity_curve_task_id)
                .fetch_optional(&state.db)
                .await
                .ok()
                .flatten(),
            }
            .unwrap_or_else(|| "dv-v19-audit-ready-20260615".to_string());
            let payload = serde_json::json!({
                "combo_name": cfg.combo_name,
                "strategy_version_id": "factor-combo-v1",
                "data_version_id": data_version_id,
                "prediction_set_id": cfg.prediction_set_id,
                "prediction_blend_weight": 0.5,
                "top_n": 30,
                "rebalance": "10",
                "start_date": curve_start_date,
                "end_date": end_date,
                "score_direction": "ascending",
                "effective_coverage": {
                    "enabled": true,
                    "mode": "guard_only",
                    "min_rows": 30,
                    "include_rebalance_warmup": false
                }
            });
            match reqwest::Client::new()
                .post(format!(
                    "http://localhost:{}/api/v1/quant/backtests/run-factor",
                    port
                ))
                .json(&payload)
                .timeout(std::time::Duration::from_secs(600))
                .send()
                .await
            {
                Ok(resp) => {
                    if let Ok(result) = resp.json::<serde_json::Value>().await {
                        if let Some(tid) = result["data"]["task_id"].as_str() {
                            // 自动更新 strategy_config 中的 equity_curve_task_id
                            let _ = sqlx::query(
                                "UPDATE strategy_config SET equity_curve_task_id = $1, combo_name = $2,
                                 prediction_set_id = $3, score_direction = 'ascending',
                                 updated_at = NOW()
                                 WHERE strategy_id = $4 AND status = 'active'"
                            )
                            .bind(tid)
                            .bind(&cfg.combo_name)
                            .bind(&cfg.prediction_set_id)
                            .bind(&cfg.strategy_id)
                            .execute(&state.db).await;
                            Json(
                                serde_json::json!({"code": 0, "message": format!("{} 权益曲线回测已触发: {}", cfg.strategy_id, tid)}),
                            )
                        } else {
                            Json(
                                serde_json::json!({"code": 1, "message": "回测提交失败，未返回task_id"}),
                            )
                        }
                    } else {
                        Json(serde_json::json!({"code": 1, "message": "回测响应解析失败"}))
                    }
                }
                Err(e) => {
                    Json(serde_json::json!({"code": 1, "message": format!("触发失败: {}", e)}))
                }
            }
        }
        _ => Json(serde_json::json!({"code": 1, "message": format!("未知数据项: {}", req.name)})),
    };
    result.into_response()
}

// ── ML 全市场预测重建 ─────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct RebuildFullUniverseRequest {
    /// 预测起始日期（YYYYMMDD），默认今天
    pub start_date: Option<String>,
    /// 预测结束日期（YYYYMMDD），默认 +63 天
    pub end_date: Option<String>,
    /// 是否清理旧的 ETF-only 预测集
    #[serde(default)]
    pub cleanup_old: bool,
}

/// POST /api/v1/admin/ml/rebuild-full-universe
///
/// 从现有数据重建全市场预测集（ETF + A 股个股）。
/// 合并 pred-nlqr_mr（ETF预测）和 pred-nlqr_v16（A股预测）的数据。
pub async fn rebuild_full_universe(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<RebuildFullUniverseRequest>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    match crate::routes::scheduler::rebuild_full_universe_prediction_set(&state.db).await {
        Ok(pid) => {
            // 可选清理旧数据
            if req.cleanup_old {
                let old_sets: Vec<String> = sqlx::query_scalar(
                    "SELECT ps.prediction_set_id FROM prediction_set ps
                     WHERE ps.prediction_set_id LIKE '%nlqr_mr%'
                       AND (SELECT COUNT(DISTINCT symbol) FROM model_prediction WHERE prediction_set_id = ps.prediction_set_id) < 20"
                ).fetch_all(&state.db).await.unwrap_or_default();
                for old_pid in &old_sets {
                    let _ =
                        sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
                            .bind(old_pid)
                            .execute(&state.db)
                            .await;
                    let _ = sqlx::query("DELETE FROM prediction_set WHERE prediction_set_id = $1")
                        .bind(old_pid)
                        .execute(&state.db)
                        .await;
                }
            }
            Json(serde_json::json!({"code": 0, "message": format!("全市场预测集已重建: {}", pid)}))
                .into_response()
        }
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("重建失败: {}", e)}))
            .into_response(),
    }
}

// ── Manual Rebalance ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ManualRebalanceRequest {
    /// 调仓日期(YYYYMMDD),不传则用今日
    pub date: Option<String>,
}

/// POST /api/v1/admin/rebalance — 手动触发调仓(遍历所有 active 模拟盘)
///
/// 复用 14:40 调仓链路:validate_pre_trade_data 前置校验 + generate_paper_signals_for_all。
/// 内部 per-account 今日已交易检查保证幂等(今日已调仓账号跳过,不会重复下单)。
/// 调仓成功后推送持仓摘要钉钉通知。
pub async fn manual_rebalance(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<ManualRebalanceRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let today = req
        .date
        .as_deref()
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y%m%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);

    // 取第一个 active 复合策略为代表(_sc 参数未用,每账号用自己的 strategy_version_id)
    let sc = match crate::routes::scheduler::load_first_active_strategy_config(&state.db).await {
        Some(s) => s,
        None => {
            return Json(serde_json::json!({
                "code": 1,
                "message": "无 active 复合策略,无法调仓"
            }))
            .into_response();
        }
    };

    // 前置数据校验+自动修复(复用 14:40 链路),数据未就绪则拒绝调仓
    let errors =
        crate::routes::scheduler::validate_pre_trade_data(&state.db, &state.tushare, today, &sc)
            .await;
    if !errors.is_empty() {
        return Json(serde_json::json!({
            "code": 1,
            "message": format!("数据未就绪,调仓拒绝:\n{}", errors.join("\n"))
        }))
        .into_response();
    }

    // 构造空 mvo_cache(首次计算时填充,先例 compute_mvo_weights_for_date)
    let mvo_cache = Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::scheduler::MvoWeightCache>,
    ));

    // 记录调仓前订单数(用于检测账号是否被数据门禁跳过)
    let orders_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM paper_order WHERE DATE(created_at) = $1",
    )
    .bind(today)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    match crate::routes::scheduler::generate_paper_signals_for_all(
        &state.db,
        &mvo_cache,
        port,
        today,
        &sc,
        &state.tushare,
    )
    .await
    {
        Ok(_) => {
            // 检测是否有新订单(账号可能因数据门禁被跳过,此时无新订单)
            let orders_after: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM paper_order WHERE DATE(created_at) = $1",
            )
            .bind(today)
            .fetch_one(&state.db)
            .await
            .unwrap_or(0);
            let new_orders = orders_after - orders_before;

            // 调仓成功后推送持仓摘要钉钉通知(复用公开版本)
            let _ =
                crate::routes::scheduler::push_dingtalk_for_all_accounts_public(&state.db, today)
                    .await;

            if new_orders > 0 {
                Json(serde_json::json!({
                    "code": 0,
                    "message": format!("调仓完成 {} (新增 {} 笔订单)", today.format("%Y-%m-%d"), new_orders)
                }))
                .into_response()
            } else {
                // 无新订单:账号被数据门禁跳过(权益曲线/因子等滞后),钉钉已发跳过原因
                Json(serde_json::json!({
                    "code": 0,
                    "message": format!("调仓链路已执行 {} 但无账号下单(数据门禁跳过,查钉钉/日志)", today.format("%Y-%m-%d"))
                }))
                .into_response()
            }
        }
        Err(e) => Json(serde_json::json!({
            "code": 1,
            "message": format!("调仓失败: {}", e)
        }))
        .into_response(),
    }
}

// ── Factor Health (P3-2) ──────────────────────────────────────

/// GET /api/v1/admin/factor-health — v24 14 因子每日覆盖率 + 滞缓状态
pub async fn factor_health(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) {
        return e;
    }

    let today = chrono::Utc::now().date_naive();

    // 最近交易日
    let latest_trade: Option<chrono::NaiveDate> = sqlx::query_scalar(
        "SELECT trade_date FROM market_trade_calendar
         WHERE is_open = true AND trade_date <= $1
         ORDER BY trade_date DESC LIMIT 1",
    )
    .bind(today)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let mut factors: Vec<serde_json::Value> = Vec::new();
    if let Some(latest_td) = latest_trade {
        let window_start = latest_td - chrono::Duration::days(30);

        // 单次查询拿每因子最新日覆盖数 + 近 30 日 max
        for code in crate::routes::scheduler::V24_FACTOR_CODES {
            let fresh: Option<(chrono::NaiveDate, i64, i64)> = sqlx::query_as(
                "WITH per_day AS (
                    SELECT trade_date, COUNT(DISTINCT symbol) AS cnt
                    FROM factor_value
                    WHERE factor_code = $1 AND factor_version = '1.0.0'
                      AND trade_date >= $2
                    GROUP BY trade_date
                 ),
                 latest AS (
                    SELECT trade_date, cnt FROM per_day ORDER BY trade_date DESC LIMIT 1
                 )
                 SELECT l.trade_date, l.cnt, COALESCE(MAX(p.cnt), 0) AS recent_max
                 FROM latest l CROSS JOIN per_day p",
            )
            .bind(code)
            .bind(window_start)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();

            let (factor_dt, latest_cnt, recent_max) = fresh.unwrap_or((today, 0, 0));
            let is_stale = factor_dt < latest_td;
            let coverage_pct = if recent_max > 0 {
                (latest_cnt as f64 / recent_max as f64 * 1000.0).round() / 10.0
            } else {
                0.0
            };

            factors.push(serde_json::json!({
                "factor_code": code,
                "latest_date": factor_dt.format("%Y-%m-%d").to_string(),
                "latest_count": latest_cnt,
                "recent_max_count": recent_max,
                "coverage_pct": coverage_pct,
                "is_stale": is_stale,
                "gap_days": if is_stale { (latest_td - factor_dt).num_days() } else { 0 },
            }));
        }
    }

    Json(serde_json::json!({
        "code": 0,
        "data": {
            "check_date": today.format("%Y-%m-%d").to_string(),
            "latest_trade_date": latest_trade.map(|d| d.format("%Y-%m-%d").to_string()),
            "factors": factors,
        }
    }))
    .into_response()
}
