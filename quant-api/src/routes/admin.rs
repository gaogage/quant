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

// ── User Management ─────────────────────────────────────────

/// GET /api/v1/admin/users — list all users
pub async fn list_users(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) { return e; }
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
    if let Err(e) = require_admin(&admin) { return e; }

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
    if let Err(e) = require_admin(&admin) { return e; }

    let _ = sqlx::query(
        "UPDATE quant_user SET display_name = COALESCE($1, display_name),
         email = COALESCE($2, email), role = COALESCE($3, role),
         status = COALESCE($4, status), updated_by = $5, updated_at = NOW()
         WHERE user_id = $6"
    )
    .bind(&req.display_name).bind(&req.email).bind(&req.role)
    .bind(&req.status).bind(&admin.user_id).bind(&user_id)
    .execute(&state.db).await;

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
    if let Err(e) = require_admin(&admin) { return e; }
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
    if let Err(e) = require_admin(&admin) { return e; }
    // 检查是否是超级管理员（通过 username 判断，user_id 是 UUID）
    let is_admin_user: Option<(bool,)> = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM quant_user WHERE user_id = $1 AND username = 'admin')"
    )
    .bind(&user_id)
    .fetch_optional(&state.db).await.ok().flatten();
    if is_admin_user.map(|(b,)| b).unwrap_or(false) {
        return Json(serde_json::json!({"code": 1, "message": "不能删除超级管理员"})).into_response();
    }
    let _ = sqlx::query("DELETE FROM quant_user WHERE user_id = $1").bind(&user_id).execute(&state.db).await;
    Json(serde_json::json!({"code": 0})).into_response()
}

// ── Scheduled Task Management ───────────────────────────────

/// GET /api/v1/admin/tasks — list scheduled tasks
pub async fn list_tasks(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) { return e; }

    let tasks: Vec<(String, String, bool, String, serde_json::Value, Option<chrono::DateTime<chrono::Utc>>, i32)> =
        sqlx::query_as("SELECT task_name, task_type, enabled, schedule_cron, params, last_run_at, run_count FROM scheduled_task_config ORDER BY task_name")
        .fetch_all(&state.db).await.unwrap_or_default();

    let list: Vec<serde_json::Value> = tasks.into_iter().map(|(n, tt, en, cron, p, lr, rc)| {
        serde_json::json!({
            "task_name": n, "task_type": tt, "enabled": en, "schedule_cron": cron,
            "params": p, "last_run_at": lr.map(|t| t.to_string()), "run_count": rc
        })
    }).collect();

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
    if let Err(e) = require_admin(&admin) { return e; }

    let _ = sqlx::query(
        "UPDATE scheduled_task_config SET enabled = COALESCE($1, enabled),
         schedule_cron = COALESCE($2, schedule_cron),
         params = COALESCE($3, params), updated_by = $4, updated_at = NOW()
         WHERE task_name = $5"
    )
    .bind(req.enabled).bind(&req.schedule_cron).bind(&req.params)
    .bind(&admin.user_id).bind(&name)
    .execute(&state.db).await;

    Json(serde_json::json!({"code": 0})).into_response()
}

/// POST /api/v1/admin/tasks/:name/run — manually trigger task
pub async fn run_task(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Path(name): Path<String>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) { return e; }

    // Set next_run_at to NOW to trigger on next scheduler tick
    let _ = sqlx::query(
        "UPDATE scheduled_task_config SET next_run_at = NOW(), updated_by = $1, updated_at = NOW() WHERE task_name = $2"
    )
    .bind(&admin.user_id).bind(&name)
    .execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": format!("任务 {} 已触发", name)})).into_response()
}

// ── Data Sync Status ────────────────────────────────────────

/// GET /api/v1/admin/sync/status — check data freshness
pub async fn sync_status(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) { return e; }

    let checks = vec![
        ("A股日线", "market_stock_daily_bar_adj", 2i64),
        ("ETF日线(原油)", "market_stock_daily_bar_adj", 2i64),
        ("停牌", "market_stock_suspension", 2i64),
        ("复权因子", "market_adjustment_factor", 30i64),
        ("因子(pv)", "multi_factor_value", 2i64),
        ("CSI300", "market_index_daily_bar", 2i64),
        ("涨跌停", "market_stock_limit", 90i64),
        ("ML预测", "model_prediction", 5i64),
        ("权益曲线", "backtest_equity_curve", 60i64),
    ];

    let mut results = Vec::new();
    for (name, table, max_gap) in &checks {
        let extra = if *table == "market_stock_daily_bar_adj" && *name == "ETF日线(原油)" {
            " WHERE symbol = '501018.SH'"
        } else if *table == "multi_factor_value" {
            " WHERE combo_name = 'phase7_price_volume_expanded_v1'"
        } else if *table == "market_index_daily_bar" {
            " WHERE symbol = '000300.SH'"
        } else if *table == "backtest_equity_curve" {
            " WHERE task_id = 'fbt-36e18e12-effc-40fe-9fc0-d539a336bf2e'"
        } else { "" };

        let sql_str = format!("SELECT MAX(trade_date)::text FROM {} {}", table, extra);
        let max_date: Option<(String,)> = sqlx::query_as(&sql_str).fetch_optional(&state.db).await.ok().flatten();
        let gap = max_date.and_then(|(d,)| {
            chrono::NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok()
                .map(|dt| (chrono::Utc::now().date_naive() - dt).num_days())
        }).unwrap_or(999);

        results.push(serde_json::json!({
            "name": name, "max_gap_days": max_gap,
            "current_gap_days": gap, "healthy": gap <= *max_gap
        }));
    }

    Json(serde_json::json!({"code": 0, "data": results})).into_response()
}

// ── Data Repair ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RepairRequest {
    pub name: String,
}

/// POST /api/v1/admin/sync/repair — repair a specific data source
pub async fn repair_sync(
    State(state): State<Arc<AppState>>,
    admin: UserContext,
    Json(req): Json<RepairRequest>,
) -> axum::response::Response {
    if let Err(e) = require_admin(&admin) { return e; }

    let dv_id = uuid::Uuid::new_v4().simple().to_string();
    let today = chrono::Utc::now().format("%Y%m%d").to_string();
    let recent_start = (chrono::Utc::now() - chrono::Duration::days(30)).format("%Y%m%d").to_string();

    let result: Json<serde_json::Value> = match req.name.as_str() {
        "A股日线" => {
            let symbols: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol"
            ).fetch_all(&state.db).await.unwrap_or_default();
            match quant_data::sync::sync_daily_bars(&state.db, &state.tushare, &symbols, &recent_start, &today, &dv_id).await {
                Ok(n) => Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "ETF日线(原油)" => {
            let symbols = vec!["501018.SH".to_string()];
            match quant_data::sync::sync_fund_daily(&state.db, &state.tushare, &symbols, &recent_start, &today, &dv_id).await {
                Ok(n) => Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "停牌" => {
            match quant_data::sync::sync_suspension(&state.db, &state.tushare, &today).await {
                Ok(count) => Json(serde_json::json!({"code": 0, "message": format!("同步 {} 条", count)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
            }
        }
        "复权因子" => {
            let symbols: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol"
            ).fetch_all(&state.db).await.unwrap_or_default();
            match quant_data::sync::sync_adj_factor(&state.db, &state.tushare, &symbols, &recent_start, &today, &dv_id).await {
                Ok(n) => Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "涨跌停" => {
            match quant_data::sync::sync_limit_list(&state.db, &state.tushare, &today).await {
                Ok(count) => Json(serde_json::json!({"code": 0, "message": format!("同步 {} 条", count)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
            }
        }
        "CSI300" => {
            let codes = vec!["000300.SH".to_string()];
            match quant_data::sync::sync_index_daily(&state.db, &state.tushare, &codes, &recent_start, &today, &dv_id).await {
                Ok(n) => Json(serde_json::json!({"code": 0, "message": format!("同步完成 {} 条", n)})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": e.to_string()})),
            }
        }
        "因子(pv)" => {
            // 直接调用因子回填端点（与 scheduler T+1 同步一致）
            let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
            let url = format!("http://localhost:{}/api/v1/quant/factors/phase7-price-volume-backfill/background", port);
            let backfill_start = (chrono::Utc::now() - chrono::Duration::days(7)).format("%Y%m%d").to_string();
            let payload = serde_json::json!({"start_date": backfill_start, "end_date": today});
            match reqwest::Client::new().post(&url).json(&payload).send().await {
                Ok(_) => Json(serde_json::json!({"code": 0, "message": "因子回填任务已触发，请等待1-2分钟后刷新状态"})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": format!("触发失败: {}", e)})),
            }
        }
        "ML预测" => {
            let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
            let url = format!("http://localhost:{}/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker", port);
            let pred_date = chrono::Utc::now().date_naive();
            let payload = serde_json::json!({
                "model_code": "nlqr_mr", "model_version": "1.0.0",
                "model_version_id": "mdl-p7-wf-wide-qgvrel-h60-v1",
                "data_version_id": "research-full-2016-2026-20260515",
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
            match reqwest::Client::new().post(&url).json(&payload).send().await {
                Ok(_) => Json(serde_json::json!({"code": 0, "message": "ML预测训练已触发，请等待3-5分钟后刷新状态"})),
                Err(e) => Json(serde_json::json!({"code": 1, "message": format!("触发失败: {}", e)})),
            }
        }
        "权益曲线" => {
            Json(serde_json::json!({"code": 1, "message": "权益曲线由回测计算产生，无法直接修复"}))
        }
        _ => {
            Json(serde_json::json!({"code": 1, "message": format!("未知数据项: {}", req.name)}))
        }
    };
    result.into_response()
}
