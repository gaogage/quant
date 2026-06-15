//! 策略管理 API — 用户可从系统策略派生自己的策略，修改参数，删除。
//!
//! GET    /api/v1/strategies        — 策略列表（系统 + 自己的）
//! GET    /api/v1/strategies/{id}   — 策略详情 + 参数
//! POST   /api/v1/strategies        — 从系统策略派生新策略
//! PUT    /api/v1/strategies/{id}   — 修改自己的策略参数
//! DELETE /api/v1/strategies/{id}   — 删除自己的策略

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::UserContext;
use crate::AppState;

// ── 请求体 ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateStrategyRequest {
    pub base_strategy_id: String,
    pub name: String,
    pub description: Option<String>,
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStrategyRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub params: Option<serde_json::Value>,
}

// ── 列表 ────────────────────────────────────────────────

/// GET /api/v1/strategies — 策略列表（系统 strategy_config + 自己的 user_strategy）
pub async fn list_strategies(
    State(state): State<Arc<AppState>>,
    user: UserContext,
) -> impl IntoResponse {
    // 系统策略
    let sys_rows = sqlx::query(
        "SELECT strategy_id, name, description, status,
                etf_symbols, default_weights, vol_target, leverage_cap, leverage_floor,
                min_stock, max_single, max_single_bull, momentum_blend_ratio,
                rebalance_freq, ga_population, ga_generations, risk_free_rate
         FROM strategy_config ORDER BY strategy_id",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    use sqlx::Row;
    let mut list: Vec<serde_json::Value> = sys_rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "strategy_id": r.try_get::<String, _>("strategy_id").unwrap_or_default(),
                "name": r.try_get::<String, _>("name").unwrap_or_default(),
                "description": r.try_get::<Option<String>, _>("description").unwrap_or(None),
                "status": r.try_get::<String, _>("status").unwrap_or_default(),
                "params": serde_json::json!({
                    "etf_symbols": r.try_get::<serde_json::Value, _>("etf_symbols").unwrap_or(serde_json::Value::Null),
                    "default_weights": r.try_get::<serde_json::Value, _>("default_weights").unwrap_or(serde_json::Value::Null),
                    "vol_target": r.try_get::<Option<f64>, _>("vol_target").unwrap_or(None),
                    "leverage_cap": r.try_get::<Option<f64>, _>("leverage_cap").unwrap_or(None),
                    "leverage_floor": r.try_get::<Option<f64>, _>("leverage_floor").unwrap_or(None),
                    "min_stock": r.try_get::<Option<f64>, _>("min_stock").unwrap_or(None),
                    "max_single": r.try_get::<Option<f64>, _>("max_single").unwrap_or(None),
                    "max_single_bull": r.try_get::<Option<f64>, _>("max_single_bull").unwrap_or(None),
                    "momentum_blend_ratio": r.try_get::<Option<f64>, _>("momentum_blend_ratio").unwrap_or(None),
                    "rebalance_freq": r.try_get::<String, _>("rebalance_freq").unwrap_or_default(),
                    "ga_population": r.try_get::<Option<i32>, _>("ga_population").unwrap_or(None),
                    "ga_generations": r.try_get::<Option<i32>, _>("ga_generations").unwrap_or(None),
                    "risk_free_rate": r.try_get::<Option<f64>, _>("risk_free_rate").unwrap_or(None),
                }),
                "owner": "system",
            })
        })
        .collect();

    // 用户自己的策略
    let my_rows: Vec<(String, String, Option<String>, serde_json::Value, String)> = sqlx::query_as(
        "SELECT strategy_id, name, description, params, status FROM user_strategy WHERE user_id = $1 ORDER BY created_at DESC"
    )
    .bind(&user.user_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    for (id, name, desc, params, status) in my_rows {
        list.push(serde_json::json!({
            "strategy_id": id, "name": name, "description": desc,
            "params": params, "status": status, "owner": "me",
        }));
    }

    Json(serde_json::json!({"code": 0, "data": list}))
}

// ── 详情 ────────────────────────────────────────────────

/// GET /api/v1/strategies/{id}
pub async fn get_strategy(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // 先查系统策略
    let sys = sqlx::query_as::<_, (String, Option<String>, serde_json::Value, String)>(
        "SELECT strategy_name, description, params, status FROM strategy_config WHERE strategy_id = $1"
    )
    .bind(&strategy_id)
    .fetch_optional(&state.db)
    .await;

    if let Ok(Some((name, desc, params, status))) = sys {
        return Json(serde_json::json!({
            "code": 0, "data": {
                "strategy_id": strategy_id, "name": name, "description": desc,
                "params": params, "status": status, "owner": "system",
            }
        }));
    }

    // 再查用户策略
    let my = sqlx::query_as::<_, (String, Option<String>, serde_json::Value, String, String)>(
        "SELECT name, description, params, status, user_id FROM user_strategy WHERE strategy_id = $1"
    )
    .bind(&strategy_id)
    .fetch_optional(&state.db)
    .await;

    match my {
        Ok(Some((name, desc, params, status, owner_id))) => {
            let owner = if owner_id == user.user_id {
                "me"
            } else {
                &owner_id
            };
            Json(serde_json::json!({
                "code": 0, "data": {
                    "strategy_id": strategy_id, "name": name, "description": desc,
                    "params": params, "status": status, "owner": owner,
                }
            }))
        }
        _ => Json(serde_json::json!({"code": 404, "message": "策略不存在"})),
    }
}

// ── 创建（派生） ────────────────────────────────────────

/// POST /api/v1/strategies — 从系统策略派生用户策略
pub async fn create_strategy(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<CreateStrategyRequest>,
) -> impl IntoResponse {
    let sid = Uuid::new_v4().to_string();

    match sqlx::query(
        "INSERT INTO user_strategy (strategy_id, user_id, base_strategy_id, name, description, params, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    )
    .bind(&sid).bind(&user.user_id).bind(&req.base_strategy_id)
    .bind(&req.name).bind(&req.description).bind(&req.params).bind(&user.user_id)
    .execute(&state.db).await
    {
        Ok(_) => Json(serde_json::json!({"code": 0, "data": {"strategy_id": sid}})),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("创建失败: {}", e)})),
    }
}

// ── 修改 ────────────────────────────────────────────────

/// PUT /api/v1/strategies/{id} — 修改自己的策略参数
pub async fn update_strategy(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(strategy_id): Path<String>,
    Json(req): Json<UpdateStrategyRequest>,
) -> impl IntoResponse {
    // 校验所有权
    let owner =
        sqlx::query_as::<_, (String,)>("SELECT user_id FROM user_strategy WHERE strategy_id = $1")
            .bind(&strategy_id)
            .fetch_optional(&state.db)
            .await;

    match owner {
        Ok(Some((oid,))) if oid == user.user_id => {}
        Ok(Some(_)) => {
            return Json(serde_json::json!({"code": 403, "message": "只能修改自己的策略"}))
        }
        _ => return Json(serde_json::json!({"code": 404, "message": "策略不存在"})),
    }

    let _ = sqlx::query(
        "UPDATE user_strategy SET name = COALESCE($1, name),
         description = COALESCE($2, description),
         params = COALESCE($3, params),
         updated_by = $4, updated_at = NOW()
         WHERE strategy_id = $5",
    )
    .bind(&req.name)
    .bind(&req.description)
    .bind(&req.params)
    .bind(&user.user_id)
    .bind(&strategy_id)
    .execute(&state.db)
    .await;

    Json(serde_json::json!({"code": 0}))
}

// ── 删除 ────────────────────────────────────────────────

/// DELETE /api/v1/strategies/{id} — 删除自己的策略
pub async fn delete_strategy(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // 校验所有权
    let owner =
        sqlx::query_as::<_, (String,)>("SELECT user_id FROM user_strategy WHERE strategy_id = $1")
            .bind(&strategy_id)
            .fetch_optional(&state.db)
            .await;

    match owner {
        Ok(Some((oid,))) if oid == user.user_id => {}
        Ok(Some(_)) => {
            return Json(serde_json::json!({"code": 403, "message": "只能删除自己的策略"}))
        }
        _ => return Json(serde_json::json!({"code": 404, "message": "策略不存在"})),
    }

    let _ = sqlx::query("DELETE FROM user_strategy WHERE strategy_id = $1")
        .bind(&strategy_id)
        .execute(&state.db)
        .await;

    Json(serde_json::json!({"code": 0, "message": "已删除"}))
}
