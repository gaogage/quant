//! User management and authentication API.
//!
//! POST /api/v1/auth/login    — login, returns JWT pair
//! POST /api/v1/auth/refresh  — refresh access token
//! POST /api/v1/auth/logout   — logout
//! GET  /api/v1/users/me      — get current user info
//! PUT  /api/v1/users/me/password — change own password

use axum::{extract::State, response::IntoResponse, Json};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::AppState;
use crate::auth::jwt::{create_access_token, create_refresh_token, verify_token};
use crate::auth::middleware::UserContext;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub user: UserInfo,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct UserInfo {
    pub user_id: String,
    pub username: String,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub role: String,
    pub status: String,
}

/// POST /api/v1/auth/login
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    // Find user
    let user = sqlx::query_as::<_, (String, String, String, Option<String>, Option<String>, String, String)>(
        "SELECT user_id, username, password_hash, display_name, email, role, status FROM quant_user WHERE username = $1"
    )
    .bind(&req.username)
    .fetch_optional(&state.db)
    .await;

    let (uid, uname, hash, dname, email, role, status) = match user {
        Ok(Some(row)) => row,
        _ => return Json(serde_json::json!({"code": 401, "message": "用户名或密码错误"})),
    };

    if status != "active" {
        return Json(serde_json::json!({"code": 403, "message": "账号已禁用"}));
    }

    // Verify password
    match bcrypt::verify(&req.password, &hash) {
        Ok(true) => {}
        _ => return Json(serde_json::json!({"code": 401, "message": "用户名或密码错误"})),
    }

    // Generate tokens
    let access = create_access_token(&uid, &uname, &role).unwrap_or_default();
    let refresh = create_refresh_token(&uid, &uname, &role).unwrap_or_default();

    // Store refresh token
    let session_id = Uuid::new_v4().to_string();
    let expires = Utc::now() + Duration::days(7);
    let _ = sqlx::query(
        "INSERT INTO user_session (session_id, user_id, refresh_token, expires_at) VALUES ($1, $2, $3, $4)"
    )
    .bind(&session_id).bind(&uid).bind(&refresh).bind(expires)
    .execute(&state.db).await;

    // Update last login
    let _ = sqlx::query("UPDATE quant_user SET last_login_at = NOW() WHERE user_id = $1")
        .bind(&uid).execute(&state.db).await;

    Json(serde_json::json!({
        "code": 0,
        "data": LoginResponse {
            access_token: access,
            refresh_token: refresh,
            user: UserInfo { user_id: uid, username: uname, display_name: dname, email, role, status },
        }
    }))
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// POST /api/v1/auth/refresh
pub async fn refresh(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    // Verify refresh token
    let claims = match verify_token(&req.refresh_token) {
        Ok(c) => c,
        Err(_) => return Json(serde_json::json!({"code": 401, "message": "Refresh token无效"})),
    };

    // Check session exists
    let exists = sqlx::query_as::<_, (bool,)>(
        "SELECT EXISTS(SELECT 1 FROM user_session WHERE refresh_token = $1 AND expires_at > NOW())"
    )
    .bind(&req.refresh_token)
    .fetch_optional(&state.db).await;

    match exists {
        Ok(Some((true,))) => {}
        _ => return Json(serde_json::json!({"code": 401, "message": "Refresh token已过期"})),
    }

    // Issue new access token
    let access = create_access_token(&claims.sub, &claims.username, &claims.role).unwrap_or_default();

    Json(serde_json::json!({"code": 0, "data": {"access_token": access}}))
}

/// POST /api/v1/auth/logout
pub async fn logout(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    let _ = sqlx::query("DELETE FROM user_session WHERE refresh_token = $1 AND user_id = $2")
        .bind(&req.refresh_token).bind(&user.user_id)
        .execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": "已登出"}))
}

/// GET /api/v1/users/me
pub async fn get_me(user: UserContext) -> impl IntoResponse {
    Json(serde_json::json!({
        "code": 0,
        "data": {
            "user_id": user.user_id,
            "username": user.username,
            "role": user.role,
        }
    }))
}

#[derive(Debug, Deserialize)]
pub struct ChangePasswordRequest {
    pub old_password: String,
    pub new_password: String,
}

/// PUT /api/v1/users/me/password
pub async fn change_password(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<ChangePasswordRequest>,
) -> impl IntoResponse {
    // Verify old password
    let hash: (String,) = match sqlx::query_as("SELECT password_hash FROM quant_user WHERE user_id = $1")
        .bind(&user.user_id).fetch_one(&state.db).await
    {
        Ok(row) => row,
        Err(_) => return Json(serde_json::json!({"code": 500, "message": "查询用户失败"})),
    };

    match bcrypt::verify(&req.old_password, &hash.0) {
        Ok(true) => {}
        _ => return Json(serde_json::json!({"code": 400, "message": "旧密码错误"})),
    }

    // Hash and update new password
    let new_hash = bcrypt::hash(&req.new_password, 12).unwrap_or_default();
    let _ = sqlx::query("UPDATE quant_user SET password_hash = $1, updated_at = NOW() WHERE user_id = $2")
        .bind(&new_hash).bind(&user.user_id)
        .execute(&state.db).await;

    // Invalidate all sessions
    let _ = sqlx::query("DELETE FROM user_session WHERE user_id = $1")
        .bind(&user.user_id).execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": "密码已修改，请重新登录"}))
}
