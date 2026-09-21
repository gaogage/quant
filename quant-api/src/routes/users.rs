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

use crate::auth::jwt::{create_access_token, create_refresh_token, verify_token};
use crate::auth::middleware::UserContext;
use crate::AppState;

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
        .bind(&uid)
        .execute(&state.db)
        .await;

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
        "SELECT EXISTS(SELECT 1 FROM user_session WHERE refresh_token = $1 AND expires_at > NOW())",
    )
    .bind(&req.refresh_token)
    .fetch_optional(&state.db)
    .await;

    match exists {
        Ok(Some((true,))) => {}
        _ => return Json(serde_json::json!({"code": 401, "message": "Refresh token已过期"})),
    }

    // Issue new access token
    let access =
        create_access_token(&claims.sub, &claims.username, &claims.role).unwrap_or_default();

    Json(serde_json::json!({"code": 0, "data": {"access_token": access}}))
}

/// POST /api/v1/auth/logout
pub async fn logout(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    let _ = sqlx::query("DELETE FROM user_session WHERE refresh_token = $1 AND user_id = $2")
        .bind(&req.refresh_token)
        .bind(&user.user_id)
        .execute(&state.db)
        .await;

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
    let hash: (String,) =
        match sqlx::query_as("SELECT password_hash FROM quant_user WHERE user_id = $1")
            .bind(&user.user_id)
            .fetch_one(&state.db)
            .await
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
    let _ = sqlx::query(
        "UPDATE quant_user SET password_hash = $1, updated_at = NOW() WHERE user_id = $2",
    )
    .bind(&new_hash)
    .bind(&user.user_id)
    .execute(&state.db)
    .await;

    // Invalidate all sessions
    let _ = sqlx::query("DELETE FROM user_session WHERE user_id = $1")
        .bind(&user.user_id)
        .execute(&state.db)
        .await;

    Json(serde_json::json!({"code": 0, "message": "密码已修改，请重新登录"}))
}

// ─── 第三批补充测试（非 ignored，秒级，真实本机 PG）─────────────────────
//
// 安全边界：不测登录成功/改密成功等写库路径（会写 quant_user / user_session）；
// login 只测未知用户早退；refresh 测无效 token 与无 session 早退（只读）；
// change_password 测用户缺失早退；logout 对未知 session 删除零行（幂等）。

#[cfg(test)]
mod third_batch {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;

    async fn test_state() -> std::sync::Arc<crate::AppState> {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("test db connect");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("Tushare client init (需 TUSHARE_TOKEN: source ../.env)");
        std::sync::Arc::new(crate::AppState {
            start_time: Utc::now(),
            db,
            tushare,
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    async fn resp_json(resp: impl IntoResponse) -> serde_json::Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    #[tokio::test]
    async fn login_rejects_unknown_username_with_401() {
        let state = test_state().await;
        let resp = login(
            State(state),
            Json(LoginRequest {
                username: "zzz_test_no_such_user".to_string(),
                password: "whatever".to_string(),
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // 用户不存在与密码错误统一话术（防用户名枚举）
        assert_eq!(body["code"], 401, "{body}");
        assert_eq!(body["message"], "用户名或密码错误");
    }

    #[tokio::test]
    async fn refresh_rejects_malformed_token_without_db_lookup() {
        let state = test_state().await;
        let resp = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: "zzz-not-a-jwt".to_string(),
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // JWT 签名校验在 session 查询之前失败
        assert_eq!(body["code"], 401, "{body}");
        assert_eq!(body["message"], "Refresh token无效");
    }

    #[tokio::test]
    async fn refresh_rejects_valid_token_without_live_session() {
        let state = test_state().await;
        // 签名合法但从未落 session 的 token（jti 随机，不可能命中库内行）
        let token = create_refresh_token("zzz_test_user", "zzz_test", "user")
            .expect("create refresh token");
        let resp = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: token,
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // 签名通过 → 走 session EXISTS 只读查询 → 无 session 判过期
        assert_eq!(body["code"], 401, "{body}");
        assert_eq!(body["message"], "Refresh token已过期");
    }

    #[tokio::test]
    async fn logout_is_idempotent_for_unknown_session() {
        let state = test_state().await;
        let user = UserContext {
            user_id: "zzz_test_user".to_string(),
            username: "zzz_test".to_string(),
            role: "user".to_string(),
        };
        let resp = logout(
            State(state),
            user,
            Json(RefreshRequest {
                refresh_token: "zzz_test_unknown_refresh_token".to_string(),
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // DELETE 无匹配行也按成功返回（幂等登出）
        assert_eq!(body["code"], 0, "{body}");
        assert_eq!(body["message"], "已登出");
    }

    #[tokio::test]
    async fn change_password_reports_error_when_user_row_missing() {
        let state = test_state().await;
        let resp = change_password(
            State(state),
            UserContext {
                user_id: "zzz_test_no_such_user_id".to_string(),
                username: "zzz_test".to_string(),
                role: "user".to_string(),
            },
            Json(ChangePasswordRequest {
                old_password: "old".to_string(),
                new_password: "new".to_string(),
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // fetch_one 未命中行 → 查询失败早退，不触任何写路径
        assert_eq!(body["code"], 500, "{body}");
        assert_eq!(body["message"], "查询用户失败");
    }

    #[tokio::test]
    async fn get_me_echoes_user_context() {
        // 纯 extractor：不触库，直接回显上下文
        let resp = get_me(UserContext {
            user_id: "u-123".to_string(),
            username: "alice".to_string(),
            role: "admin".to_string(),
        })
        .await;
        let body = resp_json(resp).await;
        assert_eq!(body["code"], 0);
        assert_eq!(body["data"]["user_id"], "u-123");
        assert_eq!(body["data"]["username"], "alice");
        assert_eq!(body["data"]["role"], "admin");
    }
}
