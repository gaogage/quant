//! API 客户端 — 封装对 quant-api 的 HTTP 请求。
//! 使用 gloo-net (WASM-friendly)，自动附加 JWT + 401 自动刷新。

use gloo_net::http;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::AuthState;

const BASE_URL: &str = "http://localhost:8080";

/// 健康检查 — 轻量级，不含认证
pub async fn health_check() -> Result<(), String> {
    let resp = http::Request::get(&format!("{}/health", BASE_URL))
        .send()
        .await
        .map_err(|e| format!("连接失败: {}", e))?;
    if resp.status() == 200 {
        Ok(())
    } else {
        Err(format!("服务异常: {}", resp.status()))
    }
}

fn auth_header() -> Option<String> {
    let token = AuthState::access_token();
    if token.is_empty() {
        None
    } else {
        Some(format!("Bearer {}", token))
    }
}

fn add_auth(mut req: http::RequestBuilder) -> http::RequestBuilder {
    if let Some(auth) = auth_header() {
        req = req.header("Authorization", &auth);
    }
    req
}

// ── 401 自动刷新 ───────────────────────────────────────

async fn try_refresh() -> bool {
    let refresh = AuthState::refresh_token();
    if refresh.is_empty() {
        AuthState::logout();
        return false;
    }
    let url = format!("{}/api/v1/auth/refresh", BASE_URL);
    let body_str = serde_json::to_string(&serde_json::json!({"refresh_token": refresh})).unwrap();

    let builder = http::Request::post(&url).header("Content-Type", "application/json");
    let req = match builder.body(body_str) {
        Ok(r) => r,
        Err(_) => { AuthState::logout(); return false; }
    };
    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => { AuthState::logout(); return false; }
    };

    if let Ok(body) = resp.json::<Value>().await {
        if body["code"].as_i64().unwrap_or(-1) == 0 {
            if let Some(token) = body["data"]["access_token"].as_str() {
                AuthState::update_access_token(token);
                return true;
            }
        }
    }
    AuthState::logout();
    false
}

// ── Generic helpers ───────────────────────────────────

async fn get(path: &str) -> Result<Value, String> {
    let url = format!("{}{}", BASE_URL, path);
    // first try
    let builder1 = http::Request::get(&url).header("Content-Type", "application/json");
    let resp = add_auth(builder1).send().await.map_err(|e| format!("网络错误: {}", e))?;
    if resp.status() == 401 && try_refresh().await {
        // retry
        let builder2 = http::Request::get(&url).header("Content-Type", "application/json");
        let resp2 = add_auth(builder2).send().await.map_err(|e| format!("网络错误: {}", e))?;
        return resp2.json().await.map_err(|e| format!("解析失败: {}", e));
    }
    resp.json().await.map_err(|e| format!("解析失败: {}", e))
}

async fn post(path: &str, payload: &Value) -> Result<Value, String> {
    let url = format!("{}{}", BASE_URL, path);
    let body_str = serde_json::to_string(payload).map_err(|e| format!("序列化失败: {}", e))?;
    // first try
    let b1 = http::Request::post(&url).header("Content-Type", "application/json");
    let req1 = add_auth(b1).body(body_str.clone()).map_err(|e| format!("构建请求失败: {}", e))?;
    let resp = req1.send().await.map_err(|e| format!("网络错误: {}", e))?;
    if resp.status() == 401 && try_refresh().await {
        // retry
        let b2 = http::Request::post(&url).header("Content-Type", "application/json");
        let req2 = add_auth(b2).body(body_str).map_err(|e| format!("构建请求失败: {}", e))?;
        let resp2 = req2.send().await.map_err(|e| format!("网络错误: {}", e))?;
        return resp2.json().await.map_err(|e| format!("解析失败: {}", e));
    }
    resp.json().await.map_err(|e| format!("解析失败: {}", e))
}

async fn put(path: &str, payload: &Value) -> Result<Value, String> {
    let url = format!("{}{}", BASE_URL, path);
    let body_str = serde_json::to_string(payload).map_err(|e| format!("序列化失败: {}", e))?;
    // first try
    let b1 = http::Request::put(&url).header("Content-Type", "application/json");
    let req1 = add_auth(b1).body(body_str.clone()).map_err(|e| format!("构建请求失败: {}", e))?;
    let resp = req1.send().await.map_err(|e| format!("网络错误: {}", e))?;
    if resp.status() == 401 && try_refresh().await {
        // retry
        let b2 = http::Request::put(&url).header("Content-Type", "application/json");
        let req2 = add_auth(b2).body(body_str).map_err(|e| format!("构建请求失败: {}", e))?;
        let resp2 = req2.send().await.map_err(|e| format!("网络错误: {}", e))?;
        return resp2.json().await.map_err(|e| format!("解析失败: {}", e));
    }
    resp.json().await.map_err(|e| format!("解析失败: {}", e))
}

#[allow(dead_code)]
async fn delete(path: &str) -> Result<Value, String> {
    let url = format!("{}{}", BASE_URL, path);
    let b1 = http::Request::delete(&url).header("Content-Type", "application/json");
    let resp = add_auth(b1).send().await.map_err(|e| format!("网络错误: {}", e))?;
    if resp.status() == 401 && try_refresh().await {
        let b2 = http::Request::delete(&url).header("Content-Type", "application/json");
        let resp2 = add_auth(b2).send().await.map_err(|e| format!("网络错误: {}", e))?;
        return resp2.json().await.map_err(|e| format!("解析失败: {}", e));
    }
    resp.json().await.map_err(|e| format!("解析失败: {}", e))
}

// ── Auth ──────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginData {
    pub access_token: String,
    pub refresh_token: String,
    pub user: UserInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: String,
    pub username: String,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub role: String,
    pub status: String,
}

pub async fn login(username: &str, password: &str) -> Result<LoginData, String> {
    let body = serde_json::json!({"username": username, "password": password});
    let body_str = serde_json::to_string(&body).unwrap();
    let req = http::Request::post(&format!("{}/api/v1/auth/login", BASE_URL))
        .header("Content-Type", "application/json")
        .body(body_str)
        .map_err(|e| format!("构建请求失败: {}", e))?;
    let resp = req.send().await.map_err(|e| format!("网络错误: {}", e))?;
    let raw: Value = resp.json().await.map_err(|e| format!("解析失败: {}", e))?;
    let code = raw["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        return Err(raw["message"].as_str().unwrap_or("登录失败").to_string());
    }
    serde_json::from_value::<LoginData>(raw["data"].clone())
        .map_err(|e| format!("解析失败: {}", e))
}

// ── Users ─────────────────────────────────────────────

pub async fn get_me() -> Result<Value, String> {
    get("/api/v1/users/me").await
}

pub async fn change_password(old_password: &str, new_password: &str) -> Result<Value, String> {
    put(
        "/api/v1/users/me/password",
        &serde_json::json!({"old_password": old_password, "new_password": new_password}),
    ).await
}

// ── Strategies ────────────────────────────────────────

pub async fn list_strategies() -> Result<Value, String> {
    get("/api/v1/strategies").await
}

pub async fn get_strategy(id: &str) -> Result<Value, String> {
    get(&format!("/api/v1/strategies/{}", id)).await
}

pub async fn create_strategy(
    base_strategy_id: &str, name: &str, description: &str, params: &Value,
) -> Result<Value, String> {
    post("/api/v1/strategies", &serde_json::json!({
        "base_strategy_id": base_strategy_id, "name": name,
        "description": description, "params": params,
    })).await
}

pub async fn update_strategy(id: &str, payload: &Value) -> Result<Value, String> {
    put(&format!("/api/v1/strategies/{}", id), payload).await
}

pub async fn delete_strategy(id: &str) -> Result<Value, String> {
    delete(&format!("/api/v1/strategies/{}", id)).await
}

// ── Accounts ──────────────────────────────────────────

pub async fn list_accounts(filter: &str) -> Result<Value, String> {
    let path = if filter.is_empty() {
        "/api/v1/accounts".to_string()
    } else {
        format!("/api/v1/accounts?{}", filter)
    };
    get(&path).await
}

pub async fn create_account(payload: &Value) -> Result<Value, String> {
    post("/api/v1/accounts", payload).await
}

pub async fn update_account(id: &str, payload: &Value) -> Result<Value, String> {
    put(&format!("/api/v1/accounts/{}", id), payload).await
}

pub async fn get_account_detail(id: &str) -> Result<Value, String> {
    get(&format!("/api/v1/accounts/{}", id)).await
}

pub async fn delete_account(id: &str) -> Result<Value, String> {
    delete(&format!("/api/v1/accounts/{}", id)).await
}

// ── Admin ─────────────────────────────────────────────

pub async fn admin_list_users() -> Result<Value, String> {
    get("/api/v1/admin/users").await
}

pub async fn admin_create_user(payload: &Value) -> Result<Value, String> {
    post("/api/v1/admin/users", payload).await
}

pub async fn admin_update_user(id: &str, payload: &Value) -> Result<Value, String> {
    put(&format!("/api/v1/admin/users/{}", id), payload).await
}

pub async fn admin_delete_user(id: &str) -> Result<Value, String> {
    delete(&format!("/api/v1/admin/users/{}", id)).await
}

pub async fn admin_reset_password(id: &str, new_password: &str) -> Result<Value, String> {
    put(
        &format!("/api/v1/admin/users/{}/password", id),
        &serde_json::json!({"new_password": new_password}),
    ).await
}

pub async fn admin_list_tasks() -> Result<Value, String> {
    get("/api/v1/admin/tasks").await
}

pub async fn admin_sync_status() -> Result<Value, String> {
    get("/api/v1/admin/sync/status").await
}

pub async fn trigger_dingtalk_notify() -> Result<Value, String> {
    post("/api/v1/quant/paper/notify", &serde_json::json!({})).await
}
