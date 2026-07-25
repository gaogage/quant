//! API 客户端 — 封装对 quant-api 的 HTTP 请求。
//! 使用 gloo-net (WASM-friendly)，自动附加 JWT + 401 自动刷新。

use gloo_net::http;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::AuthState;

/// API 基址：自适应当前页面 origin（前端从哪个端口加载就请求哪个端口的后端）。
/// 后端单端口同时托管前端静态文件 + API，故 origin 即后端地址。
/// 生产 8080 → 请求 8080；本地 8081 → 请求 8081。零硬编码端口。
fn base_url() -> String {
    web_sys::window()
        .and_then(|w| w.location().origin().ok())
        .filter(|o| o.starts_with("http"))
        .unwrap_or_else(|| "http://localhost:8080".into())
}

/// 健康检查 — 轻量级，不含认证
pub async fn health_check() -> Result<(), String> {
    let resp = http::Request::get(&format!("{}/health", base_url()))
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
    let url = format!("{}/api/v1/auth/refresh", base_url());
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
    let url = format!("{}{}", base_url(), path);
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
    let url = format!("{}{}", base_url(), path);
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
    let url = format!("{}{}", base_url(), path);
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
    let url = format!("{}{}", base_url(), path);
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
    let req = http::Request::post(&format!("{}/api/v1/auth/login", base_url()))
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

pub async fn equity_curve_readiness_audit(strategy_id: &str) -> Result<Value, String> {
    get(&format!("/api/v1/strategies/{}/equity-curve/readiness-audit", strategy_id)).await
}

pub async fn equity_curve_sync(
    strategy_id: &str,
    start_date: Option<String>,
    end_date: Option<String>,
    background: Option<bool>,
) -> Result<Value, String> {
    post(&format!("/api/v1/strategies/{}/equity-curve/sync", strategy_id), &serde_json::json!({
        "start_date": start_date, "end_date": end_date, "background": background,
    })).await
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

pub async fn account_reset(id: &str, initial_capital: f64, start_date: &str) -> Result<Value, String> {
    post(&format!("/api/v1/accounts/{}/reset", id), &serde_json::json!({"initial_capital": initial_capital, "start_date": start_date})).await
}

pub async fn account_push_dingtalk(id: &str) -> Result<Value, String> {
    post(&format!("/api/v1/accounts/{}/push-dingtalk", id), &serde_json::json!({})).await
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

pub async fn admin_update_task(name: &str, enabled: Option<bool>, cron: Option<&str>) -> Result<Value, String> {
    let mut payload = serde_json::json!({});
    if let Some(en) = enabled { payload["enabled"] = serde_json::json!(en); }
    if let Some(cr) = cron { payload["schedule_cron"] = serde_json::json!(cr); }
    put(&format!("/api/v1/admin/tasks/{}", name), &payload).await
}

pub async fn admin_check_task_deps() -> Result<Value, String> {
    get("/api/v1/admin/tasks/check-deps").await
}

pub async fn admin_sync_status() -> Result<Value, String> {
    get("/api/v1/admin/sync/status").await
}

pub async fn blueprint_progress() -> Result<Value, String> {
    get("/api/v1/quant/blueprint/progress").await
}

/// 根据数据源名称触发对应的修复同步（调用后端统一修复端点）
pub async fn admin_repair_data(name: &str) -> Result<Value, String> {
    post("/api/v1/admin/sync/repair", &serde_json::json!({"name": name})).await
}

/// 组件4: 账号依赖加工数据健康检查。无 start/end=轻量新鲜度；带=逐年深度扫描。
pub async fn admin_account_data_health(
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<Value, String> {
    post(
        "/api/v1/quant/data/account-data-health",
        &serde_json::json!({"start_date": start_date, "end_date": end_date}),
    )
    .await
}

/// 组件4: 按检查项返回的 fix_endpoint + fix_params 通用触发修复。
pub async fn admin_repair_by_endpoint(endpoint: &str, params: &Value) -> Result<Value, String> {
    post(endpoint, params).await
}

pub async fn trigger_dingtalk_notify() -> Result<Value, String> {
    post("/api/v1/quant/paper/notify", &serde_json::json!({})).await
}

/// 手动触发调仓(遍历所有 active 模拟盘,复用 14:40 调仓链路)
/// date: None=今日, Some("YYYYMMDD")=指定日期
pub async fn admin_manual_rebalance(date: Option<&str>) -> Result<Value, String> {
    let payload = match date {
        Some(d) => serde_json::json!({ "date": d }),
        None => serde_json::json!({}),
    };
    post("/api/v1/admin/rebalance", &payload).await
}

/// 历史模拟回放 — 清空旧数据后重新回放，一个账号仅保留最新结果
pub async fn run_historical_replay(
    paper_account_id: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Value, String> {
    post("/api/v1/quant/paper/historical-replay", &serde_json::json!({
        "paper_account_id": paper_account_id,
        "start_date": start_date,
        "end_date": end_date,
    })).await
}

/// P3-1: NAV 历史序列(逐日 NAV/日收益/累计收益/回撤)+ 三基准对比，供 dashboard NAV 曲线用
pub async fn get_nav_history(account_id: &str) -> Result<Value, String> {
    get(&format!("/api/v1/accounts/{}/nav-history", account_id)).await
}

/// 带日期范围的 NAV 历史（账号详情页收益率曲线用），start/end 格式 YYYY-MM-DD
pub async fn get_nav_history_range(account_id: &str, start: &str, end: &str) -> Result<Value, String> {
    get(&format!("/api/v1/accounts/{}/nav-history?start_date={}&end_date={}", account_id, start, end)).await
}

/// P3-2: v24 14 因子每日覆盖率 + 滞缓状态
pub async fn admin_factor_health() -> Result<Value, String> {
    get("/api/v1/admin/factor-health").await
}

/// P3-3: 调仓历史(按交易日分组摘要)
pub async fn get_rebalance_history(account_id: &str) -> Result<Value, String> {
    get(&format!("/api/v1/accounts/{}/rebalance-history", account_id)).await
}
