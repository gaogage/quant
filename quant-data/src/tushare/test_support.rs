//! 测试支撑基建：零依赖本地 mock Tushare HTTP server + 三个测试模块挂载点。
//!
//! 本文件整体 `#![cfg(test)]`，不进产品构建；产品代码唯一触点是
//! `mod.rs` 中的 `#[cfg(test)] pub(crate) mod test_support;` 一行。
//!
//! 三个测试模块经 `#[path]` 挂在本模块下（同样仅测试编译）：
//! - `client_tests`：client.rs 的 HTTP 层行为（参数组装/错误映射/双 token 配对切换）
//! - `repository_tests`：repository.rs 的真实 PG 读写（zzz_test 前缀自造自清理）
//! - `sync_tests`：sync.rs 主链（mock client + 真实 PG）
#![cfg(test)]

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::tushare::client::{TushareClient, TushareConfig};

/// mock 路由的单条响应定义。
///
/// 与真实 Tushare HTTP 形态对齐：
/// 成功 `{"request_id":..,"code":0,"data":{"fields":[..],"items":[[..]]}}`；
/// 业务错误 `{"code":40101,"msg":..}`（HTTP 仍 200）。
#[derive(Clone)]
pub(crate) enum MockResponse {
    /// 恒定返回固定行（单页，行数由 items 决定）
    Rows {
        fields: Vec<&'static str>,
        items: Vec<Vec<Value>>,
    },
    /// 按请求 params.offset 切片分页返回：`items[offset..offset+page_size]`。
    /// 返回行数 < 调用方 page_limit 时调用方停止翻页（对齐 sync 层分页协议）。
    Paged {
        fields: Vec<&'static str>,
        items: Vec<Vec<Value>>,
        page_size: usize,
    },
    /// Tushare 业务错误：HTTP 200 + code!=0（如 40101 token 无效 / 40203 限流）
    ApiErr { code: i32, msg: String },
    /// HTTP 层非 2xx（如网关 502）
    HttpErr { status: u16, body: String },
    /// 延迟 delay_ms 后返回 Ok 空数据（超时测试用）
    SlowOk { delay_ms: u64 },
    /// code=0 且 data 为 null（接口正常但无数据）
    EmptyOk,
}

/// mock server 收到的一次请求快照（供断言参数组装 / token 配对切换）
#[derive(Clone, Debug)]
pub(crate) struct RecordedRequest {
    pub api_name: String,
    pub token: String,
    /// 请求 params（与 TushareRequest 同构，值为 JSON 字符串）
    pub params: HashMap<String, Value>,
    /// 请求 fields（client 层空 fields 会序列化为 Some([])）
    pub fields: Option<Vec<String>>,
}

/// 运行中的 mock server 句柄。
///
/// Drop 时自动通知 accept 循环退出（oneshot sender drop 即完成信号），
/// 测试结尾也可显式 `shutdown().await`；二者均不泄漏进程。
pub(crate) struct MockTushare {
    /// 直接可作为 TushareConfig.base_url（client 会 POST 到该 URL 本身）
    pub base_url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockTushare {
    /// 全部收到的请求（按到达顺序）
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.recorded.lock().unwrap().clone()
    }

    /// 只看指定 api_name 的请求
    pub fn requests_for(&self, api_name: &str) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|r| r.api_name == api_name)
            .collect()
    }

    /// 显式关闭（Drop 亦会自动关闭，这里提供确定性的 await 点）
    pub fn shutdown(mut self) {
        let _ = self.shutdown_tx.take().map(|tx| tx.send(()));
    }
}

impl Drop for MockTushare {
    fn drop(&mut self) {
        // sender drop 后 select! 分支立即完成，accept 循环退出
        let _ = self.shutdown_tx.take();
    }
}

/// 启动本地 mock Tushare server：127.0.0.1 随机端口，按 api_name 路由。
///
/// `routes`: (api_name, MockResponse) 列表；未注册的 api_name 返回
/// 业务错误 code=-100（让测试 fail-fast 而非静默拿空数据）。
pub(crate) async fn spawn_mock_tushare(routes: Vec<(&'static str, MockResponse)>) -> MockTushare {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 随机端口");
    let port = listener.local_addr().expect("local addr").port();
    let base_url = format!("http://127.0.0.1:{}", port);

    let router: HashMap<&'static str, MockResponse> = routes.into_iter().collect();
    let router = Arc::new(router);
    let recorded: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));

    let (tx, rx) = oneshot::channel::<()>();
    let mut rx = rx;
    // 循环外一次性转为 tokio listener（from_std 设非阻塞；accept 借用 &self，
    // select 每轮重新求值；TcpListener::accept 是 cancel-safe，无丢失连接风险）
    listener.set_nonblocking(true).expect("listener 设非阻塞");
    let listener = tokio::net::TcpListener::from_std(listener).expect("listener 转tokio");

    // async move 块会 move 捕获，先 clone 循环内副本，原件留给 MockTushare 返回值。
    let recorded_loop = Arc::clone(&recorded);
    let router_loop = Arc::clone(&router);
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                _ = &mut rx => break,
                accepted = listener.accept() => accepted,
            };
            let (stream, _) = match accepted {
                Ok(pair) => pair,
                Err(_) => continue, // accept 瞬时错误（如 EMFILE）跳过本轮
            };
            let router = Arc::clone(&router_loop);
            let recorded = Arc::clone(&recorded_loop);
            tokio::spawn(async move {
                handle_connection(stream, router, recorded).await;
            });
        }
    });

    MockTushare {
        base_url,
        recorded,
        shutdown_tx: Some(tx),
    }
}

/// 构造直连 mock 的客户端配置（限流放大避免测试被 governor 桶拖慢）
pub(crate) fn mock_config(base_url: &str) -> TushareConfig {
    TushareConfig {
        base_url: base_url.to_string(),
        token: "test-primary-token".to_string(),
        fallback_token: None,
        fallback_base_url: None,
        timeout_secs: 10,
        max_retries: 3,
        retry_delay_ms: 100,
        // governor 令牌桶：10 万/分钟 = ~1667/s 补充，单测试几十次调用零等待
        rate_limit_per_minute: 100_000,
    }
}

/// 基于本机 quant 库的连接池。
/// 默认地址用 127.0.0.1 而非 localhost：避免域名解析先试 ::1 的
/// 连接抖动（慢源治理）；DATABASE_URL 显式设置时优先。
pub(crate) async fn local_pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@127.0.0.1/quant".into());
    sqlx::PgPool::connect(&url)
        .await
        .expect("connect local quant db")
}

/// 单个连接处理：读一个 HTTP 请求 → 路由 → 写响应 → 关连接（Connection: close）
async fn handle_connection(
    mut stream: TcpStream,
    router: Arc<HashMap<&'static str, MockResponse>>,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
) {
    use tokio::io::AsyncReadExt;

    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    let mut chunk = [0u8; 4096];

    // 1) 读到 header 结束符 \r\n\r\n
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return, // 连接中断，放弃
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };

    // 2) 解析 Content-Length，继续读完 body
    let header = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let content_length = header
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let body_end = header_end + 4 + content_length;
    while buf.len() < body_end {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let body_bytes = &buf[header_end + 4..body_end];

    // 3) 解析 Tushare JSON 请求体（与 client 的 TushareRequest 同构）
    let parsed: Result<crate::model::tushare_dto::TushareRequest, _> =
        serde_json::from_slice(body_bytes);
    let req = match parsed {
        Ok(req) => req,
        Err(_) => {
            // 请求体不合法：回 400（client 走 HTTP 错误分支）
            let body = br#"{"code":-3,"msg":"mock: bad json"}"#.to_vec();
            let _ = write_simple_response(&mut stream, 400, body).await;
            return;
        }
    };

    // 4) 记录请求（断言参数组装/双 token 切换的事实依据）
    recorded.lock().unwrap().push(RecordedRequest {
        api_name: req.api_name.clone(),
        token: req.token.clone(),
        params: req.params.clone(),
        fields: req.fields.clone(),
    });

    // 5) 路由分发
    let response = match router.get(req.api_name.as_str()) {
        Some(mock) => build_response(mock, &req).await,
        None => (
            200u16,
            json!({"request_id": "mock", "code": -100,
                   "msg": format!("mock: no route for api {}", req.api_name)}),
        ),
    };

    let (status, body) = response;
    let body_bytes = body.to_string().into_bytes();
    let _ = write_simple_response(&mut stream, status, body_bytes).await;
}

/// 按响应变体构造 (HTTP status, JSON body)
async fn build_response(
    mock: &MockResponse,
    req: &crate::model::tushare_dto::TushareRequest,
) -> (u16, Value) {
    match mock {
        MockResponse::Rows { fields, items } => (
            200,
            json!({
                "request_id": "mock",
                "code": 0,
                "data": {"fields": fields, "items": items},
            }),
        ),
        MockResponse::Paged {
            fields,
            items,
            page_size,
        } => {
            // 从请求 params 读 offset（缺省 0），返回 items[offset..offset+page_size]
            let offset = req
                .params
                .get("offset")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let start = offset.min(items.len());
            let end = (start + page_size).min(items.len());
            let page = &items[start..end];
            (
                200,
                json!({
                    "request_id": "mock",
                    "code": 0,
                    "data": {"fields": fields, "items": page},
                }),
            )
        }
        MockResponse::ApiErr { code, msg } => {
            (200, json!({"request_id": "mock", "code": code, "msg": msg}))
        }
        MockResponse::HttpErr { status, body } => (*status, json!(body.clone())),
        MockResponse::SlowOk { delay_ms } => {
            tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
            (
                200,
                json!({
                    "request_id": "mock",
                    "code": 0,
                    "data": {"fields": ["ts_code"], "items": []},
                }),
            )
        }
        MockResponse::EmptyOk => (200, json!({"request_id": "mock", "code": 0, "data": null})),
    }
}

/// 写一个最小 HTTP 响应并关连接
async fn write_simple_response(
    stream: &mut TcpStream,
    status: u16,
    body: Vec<u8>,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        502 => "Bad Gateway",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        reason,
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

/// 在 buf 中查找子序列（HTTP 协议边界探测用，避免引入 memchr 依赖）
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// 为 mock server 构造客户端（全部测试共用入口）
pub(crate) fn client_for(base_url: &str) -> TushareClient {
    TushareClient::new(mock_config(base_url)).expect("mock client")
}

// 三个测试模块挂在 test_support（cfg(test)）下，产品构建不编译
#[path = "client_tests.rs"]
pub(crate) mod client_tests;
#[path = "repository_tests.rs"]
pub(crate) mod repository_tests;
#[path = "sync_tests.rs"]
pub(crate) mod sync_tests;
