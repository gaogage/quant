//! client.rs 的 mock server 测试（Application 层覆盖率专项）。
//!
//! 覆盖：构造分支 / 参数组装 / 响应解码 / call_api 错误映射（业务码、
//! HTTP 状态、解码失败、超时）/ 主备凭证配对切换（2026-09-15 行为）/
//! 剩余 28 个薄封装接口的参数形态与 fields 白名单（表驱动批产）。

use serde_json::{json, Value};

use quant_common::QuantError;

use super::{client_for, mock_config, spawn_mock_tushare, MockResponse, MockTushare};
use crate::tushare::client::TushareClient;

/// 轻量启动只含一个 api 的 mock
async fn single_route(api: &'static str, resp: MockResponse) -> MockTushare {
    spawn_mock_tushare(vec![(api, resp)]).await
}

// ─── 构造分支 ────────────────────────────────────────────────────

#[test]
fn new_rejects_empty_token_with_auth_error() {
    // token 为空 → Auth 错误（不触网）。
    // 注意用 match 而非 expect_err：TushareClient 未实现 Debug
    match TushareClient::new({
        let mut cfg = mock_config("http://127.0.0.1:1");
        cfg.token = String::new();
        cfg
    }) {
        Err(QuantError::Auth(msg)) => {
            assert!(msg.contains("TUSHARE_TOKEN"), "msg={}", msg);
        }
        Err(other) => panic!("应为 Auth 变体，实际 {:?}", other),
        Ok(_) => panic!("空 token 应被 new 拒绝"),
    }
}

#[test]
fn new_accepts_custom_base_url_and_builds_client() {
    // 任意 base_url（含本地 mock）均可构造成功——mock 测试路径的结构性前提
    let cfg = mock_config("http://127.0.0.1:1");
    let _client = TushareClient::new(cfg).expect("非空 token 应构造成功");
}

// ─── 参数组装 + 响应解码（代表性接口）────────────────────────────

#[tokio::test]
async fn stock_basic_sends_exchange_and_list_status_params() {
    let mock = single_route(
        "stock_basic",
        MockResponse::Rows {
            fields: vec!["ts_code", "name", "list_status", "list_date"],
            items: vec![
                vec![
                    json!("000001.SZ"),
                    json!("平安银行"),
                    json!("L"),
                    json!("19910403"),
                ],
                // 第二行含 null：验证 to_maps 用 Null 填充缺失位
                vec![json!("ZZZTST.SH"), Value::Null, json!("L"), Value::Null],
            ],
        },
    )
    .await;
    let client = client_for(&mock.base_url);

    let resp = client
        .stock_basic(Some("SSE"), Some("L"))
        .await
        .expect("stock_basic 应成功");

    // 请求侧：参数组装断言
    let reqs = mock.requests_for("stock_basic");
    assert_eq!(reqs.len(), 1, "应恰好发出一次 stock_basic 调用");
    assert_eq!(reqs[0].params.get("exchange"), Some(&json!("SSE")));
    assert_eq!(reqs[0].params.get("list_status"), Some(&json!("L")));
    assert_eq!(reqs[0].token, "test-primary-token", "应携带主 token");

    // 响应侧：TushareResponse 解码 + to_maps 字段映射断言
    assert_eq!(resp.code, 0);
    let data = resp.data.expect("应有 data");
    assert_eq!(
        data.fields,
        vec!["ts_code", "name", "list_status", "list_date"]
    );
    assert_eq!(data.items.len(), 2);
    let maps = data.to_maps();
    assert_eq!(maps[0].get("ts_code"), Some(&json!("000001.SZ")));
    assert_eq!(maps[0].get("name"), Some(&json!("平安银行")));
    assert_eq!(maps[0].get("list_date"), Some(&json!("19910403")));
    // null 原样映射为 Value::Null（下游 get_str 兜底空串）
    assert_eq!(maps[1].get("name"), Some(&Value::Null));
    assert_eq!(maps[1].get("list_date"), Some(&Value::Null));

    mock.shutdown();
}

#[tokio::test]
async fn stock_basic_omits_absent_params_and_sends_empty_fields() {
    let mock = single_route("stock_basic", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .stock_basic(None, None)
        .await
        .expect("无参数调用应成功");

    let reqs = mock.requests_for("stock_basic");
    assert_eq!(reqs.len(), 1);
    // 两个可选参数均未传 → params 为空（组装分支：None 跳过）
    assert!(reqs[0].params.is_empty(), "params={:?}", reqs[0].params);
    // 空 fields 切片 → Some([])（serde 序列化为 []，非 null）
    assert!(
        matches!(&reqs[0].fields, Some(f) if f.is_empty()),
        "空 fields 应序列化为 Some([])，实际 {:?}",
        reqs[0].fields
    );

    mock.shutdown();
}

#[tokio::test]
async fn daily_batch_joins_codes_and_assembles_paging_params() {
    let mock = single_route("daily", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    // limit + offset 同时传：owned 索引技巧（limit 在前 offset 在后）
    client
        .daily_batch(
            &["000001.SZ".to_string(), "600000.SH".to_string()],
            Some("20260101"),
            Some("20260131"),
            Some(4000),
            Some(8000),
        )
        .await
        .expect("daily_batch 应成功");

    let reqs = mock.requests_for("daily");
    assert_eq!(reqs.len(), 1);
    // 多代码逗号 join
    assert_eq!(
        reqs[0].params.get("ts_code"),
        Some(&json!("000001.SZ,600000.SH"))
    );
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20260131")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("4000")));
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("8000")));

    // 只传 offset 不传 limit：owned 索引回退分支（offset 取 owned[0]）
    client
        .daily_batch(&["000001.SZ".to_string()], None, None, None, Some(123))
        .await
        .expect("仅 offset 调用应成功");
    let reqs = mock.requests_for("daily");
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[1].params.get("offset"), Some(&json!("123")));
    assert!(
        !reqs[1].params.contains_key("limit"),
        "未传 limit 时不应出现 limit 键"
    );

    mock.shutdown();
}

#[tokio::test]
async fn fund_daily_passes_through_all_four_date_params() {
    let mock = single_route("fund_daily", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .fund_daily(Some("510300.SH"), None, Some("20260101"), Some("20261231"))
        .await
        .expect("fund_daily 应成功");

    let reqs = mock.requests_for("fund_daily");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!("510300.SH")));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261231")));
    assert!(
        !reqs[0].params.contains_key("trade_date"),
        "未传 trade_date 不应出现"
    );

    mock.shutdown();
}

#[tokio::test]
async fn trade_cal_sends_mandatory_exchange_param() {
    let mock = single_route("trade_cal", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .trade_cal("SSE", Some("20260101"), Some("20261231"))
        .await
        .expect("trade_cal 应成功");

    let reqs = mock.requests_for("trade_cal");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("exchange"), Some(&json!("SSE")));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261231")));

    mock.shutdown();
}

#[tokio::test]
async fn daily_basic_assembles_params_and_requests_valuation_fields() {
    let mock = single_route("daily_basic", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .daily_basic(
            Some("000001.SZ"),
            Some("20260511"),
            None,
            None,
            Some(1000),
            Some(2000),
        )
        .await
        .expect("daily_basic 应成功");

    let reqs = mock.requests_for("daily_basic");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!("000001.SZ")));
    assert_eq!(reqs[0].params.get("trade_date"), Some(&json!("20260511")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("1000")));
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("2000")));
    // fields 固定 11 个估值字段（手算：ts_code/trade_date/pe_ttm/pb/ps_ttm/dv_ttm/
    // total_share/float_share/free_share/total_mv/circ_mv）
    let fields = reqs[0].fields.as_ref().expect("fields 应为 Some");
    assert_eq!(fields.len(), 11, "fields={:?}", fields);
    assert!(fields.contains(&"pe_ttm".to_string()));
    assert!(fields.contains(&"circ_mv".to_string()));

    mock.shutdown();
}

#[tokio::test]
async fn moneyflow_requests_full_twenty_fields() {
    let mock = single_route("moneyflow", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .moneyflow(Some("000001.SZ"), Some("20260511"), None, None, None, None)
        .await
        .expect("moneyflow 应成功");

    let reqs = mock.requests_for("moneyflow");
    assert_eq!(reqs.len(), 1);
    // 4 组买卖(各 vol+amount)×3 档 + elg 同构 + net_mf_vol/net_mf_amount = 20 字段
    let fields = reqs[0].fields.as_ref().expect("fields 应为 Some");
    assert_eq!(fields.len(), 20, "fields={:?}", fields);
    assert!(fields.contains(&"buy_elg_amount".to_string()));
    assert!(fields.contains(&"net_mf_amount".to_string()));

    mock.shutdown();
}

#[tokio::test]
async fn forecast_maps_forecast_type_to_type_param() {
    let mock = single_route("forecast", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .forecast(
            Some("000001.SZ"),
            Some("20260110"),
            None,
            None,
            Some("20251231"),
            Some("预增"),
            Some(500),
            Some(0),
        )
        .await
        .expect("forecast 应成功");

    let reqs = mock.requests_for("forecast");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ann_date"), Some(&json!("20260110")));
    assert_eq!(reqs[0].params.get("period"), Some(&json!("20251231")));
    // forecast_type 参数名映射为 "type"（Tushare 协议）
    assert_eq!(reqs[0].params.get("type"), Some(&json!("预增")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("500")));
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("0")));

    mock.shutdown();
}

#[tokio::test]
async fn express_requires_ts_code_and_passes_period() {
    let mock = single_route("express", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .express("000001.SZ", None, None, None, Some("20251231"), None, None)
        .await
        .expect("express 应成功");

    let reqs = mock.requests_for("express");
    assert_eq!(reqs.len(), 1);
    // ts_code 是必填参数（签名非 Option）
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!("000001.SZ")));
    assert_eq!(reqs[0].params.get("period"), Some(&json!("20251231")));

    mock.shutdown();
}

#[tokio::test]
async fn disclosure_date_passes_five_optional_filters() {
    let mock = single_route("disclosure_date", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .disclosure_date(
            Some("000001.SZ"),
            Some("20251231"),
            Some("20260320"),
            Some("20260328"),
            Some("20260328"),
            None,
            None,
        )
        .await
        .expect("disclosure_date 应成功");

    let reqs = mock.requests_for("disclosure_date");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!("000001.SZ")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20251231")));
    assert_eq!(reqs[0].params.get("pre_date"), Some(&json!("20260320")));
    assert_eq!(reqs[0].params.get("ann_date"), Some(&json!("20260328")));
    assert_eq!(reqs[0].params.get("actual_date"), Some(&json!("20260328")));

    mock.shutdown();
}

#[tokio::test]
async fn fund_nav_uses_nav_date_param_name() {
    let mock = single_route("fund_nav", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .fund_nav(Some("510300.SH"), Some("20260918"), None, None)
        .await
        .expect("fund_nav 应成功");

    let reqs = mock.requests_for("fund_nav");
    assert_eq!(reqs.len(), 1);
    // 净值日期参数名是 nav_date（非 trade_date）
    assert_eq!(reqs[0].params.get("nav_date"), Some(&json!("20260918")));
    assert!(!reqs[0].params.contains_key("trade_date"));

    mock.shutdown();
}

#[tokio::test]
async fn fund_div_requests_eight_dividend_fields() {
    let mock = single_route("fund_div", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .fund_div(Some("510300.SH"), Some("20260115"))
        .await
        .expect("fund_div 应成功");

    let reqs = mock.requests_for("fund_div");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!("510300.SH")));
    assert_eq!(reqs[0].params.get("ex_date"), Some(&json!("20260115")));
    // 8 个分红字段：ts_code/ann_date/imp_anndate/div_proc/record_date/ex_date/pay_date/div_cash
    let fields = reqs[0].fields.as_ref().expect("fields 应为 Some");
    assert_eq!(fields.len(), 8, "fields={:?}", fields);
    assert!(fields.contains(&"div_cash".to_string()));
    assert!(fields.contains(&"div_proc".to_string()));

    mock.shutdown();
}

#[tokio::test]
async fn index_daily_and_fund_basic_and_adj_factor_param_shapes() {
    // 三个薄封装合测：index_daily（ts_code 必填）/ fund_basic（market + 24 字段）/
    // adj_factor（trade_date + 分页参数）
    let mock = spawn_mock_tushare(vec![
        ("index_daily", MockResponse::EmptyOk),
        ("fund_basic", MockResponse::EmptyOk),
        ("adj_factor", MockResponse::EmptyOk),
    ])
    .await;
    let client = client_for(&mock.base_url);

    client
        .index_daily("000300.SH", Some("20260101"), Some("20260131"))
        .await
        .expect("index_daily 应成功");
    client
        .fund_basic(Some("E"))
        .await
        .expect("fund_basic 应成功");
    client
        .adj_factor(None, Some("20260105"), None, None, Some(6000), Some(0))
        .await
        .expect("adj_factor 应成功");

    let idx = mock.requests_for("index_daily");
    assert_eq!(idx[0].params.get("ts_code"), Some(&json!("000300.SH")));
    assert_eq!(idx[0].params.get("start_date"), Some(&json!("20260101")));

    let fb = mock.requests_for("fund_basic");
    assert_eq!(fb[0].params.get("market"), Some(&json!("E")));
    // fund_basic 固定 25 个字段（ts_code..market，含 market 收尾）
    let fields = fb[0].fields.as_ref().expect("fund_basic fields");
    assert_eq!(fields.len(), 25, "fields={:?}", fields);
    assert!(fields.contains(&"list_date".to_string()));

    let adj = mock.requests_for("adj_factor");
    assert_eq!(adj[0].params.get("trade_date"), Some(&json!("20260105")));
    assert_eq!(adj[0].params.get("limit"), Some(&json!("6000")));
    assert_eq!(adj[0].params.get("offset"), Some(&json!("0")));
    // adj_factor 未传 ts_code → 不应出现（批量路径语义）
    assert!(!adj[0].params.contains_key("ts_code"));

    mock.shutdown();
}

// ─── call_api 错误映射 ───────────────────────────────────────────

#[tokio::test]
async fn api_error_code_maps_to_quant_api_error() {
    // HTTP 200 + code=40101（token 失效）→ Api{code:40101}
    let mock = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "token无效".to_string(),
        },
    )
    .await;
    let client = client_for(&mock.base_url);

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("业务错误码应转为 Err");
    match err {
        QuantError::Api { code, message } => {
            assert_eq!(code, 40101);
            assert_eq!(message, "token无效");
        }
        other => panic!("应为 Api 变体，实际 {:?}", other),
    }
    // 无 fallback 配置 → 只打主端点一次
    assert_eq!(mock.requests().len(), 1);

    mock.shutdown();
}

#[tokio::test]
async fn http_error_status_maps_to_api_error_with_status_code() {
    let mock = single_route(
        "stock_basic",
        MockResponse::HttpErr {
            status: 502,
            body: "bad gateway".to_string(),
        },
    )
    .await;
    let client = client_for(&mock.base_url);

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("HTTP 502 应转 Err");
    match err {
        QuantError::Api { code, message } => {
            assert_eq!(code, 502, "HTTP 状态码进入 Api.code");
            assert!(message.contains("502"), "message={}", message);
        }
        other => panic!("应为 Api 变体，实际 {:?}", other),
    }

    mock.shutdown();
}

#[tokio::test]
async fn undecodable_body_maps_to_decode_error() {
    // HTTP 200 但 body 是合法 JSON 字符串（非 TushareResponse 对象）→ decode: -2
    let mock = single_route(
        "stock_basic",
        MockResponse::HttpErr {
            status: 200,
            body: "not-a-tushare-response".to_string(),
        },
    )
    .await;
    let client = client_for(&mock.base_url);

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("解码失败应转 Err");
    match err {
        QuantError::Api { code, message } => {
            assert_eq!(code, -2, "解码失败固定 code=-2");
            assert!(message.contains("decode"), "message={}", message);
        }
        other => panic!("应为 Api 变体，实际 {:?}", other),
    }

    mock.shutdown();
}

#[tokio::test]
async fn slow_response_times_out_and_maps_to_send_error() {
    // timeout_secs=2 < mock 延迟 5000ms → send 超时 → Api{code:-1}
    let mock = single_route("stock_basic", MockResponse::SlowOk { delay_ms: 5000 }).await;
    let mut cfg = mock_config(&mock.base_url);
    cfg.timeout_secs = 2;
    let client = TushareClient::new(cfg).expect("client");

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("慢响应应超时");
    match err {
        QuantError::Api { code, message } => {
            assert_eq!(code, -1, "网络层错误固定 code=-1");
            assert!(message.contains("send"), "message={}", message);
        }
        other => panic!("应为 Api 变体，实际 {:?}", other),
    }

    mock.shutdown();
}

// ─── 主备凭证配对切换（2026-09-15：URL+token 成对切换）────────────

#[tokio::test]
async fn primary_failure_switches_url_and_token_as_a_pair() {
    // 主 mock 返回 40101，备 mock（第二个端口）返回成功——
    // 凭证配对切换的核心场景：fallback_base_url + fallback_token 成对生效
    let primary = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )
    .await;
    let fallback = single_route(
        "stock_basic",
        MockResponse::Rows {
            fields: vec!["ts_code"],
            items: vec![vec![json!("000001.SZ")]],
        },
    )
    .await;

    let mut cfg = mock_config(&primary.base_url);
    cfg.fallback_base_url = Some(fallback.base_url.clone());
    cfg.fallback_token = Some("test-alt-token".to_string());
    let client = TushareClient::new(cfg).expect("client");

    let resp = client
        .stock_basic(Some("SSE"), Some("L"))
        .await
        .expect("备用数据源应接管成功");
    assert_eq!(resp.code, 0);
    let data = resp.data.expect("data");
    assert_eq!(data.to_maps()[0].get("ts_code"), Some(&json!("000001.SZ")));

    // 主端点恰好收到 1 请求（主 token），备端点恰好 1 请求（备 token）——
    // token 与 URL 严格配对，不存在换 token 不换 URL 的错配请求
    let primary_reqs = primary.requests();
    assert_eq!(primary_reqs.len(), 1, "主端点应只收到 1 次请求");
    assert_eq!(primary_reqs[0].token, "test-primary-token");
    let fallback_reqs = fallback.requests();
    assert_eq!(fallback_reqs.len(), 1, "备端点应接管 1 次请求");
    assert_eq!(fallback_reqs[0].token, "test-alt-token");
    assert_eq!(
        fallback_reqs[0].api_name, "stock_basic",
        "api 与参数应原样重放"
    );
    assert_eq!(fallback_reqs[0].params.get("exchange"), Some(&json!("SSE")));

    primary.shutdown();
    fallback.shutdown();
}

#[tokio::test]
async fn fallback_not_configured_keeps_primary_error() {
    // 无 fallback_token：主失败直接透传错误（对照分支）
    let mock = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "token无效".to_string(),
        },
    )
    .await;
    let client = client_for(&mock.base_url);

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("无备用应直接失败");
    assert!(matches!(err, QuantError::Api { code: 40101, .. }));
    assert_eq!(mock.requests().len(), 1, "只应有 1 次主端点请求");

    mock.shutdown();
}

#[tokio::test]
async fn fallback_token_equal_to_primary_skips_switch() {
    // fallback_token == 主 token：视为未配置，不做无意义重试
    let mock = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "token无效".to_string(),
        },
    )
    .await;
    let mut cfg = mock_config(&mock.base_url);
    cfg.fallback_token = Some("test-primary-token".to_string()); // 与主 token 相同
    let client = TushareClient::new(cfg).expect("client");

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("同 token 不应切换");
    assert!(matches!(err, QuantError::Api { code: 40101, .. }));
    assert_eq!(mock.requests().len(), 1, "同 token 应只打 1 次");

    mock.shutdown();
}

#[tokio::test]
async fn fallback_without_url_retries_primary_url_with_alt_token() {
    // fallback_base_url 未配置 → 备请求回落主 URL，但 token 换成备用。
    // mock 恒定返回 40203：主备两次都失败，但请求数与 token 序列
    // 恰好证明"回落主 URL + 换备 token"的切换行为发生了。
    let mock = spawn_mock_tushare(vec![(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40203,
            msg: "每分钟最多访问该接口4000次".to_string(),
        },
    )])
    .await;
    let mut cfg = mock_config(&mock.base_url);
    cfg.fallback_token = Some("test-alt-token".to_string());
    let client = TushareClient::new(cfg).expect("client");

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("两次都失败应透传");
    assert!(matches!(err, QuantError::Api { code: 40203, .. }));
    let reqs = mock.requests();
    assert_eq!(reqs.len(), 2, "主备各 1 次，共 2 次请求打到同一 URL");
    assert_eq!(reqs[0].token, "test-primary-token", "第 1 次用主 token");
    assert_eq!(
        reqs[1].token, "test-alt-token",
        "第 2 次回落主 URL 但换备 token"
    );

    mock.shutdown();
}

#[tokio::test]
async fn fallback_failure_returns_secondary_error() {
    // 主备都失败 → 返回"备用"的错误（call_api 返回 fallback 分支结果）
    let primary = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "主端点 token 失效".to_string(),
        },
    )
    .await;
    let fallback = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40203,
            msg: "备用端点限流".to_string(),
        },
    )
    .await;
    let mut cfg = mock_config(&primary.base_url);
    cfg.fallback_base_url = Some(fallback.base_url.clone());
    cfg.fallback_token = Some("test-alt-token".to_string());
    let client = TushareClient::new(cfg).expect("client");

    let err = client
        .stock_basic(None, None)
        .await
        .expect_err("主备均败应失败");
    match err {
        QuantError::Api { code, message } => {
            // 透传的是备用端点的错误（最后尝试者胜）
            assert_eq!(code, 40203);
            assert_eq!(message, "备用端点限流");
        }
        other => panic!("应为 Api 变体，实际 {:?}", other),
    }

    primary.shutdown();
    fallback.shutdown();
}

#[tokio::test]
async fn primary_success_never_touches_fallback() {
    // 主端点成功 → 备用配置完全不动用（短路分支）。
    // 备端点指向"若被调用必失败"的哨兵 mock，以零请求数证明短路。
    let primary = single_route("stock_basic", MockResponse::EmptyOk).await;
    let sentinel = single_route(
        "stock_basic",
        MockResponse::ApiErr {
            code: -999,
            msg: "sentinel".to_string(),
        },
    )
    .await;
    let mut cfg = mock_config(&primary.base_url);
    cfg.fallback_base_url = Some(sentinel.base_url.clone());
    cfg.fallback_token = Some("test-alt-token".to_string());
    let client = TushareClient::new(cfg).expect("client");

    client
        .stock_basic(None, None)
        .await
        .expect("主端点成功即返回");

    assert_eq!(primary.requests().len(), 1);
    assert_eq!(sentinel.requests().len(), 0, "主成功时备用端点零请求");

    primary.shutdown();
    sentinel.shutdown();
}

// ═══════════════════════════════════════════════════════════════
// 剩余 28 个薄封装接口批产（任务二）。
//
// 全部与已测方法共用同一 call_api 模板，此处统一断言三件事：
// 1. 恰好发出一次指定 api 的调用；
// 2. 参数组装形态（必传参数值 / None 参数不出现）；
// 3. fields 白名单数量（固定 fields 的接口手算条数）。
// 响应解码路径由共享的 call_api/serde 链路覆盖（EmptyOk 即走完整解码）。
// ═══════════════════════════════════════════════════════════════

/// 薄封装统一断言：恰好一次调用 + 参数形态 + fields 形态
///
/// `fields_len`: Some(n) 断言固定 fields 白名单恰 n 个；
/// None 断言空 fields 序列化为 Some([])（非 null）。
fn assert_thin_call(
    mock: &MockTushare,
    api: &str,
    expect_params: &[(&str, Value)],
    absent_params: &[&str],
    fields_len: Option<usize>,
) {
    let reqs = mock.requests_for(api);
    assert_eq!(reqs.len(), 1, "{} 应恰好调用一次", api);
    for (key, value) in expect_params {
        assert_eq!(
            reqs[0].params.get(*key),
            Some(value),
            "{} 的 {} 参数：实际 {:?}",
            api,
            key,
            reqs[0].params
        );
    }
    for key in absent_params {
        assert!(
            !reqs[0].params.contains_key(*key),
            "{} 不应出现 {} 参数（None 跳过），实际 {:?}",
            api,
            key,
            reqs[0].params
        );
    }
    match fields_len {
        Some(n) => {
            let fields = reqs[0]
                .fields
                .as_ref()
                .unwrap_or_else(|| panic!("{} 的 fields 应为 Some", api));
            assert_eq!(fields.len(), n, "{} fields={:?}", api, fields);
        }
        None => assert!(
            matches!(&reqs[0].fields, Some(f) if f.is_empty()),
            "{} 空 fields 应序列化为 Some([])，实际 {:?}",
            api,
            reqs[0].fields
        ),
    }
}

/// 期货三接口 + 质押两接口（5 个薄封装）
#[tokio::test]
async fn futures_and_pledge_thin_wrappers_assemble_params_and_field_whitelists() {
    let mock = spawn_mock_tushare(vec![
        ("fut_daily", MockResponse::EmptyOk),
        ("fut_wsr", MockResponse::EmptyOk),
        ("fut_holding", MockResponse::EmptyOk),
        ("pledge_stat", MockResponse::EmptyOk),
        ("pledge_detail", MockResponse::EmptyOk),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // fut_daily：ts_code + trade_date + exchange + 分页；fields 16
    // （pre_close..delv_settle 全量行情字段）
    client
        .fut_daily(
            Some("ZZZ2609.SHFE"),
            Some("20260803"),
            Some("SHFE"),
            None,
            None,
            Some(5000),
            Some(0),
        )
        .await
        .expect("fut_daily 应成功");
    assert_thin_call(
        &mock,
        "fut_daily",
        &[
            ("ts_code", json!("ZZZ2609.SHFE")),
            ("trade_date", json!("20260803")),
            ("exchange", json!("SHFE")),
            ("limit", json!("5000")),
            ("offset", json!("0")),
        ],
        &["start_date", "end_date"],
        Some(16),
    );

    // fut_wsr：trade_date + symbol（品种码非 ts_code）+ exchange + 分页；fields 17
    client
        .fut_wsr(
            Some("20260803"),
            Some("ZZZFUT"),
            None,
            None,
            Some("SHFE"),
            Some(5000),
            Some(10000),
        )
        .await
        .expect("fut_wsr 应成功");
    assert_thin_call(
        &mock,
        "fut_wsr",
        &[
            ("trade_date", json!("20260803")),
            ("symbol", json!("ZZZFUT")),
            ("exchange", json!("SHFE")),
            ("offset", json!("10000")),
        ],
        &["start_date", "end_date"],
        Some(17),
    );

    // fut_holding：仅日期与品种（无分页）；fields 10（含 long_hld/short_hld）
    client
        .fut_holding(
            Some("20260803"),
            Some("ZZZFUT"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("fut_holding 应成功");
    assert_thin_call(
        &mock,
        "fut_holding",
        &[
            ("trade_date", json!("20260803")),
            ("symbol", json!("ZZZFUT")),
        ],
        &["limit", "offset", "exchange"],
        Some(10),
    );

    // pledge_stat：ts_code + end_date（统计截止日）+ 分页；fields 7（无 ann_date）
    client
        .pledge_stat(Some("000001.SZ"), Some("20251231"), Some(5000), Some(0))
        .await
        .expect("pledge_stat 应成功");
    assert_thin_call(
        &mock,
        "pledge_stat",
        &[
            ("ts_code", json!("000001.SZ")),
            ("end_date", json!("20251231")),
            ("limit", json!("5000")),
        ],
        &[],
        Some(7),
    );

    // pledge_detail：ts_code + ann_date（PIT 可得日）+ 窗口 + limit；fields 14
    client
        .pledge_detail(
            Some("000001.SZ"),
            Some("20260110"),
            Some("20250101"),
            Some("20251231"),
            Some(5000),
            None,
        )
        .await
        .expect("pledge_detail 应成功");
    assert_thin_call(
        &mock,
        "pledge_detail",
        &[
            ("ts_code", json!("000001.SZ")),
            ("ann_date", json!("20260110")),
            ("start_date", json!("20250101")),
            ("end_date", json!("20251231")),
        ],
        &["offset"],
        Some(14),
    );

    mock.shutdown();
}

/// 股东结构四接口 + 事件类七接口（11 个薄封装）
#[tokio::test]
async fn shareholder_and_event_thin_wrappers_assemble_params() {
    let mock = spawn_mock_tushare(vec![
        ("stk_holdernumber", MockResponse::EmptyOk),
        ("top10_holders", MockResponse::EmptyOk),
        ("top10_floatholders", MockResponse::EmptyOk),
        ("stk_holdertrade", MockResponse::EmptyOk),
        ("block_trade", MockResponse::EmptyOk),
        ("moneyflow_hsgt", MockResponse::EmptyOk),
        ("margin", MockResponse::EmptyOk),
        ("margin_detail", MockResponse::EmptyOk),
        ("namechange", MockResponse::EmptyOk),
        ("suspend_d", MockResponse::EmptyOk),
        ("limit_list_d", MockResponse::EmptyOk),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // stk_holdernumber：ts_code + ann/end 窗口 + 分页；fields 4
    client
        .stk_holdernumber(
            Some("000001.SZ"),
            None,
            Some("20250101"),
            Some("20251231"),
            Some(5000),
            Some(0),
        )
        .await
        .expect("stk_holdernumber 应成功");
    assert_thin_call(
        &mock,
        "stk_holdernumber",
        &[
            ("ts_code", json!("000001.SZ")),
            ("start_date", json!("20250101")),
            ("end_date", json!("20251231")),
            ("limit", json!("5000")),
        ],
        &["ann_date"],
        Some(4),
    );

    // top10_holders：ts_code + ann_date；fields 9（含 hold_change）
    client
        .top10_holders(Some("000001.SZ"), Some("20260110"), None, None, None, None)
        .await
        .expect("top10_holders 应成功");
    assert_thin_call(
        &mock,
        "top10_holders",
        &[
            ("ts_code", json!("000001.SZ")),
            ("ann_date", json!("20260110")),
        ],
        &["start_date", "end_date", "limit"],
        Some(9),
    );

    // top10_floatholders：ts_code + start/end 窗口 + 分页；fields 9
    client
        .top10_floatholders(
            Some("000001.SZ"),
            None,
            Some("20250101"),
            Some("20251231"),
            Some(5000),
            Some(0),
        )
        .await
        .expect("top10_floatholders 应成功");
    assert_thin_call(
        &mock,
        "top10_floatholders",
        &[
            ("ts_code", json!("000001.SZ")),
            ("start_date", json!("20250101")),
            ("end_date", json!("20251231")),
            ("offset", json!("0")),
        ],
        &["ann_date"],
        Some(9),
    );

    // stk_holdertrade：仅 ann_date（全局按公告日）；fields 13
    client
        .stk_holdertrade(None, Some("20260110"), None, None, None, None)
        .await
        .expect("stk_holdertrade 应成功");
    assert_thin_call(
        &mock,
        "stk_holdertrade",
        &[("ann_date", json!("20260110"))],
        &["ts_code", "start_date", "limit"],
        Some(13),
    );

    // block_trade：ts_code + trade_date；fields 7（price/vol/amount/buyer/seller）
    client
        .block_trade(Some("000001.SZ"), Some("20260105"), None, None)
        .await
        .expect("block_trade 应成功");
    assert_thin_call(
        &mock,
        "block_trade",
        &[
            ("ts_code", json!("000001.SZ")),
            ("trade_date", json!("20260105")),
        ],
        &["start_date", "end_date"],
        Some(7),
    );

    // moneyflow_hsgt：start/end 成对（无 ts_code 维度）；空 fields
    client
        .moneyflow_hsgt(None, Some("20260101"), Some("20260131"))
        .await
        .expect("moneyflow_hsgt 应成功");
    assert_thin_call(
        &mock,
        "moneyflow_hsgt",
        &[
            ("start_date", json!("20260101")),
            ("end_date", json!("20260131")),
        ],
        &["trade_date"],
        None,
    );

    // margin：单日 trade_date；空 fields
    client
        .margin(Some("20260105"), None, None)
        .await
        .expect("margin 应成功");
    assert_thin_call(
        &mock,
        "margin",
        &[("trade_date", json!("20260105"))],
        &["start_date"],
        None,
    );

    // margin_detail：ts_code + limit/offset；fields 11（证券级两融字段：
    // trade_date/ts_code/name/rzye/rqye/rzmre/rqyl/rzche/rqchl/rqmcl/rzrqye）
    client
        .margin_detail(Some("000001.SZ"), None, None, None, Some(6000), Some(0))
        .await
        .expect("margin_detail 应成功");
    assert_thin_call(
        &mock,
        "margin_detail",
        &[
            ("ts_code", json!("000001.SZ")),
            ("limit", json!("6000")),
            ("offset", json!("0")),
        ],
        &["trade_date"],
        Some(11),
    );

    // namechange：ts_code + end_date；空 fields
    client
        .namechange(Some("000001.SZ"), None, Some("20261231"))
        .await
        .expect("namechange 应成功");
    assert_thin_call(
        &mock,
        "namechange",
        &[
            ("ts_code", json!("000001.SZ")),
            ("end_date", json!("20261231")),
        ],
        &["start_date"],
        None,
    );

    // suspend_d：trade_date（单日停牌列表）；空 fields
    client
        .suspend_d(Some("20260105"), None, None, None)
        .await
        .expect("suspend_d 应成功");
    assert_thin_call(
        &mock,
        "suspend_d",
        &[("trade_date", json!("20260105"))],
        &["ts_code", "start_date"],
        None,
    );

    // limit_list_d：start/end 区间（无单日参数）；空 fields
    client
        .limit_list_d(None, None, Some("20260101"), Some("20260131"))
        .await
        .expect("limit_list_d 应成功");
    assert_thin_call(
        &mock,
        "limit_list_d",
        &[
            ("start_date", json!("20260101")),
            ("end_date", json!("20260131")),
        ],
        &["trade_date", "ts_code"],
        None,
    );

    mock.shutdown();
}

/// 财务与行业类 12 个薄封装
#[tokio::test]
async fn financial_and_industry_thin_wrappers_assemble_params() {
    let mock = spawn_mock_tushare(vec![
        ("income", MockResponse::EmptyOk),
        ("balancesheet", MockResponse::EmptyOk),
        ("fina_indicator", MockResponse::EmptyOk),
        ("fina_mainbz", MockResponse::EmptyOk),
        ("fina_mainbz_vip", MockResponse::EmptyOk),
        ("report_rc", MockResponse::EmptyOk),
        ("cashflow", MockResponse::EmptyOk),
        ("dividend", MockResponse::EmptyOk),
        ("repurchase", MockResponse::EmptyOk),
        ("share_float", MockResponse::EmptyOk),
        ("index_classify", MockResponse::EmptyOk),
        ("index_member_all", MockResponse::EmptyOk),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // income：ts_code 必填 + start_date；空 fields
    client
        .income("000001.SZ", Some("20250101"), None)
        .await
        .expect("income 应成功");
    assert_thin_call(
        &mock,
        "income",
        &[
            ("ts_code", json!("000001.SZ")),
            ("start_date", json!("20250101")),
        ],
        &["end_date"],
        None,
    );

    // balancesheet：ts_code + end_date；空 fields
    client
        .balancesheet("000001.SZ", None, Some("20251231"))
        .await
        .expect("balancesheet 应成功");
    assert_thin_call(
        &mock,
        "balancesheet",
        &[
            ("ts_code", json!("000001.SZ")),
            ("end_date", json!("20251231")),
        ],
        &["start_date"],
        None,
    );

    // fina_indicator：仅 ts_code；空 fields
    client
        .fina_indicator("000001.SZ", None, None)
        .await
        .expect("fina_indicator 应成功");
    assert_thin_call(
        &mock,
        "fina_indicator",
        &[("ts_code", json!("000001.SZ"))],
        &["start_date", "end_date"],
        None,
    );

    // fina_mainbz：ts_code + period + type（无公告日字段，PIT 需外部补）；fields 9
    client
        .fina_mainbz("000001.SZ", Some("20251231"), Some("P"), None, None)
        .await
        .expect("fina_mainbz 应成功");
    assert_thin_call(
        &mock,
        "fina_mainbz",
        &[
            ("ts_code", json!("000001.SZ")),
            ("period", json!("20251231")),
            ("type", json!("P")),
        ],
        &[],
        Some(9),
    );

    // fina_mainbz_vip：period + 分页（无 ts_code，按报告期全市场）；fields 9
    client
        .fina_mainbz_vip("20251231", None, Some(5000), Some(0))
        .await
        .expect("fina_mainbz_vip 应成功");
    assert_thin_call(
        &mock,
        "fina_mainbz_vip",
        &[
            ("period", json!("20251231")),
            ("limit", json!("5000")),
            ("offset", json!("0")),
        ],
        &["ts_code", "type"],
        Some(9),
    );

    // report_rc：report_date + limit（卖方报告）；fields 23
    client
        .report_rc(None, Some("20260110"), None, None, Some(300), None)
        .await
        .expect("report_rc 应成功");
    assert_thin_call(
        &mock,
        "report_rc",
        &[("report_date", json!("20260110")), ("limit", json!("300"))],
        &["ts_code", "offset"],
        Some(23),
    );

    // cashflow：ts_code + 窗口 + 分页；fields 7（含 f_ann_date PIT 字段）
    client
        .cashflow(
            "000001.SZ",
            Some("20250101"),
            Some("20251231"),
            Some(2000),
            Some(0),
        )
        .await
        .expect("cashflow 应成功");
    assert_thin_call(
        &mock,
        "cashflow",
        &[
            ("ts_code", json!("000001.SZ")),
            ("start_date", json!("20250101")),
            ("end_date", json!("20251231")),
            ("limit", json!("2000")),
        ],
        &[],
        Some(7),
    );

    // dividend：ts_code + ann_date + ex_date（ex_date 非空时必传）；fields 14
    // （含 2026-09-17 补的送转三字段）
    client
        .dividend(
            "000001.SZ",
            Some("20260110"),
            None,
            Some("20260121"),
            None,
            None,
            None,
        )
        .await
        .expect("dividend 应成功");
    assert_thin_call(
        &mock,
        "dividend",
        &[
            ("ts_code", json!("000001.SZ")),
            ("ann_date", json!("20260110")),
            ("ex_date", json!("20260121")),
        ],
        &["record_date", "imp_ann_date", "limit"],
        Some(14),
    );

    // repurchase：ann_date + 分页（官方参数不含 ts_code）；fields 9
    client
        .repurchase(Some("20260110"), None, None, Some(2000), Some(0))
        .await
        .expect("repurchase 应成功");
    assert_thin_call(
        &mock,
        "repurchase",
        &[
            ("ann_date", json!("20260110")),
            ("limit", json!("2000")),
            ("offset", json!("0")),
        ],
        &["ts_code"],
        Some(9),
    );

    // share_float：ts_code + 解禁日窗口（无分页）；fields 7
    client
        .share_float(
            Some("000001.SZ"),
            None,
            Some("20260101"),
            Some("20260131"),
            None,
            None,
        )
        .await
        .expect("share_float 应成功");
    assert_thin_call(
        &mock,
        "share_float",
        &[
            ("ts_code", json!("000001.SZ")),
            ("start_date", json!("20260101")),
            ("end_date", json!("20260131")),
        ],
        &["ann_date", "limit", "offset"],
        Some(7),
    );

    // index_classify：level + src（申万分类元数据）；fields 7
    client
        .index_classify(None, Some("L1"), None, Some("SW2021"))
        .await
        .expect("index_classify 应成功");
    assert_thin_call(
        &mock,
        "index_classify",
        &[("level", json!("L1")), ("src", json!("SW2021"))],
        &["index_code", "parent_code"],
        Some(7),
    );

    // index_member：801 前缀码映射 l1_code（fields 8 含 in_date/out_date/is_new）
    client
        .index_member(Some("801010.SI"), None, None, None, None)
        .await
        .expect("index_member 应成功");
    assert_thin_call(
        &mock,
        "index_member_all",
        &[("l1_code", json!("801010.SI"))],
        &["l2_code", "l3_code", "ts_code", "limit", "offset"],
        Some(8),
    );

    mock.shutdown();
}

/// index_member 的申万码前缀分发：801→l1 / 85→l3 / 其他→l2
#[tokio::test]
async fn index_member_dispatches_level_param_by_code_prefix() {
    let mock = single_route("index_member_all", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    // 801 前缀（L1 码）→ l1_code
    client
        .index_member(Some("801010.SI"), None, None, None, None)
        .await
        .expect("801 前缀应成功");
    // 85 前缀（L3 码）→ l3_code
    client
        .index_member(Some("851010.SI"), None, None, None, None)
        .await
        .expect("85 前缀应成功");
    // 其他码段（非 801/85 开头）→ l2_code
    client
        .index_member(Some("700100.SI"), None, None, None, None)
        .await
        .expect("L2 码段应成功");

    let reqs = mock.requests_for("index_member_all");
    assert_eq!(reqs.len(), 3);
    assert_eq!(reqs[0].params.get("l1_code"), Some(&json!("801010.SI")));
    assert_eq!(reqs[1].params.get("l3_code"), Some(&json!("851010.SI")));
    assert_eq!(reqs[2].params.get("l2_code"), Some(&json!("700100.SI")));
    // 每次请求只带一个级别参数
    for req in &reqs {
        assert_eq!(
            req.params.len(),
            1,
            "应只带一个级别参数，实际 {:?}",
            req.params
        );
    }

    mock.shutdown();
}

// ═══════════════════════════════════════════════════════════════
// 第五批：收尾冲刺——fund_adj 直测 + 分页 offset-without-limit 分支。
//
// fund_adj 是唯一无 client 直测的薄封装（sync_tests 仅间接覆盖成功路径）；
// daily_basic/moneyflow/forecast 的分页组装在 offset 有、limit 无时走
// owned[0] 索引分支（idx=0），既有测试均传 limit+offset 成对，该分支未走。
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn fund_adj_passes_code_and_window_params() {
    let mock = single_route("fund_adj", MockResponse::EmptyOk).await;
    let client = client_for(&mock.base_url);

    client
        .fund_adj("ZZZSYNC60.SH", Some("20250101"), Some("20251231"))
        .await
        .expect("fund_adj 应成功");

    // ts_code 必填 + start/end 窗口；空 fields 序列化为 Some([])（非 null）
    assert_thin_call(
        &mock,
        "fund_adj",
        &[
            ("ts_code", json!("ZZZSYNC60.SH")),
            ("start_date", json!("20250101")),
            ("end_date", json!("20251231")),
        ],
        &[],
        None,
    );

    mock.shutdown();
}

#[tokio::test]
async fn pagination_offset_without_limit_takes_index_zero_branch() {
    let mock = spawn_mock_tushare(vec![
        ("daily_basic", MockResponse::EmptyOk),
        ("moneyflow", MockResponse::EmptyOk),
        ("forecast", MockResponse::EmptyOk),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // 三接口均只传 offset 不传 limit：owned 向量只有 offset 一项，
    // 组装走 idx=0 分支——offset 正确透传且不误发 limit 参数
    client
        .daily_basic(None, Some("20260511"), None, None, None, Some(700))
        .await
        .expect("daily_basic 仅 offset 应成功");
    assert_thin_call(
        &mock,
        "daily_basic",
        &[("trade_date", json!("20260511")), ("offset", json!("700"))],
        &["ts_code", "limit"],
        Some(11),
    );

    client
        .moneyflow(None, Some("20260511"), None, None, None, Some(900))
        .await
        .expect("moneyflow 仅 offset 应成功");
    assert_thin_call(
        &mock,
        "moneyflow",
        &[("trade_date", json!("20260511")), ("offset", json!("900"))],
        &["ts_code", "limit"],
        Some(20),
    );

    client
        .forecast(None, None, None, None, None, None, None, Some(300))
        .await
        .expect("forecast 仅 offset 应成功");
    assert_thin_call(
        &mock,
        "forecast",
        &[("offset", json!("300"))],
        &["ts_code", "limit"],
        Some(11),
    );

    mock.shutdown();
}
