//! client.rs 的 mock server 测试（Application 层覆盖率专项）。
//!
//! 覆盖：构造分支 / 参数组装 / 响应解码 / call_api 错误映射（业务码、
//! HTTP 状态、解码失败、超时）/ 主备凭证配对切换（2026-09-15 行为）。
//! 其余 30+ 薄封装接口与已测方法共用同一 call_api 模板，见报告跳过清单。

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
