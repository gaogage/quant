//! Tushare 数据源 HTTP 客户端
//!
//! 从 valentina 迁移，增加 governor rate limiter 和复权因子 API。

use governor::{
    clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota,
    RateLimiter as GovernorRateLimiter,
};
use quant_common::{QuantError, QuantResult};
use reqwest::Client as HttpClient;
use serde::de::DeserializeOwned;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

use super::super::model::tushare_dto::{TushareRequest, TushareResponse};

/// Tushare 客户端配置
#[derive(Debug, Clone)]
pub struct TushareConfig {
    pub base_url: String,
    pub token: String,
    pub timeout_secs: u64,
    pub max_retries: usize,
    pub retry_delay_ms: u64,
    /// API 速率限制：每分钟最大调用数（Tushare 免费版默认 ~200）
    pub rate_limit_per_minute: u32,
}

impl Default for TushareConfig {
    fn default() -> Self {
        let rate_limit_per_minute = std::env::var("TUSHARE_RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(60);
        Self {
            base_url: std::env::var("TUSHARE_API_URL")
                .unwrap_or_else(|_| "http://api.tushare.pro".to_string()),
            token: std::env::var("TUSHARE_TOKEN").unwrap_or_default(),
            timeout_secs: 30,
            max_retries: 3,
            retry_delay_ms: 1000,
            rate_limit_per_minute,
        }
    }
}

const INDEX_CLASSIFY_FIELDS: &[&str] = &[
    "index_code",
    "industry_name",
    "parent_code",
    "level",
    "industry_code",
    "is_pub",
    "src",
];
const INDEX_MEMBER_FIELDS: &[&str] = &[
    "index_code",
    "index_name",
    "con_code",
    "con_name",
    "in_date",
    "out_date",
    "is_new",
];
const FINA_MAINBZ_FIELDS: &[&str] = &[
    "ts_code",
    "end_date",
    "bz_item",
    "bz_code",
    "bz_sales",
    "bz_profit",
    "bz_cost",
    "curr_type",
    "update_flag",
];
const FINA_MAINBZ_VIP_FIELDS: &[&str] = FINA_MAINBZ_FIELDS;
const REPORT_RC_FIELDS: &[&str] = &[
    "ts_code",
    "name",
    "report_date",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
    "op_rt",
    "op_pr",
    "tp",
    "np",
    "eps",
    "pe",
    "rd",
    "roe",
    "ev_ebitda",
    "rating",
    "max_price",
    "min_price",
    "imp_dg",
    "create_time",
];
const FUT_DAILY_FIELDS: &[&str] = &[
    "ts_code",
    "trade_date",
    "pre_close",
    "pre_settle",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "change1",
    "change2",
    "vol",
    "amount",
    "oi",
    "oi_chg",
    "delv_settle",
];
const FUT_WSR_FIELDS: &[&str] = &[
    "trade_date",
    "symbol",
    "fut_name",
    "warehouse",
    "wh_id",
    "pre_vol",
    "vol",
    "vol_chg",
    "area",
    "year",
    "grade",
    "brand",
    "place",
    "pd",
    "is_ct",
    "unit",
    "exchange",
];
const FUT_HOLDING_FIELDS: &[&str] = &[
    "trade_date",
    "symbol",
    "broker",
    "vol",
    "vol_chg",
    "long_hld",
    "long_chg",
    "short_hld",
    "short_chg",
    "exchange",
];
const PLEDGE_STAT_FIELDS: &[&str] = &[
    "ts_code",
    "end_date",
    "pledge_count",
    "unrest_pledge",
    "rest_pledge",
    "total_share",
    "pledge_ratio",
];
const PLEDGE_DETAIL_FIELDS: &[&str] = &[
    "ts_code",
    "ann_date",
    "holder_name",
    "pledge_amount",
    "start_date",
    "end_date",
    "is_release",
    "release_date",
    "pledgor",
    "holding_amount",
    "pledged_amount",
    "p_total_ratio",
    "h_total_ratio",
    "is_buyback",
];
const STK_HOLDER_NUMBER_FIELDS: &[&str] = &["ts_code", "ann_date", "end_date", "holder_num"];
const TOP10_HOLDERS_FIELDS: &[&str] = &[
    "ts_code",
    "ann_date",
    "end_date",
    "holder_name",
    "hold_amount",
    "hold_ratio",
    "hold_float_ratio",
    "hold_change",
    "holder_type",
];
const TOP10_FLOAT_HOLDERS_FIELDS: &[&str] = &[
    "ts_code",
    "ann_date",
    "end_date",
    "holder_name",
    "hold_amount",
    "hold_ratio",
    "hold_float_ratio",
    "holder_type",
    "hold_change",
];
const STK_HOLDER_TRADE_FIELDS: &[&str] = &[
    "ts_code",
    "ann_date",
    "holder_name",
    "holder_type",
    "in_de",
    "change_vol",
    "change_ratio",
    "after_share",
    "after_ratio",
    "avg_price",
    "total_share",
    "begin_date",
    "close_date",
];
const MARGIN_DETAIL_FIELDS: &[&str] = &[
    "trade_date",
    "ts_code",
    "name",
    "rzye",
    "rqye",
    "rzmre",
    "rqyl",
    "rzche",
    "rqchl",
    "rqmcl",
    "rzrqye",
];
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn industry_membership_specs_keep_pit_audit_fields() {
        assert_eq!(
            INDEX_CLASSIFY_FIELDS,
            &[
                "index_code",
                "industry_name",
                "parent_code",
                "level",
                "industry_code",
                "is_pub",
                "src",
            ]
        );
        assert!(INDEX_MEMBER_FIELDS.contains(&"in_date"));
        assert!(INDEX_MEMBER_FIELDS.contains(&"out_date"));
        assert!(INDEX_MEMBER_FIELDS.contains(&"is_new"));
    }

    #[test]
    fn main_business_specs_require_available_at_join() {
        assert_eq!(
            FINA_MAINBZ_FIELDS,
            &[
                "ts_code",
                "end_date",
                "bz_item",
                "bz_code",
                "bz_sales",
                "bz_profit",
                "bz_cost",
                "curr_type",
                "update_flag",
            ]
        );
        assert!(!FINA_MAINBZ_FIELDS.contains(&"ann_date"));
    }

    #[test]
    fn main_business_vip_specs_support_period_paging_without_native_available_at() {
        assert_eq!(FINA_MAINBZ_VIP_FIELDS, FINA_MAINBZ_FIELDS);
        assert!(!FINA_MAINBZ_VIP_FIELDS.contains(&"ann_date"));
        assert!(!FINA_MAINBZ_VIP_FIELDS.contains(&"f_ann_date"));
    }

    #[test]
    fn report_rc_specs_include_pit_and_revision_fields() {
        assert!(REPORT_RC_FIELDS.contains(&"report_date"));
        assert!(REPORT_RC_FIELDS.contains(&"quarter"));
        assert!(REPORT_RC_FIELDS.contains(&"eps"));
        assert!(REPORT_RC_FIELDS.contains(&"rating"));
        assert!(REPORT_RC_FIELDS.contains(&"max_price"));
        assert!(REPORT_RC_FIELDS.contains(&"min_price"));
    }

    #[test]
    fn futures_price_chain_specs_keep_daily_pit_and_supply_demand_fields() {
        assert!(FUT_DAILY_FIELDS.contains(&"trade_date"));
        assert!(FUT_DAILY_FIELDS.contains(&"close"));
        assert!(FUT_DAILY_FIELDS.contains(&"settle"));
        assert!(FUT_DAILY_FIELDS.contains(&"oi"));
        assert!(FUT_WSR_FIELDS.contains(&"trade_date"));
        assert!(FUT_WSR_FIELDS.contains(&"vol_chg"));
        assert!(FUT_HOLDING_FIELDS.contains(&"long_hld"));
        assert!(FUT_HOLDING_FIELDS.contains(&"short_hld"));
    }

    #[test]
    fn pledge_pressure_specs_separate_detail_available_at_from_snapshot_date() {
        assert!(PLEDGE_DETAIL_FIELDS.contains(&"ann_date"));
        assert!(PLEDGE_DETAIL_FIELDS.contains(&"pledge_amount"));
        assert!(PLEDGE_DETAIL_FIELDS.contains(&"release_date"));
        assert!(PLEDGE_STAT_FIELDS.contains(&"end_date"));
        assert!(!PLEDGE_STAT_FIELDS.contains(&"ann_date"));
    }

    #[test]
    fn shareholder_structure_specs_keep_native_announcement_dates() {
        assert!(STK_HOLDER_NUMBER_FIELDS.contains(&"ann_date"));
        assert!(STK_HOLDER_NUMBER_FIELDS.contains(&"holder_num"));
        assert!(TOP10_HOLDERS_FIELDS.contains(&"ann_date"));
        assert!(TOP10_HOLDERS_FIELDS.contains(&"hold_change"));
        assert!(TOP10_FLOAT_HOLDERS_FIELDS.contains(&"ann_date"));
        assert!(TOP10_FLOAT_HOLDERS_FIELDS.contains(&"hold_float_ratio"));
        assert!(STK_HOLDER_TRADE_FIELDS.contains(&"ann_date"));
        assert!(STK_HOLDER_TRADE_FIELDS.contains(&"in_de"));
    }

    #[test]
    fn margin_detail_specs_keep_daily_security_level_leverage_fields() {
        assert!(MARGIN_DETAIL_FIELDS.contains(&"trade_date"));
        assert!(MARGIN_DETAIL_FIELDS.contains(&"ts_code"));
        assert!(MARGIN_DETAIL_FIELDS.contains(&"rzye"));
        assert!(MARGIN_DETAIL_FIELDS.contains(&"rqye"));
        assert!(MARGIN_DETAIL_FIELDS.contains(&"rzmre"));
        assert!(MARGIN_DETAIL_FIELDS.contains(&"rqmcl"));
    }
}

/// Tushare API 客户端（带速率限制）
#[derive(Clone)]
pub struct TushareClient {
    http: HttpClient,
    config: TushareConfig,
    limiter: Arc<GovernorRateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>>,
}

impl TushareClient {
    pub fn new(config: TushareConfig) -> QuantResult<Self> {
        if config.token.is_empty() {
            return Err(QuantError::Auth("TUSHARE_TOKEN 未设置".into()));
        }
        let http = HttpClient::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .pool_max_idle_per_host(20)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .user_agent("Quant/0.1.0")
            .build()?;

        // 令牌桶：每分钟 replenish N 次，允许 10 次突发
        let quota = Quota::per_minute(
            NonZeroU32::new(config.rate_limit_per_minute).unwrap_or(NonZeroU32::new(200).unwrap()),
        )
        .allow_burst(NonZeroU32::new(10).unwrap());
        let limiter = Arc::new(GovernorRateLimiter::keyed(quota));

        info!(
            rate_limit_per_minute = config.rate_limit_per_minute,
            "Tushare client 初始化"
        );
        Ok(Self {
            http,
            config,
            limiter,
        })
    }

    /// 从环境变量创建
    pub fn from_env() -> QuantResult<Self> {
        Self::new(TushareConfig::default())
    }

    /// 等待速率限制许可后发送 API 请求
    async fn call_api<T: DeserializeOwned>(
        &self,
        api_name: &str,
        params: Vec<(&str, &str)>,
        fields: &[&str],
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        // 速率限制：使用 "tushare" 作为全局 key
        // until_key_ready() 会阻塞直到有可用令牌
        self.limiter.until_key_ready(&"tushare".to_string()).await;

        let request = TushareRequest {
            api_name: api_name.into(),
            token: self.config.token.clone(),
            params: params
                .into_iter()
                .map(|(k, v)| (k.into(), serde_json::Value::String(v.into())))
                .collect(),
            fields: Some(fields.iter().map(|&s| s.into()).collect()),
        };

        debug!(api = %api_name, "Tushare API 调用");

        let resp = self
            .http
            .post(&self.config.base_url)
            .json(&request)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(QuantError::Api {
                code: resp.status().as_u16() as i32,
                message: format!("HTTP {}", resp.status()),
            });
        }

        let body: TushareResponse<Vec<serde_json::Value>> = resp.json().await?;
        if body.code != 0 {
            return Err(QuantError::Api {
                code: body.code,
                message: body.msg.clone().unwrap_or_default(),
            });
        }
        Ok(body)
    }

    /// 获取股票基本信息
    pub async fn stock_basic(
        &self,
        exchange: Option<&str>,
        list_status: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = Vec::new();
        if let Some(ex) = exchange {
            params.push(("exchange", ex));
        }
        if let Some(ls) = list_status {
            params.push(("list_status", ls));
        }
        self.call_api::<Vec<serde_json::Value>>("stock_basic", params, &[])
            .await
    }

    /// 获取日线行情（批量，支持逗号分隔多只股票，支持分页）
    pub async fn daily_batch(
        &self,
        ts_codes: &[String],
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let codes = ts_codes.join(",");
        let mut owned: Vec<String> = vec![codes];
        // Pre-push limit/offset so references stay valid
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = vec![("ts_code", owned[0].as_str())];
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if limit.is_some() {
            params.push(("limit", owned[1].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 2 } else { 1 };
            params.push(("offset", owned[idx].as_str()));
        }
        self.call_api::<Vec<serde_json::Value>>("daily", params, &[])
            .await
    }

    /// 获取基金/ETF 日线数据，支持按代码 + 交易日期区间拉取。
    /// Tushare fund_daily endpoint — 适用于场内 ETF/LOF
    /// 获取基金/ETF 基本信息（名称、类型、管理人）。Tushare fund_basic 接口。
    pub async fn fund_basic(
        &self,
        market: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(m) = market {
            params.push(("market", m));
        }
        self.call_api::<Vec<serde_json::Value>>(
            "fund_basic",
            params,
            &[
                "ts_code",
                "name",
                "management",
                "custodian",
                "fund_type",
                "found_date",
                "due_date",
                "list_date",
                "issue_date",
                "delist_date",
                "issue_amount",
                "m_fee",
                "c_fee",
                "duration_year",
                "p_value",
                "min_amount",
                "exp_return",
                "benchmark",
                "status",
                "invest_type",
                "type",
                "trustee",
                "purc_startdate",
                "redm_startdate",
                "market",
            ],
        )
        .await
    }

    pub async fn fund_daily(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(d) = trade_date {
            params.push(("trade_date", d));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d));
        }
        self.call_api::<Vec<serde_json::Value>>("fund_daily", params, &[])
            .await
    }

    /// Tushare realtime_quote — 盘中实时行情（需 PRO 积分>=2000）
    /// 返回字段: ts_code, name, price, open, pre_close, high, low, volume, amount, bid, ask
    pub async fn realtime_quote(
        &self,
        ts_code: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        self.call_api::<Vec<serde_json::Value>>("realtime_quote", params, &[])
            .await
    }

    /// 获取每日基础/估值数据，支持按交易日或按单只股票区间分页拉取。
    pub async fn daily_basic(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = trade_date {
            params.push(("trade_date", date));
        }
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "daily_basic",
            params,
            &[
                "ts_code",
                "trade_date",
                "pe_ttm",
                "pb",
                "ps_ttm",
                "dv_ttm",
                "total_share",
                "float_share",
                "free_share",
                "total_mv",
                "circ_mv",
            ],
        )
        .await
    }

    /// 获取个股资金流向数据，支持按交易日或按单只股票区间分页拉取。
    pub async fn moneyflow(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = trade_date {
            params.push(("trade_date", date));
        }
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "moneyflow",
            params,
            &[
                "ts_code",
                "trade_date",
                "buy_sm_vol",
                "buy_sm_amount",
                "sell_sm_vol",
                "sell_sm_amount",
                "buy_md_vol",
                "buy_md_amount",
                "sell_md_vol",
                "sell_md_amount",
                "buy_lg_vol",
                "buy_lg_amount",
                "sell_lg_vol",
                "sell_lg_amount",
                "buy_elg_vol",
                "buy_elg_amount",
                "sell_elg_vol",
                "sell_elg_amount",
                "net_mf_vol",
                "net_mf_amount",
            ],
        )
        .await
    }

    /// 获取业绩预告。
    pub async fn forecast(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        period: Option<&str>,
        forecast_type: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if let Some(p) = period {
            params.push(("period", p));
        }
        if let Some(t) = forecast_type {
            params.push(("type", t));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "forecast",
            params,
            &[
                "ts_code",
                "ann_date",
                "end_date",
                "type",
                "p_change_min",
                "p_change_max",
                "net_profit_min",
                "net_profit_max",
                "first_ann_date",
                "summary",
                "change_reason",
            ],
        )
        .await
    }

    /// 获取业绩快报。
    pub async fn express(
        &self,
        ts_code: &str,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        period: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = vec![("ts_code", ts_code)];
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if let Some(p) = period {
            params.push(("period", p));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "express",
            params,
            &[
                "ts_code",
                "ann_date",
                "end_date",
                "revenue",
                "operate_profit",
                "total_profit",
                "n_income",
                "diluted_eps",
                "diluted_roe",
                "yoy_sales",
                "yoy_dedu_np",
                "is_audit",
                "perf_summary",
                "remark",
            ],
        )
        .await
    }

    /// 获取财报披露计划日期。
    pub async fn disclosure_date(
        &self,
        ts_code: Option<&str>,
        end_date: Option<&str>,
        pre_date: Option<&str>,
        ann_date: Option<&str>,
        actual_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        if let Some(date) = pre_date {
            params.push(("pre_date", date));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(date) = actual_date {
            params.push(("actual_date", date));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "disclosure_date",
            params,
            &[
                "ts_code",
                "ann_date",
                "end_date",
                "pre_date",
                "actual_date",
                "modify_date",
            ],
        )
        .await
    }

    /// 获取交易日历
    pub async fn trade_cal(
        &self,
        exchange: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("exchange", exchange)];
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        self.call_api::<Vec<serde_json::Value>>("trade_cal", params, &[])
            .await
    }

    /// 获取复权因子
    pub async fn adj_factor(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        self.call_api::<Vec<serde_json::Value>>("adj_factor", params, &[])
            .await
    }

    /// 获取基金(ETF/LOF)复权因子 — Tushare `fund_adj` 接口（股票用 adj_factor，基金必须用此接口）
    pub async fn fund_adj(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        self.call_api::<Vec<serde_json::Value>>("fund_adj", params, &[])
            .await
    }

    /// 获取指数日线
    pub async fn index_daily(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(sd) = start_date {
            params.push(("start_date", sd));
        }
        if let Some(ed) = end_date {
            params.push(("end_date", ed));
        }
        self.call_api::<Vec<serde_json::Value>>("index_daily", params, &[])
            .await
    }

    /// 申万行业分类。用于 PIT 行业成员源接入前的权限与字段探针。
    pub async fn index_classify(
        &self,
        index_code: Option<&str>,
        level: Option<&str>,
        parent_code: Option<&str>,
        src: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = index_code {
            params.push(("index_code", code));
        }
        if let Some(value) = level {
            params.push(("level", value));
        }
        if let Some(code) = parent_code {
            params.push(("parent_code", code));
        }
        if let Some(value) = src {
            params.push(("src", value));
        }

        self.call_api::<Vec<serde_json::Value>>("index_classify", params, INDEX_CLASSIFY_FIELDS)
            .await
    }

    /// 申万行业成分。`in_date/out_date/is_new` 是后续 PIT membership 审计的关键字段。
    pub async fn index_member(
        &self,
        index_code: Option<&str>,
        ts_code: Option<&str>,
        is_new: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_value = limit.map(|value| value.to_string());
        let offset_value = offset.map(|value| value.to_string());

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = index_code {
            params.push(("index_code", code));
        }
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(value) = is_new {
            params.push(("is_new", value));
        }
        if let Some(value) = limit_value.as_deref() {
            params.push(("limit", value));
        }
        if let Some(value) = offset_value.as_deref() {
            params.push(("offset", value));
        }

        self.call_api::<Vec<serde_json::Value>>("index_member", params, INDEX_MEMBER_FIELDS)
            .await
    }

    // ─── 财务数据 ────────────────────────────────────────────

    /// 利润表
    pub async fn income(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        self.call_api::<Vec<serde_json::Value>>("income", params, &[])
            .await
    }

    /// 资产负债表
    pub async fn balancesheet(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        self.call_api::<Vec<serde_json::Value>>("balancesheet", params, &[])
            .await
    }

    /// 财务指标
    pub async fn fina_indicator(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params = vec![("ts_code", ts_code)];
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        self.call_api::<Vec<serde_json::Value>>("fina_indicator", params, &[])
            .await
    }

    /// 主营业务构成。该接口没有公告日字段，任何 PIT 特征都必须先用财报公告/披露链路补 available_at。
    pub async fn fina_mainbz(
        &self,
        ts_code: &str,
        period: Option<&str>,
        business_type: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = vec![("ts_code", ts_code)];
        if let Some(value) = period {
            params.push(("period", value));
        }
        if let Some(value) = business_type {
            params.push(("type", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }

        self.call_api::<Vec<serde_json::Value>>("fina_mainbz", params, FINA_MAINBZ_FIELDS)
            .await
    }

    /// 主营业务构成 VIP。按报告期拉全市场，适合 P3.19E 全历史分块补数。
    pub async fn fina_mainbz_vip(
        &self,
        period: &str,
        business_type: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = vec![("period", period)];
        if let Some(value) = business_type {
            params.push(("type", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("fina_mainbz_vip", params, FINA_MAINBZ_VIP_FIELDS)
            .await
    }

    /// 卖方盈利预测数据。用于 P3.19 后续 broad analyst revision 源发现的只读权限探针。
    pub async fn report_rc(
        &self,
        ts_code: Option<&str>,
        report_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = ts_code {
            params.push(("ts_code", value));
        }
        if let Some(value) = report_date {
            params.push(("report_date", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("report_rc", params, REPORT_RC_FIELDS)
            .await
    }

    /// 期货日线行情。用于 P3.19 产业链/价格链高频代理的只读权限探针。
    pub async fn fut_daily(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        exchange: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = ts_code {
            params.push(("ts_code", value));
        }
        if let Some(value) = trade_date {
            params.push(("trade_date", value));
        }
        if let Some(value) = exchange {
            params.push(("exchange", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("fut_daily", params, FUT_DAILY_FIELDS)
            .await
    }

    /// 期货仓单日报。用于 P3.19 产业链/价格链高频代理的只读权限探针。
    pub async fn fut_wsr(
        &self,
        trade_date: Option<&str>,
        symbol: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        exchange: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = trade_date {
            params.push(("trade_date", value));
        }
        if let Some(value) = symbol {
            params.push(("symbol", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = exchange {
            params.push(("exchange", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("fut_wsr", params, FUT_WSR_FIELDS)
            .await
    }

    /// 期货每日成交持仓排名。用于 P3.19 产业链/价格链高频代理的只读权限探针。
    pub async fn fut_holding(
        &self,
        trade_date: Option<&str>,
        symbol: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        exchange: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = trade_date {
            params.push(("trade_date", value));
        }
        if let Some(value) = symbol {
            params.push(("symbol", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = exchange {
            params.push(("exchange", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("fut_holding", params, FUT_HOLDING_FIELDS)
            .await
    }

    /// 股权质押统计。`end_date` 是统计截止日，不是公告可得日；进入因子前必须完成 available_at 审计。
    pub async fn pledge_stat(
        &self,
        ts_code: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = ts_code {
            params.push(("ts_code", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("pledge_stat", params, PLEDGE_STAT_FIELDS)
            .await
    }

    /// 股权质押明细。`ann_date` 是原生 PIT 可得日候选，后续 schema 必须保留并按其过滤。
    pub async fn pledge_detail(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let limit_s;
        let offset_s;
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = ts_code {
            params.push(("ts_code", value));
        }
        if let Some(value) = ann_date {
            params.push(("ann_date", value));
        }
        if let Some(value) = start_date {
            params.push(("start_date", value));
        }
        if let Some(value) = end_date {
            params.push(("end_date", value));
        }
        if let Some(value) = limit {
            limit_s = value.to_string();
            params.push(("limit", limit_s.as_str()));
        }
        if let Some(value) = offset {
            offset_s = value.to_string();
            params.push(("offset", offset_s.as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("pledge_detail", params, PLEDGE_DETAIL_FIELDS)
            .await
    }

    /// 现金流量表，用于 Phase 7 数据权限 smoke 与后续现金流质量特征。
    pub async fn cashflow(
        &self,
        ts_code: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = vec![("ts_code", ts_code)];
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "cashflow",
            params,
            &[
                "ts_code",
                "ann_date",
                "f_ann_date",
                "end_date",
                "net_profit",
                "n_cashflow_act",
                "c_cash_equ_end_period",
            ],
        )
        .await
    }

    /// 分红送股，用于 Phase 7 数据权限 smoke 与后续股息质量特征。
    pub async fn dividend(
        &self,
        ts_code: &str,
        ann_date: Option<&str>,
        record_date: Option<&str>,
        ex_date: Option<&str>,
        imp_ann_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = vec![("ts_code", ts_code)];
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(date) = record_date {
            params.push(("record_date", date));
        }
        if let Some(date) = ex_date {
            params.push(("ex_date", date));
        }
        if let Some(date) = imp_ann_date {
            params.push(("imp_ann_date", date));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "dividend",
            params,
            &[
                "ts_code",
                "end_date",
                "ann_date",
                "div_proc",
                "cash_div",
                "cash_div_tax",
                "record_date",
                "ex_date",
                "pay_date",
                "imp_ann_date",
            ],
        )
        .await
    }

    /// 股票回购。Tushare 该接口按公告日期查询，官方参数不包含 ts_code。
    pub async fn repurchase(
        &self,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "repurchase",
            params,
            &[
                "ts_code",
                "ann_date",
                "end_date",
                "proc",
                "exp_date",
                "vol",
                "amount",
                "high_limit",
                "low_limit",
            ],
        )
        .await
    }

    /// 限售股解禁。按解禁日期区间拉取，`ann_date` 是 PIT 可得日。
    pub async fn share_float(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "share_float",
            params,
            &[
                "ts_code",
                "ann_date",
                "float_date",
                "float_share",
                "float_ratio",
                "holder_name",
                "share_type",
            ],
        )
        .await
    }

    /// 股东人数。`ann_date` 是原生 PIT 可得日，`end_date` 是统计截止日。
    pub async fn stk_holdernumber(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "stk_holdernumber",
            params,
            STK_HOLDER_NUMBER_FIELDS,
        )
        .await
    }

    /// 前十大股东。`ann_date` 是披露日，不能用 `end_date` 提前可得性。
    pub async fn top10_holders(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("top10_holders", params, TOP10_HOLDERS_FIELDS)
            .await
    }

    /// 前十大流通股东。`ann_date` 是披露日。
    pub async fn top10_floatholders(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "top10_floatholders",
            params,
            TOP10_FLOAT_HOLDERS_FIELDS,
        )
        .await
    }

    /// 股东增减持。事件型数据，只能作为 shareholder_structure 的辅助审计源。
    pub async fn stk_holdertrade(
        &self,
        ts_code: Option<&str>,
        ann_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut owned: Vec<String> = Vec::new();
        if let Some(l) = limit {
            owned.push(l.to_string());
        }
        if let Some(o) = offset {
            owned.push(o.to_string());
        }

        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = ann_date {
            params.push(("ann_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        if limit.is_some() {
            params.push(("limit", owned[0].as_str()));
        }
        if offset.is_some() {
            let idx = if limit.is_some() { 1 } else { 0 };
            params.push(("offset", owned[idx].as_str()));
        }

        self.call_api::<Vec<serde_json::Value>>("stk_holdertrade", params, STK_HOLDER_TRADE_FIELDS)
            .await
    }

    /// 大宗交易。交易日后披露，使用时应按 available_at 做 PIT 过滤。
    pub async fn block_trade(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code));
        }
        if let Some(date) = trade_date {
            params.push(("trade_date", date));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }

        self.call_api::<Vec<serde_json::Value>>(
            "block_trade",
            params,
            &[
                "ts_code",
                "trade_date",
                "price",
                "vol",
                "amount",
                "buyer",
                "seller",
            ],
        )
        .await
    }

    /// 沪深港通资金流向 (North/South-bound capital flow)
    pub async fn moneyflow_hsgt(
        &self,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(d) = trade_date {
            params.push(("trade_date", d));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d));
        }
        self.call_api::<Vec<serde_json::Value>>("moneyflow_hsgt", params, &[])
            .await
    }

    /// 融资融券交易汇总（市场整体，按日）
    /// Tushare margin endpoint — 返回 SSE/SZSE 的融资余额/融券余额等
    pub async fn margin(
        &self,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(d) = trade_date {
            params.push(("trade_date", d));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d));
        }
        self.call_api::<Vec<serde_json::Value>>("margin", params, &[])
            .await
    }

    /// 个股融资融券交易明细（证券级，按日）
    pub async fn margin_detail(
        &self,
        ts_code: Option<&str>,
        trade_date: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(code) = ts_code {
            params.push(("ts_code", code.to_string()));
        }
        if let Some(d) = trade_date {
            params.push(("trade_date", d.to_string()));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d.to_string()));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d.to_string()));
        }
        if let Some(limit) = limit {
            params.push(("limit", limit.to_string()));
        }
        if let Some(offset) = offset {
            params.push(("offset", offset.to_string()));
        }
        let borrowed: Vec<(&str, &str)> = params
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        self.call_api::<Vec<serde_json::Value>>("margin_detail", borrowed, MARGIN_DETAIL_FIELDS)
            .await
    }

    /// 股票曾用名 / 名称变更历史
    /// Tushare namechange endpoint — 返回 ts_code, name, start_date, end_date, change_reason
    pub async fn namechange(
        &self,
        ts_code: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(c) = ts_code {
            params.push(("ts_code", c));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d));
        }
        self.call_api::<Vec<serde_json::Value>>("namechange", params, &[])
            .await
    }

    /// 股票停牌信息
    /// Tushare suspend_d endpoint — 返回 ts_code, trade_date, suspend_type
    pub async fn suspend_d(
        &self,
        trade_date: Option<&str>,
        ts_code: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(d) = trade_date {
            params.push(("trade_date", d));
        }
        if let Some(c) = ts_code {
            params.push(("ts_code", c));
        }
        if let Some(d) = start_date {
            params.push(("start_date", d));
        }
        if let Some(d) = end_date {
            params.push(("end_date", d));
        }
        self.call_api::<Vec<serde_json::Value>>("suspend_d", params, &[])
            .await
    }

    /// 涨跌停列表
    /// Tushare limit_list_d endpoint — 返回涨跌停股票
    /// 支持 trade_date(单日) 或 start_date+end_date(日期范围)
    pub async fn limit_list_d(
        &self,
        trade_date: Option<&str>,
        ts_code: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> QuantResult<TushareResponse<Vec<serde_json::Value>>> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        if let Some(d) = trade_date {
            params.push(("trade_date", d));
        }
        if let Some(c) = ts_code {
            params.push(("ts_code", c));
        }
        if let Some(s) = start_date {
            params.push(("start_date", s));
        }
        if let Some(e) = end_date {
            params.push(("end_date", e));
        }
        self.call_api::<Vec<serde_json::Value>>("limit_list_d", params, &[])
            .await
    }
}
