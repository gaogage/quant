//! Tushare 数据源 HTTP 客户端
//!
//! 从 valentina 迁移，增加 governor rate limiter 和复权因子 API。

use governor::{
    clock::DefaultClock,
    state::keyed::DefaultKeyedStateStore,
    Quota, RateLimiter as GovernorRateLimiter,
};
use quant_common::{QuantError, QuantResult};
use reqwest::Client as HttpClient;
use serde::de::DeserializeOwned;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

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
        Self {
            base_url: std::env::var("TUSHARE_API_URL")
                .unwrap_or_else(|_| "http://api.tushare.pro".to_string()),
            token: std::env::var("TUSHARE_TOKEN").unwrap_or_default(),
            timeout_secs: 30,
            max_retries: 3,
            retry_delay_ms: 1000,
            rate_limit_per_minute: 200,
        }
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
            NonZeroU32::new(config.rate_limit_per_minute)
                .unwrap_or(NonZeroU32::new(200).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(10).unwrap(),
        );
        let limiter = Arc::new(GovernorRateLimiter::keyed(quota));

        info!(
            rate_limit_per_minute = config.rate_limit_per_minute,
            "Tushare client 初始化"
        );
        Ok(Self { http, config, limiter })
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
        self.limiter
            .until_key_ready(&"tushare".to_string())
            .await;

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
        self.call_api::<Vec<serde_json::Value>>("stock_basic", params, &[]).await
    }

    /// 获取日线行情
    pub async fn daily(
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
        self.call_api::<Vec<serde_json::Value>>("daily", params, &[]).await
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
        self.call_api::<Vec<serde_json::Value>>("trade_cal", params, &[]).await
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
        self.call_api::<Vec<serde_json::Value>>("adj_factor", params, &[]).await
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
        self.call_api::<Vec<serde_json::Value>>("index_daily", params, &[]).await
    }
}
