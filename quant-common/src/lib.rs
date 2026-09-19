/// Quant 系统统一错误类型
use thiserror::Error;

pub mod identifiers;
pub mod mvo;
pub mod time_utils;
pub mod trading_rules;

#[derive(Error, Debug)]
pub enum QuantError {
    // P3 瘦身（2026-09-19）：删除 Http/Database 两个 #[from] 桥接变体——全仓
    // 零使用（tushare client 用 Auth/Api 装载错误），却让共享内核携带
    // sqlx/reqwest 依赖传递给全部下游（含纯计算 crate）。HTTP/DB 错误由
    // 各 crate 在边界处 map_err 到 Api/Validation 等语义变体。
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("API error (code={code}): {message}")]
    Api { code: i32, message: String },

    #[error("Parameter error: {0}")]
    Parameter(String),

    #[error("Data validation error: {0}")]
    Validation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Task error: {0}")]
    Task(String),

    #[error("{0}")]
    Other(String),
}

pub type QuantResult<T> = Result<T, QuantError>;

/// 统一任务状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Partial,
    Timeout,
    Failed,
    CancelRequested,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Timeout
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_string(self).unwrap_or_default();
        write!(f, "{}", s.trim_matches('"'))
    }
}

/// 统一 ID 类型
pub type TaskId = uuid::Uuid;
pub type DataVersionId = String;
pub type StrategyVersionId = String;
