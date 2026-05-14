/// Quant 系统统一错误类型
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QuantError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

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
