//! Paper 账号仓储（DDD 重构：收敛 paper_account 散落 SQL 为 Repository trait）。
//!
//! 背景：paper_account 表的裸 SQL 散落 quant-api/src/routes/ 14 文件 58 处，
//! 识别出 10 个重复组。本模块定义 Repository trait，逐步收敛重复 SQL。
//!
//! 设计原则（参照 R11 versioning.rs 的 PgDataVersionRegistry 模式）：
//! - trait 定义统一契约，PG 实现同文件，用原生 `impl Future`（Rust 1.75+，非 async_trait）
//! - 错误用 thiserror 枚举 `PaperRepositoryError`
//! - 单 crate（paper_account 仅 quant-api 使用），trait 放 shared/ 不放 quant-common
//! - 资金路径（apply_fill/update_current_nav/mark_to_market 等）保留原处不抽象
//!
//! 分批推进：批次1 先收敛重复组 B（find_user_id，5 处 SQL 完全相同）+
//! 重复组 A（find_initial_capital，4 处 SQL 完全相同）。

use sqlx::PgPool;

/// Paper 仓储统一错误类型。
#[derive(Debug, thiserror::Error)]
pub enum PaperRepositoryError {
    #[error("paper account not found: {0}")]
    NotFound(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Paper 账号领域实体（聚合 paper_account 表常用字段）。
///
/// 字段按 schema 完整定义，调用方按需取用。Option 字段对应可空列。
#[derive(Debug, Clone)]
pub struct PaperAccount {
    pub account_id: String,
    pub name: String,
    pub base_currency: String,
    pub initial_capital: f64,
    pub cash: f64,
    pub status: String,
    pub account_type: String,
    pub strategy_version_id: Option<String>,
    pub signal_source: String,
    pub current_nav: Option<f64>,
    pub peak_nav: Option<f64>,
    pub max_drawdown_pct: Option<f64>,
    pub total_trades: i32,
    pub last_signal_date: Option<chrono::NaiveDate>,
    pub leverage_enabled: bool,
    pub leverage_multiplier: f64,
    pub leverage_mode: String,
    pub margin_amount: f64,
    pub reserve_amount: f64,
    pub liquidation_threshold: Option<f64>,
    pub warning_threshold: Option<f64>,
    pub user_id: Option<String>,
    pub dingtalk_webhook_url: Option<String>,
}

/// 杠杆配置（重复组 F：portfolio.rs:1777 + rebalance.rs:709）。
#[derive(Debug, Clone)]
pub struct LeverageConfig {
    pub enabled: bool,
    pub multiplier: f64,
    pub mode: String,
    pub liquidation_threshold: Option<f64>,
    pub warning_threshold: Option<f64>,
}

/// 创建账号输入（重复组 G：统一 paper.rs:1255 + accounts.rs:848 字段集）。
#[derive(Debug, Clone)]
pub struct CreateAccountInput {
    pub name: String,
    pub account_type: String,
    pub initial_capital: f64,
    pub leverage_enabled: bool,
    pub leverage_mode: String,
    pub leverage_multiplier: f64,
    pub signal_source: String,
    pub user_id: Option<String>,
    pub dingtalk_webhook_url: Option<String>,
}

/// Paper 账号仓储契约。
///
/// 集中 paper_account 的查询/写入，消除散落 SQL 样板。
/// 注：用原生 `impl Future`（Rust 1.75+），暂非 dyn-compatible（参照 R11）。
pub trait PaperAccountRepository {
    /// 查账号归属用户 ID（鉴权专用，重复组 B：5 处 SQL 完全相同）。
    fn find_user_id(&self, id: &str)
        -> impl std::future::Future<Output = Result<Option<String>, PaperRepositoryError>> + Send;

    /// 查初始资金（重复组 A：4 处 SQL 完全相同，`initial_capital::double precision`）。
    fn find_initial_capital(&self, id: &str)
        -> impl std::future::Future<Output = Result<Option<f64>, PaperRepositoryError>> + Send;
}

/// PostgreSQL 实现的 paper 账号仓储。
pub struct PgPaperAccountRepo<'a> {
    pool: &'a PgPool,
}

impl<'a> PgPaperAccountRepo<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

impl<'a> PaperAccountRepository for PgPaperAccountRepo<'a> {
    /// `SELECT user_id FROM paper_account WHERE paper_account_id = $1`
    ///
    /// 5 处原样收敛：accounts.rs:326/882/939/978/1081（SQL 完全相同）。
    fn find_user_id(&self, id: &str) -> impl std::future::Future<Output = Result<Option<String>, PaperRepositoryError>> + Send {
        async move {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT user_id FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
            Ok(row.and_then(|(uid,)| uid))
        }
    }

    /// `SELECT initial_capital::double precision FROM paper_account WHERE paper_account_id = $1`
    ///
    /// 4 处原样收敛：paper.rs:439/653/942 + accounts.rs:1151（SQL 完全相同）。
    /// 另 mvo_engine.rs:129（多查 strategy_version_id）+ report.rs:466（多查 max_drawdown_pct）
    /// 字段集不同，留 find_by_id 处理。
    fn find_initial_capital(&self, id: &str) -> impl std::future::Future<Output = Result<Option<f64>, PaperRepositoryError>> + Send {
        async move {
            let row: Option<(f64,)> = sqlx::query_as(
                "SELECT initial_capital::double precision FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
            Ok(row.map(|(c,)| c))
        }
    }
}
