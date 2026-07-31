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
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// 创建账号输入（重复组 G：统一 paper.rs:1231 + accounts.rs:848 字段集）。
#[derive(Debug, Clone)]
pub struct CreateAccountInput {
    pub name: String,
    pub base_currency: String,
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

    /// 更新 NAV 摘要（重复组 C：paper.rs:858/1220 两处 SQL 完全相同）。
    ///
    /// `UPDATE paper_account SET current_nav=$1, peak_nav=$2, max_drawdown_pct=$3,
    ///  total_trades=$4, updated_at=now() WHERE paper_account_id=$5`
    ///
    /// 注：paper.rs:586（无 total_trades）与 rebalance.rs:537（含 last_signal_date）是变体，
    /// 字段集不同，保留原处不合并。
    fn update_nav(
        &self,
        id: &str,
        current_nav: f64,
        peak_nav: f64,
        max_drawdown_pct: f64,
        total_trades: i32,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 创建账号（重复组 G：统一 paper.rs:1231 + accounts.rs:848 字段集，消除 schema 漂移）。
    ///
    /// 两处原 INSERT 字段集不一致：
    /// - paper.rs:1231：base_currency/dingtalk_webhook_url（无 leverage/user_id）
    /// - accounts.rs:848：leverage_enabled/mode/multiplier/signal_source/user_id（无 base_currency/webhook）
    ///
    /// 统一为并集 SQL，未传字段用表默认值（base_currency 默认 CNY，leverage 默认 false/1.0/fixed，
    /// signal_source 默认 factor，user_id/webhook 允许 NULL）。
    /// 返回新生成的 account_id（`pa-{uuid}` 前缀）。
    fn create(&self, id: &str, input: &CreateAccountInput)
        -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;
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

    fn update_nav(
        &self,
        id: &str,
        current_nav: f64,
        peak_nav: f64,
        max_drawdown_pct: f64,
        total_trades: i32,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query(
                "UPDATE paper_account SET current_nav=$1, peak_nav=$2, max_drawdown_pct=$3,
                 total_trades=$4, updated_at=now() WHERE paper_account_id=$5",
            )
            .bind(current_nav)
            .bind(peak_nav)
            .bind(max_drawdown_pct)
            .bind(total_trades)
            .bind(id)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }

    fn create(&self, id: &str, input: &CreateAccountInput) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            // 字段并集：统一 paper.rs（base_currency/webhook）+ accounts.rs（leverage/signal_source/user_id）。
            // cash = initial_capital（两处原语义一致）。status 固定 'active'。
            sqlx::query(
                "INSERT INTO paper_account
                   (paper_account_id, name, base_currency, account_type, initial_capital, cash,
                    leverage_enabled, leverage_mode, leverage_multiplier, signal_source,
                    status, user_id, dingtalk_webhook_url)
                 VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9, 'active', $10, $11)",
            )
            .bind(id)
            .bind(&input.name)
            .bind(&input.base_currency)
            .bind(&input.account_type)
            .bind(input.initial_capital)
            .bind(input.leverage_enabled)
            .bind(&input.leverage_mode)
            .bind(input.leverage_multiplier)
            .bind(&input.signal_source)
            .bind(&input.user_id)
            .bind(&input.dingtalk_webhook_url)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }
}
