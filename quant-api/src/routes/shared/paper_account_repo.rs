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

use rust_decimal::Decimal;
use sqlx::PgPool;

/// Paper 仓储统一错误类型。
#[derive(Debug, thiserror::Error)]
pub enum PaperRepositoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// 重置清表失败（带表名上下文，对齐原 `clean {table}` 错误消息语义）。
    #[error("wipe {table}: {source}")]
    Wipe {
        table: &'static str,
        source: sqlx::Error,
    },
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
    fn find_user_id(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<String>, PaperRepositoryError>> + Send;

    /// 查初始资金（重复组 A：4 处 SQL 完全相同，`initial_capital::double precision`）。
    fn find_initial_capital(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<f64>, PaperRepositoryError>> + Send;

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
    fn create(
        &self,
        id: &str,
        input: &CreateAccountInput,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 查策略版本 + 杠杆倍数（重复组：report.rs/accounts.rs 2 处 SQL 完全相同）。
    ///
    /// 业务语义：取账号策略与杠杆以对齐回测基准口径（日报偏离放大系数 /
    /// 账号页"实盘 vs 回测"对比曲线）。
    fn find_strategy_and_leverage(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<(Option<String>, f64)>, PaperRepositoryError>>
           + Send;

    /// 查全部 active+simulated 账号 ID（重复组：scheduler.rs×2 + sync/eod.rs，3 处 SQL 完全相同）。
    ///
    /// 遍历活跃模拟账号做盯市/调仓的驱动清单。查询只读，结果驱动资金操作循环。
    fn find_active_simulated_ids(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<String>, PaperRepositoryError>> + Send;

    /// 查资金基准 NAV（NULL 降级 initial_capital）（2 处 SQL 完全相同）。
    ///
    /// 资金读路径：决定建仓规模（rebalance_account）/ PTrade 信号名义额（signal_export）。
    /// 返回 Decimal（列类型 numeric，无 cast——调用侧保留原转换）。
    fn find_current_nav_or_capital(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<Decimal>, PaperRepositoryError>> + Send;

    /// 查维保三要素：持仓市值 + cash + margin（2 处 SQL 完全相同）。
    ///
    /// 资金读路径（强平核心）：maintenance_ratio 与 force_liquidation 强平循环逐轮读取。
    /// paper_position 仅在标量子查询，以 account 为主体归本仓储。
    /// 无行返回 None（调用侧自行处理为无融资/报错）。
    fn find_maintenance_components(
        &self,
        id: &str,
    ) -> impl std::future::Future<
        Output = Result<Option<(Decimal, Decimal, Decimal)>, PaperRepositoryError>,
    > + Send;

    /// 清空账号全部关联子表（重复组：accounts.rs reset + mvo_engine.rs 回放重置，2 处表清单完全相同）。
    ///
    /// 顺序删除 paper_order / paper_fill / paper_position / paper_nav_snapshot /
    /// paper_replay / paper_margin_trade。表名是编译期常量（非用户输入，无注入面），
    /// 收敛消除两处表清单漂移风险（一处加表忘另一处）。
    fn wipe_account_tables(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;
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

// R5b 有意的"原生 impl Future" trait 形态（2026-08-01 设计），AFIT 迁移待 trait
// 定义与全部调用方一并现代化；显式豁免 manual_async_fn。
#[allow(clippy::manual_async_fn)]
impl<'a> PaperAccountRepository for PgPaperAccountRepo<'a> {
    /// `SELECT user_id FROM paper_account WHERE paper_account_id = $1`
    ///
    /// 5 处原样收敛：accounts.rs:326/882/939/978/1081（SQL 完全相同）。
    fn find_user_id(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<String>, PaperRepositoryError>> + Send
    {
        async move {
            let row: Option<(Option<String>,)> =
                sqlx::query_as("SELECT user_id FROM paper_account WHERE paper_account_id = $1")
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
    fn find_initial_capital(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<f64>, PaperRepositoryError>> + Send {
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

    fn create(
        &self,
        id: &str,
        input: &CreateAccountInput,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
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

    /// `SELECT strategy_version_id, COALESCE(leverage_multiplier, 1.0)::double precision
    ///  FROM paper_account WHERE paper_account_id = $1`
    ///
    /// 2 处原样收敛：report.rs:247（日报回测偏离基准放大系数）+
    /// accounts.rs:622（账号页"实盘 vs 回测"对比曲线）。
    fn find_strategy_and_leverage(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<(Option<String>, f64)>, PaperRepositoryError>>
           + Send {
        async move {
            let row = sqlx::query_as::<_, (Option<String>, f64)>(
                "SELECT strategy_version_id, COALESCE(leverage_multiplier, 1.0)::double precision \
                 FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
            Ok(row)
        }
    }

    /// `SELECT paper_account_id FROM paper_account
    ///  WHERE status = 'active' AND account_type = 'simulated'`
    ///
    /// 3 处原样收敛：scheduler.rs:1217（T+1 补盯市循环）+ scheduler.rs:2299（实盘调仓
    /// 账号循环）+ sync/eod.rs:555（EOD 日终盯市循环）。
    fn find_active_simulated_ids(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<String>, PaperRepositoryError>> + Send {
        async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT paper_account_id FROM paper_account \
                 WHERE status = 'active' AND account_type = 'simulated'",
            )
            .fetch_all(self.pool)
            .await?;
            Ok(rows.into_iter().map(|(id,)| id).collect())
        }
    }

    /// `SELECT COALESCE(current_nav, initial_capital) FROM paper_account
    ///  WHERE paper_account_id = $1`
    ///
    /// 2 处原样收敛：rebalance.rs:223（建仓资金基准）+ signal_export.rs:298（PTrade 信号
    /// NAV 规模）。列类型 numeric 无 cast，返回 Decimal 由调用侧转换（保留原语义）。
    fn find_current_nav_or_capital(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<Decimal>, PaperRepositoryError>> + Send
    {
        async move {
            let row: Option<(Decimal,)> = sqlx::query_as(
                "SELECT COALESCE(current_nav, initial_capital) FROM paper_account \
                 WHERE paper_account_id = $1",
            )
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
            Ok(row.map(|(v,)| v))
        }
    }

    /// `SELECT (SELECT COALESCE(SUM(market_value),0) FROM paper_position
    ///          WHERE paper_account_id=$1),
    ///         COALESCE(cash,0), COALESCE(margin_amount,0)
    ///  FROM paper_account WHERE paper_account_id = $1`
    ///
    /// 2 处原样收敛：rebalance.rs:847（maintenance_ratio 维保比例）+
    /// rebalance.rs:888（force_liquidation 强平循环逐轮读取）。
    fn find_maintenance_components(
        &self,
        id: &str,
    ) -> impl std::future::Future<
        Output = Result<Option<(Decimal, Decimal, Decimal)>, PaperRepositoryError>,
    > + Send {
        async move {
            let row = sqlx::query_as::<_, (Decimal, Decimal, Decimal)>(
                "SELECT (SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id=$1),
                        COALESCE(cash,0), COALESCE(margin_amount,0)
                 FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
            Ok(row)
        }
    }

    /// 重置账号关联子表清理序列（6 张表按固定顺序 DELETE）。
    ///
    /// 2 处原样收敛：accounts.rs:1031（reset_account）+ mvo_engine.rs:150
    /// （run_daily_simulation 回放 reset 分支）。表清单为编译期常量。
    fn wipe_account_tables(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            const TABLES: [&str; 6] = [
                "paper_order",
                "paper_fill",
                "paper_position",
                "paper_nav_snapshot",
                "paper_replay",
                "paper_margin_trade",
            ];
            for table in TABLES {
                sqlx::query(&format!(
                    "DELETE FROM {} WHERE paper_account_id = $1",
                    table
                ))
                .bind(id)
                .execute(self.pool)
                .await
                .map_err(|e| PaperRepositoryError::Wipe { table, source: e })?;
            }
            Ok(())
        }
    }
}
