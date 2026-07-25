//! 回测领域类型状态骨架（DDD 重构 Step 2 引入）。
//!
//! 背景：当前策略生命周期（草稿->验证->回测->模拟盘->实盘）靠运行时字符串
//! `status` 字段约定，无编译期保护。本模块引入 Type State 模式，让非法状态
//! 转换在编译期被拒绝（如：未回测的策略不能调 `into_production`）。
//!
//! 设计原则（本步只引入骨架，不改现有实现）：
//! - `VerifiedBar`：带 PIT 校验印记的行情 bar，区别于裸 bar。
//! - `Strategy<S>`：策略类型状态机，`S` 是状态标记类型。
//! - 状态转换通过 `impl From<Strategy<A> for Strategy<B>>` 实现，编译期强制合法路径。
//!
//! Step 5 将把现有散落的策略状态切换代码迁移到这套类型状态轨道。

use chrono::NaiveDate;
use quant_common::identifiers::{DataVersionId, Symbol};
use rust_decimal::Decimal;
use sqlx::PgPool;

/// DB 加载的原始行情 bar，尚未做 PIT 校验。
///
/// 仅 `quant-backtest` crate 内部可构造（DB 查询结果直接组装），外部拿到后
/// 必须调 `try_from_raw` 升级为 `VerifiedBar` 才能进入回测/调仓管线。
///
/// 字段与 `VerifiedBar` 相同，但 `data_version_id` 为裸 `String`（未校验存在性）。
#[derive(Debug, Clone, PartialEq)]
pub struct RawBar {
    pub symbol: Symbol,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    /// 该 bar 声称所属的数据版本（尚未校验是否在 data_version 表注册）。
    pub data_version_id: String,
}

/// PIT 校验通过的行情 bar。
///
/// 类型门禁：`VerifiedBar` 只能由 `RawBar::try_from_raw` 校验后构造，
/// 回测/调仓只接受 `VerifiedBar`，从类型层杜绝"用了未注册版本的脏数据"。
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedBar {
    pub symbol: Symbol,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    /// 该 bar 所属数据版本（PIT 追溯依据，已校验存在）。
    pub data_version_id: DataVersionId,
}

/// `try_from_raw` 校验失败时的错误。
#[derive(Debug, thiserror::Error)]
pub enum VerifiedBarError {
    /// `data_version_id` 在 `data_version` 表中不存在（未注册的脏数据）。
    #[error("data version not registered: {0}")]
    NotRegistered(String),
    /// 数据库查询错误。
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl RawBar {
    /// 唯一公开出口：把原始 bar 升级为 PIT 校验通过的 bar。
    ///
    /// 校验逻辑：查 `data_version` 表确认 `data_version_id` 存在（方案 C，不依赖
    /// 完整 `PgDataVersionRegistry`）。`data_version` 表无 state 列，只校验存在性；
    /// 完整的状态校验（Active/Deprecated）留给后续 Step 4a 补做。
    ///
    /// 注意：此方法为 async（查 DB）。批量校验场景应避免逐条调用，
    /// 可先收集 dv_id 集合一次性查 `WHERE data_version_id = ANY($1)`。
    pub async fn try_from_raw(self, db: &PgPool) -> Result<VerifiedBar, VerifiedBarError> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM data_version WHERE data_version_id = $1)",
        )
        .bind(&self.data_version_id)
        .fetch_one(db)
        .await?;
        if !exists {
            return Err(VerifiedBarError::NotRegistered(self.data_version_id.clone()));
        }
        Ok(VerifiedBar {
            symbol: self.symbol,
            trade_date: self.trade_date,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            data_version_id: DataVersionId::new(self.data_version_id),
        })
    }

    /// crate 内部构造（DB 行直接组装，未校验）。
    pub(crate) fn new(
        symbol: impl Into<String>,
        trade_date: NaiveDate,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        data_version_id: impl Into<String>,
    ) -> Self {
        Self {
            symbol: Symbol::new(symbol),
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            data_version_id: data_version_id.into(),
        }
    }
}

// ─── 策略类型状态机 ───────────────────────────────────────────────

/// 策略生命周期状态标记 trait。
///
/// 每个实现标记一个不可回退的状态节点，转换通过 `From` 实现，
/// 非法转换（如 Production -> Draft）因无 `From` 实现而编译失败。
pub trait StrategyState: private::Sealed {}

/// 草稿态：策略参数未验证，不可回测。
pub struct Draft;
/// 已验证态：参数通过 WFA + bootstrap，可回测。
pub struct Validated;
/// 已回测态：产出回测绩效，可上模拟盘。
pub struct Backtested;
/// 模拟盘态：实时跟踪但无真实资金。
pub struct PaperLive;
/// 实盘态：真实资金运行，最高限制。
pub struct Production;

mod private {
    pub trait Sealed {}
    impl Sealed for super::Draft {}
    impl Sealed for super::Validated {}
    impl Sealed for super::Backtested {}
    impl Sealed for super::PaperLive {}
    impl Sealed for super::Production {}
}

impl StrategyState for Draft {}
impl StrategyState for Validated {}
impl StrategyState for Backtested {}
impl StrategyState for PaperLive {}
impl StrategyState for Production {}

/// 策略类型状态机。
///
/// `S` 标记当前生命周期状态，编译期保证只能沿合法路径推进：
/// `Draft -> Validated -> Backtested -> PaperLive -> Production`
///
/// 当前为骨架，不含策略参数字段（Step 5 填充并迁移现有策略切换代码）。
#[derive(Debug, Clone)]
pub struct Strategy<S: StrategyState> {
    /// 策略版本 ID（不变量，跨状态保持）。
    pub strategy_version_id: String,
    _state: std::marker::PhantomData<S>,
}

impl Strategy<Draft> {
    pub fn new(strategy_version_id: impl Into<String>) -> Self {
        Self {
            strategy_version_id: strategy_version_id.into(),
            _state: std::marker::PhantomData,
        }
    }
}

// 合法状态转换（编译期门禁）：只允许向前推进。
impl From<Strategy<Draft>> for Strategy<Validated> {
    fn from(s: Strategy<Draft>) -> Self {
        Self {
            strategy_version_id: s.strategy_version_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl From<Strategy<Validated>> for Strategy<Backtested> {
    fn from(s: Strategy<Validated>) -> Self {
        Self {
            strategy_version_id: s.strategy_version_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl From<Strategy<Backtested>> for Strategy<PaperLive> {
    fn from(s: Strategy<Backtested>) -> Self {
        Self {
            strategy_version_id: s.strategy_version_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl From<Strategy<PaperLive>> for Strategy<Production> {
    fn from(s: Strategy<PaperLive>) -> Self {
        Self {
            strategy_version_id: s.strategy_version_id,
            _state: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_advances_through_lifecycle() {
        let draft = Strategy::<Draft>::new("h20_v1");
        let validated: Strategy<Validated> = draft.into();
        let backtested: Strategy<Backtested> = validated.into();
        let paper: Strategy<PaperLive> = backtested.into();
        let prod: Strategy<Production> = paper.into();
        assert_eq!(prod.strategy_version_id, "h20_v1");
    }

    #[test]
    fn verified_bar_holds_data_version() {
        let bar = VerifiedBar {
            symbol: Symbol::new("000001"),
            trade_date: NaiveDate::from_ymd_opt(2026, 7, 24).unwrap(),
            open: Decimal::new(12, 1),
            high: Decimal::new(125, 1),
            low: Decimal::new(118, 1),
            close: Decimal::new(122, 1),
            volume: Decimal::from(1_000_000),
            data_version_id: DataVersionId::new("dv_20260724_v1"),
        };
        assert_eq!(bar.symbol.as_str(), "000001");
        assert_eq!(bar.data_version_id.as_str(), "dv_20260724_v1");
    }

    /// 验证 `RawBar::try_from_raw` 的 DB 校验逻辑。
    ///
    /// 需 DB,默认不跑(--ignored 触发)。运行:
    ///   cargo test -p quant-backtest --lib types::tests -- --ignored
    #[tokio::test]
    #[ignore]
    async fn try_from_raw_accepts_registered_rejects_unknown_dv_id() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("DB 连接成功");

        // 取一条真实存在的 data_version_id
        let real_dv_id: String = sqlx::query_scalar(
            "SELECT data_version_id FROM data_version ORDER BY created_at DESC LIMIT 1",
        )
        .fetch_one(&db)
        .await
        .expect("查到至少一条 data_version");

        let raw_ok = RawBar::new(
            "000001",
            NaiveDate::from_ymd_opt(2026, 7, 24).unwrap(),
            Decimal::new(12, 1),
            Decimal::new(125, 1),
            Decimal::new(118, 1),
            Decimal::new(122, 1),
            Decimal::from(1_000_000),
            &real_dv_id,
        );
        let verified = raw_ok.try_from_raw(&db).await.expect("已注册 dv_id 应通过");
        assert_eq!(verified.data_version_id.as_str(), real_dv_id);

        // 未注册的 dv_id 应被拒
        let raw_bad = RawBar::new(
            "000001",
            NaiveDate::from_ymd_opt(2026, 7, 24).unwrap(),
            Decimal::new(12, 1),
            Decimal::new(125, 1),
            Decimal::new(118, 1),
            Decimal::new(122, 1),
            Decimal::from(1_000_000),
            "dv-nonexistent-9999",
        );
        let err = raw_bad.try_from_raw(&db).await.unwrap_err();
        assert!(matches!(err, VerifiedBarError::NotRegistered(_)));
    }
}
