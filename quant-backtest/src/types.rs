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

/// PIT 校验通过的行情 bar。
///
/// 类型门禁：`VerifiedBar` 只能由 `RawBar` 经 `DataVersionRegistry` 校验后构造，
/// 回测/调仓只接受 `VerifiedBar`，从类型层杜绝"用了未注册版本的脏数据"。
/// Step 5 引入 `try_from_raw` 做实际校验，当前仅为类型骨架。
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedBar {
    pub symbol: Symbol,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    /// 该 bar 所属数据版本（PIT 追溯依据）。
    pub data_version_id: DataVersionId,
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
}
