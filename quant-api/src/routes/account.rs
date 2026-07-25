//! 账户策略类型状态骨架（BC7 账户持仓，DDD 重构 Step 2 引入）。
//!
//! 背景：当前账户策略模式（无杠杆/杠杆/多策略 blend）靠运行时字符串 `account_type`
//! 和 `leverage_multiplier` 字段约定，杠杆融资建模与无杠杆账户混用同一代码路径，
//! 曾导致 [[quant-unleveraged-no-margin-cash-shortfall]] 类 bug。本模块引入
//! `Account<Policy>` 类型状态，让不同策略模式的账户在类型层分离。
//!
//! 设计原则（本步只引入骨架，不改现有实现）：
//! - `Account<Policy>`：`Policy` 标记账户策略模式，编译期区分无杠杆/杠杆/多策略。
//! - 状态转换通过 `From` 实现，强制合法路径。
//!
//! 本模块尚未被路由挂载（Step 5 迁移 accounts.rs 账户构建逻辑时接入），
//! 允许 dead_code 直到届时启用。

#![allow(dead_code)]

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 账户策略状态标记 trait。
pub trait AccountPolicy: private::Sealed {}

/// 现金账户：无杠杆、无融资融券，仅做多现货。
pub struct CashAccount;
/// 保证金账户：允许杠杆融资做多（leverage_multiplier > 1）。
pub struct MarginAccount;
/// 多策略 blend 账户：多个子策略加权混合。
pub struct BlendAccount;

mod private {
    pub trait Sealed {}
    impl Sealed for super::CashAccount {}
    impl Sealed for super::MarginAccount {}
    impl Sealed for super::BlendAccount {}
}

impl AccountPolicy for CashAccount {}
impl AccountPolicy for MarginAccount {}
impl AccountPolicy for BlendAccount {}

/// 账户类型状态机。
///
/// `Policy` 标记账户策略模式，编译期保证：
/// - 只有 `MarginAccount` 可设置 `leverage_multiplier > 1`。
/// - `BlendAccount` 才有 `sub_strategy_weights`。
///
/// 当前为骨架，Step 5 将迁移 accounts.rs 的账户构建逻辑到此类型轨道。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account<P: AccountPolicy> {
    pub account_id: String,
    pub strategy_version_id: String,
    /// 账户净值（NAV）。
    pub nav: Decimal,
    _policy: std::marker::PhantomData<P>,
}

impl Account<CashAccount> {
    pub fn new_cash(account_id: impl Into<String>, strategy_version_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            strategy_version_id: strategy_version_id.into(),
            nav: Decimal::ZERO,
            _policy: std::marker::PhantomData,
        }
    }

    /// 现金账户升级为保证金账户（允许开通杠杆）。
    pub fn enable_margin(self) -> Account<MarginAccount> {
        Account {
            account_id: self.account_id,
            strategy_version_id: self.strategy_version_id,
            nav: self.nav,
            _policy: std::marker::PhantomData,
        }
    }
}

impl Account<MarginAccount> {
    pub fn new_margin(
        account_id: impl Into<String>,
        strategy_version_id: impl Into<String>,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            strategy_version_id: strategy_version_id.into(),
            nav: Decimal::ZERO,
            _policy: std::marker::PhantomData,
        }
    }
}

/// 杠杆倍数：仅保证金账户可设置 > 1。
///
/// 类型门禁：此函数签名为 `&Account<MarginAccount>`，编译期保证现金账户无法调用。
pub fn leverage_multiplier(_account: &Account<MarginAccount>) -> Decimal {
    // Step 5 迁移实际杠杆配置，当前返回占位值。
    Decimal::ONE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cash_account_can_become_margin() {
        let cash = Account::<CashAccount>::new_cash("acc_v24", "h20_v1");
        let margin: Account<MarginAccount> = cash.enable_margin();
        assert_eq!(margin.account_id, "acc_v24");
    }

    #[test]
    fn leverage_only_on_margin() {
        let margin = Account::<MarginAccount>::new_margin("acc_v24", "h20_v1");
        // 编译期门禁：下行若改为 CashAccount 会编译失败（无该函数实现）。
        let _lev = leverage_multiplier(&margin);
    }
}
