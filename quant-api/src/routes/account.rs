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
    /// 杠杆配置：仅 `MarginAccount` 非空，编译期区分有无杠杆。
    pub leverage_config: Option<LeverageConfig>,
    _policy: std::marker::PhantomData<P>,
}

/// 杠杆配置（仅保证金账户语义）。
///
/// 类型门禁：`Account<CashAccount>` 的 `leverage_config` 始终为 `None`，
/// 编译期消除"无杠杆账户误融资"（commit 10695d7 修过运行期版本）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeverageConfig {
    /// 杠杆倍数（> 1.0 才生效）。
    pub multiplier: f64,
    /// 杠杆模式：'fixed' 固定倍数 / 'vol_target' 波动率目标动态杠杆。
    pub mode: String,
    /// 维持担保比例平仓线（如 1.3）。
    pub liquidation_threshold: f64,
    /// 维持担保比例警戒线（如 1.5）。
    pub warning_threshold: f64,
}

impl Account<CashAccount> {
    pub fn new_cash(account_id: impl Into<String>, strategy_version_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            strategy_version_id: strategy_version_id.into(),
            nav: Decimal::ZERO,
            leverage_config: None, // 现金账户无杠杆
            _policy: std::marker::PhantomData,
        }
    }

    /// 现金账户升级为保证金账户（允许开通杠杆）。
    pub fn enable_margin(self, config: LeverageConfig) -> Account<MarginAccount> {
        Account {
            account_id: self.account_id,
            strategy_version_id: self.strategy_version_id,
            nav: self.nav,
            leverage_config: Some(config),
            _policy: std::marker::PhantomData,
        }
    }
}

impl Account<MarginAccount> {
    pub fn new_margin(
        account_id: impl Into<String>,
        strategy_version_id: impl Into<String>,
        config: LeverageConfig,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            strategy_version_id: strategy_version_id.into(),
            nav: Decimal::ZERO,
            leverage_config: Some(config),
            _policy: std::marker::PhantomData,
        }
    }

    /// 杠杆配置（编译期保证只有 MarginAccount 可调用）。
    pub fn leverage_config(&self) -> &LeverageConfig {
        self.leverage_config.as_ref().expect("MarginAccount 必有 leverage_config")
    }
}

// ─── Step 5d 接入：load_account 工厂 ──────────────────────────────

/// 从 DB 加载的账号类型（运行时分派 CashAccount / MarginAccount）。
///
/// `leverage_enabled` 是 paper_account 表的运行时值，无法用编译期类型直接覆盖，
/// 故用枚举分派：调用方 match 后调 `rebalance_cash_account` / `rebalance_margin_account`。
pub enum LoadedAccount {
    Cash(Account<CashAccount>),
    Margin(Account<MarginAccount>),
}

impl LoadedAccount {
    /// 账号 ID（无论 Cash/Margin 都有）。
    pub fn account_id(&self) -> &str {
        match self {
            LoadedAccount::Cash(a) => &a.account_id,
            LoadedAccount::Margin(a) => &a.account_id,
        }
    }

    /// 策略版本 ID（无论 Cash/Margin 都有）。
    pub fn strategy_version_id(&self) -> &str {
        match self {
            LoadedAccount::Cash(a) => &a.strategy_version_id,
            LoadedAccount::Margin(a) => &a.strategy_version_id,
        }
    }
}

/// 从 paper_account 表加载账号，按 leverage_enabled 分派为 Cash/Margin。
///
/// 类型门禁：leverage_enabled=false → CashAccount（编译期无杠杆路径），
/// leverage_enabled=true → MarginAccount（携带 LeverageConfig）。
/// 调用方 match LoadedAccount 后调 rebalance_cash_account / rebalance_margin_account。
pub async fn load_account(
    db: &sqlx::PgPool,
    account_id: &str,
) -> Result<LoadedAccount, String> {
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT leverage_enabled, leverage_multiplier, leverage_mode,
                COALESCE(liquidation_threshold, 1.3), COALESCE(warning_threshold, 1.5),
                strategy_version_id
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("load_account {}: {}", account_id, e))?
    .ok_or_else(|| format!("账号不存在: {}", account_id))?;

    let leverage_enabled: bool = row.get("leverage_enabled");
    let strategy_version_id: String = row.get::<Option<String>, _>("strategy_version_id")
        .unwrap_or_default();

    if !leverage_enabled {
        Ok(LoadedAccount::Cash(Account::<CashAccount>::new_cash(
            account_id,
            strategy_version_id,
        )))
    } else {
        let config = LeverageConfig {
            multiplier: row.get::<f64, _>("leverage_multiplier"),
            mode: row.get::<Option<String>, _>("leverage_mode").unwrap_or_else(|| "fixed".into()),
            liquidation_threshold: row.get("liquidation_threshold"),
            warning_threshold: row.get("warning_threshold"),
        };
        Ok(LoadedAccount::Margin(Account::<MarginAccount>::new_margin(
            account_id,
            strategy_version_id,
            config,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_leverage_config() -> LeverageConfig {
        LeverageConfig {
            multiplier: 2.0,
            mode: "fixed".into(),
            liquidation_threshold: 1.3,
            warning_threshold: 1.5,
        }
    }

    #[test]
    fn cash_account_has_no_leverage_config() {
        let cash = Account::<CashAccount>::new_cash("acc_v24", "h20_v1");
        assert!(cash.leverage_config.is_none(), "现金账户无杠杆配置");
    }

    #[test]
    fn cash_account_can_become_margin() {
        let cash = Account::<CashAccount>::new_cash("acc_v24", "h20_v1");
        let margin: Account<MarginAccount> = cash.enable_margin(sample_leverage_config());
        assert_eq!(margin.account_id, "acc_v24");
        assert_eq!(margin.leverage_config().multiplier, 2.0);
    }

    #[test]
    fn margin_account_holds_leverage_config() {
        let margin = Account::<MarginAccount>::new_margin("acc_v24", "h20_v1", sample_leverage_config());
        // 编译期门禁：leverage_config() 仅 MarginAccount 可调用
        assert_eq!(margin.leverage_config().mode, "fixed");
        assert_eq!(margin.leverage_config().liquidation_threshold, 1.3);
    }
}
