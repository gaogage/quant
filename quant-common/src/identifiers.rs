//! 领域标识符 newtype（DDD 重构 Step 2 引入）。
//!
//! 目的：用类型门禁替代散落的 `String` / `&str`，防止把 symbol 当成 date、
//! 把 data_version_id 当成 strategy_id 这类跨域误用。
//!
//! 设计原则（本步只引入骨架，不改现有调用方）：
//! - newtype 包装原始类型，零运行时开销（#[repr(transparent)]）。
//! - 提供 `From`/`AsRef` 双向转换，便于与现有 `String` 代码互操作。
//! - 不含校验逻辑（校验留到 Step 5 类型状态模式，届时 newtype 构造改为 `try_new`）。
//!
//! 迁移策略：本步新建此模块并 pub，现有 `pub type DataVersionId = String` 等 alias
//! 保留在 lib.rs 作为过渡，避免一次性破坏全项目调用方。后续 Step 4/5 逐步把调用方
//! 切换到 newtype 后，再删除旧 alias。

use serde::{Deserialize, Serialize};

/// 证券代码（A 股 6 位数字，如 "000001"）。
///
/// 类型门禁：防止与 `TradeDate` 等 String 域标识符混用。
/// 当前不校验格式（Step 5 引入 `Symbol::try_new` 做格式校验）。
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
#[repr(transparent)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        value.0
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// 交易日（YYYY-MM-DD，交易日历口径，非自然日）。
///
/// 类型门禁：与 `NaiveDate` 区分——`TradeDate` 强调"交易日"语义，
/// 回测/调仓只在交易日推进。当前不校验是否真为交易日（Step 5 引入）。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
pub struct TradeDate(pub chrono::NaiveDate);

impl TradeDate {
    pub fn new(date: chrono::NaiveDate) -> Self {
        Self(date)
    }

    pub fn as_naive_date(&self) -> chrono::NaiveDate {
        self.0
    }
}

impl From<chrono::NaiveDate> for TradeDate {
    fn from(date: chrono::NaiveDate) -> Self {
        Self(date)
    }
}

impl From<TradeDate> for chrono::NaiveDate {
    fn from(value: TradeDate) -> Self {
        value.0
    }
}

impl AsRef<chrono::NaiveDate> for TradeDate {
    fn as_ref(&self) -> &chrono::NaiveDate {
        &self.0
    }
}

/// 数据版本 ID（标识数据快照版本，PIT 语义的核心）。
///
/// 类型门禁：当前全项目 `data_version_id` 散落为 `String`，易与 `strategy_version_id`
/// 混用。本 newtype 后续 Step 4 集中化时切换调用方。
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
#[repr(transparent)]
pub struct DataVersionId(pub String);

impl DataVersionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for DataVersionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for DataVersionId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<DataVersionId> for String {
    fn from(value: DataVersionId) -> Self {
        value.0
    }
}

impl AsRef<str> for DataVersionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DataVersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_round_trips_with_string() {
        let s = Symbol::new("000001");
        assert_eq!(s.as_str(), "000001");
        let raw: String = s.into();
        assert_eq!(raw, "000001");
        let s2 = Symbol::from("600000");
        assert_eq!(s2.as_ref(), "600000");
    }

    #[test]
    fn trade_date_wraps_naive_date() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        let td = TradeDate::new(d);
        assert_eq!(td.as_naive_date(), d);
        let back: chrono::NaiveDate = td.into();
        assert_eq!(back, d);
    }

    #[test]
    fn data_version_id_round_trips() {
        let v = DataVersionId::new("dv_20260724_v1");
        assert_eq!(v.as_str(), "dv_20260724_v1");
        assert_eq!(v.to_string(), "dv_20260724_v1");
    }

    #[test]
    fn newtypes_are_distinct_at_type_level() {
        // 编译期门禁：这行若取消注释会编译失败（Symbol != DataVersionId）：
        // let _: DataVersionId = Symbol::new("x").into();
        let s = Symbol::new("000001");
        let v = DataVersionId::new("000001");
        // 同名原始值，但不同类型——类型系统阻止误用。
        assert_eq!(s.as_str(), v.as_str());
    }
}
