//! 标的元数据:symbol → 中文名/asset_class 共享映射。
//! 消除 accounts.rs classify_asset 和 scheduler.rs ETF 日志中文名的重复硬编码。
//!
//! 两个函数语义不同,各自保持独立:
//! - [`classify_asset`]:持仓资产分类标签(带 ETF/LOF 后缀,钉钉推送/UI 用)
//! - [`etf_display_name`]:ETF/LOF 日志简称(无后缀,跌破 MA200 告警用)

/// 持仓资产分类(钉钉推送/UI 展示用),返回带 ETF/LOF 后缀的分类标签。
pub fn classify_asset(symbol: &str) -> String {
    match symbol {
        "511010.SH" | "511260.SH" => "国债ETF".into(),
        "518880.SH" => "黄金ETF".into(),
        "513100.SH" => "纳指ETF".into(),
        "513500.SH" => "标普ETF".into(),
        "501018.SH" => "原油LOF".into(),
        "159980.SZ" => "商品ETF".into(),
        "159985.SZ" => "商品ETF".into(),
        s if s.ends_with(".SH") || s.ends_with(".SZ") => "A股".into(),
        _ => "其他".into(),
    }
}

/// ETF/LOF 的中文展示名(日志简称,跌破 MA200 告警用)。
///
/// 注意:与 [`classify_asset`] 的分类标签不同,这里返回无后缀的简称
/// (如 "黄金" 而非 "黄金ETF"),且 `513500.SH` 返回 "SP500"(非"标普"),
/// `511260.SH` 未列出(走回退返回原 symbol)。
pub fn etf_display_name(sym: &str) -> String {
    match sym {
        "518880.SH" => "黄金".into(),
        "511010.SH" => "国债".into(),
        "513500.SH" => "SP500".into(),
        "513100.SH" => "纳指".into(),
        "159980.SZ" => "有色".into(),
        "159985.SZ" => "豆粕".into(),
        "501018.SH" => "原油".into(),
        _ => sym.into(),
    }
}
