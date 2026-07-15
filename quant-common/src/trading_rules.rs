//! A股交易规则公共工具。
//!
//! 回测引擎与实盘调仓共享,确保符合交易所规则:
//! - 最小交易单位:A股 1手=100股、ETF 100份(买入必须100整数倍,卖出可零头清仓)
//! - 涨跌幅:主板±10%、创业板/科创板±20%、ST±5%
//! - 涨停封板禁买、跌停封板禁卖、T+1(当日买入次日才能卖)
//!
//! 本模块只提供「取整」与「品种识别」的纯函数;
//! 涨跌停方向、T+1 等带状态校验由各调用方(回测 portfolio/engine、实盘 rebalance)实现。

use rust_decimal::Decimal;

/// A股/ETF 最小交易单位(1手=100股/份)。
pub const LOT_SIZE: u32 = 100;

/// 按最小交易单位向下取整。
///
/// 用于**买入**:买入量必须是 100 的整数倍。
/// 卖出不应调用本函数 —— A股允许卖出零头(不足100股部分)一次性清仓,
/// 强制取整会破坏零头清仓。
///
/// `qty <= 0` 或取整后为 0 时返回 0(不足1手不买)。
pub fn round_down_to_lot(qty: Decimal, lot: u32) -> Decimal {
    if qty <= Decimal::ZERO || lot == 0 {
        return Decimal::ZERO;
    }
    let lot_d = Decimal::from(lot);
    // (qty / lot).floor() * lot —— 向下取整到 lot 的整数倍
    (qty / lot_d).floor() * lot_d
}

/// 判断 symbol 是否为 ETF/LOF(基金)。
///
/// A股 ETF/LOF 代码段:
/// - 沪市:510xxx(ETF)、511xxx(货基/债基)、512xxx(ETF)、513xxx(QDII)、515xxx(ETF)、516xxx(ETF)、517xxx(ETF)、518xxx(黄金ETF)、561xxx/562xxx/563xxx(ETF)
/// - 深市:159xxx(ETF/LOF)
///
/// 注意:本函数仅按代码段启发式判断(回测快速识别用)。
/// 实盘权威判断应查 market_stock.instrument_type 字段(模块5 后),symbol 前缀为兜底。
pub fn is_etf_symbol(symbol: &str) -> bool {
    let code = symbol.split('.').next().unwrap_or("");
    if code.len() != 6 {
        return false;
    }
    let prefix2 = &code[..2];
    let prefix3 = &code[..3];
    // 沪市 5 开头:51x/56x 系列 ETF/LOF/货基/QDII/黄金
    prefix2 == "51" || prefix3 == "561" || prefix3 == "562" || prefix3 == "563"
    // 深市 159xxx ETF/LOF
    || prefix3 == "159"
}

/// 涨跌幅限制(小数)。主板 10%、创业板/科创板 20%、ST 5%。
///
/// - 创业板(300xxx)/科创板(688xxx):20%
/// - ST(*ST 前缀,由调用方判定 name):5% —— 本函数不查 name,ST 由调用方传入 is_st
/// - 其余主板:10%
/// - ETF:默认 10%(跨境 QDII 513xxx/159xxx 部分 20%,货基 511880 无限制,
///   精细化在 runner.rs limit_rate_for 已实现,本函数给保守默认 10%)
pub fn limit_rate(symbol: &str, is_st: bool) -> Decimal {
    let code = symbol.split('.').next().unwrap_or("");
    if is_st {
        return Decimal::new(5, 2); // 0.05
    }
    if code.len() == 6 {
        let prefix3 = &code[..3];
        // 创业板 300/301、科创板 688/689
        if prefix3 == "300" || prefix3 == "301" || prefix3 == "688" || prefix3 == "689" {
            return Decimal::new(20, 2); // 0.20
        }
    }
    Decimal::new(10, 2) // 0.10
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(n: i64, scale: u32) -> Decimal {
        Decimal::new(n, scale)
    }

    #[test]
    fn test_round_down_to_lot() {
        assert_eq!(round_down_to_lot(d(1480, 1), 100), d(100, 0)); // 148.0 → 100
        assert_eq!(round_down_to_lot(d(3410, 1), 100), d(300, 0)); // 341.0 → 300
        assert_eq!(round_down_to_lot(d(100, 0), 100), d(100, 0)); // 100 → 100
        assert_eq!(round_down_to_lot(d(99, 0), 100), d(0, 0)); // 不足1手
        assert_eq!(round_down_to_lot(d(5, 1), 100), d(0, 0)); // 0.5 → 0
        assert_eq!(round_down_to_lot(d(-10, 0), 100), d(0, 0)); // 负数 → 0
        // 小数持仓(如除权后 1.48 股)向下取整到 0
        assert_eq!(round_down_to_lot(d(148, 2), 100), d(0, 0)); // 1.48 → 0
        assert_eq!(round_down_to_lot(d(10148, 2), 100), d(100, 0)); // 101.48 → 100
    }

    #[test]
    fn test_is_etf_symbol() {
        assert!(is_etf_symbol("510300.SH")); // 沪深300ETF
        assert!(is_etf_symbol("511880.SH")); // 银华日利货基
        assert!(is_etf_symbol("513100.SH")); // QDII
        assert!(is_etf_symbol("518880.SH")); // 黄金ETF
        assert!(is_etf_symbol("159915.SZ")); // 创业板ETF
        assert!(is_etf_symbol("561360.SH")); // ETF
        assert!(!is_etf_symbol("600519.SH")); // 茅台股票
        assert!(!is_etf_symbol("000001.SZ")); // 平安银行
        assert!(!is_etf_symbol("300750.SZ")); // 宁德时代
    }

    #[test]
    fn test_limit_rate() {
        assert_eq!(limit_rate("600519.SH", false), d(10, 2)); // 主板 0.10
        assert_eq!(limit_rate("000001.SZ", false), d(10, 2)); // 主板
        assert_eq!(limit_rate("300750.SZ", false), d(20, 2)); // 创业板 0.20
        assert_eq!(limit_rate("301236.SZ", false), d(20, 2)); // 创业板
        assert_eq!(limit_rate("688981.SH", false), d(20, 2)); // 科创板
        assert_eq!(limit_rate("600519.SH", true), d(5, 2)); // ST 0.05
    }
}
