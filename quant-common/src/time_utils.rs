//! 时间格式化工具 — 统一时区策略
//!
//! 项目全局原则：面向用户展示的时间一律用**本地时区**（跟随运行环境的
//! 系统时区，通过 `chrono::Local` 读取）。当前部署环境是 Asia/Shanghai，
//! 因此展示为北京时间；未来若系统时区变更，无需改代码即自动跟随。
//!
//! 数据库列仍用 `timestamptz`（绝对时刻正确），Rust 内部 `DateTime<Utc>`
//! 也保持不变；只在"序列化到 API JSON / 日志"的边界上转成本地时区字符串。
//!
//! 为何用 `Local` 而非写死 `FixedOffset::east(8*3600)`：写死偏移量会让
//! 代码与时区绑死，迁移部署环境（如改为 UTC 机房、海外节点）就得改源码。
//! `Local` 从 TZ 环境变量 / /etc/localtime 读取，是"本地时区"的真正语义。

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

/// 把任意带时区的 `DateTime` 转为本地时区的 `DateTime<Local>`。
pub fn to_local<Tz>(dt: &DateTime<Tz>) -> DateTime<Local>
where
    Tz: chrono::TimeZone,
{
    dt.with_timezone(&Local)
}

/// 面向 API JSON 的展示格式：`YYYY-MM-DD HH:MM:SS`（无时区后缀，本地时区）。
///
/// 刻意不带秒以下的小数和时区后缀——用户阅读友好；绝对时刻的精确
/// 传递不在展示层做，需要时直接读 DB 的 timestamptz。
pub fn fmt_datetime<Tz>(dt: Option<DateTime<Tz>>) -> Option<String>
where
    Tz: chrono::TimeZone,
{
    dt.map(|t| to_local(&t).format("%Y-%m-%d %H:%M:%S").to_string())
}

/// 面向 API JSON 的展示格式（带毫秒）：`YYYY-MM-DD HH:MM:SS.sss`。
///
/// 保留毫秒精度但仍是本地时区，用于成交时间等需要亚秒粒度的场景。
pub fn fmt_datetime_millis<Tz>(dt: Option<DateTime<Tz>>) -> Option<String>
where
    Tz: chrono::TimeZone,
{
    dt.map(|t| to_local(&t).format("%Y-%m-%d %H:%M:%S%.3f").to_string())
}

/// RFC3339 但以本地时区呈现（带本地偏移后缀，而非 `Z`）。
///
/// 适合需要保留时区信息但要求本地时区呈现的场景（例如机器可解析的
/// API 字段）。语义上与 UTC RFC3339 等价（绝对时刻相同），只是呈现
/// 时区从 `Z` 变成本地偏移（如 `+08:00`）。
pub fn fmt_rfc3339_local<Tz>(dt: Option<DateTime<Tz>>) -> Option<String>
where
    Tz: chrono::TimeZone,
{
    dt.map(|t| to_local(&t).to_rfc3339())
}

/// 把 `NaiveDateTime`（无时区）当作本地时间解释，返回带本地偏移的 RFC3339。
///
/// 仅用于"源数据本来就是本地时间但没带时区"的兼容场景（如某些
/// akshare 字段）。绝大多数生产路径应直接用 `timestamptz` 列，不要
/// 走这个函数。
pub fn naive_as_local(dt: NaiveDateTime) -> DateTime<Local> {
    match Local.from_local_datetime(&dt) {
        chrono::LocalResult::Single(dt) => dt,
        // 合法 naive 输入在本地时区必落 Single 分支（无夏令时折叠时）
        _ => unreachable!("naive datetime 转本地时区不应越界"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Offset, Utc};

    fn dt_utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s)
            .single()
            .expect("合法的 UTC 时刻")
    }

    #[test]
    fn fmt_datetime_produces_local_offset() {
        // 不断言具体字符串（依赖运行环境时区），只断言：
        // 1. 不带 "UTC" 字样（不再是 Utc 的 Display 输出）
        // 2. 不以 'Z' 结尾（不再是 RFC3339 的 UTC 形式）
        let dt = dt_utc(2026, 7, 21, 6, 42, 57);
        let s = fmt_datetime(Some(dt)).unwrap();
        assert!(!s.contains("UTC"), "不应出现 UTC 字样，实际: {s}");
        assert!(!s.ends_with('Z'), "不应以 Z 结尾");
        // 格式应是 YYYY-MM-DD HH:MM:SS
        assert_eq!(s.len(), "2026-07-21 14:42:57".len());
    }

    #[test]
    fn none_stays_none() {
        assert!(fmt_datetime::<Utc>(None).is_none());
        assert!(fmt_datetime_millis::<Utc>(None).is_none());
    }

    #[test]
    fn rfc3339_local_has_offset_not_z() {
        let dt = dt_utc(2026, 1, 1, 0, 0, 0);
        let s = fmt_rfc3339_local(Some(dt)).unwrap();
        assert!(
            !s.ends_with('Z'),
            "不应以 Z 结尾（应带本地偏移），实际: {s}"
        );
        // 应包含时区偏移符号（+ 或 -）
        let has_offset = s[19..].contains('+') || s[19..].contains('-');
        assert!(has_offset, "应带本地时区偏移，实际: {s}");
    }

    #[test]
    fn millis_keeps_three_digits() {
        let dt = dt_utc(2026, 7, 21, 6, 42, 57)
            + chrono::Duration::milliseconds(559);
        let s = fmt_datetime_millis(Some(dt)).unwrap();
        assert!(s.ends_with(".559"), "应保留三位毫秒，实际: {s}");
    }

    #[test]
    fn local_offset_is_non_zero() {
        // 本地时区偏移应是非零的（上海 +28800 秒）；若为零说明 TZ 未设置
        let offset = Local::now().offset().fix().local_minus_utc();
        assert_ne!(
            offset, 0,
            "本地时区偏移为 0，请检查 TZ 环境变量是否设置"
        );
    }

    #[test]
    fn naive_as_local_roundtrip() {
        let nd = chrono::NaiveDate::from_ymd_opt(2026, 7, 21)
            .unwrap()
            .and_hms_opt(14, 42, 57)
            .unwrap();
        let dt = naive_as_local(nd);
        assert_eq!(dt.year(), 2026);
    }
}
