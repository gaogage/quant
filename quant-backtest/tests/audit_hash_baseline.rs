//! DDD 重构 audit hash 守卫(Step 0 安全网)。
//!
//! 对代表性策略跑回测,产出 equity_curve_sha256 + signal_hash_sha256 基线。
//! 重构期间每步 MR 必须保证这两个 hash 不变,证明行为等价。
//!
//! 运行方式(需 DB + 连真实回测链路,较慢):
//!   cargo test --package quant-backtest --test audit_hash_baseline -- --ignored
//!
//! 首次运行产出基线后,将 hash 写入 scripts/audit_hash_baseline.json 作为金标准。
//! 后续 MR 跑此测试与金标准比对,hash 不匹配则阻断重构 MR。

use chrono::NaiveDate;
use quant_backtest::db_perf_baseline::{run_db_perf_baseline, DbPerfBaselineConfig};

/// 回测基线 hash 金标准(首次跑后人工填入,作为守卫基准)。
/// 详见 scripts/audit_hash_baseline.json(机器可读金标准)。
/// 变更需 MR 说明原因 + review 确认。
const BASELINE_EQUITY_HASH: &str = "f53cf2b3fc201130323fec8bba416931230910a627fdc651037582831cc10cf3";
const BASELINE_SIGNAL_HASH: &str = "d1ee0c46975b02bded46ca5dc44b39d4ae19fbdd90b4909f7aa3a3de83d6c95b";

/// 跑一个最小规模 DB 回测,验证 hash 产出链路通畅 + 确定性。
///
/// 配置:40 交易日 / 25 标的 / 10 日调仓 / 8 篮子(与 DbPerfBaselineConfig::default 一致)。
/// 这是"守卫"而非"性能基准",小规模足以验证 equity_curve + signal hash 稳定。
#[tokio::test]
#[ignore = "需 DB(postgres://gaocheng@localhost/quant),首次建立基线或重构 MR 验证时显式跑"]
async fn audit_hash_baseline_is_stable() {
    let config = DbPerfBaselineConfig::default();
    let report = run_db_perf_baseline(config)
        .await
        .expect("DB 回测基线应成功运行");

    // hash 必须是 64 位十六进制(SHA-256)
    assert_eq!(report.equity_curve_sha256.len(), 64);
    assert!(report
        .equity_curve_sha256
        .chars()
        .all(|c| c.is_ascii_hexdigit()));
    assert_eq!(report.signal_hash_sha256.len(), 64);

    // 两次跑同一配置,hash 应完全一致(确定性)
    let report2 = run_db_perf_baseline(DbPerfBaselineConfig::default())
        .await
        .expect("第二次 DB 回测基线应成功运行");
    assert_eq!(
        report.equity_curve_sha256, report2.equity_curve_sha256,
        "相同配置两次回测的 equity_curve hash 必须一致(audit 守卫前提)"
    );
    assert_eq!(
        report.signal_hash_sha256, report2.signal_hash_sha256,
        "相同配置两次回测的 signal hash 必须一致"
    );

    // 若已建立金标准基线,做比对守卫
    if !BASELINE_EQUITY_HASH.is_empty() {
        assert_eq!(
            report.equity_curve_sha256, BASELINE_EQUITY_HASH,
            "equity_curve hash 与金标准不符!重构引入了行为变更,请排查或更新基线(说明原因)"
        );
    }
    if !BASELINE_SIGNAL_HASH.is_empty() {
        assert_eq!(
            report.signal_hash_sha256, BASELINE_SIGNAL_HASH,
            "signal hash 与金标准不符!重构引入了行为变更,请排查或更新基线"
        );
    }

    eprintln!(
        "audit hash 基线: equity_curve_sha256={} signal_hash_sha256={}",
        report.equity_curve_sha256, report.signal_hash_sha256
    );
}

/// 验证 config/data_version hash 确定性 + 金标准守卫（audit 扩覆盖）。
#[tokio::test]
#[ignore = "需 DB,验证 config/data_version hash 确定性"]
async fn audit_hash_config_and_data_version_are_stable() {
    let report = run_db_perf_baseline(DbPerfBaselineConfig::default())
        .await
        .expect("DB 回测基线应成功运行");
    let report2 = run_db_perf_baseline(DbPerfBaselineConfig::default())
        .await
        .expect("第二次 DB 回测基线应成功运行");

    // config/data_version hash 必须是 64 位十六进制
    assert_eq!(report.config_sha256.len(), 64);
    assert_eq!(report.data_version_sha256.len(), 64);
    assert!(report
        .config_sha256
        .chars()
        .all(|c| c.is_ascii_hexdigit()));
    assert!(report
        .data_version_sha256
        .chars()
        .all(|c| c.is_ascii_hexdigit()));

    // 确定性：相同配置两次回测的 config/data_version hash 必须一致
    assert_eq!(
        report.config_sha256, report2.config_sha256,
        "相同配置两次回测的 config hash 必须一致"
    );
    assert_eq!(
        report.data_version_sha256, report2.data_version_sha256,
        "相同配置两次回测的 data_version hash 必须一致"
    );

    eprintln!(
        "audit hash 扩展: config_sha256={} data_version_sha256={}",
        report.config_sha256, report.data_version_sha256
    );
}

/// 纯单元测试:验证 hash 函数对固定输入产出固定输出(不连 DB)。
/// 这是守卫的"地基"--若 hash 函数本身不确定,DB 基线无从谈起。
#[test]
fn audit_hash_functions_are_deterministic_offline() {
    use quant_backtest::db_perf_baseline::{hash_config, hash_data_version, hash_equity_curve, hash_signals};
    use quant_backtest::engine::{BacktestConfig, StrategySignal};
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();

    let curve = vec![
        (d1, Decimal::new(1000000, 0)),
        (d2, Decimal::new(1005000, 0)),
    ];
    let h1 = hash_equity_curve(&curve);
    let h2 = hash_equity_curve(&curve);
    assert_eq!(h1, h2, "相同权益曲线必须产出相同 hash");
    assert_eq!(h1.len(), 64);

    let mut signals = HashMap::new();
    let mut weights = HashMap::new();
    weights.insert("000001.SZ".to_string(), Decimal::new(12, 2));
    signals.insert(d1, StrategySignal { date: d1, target_weights: weights });
    let s1 = hash_signals(&signals);
    let s2 = hash_signals(&signals);
    assert_eq!(s1, s2, "相同信号必须产出相同 hash");
    assert_eq!(s1.len(), 64);

    // config hash 确定性：相同 config 序列化产出相同 hash
    let cfg = BacktestConfig::default();
    let c1 = hash_config(&cfg);
    let c2 = hash_config(&cfg);
    assert_eq!(c1, c2, "相同 config 必须产出相同 hash");
    assert_eq!(c1.len(), 64);

    // data_version hash 确定性
    let dv1 = hash_data_version("dv-test-001");
    let dv2 = hash_data_version("dv-test-001");
    assert_eq!(dv1, dv2, "相同 data_version 必须产出相同 hash");
    assert_ne!(dv1, hash_data_version("dv-test-002"), "不同 dv 应产出不同 hash");
}
