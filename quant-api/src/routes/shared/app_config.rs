//! 系统级 KV 配置读取（任务80 C4/C5/C6 落点）。
//!
//! `app_config` 表承载执行器/渠道成本口径与系统缺省线。优先级约定：
//! **调用方显式参数 > 账户级列（如 paper_account 维保线）> 本表 > 代码兜底默认**。
//! 读失败/键缺失/值非法一律返回 None——由调用方决定兜底值（本模块不猜默认），
//! 与"宁可报错"纪律一致：配置错误在调用点显式暴露而非静默替换。

use sqlx::PgPool;

/// 读 f64 型配置。键缺失/解析失败返回 None（调用方兜底）。
pub async fn app_config_f64(db: &PgPool, key: &str) -> Option<f64> {
    let v: Option<String> =
        sqlx::query_scalar("SELECT config_value FROM app_config WHERE config_key = $1")
            .bind(key)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    v.and_then(|s| s.trim().parse::<f64>().ok())
}

/// 任务80 C4: 回测费率默认从 app_config 读（当前渠道成本口径，backtest.* 八键）。
/// 键缺失/脏值逐项回落 FeeConfig::default()（回测成本有代码默认兜底）。
pub async fn backtest_fee_base(db: &PgPool) -> quant_backtest::portfolio::FeeConfig {
    use quant_backtest::portfolio::FeeConfig;
    use rust_decimal::prelude::FromPrimitive;
    let base = FeeConfig::default();
    fn dec_of(base_v: rust_decimal::Decimal, cfg: Option<f64>) -> rust_decimal::Decimal {
        cfg.and_then(rust_decimal::Decimal::from_f64)
            .unwrap_or(base_v)
    }
    FeeConfig {
        commission_rate: dec_of(
            base.commission_rate,
            app_config_f64(db, "backtest.commission_rate").await,
        ),
        min_commission: dec_of(
            base.min_commission,
            app_config_f64(db, "backtest.min_commission").await,
        ),
        tax_rate: dec_of(base.tax_rate, app_config_f64(db, "backtest.tax_rate").await),
        slippage_bps: dec_of(
            base.slippage_bps,
            app_config_f64(db, "backtest.slippage_bps").await,
        ),
        cost_multiplier: dec_of(
            base.cost_multiplier,
            app_config_f64(db, "backtest.cost_multiplier").await,
        ),
        impact_cost_coefficient: dec_of(
            base.impact_cost_coefficient,
            app_config_f64(db, "backtest.impact_cost_coefficient").await,
        ),
        impact_cost_exponent: dec_of(
            base.impact_cost_exponent,
            app_config_f64(db, "backtest.impact_cost_exponent").await,
        ),
        transfer_fee_rate: dec_of(
            base.transfer_fee_rate,
            app_config_f64(db, "backtest.transfer_fee_rate").await,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    #[tokio::test]
    async fn app_config_f64_reads_production_keys() {
        let db = test_db().await;
        // 生产已初始化的键（migration 20260923000002）
        let v = app_config_f64(&db, "margin.default_liquidation_threshold")
            .await
            .expect("维保缺省键应存在");
        assert!((v - 1.3).abs() < 1e-9);
    }

    #[tokio::test]
    async fn app_config_missing_key_returns_none() {
        let db = test_db().await;
        assert!(app_config_f64(&db, "zzz_test_no_such_key").await.is_none());
    }
}
