//! 共享工具模块（DDD 重构 Step 6b 解 scheduler↔rebalance 循环依赖）。
//!
//! 背景：rebalance.rs `use crate::routes::scheduler::{...}` 导入 8 项工具函数/类型，
//! 而 scheduler.rs 又调 `rebalance::rebalance_account`，形成循环依赖。本模块把
//! rebalance 依赖的共享项迁出 scheduler，让双方都 `use shared::*`，变单向依赖。
//!
//! 迁移项（9 项核心 + 附属私有函数）：
//! - compute_lw_mvo_weights / compute_vol_target_leverage（MVO 权重计算）
//! - detect_regime_exposure / detect_regime_exposure_cached（体制识别）
//! - fetch_intraday_etf_prices（实时 ETF 价格）
//! - resolved_to_legacy_sc（策略配置桥接）
//! - send_quality_alert（质量告警）
//! - preload_trade_block_map（涨跌停预加载）
//! - MvoWeightCache / StrategyConfig（类型）

mod strategy_config;
mod mvo_weights;
mod regime;
mod alerts;
mod trade_block;
mod etf_prices;

// pub(crate) 项用 pub(crate) use re-export（不能 pub use，否则 E0364）
pub(crate) use strategy_config::resolved_to_legacy_sc;
pub use strategy_config::StrategyConfig;
pub(crate) use mvo_weights::{compute_lw_mvo_weights, compute_vol_target_leverage};
pub use mvo_weights::MvoWeightCache;
pub use regime::{detect_regime_exposure, detect_regime_exposure_cached};
pub(crate) use alerts::send_quality_alert;
pub use trade_block::{TradeBlock, preload_trade_block_map};
pub(crate) use etf_prices::fetch_intraday_etf_prices;
