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
//!
//! 状态：模块骨架已建，迁移待实施（Step 6b 第二阶段）。
//! 当前循环依赖不阻塞编译（Rust 允许模块间互 use），6b 是架构清洁非 bug 修复。
