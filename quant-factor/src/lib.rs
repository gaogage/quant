//! Quant Factor Framework — Phase 3
//!
//! Provides:
//! - Factor definition and computation (price-volume, fundamental, etc.)
//! - Cross-sectional standardization (z-score, rank, winsorized)
//! - Factor evaluation (IC, RankIC, quantile returns)
//! - Factor registry for managing factor versions

pub mod batch;
pub mod combine;
pub mod evaluate;
pub mod neutralize;
pub mod repository;
pub mod standardize;
pub mod types;
pub mod factors {
    pub mod price_volume;
}

// Re-export key types for convenience
// (2026-09-18 分层: 权重算法在 combine(领域), DB 编排在 repository(仓储),
//  顶层 re-export 名不变——调用方零改动)
pub use combine::{CombineMethod, FactorWeight};
pub use repository::{combine_and_persist, compute_weights, compute_weights_pit};
pub use evaluate::evaluate;
pub use standardize::standardize;
pub use types::*;
