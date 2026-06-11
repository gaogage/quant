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
pub mod standardize;
pub mod types;
pub mod factors {
    pub mod price_volume;
}

// Re-export key types for convenience
pub use combine::{
    combine_and_persist, compute_weights, compute_weights_pit, CombineMethod, FactorWeight,
};
pub use evaluate::evaluate;
pub use standardize::standardize;
pub use types::*;
