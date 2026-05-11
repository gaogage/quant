//! Quant Factor Framework — Phase 3
//!
//! Provides:
//! - Factor definition and computation (price-volume, fundamental, etc.)
//! - Cross-sectional standardization (z-score, rank, winsorized)
//! - Factor evaluation (IC, RankIC, quantile returns)
//! - Factor registry for managing factor versions

pub mod types;
pub mod standardize;
pub mod evaluate;
pub mod combine;
pub mod batch;
pub mod factors {
    pub mod price_volume;
}

// Re-export key types for convenience
pub use types::*;
pub use standardize::standardize;
pub use evaluate::evaluate;
pub use combine::{combine_and_persist, compute_weights, CombineMethod, FactorWeight};
