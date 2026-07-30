//! BC4 策略发现 / strategy_discovery 领域内核：phase7 搜索规划与 alpha 准入。
//!
//! 由 quant-common 共享内核迁入（DDD 重构 Step 1），仅 quant-api 引用。
//! R8 批次1 重命名 phase7 -> strategy_discovery，过渡法拆出叶子子模块：
//! - profiles：搜索空间配置类型层（11 类型 + ScoreDirection）
//! - alpha_admission：alpha 源准入分级（4 函数）
//! - candidate_screening：候选筛选门禁
//! - seed_generators：种子生成器（73+ with_*_seed builder + 7 公共 helper）
//! - search_space：搜索空间规格（LayeredSearchConfig + 种子 + 计划）— 批次3a 拆出
//!
//! mod.rs 主体保留 tests（后续批次处理）。

mod profiles;
mod alpha_admission;
mod candidate_screening;
mod seed_generators;
mod search_space;

#[cfg(test)]
mod tests;

pub use profiles::{
    LocalResourcePlan, ComboVersion, Phase7AlphaSourceRole, Phase7AlphaSourceAdmission,
    AlphaBlendSource, AlphaBlendProfile, PortfolioDrawdownControlProfile,
    PortfolioVolatilityControlProfile, PortfolioSharpeControlProfile,
    PositionRiskControlProfile, CostCapacityStressProfile, EventGateProfile, ScoreDirection,
};
pub use alpha_admission::{
    phase7_alpha_source_admission, is_phase7_base_trainable_alpha,
    phase7_alpha_blend_profiles,
};
pub use candidate_screening::{
    CandidateTargets, CandidateType, CandidateMetrics, CandidateScreeningRow,
    screen_optimization_results,
};
pub use search_space::{
    LayeredSearchConfig, LayeredSearchPlan, LayeredSearchTrial, build_layered_search_plan,
};

// search_space.rs 的 helper 被 seed_generators 跨模块调用（use super::{...}）。
// decimal_f64 / insert_execution_rule_value 本批迁至 search_space（pub(crate)），
// 此处 pub(crate) use 重导出保持原可见性；decimal_string 来自 candidate_screening
// （pub(super)），私有 use 引入即可被子模块 super:: 解析。
pub(crate) use search_space::{decimal_f64, insert_execution_rule_value};
use candidate_screening::decimal_string;


