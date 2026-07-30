//! BC4 策略发现 / strategy_discovery 领域内核：phase7 搜索规划与 alpha 准入。
//!
//! 由 quant-common 共享内核迁入（DDD 重构 Step 1），仅 quant-api 引用。
//! R8 批次1 重命名 phase7 -> strategy_discovery，过渡法拆出 3 个叶子子模块：
//! - profiles：搜索空间配置类型层
//! - alpha_admission：alpha 源准入分级
//! - candidate_screening：候选筛选门禁
//! mod.rs 主体保留 LayeredSearchConfig / seeds / tests（后续批次拆分）。

pub mod strategy_discovery;
