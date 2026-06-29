//! 策略组合嵌套架构 —— 三层策略树的运行时模型与加载。
//!
//! 层级:composite(组合,多资产MVO) → asset(单资产) → security(选股参数,内联 asset)。
//! strategy_config 单表用 strategy_type 区分层级,parent_strategy_id 表父子关系。
//!
//! MVO 是 composite 的固有属性(strategy_type='composite' ⟹ 必有 MVO),非开关字段。

/// 解析后的完整策略树(composite 或单 asset 账号都会解析成此结构)
#[derive(Debug, Clone)]
pub struct ResolvedStrategy {
    pub strategy_id: String,
    pub name: String,
    pub strategy_type: StrategyType,
    pub mvo: Option<MvoParams>,     // 仅 composite 非空;asset 账号 None
    pub assets: Vec<AssetStrategy>, // composite=多个;单 asset 账号=1个
    pub rebalance_freq: String,     // quarterly/monthly/weekly
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrategyType {
    Composite,
    Asset,
}

#[derive(Debug, Clone)]
pub struct MvoParams {
    pub vol_target: f64,
    pub leverage_cap: f64,
    pub leverage_floor: f64,
    pub default_weights: Vec<f64>, // 7维 ETF 权重(MVO 第1-7列)
    pub min_stock: f64,            // A股第0列默认权重(MVO 第0列)
    pub momentum_blend_ratio: f64,
    pub ga_population: usize,
    pub ga_generations: usize,
    pub ga_elite_count: usize,
    pub regime_bull_threshold: f64,
    pub regime_bear_threshold: f64,
    pub regime_bull_min_stock: i64,
    pub regime_bear_min_stock: i64,
    pub dynamic_target_cap: f64,
    pub dynamic_target_floor: f64,
    pub risk_free_rate: f64,
    pub grid_step: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssetClass {
    AShare,
    UsStock,
    Bond,
    Cash,
    Commodity,
}

impl AssetClass {
    pub fn from_db(s: &str) -> Result<Self, String> {
        match s {
            "a_share" => Ok(AssetClass::AShare),
            "us_stock" => Ok(AssetClass::UsStock),
            "bond" => Ok(AssetClass::Bond),
            "cash" => Ok(AssetClass::Cash),
            "commodity" => Ok(AssetClass::Commodity),
            other => Err(format!("非法 asset_class: {}", other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AssetStrategy {
    pub strategy_id: String,
    pub asset_class: AssetClass,
    pub security: SecurityConfig,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub signal_source: String, // prediction_blend / factor_combo / fixed
    pub combo_name: String,
    pub top_n: i64,
    pub prediction_set_id: Option<String>,
    pub prediction_blend_weight: f64,
    pub score_direction: String,
    pub candidate_tier: String,
    pub equity_curve_task_id: Option<String>,
    pub fixed_symbols: Vec<String>, // ETF/商品固定标的(选股=fixed);DB 列名 etf_symbols
    pub default_weights: Vec<f64>,  // 标的级配比(仅 MVO 数据不足 fallback 时生效)
    pub max_single: f64,            // 单票集中度上限(仅 a_share)
    pub max_single_bull: f64,       // 牛市单票上限(仅 a_share)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asset_class_from_db() {
        assert_eq!(AssetClass::from_db("a_share").unwrap(), AssetClass::AShare);
        assert_eq!(
            AssetClass::from_db("us_stock").unwrap(),
            AssetClass::UsStock
        );
        assert_eq!(
            AssetClass::from_db("commodity").unwrap(),
            AssetClass::Commodity
        );
        assert!(AssetClass::from_db("invalid").is_err());
    }

    #[test]
    fn test_resolved_strategy_construct() {
        let rs = ResolvedStrategy {
            strategy_id: "v19".into(),
            name: "test".into(),
            strategy_type: StrategyType::Composite,
            mvo: Some(MvoParams {
                vol_target: 0.2,
                leverage_cap: 2.5,
                leverage_floor: 1.0,
                default_weights: vec![0.22, 0.28, 0.05, 0.10, 0.03, 0.03, 0.03],
                min_stock: 0.12,
                momentum_blend_ratio: 0.5,
                ga_population: 600,
                ga_generations: 250,
                ga_elite_count: 10,
                regime_bull_threshold: 0.0,
                regime_bear_threshold: 0.0,
                regime_bull_min_stock: 0,
                regime_bear_min_stock: 0,
                dynamic_target_cap: 0.30,
                dynamic_target_floor: 0.12,
                risk_free_rate: 0.03,
                grid_step: 0.0,
            }),
            assets: vec![],
            rebalance_freq: "quarterly".into(),
        };
        assert_eq!(rs.strategy_type, StrategyType::Composite);
        assert!(rs.mvo.is_some());
    }
}
