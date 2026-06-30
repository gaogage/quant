//! 策略组合嵌套架构 —— 三层策略树的运行时模型与加载。
//!
//! 层级:composite(组合,多资产MVO) → asset(单资产) → security(选股参数,内联 asset)。
//! strategy_config 单表用 strategy_type 区分层级,parent_strategy_id 表父子关系。
//!
//! MVO 是 composite 的固有属性(strategy_type='composite' ⟹ 必有 MVO),非开关字段。

use sqlx::PgPool;

/// 解析后的完整策略树(composite 或单 asset 账号都会解析成此结构)
#[derive(Debug, Clone)]
pub struct ResolvedStrategy {
    pub strategy_id: String,
    pub name: String,
    pub strategy_type: StrategyType,
    pub mvo: Option<MvoParams>,     // 仅 composite 非空;asset 账号 None
    pub assets: Vec<AssetStrategy>, // composite=多个;单 asset 账号=1个
    pub etf_symbols: Vec<String>,   // MVO 8 维的第 1-7 列标的(标准顺序,取自 composite 行)
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
    pub regime_bull_min_stock: f64,
    pub regime_bear_min_stock: f64,
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

// ===== DB 行映射(sqlx::FromRow) =====

#[derive(Debug, sqlx::FromRow)]
struct CompositeRow {
    strategy_id: String,
    name: String,
    strategy_type: String,
    rebalance_freq: Option<String>,
    etf_symbols: Option<serde_json::Value>,
    vol_target: Option<f64>,
    leverage_cap: Option<f64>,
    leverage_floor: Option<f64>,
    default_weights: Option<serde_json::Value>,
    min_stock: Option<f64>,
    momentum_blend_ratio: Option<f64>,
    ga_population: Option<i32>,
    ga_generations: Option<i32>,
    ga_elite_count: Option<i32>,
    regime_bull_threshold: Option<f64>,
    regime_bear_threshold: Option<f64>,
    regime_bull_min_stock: Option<f64>,
    regime_bear_min_stock: Option<f64>,
    dynamic_target_cap: Option<f64>,
    dynamic_target_floor: Option<f64>,
    risk_free_rate: Option<f64>,
    grid_step: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
struct AssetRow {
    strategy_id: String,
    asset_class: String,
    signal_source: Option<String>,
    combo_name: Option<String>,
    top_n: Option<i32>,
    prediction_set_id: Option<String>,
    prediction_blend_weight: Option<f64>,
    score_direction: Option<String>,
    candidate_tier: Option<String>,
    equity_curve_task_id: Option<String>,
    etf_symbols: Option<serde_json::Value>,
    default_weights: Option<serde_json::Value>,
    max_single: Option<f64>,
    max_single_bull: Option<f64>,
}

fn parse_f64_array(v: &Option<serde_json::Value>) -> Vec<f64> {
    v.as_ref()
        .and_then(|j| j.as_array())
        .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect())
        .unwrap_or_default()
}

fn parse_str_array(v: &Option<serde_json::Value>) -> Vec<String> {
    v.as_ref()
        .and_then(|j| j.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// 从 DB 加载策略树。composite 加载 MVO + 子 asset 行;单 asset 账号 mvo=None,assets=[自身]。
pub async fn load_resolved_strategy(
    db: &PgPool,
    strategy_id: &str,
) -> Result<ResolvedStrategy, String> {
    // 1. 主行
    let main: Option<CompositeRow> = sqlx::query_as::<_, CompositeRow>(
        "SELECT strategy_id, name, strategy_type,
                rebalance_freq, etf_symbols, vol_target, leverage_cap, leverage_floor,
                default_weights, min_stock, momentum_blend_ratio,
                ga_population, ga_generations, ga_elite_count,
                regime_bull_threshold, regime_bear_threshold,
                regime_bull_min_stock, regime_bear_min_stock,
                dynamic_target_cap, dynamic_target_floor, risk_free_rate, grid_step
         FROM strategy_config WHERE strategy_id = $1 AND status = 'active'",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("load main row ({}): {}", strategy_id, e))?;

    let main = main.ok_or_else(|| format!("strategy not found: {}", strategy_id))?;

    let stype = if main.strategy_type == "asset" {
        StrategyType::Asset
    } else {
        StrategyType::Composite
    };

    match stype {
        StrategyType::Asset => {
            // 单 asset 账号:mvo=None,assets=[自身]
            let asset_row = load_asset_row(db, strategy_id).await?;
            Ok(ResolvedStrategy {
                strategy_id: main.strategy_id,
                name: main.name,
                strategy_type: StrategyType::Asset,
                mvo: None,
                assets: vec![asset_row],
                etf_symbols: parse_str_array(&main.etf_symbols),
                rebalance_freq: main.rebalance_freq.unwrap_or_else(|| "quarterly".into()),
            })
        }
        StrategyType::Composite => {
            // composite:加载 MVO + 所有子 asset 行
            let asset_rows = load_child_asset_rows(db, strategy_id).await?;
            if asset_rows.is_empty() {
                return Err(format!("composite 无资产子策略: {}", strategy_id));
            }
            let mvo = MvoParams {
                vol_target: main.vol_target.unwrap_or(0.2),
                leverage_cap: main.leverage_cap.unwrap_or(2.5),
                leverage_floor: main.leverage_floor.unwrap_or(1.0),
                default_weights: parse_f64_array(&main.default_weights),
                min_stock: main.min_stock.unwrap_or(0.12),
                momentum_blend_ratio: main.momentum_blend_ratio.unwrap_or(0.5),
                ga_population: main.ga_population.unwrap_or(600) as usize,
                ga_generations: main.ga_generations.unwrap_or(250) as usize,
                ga_elite_count: main.ga_elite_count.unwrap_or(10) as usize,
                regime_bull_threshold: main.regime_bull_threshold.unwrap_or(0.0),
                regime_bear_threshold: main.regime_bear_threshold.unwrap_or(0.0),
                regime_bull_min_stock: main.regime_bull_min_stock.unwrap_or(0.0),
                regime_bear_min_stock: main.regime_bear_min_stock.unwrap_or(0.0),
                dynamic_target_cap: main.dynamic_target_cap.unwrap_or(0.30),
                dynamic_target_floor: main.dynamic_target_floor.unwrap_or(0.12),
                risk_free_rate: main.risk_free_rate.unwrap_or(0.03),
                grid_step: main.grid_step.unwrap_or(0.0),
            };
            Ok(ResolvedStrategy {
                strategy_id: main.strategy_id,
                name: main.name,
                strategy_type: StrategyType::Composite,
                mvo: Some(mvo),
                assets: asset_rows,
                etf_symbols: parse_str_array(&main.etf_symbols),
                rebalance_freq: main.rebalance_freq.unwrap_or_else(|| "quarterly".into()),
            })
        }
    }
}

async fn load_asset_row(db: &PgPool, strategy_id: &str) -> Result<AssetStrategy, String> {
    let row: Option<AssetRow> = sqlx::query_as::<_, AssetRow>(
        "SELECT strategy_id, asset_class, signal_source, combo_name, top_n,
                prediction_set_id, prediction_blend_weight, score_direction, candidate_tier,
                equity_curve_task_id, etf_symbols, default_weights, max_single, max_single_bull
         FROM strategy_config WHERE strategy_id = $1 AND status = 'active'",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("load asset row ({}): {}", strategy_id, e))?;
    let row = row.ok_or_else(|| format!("asset row not found: {}", strategy_id))?;
    build_asset_strategy(row)
}

async fn load_child_asset_rows(
    db: &PgPool,
    parent_strategy_id: &str,
) -> Result<Vec<AssetStrategy>, String> {
    let rows: Vec<AssetRow> = sqlx::query_as::<_, AssetRow>(
        "SELECT strategy_id, asset_class, signal_source, combo_name, top_n,
                prediction_set_id, prediction_blend_weight, score_direction, candidate_tier,
                equity_curve_task_id, etf_symbols, default_weights, max_single, max_single_bull
         FROM strategy_config
         WHERE parent_strategy_id = $1 AND status = 'active'
         ORDER BY asset_class",
    )
    .bind(parent_strategy_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("load child assets ({}): {}", parent_strategy_id, e))?;
    rows.into_iter().map(build_asset_strategy).collect()
}

fn build_asset_strategy(row: AssetRow) -> Result<AssetStrategy, String> {
    let asset_class = AssetClass::from_db(&row.asset_class)
        .map_err(|e| format!("asset {} ({}): {}", row.strategy_id, row.asset_class, e))?;
    Ok(AssetStrategy {
        strategy_id: row.strategy_id,
        asset_class,
        security: SecurityConfig {
            signal_source: row.signal_source.unwrap_or_else(|| "fixed".into()),
            combo_name: row.combo_name.unwrap_or_default(),
            top_n: row.top_n.unwrap_or(30) as i64,
            prediction_set_id: row.prediction_set_id,
            prediction_blend_weight: row.prediction_blend_weight.unwrap_or(0.5),
            score_direction: row.score_direction.unwrap_or_else(|| "descending".into()),
            candidate_tier: row
                .candidate_tier
                .unwrap_or_else(|| "research_baseline".into()),
            equity_curve_task_id: row.equity_curve_task_id,
            fixed_symbols: parse_str_array(&row.etf_symbols),
            default_weights: parse_f64_array(&row.default_weights),
            max_single: row.max_single.unwrap_or(0.75),
            max_single_bull: row.max_single_bull.unwrap_or(0.80),
        },
    })
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
                regime_bull_min_stock: 0.0,
                regime_bear_min_stock: 0.0,
                dynamic_target_cap: 0.30,
                dynamic_target_floor: 0.12,
                risk_free_rate: 0.03,
                grid_step: 0.0,
            }),
            assets: vec![],
            etf_symbols: vec![
                "518880.SH".into(),
                "511010.SH".into(),
                "513500.SH".into(),
                "513100.SH".into(),
                "159980.SZ".into(),
                "159985.SZ".into(),
                "501018.SH".into(),
            ],
            rebalance_freq: "quarterly".into(),
        };
        assert_eq!(rs.strategy_type, StrategyType::Composite);
        assert!(rs.mvo.is_some());
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_resolved_strategy_v19() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("connect db");

        let rs = load_resolved_strategy(&db, "v19").await.expect("load v19");
        assert_eq!(rs.strategy_type, StrategyType::Composite);
        assert!(rs.mvo.is_some(), "composite 必有 MVO");
        let mvo = rs.mvo.as_ref().unwrap();
        assert!((mvo.min_stock - 0.12).abs() < 1e-6, "min_stock=0.12");
        assert_eq!(mvo.default_weights.len(), 7, "7维 ETF 权重");
        assert_eq!(rs.assets.len(), 4, "v19 有 4 个 asset 子行");
        // etf_symbols 保持 MVO 标准顺序(黄金/国债/标普/纳指/有色/豆粕/原油)
        assert_eq!(
            rs.etf_symbols,
            vec![
                "518880.SH",
                "511010.SH",
                "513500.SH",
                "513100.SH",
                "159980.SZ",
                "159985.SZ",
                "501018.SH"
            ],
            "etf_symbols MVO 标准顺序"
        );
        // a_share asset 必须有 equity_curve_task_id
        let a_share = rs
            .assets
            .iter()
            .find(|a| a.asset_class == AssetClass::AShare);
        assert!(a_share.is_some(), "必有 a_share asset");
        assert!(a_share.unwrap().security.equity_curve_task_id.is_some());
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_resolved_strategy_invalid() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("connect db");

        let r = load_resolved_strategy(&db, "nonexistent_strategy").await;
        assert!(r.is_err(), "查无主行应报错");
        assert!(r.unwrap_err().contains("strategy not found"));
    }
}
