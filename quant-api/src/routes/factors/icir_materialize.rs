//! PIT rolling ICIR combo materialization routes.

use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use crate::AppState;
use super::*;
use super::{background_factor_task_id, parse_phase7_backfill_date, usize_to_i32};

/// PIT 滚动 ICIR combo 物化入口（可复用，供未来实盘调度器增量触发保鲜）。
///
/// 对 `[start, end]` 区间内每个交易日，用其所属季度调仓点（季度首个交易日）的
/// PIT 滚动 ICIR 权重（只取 `end_date <= 调仓点` 的最新 IC，绝不用未来），
/// 加权 `factor_value.normalized_value` 得 combo 分，幂等写入 `multi_factor_value`。
///
/// 候选池 = 量价技术因子（排除未过数据审计的基本面/另类因子）。
/// 增量保鲜：实盘只需传最近季度区间，ON CONFLICT 刷新即可。
pub async fn materialize_pit_combo(
    db: &sqlx::PgPool,
    combo_name: &str,
    factor_version: &str,
    horizon: i16,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<u64, String> {
    materialize_pit_combo_ext(db, combo_name, factor_version, horizon, start_date, end_date, false, None, None, false).await
}

/// 扩展版物化:支持纳入基本面因子(fin_/mf_/north_)与 IC 强度阈值筛选。
/// - include_fundamentals: true 时黑名单移除 fin_|margin_|mf_|north_(block_/ar_ 原本不在黑名单,自动进)。
///   用于新建含基本面因子的 combo(如 full_pit_icir_37f_h20_fund),与原量价 combo 做 A/B 对比。
/// - min_abs_ic_ir: PIT 因子 ic_ir 绝对值下限(如 0.20),只让强预测力因子进 combo,弱因子(量价/基本面)都排除。
///   None 时不加阈值(保留原行为,兼容现有 combo)。
pub async fn materialize_pit_combo_ext(
    db: &sqlx::PgPool,
    combo_name: &str,
    factor_version: &str,
    horizon: i16,
    start_date: NaiveDate,
    end_date: NaiveDate,
    include_fundamentals: bool,
    min_abs_ic_ir: Option<f64>,
    factor_whitelist: Option<&[String]>,
    ind_neutral: bool,
) -> Result<u64, String> {
    // 黑名单正则:include_fundamentals=true 时移除 fin_|margin_|mf_|north_|val_,让基本面+估值因子进 combo。
    // block_/ar_ 原本不在黑名单。cf_/div_/event_ 等始终排除(数据口径/事件类未纳入)。
    // val_ 解除排除(2026-08-13 阶段2):价值因子(PS/PB/PE/股息率) ICIR 最强(5-38),是 A 股核心 alpha 源,
    //   之前被黑名单拦截导致 sleeve alpha 不显著(WFA -0.56)。解除后仅白名单含 val_ 的 combo 纳入(交集过滤),
    //   v24 白名单无 val_ 故不变,只有新价值 combo 才纳入。
    let blacklist_re = if include_fundamentals {
        "^(cf_|div_|event_|external|debt_|gross_|pe_|roe|ind_rel|mkt_rel)"
    } else {
        "^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)"
    };
    // IC 强度阈值片段:min_abs_ic_ir 给定时加 ABS(fe.ic_ir) >= $n 条件,只留强因子。
    // 占位 {ic_threshold} 替换为 "" 或 "AND ABS(fe.ic_ir) >= $N"(参数索引在调用处绑定)。
    // factor_whitelist:去冗余白名单(按 IC 相关性聚类选的代表因子)。给定时只让指定因子进 combo,
    // 避免同源因子(如 fin_roe/fin_roa/fin_margin 相关 0.9+)放大信号扭曲 ICIR 加权。None 时不过滤。
    // 逐季度循环：每季用其调仓点(季度首个交易日)的 PIT 权重，单独 execute（自动提交）。
    // 可观测(逐季写入)、可增量(实盘只重跑最近季度)、避免单事务过重。
    let quarters: Vec<NaiveDate> = sqlx::query_scalar::<_, NaiveDate>(
        "SELECT MIN(trade_date) AS as_of
         FROM (SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
               WHERE trade_date >= $1 AND trade_date <= $2) d
         GROUP BY date_trunc('quarter', trade_date)
         ORDER BY 1",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("materialize_pit_combo quarters: {}", e))?;

    // $4 = as_of（季度调仓点，同时是 PIT 截止日）；$5 = 下季 as_of（开区间末）
    // $6 = blacklist_re（黑名单正则，参数化支持 include_fundamentals 分支）
    // $7 = min_abs_ic_ir（IC 强度阈值，NULL 时不筛选）
    // $8 = factor_whitelist（去冗余白名单，NULL 时不筛选）
    let per_quarter_sql = if ind_neutral {
        // 行业中性化版(2026-08-13 阶段2):因子值减同行业均值(market_stock.industry 申万L1 约28行业),
        // 剥离行业 beta(价值因子不再集中银行/地产陷阱),保留行业内相对 alpha,解决纯多头 value trap。
        // ind_avg CTE 预算每因子每日各行业的 normalized_value 均值,scores 用 normalized_value - industry_avg。
        r#"
WITH pit AS (
    SELECT DISTINCT ON (fe.factor_code) fe.factor_code, fe.mean_ic, fe.ic_ir
    FROM factor_evaluation fe
    JOIN factor_definition fd ON fd.factor_code = fe.factor_code AND fd.status = 'active'
    WHERE fe.horizon = $3 AND fe.end_date <= $4
      AND fe.mean_ic IS NOT NULL AND fe.ic_ir IS NOT NULL
      AND fe.factor_code !~ $6
      AND ($7 IS NULL OR ABS(fe.ic_ir) >= $7)
      AND ($8 IS NULL OR fe.factor_code = ANY($8))
    ORDER BY fe.factor_code, fe.end_date DESC
),
wsum AS (SELECT SUM(ABS(ic_ir)) AS tot FROM pit),
ind_avg AS (
    SELECT fv2.factor_code, fv2.trade_date, ms.industry, AVG(fv2.normalized_value) AS industry_avg
    FROM factor_value fv2
    JOIN pit p2 ON p2.factor_code = fv2.factor_code
    JOIN market_stock ms ON ms.symbol = fv2.symbol AND ms.industry IS NOT NULL
    WHERE fv2.factor_version = $2 AND fv2.trade_date >= $4 AND fv2.trade_date < $5
      AND fv2.normalized_value IS NOT NULL
    GROUP BY fv2.factor_code, fv2.trade_date, ms.industry
),
scores AS (
    SELECT fv.symbol, fv.trade_date,
        SUM((fv.normalized_value - COALESCE(ia.industry_avg, 0)) * (p.ic_ir / NULLIF(w.tot, 0.0)) * SIGN(p.mean_ic)) AS raw_score,
        MAX(COALESCE(fv.available_at, fv.trade_date)) AS available_at
    FROM pit p CROSS JOIN wsum w
    JOIN factor_value fv
      ON fv.factor_code = p.factor_code AND fv.factor_version = $2
     AND fv.trade_date >= $4 AND fv.trade_date < $5 AND fv.normalized_value IS NOT NULL
    JOIN market_stock ms2 ON ms2.symbol = fv.symbol AND ms2.industry IS NOT NULL
    LEFT JOIN ind_avg ia ON ia.factor_code = fv.factor_code AND ia.trade_date = fv.trade_date AND ia.industry = ms2.industry
    GROUP BY fv.symbol, fv.trade_date
)
INSERT INTO multi_factor_value
    (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
FROM scores
ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
    raw_score = EXCLUDED.raw_score,
    normalized_score = EXCLUDED.normalized_score,
    available_at = EXCLUDED.available_at,
    created_at = NOW()
"#
    } else {
        r#"
WITH pit AS (
    -- PIT 因子集:JOIN factor_definition status='active' 过滤废弃因子。
    -- 早期原始版因子(mom_20d 等)未进 definition 表,后期 _std 版才注册;
    -- 废弃因子的 factor_evaluation 历史 IC 残留,若不过滤会进 combo 导致:
    -- 1)INNER JOIN factor_value 取不到数据静默跳过;2)权重分母含废弃因子 ICIR 虚高→raw_score 压低。
    -- 黑名单正则($6)与 IC 阈值($7)参数化:include_fundamentals=true 时移除 fin_/mf_/north_,
    -- min_abs_ic_ir 给定时只留强预测力因子(量价+基本面统一筛选,避免弱因子稀释权重)。
    -- factor_whitelist($8)给定时只让去冗余代表因子进 combo,避免同源信号放大。
    SELECT DISTINCT ON (fe.factor_code) fe.factor_code, fe.mean_ic, fe.ic_ir
    FROM factor_evaluation fe
    JOIN factor_definition fd ON fd.factor_code = fe.factor_code AND fd.status = 'active'
    WHERE fe.horizon = $3 AND fe.end_date <= $4
      AND fe.mean_ic IS NOT NULL AND fe.ic_ir IS NOT NULL
      AND fe.factor_code !~ $6
      AND ($7 IS NULL OR ABS(fe.ic_ir) >= $7)
      AND ($8 IS NULL OR fe.factor_code = ANY($8))
    ORDER BY fe.factor_code, fe.end_date DESC
),
wsum AS (SELECT SUM(ABS(ic_ir)) AS tot FROM pit),
scores AS (
    SELECT fv.symbol, fv.trade_date,
        SUM(fv.normalized_value * (p.ic_ir / NULLIF(w.tot, 0.0)) * SIGN(p.mean_ic)) AS raw_score,
        MAX(COALESCE(fv.available_at, fv.trade_date)) AS available_at
    FROM pit p CROSS JOIN wsum w
    JOIN factor_value fv
      ON fv.factor_code = p.factor_code AND fv.factor_version = $2
     AND fv.trade_date >= $4 AND fv.trade_date < $5 AND fv.normalized_value IS NOT NULL
    GROUP BY fv.symbol, fv.trade_date
)
INSERT INTO multi_factor_value
    (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
FROM scores
ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
    raw_score = EXCLUDED.raw_score,
    normalized_score = EXCLUDED.normalized_score,
    available_at = EXCLUDED.available_at,
    created_at = NOW()
"#
    };

    let mut total: u64 = 0;
    for (i, as_of) in quarters.iter().enumerate() {
        let q_end = quarters
            .get(i + 1)
            .copied()
            .unwrap_or_else(|| end_date + chrono::Duration::days(1));

        // 覆盖率门禁:季度调仓点因子数据完整性检查。
        // pit 因子集(活跃因子)中,实际有当日 factor_value 数据的比例。
        // 覆盖率<50%告警(仍物化但记日志,供排查),=0 跳过该季度(无数据无法打分)。
        // 黑名单($4)与 IC 阈值($5)与白名单($6)同 per_quarter_sql 口径,保证门禁与实际物化一致。
        let coverage: Option<(i64, i64)> = sqlx::query_as(
            r#"SELECT
                 (SELECT COUNT(DISTINCT fe.factor_code) FROM factor_evaluation fe
                   JOIN factor_definition fd ON fd.factor_code=fe.factor_code AND fd.status='active'
                   WHERE fe.horizon=$1 AND fe.end_date<=$2 AND fe.mean_ic IS NOT NULL AND fe.ic_ir IS NOT NULL
                     AND fe.factor_code !~ $4
                     AND ($5 IS NULL OR ABS(fe.ic_ir) >= $5)
                     AND ($6 IS NULL OR fe.factor_code = ANY($6))) AS pit_n,
                 (SELECT COUNT(DISTINCT fe.factor_code) FROM factor_evaluation fe
                   JOIN factor_definition fd ON fd.factor_code=fe.factor_code AND fd.status='active'
                   JOIN factor_value fv ON fv.factor_code=fe.factor_code AND fv.factor_version=$3
                     AND fv.trade_date=$2 AND fv.normalized_value IS NOT NULL
                   WHERE fe.horizon=$1 AND fe.end_date<=$2 AND fe.mean_ic IS NOT NULL AND fe.ic_ir IS NOT NULL
                     AND fe.factor_code !~ $4
                     AND ($5 IS NULL OR ABS(fe.ic_ir) >= $5)
                     AND ($6 IS NULL OR fe.factor_code = ANY($6))) AS have_n"#,
        )
        .bind(horizon)
        .bind(as_of)
        .bind(factor_version)
        .bind(blacklist_re)
        .bind(min_abs_ic_ir)
        .bind(factor_whitelist)
        .fetch_one(db)
        .await
        .ok();
        if let Some((pit_n, have_n)) = coverage {
            if pit_n > 0 && have_n == 0 {
                tracing::warn!(combo = combo_name, quarter = %as_of, pit_factors = pit_n, "季度因子数据完全缺失,跳过物化(避免空打分)");
                continue;
            }
            if pit_n > 0 && have_n * 2 < pit_n {
                tracing::warn!(combo = combo_name, quarter = %as_of, pit = pit_n, have = have_n, pct = have_n * 100 / pit_n, "季度因子覆盖率<50%,打分将基于部分因子(权重自动按有数据因子重归一化)");
            }
        }

        let res = sqlx::query(per_quarter_sql)
            .bind(combo_name)
            .bind(factor_version)
            .bind(horizon)
            .bind(as_of)
            .bind(q_end)
            .bind(blacklist_re)
            .bind(min_abs_ic_ir)
            .bind(factor_whitelist)
            .execute(db)
            .await
            .map_err(|e| format!("materialize_pit_combo q={}: {}", as_of, e))?;
        total += res.rows_affected();
        info!(combo = combo_name, quarter = %as_of, rows = res.rows_affected(), "PIT combo 季度物化");
    }
    Ok(total)
}


/// P4.2b overlay combo 物化:等权平均两个交互特征(normalized_value)。
/// combo = p42b_large_cap_alpha_overlay_v1,成分:large_cap_mom_rev_daily_std +
/// defensive_lowvol_quality_daily_std。两因子均正向 IC(descending 有效)。
/// **PIT ICIR 加权**:每个 trade_date,用 end_date <= trade_date 的历史窗口 mean_rank_ic
/// 算累积 ICIR(均值/std),归一化为权重。窗口数 <3 时回退等权(早期冷启动)。
/// PIT:ICIR 只用窗口结束日 <= trade_date 的数据,无未来信息;factor_value 用 available_at <= trade_date。
pub async fn materialize_p42b_overlay_combo(
    db: &sqlx::PgPool,
    combo_name: &str,
    factor_version: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<u64, String> {
    // PIT ICIR 加权:每因子在 trade_date 的权重 = 累积 ICIR / (两因子累积 ICIR 之和)。
    // 累积 ICIR = avg(mean_rank_ic over windows with end_date <= trade_date)
    //             / nullif(stddev(mean_rank_ic over same), 0)
    // 窗口数 <3 或 ICIR<=0 时回退等权(0.5/0.5),避免冷启动期不稳定权重。
    // 因子 normalized_value 均为 0-1 截面 percent_rank,加权求和后仍为 0-1,可作 raw_score。
    let sql = r#"
WITH pairs AS (
    SELECT a.symbol, a.trade_date,
        a.normalized_value AS a_val,
        b.normalized_value AS b_val,
        GREATEST(COALESCE(a.available_at, a.trade_date), COALESCE(b.available_at, b.trade_date)) AS available_at
    FROM factor_value a
    JOIN factor_value b
      ON a.symbol = b.symbol AND a.trade_date = b.trade_date
    WHERE a.factor_code = 'large_cap_mom_rev_daily_std'
      AND a.factor_version = $2
      AND b.factor_code = 'defensive_lowvol_quality_daily_std'
      AND b.factor_version = $2
      AND a.trade_date BETWEEN $3 AND $4
      AND a.normalized_value IS NOT NULL
      AND b.normalized_value IS NOT NULL
      AND a.available_at <= a.trade_date
      AND b.available_at <= b.trade_date
),
-- 每因子的 PIT 累积 ICIR(按 trade_date 滚动,只用 end_date <= trade_date 的窗口)
pit_icir AS (
    SELECT
        p.trade_date,
        -- large_cap 累积 ICIR
        CASE WHEN COUNT(fe_a.mean_rank_ic) >= 3
             THEN AVG(fe_a.mean_rank_ic) / NULLIF(STDDEV(fe_a.mean_rank_ic), 0)
             ELSE NULL
        END AS icir_a,
        -- defensive 累积 ICIR
        CASE WHEN COUNT(fe_b.mean_rank_ic) >= 3
             THEN AVG(fe_b.mean_rank_ic) / NULLIF(STDDEV(fe_b.mean_rank_ic), 0)
             ELSE NULL
        END AS icir_b
    FROM (SELECT DISTINCT trade_date FROM pairs) p
    LEFT JOIN factor_evaluation fe_a
      ON fe_a.factor_code = 'large_cap_mom_rev_daily_std'
     AND fe_a.factor_version = $2
     AND fe_a.end_date <= p.trade_date
    LEFT JOIN factor_evaluation fe_b
      ON fe_b.factor_code = 'defensive_lowvol_quality_daily_std'
     AND fe_b.factor_version = $2
     AND fe_b.end_date <= p.trade_date
    GROUP BY p.trade_date
),
weighted AS (
    SELECT
        p.symbol,
        p.trade_date,
        p.available_at,
        p.a_val,
        p.b_val,
        -- ICIR 归一化权重;任一 ICIR 为 NULL/<=0 或窗口不足 → 等权 0.5/0.5
        CASE WHEN ic.icir_a IS NOT NULL AND ic.icir_b IS NOT NULL
                  AND ic.icir_a > 0 AND ic.icir_b > 0
             THEN ic.icir_a / (ic.icir_a + ic.icir_b)
             ELSE 0.5
        END AS w_a,
        CASE WHEN ic.icir_a IS NOT NULL AND ic.icir_b IS NOT NULL
                  AND ic.icir_a > 0 AND ic.icir_b > 0
             THEN ic.icir_b / (ic.icir_a + ic.icir_b)
             ELSE 0.5
        END AS w_b
    FROM pairs p
    JOIN pit_icir ic ON ic.trade_date = p.trade_date
)
INSERT INTO multi_factor_value
    (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
SELECT $1, $2, symbol, trade_date,
    w_a * a_val + w_b * b_val AS raw_score,
    w_a * a_val + w_b * b_val AS normalized_score,
    available_at
FROM weighted
ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
    raw_score = EXCLUDED.raw_score,
    normalized_score = EXCLUDED.normalized_score,
    available_at = EXCLUDED.available_at,
    created_at = NOW()
"#;
    let res = sqlx::query(sql)
        .bind(combo_name)
        .bind(factor_version)
        .bind(start_date)
        .bind(end_date)
        .execute(db)
        .await
        .map_err(|e| format!("materialize_p42b_overlay_combo: {}", e))?;
    let total = res.rows_affected();
    info!(combo = combo_name, rows = total, "P4.2b overlay combo 物化完成(PIT ICIR 加权)");
    Ok(total)
}

#[derive(Debug, Deserialize)]
pub struct MaterializePitComboRequest {
    pub combo_name: String,
    #[serde(default = "default_pit_combo_version")]
    pub version: String,
    #[serde(default = "default_pit_horizon")]
    pub horizon: i16,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// 是否纳入基本面因子(fin_/mf_/north_)。true 时黑名单移除这些前缀,用于新建含基本面 combo。
    /// 默认 false:保持原量价 combo 行为。
    #[serde(default)]
    pub include_fundamentals: bool,
    /// PIT 因子 ic_ir 绝对值下限。给定(如 0.20)时只留强预测力因子进 combo。
    /// 默认 None:不加阈值,兼容现有 combo。
    pub min_abs_ic_ir: Option<f64>,
    /// 去冗余白名单:按 IC 相关性聚类选的代表因子列表。
    /// 给定时只让指定因子进 combo,避免同源信号放大。默认 None:不额外过滤。
    pub factor_whitelist: Option<Vec<String>>,
    /// 行业中性化(2026-08-13 阶段2):true 时因子值减同行业均值(申万L1 约28行业)再 ICIR 加权,
    /// 剥离行业 beta 解决价值因子纯多头 value trap。默认 false:原全市场加权。
    #[serde(default)]
    pub ind_neutral: bool,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRollingPitRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default = "default_pit_combo_version")]
    pub version: String,
    #[serde(default = "default_pit_horizon")]
    pub horizon: i16,
    pub train_lookback_days: Option<i64>,
    pub max_windows: Option<usize>,
    /// 可选:只评估指定因子列表(与 version 一同校验存在)。不传则评估 version 下所有未排除前缀的 active 技术因子。
    pub factor_codes: Option<Vec<String>>,
}

#[derive(Debug, Clone)]

pub(crate) struct EvaluateRollingPitPlan {
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) version: String,
    pub(crate) horizon: i16,
    pub(crate) train_lookback_days: i64,
    pub(crate) max_windows: Option<usize>,
    pub(crate) factor_codes: Option<Vec<String>>,
}

impl EvaluateRollingPitRequest {
    pub(crate) fn into_plan(self) -> Result<EvaluateRollingPitPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2014, 1, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;
        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }
        if self.horizon <= 0 {
            return Err("horizon must be positive".to_string());
        }
        let train_lookback_days = self.train_lookback_days.unwrap_or(756);
        if train_lookback_days < self.horizon as i64 + 30 {
            return Err("train_lookback_days is too short for PIT IC evaluation".to_string());
        }
        let version = trim_or_default(Some(self.version), "1.0.0", "version")?;
        Ok(EvaluateRollingPitPlan {
            start_date,
            end_date,
            version,
            horizon: self.horizon,
            train_lookback_days,
            max_windows: self.max_windows,
            factor_codes: self.factor_codes,
        })
    }
}

fn default_pit_combo_version() -> String {
    "1.0.0".to_string()
}

fn default_pit_horizon() -> i16 {
    20
}

async fn load_rolling_pit_quarter_as_of_dates(
    db: &sqlx::PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
    max_windows: Option<usize>,
) -> Result<Vec<NaiveDate>, String> {
    let mut dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT MIN(trade_date) AS as_of
         FROM (SELECT DISTINCT trade_date FROM market_stock_daily_bar_adj
               WHERE trade_date >= $1 AND trade_date <= $2) d
         GROUP BY date_trunc('quarter', trade_date)
         ORDER BY 1",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(db)
    .await
    .map_err(|error| format!("load rolling PIT quarters: {}", error))?;
    if let Some(max_windows) = max_windows {
        dates.truncate(max_windows);
    }
    Ok(dates)
}


async fn load_candidate_technical_factors(
    db: &sqlx::PgPool,
    version: &str,
    horizon: i16,
    factor_codes: Option<&[String]>,
) -> Result<Vec<(String, String)>, String> {
    // 指定 factor_codes 时:只取这些因子(校验 version 匹配),跳过全量扫描与前缀排除。
    if let Some(codes) = factor_codes {
        if !codes.is_empty() {
            let rows = sqlx::query_as::<_, (String, String)>(
                "SELECT DISTINCT factor_code, version AS factor_version
                 FROM factor_definition
                 WHERE version=$1
                   AND status='active'
                   AND factor_code = ANY($2)
                 ORDER BY factor_code",
            )
            .bind(version)
            .bind(codes)
            .fetch_all(db)
            .await
            .map_err(|error| format!("load specified candidate factors: {}", error))?;
            if rows.is_empty() {
                return Err(format!(
                    "no active factor_definition found for version {} with specified factor_codes",
                    version
                ));
            }
            return Ok(rows);
        }
    }
    sqlx::query_as::<_, (String, String)>(
        "WITH candidates AS (
           SELECT factor_code, factor_version
           FROM factor_evaluation
           WHERE factor_version=$1
             AND horizon=$2
             AND factor_code !~ '^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)'
           UNION
           SELECT factor_code, version AS factor_version
           FROM factor_definition
           WHERE version=$1
             AND status='active'
             AND factor_code !~ '^(cf_|div_|event_|fin_|external|margin_|mf_|north_|debt_|gross_|pe_|roe|ind_rel|mkt_rel|val_)'
         )
         SELECT DISTINCT factor_code, factor_version
         FROM candidates
         ORDER BY factor_code",
    )
    .bind(version)
    .bind(horizon as i32)
    .fetch_all(db)
    .await
    .map_err(|error| format!("load candidate technical factors: {}", error))
}


async fn previous_open_trade_date(
    db: &sqlx::PgPool,
    as_of: NaiveDate,
) -> Result<NaiveDate, String> {
    sqlx::query_scalar(
        "SELECT MAX(trade_date)
         FROM market_trade_calendar
         WHERE is_open = true AND trade_date < $1",
    )
    .bind(as_of)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("load previous trade date: {}", error))?
    .flatten()
    .ok_or_else(|| format!("no open trade date before {}", as_of))
}


async fn evaluate_factor_ic_window(
    db: &sqlx::PgPool,
    factors: &[(String, String)],
    horizon: i16,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize, String> {
    let horizon_usize = horizon as usize;
    let fwd_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, close
         FROM market_stock_daily_bar_adj
         WHERE trade_date >= $1 AND trade_date <= $2
           AND close > 0
         ORDER BY symbol, trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("load PIT IC close prices: {}", error))?;

    let mut close_by_sym: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    for (sym, date, close) in &fwd_rows {
        if let Some(close) = close {
            let close_f: f64 = (*close).try_into().unwrap_or(0.0);
            if close_f > 0.0 {
                close_by_sym
                    .entry(sym.clone())
                    .or_default()
                    .push((*date, close_f));
            }
        }
    }

    let mut forward_returns: HashMap<(String, NaiveDate), f64> = HashMap::new();
    for (sym, prices) in &close_by_sym {
        for i in 0..prices.len().saturating_sub(horizon_usize) {
            let (date, close_t) = prices[i];
            let (_target_date, close_n) = prices[i + horizon_usize];
            if close_t > 0.0 {
                forward_returns.insert((sym.clone(), date), (close_n - close_t) / close_t);
            }
        }
    }

    let mut count = 0usize;
    for (code, ver) in factors {
        let fv_rows = sqlx::query_as::<
            _,
            (
                String,
                NaiveDate,
                Option<rust_decimal::Decimal>,
                Option<NaiveDate>,
            ),
        >(
            "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value), available_at
             FROM factor_value
             WHERE factor_code=$1 AND factor_version=$2
               AND trade_date >= $3 AND trade_date <= $4
               AND (available_at IS NULL OR available_at <= trade_date)
             ORDER BY symbol, trade_date",
        )
        .bind(code)
        .bind(ver)
        .bind(start)
        .bind(end)
        .fetch_all(db)
        .await
        .map_err(|error| format!("load factor values {}@{}: {}", code, ver, error))?;

        if fv_rows.len() < 100 {
            continue;
        }

        let values = fv_rows
            .into_iter()
            .filter_map(|(sym, date, value, available_at)| {
                value.map(|value| FactorValue {
                    symbol: sym,
                    date,
                    value: value.try_into().unwrap_or(f64::NAN),
                    available_at,
                })
            })
            .filter(|value| value.value.is_finite())
            .collect::<Vec<_>>();

        if values.len() < 100 {
            continue;
        }

        let output = FactorOutput {
            name: code.clone(),
            values,
            metadata: FactorMetadata {
                factor_name: code.clone(),
                category: FactorCategory::PriceVolume,
                version: ver.clone(),
                params: json!({"rolling_pit_eval": true}),
                computed_at: chrono::Utc::now(),
                symbol_count: 0,
                date_count: 0,
                coverage_ratio: 0.0,
                mean: 0.0,
                std: 0.0,
                min: 0.0,
                max: 0.0,
            },
        };

        let evaluation = evaluate(&output, &forward_returns, 5);
        if evaluation.period_count == 0 {
            continue;
        }
        let ic_json = serde_json::to_value(&evaluation.ic_series).unwrap_or(json!([]));
        let rank_ic_json = serde_json::to_value(&evaluation.rank_ic_series).unwrap_or(json!([]));
        let qr_json = serde_json::to_value(&evaluation.quantile_returns).unwrap_or(json!([]));
        sqlx::query(
            "INSERT INTO factor_evaluation (factor_code, factor_version, horizon, start_date, end_date,
             mean_ic, ic_ir, mean_rank_ic, rank_ic_ir, ic_series, rank_ic_series,
             quantile_spread, quantile_returns, period_count, symbol_count, total_pairs)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,0,0)
             ON CONFLICT (factor_code, factor_version, horizon, start_date, end_date) DO UPDATE SET
             mean_ic=EXCLUDED.mean_ic, ic_ir=EXCLUDED.ic_ir,
             mean_rank_ic=EXCLUDED.mean_rank_ic, rank_ic_ir=EXCLUDED.rank_ic_ir,
             ic_series=EXCLUDED.ic_series, rank_ic_series=EXCLUDED.rank_ic_series,
             quantile_spread=EXCLUDED.quantile_spread, quantile_returns=EXCLUDED.quantile_returns,
             period_count=EXCLUDED.period_count",
        )
        .bind(code)
        .bind(ver)
        .bind(horizon as i32)
        .bind(evaluation.date_range.0)
        .bind(evaluation.date_range.1)
        .bind(evaluation.mean_ic)
        .bind(evaluation.ic_ir)
        .bind(evaluation.mean_rank_ic)
        .bind(evaluation.rank_ic_ir)
        .bind(&ic_json)
        .bind(&rank_ic_json)
        .bind(evaluation.quantile_spread)
        .bind(&qr_json)
        .bind(evaluation.period_count as i32)
        .execute(db)
        .await
        .map_err(|error| format!("insert rolling PIT evaluation {}@{}: {}", code, ver, error))?;
        count += 1;
    }

    Ok(count)
}


async fn run_rolling_pit_evaluation_backfill(
    db: &sqlx::PgPool,
    task_id: &str,
    plan: &EvaluateRollingPitPlan,
) -> Result<serde_json::Value, String> {
    let as_of_dates =
        load_rolling_pit_quarter_as_of_dates(db, plan.start_date, plan.end_date, plan.max_windows)
            .await?;
    if as_of_dates.is_empty() {
        return Err("no market quarters found for requested range".to_string());
    }
    let factors = load_candidate_technical_factors(
        db,
        &plan.version,
        plan.horizon,
        plan.factor_codes.as_deref(),
    )
    .await?;
    if factors.is_empty() {
        return Err(format!(
            "no technical factor_value found for version {}",
            plan.version
        ));
    }

    let total_windows = as_of_dates.len();
    let mut evaluated_windows = 0usize;
    let mut inserted_evaluations = 0usize;
    for (index, as_of) in as_of_dates.iter().enumerate() {
        let eval_end = previous_open_trade_date(db, *as_of).await?;
        let eval_start = eval_end - chrono::Duration::days(plan.train_lookback_days);
        let inserted =
            evaluate_factor_ic_window(db, &factors, plan.horizon, eval_start, eval_end).await?;
        inserted_evaluations = inserted_evaluations.saturating_add(inserted);
        evaluated_windows += 1;
        let progress = (((index + 1) as f64 / total_windows as f64) * 100.0).round() as i32;
        let _ = sqlx::query(
            "UPDATE data_sync_task
             SET success_count=$2, total_count=$3, progress=$4, last_heartbeat_at=now()
             WHERE task_id=$1",
        )
        .bind(task_id)
        .bind(usize_to_i32(inserted_evaluations))
        .bind(usize_to_i32(total_windows))
        .bind(progress)
        .execute(db)
        .await;
        info!(
            task_id = %task_id,
            as_of = %as_of,
            eval_start = %eval_start,
            eval_end = %eval_end,
            inserted,
            "rolling PIT IC window evaluated"
        );
    }

    Ok(json!({
        "windows": evaluated_windows,
        "candidate_factors": factors.len(),
        "inserted_evaluations": inserted_evaluations,
    }))
}

/// POST /api/v1/quant/factors/evaluate-rolling-pit/background
///
/// Backfill PIT-safe rolling IC/ICIR evaluations by quarter. For each quarter
/// as-of date in the requested range, labels are computed only from close
/// prices available before that as-of date.

pub async fn evaluate_rolling_pit_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateRollingPitRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => return Json(json!({"code": 1, "message": error})),
    };
    let task_id = background_factor_task_id();
    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, 'evaluate_rolling_pit', 'factor', $2, $3, 'running', 0, 0, 0, 0, now(), 3600, now())",
    )
    .bind(&task_id)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create rolling PIT evaluation task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();
    tokio::spawn(async move {
        let result = run_rolling_pit_evaluation_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(report) => {
                let success_count = report["inserted_evaluations"].as_i64().unwrap_or(0) as i32;
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='completed', success_count=$2, total_count=$3, failed_count=0,
                         progress=100, error_message=NULL, last_heartbeat_at=now(), completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(success_count)
                .bind(report["windows"].as_i64().unwrap_or(0) as i32)
                .execute(&state.db)
                .await;
                info!(task_id = %tid, ?report, "rolling PIT IC backfill completed");
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "rolling PIT IC backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed', failed_count=1, error_message=$2,
                         last_heartbeat_at=now(), completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": "evaluate_rolling_pit",
            "start_date": plan.start_date,
            "end_date": plan.end_date,
            "horizon": plan.horizon,
            "train_lookback_days": plan.train_lookback_days,
        }
    }))
}

/// POST /api/v1/quant/factors/materialize-pit-combo/background
///
/// 后台物化 PIT 滚动 ICIR combo（供未来实盘调度器增量触发保鲜）。
/// 默认区间 2014-01-01 ~ 今。若早期季度缺少 PIT IC/ICIR，应先运行
/// `/api/v1/quant/factors/evaluate-rolling-pit/background`。

pub async fn materialize_pit_combo_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MaterializePitComboRequest>,
) -> impl IntoResponse {
    let start = req
        .start_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
    let end = req
        .end_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    let combo_name = req.combo_name.clone();
    let version = req.version.clone();
    let horizon = req.horizon;
    let task_id = background_factor_task_id();

    let _ = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at, started_at)
         VALUES ($1, 'materialize_pit_combo', 'factor', $2, $3, 'running', 0, 0, 0, 0, now(), now())",
    )
    .bind(&task_id)
    .bind(start)
    .bind(end)
    .execute(&state.db)
    .await;

    let include_fund = req.include_fundamentals;
    let min_ic_ir = req.min_abs_ic_ir;
    let whitelist = req.factor_whitelist.clone();
    let ind_neutral = req.ind_neutral;
    let state = state.clone();
    let tid = task_id.clone();
    tokio::spawn(async move {
        match materialize_pit_combo_ext(&state.db, &combo_name, &version, horizon, start, end, include_fund, min_ic_ir, whitelist.as_deref(), ind_neutral).await {
            Ok(rows) => {
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='completed', total_count=$2, success_count=$2,
                     progress=100, last_heartbeat_at=now(), completed_at=now() WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(rows as i32)
                .execute(&state.db)
                .await;
                info!(task_id = %tid, rows = rows, "PIT combo 物化完成");
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "PIT combo 物化失败");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='failed', error_message=$2,
                     last_heartbeat_at=now(), completed_at=now() WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&e)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": task_id, "status": "running"}}))
}

/// POST /api/v1/quant/factors/p42b-overlay-combo/materialize/background
///
/// P4.2b overlay combo 物化:等权平均 large_cap_mom_rev_daily_std +
/// defensive_lowvol_quality_daily_std,写入 multi_factor_value(combo_name 默认
/// p42b_large_cap_alpha_overlay_v1)。两因子均正向 IC,等权起点,后续可升级 ICIR。

pub async fn materialize_p42b_overlay_combo_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MaterializeP42bOverlayComboRequest>,
) -> impl IntoResponse {
    let start = req
        .start_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
    let end = req
        .end_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    let combo_name = req
        .combo_name
        .clone()
        .unwrap_or_else(|| "p42b_large_cap_alpha_overlay_v1".to_string());
    let version = req.version.clone();
    let task_id = background_factor_task_id();

    let _ = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at, started_at)
         VALUES ($1, 'materialize_p42b_overlay_combo', 'factor', $2, $3, 'running', 0, 0, 0, 0, now(), now())",
    )
    .bind(&task_id)
    .bind(start)
    .bind(end)
    .execute(&state.db)
    .await;

    let state = state.clone();
    let tid = task_id.clone();
    tokio::spawn(async move {
        match materialize_p42b_overlay_combo(&state.db, &combo_name, &version, start, end).await {
            Ok(rows) => {
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='completed', total_count=$2, success_count=$2,
                     progress=100, last_heartbeat_at=now(), completed_at=now() WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(rows as i32)
                .execute(&state.db)
                .await;
                info!(task_id = %tid, rows = rows, "P4.2b overlay combo 物化完成");
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "P4.2b overlay combo 物化失败");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='failed', error_message=$2,
                     last_heartbeat_at=now(), completed_at=now() WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&e)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": task_id, "status": "running"}}))
}

#[derive(Debug, Deserialize)]
pub struct MaterializeP42bOverlayComboRequest {
    pub combo_name: Option<String>,
    #[serde(default = "default_pit_combo_version")]
    pub version: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

