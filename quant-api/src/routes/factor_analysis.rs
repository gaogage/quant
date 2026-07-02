//! 因子强度分桶分析:按 regime/行业/市值/流动性分解因子 IC/IR。
//! 蓝图§4.2 最大化已知 alpha:深挖 financial_quality 信号最强子域。

use std::collections::HashMap;
use chrono::NaiveDate;
use serde_json::{json, Value};
use axum::{extract::State, response::IntoResponse, Json};
use std::sync::Arc;

use crate::AppState;

/// 单组 IC 评估结果(分桶或全市场)。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IcResult {
    pub mean_ic: f64,
    pub ic_ir: f64,
    pub mean_rank_ic: f64,
    pub rank_ic_ir: f64,
    pub n_obs: usize,
}

/// 算一组 aligned(factor_value, forward_return) 数据的 IC/IR。
/// mean_ic = 截面 IC 的均值(每日 IC 的平均);ic_ir = mean_ic / std_ic。
/// 简化:按日分组算截面 rank IC,再聚合(与 evaluate.rs 口径一致)。
pub fn compute_ic(
    factor_values: &[(String, NaiveDate, f64)], // (symbol, date, factor_value)
    forward_returns: &HashMap<(String, NaiveDate), f64>,
) -> IcResult {
    // 1. 按日分组,对齐 factor_value + forward_return
    let mut by_date: HashMap<NaiveDate, Vec<(String, f64, f64)>> = HashMap::new();
    for (symbol, date, fv) in factor_values {
        if !fv.is_finite() {
            continue;
        }
        if let Some(&fw_ret) = forward_returns.get(&(symbol.clone(), *date)) {
            if fw_ret.is_finite() {
                by_date.entry(*date).or_default().push((symbol.clone(), *fv, fw_ret));
            }
        }
    }
    if by_date.is_empty() {
        return IcResult::default();
    }

    // 2. 逐日算 rank IC(Spearman),收集序列
    let mut rank_ic_series: Vec<f64> = Vec::new();
    let mut n_total: usize = 0;
    for (_date, rows) in &by_date {
        if rows.len() < 5 {
            continue; // 截面样本太少,跳过
        }
        let ic = spearman_rank_ic(&rows.iter().map(|(s, fv, fr)| (fv.clone(), fr.clone())).collect::<Vec<_>>());
        if ic.is_finite() {
            rank_ic_series.push(ic);
        }
        n_total += rows.len();
    }
    if rank_ic_series.is_empty() {
        return IcResult { n_obs: n_total, ..Default::default() };
    }

    // 3. 聚合:mean/std/IR
    let mean_rank_ic = mean(&rank_ic_series);
    let std_ic = std(&rank_ic_series);
    let rank_ic_ir = if std_ic > 1e-12 { mean_rank_ic / std_ic } else { 0.0 };

    // mean_ic 用 Pearson IC(同样逐日算)
    let mut pearson_series: Vec<f64> = Vec::new();
    for (_date, rows) in &by_date {
        if rows.len() < 5 {
            continue;
        }
        let ic = pearson_ic(&rows.iter().map(|(_, fv, fr)| (fv.clone(), fr.clone())).collect::<Vec<_>>());
        if ic.is_finite() {
            pearson_series.push(ic);
        }
    }
    let mean_ic = mean(&pearson_series);
    let std_p = std(&pearson_series);
    let ic_ir = if std_p > 1e-12 { mean_ic / std_p } else { 0.0 };

    IcResult { mean_ic, ic_ir, mean_rank_ic, rank_ic_ir, n_obs: n_total }
}

/// 按分桶函数对 aligned 数据分组算 IC。
/// bucket_key_fn: (symbol, date) -> Option<String>(分桶名),None 则跳过。
pub fn compute_ic_by_bucket<F>(
    factor_values: &[(String, NaiveDate, f64)],
    forward_returns: &HashMap<(String, NaiveDate), f64>,
    bucket_key_fn: F,
) -> HashMap<String, IcResult>
where
    F: Fn(&str, NaiveDate) -> Option<String>,
{
    let mut buckets: HashMap<String, Vec<(String, NaiveDate, f64)>> = HashMap::new();
    for (symbol, date, fv) in factor_values {
        if let Some(key) = bucket_key_fn(symbol, *date) {
            buckets.entry(key).or_default().push((symbol.clone(), *date, *fv));
        }
    }
    buckets
        .into_iter()
        .map(|(key, fvs)| (key, compute_ic(&fvs, forward_returns)))
        .collect()
}

/// 取因子值(PIT:available_at <= trade_date 过滤,与 evaluate.rs 口径一致——
/// available_at 是因子值的公告可得日,晚于 trade_date 的属前瞻数据,必须剔除)。
pub async fn fetch_factor_values(
    db: &sqlx::PgPool,
    factor_code: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(String, NaiveDate, f64)>, String> {
    let rows: Vec<(String, NaiveDate, f64)> = sqlx::query_as(
        "SELECT symbol, trade_date, normalized_value::double precision
         FROM factor_value
         WHERE factor_code = $1 AND trade_date BETWEEN $2 AND $3
           AND normalized_value IS NOT NULL
           AND available_at <= trade_date
         ORDER BY trade_date",
    )
    .bind(factor_code)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch factor_values: {}", e))?;
    Ok(rows)
}

/// 取 h-day forward return:t 日收盘到 t+h 日收盘的收益(close_{t+h}/close_t - 1)。
/// key=(symbol, t日),value=(close_{t+h}-close_t)/close_t。
/// PIT:t+h 收盘价在 t+h 日已知,查询区间 end 需包含 t+h 才能取到 t 日的 forward return。
/// horizon=1 即单日 forward return(与原口径一致)。
pub async fn fetch_forward_returns(
    db: &sqlx::PgPool,
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
    horizon: usize,
) -> Result<HashMap<(String, NaiveDate), f64>, String> {
    // LEAD(close, horizon) 在分区末 h 行返回 NULL(无 t+h),用 Option<f64> 接收避免 sqlx 解码 NULL 报错。
    let rows: Vec<(String, NaiveDate, Option<f64>)> = sqlx::query_as(
        "SELECT symbol, trade_date,
         (LEAD(close, $4) OVER (PARTITION BY symbol ORDER BY trade_date) - close)::double precision / close AS fwd_ret
         FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1) AND trade_date BETWEEN $2 AND $3 AND close > 0",
    )
    .bind(symbols)
    .bind(start)
    .bind(end)
    .bind(horizon as i32)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch forward_returns(h={}): {}", horizon, e))?;
    Ok(rows
        .into_iter()
        .filter_map(|(s, d, r)| {
            // None(末 h 行 NULL)或 NaN 跳过;PIT 注释:t+h 收盘在 t+h 日已知,末日无 t+h 故丢弃
            r.filter(|v| v.is_finite()).map(|v| ((s, d), v))
        })
        .collect())
}

/// 取 symbols 在 date 的行业 + 市值(亿元),用于分桶。
/// industry 来自 market_stock 静态表;total_mv 取 trade_date <= date 最近值(万元→亿元,÷1e4)。
/// 返回 HashMap<symbol, (industry, market_cap_bucket)>。
pub async fn fetch_bucket_labels(
    db: &sqlx::PgPool,
    symbols: &[String],
    date: NaiveDate,
) -> Result<HashMap<String, (Option<String>, String)>, String> {
    let rows: Vec<(String, Option<String>, f64)> = sqlx::query_as(
        "SELECT DISTINCT ON (ms.symbol) ms.symbol, ms.industry,
         COALESCE(b.total_mv, 0)::double precision / 1e4 AS mv_yi
         FROM market_stock ms
         LEFT JOIN LATERAL (
             SELECT total_mv FROM market_stock_daily_basic
             WHERE symbol = ms.symbol AND trade_date <= $2 AND total_mv > 0
             ORDER BY trade_date DESC LIMIT 1
         ) b ON true
         WHERE ms.symbol = ANY($1)
         ORDER BY ms.symbol",
    )
    .bind(symbols)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch bucket_labels: {}", e))?;
    Ok(rows
        .into_iter()
        .map(|(s, ind, mv)| {
            let cap = if mv > 500.0 {
                "large"
            } else if mv > 100.0 {
                "mid"
            } else if mv > 0.0 {
                "small"
            } else {
                "unknown"
            };
            (s, (ind, cap.to_string()))
        })
        .collect())
}

/// Spearman rank IC:对 factor_value 和 forward_return 分别排名后算 Pearson。
fn spearman_rank_ic(pairs: &[(f64, f64)]) -> f64 {
    let ranks_fv = rank(pairs.iter().map(|(fv, _)| *fv).collect::<Vec<_>>().as_slice());
    let ranks_fr = rank(pairs.iter().map(|(_, fr)| *fr).collect::<Vec<_>>().as_slice());
    pearson_ic(&ranks_fv.iter().zip(ranks_fr.iter()).map(|(a, b)| (*a, *b)).collect::<Vec<_>>())
}

/// Pearson 相关系数。
fn pearson_ic(pairs: &[(f64, f64)]) -> f64 {
    let n = pairs.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let mean_x = mean(&pairs.iter().map(|(x, _)| *x).collect::<Vec<_>>());
    let mean_y = mean(&pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>());
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (x, y) in pairs {
        cov += (x - mean_x) * (y - mean_y);
        var_x += (x - mean_x).powi(2);
        var_y += (y - mean_y).powi(2);
    }
    let denom = (var_x * var_y).sqrt();
    if denom > 1e-12 { cov / denom } else { 0.0 }
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() { 0.0 } else { v.iter().sum::<f64>() / v.len() as f64 }
}

fn std(v: &[f64]) -> f64 {
    if v.len() < 2 { return 0.0; }
    let m = mean(v);
    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
}

/// 简单排名(平均秩处理 ties)。
fn rank(values: &[f64]) -> Vec<f64> {
    let mut indexed: Vec<(usize, f64)> = values.iter().enumerate().map(|(i, &v)| (i, v)).collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0; values.len()];
    let mut i = 0;
    while i < indexed.len() {
        let mut j = i;
        while j + 1 < indexed.len() && indexed[j + 1].1 == indexed[i].1 {
            j += 1;
        }
        let avg_rank = (i + j) as f64 / 2.0 + 1.0;
        for k in i..=j {
            ranks[indexed[k].0] = avg_rank;
        }
        i = j + 1;
    }
    ranks
}

// ===== 分桶 IC API handler =====

/// 分桶 IC 分析请求。
#[derive(Debug, serde::Deserialize)]
pub struct BucketedIcRequest {
    pub factor_code: String,
    pub start_date: String,
    pub end_date: String,
    /// 分桶日期(取该日持仓截面分桶;缺省用 start_date)。
    pub bucket_date: Option<String>,
    /// forward return horizon 列表(单位:交易日);缺省 [1]。
    /// 多个 horizon 时产出 decay_curve,overall/分桶仍用 horizons[0]。
    pub horizons: Option<Vec<usize>>,
}

/// POST /api/v1/quant/factors/analysis/bucketed-ic
pub async fn bucketed_ic_analysis(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BucketedIcRequest>,
) -> impl IntoResponse {
    match run_bucketed_ic(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})).into_response(),
        Err(e) => Json(json!({"code": 1, "message": e})).into_response(),
    }
}

/// 解析日期(支持 YYYYMMDD 或 YYYY-MM-DD)。
fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let s = s.trim();
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y%m%d") {
        return Ok(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d);
    }
    Err(format!("日期格式错误: {} (需 YYYYMMDD 或 YYYY-MM-DD)", s))
}

/// 分桶 IC 分析核心:取因子值→forward_returns→全市场IC→分桶 labels→按 industry/market_cap 分桶。
/// horizons 多于 1 时额外产出 decay_curve(各 horizon 的全市场 IC)。
async fn run_bucketed_ic(db: &sqlx::PgPool, req: BucketedIcRequest) -> Result<Value, String> {
    let start = parse_date(&req.start_date)?;
    let end = parse_date(&req.end_date)?;
    let bucket_date = match req.bucket_date.as_deref() {
        Some(d) => parse_date(d)?,
        None => start,
    };

    // horizons 去重排序(缺省 [1]);overall/分桶用首个 horizon
    let mut horizons: Vec<usize> = req.horizons.clone().unwrap_or_else(|| vec![1]);
    horizons.sort_unstable();
    horizons.dedup();
    if horizons.is_empty() {
        horizons = vec![1];
    }
    let primary_h = horizons[0];

    // 1. 因子值
    let fvs = fetch_factor_values(db, &req.factor_code, start, end).await?;
    if fvs.is_empty() {
        return Err(format!("因子 {} 在 {}~{} 无数据", req.factor_code, start, end));
    }
    // symbols 用 HashSet 去重(factor_values 里同 symbol 多日)
    let symbols: Vec<String> = fvs
        .iter()
        .map(|(s, _, _)| s.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // 2. forward_returns(primary horizon,用于 overall + 分桶)
    let forward_returns = fetch_forward_returns(db, &symbols, start, end, primary_h).await?;

    // 3. 全市场 IC
    let overall = compute_ic(&fvs, &forward_returns);

    // 4. 分桶(用 bucket_date 的行业+市值标签,简化:全周期用同一日标签)
    let labels = fetch_bucket_labels(db, &symbols, bucket_date).await?;

    // 5. 按 industry 分桶
    let by_industry = compute_ic_by_bucket(&fvs, &forward_returns, |sym, _| {
        labels.get(sym).and_then(|(ind, _)| ind.clone())
    });

    // 6. 按 market_cap 分桶
    let by_market_cap = compute_ic_by_bucket(&fvs, &forward_returns, |sym, _| {
        labels.get(sym).map(|(_, cap)| cap.clone())
    });

    // 7. decay_curve(多 horizon 时产出:各 horizon 的全市场 IC + 半衰期估计)
    let decay_curve = if horizons.len() > 1 {
        let mut entries: Vec<Value> = Vec::with_capacity(horizons.len());
        for h in &horizons {
            // primary horizon 已算过,复用避免重复查询
            let ic = if *h == primary_h {
                overall.clone()
            } else {
                let fr = fetch_forward_returns(db, &symbols, start, end, *h).await?;
                compute_ic(&fvs, &fr)
            };
            entries.push(json!({
                "horizon": h,
                "mean_ic": ic.mean_ic,
                "ic_ir": ic.ic_ir,
                "mean_rank_ic": ic.mean_rank_ic,
                "rank_ic_ir": ic.rank_ic_ir,
                "n_obs": ic.n_obs,
            }));
        }
        // 半衰期:rank_ic 衰减到首日一半的 horizon(线性插值,粗估)
        let half_life = estimate_half_life(&entries);
        json!({ "entries": entries, "half_life_horizon": half_life })
    } else {
        json!(null)
    };

    Ok(json!({
        "factor_code": req.factor_code,
        "period": { "start": start.to_string(), "end": end.to_string() },
        "horizon": primary_h,
        "n_symbols": symbols.len(),
        "overall_ic": overall,
        "by_industry": by_industry,
        "by_market_cap": by_market_cap,
        "decay_curve": decay_curve,
    }))
}

/// 估计 IC 半衰期:rank_ic 衰减到首 horizon 一半时的 horizon(线性插值)。
/// 无衰减或反向衰减返回 None。
fn estimate_half_life(entries: &[Value]) -> Option<f64> {
    if entries.len() < 2 {
        return None;
    }
    let points: Vec<(f64, f64)> = entries
        .iter()
        .filter_map(|e| {
            let h = e.get("horizon")?.as_f64()?;
            let ic = e.get("mean_rank_ic")?.as_f64()?;
            Some((h, ic))
        })
        .collect();
    if points.len() < 2 {
        return None;
    }
    let (_, ic0) = points[0];
    if ic0.abs() < 1e-9 {
        return None; // 首 horizon IC 近零,无法定义半衰期
    }
    let target = ic0 / 2.0;
    // 找 rank_ic 跨越 target 的相邻点,线性插值
    for w in points.windows(2) {
        let (h1, ic1) = w[0];
        let (h2, ic2) = w[1];
        // ic 递减场景:ic1 >= target >= ic2
        if (ic1 - target) * (ic2 - target) <= 0.0 && (ic2 - ic1).abs() > 1e-12 {
            let frac = (target - ic1) / (ic2 - ic1);
            return Some(h1 + frac * (h2 - h1));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pearson_ic_perfect_positive() {
        let pairs = vec![(1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)];
        let ic = pearson_ic(&pairs);
        assert!((ic - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_pearson_ic_perfect_negative() {
        let pairs = vec![(1.0, 8.0), (2.0, 6.0), (3.0, 4.0), (4.0, 2.0)];
        let ic = pearson_ic(&pairs);
        assert!((ic - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn test_rank_ties() {
        let r = rank(&[3.0, 1.0, 2.0, 1.0]);
        // 1.0 出现两次,平均秩 (1+2)/2=1.5;2.0 秩 3;3.0 秩 4
        assert!((r[0] - 4.0).abs() < 1e-9);
        assert!((r[1] - 1.5).abs() < 1e-9);
        assert!((r[2] - 3.0).abs() < 1e-9);
        assert!((r[3] - 1.5).abs() < 1e-9);
    }

    #[test]
    fn test_compute_ic_basic() {
        let fvs = vec![
            ("A".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 1.0),
            ("B".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 2.0),
            ("C".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 3.0),
            ("D".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 4.0),
            ("E".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 5.0),
        ];
        let mut fr = HashMap::new();
        fr.insert(("A".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.01);
        fr.insert(("B".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.02);
        fr.insert(("C".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.03);
        fr.insert(("D".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.04);
        fr.insert(("E".into(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()), 0.05);
        let r = compute_ic(&fvs, &fr);
        assert!((r.mean_ic - 1.0).abs() < 1e-9); // 完美正相关
        assert_eq!(r.n_obs, 5);
    }
}
