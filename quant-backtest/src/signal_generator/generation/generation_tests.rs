//! Unit tests for the pure (non-IO) logic in `generation.rs`.
//!
//! These tests intentionally avoid `PgPool`, `SignalDataCache`, and any async
//! orchestration: they only exercise deterministic score/rank/blend helpers.

use super::*;

fn d(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
}

fn rows(pairs: &[(&str, f64)]) -> Vec<(String, f64)> {
    pairs
        .iter()
        .map(|(symbol, score)| (symbol.to_string(), *score))
        .collect()
}

fn prediction_rows(pairs: &[(&str, f64, Option<i32>)]) -> Vec<(String, f64, Option<i32>)> {
    pairs
        .iter()
        .map(|(symbol, score, rank)| (symbol.to_string(), *score, *rank))
        .collect()
}

fn overlay_config(
    combo_name: &str,
    version: &str,
    weight: f64,
    score_direction: ScoreDirection,
) -> FactorScoreOverlayConfig {
    FactorScoreOverlayConfig {
        combo_name: combo_name.to_string(),
        version: version.to_string(),
        weight,
        score_direction,
    }
}

fn sleeve_config(
    combo_name: &str,
    version: &str,
    weight: f64,
    score_direction: ScoreDirection,
) -> FactorPortfolioSleeveConfig {
    FactorPortfolioSleeveConfig {
        combo_name: combo_name.to_string(),
        version: version.to_string(),
        weight,
        score_direction,
    }
}

fn event_gate(mode: EventGateMode, min_score: f64, boost_weight: f64) -> EventGateConfig {
    EventGateConfig {
        combo_name: "event_combo".to_string(),
        version: "1.0.0".to_string(),
        mode,
        score_direction: ScoreDirection::Descending,
        min_score,
        boost_weight,
        active_regimes: Vec::new(),
    }
}

fn blend_config(
    factor_weight: f64,
    prediction_weight: f64,
    prediction_min_percentile: Option<f64>,
    prediction_min_score: Option<f64>,
) -> PredictionBlendConfig {
    PredictionBlendConfig {
        prediction_set_id: "test-set".to_string(),
        factor_weight,
        prediction_weight,
        prediction_min_percentile,
        prediction_min_score,
    }
}

fn minimal_regime_policy(rules: HashMap<MarketRegime, RegimeSignalRule>) -> MarketRegimePolicy {
    MarketRegimePolicy {
        benchmark: "000300.SH".to_string(),
        lookback_days: 63,
        min_observations: 40,
        high_volatility_threshold: 0.20,
        bear_return_threshold: -0.10,
        bear_drawdown_threshold: 0.15,
        bull_return_threshold: 0.10,
        bull_max_drawdown: 0.10,
        sideways_volatility_threshold: 0.10,
        sideways_abs_return_threshold: 0.05,
        rules,
    }
}

// ---------------------------------------------------------------------------
// score_stats
// ---------------------------------------------------------------------------

#[test]
fn score_stats_empty_input_returns_safe_defaults() {
    // 业务含义:截面无有效分数时返回 (mean=0, std=1),保证下游 z-score 分母不为 0。
    let stats = score_stats(std::iter::empty());
    assert_close(stats.0, 0.0);
    assert_close(stats.1, 1.0);
}

#[test]
fn score_stats_constant_values_degrade_to_unit_stddev() {
    // 业务含义:所有分数相同(零方差)时 std 退化为 1,避免除零。
    let stats = score_stats([5.0, 5.0, 5.0].into_iter());
    assert_close(stats.0, 5.0);
    assert_close(stats.1, 1.0);
}

#[test]
fn score_stats_computes_population_mean_and_stddev() {
    // 总体方差(除以 n,而非 n-1):[1,2,3] -> mean 2, var 2/3, std sqrt(2/3)。
    let stats = score_stats([1.0, 2.0, 3.0].into_iter());
    assert_close(stats.0, 2.0);
    assert_close(stats.1, (2.0_f64 / 3.0).sqrt());
}

#[test]
fn score_stats_ignores_non_finite_values() {
    // 业务含义:NaN/Inf 分数不参与截面统计,防止污染 mean/std。
    let stats = score_stats([1.0, f64::NAN, 2.0, f64::INFINITY].into_iter());
    assert_close(stats.0, 1.5);
    assert_close(stats.1, 0.5);
}

// ---------------------------------------------------------------------------
// standard_score / oriented_standard_score
// ---------------------------------------------------------------------------

#[test]
fn standard_score_computes_z_score() {
    // z = (value - mean) / std。
    assert_close(standard_score(3.0, (2.0, 0.5)), 2.0);
}

#[test]
fn standard_score_at_mean_is_zero_and_below_mean_is_negative() {
    // 方向性:等于均值 → 0;低于均值 → 负 z 分。
    assert_close(standard_score(2.0, (2.0, 0.5)), 0.0);
    assert!(standard_score(1.0, (2.0, 0.5)) < 0.0);
}

#[test]
fn oriented_standard_score_descending_keeps_raw_z() {
    // Descending:高分更好,z 分原样输出。
    assert_close(
        oriented_standard_score(3.0, (2.0, 1.0), ScoreDirection::Descending),
        1.0,
    );
}

#[test]
fn oriented_standard_score_ascending_negates_z() {
    // Ascending:低分更好,z 分取负,使"越小越好"统一编码为"越大越好"。
    assert_close(
        oriented_standard_score(3.0, (2.0, 1.0), ScoreDirection::Ascending),
        -1.0,
    );
}

#[test]
fn oriented_standard_score_ascending_flips_cross_sectional_ranking() {
    // 方向性:同一截面上,Ascending 使低分值获得更高的 oriented score。
    let stats = score_stats([1.0, 2.0, 3.0].into_iter());
    let low = oriented_standard_score(1.0, stats, ScoreDirection::Ascending);
    let high = oriented_standard_score(3.0, stats, ScoreDirection::Ascending);
    assert!(low > high);
}

// ---------------------------------------------------------------------------
// prediction_percentiles_by_symbol
// ---------------------------------------------------------------------------

#[test]
fn prediction_percentiles_rank_by_ascending_score() {
    // 业务含义:预测分数越高百分位越高(0=最低,1=最高),供 min_percentile 过滤。
    let percentiles =
        prediction_percentiles_by_symbol([("ccc", 30.0), ("aaa", 10.0), ("bbb", 20.0)].into_iter());
    let (_, aaa_percentile) = *percentiles.get("aaa").expect("aaa present");
    let (_, bbb_percentile) = *percentiles.get("bbb").expect("bbb present");
    let (_, ccc_percentile) = *percentiles.get("ccc").expect("ccc present");
    assert_close(aaa_percentile, 0.0);
    assert_close(bbb_percentile, 0.5);
    assert_close(ccc_percentile, 1.0);
}

#[test]
fn prediction_percentiles_single_element_gets_zero_percentile() {
    // 边界:单元素截面 denominator 退化为 1,唯一元素百分位为 0(不做除零)。
    let percentiles = prediction_percentiles_by_symbol([("aaa", 10.0)].into_iter());
    let (score, percentile) = *percentiles.get("aaa").expect("aaa present");
    assert_close(score, 10.0);
    assert_close(percentile, 0.0);
}

#[test]
fn prediction_percentiles_ignore_non_finite_scores() {
    // NaN/Inf 预测分不参与百分位,剩余有效分数重新排名。
    let percentiles = prediction_percentiles_by_symbol(
        [("aaa", f64::NAN), ("bbb", 20.0), ("ccc", f64::INFINITY)].into_iter(),
    );
    assert_eq!(percentiles.len(), 1);
    let (score, percentile) = *percentiles.get("bbb").expect("bbb present");
    assert_close(score, 20.0);
    assert_close(percentile, 0.0);
}

#[test]
fn prediction_percentiles_empty_input_returns_empty_map() {
    let percentiles = prediction_percentiles_by_symbol(std::iter::empty());
    assert!(percentiles.is_empty());
}

// ---------------------------------------------------------------------------
// sort_factor_scores
// ---------------------------------------------------------------------------

#[test]
fn sort_factor_scores_descending_orders_high_scores_first() {
    // 方向性:Descending = 高分优先(默认打分方向)。
    let mut items = rows(&[("LOW", 1.0), ("HIGH", 9.0), ("MID", 5.0)]);
    sort_factor_scores(&mut items, ScoreDirection::Descending);
    assert_eq!(items[0].0, "HIGH");
    assert_eq!(items[1].0, "MID");
    assert_eq!(items[2].0, "LOW");
}

#[test]
fn sort_factor_scores_ascending_orders_low_scores_first() {
    // 方向性:Ascending = 低分优先(反转类因子,如估值因子)。
    let mut items = rows(&[("LOW", 1.0), ("HIGH", 9.0), ("MID", 5.0)]);
    sort_factor_scores(&mut items, ScoreDirection::Ascending);
    assert_eq!(items[0].0, "LOW");
    assert_eq!(items[1].0, "MID");
    assert_eq!(items[2].0, "HIGH");
}

#[test]
fn sort_factor_scores_breaks_score_ties_by_symbol_asc() {
    // 边界:并列分数按 symbol 字典序,保证排序确定性。
    let mut items = rows(&[("BBB", 1.0), ("AAA", 1.0), ("CCC", 2.0)]);
    sort_factor_scores(&mut items, ScoreDirection::Descending);
    assert_eq!(items[0].0, "CCC");
    assert_eq!(items[1].0, "AAA");
    assert_eq!(items[2].0, "BBB");
}

// ---------------------------------------------------------------------------
// sort_prediction_scores
// ---------------------------------------------------------------------------

#[test]
fn sort_prediction_scores_descending_orders_by_score_then_rank_then_symbol() {
    // 排序键优先级:分数降序 → rank 升序(预测集自带排名为次序权威)→ symbol 字典序。
    let mut scores = HashMap::from([(
        d(2026, 1, 5),
        prediction_rows(&[
            ("AAA", 2.0, Some(2)),
            ("BBB", 2.0, Some(1)),
            ("CCC", 3.0, None),
        ]),
    )]);
    sort_prediction_scores(&mut scores, ScoreDirection::Descending);
    let day_rows = scores.get(&d(2026, 1, 5)).unwrap();
    assert_eq!(day_rows[0].0, "CCC"); // 3.0 最高分
    assert_eq!(day_rows[1].0, "BBB"); // 并列 2.0,rank 1 < 2
    assert_eq!(day_rows[2].0, "AAA");
}

#[test]
fn sort_prediction_scores_ascending_orders_low_scores_first() {
    let mut scores = HashMap::from([(
        d(2026, 1, 5),
        prediction_rows(&[("AAA", 2.0, None), ("BBB", 1.0, None), ("CCC", 3.0, None)]),
    )]);
    sort_prediction_scores(&mut scores, ScoreDirection::Ascending);
    let day_rows = scores.get(&d(2026, 1, 5)).unwrap();
    assert_eq!(day_rows[0].0, "BBB");
    assert_eq!(day_rows[1].0, "AAA");
    assert_eq!(day_rows[2].0, "CCC");
}

#[test]
fn sort_prediction_scores_missing_rank_sorts_after_explicit_ranks() {
    // 边界:rank 缺失视为 i32::MAX,同分时排在所有显式 rank 之后。
    let mut scores = HashMap::from([(
        d(2026, 1, 5),
        prediction_rows(&[("NO_RANK", 2.0, None), ("RANKED", 2.0, Some(5))]),
    )]);
    sort_prediction_scores(&mut scores, ScoreDirection::Descending);
    let day_rows = scores.get(&d(2026, 1, 5)).unwrap();
    assert_eq!(day_rows[0].0, "RANKED");
    assert_eq!(day_rows[1].0, "NO_RANK");
}

#[test]
fn sort_prediction_scores_empty_map_is_noop() {
    let mut scores: HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>> = HashMap::new();
    sort_prediction_scores(&mut scores, ScoreDirection::Descending);
    assert!(scores.is_empty());
}

// ---------------------------------------------------------------------------
// prediction_load_start_date
// ---------------------------------------------------------------------------

#[test]
fn prediction_load_start_date_zero_delay_adds_30_calendar_day_buffer() {
    // 业务含义:即使无入场延迟,也向前多拉 30 天预测,容忍交易日/日历日错位。
    let start = prediction_load_start_date(d(2026, 6, 1), 0);
    assert_eq!(start, d(2026, 5, 2));
}

#[test]
fn prediction_load_start_date_scales_buffer_with_entry_delay() {
    // 缓冲 = 30 + 3 * entry_delay_days 个日历日(1 交易日 ≈ 3 日历日上限的保守估计)。
    let start = prediction_load_start_date(d(2026, 6, 1), 10);
    assert_eq!(start, d(2026, 4, 2));
}

// ---------------------------------------------------------------------------
// blend_factor_prediction_scores
// ---------------------------------------------------------------------------

#[test]
fn blend_factor_prediction_scores_drops_dates_without_predictions() {
    // 业务含义:因子分与预测分必须同日配对,无预测数据的日期整体剔除。
    let day_with = d(2026, 1, 5);
    let day_without = d(2026, 1, 6);
    let mut factor_scores = HashMap::from([
        (day_with, rows(&[("AAA", 1.0), ("BBB", 3.0)])),
        (day_without, rows(&[("AAA", 2.0)])),
    ]);
    let prediction_scores = HashMap::from([(
        day_with,
        prediction_rows(&[("AAA", 10.0, None), ("BBB", 20.0, None)]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(1.0, 1.0, None, None),
        ScoreDirection::Descending,
    );
    assert!(factor_scores.contains_key(&day_with));
    assert!(!factor_scores.contains_key(&day_without));
}

#[test]
fn blend_factor_prediction_scores_zero_gross_weight_keeps_scores_unchanged() {
    // 边界:两侧权重均为 0 时提前返回,原分数保持不变。
    let day = d(2026, 1, 5);
    let original = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let mut factor_scores = HashMap::from([(day, original.clone())]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[("AAA", 10.0, None), ("BBB", 20.0, None)]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(0.0, 0.0, None, None),
        ScoreDirection::Descending,
    );
    assert_eq!(factor_scores.get(&day), Some(&original));
}

#[test]
fn blend_factor_prediction_scores_normalizes_weights_and_blends_z_scores() {
    // 权重归一化:factor:prediction = 2:1 -> 2/3 : 1/3,两侧各自截面标准化后线性混合。
    // factor z: AAA=-1, BBB=+1 (mean 2, std 1); prediction z: AAA=-1, BBB=+1 (mean 15, std 5)
    // blended: AAA = 2/3*(-1)+1/3*(-1) = -1; BBB = +1。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[("AAA", 10.0, None), ("BBB", 20.0, None)]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(2.0, 1.0, None, None),
        ScoreDirection::Descending,
    );
    let blended = factor_scores.get(&day).expect("day retained");
    assert_eq!(blended.len(), 2);
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -1.0);
    assert_close(by_symbol["BBB"], 1.0);
}

#[test]
fn blend_factor_prediction_scores_ascending_negates_prediction_z_only() {
    // 方向性:Ascending 只翻转预测侧贡献,因子侧 z 分保持原号。
    // AAA = 2/3*(-1) + 1/3*(+1) = -1/3; BBB = 2/3*(+1) + 1/3*(-1) = +1/3。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[("AAA", 10.0, None), ("BBB", 20.0, None)]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(2.0, 1.0, None, None),
        ScoreDirection::Ascending,
    );
    let blended = factor_scores.get(&day).expect("day retained");
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -1.0 / 3.0);
    assert_close(by_symbol["BBB"], 1.0 / 3.0);
}

#[test]
fn blend_factor_prediction_scores_applies_min_percentile_filter() {
    // 业务含义:prediction_min_percentile 只保留预测百分位达标的票
    // (AAA=0.0, BBB=0.5, CCC=1.0;阈值 0.5 剔除 AAA),且 z 分基于幸存者重算。
    // 幸存者 factor [2,3] -> mean 2.5 std 0.5; prediction [20,30] -> mean 25 std 5;
    // 等权混合: BBB = 0.5*(-1)+0.5*(-1) = -1; CCC = +1。
    let day = d(2026, 1, 5);
    let mut factor_scores =
        HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 2.0), ("CCC", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[
            ("AAA", 10.0, None),
            ("BBB", 20.0, None),
            ("CCC", 30.0, None),
        ]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(1.0, 1.0, Some(0.5), None),
        ScoreDirection::Descending,
    );
    let blended = factor_scores.get(&day).expect("day retained");
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert!(!by_symbol.contains_key("AAA"));
    assert_close(by_symbol["BBB"], -1.0);
    assert_close(by_symbol["CCC"], 1.0);
}

#[test]
fn blend_factor_prediction_scores_applies_min_raw_score_filter() {
    // 业务含义:prediction_min_score 按原始预测分(非 z 分)过滤,AAA(10) 低于 15 被剔除。
    let day = d(2026, 1, 5);
    let mut factor_scores =
        HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 2.0), ("CCC", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[
            ("AAA", 10.0, None),
            ("BBB", 20.0, None),
            ("CCC", 30.0, None),
        ]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(1.0, 1.0, None, Some(15.0)),
        ScoreDirection::Descending,
    );
    let blended = factor_scores.get(&day).expect("day retained");
    let symbols: Vec<&str> = blended.iter().map(|(s, _)| s.as_str()).collect();
    assert!(!symbols.contains(&"AAA"));
    assert!(symbols.contains(&"BBB"));
    assert!(symbols.contains(&"CCC"));
}

#[test]
fn blend_factor_prediction_scores_drops_date_when_no_pairs_survive_filters() {
    // 边界:过滤后配对为空 -> 该日期从因子分中剔除。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[("AAA", 10.0, None), ("BBB", 20.0, None)]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(1.0, 1.0, None, Some(100.0)),
        ScoreDirection::Descending,
    );
    assert!(!factor_scores.contains_key(&day));
}

#[test]
fn blend_factor_prediction_scores_skips_non_finite_scores() {
    // NaN 因子分或 NaN/Inf 预测分的票不参与混合(预测侧非有限在百分位阶段已被剔除)。
    let day = d(2026, 1, 5);
    let mut factor_scores =
        HashMap::from([(day, rows(&[("AAA", f64::NAN), ("BBB", 1.0), ("CCC", 3.0)]))]);
    let prediction_scores = HashMap::from([(
        day,
        prediction_rows(&[
            ("AAA", 10.0, None),
            ("BBB", f64::INFINITY, None),
            ("CCC", 20.0, None),
        ]),
    )]);
    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend_config(1.0, 1.0, None, None),
        ScoreDirection::Descending,
    );
    let blended = factor_scores.get(&day).expect("day retained");
    let symbols: Vec<&str> = blended.iter().map(|(s, _)| s.as_str()).collect();
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0], "CCC");
}

// ---------------------------------------------------------------------------
// apply_event_gate_scores_when / apply_event_gate_scores /
// apply_event_gate_scores_for_regime
// ---------------------------------------------------------------------------

#[test]
fn event_gate_boost_positive_adds_weighted_z_for_above_min_event_scores() {
    // 业务含义:BoostPositive 只给 event 分 > min_score 的票加分,
    // 加分量 = boost_weight * event 截面 z 分;event 缺席/不达标的票分数不变。
    // event [10,-5] -> mean 2.5, std 7.5 -> z(10)=+1, z(-5)=-1。
    let day = d(2026, 1, 5);
    let mut factor_scores =
        HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 1.0), ("CCC", 1.0)]))]);
    let event_scores = HashMap::from([(day, rows(&[("AAA", 10.0), ("BBB", -5.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::BoostPositive, 0.0, 0.5),
    );
    let gated = factor_scores.get(&day).expect("day retained");
    let by_symbol: HashMap<&str, f64> = gated
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], 1.5); // 1.0 + 0.5 * 1.0
    assert_close(by_symbol["BBB"], 1.0); // event -5 <= min 0,不加成
    assert_close(by_symbol["CCC"], 1.0); // 无 event 数据,不加成
}

#[test]
fn event_gate_boost_positive_stats_exclude_out_of_pool_symbols() {
    // 回归锁定(2026-09-18 定版): z 加成的截面统计仅按因子池内票计算。
    // event 表加入 3 只池外中性票(不在因子池, 无 factor row):
    //   池内口径 [10,-5] -> mean 2.5, std 7.5 -> z(10)=+1 -> AAA=1.5;
    //   全市场口径 [10,-5,0.5,0.5,0.5] -> mean 1.3, std≈4.06 -> z≈2.14 -> AAA≈2.07(稀释性放大)。
    // 断言 1.5 即证明池外票未参与统计——event 覆盖面远大于因子池时加成不被扭曲。
    let day = d(2026, 1, 5);
    let mut factor_scores =
        HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 1.0), ("CCC", 1.0)]))]);
    let event_scores = HashMap::from([(
        day,
        rows(&[
            ("AAA", 10.0),
            ("BBB", -5.0),
            ("OUT1", 0.5),
            ("OUT2", 0.5),
            ("OUT3", 0.5),
        ]),
    )]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::BoostPositive, 0.0, 0.5),
    );
    let gated = factor_scores.get(&day).expect("day retained");
    let by_symbol: HashMap<&str, f64> = gated
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], 1.5);
    assert_close(by_symbol["BBB"], 1.0);
    assert_close(by_symbol["CCC"], 1.0);
    // 池外票不应被加入因子池
    assert_eq!(gated.len(), 3);
}

#[test]
fn event_gate_boost_positive_zero_boost_weight_is_noop() {
    // 边界:boost_weight = 0 时 BoostPositive 不改任何分数,日期保留。
    let day = d(2026, 1, 5);
    let original = rows(&[("AAA", 1.0), ("BBB", 2.0)]);
    let mut factor_scores = HashMap::from([(day, original.clone())]);
    let event_scores = HashMap::from([(day, rows(&[("AAA", 10.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::BoostPositive, 0.0, 0.0),
    );
    assert_eq!(factor_scores.get(&day), Some(&original));
}

#[test]
fn event_gate_exclude_negative_keeps_symbols_at_or_above_min() {
    // 业务含义:ExcludeNegative 剔除 event 分 < min_score 的票;
    // 缺 event 数据的票保留(unwrap_or(true)),等于阈值(>=)的票保留。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(
        day,
        rows(&[
            ("AT_MIN", 1.0),
            ("BELOW", 2.0),
            ("NO_EVENT", 3.0),
            ("ABOVE", 4.0),
        ]),
    )]);
    let event_scores = HashMap::from([(
        day,
        rows(&[("AT_MIN", 2.0), ("BELOW", 1.0), ("ABOVE", 3.0)]),
    )]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::ExcludeNegative, 2.0, 0.0),
    );
    let gated = factor_scores.get(&day).expect("day retained");
    let symbols: Vec<&str> = gated.iter().map(|(s, _)| s.as_str()).collect();
    assert_eq!(symbols.len(), 3);
    assert!(symbols.contains(&"AT_MIN"));
    assert!(symbols.contains(&"NO_EVENT"));
    assert!(symbols.contains(&"ABOVE"));
    assert!(!symbols.contains(&"BELOW"));
}

#[test]
fn event_gate_exclude_negative_keeps_all_symbols_when_event_date_missing() {
    // 边界:event 分整个日期缺失 -> 全部票视为无事件,一律保留。
    let day = d(2026, 1, 5);
    let original = rows(&[("AAA", 1.0), ("BBB", 2.0)]);
    let mut factor_scores = HashMap::from([(day, original.clone())]);
    let event_scores: FactorScoresByDate = HashMap::new();
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::ExcludeNegative, 0.0, 0.0),
    );
    assert_eq!(factor_scores.get(&day), Some(&original));
}

#[test]
fn event_gate_require_positive_keeps_only_strictly_above_min() {
    // 业务含义:RequirePositive 要求 event 分严格 > min_score;
    // 等于阈值与无 event 数据的票都被剔除(比 ExcludeNegative 更激进的白名单模式)。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(
        day,
        rows(&[("AT_MIN", 1.0), ("NO_EVENT", 2.0), ("ABOVE", 3.0)]),
    )]);
    let event_scores = HashMap::from([(day, rows(&[("AT_MIN", 2.0), ("ABOVE", 3.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::RequirePositive, 2.0, 0.0),
    );
    let gated = factor_scores.get(&day).expect("day retained");
    assert_eq!(gated.len(), 1);
    assert_eq!(gated[0].0, "ABOVE");
}

#[test]
fn event_gate_require_positive_drops_date_when_no_rows_survive() {
    // 边界:模式剔除后该日期行为空 -> 日期从因子分中删除。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 2.0)]))]);
    let event_scores = HashMap::from([(day, rows(&[("AAA", 0.5), ("BBB", 1.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::RequirePositive, 2.0, 0.0),
    );
    assert!(!factor_scores.contains_key(&day));
}

#[test]
fn event_gate_non_finite_min_score_defaults_to_zero() {
    // 边界:min_score = NaN(非有限)时按 0 处理,而不是整体失效。
    let day = d(2026, 1, 5);
    let mut factor_scores = HashMap::from([(day, rows(&[("NEG", 1.0), ("POS", 2.0)]))]);
    let event_scores = HashMap::from([(day, rows(&[("NEG", -1.0), ("POS", 1.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::ExcludeNegative, f64::NAN, 0.0),
    );
    let gated = factor_scores.get(&day).expect("day retained");
    let symbols: Vec<&str> = gated.iter().map(|(s, _)| s.as_str()).collect();
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0], "POS");
}

#[test]
fn event_gate_negative_boost_weight_clamped_to_zero() {
    // 边界:负 boost_weight 被 max(0.0) 钳制,BoostPositive 退化为不加成。
    let day = d(2026, 1, 5);
    let original = rows(&[("AAA", 1.0)]);
    let mut factor_scores = HashMap::from([(day, original.clone())]);
    let event_scores = HashMap::from([(day, rows(&[("AAA", 10.0)]))]);
    apply_event_gate_scores(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::BoostPositive, 0.0, -1.0),
    );
    assert_eq!(factor_scores.get(&day), Some(&original));
}

#[test]
fn event_gate_when_skips_inactive_dates_entirely() {
    // 业务含义:active_for_date(date) = false 的日期不做任何门控(原样保留),
    // 供 regime 条件门控按市场状态启停。
    let active_day = d(2026, 1, 5);
    let inactive_day = d(2026, 1, 6);
    let mut factor_scores = HashMap::from([
        (active_day, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
        (inactive_day, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
    ]);
    let event_scores = HashMap::from([
        (active_day, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
        (inactive_day, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
    ]);
    apply_event_gate_scores_when(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::RequirePositive, 0.0, 0.0),
        |date| date == active_day,
    );
    let active_rows = factor_scores.get(&active_day).expect("active day kept");
    assert_eq!(active_rows.len(), 1);
    assert_eq!(active_rows[0].0, "GOOD");
    let inactive_rows = factor_scores.get(&inactive_day).expect("inactive day kept");
    assert_eq!(inactive_rows.len(), 2);
}

#[test]
fn event_gate_for_regime_applies_only_in_active_regimes() {
    // 业务含义:active_regimes 非空时,只在列出的 regime 日期应用门控;
    // 其它 regime 的日期原样通过。
    let bear_day = d(2026, 1, 5);
    let bull_day = d(2026, 1, 6);
    let mut factor_scores = HashMap::from([
        (bear_day, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
        (bull_day, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
    ]);
    let event_scores = HashMap::from([
        (bear_day, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
        (bull_day, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
    ]);
    let mut gate = event_gate(EventGateMode::RequirePositive, 0.0, 0.0);
    gate.active_regimes = vec![MarketRegime::Bear];
    apply_event_gate_scores_for_regime(&mut factor_scores, &event_scores, &gate, |date| {
        if date == bear_day {
            MarketRegime::Bear
        } else {
            MarketRegime::Bull
        }
    });
    let bear_rows = factor_scores.get(&bear_day).expect("bear day kept");
    assert_eq!(bear_rows.len(), 1);
    assert_eq!(bear_rows[0].0, "GOOD");
    let bull_rows = factor_scores.get(&bull_day).expect("bull day kept");
    assert_eq!(bull_rows.len(), 2);
}

#[test]
fn event_gate_for_regime_empty_active_regimes_applies_everywhere() {
    // 边界:active_regimes 为空 -> 门控无条件应用于所有日期。
    let day_one = d(2026, 1, 5);
    let day_two = d(2026, 1, 6);
    let mut factor_scores = HashMap::from([
        (day_one, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
        (day_two, rows(&[("BAD", 1.0), ("GOOD", 2.0)])),
    ]);
    let event_scores = HashMap::from([
        (day_one, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
        (day_two, rows(&[("BAD", -1.0), ("GOOD", 1.0)])),
    ]);
    apply_event_gate_scores_for_regime(
        &mut factor_scores,
        &event_scores,
        &event_gate(EventGateMode::RequirePositive, 0.0, 0.0),
        |_| MarketRegime::Sideways,
    );
    for day in [day_one, day_two] {
        let rows = factor_scores.get(&day).expect("day kept");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "GOOD");
    }
}

// ---------------------------------------------------------------------------
// score_source_config_for_overlay / score_source_config_for_portfolio_sleeve
// ---------------------------------------------------------------------------

#[test]
fn score_source_config_for_overlay_overrides_source_fields_and_clears_recursion() {
    // 业务含义:overlay 源 = base 换 combo/version/direction,并清空 overlay/sleeve
    // 字段防止嵌套展开;其余组合参数(top_n 等)原样继承。
    let base = SignalConfig {
        top_n: 7,
        ..Default::default()
    };
    let config = score_source_config_for_overlay(
        &base,
        &overlay_config("overlay_combo", "2.0.0", 0.3, ScoreDirection::Ascending),
    );
    assert_eq!(config.combo_name, "overlay_combo");
    assert_eq!(config.version, "2.0.0");
    assert_eq!(config.score_direction, ScoreDirection::Ascending);
    assert!(config.score_overlay.is_none());
    assert!(config.portfolio_sleeve.is_none());
    assert_eq!(config.top_n, 7);
}

#[test]
fn score_source_config_for_portfolio_sleeve_overrides_source_fields_and_clears_recursion() {
    let base = SignalConfig {
        top_n: 7,
        ..Default::default()
    };
    let config = score_source_config_for_portfolio_sleeve(
        &base,
        &sleeve_config("sleeve_combo", "3.0.0", 0.2, ScoreDirection::Ascending),
    );
    assert_eq!(config.combo_name, "sleeve_combo");
    assert_eq!(config.version, "3.0.0");
    assert_eq!(config.score_direction, ScoreDirection::Ascending);
    assert!(config.score_overlay.is_none());
    assert!(config.portfolio_sleeve.is_none());
    assert_eq!(config.top_n, 7);
}

// ---------------------------------------------------------------------------
// score_source_configs / regime_score_source_configs
// ---------------------------------------------------------------------------

#[test]
fn score_source_configs_without_overlay_or_sleeve_returns_base_only() {
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let configs = score_source_configs(&base);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].combo_name, "base_combo");
}

#[test]
fn score_source_configs_expands_overlay_and_sleeve_deduped() {
    // 业务含义:base + overlay + sleeve 最多展开 3 个独立打分源,按 source key 去重。
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay_config(
            "overlay_combo",
            "1.0.0",
            0.5,
            ScoreDirection::Descending,
        )),
        portfolio_sleeve: Some(sleeve_config(
            "sleeve_combo",
            "1.0.0",
            0.2,
            ScoreDirection::Ascending,
        )),
        ..Default::default()
    };
    let configs = score_source_configs(&base);
    assert_eq!(configs.len(), 3);
    let keys: HashSet<String> = configs.iter().map(|c| c.combo_name.clone()).collect();
    assert!(keys.contains("base_combo"));
    assert!(keys.contains("overlay_combo"));
    assert!(keys.contains("sleeve_combo"));
}

#[test]
fn score_source_configs_dedupes_overlay_matching_base_source() {
    // 边界:overlay 与 base 同 combo/version/direction -> 同 source key,只保留一份。
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let config = SignalConfig {
        score_overlay: Some(overlay_config(
            "base_combo",
            "1.0.0",
            0.5,
            ScoreDirection::Descending,
        )),
        ..base.clone()
    };
    let configs = score_source_configs(&config);
    assert_eq!(configs.len(), 1);
}

#[test]
fn score_source_configs_dedupes_overlay_and_sleeve_sharing_source() {
    // 边界:overlay 与 sleeve 指向同一 combo -> 展开后互相去重,共 2 个源。
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay_config(
            "shared_combo",
            "1.0.0",
            0.5,
            ScoreDirection::Descending,
        )),
        portfolio_sleeve: Some(sleeve_config(
            "shared_combo",
            "1.0.0",
            0.2,
            ScoreDirection::Descending,
        )),
        ..Default::default()
    };
    let configs = score_source_configs(&config);
    assert_eq!(configs.len(), 2);
}

#[test]
fn regime_score_source_configs_includes_base_and_unique_regime_sources() {
    // 业务含义:base + 5 个 regime 变体各自 apply 后按 source key 去重;
    // 未配置规则的 regime 回退 base(或 Mixed 规则),与 base 同 key 被去重。
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let mut rules = HashMap::new();
    rules.insert(
        MarketRegime::Bear,
        RegimeSignalRule {
            combo_name: Some("bear_combo".to_string()),
            ..Default::default()
        },
    );
    let policy = minimal_regime_policy(rules);
    let configs = regime_score_source_configs(&base, &policy);
    let combos: Vec<&str> = configs.iter().map(|c| c.combo_name.as_str()).collect();
    assert_eq!(configs.len(), 2);
    assert!(combos.contains(&"base_combo"));
    assert!(combos.contains(&"bear_combo"));
}

#[test]
fn regime_score_source_configs_expands_regime_overlay_sources() {
    // 业务含义:regime 规则自带的 score_overlay 也会展开为独立打分源。
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let mut rules = HashMap::new();
    rules.insert(
        MarketRegime::Bear,
        RegimeSignalRule {
            combo_name: Some("bear_combo".to_string()),
            score_overlay: Some(overlay_config(
                "bear_overlay",
                "1.0.0",
                0.4,
                ScoreDirection::Descending,
            )),
            ..Default::default()
        },
    );
    let policy = minimal_regime_policy(rules);
    let configs = regime_score_source_configs(&base, &policy);
    let combos: Vec<&str> = configs.iter().map(|c| c.combo_name.as_str()).collect();
    assert_eq!(configs.len(), 3);
    assert!(combos.contains(&"bear_overlay"));
}

#[test]
fn regime_score_source_configs_dedupes_regimes_sharing_one_source() {
    // 边界:Bull 与 Bear 规则产生相同 source key -> 去重为 base + shared 两个源。
    let base = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let mut rules = HashMap::new();
    for regime in [MarketRegime::Bull, MarketRegime::Bear] {
        rules.insert(
            regime,
            RegimeSignalRule {
                combo_name: Some("shared_combo".to_string()),
                ..Default::default()
            },
        );
    }
    let policy = minimal_regime_policy(rules);
    let configs = regime_score_source_configs(&base, &policy);
    assert_eq!(configs.len(), 2);
}

// ---------------------------------------------------------------------------
// score_rows_for_active_config
// ---------------------------------------------------------------------------

fn score_sources_fixture(
    base_config: &SignalConfig,
    base_day_rows: FactorScoresByDate,
    overlay: &FactorScoreOverlayConfig,
    overlay_day_rows: FactorScoresByDate,
) -> HashMap<FactorScoreSourceKey, FactorScoresByDate> {
    let overlay_config = score_source_config_for_overlay(base_config, overlay);
    let mut sources = HashMap::new();
    sources.insert(
        FactorScoreSourceKey::from_config(base_config),
        base_day_rows,
    );
    sources.insert(
        FactorScoreSourceKey::from_config(&overlay_config),
        overlay_day_rows,
    );
    sources
}

#[test]
fn score_rows_for_active_config_returns_none_when_base_source_missing() {
    // 边界:active_config 对应的 base 源不存在 -> None(调用方跳过该日)。
    let day = d(2026, 1, 5);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
    assert!(score_rows_for_active_config(&sources, day, &config).is_none());
}

#[test]
fn score_rows_for_active_config_returns_base_rows_when_base_day_missing() {
    // 边界:base 源存在但该日无分数 -> None。
    let day = d(2026, 1, 5);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let sources = HashMap::from([(
        FactorScoreSourceKey::from_config(&config),
        HashMap::<NaiveDate, Vec<(String, f64)>>::new(),
    )]);
    assert!(score_rows_for_active_config(&sources, day, &config).is_none());
}

#[test]
fn score_rows_for_active_config_returns_base_without_overlay_config() {
    let day = d(2026, 1, 5);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        ..Default::default()
    };
    let expected = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let sources = HashMap::from([(
        FactorScoreSourceKey::from_config(&config),
        HashMap::from([(day, expected.clone())]),
    )]);
    let result = score_rows_for_active_config(&sources, day, &config).expect("base rows");
    assert_eq!(result, expected);
}

#[test]
fn score_rows_for_active_config_falls_back_to_base_when_overlay_source_missing() {
    // 业务含义:overlay 源整体缺失(如 overlay 数据未加载)时回退纯 base 分数。
    let day = d(2026, 1, 5);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay_config(
            "overlay_combo",
            "1.0.0",
            0.5,
            ScoreDirection::Descending,
        )),
        ..Default::default()
    };
    let expected = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let sources = HashMap::from([(
        FactorScoreSourceKey::from_config(&config),
        HashMap::from([(day, expected.clone())]),
    )]);
    let result = score_rows_for_active_config(&sources, day, &config).expect("base rows");
    assert_eq!(result, expected);
}

#[test]
fn score_rows_for_active_config_falls_back_to_base_when_overlay_day_missing() {
    // 边界:overlay 源存在但该日无数据 -> 回退 base 分数。
    let day = d(2026, 1, 5);
    let overlay = overlay_config("overlay_combo", "1.0.0", 0.5, ScoreDirection::Descending);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay.clone()),
        ..Default::default()
    };
    let expected = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let sources = score_sources_fixture(
        &config,
        HashMap::from([(day, expected.clone())]),
        &overlay,
        HashMap::new(),
    );
    let result = score_rows_for_active_config(&sources, day, &config).expect("base rows fallback");
    assert_eq!(result, expected);
}

#[test]
fn score_rows_for_active_config_falls_back_to_base_when_overlay_day_empty() {
    // 边界:overlay 源该日数据为空列表 -> 回退 base 分数(空 overlay 不稀释 base)。
    let day = d(2026, 1, 5);
    let overlay = overlay_config("overlay_combo", "1.0.0", 0.5, ScoreDirection::Descending);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay.clone()),
        ..Default::default()
    };
    let expected = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let sources = score_sources_fixture(
        &config,
        HashMap::from([(day, expected.clone())]),
        &overlay,
        HashMap::from([(day, Vec::new())]),
    );
    let result = score_rows_for_active_config(&sources, day, &config).expect("base rows fallback");
    assert_eq!(result, expected);
}

#[test]
fn score_rows_for_active_config_blends_overlay_z_scores() {
    // 业务含义:overlay 混合 = base_weight * z(base) + overlay_weight * z(overlay),
    // 两侧各自截面标准化,overlay 缺席的票按中性 0 处理。
    // base [1,3] -> z -1/+1;overlay [10,20] -> z -1/+1;weight 0.5 -> AAA -1, BBB +1。
    let day = d(2026, 1, 5);
    let overlay = overlay_config("overlay_combo", "1.0.0", 0.5, ScoreDirection::Descending);
    let config = SignalConfig {
        combo_name: "base_combo".into(),
        score_overlay: Some(overlay.clone()),
        ..Default::default()
    };
    let sources = score_sources_fixture(
        &config,
        HashMap::from([(day, rows(&[("AAA", 1.0), ("BBB", 3.0)]))]),
        &overlay,
        HashMap::from([(day, rows(&[("AAA", 10.0), ("BBB", 20.0)]))]),
    );
    let result = score_rows_for_active_config(&sources, day, &config).expect("blended rows");
    let by_symbol: HashMap<&str, f64> = result
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -1.0);
    assert_close(by_symbol["BBB"], 1.0);
}

// ---------------------------------------------------------------------------
// blend_factor_overlay_scores
// ---------------------------------------------------------------------------

#[test]
fn blend_factor_overlay_scores_zero_weight_returns_base_unchanged() {
    // 边界:overlay 权重 0 -> 直接返回 base,不做任何混合。
    let base = rows(&[("AAA", 1.0), ("BBB", 3.0)]);
    let blended = blend_factor_overlay_scores(
        base.clone(),
        rows(&[("AAA", 10.0)]),
        ScoreDirection::Descending,
        ScoreDirection::Descending,
        0.0,
    );
    assert_eq!(blended, base);
}

#[test]
fn blend_factor_overlay_scores_weight_above_one_clamps_to_full_overlay() {
    // 边界:权重 > 1 被钳制到 1 -> base 权重 0,输出完全由 overlay z 分决定。
    // overlay [10,20] -> z -1/+1(Descending 原样)。
    let blended = blend_factor_overlay_scores(
        rows(&[("AAA", 1.0), ("BBB", 3.0)]),
        rows(&[("AAA", 10.0), ("BBB", 20.0)]),
        ScoreDirection::Descending,
        ScoreDirection::Descending,
        2.0,
    );
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -1.0);
    assert_close(by_symbol["BBB"], 1.0);
}

#[test]
fn blend_factor_overlay_scores_missing_overlay_symbol_treated_as_neutral_zero() {
    // 业务含义:overlay 缺席的票按 overlay_good = 0(中性)混合,不做惩罚;
    // base z: AAA=-1, BBB=+1;AAA 有 overlay(z=-1),BBB 无 -> 0.5*(-1)+0.5*0 = -0.5,
    // BBB = 0.5*(+1)+0.5*0 = +0.5。
    let blended = blend_factor_overlay_scores(
        rows(&[("AAA", 1.0), ("BBB", 3.0)]),
        rows(&[("AAA", 10.0)]),
        ScoreDirection::Descending,
        ScoreDirection::Descending,
        0.5,
    );
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -0.5);
    assert_close(by_symbol["BBB"], 0.5);
}

#[test]
fn blend_factor_overlay_scores_treats_non_finite_scores_as_neutral_or_dropped() {
    // 业务含义:base 分非有限的票整体剔除;overlay 分非有限的票保留,
    // 但其 overlay 贡献按中性 0 处理。stats 基于各自幸存者重算
    // (base 有限 [1,3] -> mean 2 std 1;overlay 有限 [10,20] -> mean 15 std 5)。
    // INF_OVERLAY: base z -1, overlay 贡献 0 -> 0.5*(-1)+0 = -0.5;
    // OK: base z +1, overlay z +1 -> 1。
    let blended = blend_factor_overlay_scores(
        rows(&[("NAN_BASE", f64::NAN), ("INF_OVERLAY", 1.0), ("OK", 3.0)]),
        rows(&[
            ("NAN_BASE", 10.0),
            ("INF_OVERLAY", f64::INFINITY),
            ("OK", 20.0),
        ]),
        ScoreDirection::Descending,
        ScoreDirection::Descending,
        0.5,
    );
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_eq!(by_symbol.len(), 2);
    assert!(!by_symbol.contains_key("NAN_BASE"));
    assert_close(by_symbol["INF_OVERLAY"], -0.5);
    assert_close(by_symbol["OK"], 1.0);
}

#[test]
fn blend_factor_overlay_scores_ascending_base_flips_final_sign() {
    // 方向性:base 为 Ascending(低分好)时,混合后的"好度"取负输出,
    // 使结果仍以"小分=好"编码,供下游 Ascending 排序消费。
    // base z: AAA=-1 -> Ascending good +1;overlay z(Descending): AAA=-1;
    // weight 0.25: AAA good = 0.75*1+0.25*(-1) = 0.5 -> 输出 -0.5;
    // BBB good = 0.75*(-1)+0.25*1 = -0.5 -> 输出 +0.5。
    let blended = blend_factor_overlay_scores(
        rows(&[("AAA", 1.0), ("BBB", 3.0)]),
        rows(&[("AAA", 10.0), ("BBB", 20.0)]),
        ScoreDirection::Ascending,
        ScoreDirection::Descending,
        0.25,
    );
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -0.5);
    assert_close(by_symbol["BBB"], 0.5);
}

#[test]
fn blend_factor_overlay_scores_equal_weights_average_both_z_scores() {
    // 正常路径:0.5/0.5 等权 -> 两侧 z 分均值。
    // base [1,3] -> z -1/+1;overlay [10,20] -> z -1/+1 -> AAA -1, BBB +1。
    let blended = blend_factor_overlay_scores(
        rows(&[("AAA", 1.0), ("BBB", 3.0)]),
        rows(&[("AAA", 10.0), ("BBB", 20.0)]),
        ScoreDirection::Descending,
        ScoreDirection::Descending,
        0.5,
    );
    let by_symbol: HashMap<&str, f64> = blended
        .iter()
        .map(|(symbol, score)| (symbol.as_str(), *score))
        .collect();
    assert_close(by_symbol["AAA"], -1.0);
    assert_close(by_symbol["BBB"], 1.0);
}
